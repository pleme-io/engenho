# How watch clients react to 429, 410 and 504

These notes back the oracle data in `watch-429-clients.json` (68 cases). Sources are client-go and apiserver from `kubernetes/kubernetes@v1.34.0`, `kube-rs/kube@4.2.0`, `backon` v1.3.0–v1.6.0 and `tower` 0.5.2. Each case's `upstream_ref` names the function and line.

## client-go: three layers, in order

### 1. REST layer (`rest/request.go` `Watch`, `rest/with_retry.go`)
- A watch that gets HTTP 429 or any status of 500 or above is retried **inside the REST client**. This only happens when a `Retry-After` header parses with `strconv.Atoi`. The client makes up to 11 attempts in total (`maxRetries: 10`) and sleeps exactly the header value before each retry. Retries after the first attempt also wait on the client-side rate limiter. The first watch attempt does not.
  - `case r == http.StatusTooManyRequests, r >= 500:` (checkWait)
  - `if i, err := strconv.Atoi(h); err == nil {` (retryAfterSeconds). An HTTP-date `Retry-After` is silently ignored, so there is no retry.
- HTTP 410 is never retried here, even when it carries `Retry-After`.
- The `details.retryAfterSeconds` field in the body is not read. Only the header counts. The apiserver copies it into the header itself: `if status.Details != nil && status.Details.RetryAfterSeconds > 0 { ... w.Header().Set("Retry-After", delay)` (writers.go `ErrorNegotiated`).
- A JSON `Status` in the body with `status: Failure` **replaces** the error built from the HTTP code (`Result.Error`). The body's code and reason then decide the branch, not the HTTP status.
- A transport EOF or timeout is retried with a synthetic `Retry-After: 1`. When retries run out, `Watch` returns `watch.NewEmptyWatch(), nil`, not an error.

### 2. Reflector watch loop (`tools/cache/reflector.go` `watch()`)
- Error at watch start: `isWatchErrorRetriable` (ECONNREFUSED or `IsTooManyRequests`) triggers backoff and a new watch at `LastSyncResourceVersion`, with **no LIST**. Any other start error is returned from `ListAndWatch`. The `watchErrorHandler` runs, then `BackoffUntil` backs off, then the reflector re-LISTs.
- In-band ERROR events are handled by `apierrors.FromObject(event.Object)`, and the switch order matters:
  ```go
  case isExpiredError(err):            // IsResourceExpired || IsGone -> falls through to `return nil` => relist
  case apierrors.IsTooManyRequests(err):
      ... case <-r.backoffManager.Backoff().C(): continue   // re-watch, no relist
  case apierrors.IsInternalError(err) && retry.ShouldRetry(): continue  // immediate, no backoff
  default: logger.Info("Warning: watch ended with error", ...)          // relist
  ```
- Behaviour after a 410 (in-band or at watch start):
  - It does **not** set `isLastSyncResourceVersionUnavailable`. The code says: "Don't set LastSyncResourceVersionUnavailable - LIST call with ResourceVersion=RV already has a semantic that it returns data at least as fresh as provided RV."
  - The relist therefore uses the **last seen RV**, not `""`.
  - Only if that LIST also returns 410 or TooLarge does the reflector set the flag and immediately re-LIST with `RV=""`. The upstream test sequence is `["0","10",""]`.
- A 504 or TooLarge error on the **watch** path goes to `default`, which relists at the last RV. Only the LIST path checks `isTooLargeResourceVersionError`.
- A watch that closes with no error re-watches **immediately**, with no backoff and no relist.
- Watch start is timed from `start` (taken before the request). If the watch then closes in under 1s with 0 events, the reflector returns `VeryShortWatchError` and relists. Bookmarks count as events. Objects of the wrong type do not.
- The internal-error retry window is off by default (`MaxInternalErrorRetryDuration` is 0, and informers never set it).
- The WatchList path (`WatchListClient`, default **false** in 1.34) checks the same predicates in the **opposite order**. It tests 429 first, then expired. On 410 it retries immediately with `RV=""` and no backoff.

### 3. Outer loop and backoff
- `wait.BackoffUntil(..., r.backoffManager, true /*sliding*/, ...)` calls `Backoff()` after **every** `ListAndWatch` return, including `nil`. So a relist after a 410 always waits one backoff first.
- The reflector uses a single backoff manager for both paths: `NewExponentialBackoffManager(800*time.Millisecond, 30*time.Second, 2*time.Minute, 2.0, 1.0, reflectorClock)`. The 429 path and the relist path therefore advance the same exponential state.
- Sleep n falls in `[base_n, 2·base_n)` with bases 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, then 30 s. The cap applies to the base, not the jittered value. The sixth sleep can therefore be up to 51.2 s, and the steady state is `[30 s, 60 s)`.
- The backoff resets only when the gap between two `Backoff()` calls is **strictly** greater than 2 min.

### Error predicates (`apimachinery/pkg/api/errors`)
- `IsTooManyRequests` is true when the reason is TooManyRequests **or** the code is 429, even if the reason is a different known reason. The code notes: "does not check that the reason is unknown".
- `IsGone` is true when the reason is Gone, or the code is 410 with an *unknown* reason. `IsResourceExpired` checks the reason only, and ignores the code.
- As a result, `{code:429, reason:Expired}` relists on the watch path. `{code:410, reason:TooManyRequests}` backs off and re-watches. `{code:500, reason:Expired}` relists.

## kube-rs `watcher` (4.2.0)
- The state machine relists **only** on an in-band `WatchEvent::Error` whose `code == 410` (`// HTTP GONE, means we have desynced and need to start over and re-list :(`). The reason field is ignored.
- The relist has no RV by default (`ListSemantic::MostRecent`, a quorum read). client-go relists at the last seen RV.
- Any other in-band error keeps the **same stream and RV**. When the stream ends, the watcher goes to `InitListed{rv}` and re-watches. So 429 and 504 never cause a relist.
- `Client::request_events` does **not** check the HTTP status. It has no `handle_api_errors`, unlike `request_text`. A 410/429/504 response body is parsed line by line as a `Status` and becomes `Err(Error::Api(..))`, which surfaces as `WatchFailed` with the state kept.
- **An HTTP-level 410 never triggers a relist in kube-rs.** The watcher re-watches the dead RV forever.
- The HTTP layer (`default_retry: true`) retries 429, 503 and 504 up to 15 times per request. It does not retry 500 or 502.
  - The sleep is `max(backoff, Retry-After)`. The backoff base is 5 ms·2^i, capped at 1000 s.
  - `RetryPolicy::new` passes `2.0` into tower's **jitter** parameter, not a growth factor. The sleep is therefore about `[base, 3·base)`.
- `watcher()` on its own has no backoff. `.default_backoff()` and `Controller` add one: `ResetTimerBackoff(Exponential(800ms, 30s, 2.0, jitter), 120s)`. `StreamBackoff` resets it on **any** `Ok` item, including `Event::Init`.
- The jitter depends on which backon version is resolved (kube-rs requires `backon = "1.3"`):
  - v1.3.0–v1.4.0: `cur + min_delay·rand`, so `[base, base+0.8s)`.
  - v1.4.1 and later: `cur + cur·rand`, so `[base, 2·base)`.

## What engenho must emit
- The apiserver sends post-admission watch errors (410 too-old RV, 504 TooLarge) **in-band**: HTTP 200, one ERROR event, then close (`newErrWatcher`). "Once we have passed authn/authz/admission ... other errors must fail with a watch event of type ERROR." Anything that is not a StatusError becomes `{500, InternalError}`.
- It sends a 429 at the **HTTP level**, before any watcher exists, when the watch cache is unready. `retryAfterSeconds` is `int(clamp(e^(0.06·downtime_s), 1, 30))`, and `ErrorNegotiated` copies it into `Retry-After`.
- A 410 sent as an HTTP status is handled correctly by client-go, but it traps kube-rs in a loop. Keep `code` and `reason` consistent, because the two clients disagree whenever they don't.

## Surprising
1. **kube-rs never relists on an HTTP-level 410.** Only an in-band ERROR event with code 410 triggers a relist.
2. client-go absorbs up to 10 retries of a 429 that carries `Retry-After` inside `rest.Request.Watch`, so the reflector never sees them. An HTTP-date `Retry-After` turns this off.
3. client-go relists after a 410 at the *last seen* RV, not `""`, and only after a backoff sleep. That sleep shares its exponential state with 429 backoffs.
4. The watch path and the WatchList path check 429 and expired in opposite orders.
5. When transport EOF retries run out, `Watch` returns an empty watch that lasted ≥10 s. That is not a "very short watch", so the reflector re-watches without backoff or relist.
