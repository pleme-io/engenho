# Watch cache: 410, too-large RV, bookmarks and slow watchers

Upstream versions: kubernetes `v1.34.0` (`staging/src/k8s.io/apiserver/pkg/storage/cacher/*`, plus `storage/errors.go`, `endpoints/handlers/{watch,get}.go`, `features/kube_features.go`, apimachinery `errors.go` and `validation.go`, client-go `reflector.go`) and kube-rs `4.2.0` (`kube-runtime/src/watcher.rs`, `kube-core/src/params.rs`). Every file was fetched raw from GitHub at the tag. The fixture file is `watch-410-bookmark.json`, with 82 cases: 45 asserted by an upstream test and 37 read directly off the code. Each case carries an `evidence` field saying which.

Feature gates in 1.34 that matter here:

| Gate | 1.34 value |
|---|---|
| `WatchList` | Beta, default **true** (it was false in 1.33) |
| `ResilientWatchCacheInitialization` | GA, locked true |
| `WatchFromStorageWithoutResourceVersion` | Deprecated, locked **false** |
| `ListFromCacheSnapshot` | Beta, true |

## Rules

### 1. Too old → 410

- The floor is computed as follows:
  - If the cache has relisted and no event has been evicted since, `oldest = listRV + 1`.
  - Otherwise `oldest` is the RV of the first buffered event.
- A watch fails when `rv < oldest-1`, so `rv == oldest-1` is accepted.
- `sendInitialEvents=true` skips this check completely.
- `rv=0` with `sendInitialEvents` unset replays the whole store as ADDED events.
- `rv=0` with `sendInitialEvents=false` starts at the cache's current RV and sends no initial state.

```go
// watch_cache.go:885-916
case w.listResourceVersion > 0 && !w.removedEventSinceRelist:
    oldest = w.listResourceVersion + 1
case size > 0:
    oldest = w.cache[w.startIndex%w.capacity].ResourceVersion
...
if resourceVersion < oldest-1 {
    return nil, errors.NewResourceExpired(fmt.Sprintf("too old resource version: %d (%d)", resourceVersion, oldest-1))
```

- The status is `{code:410, reason:"Expired"}`. The reason is **not** `"Gone"`.
- Once the RV has parsed, every error is sent as **one ERROR event on an HTTP 200 chunked stream**, and then the stream closes (`cacher.go:612-615`, `handlers/watch.go:240`):
  > "To match the uncached watch implementation, once we have passed authn/authz/admission, and successfully parsed a resource version, other errors must fail with a watch event of type ERROR, rather than a directly returned error."
- Two errors are returned directly instead:
  - an unparseable RV;
  - an unready cache, which returns 429 with `retryAfterSeconds = int(min(30, max(1, e^(0.06·downtime_s))))`.

### 2. RV ahead of the cache: 504 only for watch-list

- `blockTimeout = 3s` and `resourceVersionTooHighRetrySeconds = 1`.
- The wait happens **only** when `sendInitialEvents == true` (`cacher.go:1248-1266`). It returns:
  - `504 reason=Timeout`
  - message `"Timeout: Too large resource version: 105, current: 100"`
  - `details.retryAfterSeconds=1`
  - `causes=[{reason:"ResourceVersionTooLarge", message:"Too large resource version"}]`

  This comes from `storage/errors.go:233-242` and is asserted in `TestWaitUntilWatchCacheFreshAndForceAllEvents`.
- A **plain watch ahead of the cache is accepted immediately**:
  - it does not block;
  - it gets no 504 and no 410;
  - it gets no events and no bookmarks until the cache passes its RV.

  `TestEmptyWatchEventCache` "RV+1" asserts that the watch "remained established".
- An event whose RV equals the requested RV is not delivered, because `process()` only sends `event.ResourceVersion > resourceVersion` (`cache_watcher.go:538`).
- With RV unset and `sendInitialEvents` unset, the required RV is **0**. The watch is served from the cache, which can be stale, exactly like `rv=0`. The API docs describe this as "most recent"; the code does not implement that.
- With `sendInitialEvents` set and RV unset, the required RV is the current etcd RV, and the request can return 504.

### 3. Bookmarks

- **Periodic tick:** the cacher ticks every `wait.Jitter(1s, 0.25)`. It sends a bookmark carrying `lastProcessedResourceVersion`, and only to watchers whose 1-second time bucket has expired.
- **Bucket schedule:** the next bucket is `min(now+60s, deadline-2s)`. If the watch has no deadline, it is `now+60s`. If `deadline-2s` is already in the past, nothing is scheduled. A watch-list watcher still waiting for its bookmark-after-RV is scheduled on every tick.
- **Storage bookmarks:** bookmarks coming from storage (progress-notify) are **never forwarded**. They only advance `lastProcessedResourceVersion` (`cacher.go:868-881`).
- **Plain watchers:** these start in state `BookmarkSent`. A bookmark whose RV equals the watcher's RV is dropped, and bookmarks are never annotated.
- **Watch-list requests:** these need `sendInitialEvents=true` and `allowWatchBookmarks=true`. They get exactly one bookmark annotated `k8s.io/initial-events-end: "true"`. Its RV is the cache RV at registration, or the requested RV, or the etcd RV when RV is unset. Validation does **not** require bookmarks with `sendInitialEvents`. Without them the request is valid, but that bookmark never arrives.
- **Bookmark object:** it is `newFunc()` with only `metadata.resourceVersion` set.
- **Full buffers:** bookmarks use only `nonblockingAdd`. They never block and never kill a watcher. If the buffer is full, the bookmark is dropped:
  > "Note that bookmark events are never added via the add method only via the nonblockingAdd." (`cache_watcher.go:165-167`)

### 4. Unresponsive and overflowing watchers

- **Channel size:** `chanSize = clamp(ceil(capacity/75s), 10, {10 | 1000 | 100})`. The ceiling depends on whether the index and trigger exist. Each watcher has an **input and a result buffer** of that size, plus one event in flight.
- **Dispatch:** for each event, the cacher first runs a non-blocking pass over all watchers. The blocked watchers then share **one timer**, set to `dispatchTimeoutBudget.takeAvailable()`. After the timer fires, every remaining blocked watcher is closed without waiting (`timer=nil`).
- **Budget:** it refills at 50ms per second, is capped at 100ms, **starts at 0**, and unused time is returned.
- **Graceful drain:** a closed watcher is drained only when its state is `BookmarkReceived` but the bookmark has not yet been sent. Every other watcher is stopped at once.
- **What the client sees:** the watch just ends, with **no ERROR event**. Events still in the input buffer are dropped.
- **Cache wrap during replay:** if the ring buffer wraps under a watcher that is still replaying history, the watch closes silently after the events it already got. That is 100 in `TestCacheIntervalInvalidationStopsWatch`. Upstream chose this on purpose: "because historically such events weren't sent out of the watchCache, we decided not to".
- **DELETED events:** when an object stops matching a selector, the DELETED event carries the old object body stamped with the **event's** RV.

### 5. Clients

- **kube-rs:**
  - An ERROR event with `code == 410`, and only that code, moves the watcher to `Empty`, so it relists (default `ListSemantic::MostRecent`).
  - Any other ERROR, including 504, keeps the RV.
  - A clean end of stream moves it to `InitListed{last rv}`, and it re-watches from that RV.
  - Its streaming list sends `resourceVersion=0&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&timeoutSeconds=290`, so the cacher can never return 504 to it.
- **client-go:**
  - On a watch 410 it relists at the **last-synced RV** first.
  - It falls back to `resourceVersion=""` only if that list itself returns 410 or too-large.

## Surprising findings

1. **A plain watch ahead of the cache is never rejected.** 504 is only for watch-list requests (and for LIST/GET with an RV). A naive implementation that returns 504 or 410 here is wrong, and so is one that sends bookmarks at the cache's RV.
2. **An RV-unset watch in 1.34 is `rv=0` in disguise.** The gate `WatchFromStorageWithoutResourceVersion` is locked off, so the watch is served from the cache and is not a quorum read.
3. **The 410 floor is conservative.** The cacher does not remember the last evicted RV. After one eviction since the relist, `rv=15` against `listRV=10` still gets 410 (`TestCacheIncreaseDoesNotBreakWatch`).
4. **`sendInitialEvents=true` never returns 410**, however old the RV.
5. **Watchers are killed silently.** When a slow watcher is killed, or its interval is invalidated, the stream ends with no ERROR. The client only learns it fell behind when its re-watch gets 410.
6. **The first blocked watcher on a fresh cacher gets zero wait budget**, because the budget starts at 0.
7. **One upstream test proves nothing.** `TestResourceVersionAfterInitEvents` calls `w.stopLocked()`, which closes `done`, before `processInterval`. Its ten `<-w.ResultChan()` reads then succeed on a closed channel. The test passes without any event being delivered, so its dedup rule is recorded as `evidence: code`.
8. **An upstream test name is wrong.** In `TestInitialEventsEndBookmark`, the scenario named `"allowWatchBookmarks=false, sendInitialEvents=nil"` actually sets `allowWatchBookmarks: true`. The fixture records what the code does.
