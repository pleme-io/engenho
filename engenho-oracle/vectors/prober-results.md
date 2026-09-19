# Kubelet prober result handling: upstream oracle notes (kubernetes v1.34.0)

Every file listed below was downloaded from `raw.githubusercontent.com/kubernetes/kubernetes/v1.34.0/...` and read in full. The oracle is `prober-results.json`, with 93 cases. kube-rs was not used as a source because this behaviour lives in the kubelet, not in an API client.

## Fixture vocabulary
- `probe` in a `worker_sequence` step is the value `prober.probe()` would return if called. `S` means `(Success, nil)`, `F` means `(Failure, nil)` and `E` means `(Failure, err)`.
- `result` is the cached `results.Manager` value for that step's container ID after the tick.
- Low-level handlers return `probe.Result`, one of `success`, `warning`, `failure` or `unknown`. The kubelet collapses that to `Success` or `Failure` (see the mapping below).
- An `upstream_ref` starting with `test:` is asserted by an upstream Go test. One starting with `code:` is derived by reading the code; no upstream test asserts it.

## The rules in plain words

**1. Errors are thrown away, not counted.** If `prober.probe` returns a non-nil error, `doProbe` returns before it touches `lastResult` or `resultRun`. An error does not count toward either threshold, does not break a run, and does not change the cached result.
```go
result, err := w.probeManager.prober.probe(ctx, w.probeType, w.pod, status, w.container, w.containerID)
if err != nil {
    // Prober error, throw away the result.
    return true
}
```
(worker.go:297-301)

**2. Error vs. failure is decided per handler:**
| Handler | Counted Failure (`err == nil`) | Discarded (`err != nil`) |
|---|---|---|
| exec | non-zero exit code (any); timeout while `ExecProbeTimeout` is on (the default, GA) | a runtime error that is not an ExitError; timeout while the gate is off |
| http | transport error, connect refused, timeout before the response, status outside 200-399 | port resolution failure; an error reading the body (not the 10 KiB truncation) |
| tcp | any dial error, including timeout | port resolution failure |
| grpc | everything: "err is always nil" | never |
| none | n/a | no handler: `missing probe handler for ...` |

A handler error is retried in the same tick, up to `maxProbeRetries = 3` attempts, and only when `err != nil` (prober.go:41, 134-147). So a flaky runtime error followed by a success gives Success within one tick.

**3. Mapping from handler result to kubelet result** (prober.go:109-131): `success` becomes Success. `warning` becomes Success and records a `ProbeWarning` event. `failure` becomes Failure. `unknown` with no error becomes a **counted** Failure. Any other value becomes Failure.

**4. Thresholds count consecutive identical verdicts:**
```go
if w.lastResult == result { w.resultRun++ } else { w.lastResult = result; w.resultRun = 1 }
if (result == results.Failure && w.resultRun < int(w.spec.FailureThreshold)) ||
    (result == results.Success && w.resultRun < int(w.spec.SuccessThreshold)) {
    return true   // below threshold: cached state unchanged
}
w.resultsManager.Set(w.containerID, result, w.pod)
```
While a run is below its threshold, the cached value stays at whatever it was before. On the first ticks that is the initial value: liveness=Success, readiness=Failure, startup=Unknown (worker.go:102-115).

**5. Hold.** After liveness Sets Failure, and after startup Sets anything (Success or Failure), the worker sets `onHold = true, resultRun = 0` and stops probing until it sees a new container ID. A new container ID Sets the initial value, then clears `onHold` (worker.go:235-248, 329-339). Readiness never holds.

**6. Order of gates in each tick** (worker.go:209-294):
1. No pod status: continue.
2. Pod phase Failed or Succeeded: stop, and nothing is Set.
3. No container status, or its ID is empty: continue.
4. New container ID: Set the initial value.
5. `onHold`: continue.
6. Container not running: Set **Failure** for all three types. Stop only if the container is Terminated and RestartPolicy is Never.
7. Pod has a DeletionTimestamp: liveness and startup Set Success and stop; readiness keeps probing.
8. `int32(elapsedSeconds) < initialDelaySeconds`: continue.
9. Started gate: liveness and readiness are skipped unless `Started` is true (nil counts as not started); startup is skipped once `Started` is true.

**7. HTTP success range:** `200 <= code < 400`. A 3xx that reaches this point is `warning`, which the kubelet counts as Success. The kubelet does not follow redirects to a different hostname (`followNonLocalRedirects = false`), so such a redirect returns the 302 itself and counts as Success. Hostname comparison ignores the port. A redirect loop stops after 10 hops and counts as Failure. The failure message is exactly `HTTP probe failed with statuscode: %d` and never includes the body. The body is capped at 10240 bytes, and truncation is not an error.

**8. Timeouts:** `timeoutSeconds` defaults to 1. HTTP uses `http.Client.Timeout`, TCP uses `Dialer.Timeout`, and gRPC uses a context deadline; all three count as Failure. For exec, the CRI gRPC deadline is the runtime timeout (default 2m) plus the probe timeout. `DeadlineExceeded` is wrapped as `ErrCommandTimedOut`, which counts as Failure only while `ExecProbeTimeout` is on.

**9. Defaults and validation:** timeout 1, period 10, successThreshold 1, failureThreshold 3. Liveness and startup `successThreshold` must be 1. Readiness may not set `terminationGracePeriodSeconds`.

## Surprises (where a naive reimplementation goes wrong)
1. **An unresolvable named port leaves the probe at its initial value forever.** Validation only checks the syntax of a port name, not whether the container has that port. At runtime every tick fails three times with `strconv.Atoi: parsing "http": invalid syntax`, and every result is discarded. The effect by probe type:
   - Liveness stays Success, so the container is never restarted.
   - Readiness stays Failure, so the container is never Ready.
   - Startup stays Unknown, so the container never starts, is never restarted, and liveness and readiness never run.

   The error text comes from the `Atoi` fallback, not "port not found", because `util.go:36` overwrites the first error.
2. **The readiness success run carries across a container restart.** A new container ID resets only `onHold`, not `lastResult` or `resultRun`. With `successThreshold: 3`, a restarted container becomes Ready on its **first** successful probe if the previous container's run had already reached 3. No upstream test covers this; it is derived from worker.go:235-243 and 314-327.
3. `unknown` **without** an error counts as a Failure, while `unknown` **with** an error is discarded. An HTTP body-read error (for example, the client timeout firing while the body is streaming) is discarded, but a timeout before the headers arrive is counted.
4. A non-running container Sets **Failure** even for liveness and startup, and this happens before the deletion branch. So a deleted pod whose container is not running shows Failure, not the "quiet shutdown" Success.
5. Initial delay truncates elapsed time: 9.999s is not enough for a 10s delay.
6. `results.Result`'s zero value is `Success`, so `lastResult` starts as Success. Behaviour is unaffected because `resultRun` starts at 0.
7. exec output is capped at 10240 bytes. The comment in `exec_test.go` says the input is "11KB", but `Repeat("logs-123", 8*128*11)` is actually 90112 bytes.
8. gRPC probes always dial the pod IP (there is no host field), and the gRPC prober's error return is always nil.

## Files read (all @v1.34.0)
pkg/kubelet/prober/{worker.go, worker_test.go, common_test.go, prober.go, prober_test.go, prober_manager.go, prober_manager_test.go, results/results_manager.go}; pkg/probe/{probe.go, util.go, exec/exec.go, exec/exec_test.go, http/http.go, http/http_test.go, http/request.go, tcp/tcp.go, grpc/grpc.go}; pkg/features/kube_features.go; pkg/apis/core/v1/defaults.go; pkg/apis/core/validation/validation.go; staging/src/k8s.io/cri-client/pkg/remote_runtime.go; pkg/kubelet/kuberuntime/kuberuntime_container.go; pkg/kubelet/util/ioutils/ioutils.go; pkg/kubelet/events/event.go.
