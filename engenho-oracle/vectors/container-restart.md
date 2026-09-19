# Container restart, unknown-exit rendering and CrashLoopBackOff: upstream oracle notes

Pinned to kubernetes/kubernetes **v1.34.0**. Every file was fetched raw from
`https://raw.githubusercontent.com/kubernetes/kubernetes/v1.34.0/<path>` on 2026-09-19.
Vectors are in `container-restart.json`: **155 cases**. 108 are `upstream_test`, meaning an assertion in an upstream `_test.go`. The other 47 are `upstream_source`, meaning I traced them from the cited source lines and no upstream test covers them directly.
kube-rs has no kubelet logic here: a GitHub code search of `kube-rs/kube` for `CrashLoopBackOff` and
`ContainerStatusUnknown` returned zero hits. Because of that, every rule below comes from kubernetes/kubernetes.

Feature-gate defaults at v1.34.0 (`pkg/features/kube_features.go`): `ContainerRestartRules`=false (alpha 1.34),
`ReduceDefaultCrashLoopBackOffDecay`=false (alpha 1.33), `KubeletCrashLoopBackOffMax`=false (alpha 1.32).

## 1. ShouldContainerBeRestarted (`pkg/kubelet/container/helpers.go:82-117`)

The checks run in this order. The first one that matches decides the result:

1. Pod has a deletionTimestamp: **false**.
2. There is no runtime record for the container name: **true**. This applies under `Never` too.
3. The newest record is `running`: **false**.
4. The newest record is `unknown` or `created`: **true**. This applies under `Never` too.
5. (Only when `ContainerRestartRules` is on) the result comes from `podutil.ContainerShouldRestart`.
6. Pod policy is `Never`: **false**.
7. Pod policy is `OnFailure` and exit code == 0: **false**.
8. Anything else: **true**. This covers `Always`, an empty policy, and `OnFailure` with any non-zero exit code, including -1 and 137.

"Newest record" means the **first** status whose name matches (`runtime.go:417-425`). kuberuntime sorts statuses
newest-first by CreatedAt (`kuberuntime/helpers.go:62`).

```go
// NOTE(random-liu): If all historical containers were GC'd, we'll also return true here.
if status == nil { return true }
...
// Always restart container in the unknown, or in the created state.
if status.State == ContainerStateUnknown || status.State == ContainerStateCreated { return true }
```

With the gate on, `ContainerShouldRestart` (`pkg/api/v1/pod/util.go:419-455`) checks in this order:

1. The container's `restartPolicyRules`, first match wins. They are consulted **only if the container-level `restartPolicy` is non-nil**.
2. The container-level `restartPolicy`.
3. The pod `restartPolicy`, where the default arm is `true`.

A rule whose `exitCodes` is nil never matches (`util.go:462`). `IsContainerRestartable` disagrees and still
counts that rule as restartable (`util.go:403-413`).

## 2. How an unobserved or unknown exit is rendered (`pkg/kubelet/kubelet_pods.go`, `convertToAPIContainerStatuses`)

Two separate code paths produce `ContainerStatusUnknown` with exit code 137. Their messages differ and so does their handling of restartCount:

| Situation | `state` | `lastState` | restartCount |
|---|---|---|---|
| The CRI still lists the container but reports UNKNOWN, and the previous API status was Running (`:2138-2153`) | `terminated{137, ContainerStatusUnknown, "The container could not be located when the pod was terminated"}` | from other records | old + 1, **always** |
| The CRI no longer lists the container at all, the previous status was Running, it has no prior lastState, and the current status is the default waiting status (`:2314-2378`) | `waiting{ContainerCreating}` (`PodInitializing` if the pod has init containers) | `terminated{137, ContainerStatusUnknown, "The container could not be located when the pod was deleted.  The container used to be Running"}` | old + 1 **only if the pod is not being deleted** |
| The CRI reports UNKNOWN and the previous status was not Running, or there is none | `waiting{}` with an **empty** reason | - | from the CRI |
| The container vanished and the previous status was Terminated | the old status, carried over as-is | old | old |

Quotes:
```go
Reason:   kubecontainer.ContainerReasonStatusUnknown,   // "ContainerStatusUnknown", runtime.go:288
Message:  "The container could not be located when the pod was deleted.  The container used to be Running",
ExitCode: 137,
...
// If the pod was not deleted, then it's been restarted. Increment restart count.
if pod.DeletionTimestamp == nil { status.RestartCount += 1 }
```

**How restartPolicy affects this.** The 137 rendering itself does not depend on the policy. The policy has two indirect effects:
- **Pod phase** (`getPhase`, `:1760-1771`, `:1829-1845`). A `waiting` container that has a terminated `lastState` counts as *stopped*. Exit 137 is not success. When every container is in that state, the phase is `Always` → **Running**, `OnFailure` → **Running**, and `Never` → **Failed**.
- **The final Waiting override** (`:2424-2459`). A container is moved to `waiting{reason: <cached error>}`, with its old terminated state moved into `lastState`, only when both of these hold:
  - `ShouldContainerBeRestarted` is true.
  - The kubelet's reason cache holds an entry for that container.

  Without a cached reason, a crashed `Always` container stays `terminated`. It is not shown as `waiting`.

## 3. CrashLoopBackOff

Constants (`pkg/kubelet/kubelet.go`, `apis/config/v1beta1/defaults.go:46`):

| Setting | Initial | Max |
|---|---|---|
| Default | 10s (`initialCrashLoopBackOff`) | 300s (`MaxContainerBackOff`) |
| `ReduceDefaultCrashLoopBackOffDecay` on | 1s | 60s |
| `KubeletCrashLoopBackOffMax` on | unchanged, but clamped down to the node max if the node max is lower | node config value |

- The multiplier is 2 with no jitter (`NewBackOff`, jitter 0). The sequence is 10, 20, 40, 80, 160, 300, 300, ...
- The reset rule is hard-coded and does **not** use client-go's `2*max` default:

```go
klet.crashLoopBackOff.HasExpiredFunc = func(eventTime time.Time, lastUpdate time.Time, maxDuration time.Duration) bool {
    return eventTime.Sub(lastUpdate) > 600*time.Second
}
```

How `doBackOff` works (`kuberuntime_manager.go:1607-1638`):
- `eventTime` is the **FinishedAt of the newest exited record**.
- It is in backoff when `now - FinishedAt < current` (strict `<`), and when it is, the container shows `Waiting` with reason `CrashLoopBackOff`.
- The message is `back-off <Go duration> restarting failed container=<name> pod=<name>_<ns>(<uid>)`. The event reason is `BackOff`.

The backoff key is an FNV-64a hash of `name/namespace/uid/containerName/image/resources.String()` (`kuberuntime/helpers.go:182-196`).

## Surprises (the places a naive reimplementation goes wrong)

1. **The reset is based on how long the container ran, not on wall-clock time.** The 600s is compared against `FinishedAt - lastUpdate`, and `lastUpdate` is set when the kubelet last called `Next` (just before the restart). The backoff resets only if the container **ran for more than 10 minutes** before it exited. If a container crashes after a short run, waits an hour, and restarts, the backoff still doubles. The threshold stays at 600s under the reduced-decay gate as well (60s max), not 120s.
2. **The first crash restarts with no delay.** There is no entry yet, so `IsInBackOffSince` is false. The first `Next` then sets the entry to 10s. The 10s delay applies before the *second* restart.
3. **`Never` does not always mean never.** A container with no runtime record, or in the `created` or `unknown` state, is started under `Never`. `computePodActions` kills and restarts an UNKNOWN container under `Never` (upstream test). A change to the spec hash restarts the container whatever the policy.
4. **A vanished container shows as `Waiting/ContainerCreating`, not `Terminated`.** The 137 goes into `lastState`, and restartCount is not incremented when the pod is being deleted. The two unknown paths use different message strings, and the "deleted" one has **two spaces** after the period. The CRI-UNKNOWN path increments restartCount even for a deleted pod.
5. **Under `Always`, a pod whose containers all exited 0 reports `Running`, not `Succeeded`,** unless the pod is terminal. An empty `restartPolicy` behaves like `Always` inside the kubelet.
6. **The backoff message uses Go duration format**: `1m20s`, `2m40s`, `5m0s`, never `300s`. An image or resources change produces a new key, which resets the backoff. A change to command or env does not.
7. When `ContainerRestartRules` is off (the 1.34 default), `ShouldContainerBeRestarted` ignores the container-level `restartPolicy` entirely.
