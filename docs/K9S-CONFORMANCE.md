# k9s conformance

"Every k9s action works on engenho" is a test, not a claim:
`cargo test -p engenho-apiserver --test k9s_conformance`, run in CI with the
rest of the workspace.

The matrix is data in
[`engenho-apiserver/tests/k9s_conformance/matrix.rs`](../engenho-apiserver/tests/k9s_conformance/matrix.rs):
one `K9sAction` per thing a k9s user does, one `Row` per request that action
puts on the wire, with the request shape k9s v0.50.16 sends and the answer
upstream gives. The suite fails when an action has no row, when a row's
answer diverges, and when a not-served row (`Expected::Absent`) stops
degrading the way k9s needs (absent from discovery, a clean 404, no 101).
Turning an `Absent` row into a served one is the way a gap closes.

Beyond the rows, the suite holds what no single request shows: 40 watches and
40 log follows multiplexed on one HTTP/2 connection, all delivering, other
requests still prompt beside them, and cancelled follows no longer polling.

## Where the matrix came from

Mined from k9s source (`internal/dao`, `internal/client`, `internal/watch`)
and checked against a real k9s session on ryn (2026-10-13, headless in a mado
terminal, `--logFile` at debug level). Shapes worth knowing:

- Every action is gated by a `SelfSubjectAccessReview` first.
- Logs always ask `follow=true&timestamps=true&tailLines=N`. A pod without a
  default-container annotation and with more than one container gets one
  follow per container, **init containers included**, each retried 20 times
  with backoff on error.
- Head mode is `limitBytes=5000` without follow; `1m`..`1h` are `sinceSeconds`.
- Shell runs `kubectl exec` (WebSocket v5); port-forward dials SPDY tunnelled
  in WebSocket (`SPDY/3.1+portforward.k8s.io`), then plain SPDY.
- Drain uses kubectl's drain helper: eviction when discovery lists
  `pods/eviction`, delete otherwise.
- CPU/MEM columns appear only when discovery lists `metrics.k8s.io`.

## The Logs-view hang (2026-10-13)

k9s on `pangea-system/pangea-pangea-operator-*` sat in its log retry loop:
the pod has an init container (`pg-wait`), k9s fanned out a follow for it,
and the in-process `/log` path resolved `?container=` against app containers
only, so every attempt was a 404. The kubelet's own HTTP API already fell
back to init containers; the two lookups had drifted. One resolver,
`LocalPod::container_named`, now serves both.

## Logs carry their time (2026-10-13)

The native backend writes container logs as CRI records
(`<RFC3339Nano> <stdout|stderr> <F|P> <msg>`), stamped by the kubelet as each
line arrives (`engenho-kubelet/src/cri_log.rs`). `timestamps=true` prefixes
each line with that stamp and nothing else, so k9s strips the stamp and not a
word of the line; `sinceSeconds` and `sinceTime` filter on it. A follow
resumes from a cursor the backend returns (a record offset on native, a line
count on podman), never from the length of the text it printed, which with
timestamps on was a different number. Podman now always asks libpod for
timestamps and orders stdout against stderr by them; its follow used to end
after the first chunk.

A request with no `container` reads the pod's
`kubectl.kubernetes.io/default-container`, else its first container in spec
order, for logs and for exec and attach alike
(`engenho_types::pod::default_container`). Upstream's apiserver refuses a
multi-container pod with no container named; kubectl and k9s default
client-side the same way, so the answer they see is unchanged.

## Eviction, metrics, review discovery (2026-10-13)

- `pods/eviction` (policy/v1) is a catalog subresource, so discovery lists it
  and drain evicts instead of deleting. A pod selected by one
  PodDisruptionBudget spends one of its `disruptionsAllowed` (recorded in
  `disruptedPods`, which the PDB controller now excludes from the healthy
  count for two minutes) or is refused with 429 and a `DisruptionBudget`
  cause; terminal, pending and terminating pods skip budgets; two budgets
  are a 500, as upstream.
- `metrics.k8s.io/v1beta1` serves PodMetrics and NodeMetrics from the node
  runtime: native reads each container's cgroup leaf (every container now
  gets one when the tree is delegated), podman reads its stats API, the node
  reads `/proc`. What cannot be measured (macOS, cgroups off) is absent from
  the list and a 404 naming why, never zeros. Single node: only the
  in-process kubelet's pods and node.
- `authorization.k8s.io/v1` is listed in discovery and its OpenAPI v3
  document is served (vendored, BLAKE3-pinned), so `kubectl create -f` of a
  SelfSubjectAccessReview maps and validates.

## Known gaps

Each is an `Absent` row with its reason: port-forward and its benchmark (no
SPDY or SPDY-over-WebSocket), `events.k8s.io/v1`. Eviction ignores
`deleteOptions.gracePeriodSeconds` (a native delete has no grace period to
carry it to).
