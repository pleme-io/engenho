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
Port-forward runs through the relay into a real engenho kubelet and a TCP
echo listener: several ports on one session, a refused port reported on its
error stream while the others keep forwarding, and a client reset or hang-up
closing the pod connection — over both the WebSocket tunnel and plain SPDY.

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
  in WebSocket (`SPDY/3.1+portforward.k8s.io`), then plain SPDY. Both are
  served (2026-10-13): the kubelet speaks SPDY/3.1 (`engenho-kubelet/src/spdy`),
  the apiserver relays the tunnel byte for byte and turns a plain SPDY upgrade
  into the same tunnel. Checked by hand with kubectl 1.34.3 against the
  suite's cluster: the tunnel, `KUBECTL_PORT_FORWARD_WEBSOCKETS=false`, two
  ports at once, a 300 KB round trip (past SPDY's 64 KiB initial window,
  which upstream's peer never credits), and a refused port's error.
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

## Known gaps

Each is an `Absent` row with its reason: `sinceSeconds` (validated, not
honoured), `pods/eviction`, `metrics.k8s.io`, `events.k8s.io/v1`. Not yet a row: the
native backend ignores `timestamps=true`, so k9s strips the first word of
each line as if it were a timestamp.
