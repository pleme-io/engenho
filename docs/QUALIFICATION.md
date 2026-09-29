# engenho as a qualification substrate

A local engenho cluster is where manifests bound for an upstream Kubernetes
cluster get their first real test: rendered, applied, installed and exercised
before anything leaves the machine. That only works if engenho behaves like
upstream on every path a consumer relies on.

## The rule

When a qualification needs something engenho does not do, **engenho gains the
capability.** The consumer does not work around it, lower its qualification, or
skip the step quietly.

1. Measure the gap on a live engenho with a real manifest, and record it in the
   backlog below with the date.
2. Prove the divergence with `engenho-diff` (the same `Operation` against engenho
   and a reference cluster; `Divergent` is the receipt). Add an `engenho-oracle`
   table row when the behaviour is a fixed upstream rule.
3. Fix it in engenho, typed, with the diff or oracle case that goes red without
   the fix.
4. Until it lands, the consumer says which qualification tier is blocked and by
   which backlog row. A blocked tier is reported, never silently treated as passed.

## What a consumer can rely on today (measured 2026-09-29, v1.34.0 face)

| Tier | Works | Notes |
|---|---|---|
| server-side dry-run of built-in kinds | yes | standard API groups served |
| strict unknown-field rejection | yes | `fieldValidation=Strict` rejects `spec.prots` |
| upstream CRDs accepted | yes | Gateway API v1.2.1 applies (server side) |
| scalar type checking (`int32` given a string) | **no** | row 1 |
| client-side validation of CRD manifests | **no** | row 2 |
| running an OCI image on a `native` node | **no** | row 3 |
| admission webhooks called | **no** | row 5 |
| namespace delete removes its contents | **no** | row 6 |
| `DaemonSet` / `StatefulSet` pods follow a template change (`RollingUpdate`, `OnDelete`) | yes | row 7 |
| `Deployment` rollout within `maxSurge` / `maxUnavailable` | **no** | row 9 |

## Backlog (source-mapped at b3492a8)

| # | Gap | Where | Smallest change |
|---|---|---|---|
| 1 | a JSON string is accepted for an `int32` (`Service.spec.ports[].port: "8010"`), even with strict validation | `typed_decode.rs:44` runs as `Rollout::Shadow` (counted in `engenho_would_reject_total`, not enforced); `field_validation/schema.rs:235` treats every scalar as `Shape::Leaf`; `validation.rs:237` reads the port with `as_i64()` and skips a string | enforce `TYPED_DECODE` after checking the would-reject metric for bodies upstream accepts (e.g. a `Quantity` sent as a number); split `Leaf` by JSON type in `scan.rs:229`; make a non-integer port a violation |
| 2 | `/openapi/v3` publishes 11 group-versions of ~20 served, no CRD schemas; `/openapi/v2` absent | `engenho-types/src/openapi_v3.rs:109` fixed `SERVED` table; `router.rs:483-486,776-821`; CRD schemas used only on write (`engenho-controllers/src/crd.rs:198,287`) | vendor the missing upstream documents; build a document per CRD group-version from the registry; add a v2 route |
| 3 | an OCI image never starts on a `native` node; one backend per node, no routing | `native_backend.rs:158,662-682` refuses OCI; `engenho-config/src/runtime.rs:339` single backend; pod logs read only from the in-process kubelet (`runtime.rs:1196-1217`) | a node label for the backend plus a scheduler filter (or a backend that routes by image scheme), so OCI pods land on a `podman_api` node; a node-proxy log reader |
| 4 | a stuck pod can produce zero Events | `kubelet.rs:3581,3166-3215` (volume-pending paths emit nothing); `event_recorder.rs:525-531` swallows write failures at `debug`; the scheduler emits no Events (`engenho-scheduler/src/scheduler.rs:215`) | emit `Failed` in `hold_unmounted`; log dropped events at `warn`; give the scheduler an event sink |
| 5 | admission webhooks are never called | `mutating_webhook_plugin()` has no production caller; the chain in `runtime.rs:417-424` holds only ClusterIP defaulting; `caBundle` verification unimplemented (`webhook_admission.rs:136-143`) | add the webhook plugin to the chain; build the client with the `caBundle` as its root |
| 6 | deleting a namespace left a Pod behind in it | namespace deletion path (not yet source-mapped) | finalize the namespace only after its contents are gone, as upstream's namespace controller does |
| 7 | **closed.** A template change never reached a `DaemonSet` or `StatefulSet` pod: both decided by NAME (`{ds}-{node}`, `{sts}-{n}`), so a re-rendered image left every running pod on the old one. Measured 2026-09-29, 0.53.118 → 0.53.119 on a native node: eight pods kept a closure GC then removed, `ContainerCreating` for ~5.5 h | `daemonset.rs`, `statefulset.rs` | `rollout.rs`: pods stamped `controller-revision-hash` (+ `pod-template-generation` for a `DaemonSet`); `updateStrategy` read, default `RollingUpdate` / `maxUnavailable: 1`; `OnDelete` honoured; out-of-date unready pods replaced at once, Ready ones within the budget; a `StatefulSet` rolls highest ordinal first and waits on an unhealthy one, with `partition`; `Failed` pods replaced (a `DaemonSet` with upstream's per-node backoff). `tests/r16_workload_rollout.rs`, red on the old controllers (10 of 13). Compared against upstream source (`daemon/update.go rollingUpdate`, `stateful_set_control.go updateStatefulSet`); no reference cluster was reachable for `engenho-diff`. One deliberate difference: a node with NO pod counts as unavailable, because engenho's replacement reuses the name and waits for the old pod to go |
| 8 | no `ControllerRevision` objects, so `kubectl rollout history` / `undo` for a `DaemonSet` or `StatefulSet` has nothing to read; the revision hash is engenho's normalized-template hash, not upstream's | `rollout.rs` | write a `ControllerRevision` per template revision and name the hash from it |
| 9 | a `Deployment` rollout ignores `maxSurge` / `maxUnavailable`: every old `ReplicaSet` is scaled to 0 at once | `deployment.rs` (scale-others-to-0) | scale new up / old down within the budget. Case: `gap_a_deployment_rollout_keeps_old_pods_within_max_unavailable` (`#[ignore]`d, red) |
| 10 | a `ReplicaSet` counts a `Failed` pod as a replica, so it is never replaced (upstream's `FilterActivePods` excludes it) | `replicaset.rs` `live_children` | exclude terminal pods once a terminated-pod GC exists (without one, each retry would leave a failed pod behind). Case: `gap_a_replicaset_replaces_a_failed_replica` (`#[ignore]`d, red) |
| 15 | **closed.** A pod recreated under the same name inherited the dead pod's start backoff: the start curve and the volume-pending streak were keyed by `namespace/name`. Measured 2026-09-29: after the stale `DaemonSet` pods were deleted, two recreated pods sat `ContainerCreating` for ~4.5 min with no log line, waiting out their predecessors' cap. And a held start rendered as bare `ContainerCreating` | `kubelet.rs` `start_permit`, `hold_unmounted`, `reconcile_running` | keyed by name + `metadata.uid` (`pod_instance`), as upstream keys by pod UID; a held never-started container is `Waiting{CrashLoopBackOff}` with `back-off <n>s restarting failed container=<c> pod=<name>_<ns>(<uid>)`. `tests/m0_11_backoff_follows_the_pod_not_its_name.rs`, red before (2 of 2) |

Rows 1, 2 and 5 are code that exists and is not switched on or not connected;
they are the cheapest wins.
