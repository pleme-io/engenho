# Standard observability layer on engenho

A self-contained, reproducible deploy of the pleme-io **homelab observability
stack** — VictoriaMetrics + VictoriaLogs + Vector + Grafana — running natively
on a local engenho cluster. It doubles as the live proof of two engenho
data-plane bricks:

| Brick | Proven by |
|---|---|
| **DaemonSet controller** (one Pod per schedulable Node) | `vector` runs as a `DaemonSet` → pod `vector-engenho-local` |
| **kubelet ConfigMap-volume materialization** | Vector mounts `/etc/vector/vector.toml` and Grafana mounts its datasource provisioning, both from `ConfigMap` volumes |

## Topology

```
            ┌──────────────┐   prometheus_remote_write    ┌──────────────────┐
   host +   │    Vector    │ ───────────────────────────► │ VictoriaMetrics  │
 internal ─►│ (DaemonSet,  │      (svc-name DNS)          │   :8428          │
  metrics   │ ConfigMap-   │                              └──────────────────┘
            │  mounted)    │   http jsonline               ┌──────────────────┐
            │              │ ───────────────────────────► │  VictoriaLogs    │
            └──────────────┘                              │   :9428          │
                                                          └──────────────────┘
                                   ┌─────────┐  provisioned datasources
                                   │ Grafana │ ──────────► VM + VLogs (svc-name DNS)
                                   │  :3000  │
                                   └─────────┘
```

All cross-pod traffic uses engenho's **service-name DNS** (the aardvark
network-alias engenho assigns from each `Service`), e.g. `victoria-metrics:8428`.
No ClusterIP VIP / kube-proxy is required for this layer.

## Deploy

```
export KUBECONFIG=~/.local/share/engenho-local/kubeconfig   # see docs/LOCAL-BRINGUP.md
kubectl apply -f examples/observability/
```

Manifests are numbered for apply order (namespace → stores → agent → grafana).

## Verify (pod-to-pod, via the kubelet's podman backend)

`kubectl exec` is not yet a POST subresource on engenho, so verification reaches
the containers through the kubelet's runtime directly:

```
# metrics landed in VictoriaMetrics (host_* series come from Vector's host source)
podman exec <vm-container> wget -qO- \
  'http://127.0.0.1:8428/api/v1/label/__name__/values'

# logs landed in VictoriaLogs (Vector demo_logs stream)
podman exec <vlogs-container> wget -qO- \
  'http://127.0.0.1:9428/select/logsql/query?query=*&limit=2'

# Grafana healthy AND can reach VM by service-name (datasource works)
podman exec <grafana-container> wget -qO- 'http://127.0.0.1:3000/api/health'
podman exec <grafana-container> wget -qO- 'http://victoria-metrics:8428/health'
```

## FluxCD

This directory is a plain Kustomize-free manifest set; point a FluxCD
`Kustomization` (or the native `engenho-fonte` reconciler) at it to GitOps the
layer onto an engenho cluster.

## Known engenho gaps this layer routes around (honest status)

- **ClusterIP allocator / kube-proxy** — Services report `clusterIP: None`; this
  layer uses pod-to-pod service-name DNS instead of VIP routing. (M0.5 brick.)
- **`kubectl exec` POST** — engenho serves exec over the kubelet's runtime, not
  the SPDY/WebSocket POST subresource yet, so verification uses `podman exec`.
- **Ingress / NodePort** — Grafana is reachable in-cluster only; external
  exposure waits on the Service-networking brick.
