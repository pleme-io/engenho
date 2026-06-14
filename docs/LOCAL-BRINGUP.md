# engenho — local native bring-up (no VM, no k3s)

engenho runs as a single host process: StoreMesh (raft datastore) + apiserver
(`:6443`, self-signed TLS) + the 18-controller set + scheduler + the
podman-driven kubelet. This is the "run native locally, fully bootstrapped"
path — `kikai`'s `engenho-local` is a *k3s* VM and is NOT this.

## Prereqs
- `podman` machine running (`podman machine start`), `kubectl`, an image
  cached for the workload (`podman pull busybox`).

## Bring up
```bash
# 1. config (or copy examples/local/engenho.yaml → ~/.config/engenho/engenho.yaml)
export ENGENHO_CONFIG=$PWD/examples/local/engenho.yaml

# 2. build + boot the daemon (it writes data_dir/kubeconfig on boot)
cargo build -p engenho
#    REGISTRY_AUTH_FILE points podman at a CLEAN auth file, bypassing a
#    broken host ~/.docker/config.json credHelper (e.g. a stale gcloud
#    docker-helper). Skip it if your host docker config is clean.
printf '{"auths":{}}' > ~/.config/engenho/registry-auth.json
REGISTRY_AUTH_FILE=~/.config/engenho/registry-auth.json ./target/debug/engenho &

# 3. talk to it
export KUBECONFIG=~/.local/share/engenho-local/kubeconfig
kubectl version                 # Server Version: v1.34.0
kubectl get nodes               # engenho-local  Ready

# 4. deploy the hello-world (Namespace + Deployment + Service)
kubectl apply -f examples/local/hello-world.yaml
kubectl get deploy,rs,pods,svc,endpoints -n hello
kubectl logs -n hello -l app=busybox      # -> hello-from-engenho
```

## Proven (2026-06-14)
A Deployment in ns `hello` reconciles end-to-end: Deployment → ReplicaSet →
2 Pods (Running, real podman containers on `engenho-net`, real IPs), 2/2
ready, Service Endpoints populated with both pod IPs, `kubectl logs` streams
the container stdout. The same chain is asserted headlessly by the
`engenho-runtime` integration tests (no podman needed).

## Known follow-ups
- `creationTimestamp` on controller-created objects (RS/Pods/Endpoints) →
  `kubectl` AGE; the apiserver stamps kubectl-applied objects already.
- A `kubelet.registry_auth_file` config field so the `REGISTRY_AUTH_FILE`
  workaround becomes declarative.
- FluxCD-API compatibility (CRDs are served; WATCH-streaming + SSA are the
  remaining gaps before arbitrary Flux configs reconcile) — the north star.
