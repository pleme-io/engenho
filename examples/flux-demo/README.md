# engenho-local FluxCD demo

Minimal FluxCD example for the local engenho cluster — a single
GitRepository + Kustomization that deploys the upstream
[podinfo](https://github.com/stefanprodan/podinfo) chart.

## Prereqs

1. `engenho-local` cluster up (kikai-supervised):
   ```bash
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/io.pleme.kikai.engenho-local.plist
   ```
2. `export KUBECONFIG=~/.kube/configs/engenho-local`
3. `kubectl get nodes` returns the local control-plane node

## Install FluxCD

```bash
export KUBECONFIG=~/.kube/configs/engenho-local
flux install
flux check
```

## Apply the demo

```bash
cd ~/code/github/pleme-io/engenho/examples/flux-demo
kubectl apply -f namespace.yaml
kubectl apply -f gitrepository.yaml
kubectl apply -f flux-kustomization.yaml

flux get sources git
flux get kustomizations
kubectl -n podinfo get pods
```

Expected: within ~30s flux pulls podinfo from GitHub, applies its
`kustomize/` directory into the `podinfo` namespace, and a
`podinfo` Deployment + Service materializes.

## Uninstall

```bash
kubectl delete -f flux-kustomization.yaml
kubectl delete -f gitrepository.yaml
kubectl delete -f namespace.yaml
flux uninstall --silent
```
