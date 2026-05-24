#!/usr/bin/env bash
# Apply the engenho-local FluxCD demo end-to-end.
#
# Idempotent: re-running is safe. Skips already-applied steps.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/configs/engenho-local}"

echo "→ KUBECONFIG=$KUBECONFIG"
echo "→ kubectl cluster-info"
kubectl cluster-info --request-timeout=5s | head -3

if ! kubectl get ns flux-system >/dev/null 2>&1; then
  echo "→ flux install"
  flux install --network-policy=false
else
  echo "→ flux-system namespace already present, skipping flux install"
fi

echo "→ kubectl apply namespace.yaml"
kubectl apply -f "$DIR/namespace.yaml"

echo "→ kubectl apply gitrepository.yaml"
kubectl apply -f "$DIR/gitrepository.yaml"

echo "→ kubectl apply flux-kustomization.yaml"
kubectl apply -f "$DIR/flux-kustomization.yaml"

echo
echo "→ flux get sources git"
flux get sources git

echo
echo "→ flux get kustomizations"
flux get kustomizations

echo
echo "→ Waiting up to 2m for podinfo deployment to be ready…"
kubectl -n podinfo wait --for=condition=Available --timeout=120s deployment/podinfo || {
  echo "podinfo Deployment not yet ready — check 'flux get kustomizations'"
  kubectl -n podinfo get pods
  exit 1
}

echo
echo "✓ podinfo running"
kubectl -n podinfo get pods,svc
