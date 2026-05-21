# engenho-stack — FluxCD orchestrated bring-up

This directory ships a complete FluxCD-driven deployment of the
engenho stack. The HelmRelease + Kustomization manifests enforce
the dependency order documented in `docs/FABRIC.md`:

```
        HelmRepository (engenho)          HelmRepository (nats)
              │                                  │
              └──────────────┬───────────────────┘
                             ↓
                    ┌──────────────────┐
                    │  HelmRelease     │
                    │   nats           │  ← Layer 1 (fabric carrier)
                    └──────────────────┘
                             │  dependsOn
                             ↓
                    ┌──────────────────┐
                    │  HelmRelease     │
                    │   vector         │  ← Layer 2 (observability)
                    └──────────────────┘
                             │  dependsOn
                             ↓
                    ┌──────────────────┐
                    │  HelmRelease     │
                    │   engenho        │  ← Layer 3-5 (store + revoada + apiserver)
                    │   (umbrella)     │
                    └──────────────────┘
```

## Quickstart

```sh
# 1. Apply the GitRepository + Kustomization
kubectl apply -f gitrepository.yaml
kubectl apply -f kustomization.yaml

# 2. Watch FluxCD reconcile in order
flux get helmreleases
flux get kustomizations

# 3. Verify the engenho stack is up
kubectl -n engenho get all
```

## Federating with a global mesh

To join the global engenho web (per FABRIC.md), edit
`engenho-helmrelease.yaml` to enable leaf-node connections:

```yaml
spec:
  values:
    nats:
      config:
        leafnodes:
          enabled: true
          remotes:
            - url: nats://hub.engenho.global:7422
              credentials: /etc/nats/leaf.creds
```

The cluster joins the global NATS supercluster. Cross-region
observability + watch fan-out + content sync ride the leaf-node
connection automatically.

## Files

- `gitrepository.yaml` — FluxCD GitRepository pointing at engenho main
- `kustomization.yaml` — Kustomization for this stack
- `helmrepository-nats.yaml` — NATS official chart repo
- `helmrepository-vector.yaml` — Vector official chart repo
- `nats-helmrelease.yaml` — Layer 1
- `vector-helmrelease.yaml` — Layer 2
- `engenho-helmrelease.yaml` — Layers 3-5 (umbrella chart)
- `namespace.yaml` — engenho namespace
