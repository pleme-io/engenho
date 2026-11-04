# Security by default

A native pod is secure by what it does not say. The policy is blue: `engenho-kubelet/blue/kakoi/kakoi.b`, loaded under an empty posture. Rust states the facts and applies the verdict (`engenho-kubelet/src/kakoi.rs`).

## What a pod gets

| A pod that says nothing | On Linux, with a root daemon |
|---|---|
| Account | its namespace's own: `100000 + blake3(namespace)`, stable and non-root |
| Privileges | `no_new_privs`, no capabilities |
| Filesystem | its own mount namespace: the host read-only, private `/tmp` and `/var/tmp`, `/root`, `/home`, `/run/secrets`, `/etc/ssh`, `/etc/engenho`, `/var/lib`, `/var/log`, `/srv` hidden unless its own volume lives there |
| Its volumes | a declared mount stays writable; `emptyDir` and materialized volumes are handed to its account |

## Root is a grant

`runAsUser: 0`, `privileged`, `allowPrivilegeEscalation: true` and `capabilities.add` need the pod annotation `kakoi.pleme.io/root`, whose value is the reason. With no grant that one pod is refused, by name. A grant keeps the daemon's identity and no enclosure.

A `runAsUser` at pod level now reaches its containers, as Kubernetes specifies. It was silently dropped before.

## Modes

`runtime.native_security`:

| Value | Meaning |
|---|---|
| `enforce` | the default |
| `audit` | apply what is declared; a `ConfinementNotEnforced` event names what `enforce` would refuse, for bringing an existing node across |
| `off` | apply what is declared, silently |

## The node-local surfaces

The kubelet API (`/exec`, `/containerLogs`, `/pods`) needs a bearer token that is random per process and held only in memory, so the one door is the apiserver and its RBAC. `runtime.kubelet_auth: off` removes the gate. `/healthz` stays open.

The etcd facade serves every Secret, so it binds only when `runtime.etcd_listen_addr` names an address.

## What this does not do yet

- **No network or PID namespace.** A native pod shares the host's network and sees its processes. Its account limits what it can do to them, and two pods that declare the same `runAsUser` can signal each other.
- **No seccomp filter.**
- **macOS.** Neither identity nor enclosure applies, and each pod gets a `ConfinementNotEnforced` event. The design is `theory/NAWABARI.md`; its code is not in `pleme-io/nawabari`, which is empty.
- **A daemon that is not root** cannot change identity, and says so the same way.
- **Anonymous discovery** (`/api`) still answers; RBAC limits it to discovery.
