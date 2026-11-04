# Security by default

A native pod is secure by what it does not say. The policy is blue: `engenho-kubelet/blue/kakoi/kakoi.b`, loaded under an empty posture. Rust states the facts and applies the verdict (`engenho-kubelet/src/kakoi.rs`). The laws are `theory/KAKOI.md`; this file says what holds today.

## What a pod gets

| A pod that says nothing | On Linux, with a root daemon |
|---|---|
| Account | its namespace's own: `100000 + blake3(namespace)`, stable and non-root |
| Privileges | `no_new_privs`, no capabilities |
| Filesystem | its own mount namespace: the host read-only, private `/tmp` and `/var/tmp`, `/root`, `/home`, `/run/secrets`, `/etc/ssh`, `/etc/engenho`, `/var/lib`, `/var/log`, `/srv` hidden unless its own volume lives there |
| Processes | its own `/proc` shows only its account's processes |
| Syscalls | a seccomp filter denies `kakoi::denied_syscalls` with `EPERM`, among them `mount` and the new mount API, `unshare`, `setns`, `ptrace`, `bpf`, `io_uring` and module loading. `clone` with a namespace flag and a kernel crypto or vsock socket answer `EPERM`. `clone3` and any call above `kakoi::newest_allowed_syscall` answer `ENOSYS`, which also closes the x32 range. A call of another architecture kills it. An architecture with no syscall table refuses the pod |
| Its volumes | a declared mount stays writable; `emptyDir` and materialized volumes are handed to its account |
| A command run in it | an exec or an exec probe joins its namespaces and takes its account, capabilities, filter and declared environment, never the daemon's |
| Other namespaces | its own IPC, UTS and cgroup namespaces, and a private `/dev/shm`; host sockets such as the nix daemon's, dbus and container runtimes are hidden |

## Its declared capabilities

A pod may carry blue's `access` value as the annotation `kakoi.pleme.io/access`. It only narrows, so writing it takes no grant.

| The access says | The pod gets |
|---|---|
| `effects` lacks `network` | an empty network namespace |
| `effects` lacks `filesystem`, or a volume is not under `writes` | that volume stays read-only |
| `writes` without `filesystem`, `listen` without `network`, or text that does not read as access | that pod is refused, by name |

`kakoi::capability_rendering` holds a row for each host effect, and a test fails on an effect with none. `haibi` does not yet render this annotation from a Bluefile.

## Root is a grant

`runAsUser: 0`, `privileged`, `allowPrivilegeEscalation: true` and `capabilities.add` need the pod annotation `kakoi.pleme.io/root`, whose value is the reason. With no grant that one pod is refused, by name. A grant keeps the daemon's identity and no enclosure.

Only `system:masters` may add or change a grant, on a pod or in a workload's template (`kakoi::admit`, at the API boundary). A controller's own writes pass no admission, so a grant is caught where a template arrives. Under `audit` a write that `enforce` would refuse logs a warning instead.

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

## The API's record

Every API request is recorded by default as a JSON-line `audit.k8s.io/v1` Event in `data_dir/audit/audit.log`, mode 0600, rotated at 64 MiB with 4 files kept. A denied request is recorded too. An Event has no body field, so no body or token can reach it; `engenho-apiserver/blue/audit/audit.b` decides the level. A full queue or a failed write loses that event, counts in `engenho_audit_events_total{result}` and warns, and never fails the request. `runtime.api_audit: off` configures it off. It does not yet record source addresses, and a handler that panics goes unrecorded.

## What this does not do yet

- **The network is not yet per pod.** A pod with the network effect, which is every pod with no access value, shares the host's network, so it can reach whatever listens on loopback. Each surface engenho itself listens on is gated above; a workload's own listener must authenticate.
- **Two pods that declare the same `runAsUser`** see and can signal each other. A pod that declares none has its namespace's account.
- **The seccomp list is a deny-list** with a ceiling, covering x86_64 and aarch64. seccomp(2) recommends an allow-list; moby's default profile is the base to start from.
- **macOS.** Neither identity nor enclosure applies, and each pod gets a `ConfinementNotEnforced` event. `sandbox-exec` is deprecated, and Apple's own `container` tool runs one VM per container, so Linux workloads on a Mac belong in a VM. The design is `theory/NAWABARI.md`; its code is not in `pleme-io/nawabari`, which is empty.
- **A daemon that is not root** cannot enclose. On Linux the kernel still refuses a uid the pod declares that is not the daemon's.
- **Anonymous discovery** (`/api`) still answers; RBAC limits it to discovery.

## Measured against the baseline

Read 2026-11-04 from engenho's source, against Kubernetes Pod Security Standards, moby's seccomp profile, seccomp(2), landlock(7), Bottlerocket, RKE2 and Apple `container`. These are the open roots, in the order that removes the most risk for a native runner on a small fleet of first-party workloads.

| # | Root | Where it lands |
|---|---|---|
| 2 | The fleet default in `nix/modules/pleme/nixos/engenho-node.nix` is `audit`, not `enforce` | each node brought across, then the default raised |
| 3 | Some host surfaces stay reachable: other `/run` sockets, and abstract sockets for a pod with the network effect | Landlock (ABI 6 for abstract sockets, ABI 9 for pathname sockets) |
| 4 | A pod with the network effect still shares the host's network: `hostNetwork` is true in effect, which fails even the Baseline standard, and `NetworkPolicy` is computed and never installed | a per-pod netns with nftables default deny, from `egress` and `listen` |
| 5 | Two pods with one `runAsUser` share an identity; no default `pids.max` or `memory.max` | kakoi identity and cgroup defaults |
| 6 | `capabilities.add: NET_BIND_SERVICE`, which Restricted allows, needs the root grant, which removes the whole enclosure | kakoi: name capabilities an enclosed pod may keep |
| 7 | `hostPID`, `hostIPC`, `readOnlyRootFilesystem`, `capabilities.drop`, `seccompProfile`, sysctls and devices are ignored or unread | kakoi facts, refused when they cannot be honoured |
| 9 | Secrets are plaintext in the store | envelope encryption with a node-held key |
| 10 | No image or closure verification at admission; tameshi's verifier has no caller | the admission rule, then Nix signature checks |
| 11 | Certificates run to 2100 with no rotation | a shorter span and rotation in `engenho-apiserver/src/pki.rs` |
| 12 | The podman backend passes `privileged` and `cap_add` straight through | the same verdict as native |

Policy still written in Rust that belongs in blue: the hostPath permit, mount classification, always-allowed API paths and RBAC, audit levels, token clamping, image reference extraction, webhook matching, NetworkPolicy translation and control tiers. A second standard bidama already loads in an embedded program; a second private package needs `EmbeddedOver`, which the pinned `blue-lang-host` 0.0.125 lacks. `shisei`, `bougyo`, `kyohi` and `kousei` hold shapes to reuse.

Of little value for this threat model: gVisor, Kata and Firecracker for first-party code, mesh mTLS and SPIRE before a netns exists, and user namespaces before unique uids and a netns, since namespaced capabilities widen kernel surface.
