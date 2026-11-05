# Security by default

A native pod is secure by what it does not say. The policy is blue: `engenho-kubelet/blue/kakoi/kakoi.b`, loaded under an empty posture. Rust states the facts and applies the verdict (`engenho-kubelet/src/kakoi.rs`). The laws are `theory/KAKOI.md`; this file says what holds today.

## What a pod gets

| A pod that says nothing | On Linux, with a root daemon |
|---|---|
| Account | its namespace's own: `100000 + blake3(namespace)`, stable and non-root |
| Privileges | `no_new_privs`, no capabilities, except `capabilities.add: NET_BIND_SERVICE`, which it keeps with no grant and stays enclosed and filtered (`kakoi::keepable_capabilities`). Any other capability needs the grant |
| Filesystem | its own mount namespace: the host read-only, private `/tmp` and `/var/tmp`, `/root`, `/home`, `/run/secrets`, `/etc/ssh`, `/etc/engenho`, `/var/lib`, `/var/log`, `/srv` hidden unless its own volume lives there |
| Processes | its own `/proc` shows only its account's processes |
| Syscalls | a seccomp filter denies `kakoi::denied_syscalls` with `EPERM`, among them `mount` and the new mount API, `unshare`, `setns`, `ptrace`, `bpf`, `io_uring` and module loading. `clone` with a namespace flag and a kernel crypto or vsock socket answer `EPERM`. `clone3` and any call above `kakoi::newest_allowed_syscall` answer `ENOSYS`, which also closes the x32 range. A call of another architecture kills it. An architecture with no syscall table refuses the pod |
| Its volumes | a declared mount stays writable; `emptyDir` and materialized volumes are handed to its account |
| Files | a Landlock domain under the mount namespace, unprivileged and layered: it reads the host, less what is hidden; writes only its writable volumes, its scratch and the plain devices (`kakoi::safe_devices`); runs programs only from the store, its volumes and its scratch; makes no device node |
| Sockets and signals | Landlock scopes it, from ABI 6: no abstract socket and no signal crosses out of its domain, so a second pod with its uid cannot signal it. A command run in it holds the socket scope and not the signal scope, so `kill -HUP 1` and `nginx -s reload` work in an exec. An abstract socket does not cross between a pod's containers either. Pathname sockets: see below |
| Bounds | on a node with delegated cgroups, `pids.max` 4096 and, with no declared limit, `memory.max` of a quarter of the node, never under 1 GiB, and raised to its memory request. A node that cannot write one warns and runs the pod |
| A command run in it | an exec or an exec probe joins its namespaces and takes its account, capabilities, filter and declared environment, never the daemon's |
| Other namespaces | its own IPC, UTS and cgroup namespaces, and a private `/dev/shm`; host sockets such as the nix daemon's, dbus and container runtimes are hidden |

## Its declared capabilities

A pod may carry blue's `access` value as the annotation `kakoi.pleme.io/access`. It only narrows, so writing it takes no grant.

| The access says | The pod gets |
|---|---|
| `effects` lacks `network` | an empty network namespace |
| `effects` lacks `filesystem`, or a volume is not under `writes` | that volume stays read-only |
| `reads` | Landlock reads only those paths and what a program needs at all (`kakoi::runtime_reads`). A path under a hidden directory shows, and only that path, on a kernel with Landlock |
| `effects` holds `process` | programs run from the store and `exec` only |
| `effects` lacks `process` | programs run from the store only. The whole store: it holds no secret, and the closure's requisites are not listed yet |
| `writes` without `filesystem`, `exec` without `process`, `listen` without `network`, a relative path or one in the store, or text that does not read as access | that pod is refused, by name |

`kakoi::capability_rendering` holds a row for each host effect, and a test fails on an effect with none. `haibi` does not yet render this annotation from a Bluefile.

## Root is a grant

`runAsUser: 0`, `privileged`, `allowPrivilegeEscalation: true` and a `capabilities.add` other than `NET_BIND_SERVICE` need the pod annotation `kakoi.pleme.io/root`, whose value is the reason. With no grant that one pod is refused, by name. A grant keeps the daemon's identity and no enclosure.

Only `system:masters` may add or change a grant, on a pod or in a workload's template (`kakoi::admit`, at the API boundary). A controller's own writes pass no admission, so a grant is caught where a template arrives. Under `audit` a write that `enforce` would refuse logs a warning instead.

A `runAsUser` at pod level now reaches its containers, as Kubernetes specifies. It was silently dropped before.

## A pod's own declarations

| The pod says | With no grant | With a grant |
|---|---|---|
| `hostPID`, `hostIPC`, `seccompProfile: Unconfined`, a `hostPath` of type `CharDevice` or `BlockDevice` | refused, by name | honoured: it keeps no enclosure |
| `seccompProfile: Localhost` or an unknown type, `securityContext.sysctls`, `volumeDevices` | refused: this node has no way to apply them | refused too |
| `seccompProfile: RuntimeDefault` | satisfied: the enclosure's filter is what it gets | runs, with a warning that no filter applies |
| `hostNetwork: true` | runs. It changes nothing today, since every pod with the network effect already shares the host. With an access that lacks `network` it is refused as a contradiction | runs |
| `runAsUser` from 1000 up | **kept as declared.** A pod's own declaration is honoured, since a volume's owner may rest on it. Two pods that name one uid share that identity: each can read the other's files and see its processes. Landlock stops signals between them, and the filter stops `ptrace` | kept |
| `runAsUser` from 1 to 999 | kept, with a warning that it shares that account with any host service running as it | kept |
| `runAsUser` or `runAsGroup` from 100000 to 165535 | refused, unless it is its own namespace's account, since the range belongs to the namespaces that hash to it | kept |
| `fsGroup`, `supplementalGroups`, `procMount` other than `Default`, `appArmorProfile`, `seLinuxOptions`, `shareProcessNamespace`, `hostUsers: false` | runs, and a `ConfinementNotEnforced` event names each, since this node does not apply it | the same |

`readOnlyRootFilesystem` and `capabilities.drop` hold for every enclosed pod by what the enclosure is. On a grant, a warning says they are not applied.

## A pod on a container runtime

A podman or CRI node puts `kakoi::enclose` in front of its runtime (`engenho-kubelet/src/enclosure_gate.rs`), so a pod that asks for `runAsUser: 0`, `privileged`, `allowPrivilegeEscalation: true` or `capabilities.add` gets the verdict a native pod gets: refused by name with no grant under `enforce`, a `ConfinementNotEnforced` event under `audit`, silence under `off`. With a grant the runtime receives what the pod declares. A pod that declares nothing never reaches blue, so a program that failed to load refuses only what it decides.

Only the refusal crosses. A pod that says nothing keeps the image's own user and the runtime's namespaces, and its `kakoi.pleme.io/access` narrowing is not rendered on a runtime yet. What a runtime applies by itself, such as `sysctls` or a `Localhost` profile, stays its own to judge; who may ask for root, a capability, `hostPID` or `hostIPC` is still kakoi's.

## Certificates renew themselves

`engenho-apiserver/blue/pki/pki.b` names each leaf's lease. The server leaf lasts 90 days and renews every 30; the admin client leaf lasts a year and renews every 90. A leaf belongs to the period the clock is in, so a restart within one period issues the same bytes. The `cert-renewal` driver asks on every node, swaps the server leaf under the live listener with no restart (`engenho-apiserver/src/pki_rotation.rs`), and rewrites `pki/admin.crt`, `pki/admin.key` and each published kubeconfig in place.

A failure keeps the leaf already held, warns and counts in `engenho_cert_renewals_total{result}`, and the next tick tries again. A lease that cannot be read at boot issues a leaf valid to 2100 rather than stopping the API. `runtime.cert_rotation: off` issues leaves valid to 2100 and never renews.

The CA stays, to 2100, and an existing data directory is read as it is: its first boot issues bounded leaves from the same CA and key.

| A client | Sees |
|---|---|
| holding a published kubeconfig at its path | the file rewritten with a new client leaf; nothing to do |
| pinning the CA only | a new serial and dates on the same key; nothing to do |
| holding a kubeconfig copied before this change | a client leaf valid to 2100 that keeps authenticating, since nothing revokes it; only a reseed of the CA retires it |
| holding a kubeconfig copied after this change | a client leaf with at most a year to run, and at least 275 days from the renewal that wrote it; copy it again before then, or a handshake fails as expired |

## A closure is checked before it runs

Before a native pod's first start the kubelet reads every path of each closure the pod names from the node's own store (`nix path-info --recursive --json`) and verifies each signature with ed25519 (`engenho-kubelet/src/closure_trust.rs`). `engenho-kubelet/blue/trust/trust.b` decides. The kubelet is the door, not admission, because a store is per node, a closure may be realised only at that moment, and a controller's write passes no admission. A pod already running is not checked again.

| A path stands on | Under `enforce` |
|---|---|
| a signature that verifies under a trusted key | it runs |
| a content address | it runs |
| a build on this node (Nix's `ultimate`), with `runtime.closure_local_builds: trust`, the default | it runs, and a `ConfinementNotEnforced` event says how many paths |
| nothing, a key this node does not trust, or a signature that does not verify | that pod is `Failed` with reason `ClosureUntrusted`, naming the path |
| a store that cannot be read | that pod is `Failed` with reason `ClosureUntrusted`, naming why |

`audit` runs it and names what `enforce` would refuse; `off` does not read the store. `runtime.closure_signatures` sets this check apart; absent, it follows `runtime.native_security`. `runtime.closure_trusted_keys` replaces the keys the node's Nix store trusts, for a node that wants fewer.

Local builds are trusted by default because of what the house runs. Measured on plo 2026-11-04 across the five closures its manifests name, through this code: 306 paths, 92 verified under a trusted key, 212 built on plo, 2 content-addressed, 0 on nothing. Three of the five are local builds at the top, so requiring a signature would refuse the house's workloads. `closure_local_builds: refuse` is the setting for a node whose builds are all signed.

It does not check an OCI image. `TameshiAttestationWebhook` has no caller and no registry producer: root 10 below.

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

## Secrets at rest

`runtime.secrets_at_rest` is `encrypt` by default: a Secret is sealed in the state machine, the log and snapshots under `<dataDir>/store.keys/secrets-at-rest.key`, generated once per node and never regenerated. Back that file up, since nothing else opens the data. The first write of the store's image seals every Secret, and an older binary cannot read them, so going back means setting `off` and waiting for the log to purge. A LIST covering a Secret that cannot be read refuses rather than omits, and a sealed replay tail with a lost key blocks boot, by name, until the key is restored. Each node seals under its own key.

It does not cover a kubelet's Secret volumes on disk, or plaintext already in old log entries, snapshots and store versions until they are purged or compacted. The Raft wire is TLS-pinned over QUIC; the off-by-default NATS transport is unchecked. The fleet holds this `off` until each node's key has a backup: `pleme.nixos.engenhoNode.secretsAtRest`.

## The API's record

Every API request is recorded by default as a JSON-line `audit.k8s.io/v1` Event in `data_dir/audit/audit.log`, mode 0600, rotated at 64 MiB with 4 files kept. A denied request is recorded too. An Event has no body field, so no body or token can reach it; `engenho-apiserver/blue/audit/audit.b` decides the level. A full queue or a failed write loses that event, counts in `engenho_audit_events_total{result}` and warns, and never fails the request. `runtime.api_audit: off` configures it off. It does not yet record source addresses, and a handler that panics goes unrecorded.

## What this does not do yet

- **The network is not yet per pod.** A pod with the network effect, which is every pod with no access value, shares the host's network, so it can reach whatever listens on loopback. Each surface engenho itself listens on is gated above; a workload's own listener must authenticate.
- **Pathname sockets on a kernel below Landlock ABI 9 (7.1).** plo runs 6.12.93, ABI 6, so a host socket the pod's account may open stays reachable unless its directory is hidden. `kakoi::hidden_paths` hides engenho's, tailscale's, systemd's, udev's, dhcpcd's, dbus's and the nix daemon's; `/run/nscd` stays. On ABI 9 the domain reaches only sockets under its volumes and scratch. That half is built and has no kernel to run on yet.
- **A kernel below Landlock ABI 6** leaves abstract sockets and signals unscoped, and one with no Landlock leaves every file rule unheld. Each is a `ConfinementNotEnforced` event naming what is missing, never a refused pod.
- **A pod with no access** may run what it wrote to a volume or its scratch, since a chart's ConfigMap script and an init container's copied tool are real workloads. An access value closes that.
- **The seccomp list is a deny-list** with a ceiling, covering x86_64 and aarch64. seccomp(2) recommends an allow-list; moby's default profile is the base to start from.
- **macOS.** Neither identity nor enclosure applies, and each pod gets a `ConfinementNotEnforced` event. `sandbox-exec` is deprecated, and Apple's own `container` tool runs one VM per container, so Linux workloads on a Mac belong in a VM. The design is `theory/NAWABARI.md`; its code is not in `pleme-io/nawabari`, which is empty.
- **A daemon that is not root** cannot enclose. On Linux the kernel still refuses a uid the pod declares that is not the daemon's.
- **Anonymous discovery** (`/api`) still answers; RBAC limits it to discovery.

## Measured against the baseline

Read 2026-11-04 from engenho's source, against Kubernetes Pod Security Standards, moby's seccomp profile, seccomp(2), landlock(7), Bottlerocket, RKE2 and Apple `container`. These are the open roots, in the order that removes the most risk for a native runner on a small fleet of first-party workloads.

| # | Root | Where it lands |
|---|---|---|
| 2 | The fleet default in `nix/modules/pleme/nixos/engenho-node.nix` is `audit`, not `enforce` | each node brought across, then the default raised |
| 4 | A pod with the network effect still shares the host's network: `hostNetwork` is true in effect, which fails even the Baseline standard, and `NetworkPolicy` is computed and never installed | a per-pod netns with nftables default deny, from `egress` and `listen` |
| 10 | An OCI image a pod names is not verified; `TameshiAttestationWebhook` has no caller and no registry producer | a registry verifier behind `SignatureVerifier` |
| 11 | The CA runs to 2100, and a client leaf copied before rotation stays valid with nothing to revoke it | CA rollover and a revocation list |

Policy still written in Rust that belongs in blue: the hostPath permit, mount classification, always-allowed API paths and RBAC, audit levels, token clamping, image reference extraction, webhook matching, NetworkPolicy translation and control tiers. A second standard bidama already loads in an embedded program; a second private package needs `EmbeddedOver`, which the pinned `blue-lang-host` 0.0.125 lacks. `shisei`, `bougyo`, `kyohi` and `kousei` hold shapes to reuse.

Of little value for this threat model: gVisor, Kata and Firecracker for first-party code, mesh mTLS and SPIRE before a netns exists, and user namespaces before unique uids and a netns, since namespaced capabilities widen kernel surface.
