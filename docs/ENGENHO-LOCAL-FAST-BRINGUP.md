# engenho-local fast bring-up — root-cause + substrate fix

## The 50-minute cold bring-up (2026-05-23)

Verified timeline from `~/Library/Logs/kikai-engenho-local.log`:

| Phase | Duration | What |
|---|---|---|
| `preflight: checking image attribute` | **10m 29s** | `nix eval --raw .#packages.aarch64-linux.engenho-local-image.outPath` |
| `locating root disk image` | **39m 5s** | Misleading log message — actually `nix build` of the entire NixOS disk image from scratch |
| `extracting kernel and initrd from image` | 21s | `nix eval` for boot file paths |
| `creating writable root disk copy` | 4s | `cp` (clonefile unavailable cross-FS) |
| `launching VM` → `cluster brought up` | 16s | VZ launch + DHCP + API + node + flux |
| **Total** | **50m 15s** | |

Compare warm restart per kikai's own comment: `Total warm restart drops from ~20s to ~5s` (kikai/src/up.rs:100).

## Root causes

### 1. Snapshot fast path didn't fire

kikai/src/up.rs:101 has a `try_snapshot_fast_path` that skips preflight + locate + extract when:
- A valid snapshot exists, AND
- Every store path the snapshot references still resolves

The 2026-05-23 03:17 SIGTERM did emit `"preserving VM state before exit"` so a snapshot was attempted. But on resume nix-gc had evicted the prior `nixos-disk-image` store path between 03:17 and 01:00 (~21h window), so the snapshot's metadata referenced a now-dead path and the fast path bailed.

### 2. Disk image rebuilt from scratch (39 min)

`/etc/nix/nix.conf` has:
```
keep-derivations = false
keep-outputs = false
```

Plus no per-cluster gcroot pinning the built image. So when `seibi`'s automated `nix-gc --keep-days 14` ran (or any other GC), the multi-GB `nixos-disk-image` got evicted along with the kernel/initrd/nixos-system store paths.

Next bring-up: `nix build` from scratch.

### 3. Preflight comment is wrong

kikai/src/up.rs:41 claims:
```rust
// `nix eval --raw .#<attr>.outPath` is the cheapest way to confirm
// an attribute exists. Costs ~100ms (vs minutes for `nix build`).
```

This holds for a warm flake-eval cache. For a cold cache against the pleme-io/nix flake (~hundreds of inputs + overlays), measured cost is **10 minutes**, not 100ms.

## Substrate fix

Three concrete changes to kikai that together drop the cold cycle from 50min → ~5min:

### Fix A — pin host-side gcroots (kikai/src/up.rs after locate)

After `locate_root_disk` returns the store path, register it as a per-cluster gcroot:

```rust
// After locating the root disk:
let gcroot_path = crate::config::data_dir(cluster_name)?.join("root-disk.gcroot");
let _ = std::process::Command::new("nix-store")
    .arg("--add-root").arg(&gcroot_path)
    .arg("-r").arg(&img_store_path)
    .output();
```

Same for `kernel`, `initrd`, and `nixos-system` after extraction. Files end up at:
```
~/.local/share/kikai/<cluster>/root-disk.gcroot
~/.local/share/kikai/<cluster>/kernel.gcroot
~/.local/share/kikai/<cluster>/initrd.gcroot
~/.local/share/kikai/<cluster>/system.gcroot
```

`seibi` (or operator-run `nix-gc`) can no longer evict these paths. Snapshot fast path becomes reliable.

### Fix B — fix the misleading preflight cost comment

```rust
// `nix eval --raw .#<attr>.outPath` confirms the attribute exists.
// Warm: ~100ms. Cold (no eval cache): can take 5-10 minutes on
// the pleme-io/nix flake — the cost is paid once per flake-input
// change. Subsequent invocations hit the eval cache.
```

Honest documentation. Operators stop wondering why the daemon is "stuck on preflight".

### Fix C — fold preflight into locate

`nix build --no-link --print-out-paths .#<attr>` already implicitly verifies the attribute exists (exits non-zero with a typed error if not). The separate preflight eval is redundant for the build path.

Move preflight to a SEPARATE `kikai check` subcommand that operators run pre-merge in CI; remove it from the up-path hot loop. Saves 10 min per cold bring-up.

### Fix D — add a `kikai prewarm` subcommand

```
kikai prewarm --cluster engenho-local
```

Pre-builds the disk image + pins all 4 gcroots without launching the VM. Operators run this on a schedule (or on flake-update) so the next operator-triggered `up` is always fast.

## Workaround applied today (2026-05-23 21:59)

Pinned the 4 store paths manually as gcroots after the slow cold bring-up:

```bash
nix-store --add-root ~/.local/share/kikai/engenho-local/root-disk.gcroot -r /nix/store/h8slc7civwbmjxh7df6fqm98yndsiabc-nixos-disk-image
nix-store --add-root ~/.local/share/kikai/engenho-local/kernel.gcroot   -r /nix/store/fdap4sc3prs1hzn5l77wf7pzihy5wlc2-linux-6.12.90
nix-store --add-root ~/.local/share/kikai/engenho-local/initrd.gcroot   -r /nix/store/ba8b68lml2wqpq68j7j6hj7v9b168fq1-initrd-linux-6.12.90
nix-store --add-root ~/.local/share/kikai/engenho-local/system.gcroot   -r /nix/store/4bhpaip4mpcryvr9paicvbhwjv448hys-nixos-system-engenho-local-25.11.20260518.687f05a
```

This makes the NEXT bring-up snapshot-fast-path-eligible — provided
flake inputs haven't changed in the meantime. After the kikai fixes
above land, this whole procedure is automatic.

## Verification

After applying Fix A in kikai + bumping the kikai-derived `seibi` GC
exclusion list, a cold-cache `kikai cycle --cluster engenho-local`
should:
- Skip preflight entirely → save 10 min
- Snapshot fast path fires → save 39 min for rebuild
- Land at "cluster brought up" in ~30s

Target: cold restart 30s, warm restart 5s.
