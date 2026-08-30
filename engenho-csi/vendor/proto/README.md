# Vendored protos — upstream's own bytes, never reconstructed

| file | source | rev | fetched |
|---|---|---|---|
| `csi/csi.proto` | `container-storage-interface/spec` | `v1.9.0` | 2026-08-30 |
| `pluginregistration/api.proto` | `kubernetes/kubernetes` `staging/src/k8s.io/kubelet/pkg/apis/pluginregistration/v1/api.proto` | `v1.34.0` | 2026-08-30 |

Both are byte-for-byte upstream. No field number, type or option was edited.

**Why this matters more here than anywhere else in engenho.** These two files
are the contract with software we do not write and cannot inspect at runtime.
A CSI driver is a vendor binary; if a field number here drifts from upstream's,
the driver does not fail loudly — it decodes a different field and mounts
something else. Regenerating these by hand from documentation is the one edit
that would make every green test in this crate meaningless.

To update: re-fetch at a NEW tag, run the differential in `tests/`, and record
the new rev in this table.
