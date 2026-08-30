//! The CSI provisioning seam — dynamic `CreateVolume` from a StorageClass.
//!
//! ★ WHY A NARROW TRAIT HERE RATHER THAN A DEPENDENCY ON `engenho-csi`.
//! `engenho-kubelet` depends on `engenho-controllers`, so the arrow cannot
//! be reversed. The same shape `EventStore` already takes in this crate:
//! the controller declares the *verbs it needs*, and the crate that owns
//! the transport supplies them. It also keeps every protobuf type out of
//! the binder, so the binder's tests stay pure JSON.
//!
//! ★ WHY THIS IS NOT `VolumeRuntime`, WHICH ALREADY EXISTS AND LOOKS RIGHT.
//! Measured, not assumed. `VolumeRuntime::mount(&VolumeSpec) ->
//! MountedVolume` has provisioning-shaped INPUTS (storage class, size,
//! access mode, parameters — very nearly a `CreateVolumeRequest`) and
//! mounting-shaped OUTPUTS (`mount_path`, a host directory). CSI's
//! `CreateVolume` returns a volume ID and no path, because at provision
//! time no node has mounted anything and there IS no path.
//!
//! That is the org rule's "same goal, different shapes" case: forcing them
//! into one type would make a bad abstraction that looks well-motivated,
//! and every caller would then carry a `mount_path` that is empty exactly
//! when it matters. So the rule is written down instead —
//! [`crate::csi_provisioner`] owns provisioning, the kubelet's
//! `CsiVolumeMaterializer` owns the node path, and `VolumeRuntime` is
//! superseded by both. Per ★★ MODULARIZE, DON'T DELETE the declaration
//! stays; its header carries the note.

use async_trait::async_trait;
use std::collections::BTreeMap;

/// What the binder asks a driver to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsiCreateRequest {
    /// The driver to call (`StorageClass.provisioner`).
    pub driver: String,
    /// The volume NAME the driver should key idempotency on.
    ///
    /// ★ THIS IS WHY RETRIES DO NOT LEAK DISKS. `CreateVolume` is specified
    /// as idempotent on `name`: calling it twice with the same name and
    /// compatible capacity returns the SAME volume. A random name per
    /// attempt would provision a new EBS volume on every reconcile after a
    /// transient failure, and nothing would ever delete the orphans.
    pub name: String,
    /// Requested bytes; 0 means the claim named no size.
    pub capacity_bytes: i64,
    /// `StorageClass.parameters`, passed through verbatim.
    pub parameters: BTreeMap<String, String>,
    /// Whether the claim wants multi-node write access.
    pub multi_node: bool,
}

/// What the driver created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsiCreatedVolume {
    /// The driver's id for the volume. Becomes `PV.spec.csi.volumeHandle`.
    pub volume_handle: String,
    /// Actual capacity, which may EXCEED the request (a driver rounds up to
    /// its allocation unit). Recorded as the PV's capacity so a later
    /// binding check compares against what exists, not what was asked for.
    pub capacity_bytes: i64,
    /// The opaque bag the driver wants handed back at publish time.
    pub volume_attributes: BTreeMap<String, String>,
}

/// The provisioning verbs the binder needs.
#[async_trait]
pub trait CsiProvisioner: Send + Sync {
    /// Whether a driver by this name is registered and can provision.
    ///
    /// Asked BEFORE `create_volume` so the binder can leave a PVC Pending
    /// with an honest reason instead of turning an absent driver into a
    /// failed provision — the first is "waiting for your driver", the
    /// second reads as "your storage is broken".
    async fn can_provision(&self, driver: &str) -> bool;

    /// `Controller.CreateVolume`.
    ///
    /// # Errors
    /// A human-readable reason; the binder surfaces it and retries.
    async fn create_volume(&self, req: &CsiCreateRequest) -> Result<CsiCreatedVolume, String>;

    /// `Controller.DeleteVolume`.
    ///
    /// # Errors
    /// A human-readable reason.
    async fn delete_volume(&self, driver: &str, volume_handle: &str) -> Result<(), String>;
}

/// A provisioner that serves nothing.
///
/// The honest default for a binder with no CSI plane wired: every CSI
/// StorageClass stays Pending, which is exactly what happens today and
/// what an external provisioner would eventually handle.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCsiProvisioner;

#[async_trait]
impl CsiProvisioner for NoCsiProvisioner {
    async fn can_provision(&self, _driver: &str) -> bool {
        false
    }

    async fn create_volume(&self, req: &CsiCreateRequest) -> Result<CsiCreatedVolume, String> {
        Err(format!(
            "no CSI plane is wired on this node: cannot provision through driver {}",
            req.driver
        ))
    }

    async fn delete_volume(&self, driver: &str, _handle: &str) -> Result<(), String> {
        Err(format!(
            "no CSI plane is wired: cannot delete through {driver}"
        ))
    }
}

/// Parse a Kubernetes quantity string (`"1Gi"`, `"500M"`, `"1000"`) to bytes.
///
/// ★ THE TWO SUFFIX FAMILIES ARE NOT THE SAME AND THE DIFFERENCE IS REAL
/// MONEY. `Gi` is 2^30 and `G` is 10^9 — a 7.4% gap. Treating `G` as `Gi`
/// under-provisions every volume by that margin, and the failure surfaces
/// as a disk full at 93% with a monitoring system that says there is room.
///
/// Returns `None` for anything unparseable rather than a guess: a wrong
/// size silently provisions the wrong disk.
#[must_use]
pub fn parse_quantity(q: &str) -> Option<i64> {
    let q = q.trim();
    if q.is_empty() {
        return None;
    }
    // Longest suffix first: "Ki" must not match "K".
    const SUFFIXES: &[(&str, i64)] = &[
        ("Ki", 1024),
        ("Mi", 1024 * 1024),
        ("Gi", 1024 * 1024 * 1024),
        ("Ti", 1024_i64.pow(4)),
        ("Pi", 1024_i64.pow(5)),
        ("Ei", 1024_i64.pow(6)),
        ("k", 1_000),
        ("K", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
        ("P", 1_000_000_000_000_000),
        ("E", 1_000_000_000_000_000_000),
    ];
    for (suffix, mult) in SUFFIXES {
        if let Some(head) = q.strip_suffix(suffix) {
            let n: i64 = head.trim().parse().ok()?;
            return n.checked_mul(*mult);
        }
    }
    q.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_and_decimal_suffixes_are_not_confused() {
        // The 7.4% gap that presents as a disk full at 93% while monitoring
        // says there is room.
        assert_eq!(parse_quantity("1Gi"), Some(1_073_741_824));
        assert_eq!(parse_quantity("1G"), Some(1_000_000_000));
        assert_ne!(parse_quantity("1Gi"), parse_quantity("1G"));
    }

    #[test]
    fn the_longest_suffix_wins_so_ki_is_not_read_as_k() {
        assert_eq!(parse_quantity("1Ki"), Some(1024));
        assert_eq!(parse_quantity("1k"), Some(1_000));
        assert_eq!(parse_quantity("1K"), Some(1_000));
    }

    #[test]
    fn a_bare_number_is_bytes() {
        assert_eq!(parse_quantity("1000"), Some(1000));
        assert_eq!(parse_quantity(" 2048 "), Some(2048));
    }

    #[test]
    fn an_unparseable_quantity_is_none_never_a_guess() {
        // A guessed size silently provisions the wrong disk.
        for bad in ["", "  ", "abc", "1.5Gi", "Gi", "-", "1Gib", "9999999999Ei"] {
            assert_eq!(parse_quantity(bad), None, "{bad:?}");
        }
    }

    #[tokio::test]
    async fn the_default_provisioner_refuses_by_name() {
        let p = NoCsiProvisioner;
        assert!(!p.can_provision("ebs.csi.aws.com").await);
        let e = p
            .create_volume(&CsiCreateRequest {
                driver: "ebs.csi.aws.com".into(),
                name: "pvc-1".into(),
                capacity_bytes: 1,
                parameters: BTreeMap::new(),
                multi_node: false,
            })
            .await
            .unwrap_err();
        assert!(e.contains("ebs.csi.aws.com"), "{e}");
    }
}
