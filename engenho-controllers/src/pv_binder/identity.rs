//! A claim's volume identity is its UID, never its name.
//!
//! ★ THE DEFECT. The binder used to name a dynamically provisioned PV
//! `pvc-<ns>-<name>` and back it with `<root>/<ns>-<name>`. A name is
//! reusable; an object's uid is not. Delete a claim, create another under
//! the same namespace and name, and the new claim was handed the same PV
//! name and the same directory: the provision re-Put the old PV over itself
//! and the new claim mounted every byte the deleted claim had written.
//!
//! ★ THE SHAPE.
//!
//!   * [`ClaimUid`] is the only way to hold a claim's uid, and
//!     [`ClaimUid::of`] is its only constructor. It reads `metadata.uid` and
//!     refuses an absent one ([`NoVolumeIdentity::UidAbsent`]) and one that
//!     cannot safely name a volume or a directory
//!     ([`NoVolumeIdentity::UidMalformed`]): the uid becomes part of a
//!     filesystem path, so `../..` is not a uid.
//!   * [`PvName::for_claim`] is the only constructor of a dynamic PV's name,
//!     and it takes a `&ClaimUid`, so a name derived from anything else does
//!     not compile. It renders upstream's `pvc-<uid>`.
//!   * [`PvName::local_path_dir`] is the only constructor of the backing
//!     directory, `<root>/pvc-<uid>_<ns>_<name>` (the rancher local-path
//!     layout: the uid keeps it unique, the namespace and name keep it
//!     readable on the host).
//!
//! Tier-honest: deriving a name or a directory from a claim's name alone is
//! unrepresentable through these types. The binder could still build a
//! string by hand; only review keeps it on this path.

use std::fmt;

use serde_json::Value;

/// Upstream's prefix for a dynamically provisioned PV.
const PV_PREFIX: &str = "pvc-";

/// The longest uid accepted: a DNS label, so `pvc-<uid>` is a valid object
/// name. An upstream uid is a 36-character UUID.
const MAX_UID_LEN: usize = 63;

/// Why a claim cannot be given a volume yet. The claim stays Pending, and
/// [`note`](Self::note) is the sweep's report note.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoVolumeIdentity {
    /// `metadata.uid` is absent or null.
    UidAbsent,
    /// `metadata.uid` is present but is not a lowercase DNS label, so it
    /// cannot name a PV or a directory.
    UidMalformed,
    /// The claim's namespace or name is not a single path component, so
    /// the backing directory would not be one directory under the root.
    NameNotAPathComponent,
}

impl NoVolumeIdentity {
    /// A fixed description, used as the sweep's skip note.
    #[must_use]
    pub const fn note(self) -> &'static str {
        match self {
            Self::UidAbsent => {
                "PVC has no metadata.uid; a volume is named by the claim's uid, so the claim \
                 stays Pending until it has one"
            }
            Self::UidMalformed => {
                "PVC metadata.uid is not a lowercase DNS label, so it cannot name a volume or \
                 its directory; the claim stays Pending"
            }
            Self::NameNotAPathComponent => {
                "PVC namespace or name is empty or contains a path separator, so it cannot \
                 name a local-path directory; the claim stays Pending"
            }
        }
    }
}

impl fmt::Display for NoVolumeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.note())
    }
}

/// A claim's `metadata.uid`, checked to be safe as part of an object name
/// and a directory name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimUid(String);

impl ClaimUid {
    /// Read `metadata.uid` off a claim.
    ///
    /// # Errors
    ///
    /// [`NoVolumeIdentity::UidAbsent`] when the field is absent or null;
    /// [`NoVolumeIdentity::UidMalformed`] when it is not a string, or not a
    /// lowercase DNS label (1-63 of `a-z`, `0-9`, `-`, alphanumeric at both
    /// ends).
    pub fn of(claim: &Value) -> Result<Self, NoVolumeIdentity> {
        match claim.get("metadata").and_then(|m| m.get("uid")) {
            None | Some(Value::Null) => Err(NoVolumeIdentity::UidAbsent),
            Some(Value::String(uid)) => Self::recorded(uid),
            Some(_) => Err(NoVolumeIdentity::UidMalformed),
        }
    }

    /// The uid a PV's `claimRef` recorded for its claim, held to the same
    /// rule as the claim's own: at reclaim the backing directory is derived
    /// from it again, and a directory about to be removed is exactly where
    /// `../..` must not reach.
    ///
    /// # Errors
    ///
    /// [`NoVolumeIdentity::UidMalformed`] when `uid` is not a lowercase DNS
    /// label.
    pub fn recorded(uid: &str) -> Result<Self, NoVolumeIdentity> {
        if is_dns_label(uid) {
            Ok(Self(uid.to_owned()))
        } else {
            Err(NoVolumeIdentity::UidMalformed)
        }
    }

    /// The uid.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Does a `claimRef.uid` name this claim? Only a string equal to it
    /// does; a number or an object is not a uid that can be checked.
    #[must_use]
    pub fn is(&self, claim_ref_uid: &Value) -> bool {
        claim_ref_uid.as_str() == Some(self.as_str())
    }
}

impl fmt::Display for ClaimUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The name of the PV dynamically provisioned for one claim: `pvc-<uid>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PvName {
    uid: ClaimUid,
}

impl PvName {
    /// The PV name for the claim with this uid. The only constructor.
    #[must_use]
    pub fn for_claim(uid: &ClaimUid) -> Self {
        Self { uid: uid.clone() }
    }

    /// Is `name` this PV's name?
    #[must_use]
    pub fn names(&self, name: &str) -> bool {
        name.strip_prefix(PV_PREFIX) == Some(self.uid.as_str())
    }

    /// The local-path backing directory, `<root>/pvc-<uid>_<ns>_<name>`.
    ///
    /// # Errors
    ///
    /// [`NoVolumeIdentity::NameNotAPathComponent`] when `namespace` or
    /// `name` is empty or holds `/`, `\` or NUL. The `pvc-` prefix already
    /// keeps the component from being `.` or `..`.
    pub fn local_path_dir<'a>(
        &'a self,
        root: &'a str,
        namespace: &'a str,
        name: &'a str,
    ) -> Result<LocalPathDir<'a>, NoVolumeIdentity> {
        if is_path_component(namespace) && is_path_component(name) {
            Ok(LocalPathDir {
                root: root.trim_end_matches('/'),
                pv: self,
                namespace,
                name,
            })
        } else {
            Err(NoVolumeIdentity::NameNotAPathComponent)
        }
    }
}

impl fmt::Display for PvName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PV_PREFIX)?;
        f.write_str(self.uid.as_str())
    }
}

/// A local-path PV's backing directory. Built only by
/// [`PvName::local_path_dir`]; renders `<root>/pvc-<uid>_<ns>_<name>`.
#[derive(Debug, Clone, Copy)]
pub struct LocalPathDir<'a> {
    root: &'a str,
    pv: &'a PvName,
    namespace: &'a str,
    name: &'a str,
}

impl fmt::Display for LocalPathDir<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}_{}_{}",
            self.root, self.pv, self.namespace, self.name
        )
    }
}

/// A lowercase DNS label: 1-63 of `a-z0-9-`, alphanumeric at both ends.
fn is_dns_label(s: &str) -> bool {
    let bytes = s.as_bytes();
    let alnum = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    match (bytes.first(), bytes.last()) {
        (Some(first), Some(last)) => {
            bytes.len() <= MAX_UID_LEN
                && alnum(first)
                && alnum(last)
                && bytes.iter().all(|b| alnum(b) || *b == b'-')
        }
        _ => false,
    }
}

/// One path component: non-empty, and no separator or NUL.
fn is_path_component(s: &str) -> bool {
    !s.is_empty() && !s.contains(['/', '\\', '\0'])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn claim(uid: &Value) -> Value {
        json!({ "metadata": { "name": "data", "namespace": "ns1", "uid": uid } })
    }

    #[test]
    fn a_uuid_uid_names_the_pv_and_the_directory() {
        let uid = ClaimUid::of(&claim(&json!("3f2a9c1e-0b7d-4e8a-9f10-2c3d4e5f6a7b"))).unwrap();
        let pv = PvName::for_claim(&uid);
        assert_eq!(pv.to_string(), "pvc-3f2a9c1e-0b7d-4e8a-9f10-2c3d4e5f6a7b");
        assert!(pv.names("pvc-3f2a9c1e-0b7d-4e8a-9f10-2c3d4e5f6a7b"));
        assert_eq!(
            pv.local_path_dir("/data/local-path/", "ns1", "data")
                .unwrap()
                .to_string(),
            "/data/local-path/pvc-3f2a9c1e-0b7d-4e8a-9f10-2c3d4e5f6a7b_ns1_data"
        );
    }

    /// Two incarnations of one namespace/name share nothing.
    #[test]
    fn two_uids_under_one_name_get_two_volumes() {
        let first = PvName::for_claim(&ClaimUid::of(&claim(&json!("uid-first"))).unwrap());
        let second = PvName::for_claim(&ClaimUid::of(&claim(&json!("uid-second"))).unwrap());
        assert_ne!(first, second);
        assert!(!second.names(&first.to_string()));
        assert!(
            !second.names("pvc-ns1-data"),
            "a legacy name is not this claim's"
        );
        assert_ne!(
            first
                .local_path_dir("/r", "ns1", "data")
                .unwrap()
                .to_string(),
            second
                .local_path_dir("/r", "ns1", "data")
                .unwrap()
                .to_string()
        );
    }

    #[test]
    fn an_absent_or_null_uid_is_absent() {
        assert_eq!(
            ClaimUid::of(&json!({"metadata": {"name": "data"}})),
            Err(NoVolumeIdentity::UidAbsent)
        );
        assert_eq!(
            ClaimUid::of(&claim(&Value::Null)),
            Err(NoVolumeIdentity::UidAbsent)
        );
        assert_eq!(ClaimUid::of(&json!({})), Err(NoVolumeIdentity::UidAbsent));
    }

    /// The uid becomes part of a path. Anything that is not a DNS label is
    /// refused, a traversal first among them.
    #[test]
    fn a_uid_that_is_not_a_dns_label_is_malformed() {
        for bad in [
            json!(""),
            json!("../../etc"),
            json!("a/b"),
            json!("UPPER"),
            json!("-leading"),
            json!("trailing-"),
            json!("has_underscore"),
            json!("x".repeat(MAX_UID_LEN + 1)),
            json!(42),
            json!({"uid": "x"}),
        ] {
            assert_eq!(
                ClaimUid::of(&claim(&bad)),
                Err(NoVolumeIdentity::UidMalformed),
                "{bad}"
            );
        }
        assert!(ClaimUid::of(&claim(&json!("x".repeat(MAX_UID_LEN)))).is_ok());
        assert!(ClaimUid::of(&claim(&json!("u-1"))).is_ok());
    }

    #[test]
    fn a_namespace_or_name_that_is_not_one_component_has_no_directory() {
        let pv = PvName::for_claim(&ClaimUid::of(&claim(&json!("u-1"))).unwrap());
        for (ns, name) in [
            ("", "data"),
            ("ns1", ""),
            ("ns1", "a/../../b"),
            ("n\\s", "d"),
        ] {
            assert_eq!(
                pv.local_path_dir("/r", ns, name).map(|d| d.to_string()),
                Err(NoVolumeIdentity::NameNotAPathComponent),
                "{ns:?}/{name:?}"
            );
        }
    }

    /// A recorded uid is read by the claim's own rule, so reclaim derives
    /// the directory provision derived and nothing else.
    #[test]
    fn a_recorded_uid_obeys_the_claims_rule() {
        let provisioned = ClaimUid::of(&claim(&json!("u-1"))).unwrap();
        assert_eq!(ClaimUid::recorded("u-1"), Ok(provisioned));
        for bad in ["", "../../etc", "a/b", "UPPER"] {
            assert_eq!(
                ClaimUid::recorded(bad),
                Err(NoVolumeIdentity::UidMalformed),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn only_an_equal_string_claim_ref_uid_names_the_claim() {
        let uid = ClaimUid::of(&claim(&json!("u-1"))).unwrap();
        assert!(uid.is(&json!("u-1")));
        assert!(!uid.is(&json!("u-2")));
        assert!(!uid.is(&Value::Null));
        assert!(!uid.is(&json!(1)));
    }
}
