//! The confirmation handshake every destructive operation passes (P7).
//!
//! 1. **Prepare** (`createConfirmation`, `engenho ctl reinit prepare`): the
//!    daemon binds a challenge to what the operation would act on — the
//!    cluster and node names, the CA's fingerprint, the stop epoch for an
//!    operation that touches the store, the operation and its parameters, and
//!    who asked — and hands back a single-use id and the phrase to type (the
//!    cluster's name). It stands for [`TTL`].
//! 2. **Execute**: the operation carries the id (`Engenho-Confirmation`) and
//!    the phrase. The daemon takes the challenge out of the book — it is used
//!    up whatever happens next — and checks it ([`Binding::check`]): the same
//!    operation and parameters, the same caller, the phrase, and, computed
//!    afresh, the same cluster, node, CA and epoch.
//!
//! What this proves, tier-honestly: the operation is aimed at the cluster
//! the operator named, as it was when they looked, by whoever looked, once.
//! It does not prove a human typed it — which is why no destructive
//! operation is ever an MCP tool.
//!
//! No MAC. The plan named `Selo` (a keyed-hash capability token) for the
//! challenge. The client never carries the binding, only a 128-bit random id
//! the daemon keeps beside it, so there is nothing for a MAC to protect; and
//! single use needs the daemon's state whatever the token looks like.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};
use engenho_apiserver::pki_inventory::CaFact;
use engenho_control_types::types;

use crate::entropy;

/// How long a challenge stands.
pub const TTL: TimeDelta = TimeDelta::seconds(120);

/// The most challenges the daemon holds at once. Only a destructive-tier
/// caller can issue one, but a bound on what it can make the daemon hold
/// costs nothing.
pub const PENDING_MAX: usize = 64;

/// What the CA was, when a challenge was bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaBound {
    /// A CA certificate with this SHA-256 (lowercase hex).
    Present(String),
    /// No CA.
    Absent,
    /// Something there that does not read as a CA.
    Unreadable,
}

impl CaBound {
    /// What the PKI inventory says about the CA.
    #[must_use]
    pub fn of(fact: &CaFact) -> Self {
        match fact {
            CaFact::Present { sha256, .. } => Self::Present(sha256.clone()),
            CaFact::Absent => Self::Absent,
            CaFact::Unreadable(_) => Self::Unreadable,
        }
    }

    /// The control API's view of it. A fingerprint the API's pattern refuses
    /// (not expected: the inventory writes lowercase SHA-256 hex) is not a
    /// CA anything can bind to, and reads as unreadable.
    #[must_use]
    pub fn to_wire(&self) -> types::CaBinding {
        match self {
            Self::Present(sha256) => types::Sha256Hex::try_from(sha256.as_str())
                .map_or(types::CaBinding::Unreadable, types::CaBinding::Present),
            Self::Absent => types::CaBinding::Absent,
            Self::Unreadable => types::CaBinding::Unreadable,
        }
    }
}

/// Whether a challenge is bound to the runtime staying stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochBound {
    /// It touches the store: it runs only while the runtime rests in the
    /// stop epoch it was prepared in.
    Stopped(u64),
    /// It does not.
    NotRequired,
}

/// Everything a challenge is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The cluster's name: the phrase the operator types.
    pub cluster_name: String,
    /// This node's name.
    pub node_name: String,
    /// The CA.
    pub ca: CaBound,
    /// The epoch, for an operation that touches the store.
    pub epoch: EpochBound,
    /// The operation.
    pub operation: types::ReinitOp,
    /// BLAKE3 of the operation's request, parameters included.
    pub params: blake3::Hash,
    /// BLAKE3 of what the transport attested about the caller.
    pub principal: blake3::Hash,
}

/// How an executed operation differs from the challenge it names.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Mismatch {
    /// The challenge was prepared for another operation.
    #[error("the challenge was prepared for {bound:?}, not {presented:?}")]
    Operation {
        /// What it was prepared for.
        bound: types::ReinitOp,
        /// What was executed.
        presented: types::ReinitOp,
    },
    /// The same operation, other parameters.
    #[error("the challenge was prepared with other parameters")]
    Params,
    /// Someone else prepared it.
    #[error("the challenge was prepared by another caller")]
    Principal,
    /// The phrase is not the cluster's name.
    #[error("the confirmation phrase is not the cluster's name")]
    Phrase,
    /// The cluster is not the one it was prepared against.
    #[error("the cluster is now {now:?}, not {bound:?}")]
    Cluster {
        /// Then.
        bound: String,
        /// Now.
        now: String,
    },
    /// The node is not the one it was prepared on.
    #[error("the node is now {now:?}, not {bound:?}")]
    Node {
        /// Then.
        bound: String,
        /// Now.
        now: String,
    },
    /// The CA changed since.
    #[error("the CA changed since the challenge was prepared")]
    Ca,
    /// The runtime ran since, or is not stopped.
    #[error("the runtime is not resting in the stop epoch the challenge was prepared in")]
    Epoch,
}

impl Binding {
    /// Check `now` — the operation as executed, its facts read afresh —
    /// against this challenge, and `phrase` against the cluster's name.
    ///
    /// # Errors
    ///
    /// The first [`Mismatch`], what was asked before what was found.
    pub fn check(&self, now: &Self, phrase: &str) -> Result<(), Mismatch> {
        if self.operation != now.operation {
            return Err(Mismatch::Operation {
                bound: self.operation,
                presented: now.operation,
            });
        }
        if self.params != now.params {
            return Err(Mismatch::Params);
        }
        if self.principal != now.principal {
            return Err(Mismatch::Principal);
        }
        if phrase != self.cluster_name {
            return Err(Mismatch::Phrase);
        }
        if self.cluster_name != now.cluster_name {
            return Err(Mismatch::Cluster {
                bound: self.cluster_name.clone(),
                now: now.cluster_name.clone(),
            });
        }
        if self.node_name != now.node_name {
            return Err(Mismatch::Node {
                bound: self.node_name.clone(),
                now: now.node_name.clone(),
            });
        }
        if self.ca != now.ca {
            return Err(Mismatch::Ca);
        }
        if self.epoch != now.epoch {
            return Err(Mismatch::Epoch);
        }
        Ok(())
    }

    /// The control API's view of it.
    ///
    /// # Errors
    ///
    /// A hash or fingerprint the API's pattern refuses (not expected: both
    /// are lowercase hex of the right length).
    pub fn to_wire(&self) -> Result<types::ChallengeBinding, String> {
        let hex = |h: &blake3::Hash| {
            types::Blake3Hex::try_from(h.to_hex().as_str()).map_err(|e| e.to_string())
        };
        Ok(types::ChallengeBinding {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            ca: self.ca.to_wire(),
            epoch: match self.epoch {
                EpochBound::Stopped(epoch) => types::EpochBinding::StopEpoch(epoch),
                EpochBound::NotRequired => types::EpochBinding::NotRequired,
            },
            operation: self.operation,
            params_blake3: hex(&self.params)?,
            principal_fingerprint: hex(&self.principal)?,
        })
    }
}

/// BLAKE3 of a value's JSON: the digest a challenge binds parameters by.
/// JSON of the same value is the same bytes (field order is declaration
/// order).
#[must_use]
pub fn digest<T: serde::Serialize>(value: &T) -> blake3::Hash {
    blake3::hash(&serde_json::to_vec(value).unwrap_or_default())
}

/// The digest a challenge binds its caller by: WHO, as the transport
/// attested it — the uid on the local socket, the pinned key (and the name
/// it is authorized under) remotely. Not the process: preparing and
/// executing are two `engenho ctl` runs, with two pids, by one operator.
#[must_use]
pub fn caller(attested: &types::AttestedView) -> blake3::Hash {
    match attested {
        types::AttestedView::LocalUid { uid, .. } => digest(&("local_uid", uid)),
        types::AttestedView::RemotePin { client, spki } => {
            digest(&("remote_pin", client, spki.as_str()))
        }
    }
}

/// A challenge the book issued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issued {
    /// Its single-use id: 32 lowercase hex characters.
    pub id: String,
    /// When it stops standing.
    pub expires_at: DateTime<Utc>,
}

/// Why a challenge was not issued.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IssueError {
    /// [`PENDING_MAX`] stand already.
    #[error("{PENDING_MAX} challenges are already pending; cancel one or let them expire")]
    Full,
    /// No entropy for an id.
    #[error("no entropy for a challenge id: {0}")]
    Entropy(String),
}

/// Why a challenge could not be taken.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedeemError {
    /// No such challenge: never issued, already used, or cancelled.
    #[error("no pending challenge {0}: it was never issued, or it has been used or cancelled")]
    Unknown(String),
    /// It stood until then.
    #[error("the challenge expired at {0}")]
    Expired(DateTime<Utc>),
}

#[derive(Debug)]
struct Pending {
    binding: Binding,
    expires_at: DateTime<Utc>,
}

/// The daemon's pending challenges. Held in memory: a challenge does not
/// outlive the process that issued it.
#[derive(Debug, Default)]
pub struct ConfirmationBook {
    pending: Mutex<HashMap<String, Pending>>,
}

impl ConfirmationBook {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Pending>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Issue a challenge bound to `binding`, standing until [`TTL`] after
    /// `now`. Expired challenges are dropped first.
    ///
    /// # Errors
    ///
    /// [`IssueError`].
    pub fn issue(&self, binding: Binding, now: DateTime<Utc>) -> Result<Issued, IssueError> {
        let mut pending = self.lock();
        pending.retain(|_, p| p.expires_at > now);
        if pending.len() >= PENDING_MAX {
            return Err(IssueError::Full);
        }
        let id = entropy::random::<16>()
            .map(|bytes| entropy::lower_hex(&bytes))
            .map_err(|e| IssueError::Entropy(e.to_string()))?;
        let expires_at = now + TTL;
        pending.insert(
            id.clone(),
            Pending {
                binding,
                expires_at,
            },
        );
        Ok(Issued { id, expires_at })
    }

    /// Take a challenge out of the book: it cannot be used again, whatever
    /// the caller does with it.
    ///
    /// # Errors
    ///
    /// [`RedeemError::Unknown`], or [`RedeemError::Expired`] (taken out all
    /// the same).
    pub fn redeem(&self, id: &str, now: DateTime<Utc>) -> Result<Binding, RedeemError> {
        let taken = self
            .lock()
            .remove(id)
            .ok_or_else(|| RedeemError::Unknown(id.to_owned()))?;
        if taken.expires_at <= now {
            return Err(RedeemError::Expired(taken.expires_at));
        }
        Ok(taken.binding)
    }

    /// Withdraw a challenge without executing anything.
    ///
    /// # Errors
    ///
    /// [`RedeemError::Unknown`].
    pub fn cancel(&self, id: &str) -> Result<(), RedeemError> {
        self.lock()
            .remove(id)
            .map(drop)
            .ok_or_else(|| RedeemError::Unknown(id.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).expect("a moment")
    }

    fn binding() -> Binding {
        Binding {
            cluster_name: "ryn".into(),
            node_name: "ryn".into(),
            ca: CaBound::Present("ab".repeat(32)),
            epoch: EpochBound::Stopped(3),
            operation: types::ReinitOp::WipeStore,
            params: digest(&"store_only"),
            principal: digest(&501_u32),
        }
    }

    #[test]
    fn a_challenge_is_used_once() {
        let book = ConfirmationBook::default();
        let issued = book.issue(binding(), at(0)).expect("issued");
        assert_eq!(issued.id.len(), 32);
        assert!(issued.id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(issued.expires_at, at(120));

        assert_eq!(book.redeem(&issued.id, at(1)), Ok(binding()));
        assert_eq!(
            book.redeem(&issued.id, at(2)),
            Err(RedeemError::Unknown(issued.id.clone())),
            "a used challenge was accepted again"
        );
    }

    #[test]
    fn an_expired_challenge_is_refused_and_used_up() {
        let book = ConfirmationBook::default();
        let issued = book.issue(binding(), at(0)).expect("issued");
        assert_eq!(
            book.redeem(&issued.id, at(120)),
            Err(RedeemError::Expired(at(120))),
            "it stands until, not through, its expiry"
        );
        assert!(matches!(
            book.redeem(&issued.id, at(0)),
            Err(RedeemError::Unknown(_))
        ));
        let fresh = book.issue(binding(), at(0)).expect("issued");
        assert!(book.redeem(&fresh.id, at(119)).is_ok());
    }

    #[test]
    fn a_cancelled_challenge_cannot_be_used() {
        let book = ConfirmationBook::default();
        let issued = book.issue(binding(), at(0)).expect("issued");
        assert_eq!(book.cancel(&issued.id), Ok(()));
        assert!(book.cancel(&issued.id).is_err());
        assert!(matches!(
            book.redeem(&issued.id, at(1)),
            Err(RedeemError::Unknown(_))
        ));
    }

    #[test]
    fn the_book_is_bounded_and_forgets_what_expired() {
        let book = ConfirmationBook::default();
        let ids: Vec<String> = (0..PENDING_MAX)
            .map(|_| book.issue(binding(), at(0)).expect("issued").id)
            .collect();
        assert_eq!(book.issue(binding(), at(1)), Err(IssueError::Full));
        assert_eq!(
            ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
            PENDING_MAX,
            "two challenges share an id"
        );
        assert!(
            book.issue(binding(), at(120)).is_ok(),
            "the expired ones were not dropped"
        );
    }

    /// Every field of the binding is checked, each with its own reason, and
    /// what was asked is checked before what was found.
    #[test]
    fn every_bound_fact_is_checked() {
        type Change = fn(&mut Binding);
        let bound = binding();
        assert_eq!(bound.check(&bound, "ryn"), Ok(()));
        let cases: [(Change, Mismatch); 8] = [
            (
                |b| b.operation = types::ReinitOp::ReseedPki,
                Mismatch::Operation {
                    bound: types::ReinitOp::WipeStore,
                    presented: types::ReinitOp::ReseedPki,
                },
            ),
            (
                |b| b.params = digest(&"store_and_node_local"),
                Mismatch::Params,
            ),
            (|b| b.principal = digest(&0_u32), Mismatch::Principal),
            (
                |b| b.cluster_name = "plo".into(),
                Mismatch::Cluster {
                    bound: "ryn".into(),
                    now: "plo".into(),
                },
            ),
            (
                |b| b.node_name = "plo".into(),
                Mismatch::Node {
                    bound: "ryn".into(),
                    now: "plo".into(),
                },
            ),
            (|b| b.ca = CaBound::Absent, Mismatch::Ca),
            (|b| b.ca = CaBound::Unreadable, Mismatch::Ca),
            (|b| b.epoch = EpochBound::Stopped(4), Mismatch::Epoch),
        ];
        for (change, expected) in cases {
            let mut now = bound.clone();
            change(&mut now);
            assert_eq!(bound.check(&now, "ryn"), Err(expected));
        }
        assert_eq!(bound.check(&bound, "RYN"), Err(Mismatch::Phrase));
        assert_eq!(bound.check(&bound, ""), Err(Mismatch::Phrase));
        let mut retargeted = bound.clone();
        retargeted.operation = types::ReinitOp::RotateAdminToken;
        assert!(
            matches!(
                bound.check(&retargeted, "wrong"),
                Err(Mismatch::Operation { .. })
            ),
            "the operation is checked before the phrase"
        );
    }

    /// Preparing and executing are two processes of one operator: the
    /// caller is the uid, not the pid; another uid, or a remote key, is
    /// someone else.
    #[test]
    fn the_caller_is_who_not_which_process() {
        let local = |uid, pid| types::AttestedView::LocalUid { uid, gid: 20, pid };
        assert_eq!(caller(&local(501, 100)), caller(&local(501, 200)));
        assert_ne!(caller(&local(501, 100)), caller(&local(0, 100)));
        let remote = types::AttestedView::RemotePin {
            client: "ryn".into(),
            spki: types::SpkiSha256::try_from(["sha256:", &"ab".repeat(32)].concat())
                .expect("a pin"),
        };
        assert_ne!(caller(&remote), caller(&local(501, 100)));
    }

    #[test]
    fn the_binding_crosses_to_the_api() {
        let wire = binding().to_wire().expect("crosses");
        assert_eq!(wire.cluster_name, "ryn");
        assert_eq!(wire.epoch, types::EpochBinding::StopEpoch(3));
        assert_eq!(
            wire.params_blake3.as_str(),
            digest(&"store_only").to_hex().as_str()
        );
        let mut absent = binding();
        absent.ca = CaBound::Unreadable;
        absent.epoch = EpochBound::NotRequired;
        let wire = absent.to_wire().expect("crosses");
        assert_eq!(wire.ca, types::CaBinding::Unreadable);
        assert_eq!(wire.epoch, types::EpochBinding::NotRequired);
    }
}
