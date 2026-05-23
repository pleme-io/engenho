//! Sample fixtures extracted per the third-site rule.
//!
//! Test files across the substrate-props crate repeatedly hand-rolled
//! the same throwaway constructors:
//!
//!   - `fn drv(b: u8) -> Drv` — 4 files (derivation, watched_cache,
//!     tiered_cache, oci_renderer)
//!   - `fn receipt(subject, node_b) -> MaterializationReceipt` — 3
//!     files (ledger, broadcast_ledger, gossip_ledger)
//!   - `fn emitter(b: u8) -> NodeId` — 3 files (chained_verifier,
//!     verifier_impls, gossip_ledger)
//!   - `fn nar(b: u8) -> NarBlob` — 2 files (watched_cache,
//!     tiered_cache)
//!
//! Extracted here so every future property test reaches for one
//! canonical constructor instead of re-implementing the shape.
//!
//! ## Usage
//!
//! ```ignore
//! use engenho_substrate_props::helpers::{sample_drv, sample_emitter, sample_receipt};
//!
//! proptest_with_env! {
//!     #[test]
//!     fn my_invariant(b in any::<u8>()) {
//!         let drv = sample_drv(b);
//!         // ...
//!     }
//! }
//! ```

use engenho_substrate::{
    Drv, DrvHash, FrozenClock, MaterializationReceipt, NarBlob, NodeId, ReceiptKind,
};
use std::sync::Arc;

/// Sample synthetic `Drv` with `drv_hash = [b; 32]` and the default
/// `x86_64-linux` system tag.
#[must_use]
pub fn sample_drv(b: u8) -> Drv {
    Drv::synthetic(DrvHash::new([b; 32]), "x86_64-linux")
}

/// Sample `NodeId` with all-bytes-equal-`b` (`[b; 32]`).
#[must_use]
pub fn sample_emitter(b: u8) -> NodeId {
    NodeId::from_bytes(&[b; 32])
}

/// Sample `NarBlob` whose payload is 16 bytes of `b`.
#[must_use]
pub fn sample_nar(b: u8) -> NarBlob {
    NarBlob::from_bytes(vec![b; 16])
}

/// Sample `MaterializationReceipt` carrying `ReceiptKind::Shape("test")`,
/// the given `subject` + `node_byte`-derived emitter, `emitted_at = 0`,
/// and `evidence == subject` (faithful-node convergence shape).
#[must_use]
pub fn sample_receipt(subject: [u8; 32], node_byte: u8) -> MaterializationReceipt {
    MaterializationReceipt::new(
        ReceiptKind::Shape("test".into()),
        subject,
        sample_emitter(node_byte),
        0,
        subject,
    )
}

/// Construct an `Arc<FrozenClock>` pinned at `t_ms` ms-since-epoch.
/// Extracted per the third-site rule — the `Arc::new(FrozenClock::at(t))`
/// pattern appeared in 11+ test files.
///
/// Returns the concrete `Arc<FrozenClock>` (not `Arc<dyn Clock>`) so
/// callers can still reach `.advance()` / `.tick_logical()` via Deref.
/// Pass to APIs expecting `Arc<dyn Clock>` via `clock.clone() as
/// Arc<dyn Clock>`.
#[must_use]
pub fn frozen_clock(t_ms: u64) -> Arc<FrozenClock> {
    Arc::new(FrozenClock::at(t_ms))
}
