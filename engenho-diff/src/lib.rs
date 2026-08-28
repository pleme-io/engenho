//! # engenho-diff
//!
//! A differential integration-test harness: detect **where engenho's
//! Kubernetes surface diverges from k3s**, using a live k3s cluster as the
//! reference oracle. This is engenho's mechanical path to CNCF conformance —
//! every divergence it surfaces is a typed, attributed gap to close.
//!
//! ## Shape
//!
//! One authored set of typed [`Operation`]s is fired at TWO [`DiffTarget`]s
//! — engenho ([`EngenhoTarget`], booted in-process) and k3s
//! ([`K3sTarget`], the oracle) — and each paired [`Observation`] is folded
//! through a typed [`Normalizer`] into a recursive JSON differ that yields a
//! typed [`Verdict`]:
//!
//! ```text
//!            ┌──────────── Operation ────────────┐
//!            ▼                                    ▼
//!     EngenhoTarget.raw()                  K3sTarget.raw()
//!            │                                    │
//!         Observation ───┐          ┌─── Observation
//!                        ▼          ▼
//!                    Normalizer (mask volatile fields)
//!                        │          │
//!                        ▼          ▼
//!                 cotejo::diff_observations  → Verdict
//!                 { Parity | Divergent(..) | ReferenceUnreachable }
//! ```
//!
//! [`Verdict::Parity`] is proof-by-absence — no code outside this crate can
//! construct one (★★ UNREPRESENTABILITY). [`JsonPath`] is a typed value,
//! rendered only through `Display` (★★ TYPED EMISSION).

pub mod cotejo;
pub mod junkyo;
pub mod normalize;
pub mod observe;
pub mod op;
pub mod path;
pub mod target;
pub mod verdict;

pub use cotejo::{diff_observations, run, run_strict};
pub use junkyo::{DivergenceClass, Expect, MatrixReport, Outcome, adjudicate};
pub use normalize::{Mask, Normalizer, volatile_meta};
pub use observe::{DiffError, HttpMethod, Observation};
pub use op::{OpKind, Operation, RestPath};
pub use path::{JsonPath, PathSeg};
pub use target::{DiffTarget, EngenhoTarget, K3sTarget};
pub use verdict::{Divergence, Gvr, ParityWitness, Severity, Side, StatusCause, Verb, Verdict};
