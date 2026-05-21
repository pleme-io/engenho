//! Cross-kind typed primitives.
//!
//! Lives at the engenho-types root so it can be shared by
//! `generated_v1_34/<group>/<kind>_spec.rs` without crossing
//! module boundaries that would require pub-re-exports per kind.
//!
//! Current primitives:
//!   * [`quantity::Quantity`] — K8s's `1Gi`/`100m`/`2.5` typed
//!     numeric. Wire shape: bare string; in-memory: typed milli-units.
//!   * [`int_or_string::IntOrString`] — polyglot port/scale type.
//!     Wire shape: int OR string; in-memory: tagged enum.

pub mod int_or_string;
pub mod quantity;

pub use int_or_string::IntOrString;
pub use quantity::{Quantity, QuantityParseError, Suffix};
