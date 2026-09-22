//! The daemon's side of the control plane: its answer to every operation
//! of `spec/engenho-control.openapi.yaml`.
//!
//! * [`service`] — [`DaemonControl`], the one [`engenho_control_types::EngenhoControl`].
//! * [`overrides`] — the override tier: `data_dir/control/overrides.yaml`.
//! * [`apply`] — the pipeline every configuration change goes through.
//! * [`names`] — the runtime's children by the API's names (a compiled bijection).
//! * [`ring`] — the sequenced ring the streams page through.
//! * [`logs`] — the tracing layer that fills the log ring.

pub mod apply;
mod configure;
pub mod confirm;
mod destructive;
pub mod logs;
pub mod names;
pub mod overrides;
pub mod reinit;
pub mod ring;
pub mod service;

pub use apply::{ApplyEffect, LeafChange};
pub use logs::{LOGS, LogEntry, LogLayer};
pub use overrides::{Durability, OverrideSet, OverrideStore};
pub use ring::{Page, Ring, Sequenced};
pub use service::{DaemonControl, DaemonControlParts, RemoteFacts, SocketFacts, wire};
