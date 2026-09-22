//! The daemon's side of the control plane: its answer to every operation
//! of `spec/engenho-control.openapi.yaml`.
//!
//! * [`service`] — [`DaemonControl`], the one [`engenho_control_types::EngenhoControl`].
//! * [`names`] — the runtime's children by the API's names (a compiled bijection).
//! * [`ring`] — the sequenced ring the streams page through.
//! * [`logs`] — the tracing layer that fills the log ring.

pub mod logs;
pub mod names;
pub mod ring;
pub mod service;

pub use logs::{LOGS, LogEntry, LogLayer};
pub use ring::{Page, Ring, Sequenced};
pub use service::{DaemonControl, DaemonControlParts, SocketFacts, wire};
