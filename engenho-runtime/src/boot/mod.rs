//! A boot, as something that can be watched, cancelled and judged: the
//! phases it runs through ([`phase`]) and what a failure means for the next
//! attempt ([`failure`]).

pub mod failure;
pub mod phase;

pub use failure::{BACKOFF_CAP, BACKOFF_FLOOR, FailureClass, backoff};
pub use phase::{BootKind, BootPhase, BootProgress, BootRecorder, Timestamp};
