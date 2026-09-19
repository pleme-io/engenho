//! Shared by the two watch oracle tests (`oracle_watch_410.rs`,
//! `oracle_watch_429.rs`): an engenho cluster whose revisions a row can
//! place, and a model of the two clients that read engenho's watch ends.

// Each test binary uses a different subset.
#![allow(dead_code)]

pub mod clients;
pub mod cluster;

use serde_json::Value;

/// `text` when the typed decision agrees with the one the row's prose
/// describes, and otherwise the decision itself, so a disagreement shows the
/// value that was reached rather than a bare `false`.
pub fn says(agrees: bool, text: &str, actually: impl std::fmt::Debug) -> Value {
    if agrees {
        Value::String(text.to_owned())
    } else {
        Value::String(format!("{actually:?}"))
    }
}
