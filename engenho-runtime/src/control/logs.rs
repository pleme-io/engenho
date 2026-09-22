//! The daemon's recent log, served by the control plane.
//!
//! A [`LogLayer`] sits beside the daemon's stdout formatter in the tracing
//! subscriber and keeps the last [`LOGS`] events in a [`Ring`], so `engenho
//! ctl logs list` reads what the daemon said without access to wherever its
//! stdout went (a journal, a launchd log file, nowhere).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

use super::ring::Ring;

/// How many log lines are kept.
pub const LOGS: usize = 4096;

/// One log event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Its level.
    pub level: Level,
    /// Its target (the module that logged it).
    pub target: String,
    /// Its message.
    pub message: String,
    /// Its other fields, rendered.
    pub fields: BTreeMap<String, String>,
}

/// Keeps every event it sees in a ring.
#[derive(Debug, Clone)]
pub struct LogLayer {
    ring: Arc<Ring<LogEntry>>,
}

impl LogLayer {
    /// A layer and the ring it fills.
    #[must_use]
    pub fn new() -> (Self, Arc<Ring<LogEntry>>) {
        let ring = Arc::new(Ring::new(LOGS));
        (
            Self {
                ring: Arc::clone(&ring),
            },
            ring,
        )
    }
}

impl<S: Subscriber> Layer<S> for LogLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let meta = event.metadata();
        self.ring.push(LogEntry {
            level: *meta.level(),
            target: meta.target().to_owned(),
            message: fields.message,
            fields: fields.rest,
        });
    }
}

#[derive(Default)]
struct Fields {
    message: String,
    rest: BTreeMap<String, String>,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        } else {
            self.rest.insert(field.name().to_owned(), value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        let rendered = format!("{value:?}");
        if field.name() == "message" {
            self.message = rendered;
        } else {
            self.rest.insert(field.name().to_owned(), rendered);
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn every_event_lands_in_the_ring_with_its_fields() {
        let (layer, ring) = LogLayer::new();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(phase = "open_store", attempt = 2, "boot failed");
            tracing::info!("hello");
        });
        let page = ring.page(0, 10, |_| true);
        assert_eq!(page.items.len(), 2);
        let first = &page.items[0].item;
        assert_eq!(first.level, Level::WARN);
        assert_eq!(first.message, "boot failed");
        assert_eq!(
            first.fields.get("phase").map(String::as_str),
            Some("open_store")
        );
        assert_eq!(first.fields.get("attempt").map(String::as_str), Some("2"));
        let warnings = ring.page(0, 10, |e| e.level <= Level::WARN);
        assert_eq!(warnings.items.len(), 1);
    }
}
