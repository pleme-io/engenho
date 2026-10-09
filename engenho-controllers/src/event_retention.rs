use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};
use engenho_store::{
    ResourceKey, ResourceValue, StoreMesh,
    command::{Reason, ResourceCommand, TxnOp},
};
use tracing::debug;

use crate::controller::ReconcileReport;
use crate::error::ControllerError;

pub const DEFAULT_EVENT_TTL: Duration = Duration::from_secs(60 * 60);

pub const DEFAULT_MAX_EVENTS: NonZeroUsize = match NonZeroUsize::new(20_000) {
    Some(n) => n,
    None => unreachable!(),
};

pub const DELETES_PER_TXN: NonZeroUsize = match NonZeroUsize::new(1024) {
    Some(n) => n,
    None => unreachable!(),
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventRetention {
    pub ttl: Duration,
    pub max_events: NonZeroUsize,
}

impl Default for EventRetention {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_EVENT_TTL,
            max_events: DEFAULT_MAX_EVENTS,
        }
    }
}

fn parse(stamp: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(stamp)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

#[must_use]
pub fn last_seen(event: &ResourceValue) -> Option<DateTime<Utc>> {
    let field = |v: &serde_json::Value| v.as_str().and_then(parse);
    [
        &event["series"]["lastObservedTime"],
        &event["lastTimestamp"],
        &event["eventTime"],
        &event["metadata"]["creationTimestamp"],
    ]
    .into_iter()
    .filter_map(field)
    .max()
}

impl EventRetention {
    #[must_use]
    pub fn expired(
        &self,
        events: &[(ResourceKey, ResourceValue)],
        now: DateTime<Utc>,
    ) -> Vec<ResourceKey> {
        let horizon = chrono::Duration::from_std(self.ttl)
            .ok()
            .and_then(|ttl| now.checked_sub_signed(ttl));
        let mut kept: Vec<(Option<DateTime<Utc>>, &ResourceKey)> = Vec::new();
        let mut doomed: Vec<ResourceKey> = Vec::new();
        for (key, value) in events {
            let seen = last_seen(value);
            match (seen, horizon) {
                (Some(t), Some(h)) if t < h => doomed.push(key.clone()),
                _ => kept.push((seen, key)),
            }
        }
        if kept.len() > self.max_events.get() {
            kept.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.name.cmp(&b.1.name)));
            let excess = kept.len() - self.max_events.get();
            doomed.extend(kept.into_iter().take(excess).map(|(_, k)| k.clone()));
        }
        doomed
    }
}

pub(crate) async fn expire_events(
    store: &StoreMesh,
    namespace: Option<&str>,
    retention: EventRetention,
    report: &mut ReconcileReport,
) -> Result<(), ControllerError> {
    let events = store.list("", "v1", "Event", namespace).await;
    report.objects_examined += events.len();
    let doomed = retention.expired(&events, Utc::now());
    drop(events);
    if doomed.is_empty() {
        return Ok(());
    }
    let stamp = engenho_types::time::now_rfc3339_utc();
    for chunk in doomed.chunks(DELETES_PER_TXN.get()) {
        let success = chunk
            .iter()
            .map(|key| TxnOp::Delete {
                key: key.clone(),
                deletion_timestamp: Some(stamp.clone()),
            })
            .collect();
        store
            .propose(ResourceCommand::Txn {
                compares: Vec::new(),
                success,
                failure: Vec::new(),
                reason: Reason::GarbageCollector,
            })
            .await?;
        report.objects_changed += chunk.len();
    }
    debug!(expired = doomed.len(), "events past retention deleted");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(name: &str, stamp: &str) -> (ResourceKey, ResourceValue) {
        (
            ResourceKey::namespaced("", "v1", "Event", "default", name),
            json!({"metadata": {"name": name}, "lastTimestamp": stamp}),
        )
    }

    fn now() -> DateTime<Utc> {
        parse("2026-10-08T22:00:00Z").expect("now")
    }

    fn names(keys: Vec<ResourceKey>) -> Vec<String> {
        let mut n: Vec<String> = keys.into_iter().map(|k| k.name).collect();
        n.sort();
        n
    }

    #[test]
    fn events_older_than_the_ttl_expire() {
        let events = [
            ev("old", "2026-10-08T20:59:59Z"),
            ev("edge", "2026-10-08T21:00:00Z"),
            ev("fresh", "2026-10-08T21:59:00Z"),
        ];
        assert_eq!(
            names(EventRetention::default().expired(&events, now())),
            ["old"]
        );
    }

    #[test]
    fn the_newest_timestamp_decides() {
        let events = [(
            ResourceKey::namespaced("", "v1", "Event", "default", "repeated"),
            json!({
                "metadata": {"creationTimestamp": "2026-10-08T10:00:00Z"},
                "series": {"lastObservedTime": "2026-10-08T21:30:00.000000Z"},
            }),
        )];
        assert!(EventRetention::default().expired(&events, now()).is_empty());
    }

    #[test]
    fn the_cap_evicts_the_oldest_first() {
        let retention = EventRetention {
            ttl: DEFAULT_EVENT_TTL,
            max_events: NonZeroUsize::new(2).expect("two"),
        };
        let events = [
            ev("c", "2026-10-08T21:50:00Z"),
            ev("a", "2026-10-08T21:10:00Z"),
            (
                ResourceKey::namespaced("", "v1", "Event", "default", "untimed"),
                json!({"metadata": {"name": "untimed"}}),
            ),
            ev("b", "2026-10-08T21:30:00Z"),
        ];
        assert_eq!(names(retention.expired(&events, now())), ["a", "untimed"]);
    }

    #[test]
    fn an_untimed_event_never_expires_by_age() {
        let events = [(
            ResourceKey::namespaced("", "v1", "Event", "default", "untimed"),
            json!({"metadata": {"name": "untimed"}}),
        )];
        assert!(EventRetention::default().expired(&events, now()).is_empty());
    }
}
