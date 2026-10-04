use kazari::prelude::*;
use kazari::{Capability, Stream};
use serde::Serialize;

pub fn colored(stream: Stream) -> bool {
    Capability::probe_stream(stream).level.is_colored()
}

pub fn severity(word: &str) -> Option<CalloutSeverity> {
    match word {
        "running"
        | "live"
        | "present"
        | "written"
        | "in_sync"
        | "completed"
        | "succeeded"
        | "current"
        | "issued"
        | "held_by_this_daemon"
        | "serving"
        | "up"
        | "clean_stop" => Some(CalloutSeverity::Ok),
        "booting" | "resolving" | "draining" | "in_progress" | "restart_needed"
        | "changed_since_load" | "present_offline" | "not_yet_known" | "exiting" | "unknown"
        | "truncated" => Some(CalloutSeverity::Warn),
        "failed"
        | "dead"
        | "wedged"
        | "unreadable"
        | "unclean"
        | "panicked"
        | "held_by_other_process"
        | "down"
        | "stopped"
        | "unavailable" => Some(CalloutSeverity::Error),
        "absent" | "skipped" | "disabled" | "never" | "not_configured" | "not_recorded"
        | "first_ever" | "free" | "ephemeral" | "cancelled" | "none" => Some(CalloutSeverity::Note),
        _ => None,
    }
}

pub fn role(word: &str) -> Role {
    severity(word).map_or(Role::Text, CalloutSeverity::role)
}

pub fn word(text: impl Into<String>) -> Fragment {
    let text = text.into();
    let role = role(&text);
    Fragment::styled(text, role)
}

pub fn detail(text: impl Into<String>) -> Fragment {
    Fragment::styled(text, Role::TextMuted)
}

pub fn tag<T: Serialize>(value: &T, key: &str) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(v) => v
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        Err(_) => "unknown".to_owned(),
    }
}

pub fn document(value: &serde_json::Value) -> Document {
    Document::new(Node::from(value)).classify(|_, text| severity(text))
}

#[allow(
    clippy::disallowed_macros,
    reason = "a human-facing duration, never wire or syntax"
)]
pub fn ago(since: chrono::DateTime<chrono::Utc>) -> String {
    let secs = (chrono::Utc::now() - since).num_seconds().max(0);
    let (d, h, m, s) = (secs / 86_400, secs / 3600 % 24, secs / 60 % 60, secs % 60);
    match (d, h, m) {
        (0, 0, 0) => format!("{s}s"),
        (0, 0, _) => format!("{m}m {s}s"),
        (0, _, _) => format!("{h}h {m}m"),
        _ => format!("{d}d {h}h"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_words_carry_their_severity() {
        assert_eq!(severity("running"), Some(CalloutSeverity::Ok));
        assert_eq!(severity("restart_needed"), Some(CalloutSeverity::Warn));
        assert_eq!(severity("dead"), Some(CalloutSeverity::Error));
        assert_eq!(severity("skipped"), Some(CalloutSeverity::Note));
        assert_eq!(severity("/var/lib/engenho"), None);
    }

    #[test]
    fn a_reply_renders_as_yaml_without_colour() {
        let value = serde_json::json!({"facts": {"lock": "held_by_this_daemon", "state": {"store": "live", "revision": 7}}});
        let plain = document(&value).to_string_at(Capability::plain());
        assert_eq!(
            plain,
            "facts:\n  lock: held_by_this_daemon\n  state:\n    revision: 7\n    store: live\n"
        );
    }

    #[test]
    fn tag_reads_an_internally_tagged_enum_and_a_unit_string() {
        assert_eq!(
            tag(
                &serde_json::json!({"state": "running", "since": "x"}),
                "state"
            ),
            "running"
        );
        assert_eq!(tag(&"in_sync", "kind"), "in_sync");
    }

    #[test]
    fn ago_is_compact() {
        let now = chrono::Utc::now();
        assert_eq!(ago(now - chrono::TimeDelta::seconds(5)), "5s");
        assert_eq!(
            ago(now - chrono::TimeDelta::seconds(3 * 86_400 + 2 * 3600 + 7)),
            "3d 2h"
        );
    }
}
