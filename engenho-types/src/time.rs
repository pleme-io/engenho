//! Typed RFC3339 timestamp render surface — the ONE place the platform
//! turns a wall-clock instant into the Kubernetes-wire timestamp string.
//!
//! ## Why this exists (★★ TYPED EMISSION)
//!
//! `metadata.creationTimestamp` + `metadata.deletionTimestamp` are
//! RFC3339 UTC strings on the wire (`2026-06-08T12:34:56Z`). Per the
//! ★★ TYPED EMISSION rule, the platform never `format!()`s a wire
//! string by hand — every emitted string comes from a typed surface.
//! The third allowed surface is "a typed AST renderer / value builder";
//! here the renderer is chrono's typed serializer
//! [`chrono::DateTime::to_rfc3339_opts`]. This module wraps that single
//! call so every timestamp the apiserver/runtime stamps flows through
//! ONE typed render — not a scattered set of ad-hoc format strings.
//!
//! ## Determinism note (the boundary clock read)
//!
//! [`now_rfc3339_utc`] reads the wall clock — it is the ONE
//! intentionally non-deterministic input on the create/delete boundary.
//! It is captured ONCE at the apiserver boundary and frozen into the
//! single replicated `ResourceCommand`, so every Raft replica replays
//! the identical bytes. It MUST NOT be called inside the deterministic
//! store-apply path (`engenho-store::state::apply_*`), which runs
//! per-node on replay — calling a clock there would diverge replicas
//! (the exact failure mode the codebase already avoids for
//! uid/resourceVersion by computing them from replicated inputs).

use chrono::{DateTime, SecondsFormat, Utc};

/// Render a [`DateTime<Utc>`] to the Kubernetes-wire RFC3339 form:
/// second precision, Zulu (`Z`) suffix — e.g. `2026-06-08T12:34:56Z`.
/// This is the kubectl-AGE-parseable shape (matches upstream
/// `metav1.Time` marshalling).
///
/// The typed render surface: the `to_rfc3339_opts` call is chrono's
/// typed serializer; no `format!()` of the wire string.
#[must_use]
pub fn to_rfc3339_utc(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Read the wall clock and render it to the Kubernetes-wire RFC3339
/// form — the apiserver-boundary timestamp stamp.
///
/// The ONE non-deterministic input on the create/delete boundary;
/// capture it once and freeze it into the replicated command (see the
/// module-level determinism note).
#[must_use]
pub fn now_rfc3339_utc() -> String {
    to_rfc3339_utc(Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn now_ends_in_zulu_and_round_trips() {
        let s = now_rfc3339_utc();
        assert!(s.ends_with('Z'), "wire timestamp must be Zulu: {s}");
        // Parses back via chrono — proves it is a valid RFC3339 string.
        let parsed = DateTime::parse_from_rfc3339(&s).expect("valid RFC3339");
        // Round-trip render is byte-identical (idempotent at second precision).
        assert_eq!(to_rfc3339_utc(parsed.with_timezone(&Utc)), s);
    }

    #[test]
    fn second_precision_no_subsecond_digits() {
        let s = now_rfc3339_utc();
        // `…:56Z` — the char before the trailing Z is a digit, and there
        // is no fractional-second dot anywhere in the string.
        assert!(!s.contains('.'), "second precision must drop subseconds: {s}");
    }

    #[test]
    fn deterministic_render_of_fixed_datetime() {
        // A fixed instant renders to a fixed, known string — the property
        // the determinism contract relies on (same DateTime ⇒ same bytes).
        let t = Utc.with_ymd_and_hms(2026, 6, 8, 12, 34, 56).single().unwrap();
        assert_eq!(to_rfc3339_utc(t), "2026-06-08T12:34:56Z");
    }

    #[test]
    fn drops_subseconds_to_second_boundary() {
        // An instant carrying sub-second nanos renders truncated to the
        // second (SecondsFormat::Secs) — deterministic regardless of the
        // sub-second tail.
        let t = Utc
            .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
            .single()
            .unwrap()
            + chrono::Duration::nanoseconds(987_654_321);
        assert_eq!(to_rfc3339_utc(t), "2026-01-02T03:04:05Z");
    }
}
