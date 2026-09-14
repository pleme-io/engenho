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

/// Render a Unix epoch second count to the Kubernetes-wire RFC3339 form.
///
/// Exists for the `TokenRequest` mint path, which computes an expiry in epoch
/// seconds (because that is what a JWT `exp` claim IS) and must echo the same
/// instant back as `status.expirationTimestamp`. Deriving both from ONE
/// integer is what makes the token and the advertised expiry unable to
/// disagree — rendering the timestamp from a second clock read would let them
/// drift by the duration of the mint.
///
/// An out-of-range epoch yields `None` rather than a silently-wrong instant:
/// chrono's conversion is fallible and papering over it with a default would
/// advertise an expiry the token does not carry.
#[must_use]
pub fn epoch_to_rfc3339_utc(secs: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(secs, 0).map(to_rfc3339_utc)
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
        assert!(
            !s.contains('.'),
            "second precision must drop subseconds: {s}"
        );
    }

    #[test]
    fn deterministic_render_of_fixed_datetime() {
        // A fixed instant renders to a fixed, known string — the property
        // the determinism contract relies on (same DateTime ⇒ same bytes).
        let t = Utc
            .with_ymd_and_hms(2026, 6, 8, 12, 34, 56)
            .single()
            .unwrap();
        assert_eq!(to_rfc3339_utc(t), "2026-06-08T12:34:56Z");
    }

    #[test]
    fn drops_subseconds_to_second_boundary() {
        // An instant carrying sub-second nanos renders truncated to the
        // second (SecondsFormat::Secs) — deterministic regardless of the
        // sub-second tail.
        let t = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).single().unwrap()
            + chrono::Duration::nanoseconds(987_654_321);
        assert_eq!(to_rfc3339_utc(t), "2026-01-02T03:04:05Z");
    }
}

#[cfg(test)]
mod epoch_render {
    use super::{epoch_to_rfc3339_utc, now_rfc3339_utc};

    #[test]
    fn renders_the_kubernetes_wire_shape() {
        // Second precision, Zulu suffix — the metav1.Time shape, so a client
        // parsing status.expirationTimestamp sees what it sees from upstream.
        assert_eq!(
            epoch_to_rfc3339_utc(1_700_000_000).as_deref(),
            Some("2023-11-14T22:13:20Z")
        );
    }

    #[test]
    fn the_epoch_itself_is_representable() {
        assert_eq!(
            epoch_to_rfc3339_utc(0).as_deref(),
            Some("1970-01-01T00:00:00Z")
        );
    }

    /// ★ The whole reason this returns `Option`. chrono's conversion is
    /// fallible, and defaulting an out-of-range instant would advertise an
    /// expiry the token does not carry — a lie the client cannot detect.
    #[test]
    fn an_unrepresentable_instant_is_none_not_a_default() {
        assert_eq!(epoch_to_rfc3339_utc(i64::MAX), None);
        assert_eq!(epoch_to_rfc3339_utc(i64::MIN), None);
    }

    /// ★ NEGATIVE CONTROL: without this the function could return a constant
    /// and every assertion above that uses a fixed epoch would still pass.
    #[test]
    fn distinct_instants_render_distinctly() {
        assert_ne!(
            epoch_to_rfc3339_utc(1_700_000_000),
            epoch_to_rfc3339_utc(1_700_000_001)
        );
    }

    /// The two surfaces must agree on SHAPE — a mint reads `now_rfc3339_utc`
    /// for creationTimestamp and `epoch_to_rfc3339_utc` for the expiry, and a
    /// client parses both with one parser.
    #[test]
    fn both_render_surfaces_agree_on_shape() {
        let now = now_rfc3339_utc();
        let epoch = epoch_to_rfc3339_utc(1_700_000_000).expect("representable");
        assert_eq!(now.len(), epoch.len(), "{now} vs {epoch}");
        assert!(now.ends_with('Z') && epoch.ends_with('Z'));
    }
}
