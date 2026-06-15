//! Minimal typed 5-field cron parser — the engenho-native cron surface.
//!
//! No external cron crate exists in the workspace (checked Cargo.lock),
//! and the upstream `cron`/`saffron` crates are heavier than the surface
//! the `CronJobController` needs. Per the Prime Directive (don't add a
//! heavy dep for a small typed surface that we can author once), this is
//! a lightweight typed parser for standard Vixie-style 5-field cron:
//!
//! ```text
//!   ┌───────────── minute        (0-59)
//!   │ ┌─────────── hour          (0-23)
//!   │ │ ┌───────── day-of-month  (1-31)
//!   │ │ │ ┌─────── month         (1-12)
//!   │ │ │ │ ┌───── day-of-week   (0-6, Sunday=0)
//!   │ │ │ │ │
//!   * * * * *
//! ```
//!
//! Each field supports `*`, a single number `N`, a comma list
//! `a,b,c`, a range `a-b`, a step `*/k`, and a ranged step `a-b/k`.
//! Day-of-week `7` is normalised to Sunday (`0`), matching cron(5).
//!
//! ## Day-of-month ∧ day-of-week semantics (cron(5) "OR" rule)
//!
//! When BOTH day-of-month and day-of-week are restricted (neither is
//! `*`), a timestamp matches if it satisfies EITHER field — the classic
//! cron-OR rule. When one is `*`, both must match (the `*` is vacuously
//! satisfied). This mirrors Vixie cron + the Kubernetes `CronJob`
//! controller's behaviour.
//!
//! ## Time decomposition
//!
//! A unix-seconds instant is decomposed to UTC calendar fields via
//! chrono (the typed calendar surface engenho-types already depends on).
//! Timezone-aware schedules (`spec.timeZone`) are a named follow-up —
//! this parser evaluates against UTC.

use chrono::{DateTime, Datelike, Timelike, Utc};

/// A parse failure for one cron field or the whole expression. Typed so
/// the controller surfaces a bad `spec.schedule` as a skip-with-reason,
/// never a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronParseError {
    /// The expression did not have exactly 5 whitespace-separated fields.
    FieldCount {
        /// The number of fields actually found.
        found: usize,
    },
    /// A field's token could not be parsed (bad number, empty range, …).
    Field {
        /// Which field (0=minute … 4=day-of-week).
        index: usize,
        /// The offending token.
        token: String,
    },
}

impl std::fmt::Display for CronParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldCount { found } => {
                write!(f, "cron expression must have 5 fields, found {found}")
            }
            Self::Field { index, token } => {
                write!(f, "invalid cron field {index}: {token:?}")
            }
        }
    }
}

impl std::error::Error for CronParseError {}

/// A single parsed cron field — the set of integer values it admits.
///
/// Stored as an explicit sorted value set (each field's domain is tiny —
/// at most 60 values), plus a `wildcard` flag so the day-of-month ∧
/// day-of-week OR-rule can ask "was this field restricted?".
#[derive(Debug, Clone, PartialEq, Eq)]
struct CronField {
    values: Vec<u32>,
    wildcard: bool,
}

impl CronField {
    fn matches(&self, v: u32) -> bool {
        self.values.binary_search(&v).is_ok()
    }
}

/// A parsed 5-field cron schedule. Construct via [`CronSchedule::parse`];
/// match an instant via [`CronSchedule::matches_unix`]; find the next due
/// time via [`CronSchedule::next_after_unix`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    minute: CronField,
    hour: CronField,
    dom: CronField,
    month: CronField,
    dow: CronField,
}

impl CronSchedule {
    /// Parse a standard 5-field cron expression.
    ///
    /// # Errors
    ///
    /// [`CronParseError::FieldCount`] if not exactly 5 fields;
    /// [`CronParseError::Field`] if any field's token is malformed or
    /// out of range.
    pub fn parse(expr: &str) -> Result<Self, CronParseError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronParseError::FieldCount {
                found: fields.len(),
            });
        }
        Ok(Self {
            minute: parse_field(fields[0], 0, 0, 59)?,
            hour: parse_field(fields[1], 1, 0, 23)?,
            dom: parse_field(fields[2], 2, 1, 31)?,
            month: parse_field(fields[3], 3, 1, 12)?,
            dow: parse_field(fields[4], 4, 0, 6)?,
        })
    }

    /// Does the given UTC instant (unix seconds) match this schedule, to
    /// minute precision (seconds are ignored, as in cron)?
    #[must_use]
    pub fn matches_unix(&self, unix_secs: u64) -> bool {
        let Some(dt) = unix_to_utc(unix_secs) else {
            return false;
        };
        self.matches_dt(&dt)
    }

    fn matches_dt(&self, dt: &DateTime<Utc>) -> bool {
        if !self.minute.matches(dt.minute()) || !self.hour.matches(dt.hour()) {
            return false;
        }
        if !self.month.matches(dt.month()) {
            return false;
        }
        let day_of_month_ok = self.dom.matches(dt.day());
        // chrono Weekday: Mon=0..Sun=6 via num_days_from_monday; cron wants
        // Sun=0..Sat=6. num_days_from_sunday gives exactly that.
        let weekday_ok = self.dow.matches(dt.weekday().num_days_from_sunday());
        // cron(5) OR-rule: when BOTH dom and dow are restricted, match on
        // either; otherwise both must hold (a `*` field is vacuously true).
        if self.dom.wildcard || self.dow.wildcard {
            day_of_month_ok && weekday_ok
        } else {
            day_of_month_ok || weekday_ok
        }
    }

    /// The first scheduled minute STRICTLY AFTER `after_unix`, or `None`
    /// if no match is found within a bounded search horizon (4 years —
    /// covers Feb-29-only schedules). The returned value is aligned to the
    /// top of the minute (seconds = 0).
    #[must_use]
    pub fn next_after_unix(&self, after_unix: u64) -> Option<u64> {
        // 4-year horizon in minutes (catches Feb-29 leap-day schedules).
        const HORIZON_MINUTES: u64 = 4 * 366 * 24 * 60;
        // Start at the next whole minute strictly after `after_unix`.
        let mut t = (after_unix / 60 + 1) * 60;
        for _ in 0..HORIZON_MINUTES {
            if self.matches_unix(t) {
                return Some(t);
            }
            t += 60;
        }
        None
    }
}

/// Decompose a unix-seconds instant to a UTC `DateTime`. `None` only for
/// timestamps chrono cannot represent (far outside the supported range).
fn unix_to_utc(unix_secs: u64) -> Option<DateTime<Utc>> {
    let secs = i64::try_from(unix_secs).ok()?;
    DateTime::<Utc>::from_timestamp(secs, 0)
}

/// Parse one cron field into its admissible value set.
fn parse_field(
    field: &str,
    index: usize,
    min: u32,
    max: u32,
) -> Result<CronField, CronParseError> {
    let err = || CronParseError::Field {
        index,
        token: field.to_string(),
    };
    let wildcard = field == "*" || field.starts_with("*/");
    let mut values: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for part in field.split(',') {
        parse_part(part, index, min, max, &mut values)?;
    }
    if values.is_empty() {
        return Err(err());
    }
    Ok(CronField {
        values: values.into_iter().collect(),
        wildcard,
    })
}

/// Parse one comma-separated component of a cron field: `*`, `N`, `a-b`,
/// `*/k`, or `a-b/k`. Day-of-week `7` is folded to `0` (Sunday).
fn parse_part(
    part: &str,
    index: usize,
    min: u32,
    max: u32,
    out: &mut std::collections::BTreeSet<u32>,
) -> Result<(), CronParseError> {
    let err = || CronParseError::Field {
        index,
        token: part.to_string(),
    };

    // Split off an optional `/step` suffix.
    let (range_spec, step) = match part.split_once('/') {
        Some((r, s)) => {
            let step: u32 = s.parse().map_err(|_| err())?;
            if step == 0 {
                return Err(err());
            }
            (r, step)
        }
        None => (part, 1),
    };

    // Resolve the range the step walks over.
    let (lo, hi) = if range_spec == "*" {
        (min, max)
    } else if let Some((a, b)) = range_spec.split_once('-') {
        let lo = fold_dow(a.parse().map_err(|_| err())?, index);
        let hi = fold_dow(b.parse().map_err(|_| err())?, index);
        if lo > hi {
            return Err(err());
        }
        (lo, hi)
    } else {
        // A bare number. With a step (`N/k`) it means "from N, stepping".
        let n = fold_dow(range_spec.parse().map_err(|_| err())?, index);
        if step == 1 {
            (n, n)
        } else {
            (n, max)
        }
    };

    if lo < min || hi > max {
        return Err(err());
    }

    let mut v = lo;
    while v <= hi {
        out.insert(v);
        v += step;
    }
    Ok(())
}

/// Fold day-of-week `7` → `0` (both are Sunday in cron(5)). Only applies
/// to the day-of-week field (index 4); a no-op for every other field.
fn fold_dow(v: u32, index: usize) -> u32 {
    if index == 4 && v == 7 { 0 } else { v }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rejects_wrong_field_count() {
        assert_eq!(
            CronSchedule::parse("* * * *"),
            Err(CronParseError::FieldCount { found: 4 })
        );
        assert_eq!(
            CronSchedule::parse("* * * * * *"),
            Err(CronParseError::FieldCount { found: 6 })
        );
    }

    #[test]
    fn every_minute_matches_any_instant() {
        let s = CronSchedule::parse("* * * * *").unwrap();
        // 2026-06-15T12:34:00Z = 1781613240. Any minute matches.
        assert!(s.matches_unix(1_781_613_240));
        assert!(s.matches_unix(0));
    }

    #[test]
    fn next_after_every_minute_is_next_whole_minute() {
        let s = CronSchedule::parse("* * * * *").unwrap();
        // After 100s (01:40) the next due minute is 120s (02:00).
        assert_eq!(s.next_after_unix(100), Some(120));
        // Exactly on a minute boundary → strictly-after gives the next one.
        assert_eq!(s.next_after_unix(120), Some(180));
    }

    #[test]
    fn specific_minute_hour() {
        // "30 2 * * *" = 02:30 UTC daily.
        let s = CronSchedule::parse("30 2 * * *").unwrap();
        // 2026-01-01T00:00:00Z = 1767225600.
        let next = s.next_after_unix(1_767_225_600).unwrap();
        let dt = unix_to_utc(next).unwrap();
        assert_eq!(dt.hour(), 2);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn step_field() {
        // Every 15 minutes: matches :00 :15 :30 :45.
        let s = CronSchedule::parse("*/15 * * * *").unwrap();
        // 1767225600 = 2026-01-01T00:00:00Z (minute 0 → matches).
        assert!(s.matches_unix(1_767_225_600));
        // +15 min.
        assert!(s.matches_unix(1_767_225_600 + 15 * 60));
        // +7 min → minute 7, no match.
        assert!(!s.matches_unix(1_767_225_600 + 7 * 60));
    }

    #[test]
    fn range_and_list() {
        let s = CronSchedule::parse("0 9-17 * * 1-5").unwrap();
        // Hour 9-17, weekdays Mon-Fri, minute 0.
        // 2026-01-05 is a Monday (1767571200 = 2026-01-05T00:00:00Z).
        // 09:00 on that Monday:
        let mon_9 = 1_767_603_600; // 2026-01-05T09:00:00Z
        assert!(s.matches_unix(mon_9));
        // 18:00 same day → hour 18 out of 9-17.
        assert!(!s.matches_unix(mon_9 + 9 * 3600));
    }

    #[test]
    fn dow_seven_is_sunday() {
        let s7 = CronSchedule::parse("0 0 * * 7").unwrap();
        let s0 = CronSchedule::parse("0 0 * * 0").unwrap();
        // 2026-01-04 is a Sunday (1767484800 = 2026-01-04T00:00:00Z).
        let sun = 1_767_484_800;
        assert!(s7.matches_unix(sun));
        assert!(s0.matches_unix(sun));
    }

    #[test]
    fn dom_dow_or_rule() {
        // "0 0 1 * 1" = midnight on the 1st OR on a Monday.
        let s = CronSchedule::parse("0 0 1 * 1").unwrap();
        // 2026-02-01 is a Sunday (the 1st, not a Monday).
        // 1769904000 = 2026-02-01T00:00:00Z. Matches via day-of-month.
        assert!(s.matches_unix(1_769_904_000));
        // 2026-02-02 is a Monday (not the 1st). Matches via day-of-week.
        assert!(s.matches_unix(1_769_904_000 + 86_400));
        // 2026-02-03 is a Tuesday (not the 1st). No match.
        assert!(!s.matches_unix(1_769_904_000 + 2 * 86_400));
    }

    #[test]
    fn invalid_tokens_rejected() {
        assert!(matches!(
            CronSchedule::parse("60 * * * *"),
            Err(CronParseError::Field { index: 0, .. })
        ));
        assert!(matches!(
            CronSchedule::parse("* 24 * * *"),
            Err(CronParseError::Field { index: 1, .. })
        ));
        assert!(matches!(
            CronSchedule::parse("* * * * abc"),
            Err(CronParseError::Field { index: 4, .. })
        ));
        // Zero step is invalid.
        assert!(matches!(
            CronSchedule::parse("*/0 * * * *"),
            Err(CronParseError::Field { index: 0, .. })
        ));
    }
}
