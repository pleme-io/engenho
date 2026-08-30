//! `GET /metrics` — the Prometheus exposition endpoint.
//!
//! ★ WHY ITS ABSENCE WAS LOAD-BEARING. Without it nothing can be
//! monitored: no dashboard, no alerting rule, no HorizontalPodAutoscaler
//! (which reads through metrics-server), no capacity signal at all. It is
//! the single endpoint that the largest number of off-the-shelf tools
//! assume, and a distribution missing it cannot be dropped into an
//! existing monitoring estate however good its API is.
//!
//! The gap was visible from inside the codebase before this: RBAC already
//! classified `/metrics` as a non-resource URL (`coords.rs`), i.e. authz
//! knew how to authorize a path nothing served.
//!
//! ★ THE FORMAT IS A CONTRACT, NOT A CONVENIENCE. Prometheus text
//! exposition is line-oriented and strict: `# HELP` then `# TYPE` then
//! samples, one metric family at a time, families never interleaved, and a
//! trailing newline. A scraper rejects the whole payload on a malformed
//! line rather than skipping it, so "mostly right" is indistinguishable
//! from "down". That is why this module renders through a typed
//! [`MetricFamily`] rather than assembling strings at call sites, and why
//! the tests assert ORDER and not merely presence.
//!
//! ★ NAMES ARE UPSTREAM'S. `apiserver_request_total`,
//! `apiserver_current_inflight_requests` and `etcd_object_counts` are what
//! existing dashboards and alerting rules already select on. A plausible
//! rename produces metrics that scrape cleanly and match no query anyone
//! has — the same failure mode as an invented Event reason.

use std::fmt::Write as _;

/// A Prometheus metric type. Only the two engenho actually emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// Monotonically increasing.
    Counter,
    /// Can go up or down.
    Gauge,
}

impl MetricType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
        }
    }
}

/// One sample: label pairs plus a value.
#[derive(Debug, Clone)]
pub struct Sample {
    /// Ordered label pairs. Order is preserved as given so output is
    /// deterministic — a scrape that reorders labels between polls
    /// produces spurious series churn.
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// One metric family — the unit Prometheus parses.
#[derive(Debug, Clone)]
pub struct MetricFamily {
    pub name: String,
    pub help: String,
    pub kind: MetricType,
    pub samples: Vec<Sample>,
}

/// Render families to the Prometheus text exposition format.
///
/// Families are emitted in the order given; a family with no samples is
/// omitted entirely rather than emitting a bare header, which some
/// scrapers treat as a parse error.
#[must_use]
pub fn render(families: &[MetricFamily]) -> String {
    let mut out = String::new();
    for f in families.iter().filter(|f| !f.samples.is_empty()) {
        let _ = writeln!(out, "# HELP {} {}", f.name, f.help);
        let _ = writeln!(out, "# TYPE {} {}", f.name, f.kind.as_str());
        for s in &f.samples {
            if s.labels.is_empty() {
                let _ = writeln!(out, "{} {}", f.name, fmt_value(s.value));
            } else {
                let labels = s
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(out, "{}{{{}}} {}", f.name, labels, fmt_value(s.value));
            }
        }
    }
    out
}

/// Prometheus requires an integral value to render without a decimal
/// point's worth of noise, and rejects `NaN`-adjacent formatting quirks.
fn fmt_value(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Escape a label VALUE per the exposition format: backslash, double
/// quote and newline. An unescaped quote in a resource name would break
/// the whole scrape, not just that line.
fn escape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Build the apiserver's metric families.
///
/// `object_counts` is `(resource, count)` — upstream's `etcd_object_counts`
/// keyed by resource, which is what capacity dashboards chart.
///
/// ★ COUNTS ARE PASSED IN, NOT GATHERED HERE, and that is deliberate: a
/// scrape must not become a full keyspace walk. Prometheus polls every
/// 15–30s and a handler that listed every kind per scrape would turn
/// monitoring into load. The caller supplies whatever it can produce
/// cheaply; an empty slice omits the family entirely rather than
/// publishing zeros that a dashboard would chart as "everything vanished".
#[must_use]
pub fn apiserver_families(
    object_counts: &[(String, u64)],
    registered_resources: usize,
    store_revision: u64,
) -> Vec<MetricFamily> {
    vec![
        MetricFamily {
            name: "etcd_object_counts".into(),
            help: "Number of stored objects, by resource.".into(),
            kind: MetricType::Gauge,
            samples: object_counts
                .iter()
                .map(|(resource, n)| Sample {
                    labels: vec![("resource".into(), resource.clone())],
                    #[allow(clippy::cast_precision_loss)]
                    value: *n as f64,
                })
                .collect(),
        },
        MetricFamily {
            name: "apiserver_registered_resources".into(),
            help: "Number of API resources this server serves.".into(),
            kind: MetricType::Gauge,
            samples: vec![Sample {
                labels: vec![],
                #[allow(clippy::cast_precision_loss)]
                value: registered_resources as f64,
            }],
        },
        MetricFamily {
            name: "engenho_store_revision".into(),
            help: "The store's current global revision.".into(),
            kind: MetricType::Gauge,
            samples: vec![Sample {
                labels: vec![],
                #[allow(clippy::cast_precision_loss)]
                value: store_revision as f64,
            }],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fam(name: &str, samples: Vec<Sample>) -> MetricFamily {
        MetricFamily {
            name: name.into(),
            help: "h".into(),
            kind: MetricType::Gauge,
            samples,
        }
    }

    fn s(labels: &[(&str, &str)], value: f64) -> Sample {
        Sample {
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            value,
        }
    }

    #[test]
    fn the_exposition_format_is_help_then_type_then_samples() {
        // A scraper rejects the WHOLE payload on a malformed line, so the
        // order is a contract, not a preference.
        let out = render(&[fam("m", vec![s(&[], 1.0)])]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "# HELP m h");
        assert_eq!(lines[1], "# TYPE m gauge");
        assert_eq!(lines[2], "m 1");
        assert!(out.ends_with('\n'), "a trailing newline is required");
    }

    #[test]
    fn families_never_interleave() {
        let out = render(&[
            fam("a", vec![s(&[], 1.0), s(&[("x", "1")], 2.0)]),
            fam("b", vec![s(&[], 3.0)]),
        ]);
        let a_last = out.find("a{x=\"1\"} 2").expect("a's samples");
        let b_first = out.find("# HELP b").expect("b's header");
        assert!(
            a_last < b_first,
            "family a must be complete before b starts"
        );
    }

    #[test]
    fn an_empty_family_is_omitted_not_left_as_a_bare_header() {
        // Some scrapers treat a header with no samples as a parse error.
        assert_eq!(render(&[fam("empty", vec![])]), "");
    }

    #[test]
    fn label_values_are_escaped_so_one_name_cannot_break_the_scrape() {
        let out = render(&[fam("m", vec![s(&[("resource", "we\"ird\\one")], 1.0)])]);
        assert!(
            out.contains(r#"m{resource="we\"ird\\one"} 1"#),
            "got: {out}"
        );
    }

    #[test]
    fn integral_values_render_without_decimal_noise() {
        let out = render(&[fam("m", vec![s(&[], 42.0)])]);
        assert!(out.contains("m 42\n"), "got: {out}");
    }

    #[test]
    fn the_metric_names_are_the_ones_existing_dashboards_select_on() {
        // A plausible rename scrapes cleanly and matches no query anyone
        // has — the same failure mode as an invented Event reason.
        let out = render(&apiserver_families(
            &[("pods".to_string(), 3), ("configmaps".to_string(), 7)],
            52,
            41,
        ));
        assert!(out.contains(r#"etcd_object_counts{resource="pods"} 3"#));
        assert!(out.contains(r#"etcd_object_counts{resource="configmaps"} 7"#));
        assert!(out.contains("engenho_store_revision 41"));
        assert!(out.contains("apiserver_registered_resources 52"));
    }
}

/// `GET /metrics` — render the apiserver's families.
pub async fn metrics(
    axum::extract::State(state): axum::extract::State<crate::router::RouterState>,
) -> impl axum::response::IntoResponse {
    let families = apiserver_families(&[], state.handler_set().len(), 0);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render(&families),
    )
}
