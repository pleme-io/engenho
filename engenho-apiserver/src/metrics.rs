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
//! `controller_runtime_reconcile_total{controller,result}` is
//! controller-runtime's name and label set for the same reason.
//!
//! ★ THE RUNTIME'S FAMILIES COME FROM A SOURCE, NOT A LITERAL. The store
//! revision used to be the literal `0`: a scrape that parsed cleanly and
//! said nothing true. `engenho_store_revision`, `engenho_panics_total`,
//! `controller_runtime_reconcile_total` and
//! `engenho_controller_last_tick_timestamp_seconds` are rendered from one
//! [`MetricsSnapshot`] read through the [`MetricsSource`] the runtime
//! installs. With no source installed they are ABSENT, which a dashboard
//! shows as "no data" rather than charting a zero as fact.

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
    ]
}

/// What the runtime's metric families are rendered from, read once per
/// scrape through a [`MetricsSource`].
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    /// The store's current global revision (`engenho_store_revision`).
    pub store_revision: engenho_store::Revision,
    /// Panics counted since the process started (`engenho_panics_total`).
    /// `None` until a panic hook counts them: absent, not zero, because
    /// zero would claim a count nobody took.
    pub panics_total: Option<u64>,
    /// `controller_runtime_reconcile_total{controller,result}`.
    pub reconciles: Vec<ReconcileCount>,
    /// `engenho_controller_last_tick_timestamp_seconds{controller}`.
    pub last_ticks: Vec<LastTick>,
}

/// How one reconcile ended, in controller-runtime's `result` vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReconcileResult {
    /// Done; nothing re-queued.
    Success,
    /// Returned an error.
    Error,
    /// Asked to be re-queued with rate limiting.
    Requeue,
    /// Asked to be re-queued after a fixed delay.
    RequeueAfter,
}

impl ReconcileResult {
    /// The `result` label, exactly as controller-runtime spells it.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Requeue => "requeue",
            Self::RequeueAfter => "requeue_after",
        }
    }
}

/// One `(controller, result)` reconcile counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileCount {
    /// The controller's name: a `&'static str`, so a label value can only
    /// come from code, never from a request or an object.
    pub controller: &'static str,
    /// How the counted reconciles ended.
    pub result: ReconcileResult,
    /// How many.
    pub count: u64,
}

/// When one controller last finished a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastTick {
    /// The controller's name.
    pub controller: &'static str,
    /// When its last tick finished.
    pub at: engenho_substrate::Instant,
}

/// Where `/metrics` reads the runtime's families from. Implemented by the
/// runtime; [`FixedMetrics`] is the test double. Installed with
/// [`crate::router::RouterState::with_metrics_source`].
#[async_trait::async_trait]
pub trait MetricsSource: Send + Sync {
    /// The current values, read once per scrape.
    async fn snapshot(&self) -> MetricsSnapshot;
}

/// A [`MetricsSource`] that answers one fixed snapshot: the test double for
/// the runtime's source.
#[derive(Debug, Clone)]
pub struct FixedMetrics(pub MetricsSnapshot);

#[async_trait::async_trait]
impl MetricsSource for FixedMetrics {
    async fn snapshot(&self) -> MetricsSnapshot {
        self.0.clone()
    }
}

/// The name of the last-tick gauge. engenho-native: controller-runtime has
/// no equivalent, and `_timestamp_seconds` is Prometheus's suffix for a
/// Unix time.
pub const LAST_TICK_TIMESTAMP_SECONDS: &str = "engenho_controller_last_tick_timestamp_seconds";

/// The runtime's families, rendered from one snapshot.
///
/// Reconcile and last-tick samples are sorted by their labels, so the
/// output does not depend on the order the source happened to report in.
#[must_use]
pub fn source_families(snapshot: &MetricsSnapshot) -> Vec<MetricFamily> {
    let mut reconciles = snapshot.reconciles.clone();
    reconciles.sort_by_key(|r| (r.controller, r.result));
    let mut last_ticks = snapshot.last_ticks.clone();
    last_ticks.sort_by_key(|t| t.controller);
    vec![
        MetricFamily {
            name: "engenho_store_revision".into(),
            help: "The store's current global revision.".into(),
            kind: MetricType::Gauge,
            samples: vec![Sample {
                labels: vec![],
                value: as_sample(snapshot.store_revision.get()),
            }],
        },
        MetricFamily {
            name: "engenho_panics_total".into(),
            help: "Panics counted since the process started.".into(),
            kind: MetricType::Counter,
            samples: snapshot
                .panics_total
                .map(|n| Sample {
                    labels: vec![],
                    value: as_sample(n),
                })
                .into_iter()
                .collect(),
        },
        MetricFamily {
            name: "controller_runtime_reconcile_total".into(),
            help: "Total number of reconciliations per controller, by result.".into(),
            kind: MetricType::Counter,
            samples: reconciles
                .iter()
                .map(|r| Sample {
                    labels: vec![
                        ("controller".into(), r.controller.into()),
                        ("result".into(), r.result.label().into()),
                    ],
                    value: as_sample(r.count),
                })
                .collect(),
        },
        MetricFamily {
            name: LAST_TICK_TIMESTAMP_SECONDS.into(),
            help: "Unix time each controller last finished a reconcile tick.".into(),
            kind: MetricType::Gauge,
            samples: last_ticks
                .iter()
                .map(|t| Sample {
                    labels: vec![("controller".into(), t.controller.into())],
                    value: as_sample(t.at.physical_ms) / 1000.0,
                })
                .collect(),
        },
    ]
}

/// A count as a Prometheus sample, which is an `f64`.
#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus samples are f64; a count past 2^53 loses low bits, which every scraper accepts"
)]
fn as_sample(n: u64) -> f64 {
    n as f64
}

/// The one metric family every rollout gate reports through.
///
/// engenho-native: upstream has no shadow gates. Shares its vocabulary with
/// the Event reason [`engenho_substrate::WouldReject::EVENT_REASON`].
pub const WOULD_REJECT_TOTAL: &str = "engenho_would_reject_total";

/// `engenho_would_reject_total{gate,reason}`: what each gate in
/// [`engenho_substrate::Rollout::Shadow`] allowed that `Enforce` would have
/// refused.
///
/// Label order is fixed (`gate`, then `reason`) and the samples keep the
/// ledger's order, so the series set is stable from scrape to scrape.
#[must_use]
pub fn would_reject_family(counts: &[engenho_substrate::WouldRejectCount]) -> MetricFamily {
    MetricFamily {
        name: WOULD_REJECT_TOTAL.into(),
        help: "Refusals a gate in shadow rollout allowed that enforce would have made, by gate and reason.".into(),
        kind: MetricType::Counter,
        samples: counts
            .iter()
            .map(|c| Sample {
                labels: vec![
                    ("gate".into(), c.gate.into()),
                    ("reason".into(), c.reason.into()),
                ],
                #[allow(clippy::cast_precision_loss)]
                value: c.count as f64,
            })
            .collect(),
    }
}

/// The production log hook for a [`engenho_substrate::WouldRejectLedger`]:
/// one structured WARN per refusal a Shadow gate allowed.
pub fn log_would_reject(event: &engenho_substrate::WouldReject<'_>) {
    tracing::warn!(
        gate = event.gate,
        reason = event.reason,
        subject = %event.subject,
        event_reason = engenho_substrate::WouldReject::EVENT_REASON,
        "shadow gate allowed what enforce would refuse"
    );
}

/// Everything `GET /metrics` serves, rendered from the router's state and,
/// when one is installed, its [`MetricsSource`].
pub async fn render_state(state: &crate::router::RouterState) -> String {
    let mut families = apiserver_families(&[], state.handler_set().len());
    if let Some(source) = &state.metrics_source {
        families.extend(source_families(&source.snapshot().await));
    }
    families.push(would_reject_family(&state.would_reject.snapshot()));
    render(&families)
}

/// `GET /metrics` — render the apiserver's families.
pub async fn metrics(
    axum::extract::State(state): axum::extract::State<crate::router::RouterState>,
) -> impl axum::response::IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_state(&state).await,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::{Gate, RejectReason, Rollout, WouldRejectLedger};
    use std::sync::Arc;

    #[derive(Debug)]
    struct Stale;

    impl RejectReason for Stale {
        fn label(&self) -> &'static str {
            "stale"
        }
    }

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
        let mut families = apiserver_families(
            &[("pods".to_string(), 3), ("configmaps".to_string(), 7)],
            52,
        );
        families.extend(source_families(&snapshot(41)));
        let out = render(&families);
        assert!(out.contains(r#"etcd_object_counts{resource="pods"} 3"#));
        assert!(out.contains(r#"etcd_object_counts{resource="configmaps"} 7"#));
        assert!(out.contains("engenho_store_revision 41"));
        assert!(out.contains("apiserver_registered_resources 52"));
        assert!(out.contains("# TYPE controller_runtime_reconcile_total counter\n"));
        assert!(out.contains("# TYPE engenho_panics_total counter\n"));
    }

    #[test]
    fn would_reject_is_one_counter_labelled_by_gate_then_reason() {
        let out = render(&[would_reject_family(&[
            engenho_substrate::WouldRejectCount {
                gate: "boot_tripwire",
                reason: "stale",
                count: 2,
            },
            engenho_substrate::WouldRejectCount {
                gate: "rbac",
                reason: "no_rule",
                count: 5,
            },
        ])]);
        assert!(
            out.contains("# TYPE engenho_would_reject_total counter\n"),
            "got: {out}"
        );
        assert!(
            out.contains("engenho_would_reject_total{gate=\"boot_tripwire\",reason=\"stale\"} 2\n"),
            "got: {out}"
        );
        assert!(
            out.contains("engenho_would_reject_total{gate=\"rbac\",reason=\"no_rule\"} 5\n"),
            "got: {out}"
        );
    }

    #[tokio::test]
    async fn a_shadow_gate_refusal_reaches_the_scrape_and_an_enforced_one_does_not() {
        let state = crate::router::RouterState::new(vec![]);
        let shadow = Gate::new("shadow_gate", Rollout::Shadow);
        let enforce = Gate::new("enforce_gate", Rollout::Enforce);
        let _ = shadow.judge(Err(Stale), &state.would_reject, &"ns/a");
        let _ = shadow.judge(Err(Stale), &state.would_reject, &"ns/b");
        let _ = enforce.judge(Err(Stale), &state.would_reject, &"ns/c");
        let out = render_state(&state).await;
        assert!(
            out.contains("engenho_would_reject_total{gate=\"shadow_gate\",reason=\"stale\"} 2\n"),
            "got: {out}"
        );
        assert!(!out.contains("enforce_gate"), "got: {out}");
    }

    #[tokio::test]
    async fn the_scrape_reads_the_ledger_the_runtime_shares() {
        // Gates outside the apiserver (store, scheduler) count into the
        // runtime's ledger; the scrape must read THAT one, not a private one.
        let shared = Arc::new(WouldRejectLedger::new(log_would_reject));
        let state =
            crate::router::RouterState::new(vec![]).with_would_reject_ledger(shared.clone());
        let elsewhere = Gate::new("scheduler_filter", Rollout::Shadow);
        let _ = elsewhere.judge(Err(Stale), &shared, &"ns/pod");
        assert!(render_state(&state).await.contains(
            "engenho_would_reject_total{gate=\"scheduler_filter\",reason=\"stale\"} 1\n"
        ));
    }

    fn snapshot(store_revision: u64) -> MetricsSnapshot {
        MetricsSnapshot {
            store_revision: engenho_store::Revision(store_revision),
            panics_total: Some(3),
            reconciles: vec![
                ReconcileCount {
                    controller: "replicaset",
                    result: ReconcileResult::Success,
                    count: 9,
                },
                ReconcileCount {
                    controller: "deployment",
                    result: ReconcileResult::RequeueAfter,
                    count: 2,
                },
                ReconcileCount {
                    controller: "deployment",
                    result: ReconcileResult::Error,
                    count: 1,
                },
            ],
            last_ticks: vec![
                LastTick {
                    controller: "replicaset",
                    at: engenho_substrate::Instant::from_ms(1_758_000_000_250),
                },
                LastTick {
                    controller: "deployment",
                    at: engenho_substrate::Instant::from_ms(1_758_000_000_000),
                },
            ],
        }
    }

    fn sourced(snapshot: MetricsSnapshot) -> crate::router::RouterState {
        crate::router::RouterState::new(vec![])
            .with_metrics_source(Arc::new(FixedMetrics(snapshot)))
    }

    #[tokio::test]
    async fn the_scrape_renders_the_store_revision_the_source_reports() {
        // Red before T2.8: `render_state` passed the literal 0.
        let out = render_state(&sourced(snapshot(41))).await;
        assert!(out.contains("engenho_store_revision 41\n"), "got: {out}");
        assert!(!out.contains("engenho_store_revision 0\n"), "got: {out}");
    }

    #[tokio::test]
    async fn panics_reconciles_and_last_ticks_render_from_the_source() {
        let out = render_state(&sourced(snapshot(41))).await;
        assert!(out.contains("engenho_panics_total 3\n"), "got: {out}");
        // controller-runtime's name, label order and result vocabulary,
        // sorted by label so the source's order does not leak.
        let reconcile_lines: Vec<&str> = out
            .lines()
            .filter(|l| l.starts_with("controller_runtime_reconcile_total{"))
            .collect();
        assert_eq!(
            reconcile_lines,
            [
                "controller_runtime_reconcile_total{controller=\"deployment\",result=\"error\"} 1",
                "controller_runtime_reconcile_total{controller=\"deployment\",result=\"requeue_after\"} 2",
                "controller_runtime_reconcile_total{controller=\"replicaset\",result=\"success\"} 9",
            ]
        );
        assert!(
            out.contains(
                "engenho_controller_last_tick_timestamp_seconds{controller=\"deployment\"} 1758000000\n"
            ),
            "got: {out}"
        );
        assert!(
            out.contains(
                "engenho_controller_last_tick_timestamp_seconds{controller=\"replicaset\"} 1758000000.25\n"
            ),
            "got: {out}"
        );
    }

    #[tokio::test]
    async fn an_unwired_router_publishes_no_runtime_families_rather_than_zeros() {
        let out = render_state(&crate::router::RouterState::new(vec![])).await;
        for absent in [
            "engenho_store_revision",
            "engenho_panics_total",
            "controller_runtime_reconcile_total",
            LAST_TICK_TIMESTAMP_SECONDS,
        ] {
            assert!(!out.contains(absent), "{absent} must be absent; got: {out}");
        }
        assert!(
            out.contains("apiserver_registered_resources 0\n"),
            "got: {out}"
        );
    }

    #[tokio::test]
    async fn an_uncounted_panic_total_is_absent_not_zero() {
        let mut snap = snapshot(7);
        snap.panics_total = None;
        let out = render_state(&sourced(snap)).await;
        assert!(!out.contains("engenho_panics_total"), "got: {out}");
        assert!(out.contains("engenho_store_revision 7\n"), "got: {out}");
    }

    #[test]
    fn result_labels_are_controller_runtimes() {
        let labels: Vec<&str> = [
            ReconcileResult::Success,
            ReconcileResult::Error,
            ReconcileResult::Requeue,
            ReconcileResult::RequeueAfter,
        ]
        .iter()
        .map(|r| r.label())
        .collect();
        assert_eq!(labels, ["success", "error", "requeue", "requeue_after"]);
    }
}
