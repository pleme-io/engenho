//! T5.10 — the `/metrics` header is held to the scrape.
//!
//! The module header of `src/metrics.rs` lists every metric family the
//! endpoint renders. It used to name `apiserver_request_total` and
//! `apiserver_current_inflight_requests`, which nothing rendered, and
//! `etcd_object_counts`, whose samples the scrape took from a literal empty
//! slice. These tests read the header as text and the scrape as Prometheus
//! exposition, and fail when the two disagree:
//!
//!   * the header names a family in backticks that the scrape lacks;
//!   * the header gives a label set in braces that the family's samples do
//!     not carry, in that order;
//!   * the scrape renders a family the header does not name.
//!
//! The third direction is also the positive control: an extractor that
//! found nothing would pass the first two vacuously and fail the third.
//!
//! Tier: a CI gate over text, not a type. A family written without
//! backticks escapes it. The scrape is the fully sourced one, with every
//! source family populated; whether the runtime's source populates them in
//! production is the runtime's binding, not this test's.

use std::collections::BTreeSet;
use std::sync::Arc;

use engenho_apiserver::metrics::{FixedMetrics, render_state};
use engenho_apiserver::router::RouterState;
use engenho_apiserver::{LastTick, MetricsSnapshot, ObjectCount, ReconcileCount, ReconcileResult};
use engenho_substrate::rollout::{Gate, RejectReason, Rollout};

/// The source the header lives in, read at compile time so an edit to the
/// header rebuilds this test.
const METRICS_RS: &str = include_str!("../src/metrics.rs");

/// One family the header names, with its label set when the header gives
/// one in braces.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Claim {
    family: String,
    labels: Option<Vec<String>>,
}

/// The module header: the leading run of `//!` lines.
fn header(source: &str) -> String {
    source
        .lines()
        .map_while(|l| l.strip_prefix("//!"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every family the header names in backticks. An intra-doc link
/// (`[` immediately before the backtick) is a Rust item, not a family.
fn claims(header: &str) -> Vec<Claim> {
    let mut out = Vec::new();
    let mut rest = header;
    while let Some(open) = rest.find('`') {
        let is_link = rest[..open].ends_with('[');
        let after = &rest[open + 1..];
        let Some(close) = after.find('`') else {
            break;
        };
        let span = &after[..close];
        rest = &after[close + 1..];
        if !is_link {
            out.extend(claim(span));
        }
    }
    out
}

/// A Prometheus family name as this crate writes them: lowercase
/// `snake_case` with at least one `_`. A word ending in `_` is a namespace
/// prefix (`engenho_`), not a family.
fn is_family_word(w: &str) -> bool {
    w.starts_with(|c: char| c.is_ascii_lowercase())
        && w.contains('_')
        && !w.ends_with('_')
        && w.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_label_word(w: &str) -> bool {
    w.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && w.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// `family` or `family{label,label}`; anything else is not a claim.
fn claim(span: &str) -> Option<Claim> {
    let (family, labels) = match span.split_once('{') {
        None => (span, None),
        Some((family, rest)) => {
            let labels: Vec<String> = rest
                .strip_suffix('}')?
                .split(',')
                .map(|l| l.trim().to_string())
                .collect();
            if !labels.iter().all(|l| is_label_word(l)) {
                return None;
            }
            (family, Some(labels))
        }
    };
    is_family_word(family).then(|| Claim {
        family: family.to_string(),
        labels,
    })
}

/// The families the scrape declares, from its `# TYPE` lines.
fn rendered_families(scrape: &str) -> BTreeSet<&str> {
    scrape
        .lines()
        .filter_map(|l| l.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_whitespace().next())
        .collect()
}

/// The label names of each sample of `family`, in order.
fn sample_label_names(scrape: &str, family: &str) -> Vec<Vec<String>> {
    scrape
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let rest = l.strip_prefix(family)?;
            if let Some(labels) = rest.strip_prefix('{') {
                Some(label_names(labels))
            } else if rest.starts_with(' ') {
                Some(Vec::new())
            } else {
                // A longer family that shares this one's prefix.
                None
            }
        })
        .collect()
}

/// Label names from `k="v",k2="v2"} value`, skipping each quoted value
/// with its `\` escapes, so a `,` or `"` inside a value cannot split it.
fn label_names(mut rest: &str) -> Vec<String> {
    let mut names = Vec::new();
    while let Some((name, after)) = rest.split_once("=\"") {
        names.push(name.to_string());
        let mut chars = after.char_indices();
        let mut end = None;
        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => {
                    chars.next();
                }
                '"' => {
                    end = Some(i);
                    break;
                }
                _ => {}
            }
        }
        let Some(end) = end else {
            break;
        };
        match after[end + 1..].strip_prefix(',') {
            Some(next) => rest = next,
            None => break,
        }
    }
    names
}

/// Families the header names that the scrape does not render.
fn unrendered(claims: &[Claim], scrape: &str) -> BTreeSet<String> {
    let rendered = rendered_families(scrape);
    claims
        .iter()
        .filter(|c| !rendered.contains(c.family.as_str()))
        .map(|c| c.family.clone())
        .collect()
}

/// `(family, claimed labels, rendered label sets)` wherever a sample of a
/// family the header gives labels for carries a different set.
fn label_mismatches(
    claims: &[Claim],
    scrape: &str,
) -> Vec<(String, Vec<String>, Vec<Vec<String>>)> {
    claims
        .iter()
        .filter_map(|c| {
            let claimed = c.labels.as_ref()?;
            let samples = sample_label_names(scrape, &c.family);
            let agrees = !samples.is_empty() && samples.iter().all(|s| s == claimed);
            (!agrees).then(|| (c.family.clone(), claimed.clone(), samples))
        })
        .collect()
}

/// Families the scrape renders that the header does not name.
fn unnamed(claims: &[Claim], scrape: &str) -> BTreeSet<String> {
    let named: BTreeSet<&str> = claims.iter().map(|c| c.family.as_str()).collect();
    rendered_families(scrape)
        .into_iter()
        .filter(|f| !named.contains(f))
        .map(str::to_string)
        .collect()
}

#[derive(Debug)]
struct DocTruth;

impl RejectReason for DocTruth {
    fn label(&self) -> &'static str {
        "doc_truth"
    }
}

/// The scrape with every family's input populated: a source reporting
/// every field, and one refusal a Shadow gate allowed.
async fn full_scrape() -> String {
    let snapshot = MetricsSnapshot {
        object_counts: vec![ObjectCount {
            resource: "pods".into(),
            count: 2,
        }],
        store_revision: engenho_store::Revision(11),
        panics_total: Some(1),
        reconciles: vec![ReconcileCount {
            controller: "deployment",
            result: ReconcileResult::Success,
            count: 4,
        }],
        last_ticks: vec![LastTick {
            controller: "deployment",
            at: engenho_substrate::Instant::from_ms(1_758_000_000_000),
        }],
    };
    let state = RouterState::new(vec![]).with_metrics_source(Arc::new(FixedMetrics(snapshot)));
    let shadow = Gate::new("doc_truth", Rollout::Shadow);
    assert!(
        shadow
            .judge(Err(DocTruth), &state.would_reject, &"ns/doc-truth")
            .is_ok(),
        "a shadow gate proceeds"
    );
    render_state(&state).await
}

#[tokio::test]
async fn every_family_the_header_names_is_rendered() {
    let scrape = full_scrape().await;
    let missing = unrendered(&claims(&header(METRICS_RS)), &scrape);
    assert!(
        missing.is_empty(),
        "metrics.rs's header names families the scrape lacks: {missing:?}\nscrape:\n{scrape}"
    );
}

#[tokio::test]
async fn every_label_set_the_header_gives_is_the_one_its_samples_carry() {
    let scrape = full_scrape().await;
    let mismatches = label_mismatches(&claims(&header(METRICS_RS)), &scrape);
    assert!(
        mismatches.is_empty(),
        "(family, header's labels, rendered labels): {mismatches:?}\nscrape:\n{scrape}"
    );
}

#[tokio::test]
async fn every_rendered_family_is_named_in_the_header() {
    let scrape = full_scrape().await;
    let unlisted = unnamed(&claims(&header(METRICS_RS)), &scrape);
    assert!(
        unlisted.is_empty(),
        "the scrape renders families metrics.rs's header does not name: {unlisted:?}"
    );
}

#[test]
fn the_check_fails_on_a_phantom_family_a_wrong_label_set_and_an_unnamed_family() {
    // The gate itself can go red: a header naming a family nobody renders,
    // or the wrong labels, or leaving one out.
    let scrape = "# HELP a_total h\n# TYPE a_total counter\na_total{x=\"1\",y=\"2\"} 1\n\
                  # HELP b_gauge h\n# TYPE b_gauge gauge\nb_gauge 1\n";
    let header = "names `a_total{y,x}` and `phantom_total`";
    let c = claims(header);
    assert_eq!(
        unrendered(&c, scrape),
        BTreeSet::from(["phantom_total".to_string()])
    );
    assert_eq!(
        label_mismatches(&c, scrape),
        [(
            "a_total".to_string(),
            vec!["y".to_string(), "x".to_string()],
            vec![vec!["x".to_string(), "y".to_string()]],
        )]
    );
    assert_eq!(unnamed(&c, scrape), BTreeSet::from(["b_gauge".to_string()]));
}

#[test]
fn rust_items_paths_prefixes_and_literals_are_not_families() {
    let header = "[`render_state`], `coords.rs`, `/metrics`, `engenho_`, `0`, \
                  `# TYPE`, [`MetricsSource`], `a_total{bad label}`, and `b_total{x}`";
    assert_eq!(
        claims(header),
        [Claim {
            family: "b_total".into(),
            labels: Some(vec!["x".into()]),
        }]
    );
}

#[test]
fn an_escaped_quote_or_comma_in_a_label_value_does_not_split_it() {
    let scrape = "# TYPE m gauge\nm{a=\"x\\\",b=\\\"y\",c=\"z\"} 1\n";
    assert_eq!(
        sample_label_names(scrape, "m"),
        [vec!["a".to_string(), "c".to_string()]]
    );
}
