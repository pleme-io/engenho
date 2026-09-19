//! `Accept`, read the way upstream's negotiator reads it.
//!
//! k8s.io/apiserver's `NegotiateMediaTypeOptions` parses `Accept` with
//! goautoneg's `ParseAccept`, ranks the ranges, and answers with the first
//! serializer that the first matching range takes. Two consequences a client
//! can see, both measured by running that code (k8s.io/apiserver v0.35.0
//! with goautoneg a7dc8b61c822, the pin v1.34 carries too; the table is in
//! `tests/w3_protobuf_codec.rs`):
//!
//! - `q` ranks and never refuses. A missing `q` is 1 and higher goes first,
//!   but a `q=0` range still matches: upstream answers
//!   `application/vnd.kubernetes.protobuf,application/json;q=0` with JSON for
//!   a kind that has no protobuf form, not with 406.
//! - Order decides between equals: `application/json,application/vnd.kubernetes.protobuf`
//!   is answered with JSON.
//!
//! The ranking is Go's `sort.Sort` over goautoneg's `Less`, which is not a
//! strict weak order: a concrete type ranks ahead of `*/*` whatever their
//! `q`s. So the algorithm decides, not only the key. Go insertion-sorts up to
//! twelve ranges, and [`ranked`] insertion-sorts any number; past twelve Go's
//! pdqsort can order them differently. No client sends that many.
//!
//! One deliberate difference: media types compare case-insensitively, as
//! RFC 9110 has them, where upstream compares them exactly. It only accepts
//! what upstream would refuse, never the reverse.

use engenho_kube_proto::CONTENT_TYPE_PROTOBUF;

/// A response serialization, in upstream's serializer order: a wildcard range
/// takes the first one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Served {
    Json,
    Yaml,
    Protobuf,
}

impl Served {
    /// Every serialization, in upstream's order.
    pub const ALL: [Self; 3] = [Self::Json, Self::Yaml, Self::Protobuf];

    /// The media type.
    #[must_use]
    pub const fn media_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Yaml => "application/yaml",
            Self::Protobuf => CONTENT_TYPE_PROTOBUF,
        }
    }
}

/// One media range of an `Accept` header, as goautoneg parses it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AcceptRange<'a> {
    /// The type, `*` for any.
    pub kind: &'a str,
    /// The subtype, `*` for any.
    pub subtype: &'a str,
    /// The weight: 1 when absent, and 0 when present but not a number, which
    /// is what Go's `ParseFloat` leaves behind on an error.
    pub q: f32,
}

impl<'a> AcceptRange<'a> {
    /// One comma-separated part, or `None` for a part goautoneg skips: a
    /// type with no subtype (other than `*`), or a third `/` segment.
    fn parse(part: &'a str) -> Option<Self> {
        let part = part.trim_matches(' ');
        let (media, params) = split(part, ';');
        let (kind, rest) = split(media, '/');
        let kind = kind.trim_matches(' ');
        let subtype = match rest {
            None if kind == "*" => "*",
            None => return None,
            Some(rest) => match split(rest, '/') {
                (_, Some(_)) => return None,
                (subtype, None) => subtype.trim_matches(' '),
            },
        };
        let mut q = 1.0;
        let mut params = params;
        while let Some(remaining) = params {
            let (param, next) = split(remaining, ';');
            params = next;
            // A parameter needs one `=` with something after it.
            let (token, Some(value)) = split(param, '=') else {
                continue;
            };
            let (value, None) = split(value, '=') else {
                continue;
            };
            // Only the token is trimmed: `q= 0.5` does not parse, so it is 0.
            if token.trim_matches(' ') == "q" {
                q = value.parse().unwrap_or(0.0);
            }
        }
        Some(Self { kind, subtype, q })
    }

    /// goautoneg's `Less`: a higher `q`, a concrete type over `*`, or a
    /// concrete subtype over `*`.
    fn ranks_before(&self, other: &Self) -> bool {
        self.q > other.q
            || (self.kind != "*" && other.kind == "*")
            || (self.subtype != "*" && other.subtype == "*")
    }

    /// Whether this range takes `served`.
    #[must_use]
    pub fn takes(&self, served: Served) -> bool {
        let media = served.media_type();
        let (kind, subtype) = media.split_once('/').unwrap_or((media, ""));
        (self.kind == "*" && self.subtype == "*")
            || (self.kind.eq_ignore_ascii_case(kind)
                && (self.subtype == "*" || self.subtype.eq_ignore_ascii_case(subtype)))
    }

    /// The serialization upstream picks for this range when it offers them
    /// all: the first one the range takes.
    #[must_use]
    pub fn serialization(&self) -> Option<Served> {
        Served::ALL.into_iter().find(|served| self.takes(*served))
    }
}

/// goautoneg's `nextSplitElement`: the text before the first `sep`, and what
/// follows it, which is absent when nothing does (`"a;"` is just `"a"`).
fn split(s: &str, sep: char) -> (&str, Option<&str>) {
    match s.split_once(sep) {
        Some((head, tail)) if !tail.is_empty() => (head, Some(tail)),
        Some((head, _)) => (head, None),
        None => (s, None),
    }
}

/// `accept`'s ranges in upstream's rank order: goautoneg's `Less`, applied
/// by the insertion sort Go's `sort.Sort` runs on up to twelve elements.
#[must_use]
pub fn ranked(accept: &str) -> Vec<AcceptRange<'_>> {
    let mut ranges: Vec<AcceptRange<'_>> = parts(accept).filter_map(AcceptRange::parse).collect();
    for i in 1..ranges.len() {
        let mut j = i;
        while j > 0 && ranges[j].ranks_before(&ranges[j - 1]) {
            ranges.swap(j, j - 1);
            j -= 1;
        }
    }
    ranges
}

/// What upstream answers `accept` with for a kind that offers every
/// serialization: the first ranked range that takes one decides. `None` is
/// upstream's 406.
#[must_use]
pub fn negotiate(accept: &str) -> Option<Served> {
    ranked(accept).iter().find_map(AcceptRange::serialization)
}

/// Whether some range takes `served`, at any `q`. For a kind that offers
/// only `served` (as a kind with no protobuf form offers only JSON), this is
/// whether upstream answers at all.
#[must_use]
pub fn admits(accept: &str, served: Served) -> bool {
    parts(accept)
        .filter_map(AcceptRange::parse)
        .any(|range| range.takes(served))
}

/// The comma-separated parts, as goautoneg walks them.
fn parts(accept: &str) -> impl Iterator<Item = &str> {
    let mut rest = (!accept.is_empty()).then_some(accept);
    std::iter::from_fn(move || {
        let (part, next) = split(rest?, ',');
        rest = next;
        Some(part)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// goautoneg's parsing quirks, each as `ParseAccept` has it.
    #[test]
    fn ranges_parse_as_goautoneg_parses_them() {
        let one = AcceptRange::parse;
        assert_eq!(
            one("application/json"),
            Some(AcceptRange {
                kind: "application",
                subtype: "json",
                q: 1.0
            })
        );
        assert_eq!(
            one("*"),
            Some(AcceptRange {
                kind: "*",
                subtype: "*",
                q: 1.0
            })
        );
        assert_eq!(
            one("application"),
            None,
            "a type with no subtype is skipped"
        );
        assert_eq!(one("application/"), None, "an empty subtype is no subtype");
        assert_eq!(one("a/b/c"), None, "a third segment is skipped");
        assert_eq!(
            one("a/b/").map(|r| r.subtype),
            Some("b"),
            "an empty third segment is no segment"
        );
        assert_eq!(one("application/json;q=0.5").map(|r| r.q), Some(0.5));
        assert_eq!(one("application/json;q=bogus").map(|r| r.q), Some(0.0));
        assert_eq!(one("application/json;q= 0.5").map(|r| r.q), Some(0.0));
        assert_eq!(
            one("application/json;Q=0.5").map(|r| r.q),
            Some(1.0),
            "q is case-sensitive"
        );
        assert_eq!(
            one("application/json;q=0.5=1").map(|r| r.q),
            Some(1.0),
            "two = are skipped"
        );
        assert_eq!(
            one("application/json;q=0.5=").map(|r| r.q),
            Some(0.5),
            "a trailing = is not"
        );
        assert_eq!(
            one("application/json;q=").map(|r| r.q),
            Some(1.0),
            "an empty value is skipped"
        );
    }

    /// A concrete type ranks ahead of `*/*` whatever the two `q`s are.
    #[test]
    fn a_concrete_type_outranks_a_wildcard_at_any_q() {
        let order: Vec<&str> = ranked("*/*;q=0.9,application/json;q=0.1")
            .iter()
            .map(|r| r.subtype)
            .collect();
        assert_eq!(order, ["json", "*"]);
    }
}
