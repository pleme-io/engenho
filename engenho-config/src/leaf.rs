//! Leaves: the dotted paths a configuration flattens to.
//!
//! A leaf is a value of [`crate::EngenhoConfig`] as it serializes that is not
//! itself a mapping: a scalar, an enum, or a list (lists are replaced whole,
//! never merged, so a list is one leaf). `runtime.tls.enabled` names one.
//! The override tier sets leaves; [`crate::mutability`] classifies them.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A dotted path to one leaf, e.g. `runtime.kubeconfig_publish_visibility`:
/// segments of `[a-z0-9_]`, the first starting with a letter.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LeafPath(String);

/// A string that is not a [`LeafPath`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "{0:?} is not a leaf path: dotted segments of lowercase letters, digits and `_`, the first starting with a letter"
)]
pub struct BadLeafPath(pub String);

impl LeafPath {
    /// Parse a dotted path.
    ///
    /// # Errors
    ///
    /// [`BadLeafPath`] when `s` is not one.
    pub fn parse(s: &str) -> Result<Self, BadLeafPath> {
        let segment_ok = |seg: &str| {
            !seg.is_empty()
                && seg
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        };
        let first_ok = s.bytes().next().is_some_and(|b| b.is_ascii_lowercase());
        if first_ok && s.split('.').all(segment_ok) {
            Ok(Self(s.to_owned()))
        } else {
            Err(BadLeafPath(s.to_owned()))
        }
    }

    /// The path, dotted.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The path's segments.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }

    /// Whether this path is `prefix` or lies under it, segment by segment:
    /// `runtime.tls.enabled` is under `runtime.tls`, not under `runtime.tl`.
    #[must_use]
    pub fn is_under(&self, prefix: &str) -> bool {
        self.0 == prefix
            || self
                .0
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('.'))
    }

    /// `prefix.segment`, or `segment` under the root.
    pub(crate) fn join(prefix: &str, segment: &str) -> Result<Self, BadLeafPath> {
        if prefix.is_empty() {
            Self::parse(segment)
        } else {
            let mut path = String::with_capacity(prefix.len() + 1 + segment.len());
            path.push_str(prefix);
            path.push('.');
            path.push_str(segment);
            Self::parse(&path)
        }
    }
}

impl fmt::Display for LeafPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for LeafPath {
    type Err = BadLeafPath;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for LeafPath {
    type Error = BadLeafPath;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(&s)
    }
}

impl From<LeafPath> for String {
    fn from(path: LeafPath) -> Self {
        path.0
    }
}

/// Every leaf of `value`, by path. A mapping is descended into; anything else
/// is a leaf. An empty mapping has no leaves, and a key that is not a leaf
/// segment is not descended into (the configuration's keys all are).
#[must_use]
pub fn flatten(value: &Value) -> BTreeMap<LeafPath, Value> {
    let mut leaves = BTreeMap::new();
    if let Value::Object(map) = value {
        flatten_into("", map, &mut leaves);
    }
    leaves
}

fn flatten_into(prefix: &str, map: &Map<String, Value>, leaves: &mut BTreeMap<LeafPath, Value>) {
    for (key, value) in map {
        let Ok(path) = LeafPath::join(prefix, key) else {
            continue;
        };
        match value {
            Value::Object(inner) => flatten_into(path.as_str(), inner, leaves),
            leaf => {
                leaves.insert(path, leaf.clone());
            }
        }
    }
}

/// The mapping `leaves` flatten from: the inverse of [`flatten`].
#[must_use]
pub fn nest<'a>(leaves: impl IntoIterator<Item = (&'a LeafPath, &'a Value)>) -> Value {
    let mut root = Map::new();
    for (path, value) in leaves {
        let segments: Vec<&str> = path.segments().collect();
        let Some((last, parents)) = segments.split_last() else {
            continue;
        };
        let mut map = &mut root;
        for segment in parents {
            let entry = map
                .entry((*segment).to_owned())
                .or_insert_with(|| Value::Object(Map::new()));
            if !entry.is_object() {
                *entry = Value::Object(Map::new());
            }
            let Value::Object(inner) = entry else {
                unreachable!("made an object just above");
            };
            map = inner;
        }
        map.insert((*last).to_owned(), value.clone());
    }
    Value::Object(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_leaf_path_is_dotted_lowercase_segments() {
        for good in [
            "runtime",
            "runtime.tls.enabled",
            "controllers.enable.pv_binder",
            "a1.b_2",
        ] {
            assert_eq!(LeafPath::parse(good).unwrap().as_str(), good);
        }
        for bad in [
            "",
            "Runtime",
            "runtime.",
            ".runtime",
            "runtime..tls",
            "1abc",
            "run-time",
            "runtime.TLS",
        ] {
            assert_eq!(
                LeafPath::parse(bad),
                Err(BadLeafPath(bad.to_owned())),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn under_is_segment_wise() {
        let leaf = LeafPath::parse("runtime.tls.enabled").unwrap();
        assert!(leaf.is_under("runtime"));
        assert!(leaf.is_under("runtime.tls"));
        assert!(leaf.is_under("runtime.tls.enabled"));
        assert!(!leaf.is_under("runtime.tl"));
        assert!(!leaf.is_under("runtime.tls.enabled.x"));
    }

    #[test]
    fn flatten_and_nest_are_inverse() {
        let value = json!({
            "runtime": {"listen_addr": "127.0.0.1:0", "tls": {"enabled": true, "extra_sans": ["a", "b"]}},
            "fabric": "in_binary",
            "empty": {},
        });
        let leaves = flatten(&value);
        assert_eq!(
            leaves.keys().map(LeafPath::as_str).collect::<Vec<_>>(),
            [
                "fabric",
                "runtime.listen_addr",
                "runtime.tls.enabled",
                "runtime.tls.extra_sans"
            ],
            "a list is one leaf; an empty mapping has none"
        );
        let mut expected = value;
        expected.as_object_mut().unwrap().remove("empty");
        assert_eq!(nest(&leaves), expected);
    }
}
