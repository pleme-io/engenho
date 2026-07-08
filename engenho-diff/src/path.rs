//! `JsonPath` — the typed coordinate a divergence lives at.
//!
//! Constructed segment-by-segment (a `Vec<PathSeg>`), NEVER built by
//! `format!()`-ing a string. It is rendered ONLY through the [`Display`]
//! impl — `write!()` inside a `Display` block is the sanctioned typed
//! emission surface (★★ TYPED EMISSION). A path is therefore always a
//! typed value, never free-form text.

use std::fmt;

/// One step along a JSON document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathSeg {
    /// An object key.
    Key(String),
    /// An array index.
    Index(usize),
    /// Every element of an array — used by masks such as
    /// `metadata.managedFields[*].time`.
    Wildcard,
}

/// A typed path into a JSON document.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct JsonPath(Vec<PathSeg>);

impl JsonPath {
    /// The document root (empty path).
    #[must_use]
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// The segments, in order.
    #[must_use]
    pub fn segs(&self) -> &[PathSeg] {
        &self.0
    }

    /// Extend with an object key (builder style).
    #[must_use]
    pub fn key(mut self, k: impl Into<String>) -> Self {
        self.0.push(PathSeg::Key(k.into()));
        self
    }

    /// Extend with an array index (builder style).
    #[must_use]
    pub fn index(mut self, i: usize) -> Self {
        self.0.push(PathSeg::Index(i));
        self
    }

    /// Extend with a wildcard (builder style).
    #[must_use]
    pub fn wildcard(mut self) -> Self {
        self.0.push(PathSeg::Wildcard);
        self
    }

    /// Parse a dotted spec into a typed path. Supports `.key`, `[*]`
    /// (wildcard) and `[N]` (index) segments — e.g.
    /// `"metadata.managedFields[*].time"` or `"spec.clusterIPs"`.
    ///
    /// This is a convenience for authoring mask paths as compact
    /// literals; the result is a fully typed `Vec<PathSeg>`, not a
    /// string that gets re-parsed downstream.
    #[must_use]
    pub fn parse(spec: &str) -> Self {
        let mut p = Self::root();
        for raw in spec.split('.') {
            if raw.is_empty() {
                continue;
            }
            if let Some(br_start) = raw.find('[') {
                let name = &raw[..br_start];
                if !name.is_empty() {
                    p.0.push(PathSeg::Key(name.to_string()));
                }
                for br in raw[br_start..].split(']') {
                    let inner = br.trim_start_matches('[');
                    if inner.is_empty() {
                        continue;
                    }
                    if inner == "*" {
                        p.0.push(PathSeg::Wildcard);
                    } else if let Ok(n) = inner.parse::<usize>() {
                        p.0.push(PathSeg::Index(n));
                    }
                }
            } else {
                p.0.push(PathSeg::Key(raw.to_string()));
            }
        }
        p
    }
}

impl fmt::Display for JsonPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return write!(f, ".");
        }
        for seg in &self.0 {
            match seg {
                PathSeg::Key(k) => write!(f, ".{k}")?,
                PathSeg::Index(i) => write!(f, "[{i}]")?,
                PathSeg::Wildcard => write!(f, "[*]")?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_and_display_round_trip() {
        let p = JsonPath::root().key("metadata").key("name");
        assert_eq!(p.to_string(), ".metadata.name");
        assert_eq!(JsonPath::root().to_string(), ".");
        let idx = JsonPath::root().key("items").index(0).key("uid");
        assert_eq!(idx.to_string(), ".items[0].uid");
    }

    #[test]
    fn parse_handles_wildcard_and_index() {
        let p = JsonPath::parse("metadata.managedFields[*].time");
        assert_eq!(
            p.segs(),
            &[
                PathSeg::Key("metadata".into()),
                PathSeg::Key("managedFields".into()),
                PathSeg::Wildcard,
                PathSeg::Key("time".into()),
            ]
        );
        assert_eq!(p.to_string(), ".metadata.managedFields[*].time");

        let idx = JsonPath::parse("spec.containers[0].image");
        assert_eq!(idx.to_string(), ".spec.containers[0].image");
    }
}
