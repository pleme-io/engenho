//! `KubeUrlBuilder` — typed query-string composition for the
//! Kubernetes REST surface.
//!
//! Per the org-level ★★ TYPED EMISSION rule, query strings are never
//! assembled by `format!()`-ing `key=value&key=value` fragments and
//! hand-encoding values. The single typed surface is [`url::Url`]'s
//! `query_pairs_mut()`, which owns the `application/x-www-form-urlencoded`
//! serialization (the same `url::form_urlencoded` encoder the old
//! hand-rolled `url_enc` helper called). The builder accumulates
//! typed pairs and renders one canonical URL string.
//!
//! Output is byte-identical to the prior hand-built strings: the
//! encoder, the `&` join, and the append order all match.

use engenho_types::error::KubeError;
use url::Url;

/// Builds a Kubernetes request URL from a base (scheme + host + path)
/// plus typed query parameters.
///
/// ```ignore
/// let url = KubeUrlBuilder::new(&base)?
///     .opt_pair("labelSelector", (!sel.is_empty()).then_some(sel))
///     .opt_int("limit", opts.limit)
///     .finish();
/// ```
#[derive(Debug, Clone)]
pub struct KubeUrlBuilder {
    url: Url,
}

impl KubeUrlBuilder {
    /// Parse `base` (an absolute `https://host/path` URL) into a
    /// builder.
    ///
    /// # Errors
    ///
    /// [`KubeError::Network`] if `base` is not a valid absolute URL.
    pub fn new(base: &str) -> Result<Self, KubeError> {
        let url =
            Url::parse(base).map_err(|e| KubeError::Network(format!("parse url {base}: {e}")))?;
        Ok(Self { url })
    }

    /// Append `key=value`. The value is form-urlencoded by `url`.
    #[must_use]
    pub fn pair(mut self, key: &str, value: &str) -> Self {
        self.url.query_pairs_mut().append_pair(key, value);
        self
    }

    /// Append `key=value` only when `value` is `Some` and non-empty.
    /// Mirrors the `if !field.is_empty()` guards the hand-rolled
    /// builders used.
    #[must_use]
    pub fn opt_pair(self, key: &str, value: Option<&str>) -> Self {
        match value {
            Some(v) if !v.is_empty() => self.pair(key, v),
            _ => self,
        }
    }

    /// Append `key=value` from a non-empty `&str` (convenience for the
    /// `if !field.is_empty()` guard over an owned/borrowed string).
    #[must_use]
    pub fn pair_if_nonempty(self, key: &str, value: &str) -> Self {
        if value.is_empty() {
            self
        } else {
            self.pair(key, value)
        }
    }

    /// Append `key=<n>` (any `Display` integer) when `value` is
    /// `Some`.
    #[must_use]
    pub fn opt_int<N: std::fmt::Display>(self, key: &str, value: Option<N>) -> Self {
        match value {
            Some(n) => self.pair(key, &n.to_string()),
            None => self,
        }
    }

    /// Append a bare `key=value` flag unconditionally (e.g.
    /// `watch=true`, `force=true`).
    #[must_use]
    pub fn flag(self, key: &str, value: &str) -> Self {
        self.pair(key, value)
    }

    /// Render the final URL string.
    #[must_use]
    pub fn finish(self) -> String {
        self.url.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://api.example.com/api/v1/namespaces/default/pods";

    #[test]
    fn no_pairs_returns_base() {
        let url = KubeUrlBuilder::new(BASE).unwrap().finish();
        assert_eq!(url, BASE);
    }

    #[test]
    fn list_pairs_match_hand_rolled_encoding() {
        let url = KubeUrlBuilder::new(BASE)
            .unwrap()
            .pair_if_nonempty("labelSelector", "app=podinfo")
            .pair_if_nonempty("fieldSelector", "")
            .opt_int("limit", Some(100))
            .opt_pair("continue", Some("tok en+/"))
            .finish();
        assert_eq!(
            url,
            "https://api.example.com/api/v1/namespaces/default/pods?labelSelector=app%3Dpodinfo&limit=100&continue=tok+en%2B%2F"
        );
    }

    #[test]
    fn empty_values_are_skipped() {
        let url = KubeUrlBuilder::new(BASE)
            .unwrap()
            .pair_if_nonempty("labelSelector", "")
            .opt_pair("continue", None)
            .opt_pair("resourceVersion", Some(""))
            .opt_int("limit", None::<u32>)
            .finish();
        assert_eq!(url, BASE);
    }

    #[test]
    fn watch_flag_and_resource_version() {
        let url = KubeUrlBuilder::new(BASE)
            .unwrap()
            .flag("watch", "true")
            .pair_if_nonempty("resourceVersion", "12345")
            .finish();
        assert_eq!(
            url,
            "https://api.example.com/api/v1/namespaces/default/pods?watch=true&resourceVersion=12345"
        );
    }

    #[test]
    fn watch_without_resource_version() {
        let url = KubeUrlBuilder::new(BASE)
            .unwrap()
            .flag("watch", "true")
            .pair_if_nonempty("resourceVersion", "")
            .finish();
        assert_eq!(
            url,
            "https://api.example.com/api/v1/namespaces/default/pods?watch=true"
        );
    }

    #[test]
    fn patch_field_manager_with_force() {
        let url = KubeUrlBuilder::new(BASE)
            .unwrap()
            .pair("fieldManager", "engenho")
            .flag("force", "true")
            .finish();
        assert_eq!(
            url,
            "https://api.example.com/api/v1/namespaces/default/pods?fieldManager=engenho&force=true"
        );
    }

    #[test]
    fn patch_field_manager_without_force() {
        let url = KubeUrlBuilder::new(BASE)
            .unwrap()
            .pair("fieldManager", "engenho")
            .finish();
        assert_eq!(
            url,
            "https://api.example.com/api/v1/namespaces/default/pods?fieldManager=engenho"
        );
    }

    #[test]
    fn invalid_base_is_typed_error() {
        let err = KubeUrlBuilder::new("not a url").unwrap_err();
        assert!(matches!(err, KubeError::Network(_)));
    }
}
