//! Object-name validation — DNS-1123, per upstream's rules.
//!
//! Measured 2026-08-28 on the live daemon: `UPPERCASE`, `has spaces`,
//! `-leading-dash`, `has_underscore` and a 64-character name were **all
//! accepted with 201**. There was no name validation anywhere in the
//! workspace.
//!
//! That is not cosmetic. A name is the primary key of the REST path, so an
//! object named `has spaces` is addressable only through percent-encoding, and
//! an object named `UPPERCASE` collides with `uppercase` under any consumer
//! that case-folds. Upstream rejects all of them at admission with `422
//! Invalid`, and every client is written against that promise.
//!
//! ## Two rules, not one
//!
//! Upstream applies **DNS-1123 subdomain** to most resources and the stricter
//! **DNS-1123 label** to those whose names become DNS labels in their own
//! right — a Namespace becomes a DNS segment in every in-cluster hostname, and
//! a Service name is the leftmost label of `<svc>.<ns>.svc.cluster.local`. A
//! 64-character Namespace is therefore not a style violation; it is an
//! unresolvable hostname.
//!
//! | rule | max | alphabet | example holder |
//! |---|---|---|---|
//! | [`NameRule::Label`] | 63 | `[a-z0-9-]`, must start+end alphanumeric | Namespace, Service |
//! | [`NameRule::Subdomain`] | 253 | `[a-z0-9-.]`, each dot-separated part a label | Pod, ConfigMap, … |
//!
//! ## Tier honesty
//!
//! [`ResourceName`] is **parse-time-rejected**, not truly-unrepresentable: the
//! constructor is the only way to build one and it returns a `Result`, so an
//! invalid name cannot exist *as a `ResourceName`* — but the store still
//! accepts a `&str` elsewhere, so this is a boundary that must be *called*
//! rather than an illegal state with no code path. Making it
//! truly-unrepresentable means threading `ResourceName` through `ResourceKey`,
//! which is a larger change than this one; until then, do not describe it as
//! unrepresentable.

use core::fmt;

/// Which DNS-1123 rule a kind's name must satisfy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NameRule {
    /// DNS-1123 **label** — ≤63 chars, `[a-z0-9-]`, start+end alphanumeric.
    /// For kinds whose name becomes a DNS label (Namespace, Service).
    Label,
    /// DNS-1123 **subdomain** — ≤253 chars, dot-separated labels. The default
    /// for most kinds.
    Subdomain,
}

impl NameRule {
    /// The rule upstream applies to `kind`.
    ///
    /// Deliberately keyed on the KIND rather than a per-call argument, so a
    /// call site cannot pick the wrong strictness. The label-ruled set is
    /// small and closed; everything else is subdomain.
    #[must_use]
    pub fn for_kind(kind: &str) -> Self {
        match kind {
            // A Namespace is a DNS segment in every in-cluster hostname; a
            // Service is the leftmost label of its cluster DNS name.
            "Namespace" | "Service" => Self::Label,
            _ => Self::Subdomain,
        }
    }

    /// The maximum length this rule permits.
    #[must_use]
    pub fn max_len(self) -> usize {
        match self {
            Self::Label => 63,
            Self::Subdomain => 253,
        }
    }
}

/// Why a name was rejected. Closed, so the 422 `details.causes` message is
/// derived from a typed reason rather than composed free text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NameError {
    /// The name was empty.
    Empty,
    /// The name exceeded the rule's length limit.
    TooLong {
        /// Actual length.
        len: usize,
        /// The rule's maximum.
        max: usize,
    },
    /// A character outside the permitted alphabet.
    IllegalCharacter {
        /// The offending character.
        ch: char,
    },
    /// Did not start or end with an alphanumeric.
    BadBoundary,
    /// A dot appeared where the rule forbids one (label rule), or an empty
    /// dot-separated part appeared (subdomain rule).
    BadDotting,
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "name must not be empty"),
            Self::TooLong { len, max } => {
                write!(f, "name is {len} characters; must be at most {max}")
            }
            Self::IllegalCharacter { ch } => write!(
                f,
                "name contains {ch:?}; must consist of lowercase alphanumeric characters, \
                 '-', and (for subdomains) '.'"
            ),
            Self::BadBoundary => write!(
                f,
                "name must start and end with a lowercase alphanumeric character"
            ),
            Self::BadDotting => write!(f, "name has an empty or misplaced '.' separated part"),
        }
    }
}

/// A validated object name.
///
/// The only constructor is [`ResourceName::parse`], so holding one is evidence
/// the name satisfied its kind's rule (parse-don't-validate).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResourceName(String);

impl ResourceName {
    /// Validate `name` against `rule`.
    ///
    /// # Errors
    /// Returns the typed [`NameError`] describing the first violation.
    pub fn parse(name: &str, rule: NameRule) -> Result<Self, NameError> {
        if name.is_empty() {
            return Err(NameError::Empty);
        }
        if name.len() > rule.max_len() {
            return Err(NameError::TooLong {
                len: name.len(),
                max: rule.max_len(),
            });
        }

        for ch in name.chars() {
            let ok = ch.is_ascii_lowercase()
                || ch.is_ascii_digit()
                || ch == '-'
                || (ch == '.' && rule == NameRule::Subdomain);
            if !ok {
                return Err(NameError::IllegalCharacter { ch });
            }
        }

        // Each dot-separated part must itself be a well-formed label. For the
        // label rule there is exactly one part, so this covers both cases
        // without a second code path.
        for part in name.split('.') {
            if part.is_empty() {
                return Err(NameError::BadDotting);
            }
            let first = part.chars().next().unwrap_or('-');
            let last = part.chars().last().unwrap_or('-');
            if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
                return Err(NameError::BadBoundary);
            }
        }

        Ok(Self(name.to_string()))
    }

    /// Validate against the rule upstream applies to `kind`.
    ///
    /// # Errors
    /// Returns the typed [`NameError`] describing the first violation.
    pub fn parse_for_kind(name: &str, kind: &str) -> Result<Self, NameError> {
        Self::parse(name, NameRule::for_kind(kind))
    }

    /// The validated name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every name the live daemon wrongly accepted on 2026-08-28.
    #[test]
    fn rejects_every_name_the_live_daemon_accepted() {
        for bad in [
            "UPPERCASE",
            "has spaces",
            "-leading-dash",
            "trailing-dash-",
            "has_underscore",
        ] {
            assert!(
                ResourceName::parse(bad, NameRule::Label).is_err(),
                "{bad:?} must be rejected — the live daemon returned 201 for it"
            );
        }
        // The 64-char name, one over the label limit.
        let sixty_four = "b".repeat(64);
        assert_eq!(
            ResourceName::parse(&sixty_four, NameRule::Label),
            Err(NameError::TooLong { len: 64, max: 63 })
        );
    }

    #[test]
    fn accepts_real_upstream_names() {
        for good in ["default", "kube-system", "kube-node-lease", "my-app-1", "x"] {
            assert!(
                ResourceName::parse(good, NameRule::Label).is_ok(),
                "{good:?} is a valid label"
            );
        }
    }

    /// The label/subdomain split is real, not decorative.
    #[test]
    fn dots_are_subdomain_only() {
        assert!(ResourceName::parse("a.b.c", NameRule::Subdomain).is_ok());
        assert_eq!(
            ResourceName::parse("a.b.c", NameRule::Label),
            Err(NameError::IllegalCharacter { ch: '.' }),
            "a Namespace/Service name becomes a DNS label — dots are illegal"
        );
    }

    #[test]
    fn subdomain_is_longer_but_still_bounded() {
        let long = "a".repeat(253);
        assert!(ResourceName::parse(&long, NameRule::Subdomain).is_ok());
        let too_long = "a".repeat(254);
        assert_eq!(
            ResourceName::parse(&too_long, NameRule::Subdomain),
            Err(NameError::TooLong { len: 254, max: 253 })
        );
    }

    #[test]
    fn empty_dot_parts_are_rejected() {
        for bad in ["a..b", ".a", "a."] {
            assert!(
                ResourceName::parse(bad, NameRule::Subdomain).is_err(),
                "{bad:?} has an empty dot-part"
            );
        }
    }

    /// The rule is chosen by KIND so a call site cannot pick wrong.
    #[test]
    fn kinds_that_become_dns_labels_get_the_strict_rule() {
        assert_eq!(NameRule::for_kind("Namespace"), NameRule::Label);
        assert_eq!(NameRule::for_kind("Service"), NameRule::Label);
        assert_eq!(NameRule::for_kind("Pod"), NameRule::Subdomain);
        assert_eq!(NameRule::for_kind("ConfigMap"), NameRule::Subdomain);
        // A Pod may carry dots; a Namespace may not.
        assert!(ResourceName::parse_for_kind("a.b", "Pod").is_ok());
        assert!(ResourceName::parse_for_kind("a.b", "Namespace").is_err());
    }

    /// Errors render through Display, never format!()-composed free text.
    #[test]
    fn errors_render_actionably() {
        let e = ResourceName::parse("Bad", NameRule::Label).unwrap_err();
        assert!(e.to_string().contains("lowercase"), "got: {e}");
    }
}
