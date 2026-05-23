//! risca — typed PII/secret redaction marker.
//!
//! Per the research brief — eighth inventive primitive. Wrap any
//! sensitive value in `Risca<T>` and the substrate makes accidental
//! leakage IMPOSSIBLE at the type level:
//!
//!   - `Debug` impl prints `<RISCA:T>` — never the value
//!   - `Display` impl prints `<RISCA>` — never the value
//!   - `Serialize` impl writes the string `<REDACTED>` — never the value
//!
//! Unredaction is explicit + auditable via `into_inner()` /
//! `expose_secret()` — operators have to type those names, making
//! every leak a code-review-visible deliberate act.
//!
//! ## Surface
//!
//!   - `Risca<T>` — wrapper that opaques `T` from all default
//!     emission surfaces
//!   - `Redact` trait — typed transform `T -> T` that produces a
//!     redacted variant (for compound types where you can't just
//!     wrap the whole thing)
//!   - `redact_email` / `redact_token` / `redact_credit_card` —
//!     typed transforms over `&str` with stable outputs
//!
//! ## Composition with other substrate primitives
//!
//!   - `mirante::Observable`: snapshots of types containing sensitive
//!     fields wrap those fields in Risca; dashboards render
//!     `<REDACTED>`, not the value
//!   - `linhagem-aberta::LineageNode`: node values may contain Risca
//!     fields; lineage hashes still compute over the inner value but
//!     proofs surface redacted versions
//!   - `replay::ReplayCursor`: events containing Risca fields stay
//!     replayable but serialize redacted

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The redaction placeholder string written by all default emission paths.
pub const REDACTED: &str = "<REDACTED>";

/// The substring every `Risca<T>`'s `Debug` impl emits — used by
/// [`assert_risca_no_leak!`] to confirm the framing is in place.
pub const RISCA_FRAMING: &str = "RISCA";

/// Assert at test time that a value's `Debug` output does NOT leak
/// the given secret + DOES carry the `Risca` framing. Extracted per
/// the third-site rule (substrate's own risca tests + kube-client
/// bearer_token + revoada NodeIdentity signing_key_bytes).
///
/// ## Usage
///
/// ```ignore
/// use engenho_substrate::assert_risca_no_leak;
///
/// #[test]
/// fn bearer_token_does_not_leak() {
///     let secret = "super-secret-bearer-9f3a";
///     let conn = make_connection_with(secret);
///     let token = conn.bearer_token().unwrap().unwrap();
///     assert_risca_no_leak!(token, secret);
/// }
/// ```
///
/// Equivalent to:
///
/// ```ignore
/// let dbg = format!("{token:?}");
/// assert!(!dbg.contains(secret), "leaked: {dbg}");
/// assert!(dbg.contains("RISCA"));
/// ```
#[macro_export]
macro_rules! assert_risca_no_leak {
    ($risca:expr, $secret:expr $(,)?) => {{
        let __dbg = ::std::format!("{:?}", $risca);
        ::std::assert!(
            !__dbg.contains($secret),
            "Risca leaked secret '{}' in Debug output: {}",
            $secret,
            __dbg
        );
        ::std::assert!(
            __dbg.contains($crate::risca::RISCA_FRAMING),
            "Risca framing string not found in Debug output: {}",
            __dbg
        );
    }};
}

/// Typed redaction wrapper. `Debug`, `Display`, `Serialize` all
/// produce a redacted form — the inner value is reachable only via
/// the explicit `into_inner()` / `expose_secret()` methods.
#[derive(Clone, PartialEq, Eq)]
pub struct Risca<T> {
    inner: T,
}

impl<T> Risca<T> {
    /// Wrap a sensitive value.
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Unwrap. Named verbosely so every call is grep-able + reviewable.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Borrow the inner value. Named verbosely to mark each call site
    /// as an explicit secret-exposure.
    pub fn expose_secret(&self) -> &T {
        &self.inner
    }
}

impl<T> fmt::Debug for Risca<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<RISCA:{}>", std::any::type_name::<T>())
    }
}

impl<T> fmt::Display for Risca<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<RISCA>")
    }
}

impl<T> Serialize for Risca<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(REDACTED)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Risca<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Round-trip behavior: a Risca<T> read back from serde gets
        // the inner T deserialized normally. The serialize path
        // redacts; the deserialize path trusts the operator put real
        // data in the input.
        let inner = T::deserialize(deserializer)?;
        Ok(Self { inner })
    }
}

/// Typed transform producing a redacted form of `Self`. Use for
/// compound types where you'd rather redact specific fields than
/// wrap the whole struct in `Risca`.
pub trait Redact {
    /// Return a copy with sensitive fields redacted.
    #[must_use]
    fn redact(&self) -> Self;
}

/// Redact an email — keep first char + domain, replace local part.
///
/// `"alice@example.com"` → `"a***@example.com"`
/// `"@example.com"` → `"<REDACTED>@example.com"` (no local part)
/// `"no-at-sign"` → `"<REDACTED>"`
#[must_use]
pub fn redact_email(s: &str) -> String {
    let Some(at_idx) = s.find('@') else {
        return REDACTED.to_string();
    };
    let local = &s[..at_idx];
    let domain = &s[at_idx..];
    if local.is_empty() {
        return format!("{REDACTED}{domain}");
    }
    let Some(first) = local.chars().next() else {
        return REDACTED.to_string();
    };
    format!("{first}***{domain}")
}

/// Redact a credential token — keep first 4 + last 4 chars, mask middle.
///
/// `"ghp_1234567890abcdef"` → `"ghp_****cdef"`
/// `"short"` → `"<REDACTED>"` (under 8 chars, full redact)
#[must_use]
pub fn redact_token(s: &str) -> String {
    if s.len() < 8 {
        return REDACTED.to_string();
    }
    let head = &s[..4];
    let tail = &s[s.len() - 4..];
    format!("{head}****{tail}")
}

/// Redact a credit-card-shaped string — keep last 4 digits, mask rest.
///
/// `"4111111111111111"` → `"************1111"`
/// `"42 42 42"` → `"<REDACTED>"` (not 16 digits)
#[must_use]
pub fn redact_credit_card(s: &str) -> String {
    let digits: String = s.chars().filter(char::is_ascii_digit).collect();
    if digits.len() != 16 {
        return REDACTED.to_string();
    }
    let tail = &digits[12..];
    format!("************{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn risca_debug_does_not_leak() {
        let r = Risca::new(String::from("super-secret"));
        // Consume the assert_risca_no_leak! macro — substrate eats its own dogfood.
        crate::assert_risca_no_leak!(r, "super-secret");
    }

    #[test]
    fn assert_risca_no_leak_macro_passes_on_clean_wrapper() {
        let r = Risca::new(String::from("xyzzy-secret-123"));
        crate::assert_risca_no_leak!(r, "xyzzy-secret-123");
    }

    #[test]
    #[should_panic(expected = "Risca leaked secret")]
    fn assert_risca_no_leak_macro_panics_when_secret_leaks() {
        // Construct a synthetic value whose Debug DOES contain the
        // "secret" — proves the macro fires correctly when the
        // wrapper is bypassed (e.g. operator forgets to use Risca).
        #[derive(Debug)]
        struct NotRisca {
            #[allow(dead_code)]
            value: String,
        }
        let exposed = NotRisca {
            value: "leaked-token-abc".into(),
        };
        crate::assert_risca_no_leak!(exposed, "leaked-token-abc");
    }

    #[test]
    fn risca_display_does_not_leak() {
        let r = Risca::new(String::from("super-secret"));
        let display = format!("{r}");
        assert_eq!(display, "<RISCA>");
    }

    #[test]
    fn risca_serialize_emits_redacted_string() {
        let r = Risca::new(String::from("super-secret"));
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, "\"<REDACTED>\"");
    }

    #[test]
    fn risca_expose_secret_returns_inner_borrow() {
        let r = Risca::new(String::from("super-secret"));
        assert_eq!(r.expose_secret(), "super-secret");
    }

    #[test]
    fn risca_into_inner_consumes_and_returns() {
        let r = Risca::new(42_u32);
        assert_eq!(r.into_inner(), 42);
    }

    #[test]
    fn risca_wraps_arbitrary_types() {
        let r = Risca::new(vec![1, 2, 3]);
        assert_eq!(r.expose_secret(), &vec![1, 2, 3]);
    }

    #[test]
    fn risca_eq_compares_inner() {
        let a = Risca::new(42);
        let b = Risca::new(42);
        let c = Risca::new(43);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn risca_round_trips_through_json_via_deserialize() {
        // Deserialize accepts raw inner data (not the REDACTED placeholder).
        let json = "\"real-value\"";
        let r: Risca<String> = serde_json::from_str(json).unwrap();
        assert_eq!(r.expose_secret(), "real-value");
    }

    #[test]
    fn risca_in_struct_redacts_only_marked_fields() {
        #[derive(Serialize, Deserialize)]
        struct UserRecord {
            id: u64,
            email: Risca<String>,
            display_name: String,
        }

        let u = UserRecord {
            id: 42,
            email: Risca::new("alice@example.com".to_string()),
            display_name: "Alice".to_string(),
        };
        let json = serde_json::to_string(&u).unwrap();
        assert!(json.contains("\"id\":42"));
        assert!(json.contains("\"display_name\":\"Alice\""));
        assert!(json.contains("\"email\":\"<REDACTED>\""));
        assert!(!json.contains("alice@example.com"));
    }

    // ── redact_email ────────────────────────────────────────────

    #[test]
    fn redact_email_keeps_first_char_and_domain() {
        assert_eq!(redact_email("alice@example.com"), "a***@example.com");
    }

    #[test]
    fn redact_email_handles_empty_local() {
        assert_eq!(redact_email("@example.com"), "<REDACTED>@example.com");
    }

    #[test]
    fn redact_email_handles_no_at_sign() {
        assert_eq!(redact_email("no-at-sign"), "<REDACTED>");
    }

    // ── redact_token ────────────────────────────────────────────

    #[test]
    fn redact_token_keeps_head_and_tail() {
        assert_eq!(redact_token("ghp_1234567890abcdef"), "ghp_****cdef");
    }

    #[test]
    fn redact_token_under_8_chars_fully_redacts() {
        assert_eq!(redact_token("short"), "<REDACTED>");
    }

    // ── redact_credit_card ──────────────────────────────────────

    #[test]
    fn redact_credit_card_keeps_last_four_digits() {
        assert_eq!(redact_credit_card("4111111111111111"), "************1111");
    }

    #[test]
    fn redact_credit_card_strips_spaces_first() {
        assert_eq!(
            redact_credit_card("4111 1111 1111 1111"),
            "************1111"
        );
    }

    #[test]
    fn redact_credit_card_rejects_non_16_digit() {
        assert_eq!(redact_credit_card("42 42 42"), "<REDACTED>");
    }

    // ── Redact trait ────────────────────────────────────────────

    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Config {
        host: String,
        token: String,
    }

    impl Redact for Config {
        fn redact(&self) -> Self {
            Self {
                host: self.host.clone(),
                token: redact_token(&self.token),
            }
        }
    }

    #[test]
    fn redact_trait_produces_field_level_redaction() {
        let c = Config {
            host: "example.com".to_string(),
            token: "ghp_1234567890abcdef".to_string(),
        };
        let r = c.redact();
        assert_eq!(r.host, "example.com");
        assert_eq!(r.token, "ghp_****cdef");
    }
}
