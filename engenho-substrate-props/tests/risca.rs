//! Property: risca Risca<T> + redaction helpers.

use engenho_substrate::{REDACTED, Risca, redact_credit_card, redact_email, redact_token};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// Risca<String> Debug output is exactly the framing template — it
    /// never inspects the inner value at all (so it CAN'T leak).
    /// Constant-output is a stronger contract than "doesn't contain":
    /// if the secret happens to be a substring of the framing string
    /// (e.g. "I" is in "RISCA"), substring-checks falsely fail. The
    /// real guarantee is bit-identical output across all inputs.
    #[test]
    fn debug_is_constant_framing(secret in any::<String>()) {
        let r = Risca::new(secret);
        prop_assert_eq!(format!("{r:?}"), "<RISCA:alloc::string::String>");
    }

    /// Risca<String> Display always emits exactly "<RISCA>".
    #[test]
    fn display_is_constant(secret in ".{0,500}") {
        let r = Risca::new(secret);
        prop_assert_eq!(format!("{r}"), "<RISCA>");
    }

    /// Risca<String> Serialize always emits "<REDACTED>".
    #[test]
    fn serialize_is_constant(secret in ".{0,500}") {
        let r = Risca::new(secret);
        prop_assert_eq!(serde_json::to_string(&r).unwrap(), format!("\"{REDACTED}\""));
    }

    /// expose_secret returns the original value byte-for-byte.
    #[test]
    fn expose_secret_returns_original(secret in any::<String>()) {
        let r = Risca::new(secret.clone());
        prop_assert_eq!(r.expose_secret(), &secret);
    }

    /// into_inner returns the original value byte-for-byte.
    #[test]
    fn into_inner_returns_original(secret in any::<String>()) {
        let r = Risca::new(secret.clone());
        prop_assert_eq!(r.into_inner(), secret);
    }

    /// Risca preserves equality based on inner value only.
    #[test]
    fn equality_reflects_inner_only(a in any::<u32>(), b in any::<u32>()) {
        let ra = Risca::new(a);
        let rb = Risca::new(b);
        prop_assert_eq!(ra == rb, a == b);
    }

    /// redact_email always keeps the @domain suffix verbatim.
    #[test]
    fn redact_email_preserves_domain(
        local in "[a-zA-Z0-9]{1,20}",
        domain in "[a-z]{2,15}\\.[a-z]{2,5}",
    ) {
        let email = format!("{local}@{domain}");
        let redacted = redact_email(&email);
        let want_suffix = format!("@{domain}");
        prop_assert!(redacted.ends_with(&want_suffix));
    }

    /// redact_email always returns REDACTED for inputs with no '@'.
    #[test]
    fn redact_email_no_at_returns_redacted(local in "[a-zA-Z0-9]{1,30}") {
        // Filter out any string that happens to contain '@' (excluded by regex).
        prop_assume!(!local.contains('@'));
        prop_assert_eq!(redact_email(&local), REDACTED);
    }

    /// redact_token keeps the first 4 and last 4 chars verbatim.
    #[test]
    fn redact_token_preserves_head_and_tail(token in "[a-zA-Z0-9]{8,50}") {
        let redacted = redact_token(&token);
        prop_assert!(redacted.starts_with(&token[..4]));
        prop_assert!(redacted.ends_with(&token[token.len() - 4..]));
    }

    /// redact_token returns REDACTED for too-short inputs.
    #[test]
    fn redact_token_under_8_fully_redacts(token in "[a-zA-Z0-9]{0,7}") {
        prop_assert_eq!(redact_token(&token), REDACTED);
    }

    /// redact_credit_card keeps exactly 4 trailing digits.
    #[test]
    fn redact_credit_card_preserves_last_four(prefix in "[0-9]{12}", last in "[0-9]{4}") {
        let card = format!("{prefix}{last}");
        prop_assert_eq!(redact_credit_card(&card), format!("************{last}"));
    }

    /// redact_credit_card returns REDACTED when digit count != 16.
    #[test]
    fn redact_credit_card_rejects_wrong_digit_count(digits in "[0-9]{0,15}") {
        prop_assert_eq!(redact_credit_card(&digits), REDACTED);
    }

    /// Roundtrip via serde deserialize gives the original.
    #[test]
    fn serde_roundtrip_via_deserialize(secret in any::<String>()) {
        // Note: serializing a Risca emits "<REDACTED>"; to roundtrip
        // the real value we serialize the raw inner string and
        // deserialize into Risca.
        let json = serde_json::to_string(&secret).unwrap();
        let r: Risca<String> = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(r.into_inner(), secret);
    }
}
