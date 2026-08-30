//! SERVICEACCOUNT TOKENS — issuing and validating the credential every
//! in-cluster workload uses.
//!
//! ★ WHAT ITS ABSENCE BLOCKED. `authn.rs` recognised a ServiceAccount
//! bearer token and returned a typed 401: correct, honest, and total. Every
//! in-cluster client authenticates with one of these — controllers,
//! operators, anything using the mounted token — so without issuance and
//! validation NOTHING running inside the cluster can talk to the apiserver
//! as itself. It is the single credential a Kubernetes workload is
//! guaranteed to have, and it was the one credential engenho could not
//! accept.
//!
//! ★ BOUND TOKENS, NOT LEGACY ONES. Upstream deprecated the forever-valid
//! Secret-backed token precisely because it never expires and survives the
//! deletion of the ServiceAccount it names. Tokens here always carry `exp`
//! and always name what they are bound to, so a leaked token is bounded in
//! time and a token for a deleted workload is identifiable as stale. There
//! is deliberately no way to mint one without an expiry — the API takes a
//! lifetime, it does not default to forever.
//!
//! ★ VALIDATION REJECTS BY DEFAULT AND NAMES THE REASON. Every failure is a
//! distinct variant: a signature that does not verify, an expired token, a
//! wrong issuer, and a wrong audience are four different security events,
//! and collapsing them to "invalid" makes the audit log useless for telling
//! an attack from a clock skew.
//!
//! ★ AUDIENCE IS CHECKED, NOT ADVISORY. A token minted for a webhook must
//! not authenticate against the apiserver; that is the entire reason bound
//! tokens carry `aud`. Skipping the check turns every audience-scoped token
//! into a cluster-wide credential.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// What a token says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// Issuer — the apiserver that minted it.
    pub iss: String,
    /// Subject: `system:serviceaccount:<namespace>:<name>`.
    pub sub: String,
    /// Audiences this token is valid for.
    pub aud: Vec<String>,
    /// Expiry, seconds since the epoch.
    pub exp: i64,
    /// Issued-at, seconds since the epoch.
    pub iat: i64,
    /// Upstream's bound-object claim namespace.
    #[serde(rename = "kubernetes.io", skip_serializing_if = "Option::is_none")]
    pub kubernetes: Option<KubeClaims>,
}

/// The `kubernetes.io` claim block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubeClaims {
    pub namespace: String,
    pub serviceaccount: NamedUid,
    /// The Pod this token was bound to, when it was projected into one.
    ///
    /// Present so a token that outlives its Pod is IDENTIFIABLE as stale —
    /// the whole point of a bound token over the legacy forever-token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod: Option<NamedUid>,
}

/// A name/uid pair, as upstream spells it in the claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedUid {
    pub name: String,
    pub uid: String,
}

/// Why a token was rejected.
///
/// Four distinct variants on purpose: a bad signature, an expiry, a wrong
/// issuer and a wrong audience are four different security events, and
/// collapsing them makes an audit log useless for telling an attack from
/// clock skew.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TokenError {
    #[error("token is malformed: {0}")]
    Malformed(String),
    #[error("token signature does not verify")]
    BadSignature,
    #[error("token expired at {exp} (now {now})")]
    Expired { exp: i64, now: i64 },
    #[error("token issuer {got:?} is not {want:?}")]
    WrongIssuer { got: String, want: String },
    #[error("token audience {got:?} does not include {want:?}")]
    WrongAudience { got: Vec<String>, want: String },
}

/// The subject string upstream uses for a ServiceAccount.
///
/// A constant function rather than a literal at each call site: the
/// `system:serviceaccount:` prefix is what RBAC matches on, and a typo in
/// one place would produce a token that authenticates as a principal no
/// RoleBinding names — an authenticated request that can do nothing, which
/// reads as a permissions bug rather than a typo.
#[must_use]
pub fn subject_for(namespace: &str, name: &str) -> String {
    format!("system:serviceaccount:{namespace}:{name}")
}

/// The group set a ServiceAccount principal carries.
///
/// Upstream puts every SA in `system:serviceaccounts` AND in a per-namespace
/// group, and a great many default RoleBindings key on the latter. Emitting
/// only the first would silently strip permissions the cluster grants by
/// convention.
#[must_use]
pub fn groups_for(namespace: &str) -> Vec<String> {
    vec![
        "system:serviceaccounts".to_string(),
        format!("system:serviceaccounts:{namespace}"),
    ]
}

/// Mint a bound ServiceAccount token.
///
/// `now` and `lifetime_secs` are supplied rather than read: the clock is
/// not this module's to read, and a caller that must choose a lifetime
/// cannot accidentally mint a forever-token.
pub fn issue(
    key: &SigningKey,
    issuer: &str,
    namespace: &str,
    name: &str,
    uid: &str,
    audiences: &[String],
    pod: Option<NamedUid>,
    now: i64,
    lifetime_secs: i64,
) -> Result<String, TokenError> {
    let claims = Claims {
        iss: issuer.to_string(),
        sub: subject_for(namespace, name),
        aud: audiences.to_vec(),
        exp: now + lifetime_secs,
        iat: now,
        kubernetes: Some(KubeClaims {
            namespace: namespace.to_string(),
            serviceaccount: NamedUid {
                name: name.to_string(),
                uid: uid.to_string(),
            },
            pod,
        }),
    };
    // `EdDSA` is the JOSE name for ed25519. `typ: JWT` is required by
    // strict verifiers and costs nothing to emit.
    let header = serde_json::json!({ "alg": "EdDSA", "typ": "JWT" });
    let h =
        B64.encode(serde_json::to_vec(&header).map_err(|e| TokenError::Malformed(e.to_string()))?);
    let c =
        B64.encode(serde_json::to_vec(&claims).map_err(|e| TokenError::Malformed(e.to_string()))?);
    let signing_input = [h.as_str(), ".", c.as_str()].concat();
    let sig = key.sign(signing_input.as_bytes());
    Ok([signing_input.as_str(), ".", &B64.encode(sig.to_bytes())].concat())
}

/// Verify a token and return its claims.
///
/// Checks, in order: structure, signature, expiry, issuer, audience. The
/// SIGNATURE is checked before anything else that reads the claims —
/// trusting an unverified claim to decide whether to keep verifying is how
/// a forged token steers its own validation.
pub fn verify(
    key: &VerifyingKey,
    token: &str,
    issuer: &str,
    audience: &str,
    now: i64,
) -> Result<Claims, TokenError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(TokenError::Malformed(format!(
            "expected 3 dot-separated segments, got {}",
            parts.len()
        )));
    }
    let signing_input = [parts[0], ".", parts[1]].concat();
    let sig_bytes = B64
        .decode(parts[2])
        .map_err(|e| TokenError::Malformed(e.to_string()))?;
    let sig = Signature::from_slice(&sig_bytes).map_err(|_| TokenError::BadSignature)?;
    key.verify(signing_input.as_bytes(), &sig)
        .map_err(|_| TokenError::BadSignature)?;

    let claims: Claims = serde_json::from_slice(
        &B64.decode(parts[1])
            .map_err(|e| TokenError::Malformed(e.to_string()))?,
    )
    .map_err(|e| TokenError::Malformed(e.to_string()))?;

    if claims.exp <= now {
        return Err(TokenError::Expired {
            exp: claims.exp,
            now,
        });
    }
    if claims.iss != issuer {
        return Err(TokenError::WrongIssuer {
            got: claims.iss.clone(),
            want: issuer.to_string(),
        });
    }
    // An empty audience list authenticates NOWHERE rather than everywhere.
    // The permissive reading would turn every token into a cluster-wide
    // credential the moment a minting bug dropped the field.
    if !claims.aud.iter().any(|a| a == audience) {
        return Err(TokenError::WrongAudience {
            got: claims.aud.clone(),
            want: audience.to_string(),
        });
    }
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SigningKey {
        // Deterministic so tests are reproducible; never a pattern for
        // production key material.
        SigningKey::from_bytes(&[7u8; 32])
    }

    const ISS: &str = "https://engenho.local";
    const AUD: &str = "https://kubernetes.default.svc";
    const NOW: i64 = 1_800_000_000;

    fn mint(aud: &[&str], now: i64, lifetime: i64) -> String {
        issue(
            &key(),
            ISS,
            "default",
            "builder",
            "sa-uid",
            &aud.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
            Some(NamedUid {
                name: "pod-1".into(),
                uid: "pod-uid".into(),
            }),
            now,
            lifetime,
        )
        .expect("mints")
    }

    #[test]
    fn a_freshly_minted_token_verifies_and_carries_its_identity() {
        // Anti-vacuity: a verifier that rejected everything would pass
        // every negative test below.
        let t = mint(&[AUD], NOW, 3600);
        let c = verify(&key().verifying_key(), &t, ISS, AUD, NOW + 1).expect("verifies");
        assert_eq!(c.sub, "system:serviceaccount:default:builder");
        assert_eq!(c.iss, ISS);
        let k = c.kubernetes.expect("bound claims");
        assert_eq!(k.namespace, "default");
        assert_eq!(k.serviceaccount.uid, "sa-uid");
        // The pod binding is what makes a token outliving its Pod
        // IDENTIFIABLE as stale.
        assert_eq!(k.pod.expect("pod binding").uid, "pod-uid");
    }

    #[test]
    fn a_tampered_token_fails_on_the_signature() {
        let t = mint(&[AUD], NOW, 3600);
        let mut parts: Vec<&str> = t.split('.').collect();
        // Re-encode claims with an escalated subject, keeping the original
        // signature — the exact forgery the signature exists to stop.
        let forged = B64.encode(
            serde_json::to_vec(&serde_json::json!({
                "iss": ISS, "sub": "system:serviceaccount:kube-system:admin",
                "aud": [AUD], "exp": NOW + 3600, "iat": NOW
            }))
            .unwrap(),
        );
        parts[1] = &forged;
        let tampered = parts.join(".");
        assert_eq!(
            verify(&key().verifying_key(), &tampered, ISS, AUD, NOW + 1),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn the_signature_is_checked_before_any_claim_is_trusted() {
        // Trusting an unverified claim to decide whether to keep verifying
        // is how a forged token steers its own validation. A token with a
        // bad signature AND an expired exp must report the SIGNATURE.
        let t = mint(&[AUD], NOW - 10_000, 1);
        let mut parts: Vec<&str> = t.split('.').collect();
        let other = B64.encode([9u8; 64]);
        parts[2] = &other;
        let bad = parts.join(".");
        assert_eq!(
            verify(&key().verifying_key(), &bad, ISS, AUD, NOW),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn an_expired_token_is_refused_and_says_so() {
        let t = mint(&[AUD], NOW, 60);
        match verify(&key().verifying_key(), &t, ISS, AUD, NOW + 61) {
            Err(TokenError::Expired { exp, now }) => {
                assert_eq!(exp, NOW + 60);
                assert_eq!(now, NOW + 61);
            }
            other => panic!("expected Expired, got {other:?}"),
        }
        // Exactly at expiry is expired — `<=`, not `<`. An off-by-one here
        // keeps a token alive for one more second than it promised.
        assert!(verify(&key().verifying_key(), &t, ISS, AUD, NOW + 60).is_err());
    }

    #[test]
    fn a_token_for_another_audience_does_not_authenticate_here() {
        // The entire reason bound tokens carry `aud`: a token minted for a
        // webhook must not work against the apiserver.
        let t = mint(&["https://vault.example"], NOW, 3600);
        match verify(&key().verifying_key(), &t, ISS, AUD, NOW + 1) {
            Err(TokenError::WrongAudience { got, want }) => {
                assert_eq!(got, vec!["https://vault.example".to_string()]);
                assert_eq!(want, AUD);
            }
            other => panic!("expected WrongAudience, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_audience_authenticates_nowhere_not_everywhere() {
        // The permissive reading would turn every token into a
        // cluster-wide credential the moment a minting bug dropped it.
        let t = mint(&[], NOW, 3600);
        assert!(matches!(
            verify(&key().verifying_key(), &t, ISS, AUD, NOW + 1),
            Err(TokenError::WrongAudience { .. })
        ));
    }

    #[test]
    fn a_token_from_another_issuer_is_refused() {
        let t = mint(&[AUD], NOW, 3600);
        assert!(matches!(
            verify(&key().verifying_key(), &t, "https://other", AUD, NOW + 1),
            Err(TokenError::WrongIssuer { .. })
        ));
    }

    #[test]
    fn a_token_signed_by_a_different_key_is_refused() {
        let t = mint(&[AUD], NOW, 3600);
        let other = SigningKey::from_bytes(&[8u8; 32]).verifying_key();
        assert_eq!(
            verify(&other, &t, ISS, AUD, NOW + 1),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn a_structurally_malformed_token_is_refused_not_panicked_on() {
        for bad in ["", "a", "a.b", "a.b.c.d", "not-base64.at.all"] {
            assert!(
                verify(&key().verifying_key(), bad, ISS, AUD, NOW).is_err(),
                "must refuse: {bad:?}"
            );
        }
    }

    #[test]
    fn the_group_set_includes_the_per_namespace_group() {
        // Many default RoleBindings key on it; emitting only the broad
        // group would silently strip permissions the cluster grants by
        // convention.
        let g = groups_for("kube-system");
        assert!(g.contains(&"system:serviceaccounts".to_string()));
        assert!(g.contains(&"system:serviceaccounts:kube-system".to_string()));
    }

    #[test]
    fn the_subject_prefix_is_the_one_rbac_matches_on() {
        // A typo here produces a token that authenticates as a principal no
        // RoleBinding names — an authenticated request that can do nothing,
        // which reads as a permissions bug rather than a typo.
        assert_eq!(
            subject_for("default", "builder"),
            "system:serviceaccount:default:builder"
        );
    }
}
