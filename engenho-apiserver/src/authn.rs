//! Authenticator chain — the typed-spec interpreter that turns a request's
//! credentials into a server-side [`UserInfo`] identity.
//!
//! ## The triplet shape
//!
//! This module is the WORKING INTERPRETER half of the typed-spec triplet for
//! authentication. The MOCKABLE input is [`RequestCreds`] — a request can be
//! authenticated in a unit test with NO real TLS, NO real network: you hand
//! the chain a `RequestCreds { client_cert, bearer }` and it returns a typed
//! `Result<UserInfo, AuthnError>`. The [`Authenticator`] trait is the per-stage
//! interpreter; [`ChainAuthenticator`] runs the stages in order, first
//! `Some(UserInfo)` wins.
//!
//! ## Identity, not enforcement (this brick)
//!
//! The chain RESOLVES who a request is — it never DENIES on authz. Authorize-ALL
//! is retained: a no-credential request resolves to [`UserInfo::anonymous`]
//! (still allowed downstream). The ONLY way authn surfaces an error is a
//! malformed/typed-bad credential (e.g. a structurally-SA bearer token that
//! cannot be validated yet) — never a silent wrong identity.
//!
//! ## Stage order (first `Some` wins)
//!
//!   1. [`X509Authenticator`]        — a verified client cert ⇒ its CN/O identity.
//!   2. [`ServiceAccountTokenAuthenticator`] — typed-deferred (kubelet brick):
//!      a structurally-SA bearer ⇒ `Err(ServiceAccountUnsupported)`; a non-SA
//!      bearer ⇒ `None` (fall through). NEVER a silent SA identity.
//!   3. [`BootstrapAdminTokenAuthenticator`] — the configured admin token ⇒ admin.
//!   4. [`AnonymousAuthenticator`]   — terminal; always `Some(anonymous)`.

use engenho_types::auth::UserInfo;

use crate::pki::VerifiedClientCert;

/// Everything the authenticator chain can refuse on. A typed-bad credential
/// becomes a 401 Unauthorized at the middleware; it is NEVER turned into a
/// silent wrong identity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthnError {
    /// A bearer token is structurally a `ServiceAccount` JWT, but THIS SERVER
    /// holds no `ServiceAccount` signing key, so it cannot check the signature.
    ///
    /// The honest state for a server with no key. It is a refusal, never a
    /// fall-through: without the key the request is rejected rather than
    /// admitted as anonymous or as the claimed SA.
    #[error(
        "this apiserver holds no ServiceAccount signing key, so it cannot verify \
         ServiceAccount tokens"
    )]
    ServiceAccountUnsupported,

    /// A `ServiceAccount` token was CHECKED and REJECTED, with the reason.
    ///
    /// ── ★ WHY THIS IS SEPARATE FROM THE VARIANT ABOVE ────────────────────
    /// It used to not be. Every validation failure — a forged signature, an
    /// expired token, the wrong issuer, the wrong audience — was mapped onto
    /// `ServiceAccountUnsupported`, whose message read "service account token
    /// authentication is not yet supported". `sa_token::TokenError` goes to
    /// deliberate trouble to keep those five apart, stating in its own doc that
    /// "collapsing them makes an audit log useless for telling an attack from
    /// clock skew" — and this layer collapsed them one call later.
    ///
    /// The cost was not abstract. `Err(e) => ApiError::Unauthorized(e.to_string())`
    /// puts this text in the 401 body, so an operator whose token had merely
    /// EXPIRED was told the feature did not exist. That sends them to look for a
    /// missing capability instead of at their clock, their issuer, or an
    /// attacker — and it makes a forged-signature event indistinguishable from a
    /// routine expiry in the audit log, which is precisely the distinction the
    /// log exists to record.
    #[error("service account token rejected: {0}")]
    ServiceAccountRejected(#[from] crate::sa_token::TokenError),

    /// A `ServiceAccount` token verified, but carries no `kubernetes.io` claim
    /// block naming its namespace and `ServiceAccount`.
    ///
    /// Distinct from a rejection: the signature was GOOD, so this is a
    /// well-formed credential from a trusted issuer that does not say who it is
    /// for — a token-minting bug on our side, not a bad credential on theirs,
    /// and the two want different investigations.
    #[error(
        "service account token verified but carries no kubernetes.io claims naming its \
         namespace and service account"
    )]
    ServiceAccountClaimsMissing,
}

engenho_substrate::impl_error_kind! {
    AuthnError {
        ServiceAccountUnsupported => "service_account_unsupported",
        (ServiceAccountRejected(_)) => "service_account_rejected",
        ServiceAccountClaimsMissing => "service_account_claims_missing",
    }
}

/// The MOCKABLE authenticator input — the request material the chain reads,
/// with NO dependency on TLS/HTTP/network. A unit test constructs one
/// directly; the axum middleware extracts one from request extensions +
/// headers. This is the testability contract of the typed-spec triplet.
#[derive(Debug, Clone, Default)]
pub struct RequestCreds {
    /// The verified peer client cert, if the TLS acceptor injected one.
    pub client_cert: Option<VerifiedClientCert>,
    /// The `Authorization: Bearer <token>` value, if present.
    pub bearer: Option<String>,
}

/// One authenticator stage. Returns `Ok(Some(_))` to claim the request,
/// `Ok(None)` to decline (fall through to the next stage), or `Err(_)` for a
/// typed-bad credential (→ 401, never a silent wrong identity).
pub trait Authenticator: Send + Sync {
    /// Try to authenticate `creds`.
    ///
    /// # Errors
    ///
    /// An [`AuthnError`] for a credential that is recognized-but-unvalidatable
    /// (e.g. a structurally-SA bearer this brick can't verify).
    fn authenticate(&self, creds: &RequestCreds) -> Result<Option<UserInfo>, AuthnError>;
}

/// Stage 1 — X509 client-cert authenticator. If the TLS acceptor verified +
/// injected a [`VerifiedClientCert`], map its CN + O to a `UserInfo`.
#[derive(Debug, Default, Clone, Copy)]
pub struct X509Authenticator;

impl Authenticator for X509Authenticator {
    fn authenticate(&self, creds: &RequestCreds) -> Result<Option<UserInfo>, AuthnError> {
        match &creds.client_cert {
            Some(cert) => Ok(Some(UserInfo::from_client_cert(
                &cert.common_name,
                &cert.organizations,
            ))),
            None => Ok(None),
        }
    }
}

/// Stage 2 — ServiceAccount-token authenticator. **Typed-deferred** — SA-token
/// validation (issuer keypair, JWKS, the TokenRequest API + kubelet-side token
/// projection into pods) is tied to the kubelet projection brick, OUT OF SCOPE
/// here.
///
// BRICK: SA-token validation tied to kubelet projection. This stage's
// `authenticate` does NOT validate SA JWTs yet (no issuer keypair, no
// TokenRequest API). Its typed-deferred behavior:
//   * a bearer that IS structurally an SA token (JWT with SA-shaped claims)
//     but cannot be validated → Err(ServiceAccountUnsupported) → typed 401.
//   * a bearer that is NOT SA-shaped → None (falls through to the admin/anon
//     stages, so the placeholder ANONYMOUS_TOKEN keeps authenticating as
//     anonymous). NEVER a silent admin / silent SA identity.
#[derive(Debug, Default, Clone)]
pub struct ServiceAccountTokenAuthenticator {
    /// The verifying half of the cluster's SA signing keypair, plus the
    /// issuer and audience a token must claim.
    ///
    /// `None` keeps the pre-existing typed-deferred behaviour: an SA-shaped
    /// bearer is a typed 401 rather than a silent anonymous. That is the
    /// honest state for a server with no key, and it is NEVER a fallback
    /// that authenticates — refusing to validate must not become refusing
    /// to reject.
    key: Option<SaVerifier>,
}

/// What the authenticator needs to check an SA token.
#[derive(Clone)]
pub struct SaVerifier {
    /// Public half of the cluster's SA signing key.
    pub verifying: ed25519_dalek::VerifyingKey,
    /// The `iss` a token must carry.
    pub issuer: String,
    /// The audience this API server accepts.
    pub audience: String,
}

impl std::fmt::Debug for SaVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaVerifier")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .finish_non_exhaustive()
    }
}

impl ServiceAccountTokenAuthenticator {
    /// An authenticator that can actually VALIDATE tokens.
    #[must_use]
    pub fn with_key(
        verifying: ed25519_dalek::VerifyingKey,
        issuer: String,
        audience: String,
    ) -> Self {
        Self {
            key: Some(SaVerifier {
                verifying,
                issuer,
                audience,
            }),
        }
    }
}

impl Authenticator for ServiceAccountTokenAuthenticator {
    fn authenticate(&self, creds: &RequestCreds) -> Result<Option<UserInfo>, AuthnError> {
        let Some(bearer) = &creds.bearer else {
            return Ok(None);
        };
        if !is_service_account_token(bearer) {
            // Not SA-shaped (opaque token / placeholder) → fall through.
            return Ok(None);
        }
        let Some(v) = &self.key else {
            // SA-shaped but this server holds no key → typed 401, NEVER a
            // silent anonymous-as-that-SA or a silent admin.
            return Err(AuthnError::ServiceAccountUnsupported);
        };

        // A structurally-SA bearer that FAILS validation is a typed 401, not
        // a fall-through. Falling through would hand a forged or expired
        // token the anonymous identity and answer 200 for whatever anonymous
        // may do — the silent-downgrade this stage exists to prevent.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        // The reason travels. `?` on a `TokenError` becomes
        // `ServiceAccountRejected(reason)` via `#[from]`, so the 401 body names
        // whether the token expired, was forged, or was minted for a different
        // audience — see that variant's doc for why discarding this was costly.
        let claims = crate::sa_token::verify(&v.verifying, bearer, &v.issuer, &v.audience, now)?;

        let (namespace, name) = claims
            .kubernetes
            .as_ref()
            .map(|k| (k.namespace.clone(), k.serviceaccount.name.clone()))
            .ok_or(AuthnError::ServiceAccountClaimsMissing)?;

        Ok(Some(UserInfo {
            username: crate::sa_token::subject_for(&namespace, &name),
            uid: claims
                .kubernetes
                .as_ref()
                .map(|k| k.serviceaccount.uid.clone())
                .unwrap_or_default(),
            groups: crate::sa_token::groups_for(&namespace),
            extra: std::collections::BTreeMap::new(),
        }))
    }
}

/// Stage 3 — bootstrap admin bearer-token authenticator. If a token is
/// configured AND the request's bearer matches it exactly, the request is the
/// admin identity.
pub struct BootstrapAdminTokenAuthenticator {
    /// The configured admin token, or `None` (no admin token ⇒ this stage
    /// always declines). Compared by exact equality.
    token: Option<String>,
}

impl BootstrapAdminTokenAuthenticator {
    /// New stage with the configured admin token (`None` ⇒ always declines).
    #[must_use]
    pub fn new(token: Option<String>) -> Self {
        Self { token }
    }
}

impl Authenticator for BootstrapAdminTokenAuthenticator {
    fn authenticate(&self, creds: &RequestCreds) -> Result<Option<UserInfo>, AuthnError> {
        match (&self.token, &creds.bearer) {
            (Some(configured), Some(presented)) if configured == presented => {
                Ok(Some(UserInfo::admin()))
            }
            _ => Ok(None),
        }
    }
}

/// Stage 4 — anonymous authenticator. TERMINAL: always claims the request as
/// [`UserInfo::anonymous`], so the chain never empty-fails. Authorize-ALL is
/// retained, so an anonymous request still proceeds.
#[derive(Debug, Default, Clone, Copy)]
pub struct AnonymousAuthenticator;

impl Authenticator for AnonymousAuthenticator {
    fn authenticate(&self, _creds: &RequestCreds) -> Result<Option<UserInfo>, AuthnError> {
        Ok(Some(UserInfo::anonymous()))
    }
}

/// The typed authenticator chain. Runs its stages in order; the first stage
/// returning `Ok(Some(UserInfo))` wins. A stage's `Err` short-circuits the
/// chain (a typed-bad credential is terminal — a 401, not a fall-through).
/// The terminal [`AnonymousAuthenticator`] guarantees the chain never returns
/// "no identity" — the worst case is `UserInfo::anonymous()`.
pub struct ChainAuthenticator {
    stages: Vec<Box<dyn Authenticator>>,
}

impl ChainAuthenticator {
    /// Build the canonical engenho chain: X509 → SA-token (typed-deferred) →
    /// bootstrap-admin-token → anonymous. `admin_token` is the configured
    /// bootstrap admin bearer (`None` ⇒ no token authenticates as admin).
    #[must_use]
    pub fn bootstrap(admin_token: Option<String>) -> Self {
        Self {
            stages: vec![
                Box::new(X509Authenticator),
                Box::new(ServiceAccountTokenAuthenticator::default()),
                Box::new(BootstrapAdminTokenAuthenticator::new(admin_token)),
                Box::new(AnonymousAuthenticator),
            ],
        }
    }

    /// The canonical chain WITH a live SA verifier, so in-cluster clients
    /// authenticate as their ServiceAccount instead of taking a typed 401.
    #[must_use]
    pub fn bootstrap_with_sa(
        admin_token: Option<String>,
        verifying: ed25519_dalek::VerifyingKey,
        issuer: String,
        audience: String,
    ) -> Self {
        Self {
            stages: vec![
                Box::new(X509Authenticator),
                Box::new(ServiceAccountTokenAuthenticator::with_key(
                    verifying, issuer, audience,
                )),
                Box::new(BootstrapAdminTokenAuthenticator::new(admin_token)),
                Box::new(AnonymousAuthenticator),
            ],
        }
    }

    /// Build a chain from explicit stages (test seam + future composition).
    #[must_use]
    pub fn from_stages(stages: Vec<Box<dyn Authenticator>>) -> Self {
        Self { stages }
    }

    /// Run the chain. The terminal anonymous stage means a `None` is never the
    /// final answer — a credential-less request resolves to anonymous.
    ///
    /// # Errors
    ///
    /// The first stage's [`AuthnError`] (a typed-bad credential). Because the
    /// chain is ordered X509 → SA → admin → anonymous, the only error this
    /// brick produces is [`AuthnError::ServiceAccountUnsupported`].
    pub fn authenticate(&self, creds: &RequestCreds) -> Result<UserInfo, AuthnError> {
        for stage in &self.stages {
            match stage.authenticate(creds)? {
                Some(user) => return Ok(user),
                None => continue,
            }
        }
        // Unreachable in the canonical chain (the terminal AnonymousAuthenticator
        // always claims), but kept total: an empty/custom chain that declined
        // everything resolves to anonymous rather than a silent failure.
        Ok(UserInfo::anonymous())
    }
}

/// Heuristic: is `token` STRUCTURALLY a ServiceAccount JWT? A JWT is three
/// base64url segments separated by dots; an SA token's payload carries a `sub`
/// of the form `system:serviceaccount:<ns>:<name>` (the upstream SA subject)
/// OR a `kubernetes.io` issuer claim. We decode the payload WITHOUT verifying
/// the signature — the point is only to distinguish "this is an SA token I
/// can't validate yet" (→ typed 401) from "an opaque token" (→ fall through).
///
/// Conservative by construction: anything that doesn't decode to a JSON object
/// with the SA-shaped claim is treated as NOT-SA (falls through to anonymous),
/// so the placeholder `ANONYMOUS_TOKEN` + any opaque token keep working.
fn is_service_account_token(token: &str) -> bool {
    let mut parts = token.split('.');
    let (Some(_header), Some(payload), Some(_sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        // Not a three-segment JWT → not an SA token.
        return false;
    };
    let Some(claims) = decode_jwt_payload(payload) else {
        return false;
    };
    let sub_is_sa = claims
        .get("sub")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s.starts_with("system:serviceaccount:"));
    let iss_is_k8s = claims
        .get("iss")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s.contains("kubernetes"));
    let has_k8s_sa_claim = claims.get("kubernetes.io").is_some();
    sub_is_sa || iss_is_k8s || has_k8s_sa_claim
}

/// Base64url-decode a JWT payload segment into a JSON object. Returns `None`
/// when the segment isn't valid base64url-JSON (→ treated as not-SA).
fn decode_jwt_payload(payload: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.as_object().cloned()
}

#[cfg(test)]
mod sa_verification_tests {
    use super::{Authenticator, AuthnError, RequestCreds, ServiceAccountTokenAuthenticator};
    use crate::sa_token::{issue, load_or_generate_sa_key};

    const ISS: &str = "https://kubernetes.default.svc";
    const AUD: &str = "https://kubernetes.default.svc";

    /// Wall-clock seconds. `authenticate` reads the real clock (no seam
    /// yet), so a token must be issued against the same clock or it is
    /// expired before it is ever checked.
    fn now_secs() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_secs(),
        )
        .expect("fits i64")
    }

    fn creds(bearer: &str) -> RequestCreds {
        RequestCreds {
            bearer: Some(bearer.to_string()),
            ..Default::default()
        }
    }

    /// A VALID token authenticates as its ServiceAccount, with upstream's
    /// `system:serviceaccount:<ns>:<name>` username and group set.
    #[test]
    fn a_valid_token_authenticates_as_its_service_account() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let tok = issue(
            &kp.signing,
            ISS,
            "pangea-system",
            "pangea-operator",
            "uid-7",
            &[AUD.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("issue");

        let a = ServiceAccountTokenAuthenticator::with_key(
            kp.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );
        let user = a
            .authenticate(&creds(&tok))
            .expect("a valid token is not an error")
            .expect("a valid token authenticates");
        assert_eq!(
            user.username,
            "system:serviceaccount:pangea-system:pangea-operator"
        );
        assert_eq!(user.uid, "uid-7");
        assert!(
            user.groups.iter().any(|g| g == "system:serviceaccounts"),
            "{:?}",
            user.groups
        );
    }

    /// ★ THE ONE THAT MATTERS. A forged or expired token is a typed 401 —
    /// NEVER a fall-through to anonymous.
    ///
    /// Falling through would hand a bad token the anonymous identity and
    /// answer 200 for whatever anonymous may do. That silent downgrade is
    /// the entire reason this stage exists, and it is the failure a
    /// "return Ok(None) on error" would quietly introduce.
    #[test]
    fn a_forged_or_expired_token_is_rejected_never_downgraded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let other = load_or_generate_sa_key(tempfile::tempdir().expect("tempdir2").path())
            .expect("second key");

        let a = ServiceAccountTokenAuthenticator::with_key(
            kp.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );

        // Signed by a DIFFERENT cluster's key.
        let forged = issue(
            &other.signing,
            ISS,
            "kube-system",
            "admin",
            "uid-x",
            &[AUD.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("issue");
        assert!(
            a.authenticate(&creds(&forged)).is_err(),
            "a token signed by another key must be REJECTED, not downgraded"
        );

        // Correctly signed but for the wrong audience.
        let wrong_aud = issue(
            &kp.signing,
            ISS,
            "kube-system",
            "admin",
            "uid-y",
            &["some-other-audience".to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("issue");
        assert!(
            a.authenticate(&creds(&wrong_aud)).is_err(),
            "an audience mismatch must be rejected"
        );
    }

    /// An EXPIRED token is rejected. Mandatory expiry is the whole point of
    /// a bound token — a token that outlives its pod is a credential nobody
    /// can revoke.
    #[test]
    fn an_expired_token_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        // Issued two hours ago with a one-hour life.
        let tok = issue(
            &kp.signing,
            ISS,
            "ns",
            "sa",
            "uid",
            &[AUD.to_string()],
            None,
            now_secs() - 7200,
            3600,
        )
        .expect("issue");
        let a = ServiceAccountTokenAuthenticator::with_key(
            kp.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );
        assert!(
            a.authenticate(&creds(&tok)).is_err(),
            "an expired token must be rejected, never downgraded to anonymous"
        );
    }

    /// With NO key the stage keeps its typed-deferred behaviour: an SA-shaped
    /// bearer is a 401, never a silent anonymous. Refusing to VALIDATE must
    /// not become refusing to REJECT.
    #[test]
    fn without_a_key_an_sa_bearer_is_still_refused() {
        let a = ServiceAccountTokenAuthenticator::default();
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let tok = issue(
            &kp.signing,
            ISS,
            "ns",
            "sa",
            "uid",
            &[AUD.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("issue");
        assert!(a.authenticate(&creds(&tok)).is_err());
    }

    /// A non-SA bearer still falls THROUGH, so the admin and anonymous
    /// stages behind this one keep working.
    #[test]
    fn a_non_sa_bearer_falls_through() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let a = ServiceAccountTokenAuthenticator::with_key(
            kp.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );
        assert_eq!(
            a.authenticate(&creds("an-opaque-admin-token"))
                .expect("not an error"),
            None,
            "a non-SA bearer must fall through to the later stages"
        );
    }

    // ── The rejection REASON survives to the client ──────────────────────
    //
    // These pin the split of `ServiceAccountUnsupported` into three variants.
    // Every one of these cases used to answer "service account token
    // authentication is not yet supported", which sent an operator looking for
    // a missing feature instead of at the actual fault — and made a forged
    // signature indistinguishable from a routine expiry in the audit log.

    #[test]
    fn an_expired_token_says_expired_not_unsupported() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let auth = ServiceAccountTokenAuthenticator::with_key(
            kp.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );
        // Issued two hours ago with a one-hour life.
        let token = issue(
            &kp.signing,
            ISS,
            "default",
            "sa",
            "uid",
            &[AUD.to_string()],
            None,
            now_secs() - 7200,
            3600,
        )
        .expect("mint");

        let err = auth
            .authenticate(&RequestCreds {
                client_cert: None,
                bearer: Some(token),
            })
            .expect_err("an expired token must be refused");

        assert!(
            matches!(
                err,
                AuthnError::ServiceAccountRejected(crate::sa_token::TokenError::Expired { .. })
            ),
            "expiry must reach the caller as an expiry; got {err:?}"
        );
        let rendered = err.to_string();
        assert!(
            rendered.contains("expired"),
            "the 401 body is this string — it must name the expiry: {rendered}"
        );
        assert!(
            !rendered.contains("not yet supported"),
            "an expired token must NOT report a missing feature: {rendered}"
        );
    }

    #[test]
    fn a_token_signed_by_a_stranger_says_signature_not_unsupported() {
        let ours = tempfile::tempdir().expect("tempdir");
        let theirs = tempfile::tempdir().expect("tempdir");
        let our_key = load_or_generate_sa_key(ours.path()).expect("key");
        let their_key = load_or_generate_sa_key(theirs.path()).expect("key");

        // Distinct data dirs must yield distinct keys, or the forgery case is
        // untestable and — far worse — any cluster could mint tokens for any
        // other. Assert it rather than assume it.
        assert_ne!(
            our_key.verifying.to_bytes(),
            their_key.verifying.to_bytes(),
            "two clusters must not share a ServiceAccount signing key"
        );

        let auth = ServiceAccountTokenAuthenticator::with_key(
            our_key.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );
        let forged = issue(
            &their_key.signing,
            ISS,
            "default",
            "sa",
            "uid",
            &[AUD.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("mint");

        let err = auth
            .authenticate(&RequestCreds {
                client_cert: None,
                bearer: Some(forged),
            })
            .expect_err("a token from another key must be refused");

        assert!(
            matches!(
                err,
                AuthnError::ServiceAccountRejected(crate::sa_token::TokenError::BadSignature)
            ),
            "a foreign signature is a SECURITY EVENT and must be recorded as one, \
             not as a capability gap; got {err:?}"
        );
    }

    #[test]
    fn a_token_for_another_audience_says_audience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let auth = ServiceAccountTokenAuthenticator::with_key(
            kp.verifying,
            ISS.to_string(),
            AUD.to_string(),
        );
        // The reason bound tokens carry `aud` at all: a token minted for a
        // webhook must not authenticate against the apiserver.
        let token = issue(
            &kp.signing,
            ISS,
            "default",
            "sa",
            "uid",
            &["https://some-webhook.example".to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("mint");

        let err = auth
            .authenticate(&RequestCreds {
                client_cert: None,
                bearer: Some(token),
            })
            .expect_err("a foreign-audience token must be refused");

        assert!(
            matches!(
                err,
                AuthnError::ServiceAccountRejected(
                    crate::sa_token::TokenError::WrongAudience { .. }
                )
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn only_a_keyless_server_reports_unsupported() {
        // The one case where "unsupported" is the truth. Keeping it distinct is
        // what makes the message trustworthy again: an operator who sees it now
        // knows the server genuinely has no key, rather than having to wonder
        // whether their token was simply stale.
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = load_or_generate_sa_key(dir.path()).expect("key");
        let token = issue(
            &kp.signing,
            ISS,
            "default",
            "sa",
            "uid",
            &[AUD.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("mint");

        let keyless = ServiceAccountTokenAuthenticator::default();
        let err = keyless
            .authenticate(&RequestCreds {
                client_cert: None,
                bearer: Some(token),
            })
            .expect_err("no key means refuse");

        assert_eq!(err, AuthnError::ServiceAccountUnsupported);
        assert!(
            err.to_string().contains("no ServiceAccount signing key"),
            "the message must say what is actually missing: {err}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_cert() -> VerifiedClientCert {
        VerifiedClientCert {
            common_name: "engenho-admin".to_string(),
            organizations: vec!["system:masters".to_string()],
        }
    }

    fn sa_token() -> String {
        // A structurally-SA JWT: header.payload.sig with an SA-shaped `sub`.
        use base64::Engine as _;
        let b64 = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes());
        let header = b64(r#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = b64(
            r#"{"iss":"https://kubernetes.default.svc","sub":"system:serviceaccount:default:builder"}"#,
        );
        let sig = b64("not-a-real-signature");
        [header, payload, sig].join(".")
    }

    #[test]
    fn x509_client_cert_authenticates_as_its_cn_and_orgs() {
        let chain = ChainAuthenticator::bootstrap(Some("admin-tok".into()));
        let creds = RequestCreds {
            client_cert: Some(admin_cert()),
            bearer: None,
        };
        let user = chain.authenticate(&creds).unwrap();
        assert_eq!(user.username, "engenho-admin");
        assert!(
            user.groups.iter().any(|g| g == "system:masters"),
            "X509 O=system:masters → group system:masters; got {:?}",
            user.groups
        );
        assert!(user.groups.iter().any(|g| g == "system:authenticated"));
    }

    #[test]
    fn admin_bearer_token_authenticates_as_admin() {
        let chain = ChainAuthenticator::bootstrap(Some("admin-tok".into()));
        let creds = RequestCreds {
            client_cert: None,
            bearer: Some("admin-tok".to_string()),
        };
        let user = chain.authenticate(&creds).unwrap();
        assert_eq!(user, UserInfo::admin());
    }

    #[test]
    fn no_credentials_authenticate_as_anonymous() {
        let chain = ChainAuthenticator::bootstrap(Some("admin-tok".into()));
        let user = chain.authenticate(&RequestCreds::default()).unwrap();
        assert_eq!(user.username, "system:anonymous");
        assert!(user.groups.iter().any(|g| g == "system:unauthenticated"));
    }

    #[test]
    fn structurally_sa_bearer_is_typed_unauthorized() {
        // A structurally-SA token → typed Err (→ 401), NEVER admin / anonymous.
        let chain = ChainAuthenticator::bootstrap(Some("admin-tok".into()));
        let creds = RequestCreds {
            client_cert: None,
            bearer: Some(sa_token()),
        };
        let err = chain.authenticate(&creds).unwrap_err();
        assert_eq!(err, AuthnError::ServiceAccountUnsupported);
    }

    #[test]
    fn opaque_non_admin_bearer_falls_through_to_anonymous() {
        // The placeholder ANONYMOUS_TOKEN (and any opaque non-admin token) is
        // NOT SA-shaped → falls through the SA stage → not the admin token →
        // anonymous. This keeps the existing anonymous-kubeconfig working.
        let chain = ChainAuthenticator::bootstrap(Some("admin-tok".into()));
        let creds = RequestCreds {
            client_cert: None,
            bearer: Some("engenho-anonymous".to_string()),
        };
        let user = chain.authenticate(&creds).unwrap();
        assert_eq!(user.username, "system:anonymous");
    }

    #[test]
    fn client_cert_wins_over_bearer() {
        // Stage order: X509 is first, so a request carrying BOTH a cert and a
        // bearer authenticates by the cert.
        let chain = ChainAuthenticator::bootstrap(Some("admin-tok".into()));
        let creds = RequestCreds {
            client_cert: Some(admin_cert()),
            bearer: Some("admin-tok".to_string()),
        };
        let user = chain.authenticate(&creds).unwrap();
        assert_eq!(user.username, "engenho-admin");
    }

    #[test]
    fn no_admin_token_configured_means_admin_bearer_is_just_anonymous() {
        // With no configured admin token, a random opaque bearer is anonymous
        // (the admin stage always declines). Proves the token is the gate.
        let chain = ChainAuthenticator::bootstrap(None);
        let creds = RequestCreds {
            client_cert: None,
            bearer: Some("admin-tok".to_string()),
        };
        let user = chain.authenticate(&creds).unwrap();
        assert_eq!(user.username, "system:anonymous");
    }

    #[test]
    fn is_service_account_token_classifies_correctly() {
        assert!(is_service_account_token(&sa_token()), "SA JWT detected");
        assert!(
            !is_service_account_token("engenho-anonymous"),
            "opaque token is NOT SA"
        );
        assert!(
            !is_service_account_token("a.b.c"),
            "non-JSON segments are NOT SA"
        );
        assert!(
            !is_service_account_token("two.segments"),
            "two-segment string is NOT a JWT"
        );
    }

    #[test]
    fn err_kind_is_stable() {
        // The inherent `.kind()` the impl_error_kind! macro generates.
        assert_eq!(
            AuthnError::ServiceAccountUnsupported.kind(),
            "service_account_unsupported"
        );
    }
}
