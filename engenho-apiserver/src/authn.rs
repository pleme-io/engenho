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
    /// A bearer token is structurally a ServiceAccount JWT (carries SA-shaped
    /// `sub` / `iss` claims) but engenho cannot validate it yet — SA-token
    /// validation (issuer keypair + JWKS + TokenRequest API) is tied to the
    /// kubelet projection brick. The middleware renders a typed 401
    /// Unauthorized; it does NOT silently authenticate the request as that SA
    /// nor as anonymous.
    #[error("service account token authentication is not yet supported")]
    ServiceAccountUnsupported,
}

engenho_substrate::impl_error_kind! {
    AuthnError {
        ServiceAccountUnsupported => "service_account_unsupported",
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
#[derive(Debug, Default, Clone, Copy)]
pub struct ServiceAccountTokenAuthenticator;

impl Authenticator for ServiceAccountTokenAuthenticator {
    fn authenticate(&self, creds: &RequestCreds) -> Result<Option<UserInfo>, AuthnError> {
        let Some(bearer) = &creds.bearer else {
            return Ok(None);
        };
        if is_service_account_token(bearer) {
            // Recognized as an SA token but unvalidatable this brick → typed
            // 401, NEVER a silent anonymous-as-that-SA or a silent admin.
            Err(AuthnError::ServiceAccountUnsupported)
        } else {
            // Not SA-shaped (opaque token / placeholder) → fall through.
            Ok(None)
        }
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
                Box::new(ServiceAccountTokenAuthenticator),
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
