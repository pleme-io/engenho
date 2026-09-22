//! One request, from bytes to the [`EngenhoControl`] method that answers it.
//!
//! Every step is driven by the spec's catalog, never by a hand-kept table:
//! [`OperationId::route`] names the operation, its catalog row says what
//! authority it needs, and [`engenho_control_types::visit`] dispatches it to
//! its typed request, its method and its response — one exhaustive, generated
//! match, so an operation added to the spec is served (or refused as
//! unsupported, by the implementation) the moment it exists.

use std::sync::Arc;

use bytes::Bytes;
use engenho_control_types::types::{BlindReason, RefusalReason};
use engenho_control_types::wire::{HttpParts, OperationRequest};
use engenho_control_types::{
    AuthorityTier, BoxFuture, ControlError, EngenhoControl, MediaType, Operation, OperationId,
    OperationVisitor, Principal, visit,
};

use crate::audit::{Audit, AuditEntry};
use crate::grant::{ACTOR_HEADER, CEILING_HEADER, GrantPolicy, Peer, mint};

/// The largest request body accepted.
pub const MAX_BODY: usize = 1024 * 1024;

/// A response, ready for the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// The status.
    pub status: u16,
    /// The body's `Content-Type`.
    pub content_type: &'static str,
    /// The body.
    pub body: Bytes,
}

impl Answer {
    /// The answer for an error body.
    #[must_use]
    pub fn error(err: &ControlError) -> Self {
        let body = match err {
            ControlError::Refused(r) => serde_json::to_vec(r),
            ControlError::Blind(b) => serde_json::to_vec(b),
        }
        .unwrap_or_default();
        Self {
            status: err.status(),
            content_type: MediaType::Json.content_type(),
            body: Bytes::from(body),
        }
    }
}

/// A request as the transport received it.
#[derive(Debug, Clone, Default)]
pub struct Incoming {
    /// The method.
    pub method: String,
    /// The path, undecoded.
    pub path: String,
    /// The raw query string.
    pub query: Option<String>,
    /// Every header.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Bytes,
}

/// Routes requests to one [`EngenhoControl`].
pub struct Router {
    ctl: Arc<dyn EngenhoControl>,
    policy: GrantPolicy,
    audit: Arc<dyn Audit>,
}

impl Router {
    /// A router over `ctl`, granting local callers by `policy` and recording
    /// through `audit`.
    #[must_use]
    pub fn new(ctl: Arc<dyn EngenhoControl>, policy: GrantPolicy, audit: Arc<dyn Audit>) -> Self {
        Self { ctl, policy, audit }
    }

    /// The grant policy.
    #[must_use]
    pub const fn policy(&self) -> &GrantPolicy {
        &self.policy
    }

    /// What `peer` is granted, and what its transport attested — or why it
    /// gets nothing.
    fn authenticate(
        &self,
        peer: Option<Peer>,
    ) -> Result<(AuthorityTier, engenho_control_types::types::AttestedView), ControlError> {
        let Some(peer) = peer else {
            return Err(ControlError::refused(
                RefusalReason::InsufficientAuthority,
                "the transport did not say who is calling",
            ));
        };
        let Some(granted) = self.policy.grant_peer(&peer) else {
            let who = match &peer {
                Peer::Local(local) => format!("uid {} may not use this socket", local.uid),
                Peer::Remote(client) => format!("{} may not use this listener", client.name),
            };
            return Err(ControlError::refused_with(
                RefusalReason::InsufficientAuthority,
                who,
                vec!["run as the daemon's user, or as root".into()],
            ));
        };
        let attested = peer
            .attested()
            .map_err(|why| ControlError::blind(BlindReason::Internal, why))?;
        Ok((granted, attested))
    }

    /// Answer one request from `peer` (`None`: the transport could not say
    /// who it is).
    pub async fn handle(&self, peer: Option<Peer>, req: Incoming) -> Answer {
        let Some((id, path_params)) = OperationId::route(&req.method, &req.path) else {
            return Answer::error(&ControlError::refused_with(
                RefusalReason::UnknownOperation,
                format!("no operation is {} {}", req.method, req.path),
                vec!["GET /v1/hello".into(), "GET /v1/spec".into()],
            ));
        };
        let (granted, attested) = match self.authenticate(peer) {
            Ok(caller) => caller,
            Err(refused) => return Answer::error(&refused),
        };
        let header = |name: &str| {
            req.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        let by = mint(
            attested,
            granted,
            header(ACTOR_HEADER),
            header(CEILING_HEADER),
        );
        let row = id.spec();
        if by.effective() < row.tier {
            let refusal = ControlError::refused_with(
                RefusalReason::InsufficientAuthority,
                format!(
                    "{} needs {} authority; this caller has {}",
                    id.as_str(),
                    row.tier,
                    by.effective()
                ),
                legal_for(row.tier, granted),
            );
            let answer = Answer::error(&refusal);
            self.audit
                .record(AuditEntry::refused(&by, id, answer.status));
            return answer;
        }
        let body = if req.body.is_empty() {
            None
        } else {
            match serde_json::from_slice(&req.body) {
                Ok(v) => Some(v),
                Err(err) => {
                    return Answer::error(&ControlError::refused(
                        RefusalReason::InvalidValue,
                        format!("the body is not JSON: {err}"),
                    ));
                }
            }
        };
        let parts = HttpParts {
            path_params,
            query: parse_query(req.query.as_deref()),
            headers: req.headers,
            body,
        };
        // Recorded before the call runs, so a crash mid-call leaves the
        // intent standing.
        let intent = self
            .audit
            .wants(&by, id)
            .then(|| AuditEntry::intent(&by, id, &parts));
        if let Some(intent) = &intent {
            self.audit.record(intent.clone());
        }
        let answer = visit(
            id,
            Dispatch {
                ctl: Arc::clone(&self.ctl),
                by: by.clone(),
                parts,
            },
        )
        .await;
        if let Some(intent) = intent {
            self.audit.record(intent.result(answer.status));
        }
        answer
    }
}

/// What would be accepted instead, for a caller short of `needed`.
fn legal_for(needed: AuthorityTier, granted: AuthorityTier) -> Vec<String> {
    if granted >= needed {
        // Only the caller's own ceiling stood in the way.
        vec![format!(
            "drop the Engenho-Ceiling header, or raise it to {needed}"
        )]
    } else {
        vec![format!(
            "run as the daemon's user or root (sudo engenho ctl …) for {needed} operations"
        )]
    }
}

/// `a=b&c=d`, percent-decoded; a pair that does not decode is dropped.
fn parse_query(query: Option<&str>) -> Vec<(String, String)> {
    use engenho_control_types::wire::decode_segment;
    query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            Some((decode_segment(k)?, decode_segment(v)?))
        })
        .collect()
}

/// One operation, dispatched by its marker type.
struct Dispatch {
    ctl: Arc<dyn EngenhoControl>,
    by: Principal,
    parts: HttpParts,
}

impl OperationVisitor for Dispatch {
    type Output = BoxFuture<'static, Answer>;

    fn visit<O: Operation>(self) -> Self::Output {
        Box::pin(async move {
            let req = match O::Request::from_http(&self.parts) {
                Ok(req) => req,
                Err(bad) => {
                    return Answer::error(&ControlError::refused(
                        RefusalReason::InvalidValue,
                        bad.to_string(),
                    ));
                }
            };
            match O::invoke(&*self.ctl, &self.by, req).await {
                Ok(resp) => success::<O>(&resp),
                Err(err) => Answer::error(&err),
            }
        })
    }
}

fn success<O: Operation>(resp: &O::Response) -> Answer {
    let row = O::ID.spec();
    let body = match row.response_media {
        MediaType::Json => serde_json::to_vec(resp).map(Bytes::from),
        MediaType::Yaml => serde_json::to_value(resp).map(|v| match v {
            serde_json::Value::String(text) => Bytes::from(text),
            other => Bytes::from(other.to_string()),
        }),
    };
    match body {
        Ok(body) => Answer {
            status: row.success_status,
            content_type: row.response_media.content_type(),
            body,
        },
        Err(err) => Answer::error(&ControlError::blind(
            BlindReason::Internal,
            format!("the response did not serialize: {err}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_string_is_split_and_decoded() {
        assert_eq!(
            parse_query(Some("after=5&limit=10&x=a%20b&=&bad=%zz")),
            vec![
                ("after".to_string(), "5".to_string()),
                ("limit".to_string(), "10".to_string()),
                ("x".to_string(), "a b".to_string()),
                (String::new(), String::new()),
            ]
        );
        assert!(parse_query(None).is_empty());
    }
}
