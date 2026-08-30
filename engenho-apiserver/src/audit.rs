//! AUDIT LOGGING — the record of who did what.
//!
//! ★ WHY ITS ABSENCE IS A COMPLIANCE GAP AND NOT A MISSING NICETY. Every
//! serious deployment has to answer "who deleted that Secret, and when".
//! Without an audit trail the answer is unavailable — not hard to find,
//! genuinely absent — and no amount of after-the-fact investigation
//! recovers it. It is also the one control that most compliance regimes
//! name explicitly, so a distribution without it is disqualified from
//! environments it is otherwise perfectly capable of running.
//!
//! ★ THE LEVELS ARE A PRIVACY MECHANISM, NOT A VERBOSITY KNOB. Upstream's
//! four levels exist because the request body of a Secret create CONTAINS
//! THE SECRET. `RequestResponse` on the wrong rule writes credentials into
//! a log that is, by design, shipped somewhere durable and widely read.
//! That is why the policy below defaults to `Metadata` and why raising a
//! rule to a body-capturing level is a deliberate act with a comment
//! attached, never a default.
//!
//! ★ AN AUDIT EVENT IS EMITTED EVEN WHEN THE REQUEST FAILED. A denied
//! delete is precisely the thing an investigator needs to see, and a log
//! that only records successes answers the wrong question. The
//! `responseStatus` carries the outcome.

use engenho_types::auth::UserInfo;
use serde::Serialize;
use serde_json::Value;

/// Upstream's audit levels, in increasing disclosure.
///
/// Ordered so `>=` comparisons are meaningful: a rule at `Metadata`
/// includes everything `Request` would minus the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Level {
    /// Do not log this request at all.
    None,
    /// Log who/what/when, but no request or response body.
    Metadata,
    /// Metadata plus the request body.
    Request,
    /// Metadata plus both bodies.
    RequestResponse,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Metadata => "Metadata",
            Self::Request => "Request",
            Self::RequestResponse => "RequestResponse",
        }
    }
}

/// The stage of request handling an event describes.
///
/// engenho emits `ResponseComplete`; the other stages exist upstream for
/// long-running requests (watches) and are named here so the field is a
/// closed set rather than a free string a consumer must guess at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Stage {
    RequestReceived,
    ResponseStarted,
    ResponseComplete,
    Panic,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::RequestReceived => "RequestReceived",
            Self::ResponseStarted => "ResponseStarted",
            Self::ResponseComplete => "ResponseComplete",
            Self::Panic => "Panic",
        }
    }
}

/// What is being audited.
#[derive(Debug, Clone)]
pub struct AuditRequest<'a> {
    pub verb: &'a str,
    pub group: &'a str,
    pub version: &'a str,
    pub resource: &'a str,
    pub namespace: Option<&'a str>,
    pub name: Option<&'a str>,
    pub user: &'a UserInfo,
    /// HTTP status the request produced.
    pub response_code: u16,
    /// Frozen at the boundary by the caller — the clock is not something
    /// this module should read, for the same reason the store's isn't.
    pub timestamp: &'a str,
    /// A per-request identifier. Ties an audit line to the request that
    /// produced it across every other log the server writes.
    pub audit_id: &'a str,
}

/// One policy rule.
#[derive(Debug, Clone)]
pub struct Rule {
    pub level: Level,
    /// Resources this rule matches. Empty ⇒ every resource.
    pub resources: Vec<String>,
    /// Verbs this rule matches. Empty ⇒ every verb.
    pub verbs: Vec<String>,
}

/// The audit policy: first matching rule wins, as upstream.
#[derive(Debug, Clone)]
pub struct Policy {
    pub rules: Vec<Rule>,
    /// Applied when no rule matches.
    ///
    /// `Metadata`, deliberately: a default of `None` would make an
    /// incomplete policy silently stop auditing the very requests nobody
    /// thought to write a rule for — which are exactly the interesting
    /// ones. A default of `RequestResponse` would leak bodies.
    pub default_level: Level,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            rules: vec![
                // Secrets and ServiceAccount tokens are capped at Metadata
                // NO MATTER WHAT a later rule says, because their request
                // body IS the credential. First-match-wins makes this a
                // real cap rather than a suggestion — it must stay first.
                Rule {
                    level: Level::Metadata,
                    resources: vec!["secrets".into(), "serviceaccounts/token".into()],
                    verbs: Vec::new(),
                },
                // Reads are high-volume and low-signal; auditing every GET
                // buries the writes an investigator is looking for.
                Rule {
                    level: Level::None,
                    resources: Vec::new(),
                    verbs: vec!["get".into(), "list".into(), "watch".into()],
                },
            ],
            default_level: Level::Metadata,
        }
    }
}

impl Policy {
    /// The level for one request — first matching rule wins.
    #[must_use]
    pub fn level_for(&self, req: &AuditRequest<'_>) -> Level {
        for rule in &self.rules {
            let resource_ok =
                rule.resources.is_empty() || rule.resources.iter().any(|r| r == req.resource);
            let verb_ok = rule.verbs.is_empty() || rule.verbs.iter().any(|v| v == req.verb);
            if resource_ok && verb_ok {
                return rule.level;
            }
        }
        self.default_level
    }
}

/// Render one audit event.
///
/// Returns `None` at `Level::None` — the caller must not write a line, and
/// returning an empty event would put an unlabelled record in the log.
#[must_use]
pub fn event(
    policy: &Policy,
    req: &AuditRequest<'_>,
    request_body: Option<&Value>,
    response_body: Option<&Value>,
) -> Option<Value> {
    let level = policy.level_for(req);
    if level == Level::None {
        return None;
    }

    let mut ev = serde_json::json!({
        "kind": "Event",
        "apiVersion": "audit.k8s.io/v1",
        "level": level.as_str(),
        "auditID": req.audit_id,
        "stage": Stage::ResponseComplete.as_str(),
        "verb": req.verb,
        "user": {
            "username": req.user.username,
            "groups": req.user.groups,
        },
        "objectRef": {
            "resource": req.resource,
            "apiGroup": req.group,
            "apiVersion": req.version,
            "namespace": req.namespace,
            "name": req.name,
        },
        // Present even when the request FAILED — a denied delete is
        // precisely what an investigator needs, and a log of successes
        // only answers the wrong question.
        "responseStatus": { "code": req.response_code },
        "requestReceivedTimestamp": req.timestamp,
        "stageTimestamp": req.timestamp,
    });

    if level >= Level::Request {
        if let Some(b) = request_body {
            ev["requestObject"] = b.clone();
        }
    }
    if level >= Level::RequestResponse {
        if let Some(b) = response_body {
            ev["responseObject"] = b.clone();
        }
    }
    Some(ev)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user() -> UserInfo {
        UserInfo {
            username: "alice".into(),
            uid: "u1".into(),
            groups: vec!["system:authenticated".into()],
            extra: std::collections::BTreeMap::new(),
        }
    }

    fn req<'a>(verb: &'a str, resource: &'a str, user: &'a UserInfo) -> AuditRequest<'a> {
        AuditRequest {
            verb,
            group: "",
            version: "v1",
            resource,
            namespace: Some("default"),
            name: Some("x"),
            user,
            response_code: 200,
            timestamp: "2026-08-29T21:00:00Z",
            audit_id: "aid-1",
        }
    }

    fn body() -> Value {
        serde_json::json!({ "data": { "password": "hunter2" } })
    }

    #[test]
    fn a_write_is_audited_by_default() {
        // Anti-vacuity: a policy that logged nothing would pass every
        // "sensitive data is not logged" test below.
        let u = user();
        let ev = event(&Policy::default(), &req("create", "pods", &u), None, None)
            .expect("a create must be audited");
        assert_eq!(ev["kind"], "Event");
        assert_eq!(ev["apiVersion"], "audit.k8s.io/v1");
        assert_eq!(ev["verb"], "create");
        assert_eq!(ev["user"]["username"], "alice");
        assert_eq!(ev["objectRef"]["resource"], "pods");
        assert_eq!(ev["auditID"], "aid-1");
    }

    #[test]
    fn a_secrets_body_is_never_written_even_at_a_body_capturing_level() {
        // The request body of a Secret create IS the secret. This is why
        // the cap rule is FIRST and why first-match-wins makes it a real
        // cap rather than a suggestion.
        let u = user();
        let policy = Policy {
            rules: {
                let mut r = Policy::default().rules;
                // A later rule that would capture everything — it must lose.
                r.push(Rule {
                    level: Level::RequestResponse,
                    resources: Vec::new(),
                    verbs: Vec::new(),
                });
                r
            },
            default_level: Level::RequestResponse,
        };
        let ev = event(
            &policy,
            &req("create", "secrets", &u),
            Some(&body()),
            Some(&body()),
        )
        .expect("still audited");
        assert_eq!(ev["level"], "Metadata");
        assert!(
            ev.get("requestObject").is_none(),
            "the secret body must never reach the log"
        );
        assert!(ev.get("responseObject").is_none());
    }

    #[test]
    fn the_token_subresource_is_capped_too() {
        // serviceaccounts/token returns a bearer token in its RESPONSE.
        let u = user();
        let ev = event(
            &Policy::default(),
            &req("create", "serviceaccounts/token", &u),
            Some(&body()),
            Some(&body()),
        )
        .expect("audited");
        assert_eq!(ev["level"], "Metadata");
        assert!(ev.get("responseObject").is_none());
    }

    #[test]
    fn reads_are_not_audited_because_they_bury_the_writes() {
        let u = user();
        for verb in ["get", "list", "watch"] {
            assert!(
                event(&Policy::default(), &req(verb, "pods", &u), None, None).is_none(),
                "{verb} must not produce a line"
            );
        }
    }

    #[test]
    fn a_failed_request_is_still_audited() {
        // A denied delete is precisely what an investigator needs; a log
        // of successes only answers the wrong question.
        let u = user();
        let mut r = req("delete", "secrets", &u);
        r.response_code = 403;
        let ev = event(&Policy::default(), &r, None, None).expect("must be audited");
        assert_eq!(ev["responseStatus"]["code"], 403);
        assert_eq!(ev["verb"], "delete");
    }

    #[test]
    fn bodies_appear_only_at_the_levels_that_permit_them() {
        let u = user();
        let capture_all = Policy {
            rules: Vec::new(),
            default_level: Level::RequestResponse,
        };
        let ev = event(
            &capture_all,
            &req("create", "pods", &u),
            Some(&body()),
            Some(&body()),
        )
        .expect("audited");
        assert!(ev.get("requestObject").is_some());
        assert!(ev.get("responseObject").is_some());

        let request_only = Policy {
            rules: Vec::new(),
            default_level: Level::Request,
        };
        let ev = event(
            &request_only,
            &req("create", "pods", &u),
            Some(&body()),
            Some(&body()),
        )
        .expect("audited");
        assert!(ev.get("requestObject").is_some());
        assert!(
            ev.get("responseObject").is_none(),
            "Request level must not capture the RESPONSE body"
        );
    }

    #[test]
    fn the_default_when_no_rule_matches_is_metadata_not_none() {
        // A default of None would silently stop auditing exactly the
        // requests nobody thought to write a rule for — the interesting
        // ones. A default of RequestResponse would leak bodies.
        assert_eq!(Policy::default().default_level, Level::Metadata);
        let u = user();
        // `patch` on an unlisted resource matches no rule.
        let ev = event(&Policy::default(), &req("patch", "widgets", &u), None, None)
            .expect("must fall through to the default, not to silence");
        assert_eq!(ev["level"], "Metadata");
    }

    #[test]
    fn levels_are_ordered_so_comparisons_mean_what_they_read() {
        assert!(Level::None < Level::Metadata);
        assert!(Level::Metadata < Level::Request);
        assert!(Level::Request < Level::RequestResponse);
    }
}
