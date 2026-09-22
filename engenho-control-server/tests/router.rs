//! The router, across the whole catalog: who may call what, and that every
//! operation the spec defines is routed and dispatched.

use std::sync::Arc;

use bytes::Bytes;
use engenho_config::{GroupTier, SocketAccess};
use engenho_control_server::{
    AuditLog, GrantPolicy, Incoming, NoAudit, PeerCred, Router, verify_chain,
};
use engenho_control_types::ops::Unserved;
use engenho_control_types::types::{Refusal, RefusalReason};
use engenho_control_types::{AuthorityTier, CATALOG, OperationSpec};

const DAEMON: u32 = 501;

fn peer(uid: u32) -> PeerCred {
    PeerCred {
        uid,
        gid: 20,
        pid: Some(4242),
    }
}

/// A request for `row` with every path parameter filled with a valid value.
fn request(row: &OperationSpec) -> Incoming {
    let path = row
        .path
        .split('/')
        .map(|segment| match segment {
            "{child}" | "{driver}" => "kubelet",
            "{leaf}" => "runtime.listen_addr",
            "{target}" => "operator",
            "{confirmation}" => "00000000000000000000000000000000",
            other => other,
        })
        .collect::<Vec<_>>()
        .join("/");
    Incoming {
        method: row.method.as_str().to_owned(),
        path,
        query: None,
        headers: Vec::new(),
        body: if row.body {
            Bytes::from_static(b"{}")
        } else {
            Bytes::new()
        },
    }
}

fn refusal(body: &[u8]) -> Refusal {
    serde_json::from_slice(body).expect("a refusal body")
}

fn router(access: SocketAccess, group_tier: GroupTier) -> Router {
    Router::new(
        Arc::new(Unserved),
        GrantPolicy::new(DAEMON, access, group_tier),
        Arc::new(NoAudit),
    )
}

/// An observe-tier caller is refused every operation above observe, before
/// it runs; every observe operation reaches the implementation.
#[tokio::test]
async fn an_observer_is_refused_everything_above_observe_and_reaches_the_rest() {
    let router = router(SocketAccess::Group, GroupTier::Observe);
    for row in &CATALOG {
        let answer = router.handle(Some(peer(1000)), request(row)).await;
        if row.tier > AuthorityTier::Observe {
            assert_eq!(answer.status, 403, "{}", row.id.as_str());
            assert_eq!(
                refusal(&answer.body).reason,
                RefusalReason::InsufficientAuthority,
                "{}",
                row.id.as_str()
            );
        } else {
            // Routed, parsed and dispatched: Unserved refuses it as
            // unsupported (422), never as unauthorized or unknown.
            assert_eq!(answer.status, 422, "{}", row.id.as_str());
            assert_eq!(refusal(&answer.body).reason, RefusalReason::Unsupported);
        }
    }
}

/// A group member granted mutate still cannot do anything destructive:
/// that needs the daemon's user or root.
#[tokio::test]
async fn destructive_needs_the_daemons_user_or_root() {
    let router = router(SocketAccess::Group, GroupTier::Mutate);
    for row in CATALOG
        .iter()
        .filter(|r| r.tier == AuthorityTier::Destructive)
    {
        let answer = router.handle(Some(peer(1000)), request(row)).await;
        assert_eq!(answer.status, 403, "{}", row.id.as_str());
        let legal = refusal(&answer.body).legal;
        assert!(legal.iter().any(|l| l.contains("root")), "{legal:?}");
        for owner in [DAEMON, 0] {
            let answer = router.handle(Some(peer(owner)), request(row)).await;
            assert_ne!(answer.status, 403, "{} as uid {owner}", row.id.as_str());
        }
    }
}

/// A stranger on an owner-only socket may do nothing, and a request the
/// kernel could not attribute is refused.
#[tokio::test]
async fn strangers_and_unattributed_peers_are_refused() {
    let router = router(SocketAccess::Owner, GroupTier::Mutate);
    let hello = CATALOG
        .iter()
        .find(|r| r.id.as_str() == "hello")
        .expect("hello");
    assert_eq!(
        router.handle(Some(peer(1000)), request(hello)).await.status,
        403
    );
    assert_eq!(router.handle(None, request(hello)).await.status, 403);
    assert_eq!(
        router
            .handle(Some(peer(DAEMON)), request(hello))
            .await
            .status,
        422
    );
}

/// A caller may lower its own authority, never raise it.
#[tokio::test]
async fn a_ceiling_lowers_what_even_the_owner_may_do() {
    let router = router(SocketAccess::Owner, GroupTier::Observe);
    let stop = CATALOG
        .iter()
        .find(|r| r.id.as_str() == "stopRuntime")
        .expect("stopRuntime");
    let mut req = request(stop);
    req.headers
        .push(("Engenho-Ceiling".to_owned(), "observe".to_owned()));
    let answer = router.handle(Some(peer(DAEMON)), req).await;
    assert_eq!(answer.status, 403);
    assert!(refusal(&answer.body).legal[0].contains("Engenho-Ceiling"));
}

#[tokio::test]
async fn an_unknown_path_is_refused_as_unknown() {
    let router = router(SocketAccess::Owner, GroupTier::Observe);
    let answer = router
        .handle(
            Some(peer(DAEMON)),
            Incoming {
                method: "GET".into(),
                path: "/v1/nothing".into(),
                ..Incoming::default()
            },
        )
        .await;
    assert_eq!(answer.status, 404);
    assert_eq!(
        refusal(&answer.body).reason,
        RefusalReason::UnknownOperation
    );
}

/// Every refusal of a mutation, and every mutation's intent and result, is
/// in the chain — and the chain is whole.
#[tokio::test]
async fn refusals_and_mutations_are_audited_in_a_whole_chain() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let audit = Arc::new(AuditLog::open(tmp.path()).expect("audit"));
    let router = Router::new(
        Arc::new(Unserved),
        GrantPolicy::new(DAEMON, SocketAccess::Group, GroupTier::Observe),
        Arc::clone(&audit) as Arc<dyn engenho_control_server::Audit>,
    );
    let above: Vec<&OperationSpec> = CATALOG
        .iter()
        .filter(|r| r.tier > AuthorityTier::Observe)
        .collect();
    for row in &above {
        router.handle(Some(peer(1000)), request(row)).await;
    }
    let stop = CATALOG
        .iter()
        .find(|r| r.id.as_str() == "stopRuntime")
        .expect("stopRuntime");
    router.handle(Some(peer(DAEMON)), request(stop)).await;
    let expected = u64::try_from(above.len()).expect("small") + 2;
    assert_eq!(verify_chain(audit.path()), Ok(expected));
}
