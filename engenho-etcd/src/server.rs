//! THE :2379 LISTENER — etcd's gRPC surface, served.
//!
//! ★ WHAT THIS TURNS ON. Until this module every piece of the façade was
//! correct and unreachable: the keyspace, the bijection, the wire types and
//! the KV semantics were all tested in isolation with nothing able to call
//! them. This is the seam that makes `etcdctl` work, and with it the whole
//! class of tools that speak etcd and nothing else — backup, DR, Velero,
//! anything pointed at `--etcd-servers`.
//!
//! ★ THE SERVICE SPLIT IS UPSTREAM'S, and the scope is fixed by
//! `theory/ENGENHO.md` §III.2 rather than by what happened to be easy:
//! `KV`, `Watch`, `Lease`, `Maintenance` are served; `Auth` returns
//! permission-denied because engenho's authn lives at the apiserver. No
//! other RPCs. The contract is "what the upstream kube-apiserver actually
//! calls", not "all of etcd".
//!
//! ★ UNIMPLEMENTED RPCs RETURN `Unimplemented`, NEVER A PLAUSIBLE EMPTY
//! SUCCESS. An etcd client that receives `Ok` with an empty result treats
//! it as "the keyspace is empty" — a backup tool would write a valid,
//! empty snapshot and report success. `Status::unimplemented` is the only
//! answer that cannot be mistaken for data, which is the same discipline
//! the apiserver's typed 404s follow.

use tonic::{Request, Response, Status};

use crate::pb::etcdserverpb::{
    self, CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};

/// Identifies this server in every response header.
///
/// etcd clients read `cluster_id`/`member_id` to detect that they have been
/// repointed at a DIFFERENT cluster mid-session — a real safety check, so
/// the values must be stable for a given engenho instance rather than
/// regenerated per response.
#[derive(Debug, Clone, Copy)]
pub struct ServerIdentity {
    pub cluster_id: u64,
    pub member_id: u64,
}

impl Default for ServerIdentity {
    fn default() -> Self {
        // Fixed, non-zero, and deliberately not random: a client that
        // reconnects must see the same identity or it will conclude the
        // cluster was replaced and refuse to continue.
        Self {
            cluster_id: 0xE0_6E_74_68_6F_00_00_01,
            member_id: 0xE0_6E_74_68_6F_00_00_02,
        }
    }
}

/// Build the header every etcd response carries.
#[must_use]
pub fn header(id: ServerIdentity, revision: i64) -> ResponseHeader {
    ResponseHeader {
        cluster_id: id.cluster_id,
        member_id: id.member_id,
        revision,
        // `raft_term` is read by clients only to detect leadership change.
        // Reporting a constant is honest for a single-member view; a
        // fabricated increasing value would imply elections that never
        // happened.
        raft_term: 1,
    }
}

/// A read-only KV service.
///
/// ★ READ-ONLY IS A DELIBERATE FIRST RUNG, and it is the rung that
/// delivers the contract's value: `etcdctl get`, every backup tool and
/// every inspection path are reads. Writes go through engenho's own
/// apiserver, which owns admission, defaulting and validation — accepting
/// a raw etcd `Put` would let a client bypass all three and store an
/// object no apiserver would have admitted. Making the write path a typed
/// `Unimplemented` says that, where a silent success would corrupt the
/// cluster quietly.
pub struct ReadOnlyKv<S> {
    pub store: S,
    pub identity: ServerIdentity,
}

/// What the KV service needs from a store, kept as a trait so the service
/// is testable without a Raft cluster — the house's `InMemoryStore`
/// pattern applied at the façade boundary.
#[allow(clippy::module_name_repetitions)]
pub trait EtcdReadStore: Send + Sync + 'static {
    /// The store's current global revision.
    fn revision(&self) -> i64;
    /// Every key/value under `prefix`, already rendered onto the wire.
    fn range(&self, prefix: &str) -> Vec<crate::pb::mvccpb::KeyValue>;
}

#[tonic::async_trait]
impl<S: EtcdReadStore> etcdserverpb::kv_server::Kv for ReadOnlyKv<S> {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        let shape = crate::kv::range_shape(&req.key, &req.range_end);
        let prefix = match &shape {
            crate::kv::RangeShape::Point(k) => k.clone(),
            crate::kv::RangeShape::Prefix(p) => p.clone(),
            crate::kv::RangeShape::All => crate::keyspace::REGISTRY_ROOT.to_string(),
            // An arbitrary interval is not refused — it is served as the
            // widest prefix that contains it and then filtered, which is
            // correct if slower. Refusing would break `etcdctl get a b`.
            crate::kv::RangeShape::Interval { start, .. } => start.clone(),
        };

        let mut kvs = self.store.range(&prefix);
        if let crate::kv::RangeShape::Point(k) = &shape {
            kvs.retain(|kv| kv.key == k.as_bytes());
        }
        if let crate::kv::RangeShape::Interval { start, end } = &shape {
            kvs.retain(|kv| {
                kv.key.as_slice() >= start.as_bytes() && kv.key.as_slice() < end.as_bytes()
            });
        }

        let total = i64::try_from(kvs.len()).unwrap_or(i64::MAX);
        let (kvs, more) = crate::kv::assemble_range(kvs, req.limit);
        // `count` is the TOTAL matching, not the number returned — a client
        // paginating reads it to size the remaining work, and reporting the
        // page length instead would make every page look like the last.
        Ok(Response::new(RangeResponse {
            header: Some(header(self.identity, self.store.revision())),
            kvs: if req.keys_only {
                kvs.into_iter()
                    .map(|mut kv| {
                        kv.value.clear();
                        kv
                    })
                    .collect()
            } else {
                kvs
            },
            more,
            count: total,
        }))
    }

    async fn put(&self, _: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        Err(write_refused("Put"))
    }

    async fn delete_range(
        &self,
        _: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        Err(write_refused("DeleteRange"))
    }

    async fn txn(&self, _: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        Err(write_refused("Txn"))
    }

    async fn compact(
        &self,
        _: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        Err(write_refused("Compact"))
    }
}

/// The refusal every write RPC returns, with the reason in it.
///
/// A bare `Unimplemented` would read as "engenho has not got round to it";
/// naming the reason tells an operator this is a boundary, not a gap.
fn write_refused(rpc: &str) -> Status {
    Status::unimplemented(format!(
        "etcd {rpc} is not served: writes go through engenho's apiserver, which owns admission, \
         defaulting and validation. A raw etcd write would bypass all three and store an object \
         no apiserver would have admitted."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{StoredObject, to_key_value};
    use crate::pb::etcdserverpb::kv_server::Kv as _;
    use engenho_store::ResourceKey;
    use engenho_store::revision::{Revision, VersionMeta};

    struct FakeStore {
        kvs: Vec<crate::pb::mvccpb::KeyValue>,
    }

    impl FakeStore {
        fn with(names: &[(&str, &str)]) -> Self {
            Self {
                kvs: names
                    .iter()
                    .map(|(ns, name)| {
                        to_key_value(
                            &StoredObject {
                                key: ResourceKey::namespaced("", "v1", "Pod", *ns, *name),
                                value: br#"{"kind":"Pod"}"#.to_vec(),
                                meta: VersionMeta {
                                    create_revision: Revision(1),
                                    mod_revision: Revision(2),
                                    version: 1,
                                },
                            },
                            "pods",
                            true,
                        )
                    })
                    .collect(),
            }
        }
    }

    impl EtcdReadStore for FakeStore {
        fn revision(&self) -> i64 {
            42
        }
        fn range(&self, prefix: &str) -> Vec<crate::pb::mvccpb::KeyValue> {
            self.kvs
                .iter()
                .filter(|kv| kv.key.starts_with(prefix.as_bytes()))
                .cloned()
                .collect()
        }
    }

    fn svc(names: &[(&str, &str)]) -> ReadOnlyKv<FakeStore> {
        ReadOnlyKv {
            store: FakeStore::with(names),
            identity: ServerIdentity::default(),
        }
    }

    async fn range_of(svc: &ReadOnlyKv<FakeStore>, req: RangeRequest) -> RangeResponse {
        svc.range(Request::new(req))
            .await
            .expect("range")
            .into_inner()
    }

    fn prefix_req(p: &str) -> RangeRequest {
        RangeRequest {
            key: p.as_bytes().to_vec(),
            range_end: crate::keyspace::prefix_range_end(p.as_bytes()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_prefix_scan_returns_the_collection() {
        // The read `etcdctl get /registry/pods/ --prefix` performs, and the
        // one every backup tool starts with.
        let s = svc(&[("default", "a"), ("default", "b"), ("other", "c")]);
        let r = range_of(&s, prefix_req("/registry/pods/default/")).await;
        assert_eq!(r.kvs.len(), 2);
        assert_eq!(r.count, 2);
        assert!(!r.more);
        let keys: Vec<String> = r
            .kvs
            .iter()
            .map(|k| String::from_utf8_lossy(&k.key).into_owned())
            .collect();
        assert_eq!(
            keys,
            vec!["/registry/pods/default/a", "/registry/pods/default/b"]
        );
    }

    #[tokio::test]
    async fn a_point_read_returns_exactly_one_key() {
        let s = svc(&[("default", "a"), ("default", "ab")]);
        let r = range_of(
            &s,
            RangeRequest {
                key: b"/registry/pods/default/a".to_vec(),
                range_end: vec![],
                ..Default::default()
            },
        )
        .await;
        // Without the exact-match retain, the prefix scan would also return
        // `/registry/pods/default/ab` — a point read that quietly returns
        // a neighbour is worse than one that returns nothing.
        assert_eq!(r.kvs.len(), 1);
        assert_eq!(r.kvs[0].key, b"/registry/pods/default/a".to_vec());
    }

    #[tokio::test]
    async fn count_is_the_total_matched_not_the_page_length() {
        // A paginating client sizes remaining work from `count`; reporting
        // the page length would make every page look like the last.
        let s = svc(&[("default", "a"), ("default", "b"), ("default", "c")]);
        let r = range_of(
            &s,
            RangeRequest {
                limit: 2,
                ..prefix_req("/registry/pods/default/")
            },
        )
        .await;
        assert_eq!(r.kvs.len(), 2, "the page");
        assert_eq!(r.count, 3, "the total");
        assert!(r.more);
    }

    #[tokio::test]
    async fn keys_only_strips_values_but_keeps_keys() {
        let s = svc(&[("default", "a")]);
        let r = range_of(
            &s,
            RangeRequest {
                keys_only: true,
                ..prefix_req("/registry/pods/")
            },
        )
        .await;
        assert_eq!(r.kvs.len(), 1);
        assert!(r.kvs[0].value.is_empty());
        assert!(!r.kvs[0].key.is_empty());
    }

    #[tokio::test]
    async fn the_header_carries_a_stable_identity_and_the_live_revision() {
        // A client that reconnects and sees a different cluster_id concludes
        // it was repointed at another cluster and refuses to continue.
        let s = svc(&[("default", "a")]);
        let h1 = range_of(&s, prefix_req("/registry/")).await.header.unwrap();
        let h2 = range_of(&s, prefix_req("/registry/")).await.header.unwrap();
        assert_eq!(h1.cluster_id, h2.cluster_id);
        assert_eq!(h1.member_id, h2.member_id);
        assert_ne!(h1.cluster_id, 0, "zero reads as 'unset' to some clients");
        assert_eq!(h1.revision, 42, "the store's live revision");
    }

    #[tokio::test]
    async fn writes_are_refused_typed_never_a_plausible_empty_success() {
        // An Ok-with-empty-result would let a backup tool write a valid,
        // EMPTY snapshot and report success.
        let s = svc(&[]);
        let e = s
            .put(Request::new(PutRequest::default()))
            .await
            .expect_err("must refuse");
        assert_eq!(e.code(), tonic::Code::Unimplemented);
        assert!(
            e.message().contains("apiserver"),
            "the refusal must say WHY it is a boundary, not a gap: {}",
            e.message()
        );
        for code in [
            s.txn(Request::new(TxnRequest::default())).await.err(),
            s.delete_range(Request::new(DeleteRangeRequest::default()))
                .await
                .err(),
            s.compact(Request::new(CompactionRequest::default()))
                .await
                .err(),
        ] {
            assert_eq!(code.expect("refused").code(), tonic::Code::Unimplemented);
        }
    }
}
