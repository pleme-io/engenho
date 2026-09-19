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

/// The store behind the façade is gone: the node is shutting down.
///
/// ★ A VALUE, NEVER AN EMPTY ANSWER (T3.8). The façade holds its store by
/// `Weak`, so a detached listener cannot keep a stopped node's store alive —
/// which means any read can find the store gone. Each read used to degrade
/// to an empty or zero answer: `Range` returned `Ok` with no keys at
/// revision 0, the exact shape of an empty cluster, so a backup tool dialled
/// during a shutdown would have written a valid, EMPTY snapshot and reported
/// success. Every store trait below returns `Result<_, StoreGone>`, and the
/// services turn it into gRPC `Unavailable`: the one answer an etcd client
/// retries against another endpoint instead of reading as data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the store behind this etcd facade is gone: the node is shutting down")]
pub struct StoreGone;

impl From<StoreGone> for Status {
    fn from(gone: StoreGone) -> Self {
        Status::unavailable(gone.to_string())
    }
}

/// The current revision, which every etcd service reports in its headers.
///
/// One supertrait rather than a `revision` on each store trait, so the
/// three services cannot disagree about what "the current revision" is.
#[tonic::async_trait]
pub trait EtcdRevision: Send + Sync + 'static {
    /// The store's current global revision.
    ///
    /// # Errors
    ///
    /// [`StoreGone`] once the store has been dropped — never `0`, which a
    /// client reads as a real revision.
    async fn revision(&self) -> Result<i64, StoreGone>;
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
///
/// ★ ASYNC BECAUSE THE STORE IS. The production implementation reads a
/// Raft-backed `StoreMesh`, every method of which is `async`. A synchronous
/// trait over it would force one of two bad answers: block inside the
/// runtime, or serve a cached snapshot — and a snapshot means `etcdctl get`
/// returns the cluster as it was at the last refresh, which is exactly the
/// silently-wrong answer this crate's header exists to forbid.
#[allow(clippy::module_name_repetitions)]
#[tonic::async_trait]
pub trait EtcdReadStore: EtcdRevision {
    /// Every key/value under `prefix`, already rendered onto the wire, and
    /// the revision they were read at, from ONE look at the store.
    ///
    /// # Errors
    ///
    /// [`StoreGone`] once the store has been dropped — never an empty
    /// vector, which a caller cannot tell from an empty keyspace.
    async fn range_at(&self, prefix: &str) -> Result<RangeAt, StoreGone>;

    /// The keys of [`Self::range_at`] alone, for a caller that reports no
    /// revision with them.
    ///
    /// # Errors
    ///
    /// As [`Self::range_at`].
    async fn range(&self, prefix: &str) -> Result<Vec<crate::pb::mvccpb::KeyValue>, StoreGone> {
        self.range_at(prefix).await.map(|at| at.kvs)
    }
}

/// The key/values under a prefix, and the revision they were read at.
///
/// ★ ONE LOOK, SO THE HEADER CANNOT BE NEWER THAN THE KEYS. The Range
/// service used to read the keys, then the revision in a second call. A
/// write landing between the two made the header name a revision the keys
/// did not reflect, and a client that lists at the header revision and
/// watches from the one after it — how a list-then-watch client resumes —
/// never saw that write. The revision now travels with the keys, and the
/// service has no second read to make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeAt {
    /// Every key/value under the prefix, rendered onto the wire.
    pub kvs: Vec<crate::pb::mvccpb::KeyValue>,
    /// The store's revision when `kvs` were read.
    pub revision: i64,
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

        let RangeAt { mut kvs, revision } = self.store.range_at(&prefix).await?;
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
            header: Some(header(self.identity, revision)),
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
        /// Set to answer as a store that has been dropped.
        gone: bool,
        /// The revision `range_at` reports its keys were read at. The live
        /// revision is always 42; a lower value is a store that has moved on
        /// since the keys were read.
        read_at: i64,
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
                gone: false,
                read_at: 42,
            }
        }

        fn live(&self) -> Result<(), StoreGone> {
            if self.gone { Err(StoreGone) } else { Ok(()) }
        }
    }

    #[tonic::async_trait]
    impl EtcdRevision for FakeStore {
        async fn revision(&self) -> Result<i64, StoreGone> {
            self.live().map(|()| 42)
        }
    }

    #[tonic::async_trait]
    impl EtcdReadStore for FakeStore {
        async fn range_at(&self, prefix: &str) -> Result<RangeAt, StoreGone> {
            self.live()?;
            Ok(RangeAt {
                kvs: self
                    .kvs
                    .iter()
                    .filter(|kv| kv.key.starts_with(prefix.as_bytes()))
                    .cloned()
                    .collect(),
                revision: self.read_at,
            })
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

    /// The header names the revision the keys were read at, not a later
    /// one. Read in a second call, it was newer than the keys whenever a
    /// write landed between the two reads, and a client that lists at the
    /// header and watches from the revision after it never sees that write.
    #[tokio::test]
    async fn the_range_header_is_the_revision_the_keys_were_read_at() {
        let mut s = svc(&[("default", "a")]);
        // The keys were read at 41; the store has since moved on to 42.
        s.store.read_at = 41;
        let header = range_of(&s, prefix_req("/registry/"))
            .await
            .header
            .expect("a header");
        assert_eq!(header.revision, 41);
    }

    #[tokio::test]
    async fn a_gone_store_is_unavailable_never_an_empty_range() {
        // An `Ok` with no keys at revision 0 is exactly what an empty
        // cluster looks like: a backup tool would write it out as a valid,
        // empty snapshot. `Unavailable` is the answer a client retries.
        let mut s = svc(&[("default", "a")]);
        s.store.gone = true;
        let e = s
            .range(Request::new(prefix_req("/registry/")))
            .await
            .expect_err("a gone store must not answer a Range");
        assert_eq!(e.code(), tonic::Code::Unavailable);
        assert_eq!(e.message(), StoreGone.to_string());
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

// ─────────────────────────────────────────────────────────────────────
// MAINTENANCE — the service `etcdctl endpoint status` and
// `etcdctl snapshot save` speak.
//
// ★ WHY `Status` MATTERS MORE THAN IT LOOKS. It is the first call almost
// every etcd-shaped tool makes: `etcdctl` probes it before any other RPC,
// health checks poll it, and a client uses `version` to decide which
// features to attempt. Without it a tool concludes the endpoint is not
// etcd at all and gives up before reaching the KV service that works.
//
// ★ `db_size` IS REPORTED HONESTLY OR NOT AT ALL. Capacity dashboards and
// the `etcd_mvcc_db_total_size_in_bytes` alerting family read it, and a
// fabricated number would drive real alerts. engenho's store is a
// journalled segment store whose on-disk size is not a single figure; the
// caller supplies it or it is reported as 0, which reads as "unknown"
// rather than as "empty".
// ─────────────────────────────────────────────────────────────────────

use crate::pb::etcdserverpb::{
    AlarmRequest, AlarmResponse, DefragmentRequest, DefragmentResponse, DowngradeRequest,
    DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse, MoveLeaderRequest,
    MoveLeaderResponse, SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse,
};

/// What Maintenance needs to answer about the running store.
#[tonic::async_trait]
pub trait EtcdStatusStore: EtcdRevision {
    /// Raft applied index, surfaced as both `raft_index` and
    /// `raft_applied_index` — engenho applies what it commits.
    ///
    /// # Errors
    ///
    /// [`StoreGone`] once the store has been dropped.
    async fn applied_index(&self) -> Result<u64, StoreGone>;
    /// On-disk size in bytes if the backend can report one cheaply.
    ///
    /// `None` becomes 0. A guess here would drive real capacity alerts off
    /// a number nobody measured.
    ///
    /// # Errors
    ///
    /// [`StoreGone`] once the store has been dropped.
    async fn db_size(&self) -> Result<Option<i64>, StoreGone>;
}

/// The Maintenance service.
pub struct MaintenanceSvc<S> {
    pub store: S,
    pub identity: ServerIdentity,
}

#[tonic::async_trait]
impl<S: EtcdStatusStore> etcdserverpb::maintenance_server::Maintenance for MaintenanceSvc<S> {
    async fn status(&self, _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        // A gone store is `Unavailable`. A status of zeros would read as a
        // healthy, empty member — the answer a backup tool gates on.
        let rev = self.store.revision().await?;
        let applied = self.store.applied_index().await?;
        let db_size = self.store.db_size().await?.unwrap_or(0);
        Ok(Response::new(StatusResponse {
            header: Some(header(self.identity, rev)),
            // The etcd API version engenho's wire types were generated
            // from. Clients gate feature use on this, so it must name the
            // protocol actually served, not engenho's own version.
            version: "3.5.0".to_string(),
            db_size,
            leader: self.identity.member_id,
            raft_index: applied,
            raft_term: 1,
            raft_applied_index: applied,
            errors: Vec::new(),
            db_size_in_use: db_size,
            is_learner: false,
        }))
    }

    async fn alarm(&self, _: Request<AlarmRequest>) -> Result<Response<AlarmResponse>, Status> {
        // An empty alarm list is the TRUE answer, not a stub: engenho
        // raises no etcd alarms (NOSPACE/CORRUPT are backend conditions it
        // does not have). Returning Unimplemented here would make
        // `etcdctl endpoint health` fail on a healthy server.
        Ok(Response::new(AlarmResponse {
            header: Some(header(self.identity, self.store.revision().await?)),
            alarms: Vec::new(),
        }))
    }

    async fn defragment(
        &self,
        _: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        // Also a true answer rather than a stub: there is no B-tree to
        // defragment. Succeeding is correct — the caller asked for a
        // post-condition that already holds.
        Ok(Response::new(DefragmentResponse {
            header: Some(header(self.identity, self.store.revision().await?)),
        }))
    }

    type SnapshotStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<SnapshotResponse, Status>> + Send + 'static>,
    >;

    async fn snapshot(
        &self,
        _: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        // ★ REFUSED, LOUDLY, AND THIS IS THE MOST IMPORTANT REFUSAL IN THE
        // FILE. `etcdctl snapshot save` writes whatever bytes it receives
        // to a file and reports success. If engenho streamed anything that
        // was not a genuine etcd bbolt snapshot, an operator would hold a
        // backup that restores into nothing — discovering it only during a
        // disaster. A typed Unimplemented is the only safe answer until
        // engenho can emit a real restorable image.
        Err(Status::unimplemented(
            "etcd Maintenance.Snapshot is not served: `etcdctl snapshot save` writes whatever it \
             receives and reports success, so streaming anything that is not a genuine restorable \
             etcd image would hand an operator a backup that silently restores into nothing. Back \
             engenho up through its own store snapshot instead.",
        ))
    }

    async fn hash(&self, _: Request<HashRequest>) -> Result<Response<HashResponse>, Status> {
        Err(Status::unimplemented(
            "etcd Maintenance.Hash is a bbolt-bucket hash with no engenho equivalent; a different \
             hash would fail every consistency check it is used for",
        ))
    }

    async fn hash_kv(&self, _: Request<HashKvRequest>) -> Result<Response<HashKvResponse>, Status> {
        Err(Status::unimplemented(
            "etcd Maintenance.HashKV is not served",
        ))
    }

    async fn move_leader(
        &self,
        _: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        Err(Status::unimplemented(
            "engenho leadership is managed by its own Raft, not through the etcd API",
        ))
    }

    async fn downgrade(
        &self,
        _: Request<DowngradeRequest>,
    ) -> Result<Response<DowngradeResponse>, Status> {
        Err(Status::unimplemented(
            "etcd Maintenance.Downgrade is not served",
        ))
    }
}

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    use crate::pb::etcdserverpb::maintenance_server::Maintenance as _;

    /// `gone` answers as a store that has been dropped.
    struct FakeStatus {
        gone: bool,
    }

    impl FakeStatus {
        fn live(&self) -> Result<(), StoreGone> {
            if self.gone { Err(StoreGone) } else { Ok(()) }
        }
    }

    #[tonic::async_trait]
    impl EtcdRevision for FakeStatus {
        async fn revision(&self) -> Result<i64, StoreGone> {
            self.live().map(|()| 77)
        }
    }

    #[tonic::async_trait]
    impl EtcdStatusStore for FakeStatus {
        async fn applied_index(&self) -> Result<u64, StoreGone> {
            self.live().map(|()| 123)
        }
        async fn db_size(&self) -> Result<Option<i64>, StoreGone> {
            self.live().map(|()| None)
        }
    }

    fn svc() -> MaintenanceSvc<FakeStatus> {
        MaintenanceSvc {
            store: FakeStatus { gone: false },
            identity: ServerIdentity::default(),
        }
    }

    #[tokio::test]
    async fn a_gone_store_is_unavailable_never_a_status_of_zeros() {
        // Zeros read as a healthy, empty member; `etcdctl endpoint status`
        // and every backup tool gate on this call.
        let s = MaintenanceSvc {
            store: FakeStatus { gone: true },
            identity: ServerIdentity::default(),
        };
        let status = s
            .status(Request::new(StatusRequest::default()))
            .await
            .expect_err("a gone store has no status");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        // Alarm and Defragment answer "healthy" and "done": from a gone
        // store both would be lies.
        let alarm = s.alarm(Request::new(AlarmRequest::default())).await;
        assert_eq!(alarm.expect_err("gone").code(), tonic::Code::Unavailable);
        let defrag = s
            .defragment(Request::new(DefragmentRequest::default()))
            .await;
        assert_eq!(defrag.expect_err("gone").code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn status_is_the_first_call_every_tool_makes_and_it_answers() {
        // Without it a tool concludes the endpoint is not etcd at all and
        // never reaches the KV service that works.
        let r = svc()
            .status(Request::new(StatusRequest::default()))
            .await
            .expect("status")
            .into_inner();
        assert_eq!(
            r.version, "3.5.0",
            "clients gate features on the PROTOCOL version"
        );
        assert_eq!(r.header.expect("header").revision, 77);
        assert_eq!(r.raft_index, 123);
        assert_eq!(r.raft_applied_index, 123, "engenho applies what it commits");
        assert!(r.errors.is_empty());
    }

    #[tokio::test]
    async fn an_unknown_db_size_is_zero_not_a_guess() {
        // Capacity dashboards and the etcd db-size alert family read this;
        // a fabricated number would drive real alerts.
        let r = svc()
            .status(Request::new(StatusRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.db_size, 0);
        assert_eq!(r.db_size_in_use, 0);
    }

    #[tokio::test]
    async fn alarm_and_defragment_succeed_because_that_is_the_true_answer() {
        // Not stubs. engenho raises no etcd alarms and has no B-tree to
        // defragment, so success is correct — and Unimplemented here would
        // make `etcdctl endpoint health` fail on a healthy server.
        let s = svc();
        let a = s
            .alarm(Request::new(AlarmRequest::default()))
            .await
            .expect("alarm")
            .into_inner();
        assert!(a.alarms.is_empty());
        assert!(
            s.defragment(Request::new(DefragmentRequest::default()))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn snapshot_is_refused_because_a_fake_backup_is_worse_than_none() {
        // etcdctl snapshot save writes whatever it receives and reports
        // success. Streaming non-restorable bytes would hand an operator a
        // backup discovered to be worthless only during a disaster.
        let e = svc()
            .snapshot(Request::new(SnapshotRequest::default()))
            .await
            .err()
            .expect("must refuse");
        assert_eq!(e.code(), tonic::Code::Unimplemented);
        assert!(
            e.message().contains("restore"),
            "the refusal must explain the danger: {}",
            e.message()
        );
    }
}

// ─────────────────────────────────────────────────────────────────────
// WATCH — the streaming service.
//
// ★ WHY WATCH IS THE ONE THAT MUST NOT BE APPROXIMATED. A Range that
// returns slightly wrong results is a bug a caller can notice. A Watch
// that silently drops an event is a caller whose cached view diverges from
// the cluster FOREVER, with nothing to compare against. Every controller
// ever written is built on the assumption that a watch stream is gap-free
// or says so.
//
// ★ ONE ATOMIC `watch_from`, NOT A SUBSCRIBE PLUS A HISTORY READ (T3.8).
// This service used to subscribe to the live tail and then read history
// with a second call. Two calls are two looks at the store: correctness
// depended on the caller remembering to subscribe first, and even then a
// change committed between the two calls was in both — delivered once in
// the history and again live. The store's own `watch_from`
// captures the replay and attaches the live tail under ONE lock, so the
// hand-off has no gap, no duplicate and no reorder by construction. The
// façade now asks for exactly that, and the ordering rule is no longer
// something this file has to get right.
//
// ★ EVERY END IS SAID. A watch ends for a reason — compaction, a slow
// client overflowing its buffer, the store going away, the client
// cancelling — and every one of them reaches the client as a `canceled`
// response carrying that reason. A stream that simply stops is a client
// that believes it is still tracking the cluster. `WatchFeed::next` has no
// "nothing" answer, so an end without a reason does not type-check.
//
// ★ `created` IS NOT COSMETIC. etcd sends a response with `created: true`
// and no events to acknowledge a watch before any data flows. A client
// that never receives it waits forever on a watch it believes is pending —
// so the acknowledgement is the first thing sent, before any history.
// ─────────────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::num::NonZeroU64;

use crate::pb::etcdserverpb::{WatchRequest, WatchResponse, watch_request::RequestUnion};

/// A watch event to deliver, already rendered onto the wire.
pub type WireEvent = crate::pb::mvccpb::Event;

/// Where a watch starts, parsed ONCE from etcd's `start_revision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchStart {
    /// `0`: from now — only changes committed after the watch is created.
    /// Never "from the beginning of time", which would flood a client that
    /// asked for a live tail and, on a large cluster, hang it.
    Now,
    /// `N >= 1`: every change at or after revision `N` — retained history
    /// first, then live. A start ahead of the store waits for it.
    At(NonZeroU64),
    /// `N < 0`: before any history there could be. etcd answers it as a
    /// compaction carrying the watermark, and so does this façade.
    BeforeHistory,
}

impl WatchStart {
    /// Parse etcd's `start_revision`.
    #[must_use]
    pub fn from_wire(start_revision: i64) -> Self {
        match u64::try_from(start_revision) {
            Err(_) => Self::BeforeHistory,
            Ok(n) => NonZeroU64::new(n).map_or(Self::Now, Self::At),
        }
    }
}

/// Why a watch ended. Every end has exactly one of these, and each reaches
/// the client as a `canceled` response whose `cancel_reason` is its
/// `Display`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatchEnd {
    /// The requested start has been compacted away. `compact_revision` is
    /// where the client may safely resume; without it the client retries
    /// into the same hole forever.
    #[error("required revision has been compacted")]
    Compacted { compact_revision: i64 },
    /// The client drained too slowly and the watch's buffer filled. Nothing
    /// was dropped silently: every event up to `last_seen` was delivered.
    #[error(
        "the watcher fell behind its buffer after revision {last_seen}; re-watch from the \
         revision after it"
    )]
    Overflow { last_seen: i64 },
    /// The store was dropped: the node is shutting down.
    #[error(transparent)]
    StoreGone(#[from] StoreGone),
    /// The client asked to cancel this watch.
    #[error("watch cancelled by client")]
    CancelledByClient,
}

impl WatchEnd {
    /// The `compact_revision` field of the cancel: the watermark for a
    /// compaction, `0` ("not a compaction") for every other end.
    #[must_use]
    pub fn compact_revision(&self) -> i64 {
        match self {
            Self::Compacted { compact_revision } => *compact_revision,
            Self::Overflow { .. } | Self::StoreGone(_) | Self::CancelledByClient => 0,
        }
    }

    /// The header revision the cancel carries: the last revision this end
    /// knows of, or `0` when it knows none.
    #[must_use]
    pub fn revision(&self) -> i64 {
        match self {
            Self::Compacted { compact_revision } => *compact_revision,
            Self::Overflow { last_seen } => *last_seen,
            Self::StoreGone(_) | Self::CancelledByClient => 0,
        }
    }
}

/// etcd's `InvalidWatchID`: the id of a response that is about no watch.
pub const INVALID_WATCH_ID: i64 = -1;

/// A create that named a `watch_id` already open on this stream.
///
/// ★ NOT A [`WatchEnd`], BECAUSE IT ENDS NO WATCH. The watch that holds the
/// id is still open and still the client's. A refusal sent under that id —
/// a `canceled` response for watch 7 — tells the client its open watch 7
/// is over while events for 7 keep arriving, which is the merge this
/// refusal exists to prevent. So a duplicate has its own type and one
/// builder, [`duplicate_id_response`], which takes no watch id at all:
/// the refusal cannot name the open watch. The text is upstream's, word
/// for word (`mvcc.ErrWatcherDuplicateID`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("mvcc: duplicate watch ID provided on the WatchStream")]
pub struct DuplicateWatchId;

/// One step of an open watch: an event, or the one reason it ended.
#[derive(Debug, Clone, PartialEq)]
pub enum WatchStep {
    /// A change under the watched prefix.
    Event(WireEvent),
    /// The watch is over, and this is why.
    End(WatchEnd),
}

/// An open watch, as the service drains it.
///
/// ★ `next` HAS NO THIRD ANSWER. There is no `Option`: a feed whose source
/// closes — a bare channel close included — must say which [`WatchEnd`] that
/// is. The service then sends it as a `canceled` response, so a watch can
/// never just stop.
#[tonic::async_trait]
pub trait WatchFeed: Send + 'static {
    /// The next event, or the reason this watch ended. The service calls it
    /// until the first [`WatchStep::End`] and then drops the feed.
    async fn next(&mut self) -> WatchStep;
}

/// A watch the store has opened.
#[derive(Debug)]
pub struct OpenedWatch<F> {
    /// The store's revision when the watch was opened — the header of the
    /// `created` acknowledgement.
    pub revision: i64,
    /// Every event from the requested start, then the end.
    pub feed: F,
}

/// What the Watch service needs from a store.
#[tonic::async_trait]
pub trait EtcdWatchStore: EtcdRevision {
    /// The feed an open watch is drained from.
    type Feed: WatchFeed;

    /// Open a watch on `prefix` from `start`: retained history, then the
    /// live tail, as ONE atomic act — no change is lost or doubled between
    /// the two, and none arrives out of revision order.
    ///
    /// # Errors
    ///
    /// The [`WatchEnd`] that refuses the watch before it opens:
    /// [`WatchEnd::Compacted`] (with the watermark) for a start below it,
    /// [`WatchEnd::StoreGone`] once the store has been dropped.
    async fn watch_from(
        &self,
        prefix: &str,
        start: WatchStart,
    ) -> Result<OpenedWatch<Self::Feed>, WatchEnd>;
}

/// The Watch service.
///
/// The store is held in an `Arc` because each watch runs in its own task:
/// a borrow could not outlive the request, and cloning the store per watch
/// would give each one a different view of the same cluster.
pub struct WatchSvc<S> {
    pub store: std::sync::Arc<S>,
    pub identity: ServerIdentity,
}

impl<S: EtcdWatchStore> WatchSvc<S> {
    /// New service over a shared store.
    pub fn new(store: std::sync::Arc<S>, identity: ServerIdentity) -> Self {
        Self { store, identity }
    }

    fn store_handle(&self) -> std::sync::Arc<S> {
        std::sync::Arc::clone(&self.store)
    }
}

/// Build the acknowledgement etcd sends before any events.
#[must_use]
pub fn created_response(id: ServerIdentity, watch_id: i64, revision: i64) -> WatchResponse {
    WatchResponse {
        header: Some(header(id, revision)),
        watch_id,
        created: true,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events: Vec::new(),
    }
}

/// Build the `canceled` response that ends a watch, naming why.
///
/// The ONE builder for every end, so a cancel cannot go out without a
/// reason, and a compaction cannot go out without its watermark.
#[must_use]
pub fn canceled_response(id: ServerIdentity, watch_id: i64, end: &WatchEnd) -> WatchResponse {
    WatchResponse {
        header: Some(header(id, end.revision())),
        watch_id,
        created: false,
        canceled: true,
        compact_revision: end.compact_revision(),
        cancel_reason: end.to_string(),
        fragment: false,
        events: Vec::new(),
    }
}

/// Build etcd's refusal of a create whose `watch_id` is already open: ONE
/// response, `created` and `canceled` together, under [`INVALID_WATCH_ID`].
///
/// There is no `watch_id` parameter, so this refusal cannot be sent under
/// the id of the watch that is still open (see [`DuplicateWatchId`]). The
/// header revision is `0`, the same "knows none" every other refusal that
/// has no revision of its own carries.
#[must_use]
pub fn duplicate_id_response(id: ServerIdentity) -> WatchResponse {
    WatchResponse {
        header: Some(header(id, 0)),
        watch_id: INVALID_WATCH_ID,
        created: true,
        canceled: true,
        compact_revision: 0,
        cancel_reason: DuplicateWatchId.to_string(),
        fragment: false,
        events: Vec::new(),
    }
}

/// Build a response carrying events.
#[must_use]
pub fn events_response(
    id: ServerIdentity,
    watch_id: i64,
    revision: i64,
    events: Vec<WireEvent>,
) -> WatchResponse {
    WatchResponse {
        header: Some(header(id, revision)),
        watch_id,
        created: false,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events,
    }
}

/// What one `WatchCreateRequest` resolves to.
#[derive(Debug)]
pub enum CreatedWatch<F> {
    /// Acknowledged and open: send `ack`, then drain `feed` until it ends.
    Open { ack: WatchResponse, feed: F },
    /// Acknowledged and ended at once: send `ack`, then `cancel`.
    Refused {
        ack: WatchResponse,
        cancel: WatchResponse,
    },
}

impl<F> CreatedWatch<F> {
    /// A watch refused before it opened, for `end`.
    ///
    /// Acknowledged first all the same: a client that never receives
    /// `created` waits forever on a watch it believes is pending.
    #[must_use]
    pub fn refused(id: ServerIdentity, watch_id: i64, end: &WatchEnd) -> Self {
        Self::Refused {
            ack: created_response(id, watch_id, end.revision()),
            cancel: canceled_response(id, watch_id, end),
        }
    }
}

/// Open the watch one `WatchCreateRequest` asks for.
///
/// Separate from the stream loop so the acknowledgement and the refusal
/// rules are testable without a stream.
pub async fn create_watch<S: EtcdWatchStore>(
    store: &S,
    id: ServerIdentity,
    watch_id: i64,
    key: &[u8],
    start_revision: i64,
) -> CreatedWatch<S::Feed> {
    let prefix = String::from_utf8_lossy(key);
    match store
        .watch_from(&prefix, WatchStart::from_wire(start_revision))
        .await
    {
        Ok(opened) => CreatedWatch::Open {
            ack: created_response(id, watch_id, opened.revision),
            feed: opened.feed,
        },
        Err(end) => CreatedWatch::refused(id, watch_id, &end),
    }
}

/// The response channel every watch on one gRPC stream shares.
type Responses = tokio::sync::mpsc::Sender<Result<WatchResponse, Status>>;

/// Drain one watch's feed onto the shared response channel until it ends.
///
/// ★ THE END IS ALWAYS SENT. The loop's only exits are the feed's end —
/// which goes out as a `canceled` response with its reason — and the client
/// having gone, when there is nobody left to tell.
async fn forward<F: WatchFeed>(
    identity: ServerIdentity,
    watch_id: i64,
    mut feed: F,
    tx: Responses,
) {
    loop {
        let response = match feed.next().await {
            WatchStep::Event(event) => {
                // The event's own revision is the newest the store is known
                // to have reached when it was sent.
                let revision = event.kv.as_ref().map_or(0, |kv| kv.mod_revision);
                events_response(identity, watch_id, revision, vec![event])
            }
            WatchStep::End(end) => {
                // The last thing this watch sends. A failed send means the
                // client has gone and there is nobody left to tell.
                let _ = tx
                    .send(Ok(canceled_response(identity, watch_id, &end)))
                    .await;
                return;
            }
        };
        if tx.send(Ok(response)).await.is_err() {
            return;
        }
    }
}

/// The next server-assigned watch id not already open on this stream.
fn next_free_id(next: &mut i64, open: &HashMap<i64, tokio::task::JoinHandle<()>>) -> i64 {
    while open.contains_key(next) {
        *next = next.saturating_add(1);
    }
    let id = *next;
    *next = next.saturating_add(1);
    id
}

/// Run the watch protocol over ANY request stream.
///
/// ★ SEPARATED FROM THE TRANSPORT ON PURPOSE. `tonic::Streaming` cannot be
/// constructed outside a real connection, so a protocol loop written
/// directly against it is only reachable from an integration test with a
/// live server. Every rule below — the created acknowledgement, a cancel
/// that stops its watch, every end said — would then be untested at the
/// unit level, which is precisely where a watch bug is cheapest to find and
/// most expensive to miss.
pub async fn run_watch_loop<S, R>(
    store: std::sync::Arc<S>,
    identity: ServerIdentity,
    mut inbound: R,
    tx: Responses,
) where
    S: EtcdWatchStore,
    R: futures_core::Stream<Item = Result<WatchRequest, Status>> + Unpin + Send + 'static,
{
    use tokio_stream::StreamExt as _;

    let mut next_id: i64 = 1;
    // Every watch still being forwarded, by id: a client's cancel stops
    // exactly that one, and a hang-up stops them all.
    let mut open: HashMap<i64, tokio::task::JoinHandle<()>> = HashMap::new();
    while let Some(Ok(req)) = inbound.next().await {
        // A watch whose feed ended has already said so; its id is free.
        open.retain(|_, task| !task.is_finished());

        let mut replies: Vec<Result<WatchResponse, Status>> = Vec::new();
        let mut opened: Option<(i64, S::Feed)> = None;
        match req.request_union {
            Some(RequestUnion::CreateRequest(create)) => {
                // Watch ids are assigned by the SERVER when the client
                // sends 0, which is what every client does. Reusing one
                // would silently merge two watches from the client's view.
                let watch_id = if create.watch_id == 0 {
                    next_free_id(&mut next_id, &open)
                } else {
                    create.watch_id
                };
                // A client-chosen id already open is refused under no id
                // at all: the open watch keeps it, and keeps running.
                if open.contains_key(&watch_id) {
                    replies.push(Ok(duplicate_id_response(identity)));
                } else {
                    match create_watch(
                        store.as_ref(),
                        identity,
                        watch_id,
                        &create.key,
                        create.start_revision,
                    )
                    .await
                    {
                        CreatedWatch::Open { ack, feed } => {
                            replies.push(Ok(ack));
                            opened = Some((watch_id, feed));
                        }
                        CreatedWatch::Refused { ack, cancel } => {
                            replies.push(Ok(ack));
                            replies.push(Ok(cancel));
                        }
                    }
                }
            }
            Some(RequestUnion::CancelRequest(cancel)) => {
                // Stop the watch BEFORE acknowledging: once the client has
                // the cancel it may reuse the id, and an event forwarded
                // after it would land on the wrong watch. Awaiting the
                // aborted task is what makes "before" true.
                if let Some(task) = open.remove(&cancel.watch_id) {
                    task.abort();
                    let _ = task.await;
                }
                replies.push(Ok(canceled_response(
                    identity,
                    cancel.watch_id,
                    &WatchEnd::CancelledByClient,
                )));
            }
            // A progress request asks for a bookmark-shaped reply so an
            // idle client can checkpoint without waiting for traffic. From
            // a gone store there is no revision to report: the stream ends
            // `Unavailable`, and the client reconnects elsewhere.
            Some(RequestUnion::ProgressRequest(_)) => replies.push(
                store
                    .revision()
                    .await
                    .map(|rev| events_response(identity, 0, rev, Vec::new()))
                    .map_err(Status::from),
            ),
            None => {}
        }

        let ends_stream = replies.iter().any(Result::is_err);
        for reply in replies {
            if tx.send(reply).await.is_err() {
                abort_all(open);
                return; // client hung up
            }
        }
        // The acknowledgement is on the wire before the watch's first
        // event can be: the forwarding task starts only now.
        if let Some((watch_id, feed)) = opened {
            open.insert(
                watch_id,
                tokio::spawn(forward(identity, watch_id, feed, tx.clone())),
            );
        }
        if ends_stream {
            break;
        }
    }
    // ★ DO NOT ABORT THE WATCH TASKS WHEN THE INBOUND STREAM ENDS. That
    // means the client HALF-CLOSED — it has no more requests to send —
    // which is the normal state of a client that created its watches and
    // is now waiting for events. gRPC bidi streaming is explicitly
    // half-duplex-capable, so the response side must outlive the request
    // side. Aborting on this boundary is the bug the live-event test
    // caught: the acknowledgement arrived and not one event ever did.
    //
    // The watches end when the RESPONSE channel closes — the client
    // actually going away. Then they are aborted: a watch whose feed is
    // idle would otherwise never notice, and would hold its store
    // subscription for the life of the process.
    tx.closed().await;
    abort_all(open);
}

/// Stop every watch still being forwarded.
fn abort_all(open: HashMap<i64, tokio::task::JoinHandle<()>>) {
    for task in open.into_values() {
        task.abort();
    }
}

#[tonic::async_trait]
impl<S: EtcdWatchStore> etcdserverpb::watch_server::Watch for WatchSvc<S> {
    type WatchStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<WatchResponse, Status>> + Send + 'static>,
    >;

    /// ★ ONE gRPC STREAM CARRIES MANY WATCHES, and that is why everything
    /// funnels through a single channel. etcd multiplexes: a client opens
    /// one stream and creates several watches on it, each identified by
    /// `watch_id`. Responses for all of them interleave on the same wire.
    /// Fanning each watch out to its own gRPC stream would be a different
    /// protocol that no etcd client speaks.
    ///
    /// ★ THE ORDER WITHIN ONE WATCH IS PRESERVED; ACROSS WATCHES IT IS NOT,
    /// and that is correct — etcd promises per-watch ordering only. Each
    /// watch owns a task that forwards in order; the shared channel
    /// interleaves between them, exactly as a real server does.
    ///
    /// ★ A CANCELLED OR DROPPED WATCH STOPS ITS TASK. A client cancel aborts
    /// that watch's task before acknowledging; a client hang-up aborts them
    /// all. Without that, a long-lived client that creates and cancels
    /// watches leaks a task per creation and the server degrades over hours
    /// rather than failing visibly.
    async fn watch(
        &self,
        request: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // Bounded channel: an unbounded one lets a slow client turn a fast
        // keyspace into unbounded server memory — a denial of service the
        // client does not even know it is causing.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<WatchResponse, Status>>(256);
        let store = self.store_handle();
        let identity = self.identity;
        tokio::spawn(async move {
            run_watch_loop(store, identity, request.into_inner(), tx).await;
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;
    use crate::pb::mvccpb::{Event, KeyValue};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// A store whose `watch_from` either refuses or opens a feed of `steps`.
    #[derive(Default)]
    struct FakeWatch {
        /// Answer `watch_from` with this refusal instead of a feed.
        refusal: Option<WatchEnd>,
        /// The steps each opened feed yields. After them, a feed with no
        /// `End` among them stays open with nothing to say.
        steps: Vec<WatchStep>,
        /// Answer as a store that has been dropped.
        gone: bool,
        /// Every start `watch_from` was asked for.
        asked: Mutex<Vec<WatchStart>>,
        /// Set when an opened feed is dropped.
        feed_dropped: Arc<AtomicBool>,
    }

    struct FakeFeed {
        steps: VecDeque<WatchStep>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for FakeFeed {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tonic::async_trait]
    impl WatchFeed for FakeFeed {
        async fn next(&mut self) -> WatchStep {
            match self.steps.pop_front() {
                Some(step) => step,
                None => std::future::pending().await,
            }
        }
    }

    #[tonic::async_trait]
    impl EtcdRevision for FakeWatch {
        async fn revision(&self) -> Result<i64, StoreGone> {
            if self.gone { Err(StoreGone) } else { Ok(100) }
        }
    }

    #[tonic::async_trait]
    impl EtcdWatchStore for FakeWatch {
        type Feed = FakeFeed;

        async fn watch_from(
            &self,
            _prefix: &str,
            start: WatchStart,
        ) -> Result<OpenedWatch<FakeFeed>, WatchEnd> {
            self.asked.lock().expect("asked").push(start);
            if let Some(end) = &self.refusal {
                return Err(end.clone());
            }
            Ok(OpenedWatch {
                revision: 100,
                feed: FakeFeed {
                    steps: self.steps.clone().into(),
                    dropped: Arc::clone(&self.feed_dropped),
                },
            })
        }
    }

    const ID: ServerIdentity = ServerIdentity {
        cluster_id: 1,
        member_id: 2,
    };

    fn event(revision: i64) -> Event {
        Event {
            kv: Some(KeyValue {
                mod_revision: revision,
                ..KeyValue::default()
            }),
            ..Event::default()
        }
    }

    /// Drive the REAL protocol loop over an in-memory request stream,
    /// collecting up to `want` replies — including a stream-ending `Err`.
    async fn drive_raw(
        st: Arc<FakeWatch>,
        reqs: Vec<WatchRequest>,
        want: usize,
    ) -> Vec<Result<WatchResponse, Status>> {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(8);
        for r in reqs {
            req_tx.send(Ok(r)).await.expect("queue");
        }
        drop(req_tx);
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(run_watch_loop(
            st,
            ID,
            tokio_stream::wrappers::ReceiverStream::new(req_rx),
            tx,
        ));
        let mut out = Vec::new();
        while out.len() < want {
            match tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await {
                Ok(Some(r)) => out.push(r),
                _ => break,
            }
        }
        out
    }

    /// [`drive_raw`] where every reply must be a response, not a status.
    async fn drive(st: FakeWatch, reqs: Vec<WatchRequest>, want: usize) -> Vec<WatchResponse> {
        drive_raw(Arc::new(st), reqs, want)
            .await
            .into_iter()
            .map(|r| r.expect("a response, not a stream-ending status"))
            .collect()
    }

    fn create_req(key: &str, start_revision: i64) -> WatchRequest {
        create_req_with_id(key, start_revision, 0)
    }

    fn create_req_with_id(key: &str, start_revision: i64, watch_id: i64) -> WatchRequest {
        WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(
                crate::pb::etcdserverpb::WatchCreateRequest {
                    key: key.as_bytes().to_vec(),
                    start_revision,
                    watch_id,
                    ..Default::default()
                },
            )),
        }
    }

    fn cancel_req(watch_id: i64) -> WatchRequest {
        WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(
                crate::pb::etcdserverpb::WatchCancelRequest { watch_id },
            )),
        }
    }

    #[tokio::test]
    async fn a_watch_delivers_its_events_after_the_acknowledgement() {
        let st = FakeWatch {
            steps: vec![WatchStep::Event(event(5)), WatchStep::Event(event(6))],
            ..FakeWatch::default()
        };
        let out = drive(st, vec![create_req("/registry/pods/", 0)], 3).await;
        assert!(out[0].created, "the ack comes first");
        assert_eq!(out.len(), 3, "ack + two events, got {}", out.len());
        assert!(out[1..].iter().all(|r| r.events.len() == 1 && !r.canceled));
        assert!(out[1..].iter().all(|r| r.watch_id == out[0].watch_id));
        let headers: Vec<i64> = out[1..]
            .iter()
            .map(|r| r.header.as_ref().expect("header").revision)
            .collect();
        assert_eq!(
            headers,
            vec![5, 6],
            "each header carries its event's revision, never 0"
        );
    }

    /// ★ T3.8: every end of a feed reaches the client as a cancel with its
    /// reason. A watch that just stops leaves a client believing it is
    /// still tracking the cluster.
    #[tokio::test]
    async fn every_end_of_a_watch_is_sent_as_a_cancel_with_its_reason() {
        for end in [
            WatchEnd::Compacted {
                compact_revision: 7,
            },
            WatchEnd::Overflow { last_seen: 9 },
            WatchEnd::StoreGone(StoreGone),
        ] {
            let st = FakeWatch {
                steps: vec![WatchStep::Event(event(3)), WatchStep::End(end.clone())],
                ..FakeWatch::default()
            };
            let out = drive(st, vec![create_req("/registry/pods/", 0)], 4).await;
            assert_eq!(out.len(), 3, "ack, event, cancel for {end:?}: {out:?}");
            let cancel = &out[2];
            assert!(cancel.canceled, "{end:?} must end in a cancel");
            assert_eq!(cancel.watch_id, out[0].watch_id);
            assert!(!cancel.cancel_reason.is_empty(), "{end:?} must say why");
            assert_eq!(cancel.cancel_reason, end.to_string());
            assert_eq!(cancel.compact_revision, end.compact_revision());
        }
    }

    #[tokio::test]
    async fn a_server_assigned_watch_id_is_unique_per_watch() {
        // Reusing an id silently merges two watches from the client's view.
        let out = drive(
            FakeWatch::default(),
            vec![
                create_req("/registry/pods/", 0),
                create_req("/registry/services/", 0),
            ],
            2,
        )
        .await;
        assert_eq!(out.len(), 2);
        assert_ne!(out[0].watch_id, out[1].watch_id);
    }

    /// A second create for an open client-chosen id is refused the way etcd
    /// refuses it: one response, `created` and `canceled`, under the invalid
    /// id -1. Sent under 7 instead, the refusal reads as "watch 7 is over"
    /// while watch 7 keeps delivering, which is the merge it is meant to
    /// prevent.
    #[tokio::test]
    async fn a_client_chosen_id_already_open_is_refused_not_merged() {
        let st = FakeWatch {
            steps: vec![WatchStep::Event(event(5))],
            ..FakeWatch::default()
        };
        let out = drive(
            st,
            vec![
                create_req_with_id("/registry/pods/", 0, 7),
                create_req_with_id("/registry/services/", 0, 7),
                create_req("/registry/", 0),
            ],
            6,
        )
        .await;
        // Two acks, one refusal, and one event for each open watch.
        assert_eq!(out.len(), 5, "{out:?}");
        let about =
            |id: i64| -> Vec<&WatchResponse> { out.iter().filter(|r| r.watch_id == id).collect() };

        let refusals = about(INVALID_WATCH_ID);
        assert_eq!(refusals.len(), 1, "{out:?}");
        assert!(refusals[0].created && refusals[0].canceled, "{out:?}");
        assert_eq!(refusals[0].cancel_reason, DuplicateWatchId.to_string());

        let seven = about(7);
        assert!(
            seven.iter().all(|r| !r.canceled),
            "the refusal must not cancel the open watch 7: {out:?}"
        );
        assert!(seven[0].created, "7 is acknowledged first");
        assert_eq!(
            seven.iter().filter(|r| !r.events.is_empty()).count(),
            1,
            "and keeps delivering: {out:?}"
        );

        let assigned: Vec<&WatchResponse> = out
            .iter()
            .filter(|r| r.created && !r.canceled && r.watch_id != 7)
            .collect();
        assert_eq!(assigned.len(), 1, "{out:?}");
        assert_ne!(
            assigned[0].watch_id, INVALID_WATCH_ID,
            "a server-assigned id is a real one, and skips the open 7"
        );
    }

    #[tokio::test]
    async fn a_refused_watch_is_acknowledged_then_cancelled_with_its_reason() {
        // Serving nothing and saying nothing would leave the client
        // believing it is tracking the cluster when it is not.
        let st = FakeWatch {
            refusal: Some(WatchEnd::StoreGone(StoreGone)),
            steps: vec![WatchStep::Event(event(1))],
            ..FakeWatch::default()
        };
        let out = drive(st, vec![create_req("/registry/pods/", 1)], 3).await;
        assert_eq!(out.len(), 2, "no event follows a refusal: {out:?}");
        assert!(out[0].created);
        assert!(out[1].canceled, "must cancel, not serve");
        assert_eq!(out[1].cancel_reason, StoreGone.to_string());
    }

    /// A client cancel stops the watch — its feed is dropped before the
    /// acknowledgement goes out, so nothing more is forwarded on an id the
    /// client may now reuse.
    #[tokio::test]
    async fn a_client_cancel_stops_the_watch_before_it_is_acknowledged() {
        let st = FakeWatch::default();
        let dropped = Arc::clone(&st.feed_dropped);
        let out = drive(st, vec![create_req("/registry/pods/", 0), cancel_req(1)], 2).await;
        assert!(out[0].created);
        assert!(out[1].canceled);
        assert_eq!(out[1].watch_id, 1);
        assert_eq!(
            out[1].cancel_reason,
            WatchEnd::CancelledByClient.to_string()
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "the cancelled watch's feed is still being forwarded"
        );
    }

    #[tokio::test]
    async fn a_progress_request_answers_with_the_current_revision() {
        // Lets an idle client checkpoint without waiting for traffic.
        let progress = WatchRequest {
            request_union: Some(RequestUnion::ProgressRequest(
                crate::pb::etcdserverpb::WatchProgressRequest {},
            )),
        };
        let out = drive(FakeWatch::default(), vec![progress], 1).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].header.as_ref().expect("header").revision, 100);
    }

    #[tokio::test]
    async fn a_progress_request_on_a_gone_store_ends_the_stream_unavailable() {
        // Revision 0 would be a checkpoint at a revision nobody holds.
        let progress = WatchRequest {
            request_union: Some(RequestUnion::ProgressRequest(
                crate::pb::etcdserverpb::WatchProgressRequest {},
            )),
        };
        let st = Arc::new(FakeWatch {
            gone: true,
            ..FakeWatch::default()
        });
        let out = drive_raw(st, vec![progress], 1).await;
        let status = out
            .into_iter()
            .next()
            .expect("a reply")
            .expect_err("a gone store has no revision to report");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn a_compacted_start_cancels_with_the_watermark_and_nothing_follows() {
        // THE property. A short answer here is the silent divergence the
        // service exists to prevent: the client would believe it had
        // caught up.
        let st = FakeWatch {
            refusal: Some(WatchEnd::Compacted {
                compact_revision: 50,
            }),
            steps: vec![WatchStep::Event(event(60))],
            ..FakeWatch::default()
        };
        let out = drive(st, vec![create_req("/registry/pods/", 10)], 3).await;
        assert!(out[0].created);
        assert!(out[1].canceled, "a gap must CANCEL, not return short");
        assert_eq!(
            out[1].compact_revision, 50,
            "the watermark tells the client where it may safely resume"
        );
        assert_eq!(out.len(), 2, "nothing after a cancel");
    }

    #[test]
    fn start_revision_is_parsed_once_zero_is_now_not_the_beginning() {
        assert_eq!(WatchStart::from_wire(0), WatchStart::Now);
        assert_eq!(
            WatchStart::from_wire(5),
            WatchStart::At(NonZeroU64::new(5).expect("non-zero"))
        );
        assert_eq!(WatchStart::from_wire(-1), WatchStart::BeforeHistory);
        assert_eq!(WatchStart::from_wire(i64::MIN), WatchStart::BeforeHistory);
    }

    #[tokio::test]
    async fn the_store_is_asked_for_the_start_the_client_sent() {
        // Treating 0 as "all history" would flood a client that asked for
        // a live tail — and on a large cluster, hang it.
        let st = FakeWatch::default();
        let open = create_watch(&st, ID, 1, b"/registry/", 0).await;
        assert!(matches!(open, CreatedWatch::Open { ref ack, .. } if ack.created));
        let _ = create_watch(&st, ID, 2, b"/registry/", 9).await;
        assert_eq!(
            *st.asked.lock().expect("asked"),
            vec![
                WatchStart::Now,
                WatchStart::At(NonZeroU64::new(9).expect("non-zero"))
            ]
        );
    }
}
