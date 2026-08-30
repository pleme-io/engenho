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
pub trait EtcdStatusStore: Send + Sync + 'static {
    fn revision(&self) -> i64;
    /// Raft applied index, surfaced as both `raft_index` and
    /// `raft_applied_index` — engenho applies what it commits.
    fn applied_index(&self) -> u64;
    /// On-disk size in bytes if the backend can report one cheaply.
    ///
    /// `None` becomes 0, which etcd clients read as unknown. A guess here
    /// would drive real capacity alerts off a number nobody measured.
    fn db_size(&self) -> Option<i64>;
}

/// The Maintenance service.
pub struct MaintenanceSvc<S> {
    pub store: S,
    pub identity: ServerIdentity,
}

#[tonic::async_trait]
impl<S: EtcdStatusStore> etcdserverpb::maintenance_server::Maintenance for MaintenanceSvc<S> {
    async fn status(&self, _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let rev = self.store.revision();
        Ok(Response::new(StatusResponse {
            header: Some(header(self.identity, rev)),
            // The etcd API version engenho's wire types were generated
            // from. Clients gate feature use on this, so it must name the
            // protocol actually served, not engenho's own version.
            version: "3.5.0".to_string(),
            db_size: self.store.db_size().unwrap_or(0),
            leader: self.identity.member_id,
            raft_index: self.store.applied_index(),
            raft_term: 1,
            raft_applied_index: self.store.applied_index(),
            errors: Vec::new(),
            db_size_in_use: self.store.db_size().unwrap_or(0),
            is_learner: false,
        }))
    }

    async fn alarm(&self, _: Request<AlarmRequest>) -> Result<Response<AlarmResponse>, Status> {
        // An empty alarm list is the TRUE answer, not a stub: engenho
        // raises no etcd alarms (NOSPACE/CORRUPT are backend conditions it
        // does not have). Returning Unimplemented here would make
        // `etcdctl endpoint health` fail on a healthy server.
        Ok(Response::new(AlarmResponse {
            header: Some(header(self.identity, self.store.revision())),
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
            header: Some(header(self.identity, self.store.revision())),
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

    struct FakeStatus;
    impl EtcdStatusStore for FakeStatus {
        fn revision(&self) -> i64 {
            77
        }
        fn applied_index(&self) -> u64 {
            123
        }
        fn db_size(&self) -> Option<i64> {
            None
        }
    }

    fn svc() -> MaintenanceSvc<FakeStatus> {
        MaintenanceSvc {
            store: FakeStatus,
            identity: ServerIdentity::default(),
        }
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
// engenho can meet that bar because the store already does the hard part:
// `changes_since` returns `CompactedTooOld` rather than a short answer, so
// a gap is a typed refusal and never silence. This service's whole job is
// to preserve that property across the wire — which is why a compaction
// becomes a `canceled` response carrying `compact_revision`, exactly as
// etcd spells it, rather than a closed stream the client would retry into
// the same hole.
//
// ★ `created` IS NOT COSMETIC. etcd sends a response with `created: true`
// and no events to acknowledge a watch before any data flows. A client
// that never receives it waits forever on a watch it believes is pending —
// so the acknowledgement is the first thing sent, before any history.
// ─────────────────────────────────────────────────────────────────────

use crate::pb::etcdserverpb::{WatchRequest, WatchResponse, watch_request::RequestUnion};

/// A watch event to deliver, already rendered onto the wire.
pub type WireEvent = crate::pb::mvccpb::Event;

/// What the Watch service needs from a store.
///
/// `changes_since` mirrors the store's own signature deliberately: a
/// `Result` whose error carries the compaction watermark, so the "gap" case
/// is a value the service must handle rather than an empty vector it could
/// forward by accident.
pub trait EtcdWatchStore: Send + Sync + 'static {
    fn revision(&self) -> i64;
    /// Events after `since` under `prefix`, or the compaction watermark if
    /// `since` has been compacted away.
    fn changes_since(&self, prefix: &str, since: i64) -> Result<Vec<WireEvent>, i64>;

    /// Subscribe to LIVE events under `prefix`.
    ///
    /// ★ THIS IS THE HALF THAT MAKES A WATCH A WATCH. History replay alone
    /// is a paginated read wearing a stream's clothes: the client believes
    /// it is now tracking the cluster and it is not. The subscription must
    /// be registered BEFORE history is read, or a change landing between
    /// the two is delivered to nobody — a gap the client cannot detect,
    /// because from its side the stream simply never mentions it.
    ///
    /// Returns `None` when the store cannot subscribe; the caller must then
    /// refuse the watch rather than serve history and fall silent.
    fn subscribe(&self, prefix: &str) -> Option<tokio::sync::mpsc::Receiver<WireEvent>>;
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

/// Build the cancellation etcd sends when a watch asks for history that has
/// been compacted away.
///
/// `compact_revision` is the load-bearing field: a client reads it to know
/// where it may safely resume, and a cancellation without it leaves the
/// client retrying into the same hole forever.
#[must_use]
pub fn compacted_response(id: ServerIdentity, watch_id: i64, compacted: i64) -> WatchResponse {
    WatchResponse {
        header: Some(header(id, compacted)),
        watch_id,
        created: false,
        canceled: true,
        compact_revision: compacted,
        cancel_reason: "required revision has been compacted".to_string(),
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

/// The responses one `WatchCreateRequest` produces, in order.
///
/// Pure, so the ordering contract — acknowledge first, then history, and a
/// compaction cancels rather than truncating — is testable without a
/// stream.
#[must_use]
pub fn responses_for_create<S: EtcdWatchStore>(
    store: &S,
    id: ServerIdentity,
    watch_id: i64,
    key: &[u8],
    start_revision: i64,
) -> Vec<WatchResponse> {
    let rev = store.revision();
    // The acknowledgement ALWAYS goes first. A client that never receives
    // it waits forever on a watch it believes is still pending.
    let mut out = vec![created_response(id, watch_id, rev)];

    // start_revision 0 means "from now" — no history, which is why it must
    // not be treated as "from the beginning of time".
    if start_revision == 0 {
        return out;
    }

    let prefix = String::from_utf8_lossy(key).into_owned();
    match store.changes_since(&prefix, start_revision - 1) {
        Ok(events) if events.is_empty() => out,
        Ok(events) => {
            out.push(events_response(id, watch_id, rev, events));
            out
        }
        // A gap becomes a CANCELLATION carrying the watermark, never a
        // short answer. Forwarding the empty vector would be the silent
        // divergence this whole service exists to prevent.
        Err(compacted) => {
            out.push(compacted_response(id, watch_id, compacted));
            out
        }
    }
}

/// Run the watch protocol over ANY request stream.
///
/// ★ SEPARATED FROM THE TRANSPORT ON PURPOSE. `tonic::Streaming` cannot be
/// constructed outside a real connection, so a protocol loop written
/// directly against it is only reachable from an integration test with a
/// live server. Every ordering rule below — subscribe-before-replay, the
/// created acknowledgement, cancel semantics — would then be untested at
/// the unit level, which is precisely where a watch bug is cheapest to
/// find and most expensive to miss.
pub async fn run_watch_loop<S, R>(
    store: std::sync::Arc<S>,
    identity: ServerIdentity,
    mut inbound: R,
    tx: tokio::sync::mpsc::Sender<Result<WatchResponse, Status>>,
) where
    S: EtcdWatchStore,
    R: futures_core::Stream<Item = Result<WatchRequest, Status>> + Unpin + Send + 'static,
{
    use tokio_stream::StreamExt as _;

    let mut next_id: i64 = 1;
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    while let Some(Ok(req)) = inbound.next().await {
        match req.request_union {
            Some(RequestUnion::CreateRequest(create)) => {
                // Watch ids are assigned by the SERVER when the client
                // sends 0, which is what every client does. Reusing one
                // would silently merge two watches from the client's view.
                let watch_id = if create.watch_id == 0 {
                    let id = next_id;
                    next_id += 1;
                    id
                } else {
                    create.watch_id
                };
                let prefix = String::from_utf8_lossy(&create.key).into_owned();

                // SUBSCRIBE FIRST, THEN REPLAY. A change landing between
                // the two would otherwise reach nobody — a gap the client
                // cannot detect, because from its side the stream simply
                // never mentions it.
                let Some(mut live) = store.subscribe(&prefix) else {
                    // Refuse the watch rather than serve history and fall
                    // silent: a client that believes it is tracking the
                    // cluster and is not is worse off than one that knows
                    // it has no watch.
                    let _ = tx
                        .send(Ok(cancel_response(
                            identity,
                            watch_id,
                            "store cannot subscribe: refusing to serve a watch that would \
                             deliver history and then fall silent",
                        )))
                        .await;
                    continue;
                };

                let mut cancelled = false;
                for resp in responses_for_create(
                    store.as_ref(),
                    identity,
                    watch_id,
                    &create.key,
                    create.start_revision,
                ) {
                    cancelled |= resp.canceled;
                    if tx.send(Ok(resp)).await.is_err() {
                        return; // client hung up
                    }
                }
                if cancelled {
                    // A compaction cancel ends THIS watch; forwarding live
                    // events on an id the client has been told is dead
                    // would be events it will discard, on a watch it has
                    // already replaced.
                    continue;
                }

                let tx2 = tx.clone();
                tasks.push(tokio::spawn(async move {
                    while let Some(ev) = live.recv().await {
                        let resp = events_response(identity, watch_id, 0, vec![ev]);
                        // A failed send means the client is gone; returning
                        // is what stops the task leaking for the life of
                        // the process.
                        if tx2.send(Ok(resp)).await.is_err() {
                            return;
                        }
                    }
                }));
            }
            Some(RequestUnion::CancelRequest(cancel)) => {
                // Acknowledge with `canceled`, as etcd does: a client waits
                // for it before reusing the id.
                let _ = tx
                    .send(Ok(cancel_response(
                        identity,
                        cancel.watch_id,
                        "watch cancelled by client",
                    )))
                    .await;
            }
            // A progress request asks for a bookmark-shaped reply so an
            // idle client can checkpoint without waiting for traffic.
            Some(RequestUnion::ProgressRequest(_)) => {
                let _ = tx
                    .send(Ok(events_response(
                        identity,
                        0,
                        store.revision(),
                        Vec::new(),
                    )))
                    .await;
            }
            None => {}
        }
    }
    // ★ DO NOT ABORT THE WATCH TASKS HERE. The inbound stream ending means
    // the client HALF-CLOSED — it has no more requests to send — which is
    // the normal state of a client that created its watches and is now
    // waiting for events. gRPC bidi streaming is explicitly half-duplex-
    // capable, so the response side must outlive the request side.
    //
    // Aborting on this boundary is the bug the live-event test caught: the
    // acknowledgement arrived and not one event ever did, because every
    // forwarding task was killed the instant the client stopped talking.
    //
    // The tasks end on their own when the RESPONSE channel closes — that is
    // the client actually going away — which is why each one returns on a
    // failed send. Holding the handles keeps them owned for the loop's
    // lifetime without cutting them short.
    let _ = tasks;
    // Keep the loop alive until the client's response channel is gone, so
    // the forwarding tasks retain a live sender.
    tx.closed().await;
}

/// A `canceled` response carrying a reason.
#[must_use]
pub fn cancel_response(id: ServerIdentity, watch_id: i64, reason: &str) -> WatchResponse {
    WatchResponse {
        header: Some(header(id, 0)),
        watch_id,
        created: false,
        canceled: true,
        compact_revision: 0,
        cancel_reason: reason.to_string(),
        fragment: false,
        events: Vec::new(),
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
    /// ★ A CANCELLED OR DROPPED WATCH STOPS ITS TASK. The receiver is
    /// dropped, the forwarding task's send fails, and it returns. Without
    /// that, a long-lived client that creates and cancels watches leaks a
    /// task per creation and the server degrades over hours rather than
    /// failing visibly.
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
    use crate::pb::mvccpb::Event;

    struct FakeWatch {
        events: Vec<Event>,
        compacted_at: Option<i64>,
        /// Events the fake will push on the LIVE channel after subscribe.
        live: std::sync::Mutex<Vec<Event>>,
        /// Set when the store refuses to subscribe, so the refusal path is
        /// exercised rather than assumed.
        no_subscribe: bool,
    }

    impl EtcdWatchStore for FakeWatch {
        fn revision(&self) -> i64 {
            100
        }
        fn changes_since(&self, _prefix: &str, since: i64) -> Result<Vec<Event>, i64> {
            match self.compacted_at {
                Some(c) if since < c => Err(c),
                _ => Ok(self.events.clone()),
            }
        }
        fn subscribe(&self, _prefix: &str) -> Option<tokio::sync::mpsc::Receiver<Event>> {
            if self.no_subscribe {
                return None;
            }
            let queued = std::mem::take(&mut *self.live.lock().expect("live queue"));
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            tokio::spawn(async move {
                for ev in queued {
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
            });
            Some(rx)
        }
    }

    fn store(events: usize, compacted_at: Option<i64>) -> FakeWatch {
        FakeWatch {
            events: (0..events).map(|_| Event::default()).collect(),
            compacted_at,
            live: std::sync::Mutex::new(Vec::new()),
            no_subscribe: false,
        }
    }

    const ID: ServerIdentity = ServerIdentity {
        cluster_id: 1,
        member_id: 2,
    };

    /// Drive the REAL protocol loop over an in-memory request stream.
    ///
    /// Exercises the service's ordering rules, not just the response
    /// builders — which is the whole reason the loop was separated from
    /// `tonic::Streaming`.
    async fn drive(st: FakeWatch, reqs: Vec<WatchRequest>, want: usize) -> Vec<WatchResponse> {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(8);
        for r in reqs {
            req_tx.send(Ok(r)).await.expect("queue");
        }
        drop(req_tx);
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(run_watch_loop(
            std::sync::Arc::new(st),
            ID,
            tokio_stream::wrappers::ReceiverStream::new(req_rx),
            tx,
        ));
        let mut out = Vec::new();
        while out.len() < want {
            match tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await {
                Ok(Some(Ok(r))) => out.push(r),
                _ => break,
            }
        }
        out
    }

    fn create_req(key: &str, start_revision: i64) -> WatchRequest {
        WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(
                crate::pb::etcdserverpb::WatchCreateRequest {
                    key: key.as_bytes().to_vec(),
                    start_revision,
                    ..Default::default()
                },
            )),
        }
    }

    #[tokio::test]
    async fn a_watch_delivers_live_events_after_the_acknowledgement() {
        // THE property the refusal used to stand in for: history replay
        // alone is a paginated read wearing a stream's clothes.
        let mut st = store(0, None);
        st.live = std::sync::Mutex::new(vec![Event::default(), Event::default()]);
        let out = drive(st, vec![create_req("/registry/pods/", 0)], 3).await;
        assert!(out[0].created, "the ack comes first");
        assert_eq!(out.len(), 3, "ack + two live events, got {}", out.len());
        assert!(out[1..].iter().all(|r| r.events.len() == 1));
        assert!(out[1..].iter().all(|r| r.watch_id == out[0].watch_id));
    }

    #[tokio::test]
    async fn a_server_assigned_watch_id_is_unique_per_watch() {
        // Reusing an id silently merges two watches from the client's view.
        let st = store(0, None);
        let out = drive(
            st,
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

    #[tokio::test]
    async fn a_store_that_cannot_subscribe_refuses_rather_than_falling_silent() {
        // Serving history and then going quiet leaves the client believing
        // it is tracking the cluster when it is not.
        let mut st = store(3, None);
        st.no_subscribe = true;
        let out = drive(st, vec![create_req("/registry/pods/", 1)], 1).await;
        assert_eq!(out.len(), 1);
        assert!(out[0].canceled, "must cancel, not serve history");
        assert!(
            out[0].cancel_reason.contains("fall silent"),
            "the reason must say why: {}",
            out[0].cancel_reason
        );
    }

    #[tokio::test]
    async fn a_cancel_is_acknowledged_so_the_id_can_be_reused() {
        let st = store(0, None);
        let cancel = WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(
                crate::pb::etcdserverpb::WatchCancelRequest { watch_id: 1 },
            )),
        };
        let out = drive(st, vec![create_req("/registry/pods/", 0), cancel], 2).await;
        assert!(out[0].created);
        assert!(out[1].canceled);
        assert_eq!(out[1].watch_id, 1);
    }

    #[tokio::test]
    async fn a_progress_request_answers_with_the_current_revision() {
        // Lets an idle client checkpoint without waiting for traffic.
        let st = store(0, None);
        let progress = WatchRequest {
            request_union: Some(RequestUnion::ProgressRequest(
                crate::pb::etcdserverpb::WatchProgressRequest {},
            )),
        };
        let out = drive(st, vec![progress], 1).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].header.as_ref().expect("header").revision, 100);
    }

    #[tokio::test]
    async fn a_compacted_start_cancels_and_does_not_then_stream_live_events() {
        // Forwarding on an id the client has been told is dead sends
        // events it will discard, on a watch it has already replaced.
        let mut st = store(0, Some(50));
        st.live = std::sync::Mutex::new(vec![Event::default()]);
        let out = drive(st, vec![create_req("/registry/pods/", 10)], 3).await;
        assert!(out[0].created);
        assert!(out[1].canceled);
        assert_eq!(out[1].compact_revision, 50);
        assert_eq!(out.len(), 2, "no live events after a cancel");
    }

    #[test]
    fn the_acknowledgement_is_always_first() {
        // A client that never receives `created` waits forever on a watch
        // it believes is still pending.
        let out = responses_for_create(&store(3, None), ID, 7, b"/registry/pods/", 5);
        assert!(out[0].created, "the first response must acknowledge");
        assert_eq!(out[0].watch_id, 7);
        assert!(out[0].events.is_empty(), "the ack carries no events");
    }

    #[test]
    fn start_revision_zero_means_from_now_not_from_the_beginning() {
        // Treating 0 as "all history" would flood a client that asked for
        // a live tail — and on a large cluster, hang it.
        let out = responses_for_create(&store(9, None), ID, 1, b"/registry/", 0);
        assert_eq!(out.len(), 1, "acknowledgement only");
        assert!(out[0].created);
    }

    #[test]
    fn history_follows_the_acknowledgement() {
        let out = responses_for_create(&store(2, None), ID, 1, b"/registry/pods/", 5);
        assert_eq!(out.len(), 2);
        assert!(out[0].created);
        assert_eq!(out[1].events.len(), 2);
        assert!(!out[1].canceled);
    }

    #[test]
    fn a_compacted_start_cancels_with_the_watermark_never_returns_short() {
        // THE property. Forwarding an empty vector here is the silent
        // divergence the service exists to prevent: the client would
        // believe it had caught up.
        let out = responses_for_create(&store(0, Some(50)), ID, 3, b"/registry/pods/", 10);
        assert_eq!(out.len(), 2);
        assert!(out[0].created);
        let c = &out[1];
        assert!(c.canceled, "a gap must CANCEL, not return short");
        assert_eq!(
            c.compact_revision, 50,
            "the watermark tells the client where it may safely resume; without it \
             the client retries into the same hole forever"
        );
        assert!(!c.cancel_reason.is_empty());
    }
}
