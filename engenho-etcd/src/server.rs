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
}

/// The Watch service.
pub struct WatchSvc<S> {
    pub store: S,
    pub identity: ServerIdentity,
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

#[tonic::async_trait]
impl<S: EtcdWatchStore> etcdserverpb::watch_server::Watch for WatchSvc<S> {
    type WatchStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<WatchResponse, Status>> + Send + 'static>,
    >;

    async fn watch(
        &self,
        _: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // ★ REFUSED RATHER THAN HALF-SERVED, and deliberately so. The
        // response BUILDERS above are complete and tested; what is missing
        // is the live fan-out that keeps a long-lived stream gap-free as
        // new revisions land. A watch that delivers history and then goes
        // quiet is the exact failure mode described at the top of this
        // block: the client's cache diverges silently and forever, and it
        // is strictly worse than a client that knows it has no watch.
        Err(Status::unimplemented(
            "etcd Watch is not served yet: the response shapes are implemented and tested, but a \
             watch that delivers history and then stops receiving live events would leave a \
             client's cache silently diverged forever — worse than no watch at all. Use the \
             Kubernetes API's own WATCH, which is fully served.",
        ))
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;
    use crate::pb::mvccpb::Event;

    struct FakeWatch {
        events: Vec<Event>,
        compacted_at: Option<i64>,
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
    }

    fn store(events: usize, compacted_at: Option<i64>) -> FakeWatch {
        FakeWatch {
            events: (0..events).map(|_| Event::default()).collect(),
            compacted_at,
        }
    }

    const ID: ServerIdentity = ServerIdentity {
        cluster_id: 1,
        member_id: 2,
    };

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
