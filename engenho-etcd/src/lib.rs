//! etcd v3 wire façade — the INTERFACE engenho owes the world, over the
//! technology engenho actually chose.
//!
//! ★ THE THESIS. engenho runs no etcd. It runs a journalled, partitioned
//! segment store, and that is a deliberate choice about TECHNOLOGY. It is
//! not the whole obligation, because nothing in the ecosystem ever asks
//! "do you have etcd?" — it asks to `Range` a keyspace, to take a snapshot,
//! to be pointed at `--etcd-servers`. Those verbs are the CONTRACT, and a
//! contract is load-bearing even when the thing behind it is replaced.
//! Satisfy them and engenho is transparently substitutable for k3s; skip
//! them and it is a Kubernetes-shaped thing no existing runbook, backup
//! tool or dashboard can drive.
//!
//! Establish the interface first; then do as we like underneath.
//!
//! ★ WHAT IS MET TODAY, so the thesis is not read as done. Of the three
//! verbs above only the first is met: `Range`, `Watch` and
//! `Maintenance.Status` answer. `Snapshot` is refused, and a kube-apiserver
//! pointed at `--etcd-servers` cannot run against this façade: it writes
//! through `Txn`, holds `Lease`s and calls `Compact`, and none of them is
//! served. engenho is not yet a drop-in for k3s's etcd.
//!
//! ## What is here
//!
//! * [`keyspace`] — the `/registry` layout: how a Kubernetes object is
//!   ADDRESSED. This is the load-bearing half and the half that cannot be
//!   vendored, because a wrong key does not error, it returns an empty
//!   range that reads exactly like an empty cluster.
//! * `vendor/proto/etcd/api/` — the REAL `etcdserverpb`, `mvccpb` and
//!   `authpb` definitions, fetched from `etcd-io/etcd@release-3.5` rather
//!   than reconstructed. Wire formats are never written from memory; the
//!   only edit applied is the removal of `gogoproto` and
//!   `google.api.http` options, which are Go codegen and grpc-gateway
//!   hints that do not affect encoding. Every field number and type is
//!   upstream's, byte for byte.
//!
//! * [`server`] — the gRPC services: a read-only `KV`, `Watch` and
//!   `Maintenance`, over three store traits. `engenho-runtime`'s
//!   `MeshEtcdStore` implements the traits over the live store, and the
//!   runtime mounts these three services, and only these, on
//!   `runtime.etcd_listen_addr` when that field is set.
//!
//! ## Who can call it: anyone who can reach the socket
//!
//! The façade authenticates nobody. There is no TLS, no client certificate
//! and no authorization, and `Range` returns every object the store serves
//! — Secrets included — as the JSON it holds. Its one control is where it
//! binds: `engenho-config` refuses a `runtime.etcd_listen_addr` that is not
//! a loopback `IP:port` literal until the façade has mutual TLS
//! (`NodeLocalListener::EtcdFacade`, T4.9). That is a check at config
//! validation, not a type. And loopback keeps the façade off the network,
//! not away from the host: any process sharing the host's network
//! namespace can read it, which on a node running the native backend is
//! every workload.
//!
//! ## The rules the services keep (T3.8)
//!
//! * **A gone store is never an empty answer.** Every read trait returns
//!   `Result<_, server::StoreGone>`, and `watch_from` the `WatchEnd` that
//!   carries it. `Range`, `Status`, `Alarm` and `Defragment` answer a gone
//!   store with gRPC `Unavailable`; a watch, opening or open, ends with a
//!   `canceled` response whose reason names it; a progress request ends
//!   the whole watch stream `Unavailable`. An `Ok` Range with no keys at
//!   revision 0 would be indistinguishable from an empty cluster.
//! * **A watch is one atomic `watch_from(prefix, start)`**, and every end
//!   of it — compaction, overflow, the store going away, a client cancel —
//!   is sent as a `canceled` response carrying its reason. The exceptions
//!   are a client that hangs up, which has nobody left to tell, and the
//!   progress request above.
//! * **Every `Range` shape is answered, none by accident empty**: a point,
//!   a prefix, an interval and `--from-key` are each one scan under the
//!   prefix all their keys share, then filtered ([`kv::RangeShape`]).
//! * **`Status` never sends a database size of 0**, which crashes
//!   `etcdctl endpoint status`; an unmeasured size is `Unavailable`
//!   ([`server::DbSizeUnmeasured`]).
//!
//! ## What is NOT here
//!
//! Writes (`Put`, `DeleteRange`, `Txn`, `Compact`) and `Snapshot` are
//! refused with `Unimplemented` and a message saying why; `Hash`, `HashKV`,
//! `MoveLeader` and `Downgrade` are refused with `Unimplemented` too. No
//! `Lease`, `Cluster` or `Auth` service is implemented here or mounted by
//! the runtime, so a call to one gets the transport's bare
//! `Unimplemented`. Every node of every engenho cluster reports the same
//! `cluster_id` and `member_id` (see [`server::ServerIdentity`]).

pub mod keyspace;
pub mod kv;
pub mod server;

/// The etcd v3 wire types, generated from the vendored upstream protos.
///
/// `mvccpb` carries `KeyValue` and `Event`; `etcdserverpb` carries every
/// request/response. Generated at build time by protox + prost — no
/// `protoc` on the build host, the same route `engenho-kube-proto` takes.
pub mod pb {
    /// `mvccpb` — `KeyValue`, `Event`.
    pub mod mvccpb {
        include!(concat!(env!("OUT_DIR"), "/mvccpb.rs"));
    }
    /// `authpb` — role/user messages. Present only because `etcdserverpb`
    /// imports it. No `Auth` service is implemented or mounted, and the
    /// façade authenticates no caller at all (see the crate docs);
    /// theory/ENGENHO.md §III.2's permission-denied stubs are design only.
    pub mod authpb {
        include!(concat!(env!("OUT_DIR"), "/authpb.rs"));
    }
    /// `etcdserverpb` — the KV / Watch / Lease / Maintenance messages.
    pub mod etcdserverpb {
        include!(concat!(env!("OUT_DIR"), "/etcdserverpb.rs"));
    }
}
