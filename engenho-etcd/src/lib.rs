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
//! ## What is NOT here yet
//!
//! The gRPC services themselves (`KV`, `Watch`, `Lease`, `Maintenance`).
//! They are mechanical relative to the above — `Range` in, `RangeResponse`
//! out — and the store already supplies the hard semantics: `Revision(u64)`,
//! `VersionMeta` create/mod revisions, `Change`, `CompactedTooOld` and a
//! `watch_from` stream are all in `engenho-store` today. Stated plainly so
//! nobody reads this crate as finished: **there is no listener on :2379.**

pub mod keyspace;
pub mod kv;

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
    /// `authpb` — role/user messages. Present because `etcdserverpb`
    /// imports it; engenho's authn lives at the apiserver, so the `Auth`
    /// RPCs are permission-denied stubs (theory/ENGENHO.md III.2).
    pub mod authpb {
        include!(concat!(env!("OUT_DIR"), "/authpb.rs"));
    }
    /// `etcdserverpb` — the KV / Watch / Lease / Maintenance messages.
    pub mod etcdserverpb {
        include!(concat!(env!("OUT_DIR"), "/etcdserverpb.rs"));
    }
}
