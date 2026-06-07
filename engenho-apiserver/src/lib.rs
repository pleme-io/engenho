//! # engenho-apiserver
//!
//! The HTTP K8s API surface — operators (kubectl, controllers,
//! clients) send REST requests; this server translates them into
//! typed [`engenho_store::ResourceCommand`] proposals through the
//! distributed [`StoreMesh`].
//!
//! ## Layered above
//!
//!   * Layer A (chitchat gossip)            → engenho-revoada
//!   * Layer B (openraft consensus)         → engenho-store
//!   * Layer C (policy)                     → engenho-revoada
//!   * Layer D (BLAKE3+ed25519 chain)       → engenho-revoada/store
//!   * **Layer R7 (HTTP K8s API)**          → this crate
//!
//! ## Architecture
//!
//! Single struct [`ApiServer`] wrapping `Arc<StoreMesh>`. Axum
//! router with kubectl-shaped routes; every handler decomposes
//! into [`engenho_store::ResourceCommand::{Put,Patch,Delete}`] or
//! a local catalog [`StoreMesh::get`]/`list`.
//!
//! ## Traited abstraction
//!
//! [`ResourceHandler`] trait — per-kind CRUD. The default impl
//! [`StoreBackedHandler`] works for any kind (opaque JSON store).
//! Future R7.5+ may add typed handlers per K8s kind using
//! engenho-types for schema validation.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod discovery;
pub mod error;
pub mod handler;
pub mod openapi;
pub mod params;
pub mod router;
pub mod server;

pub use discovery::{
    APIGroup, APIGroupList, APIResource, APIResourceList, APIVersions, GroupVersionForDiscovery,
    ServerAddressByClientCIDR,
};
pub use error::{ApiError, ErrorKind, status_object};
pub use handler::{
    ResourceHandler, StoreBackedHandler, gone_to_api_error, handlers_from_catalog,
};
pub use openapi::ApiDoc;
pub use params::{
    ListWatchParams, ResumePoint, Selectors, body_precondition, bookmark_line, gvk_ns_matches,
    status_410_line, to_k8s_watch_line,
};
pub use router::{RouterState, build};
pub use server::ApiServer;
