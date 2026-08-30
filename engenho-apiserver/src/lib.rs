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

pub mod authn;
pub mod authz;
pub mod coords;
pub mod defaulting;
pub mod discovery;
pub mod error;
pub mod handler;
pub mod health;
pub mod metrics;
pub mod openapi;
pub mod params;
pub mod pki;
pub mod pod_logs;
pub mod router;
pub mod scale;
pub mod schema_validation;
pub mod server;
pub mod table;
pub mod webhook_admission;

pub use authn::{
    AnonymousAuthenticator, Authenticator, AuthnError, BootstrapAdminTokenAuthenticator,
    ChainAuthenticator, RequestCreds, ServiceAccountTokenAuthenticator, X509Authenticator,
};
pub use authz::{
    AllowAllAuthorizer, Attributes, Authorizer, Decision, EffectiveRules, RbacAuthorizer,
    RbacStoreEnv, into_dyn_authorizer, store_env::StoreRbacEnv,
};
pub use coords::{RequestInfo, ResourceCoords, parse_resource_path, resource_verb};
pub use discovery::{
    APIGroup, APIGroupList, APIResource, APIResourceList, APIVersions, GroupVersionForDiscovery,
    ServerAddressByClientCIDR,
};
pub use error::{ApiError, ErrorKind, forbidden_message, status_object};
pub use handler::{
    ResourceHandler, RouterHandlerSink, StoreBackedHandler, gone_to_api_error,
    handlers_from_catalog, handlers_from_catalog_with_admission,
};
pub use health::VersionInfo;
pub use openapi::ApiDoc;
pub use params::{
    ListWatchParams, ResumePoint, Selectors, body_precondition, bookmark_line, gvk_ns_matches,
    status_410_line, to_k8s_watch_line,
};
pub use pki::{
    ClientMaterial, ClusterCa, PkiError, ServerSanInputs, TlsMaterial, VerifiedClientCert,
    client_verifier, issue_admin_client_material, issue_server_material, load_or_generate_ca,
    parse_client_cert,
};
pub use pod_logs::{LogQuery, PodLogReader};
pub use router::{RouterState, build};
pub use scale::{
    Scale, ScaleMeta, ScaleSpec, ScaleStatus, label_selector_to_string, project_scale,
};
pub use server::{ApiServer, ServerError};
pub use webhook_admission::{
    ReqwestWebhookCaller, StoreWebhookConfigSource, catalog_pluralizer, mutating_webhook_plugin,
};
