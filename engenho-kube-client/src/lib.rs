//! # engenho-kube-client
//!
//! Reqwest-backed concrete implementation of `engenho-types`' trait
//! surface: [`KubeClient`], [`Watcher`], plus a `kubeconfig` loader
//! that resolves [`KubeAuth`] from a standard kubeconfig YAML.
//!
//! ## Layering
//!
//! engenho-types defines the typed contracts (pure types, no I/O).
//! This crate adds the HTTP-layer impls. Controllers + the apiserver
//! consume the **trait surface** — they don't depend on reqwest
//! directly. That gives us:
//!
//!   * **Swappable backends** — a mock implementing `KubeClient` for
//!     tests; a tonic-based one for the engenho-native apiserver in
//!     M0.1+ (same trait, different wire).
//!   * **Single point of optimization** — perf work happens here
//!     without touching consumers.
//!
//! ## Modules
//!
//! * [`config`]      — kubeconfig parser; resolves to a [`Connection`].
//! * [`connection`]  — TLS + auth state, shared across requests.
//! * [`client`]      — [`ReqwestKubeClient`] implementing
//!                     [`engenho_types::client::KubeClient`].
//! * [`watcher`]     — [`ReqwestWatcher`] streaming `?watch=true`.
//! * [`mock`]        — in-memory mock client for tests (cfg `test` only).

#![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod connection;
pub mod url_builder;
pub mod watcher;

#[cfg(any(test, feature = "mock"))]
pub mod mock;

pub use client::ReqwestKubeClient;
pub use config::{Kubeconfig, emit_kubeconfig, emit_kubeconfig_with_admin};
pub use connection::Connection;
pub use url_builder::KubeUrlBuilder;
pub use watcher::ReqwestWatcher;
