//! CSI — the Container Storage Interface, from engenho's side of it.
//!
//! ★ ENGENHO IMPLEMENTS THE RUNTIME SIDE, NOT A DRIVER. Every storage
//! vendor ships a CSI driver; ~150 of them exist. Satisfying this seam
//! means those drivers work against engenho unmodified. Shipping our own
//! driver would compete with the ecosystem this crate exists to join.
//!
//! ★ THE INTERFACE OUTLIVES THE TECHNOLOGY. `NodePublishVolume` is a stable
//! contract regardless of what engenho does with the bytes underneath — the
//! same property that made the etcd façade worth building.
//!
//! Layout: [`client`] dials a driver; [`registry`] discovers one.
#![warn(missing_docs)]

pub mod client;
pub mod localpath;
pub mod registry;

#[cfg(any(test, feature = "test-driver"))]
pub mod testdriver;

pub use client::{CsiClient, CsiError, DriverInfo, socket_path};
pub use localpath::{DRIVER_NAME, LocalPathDriver, VolumeRecord};
pub use registry::{DiscoveredPlugin, PluginRegistry, RegistrationError};

/// Generated `csi.v1` types + clients.
pub mod pb {
    #![allow(missing_docs, clippy::pedantic, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/csi.v1.rs"));
}

/// Generated `pluginregistration` types + client.
pub mod reg {
    #![allow(missing_docs, clippy::pedantic, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/pluginregistration.rs"));
}
