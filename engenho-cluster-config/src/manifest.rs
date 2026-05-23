//! Minimal manifest representation — what the renderer emits for k3s'
//! auto-apply directory.

use serde::{Deserialize, Serialize};

/// A Kubernetes manifest the renderer emits. Identified by its
/// auto-apply filename (the basename under
/// `/var/lib/rancher/k3s/server/manifests/`) and its YAML body.
///
/// The filename uniquely identifies the manifest within the cluster's
/// boot-time apply set; the renderer guarantees no two manifests share
/// a filename.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// Filename under `/var/lib/rancher/k3s/server/manifests/`.
    /// Convention: `<kind>-<name>.yaml`, all lowercase.
    pub filename: String,

    /// YAML body — typically multiple `---`-separated documents
    /// (the install manifest is usually a bundle).
    pub body: String,
}

impl Manifest {
    /// Construct a new manifest.
    #[must_use]
    pub fn new(filename: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            filename: filename.into(),
            body: body.into(),
        }
    }
}
