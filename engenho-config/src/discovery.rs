//! The engenho **discovered() tier** — environment auto-detect wired through
//! shikumi's declarative [`shikumi::DiscoveryLayer`] seam (never a hand-rolled
//! struct literal / one-off probe fn).
//!
//! Today the substrate has exactly one field whose value is genuinely
//! *detected* from the running environment rather than *prescribed* by an
//! opinion: the node's name (this host's hostname). Before this module the
//! hostname was read by a one-off `discovered_hostname()` fn called from
//! [`crate::RuntimeConfig::prescribed_default`] — the exact anti-pattern the
//! kanchi / `DiscoveryLayer` seam replaces. It lived in the wrong tier
//! (`prescribed_default`, which outranks discovery) *and* was hand-rolled.
//!
//! [`HostnameLayer`] moves it behind a proper [`shikumi::DiscoveryLayer`]:
//! `RuntimeConfig::discovered()` composes it via
//! [`shikumi::TieredConfig::discovered_from_layers`], so a detected hostname
//! now flows through the sealed progressive fold at the **Discovered** tier
//! and is credited there (I2/I6 in
//! `shikumi/docs/PROGRESSIVE-DISCOVERY-VERIFICATION.md`).
//!
//! ## Why a local layer and not a kanchi probe
//!
//! `kanchi::probe` owns the shared platform-FFI probes (RAM / arch / cgroup /
//! screen / appearance) but has **no hostname probe** today. Per the org
//! guidance ("prefer a local `DiscoveryLayer` over bloating kanchi") this
//! layer reads `$HOSTNAME` locally — the same signal the old
//! `discovered_hostname()` read, so behavior is byte-preserved — and the
//! follow-on is to promote a real `gethostname(2)` probe into `kanchi::probe`
//! (its documented home for OS-FFI) and re-point this layer at it.

use figment::value::{Dict, Value};
use shikumi::DiscoveryLayer;

/// The documented fallback node name for the hostname axis: used when the
/// environment can't answer (`$HOSTNAME` unset / empty). Kept here as the
/// single source of truth so [`crate::RuntimeConfig::prescribed_default`]
/// applies exactly the discovery-axis fallback, never a second literal.
pub const NODE_NAME_FALLBACK: &str = "engenho-node";

/// The serde field key this layer contributes into the [`crate::RuntimeConfig`]
/// dict. One source of truth so the layer key can never drift from the struct
/// field name.
const NODE_NAME_KEY: &str = "node_name";

/// The hostname discovery axis → `runtime.node_name`.
///
/// Reads the host's name and contributes it as the `node_name` leaf of the
/// discovered [`crate::RuntimeConfig`]. An undetectable host (`$HOSTNAME`
/// unset / empty) contributes an **empty [`Dict`]** — the clean degenerate
/// (discovery totality): the next tier (`prescribed_default`, which supplies
/// [`NODE_NAME_FALLBACK`]) shows through, and nothing is guessed.
///
/// The raw signal is captured at construction (via [`Self::from_env`]) rather
/// than read inside [`DiscoveryLayer::discover`], so the layer is a pure value
/// — [`Self::from_raw`] makes the whole seam deterministically testable with
/// no process-environment mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostnameLayer {
    /// The detected host name, or `None` when the environment can't answer.
    raw: Option<String>,
}

impl HostnameLayer {
    /// Read the hostname from the running environment (`$HOSTNAME`), treating
    /// an unset / empty value as "undetectable" (`None`). The one env-reading
    /// site — everything downstream is a pure function of the captured value.
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_raw(detected_node_name())
    }

    /// Construct from an explicit raw signal — the pure constructor the seam's
    /// tests drive so hostname behavior is validated without touching the
    /// process environment. A blank string is normalized to `None`.
    #[must_use]
    pub fn from_raw(raw: Option<String>) -> Self {
        Self {
            raw: raw.filter(|h| !h.is_empty()),
        }
    }
}

impl DiscoveryLayer for HostnameLayer {
    fn name(&self) -> &'static str {
        "engenho.hostname"
    }

    fn discover(&self) -> Dict {
        let mut dict = Dict::new();
        if let Some(name) = &self.raw {
            dict.insert(NODE_NAME_KEY.to_string(), Value::from(name.clone()));
        }
        dict
    }
}

/// The host name from `$HOSTNAME`, or `None` when unset / empty. Mirrors the
/// signal the pre-seam `discovered_hostname()` read, so the effective
/// `prescribed_default()` node name is byte-preserved across the refactor.
///
/// Follow-on: this belongs in `kanchi::probe` as a real `gethostname(2)` probe
/// (its OS-FFI home); a local read keeps engenho-config free of a new git dep
/// until that probe lands.
#[must_use]
pub(crate) fn detected_node_name() -> Option<String> {
    std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_node_name_when_raw_present() {
        let dict = HostnameLayer::from_raw(Some("node-Z".into())).discover();
        assert_eq!(
            dict.get(NODE_NAME_KEY).and_then(Value::as_str),
            Some("node-Z")
        );
    }

    #[test]
    fn empty_dict_when_raw_absent() {
        // Discovery totality: an undetectable axis contributes nothing, so the
        // next tier's fallback shows through — never a guess.
        assert!(HostnameLayer::from_raw(None).discover().is_empty());
    }

    #[test]
    fn empty_dict_when_raw_blank() {
        // A blank $HOSTNAME is "undetectable", not a valid empty node name.
        let dict = HostnameLayer::from_raw(Some(String::new())).discover();
        assert!(dict.is_empty());
    }

    #[test]
    fn layer_name_is_stable() {
        assert_eq!(HostnameLayer::from_env().name(), "engenho.hostname");
    }
}
