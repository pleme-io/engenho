//! CNI — the Container Network Interface, from engenho's side of it.
//!
//! ★ CNI IS A FILE + EXEC + JSON CONTRACT, NOT AN RPC ONE. There is no
//! daemon and no socket. The runtime reads network configuration from
//! `/etc/cni/net.d`, execs a plugin binary out of `/opt/cni/bin`, writes the
//! config to its stdin with a handful of `CNI_*` environment variables set,
//! and reads a JSON `Result` back off stdout. That is the entire protocol.
//!
//! ★ EXECING A PLUGIN IS NOT A SHELL SCRIPT. It is the contract: the spec
//! defines the interface as an executable invocation. What is forbidden is
//! `sh -c` — the plugin is invoked as a typed `Command` with an explicit
//! argv and environment, never through a shell.
//!
//! ★ THE CHAIN IS ORDERED AND EACH LINK SEES THE PREVIOUS RESULT. A
//! `.conflist` names several plugins; each is invoked in order with the
//! previous plugin's `Result` injected as `prevResult`. On `DEL` the order
//! REVERSES. Getting this backwards produces a network that comes up and
//! then leaks: `portmap` would tear down before the interface it maps.
//!
//! ★ WHAT THIS CRATE DOES NOT DO, AND WHY THAT IS NOT A GAP. It does not
//! ship a plugin. engenho implements the runtime half so
//! `containernetworking/plugins`, Calico and Cilium work against it
//! unmodified — writing our own would compete with the ecosystem this
//! exists to join.
//!
//! ★ THE PARSING AND RESULT HALVES ARE PLATFORM-INDEPENDENT; THE INVOCATION
//! IS NOT. On darwin there is no network namespace to hand a plugin, so
//! `CNI_NETNS` cannot be satisfied and no plugin can run. That is a fact
//! about the world, not about our abstractions, and it is typed as
//! [`CniInstall`] rather than faked.
#![warn(missing_docs)]

pub mod config;
pub mod exec;
pub mod result;

pub use config::{ConfigError, LoadedNetD, NetworkConfigList, PluginConfig, load_conflist_dir};
pub use exec::{CniCommand, CniEnv, CniInstall, CniInvocation, CniPlugin, ExecError, PlannedCni};
pub use result::{CniResult, Interface, IpConfig, Route};
