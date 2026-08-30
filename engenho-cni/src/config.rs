//! Network configuration — reading `/etc/cni/net.d`.
//!
//! ★ FIRST FILE IN LEXICAL ORDER WINS, AND NOTHING MERGES. The runtime
//! sorts the directory and takes the first loadable configuration; the rest
//! are ignored entirely. Operators rely on this — naming a file `00-` to
//! override is the standard way to pin a CNI — so "load them all" or "merge
//! them" would break a deployment that is correct.
//!
//! ★ `.conf` AND `.conflist` ARE THE SAME THING AT DIFFERENT ARITIES. A
//! `.conf` is a single plugin; a `.conflist` is `{name, plugins: [...]}`.
//! Both are normalised to [`NetworkConfigList`] here so nothing downstream
//! carries two shapes, which is where the chain-ordering bugs live.
//!
//! ★ AN UNPARSEABLE FILE IS SKIPPED, NOT FATAL. The directory is shared:
//! a half-written file from a CNI installer that is mid-copy is a normal
//! transient state, and refusing to network any pod because of it turns a
//! momentary condition into an outage. But the skip is REPORTED, because a
//! silently-ignored config is how a cluster ends up on the wrong CNI.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One plugin's configuration, kept as raw JSON.
///
/// ★ THE BODY IS OPAQUE ON PURPOSE. A plugin's config is defined by the
/// plugin, not by the spec — `bridge` has `isGateway`, Calico has
/// `datastore_type`, Cilium has its own. Typing the known ones would
/// silently drop the fields of every plugin we did not anticipate, which is
/// precisely the class of bug that makes a network come up misconfigured
/// rather than broken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginConfig {
    /// The plugin binary's name, looked up in `CNI_PATH`.
    #[serde(rename = "type")]
    pub plugin_type: String,
    /// Everything else, verbatim.
    #[serde(flatten)]
    pub body: Map<String, Value>,
}

/// A normalised network configuration: a name plus an ordered plugin chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfigList {
    /// Spec version this configuration is written against.
    #[serde(default)]
    pub cni_version: String,
    /// The network's name. Passed to every plugin in the chain and used by
    /// plugins to key their own state, so it must be carried unchanged.
    pub name: String,
    /// Whether this network can carry the runtime's own service traffic.
    #[serde(default)]
    pub disable_check: bool,
    /// The chain, in invocation order for `ADD`.
    pub plugins: Vec<PluginConfig>,
    /// The file this was loaded from, for diagnostics.
    #[serde(skip)]
    pub source: PathBuf,
}

/// Errors loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("reading {path}: {source}")]
    Read {
        /// The file.
        path: String,
        /// The io error.
        #[source]
        source: std::io::Error,
    },
    /// The file is not valid JSON, or not a valid configuration.
    #[error("parsing {path}: {detail}")]
    Parse {
        /// The file.
        path: String,
        /// What was wrong.
        detail: String,
    },
    /// The directory could not be listed.
    #[error("listing {dir}: {source}")]
    ListDir {
        /// The directory.
        dir: String,
        /// The io error.
        #[source]
        source: std::io::Error,
    },
}

engenho_substrate::impl_error_kind! {
    ConfigError {
        { Read { .. } } => "read",
        { Parse { .. } } => "parse",
        { ListDir { .. } } => "list_dir",
    }
}

/// Parse one `.conf` or `.conflist` body.
///
/// # Errors
/// [`ConfigError::Parse`] if the JSON is malformed or names no plugin.
pub fn parse_config(path: &Path, bytes: &[u8]) -> Result<NetworkConfigList, ConfigError> {
    let bad = |detail: String| ConfigError::Parse {
        path: path.display().to_string(),
        detail,
    };

    let value: Value = serde_json::from_slice(bytes).map_err(|e| bad(e.to_string()))?;
    let obj = value
        .as_object()
        .ok_or_else(|| bad("not a JSON object".into()))?;

    let cni_version = obj
        .get("cniVersion")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let name = obj
        .get("name")
        .and_then(Value::as_str)
        .filter(|n| !n.is_empty())
        .ok_or_else(|| bad("no network name".into()))?
        .to_string();
    let disable_check = obj
        .get("disableCheck")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let plugins = if let Some(list) = obj.get("plugins").and_then(Value::as_array) {
        // `.conflist`
        let mut out = Vec::with_capacity(list.len());
        for (i, p) in list.iter().enumerate() {
            out.push(
                serde_json::from_value::<PluginConfig>(p.clone())
                    .map_err(|e| bad(format!("plugins[{i}]: {e}")))?,
            );
        }
        // An empty chain networks nothing while looking like a valid
        // configuration — the exact silent-misconfiguration shape.
        if out.is_empty() {
            return Err(bad("plugin list is empty: this networks nothing".into()));
        }
        out
    } else {
        // `.conf` — a single plugin, normalised to a chain of one so
        // nothing downstream carries two shapes.
        let mut body = obj.clone();
        body.remove("cniVersion");
        body.remove("name");
        body.remove("disableCheck");
        let plugin_type = body
            .remove("type")
            .and_then(|v| v.as_str().map(ToString::to_string))
            .filter(|t| !t.is_empty())
            .ok_or_else(|| bad("no plugin type and no plugin list".into()))?;
        vec![PluginConfig { plugin_type, body }]
    };

    Ok(NetworkConfigList {
        cni_version,
        name,
        disable_check,
        plugins,
        source: path.to_path_buf(),
    })
}

/// Whether a directory entry is a CNI configuration file.
#[must_use]
pub fn is_config_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("conf" | "conflist" | "json")
    )
}

/// The effective configuration plus every file that was skipped and why.
///
/// Both halves are returned deliberately: a silently-ignored config is how
/// a cluster ends up on the wrong CNI, so the skips are not swallowed.
pub type LoadedNetD = (Option<NetworkConfigList>, Vec<(PathBuf, ConfigError)>);

/// Load the effective network configuration from a `net.d` directory.
///
/// Returns the FIRST loadable configuration in lexical order, plus every
/// file that was skipped and why. Both halves matter: a silently-ignored
/// config is how a cluster ends up on the wrong CNI.
///
/// A missing directory yields `(None, [])` — a node with no CNI installed
/// is a normal state on darwin and during bootstrap, not an error.
///
/// # Errors
/// [`ConfigError::ListDir`] if the directory exists but cannot be read.
pub fn load_conflist_dir(dir: &Path) -> Result<LoadedNetD, ConfigError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((None, Vec::new())),
        Err(source) => {
            return Err(ConfigError::ListDir {
                dir: dir.display().to_string(),
                source,
            });
        }
    };

    let mut files: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && is_config_file(p))
        .collect();
    // Lexical order is the contract: operators name a file `00-` to pin a
    // CNI, and that only works if the sort is stable and ascending.
    files.sort();

    let mut skipped = Vec::new();
    for path in files {
        match std::fs::read(&path) {
            Ok(bytes) => match parse_config(&path, &bytes) {
                Ok(cfg) => return Ok((Some(cfg), skipped)),
                Err(e) => skipped.push((path, e)),
            },
            Err(source) => skipped.push((
                path.clone(),
                ConfigError::Read {
                    path: path.display().to_string(),
                    source,
                },
            )),
        }
    }
    Ok((None, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, body: &str) {
        std::fs::write(dir.join(name), body).unwrap();
    }

    const BRIDGE_CONFLIST: &str = r#"{
      "cniVersion": "1.0.0",
      "name": "cbr0",
      "plugins": [
        { "type": "bridge", "bridge": "cni0", "isGateway": true,
          "ipam": { "type": "host-local", "subnet": "10.244.1.0/24" } },
        { "type": "portmap", "capabilities": { "portMappings": true } }
      ]
    }"#;

    #[test]
    fn a_conflist_keeps_its_chain_in_order() {
        // Order is not cosmetic: on DEL it reverses, and getting it wrong
        // tears down portmap after the interface it maps.
        let c = parse_config(Path::new("/x/10-cbr0.conflist"), BRIDGE_CONFLIST.as_bytes()).unwrap();
        assert_eq!(c.name, "cbr0");
        assert_eq!(
            c.plugins
                .iter()
                .map(|p| p.plugin_type.as_str())
                .collect::<Vec<_>>(),
            ["bridge", "portmap"]
        );
    }

    #[test]
    fn a_plugins_own_fields_survive_verbatim() {
        // Typing the known fields would silently drop every plugin we did
        // not anticipate — a network that comes up MISCONFIGURED rather
        // than broken, which is far harder to notice.
        let c = parse_config(Path::new("/x/a.conflist"), BRIDGE_CONFLIST.as_bytes()).unwrap();
        assert_eq!(c.plugins[0].body["bridge"], "cni0");
        assert_eq!(c.plugins[0].body["isGateway"], true);
        assert_eq!(c.plugins[0].body["ipam"]["subnet"], "10.244.1.0/24");
        assert_eq!(c.plugins[1].body["capabilities"]["portMappings"], true);
    }

    #[test]
    fn a_single_conf_normalises_to_a_chain_of_one() {
        // So nothing downstream carries two shapes, which is where the
        // chain-ordering bugs live.
        let c = parse_config(
            Path::new("/x/10-flannel.conf"),
            br#"{"cniVersion":"0.3.1","name":"flannel","type":"flannel","delegate":{"hairpinMode":true}}"#,
        )
        .unwrap();
        assert_eq!(c.plugins.len(), 1);
        assert_eq!(c.plugins[0].plugin_type, "flannel");
        assert_eq!(c.plugins[0].body["delegate"]["hairpinMode"], true);
        // The envelope keys must NOT leak into the plugin body.
        assert!(!c.plugins[0].body.contains_key("name"));
        assert!(!c.plugins[0].body.contains_key("cniVersion"));
        assert!(!c.plugins[0].body.contains_key("type"));
    }

    #[test]
    fn an_empty_plugin_list_is_refused() {
        // It networks nothing while looking valid.
        let e = parse_config(
            Path::new("/x/a.conflist"),
            br#"{"cniVersion":"1.0.0","name":"n","plugins":[]}"#,
        )
        .unwrap_err();
        assert!(e.to_string().contains("networks nothing"), "{e}");
    }

    #[test]
    fn a_config_with_no_name_or_no_type_is_refused() {
        assert!(parse_config(Path::new("/x/a.conf"), br#"{"type":"bridge"}"#).is_err());
        assert!(parse_config(Path::new("/x/a.conf"), br#"{"name":"n"}"#).is_err());
        assert!(parse_config(Path::new("/x/a.conf"), b"not json").is_err());
        assert!(parse_config(Path::new("/x/a.conf"), b"[]").is_err());
    }

    #[test]
    fn the_first_file_in_lexical_order_wins() {
        // Naming a file `00-` to override is the standard way to pin a CNI.
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "10-cbr0.conflist", BRIDGE_CONFLIST);
        write(
            dir.path(),
            "00-override.conf",
            r#"{"cniVersion":"1.0.0","name":"pinned","type":"ptp"}"#,
        );
        let (cfg, skipped) = load_conflist_dir(dir.path()).unwrap();
        assert_eq!(cfg.unwrap().name, "pinned");
        assert!(skipped.is_empty());
    }

    #[test]
    fn a_half_written_file_is_skipped_and_reported_not_fatal() {
        // A CNI installer mid-copy is a normal transient state; refusing to
        // network any pod over it turns a moment into an outage. But the
        // skip is REPORTED, because a silently-ignored config is how a
        // cluster ends up on the wrong CNI.
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "05-partial.conflist", r#"{"cniVersio"#);
        write(dir.path(), "10-cbr0.conflist", BRIDGE_CONFLIST);
        let (cfg, skipped) = load_conflist_dir(dir.path()).unwrap();
        assert_eq!(cfg.unwrap().name, "cbr0", "the good one still loads");
        assert_eq!(skipped.len(), 1, "and the bad one is reported");
        assert!(skipped[0].0.ends_with("05-partial.conflist"));
    }

    #[test]
    fn a_missing_directory_is_not_an_error() {
        // A node with no CNI installed is normal on darwin and during
        // bootstrap.
        let (cfg, skipped) = load_conflist_dir(Path::new("/nonexistent/net.d")).unwrap();
        assert!(cfg.is_none());
        assert!(skipped.is_empty());
    }

    #[test]
    fn non_config_files_are_ignored_entirely() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "README.md", "hi");
        write(dir.path(), "cni.lock", "");
        write(dir.path(), "10-cbr0.conflist", BRIDGE_CONFLIST);
        let (cfg, skipped) = load_conflist_dir(dir.path()).unwrap();
        assert_eq!(cfg.unwrap().name, "cbr0");
        assert!(skipped.is_empty(), "not even reported: {skipped:?}");
    }
}
