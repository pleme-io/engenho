//! Where the daemon keeps its control state, decided before any boot.
//!
//! The supervisor has to place `data_dir/control/` — its lock, its journal,
//! its hold marker — before it boots anything, and it has to do so when the
//! configuration is broken: that is the case the control plane exists for.
//! The strict resolution (`deny_unknown_fields`, full validation) is exactly
//! what a broken configuration fails, so [`ControlBootstrap::discover`] asks
//! it first and, when it fails, reads `runtime.data_dir` alone from the
//! declared file, leniently, before settling for the compiled default.
//!
//! What it settled for is kept ([`DataDirSource`]), so the control API can say
//! how it knows. A later boot whose resolved `data_dir` differs is refused
//! ([`crate::RuntimeError::DataDirMoved`]): the data directory is fixed for
//! the life of the process.

use std::path::{Path, PathBuf};

use engenho_config::{ConfigError, ControlConfig, EngenhoConfig, TieredConfig};
use serde::{Deserialize, Serialize};

/// How the data directory was decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum DataDirSource {
    /// The configuration resolved, and this is its `runtime.data_dir`.
    Resolved,
    /// The configuration did not resolve; `runtime.data_dir` was read from
    /// the declared file alone.
    LenientFallback {
        /// Why the full resolution failed.
        error: String,
    },
    /// Neither: the compiled default.
    CompiledDefault,
}

/// What the daemon needs before its first boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlBootstrap {
    /// The data directory, fixed for the life of the process.
    pub data_dir: PathBuf,
    /// How it was decided.
    pub data_dir_source: DataDirSource,
    /// The declared configuration file, when there is one: watched, so a
    /// boot held on the configuration is retried when it changes.
    pub declared: Option<PathBuf>,
    /// The control plane's own section, read the same way: from the
    /// resolved configuration, else alone from the declared file, else the
    /// defaults. The control socket must bind when the rest does not parse.
    pub control: ControlConfig,
}

impl ControlBootstrap {
    /// Decide from the configuration the daemon would boot on.
    #[must_use]
    pub fn discover() -> Self {
        Self::from_resolution(
            EngenhoConfig::resolve_progressively()
                .map(engenho_config::ProgressiveResolution::into_value),
            EngenhoConfig::discover_path(),
        )
    }

    /// Decide from a resolution's outcome and the declared file.
    #[must_use]
    pub fn from_resolution(
        resolved: Result<EngenhoConfig, ConfigError>,
        declared: Option<PathBuf>,
    ) -> Self {
        let (data_dir, data_dir_source, control) = match resolved {
            Ok(config) => (
                config.runtime.data_dir,
                DataDirSource::Resolved,
                config.control,
            ),
            Err(err) => {
                let doc = declared.as_deref().and_then(read_yaml);
                let control = doc
                    .as_ref()
                    .and_then(|d| d.get("control"))
                    .and_then(|c| serde_yaml::from_value(c.clone()).ok())
                    .unwrap_or_default();
                match doc.as_ref().and_then(lenient_data_dir) {
                    Some(data_dir) => (
                        data_dir,
                        DataDirSource::LenientFallback {
                            error: err.to_string(),
                        },
                        control,
                    ),
                    None => (
                        EngenhoConfig::prescribed_default().runtime.data_dir,
                        DataDirSource::CompiledDefault,
                        control,
                    ),
                }
            }
        };
        Self {
            data_dir,
            data_dir_source,
            declared,
            control,
        }
    }
}

fn read_yaml(path: &Path) -> Option<serde_yaml::Value> {
    serde_yaml::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// `runtime.data_dir` from a YAML document, and nothing else from it.
fn lenient_data_dir(doc: &serde_yaml::Value) -> Option<PathBuf> {
    doc.get("runtime")?
        .get("data_dir")?
        .as_str()
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_broken_config_still_names_its_data_dir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let file = tmp.path().join("engenho.yaml");
        std::fs::write(
            &file,
            "runtime:\n  data_dir: /srv/engenho\n  not_a_field: true\ncontrol:\n  socket:\n    access: group\n",
        )
        .expect("write");
        let boot = ControlBootstrap::from_resolution(
            Err(ConfigError::Parse("unknown field `not_a_field`".into())),
            Some(file.clone()),
        );
        assert_eq!(boot.data_dir, PathBuf::from("/srv/engenho"));
        assert!(matches!(
            boot.data_dir_source,
            DataDirSource::LenientFallback { .. }
        ));
        assert_eq!(boot.declared, Some(file));
        assert_eq!(
            boot.control.socket.access,
            engenho_config::SocketAccess::Group,
            "the control section binds even when the rest does not parse"
        );
    }

    #[test]
    fn with_nothing_readable_it_falls_back_to_the_compiled_default() {
        let boot = ControlBootstrap::from_resolution(Err(ConfigError::Parse("x".into())), None);
        assert_eq!(
            boot.data_dir,
            EngenhoConfig::prescribed_default().runtime.data_dir
        );
        assert_eq!(boot.data_dir_source, DataDirSource::CompiledDefault);
    }

    #[test]
    fn a_resolved_config_is_taken_as_is() {
        let mut config = EngenhoConfig::prescribed_default();
        config.runtime.data_dir = PathBuf::from("/var/lib/engenho-test");
        let boot = ControlBootstrap::from_resolution(Ok(config), None);
        assert_eq!(boot.data_dir, PathBuf::from("/var/lib/engenho-test"));
        assert_eq!(boot.data_dir_source, DataDirSource::Resolved);
    }
}
