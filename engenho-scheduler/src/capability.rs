use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

use serde_json::Value;

pub const LABEL_PREFIX: &str = "capability.engenho.pleme.io/";

pub const REQUIREMENT_PREFIX: &str = "requirements.engenho.pleme.io/";

const NATIVE_LABEL: &str = "capability.engenho.pleme.io/runtime.native";
const OCI_LABEL: &str = "capability.engenho.pleme.io/runtime.oci";
const GPU_LABEL: &str = "capability.engenho.pleme.io/gpu";
const HOUSE_LABEL: &str = "capability.engenho.pleme.io/house";
const GPU_REQUIREMENT: &str = "requirements.engenho.pleme.io/gpu";
const HOUSE_REQUIREMENT: &str = "requirements.engenho.pleme.io/house";

pub const NIX_IMAGE_PREFIX: &str = "nix:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Runtime {
    Native,
    Oci,
}

impl Runtime {
    pub const ALL: [Self; 2] = [Self::Native, Self::Oci];

    const fn label(self) -> &'static str {
        match self {
            Self::Native => NATIVE_LABEL,
            Self::Oci => OCI_LABEL,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Native => "runtime.native",
            Self::Oci => "runtime.oci",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeCapabilities {
    runtimes: Option<BTreeSet<Runtime>>,
    gpus: u32,
    house: bool,
}

impl NodeCapabilities {
    #[must_use]
    pub fn running(runtimes: impl IntoIterator<Item = Runtime>) -> Self {
        Self {
            runtimes: Some(runtimes.into_iter().collect()),
            gpus: 0,
            house: false,
        }
    }

    #[must_use]
    pub fn observe(node: &Value) -> Self {
        let labels = node.pointer("/metadata/labels").and_then(Value::as_object);
        let label = |key: &str| labels.and_then(|l| l.get(key)).and_then(Value::as_str);
        let declared = Runtime::ALL.iter().any(|r| label(r.label()).is_some());
        let runtimes = declared.then(|| {
            Runtime::ALL
                .into_iter()
                .filter(|r| label(r.label()) == Some("true"))
                .collect()
        });
        Self {
            runtimes,
            gpus: label(GPU_LABEL).and_then(|v| v.parse().ok()).unwrap_or(0),
            house: label(HOUSE_LABEL) == Some("true"),
        }
    }

    #[must_use]
    pub fn runtime_labels(&self) -> BTreeMap<String, String> {
        let Some(runtimes) = &self.runtimes else {
            return BTreeMap::new();
        };
        Runtime::ALL
            .into_iter()
            .map(|r| (r.label().to_owned(), runtimes.contains(&r).to_string()))
            .collect()
    }

    #[must_use]
    pub fn runtimes(&self) -> Option<&BTreeSet<Runtime>> {
        self.runtimes.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Requirement {
    Runtime(Runtime),
    Gpus(NonZeroU32),
    House,
    Unreadable(String),
}

impl fmt::Display for Requirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(r) => f.write_str(r.name()),
            Self::Gpus(_) => f.write_str("gpu"),
            Self::House => f.write_str("house"),
            Self::Unreadable(key) => f.write_str(key),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkloadRequirements {
    needs: BTreeSet<Requirement>,
}

impl WorkloadRequirements {
    #[must_use]
    pub fn infer(pod: &Value) -> Self {
        let mut needs = BTreeSet::new();
        for list in ["/spec/initContainers", "/spec/containers"] {
            let containers = pod.pointer(list).and_then(Value::as_array);
            for image in containers
                .into_iter()
                .flatten()
                .filter_map(|c| c.get("image").and_then(Value::as_str))
                .filter(|i| !i.is_empty())
            {
                let runtime = if image.starts_with(NIX_IMAGE_PREFIX) {
                    Runtime::Native
                } else {
                    Runtime::Oci
                };
                needs.insert(Requirement::Runtime(runtime));
            }
        }
        let annotations = pod
            .pointer("/metadata/annotations")
            .and_then(Value::as_object);
        let annotation = |key: &str| annotations.and_then(|a| a.get(key)).and_then(Value::as_str);
        if let Some(raw) = annotation(GPU_REQUIREMENT) {
            match raw.parse::<u32>() {
                Ok(0) => {}
                Ok(n) => {
                    needs.insert(Requirement::Gpus(
                        NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN),
                    ));
                }
                Err(_) => {
                    needs.insert(Requirement::Unreadable(GPU_REQUIREMENT.to_owned()));
                }
            }
        }
        match annotation(HOUSE_REQUIREMENT) {
            None | Some("false") => {}
            Some("true") => {
                needs.insert(Requirement::House);
            }
            Some(_) => {
                needs.insert(Requirement::Unreadable(HOUSE_REQUIREMENT.to_owned()));
            }
        }
        Self { needs }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Requirement> {
        self.needs.iter()
    }
}

pub trait CapabilityMatcher: Send + Sync {
    fn unmet(&self, needs: &WorkloadRequirements, node: &NodeCapabilities) -> Option<Requirement>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LabelCapabilityMatcher;

impl CapabilityMatcher for LabelCapabilityMatcher {
    fn unmet(&self, needs: &WorkloadRequirements, node: &NodeCapabilities) -> Option<Requirement> {
        needs
            .iter()
            .find(|need| !match need {
                Requirement::Runtime(r) => node.runtimes.as_ref().is_none_or(|rs| rs.contains(r)),
                Requirement::Gpus(n) => node.gpus >= n.get(),
                Requirement::House => node.house,
                Requirement::Unreadable(_) => false,
            })
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(image: &str) -> Value {
        json!({ "spec": { "containers": [{ "name": "c", "image": image }] } })
    }

    #[test]
    fn a_nix_image_needs_the_native_runtime_and_any_other_image_needs_oci() {
        let native = WorkloadRequirements::infer(&pod("nix:/nix/store/a-b"));
        let oci = WorkloadRequirements::infer(&pod("ghcr.io/x/y:1"));
        assert_eq!(
            native.iter().collect::<Vec<_>>(),
            [&Requirement::Runtime(Runtime::Native)]
        );
        assert_eq!(
            oci.iter().collect::<Vec<_>>(),
            [&Requirement::Runtime(Runtime::Oci)]
        );
    }

    #[test]
    fn advertised_labels_round_trip_through_observe() {
        let caps = NodeCapabilities::running([Runtime::Native]);
        let node = json!({ "metadata": { "labels": caps.runtime_labels() } });
        assert_eq!(NodeCapabilities::observe(&node), caps);
    }

    #[test]
    fn a_node_without_runtime_labels_declares_none() {
        let caps = NodeCapabilities::observe(&json!({ "metadata": {} }));
        assert_eq!(caps.runtimes(), None);
        assert!(caps.runtime_labels().is_empty());
    }

    #[test]
    fn every_runtime_is_labelled_true_or_false() {
        let labels = NodeCapabilities::running([Runtime::Oci]).runtime_labels();
        assert_eq!(labels.get(NATIVE_LABEL).map(String::as_str), Some("false"));
        assert_eq!(labels.get(OCI_LABEL).map(String::as_str), Some("true"));
    }
}
