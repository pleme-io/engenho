//! The CNI `Result` and `Error` types — what a plugin writes to stdout.
//!
//! ★ THE RESULT IS VERSIONED AND THE SHAPE CHANGED. In CNI 0.3.x `ips[].
//! version` carried "4"/"6" and `interfaces` was optional; in 1.0.0 the
//! address is a CIDR whose family is derivable and `interfaces` is
//! standard. engenho parses the 1.0.0 shape and treats a missing
//! `interfaces` as empty rather than an error, because that is what a
//! chained plugin like `portmap` legitimately returns — it adds no
//! interface of its own.
//!
//! ★ THE ADDRESS IS A CIDR AND THE POD IP IS NOT THE WHOLE STRING. `ips[0].
//! address` is `10.244.1.7/24`. Recording that verbatim as `status.podIP`
//! yields a pod whose IP has a prefix length in it, which every consumer
//! then fails to parse — Endpoints, kube-proxy, DNS. Stripping it is
//! [`IpConfig::ip`], and it has a test because the mistake is invisible
//! until something downstream reads the field.

use serde::{Deserialize, Serialize};

/// One interface a plugin created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Interface {
    /// Interface name inside the sandbox (`eth0`).
    pub name: String,
    /// Hardware address, when the plugin reports one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mac: String,
    /// The netns path this interface lives in. EMPTY means the HOST side of
    /// a veth pair — which is why `ips[].interface` is an index rather than
    /// a name: a chain routinely reports both ends, and picking the wrong
    /// one gives you the host's address as the pod IP.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sandbox: String,
}

/// One address assigned by IPAM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IpConfig {
    /// CIDR form, e.g. `10.244.1.7/24`.
    pub address: String,
    /// Gateway, when IPAM assigned one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub gateway: String,
    /// Index into `interfaces` this address belongs to.
    ///
    /// OMITTED when absent rather than serialized as `null`: an IPAM plugin
    /// creates no interface, upstream's plugins leave the field out
    /// entirely, and a strict consumer reading `null` where it expects an
    /// integer is a difference we would have introduced for nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<usize>,
}

impl IpConfig {
    /// The bare address, without the prefix length.
    ///
    /// Recording the CIDR verbatim as `status.podIP` produces an IP with a
    /// `/24` in it that Endpoints, kube-proxy and DNS all fail to parse.
    #[must_use]
    pub fn ip(&self) -> &str {
        self.address
            .split_once('/')
            .map_or(self.address.as_str(), |(ip, _)| ip)
    }

    /// Whether this is an IPv6 address.
    ///
    /// By the presence of a colon, which is what actually distinguishes
    /// them — the 0.3.x `version` field this replaces is gone in 1.0.0.
    #[must_use]
    pub fn is_v6(&self) -> bool {
        self.ip().contains(':')
    }
}

/// A route a plugin installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    /// Destination CIDR.
    pub dst: String,
    /// Next hop.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub gw: String,
}

/// DNS settings a plugin reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Dns {
    /// Nameservers.
    #[serde(default)]
    pub nameservers: Vec<String>,
    /// Search domains.
    #[serde(default)]
    pub search: Vec<String>,
}

impl Dns {
    /// Whether this carries nothing, so it can be omitted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nameservers.is_empty() && self.search.is_empty()
    }
}

/// A CNI `ADD` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CniResult {
    /// The spec version the plugin answered in.
    #[serde(default)]
    pub cni_version: String,
    /// Interfaces created. Empty for a chained plugin that adds none, and
    /// omitted in that case — see `IpConfig::interface`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interfaces: Vec<Interface>,
    /// Addresses assigned.
    #[serde(default)]
    pub ips: Vec<IpConfig>,
    /// Routes installed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<Route>,
    /// DNS settings.
    #[serde(default, skip_serializing_if = "Dns::is_empty")]
    pub dns: Dns,
}

impl CniResult {
    /// The pod's primary IP.
    ///
    /// ★ THE SANDBOX FILTER IS THE LOAD-BEARING PART. A chain reports BOTH
    /// ends of a veth pair, and the host end comes first often enough that
    /// taking `ips[0]` blindly assigns the host's address as the pod IP —
    /// a pod that appears to have an address, routes nowhere, and lands in
    /// Endpoints so a Service sends it traffic. So an address whose
    /// `interface` index names an interface with an EMPTY `sandbox` is
    /// skipped.
    ///
    /// An address with no `interface` index at all is accepted: a plugin
    /// that reports one address and no interfaces (an IPAM-only chain link)
    /// is conformant, and refusing it would reject working configurations.
    #[must_use]
    pub fn pod_ip(&self) -> Option<&str> {
        self.ips
            .iter()
            .find(|ip| match ip.interface {
                Some(idx) => self
                    .interfaces
                    .get(idx)
                    .is_some_and(|i| !i.sandbox.is_empty()),
                None => true,
            })
            .map(IpConfig::ip)
    }

    /// Every sandbox-side address, in order — the dual-stack answer.
    #[must_use]
    pub fn pod_ips(&self) -> Vec<&str> {
        self.ips
            .iter()
            .filter(|ip| match ip.interface {
                Some(idx) => self
                    .interfaces
                    .get(idx)
                    .is_some_and(|i| !i.sandbox.is_empty()),
                None => true,
            })
            .map(IpConfig::ip)
            .collect()
    }
}

/// A CNI error result — what a plugin writes on failure.
///
/// A plugin signals failure by writing THIS to stdout and exiting non-zero.
/// Reading only the exit code loses `msg` and `details`, which is the
/// difference between "CNI ADD failed" and "no IP addresses available in
/// range 10.244.1.0/24".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CniError {
    /// Spec version.
    #[serde(default)]
    pub cni_version: String,
    /// Numeric error code. 1-99 are spec-reserved.
    #[serde(default)]
    pub code: u32,
    /// Short message.
    #[serde(default)]
    pub msg: String,
    /// Longer detail.
    #[serde(default)]
    pub details: String,
}

impl std::fmt::Display for CniError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CNI error {}: {}", self.code, self.msg)?;
        if !self.details.is_empty() {
            write!(f, " ({})", self.details)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn veth_pair_result() -> CniResult {
        // The shape `ptp` actually returns: the HOST end first.
        serde_json::from_str(
            r#"{
              "cniVersion": "1.0.0",
              "interfaces": [
                { "name": "veth1a2b", "mac": "0a:58:0a:f4:01:01" },
                { "name": "eth0", "mac": "0a:58:0a:f4:01:07",
                  "sandbox": "/var/run/netns/cni-1" }
              ],
              "ips": [
                { "address": "10.244.1.7/24", "gateway": "10.244.1.1", "interface": 1 }
              ],
              "routes": [{ "dst": "0.0.0.0/0", "gw": "10.244.1.1" }]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn the_pod_ip_strips_the_prefix_length() {
        // A podIP with a /24 in it is unparseable to Endpoints, kube-proxy
        // and DNS alike, and nothing complains until one of them reads it.
        assert_eq!(veth_pair_result().pod_ip(), Some("10.244.1.7"));
    }

    #[test]
    fn the_host_end_of_a_veth_pair_is_never_taken_as_the_pod_ip() {
        // The failure this prevents: a pod that appears to have an address,
        // routes nowhere, and still lands in Endpoints so a Service sends
        // it traffic.
        let mut r = veth_pair_result();
        r.ips.insert(
            0,
            IpConfig {
                address: "10.244.1.1/24".into(),
                gateway: String::new(),
                interface: Some(0), // the HOST side — sandbox is empty
            },
        );
        assert_eq!(r.pod_ip(), Some("10.244.1.7"), "the sandbox-side address");
    }

    #[test]
    fn an_address_with_no_interface_index_is_accepted() {
        // An IPAM-only chain link reports an address and no interfaces.
        // Refusing it would reject conformant configurations.
        let r: CniResult =
            serde_json::from_str(r#"{"cniVersion":"1.0.0","ips":[{"address":"10.0.0.5/24"}]}"#)
                .unwrap();
        assert_eq!(r.pod_ip(), Some("10.0.0.5"));
    }

    #[test]
    fn a_chained_plugin_that_adds_no_interface_parses() {
        // `portmap` returns essentially an echo. Requiring `interfaces`
        // would fail every chain that ends in one.
        let r: CniResult = serde_json::from_str(r#"{"cniVersion":"1.0.0"}"#).unwrap();
        assert!(r.interfaces.is_empty());
        assert_eq!(r.pod_ip(), None);
    }

    #[test]
    fn dual_stack_returns_both_families_in_order() {
        let mut r = veth_pair_result();
        r.ips.push(IpConfig {
            address: "fd00::7/64".into(),
            gateway: String::new(),
            interface: Some(1),
        });
        assert_eq!(r.pod_ips(), vec!["10.244.1.7", "fd00::7"]);
        assert!(!r.ips[0].is_v6());
        assert!(r.ips[1].is_v6());
    }

    #[test]
    fn an_error_result_keeps_the_reason_the_plugin_gave() {
        // "CNI ADD failed" versus "no IP addresses available in range
        // 10.244.1.0/24" is the whole diagnostic value.
        let e: CniError = serde_json::from_str(
            r#"{"cniVersion":"1.0.0","code":11,
                "msg":"no IP addresses available in range",
                "details":"range is full: 10.244.1.0/24"}"#,
        )
        .unwrap();
        let s = e.to_string();
        assert!(s.contains("no IP addresses available"), "{s}");
        assert!(s.contains("10.244.1.0/24"), "{s}");
    }
}
