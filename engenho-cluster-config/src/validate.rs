//! Cross-field validation. Run by [`crate::ClusterConfig::validate`]
//! at deserialize time + exposed publicly for builder-API consumers.

use std::net::Ipv4Addr;
use std::str::FromStr;

use crate::network::{CniChoice, FlannelBackend, NetworkPolicyEnforce};
use crate::ClusterConfig;

/// Possible errors deserializing or validating a [`ClusterConfig`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// YAML or JSON parse error from serde.
    #[error("parse error: {0}")]
    Parse(String),
    /// Cross-field invariant violation; the inner string is a
    /// human-readable explanation of which rule failed and why.
    #[error("invalid config: {0}")]
    Invalid(String),
}

pub(crate) fn validate(cfg: &ClusterConfig) -> Result<(), ConfigError> {
    let net = &cfg.network;

    // ── Identity ───────────────────────────────────────────────────
    if cfg.cluster_name.is_empty() {
        return Err(ConfigError::Invalid("cluster_name must not be empty".into()));
    }
    if cfg.cluster_name.contains(['/', '\\', ' ']) {
        return Err(ConfigError::Invalid(format!(
            "cluster_name {:?} must not contain slashes or spaces",
            cfg.cluster_name
        )));
    }

    // ── CIDRs ──────────────────────────────────────────────────────
    let cluster = parse_cidr(&net.cluster_cidr, "cluster_cidr")?;
    let service = parse_cidr(&net.service_cidr, "service_cidr")?;
    if cidrs_overlap(cluster, service) {
        return Err(ConfigError::Invalid(format!(
            "cluster_cidr {} overlaps service_cidr {} — pick non-overlapping ranges",
            net.cluster_cidr, net.service_cidr
        )));
    }

    // ── DNS in service CIDR ────────────────────────────────────────
    if !ipv4_in_cidr(net.cluster_dns, service) {
        return Err(ConfigError::Invalid(format!(
            "cluster_dns {} must live inside service_cidr {}",
            net.cluster_dns, net.service_cidr
        )));
    }

    // ── Node port range ────────────────────────────────────────────
    if net.node_port_range.start >= net.node_port_range.end {
        return Err(ConfigError::Invalid(format!(
            "node_port_range.start ({}) must be < end ({})",
            net.node_port_range.start, net.node_port_range.end
        )));
    }
    if net.node_port_range.start < 1024 {
        return Err(ConfigError::Invalid(format!(
            "node_port_range.start ({}) is in the privileged-port range (<1024); k3s rejects",
            net.node_port_range.start
        )));
    }

    // ── CNI / backend compatibility ────────────────────────────────
    if let Some(backend) = net.cni_backend {
        match (net.cni, backend) {
            (CniChoice::Flannel, _) => {} // any flannel backend allowed
            (CniChoice::Calico | CniChoice::Cilium | CniChoice::None, b) => {
                return Err(ConfigError::Invalid(format!(
                    "cni_backend {b:?} is only valid for cni=flannel — set cni=flannel or leave cni_backend unset"
                )));
            }
        }
    }

    // ── IPv6 / dual-stack ──────────────────────────────────────────
    if net.ipv6.dual_stack {
        if net.ipv6.cluster_cidr_v6.is_none() {
            return Err(ConfigError::Invalid(
                "ipv6.dual_stack=true requires ipv6.cluster_cidr_v6".into(),
            ));
        }
        if net.ipv6.service_cidr_v6.is_none() {
            return Err(ConfigError::Invalid(
                "ipv6.dual_stack=true requires ipv6.service_cidr_v6".into(),
            ));
        }
    } else if net.ipv6.cluster_cidr_v6.is_some() || net.ipv6.service_cidr_v6.is_some() {
        return Err(ConfigError::Invalid(
            "ipv6 cidrs set but dual_stack=false — set dual_stack=true or remove the v6 cidrs".into(),
        ));
    }

    // ── NetworkPolicy + CNI ────────────────────────────────────────
    // Delegated enforcement only makes sense when the CNI can enforce.
    if matches!(net.network_policy.enforce, NetworkPolicyEnforce::Delegated)
        && !matches!(net.cni, CniChoice::Calico | CniChoice::Cilium)
    {
        return Err(ConfigError::Invalid(format!(
            "network_policy.enforce=delegated requires cni=calico or cni=cilium (got cni={:?})",
            net.cni
        )));
    }

    // ── kube-proxy disabled requires CNI that replaces it ──────────
    if net.kube_proxy.disabled && !matches!(net.cni, CniChoice::Cilium | CniChoice::None) {
        return Err(ConfigError::Invalid(format!(
            "kube_proxy.disabled=true requires cni=cilium (kube-proxy replacement) or cni=none — got cni={:?}",
            net.cni
        )));
    }

    // ── FluxCD bootstrap consistency ───────────────────────────────
    if cfg.bootstrap.fluxcd.enable && cfg.bootstrap.fluxcd.source.is_none() {
        return Err(ConfigError::Invalid(
            "bootstrap.fluxcd.enable=true requires bootstrap.fluxcd.source".into(),
        ));
    }
    // ── ArgoCD bootstrap consistency ───────────────────────────────
    if cfg.bootstrap.argocd.enable && cfg.bootstrap.argocd.source.is_none() {
        return Err(ConfigError::Invalid(
            "bootstrap.argocd.enable=true requires bootstrap.argocd.source".into(),
        ));
    }

    Ok(())
}

/// Tiny CIDR parser — accepts `a.b.c.d/N` with v4 octets + prefix-len.
/// Returns `(network_address_as_u32, prefix_len)`.
fn parse_cidr(s: &str, field: &str) -> Result<(u32, u8), ConfigError> {
    let (addr_s, prefix_s) = s.split_once('/').ok_or_else(|| {
        ConfigError::Invalid(format!("{field} {s:?} missing /prefix (expected `a.b.c.d/N`)"))
    })?;
    let addr = Ipv4Addr::from_str(addr_s).map_err(|e| {
        ConfigError::Invalid(format!("{field} {s:?} invalid address: {e}"))
    })?;
    let prefix: u8 = prefix_s.parse().map_err(|e| {
        ConfigError::Invalid(format!("{field} {s:?} invalid prefix length: {e}"))
    })?;
    if prefix > 32 {
        return Err(ConfigError::Invalid(format!("{field} {s:?} prefix length >32")));
    }
    Ok((u32::from(addr), prefix))
}

fn cidrs_overlap((a_addr, a_prefix): (u32, u8), (b_addr, b_prefix): (u32, u8)) -> bool {
    let shorter = a_prefix.min(b_prefix);
    // shift by 32 is UB in Rust for u32; guard against the /0 corner case.
    let mask: u32 = if shorter == 0 { 0 } else { (!0u32).checked_shl(u32::from(32 - shorter)).unwrap_or(0) };
    (a_addr & mask) == (b_addr & mask)
}

fn ipv4_in_cidr(addr: Ipv4Addr, (cidr_addr, prefix): (u32, u8)) -> bool {
    let mask: u32 = if prefix == 0 { 0 } else { (!0u32).checked_shl(u32::from(32 - prefix)).unwrap_or(0) };
    (u32::from(addr) & mask) == (cidr_addr & mask)
}
