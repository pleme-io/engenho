//! Network configuration — every k3s/k8s network knob, typed.
//!
//! Defaults mirror k3s' built-in behavior (flannel/vxlan + iptables
//! kube-proxy + traefik + servicelb + coredns + single-stack IPv4) so a
//! `NetworkConfig::default()` renders to identical config as today's
//! nixos-k3s-vm profile. Every non-default value flips behavior visible
//! at runtime + carries an integration test asserting that.

use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

/// Top-level network surface. Every k3s networking decision lives here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct NetworkConfig {
    /// Choice of CNI plugin. `flannel` is k3s' default + zero-config.
    /// Switching to `calico` or `cilium` requires k3s to be started
    /// with `--flannel-backend=none` + the chosen CNI's install
    /// manifest dropped into `/var/lib/rancher/k3s/server/manifests/`.
    pub cni: CniChoice,

    /// Backend mode for the chosen CNI. For flannel: vxlan/host-gw/
    /// wireguard-native/wireguard-legacy. For calico: bgp/vxlan/ipip.
    /// For cilium: vxlan/geneve/native-routing. `None` lets the CNI
    /// pick its own default. Renderer rejects mode/CNI mismatches.
    pub cni_backend: Option<FlannelBackend>,

    /// Pod-network CIDR. k3s default `10.42.0.0/16`. Must not overlap
    /// `service_cidr` or any host-network range.
    pub cluster_cidr: String,

    /// Service-network CIDR. k3s default `10.43.0.0/16`. The
    /// kube-DNS address (see `cluster_dns`) lives inside this.
    pub service_cidr: String,

    /// CoreDNS / cluster-DNS service IP. Must live inside
    /// `service_cidr`. k3s default `10.43.0.10`.
    pub cluster_dns: Ipv4Addr,

    /// `--service-node-port-range` for NodePort services. Default
    /// `30000-32767` matches upstream Kubernetes + k3s.
    pub node_port_range: PortRange,

    /// MTU for the CNI overlay. `None` lets the CNI pick (usually
    /// detected from the host interface). Override for environments
    /// with non-standard MTUs (jumbo frames, encapsulated tunnels).
    pub mtu: Option<u32>,

    /// kube-proxy mode + tuning.
    pub kube_proxy: KubeProxyConfig,

    /// NetworkPolicy enforcement. k3s installs a network-policy
    /// controller by default; disabling it is required when running
    /// a CNI that provides its own (cilium, calico).
    pub network_policy: NetworkPolicyConfig,

    /// Ingress controller. k3s ships traefik by default. Selecting
    /// `nginx`/`contour`/`gateway-api`/`none` adds `traefik` to the
    /// disabled-components list and drops the chosen controller's
    /// install manifest.
    pub ingress: IngressChoice,

    /// Service-LoadBalancer controller. k3s ships servicelb (klipper)
    /// by default. `metallb` / `kube-vip` install via manifest;
    /// `none` disables servicelb without a replacement.
    pub load_balancer: LoadBalancerChoice,

    /// Cluster DNS provider. k3s default `coredns`. `nodelocal-dns`
    /// installs the upstream NodeLocal DNSCache addon on top.
    pub dns: DnsChoice,

    /// IPv6 + dual-stack. Off by default. Enabling adds an IPv6
    /// `cluster_cidr` + `service_cidr` to the comma-separated lists
    /// k3s expects.
    pub ipv6: Ipv6Config,

    /// Extra SANs the k3s server certificate must include. `node_ip`
    /// and `cluster_name` are added automatically; only put values
    /// here that aren't derivable.
    pub tls_sans: Vec<String>,

    /// `--advertise-address` for the API server. `None` defaults to
    /// `node_ip`. Override for clusters behind NAT or with multiple
    /// interfaces.
    pub advertise_address: Option<Ipv4Addr>,

    /// `--bind-address` for the API server. `None` defaults to
    /// `0.0.0.0` (listen on all interfaces). Set to a specific IP
    /// to constrain.
    pub bind_address: Option<Ipv4Addr>,

    /// k3s components to disable. Setting `ingress` to a non-default
    /// or `load_balancer` to a non-default auto-adds the
    /// corresponding component here; this list is for additional
    /// explicit disables (e.g. `metrics-server`, `local-storage`).
    pub disable_components: Vec<K3sComponent>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            cni: CniChoice::Flannel,
            cni_backend: None,
            cluster_cidr: "10.42.0.0/16".to_string(),
            service_cidr: "10.43.0.0/16".to_string(),
            cluster_dns: Ipv4Addr::new(10, 43, 0, 10),
            node_port_range: PortRange {
                start: 30_000,
                end: 32_767,
            },
            mtu: None,
            kube_proxy: KubeProxyConfig::default(),
            network_policy: NetworkPolicyConfig::default(),
            ingress: IngressChoice::Traefik,
            load_balancer: LoadBalancerChoice::Servicelb,
            dns: DnsChoice::Coredns,
            ipv6: Ipv6Config::default(),
            tls_sans: vec!["localhost".to_string(), "127.0.0.1".to_string()],
            advertise_address: None,
            bind_address: None,
            disable_components: vec![],
        }
    }
}

/// Choice of CNI plugin.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CniChoice {
    /// k3s' default — flannel with vxlan backend. Zero-config.
    Flannel,
    /// Calico — BGP/VXLAN/IPIP-capable, NetworkPolicy native.
    /// k3s started with `--flannel-backend=none`; install manifest
    /// drops calico's operator.
    Calico,
    /// Cilium — eBPF-based, replaces kube-proxy when configured.
    /// Same `--flannel-backend=none`; install manifest is cilium's
    /// CRDs + agent DaemonSet.
    Cilium,
    /// `--flannel-backend=none --disable-network-policy --disable-cni
    /// true` — caller installs their own CNI out-of-band.
    None,
}

/// CNI backend mode. Semantics depend on the chosen [`CniChoice`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum FlannelBackend {
    /// Flannel VXLAN — default; works across L2 boundaries.
    Vxlan,
    /// Flannel host-gw — pure L3 routing, requires nodes on same L2.
    HostGw,
    /// Flannel wireguard-native — encrypted, kernel-mode wireguard.
    WireguardNative,
    /// Flannel wireguard-legacy — userspace wireguard.
    WireguardLegacy,
    /// Flannel IPSec.
    Ipsec,
}

/// kube-proxy configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct KubeProxyConfig {
    /// Datapath mode. `iptables` is upstream default + k3s default.
    /// `ipvs` for higher throughput / more service backends. `nftables`
    /// for nft-native (Kubernetes 1.31+ beta, 1.33 GA-track).
    pub mode: KubeProxyMode,

    /// If `true`, disable kube-proxy entirely. Used when the CNI
    /// provides kube-proxy replacement (cilium with
    /// `kubeProxyReplacement=true`). Renderer ensures this implies
    /// `--disable-kube-proxy` on k3s.
    pub disabled: bool,
}

impl Default for KubeProxyConfig {
    fn default() -> Self {
        Self {
            mode: KubeProxyMode::Iptables,
            disabled: false,
        }
    }
}

/// kube-proxy datapath mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum KubeProxyMode {
    /// Iptables — the default. Best compatibility.
    Iptables,
    /// IPVS — kernel-side hash table; better at high service counts.
    Ipvs,
    /// nftables — modern nft-native backend (beta in 1.31, GA-track 1.33).
    Nftables,
}

/// NetworkPolicy enforcement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct NetworkPolicyConfig {
    /// `enabled` keeps k3s' built-in network-policy controller;
    /// `disabled` adds `--disable-network-policy`; `delegated` means
    /// the CNI (cilium/calico) enforces and the built-in controller
    /// is off.
    pub enforce: NetworkPolicyEnforce,
}

impl Default for NetworkPolicyConfig {
    fn default() -> Self {
        Self {
            enforce: NetworkPolicyEnforce::Enabled,
        }
    }
}

/// NetworkPolicy enforcement choice.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkPolicyEnforce {
    /// k3s' built-in controller enforces policies.
    Enabled,
    /// `--disable-network-policy`; no controller, policies are no-ops.
    Disabled,
    /// CNI (cilium/calico/…) enforces; built-in controller off.
    Delegated,
}

/// Ingress controller choice.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum IngressChoice {
    /// k3s' default — traefik installed via auto-apply manifest.
    Traefik,
    /// ingress-nginx — upstream's most-deployed controller.
    Nginx,
    /// Contour — Envoy-based, GatewayAPI-first.
    Contour,
    /// Gateway API + Envoy Gateway — modern spec; no traditional
    /// Ingress object support, only Gateway/HTTPRoute.
    GatewayApi,
    /// No ingress controller. Operators bring their own.
    None,
}

/// Service-LoadBalancer controller choice.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LoadBalancerChoice {
    /// k3s' default — klipper-lb (servicelb).
    Servicelb,
    /// MetalLB — L2/BGP-mode LoadBalancer for bare metal.
    Metallb,
    /// kube-vip — VIP + load balancer + control-plane HA in one pod.
    KubeVip,
    /// No LB; `Service.type=LoadBalancer` stays `Pending`.
    None,
}

/// Cluster DNS choice.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum DnsChoice {
    /// k3s' default — coredns as the only DNS service.
    Coredns,
    /// coredns + NodeLocal DNSCache for per-node caching.
    NodelocalDns,
    /// `--disable=coredns`; operator brings their own DNS.
    External,
}

/// IPv6 / dual-stack configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct Ipv6Config {
    /// `true` enables dual-stack; renderer appends an IPv6 CIDR to
    /// `cluster_cidr` and `service_cidr` (k3s expects a
    /// comma-separated list).
    pub dual_stack: bool,

    /// IPv6 pod-network CIDR. Required when `dual_stack` is true.
    pub cluster_cidr_v6: Option<String>,

    /// IPv6 service-network CIDR. Required when `dual_stack` is true.
    pub service_cidr_v6: Option<String>,
}

impl Default for Ipv6Config {
    fn default() -> Self {
        Self {
            dual_stack: false,
            cluster_cidr_v6: None,
            service_cidr_v6: None,
        }
    }
}

/// `--service-node-port-range` for NodePort services.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    /// Inclusive lower bound (default 30000).
    pub start: u16,
    /// Inclusive upper bound (default 32767).
    pub end: u16,
}

impl Default for PortRange {
    fn default() -> Self {
        Self {
            start: 30_000,
            end: 32_767,
        }
    }
}

/// k3s component flags for `--disable=<component>`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum K3sComponent {
    /// Disable the bundled traefik HelmChart.
    Traefik,
    /// Disable klipper-lb (servicelb).
    Servicelb,
    /// Disable metrics-server.
    MetricsServer,
    /// Disable local-path-provisioner.
    LocalStorage,
    /// Disable coredns.
    Coredns,
    /// Disable the built-in network-policy controller.
    NetworkPolicy,
    /// Disable Helm controller (the one k3s ships, not external Helm).
    HelmController,
    /// Disable cloud-controller-manager. Required for some externally-
    /// managed clusters.
    CloudController,
    /// Disable kube-proxy (set when a CNI replaces it).
    KubeProxy,
}

impl K3sComponent {
    /// The `--disable=<value>` string k3s expects.
    #[must_use]
    pub fn as_k3s_disable(&self) -> &'static str {
        match self {
            Self::Traefik => "traefik",
            Self::Servicelb => "servicelb",
            Self::MetricsServer => "metrics-server",
            Self::LocalStorage => "local-storage",
            Self::Coredns => "coredns",
            Self::NetworkPolicy => "network-policy",
            Self::HelmController => "helm-controller",
            Self::CloudController => "cloud-controller",
            Self::KubeProxy => "kube-proxy",
        }
    }
}
