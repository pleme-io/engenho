//! Per-knob render tests. One test per typed option: set the option to
//! a non-default value, render, assert the relevant flag / YAML key /
//! manifest contains the expected output.
//!
//! These are fast unit-style tests — pure data transforms, no VM, no
//! kubectl. Per-knob integration tests on a live VM live separately
//! (gated behind `--features e2e` or similar).

use std::net::Ipv4Addr;

use engenho_cluster_config::{
    ArgocdBootstrap, BootstrapConfig, ClusterConfig, CniChoice, DnsChoice, FlannelBackend,
    FluxcdBootstrap, GitopsSource, IngressChoice, Ipv6Config, K3sComponent, KubeProxyConfig,
    KubeProxyMode, LoadBalancerChoice, NetworkConfig, NetworkPolicyConfig, NetworkPolicyEnforce,
    PortRange, SecretRef,
};

fn base() -> ClusterConfig {
    ClusterConfig {
        cluster_name: "test".to_string(),
        node_ip:      Ipv4Addr::new(192, 168, 64, 10),
        network:      NetworkConfig::default(),
        bootstrap:    BootstrapConfig::default(),
    }
}

// ─────────────────────────────────────────────────────────────────────
// Validation tests
// ─────────────────────────────────────────────────────────────────────

#[test]
fn default_config_validates() {
    base().validate().unwrap();
}

#[test]
fn empty_cluster_name_rejected() {
    let mut c = base();
    c.cluster_name.clear();
    assert!(c.validate().is_err());
}

#[test]
fn cluster_name_with_slash_rejected() {
    let mut c = base();
    c.cluster_name = "foo/bar".into();
    assert!(c.validate().is_err());
}

#[test]
fn overlapping_cidrs_rejected() {
    let mut c = base();
    c.network.cluster_cidr = "10.42.0.0/16".into();
    c.network.service_cidr = "10.42.0.0/24".into();
    assert!(c.validate().is_err());
}

#[test]
fn cluster_dns_outside_service_cidr_rejected() {
    let mut c = base();
    c.network.cluster_dns = Ipv4Addr::new(10, 99, 0, 10);
    assert!(c.validate().is_err());
}

#[test]
fn node_port_range_start_ge_end_rejected() {
    let mut c = base();
    c.network.node_port_range = PortRange { start: 35_000, end: 30_000 };
    assert!(c.validate().is_err());
}

#[test]
fn privileged_node_port_range_rejected() {
    let mut c = base();
    c.network.node_port_range = PortRange { start: 80, end: 443 };
    assert!(c.validate().is_err());
}

#[test]
fn flannel_backend_with_calico_rejected() {
    let mut c = base();
    c.network.cni = CniChoice::Calico;
    c.network.cni_backend = Some(FlannelBackend::Vxlan);
    assert!(c.validate().is_err());
}

#[test]
fn dual_stack_without_v6_cidrs_rejected() {
    let mut c = base();
    c.network.ipv6 = Ipv6Config { dual_stack: true, cluster_cidr_v6: None, service_cidr_v6: None };
    assert!(c.validate().is_err());
}

#[test]
fn v6_cidrs_without_dual_stack_rejected() {
    let mut c = base();
    c.network.ipv6 = Ipv6Config {
        dual_stack: false,
        cluster_cidr_v6: Some("fd00::/48".into()),
        service_cidr_v6: None,
    };
    assert!(c.validate().is_err());
}

#[test]
fn delegated_policy_with_flannel_rejected() {
    let mut c = base();
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    assert!(c.validate().is_err());
}

#[test]
fn kubeproxy_disabled_with_flannel_rejected() {
    let mut c = base();
    c.network.kube_proxy.disabled = true;
    assert!(c.validate().is_err());
}

#[test]
fn kubeproxy_disabled_with_cilium_accepted() {
    let mut c = base();
    c.network.cni = CniChoice::Cilium;
    c.network.kube_proxy.disabled = true;
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    c.validate().unwrap();
}

/// Install-only mode: enable=true with no source is now valid —
/// the NixOS image bakes the flux2 HelmChart (controllers + CRDs)
/// but no GitRepository / Kustomization CR. A downstream operator
/// primitive (kikai's `gitops::bootstrap`) owns the source-CR
/// lifecycle from the fleet registry, avoiding the rio.cattle.io
/// vs operator patch fight over spec.url.
#[test]
fn flux_enable_without_source_is_install_only() {
    let mut c = base();
    c.bootstrap.fluxcd.enable = true;
    c.validate().expect("install-only mode accepted");
    // Render must emit the install manifest but NOT the source CR.
    let manifests = c.render_manifests();
    let install = manifests.iter().find(|m| m.name == "flux-system-install.yaml");
    assert!(install.is_some(), "install manifest still emitted");
    let source = manifests.iter().find(|m| m.name == "flux-system-source.yaml");
    assert!(source.is_none(), "source manifest must NOT be emitted in install-only mode");
}

#[test]
fn argo_enable_without_source_is_install_only() {
    let mut c = base();
    c.bootstrap.argocd.enable = true;
    c.validate().expect("install-only mode accepted");
    let manifests = c.render_manifests();
    let install = manifests.iter().find(|m| m.name == "argocd-install.yaml");
    assert!(install.is_some(), "install manifest still emitted");
    let app = manifests.iter().find(|m| m.name.contains("application"));
    assert!(app.is_none(), "application CR must NOT be emitted in install-only mode");
}

// ─────────────────────────────────────────────────────────────────────
// Render: k3s config.yaml — per-knob
// ─────────────────────────────────────────────────────────────────────

#[test]
fn render_yaml_includes_node_name() {
    let yaml = base().render_k3s_config_yaml();
    assert!(yaml.contains("node-name: test"), "missing node-name: {yaml}");
}

#[test]
fn render_yaml_includes_node_ip_as_tls_san() {
    let yaml = base().render_k3s_config_yaml();
    assert!(yaml.contains("192.168.64.10"), "node_ip missing from tls-san: {yaml}");
}

#[test]
fn render_yaml_extra_tls_sans_appended_and_deduped() {
    let mut c = base();
    c.network.tls_sans = vec!["foo.example.com".into(), "192.168.64.10".into()]; // dup
    let yaml = c.render_k3s_config_yaml();
    assert!(yaml.contains("foo.example.com"));
    let count = yaml.matches("192.168.64.10").count();
    assert_eq!(count, 1, "duplicate tls-san should dedupe: {yaml}");
}

#[test]
fn render_yaml_omits_cluster_and_service_cidrs() {
    // CIDRs are owned by the nixos consumer (blackmatter's
    // services.blackmatter.k3s.{clusterCIDR,serviceCIDR}) — emitting
    // them HERE too would result in k3s parsing them as dual-stack
    // (config.yaml + cmdline both set → list of 2 entries → "must be
    // of different IP family" failure). Verified empirically on
    // engenho-local: dropping these from YAML lets k3s come up.
    let mut c = base();
    c.network.cluster_cidr = "10.100.0.0/16".into();
    let yaml = c.render_k3s_config_yaml();
    assert!(!yaml.contains("cluster-cidr:"), "YAML must not emit cluster-cidr: {yaml}");
    assert!(!yaml.contains("service-cidr:"), "YAML must not emit service-cidr: {yaml}");
    assert!(!yaml.contains("cluster-dns:"),  "YAML must not emit cluster-dns: {yaml}");
}

#[test]
fn render_yaml_custom_node_port_range() {
    let mut c = base();
    c.network.node_port_range = PortRange { start: 31_000, end: 31_999 };
    let yaml = c.render_k3s_config_yaml();
    assert!(yaml.contains("service-node-port-range: 31000-31999"));
}

#[test]
fn render_yaml_mtu_when_set() {
    let mut c = base();
    c.network.mtu = Some(9000);
    let yaml = c.render_k3s_config_yaml();
    assert!(yaml.contains("flannel-iface-mtu: 9000"));
}

#[test]
fn render_yaml_mtu_omitted_when_none() {
    let yaml = base().render_k3s_config_yaml();
    assert!(!yaml.contains("flannel-iface-mtu"));
}

#[test]
fn render_yaml_advertise_address() {
    let mut c = base();
    c.network.advertise_address = Some(Ipv4Addr::new(10, 0, 0, 5));
    let yaml = c.render_k3s_config_yaml();
    assert!(yaml.contains("advertise-address: 10.0.0.5"));
}

#[test]
fn render_yaml_bind_address() {
    let mut c = base();
    c.network.bind_address = Some(Ipv4Addr::new(0, 0, 0, 0));
    let yaml = c.render_k3s_config_yaml();
    assert!(yaml.contains("bind-address: 0.0.0.0"));
}

#[test]
fn render_yaml_dual_stack_cidrs_not_in_yaml() {
    // Same reasoning as render_yaml_omits_cluster_and_service_cidrs:
    // CIDRs (including dual-stack) are consumer-owned via blackmatter's
    // k3s clusterCIDR/serviceCIDR options. Dual-stack propagation needs
    // the consumer to be aware of cfg.clusterConfig.network.ipv6 and
    // override its clusterCIDR/serviceCIDR to a comma-separated list.
    // Tracked as a follow-up (consumer-side mkForce wiring).
    let mut c = base();
    c.network.ipv6 = Ipv6Config {
        dual_stack: true,
        cluster_cidr_v6: Some("fd00:10:42::/56".into()),
        service_cidr_v6: Some("fd00:10:43::/112".into()),
    };
    let yaml = c.render_k3s_config_yaml();
    assert!(!yaml.contains("cluster-cidr:"));
    assert!(!yaml.contains("service-cidr:"));
}

// ─────────────────────────────────────────────────────────────────────
// Render: k3s server args — per-knob
// ─────────────────────────────────────────────────────────────────────

#[test]
fn render_args_default_has_no_disable() {
    let args = base().render_k3s_server_args();
    assert!(args.iter().all(|a| !a.starts_with("--disable=")), "default should have no disables: {args:?}");
}

#[test]
fn render_args_nginx_ingress_disables_traefik() {
    let mut c = base();
    c.network.ingress = IngressChoice::Nginx;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--disable=traefik".to_string()), "got: {args:?}");
}

#[test]
fn render_args_metallb_disables_servicelb() {
    let mut c = base();
    c.network.load_balancer = LoadBalancerChoice::Metallb;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--disable=servicelb".to_string()));
}

#[test]
fn render_args_external_dns_disables_coredns() {
    let mut c = base();
    c.network.dns = DnsChoice::External;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--disable=coredns".to_string()));
}

#[test]
fn render_args_network_policy_disabled_disables_controller() {
    let mut c = base();
    c.network.network_policy.enforce = NetworkPolicyEnforce::Disabled;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--disable=network-policy".to_string()));
}

#[test]
fn render_args_calico_flannel_backend_none() {
    let mut c = base();
    c.network.cni = CniChoice::Calico;
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--flannel-backend=none".to_string()));
    assert!(args.contains(&"--disable-network-policy".to_string()));
}

#[test]
fn render_args_flannel_backend_wireguard_native() {
    let mut c = base();
    c.network.cni_backend = Some(FlannelBackend::WireguardNative);
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--flannel-backend=wireguard-native".to_string()));
}

#[test]
fn render_args_ipvs_kube_proxy() {
    let mut c = base();
    c.network.kube_proxy.mode = KubeProxyMode::Ipvs;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--kube-proxy-arg=proxy-mode=ipvs".to_string()));
}

#[test]
fn render_args_nftables_kube_proxy() {
    let mut c = base();
    c.network.kube_proxy.mode = KubeProxyMode::Nftables;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--kube-proxy-arg=proxy-mode=nftables".to_string()));
}

#[test]
fn render_args_cilium_disables_kube_proxy() {
    let mut c = base();
    c.network.cni = CniChoice::Cilium;
    c.network.kube_proxy.disabled = true;
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--disable=kube-proxy".to_string()));
    assert!(args.contains(&"--disable-kube-proxy".to_string()));
}

#[test]
fn render_args_disabled_components_appear_in_disable_list() {
    let mut c = base();
    c.network.disable_components = vec![K3sComponent::MetricsServer, K3sComponent::LocalStorage];
    let args = c.render_k3s_server_args();
    assert!(args.contains(&"--disable=metrics-server".to_string()));
    assert!(args.contains(&"--disable=local-storage".to_string()));
}

// ─────────────────────────────────────────────────────────────────────
// Render: bootstrap manifests — per-knob
// ─────────────────────────────────────────────────────────────────────

#[test]
fn render_manifests_default_is_empty() {
    let m = base().render_bootstrap_manifests();
    assert!(m.is_empty(), "default should emit no manifests, got: {:?}", m.iter().map(|x| &x.filename).collect::<Vec<_>>());
}

#[test]
fn render_manifests_calico_emits_calico_helmchart() {
    let mut c = base();
    c.network.cni = CniChoice::Calico;
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    let m = c.render_bootstrap_manifests();
    let calico = m.iter().find(|x| x.filename == "calico.yaml").expect("calico manifest");
    assert!(calico.body.contains("chart: tigera-operator"));
}

#[test]
fn render_manifests_cilium_kube_proxy_replacement() {
    let mut c = base();
    c.network.cni = CniChoice::Cilium;
    c.network.kube_proxy.disabled = true;
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    let m = c.render_bootstrap_manifests();
    let cilium = m.iter().find(|x| x.filename == "cilium.yaml").expect("cilium manifest");
    assert!(cilium.body.contains("kubeProxyReplacement: true"));
}

#[test]
fn render_manifests_nginx_emits_ingress_nginx() {
    let mut c = base();
    c.network.ingress = IngressChoice::Nginx;
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "ingress-nginx.yaml"));
}

#[test]
fn render_manifests_contour_emits_contour() {
    let mut c = base();
    c.network.ingress = IngressChoice::Contour;
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "contour.yaml"));
}

#[test]
fn render_manifests_gateway_api_emits_envoy_gateway() {
    let mut c = base();
    c.network.ingress = IngressChoice::GatewayApi;
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "gateway-api.yaml"));
}

#[test]
fn render_manifests_metallb_emits_metallb() {
    let mut c = base();
    c.network.load_balancer = LoadBalancerChoice::Metallb;
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "metallb.yaml"));
}

#[test]
fn render_manifests_kubevip_emits_kubevip() {
    let mut c = base();
    c.network.load_balancer = LoadBalancerChoice::KubeVip;
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "kube-vip.yaml"));
}

#[test]
fn render_manifests_nodelocal_dns_emits_chart() {
    let mut c = base();
    c.network.dns = DnsChoice::NodelocalDns;
    let m = c.render_bootstrap_manifests();
    let dns = m.iter().find(|x| x.filename == "nodelocal-dns.yaml").expect("nodelocal-dns");
    assert!(dns.body.contains("clusterDNS: 10.43.0.10"));
}

#[test]
fn render_manifests_fluxcd_emits_install_and_source() {
    let mut c = base();
    c.bootstrap.fluxcd = FluxcdBootstrap {
        enable: true,
        source: Some(GitopsSource {
            url:    "https://github.com/pleme-io/k8s".into(),
            branch: "main".into(),
            auth:   None,
        }),
        interval: "1m".into(),
        path:     "./clusters/engenho-local".into(),
        version:  "v2.3.0".into(),
    };
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "flux-system-install.yaml"));
    let src = m.iter().find(|x| x.filename == "flux-system-source.yaml").expect("flux source");
    assert!(src.body.contains("kind: GitRepository"));
    assert!(src.body.contains("url: https://github.com/pleme-io/k8s"));
    assert!(src.body.contains("kind: Kustomization"));
    assert!(src.body.contains("path: ./clusters/engenho-local"));
}

#[test]
fn render_manifests_fluxcd_with_auth_emits_secret_placeholder() {
    let mut c = base();
    c.bootstrap.fluxcd = FluxcdBootstrap {
        enable: true,
        source: Some(GitopsSource {
            url:    "https://github.com/pleme-io/k8s".into(),
            branch: "main".into(),
            auth:   Some(SecretRef {
                kind:     engenho_cluster_config::bootstrap::SecretKind::HttpsToken,
                sops_key: "clusters/test/flux-github-token".into(),
            }),
        }),
        interval: "1m".into(),
        path:     "./clusters/test".into(),
        version:  "v2.3.0".into(),
    };
    let m = c.render_bootstrap_manifests();
    let src = m.iter().find(|x| x.filename == "flux-system-source.yaml").expect("flux source");
    assert!(src.body.contains("secretRef:"));
    assert!(src.body.contains("name: test-gitops-auth"));
    assert!(src.body.contains("REPLACED-BY-SOPS-NIX-ACTIVATION"));
}

#[test]
fn render_manifests_argocd_emits_install_and_application() {
    let mut c = base();
    c.bootstrap.argocd = ArgocdBootstrap {
        enable: true,
        source: Some(GitopsSource {
            url:    "https://github.com/pleme-io/k8s".into(),
            branch: "main".into(),
            auth:   None,
        }),
        target_revision: None,
        path:    "./argocd".into(),
        version: "v2.13.0".into(),
    };
    let m = c.render_bootstrap_manifests();
    assert!(m.iter().any(|x| x.filename == "argocd-install.yaml"));
    let app = m.iter().find(|x| x.filename == "argocd-application.yaml").expect("argo app");
    assert!(app.body.contains("kind: Application"));
    assert!(app.body.contains("repoURL: https://github.com/pleme-io/k8s"));
    assert!(app.body.contains("targetRevision: main")); // falls back to branch
    assert!(app.body.contains("path: ./argocd"));
}

// ─────────────────────────────────────────────────────────────────────
// YAML round-trip
// ─────────────────────────────────────────────────────────────────────

#[test]
fn yaml_round_trip_preserves_full_config() {
    let mut c = base();
    c.network.cni = CniChoice::Cilium;
    c.network.kube_proxy.disabled = true;
    c.network.network_policy.enforce = NetworkPolicyEnforce::Delegated;
    c.network.ingress = IngressChoice::Nginx;
    c.network.load_balancer = LoadBalancerChoice::Metallb;
    c.network.dns = DnsChoice::NodelocalDns;
    c.network.tls_sans = vec!["engenho-local.quero.cloud".into()];
    c.bootstrap.fluxcd = FluxcdBootstrap {
        enable: true,
        source: Some(GitopsSource {
            url: "https://github.com/pleme-io/k8s".into(),
            branch: "main".into(),
            auth: None,
        }),
        interval: "30s".into(),
        path:     "./clusters/test".into(),
        version:  "v2.3.0".into(),
    };
    c.validate().unwrap();
    let yaml = serde_yaml::to_string(&c).unwrap();
    let back = ClusterConfig::from_yaml_str(&yaml).unwrap();
    assert_eq!(c, back);
}
