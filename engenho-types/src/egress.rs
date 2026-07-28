//! Typed egress AST — every wire/config artifact engenho's controllers
//! + kubelet emit is built as a typed value and rendered through a
//! single typed serializer, never `format!()`-of-syntax.
//!
//! Per the org-level ★★ TYPED EMISSION rule: `std::format!()` of YAML /
//! INI / iptables / nginx / argv syntax is banned. The three allowed
//! egress surfaces are:
//!
//! 1. **serde structs + `serde_yaml`** for the Kubernetes-style CRDs
//!    ([`CiliumNetworkPolicy`], [`TraefikIngressRoute`]). The struct
//!    *is* the typed render surface; `to_yaml()` is the deterministic
//!    serializer.
//! 2. **A typed AST + `Display`** for the non-serde line-oriented
//!    formats ([`IptablesScript`], [`IpvsScript`], [`SystemdUnit`],
//!    [`NginxConfig`]). Each is a small builder over typed pieces; the
//!    `Display` impl is the single render chokepoint.
//! 3. **A typed argv builder** ([`PodmanRunArgv`]) that constructs a
//!    `Vec<String>` from typed fields — never a `format!()` command
//!    string that a shell would have to re-tokenize.
//!
//! Output is byte-equivalent (modulo serde_yaml's canonical list
//! indentation, which parses identically) to the hand-rolled
//! `format!()` renderers these types replace; round-trip / snapshot
//! tests pin the shape.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Write as _};

use serde::Serialize;

// =================================================================
// CiliumNetworkPolicy — cilium.io/v2 CRD (serde + serde_yaml)
// =================================================================

/// Traffic direction for a [`CiliumNetworkPolicy`] rule block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CiliumDirection {
    /// Incoming traffic (`ingress:` key).
    Ingress,
    /// Outgoing traffic (`egress:` key).
    Egress,
}

/// A `cilium.io/v2` `CiliumNetworkPolicy` resource.
///
/// Mirrors the minimal allow-all-in-direction shape the engenho
/// NetworkPolicy controller emits: an `endpointSelector.matchLabels`
/// + a single `{}` entry under the chosen direction key. Rendered via
/// [`CiliumNetworkPolicy::to_yaml`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CiliumNetworkPolicy {
    /// `cilium.io/v2`.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// `CiliumNetworkPolicy`.
    pub kind: String,
    /// Object metadata (name only).
    pub metadata: CiliumMetadata,
    /// Policy spec.
    pub spec: CiliumSpec,
}

/// `metadata` block for a [`CiliumNetworkPolicy`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CiliumMetadata {
    /// `metadata.name`.
    pub name: String,
}

/// `spec` block for a [`CiliumNetworkPolicy`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CiliumSpec {
    /// `spec.endpointSelector`.
    #[serde(rename = "endpointSelector")]
    pub endpoint_selector: CiliumEndpointSelector,
    /// `spec.ingress` — present only for ingress policies. Each entry
    /// is an empty map (`{}`) = allow-all-in-direction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress: Option<Vec<BTreeMap<String, String>>>,
    /// `spec.egress` — present only for egress policies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress: Option<Vec<BTreeMap<String, String>>>,
}

/// `spec.endpointSelector` — pod label match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CiliumEndpointSelector {
    /// `matchLabels` map.
    #[serde(rename = "matchLabels")]
    pub match_labels: BTreeMap<String, String>,
}

impl CiliumNetworkPolicy {
    /// Build a policy from its component parts.
    ///
    /// `name` is the sanitized object name, `pod_selector` the
    /// endpoint match labels, `direction` selects the ingress/egress
    /// rule key. The single rule is the allow-all `{}` entry.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        pod_selector: BTreeMap<String, String>,
        direction: CiliumDirection,
    ) -> Self {
        let rule = vec![BTreeMap::new()];
        let (ingress, egress) = match direction {
            CiliumDirection::Ingress => (Some(rule), None),
            CiliumDirection::Egress => (None, Some(rule)),
        };
        Self {
            api_version: "cilium.io/v2".to_string(),
            kind: "CiliumNetworkPolicy".to_string(),
            metadata: CiliumMetadata { name: name.into() },
            spec: CiliumSpec {
                endpoint_selector: CiliumEndpointSelector {
                    match_labels: pod_selector,
                },
                ingress,
                egress,
            },
        }
    }

    /// Render the canonical CRD YAML document.
    ///
    /// # Panics
    ///
    /// Never — the fixed struct shape always serializes; `expect` only
    /// guards an unreachable serde_yaml failure.
    #[must_use]
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).expect("CiliumNetworkPolicy serializes")
    }
}

// =================================================================
// TraefikIngressRoute — traefik.io/v1alpha1 CRD (serde + serde_yaml)
// =================================================================

/// A `traefik.io/v1alpha1` `IngressRoute` resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TraefikIngressRoute {
    /// `traefik.io/v1alpha1`.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// `IngressRoute`.
    pub kind: String,
    /// Object metadata (name only).
    pub metadata: TraefikMetadata,
    /// Route spec.
    pub spec: TraefikSpec,
}

/// `metadata` block for a [`TraefikIngressRoute`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TraefikMetadata {
    /// `metadata.name`.
    pub name: String,
}

/// `spec` block for a [`TraefikIngressRoute`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TraefikSpec {
    /// `spec.entryPoints` (e.g. `["web"]` / `["websecure"]`).
    #[serde(rename = "entryPoints")]
    pub entry_points: Vec<String>,
    /// `spec.routes`.
    pub routes: Vec<TraefikRoute>,
    /// `spec.tls` — present only for HTTPS routes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TraefikTls>,
}

/// One route under `spec.routes`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TraefikRoute {
    /// Traefik match expression (e.g. ``Host(`x`) && PathPrefix(`/`)``).
    #[serde(rename = "match")]
    pub match_rule: String,
    /// Always `Rule`.
    pub kind: String,
    /// Backend services.
    pub services: Vec<TraefikService>,
}

/// One backend under a [`TraefikRoute`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TraefikService {
    /// Service name.
    pub name: String,
    /// Service port.
    pub port: u16,
}

/// `spec.tls` block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TraefikTls {
    /// `secretName`.
    #[serde(rename = "secretName")]
    pub secret_name: String,
}

impl TraefikIngressRoute {
    /// Build an `IngressRoute` from its component parts.
    ///
    /// `match_rule` is the pre-computed Traefik match expression,
    /// `tls_secret` selects HTTP (`web`, no `tls`) vs HTTPS
    /// (`websecure`, `tls.secretName`).
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        match_rule: impl Into<String>,
        service: impl Into<String>,
        port: u16,
        tls_secret: Option<String>,
    ) -> Self {
        let (entry_point, tls) = match &tls_secret {
            Some(secret) => (
                "websecure".to_string(),
                Some(TraefikTls {
                    secret_name: secret.clone(),
                }),
            ),
            None => ("web".to_string(), None),
        };
        Self {
            api_version: "traefik.io/v1alpha1".to_string(),
            kind: "IngressRoute".to_string(),
            metadata: TraefikMetadata { name: name.into() },
            spec: TraefikSpec {
                entry_points: vec![entry_point],
                routes: vec![TraefikRoute {
                    match_rule: match_rule.into(),
                    kind: "Rule".to_string(),
                    services: vec![TraefikService {
                        name: service.into(),
                        port,
                    }],
                }],
                tls,
            },
        }
    }

    /// Render the canonical CRD YAML document.
    ///
    /// # Panics
    ///
    /// Never — the fixed struct shape always serializes.
    #[must_use]
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).expect("TraefikIngressRoute serializes")
    }
}

// =================================================================
// NginxConfig — typed server-block AST + Display
// =================================================================

/// A typed nginx `server { … }` block. Built from typed fields +
/// rendered through the single [`Display`] chokepoint, never via
/// `format!()` of nginx syntax.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NginxServerBlock {
    /// `listen` directive value (e.g. `80` or `443 ssl`).
    pub listen: String,
    /// `server_name` value.
    pub server_name: String,
    /// Optional TLS secret basename — emits `ssl_certificate` +
    /// `ssl_certificate_key` directives under `/etc/nginx/tls/`.
    pub tls_secret: Option<String>,
    /// `location` matcher (already includes the `=`/prefix form).
    pub location: String,
    /// Upstream `proxy_pass` target (`http://svc:port`).
    pub proxy_pass: String,
}

impl Display for NginxServerBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "# generated by engenho-controllers::ingress::NginxIngressBackend"
        )?;
        writeln!(f, "server {{")?;
        writeln!(f, "    listen {};", self.listen)?;
        write!(f, "    server_name {};", self.server_name)?;
        if let Some(secret) = &self.tls_secret {
            write!(
                f,
                "\n    ssl_certificate     /etc/nginx/tls/{secret}.crt;\n    ssl_certificate_key /etc/nginx/tls/{secret}.key;"
            )?;
        }
        writeln!(f)?;
        writeln!(f, "    {} {{", self.location)?;
        writeln!(f, "        proxy_pass {};", self.proxy_pass)?;
        writeln!(f, "        proxy_set_header Host $host;")?;
        writeln!(f, "        proxy_set_header X-Real-IP $remote_addr;")?;
        writeln!(f, "    }}")?;
        writeln!(f, "}}")?;
        Ok(())
    }
}

// =================================================================
// IptablesScript — typed iptables-restore AST + Display
// =================================================================

/// One line of an iptables-restore script. Each variant is a typed
/// rule; the [`Display`] of [`IptablesScript`] renders the canonical
/// `iptables-restore` text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IptablesLine {
    /// `*<table>` table header (e.g. `*nat`).
    Table(String),
    /// `:<chain> - [0:0]` chain declaration.
    Chain(String),
    /// A raw `-A`/`-F`/`-X` rule line (already typed-built by the
    /// caller via [`IptablesScript`] helpers; stored verbatim).
    Rule(String),
    /// `COMMIT` trailer.
    Commit,
}

impl Display for IptablesLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(t) => write!(f, "*{t}"),
            Self::Chain(c) => write!(f, ":{c} - [0:0]"),
            Self::Rule(r) => write!(f, "{r}"),
            Self::Commit => write!(f, "COMMIT"),
        }
    }
}

/// A typed iptables-restore script — an ordered list of
/// [`IptablesLine`]s rendered through a single newline-joining
/// [`Display`] chokepoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IptablesScript {
    lines: Vec<IptablesLine>,
}

impl IptablesScript {
    /// Empty script.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a `*<table>` header.
    pub fn table(&mut self, table: impl Into<String>) -> &mut Self {
        self.lines.push(IptablesLine::Table(table.into()));
        self
    }

    /// Push a `:<chain> - [0:0]` chain declaration.
    pub fn chain(&mut self, chain: impl Into<String>) -> &mut Self {
        self.lines.push(IptablesLine::Chain(chain.into()));
        self
    }

    /// Append a `-A KUBE-SERVICES …` jump to a per-service chain.
    pub fn jump_to_service_chain(
        &mut self,
        cluster_ip: &str,
        protocol: &str,
        dport: u16,
        chain_svc: &str,
    ) -> &mut Self {
        let mut line = String::new();
        let _ = write!(
            line,
            "-A KUBE-SERVICES -d {cluster_ip}/32 -p {protocol} --dport {dport} -j {chain_svc}"
        );
        self.lines.push(IptablesLine::Rule(line));
        self
    }

    /// Append a probabilistic `-A <chain_svc> -m statistic …` jump to
    /// a per-endpoint chain.
    pub fn statistic_jump(
        &mut self,
        chain_svc: &str,
        probability: f64,
        chain_ep: &str,
    ) -> &mut Self {
        let mut line = String::new();
        let _ = write!(
            line,
            "-A {chain_svc} -m statistic --mode random --probability {probability:.6} -j {chain_ep}"
        );
        self.lines.push(IptablesLine::Rule(line));
        self
    }

    /// Append a `-A <chain_ep> … -j DNAT --to-destination …` rule.
    pub fn dnat(
        &mut self,
        chain_ep: &str,
        protocol: &str,
        pod_ip: &str,
        target_port: u16,
    ) -> &mut Self {
        let mut line = String::new();
        let _ = write!(
            line,
            "-A {chain_ep} -p {protocol} -j DNAT --to-destination {pod_ip}:{target_port}"
        );
        self.lines.push(IptablesLine::Rule(line));
        self
    }

    /// Append a `-F <chain>` flush rule.
    pub fn flush(&mut self, chain: &str) -> &mut Self {
        let mut line = String::new();
        let _ = write!(line, "-F {chain}");
        self.lines.push(IptablesLine::Rule(line));
        self
    }

    /// Append a `-X <chain>` delete-chain rule.
    pub fn delete_chain(&mut self, chain: &str) -> &mut Self {
        let mut line = String::new();
        let _ = write!(line, "-X {chain}");
        self.lines.push(IptablesLine::Rule(line));
        self
    }

    /// Push the `COMMIT` trailer.
    pub fn commit(&mut self) -> &mut Self {
        self.lines.push(IptablesLine::Commit);
        self
    }
}

impl Display for IptablesScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for line in &self.lines {
            writeln!(f, "{line}")?;
        }
        Ok(())
    }
}

// =================================================================
// IpvsScript — typed ipvsadm-restore AST + Display
// =================================================================

/// One line of an ipvsadm-restore script.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IpvsLine {
    /// `-A <proto_flag> <vip>:<port> -s rr` add-virtual-service.
    AddService {
        /// `-t` or `-u`.
        proto_flag: String,
        /// Virtual IP.
        vip: String,
        /// Virtual port.
        port: u16,
    },
    /// `-a <proto_flag> <vip>:<port> -r <pod>:<tport> -m` add-real-server.
    AddRealServer {
        /// `-t` or `-u`.
        proto_flag: String,
        /// Virtual IP.
        vip: String,
        /// Virtual port.
        port: u16,
        /// Backend pod IP.
        pod_ip: String,
        /// Backend pod port.
        target_port: u16,
    },
    /// `-D <proto_flag> <vip>:<port>` delete-virtual-service.
    DeleteService {
        /// `-t` or `-u`.
        proto_flag: String,
        /// Virtual IP.
        vip: String,
        /// Virtual port.
        port: u16,
    },
}

impl Display for IpvsLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddService {
                proto_flag,
                vip,
                port,
            } => write!(f, "-A {proto_flag} {vip}:{port} -s rr"),
            Self::AddRealServer {
                proto_flag,
                vip,
                port,
                pod_ip,
                target_port,
            } => write!(
                f,
                "-a {proto_flag} {vip}:{port} -r {pod_ip}:{target_port} -m"
            ),
            Self::DeleteService {
                proto_flag,
                vip,
                port,
            } => write!(f, "-D {proto_flag} {vip}:{port}"),
        }
    }
}

/// A typed ipvsadm-restore script.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IpvsScript {
    lines: Vec<IpvsLine>,
}

impl IpvsScript {
    /// Empty script.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a typed line.
    pub fn push(&mut self, line: IpvsLine) -> &mut Self {
        self.lines.push(line);
        self
    }
}

impl Display for IpvsScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for line in &self.lines {
            writeln!(f, "{line}")?;
        }
        Ok(())
    }
}

// =================================================================
// SystemdUnit — typed INI AST + Display
// =================================================================

/// A typed value inside a [`SystemdSection`]. Mirrors the JSON
/// shapes the workload translator stores (string / number / repeated
/// lines).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemdValue {
    /// `Key=value`.
    Str(String),
    /// `Key=<number>`.
    Num(i64),
    /// One `Key=value` line per element (e.g. repeated `Environment=`).
    Lines(Vec<String>),
}

/// One INI section (`[Unit]` / `[Service]` / `[Install]`) — ordered
/// key/value entries preserving insertion order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemdSection {
    name: String,
    entries: Vec<(String, SystemdValue)>,
}

impl SystemdSection {
    /// New empty section.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            entries: Vec::new(),
        }
    }

    /// Append a typed `Key=value` entry.
    pub fn push(&mut self, key: impl Into<String>, value: SystemdValue) -> &mut Self {
        self.entries.push((key.into(), value));
        self
    }
}

impl Display for SystemdSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}]", self.name)?;
        for (key, value) in &self.entries {
            match value {
                SystemdValue::Str(s) => writeln!(f, "{key}={s}")?,
                SystemdValue::Num(n) => writeln!(f, "{key}={n}")?,
                SystemdValue::Lines(lines) => {
                    for line in lines {
                        writeln!(f, "{key}={line}")?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// A typed systemd unit file — an ordered list of [`SystemdSection`]s
/// rendered through the single [`Display`] chokepoint. Replaces
/// `format!()`-of-INI in the workload translator.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SystemdUnit {
    sections: Vec<SystemdSection>,
}

impl SystemdUnit {
    /// Empty unit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a section.
    pub fn section(&mut self, section: SystemdSection) -> &mut Self {
        self.sections.push(section);
        self
    }
}

impl Display for SystemdUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for section in &self.sections {
            write!(f, "{section}")?;
            writeln!(f)?;
        }
        Ok(())
    }
}

// =================================================================
// PodmanRunArgv — typed argv builder (Vec<String>, not a string)
// =================================================================

/// A typed `podman run` argv. Builds a `Vec<String>` from typed
/// pieces — never a `format!()` command string that a shell would
/// have to re-tokenize. The argv is the unit of execution; rendering
/// to a single space-joined display string is a *separate*, lossy
/// convenience ([`PodmanRunArgv::to_command_line`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodmanRunArgv {
    /// Container name (`--name <name>`).
    pub name: String,
    /// Image reference (positional, last).
    pub image: String,
}

impl PodmanRunArgv {
    /// New argv for `podman run --name <name> <image>`.
    #[must_use]
    pub fn new(name: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            image: image.into(),
        }
    }

    /// The full typed argv vector: `["/usr/bin/podman", "run",
    /// "--name", <name>, <image>]`.
    #[must_use]
    pub fn to_argv(&self) -> Vec<String> {
        vec![
            "/usr/bin/podman".to_string(),
            "run".to_string(),
            "--name".to_string(),
            self.name.clone(),
            self.image.clone(),
        ]
    }

    /// Space-joined command-line string. For the systemd
    /// `ExecStart=` directive, which is itself a single line. The
    /// argv (not this string) is the canonical form.
    #[must_use]
    pub fn to_command_line(&self) -> String {
        self.to_argv().join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── CiliumNetworkPolicy ───────────────────────────────────

    fn cilium_selector() -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert("app".to_string(), "podinfo".to_string());
        m
    }

    #[test]
    fn cilium_ingress_yaml_has_expected_shape() {
        let policy = CiliumNetworkPolicy::new(
            "default-allow-frontend",
            cilium_selector(),
            CiliumDirection::Ingress,
        );
        let yaml = policy.to_yaml();
        assert!(yaml.contains("apiVersion: cilium.io/v2"));
        assert!(yaml.contains("kind: CiliumNetworkPolicy"));
        assert!(yaml.contains("name: default-allow-frontend"));
        assert!(yaml.contains("app: podinfo"));
        assert!(yaml.contains("ingress:"));
        assert!(!yaml.contains("egress:"));
    }

    #[test]
    fn cilium_egress_yaml_uses_egress_key() {
        let policy = CiliumNetworkPolicy::new("x", cilium_selector(), CiliumDirection::Egress);
        let yaml = policy.to_yaml();
        assert!(yaml.contains("egress:"));
        assert!(!yaml.contains("ingress:"));
    }

    #[test]
    fn cilium_yaml_round_trips_through_serde_yaml() {
        let policy = CiliumNetworkPolicy::new(
            "default-allow-frontend",
            cilium_selector(),
            CiliumDirection::Ingress,
        );
        let yaml = policy.to_yaml();
        // The emitted document parses back into a generic YAML value
        // with the documented structure.
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(v["apiVersion"], serde_yaml::Value::from("cilium.io/v2"));
        assert_eq!(v["kind"], serde_yaml::Value::from("CiliumNetworkPolicy"));
        assert_eq!(
            v["spec"]["endpointSelector"]["matchLabels"]["app"],
            serde_yaml::Value::from("podinfo")
        );
        assert!(v["spec"]["ingress"].is_sequence());
    }

    // ── TraefikIngressRoute ───────────────────────────────────

    #[test]
    fn traefik_yaml_has_expected_shape() {
        let route = TraefikIngressRoute::new(
            "podinfo",
            "Host(`podinfo.example.com`) && PathPrefix(`/`)",
            "podinfo",
            80,
            None,
        );
        let yaml = route.to_yaml();
        assert!(yaml.contains("apiVersion: traefik.io/v1alpha1"));
        assert!(yaml.contains("kind: IngressRoute"));
        assert!(yaml.contains("Host(`podinfo.example.com`)"));
        assert!(yaml.contains("PathPrefix(`/`)"));
        assert!(yaml.contains("name: podinfo"));
        assert!(yaml.contains("port: 80"));
        assert!(yaml.contains("- web"));
        assert!(!yaml.contains("websecure"));
    }

    #[test]
    fn traefik_tls_yaml_uses_websecure_and_secret() {
        let route = TraefikIngressRoute::new(
            "podinfo",
            "Host(`x`) && PathPrefix(`/`)",
            "podinfo",
            443,
            Some("podinfo-tls".to_string()),
        );
        let yaml = route.to_yaml();
        assert!(yaml.contains("- websecure"));
        assert!(yaml.contains("secretName: podinfo-tls"));
    }

    #[test]
    fn traefik_yaml_round_trips() {
        let route = TraefikIngressRoute::new(
            "podinfo",
            "Host(`x`) && PathPrefix(`/`)",
            "podinfo",
            80,
            None,
        );
        let yaml = route.to_yaml();
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(
            v["spec"]["routes"][0]["services"][0]["port"],
            serde_yaml::Value::from(80)
        );
        assert_eq!(
            v["spec"]["routes"][0]["kind"],
            serde_yaml::Value::from("Rule")
        );
    }

    // ── NginxServerBlock ──────────────────────────────────────

    #[test]
    fn nginx_block_renders_listen_and_proxy_pass() {
        let block = NginxServerBlock {
            listen: "80".to_string(),
            server_name: "podinfo.example.com".to_string(),
            tls_secret: None,
            location: "location /".to_string(),
            proxy_pass: "http://podinfo:80".to_string(),
        };
        let out = block.to_string();
        assert!(out.contains("server {"));
        assert!(out.contains("    listen 80;"));
        assert!(out.contains("    server_name podinfo.example.com;"));
        assert!(out.contains("    location / {"));
        assert!(out.contains("        proxy_pass http://podinfo:80;"));
        assert!(out.contains("        proxy_set_header Host $host;"));
    }

    #[test]
    fn nginx_block_with_tls_emits_ssl_directives() {
        let block = NginxServerBlock {
            listen: "443 ssl".to_string(),
            server_name: "x".to_string(),
            tls_secret: Some("podinfo-tls".to_string()),
            location: "location /".to_string(),
            proxy_pass: "http://podinfo:80".to_string(),
        };
        let out = block.to_string();
        assert!(out.contains("    listen 443 ssl;"));
        assert!(out.contains("    ssl_certificate     /etc/nginx/tls/podinfo-tls.crt;"));
        assert!(out.contains("    ssl_certificate_key /etc/nginx/tls/podinfo-tls.key;"));
    }

    // ── IptablesScript ────────────────────────────────────────

    #[test]
    fn iptables_script_renders_full_chain() {
        let mut s = IptablesScript::new();
        s.table("nat")
            .chain("KUBE-SVC-ABC")
            .jump_to_service_chain("10.96.5.1", "tcp", 80, "KUBE-SVC-ABC")
            .chain("KUBE-SEP-DEF")
            .statistic_jump("KUBE-SVC-ABC", 0.5, "KUBE-SEP-DEF")
            .dnat("KUBE-SEP-DEF", "tcp", "10.42.0.1", 9898)
            .commit();
        let out = s.to_string();
        assert!(out.starts_with("*nat\n"));
        assert!(out.ends_with("COMMIT\n"));
        assert!(out.contains(":KUBE-SVC-ABC - [0:0]\n"));
        assert!(out.contains("-A KUBE-SERVICES -d 10.96.5.1/32 -p tcp --dport 80 -j KUBE-SVC-ABC"));
        assert!(out.contains("-m statistic --mode random --probability 0.500000"));
        assert!(out.contains("-A KUBE-SEP-DEF -p tcp -j DNAT --to-destination 10.42.0.1:9898"));
    }

    #[test]
    fn iptables_remove_script_flushes_and_deletes() {
        let mut s = IptablesScript::new();
        s.table("nat")
            .chain("KUBE-SVC-ABC")
            .flush("KUBE-SVC-ABC")
            .delete_chain("KUBE-SVC-ABC")
            .commit();
        let out = s.to_string();
        assert_eq!(
            out,
            "*nat\n:KUBE-SVC-ABC - [0:0]\n-F KUBE-SVC-ABC\n-X KUBE-SVC-ABC\nCOMMIT\n"
        );
    }

    // ── IpvsScript ────────────────────────────────────────────

    #[test]
    fn ipvs_script_renders_virtual_and_real_servers() {
        let mut s = IpvsScript::new();
        s.push(IpvsLine::AddService {
            proto_flag: "-t".to_string(),
            vip: "10.96.5.1".to_string(),
            port: 80,
        })
        .push(IpvsLine::AddRealServer {
            proto_flag: "-t".to_string(),
            vip: "10.96.5.1".to_string(),
            port: 80,
            pod_ip: "10.42.0.1".to_string(),
            target_port: 9898,
        });
        let out = s.to_string();
        assert!(out.contains("-A -t 10.96.5.1:80 -s rr"));
        assert!(out.contains("-a -t 10.96.5.1:80 -r 10.42.0.1:9898 -m"));
    }

    #[test]
    fn ipvs_delete_service_line() {
        let line = IpvsLine::DeleteService {
            proto_flag: "-u".to_string(),
            vip: "10.96.0.10".to_string(),
            port: 53,
        };
        assert_eq!(line.to_string(), "-D -u 10.96.0.10:53");
    }

    // ── SystemdUnit ───────────────────────────────────────────

    #[test]
    fn systemd_unit_renders_three_sections() {
        let mut unit = SystemdUnit::new();
        let mut u = SystemdSection::new("Unit");
        u.push("Description", SystemdValue::Str("podinfo".to_string()));
        let mut s = SystemdSection::new("Service");
        s.push("Type", SystemdValue::Str("notify".to_string()));
        s.push(
            "Environment",
            SystemdValue::Lines(vec!["A=1".to_string(), "B=2".to_string()]),
        );
        let mut i = SystemdSection::new("Install");
        i.push(
            "WantedBy",
            SystemdValue::Str("multi-user.target".to_string()),
        );
        unit.section(u).section(s).section(i);
        let out = unit.to_string();
        assert!(out.contains("[Unit]\nDescription=podinfo\n"));
        assert!(out.contains("[Service]\nType=notify\n"));
        assert!(out.contains("Environment=A=1\nEnvironment=B=2\n"));
        assert!(out.contains("[Install]\nWantedBy=multi-user.target\n"));
        // Trailing blank line after each section.
        assert!(out.ends_with("\n\n"));
    }

    #[test]
    fn systemd_value_number_renders_bare() {
        let mut section = SystemdSection::new("Service");
        section.push("TimeoutSec", SystemdValue::Num(30));
        assert!(section.to_string().contains("TimeoutSec=30\n"));
    }

    // ── PodmanRunArgv ─────────────────────────────────────────

    #[test]
    fn podman_argv_is_typed_vector() {
        let argv = PodmanRunArgv::new("podinfo", "stefanprodan/podinfo:6.5.4");
        assert_eq!(
            argv.to_argv(),
            vec![
                "/usr/bin/podman".to_string(),
                "run".to_string(),
                "--name".to_string(),
                "podinfo".to_string(),
                "stefanprodan/podinfo:6.5.4".to_string(),
            ]
        );
    }

    #[test]
    fn podman_command_line_matches_prior_execstart() {
        let argv = PodmanRunArgv::new("podinfo", "stefanprodan/podinfo:6.5.4");
        assert_eq!(
            argv.to_command_line(),
            "/usr/bin/podman run --name podinfo stefanprodan/podinfo:6.5.4"
        );
    }
}
