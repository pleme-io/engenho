//! CRI — the Container Runtime Interface client.
//!
//! ★ WHY THIS MATTERS MORE THAN "SUPPORTING ANOTHER RUNTIME". The kubelet
//! drove `podman` by SHELLING OUT: building an argv, spawning a process,
//! and parsing its stdout. That works, and it also means the runtime seam
//! is a text format nobody versions. A flag rename in podman, a changed
//! error string, a locale that formats a number differently — each is a
//! silent behaviour change with no compiler and no schema between it and
//! the kubelet. CRI is the typed contract every production runtime already
//! implements, and moving onto it converts that whole class of breakage
//! into a protobuf mismatch.
//!
//! It is also what makes the runtime SUBSTITUTABLE. containerd, CRI-O and
//! youki all speak CRI; none of them speak podman's argv. A distribution
//! locked to one runtime's command line is not a distribution.
//!
//! ★ CLIENT ONLY. engenho's kubelet CALLS a runtime; it does not implement
//! one. Generating the server traits would ship dead code and invite
//! someone to implement the wrong side of the seam.
//!
//! ★ THE PROTO IS UPSTREAM'S, NOT RECONSTRUCTED — `kubernetes/cri-api`
//! at `release-1.34`, matching the Kubernetes version engenho serves. The
//! only edit is removing `gogoproto` options, which are Go codegen hints
//! with no effect on the wire encoding. Writing a wire format from memory
//! is how a client compiles cleanly and talks to nothing.
//!
//! ★ THE UNIX SOCKET PATH IS NOT A CONSTANT. containerd, CRI-O and every
//! packaging of them put the endpoint somewhere different, and a hardcoded
//! path produces a kubelet that works on exactly one distribution. It is
//! configuration, with upstream's own default order as the fallback.

/// The generated CRI v1 types and client.
///
/// `runtime.v1` is the package name in the proto; keeping the module named
/// after it means a reader can find any symbol in upstream's api.proto by
/// the same path they would use in Go.
pub mod v1 {
    #![allow(clippy::doc_markdown, clippy::large_enum_variant)]
    include!(concat!(env!("OUT_DIR"), "/runtime.v1.rs"));
}

/// Endpoints upstream's kubelet probes, in order.
///
/// Order is upstream's and is load-bearing: a host with BOTH containerd and
/// CRI-O installed must land on the same one the rest of the ecosystem
/// assumes, or two tools on the same node manage different container sets
/// and each reports the other's containers as missing.
pub const DEFAULT_ENDPOINTS: &[&str] = &[
    "unix:///run/containerd/containerd.sock",
    "unix:///run/crio/crio.sock",
    "unix:///var/run/cri-dockerd.sock",
];

/// Where to reach the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint(pub String);

impl Endpoint {
    /// Parse an endpoint, accepting the `unix://` form the kubelet flag and
    /// every runtime's documentation use.
    ///
    /// A bare path is accepted too, because operators write one constantly
    /// and rejecting it would be pedantry that produces a startup failure
    /// with a confusing message rather than a working node.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Some(path) = trimmed.strip_prefix("unix://") {
            // `unix:///run/x.sock` has three slashes; the path keeps one.
            if path.is_empty() {
                return None;
            }
            return Some(Self(path.to_string()));
        }
        if trimmed.starts_with('/') {
            return Some(Self(trimmed.to_string()));
        }
        // A tcp:// or http:// endpoint is NOT accepted: CRI over TCP is
        // unauthenticated by construction, and silently allowing it would
        // put container control on the network.
        None
    }

    /// The filesystem path to connect to.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.0
    }
}

/// Translate a CRI container state to the kubelet's running/exited view.
///
/// ★ `UNKNOWN` IS NOT `EXITED`. A runtime reporting UNKNOWN has lost track
/// of the container — it may still be running. Treating it as exited would
/// make the kubelet start a SECOND copy of a container that is already up,
/// which for anything holding a lock or a port is worse than the outage it
/// was trying to fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Created,
    Running,
    Exited,
    /// The runtime does not know. Explicitly distinct from `Exited`.
    Unknown,
}

impl RunState {
    /// Decode the CRI `ContainerState` enum value.
    #[must_use]
    pub fn from_cri(state: i32) -> Self {
        match state {
            0 => Self::Created,
            1 => Self::Running,
            2 => Self::Exited,
            _ => Self::Unknown,
        }
    }

    /// Should the kubelet consider this container up?
    ///
    /// `Created` is NOT running: the container exists and has not started,
    /// and reporting it as up would make a pod Ready before its process
    /// exists.
    #[must_use]
    pub fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }

    /// May the kubelet safely conclude this container has stopped?
    ///
    /// `Unknown` yields `false` — see the type doc.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Exited)
    }
}

/// The CRI exec result, reduced to what the kubelet's `ExecOutcome` needs.
///
/// CRI returns stdout and stderr as raw BYTES; the kubelet's outcome holds
/// Strings. Lossy UTF-8 conversion is deliberate: a command emitting
/// non-UTF-8 must not fail the exec, because the exec's PURPOSE is often to
/// discover that the command is misbehaving.
#[must_use]
pub fn exec_outcome(exit_code: i32, stdout: &[u8], stderr: &[u8]) -> crate::backend::ExecOutcome {
    crate::backend::ExecOutcome {
        exit_code,
        stdout: String::from_utf8_lossy(stdout).into_owned(),
        stderr: String::from_utf8_lossy(stderr).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generated_client_and_types_are_present() {
        // Anti-vacuity for the whole module: if codegen silently produced
        // nothing, every other test here would still pass.
        let req = v1::VersionRequest {
            version: "v1".to_string(),
        };
        assert_eq!(req.version, "v1");
        // The two services upstream defines.
        let _: Option<v1::runtime_service_client::RuntimeServiceClient<tonic::transport::Channel>> =
            None;
        let _: Option<v1::image_service_client::ImageServiceClient<tonic::transport::Channel>> =
            None;
    }

    #[test]
    fn the_endpoint_accepts_the_forms_operators_actually_write() {
        assert_eq!(
            Endpoint::parse("unix:///run/containerd/containerd.sock"),
            Some(Endpoint("/run/containerd/containerd.sock".into()))
        );
        // A bare path: written constantly, and rejecting it would produce a
        // confusing startup failure rather than a working node.
        assert_eq!(
            Endpoint::parse("/run/crio/crio.sock"),
            Some(Endpoint("/run/crio/crio.sock".into()))
        );
        assert_eq!(
            Endpoint::parse("  /run/x.sock  "),
            Some(Endpoint("/run/x.sock".into()))
        );
    }

    #[test]
    fn a_tcp_endpoint_is_refused_because_cri_over_tcp_is_unauthenticated() {
        // Silently allowing it would put container control on the network.
        assert_eq!(Endpoint::parse("tcp://10.0.0.1:1234"), None);
        assert_eq!(Endpoint::parse("http://runtime:80"), None);
        assert_eq!(Endpoint::parse(""), None);
        assert_eq!(Endpoint::parse("unix://"), None);
    }

    #[test]
    fn unknown_is_not_exited() {
        // Treating UNKNOWN as exited makes the kubelet start a SECOND copy
        // of a container that may still be running — worse than the outage
        // it was trying to fix, for anything holding a lock or a port.
        assert_eq!(RunState::from_cri(3), RunState::Unknown);
        assert!(!RunState::Unknown.is_terminal());
        assert!(!RunState::Unknown.is_running());
        assert!(RunState::Exited.is_terminal());
    }

    #[test]
    fn created_is_not_running() {
        // Reporting it as up would make a pod Ready before its process
        // exists.
        assert_eq!(RunState::from_cri(0), RunState::Created);
        assert!(!RunState::Created.is_running());
        assert!(RunState::from_cri(1).is_running());
    }

    #[test]
    fn non_utf8_output_does_not_fail_the_exec() {
        // The exec's purpose is often to DISCOVER that a command is
        // misbehaving; failing on its output would hide exactly that.
        let out = exec_outcome(1, &[0xff, 0xfe, b'h', b'i'], b"err");
        assert_eq!(out.exit_code, 1);
        assert!(out.stdout.contains("hi"));
        assert_eq!(out.stderr, "err");
    }

    #[test]
    fn the_default_endpoint_order_is_upstreams() {
        // A host with both containerd and CRI-O must land on the same one
        // the rest of the ecosystem assumes, or two tools manage different
        // container sets and each reports the other's as missing.
        assert_eq!(
            DEFAULT_ENDPOINTS[0],
            "unix:///run/containerd/containerd.sock"
        );
        assert_eq!(DEFAULT_ENDPOINTS[1], "unix:///run/crio/crio.sock");
        assert!(
            DEFAULT_ENDPOINTS
                .iter()
                .all(|e| Endpoint::parse(e).is_some())
        );
    }
}
