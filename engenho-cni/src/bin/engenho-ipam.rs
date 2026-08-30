//! `engenho-ipam` — a REAL CNI IPAM plugin, in Rust.
//!
//! ★ THIS IS THE NATURALIZED ARTIFACT, NOT A TEST FIXTURE. It is a
//! conformant CNI IPAM plugin that any runtime can invoke — kubelet,
//! containerd, `cnitool`, or engenho. Drop it in `/opt/cni/bin` and name it
//! in a `.conflist`'s `ipam.type` and it replaces `host-local`.
//!
//! ★ WHY IPAM IS THE HALF WORTH OWNING. A CNI chain splits into deciding
//! WHICH address a pod gets and wiring a veth into a netns. The second is
//! Linux kernel work that `containernetworking/plugins` already does well.
//! The first is pure bookkeeping, it is where the interesting failures live
//! — a double-allocation gives two pods one address and the symptom
//! surfaces somewhere else entirely — and it is portable. So engenho owns
//! it natively and consumes the kernel half.
//!
//! ★ IT SPEAKS THE CONTRACT, NOT A DIALECT. `CNI_COMMAND` in the
//! environment, config on stdin, a versioned `Result` on stdout, an error
//! DOCUMENT plus a non-zero exit on failure. The state directory is
//! `host-local`'s layout for the reason given in [`engenho_cni::ipam`]: an
//! operator debugging exhaustion must be able to `ls` it.
//!
//! Config, in the `ipam` block of a network configuration:
//!
//! ```json
//! { "type": "engenho-ipam", "subnet": "10.244.1.0/24",
//!   "gateway": "10.244.1.1", "dataDir": "/var/lib/cni" }
//! ```

use std::io::Read as _;

use engenho_cni::ipam::{Cidr, IpamError, LeaseStore};
use engenho_cni::result::{CniResult, IpConfig, Route};

/// Upstream's default, so a config that omits `dataDir` lands where every
/// other IPAM plugin puts it.
const DEFAULT_DATA_DIR: &str = "/var/lib/cni";

/// The spec's reserved error codes. 11 is the IPAM-specific one every
/// runtime already knows how to read.
const ERR_UNSUPPORTED_FIELD: u32 = 2;
const ERR_TRY_AGAIN_LATER: u32 = 11;
const ERR_INTERNAL: u32 = 999;

fn fail(code: u32, msg: &str, details: &str) -> ! {
    // The convention a runtime depends on: an error DOCUMENT on stdout and
    // a non-zero exit. Writing only to stderr loses `msg` and `details`,
    // which is the difference between "ADD failed" and "no IP addresses
    // available in range 10.244.1.0/24".
    println!(
        "{}",
        serde_json::json!({
            "cniVersion": "1.0.0",
            "code": code,
            "msg": msg,
            "details": details,
        })
    );
    std::process::exit(1);
}

fn main() {
    let command = std::env::var("CNI_COMMAND").unwrap_or_default();
    let container_id = std::env::var("CNI_CONTAINERID").unwrap_or_default();

    let mut stdin = String::new();
    let _ = std::io::stdin().read_to_string(&mut stdin);
    let config: serde_json::Value = match serde_json::from_str(&stdin) {
        Ok(v) => v,
        Err(e) => fail(
            ERR_UNSUPPORTED_FIELD,
            "config is not valid JSON",
            &e.to_string(),
        ),
    };

    // `VERSION` must answer without any config at all: a runtime probes it
    // before it has one.
    if command == "VERSION" {
        println!(
            "{}",
            serde_json::json!({
                "cniVersion": "1.0.0",
                "supportedVersions": ["0.3.0", "0.3.1", "0.4.0", "1.0.0"],
            })
        );
        return;
    }

    // The IPAM block, or the top level when invoked directly (which is how
    // `cnitool` and our own tests drive it).
    let ipam = config.get("ipam").unwrap_or(&config);
    let network = config
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("default");
    let data_dir = ipam
        .get("dataDir")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(DEFAULT_DATA_DIR);
    let store = LeaseStore::for_network(data_dir, network);

    if container_id.is_empty() {
        fail(
            ERR_UNSUPPORTED_FIELD,
            "CNI_CONTAINERID is required",
            "an allocation with no owner can never be released",
        );
    }

    match command.as_str() {
        "ADD" => add(ipam, &store, &container_id),
        "DEL" => {
            // Releasing an unknown container is SUCCESS: teardown may
            // already have partly run, and erroring wedges pod deletion
            // behind an address that is already free.
            if let Err(e) = store.release(&container_id) {
                fail(ERR_INTERNAL, "release failed", &e.to_string());
            }
            // DEL writes nothing on success, which is what the spec says
            // and what a chaining runtime expects.
        }
        "CHECK" => {
            // CHECK asserts the attachment still holds. A container with no
            // lease is a real divergence — the runtime believes it is
            // networked and IPAM does not agree.
            match store.leases() {
                Ok(leases) if leases.iter().any(|(_, h)| h == &container_id) => {}
                Ok(_) => fail(
                    ERR_INTERNAL,
                    "container holds no address",
                    &format!("no lease for {container_id} on network {network}"),
                ),
                Err(e) => fail(ERR_INTERNAL, "check failed", &e.to_string()),
            }
        }
        other => fail(
            ERR_UNSUPPORTED_FIELD,
            "unknown CNI_COMMAND",
            &format!("{other:?} is not ADD, DEL, CHECK or VERSION"),
        ),
    }
}

/// The ADD path: allocate and render a result.
fn add(ipam: &serde_json::Value, store: &LeaseStore, container_id: &str) {
    {
        {
            let Some(subnet) = ipam.get("subnet").and_then(serde_json::Value::as_str) else {
                fail(
                    ERR_UNSUPPORTED_FIELD,
                    "ipam.subnet is required",
                    "there is no range to allocate from",
                );
            };
            let subnet = match Cidr::parse(subnet) {
                Ok(c) => c,
                Err(e) => fail(
                    ERR_UNSUPPORTED_FIELD,
                    "ipam.subnet is not a CIDR",
                    &e.to_string(),
                ),
            };
            let addr = match store.allocate(subnet, container_id) {
                Ok(a) => a,
                // Exhaustion is TRY_AGAIN_LATER, not an internal error: a
                // runtime retries on 11 and gives up on 999, and an
                // exhausted range does free up.
                Err(e @ IpamError::Exhausted(_)) => fail(
                    ERR_TRY_AGAIN_LATER,
                    &e.to_string(),
                    "release a pod to free one",
                ),
                Err(e) => fail(ERR_INTERNAL, "allocation failed", &e.to_string()),
            };

            let gateway = ipam
                .get("gateway")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let mut result = CniResult {
                cni_version: "1.0.0".into(),
                ips: vec![IpConfig {
                    address: format!("{addr}/{}", subnet.prefix),
                    gateway: gateway.clone(),
                    // No interface index: an IPAM plugin creates no
                    // interface, and claiming index 0 would point at
                    // whatever the chain's first interface happens to be.
                    interface: None,
                }],
                ..Default::default()
            };
            if !gateway.is_empty() {
                result.routes.push(Route {
                    dst: "0.0.0.0/0".into(),
                    gw: gateway,
                });
            }
            println!("{}", serde_json::to_string(&result).unwrap_or_default());
        }
    }
}
