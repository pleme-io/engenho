//! A REAL CNI plugin binary, for testing engenho's runtime half.
//!
//! ★ WHY A BINARY AND NOT A MOCK. CNI's interface IS an executable
//! invocation: argv, environment, stdin, stdout, exit code. A test that
//! calls a Rust function pretending to be a plugin exercises none of that —
//! not the env marshalling, not the stdin write, not the exit-code and
//! error-document convention. This is exec'd for real by the integration
//! tests, so a break in any of those layers shows up.
//!
//! It behaves the way `containernetworking/plugins` do:
//!   * `ADD`   → a 1.0.0 result with a host-side and a sandbox-side
//!     interface, the address on the sandbox one
//!   * `DEL`   → empty output, exit 0
//!   * `CHECK` → empty output, exit 0
//!   * an unset or unknown `CNI_COMMAND` → a CNI error document on stdout
//!     and a non-zero exit, which is the convention a runtime must read
//!
//! `CNI_ARGS` may carry `FAIL=1` to make it report a real CNI error, so the
//! failure path is exercised by an actual failing process rather than by a
//! function returning `Err`.
//!
//! It echoes what it received under `received` so a test can assert that
//! the environment and the stdin document arrived intact.

use std::io::Read;

fn main() {
    let command = std::env::var("CNI_COMMAND").unwrap_or_default();
    let args = std::env::var("CNI_ARGS").unwrap_or_default();

    let mut stdin = String::new();
    let _ = std::io::stdin().read_to_string(&mut stdin);
    let config: serde_json::Value = serde_json::from_str(&stdin).unwrap_or(serde_json::Value::Null);

    if args.split(';').any(|kv| kv == "FAIL=1") {
        // The convention: an error DOCUMENT on stdout plus a non-zero exit.
        // A runtime that reads only the exit code loses msg and details.
        println!(
            "{}",
            serde_json::json!({
                "cniVersion": "1.0.0",
                "code": 11,
                "msg": "no IP addresses available in range",
                "details": "range is full: 10.244.1.0/24"
            })
        );
        std::process::exit(1);
    }

    match command.as_str() {
        "ADD" => {
            let netns = std::env::var("CNI_NETNS").unwrap_or_default();
            let ifname = std::env::var("CNI_IFNAME").unwrap_or_else(|_| "eth0".into());
            println!(
                "{}",
                serde_json::json!({
                    "cniVersion": "1.0.0",
                    "interfaces": [
                        // The HOST end first, with no sandbox — the shape
                        // that makes a naive `ips[0]` pick the wrong address.
                        { "name": "vethhost0", "mac": "0a:58:0a:f4:01:01" },
                        { "name": ifname, "mac": "0a:58:0a:f4:01:07", "sandbox": netns }
                    ],
                    "ips": [
                        { "address": "10.244.1.7/24", "gateway": "10.244.1.1", "interface": 1 }
                    ],
                    "routes": [ { "dst": "0.0.0.0/0", "gw": "10.244.1.1" } ],
                    "received": {
                        "containerId": std::env::var("CNI_CONTAINERID").unwrap_or_default(),
                        "args": args,
                        "path": std::env::var("CNI_PATH").unwrap_or_default(),
                        "name": config.get("name").cloned().unwrap_or(serde_json::Value::Null),
                        "type": config.get("type").cloned().unwrap_or(serde_json::Value::Null),
                        "hadPrevResult": config.get("prevResult").is_some(),
                    }
                })
            );
        }
        "DEL" | "CHECK" => {}
        other => {
            println!(
                "{}",
                serde_json::json!({
                    "cniVersion": "1.0.0",
                    "code": 4,
                    "msg": "unknown CNI_COMMAND",
                    "details": other
                })
            );
            std::process::exit(1);
        }
    }
}
