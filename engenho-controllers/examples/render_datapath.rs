//! Render engenho's iptables datapath for a representative ClusterIP
//! Service → endpoints, emitting a COMPLETE `iptables-restore` script
//! (base `KUBE-SERVICES` chain + engenho's rendered per-service /
//! per-endpoint chains). Used to prove the computed datapath is valid,
//! installable Linux iptables (`iptables-restore --test`) on a real
//! Linux host — the install half of the Service-routing brick that is
//! datapath-deferred on the macOS-VM topology.
//!
//! `cargo run -p engenho-controllers --example render_datapath`

use std::collections::BTreeSet;

use engenho_controllers::service_router::{IptablesRouter, PortMap, ServiceRoute};

fn main() {
    // Mirror the live local cluster: mesh-test @ 10.96.0.1, port 80→8080,
    // two ready pod endpoints (round-robin DNAT target).
    let route = ServiceRoute {
        service_id: "default-mesh-test".to_string(),
        cluster_ip: "10.96.0.1".to_string(),
        ports: vec![PortMap {
            name: "http".to_string(),
            service_port: 80,
            target_port: 8080,
            protocol: "TCP".to_string(),
        }],
        endpoints: BTreeSet::from(["10.88.0.2".to_string(), "10.88.0.3".to_string()]),
    };

    let fragment = IptablesRouter::render_script(&route);

    // engenho renders the per-service/per-endpoint chains + appends a jump
    // from KUBE-SERVICES (the base chain kube-proxy owns). For a standalone
    // `iptables-restore --test`, declare that base chain after the `*nat`
    // header so the appended `-A KUBE-SERVICES` resolves.
    let complete = fragment.replacen("*nat\n", "*nat\n:KUBE-SERVICES - [0:0]\n", 1);
    print!("{complete}");
}
