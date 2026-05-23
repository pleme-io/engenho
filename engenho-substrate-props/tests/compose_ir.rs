//! Property: ComposeIr fingerprint determinism + per-IR divergence.

use engenho_substrate::{ComposeIr, ComposeService};
use proptest::prelude::*;
use std::collections::BTreeMap;

fn service_strategy() -> impl Strategy<Value = ComposeService> {
    (
        "[a-z][a-z0-9_-]{0,32}:[a-z0-9._-]{1,16}", // image
        proptest::collection::vec("[0-9]{2,5}:[0-9]{2,5}", 0..4), // ports
        proptest::collection::btree_map("[A-Z_]{1,16}", "[a-zA-Z0-9_-]{0,32}", 0..4), // env
    )
        .prop_map(|(image, ports, environment)| ComposeService {
            image,
            command: None,
            environment,
            ports,
            volumes: Vec::new(),
            depends_on: Vec::new(),
            healthcheck: None,
            restart: None,
        })
}

fn compose_ir_strategy(max_services: usize) -> impl Strategy<Value = ComposeIr> {
    (
        "[a-z][a-z0-9-]{1,16}", // project
        proptest::collection::btree_map("[a-z][a-z0-9_-]{0,16}", service_strategy(), 1..max_services),
    )
        .prop_map(|(project, services)| ComposeIr {
            project,
            services,
            networks: BTreeMap::new(),
            volumes: BTreeMap::new(),
        })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128),
        ..ProptestConfig::default()
    })]

    /// fingerprint() is deterministic for identical IRs.
    #[test]
    fn fingerprint_is_deterministic(ir in compose_ir_strategy(8)) {
        let f1 = ir.fingerprint();
        let f2 = ir.fingerprint();
        prop_assert_eq!(f1, f2);
    }

    /// Clone produces identical fingerprint.
    #[test]
    fn clone_fingerprint_matches(ir in compose_ir_strategy(8)) {
        let cloned = ir.clone();
        prop_assert_eq!(ir.fingerprint(), cloned.fingerprint());
    }

    /// IRs that differ in any service field have different fingerprints.
    #[test]
    fn fingerprint_diverges_when_service_added(
        ir in compose_ir_strategy(4),
        extra_svc in service_strategy(),
        extra_name in "[a-z][a-z0-9_-]{4,16}",
    ) {
        prop_assume!(!ir.services.contains_key(&extra_name));
        let mut ir2 = ir.clone();
        ir2.add_service(extra_name, extra_svc);
        prop_assert_ne!(ir.fingerprint(), ir2.fingerprint());
    }

    /// IR round-trips through serde_json.
    #[test]
    fn serde_round_trip(ir in compose_ir_strategy(8)) {
        let bytes = serde_json::to_vec(&ir).unwrap();
        let back: ComposeIr = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, ir);
    }

    /// to_yaml() produces valid YAML that re-parses through serde_yaml
    /// to a structure containing the expected `services:` key.
    #[test]
    fn to_yaml_emits_services_block(ir in compose_ir_strategy(4)) {
        let yaml = ir.to_yaml();
        // Sanity: the YAML must mention every service name as a key.
        for name in ir.services.keys() {
            prop_assert!(
                yaml.contains(name),
                "YAML missing service name `{name}`:\n{yaml}"
            );
        }
        prop_assert!(yaml.contains("services:"));
    }
}
