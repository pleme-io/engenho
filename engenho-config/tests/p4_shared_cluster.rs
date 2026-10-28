use engenho_config::{EngenhoConfig, TieredConfig};

const SHARED: &str = "\
cluster:
  shared:
    name: aldeia
    ca_cert_path: /var/lib/engenho/cluster/ca.crt
    ca_key_path: /var/lib/engenho/cluster/ca.key
    identity_key_path: /var/lib/engenho/mesh/node.key
";

fn shared(yaml: &str) -> serde_json::Value {
    let cfg = EngenhoConfig::from_yaml_with_defaults(yaml).expect("the shared block parses");
    serde_json::to_value(&cfg).unwrap()["cluster"]["shared"].clone()
}

#[test]
fn a_shared_cluster_block_parses_and_round_trips() {
    let block = shared(SHARED);
    assert_eq!(block["name"], "aldeia");
    assert_eq!(block["ca_cert_path"], "/var/lib/engenho/cluster/ca.crt");
    assert_eq!(block["ca_key_path"], "/var/lib/engenho/cluster/ca.key");
    assert_eq!(block["identity_key_path"], "/var/lib/engenho/mesh/node.key");
    assert!(block.get("seeds").is_none(), "no seeds: this node forms");
    let joining = shared(&format!(
        "{SHARED}    seeds: [\"10.0.0.1:7946\", \"10.0.0.2:7946\"]\n"
    ));
    assert_eq!(
        joining["seeds"],
        serde_json::json!(["10.0.0.1:7946", "10.0.0.2:7946"])
    );
    assert!(
        EngenhoConfig::from_yaml_with_defaults(&format!("{SHARED}    seeds: [\"rio\"]\n")).is_err(),
        "a seed is a socket address"
    );
    let cfg = EngenhoConfig::from_yaml_with_defaults(SHARED).unwrap();
    let again = EngenhoConfig::from_yaml_with_defaults(&cfg.to_yaml().unwrap()).unwrap();
    assert_eq!(cfg, again);
}

#[test]
fn without_the_block_the_node_is_its_own_cluster() {
    let cfg = EngenhoConfig::prescribed_default();
    let value = serde_json::to_value(&cfg).unwrap();
    assert!(value["cluster"].get("shared").is_none(), "{value}");
    assert!(
        cfg.cluster.name.starts_with("engenho-"),
        "{}",
        cfg.cluster.name
    );
    assert_eq!(
        EngenhoConfig::from_yaml_with_defaults("").unwrap(),
        cfg,
        "an operator file without the block is today's config exactly"
    );
}

#[test]
fn a_shared_block_with_a_relative_path_or_a_dotted_name_is_refused() {
    for (bad, field) in [
        (
            SHARED.replace("/var/lib/engenho/cluster/ca.key", "ca.key"),
            "cluster.shared.ca_key_path",
        ),
        (
            SHARED.replace("/var/lib/engenho/mesh/node.key", "mesh/node.key"),
            "cluster.shared.identity_key_path",
        ),
        (
            SHARED.replace("name: aldeia", "name: aldeia.lan"),
            "cluster.shared.name",
        ),
        (
            SHARED.replace("name: aldeia", "name: \"\""),
            "cluster.shared.name",
        ),
    ] {
        let err = EngenhoConfig::from_yaml_with_defaults(&bad).unwrap_err();
        assert!(err.to_string().contains(field), "{bad}: {err}");
    }
}

#[test]
fn a_shared_block_missing_a_field_or_carrying_an_unknown_one_is_refused() {
    let missing = SHARED.replace(
        "    identity_key_path: /var/lib/engenho/mesh/node.key\n",
        "",
    );
    assert!(EngenhoConfig::from_yaml_with_defaults(&missing).is_err());
    let unknown = format!("{SHARED}    voters: 3\n");
    assert!(EngenhoConfig::from_yaml_with_defaults(&unknown).is_err());
}

const QUIC: &str = "fabric:\n  quic:\n    bind: 127.0.0.1:7946\n";

#[cfg(feature = "mesh-quic")]
#[test]
fn a_quic_fabric_parses_for_a_shared_member_and_is_refused_without_one() {
    use engenho_config::Fabric;
    let cfg = EngenhoConfig::from_yaml_with_defaults(&format!("{SHARED}{QUIC}")).unwrap();
    assert_eq!(
        cfg.fabric,
        Fabric::Quic {
            bind: "127.0.0.1:7946".parse().unwrap()
        }
    );
    let again = EngenhoConfig::from_yaml_with_defaults(&cfg.to_yaml().unwrap()).unwrap();
    assert_eq!(again.fabric, cfg.fabric);
    let err = EngenhoConfig::from_yaml_with_defaults(QUIC).unwrap_err();
    assert!(err.to_string().contains("cluster.shared"), "{err}");
    assert_eq!(
        EngenhoConfig::prescribed_default().fabric,
        Fabric::InBinary,
        "the default fabric stays in-binary"
    );
}

#[cfg(not(feature = "mesh-quic"))]
#[test]
fn a_quic_fabric_is_refused_at_parse_by_a_build_without_it() {
    assert!(EngenhoConfig::from_yaml_with_defaults(&format!("{SHARED}{QUIC}")).is_err());
}
