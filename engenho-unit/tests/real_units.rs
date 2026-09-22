//! The parser against REAL rendered NixOS units.
//!
//! Every fixture in `tests/fixtures/` is a unit nixpkgs actually renders, not
//! a hand-written approximation:
//!
//! * `zwave-js`, `mosquitto`, `home-assistant`, `zigbee2mqtt`,
//!   `matter-server`, `esphome`, `frigate`, `go2rtc`, `music-assistant`,
//!   `node-red` and the three `wyoming-*` servers were rendered from nixpkgs
//!   (`nixos/lib/eval-config.nix`, `config.systemd.units."x.service".text`)
//!   with each service enabled — the eleven services engenho is to run on
//!   plo;
//! * `dhcpcd` and `nscd` were copied from this machine's
//!   `/nix/store/*-unit-*.service`, and carry the shapes the home-automation
//!   units do not: `ExecStart=@…` (argv[0] override), `ExecStartPre=+…`
//!   (full privileges), `Type=forking`, `SupplementaryGroups=`.
//!
//! What they pin: the whole corpus parses, and NOTHING in it lands in the
//! "unknown directive" bucket — the tell that a directive class was missed.

use std::path::Path;

use engenho_unit::layout::{HostRoot, Layout};
use engenho_unit::specifier::{AccountFacts, Context, HostFacts};
use engenho_unit::unit::{Ack, Class, ServiceType, ServiceUnit, UnitError, UnitFile};

/// Every fixture: the unit name and its text.
const FIXTURES: [(&str, &str); 15] = [
    ("dhcpcd.service", include_str!("fixtures/dhcpcd.service")),
    ("esphome.service", include_str!("fixtures/esphome.service")),
    ("frigate.service", include_str!("fixtures/frigate.service")),
    ("go2rtc.service", include_str!("fixtures/go2rtc.service")),
    (
        "home-assistant.service",
        include_str!("fixtures/home-assistant.service"),
    ),
    (
        "matter-server.service",
        include_str!("fixtures/matter-server.service"),
    ),
    (
        "mosquitto.service",
        include_str!("fixtures/mosquitto.service"),
    ),
    (
        "music-assistant.service",
        include_str!("fixtures/music-assistant.service"),
    ),
    (
        "node-red.service",
        include_str!("fixtures/node-red.service"),
    ),
    ("nscd.service", include_str!("fixtures/nscd.service")),
    (
        "wyoming-faster-whisper-main.service",
        include_str!("fixtures/wyoming-faster-whisper-main.service"),
    ),
    (
        "wyoming-openwakeword.service",
        include_str!("fixtures/wyoming-openwakeword.service"),
    ),
    (
        "wyoming-piper-main.service",
        include_str!("fixtures/wyoming-piper-main.service"),
    ),
    (
        "zigbee2mqtt.service",
        include_str!("fixtures/zigbee2mqtt.service"),
    ),
    (
        "zwave-js.service",
        include_str!("fixtures/zwave-js.service"),
    ),
];

fn fixture(name: &str) -> &'static str {
    FIXTURES
        .iter()
        .find(|(fixture, _)| *fixture == name)
        .unwrap_or_else(|| panic!("no fixture {name}"))
        .1
}

/// Parse a fixture as root would see it (no `User=` resolution yet — the
/// account facts are only what the `%u` family expands to).
fn parse(name: &str) -> Result<ServiceUnit, UnitError> {
    let text = fixture(name);
    let layout = Layout::new(HostRoot::system());
    let host = HostFacts::default();
    let account = AccountFacts::root();
    let path = Path::new("/nix/store/fixture-unit").join(name);
    let file = UnitFile::parse(&path, text)?;
    let ctx = Context {
        unit: &file.name,
        fragment: Some(&file.path),
        account: Some(&account),
        layout: &layout,
        host: &host,
    };
    file.service(&ctx)
}

#[test]
fn every_real_unit_parses_and_none_carries_an_unknown_directive() {
    let mut acknowledged = 0;
    for (name, _) in FIXTURES {
        let unit = parse(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let unknown: Vec<String> = unit.unknown().iter().map(ToString::to_string).collect();
        assert!(
            unknown.is_empty(),
            "{name} carries directives the catalog does not classify: {unknown:?}"
        );
        assert!(
            !unit.exec_start.is_empty(),
            "{name} must yield a command to run"
        );
        // Non-vacuity, per unit: every one of them carries at least the
        // `[Install] WantedBy=` this runner has nothing to do with, so an
        // empty classification would mean nothing was classified at all.
        assert!(
            !unit.noted.is_empty(),
            "{name} classified no directive at all"
        );
        acknowledged += unit.acknowledged().len();
    }
    // And corpus-wide: the sandboxing these units declare is substantial, and
    // every line of it is reported as not enforced rather than dropped.
    assert!(
        acknowledged > 100,
        "only {acknowledged} acknowledged directives across the corpus — the classification is \
         letting sandboxing through as `applied`"
    );
}

#[test]
fn zwave_js_is_the_credentials_case_end_to_end() {
    let unit = parse("zwave-js.service").unwrap();
    // DynamicUser=true, which the runner refuses without an override — the
    // Nix side passes `--user zwave-js`. The parse still records it.
    assert!(unit.identity.dynamic);
    assert_eq!(unit.identity.user.as_deref(), Some("zwave-js"));
    assert_eq!(unit.identity.supplementary, ["dialout"]);

    assert_eq!(unit.credentials.len(), 1);
    assert_eq!(unit.credentials[0].id, "secrets.json");

    // The ExecStartPre is `/bin/sh -c "jq … %d/secrets.json > …"`, and %d must
    // already be the credentials directory when the shell runs.
    let pre = &unit.exec_start_pre[0];
    assert_eq!(pre.program, "/bin/sh");
    assert_eq!(pre.argv[1], "-c");
    assert!(
        pre.argv[2].contains("/run/credentials/zwave-js.service/secrets.json"),
        "%d was not expanded: {}",
        pre.argv[2]
    );
    assert!(pre.argv[2].contains("/run/zwave-js/config.json"));

    // …and the RuntimeDirectory that output path lives in is created before
    // any command runs.
    assert!(
        unit.directories
            .iter()
            .any(|d| d.base == engenho_unit::layout::BaseDir::Runtime && d.path == "zwave-js"),
        "RuntimeDirectory=zwave-js must be planned"
    );
    assert!(
        unit.directories
            .iter()
            .any(|d| d.base == engenho_unit::layout::BaseDir::Cache && d.path == "zwave-js")
    );
    assert_eq!(unit.umask, 0o077);
}

#[test]
fn mosquitto_carries_a_credential_a_pre_start_and_a_working_directory() {
    let unit = parse("mosquitto.service").unwrap();
    assert_eq!(unit.identity.user.as_deref(), Some("mosquitto"));
    assert_eq!(unit.identity.group.as_deref(), Some("mosquitto"));
    assert!(!unit.identity.dynamic);
    assert_eq!(
        unit.credentials[0].id, "listener-0-user-0-passwordFile",
        "a per-user password file is a credential, not a store path"
    );
    assert_eq!(unit.exec_start_pre.len(), 1);
    assert!(
        unit.exec_start_pre[0]
            .program
            .ends_with("/bin/mosquitto-pre-start")
    );
    let working = unit.working_directory.clone().unwrap();
    assert_eq!(working.path.unwrap().to_str(), Some("/var/lib/mosquitto"));
    assert_eq!(unit.service_type, ServiceType::Notify);
    assert!(
        unit.service_type
            .caveat()
            .unwrap()
            .contains("NOTIFY_SOCKET"),
        "Type=notify's caveat is stated, not hidden"
    );
    // The ExecStart is one program with two arguments.
    assert_eq!(unit.exec_start[0].argv.len(), 3);
}

#[test]
fn dhcpcd_exercises_the_prefixes_and_the_capability_list() {
    let unit = parse("dhcpcd.service").unwrap();
    // `ExecStart=@…/dhcpcd dhcpcd --quiet --config …`
    let main = &unit.exec_start[0];
    assert!(main.program.ends_with("/sbin/dhcpcd"));
    assert_eq!(main.argv[0], "dhcpcd", "@ sets argv[0] from the next word");
    assert!(main.argv.contains(&"--quiet".to_string()));
    // `ExecStartPre=+/nix/store/…-migrate-dhcpcd` runs WITHOUT the drop.
    assert_eq!(unit.exec_start_pre.len(), 1);
    assert_eq!(
        unit.exec_start_pre[0].privilege,
        engenho_unit::exec::Privilege::Full
    );
    assert_eq!(
        unit.ambient.to_string(),
        "CAP_NET_BIND_SERVICE CAP_NET_ADMIN CAP_NET_RAW",
        "the three ambient capabilities, in kernel order (10, 12, 13)"
    );
    assert_eq!(unit.identity.supplementary, ["resolvconf"]);
    assert_eq!(unit.service_type, ServiceType::Forking);
    assert!(unit.service_type.caveat().is_some());
    assert!(
        unit.noted
            .iter()
            .any(|n| n.key == "SystemCallFilter" && n.class == Class::Acknowledged(Ack::Sandbox))
    );
    assert!(
        unit.noted
            .iter()
            .any(|n| n.key == "ReadWritePaths" && n.class == Class::Acknowledged(Ack::Namespace))
    );
}

#[test]
fn home_assistant_resets_its_ambient_set_before_filling_it() {
    let unit = parse("home-assistant.service").unwrap();
    // The unit writes `AmbientCapabilities=` (a reset) and then two entries.
    assert_eq!(unit.ambient.to_string(), "CAP_NET_ADMIN CAP_NET_RAW");
    assert_eq!(unit.identity.user.as_deref(), Some("hass"));
    let main = &unit.exec_start[0];
    assert!(main.program.ends_with("/bin/hass"));
    assert_eq!(main.argv[1], "--config");
}

#[test]
fn matter_server_is_the_namespace_case_and_says_so() {
    let unit = parse("matter-server.service").unwrap();
    assert!(
        unit.acknowledged().iter().any(|n| n.key == "RootDirectory"),
        "RootDirectory is NOT set up; it must appear in the not-enforced list"
    );
    assert!(
        unit.acknowledged()
            .iter()
            .any(|n| n.key == "BindReadOnlyPaths")
    );
    // Its ExecStart uses %S and %t inside a single-quoted `sh -c` script.
    let script = &unit.exec_start[0].argv[2];
    assert!(script.contains("/var/lib/matter-server/"), "{script}");
    assert!(script.contains("/run/matter-server/root/data"), "{script}");
    assert!(unit.ambient.is_empty(), "AmbientCapabilities= is a reset");
}

#[test]
fn the_directory_family_is_read_across_the_corpus() {
    let frigate = parse("frigate.service").unwrap();
    assert_eq!(
        frigate.directory_mode(engenho_unit::layout::BaseDir::State),
        0o750
    );
    assert_eq!(
        frigate.directory_mode(engenho_unit::layout::BaseDir::Cache),
        0o750
    );
    assert_eq!(frigate.exec_start_pre.len(), 2);

    let piper = parse("wyoming-piper-main.service").unwrap();
    assert!(
        piper.directories.iter().any(|d| d.path == "wyoming/piper"),
        "a nested StateDirectory keeps its shape"
    );
    // Every argument of the wyoming units is quoted by NixOS's escaping.
    assert!(piper.exec_start[0].argv.iter().any(|a| a == "--voice"));

    let esphome = parse("esphome.service").unwrap();
    assert_eq!(
        esphome.directory_mode(engenho_unit::layout::BaseDir::Runtime),
        0o750
    );
    assert_eq!(esphome.identity.supplementary, ["dialout"]);
}

#[test]
fn the_environment_of_a_real_unit_is_read_in_order() {
    let unit = parse("zigbee2mqtt.service").unwrap();
    let names: Vec<&str> = unit.environment.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"PATH"));
    assert!(names.contains(&"ZIGBEE2MQTT_DATA"));
    let data = unit
        .environment
        .iter()
        .find(|(n, _)| n == "ZIGBEE2MQTT_DATA")
        .unwrap();
    assert_eq!(data.1, "/var/lib/zigbee2mqtt");
    let path = unit.environment.iter().find(|(n, _)| n == "PATH").unwrap();
    assert!(
        path.1.starts_with("/nix/store/"),
        "the unit's PATH is the one the service gets"
    );
}
