//! The whole runner against a temporary root: plan → prepare → pre steps →
//! the command that would be `exec`ed.
//!
//! The one step not taken is the `execve` itself, because it would replace
//! the test process. What it would run is asserted instead — program, argv,
//! environment and working directory all come from the same
//! [`engenho_unit::Plan::command`] the exec path uses, so the only untested
//! line is the syscall.
//!
//! The unit is the shape zwave-js has: a credential, a runtime directory, a
//! state directory, `%d` in an `ExecStartPre=` that writes into the runtime
//! directory, and an `EnvironmentFile=` that the pre step generates.

use std::path::Path;

use engenho_unit::layout::{BaseDir, HostRoot, Layout};
use engenho_unit::{Overrides, UnitRunError, plan};

const UNIT: &str = r#"
[Unit]
Description=A service shaped like zwave-js
After=network.target

[Service]
Type=notify
User=svc
Group=svc
SupplementaryGroups=dialout
DynamicUser=true
Environment="PATH=/usr/bin:/bin"
Environment="MARKER=from-unit"
EnvironmentFile=-/run/svc/generated.env
StateDirectory=svc/state
StateDirectoryMode=0750
RuntimeDirectory=svc
CacheDirectory=svc
LoadCredential=secret.json:/run/secrets/svc.json
WorkingDirectory=/var/lib/svc/state
UMask=0077
AmbientCapabilities=CAP_NET_RAW
ProtectSystem=strict
PrivateTmp=true
MemoryMax=1G
Restart=on-failure
ExecStartPre=/bin/sh -c "cat %d/secret.json > $RUNTIME_DIRECTORY/seen && echo GENERATED=yes > $RUNTIME_DIRECTORY/generated.env"
ExecStart=/bin/echo ${MARKER} $STATE_DIRECTORY

[Install]
WantedBy=multi-user.target
"#;

/// A temporary root with the two account databases and the credential source.
fn root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let etc = root.path().join("etc");
    std::fs::create_dir_all(&etc).unwrap();
    let uid = engenho_unit::privilege::current_ids();
    std::fs::write(
        etc.join("passwd"),
        // The account the override names is THIS test's own user, so the
        // chown of the state directory succeeds without root.
        [
            "svc:x:",
            &uid.0.to_string(),
            ":",
            &uid.1.to_string(),
            "::/var/empty:/bin/sh\n",
        ]
        .concat(),
    )
    .unwrap();
    std::fs::write(
        etc.join("group"),
        ["svc:x:", &uid.1.to_string(), ":\ndialout:x:27:svc\n"].concat(),
    )
    .unwrap();
    let secrets = root.path().join("run/secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    std::fs::write(secrets.join("svc.json"), "{\"token\":\"s3cret\"}").unwrap();
    root
}

fn write_unit(root: &Path) -> std::path::PathBuf {
    let path = root.join("svc.service");
    std::fs::write(&path, UNIT).unwrap();
    path
}

#[test]
fn a_dynamic_user_unit_is_refused_without_an_override() {
    let root = root();
    let unit = write_unit(root.path());
    let err = plan(
        &unit,
        &Overrides::default(),
        Layout::new(HostRoot::at(root.path())),
    )
    .unwrap_err();
    assert!(
        matches!(err, UnitRunError::Identity(_)),
        "DynamicUser=true must be refused, not guessed: {err}"
    );
    assert_eq!(err.exit_code(), 78, "a unit that can never work here");
    assert!(err.to_string().contains("--user"), "{err}");
}

#[test]
fn plan_prepare_and_the_pre_step_produce_what_exec_would_run() {
    let root = root();
    let unit = write_unit(root.path());
    let layout = Layout::new(HostRoot::at(root.path()));
    let overrides = Overrides {
        user: Some("svc".into()),
        group: None,
    };
    let plan = plan(&unit, &overrides, layout).expect("the unit plans with an override");

    // ── the plan ──────────────────────────────────────────────────────
    let account = plan.identity.account().expect("privileges are dropped");
    assert_eq!(account.name, "svc");
    assert!(
        account.groups.contains(&27),
        "SupplementaryGroups=dialout is resolved: {:?}",
        account.groups
    );
    assert_eq!(plan.unit.umask, 0o077);
    assert_eq!(plan.unit.ambient.to_string(), "CAP_NET_RAW");
    assert_eq!(
        plan.credentials_dir.as_deref(),
        Some(root.path().join("run/credentials/svc.service").as_path()),
        "under a test root every path the service is told is inside it"
    );
    let not_enforced: Vec<&str> = plan
        .unit
        .acknowledged()
        .iter()
        .map(|n| n.key.as_str())
        .collect();
    assert_eq!(not_enforced, ["ProtectSystem", "PrivateTmp", "MemoryMax"]);
    // The report names everything, including what is not in force.
    let report = plan.report().to_string();
    assert!(
        report.contains("not enforced: ProtectSystem PrivateTmp MemoryMax"),
        "{report}"
    );
    assert!(report.contains("runs as: svc"), "{report}");

    // ── prepare ───────────────────────────────────────────────────────
    plan.prepare().expect("directories and credentials");
    let on_disk = |visible: &str| root.path().join(visible.trim_start_matches('/'));
    assert!(on_disk("var/lib/svc/state").is_dir());
    assert!(on_disk("var/cache/svc").is_dir());
    assert!(on_disk("run/svc").is_dir());
    let credential = on_disk("run/credentials/svc.service/secret.json");
    assert_eq!(
        std::fs::read_to_string(&credential).unwrap(),
        "{\"token\":\"s3cret\"}"
    );

    // ── the environment ───────────────────────────────────────────────
    let env = plan.environment().unwrap();
    let rooted = |p: &str| on_disk(p).to_string_lossy().into_owned();
    assert_eq!(env["STATE_DIRECTORY"], rooted("var/lib/svc/state"));
    assert_eq!(env["RUNTIME_DIRECTORY"], rooted("run/svc"));
    assert_eq!(env["CACHE_DIRECTORY"], rooted("var/cache/svc"));
    assert_eq!(
        env["CREDENTIALS_DIRECTORY"],
        rooted("run/credentials/svc.service")
    );
    assert_eq!(env["MARKER"], "from-unit");
    assert_eq!(
        env["PATH"], "/usr/bin:/bin",
        "the unit's PATH wins over the default"
    );
    assert_eq!(env["USER"], "svc");
    assert_eq!(env["HOME"], "/var/empty");

    // ── the pre step, run for real, through the runner ────────────────
    // It reads %d/secret.json and writes into $RUNTIME_DIRECTORY. Both came
    // out of the same layout, so both are inside the test root: the script
    // the runner built is the script that runs.
    let script = &plan.unit.exec_start_pre[0].argv[2];
    assert!(
        script.contains(
            root.path()
                .join("run/credentials/svc.service/secret.json")
                .to_str()
                .unwrap()
        ),
        "%d must be the credentials directory that was installed: {script}"
    );
    plan.run_pre_steps()
        .expect("the pre step runs and succeeds");
    assert_eq!(
        std::fs::read_to_string(on_disk("run/svc/seen")).unwrap(),
        "{\"token\":\"s3cret\"}",
        "the pre step read the credential the runner installed"
    );

    // ── the environment file the pre step generated is picked up ──────
    let env = plan.environment().unwrap();
    assert_eq!(
        env["GENERATED"], "yes",
        "EnvironmentFile= is re-read per command, so a pre step can write it"
    );

    // ── what exec would run ───────────────────────────────────────────
    let main = plan.unit.exec_start.last().unwrap();
    assert_eq!(
        main.resolved_argv(&env),
        ["/bin/echo", "from-unit", &rooted("var/lib/svc/state")],
        "${{MARKER}} is one argument, $STATE_DIRECTORY is the exported path"
    );
    let built = plan.command(main, &env).expect("the command builds");
    assert_eq!(built.get_program(), "/bin/echo");
    assert_eq!(
        built.get_current_dir(),
        Some(on_disk("var/lib/svc/state").as_path()),
        "WorkingDirectory= is where the service starts"
    );
}

#[test]
fn a_missing_working_directory_is_refused_unless_it_carries_a_dash() {
    let root = root();
    let unit_path = root.path().join("wd.service");
    let base = "[Service]\nUser=svc\nExecStart=/bin/true\nWorkingDirectory=";
    std::fs::write(&unit_path, [base, "/var/lib/absent\n"].concat()).unwrap();
    let layout = Layout::new(HostRoot::at(root.path()));
    let required = plan(&unit_path, &Overrides::default(), layout.clone()).unwrap();
    let env = required.environment().unwrap();
    let err = required
        .command(&required.unit.exec_start[0], &env)
        .expect_err("a missing working directory is an error");
    assert!(
        matches!(err, UnitRunError::WorkingDirectory { .. }),
        "{err}"
    );

    std::fs::write(&unit_path, [base, "-/var/lib/absent\n"].concat()).unwrap();
    let optional = plan(&unit_path, &Overrides::default(), layout).unwrap();
    let env = optional.environment().unwrap();
    optional
        .command(&optional.unit.exec_start[0], &env)
        .expect("a `-` makes it optional, as systemd documents");
}

#[test]
fn a_failing_pre_step_stops_the_start_unless_it_carries_a_dash() {
    let root = root();
    let path = root.path().join("pre.service");
    // `/bin/sh -c "exit 3"` rather than `/bin/false`: NixOS has no
    // /bin/false, macOS has no /usr/bin/true in the same place, and /bin/sh
    // is the one program both are guaranteed to have.
    std::fs::write(
        &path,
        "[Service]\nExecStartPre=/bin/sh -c \"exit 3\"\nExecStart=/bin/sh\n",
    )
    .unwrap();
    let layout = Layout::new(HostRoot::at(root.path()));
    let strict = plan(&path, &Overrides::default(), layout.clone()).unwrap();
    let err = strict.run_pre_steps().unwrap_err();
    assert!(
        matches!(err, UnitRunError::StepFailed { step: 1, ref program, code: Some(3) } if program == "/bin/sh"),
        "{err}"
    );
    assert_eq!(err.exit_code(), 3, "the step's own status is the exit code");

    std::fs::write(
        &path,
        "[Service]\nExecStartPre=-/bin/sh -c \"exit 3\"\nExecStart=/bin/sh\n",
    )
    .unwrap();
    let lenient = plan(&path, &Overrides::default(), layout).unwrap();
    lenient
        .run_pre_steps()
        .expect("a `-` prefix ignores the failure, as systemd does");
}

#[test]
fn the_planned_directories_are_what_the_unit_asked_for() {
    let root = root();
    let unit = write_unit(root.path());
    let planned = plan(
        &unit,
        &Overrides {
            user: Some("svc".into()),
            group: None,
        },
        Layout::new(HostRoot::at(root.path())),
    )
    .unwrap();
    let bases: Vec<BaseDir> = planned.directories.iter().map(|d| d.base).collect();
    assert_eq!(
        bases,
        [BaseDir::State, BaseDir::Runtime, BaseDir::Cache],
        "the directives' own order, so the *_DIRECTORY lists read like the unit"
    );
    assert_eq!(planned.directories[0].mode, 0o750);
    assert_eq!(planned.directories[2].mode, 0o755);
    assert!(
        planned.directories[1].recreate,
        "the runtime directory is the fresh one"
    );
}
