//! engenho-unit — run a rendered systemd `.service` file as an engenho workload.
//!
//! ## What this is for
//!
//! nixpkgs already knows how to run Home Assistant, mosquitto, zigbee2mqtt,
//! zwave-js, … : each NixOS module renders a systemd unit carrying the user,
//! the directories, the environment, the credentials and the pre-start steps
//! the service needs. `engenho unit-run --unit <file>` reads that rendered
//! unit and does what systemd would do to start its main process — then
//! `exec`s it, so the process the kubelet tracks IS the service.
//!
//! ## The pipeline
//!
//! 1. [`syntax`] lexes the file into sections and `Key=Value` entries.
//! 2. [`unit`] classifies EVERY entry against a closed directive catalog and
//!    builds a typed [`ServiceUnit`]. An entry is applied, acknowledged (a
//!    sandboxing or resource directive this runner does not enforce, logged
//!    once at start), owned by the supervisor (restart policy, stop and
//!    reload commands — engenho's kubelet owns the lifecycle), inert (unit
//!    ordering, `[Install]`, `X-` extensions) or unknown (a typed warning).
//!    Nothing is dropped silently.
//! 3. [`accounts`] + [`identity`] resolve `User=`/`Group=`/
//!    `SupplementaryGroups=` from `/etc/passwd` and `/etc/group`, parsed in
//!    Rust (no NSS, no `getpwnam`).
//! 4. [`specifier`] expands `%d`, `%S`, `%t`, `%n`, … exactly once, before a
//!    value is split, as systemd does; an unknown specifier is an error.
//! 5. [`run`] creates the `*Directory=` trees, installs the credentials,
//!    builds each command's environment, runs every `ExecStartPre=` with its
//!    prefix semantics, drops privileges ([`privilege`]) and `exec`s
//!    `ExecStart=`.
//!
//! Every filesystem path goes through a [`layout::Layout`] rooted at an
//! injectable [`layout::HostRoot`], so the whole pipeline up to the `exec`
//! runs against a temporary directory in tests.
//!
//! ## Tier-honest
//!
//! * Parsing, classification, identity resolution, directory and credential
//!   installation are pure Rust and tested on every platform.
//! * The privilege drop is Linux-only (the thread-scoped credential syscalls
//!   are a Linux fact). Elsewhere a unit that needs one is REFUSED with a
//!   typed error rather than run as the invoking user.
//! * No sandboxing directive is enforced. They are listed, by name, in the
//!   start-up log: the service runs with the host's filesystem and network
//!   view and the privileges of its `User=`.

#![deny(unsafe_code)]

pub mod accounts;
pub mod capability;
pub mod credentials;
pub mod directories;
pub mod env;
pub mod exec;
pub mod identity;
pub mod layout;
pub mod privilege;
pub mod run;
pub mod specifier;
pub mod syntax;
pub mod unit;
pub mod words;

pub use identity::{Identity, Overrides};
pub use layout::{HostRoot, Layout};
pub use run::{Plan, UnitRunError, plan};
pub use unit::{ServiceUnit, UnitFile};
