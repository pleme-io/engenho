//! `engenho remote` — this machine's side of remote control: the keys it
//! holds and the daemons `remotes.yaml` names.
//!
//! * `engenho remote list` — each remote: its address, its pins, this
//!   machine's key for it.
//! * `engenho remote keygen <name>` — make this machine's key for `<name>`
//!   (never replacing one) and print its pin, for the daemon's
//!   `control.remote.authorized_clients`.
//! * `engenho remote fingerprint <name>` — print that pin again.
//!
//! The daemon side prints its own pin with `engenho ctl control show`
//! (`identity.spki`); it goes in `remotes.yaml`'s `server_spki`.

use engenho_control_client::remote::{self, RemoteError, RemotesConfig};

/// Why `engenho remote` failed, with the exit code it maps to.
enum Failure {
    Usage,
    Remote(RemoteError),
}

impl From<RemoteError> for Failure {
    fn from(err: RemoteError) -> Self {
        Self::Remote(err)
    }
}

const USAGE: &str = "usage: engenho remote list | keygen <name> | fingerprint <name>";

/// Run `engenho remote`; the exit code: 0 done, 1 failed, 2 usage.
pub fn run(args: &[String]) -> u8 {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let done = match args.as_slice() {
        [] | ["list"] => list(),
        ["keygen", name] => keygen(name),
        ["fingerprint", name] => fingerprint(name),
        _ => Err(Failure::Usage),
    };
    match done {
        Ok(()) => 0,
        Err(Failure::Usage) => {
            eprintln!("{USAGE}");
            2
        }
        Err(Failure::Remote(err)) => {
            eprintln!("engenho remote: {err}");
            1
        }
    }
}

fn list() -> Result<(), Failure> {
    let dir = remote::config_dir()?;
    let remotes = RemotesConfig::load(&dir)?;
    if remotes.remotes.is_empty() {
        println!("no remotes in {}", dir.join(remote::REMOTES_FILE).display());
        return Ok(());
    }
    for (name, endpoint) in &remotes.remotes {
        let key = remote::key_path(&dir, name, Some(endpoint));
        let pins: Vec<String> = endpoint
            .server_spki
            .iter()
            .map(ToString::to_string)
            .collect();
        println!("{name}");
        println!("  address     {}", endpoint.address);
        println!("  server_spki {}", pins.join(", "));
        match remote::read_key(&key) {
            Ok(material) => println!("  key         {} ({})", key.display(), material.spki()),
            Err(err) => println!("  key         {err}"),
        }
    }
    Ok(())
}

/// Where the key for `name` is: as `remotes.yaml` says, else the default.
fn key_for(name: &str) -> Result<std::path::PathBuf, Failure> {
    let dir = remote::config_dir()?;
    let remotes = RemotesConfig::load(&dir)?;
    Ok(remote::key_path(&dir, name, remotes.remotes.get(name)))
}

fn keygen(name: &str) -> Result<(), Failure> {
    let path = key_for(name)?;
    let pin = remote::keygen(&path)?;
    println!("{pin}");
    eprintln!(
        "made {}; on the daemon, add to control.remote.authorized_clients:\n  \
         - {{ name: <this machine>, spki_sha256: {pin}, tier: observe|mutate|destructive }}",
        path.display()
    );
    Ok(())
}

fn fingerprint(name: &str) -> Result<(), Failure> {
    let path = key_for(name)?;
    println!("{}", remote::read_key(&path)?.spki());
    Ok(())
}
