//! IPAM — allocating pod addresses from a CIDR, natively.
//!
//! ★ THIS IS THE NATURALIZED HALF OF CNI, AND IT IS THE HALF WORTH OWNING.
//! A CNI chain splits cleanly in two: deciding WHICH address a pod gets,
//! and wiring a veth pair into a netns. The second is Linux kernel work and
//! `containernetworking/plugins` does it well. The first is pure
//! bookkeeping — a range, a set of leases, a rule for reclaiming them — and
//! it is where the interesting failures live: a double-allocation gives two
//! pods the same address and the symptom appears in a third place entirely.
//!
//! So engenho implements IPAM natively rather than shelling to
//! `host-local`, and the result is a real CNI plugin binary that any
//! runtime can invoke — not just ours.
//!
//! ★ THE STATE FORMAT IS `host-local`'s, DELIBERATELY. One file per
//! allocated address, named for the address, containing the container id.
//! That is not imitation for its own sake: an operator debugging a leak
//! reads `/var/lib/cni/networks/<net>/` with `ls`, and every runbook and
//! every piece of tribal knowledge about CNI address exhaustion assumes
//! that layout. Inventing our own would make engenho's IPAM the one thing
//! nobody could diagnose.
//!
//! ★ ALLOCATION IS LOWEST-FREE, NOT RANDOM AND NOT SEQUENTIAL-FOREVER.
//! Random makes exhaustion non-reproducible; a monotonically advancing
//! cursor exhausts a /24 after 254 pod churns even when nothing is
//! allocated. Lowest-free is what `host-local` does and what makes a
//! reused address predictable.

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

/// An IPv4 CIDR, parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    /// Network address, masked.
    pub network: Ipv4Addr,
    /// Prefix length.
    pub prefix: u8,
}

/// Errors from address management.
#[derive(Debug, thiserror::Error)]
pub enum IpamError {
    /// The subnet string is not a CIDR engenho can parse.
    #[error("not an IPv4 CIDR: {0:?}")]
    BadCidr(String),
    /// The range holds no assignable address.
    #[error("subnet {0} has no assignable addresses")]
    EmptyRange(String),
    /// Every address in the range is leased.
    #[error("no IP addresses available in range {0}")]
    Exhausted(String),
    /// The lease directory could not be read or written.
    #[error("lease store at {path}: {source}")]
    Store {
        /// The path.
        path: String,
        /// The io error.
        #[source]
        source: std::io::Error,
    },
}

engenho_substrate::impl_error_kind! {
    IpamError {
        (BadCidr(_)) => "bad_cidr",
        (EmptyRange(_)) => "empty_range",
        (Exhausted(_)) => "exhausted",
        { Store { .. } } => "store",
    }
}

impl Cidr {
    /// Parse `10.244.1.0/24`.
    ///
    /// # Errors
    /// [`IpamError::BadCidr`] for anything else. A prefix above 32 is
    /// rejected rather than clamped: a clamped prefix silently changes the
    /// size of the range an operator declared.
    pub fn parse(s: &str) -> Result<Self, IpamError> {
        let bad = || IpamError::BadCidr(s.to_string());
        let (addr, prefix) = s.split_once('/').ok_or_else(bad)?;
        let addr: Ipv4Addr = addr.parse().map_err(|_| bad())?;
        let prefix: u8 = prefix.parse().map_err(|_| bad())?;
        if prefix > 32 {
            return Err(bad());
        }
        // Mask the address, so `10.244.1.7/24` and `10.244.1.0/24` name the
        // same range. An unmasked network address makes two configs that
        // mean the same thing hash differently.
        let mask: u32 = if prefix == 0 {
            0
        } else {
            u32::MAX << (32 - prefix)
        };
        Ok(Self {
            network: Ipv4Addr::from(u32::from(addr) & mask),
            prefix,
        })
    }

    /// The addresses this range may assign to a pod.
    ///
    /// ★ THE FIRST AND LAST ARE EXCLUDED, AND THAT IS NOT PEDANTRY. The
    /// network address is not a host, and the broadcast address is not
    /// either — handing either to a pod produces a container that appears
    /// configured and cannot be reached, with no error anywhere. `host-local`
    /// additionally reserves `.1` for the gateway by convention; engenho
    /// does NOT, because the gateway is declared in the config and
    /// reserving it here would silently shrink a range the operator sized.
    pub fn assignable(self) -> impl Iterator<Item = Ipv4Addr> {
        let base = u32::from(self.network);
        let size: u32 = if self.prefix >= 31 {
            0
        } else {
            1u32 << (32 - self.prefix)
        };
        // A /31 or /32 has no room for network + host + broadcast, so it
        // assigns nothing rather than assigning the network address.
        let first = base.saturating_add(1);
        let last = base.saturating_add(size.saturating_sub(1));
        (first..last).map(Ipv4Addr::from)
    }

    /// Whether an address falls inside this range.
    #[must_use]
    pub fn contains(self, addr: Ipv4Addr) -> bool {
        let mask: u32 = if self.prefix == 0 {
            0
        } else {
            u32::MAX << (32 - self.prefix)
        };
        u32::from(addr) & mask == u32::from(self.network)
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

/// A file-backed lease store, in `host-local`'s on-disk layout.
#[derive(Debug, Clone)]
pub struct LeaseStore {
    dir: PathBuf,
}

impl LeaseStore {
    /// A store under `<data-dir>/networks/<network-name>`.
    #[must_use]
    pub fn for_network(data_dir: impl AsRef<Path>, network: &str) -> Self {
        Self {
            dir: data_dir.as_ref().join("networks").join(network),
        }
    }

    /// The directory leases live in — the thing an operator `ls`es.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn lease_file(&self, addr: Ipv4Addr) -> PathBuf {
        self.dir.join(addr.to_string())
    }

    fn io(&self, source: std::io::Error) -> IpamError {
        IpamError::Store {
            path: self.dir.display().to_string(),
            source,
        }
    }

    /// Who holds `addr`, if anyone.
    ///
    /// # Errors
    /// [`IpamError::Store`] on a read failure that is not "absent".
    pub fn holder(&self, addr: Ipv4Addr) -> Result<Option<String>, IpamError> {
        match std::fs::read_to_string(self.lease_file(addr)) {
            Ok(s) => Ok(Some(s.trim().to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(self.io(e)),
        }
    }

    /// Allocate the lowest free address in `subnet` to `container_id`.
    ///
    /// ★ IDEMPOTENT ON THE CONTAINER ID. A runtime retrying `ADD` after a
    /// timeout must get the SAME address back, not a second one — otherwise
    /// every retry burns an address and a /24 dies after 254 flaky starts.
    ///
    /// ★ THE CLAIM IS `create_new`, WHICH IS THE WHOLE CONCURRENCY STORY.
    /// Two runtimes allocating at once both compute the same lowest-free
    /// address; the loser's create fails with `AlreadyExists` and it moves
    /// to the next. A read-then-write would hand both the same address, and
    /// the symptom — two pods with one IP — surfaces somewhere else
    /// entirely, hours later.
    ///
    /// # Errors
    /// [`IpamError::Exhausted`] when every address is leased;
    /// [`IpamError::Store`] on a genuine io failure.
    pub fn allocate(&self, subnet: Cidr, container_id: &str) -> Result<Ipv4Addr, IpamError> {
        std::fs::create_dir_all(&self.dir).map_err(|e| self.io(e))?;

        // An existing lease for this container wins, so a retry is free.
        if let Some(existing) = self.find_by_container(container_id)? {
            return Ok(existing);
        }

        for addr in subnet.assignable() {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.lease_file(addr))
            {
                Ok(mut f) => {
                    use std::io::Write as _;
                    f.write_all(container_id.as_bytes())
                        .map_err(|e| self.io(e))?;
                    return Ok(addr);
                }
                // Somebody else claimed it between our scan and our
                // create. Try the next address.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(self.io(e)),
            }
        }
        Err(IpamError::Exhausted(subnet.to_string()))
    }

    /// Release every address held by `container_id`.
    ///
    /// Returns what was released. An unknown container is SUCCESS with an
    /// empty list: teardown runs on a path that may already have partly
    /// completed, and erroring would wedge pod deletion behind an address
    /// that is already free.
    ///
    /// # Errors
    /// [`IpamError::Store`] on a genuine io failure.
    pub fn release(&self, container_id: &str) -> Result<Vec<Ipv4Addr>, IpamError> {
        let mut freed = Vec::new();
        for (addr, holder) in self.leases()? {
            if holder == container_id {
                match std::fs::remove_file(self.lease_file(addr)) {
                    Ok(()) => freed.push(addr),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(self.io(e)),
                }
            }
        }
        freed.sort();
        Ok(freed)
    }

    /// Every lease, as `(address, container id)`.
    ///
    /// A missing directory is an empty list — a network nothing has been
    /// allocated on yet is normal, not an error.
    ///
    /// # Errors
    /// [`IpamError::Store`] if the directory exists but cannot be read.
    pub fn leases(&self) -> Result<Vec<(Ipv4Addr, String)>, IpamError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(self.io(e)),
        };
        let mut out = Vec::new();
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            // Only files whose NAME parses as an address. A stray file — a
            // lock, a README — is skipped rather than treated as a lease,
            // because a lease we cannot name we also cannot release.
            let Some(addr) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.parse::<Ipv4Addr>().ok())
            else {
                continue;
            };
            if let Ok(holder) = std::fs::read_to_string(&path) {
                out.push((addr, holder.trim().to_string()));
            }
        }
        out.sort();
        Ok(out)
    }

    fn find_by_container(&self, container_id: &str) -> Result<Option<Ipv4Addr>, IpamError> {
        Ok(self
            .leases()?
            .into_iter()
            .find(|(_, holder)| holder == container_id)
            .map(|(addr, _)| addr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, LeaseStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LeaseStore::for_network(dir.path(), "cbr0");
        (dir, store)
    }

    #[test]
    fn a_cidr_is_masked_so_two_spellings_of_one_range_agree() {
        // An unmasked network address makes two configs that mean the same
        // thing compare unequal.
        assert_eq!(
            Cidr::parse("10.244.1.7/24").unwrap(),
            Cidr::parse("10.244.1.0/24").unwrap()
        );
        assert_eq!(
            Cidr::parse("10.244.1.0/24").unwrap().to_string(),
            "10.244.1.0/24"
        );
    }

    #[test]
    fn a_bad_cidr_is_refused_rather_than_clamped() {
        // A clamped prefix silently changes the size of the range an
        // operator declared.
        for bad in [
            "10.244.1.0",
            "10.244.1.0/33",
            "10.244.1.0/x",
            "hello/24",
            "",
        ] {
            assert!(Cidr::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_network_and_broadcast_addresses_are_never_assignable() {
        // Handing either to a pod produces a container that looks
        // configured and cannot be reached, with no error anywhere.
        let c = Cidr::parse("10.244.1.0/24").unwrap();
        let all: Vec<_> = c.assignable().collect();
        assert_eq!(all.len(), 254);
        assert_eq!(all[0], Ipv4Addr::new(10, 244, 1, 1));
        assert_eq!(all[253], Ipv4Addr::new(10, 244, 1, 254));
        assert!(!all.contains(&Ipv4Addr::new(10, 244, 1, 0)), "network");
        assert!(!all.contains(&Ipv4Addr::new(10, 244, 1, 255)), "broadcast");
    }

    #[test]
    fn a_slash_31_or_32_assigns_nothing_rather_than_the_network_address() {
        assert_eq!(Cidr::parse("10.0.0.0/31").unwrap().assignable().count(), 0);
        assert_eq!(Cidr::parse("10.0.0.1/32").unwrap().assignable().count(), 0);
    }

    #[test]
    fn allocation_is_lowest_free_so_a_reused_address_is_predictable() {
        // Random makes exhaustion non-reproducible; an advancing cursor
        // exhausts a /24 after 254 churns even when nothing is allocated.
        let (_d, s) = store();
        let c = Cidr::parse("10.244.1.0/24").unwrap();
        assert_eq!(s.allocate(c, "a").unwrap(), Ipv4Addr::new(10, 244, 1, 1));
        assert_eq!(s.allocate(c, "b").unwrap(), Ipv4Addr::new(10, 244, 1, 2));
        assert_eq!(s.allocate(c, "c").unwrap(), Ipv4Addr::new(10, 244, 1, 3));

        // Free the middle one; the next allocation reuses it, not .4.
        assert_eq!(s.release("b").unwrap(), vec![Ipv4Addr::new(10, 244, 1, 2)]);
        assert_eq!(s.allocate(c, "d").unwrap(), Ipv4Addr::new(10, 244, 1, 2));
    }

    #[test]
    fn allocation_is_idempotent_on_the_container_id() {
        // A runtime retrying ADD after a timeout must get the SAME address,
        // or a /24 dies after 254 flaky starts.
        let (_d, s) = store();
        let c = Cidr::parse("10.244.1.0/24").unwrap();
        let first = s.allocate(c, "abc").unwrap();
        for _ in 0..5 {
            assert_eq!(
                s.allocate(c, "abc").unwrap(),
                first,
                "a retry burned an address"
            );
        }
        assert_eq!(s.leases().unwrap().len(), 1);
    }

    #[test]
    fn exhaustion_is_a_named_error_not_a_wrong_address() {
        let (_d, s) = store();
        // A /29 assigns 6 addresses.
        let c = Cidr::parse("10.0.0.0/29").unwrap();
        for i in 0..6 {
            s.allocate(c, &format!("c{i}")).unwrap();
        }
        let e = s.allocate(c, "one-too-many").unwrap_err();
        assert!(matches!(e, IpamError::Exhausted(_)), "{e:?}");
        assert!(e.to_string().contains("10.0.0.0/29"), "{e}");
    }

    #[test]
    fn releasing_an_unknown_container_is_success_not_an_error() {
        // Teardown runs on a path that may already have partly completed;
        // erroring would wedge deletion behind an address already free.
        let (_d, s) = store();
        assert!(s.release("never-existed").unwrap().is_empty());
    }

    #[test]
    fn the_on_disk_layout_is_the_one_operators_already_know() {
        // An operator debugging exhaustion `ls`es this directory, and every
        // CNI runbook assumes the layout. Inventing our own would make
        // engenho's IPAM the one thing nobody can diagnose.
        let (_d, s) = store();
        let c = Cidr::parse("10.244.1.0/24").unwrap();
        let addr = s.allocate(c, "container-xyz").unwrap();
        let file = s.dir().join(addr.to_string());
        assert!(file.is_file(), "one file per address, named for it");
        assert_eq!(
            std::fs::read_to_string(&file).unwrap().trim(),
            "container-xyz",
            "containing the holder"
        );
        assert_eq!(s.holder(addr).unwrap().as_deref(), Some("container-xyz"));
    }

    #[test]
    fn a_stray_file_is_not_mistaken_for_a_lease() {
        // A lease we cannot name we also cannot release.
        let (_d, s) = store();
        std::fs::create_dir_all(s.dir()).unwrap();
        std::fs::write(s.dir().join("lock"), b"").unwrap();
        std::fs::write(s.dir().join("last_reserved_ip.0"), b"10.244.1.9").unwrap();
        assert!(s.leases().unwrap().is_empty());
    }

    #[test]
    fn contains_answers_for_the_range_not_the_literal_address() {
        let c = Cidr::parse("10.244.1.0/24").unwrap();
        assert!(c.contains(Ipv4Addr::new(10, 244, 1, 7)));
        assert!(!c.contains(Ipv4Addr::new(10, 244, 2, 7)));
    }
}
