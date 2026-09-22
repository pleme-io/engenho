//! `AmbientCapabilities=` — Linux capabilities by name, as a typed set.
//!
//! The names are parsed on every platform (a macOS test run must still check
//! that `CAP_NET_RAW` is understood and `CAP_NONSENSE` refused); only
//! [`crate::privilege`] turns them into syscalls, and only on Linux. The
//! numeric value IS the kernel's capability number, so the Linux seam is a
//! shift, not a second table.

use std::fmt;

/// One Linux capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Capability {
    number: u8,
    name: &'static str,
}

impl Capability {
    /// Every capability up to `CAP_CHECKPOINT_RESTORE` (kernel 5.9), in
    /// kernel order — the number is the index.
    pub const ALL: [Self; 41] = {
        const fn cap(number: u8, name: &'static str) -> Capability {
            Capability { number, name }
        }
        [
            cap(0, "CAP_CHOWN"),
            cap(1, "CAP_DAC_OVERRIDE"),
            cap(2, "CAP_DAC_READ_SEARCH"),
            cap(3, "CAP_FOWNER"),
            cap(4, "CAP_FSETID"),
            cap(5, "CAP_KILL"),
            cap(6, "CAP_SETGID"),
            cap(7, "CAP_SETUID"),
            cap(8, "CAP_SETPCAP"),
            cap(9, "CAP_LINUX_IMMUTABLE"),
            cap(10, "CAP_NET_BIND_SERVICE"),
            cap(11, "CAP_NET_BROADCAST"),
            cap(12, "CAP_NET_ADMIN"),
            cap(13, "CAP_NET_RAW"),
            cap(14, "CAP_IPC_LOCK"),
            cap(15, "CAP_IPC_OWNER"),
            cap(16, "CAP_SYS_MODULE"),
            cap(17, "CAP_SYS_RAWIO"),
            cap(18, "CAP_SYS_CHROOT"),
            cap(19, "CAP_SYS_PTRACE"),
            cap(20, "CAP_SYS_PACCT"),
            cap(21, "CAP_SYS_ADMIN"),
            cap(22, "CAP_SYS_BOOT"),
            cap(23, "CAP_SYS_NICE"),
            cap(24, "CAP_SYS_RESOURCE"),
            cap(25, "CAP_SYS_TIME"),
            cap(26, "CAP_SYS_TTY_CONFIG"),
            cap(27, "CAP_MKNOD"),
            cap(28, "CAP_LEASE"),
            cap(29, "CAP_AUDIT_WRITE"),
            cap(30, "CAP_AUDIT_CONTROL"),
            cap(31, "CAP_SETFCAP"),
            cap(32, "CAP_MAC_OVERRIDE"),
            cap(33, "CAP_MAC_ADMIN"),
            cap(34, "CAP_SYSLOG"),
            cap(35, "CAP_WAKE_ALARM"),
            cap(36, "CAP_BLOCK_SUSPEND"),
            cap(37, "CAP_AUDIT_READ"),
            cap(38, "CAP_PERFMON"),
            cap(39, "CAP_BPF"),
            cap(40, "CAP_CHECKPOINT_RESTORE"),
        ]
    };

    /// The capability `name` denotes, case-insensitively and with the `CAP_`
    /// prefix optional (systemd accepts both spellings).
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        let upper = name.trim().to_ascii_uppercase();
        let with_prefix = if upper.starts_with("CAP_") {
            upper
        } else {
            let mut prefixed = String::from("CAP_");
            prefixed.push_str(&upper);
            prefixed
        };
        Self::ALL.into_iter().find(|c| c.name == with_prefix)
    }

    /// The kernel's capability number.
    #[must_use]
    pub const fn number(self) -> u8 {
        self.number
    }

    /// The canonical `CAP_*` name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// Its bit in a capability mask.
    #[must_use]
    pub const fn bit(self) -> u64 {
        1 << self.number
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

/// A set of capabilities, in kernel order, without duplicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapabilitySet(Vec<Capability>);

impl CapabilitySet {
    /// Add one; a repeat is a no-op.
    pub fn insert(&mut self, capability: Capability) {
        if !self.0.contains(&capability) {
            self.0.push(capability);
            self.0.sort_unstable();
        }
    }

    /// Drop every member — what an empty `AmbientCapabilities=` does.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The members.
    #[must_use]
    pub fn members(&self) -> &[Capability] {
        &self.0
    }

    /// The mask the kernel's `capset` takes.
    #[must_use]
    pub fn mask(&self) -> u64 {
        self.0.iter().map(|c| c.bit()).fold(0, |a, b| a | b)
    }
}

impl fmt::Display for CapabilitySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, capability) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            capability.fmt(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_number_is_the_index_and_names_are_unique() {
        for (index, capability) in Capability::ALL.into_iter().enumerate() {
            assert_eq!(usize::from(capability.number()), index, "{capability}");
            assert_eq!(Capability::parse(capability.name()), Some(capability));
        }
    }

    #[test]
    fn names_parse_with_or_without_the_prefix_and_nonsense_does_not() {
        assert_eq!(
            Capability::parse("CAP_NET_RAW").unwrap().number(),
            13,
            "dhcpcd's AmbientCapabilities"
        );
        assert_eq!(Capability::parse("net_bind_service").unwrap().number(), 10);
        assert_eq!(Capability::parse("CAP_NONSENSE"), None);
    }

    #[test]
    fn a_set_is_ordered_deduplicated_and_maskable() {
        let mut set = CapabilitySet::default();
        for name in ["CAP_NET_RAW", "CAP_NET_ADMIN", "CAP_NET_RAW"] {
            set.insert(Capability::parse(name).unwrap());
        }
        assert_eq!(set.members().len(), 2);
        assert_eq!(set.mask(), (1 << 12) | (1 << 13));
        assert_eq!(set.to_string(), "CAP_NET_ADMIN CAP_NET_RAW");
        set.clear();
        assert!(set.is_empty());
    }
}
