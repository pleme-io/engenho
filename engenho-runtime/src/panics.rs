//! A process-wide count of panics (T2.7).
//!
//! A contained tick panic is counted in its driver's heartbeat, and a child
//! whose task panicked is counted by the supervisor. Neither sees a panic in
//! a task outside the catalog — an apiserver request handler, a request's
//! watch pump — which tokio catches and drops with nothing but a line on
//! stderr. A panic hook sees every panic in the process, caught or not,
//! before any unwinding: this one counts it and then hands it to whatever
//! hook was installed before, so the message is still printed exactly as it
//! was.
//!
//! The count is read through a [`PanicCounter`], and the only way to get one
//! is to install the hook. A count read from a process where nothing counts
//! would be zero, and a zero that means "never measured" reads as "no
//! panics" — so that reading is unrepresentable, not merely discouraged.

use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};

/// Panics seen by the hook since it was installed.
static PANICS: AtomicU64 = AtomicU64::new(0);

/// The hook is installed once per process, however many runtimes start.
static INSTALL: Once = Once::new();

/// Proof that the counting panic hook is installed, and the way to read what
/// it has counted.
#[derive(Debug, Clone, Copy)]
pub struct PanicCounter {
    _installed: (),
}

impl PanicCounter {
    /// Install the counting hook, chained in front of the hook installed
    /// before it, unless this process already has it.
    ///
    /// Idempotent: a second call returns a counter over the same count and
    /// installs nothing, so no panic is ever counted twice. The hook is
    /// installed from whichever thread calls first; call it at startup, not
    /// from a thread that is itself panicking (`set_hook` refuses that).
    #[must_use]
    pub fn install() -> Self {
        INSTALL.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                // Count first: if the previous hook aborts, the count stands.
                PANICS.fetch_add(1, Ordering::Relaxed);
                previous(info);
            }));
        });
        Self { _installed: () }
    }

    /// Panics in this process since the hook was installed — caught or not,
    /// on any thread.
    #[must_use]
    pub fn total(self) -> u64 {
        PANICS.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Other tests in this binary may panic on purpose at the same time, so
    /// the count is asserted to have grown by at least what this test did.
    #[test]
    fn a_panic_is_counted_whether_or_not_it_is_caught() {
        let counter = PanicCounter::install();
        let before = counter.total();

        let caught = std::panic::catch_unwind(|| panic!("counted, then caught"));
        let joined = std::thread::spawn(|| panic!("counted on another thread")).join();

        assert!(
            caught.is_err() && joined.is_err(),
            "precondition: both panicked"
        );
        assert!(
            counter.total() >= before + 2,
            "{} panics counted after two, from {before}",
            counter.total()
        );
    }
}
