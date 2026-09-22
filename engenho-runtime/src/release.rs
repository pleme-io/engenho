//! Proof that no [`crate::Runtime`] holds this node's store, and what a boot
//! that failed did about the store it had opened.
//!
//! engenho boots a store by taking an exclusive `flock` on it
//! ([`engenho_store::data_dir_lock::DataDirLock`]) and holding it for as long
//! as the store is open. A second open — even from the same process — is
//! refused. So "the store is released" is not a matter of opinion; it is a
//! lock that can be taken, and code that is about to boot a store again (an
//! in-process restart, a retry after a failed boot) should hold proof of it
//! rather than hope.
//!
//! [`StoreReleased`] is that proof. It cannot be built by callers; it is
//! minted only by the three things that actually establish it:
//!
//!   * a [`crate::Runtime::shutdown`] that terminated the store;
//!   * the unwind of a boot that failed after opening the store
//!     ([`BootUnwind::Released`]);
//!   * [`StoreReleased::probe`], which takes the store's lock and drops it.

use std::path::Path;

use engenho_store::data_dir_lock::{DataDirLock, LockError};

use crate::error::RuntimeError;
use crate::runtime::STORE_DIR;

/// Proof that no runtime in this process (or any other) holds the store:
/// the next boot can open it.
///
/// Zero-sized; it carries no data, only the fact.
#[derive(Debug)]
#[non_exhaustive]
pub struct StoreReleased;

impl StoreReleased {
    /// Minted where the release actually happened. Crate-private on purpose.
    pub(crate) const fn minted() -> Self {
        Self
    }

    /// Prove the store under `data_dir` is free by taking its lock and
    /// dropping it. An ephemeral store lives in memory and dies with its
    /// runtime, so it is always free.
    ///
    /// # Errors
    ///
    /// [`LockError::Held`] when something still holds the store,
    /// [`LockError::Unusable`] when its lock file cannot be opened.
    pub fn probe(data_dir: &Path, durable: bool) -> Result<Self, LockError> {
        if durable {
            drop(DataDirLock::acquire(data_dir.join(STORE_DIR))?);
        }
        Ok(Self)
    }
}

/// What a failed boot did about the store it may have opened.
#[derive(Debug)]
pub enum BootUnwind {
    /// The boot failed before the store was opened: it acquired nothing.
    /// (Whether the store is free is a separate question —
    /// [`StoreReleased::probe`] answers it.)
    NeverOpened,
    /// The store had been opened; the unwind stopped everything the boot had
    /// started and terminated the store.
    Released(StoreReleased),
    /// Something still held the store after everything the boot had started
    /// was stopped, so it could not be terminated. The store — and its lock —
    /// stay open until the process exits; no further boot in this process
    /// can open it.
    StillShared {
        /// How many strong references remained.
        strong_count: usize,
    },
    /// The store was the unwind's alone, but terminating it failed.
    TerminateFailed(Box<RuntimeError>),
}

impl BootUnwind {
    /// Whether the store is known to be free for another boot.
    #[must_use]
    pub const fn store_released(&self) -> bool {
        matches!(self, Self::Released(_))
    }
}

/// A boot that did not produce a running [`crate::Runtime`].
#[derive(Debug)]
pub struct BootFailed {
    /// Why the boot failed.
    pub error: RuntimeError,
    /// What was done about the store it had opened.
    pub unwind: BootUnwind,
}

impl std::fmt::Display for BootFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for BootFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
