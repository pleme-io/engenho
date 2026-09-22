//! engenho-control-server — serves engenho's control API.
//!
//! The API is `spec/engenho-control.openapi.yaml`; this crate is the
//! transport side of it, and holds no knowledge of any operation:
//!
//! * [`socket`] binds the local control socket — one daemon per socket, only
//!   a socket ever unlinked, a directory nobody else can write.
//! * [`grant`] turns the kernel's peer credentials into a
//!   [`engenho_control_types::Principal`] with a tier.
//! * [`router`] routes each request through the spec's catalog, checks the
//!   tier, and dispatches it through the generated operation visitor to one
//!   [`engenho_control_types::EngenhoControl`].
//! * [`audit`] records every mutation, sensitive read and refusal in a
//!   BLAKE3-linked chain.
//! * [`serve`] runs it all behind engenho-serve's owned-connection loop.

#![forbid(unsafe_code)]

pub mod audit;
pub mod grant;
pub mod router;
pub mod serve;
pub mod socket;

pub use audit::{Audit, AuditEntry, AuditLog, ChainBreak, NoAudit, verify_chain};
pub use grant::{GrantPolicy, PeerCred};
pub use router::{Answer, Incoming, Router};
pub use serve::{GRACE, serve_uds};
pub use socket::{BoundSocket, SocketError, SocketGuard, bind};

/// The daemon's effective uid.
#[must_use]
pub fn daemon_euid() -> u32 {
    nix::unistd::geteuid().as_raw()
}
