//! The daemon's lifecycle: a supervisor that stays up above a restartable
//! runtime, and serves its state whether the runtime runs or not.
//!
//! * [`machine`] — the pure state machine (states, events, effects).
//! * [`supervisor`] — the loop that feeds it and performs its effects.
//! * [`journal`] — the boot journal, the run marker, the first-boot identity.
//! * [`control_dir`] — where those live on disk (`data_dir/control/`).
//! * [`bootstrap`] — deciding the data directory before any boot.

pub mod bootstrap;
pub mod control_dir;
pub mod journal;
pub mod machine;
pub mod supervisor;

pub use bootstrap::{ControlBootstrap, DataDirSource};
pub use control_dir::{ControlDir, ControlWriteError};
pub use journal::{
    AttemptResult, BootAttempt, BootJournal, BootKindObservation, IdentityRecord, LastSeen,
    PhaseRecord, PhaseResult, PreviousRun, RunMarker,
};
pub use machine::{
    AfterDrain, DaemonLifecycle, ExitIntent, FailureReport, Lifecycle, LifecycleEffect,
    LifecycleEvent, LifecycleState, PendingApply, Refused, RefusedBecause, RetryClass, StopReason,
    StoreOutcome,
};
pub use supervisor::{
    Accepted, ChildFact, CommandError, ConfigSource, DaemonEvent, DaemonInfo, EVENTS, Hold,
    Inspection, ResolvedConfig, RuntimeFacts, Snapshot, StopDone, StoreLock, Supervisor,
    SupervisorConfig, SupervisorError, SupervisorHandle, file_digest,
};
