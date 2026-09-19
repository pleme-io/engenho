//! The fault-injection matrix over the child catalog (W6). Test-only.
//!
//! ## What it holds
//!
//! [`Child::supervision`] declares what the supervisor does when each
//! [`Fault`] strikes each child. This strikes EVERY child in [`Child::all`]
//! with EVERY fault and reads back what happened from the supervisor's own
//! observations — the child's [`ChildState`] as [`Children::next_dead`] left
//! it, its heartbeat, and its liveness as `/livez` judges it — names the
//! outcome those observations are, and compares it with the declaration.
//! One table; one failure that lists every cell that disagrees.
//!
//! ## How each fault is injected
//!
//! Through the supervision wrapper production builds the child with, with
//! the fault in place of the child's real work:
//!
//! * a tick loop (each driver, the node lease): [`drive`], the one function
//!   the runtime drives every tick loop with, over a controller whose tick
//!   panics, never returns, or fails Transient. `drive` reads the loop's
//!   tick state off the loop's own catalog row; nothing here passes one.
//! * a listener: [`serve_rebinding`] over an attempt that panics, never
//!   returns, or returns; and, for a bind failure, the listener's REAL body
//!   ([`listener_task`]) against a port that is already taken.
//!
//! ## What fails the build
//!
//! The injector is an exhaustive match on the child's shape and on the
//! fault, with no wildcard arm (the catalog's wildcard scan reads this file
//! too): a new child shape or a new fault without its injection is E0004
//! here, as it is in the declaration. A new driver or listener needs no new
//! line: [`Child::all`] walks it, its catalog row declares it, and its shape
//! injects it. Every [`Supervision`] the catalog can declare must be
//! declared, and so exercised, by at least one cell.
//!
//! ## Tier
//!
//! A test, not a type. The rows are judgement (whether a driver is
//! Stateless is), and this proves the wrappers deliver what the rows say —
//! not that the real controllers never panic or hang.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use engenho_config::EngenhoConfig;
use engenho_controllers::{
    Beat, Controller, ControllerError, DeclaresReads, Heartbeat, Reads, ReconcileOutcome, TickClass,
};
use engenho_store::StoreMesh;
use engenho_substrate::{Clock as _, Liveness, WallClock};
use shikumi::TieredConfig as _;
use tokio::time::Instant;

use crate::child::{
    Child, ChildState, ChildTask, Children, DeathCause, Fault, Listener, Supervision, TickLoop,
};
use crate::health::Windows;
use crate::rebind::{REBIND, serve_rebinding};
use crate::runtime::{drive, listener_task};
use crate::testing::single_voter_store;

/// How often an idle tick loop ticks: a handful of ticks fit in the settle
/// time.
const FALLBACK: Duration = Duration::from_millis(200);

/// The stuck-tick window. Liveness rounds a window up to the next whole
/// second, so a hung tick reads stalled once it is a second old.
const STUCK: Duration = Duration::from_millis(300);

// ── the injections ─────────────────────────────────────────────────────

/// What an injected tick does.
#[derive(Debug, Clone, Copy)]
enum TickFault {
    Panic,
    Hang,
    Error,
}

impl TickFault {
    /// The tick `fault` becomes in a tick loop; `None` when it cannot strike
    /// one.
    const fn of(fault: Fault) -> Option<Self> {
        match fault {
            Fault::Panic => Some(Self::Panic),
            Fault::Hang => Some(Self::Hang),
            Fault::Error => Some(Self::Error),
            // A tick loop binds no port.
            Fault::BindFailure => None,
        }
    }
}

/// A controller whose every tick is the fault. It reads nothing, so only its
/// fallback ticks it.
struct Faulty(TickFault);

// The impl census reads a test module in its own file as shipped source;
// `#[cfg(test)]` on the impl is what tells it this controller is not one.
#[cfg(test)]
#[async_trait::async_trait]
impl Controller for Faulty {
    fn name(&self) -> &'static str {
        "fault-matrix"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        match self.0 {
            TickFault::Panic => panic!("injected: the tick panicked"),
            TickFault::Hang => std::future::pending().await,
            TickFault::Error => Err(ControllerError::Internal(
                "injected: a Transient failure".into(),
            )),
        }
    }
}

#[cfg(test)]
impl DeclaresReads for Faulty {
    fn reads(&self) -> Reads {
        Reads::nothing()
    }
}

/// A serve attempt that panics.
async fn panicking_attempt() {
    panic!("injected: the serve attempt panicked");
}

/// A serve attempt whose server returned.
async fn returning_attempt() {}

/// What every injected child is built over.
struct World {
    store: Arc<StoreMesh>,
    windows: Windows,
    /// Every child enabled, and both listen addresses `_taken`'s.
    config: EngenhoConfig,
    /// Held for the whole matrix, so every bind of its address fails.
    _taken: std::net::TcpListener,
}

impl World {
    async fn new() -> Self {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("a port to hold");
        let addr = taken
            .local_addr()
            .expect("the held port's address")
            .to_string();
        let mut config = EngenhoConfig::prescribed_default();
        config.runtime.kubelet_listen_addr.clone_from(&addr);
        config.runtime.etcd_listen_addr = addr;
        let windows = Windows::of(&config.controllers)
            .with_fallback(FALLBACK)
            .with_stuck_tick_after(STUCK);
        Self {
            store: single_voter_store("fault-matrix").await,
            windows,
            config,
            _taken: taken,
        }
    }
}

/// `child` with `fault` injected, built the way the runtime builds it;
/// `None` when the fault cannot strike it.
fn inject(child: Child, fault: Fault, world: &World) -> Option<ChildTask> {
    match child {
        Child::Driver(driver) => tick_loop_with(TickLoop::Driver(driver), fault, world),
        Child::NodeLease => tick_loop_with(TickLoop::NodeLease, fault, world),
        Child::Listener(listener) => Some(listener_with(listener, fault, world)),
    }
}

fn tick_loop_with(tick_loop: TickLoop, fault: Fault, world: &World) -> Option<ChildTask> {
    let tick = TickFault::of(fault)?;
    Some(drive(tick_loop, Faulty(tick), &world.store, world.windows))
}

fn listener_with(listener: Listener, fault: Fault, world: &World) -> ChildTask {
    let beat = Arc::new(Heartbeat::new());
    match fault {
        Fault::Panic => ChildTask::new(
            beat.clone(),
            serve_rebinding(listener, beat, panicking_attempt),
        ),
        Fault::Hang => ChildTask::new(
            beat.clone(),
            serve_rebinding(listener, beat, std::future::pending::<()>),
        ),
        Fault::Error => ChildTask::new(
            beat.clone(),
            serve_rebinding(listener, beat, returning_attempt),
        ),
        // The listener's own body, binding its own address, which is taken.
        Fault::BindFailure => listener_task(
            listener,
            &world.config,
            std::sync::Weak::new(),
            &world.store,
        ),
    }
}

// ── reading a cell back ────────────────────────────────────────────────

/// What the supervisor observed of one spawned child.
#[derive(Debug)]
struct Seen {
    state: ChildState,
    beat: Beat,
    liveness: Liveness,
}

impl Seen {
    /// `child` in `children`, read now; `None` if it was never spawned.
    fn of(children: &Children, child: Child, windows: Windows) -> Option<Self> {
        let state = children.get(child)?.state();
        let row = children.row(child)?;
        Some(Self {
            state,
            beat: children.get(child)?.beat().snapshot(),
            liveness: row.liveness(windows, Instant::now(), WallClock.now()),
        })
    }

    /// The outcome these observations are; `None` when they are none of
    /// them.
    fn outcome(&self) -> Option<Supervision> {
        let beat = &self.beat;
        let alive = self.liveness == Liveness::Alive;
        let stalled = matches!(self.liveness, Liveness::Stalled { .. });
        let one_tick_in_flight = beat.in_flight() && beat.ticks_started == 1;
        let ticked_again = beat.ticks_started >= 2;

        if self.state == ChildState::Dead(DeathCause::Panicked)
            && self.liveness == Liveness::Dead
            && beat.ticks_started == 1
        {
            return Some(Supervision::Dead);
        }
        if self.state != ChildState::Running {
            return None;
        }
        if one_tick_in_flight && stalled {
            return Some(Supervision::Stalled);
        }
        if one_tick_in_flight && alive {
            return Some(Supervision::Unobserved);
        }
        if ticked_again && alive && beat.last_class == Some(TickClass::Panicked) {
            return Some(Supervision::Contained);
        }
        if ticked_again && alive && beat.last_class == Some(TickClass::Transient) {
            return Some(Supervision::Retried);
        }
        if ticked_again
            && stalled
            && !beat.in_flight()
            && matches!(
                beat.last_class,
                Some(TickClass::Halted | TickClass::Panicked)
            )
        {
            return Some(Supervision::Rebinds);
        }
        None
    }

    /// Every panic the fault caused is counted, and nothing else is counted
    /// as one: a Dead child's one panic, or one per finished tick or
    /// attempt.
    fn panics_counted(&self, fault: Fault) -> bool {
        match fault {
            Fault::Panic => self.beat.panics == self.beat.ticks_finished.max(1),
            Fault::Hang | Fault::Error | Fault::BindFailure => self.beat.panics == 0,
        }
    }
}

/// One cell of the matrix.
#[derive(Debug)]
struct Cell {
    child: Child,
    fault: Fault,
    declared: Supervision,
    /// `None`: nothing was spawned, because the fault cannot be injected.
    seen: Option<Seen>,
    /// What `seen` is, when it is an outcome.
    observed: Option<Supervision>,
}

impl Cell {
    fn read(children: &Children, child: Child, fault: Fault, windows: Windows) -> Self {
        let seen = Seen::of(children, child, windows);
        Self {
            child,
            fault,
            declared: child.supervision(fault),
            observed: seen.as_ref().and_then(Seen::outcome),
            seen,
        }
    }

    /// Whether the runtime did what the catalog declares.
    fn holds(&self) -> bool {
        match &self.seen {
            None => self.declared == Supervision::Inapplicable,
            Some(seen) => self.observed == Some(self.declared) && seen.panics_counted(self.fault),
        }
    }
}

/// Wait out every death `children` has to report, so each dead child's
/// state is the one the supervisor gave it.
async fn drain_deaths(children: &mut Children) {
    while tokio::time::timeout(Duration::from_millis(50), children.next_dead())
        .await
        .is_ok()
    {}
}

// ── the matrix ─────────────────────────────────────────────────────────

/// Every child, struck by every fault, does what the catalog declares.
///
/// One set per fault, each holding every child the fault can strike, all
/// running at once. They are read after `settle`: a failing listener has
/// tried at 0 s and at the rebind curve's base and waits until three
/// times it; a hung tick is well past its (rounded-up) stuck window; a
/// tick loop on a 200 ms fallback has ticked several times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_child_meets_every_fault_as_the_catalog_declares() {
    let world = World::new().await;
    let settle = REBIND.base() * 5 / 2;

    let mut sets: Vec<(Fault, Children)> = Fault::ALL
        .iter()
        .map(|&fault| {
            let children =
                Children::spawn_catalog(&world.config, |child, _| inject(child, fault, &world));
            (fault, children)
        })
        .collect();
    tokio::time::sleep(settle).await;

    let mut cells = Vec::new();
    for (fault, children) in &mut sets {
        drain_deaths(children).await;
        cells.extend(Child::all().map(|child| Cell::read(children, child, *fault, world.windows)));
    }
    for (_, children) in &mut sets {
        children.stop().await;
    }

    // Positive controls: every cell was read exactly once, and the matrix is
    // not vacuous for any outcome — each one the catalog can declare, some
    // cell does.
    let read: BTreeSet<(Child, Fault)> = cells.iter().map(|c| (c.child, c.fault)).collect();
    let product: BTreeSet<(Child, Fault)> = Child::all()
        .flat_map(|child| Fault::ALL.iter().map(move |fault| (child, *fault)))
        .collect();
    assert_eq!(
        (read.len(), &read),
        (cells.len(), &product),
        "the cells read are not every (child, fault) pair, each once"
    );
    let declared: BTreeSet<Supervision> = cells.iter().map(|c| c.declared).collect();
    let every: BTreeSet<Supervision> = Supervision::ALL.iter().copied().collect();
    assert_eq!(
        declared, every,
        "an outcome the catalog can declare is declared by no cell, so nothing exercises it"
    );

    let wrong: Vec<&Cell> = cells.iter().filter(|c| !c.holds()).collect();
    assert!(
        wrong.is_empty(),
        "{} of {} cells did not do what the catalog declares: {wrong:#?}",
        wrong.len(),
        cells.len()
    );
}

/// The catalog's own declaration, spot-checked where the plan names it: the
/// kubelet's panic is fatal (its `local` map lives across ticks), a
/// stateless driver's is contained, and no tick loop can fail to bind.
#[test]
fn the_declaration_follows_the_catalog_rows() {
    use crate::child::Driver;

    assert_eq!(
        Child::Driver(Driver::Kubelet).supervision(Fault::Panic),
        Supervision::Dead
    );
    assert_eq!(
        Child::Driver(Driver::Gc).supervision(Fault::Panic),
        Supervision::Contained
    );
    for child in Child::all() {
        let binds = matches!(child, Child::Listener(_));
        assert_eq!(
            child.supervision(Fault::BindFailure) == Supervision::Inapplicable,
            !binds,
            "{child}: a bind failure applies exactly to the children that bind"
        );
        if let Some(tick_loop) = child.tick_loop() {
            let fatal = tick_loop.tick_state() == crate::TickState::Stateful;
            assert_eq!(
                child.supervision(Fault::Panic),
                if fatal {
                    Supervision::Dead
                } else {
                    Supervision::Contained
                },
                "{child}: a panic is decided by the tick loop's own row"
            );
        }
    }
}
