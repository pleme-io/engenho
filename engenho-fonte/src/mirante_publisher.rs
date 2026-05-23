//! Real [`Publisher`] backed by engenho-substrate's `Mirante` registry +
//! `ObservationChannel`s.
//!
//! Each Conduit tick's [`Outcome`] is published to a registered
//! channel. Subscribers (operator dashboards, controllers, the
//! AnomalyChain's mirror, anything that holds a `watch::Receiver`)
//! observe the latest snapshot without polling.
//!
//! Always-on — no Cargo feature flag needed; mirante is already in
//! engenho-substrate, an unconditional dep of fonte.

use crate::{FonteResult, Outcome, Publisher};
use async_trait::async_trait;
use engenho_substrate::mirante::{Mirante, ObservationChannel};
use engenho_substrate::relogio::Clock;
use std::sync::Arc;
use std::sync::Mutex;

/// Publisher that mirrors every Outcome into a typed
/// `ObservationChannel<Outcome>` registered under one stable name
/// (`"fonte.outcome"`). The channel is last-value-only — a slow
/// subscriber sees the LATEST outcome, not every intermediate.
pub struct MirantePublisher {
    channel: Arc<ObservationChannel<Outcome>>,
    mirante: Mutex<Mirante>,
}

impl MirantePublisher {
    /// New publisher with the channel registered into a fresh
    /// `Mirante`. `clock` provides the typed timestamps for the
    /// channel's `last_changed`; in tests pass a FrozenClock.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let initial = Outcome {
            revision: 0,
            proposal_id: 0,
            receipt_id: Arc::from("none"),
            finalized_at_ms: 0,
        };
        let channel = Arc::new(ObservationChannel::new(initial, clock));
        let mut m = Mirante::new();
        m.register("fonte.outcome", channel.clone());
        Self {
            channel,
            mirante: Mutex::new(m),
        }
    }

    /// Borrow the mirante registry — operators register additional
    /// channels here (e.g. an AnomalyEvent broadcast channel).
    pub fn with_mirante<R>(&self, f: impl FnOnce(&Mirante) -> R) -> R {
        f(&self.mirante.lock().expect("mirante poisoned"))
    }

    /// Borrow the underlying outcome channel directly — useful for
    /// subscribers + assertion in tests.
    #[must_use]
    pub fn channel(&self) -> Arc<ObservationChannel<Outcome>> {
        self.channel.clone()
    }
}

#[async_trait]
impl Publisher for MirantePublisher {
    async fn publish(&self, outcome: &Outcome) -> FonteResult<()> {
        self.channel.publish(outcome.clone());
        Ok(())
    }
}
