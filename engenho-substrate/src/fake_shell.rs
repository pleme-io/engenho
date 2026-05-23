//! `fake_backend_shell!` macro — generates the canonical Fake-
//! backend harness used 12+ times across the substrate.
//!
//! Per the PRIME DIRECTIVE: every Fake* test backend had the same
//! shape — `Arc<Mutex<State>>` + an events log + `fail_next`
//! injection + assertion helpers. This macro generates the
//! wrapper + methods; operator declares the state struct + trait
//! impl alongside.
//!
//! ## Authoring shape (no extra deps)
//!
//! ```ignore
//! use engenho_substrate::fake_backend_shell;
//!
//! pub enum FakeFooEvent { Insert(String), Remove(String) }
//!
//! #[derive(Debug, Clone, thiserror::Error)]
//! pub enum FakeFooError {
//!     #[error("backend: {0}")]
//!     Backend(String),
//! }
//!
//! #[derive(Default)]
//! pub struct FakeFooState {
//!     events: Vec<FakeFooEvent>,
//!     fail_next: Option<FakeFooError>,
//! }
//!
//! fake_backend_shell! {
//!     pub struct FakeFoo {
//!         state: FakeFooState,
//!         event: FakeFooEvent,
//!         error: FakeFooError,
//!     }
//! }
//! ```
//!
//! Generates the wrapper + helper methods (`new`, `record_event`,
//! `take_fail_next`, `events`, `call_count`, `fail_next`, `reset`).
//! Operator writes the state struct (with `events: Vec<E>` +
//! `fail_next: Option<Err>` fields) + the trait impl manually.

/// Generate the canonical Fake-backend harness for the given
/// wrapper + state + event + error types. See module docs.
#[macro_export]
macro_rules! fake_backend_shell {
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident {
            state: $state_ty:ty,
            event: $event_ty:ty,
            error: $error_ty:ty $(,)?
        }
    ) => {
        $(#[$attr])*
        #[derive(Default, Clone)]
        $vis struct $name {
            inner: ::std::sync::Arc<::tokio::sync::Mutex<$state_ty>>,
        }

        impl $name {
            /// Construct a fresh fake — empty event log, no
            /// pending fail injection.
            #[must_use]
            $vis fn new() -> Self {
                Self::default()
            }

            /// Push an event onto the log. Operators call this
            /// from inside their trait method impls.
            $vis async fn record_event(&self, event: $event_ty) {
                self.inner.lock().await.events.push(event);
            }

            /// Drain the pending `fail_next` slot. Operators call
            /// this at the START of their trait method impls; if
            /// `Some`, return the error and skip the rest.
            $vis async fn take_fail_next(&self) -> Option<$error_ty> {
                self.inner.lock().await.fail_next.take()
            }

            /// Snapshot of the event log.
            $vis async fn events(&self) -> Vec<$event_ty>
            where
                $event_ty: Clone,
            {
                self.inner.lock().await.events.clone()
            }

            /// Total event count.
            $vis async fn call_count(&self) -> usize {
                self.inner.lock().await.events.len()
            }

            /// Pin the NEXT trait-method call to fail with this error.
            $vis async fn fail_next(&self, err: $error_ty) {
                self.inner.lock().await.fail_next = Some(err);
            }

            /// Clear all recorded events + any pending fail injection.
            $vis async fn reset(&self) {
                let mut state = self.inner.lock().await;
                state.events.clear();
                state.fail_next = None;
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use thiserror::Error;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum FakeFooEvent {
        Insert(String),
        Remove(String),
    }

    #[derive(Debug, Clone, Error)]
    pub enum FakeFooError {
        #[error("backend: {0}")]
        Backend(String),
    }

    #[derive(Default)]
    pub struct FakeFooState {
        events: Vec<FakeFooEvent>,
        fail_next: Option<FakeFooError>,
    }

    crate::fake_backend_shell! {
        pub struct FakeFoo {
            state: FakeFooState,
            event: FakeFooEvent,
            error: FakeFooError,
        }
    }

    #[tokio::test]
    async fn fresh_fake_has_empty_event_log() {
        let f = FakeFoo::new();
        assert_eq!(f.call_count().await, 0);
        assert!(f.events().await.is_empty());
    }

    #[tokio::test]
    async fn record_event_appends_to_log() {
        let f = FakeFoo::new();
        f.record_event(FakeFooEvent::Insert("a".into())).await;
        f.record_event(FakeFooEvent::Remove("a".into())).await;
        assert_eq!(f.call_count().await, 2);
        let events = f.events().await;
        assert_eq!(events[0], FakeFooEvent::Insert("a".into()));
        assert_eq!(events[1], FakeFooEvent::Remove("a".into()));
    }

    #[tokio::test]
    async fn fail_next_pins_next_call_single_shot() {
        let f = FakeFoo::new();
        f.fail_next(FakeFooError::Backend("boom".into())).await;
        let err = f.take_fail_next().await.unwrap();
        assert!(matches!(err, FakeFooError::Backend(_)));
        // Subsequent take returns None — single-shot.
        assert!(f.take_fail_next().await.is_none());
    }

    #[tokio::test]
    async fn reset_clears_events_and_fail_next() {
        let f = FakeFoo::new();
        f.record_event(FakeFooEvent::Insert("x".into())).await;
        f.fail_next(FakeFooError::Backend("y".into())).await;
        f.reset().await;
        assert_eq!(f.call_count().await, 0);
        assert!(f.take_fail_next().await.is_none());
    }

    #[tokio::test]
    async fn clone_shares_inner_state() {
        let f1 = FakeFoo::new();
        let f2 = f1.clone();
        f1.record_event(FakeFooEvent::Insert("a".into())).await;
        // f2 sees f1's event — same Arc<Mutex<State>>.
        assert_eq!(f2.call_count().await, 1);
    }

    #[tokio::test]
    async fn cross_task_assertion_via_clone() {
        let f = FakeFoo::new();
        let f2 = f.clone();
        let h = tokio::spawn(async move {
            f2.record_event(FakeFooEvent::Insert("from-task".into())).await;
        });
        h.await.unwrap();
        assert_eq!(f.call_count().await, 1);
    }

    // ── Operator's trait impl uses the helpers ─────────────────

    #[async_trait::async_trait]
    trait FooBackend {
        async fn insert(&self, key: String) -> Result<(), FakeFooError>;
    }

    #[async_trait::async_trait]
    impl FooBackend for FakeFoo {
        async fn insert(&self, key: String) -> Result<(), FakeFooError> {
            if let Some(err) = self.take_fail_next().await {
                return Err(err);
            }
            self.record_event(FakeFooEvent::Insert(key)).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn operator_trait_impl_uses_helpers() {
        let f = FakeFoo::new();
        f.insert("x".into()).await.unwrap();
        assert_eq!(f.call_count().await, 1);
    }

    #[tokio::test]
    async fn operator_trait_impl_returns_fail_next_error() {
        let f = FakeFoo::new();
        f.fail_next(FakeFooError::Backend("rejected".into())).await;
        let err = f.insert("x".into()).await.unwrap_err();
        assert!(matches!(err, FakeFooError::Backend(_)));
        // Event NOT recorded because we returned early.
        assert_eq!(f.call_count().await, 0);
    }
}
