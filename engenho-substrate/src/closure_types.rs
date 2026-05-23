//! `async_closure_type!` macro — typed `Arc<dyn Fn(...) -> BoxFuture
//! <...> + Send + Sync>` alias generator.
//!
//! Closes the 7+-site duplication of typed async-closure aliases
//! across the substrate (BytesAccessor, SmokeBuilder,
//! IndependentRebuild, SignerCheck, OciSourceRef, OciDestRef,
//! OciDestReader, …). Per the PRIME DIRECTIVE — "any pattern that
//! appears ≥2 times → macro_rules! or proc-macro" — extracted.
//!
//! ## Authoring shape
//!
//! ```ignore
//! use engenho_substrate::async_closure_type;
//!
//! // Async closure: (subject: [u8;32]) -> Result<Option<Vec<u8>>, VerifyError>
//! async_closure_type! {
//!     /// Operator-supplied bytes accessor for HashEqualityVerifier.
//!     pub type BytesAccessor = ([u8; 32]) -> Result<Option<Vec<u8>>, VerifyError>;
//! }
//!
//! // Sync closure: (drv: &Drv) -> String  (no Result)
//! async_closure_type! {
//!     /// Operator-supplied source-ref renderer for OciImageRenderer.
//!     pub type OciSourceRef = sync (&Drv) -> String;
//! }
//! ```
//!
//! Expands respectively to:
//!
//! ```ignore
//! pub type BytesAccessor = std::sync::Arc<
//!     dyn Fn([u8; 32]) -> futures::future::BoxFuture<
//!         'static,
//!         Result<Option<Vec<u8>>, VerifyError>,
//!     > + Send + Sync,
//! >;
//!
//! pub type OciSourceRef =
//!     std::sync::Arc<dyn Fn(&Drv) -> String + Send + Sync>;
//! ```
//!
//! The `sync` keyword (parsed by the macro as an identifier) selects
//! the synchronous variant.

/// Generate a typed `Arc<dyn Fn(...) -> ...>` alias.
///
/// Two forms:
/// - **Async**: `pub type Name = (Args) -> ReturnType` — wraps in
///   `futures::future::BoxFuture<'static, ReturnType>`.
/// - **Sync**: `pub type Name = sync (Args) -> ReturnType` — bare
///   `ReturnType`.
///
/// Both forms add `+ Send + Sync` automatically — every closure
/// alias the substrate ships is shareable across tasks by default.
#[macro_export]
macro_rules! async_closure_type {
    // Sync variant — note the bare `sync` keyword.
    (
        $(#[$attr:meta])*
        $vis:vis type $name:ident = sync ( $($arg:ty),* $(,)? ) -> $ret:ty;
    ) => {
        $(#[$attr])*
        $vis type $name = ::std::sync::Arc<
            dyn Fn( $($arg),* ) -> $ret + Send + Sync,
        >;
    };
    // Async variant — wraps `$ret` in BoxFuture<'static, $ret>.
    (
        $(#[$attr:meta])*
        $vis:vis type $name:ident = ( $($arg:ty),* $(,)? ) -> $ret:ty;
    ) => {
        $(#[$attr])*
        $vis type $name = ::std::sync::Arc<
            dyn Fn( $($arg),* ) -> ::futures::future::BoxFuture<'static, $ret>
                + Send + Sync,
        >;
    };
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use std::sync::Arc;

    // ── Async variant ──────────────────────────────────────────

    async_closure_type! {
        /// Test alias: subject hash → Option<Vec<u8>>, simple Ok return.
        pub type TestBytesAccessor = ([u8; 32]) -> Result<Option<Vec<u8>>, String>;
    }

    async_closure_type! {
        /// Test alias: arity-2.
        pub type TestArity2 = (String, u32) -> Result<bool, String>;
    }

    async_closure_type! {
        /// Test alias: arity-0 (a fallible "now").
        pub type TestArity0 = () -> Result<u64, String>;
    }

    fn make_accessor() -> TestBytesAccessor {
        Arc::new(|_subject: [u8; 32]| {
            async move { Ok(Some(b"hello".to_vec())) }.boxed()
        })
    }

    fn make_arity_two() -> TestArity2 {
        Arc::new(|name: String, n: u32| {
            async move { Ok(name.len() as u32 == n) }.boxed()
        })
    }

    fn make_arity_zero() -> TestArity0 {
        Arc::new(|| async move { Ok(42u64) }.boxed())
    }

    #[tokio::test]
    async fn async_alias_arity_one_invokes() {
        let f = make_accessor();
        let out = f([0u8; 32]).await.unwrap();
        assert_eq!(out, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn async_alias_arity_two_invokes() {
        let f = make_arity_two();
        assert!(f("abc".into(), 3).await.unwrap());
        assert!(!f("abc".into(), 4).await.unwrap());
    }

    #[tokio::test]
    async fn async_alias_arity_zero_invokes() {
        let f = make_arity_zero();
        assert_eq!(f().await.unwrap(), 42);
    }

    // ── Sync variant ───────────────────────────────────────────

    async_closure_type! {
        /// Test alias: sync (no Result), arity-1.
        pub type TestSyncRefSig = sync (u32) -> String;
    }

    async_closure_type! {
        /// Test alias: sync, arity-2.
        pub type TestSyncArity2 = sync (u32, u32) -> u32;
    }

    #[test]
    fn sync_alias_arity_one_invokes() {
        let f: TestSyncRefSig = Arc::new(|n: u32| format!("n={n}"));
        assert_eq!(f(5), "n=5");
    }

    #[test]
    fn sync_alias_arity_two_invokes() {
        let f: TestSyncArity2 = Arc::new(|a: u32, b: u32| a.saturating_add(b));
        assert_eq!(f(2, 3), 5);
    }

    // ── Shareable across tasks (Send + Sync) ───────────────────

    #[tokio::test]
    async fn async_alias_is_send_plus_sync() {
        let f = make_accessor();
        let f2 = f.clone();
        let handle = tokio::spawn(async move {
            let out = f2([1u8; 32]).await.unwrap();
            out.unwrap().len()
        });
        assert_eq!(handle.await.unwrap(), 5);
        // Original still usable.
        assert_eq!(f([2u8; 32]).await.unwrap().unwrap().len(), 5);
    }

    #[test]
    fn sync_alias_is_send_plus_sync() {
        let f: TestSyncRefSig = Arc::new(|n: u32| format!("{n}"));
        let f2 = f.clone();
        let handle = std::thread::spawn(move || f2(99));
        assert_eq!(handle.join().unwrap(), "99");
        assert_eq!(f(7), "7");
    }

    // ── Macro accepts trailing commas in argument list ─────────

    async_closure_type! {
        pub type TestTrailingComma = (u32,) -> Result<u32, String>;
    }

    #[tokio::test]
    async fn macro_accepts_trailing_comma_in_args() {
        let f: TestTrailingComma = Arc::new(|n: u32| async move { Ok(n + 1) }.boxed());
        assert_eq!(f(10).await.unwrap(), 11);
    }
}
