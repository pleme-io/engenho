//! # engenho-substrate-props
//!
//! Property-based tests for engenho-substrate's typed primitives.
//! Pure-test crate — no production exports. Every test asserts a
//! TYPED INVARIANT that must hold for every well-formed input.
//!
//! ## Why a separate crate
//!
//! - The substrate's own `#[cfg(test)]` blocks run *unit* tests
//!   (specific inputs, specific outputs). This crate runs
//!   *property* tests (arbitrary inputs, invariants hold).
//! - The substrate's CI runs unit tests at `cargo test --workspace
//!   --lib` speed. This crate's `cargo test -p
//!   engenho-substrate-props` can run at higher case counts
//!   (`PROPTEST_CASES=4096`) for deep validation without
//!   slowing the fast loop.
//! - Property-test failures surface MINIMAL counterexamples via
//!   proptest's shrinking; the substrate's invariants stay
//!   honest.
//!
//! ## What's covered
//!
//! - **Plantio**: every well-formed Plantio passes `validate()`
//!   AND `topo_sort()` produces stages in dependency order.
//!   Cycle injection always produces `PlantioError::Cycle`.
//! - **WorkloadShape**: `shape_hash(drv_hash)` is deterministic +
//!   diverges per (shape, drv) pair (low collision probability).
//! - **EnsaioId**: `derive(search, gen, genotype, lineage_root)`
//!   is deterministic + diverges per input field.
//! - **QuorumTracker**: K distinct same-evidence receipts always
//!   reach Reached; mixed-evidence above threshold always reports
//!   Dissent.
//! - **MagicBlob**: encode→decode is identity for every serde
//!   value; corruption rejected.
//! - **ComposeIr**: fingerprint() is deterministic + diverges per
//!   distinct IR.
//! - **Linhagem**: fingerprint() is deterministic + diverges per
//!   distinct chain.
//!
//! ## Running
//!
//! ```bash
//! # Default (PROPTEST_CASES=256)
//! cargo test -p engenho-substrate-props
//!
//! # Deep validation (PROPTEST_CASES=4096)
//! PROPTEST_CASES=4096 cargo test -p engenho-substrate-props
//! ```
//!
//! The deep-test workflow (`/.github/workflows/deep-test.yml`)
//! runs at 4096 cases on every PR + nightly cron.

// One small public API for the test files in `tests/`: the
// `proptest_with_env!` macro, the `proptest_cases()` env helper,
// the `block_on()` async-test wrapper, and the `helpers` module
// for sample fixtures (sample_drv / sample_receipt / sample_emitter
// / sample_nar). They collapse the duplicated ceremony that was
// hand-rolled in every property-test file before extraction.

#![allow(missing_docs)]

pub mod helpers;

/// Read the `PROPTEST_CASES` environment variable as the proptest
/// case-count budget. Defaults to 256 (fast loop). Deep-test CI
/// sets `PROPTEST_CASES=4096`.
///
/// Extracted from a copy-paste block that appeared verbatim in
/// every test file's `proptest! { #![proptest_config(...)] }`.
#[must_use]
pub fn proptest_cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256)
}

/// Run an async block on a fresh tokio runtime. The async property-test
/// pattern repeats verbatim in ledger / broadcast_ledger / watched_cache
/// and will appear in every future async property file — extracted per
/// the third-site rule.
///
/// ## Before
///
/// ```ignore
/// #[test]
/// fn my_async_invariant() {
///     let rt = tokio::runtime::Runtime::new().unwrap();
///     rt.block_on(async {
///         let l = MemoryLedger::new();
///         l.ingest(...).await.unwrap();
///     });
/// }
/// ```
///
/// ## After
///
/// ```ignore
/// use engenho_substrate_props::block_on;
///
/// #[test]
/// fn my_async_invariant() {
///     block_on(async {
///         let l = MemoryLedger::new();
///         l.ingest(...).await.unwrap();
///     });
/// }
/// ```
pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
    ::tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(f)
}

/// Wraps `proptest::proptest!` with the env-driven `ProptestConfig`
/// applied automatically. Saves the 5-line ceremony in every
/// property-test file.
///
/// ## Before
///
/// ```ignore
/// use proptest::prelude::*;
/// proptest! {
///     #![proptest_config(ProptestConfig {
///         cases: std::env::var("PROPTEST_CASES").ok()
///             .and_then(|s| s.parse().ok()).unwrap_or(256),
///         ..ProptestConfig::default()
///     })]
///     #[test]
///     fn my_invariant(...) { ... }
/// }
/// ```
///
/// ## After
///
/// ```ignore
/// use engenho_substrate_props::proptest_with_env;
/// use proptest::prelude::*;
/// proptest_with_env! {
///     #[test]
///     fn my_invariant(...) { ... }
/// }
/// ```
#[macro_export]
macro_rules! proptest_with_env {
    ($($body:tt)*) => {
        ::proptest::proptest! {
            #![proptest_config(::proptest::test_runner::Config {
                cases: $crate::proptest_cases(),
                ..::proptest::test_runner::Config::default()
            })]
            $($body)*
        }
    };
}
