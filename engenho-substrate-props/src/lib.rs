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

// No public API — pure-test crate. The `proptest!` macros live
// in `src/lib.rs`'s `#[cfg(test)]` block + the per-file test
// modules in `tests/`.

#![allow(missing_docs)]
