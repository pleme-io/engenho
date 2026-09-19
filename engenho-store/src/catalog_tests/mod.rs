//! Catalog-level suites, moved into the crate by T3.2b.
//!
//! Each of these was an integration test under `tests/` that built a
//! `ResourceCatalog` and drove it directly. Sealing the catalog
//! (`pub(crate)` under `#![deny(private_interfaces)]`) makes it unnameable
//! outside the crate, so the suites moved here unchanged apart from their
//! import paths. Their proptest regression seeds moved with them, to
//! `proptest-regressions/catalog_tests/`, where proptest looks for a
//! source file under `src/`.
//!
//! Suites that need only the public surface (`StoreMesh`) stay in `tests/`.

mod m0_1_cas_pagination;
mod m1_etcd_txn;
mod m1_mvcc_historical;
mod r9_mvcc_revision;
mod t3_2a_paged_list;
