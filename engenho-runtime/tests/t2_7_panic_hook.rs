//! T2.7: the process-wide panic hook chains to the hook before it and counts
//! each panic exactly once.
//!
//! Its own test binary, with one test, because a panic hook is process
//! state: in a binary with other tests, their panics land in the count and
//! whatever hook they installed is the "previous" one. Here nothing else
//! runs, so the counts can be asserted exactly.

use std::sync::atomic::{AtomicU64, Ordering};

use engenho_runtime::PanicCounter;

/// Panics the hook installed BEFORE the counter saw.
static PREVIOUS_SAW: AtomicU64 = AtomicU64::new(0);

#[test]
fn the_counting_hook_chains_to_the_previous_hook_and_counts_each_panic_once() {
    std::panic::set_hook(Box::new(|_| {
        PREVIOUS_SAW.fetch_add(1, Ordering::SeqCst);
    }));
    let counter = PanicCounter::install();
    // A second runtime in the same process installs again; it must not stack
    // a second counting hook on the first.
    let again = PanicCounter::install();

    let counted_before = counter.total();
    let seen_before = PREVIOUS_SAW.load(Ordering::SeqCst);
    for _ in 0..3 {
        assert!(std::panic::catch_unwind(|| panic!("one panic")).is_err());
    }

    assert_eq!(
        counter.total() - counted_before,
        3,
        "three panics, installed twice: each is counted exactly once"
    );
    assert_eq!(
        PREVIOUS_SAW.load(Ordering::SeqCst) - seen_before,
        3,
        "the hook installed before the counter no longer sees panics"
    );
    assert_eq!(again.total(), counter.total(), "two counters, one count");
}
