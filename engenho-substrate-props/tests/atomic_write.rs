//! Property: write_atomic round-trips, and never tears or leaks a temp file.
//!
//! ★ CORRECTED 2026-09-19. This file asserted two properties of
//! `tmp_path_for`, which `35e2e45` made private — and it went unnoticed because
//! test.yml was already red for unrelated reasons, so HEAD's test suite simply
//! stopped compiling.
//!
//! One of them, `tmp_path_appends_tmp_suffix`, was not a property worth porting.
//! It pinned the DETERMINISTIC `<path>.tmp` name, and that determinism was the
//! defect `35e2e45` fixed: two concurrent writers of one target shared a temp
//! file, so one could publish the other's half-written bytes. The test did not
//! merely fail to catch the race — it asserted the precondition for it. It is
//! deleted rather than rewritten, and `concurrent_writers_never_tear_or_leak`
//! pins the property the fix actually guarantees.

use engenho_substrate::{AtomicWriteError, write_atomic};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_temp_path(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "engenho-substrate-props-atomic-{}-{}-{tag}",
        std::process::id(),
        n
    ))
}

proptest_with_env! {
    /// write_atomic then read back yields the exact bytes.
    #[test]
    fn write_then_read_round_trips(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let path = unique_temp_path("roundtrip");
        let _ = std::fs::remove_file(&path);
        write_atomic(&path, &bytes).unwrap();
        let read_back = std::fs::read(&path).unwrap();
        assert_eq!(read_back, bytes);
        let _ = std::fs::remove_file(&path);
    }

    /// Overwriting a path with new bytes replaces them atomically.
    #[test]
    fn overwrite_replaces_contents(
        first in proptest::collection::vec(any::<u8>(), 0..256),
        second in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let path = unique_temp_path("overwrite");
        let _ = std::fs::remove_file(&path);
        write_atomic(&path, &first).unwrap();
        write_atomic(&path, &second).unwrap();
        let read_back = std::fs::read(&path).unwrap();
        assert_eq!(read_back, second);
        let _ = std::fs::remove_file(&path);
    }

    /// After a successful write the target's directory holds the target and
    /// nothing else — no temp file under ANY name.
    ///
    /// The old form checked `!tmp_path_for(&path).exists()`, i.e. one predicted
    /// name. A leak under a different name passed it. Listing the directory is
    /// name-agnostic, which is the only honest way to assert "no temp survives"
    /// once temp names are unique.
    #[test]
    fn no_temp_file_survives_a_successful_write(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let dir = unique_temp_path("no-tmp-left");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("target.bin");
        write_atomic(&path, &bytes).unwrap();
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("target.bin")],
            "the directory must hold only the target; found {entries:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Many writers racing on ONE target: every write succeeds, the result is
    /// exactly one writer's complete payload, and no temp file is left behind.
    ///
    /// This is the property the deterministic `<path>.tmp` name violated, and
    /// no test in the tree asserted it — the previous file pinned the name
    /// that made the race possible. Payloads are uniform per writer, so a torn
    /// file (bytes from two writers, or a truncated one) is detectable by
    /// content alone.
    #[test]
    fn concurrent_writers_never_tear_or_leak(writers in 2usize..8, len in 1usize..2048) {
        let dir = unique_temp_path("race");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("target.bin");
        let handles: Vec<_> = (0..writers)
            .map(|w| {
                let path = path.clone();
                let payload = vec![u8::try_from(w).unwrap(); len];
                std::thread::spawn(move || write_atomic(&path, &payload))
            })
            .collect();
        for h in handles {
            h.join().unwrap().unwrap();
        }
        let got = std::fs::read(&path).unwrap();
        assert_eq!(got.len(), len, "a torn write changed the length");
        let first = got[0];
        assert!(got.iter().all(|b| *b == first),
            "bytes from more than one writer — a torn publish");
        assert!(usize::from(first) < writers, "payload from no writer at all");
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries.len(), 1, "a temp file leaked under the race: {entries:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// write_atomic creates parent dirs if missing.
    #[test]
    fn creates_parent_directories(
        depth in 1usize..5,
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut path = unique_temp_path("parent-creation");
        for i in 0..depth {
            path = path.join(format!("d{i}"));
        }
        path = path.join("file.bin");
        // Make sure root doesn't already have the nested structure.
        if let Some(top) = path.ancestors().nth(depth) {
            let _ = std::fs::remove_dir_all(top);
        }
        write_atomic(&path, &bytes).unwrap();
        let read_back = std::fs::read(&path).unwrap();
        assert_eq!(read_back, bytes);
        // Cleanup
        if let Some(top) = path.ancestors().nth(depth) {
            let _ = std::fs::remove_dir_all(top);
        }
    }

    /// AtomicWriteError::Io exposes the underlying message verbatim.
    #[test]
    fn error_kind_is_stable(msg in "[a-zA-Z0-9: ]{1,32}") {
        let err = AtomicWriteError::Io(msg.clone());
        assert_eq!(
            <AtomicWriteError as engenho_substrate::ErrorKind>::kind(&err),
            "io"
        );
        assert!(err.to_string().contains(&msg));
    }

    /// Empty bytes write + read round-trip.
    #[test]
    fn empty_bytes_round_trip(_seed in any::<u8>()) {
        let path = unique_temp_path("empty");
        let _ = std::fs::remove_file(&path);
        write_atomic(&path, &[]).unwrap();
        let read_back = std::fs::read(&path).unwrap();
        assert!(read_back.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
