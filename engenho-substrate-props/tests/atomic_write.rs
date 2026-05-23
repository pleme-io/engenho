//! Property: write_atomic round-trip + tmp_path_for invariants.

use engenho_substrate::{AtomicWriteError, tmp_path_for, write_atomic};
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

    /// tmp_path_for appends ".tmp" to the path's last component.
    #[test]
    fn tmp_path_appends_tmp_suffix(name in "[a-zA-Z0-9_.-]{1,32}") {
        let base = std::env::temp_dir().join(&name);
        let tmp = tmp_path_for(&base);
        let want = std::env::temp_dir().join(format!("{name}.tmp"));
        assert_eq!(tmp, want);
    }

    /// After successful write_atomic, the tmp file does NOT exist
    /// (it was renamed to the canonical path).
    #[test]
    fn tmp_file_does_not_persist_after_success(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let path = unique_temp_path("no-tmp-left");
        let tmp = tmp_path_for(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&tmp);
        write_atomic(&path, &bytes).unwrap();
        assert!(!tmp.exists(), "tmp file persisted after rename: {tmp:?}");
        assert!(path.exists(), "canonical path missing: {path:?}");
        let _ = std::fs::remove_file(&path);
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
