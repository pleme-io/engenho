//! Property: FakeCommandRunner pin/run/record invariants.

use engenho_substrate::{
    CommandError, CommandRequest, CommandResponse, CommandRunner, FakeCommandRunner,
};
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;

fn ok_response(stdout: Vec<u8>) -> CommandResponse {
    CommandResponse {
        exit_code: Some(0),
        stdout,
        stderr: Vec::new(),
    }
}

fn err_response(code: i32) -> CommandResponse {
    CommandResponse {
        exit_code: Some(code),
        stdout: Vec::new(),
        stderr: b"err".to_vec(),
    }
}

proptest_with_env! {
    /// pinned (program, args) returns the pinned response byte-for-byte.
    #[test]
    fn pin_then_run_returns_pinned(
        prog in "[a-z]{1,16}",
        args in proptest::collection::vec("[a-zA-Z0-9_-]{1,16}", 0..5),
        stdout in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        block_on(async {
            let runner = FakeCommandRunner::new();
            let response = ok_response(stdout.clone());
            runner.pin(&prog, args.clone(), response.clone()).await;
            let got = runner
                .run(&CommandRequest::new(&prog, args))
                .await
                .unwrap();
            assert_eq!(got, response);
        });
    }

    /// Non-pinned invocation returns empty success by default.
    #[test]
    fn non_pinned_default_returns_empty_success(prog in "[a-z]{1,16}") {
        block_on(async {
            let runner = FakeCommandRunner::new();
            let got = runner
                .run(&CommandRequest::new(&prog, vec![]))
                .await
                .unwrap();
            assert!(got.is_success());
            assert!(got.stdout.is_empty());
        });
    }

    /// set_default replaces the fallback for non-pinned invocations.
    #[test]
    fn set_default_replaces_fallback(
        prog in "[a-z]{1,16}",
        exit_code in 0i32..255,
    ) {
        block_on(async {
            let runner = FakeCommandRunner::new();
            let default_resp = err_response(exit_code);
            runner.set_default(default_resp.clone()).await;
            let got = runner
                .run(&CommandRequest::new(&prog, vec![]))
                .await
                .unwrap();
            assert_eq!(got, default_resp);
        });
    }

    /// fail_on_unknown makes non-pinned invocations error with Spawn.
    #[test]
    fn fail_on_unknown_errors_for_unpinned(prog in "[a-z]{1,16}") {
        block_on(async {
            let runner = FakeCommandRunner::new();
            runner.fail_on_unknown().await;
            let err = runner
                .run(&CommandRequest::new(&prog, vec![]))
                .await
                .unwrap_err();
            assert!(matches!(err, CommandError::Spawn(_)));
        });
    }

    /// Every invocation is recorded in order.
    #[test]
    fn invocations_recorded_in_order(
        progs in proptest::collection::vec("[a-z]{1,8}", 1..8),
    ) {
        block_on(async {
            let runner = FakeCommandRunner::new();
            for p in &progs {
                let _ = runner.run(&CommandRequest::new(p, vec![])).await;
            }
            let recorded = runner.invocations().await;
            assert_eq!(recorded.len(), progs.len());
            for (i, r) in recorded.iter().enumerate() {
                assert_eq!(r.program, progs[i]);
            }
        });
    }

    /// call_count matches invocations().len().
    #[test]
    fn call_count_matches_invocations_len(n in 0usize..16) {
        block_on(async {
            let runner = FakeCommandRunner::new();
            for _ in 0..n {
                let _ = runner.run(&CommandRequest::new("p", vec![])).await;
            }
            assert_eq!(runner.call_count().await, n);
            assert_eq!(runner.invocations().await.len(), n);
        });
    }

    /// Builder chain: with_stdin/with_env/with_working_dir compose.
    #[test]
    fn builder_chain_compounds(
        prog in "[a-z]{1,8}",
        stdin in proptest::collection::vec(any::<u8>(), 0..32),
        key in "[A-Z]{1,8}",
        val in "[a-zA-Z0-9]{1,8}",
    ) {
        let req = CommandRequest::new(&prog, vec![])
            .with_stdin(stdin.clone())
            .with_env(&key, &val);
        assert_eq!(req.program, prog);
        assert_eq!(req.stdin, Some(stdin));
        assert_eq!(req.env.get(&key), Some(&val));
    }

    /// is_success: only exit_code Some(0) is success.
    #[test]
    fn is_success_only_for_zero_exit(code in 1i32..255) {
        let r0 = CommandResponse {
            exit_code: Some(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        let r_nz = CommandResponse {
            exit_code: Some(code),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        let r_killed = CommandResponse {
            exit_code: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        assert!(r0.is_success());
        assert!(!r_nz.is_success());
        assert!(!r_killed.is_success());
    }
}
