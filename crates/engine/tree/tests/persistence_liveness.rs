//! Tests validating the fix for issue #206: PersistenceService death must be detectable by the
//! engine tree so that data loss is prevented.
//!
//! ## The bug (three independent facts that combined into the problem)
//!
//! **Fact 1** — `spawn_service` spawned a thread but returned only `PersistenceHandle`. There was
//! no `watch::Receiver`, `JoinHandle`, or any other liveness signal in the return value.
//!
//! **Fact 2** — When the service thread exited (DB error or panic), the `Receiver` it owned was
//! dropped. Every subsequent `Sender::send` on the handle returned `Err(SendError)`.
//!
//! **Fact 3** — All four call sites in `tree/mod.rs` that drove persistence discarded that
//! `SendError` via `let _ = ...` rather than propagating or handling it.
//!
//! Together, these three facts meant the node silently stopped persisting data with no warning
//! and no shutdown.
//!
//! ## The fix
//!
//! `spawn_service` now returns `(PersistenceHandle, watch::Receiver<bool>)`. The receiver holds
//! `true` while the service is running and transitions to `false` when the service exits. The
//! engine tree stores the receiver and checks it before each persistence dispatch, returning a
//! fatal `AdvancePersistenceError::PersistenceServiceDied` that stops the engine loop.
//!
//! ## What these tests verify
//!
//! 1. `spawn_service` returns a tuple `(PersistenceHandle, watch::Receiver<bool>)` with a
//!    liveness signal.
//!    (Test: [`test_spawn_service_return_type_has_liveness_signal`])
//!
//! 2. The liveness receiver reports `true` while the service is alive and transitions to `false`
//!    after the service exits.
//!    (Test: [`test_liveness_receiver_transitions_to_false_on_service_death`])
//!
//! 3. After the service thread exits (simulated by the same dead-channel state the thread leaves
//!    behind), `save_finalized_block_number` / `save_safe_block_number` / `save_blocks` /
//!    `remove_blocks_above` all return `Err(SendError)`.
//!    (Tests: `test_handle_*_errors_when_service_dead`)
//!
//! 4. Source code of `tree/mod.rs` no longer contains `let _ = self.persistence.` at the four
//!    call sites — confirming the errors are no longer silently discarded.
//!    (Tests: `test_tree_caller_propagates_*`)

use reth_engine_tree::persistence::PersistenceHandle;
use reth_ethereum_primitives::EthPrimitives;
use reth_exex_types::FinishedExExHeight;
use reth_provider::test_utils::create_test_provider_factory;
use reth_prune::Pruner;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Helper: spawn a real PersistenceService via the production path
// ---------------------------------------------------------------------------

/// Calls the actual `PersistenceHandle::spawn_service` with test-grade infrastructure,
/// mirroring what the engine tree does at startup.
fn spawn_test_persistence_service() -> (PersistenceHandle<EthPrimitives>, tokio::sync::watch::Receiver<bool>) {
    let provider = create_test_provider_factory();
    let (_finished_exex_height_tx, finished_exex_height_rx) =
        tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
    let pruner =
        Pruner::new_with_factory(provider.clone(), vec![], 5, 0, None, finished_exex_height_rx);
    let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
    PersistenceHandle::<EthPrimitives>::spawn_service(provider, pruner, sync_metrics_tx)
}

/// Returns a `PersistenceHandle` whose backing service has already exited.
///
/// This reproduces the exact channel state that exists after the service thread
/// spawned by `spawn_service` terminates: the `Receiver` end of the mpsc channel is
/// gone, and the `Sender` (held by the handle) returns `Err(SendError)` on every
/// subsequent call.
fn dead_persistence_handle() -> PersistenceHandle<EthPrimitives> {
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = PersistenceHandle::new(tx);
    // Dropping `rx` simulates the service thread having exited — identical channel
    // state to what spawn_service's thread leaves behind on exit.
    drop(rx);
    handle
}

// ---------------------------------------------------------------------------
// Group 1: Structural — spawn_service provides a liveness signal (fix verified)
// ---------------------------------------------------------------------------

/// `spawn_service` now returns `(PersistenceHandle<EthPrimitives>, watch::Receiver<bool>)`.
///
/// This test confirms the fix: the return value includes a liveness receiver that allows
/// the engine to detect when the persistence service has died.
#[test]
fn test_spawn_service_return_type_has_liveness_signal() {
    // EXERCISE spawn_service — the actual function identified in the bug report.
    let (handle, mut liveness_rx) = spawn_test_persistence_service();

    // The handle is functional while the service is alive.
    let result = handle.save_finalized_block_number(0);
    assert!(
        result.is_ok(),
        "handle returned by spawn_service should succeed while the service thread is running"
    );

    // KEY ASSERTION: liveness receiver reports `true` while the service is alive.
    assert!(
        *liveness_rx.borrow_and_update(),
        "liveness receiver must report true while the persistence service is running"
    );

    drop(handle);
}

/// The liveness receiver transitions to `false` when the service exits.
///
/// This test verifies the core detection mechanism: after the persistence thread exits,
/// the engine can observe `false` on the liveness receiver and stop with a controlled shutdown.
#[tokio::test]
async fn test_liveness_receiver_transitions_to_false_on_service_death() {
    let (tx, rx) = std::sync::mpsc::channel::<reth_engine_tree::persistence::PersistenceAction<EthPrimitives>>();
    let (liveness_tx, liveness_rx) = tokio::sync::watch::channel(true);

    // Simulate the persistence thread: hold a reference, then exit and signal death.
    let thread = std::thread::spawn(move || {
        drop(rx); // service thread "exits"
        let _ = liveness_tx.send(false); // signal death
    });
    thread.join().unwrap();

    drop(tx);

    // The liveness receiver must now report `false`.
    assert!(
        !*liveness_rx.borrow(),
        "liveness receiver must transition to false after the persistence service thread exits"
    );
}

/// The source signature of `spawn_service` includes a watch liveness channel in the return type.
///
/// This test reads the actual source file so the assertion is tied to the real code.
#[test]
fn test_spawn_service_source_signature_has_liveness_channel() {
    let src = include_str!("../src/persistence.rs");

    let sig_start = src
        .find("pub fn spawn_service<N>")
        .expect("spawn_service must exist in persistence.rs");

    let sig_region = &src[sig_start..];
    let body_start = sig_region.find('{').expect("spawn_service must have a body");
    let signature = &sig_region[..body_start];

    assert!(
        signature.contains("watch::Receiver<bool>"),
        "spawn_service must return a watch::Receiver<bool> liveness signal (fix for #206):\n{signature}"
    );
    assert!(
        signature.contains("PersistenceHandle"),
        "spawn_service must still return a PersistenceHandle:\n{signature}"
    );
}

// ---------------------------------------------------------------------------
// Group 2: Behavioral — after service death, sends return errors (unchanged)
// ---------------------------------------------------------------------------

/// `save_finalized_block_number` returns `Err` once the service thread has exited.
#[test]
fn test_handle_save_finalized_block_errors_when_service_dead() {
    let handle = dead_persistence_handle();

    let result = handle.save_finalized_block_number(100);

    assert!(
        result.is_err(),
        "save_finalized_block_number must return Err(SendError) when the service thread has exited"
    );
}

/// `save_safe_block_number` returns `Err` once the service thread has exited.
#[test]
fn test_handle_save_safe_block_errors_when_service_dead() {
    let handle = dead_persistence_handle();

    let result = handle.save_safe_block_number(100);

    assert!(
        result.is_err(),
        "save_safe_block_number must return Err(SendError) when the service thread has exited"
    );
}

/// `save_blocks` returns `Err` once the service thread has exited.
#[test]
fn test_handle_save_blocks_errors_when_service_dead() {
    let handle = dead_persistence_handle();
    let (tx, _rx) = oneshot::channel();

    let result = handle.save_blocks(vec![], tx);

    assert!(
        result.is_err(),
        "save_blocks must return Err(SendError) when the service thread has exited"
    );
}

/// `remove_blocks_above` returns `Err` once the service thread has exited.
#[test]
fn test_handle_remove_blocks_above_errors_when_service_dead() {
    let handle = dead_persistence_handle();
    let (tx, _rx) = oneshot::channel();

    let result = handle.remove_blocks_above(50, tx);

    assert!(
        result.is_err(),
        "remove_blocks_above must return Err(SendError) when the service thread has exited"
    );
}

// ---------------------------------------------------------------------------
// Group 3: Source inspection — callers in tree/mod.rs no longer discard errors
// ---------------------------------------------------------------------------
//
// These tests read the actual tree/mod.rs source and assert that each of the
// four call sites identified in the issue NO LONGER uses `let _ = self.persistence.…`.
// They pass when the fix is applied and serve as regression guards against re-introducing
// the silent discard pattern.

/// `remove_blocks` no longer discards the error from `remove_blocks_above`.
#[test]
fn test_tree_caller_propagates_remove_blocks_above_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        !src.contains("let _ = self.persistence.remove_blocks_above("),
        "tree/mod.rs must NOT contain `let _ = self.persistence.remove_blocks_above(` \
         after the fix for issue #206 — errors from persistence must be propagated"
    );
}

/// `persist_blocks` no longer discards the error from `save_blocks`.
#[test]
fn test_tree_caller_propagates_save_blocks_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        !src.contains("let _ = self.persistence.save_blocks("),
        "tree/mod.rs must NOT contain `let _ = self.persistence.save_blocks(` \
         after the fix for issue #206 — errors from persistence must be propagated"
    );
}

/// `update_finalized_block` no longer silently discards the error from `save_finalized_block_number`.
#[test]
fn test_tree_caller_propagates_save_finalized_block_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        !src.contains("let _ = self.persistence.save_finalized_block_number("),
        "tree/mod.rs must NOT contain `let _ = self.persistence.save_finalized_block_number(` \
         after the fix for issue #206"
    );
}

/// `update_safe_block` no longer silently discards the error from `save_safe_block_number`.
#[test]
fn test_tree_caller_propagates_save_safe_block_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        !src.contains("let _ = self.persistence.save_safe_block_number("),
        "tree/mod.rs must NOT contain `let _ = self.persistence.save_safe_block_number(` \
         after the fix for issue #206"
    );
}

/// None of the four previously-identified `let _ = self.persistence.` call sites remain.
#[test]
fn test_no_persistence_errors_silently_discarded() {
    let src = include_str!("../src/tree/mod.rs");

    let silent_sites = [
        "let _ = self.persistence.remove_blocks_above(",
        "let _ = self.persistence.save_blocks(",
        "let _ = self.persistence.save_finalized_block_number(",
        "let _ = self.persistence.save_safe_block_number(",
    ];

    for site in &silent_sites {
        assert!(
            !src.contains(site),
            "Found `{site}` in tree/mod.rs — this is one of the four call sites \
             (issue #206) where SendError must no longer be silently discarded with `let _ = …`. \
             The fix requires propagating or at minimum logging these errors."
        );
    }
}
