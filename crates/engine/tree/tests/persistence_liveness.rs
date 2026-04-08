//! Tests validating issue #206: PersistenceService death is not detectable by the engine tree.
//!
//! ## The bug (three independent facts that combine into the problem)
//!
//! **Fact 1** — `spawn_service` (`persistence.rs` lines 416–443) spawns a thread but returns only
//! `PersistenceHandle`. There is no `watch::Receiver`, `JoinHandle`, or any other liveness signal
//! in the return value. The caller has no proactive way to know the thread has exited.
//!
//! **Fact 2** — When the service thread exits (DB error or panic), the `Receiver` it owns is
//! dropped. Every subsequent `Sender::send` on the handle returns `Err(SendError)`.
//!
//! **Fact 3** — All four call sites in `tree/mod.rs` that drive persistence discard that
//! `SendError` via `let _ = ...` rather than propagating or handling it:
//!   - line 1362: `let _ = self.persistence.remove_blocks_above(…)`
//!   - line 1384: `let _ = self.persistence.save_blocks(…)`
//!   - line 2794: `let _ = self.persistence.save_finalized_block_number(…)`
//!   - line 2822: `let _ = self.persistence.save_safe_block_number(…)`
//!
//! Together, these three facts mean the node silently stops persisting data with no warning
//! and no shutdown — it continues accepting blocks and updating in-memory state indefinitely.
//!
//! ## What these tests verify
//!
//! 1. `spawn_service` is called with real test infrastructure and the return type is exactly
//!    `PersistenceHandle<EthPrimitives>` — no liveness channel of any kind.
//!    (Test: [`test_spawn_service_return_type_has_no_liveness_signal`])
//!
//! 2. The return-type signature of `spawn_service` in source does not include any watch/channel
//!    liveness type.
//!    (Test: [`test_spawn_service_source_signature_has_no_liveness_channel`])
//!
//! 3. After the service thread exits (simulated by the same dead-channel state the thread leaves
//!    behind), `save_finalized_block_number` / `save_safe_block_number` / `save_blocks` /
//!    `remove_blocks_above` all return `Err(SendError)`.
//!    (Tests: `test_handle_*_errors_when_service_dead`)
//!
//! 4. Source code of `tree/mod.rs` contains `let _ = self.persistence.` at every one of the four
//!    call sites identified in the issue — confirming the error is structurally discarded.
//!    (Tests: `test_tree_caller_discards_*`)

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
fn spawn_test_persistence_service() -> PersistenceHandle<EthPrimitives> {
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
// Group 1: Structural — spawn_service provides no liveness signal
// ---------------------------------------------------------------------------

/// `spawn_service` returns exactly `PersistenceHandle<EthPrimitives>`.
///
/// The explicit type annotation is a compile-time assertion: if the function were
/// fixed to return a liveness signal alongside the handle (e.g., as a tuple
/// `(PersistenceHandle<…>, watch::Receiver<bool>)`), this test would fail to
/// compile, confirming the fix.
///
/// Currently this compiles and passes, confirming the bug: there is no liveness
/// channel in the return value of the actual `spawn_service` call.
#[test]
fn test_spawn_service_return_type_has_no_liveness_signal() {
    // EXERCISE spawn_service — the actual function identified in the bug report.
    let handle: PersistenceHandle<EthPrimitives> = spawn_test_persistence_service();

    // The handle is functional while the service is alive.
    let result = handle.save_finalized_block_number(0);
    assert!(
        result.is_ok(),
        "handle returned by spawn_service should succeed while the service thread is running"
    );

    // KEY ASSERTION (structural, via type system):
    // There is no second return value, no `.liveness_receiver()` method, no
    // `watch::Receiver`, no `JoinHandle`.  The only way the engine can detect
    // service death is reactively, by observing `SendError` on the next send —
    // and every caller in tree/mod.rs discards that error.
    drop(handle);
}

/// The source signature of `spawn_service` ends with `-> PersistenceHandle<N::Primitives>`,
/// not a tuple, and contains no `watch` or liveness type.
///
/// This test reads the actual source file so the assertion is tied to the real code,
/// not a hand-crafted mock. It will fail once the fix adds a liveness channel to
/// the return type.
#[test]
fn test_spawn_service_source_signature_has_no_liveness_channel() {
    let src = include_str!("../src/persistence.rs");

    // Find the spawn_service function signature block.
    let sig_start = src
        .find("pub fn spawn_service<N>")
        .expect("spawn_service must exist in persistence.rs");

    // Grab enough context to cover the full signature (up to the opening `{`).
    let sig_region = &src[sig_start..];
    let body_start = sig_region.find('{').expect("spawn_service must have a body");
    let signature = &sig_region[..body_start];

    // The return type must be a bare PersistenceHandle — not a tuple, not a watch channel.
    assert!(
        signature.contains("-> PersistenceHandle<N::Primitives>"),
        "spawn_service return type should be PersistenceHandle<N::Primitives>; \
         currently no liveness channel is returned:\n{signature}"
    );
    assert!(
        !signature.contains("watch::"),
        "spawn_service must not return a watch channel (liveness signal not yet added):\n\
         {signature}"
    );
    assert!(
        !signature.contains("JoinHandle"),
        "spawn_service must not return a JoinHandle (liveness signal not yet added):\n\
         {signature}"
    );
    assert!(
        !signature.contains('(') || signature.matches('(').count() == 1,
        "spawn_service return type must not be a tuple (no liveness channel in return):\n\
         {signature}"
    );
}

// ---------------------------------------------------------------------------
// Group 2: Behavioral — after service death, sends return errors
// ---------------------------------------------------------------------------

/// `save_finalized_block_number` returns `Err` once the service thread has exited.
///
/// This is what happens on the `handle` returned by `spawn_service` after the
/// thread from lines 433–440 of `persistence.rs` terminates: the receiver is dropped
/// and the very same `SendError` is returned — but `update_finalized_block` in
/// `tree/mod.rs:2794` discards it with `let _ = …`.
#[test]
fn test_handle_save_finalized_block_errors_when_service_dead() {
    let handle = dead_persistence_handle();

    let result = handle.save_finalized_block_number(100);

    assert!(
        result.is_err(),
        "save_finalized_block_number must return Err(SendError) when the service thread \
         has exited (the state left by spawn_service's thread on death); \
         tree/mod.rs:2794 discards this error with `let _ = …`"
    );
}

/// `save_safe_block_number` returns `Err` once the service thread has exited.
///
/// `update_safe_block` in `tree/mod.rs:2822` discards this error with `let _ = …`.
#[test]
fn test_handle_save_safe_block_errors_when_service_dead() {
    let handle = dead_persistence_handle();

    let result = handle.save_safe_block_number(100);

    assert!(
        result.is_err(),
        "save_safe_block_number must return Err(SendError) when the service thread \
         has exited; tree/mod.rs:2822 discards this error with `let _ = …`"
    );
}

/// `save_blocks` returns `Err` once the service thread has exited.
///
/// `persist_blocks` in `tree/mod.rs:1384` discards this error with `let _ = …`.
/// This is the most consequential failure: blocks are never written to disk, but
/// the engine updates its in-memory canonical chain as if they were.
#[test]
fn test_handle_save_blocks_errors_when_service_dead() {
    let handle = dead_persistence_handle();
    let (tx, _rx) = oneshot::channel();

    let result = handle.save_blocks(vec![], tx);

    assert!(
        result.is_err(),
        "save_blocks must return Err(SendError) when the service thread has exited; \
         tree/mod.rs:1384 discards this error with `let _ = …`, causing block data \
         to be silently lost"
    );
}

/// `remove_blocks_above` returns `Err` once the service thread has exited.
///
/// `remove_blocks` in `tree/mod.rs:1362` discards this error with `let _ = …`.
#[test]
fn test_handle_remove_blocks_above_errors_when_service_dead() {
    let handle = dead_persistence_handle();
    let (tx, _rx) = oneshot::channel();

    let result = handle.remove_blocks_above(50, tx);

    assert!(
        result.is_err(),
        "remove_blocks_above must return Err(SendError) when the service thread has exited; \
         tree/mod.rs:1362 discards this error with `let _ = …`"
    );
}

// ---------------------------------------------------------------------------
// Group 3: Source inspection — callers in tree/mod.rs discard the errors
// ---------------------------------------------------------------------------
//
// These tests read the actual tree/mod.rs source and assert that each of the
// four call sites identified in the issue uses `let _ = self.persistence.…`
// rather than propagating the error.  They pass when the bug is present and
// will fail once the callers are fixed — providing a precise regression guard.

/// `remove_blocks` at tree/mod.rs:1362 discards the `SendError` from
/// `remove_blocks_above` via `let _ = …`.
#[test]
fn test_tree_caller_discards_remove_blocks_above_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        src.contains("let _ = self.persistence.remove_blocks_above("),
        "tree/mod.rs must contain `let _ = self.persistence.remove_blocks_above(` \
         (the `remove_blocks` helper silently discards SendError — issue #206 bug site)"
    );
}

/// `persist_blocks` at tree/mod.rs:1384 discards the `SendError` from
/// `save_blocks` via `let _ = …`.
#[test]
fn test_tree_caller_discards_save_blocks_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        src.contains("let _ = self.persistence.save_blocks("),
        "tree/mod.rs must contain `let _ = self.persistence.save_blocks(` \
         (the `persist_blocks` helper silently discards SendError — issue #206 bug site)"
    );
}

/// `update_finalized_block` at tree/mod.rs:2794 discards the `SendError` from
/// `save_finalized_block_number` via `let _ = …`.
#[test]
fn test_tree_caller_discards_save_finalized_block_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        src.contains("let _ = self.persistence.save_finalized_block_number("),
        "tree/mod.rs must contain `let _ = self.persistence.save_finalized_block_number(` \
         (update_finalized_block silently discards SendError — issue #206 bug site)"
    );
}

/// `update_safe_block` at tree/mod.rs:2822 discards the `SendError` from
/// `save_safe_block_number` via `let _ = …`.
#[test]
fn test_tree_caller_discards_save_safe_block_error() {
    let src = include_str!("../src/tree/mod.rs");

    assert!(
        src.contains("let _ = self.persistence.save_safe_block_number("),
        "tree/mod.rs must contain `let _ = self.persistence.save_safe_block_number(` \
         (update_safe_block silently discards SendError — issue #206 bug site)"
    );
}

/// All four `let _ = self.persistence.` call sites are present simultaneously.
///
/// This consolidated assertion makes it easy to see the full scope of the issue:
/// every single persistence call made by the engine tree silently discards errors.
#[test]
fn test_all_four_tree_persistence_callers_discard_errors() {
    let src = include_str!("../src/tree/mod.rs");

    let sites = [
        "let _ = self.persistence.remove_blocks_above(",
        "let _ = self.persistence.save_blocks(",
        "let _ = self.persistence.save_finalized_block_number(",
        "let _ = self.persistence.save_safe_block_number(",
    ];

    for site in &sites {
        assert!(
            src.contains(site),
            "Expected `{site}` in tree/mod.rs — this is one of the four call sites \
             (issue #206) where SendError is silently discarded with `let _ = …`. \
             If this assertion fails, the corresponding caller has been fixed."
        );
    }
}
