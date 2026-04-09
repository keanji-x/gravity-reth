//! Tests for issue #226: unsafe transmute guarded by runtime assert; double take() causes panic;
//! poisoned mutex causes panic on re-entry.
//!
//! Validates three concrete bug scenarios in `pipe_run_inner`:
//!   1. `event_rx.lock().unwrap().take().unwrap()` panics on second call (double-call panic)
//!   2. `event_rx.lock().unwrap()` panics when the mutex is poisoned (no recovery path)
//!   3. The TypeId runtime assert is inconsistent: type aliases share TypeId (bypass), while
//!      layout-compatible newtypes have different TypeId (spurious panic rather than compile error)

use alloy_primitives::TxHash;
use reth_ethereum_primitives::EthPrimitives;
use reth_pipe_exec_layer_event_bus::{PipeExecLayerEvent, PipeExecLayerEventBus};
use std::{
    any::TypeId,
    panic::{self, AssertUnwindSafe},
    sync::{mpsc, Arc, Mutex},
};
use tokio::sync::mpsc::UnboundedReceiver;

// ---------------------------------------------------------------------------
// Helper: construct a minimal PipeExecLayerEventBus<EthPrimitives> for tests.
// We only need a real receiver for `event_rx`; `discard_txs` can be None.
// ---------------------------------------------------------------------------
fn make_bus(
    rx: mpsc::Receiver<PipeExecLayerEvent<EthPrimitives>>,
) -> PipeExecLayerEventBus<EthPrimitives> {
    PipeExecLayerEventBus {
        event_rx: Mutex::new(Some(rx)),
        discard_txs: tokio::sync::Mutex::new(None::<UnboundedReceiver<Vec<TxHash>>>),
    }
}

// ---------------------------------------------------------------------------
// Bug 1 – Double-call panic
// ---------------------------------------------------------------------------
/// The first call to `pipe_run_inner` removes the receiver via `.take()`.
/// A second call finds `None` and `.unwrap()` panics unconditionally.
///
/// Reproduces the code path:
///   `get_pipe_exec_layer_event_bus().event_rx.lock().unwrap().take().unwrap()`
#[test]
fn test_double_take_panics_on_second_call() {
    let (_tx, rx) = mpsc::channel::<PipeExecLayerEvent<EthPrimitives>>();
    let bus = make_bus(rx);

    // First call: succeeds — the receiver is moved out and the Option becomes None.
    let _receiver = bus.event_rx.lock().unwrap().take().unwrap();

    // Second call: `.take()` returns None; `.unwrap()` panics.
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.event_rx.lock().unwrap().take().unwrap();
    }));

    assert!(
        result.is_err(),
        "Expected a panic on the second take().unwrap() because Option is None after the first call, \
         but no panic occurred — the bug may have been fixed"
    );
}

// ---------------------------------------------------------------------------
// Bug 2 – Poisoned mutex panic
// ---------------------------------------------------------------------------
/// If any thread panics while holding the `event_rx` mutex guard, Rust marks the
/// mutex as poisoned. Every subsequent `lock().unwrap()` call panics with a
/// `PoisonError`, providing no recovery path.
///
/// Reproduces the code path:
///   `get_pipe_exec_layer_event_bus().event_rx.lock().unwrap()`
#[test]
fn test_poisoned_mutex_lock_panics() {
    let (_tx, rx) = mpsc::channel::<PipeExecLayerEvent<EthPrimitives>>();
    let bus = Arc::new(make_bus(rx));

    // Poison the mutex: spawn a thread that acquires the lock and then panics.
    let bus_clone = Arc::clone(&bus);
    let join_result = std::thread::spawn(move || {
        let _guard = bus_clone.event_rx.lock().unwrap();
        panic!("intentional panic to poison the event_rx mutex");
    })
    .join();

    // The thread must have panicked (that's the whole point).
    assert!(join_result.is_err(), "Spawned thread should have panicked to poison the mutex");

    // Confirm the mutex is indeed poisoned.
    assert!(
        bus.event_rx.is_poisoned(),
        "Mutex should be poisoned after the thread panicked while holding the guard"
    );

    // Now simulate the code in pipe_run_inner: lock().unwrap() panics on a poisoned mutex.
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        drop(bus.event_rx.lock().unwrap());
    }));

    assert!(
        result.is_err(),
        "Expected lock().unwrap() to panic on a poisoned mutex, but it succeeded — \
         the bug may have been fixed (e.g., by using lock().unwrap_or_else(|e| e.into_inner()))"
    );
}

// ---------------------------------------------------------------------------
// Bug 3 – TypeId check inconsistency (runtime assert vs compile-time bound)
// ---------------------------------------------------------------------------
/// The `assert_eq!(TypeId::of::<N>(), TypeId::of::<EthPrimitives>(), ...)` in
/// `pipe_run_inner` behaves inconsistently:
///
///   • Type *aliases* (`type MyPrimitives = EthPrimitives`) share the same TypeId
///     and pass the check silently — the alias is accepted.
///   • Newtypes (`struct Wrapper(EthPrimitives)`) may have an identical memory layout
///     but a *different* TypeId, causing an unexpected panic rather than a compile error.
///
/// Neither outcome is desirable: the check can be silently bypassed, or it can panic
/// for types that would actually be safe. A sealed compile-time trait bound is the fix.
#[test]
fn test_typeid_runtime_check_is_bypassable_via_alias() {
    // A type alias is transparent to TypeId — passes the assert silently.
    type MyPrimitives = EthPrimitives;
    assert_eq!(
        TypeId::of::<MyPrimitives>(),
        TypeId::of::<EthPrimitives>(),
        "Type alias should have the same TypeId — assert passes (silent bypass)"
    );
}

#[test]
fn test_typeid_runtime_check_panics_for_newtype_despite_potential_layout_compatibility() {
    // A newtype wrapper has a DIFFERENT TypeId even if its layout is identical.
    // In pipe_run_inner this means: the assert fires and panics rather than producing
    // a compile error, giving a worse developer experience.
    struct NewtypePrimitives(EthPrimitives);

    let newtype_typeid = TypeId::of::<NewtypePrimitives>();
    let eth_typeid = TypeId::of::<EthPrimitives>();

    assert_ne!(
        newtype_typeid, eth_typeid,
        "Newtype should have a different TypeId from EthPrimitives"
    );

    // Simulate what the assert_eq! in pipe_run_inner does when N = NewtypePrimitives.
    // The check panics instead of refusing to compile — a runtime error, not a type error.
    let assert_result = panic::catch_unwind(|| {
        assert_eq!(
            TypeId::of::<NewtypePrimitives>(),
            TypeId::of::<EthPrimitives>(),
            "pipe_run_inner requires N = EthPrimitives"
        );
    });

    assert!(
        assert_result.is_err(),
        "The runtime TypeId check should panic for a newtype wrapper — \
         demonstrating that the guard is a panic rather than a compile-time error"
    );
}

// ---------------------------------------------------------------------------
// Extra: verify that a well-structured fix (lock().unwrap_or_else) would not panic
// ---------------------------------------------------------------------------
/// Demonstrates that recovering from a poisoned mutex is possible with
/// `lock().unwrap_or_else(|e| e.into_inner())`. This shows the fix direction:
/// replace `.unwrap()` with explicit error handling.
#[test]
fn test_poisoned_mutex_recoverable_with_into_inner() {
    let (_tx, rx) = mpsc::channel::<PipeExecLayerEvent<EthPrimitives>>();
    let bus = Arc::new(make_bus(rx));

    let bus_clone = Arc::clone(&bus);
    let _ = std::thread::spawn(move || {
        let _guard = bus_clone.event_rx.lock().unwrap();
        panic!("poison the mutex");
    })
    .join();

    assert!(bus.event_rx.is_poisoned());

    // With proper error handling the mutex is accessible even when poisoned.
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = bus.event_rx.lock().unwrap_or_else(|e| e.into_inner());
        // Successfully accessed the poisoned mutex's inner value
    }));

    assert!(
        result.is_ok(),
        "lock().unwrap_or_else(|e| e.into_inner()) should NOT panic on a poisoned mutex"
    );
}
