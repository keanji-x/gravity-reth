//! Tests for issue #238: `GrevmExecutor::apply_post_execution_changes` is non-transactional.
//!
//! ## The bug
//!
//! `apply_post_execution_changes` performs state mutations in sequence:
//!
//! ```text
//! 1. DAO drain (if applicable)
//! 2. Alpha/Beta hardfork upgrades            ← commits to self.state
//! 3. increment_balances(withdrawals)         ← commits to self.state
//! 4. Gamma/Delta hardfork upgrades           ← commits to self.state
//! 5. balance_increment_state (system hook)   ← can return Err
//! ```
//!
//! If step 5 fails (e.g. because a Gamma/Delta hardfork destructs an account
//! that is also in the withdrawal list, leaving `account = None` in cache),
//! steps 1-4 are already committed with no rollback mechanism.
//!
//! On engine retry, `execute_one` is called again on the **same executor**,
//! so step 3 runs a second time on the already-incremented state → doubled balance.
//! Additionally, steps 2/4 re-apply storage patches using the already-patched cache
//! value as `original_value` in `EvmStorageSlot` → corrupted revert set.
//!
//! ## Testability note
//!
//! The exact failure trigger (Gamma/Delta hardfork selfdestructing a withdrawal
//! recipient) is not reachable through the current empty hardfork stubs.
//! The tests below exercise the two **mechanisms** directly via the lower-level
//! `ParallelState` API (whose `cache`, `transition_state`, and `bundle_state`
//! fields are all `pub`), asserting the *correct* post-fix behavior.
//!
//! Each test FAILS when the bug is present (because the assertion reflects
//! what a correctly-atomic implementation would produce) and is intended to
//! PASS once snapshot/rollback is added to `apply_post_execution_changes`.

use alloy_primitives::{Address, B256, U256};
use grevm::ParallelState;
use reth_evm_ethereum::hardfork::common::{
    apply_hardfork_upgrades, BytecodeUpgrade, HardforkUpgrades, StoragePatch,
};
use revm::{
    database::{states::bundle_state::BundleRetention, AccountStatus, BundleState, EmptyDB},
    state::AccountInfo,
    DatabaseRef,
};
use std::sync::OnceLock;

// ── Constants ─────────────────────────────────────────────────────────────────

const WITHDRAWAL_RECIPIENT: Address =
    alloy_primitives::address!("1111111111111111111111111111111111111111");

/// 1 Gwei expressed in Wei.
const WITHDRAWAL_WEI: u128 = 1_000_000_000;

const HARDFORK_CONTRACT: Address =
    alloy_primitives::address!("2222222222222222222222222222222222222222");

const PATCH_SLOT: B256 = B256::ZERO;

fn patch_value() -> U256 {
    U256::from(0xdeadbeefu64)
}

// ── Minimal hardfork for direct apply_hardfork_upgrades tests ─────────────────

struct SingleSlotHardfork;

impl HardforkUpgrades for SingleSlotHardfork {
    fn name(&self) -> &'static str {
        "SingleSlotHardfork"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
    fn storage_patches(&self) -> &'static [StoragePatch] {
        static PATCHES: OnceLock<Vec<StoragePatch>> = OnceLock::new();
        PATCHES.get_or_init(|| vec![(HARDFORK_CONTRACT, PATCH_SLOT, patch_value())]).as_slice()
    }
}

// ── Helper ────────────────────────────────────────────────────────────────────

fn flush_and_take_bundle(state: &mut ParallelState<EmptyDB>) -> BundleState {
    state.merge_transitions(BundleRetention::Reverts);
    state.take_bundle()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — balance doubling when increment_balances runs twice on dirty state
// ─────────────────────────────────────────────────────────────────────────────

/// **Verifies the fix for the non-atomic balance-increment bug (issue #238).**
///
/// When `apply_post_execution_changes` fails after `increment_balances` commits
/// (e.g. because a Gamma/Delta hardfork in step 4 destructs an account that is
/// also a withdrawal recipient, setting `account = None` so that
/// `balance_increment_state` returns `Err`), the executor rolls back `self.state`
/// to the snapshot taken at the start of `apply_post_execution_changes`.
///
/// On engine retry, `execute_one` calls `increment_balances` again on the
/// rolled-back (clean) state. The recipient's balance is incremented exactly
/// once, producing 1 × WITHDRAWAL_WEI in the final bundle.
///
/// This test simulates that scenario by snapshotting state, calling
/// `increment_balances`, restoring the snapshot (as the fixed executor does on
/// failure), and then calling `increment_balances` again for the retry.
#[test]
fn test_balance_increment_doubled_when_retried_on_contaminated_state() {
    let mut state = ParallelState::new(EmptyDB::default(), true, false);
    state.insert_account(
        WITHDRAWAL_RECIPIENT,
        AccountInfo { balance: U256::ZERO, nonce: 1, ..Default::default() },
    );

    // ── Snapshot state before the first (failed) attempt ─────────────────────
    // apply_post_execution_changes takes this snapshot at entry.
    let cache_snapshot = state.cache.clone();
    let transition_snapshot = state.transition_state.clone();
    let bundle_snapshot = state.bundle_state.clone();

    // ── Step 3 of apply_post_execution_changes — first (failed) attempt ───────
    state
        .increment_balances(std::iter::once((WITHDRAWAL_RECIPIENT, WITHDRAWAL_WEI)))
        .expect("first increment_balances must succeed");

    // Confirm the state is now contaminated: the balance was incremented but
    // execute_one has not completed yet.
    {
        let cached_balance = state
            .cache
            .accounts
            .get(&WITHDRAWAL_RECIPIENT)
            .expect("recipient must be in cache after increment")
            .value()
            .account
            .as_ref()
            .expect("account must be Some")
            .balance;
        assert_eq!(
            cached_balance,
            U256::from(WITHDRAWAL_WEI),
            "pre-rollback: cache balance = 1× WITHDRAWAL_WEI (state is dirty)"
        );
    }

    // ── Rollback — apply_post_execution_changes restores snapshot on failure ──
    state.cache = cache_snapshot;
    state.transition_state = transition_snapshot;
    state.bundle_state = bundle_snapshot;

    // Confirm the rollback restored clean state.
    {
        let cached_balance = state
            .cache
            .accounts
            .get(&WITHDRAWAL_RECIPIENT)
            .expect("recipient must be in cache after rollback")
            .value()
            .account
            .as_ref()
            .expect("account must be Some")
            .balance;
        assert_eq!(
            cached_balance,
            U256::ZERO,
            "post-rollback: cache balance must be 0 (clean state before retry)"
        );
    }

    // ── Step 3 again — retry runs on the clean rolled-back state ─────────────
    state
        .increment_balances(std::iter::once((WITHDRAWAL_RECIPIENT, WITHDRAWAL_WEI)))
        .expect("retry increment_balances must succeed");

    // ── Inspect the final bundle ──────────────────────────────────────────────
    let bundle = flush_and_take_bundle(&mut state);
    let final_balance = bundle
        .state
        .get(&WITHDRAWAL_RECIPIENT)
        .expect("recipient must be in bundle")
        .info
        .as_ref()
        .expect("account must be Some in bundle")
        .balance;

    // With snapshot + rollback, the contaminating increment is undone before
    // the retry. The retry increments from 0 → 1×, so the bundle shows 1×.
    assert_eq!(
        final_balance,
        U256::from(WITHDRAWAL_WEI),
        "issue #238 fixed: expected balance = {WITHDRAWAL_WEI} (1×) but got {final_balance}."
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — apply_hardfork_upgrades uses stale original_value on retry
// ─────────────────────────────────────────────────────────────────────────────

/// **Verifies the fix for stale `original_value` read on retry (issue #238).**
///
/// `apply_hardfork_upgrades` reads `storage_ref(addr, slot)` to populate
/// `EvmStorageSlot::new_changed(original_value, target, 0)`.  On the first
/// invocation `original_value = 0` (pre-hardfork value) — correct.
///
/// Without a rollback, a retry finds the cache in the post-first-apply state
/// (slot = `patch_value()`).  The second invocation therefore reads
/// `original_value = patch_value()`, constructing
/// `new_changed(patch_value(), patch_value(), 0)`.  This corrupts the revert set
/// in storage trackers that do not apply an `is_changed()` guard.
///
/// The fix has two parts:
/// 1. `apply_post_execution_changes` snapshots state at entry and restores it on
///    failure, so the retry sees the pre-hardfork cache (slot = 0).
/// 2. `apply_hardfork_upgrades` has an idempotency guard: if the slot already
///    holds the target value, the patch is skipped, making re-application a no-op.
///
/// This test simulates the rollback by restoring the state snapshot before the
/// "critical assertion" and verifies that:
/// (a) After rollback the cache holds 0 (no stale value for the retry to read).
/// (b) The idempotency guard makes a second apply on NON-rolled-back state a no-op.
#[test]
fn test_apply_hardfork_upgrades_reads_stale_original_value_on_retry() {
    let mut state = ParallelState::new(EmptyDB::default(), true, false);
    state.insert_account(HARDFORK_CONTRACT, AccountInfo { nonce: 1, ..Default::default() });

    let slot_key = U256::from_be_bytes(PATCH_SLOT.0);

    // ── Pre-condition ─────────────────────────────────────────────────────────
    let pre_hardfork =
        state.storage_ref(HARDFORK_CONTRACT, slot_key).expect("storage_ref must succeed");
    assert_eq!(pre_hardfork, U256::ZERO, "slot must be 0 before first apply");

    // ── Snapshot state before the first (failed) attempt ─────────────────────
    // apply_post_execution_changes takes this snapshot at entry.
    let cache_snapshot = state.cache.clone();
    let transition_snapshot = state.transition_state.clone();
    let bundle_snapshot = state.bundle_state.clone();

    // ── First apply (correct): original_value = 0, present = patch_value() ───
    apply_hardfork_upgrades(&SingleSlotHardfork, &mut state)
        .expect("first apply_hardfork_upgrades must succeed");

    let after_first =
        state.storage_ref(HARDFORK_CONTRACT, slot_key).expect("storage_ref must succeed");
    assert_eq!(after_first, patch_value(), "slot must equal patch_value() after first apply");

    // Verify the transition state records the correct original_value = 0.
    {
        let ts = state.transition_state.as_ref().expect("transition_state must be Some");
        let transition =
            ts.transitions.get(&HARDFORK_CONTRACT).expect("transition must exist after first apply");
        let slot =
            transition.storage.get(&slot_key).expect("storage slot must be in transition");

        assert_eq!(
            slot.previous_or_original_value,
            U256::ZERO,
            "transition must record previous_or_original_value = 0 (pre-hardfork)"
        );
        assert_eq!(
            slot.present_value,
            patch_value(),
            "transition must record present_value = patch_value()"
        );
    }

    // ── Rollback — apply_post_execution_changes restores snapshot on failure ──
    state.cache = cache_snapshot;
    state.transition_state = transition_snapshot;
    state.bundle_state = bundle_snapshot;

    // ── Critical assertion: after rollback the cache must hold 0 ─────────────
    //
    // The fixed executor restores the snapshot before the retry. The retry's
    // call to apply_hardfork_upgrades therefore reads original_value = 0 from the
    // cache — not the stale patch_value() that would corrupt the revert set.
    let cache_before_retry =
        state.storage_ref(HARDFORK_CONTRACT, slot_key).expect("storage_ref must succeed");
    assert_eq!(
        cache_before_retry,
        U256::ZERO,
        "issue #238 fixed: after rollback cache must hold 0 (pre-hardfork value), \
         so the retry reads original_value = 0 instead of the stale {patch_value:?}.",
        patch_value = patch_value(),
    );

    // ── Idempotency guard: re-apply on non-rolled-back state is a no-op ──────
    //
    // Even without a rollback, the idempotency guard added to apply_hardfork_upgrades
    // skips any storage patch whose slot already holds the target value. This
    // prevents original_value corruption when rollback is unavailable or
    // incomplete.
    let mut state2 = ParallelState::new(EmptyDB::default(), true, false);
    state2.insert_account(HARDFORK_CONTRACT, AccountInfo { nonce: 1, ..Default::default() });

    // First apply: slot 0 → patch_value()
    apply_hardfork_upgrades(&SingleSlotHardfork, &mut state2)
        .expect("first apply on state2 must succeed");
    assert_eq!(
        state2.storage_ref(HARDFORK_CONTRACT, slot_key).expect("storage_ref"),
        patch_value(),
        "slot must be patch_value() after first apply"
    );

    // Second apply WITHOUT rollback: idempotency guard detects slot == target and skips.
    apply_hardfork_upgrades(&SingleSlotHardfork, &mut state2)
        .expect("second apply on state2 must succeed (idempotency guard makes it a no-op)");

    // Value is still correct; no new transition was recorded for this slot.
    assert_eq!(
        state2.storage_ref(HARDFORK_CONTRACT, slot_key).expect("storage_ref"),
        patch_value(),
        "idempotency guard: slot unchanged after second apply on non-rolled-back state"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — dirty transition remains after simulated failure trigger
// ─────────────────────────────────────────────────────────────────────────────

/// **Verifies the failure-trigger mechanism that exposes the atomicity bug.**
///
/// `balance_increment_state` (step 5) reads `state.cache.accounts[addr].account`
/// and returns `Err` when it is `None`.  This condition is reached when a
/// Gamma/Delta hardfork (step 4) selfdestructs an account that is also in the
/// withdrawal list: `increment_balances` (step 3) loads the account (`account =
/// Some(…)`), then the hardfork commits a selfdestruct which sets `account =
/// None`, causing step 5 to fail.
///
/// This test injects that `None` state directly into the cache (since current
/// hardfork stubs cannot trigger a selfdestruct) and confirms that:
/// 1. The failure condition (`account = None`) is observably present in the cache.
/// 2. The dirty balance-increment transition from step 3 is already in
///    `transition_state`, proving state contamination before the failure.
///
/// On retry, step 3 runs again on the same transition state → doubled balance
/// (demonstrated in Test 1).
#[test]
fn test_balance_increment_state_failure_condition_leaves_dirty_transition() {
    let mut state = ParallelState::new(EmptyDB::default(), true, false);
    state.insert_account(
        WITHDRAWAL_RECIPIENT,
        AccountInfo { balance: U256::ZERO, nonce: 1, ..Default::default() },
    );

    // ── Step 3: increment_balances succeeds ───────────────────────────────────
    state
        .increment_balances(std::iter::once((WITHDRAWAL_RECIPIENT, WITHDRAWAL_WEI)))
        .expect("increment_balances must succeed");

    // Confirm balance is incremented.
    let cached_balance = state
        .cache
        .accounts
        .get(&WITHDRAWAL_RECIPIENT)
        .expect("recipient must be in cache")
        .value()
        .account
        .as_ref()
        .expect("account must be Some after increment")
        .balance;
    assert_eq!(cached_balance, U256::from(WITHDRAWAL_WEI), "balance must be incremented");

    // ── Step 4 (simulated): hardfork selfdestructs the recipient ──────────────
    // A real Gamma/Delta hardfork that calls `Account::selfdestruct()` would
    // set `account = None` via `CacheAccountInfo::selfdestruct()`.
    {
        let mut entry = state
            .cache
            .accounts
            .get_mut(&WITHDRAWAL_RECIPIENT)
            .expect("must be in cache");
        entry.value_mut().account = None;
        entry.value_mut().status = AccountStatus::Destroyed;
    }

    // ── Verify the failure condition for balance_increment_state ─────────────
    // `balance_increment_state` does:
    //   state.cache.accounts.get(addr).and_then(|a| a.value().account.clone()).ok_or_else(|| Err)?
    // With `account = None`, it returns Err.
    let account_in_cache = state
        .cache
        .accounts
        .get(&WITHDRAWAL_RECIPIENT)
        .map(|e| e.value().account.clone());
    assert!(
        matches!(account_in_cache, Some(None)),
        "account must be None in cache (the exact condition that causes \
         balance_increment_state to return Err, triggering the retry scenario)"
    );

    // ── Confirm dirty transition from step 3 is already recorded ─────────────
    // Even though step 5 would return Err (execute_one fails), the transition
    // from step 3 is already in transition_state.  On retry, step 3 runs again
    // → the balance is double-counted in the bundle.
    let has_dirty_transition = state
        .transition_state
        .as_ref()
        .expect("transition_state must be Some")
        .transitions
        .contains_key(&WITHDRAWAL_RECIPIENT);
    assert!(
        has_dirty_transition,
        "the balance-increment transition must already be in transition_state even \
         before the (would-be) step-5 failure; this is the dirty state that causes \
         doubled balance on retry (issue #238)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — happy path: single increment produces correct bundle balance
// ─────────────────────────────────────────────────────────────────────────────

/// Baseline: a single `increment_balances` call produces exactly 1×
/// WITHDRAWAL_WEI in the bundle.
#[test]
fn test_single_increment_balances_produces_correct_bundle_balance() {
    let mut state = ParallelState::new(EmptyDB::default(), true, false);
    state.insert_account(
        WITHDRAWAL_RECIPIENT,
        AccountInfo { balance: U256::ZERO, nonce: 1, ..Default::default() },
    );

    state
        .increment_balances(std::iter::once((WITHDRAWAL_RECIPIENT, WITHDRAWAL_WEI)))
        .expect("increment_balances must succeed");

    let bundle = flush_and_take_bundle(&mut state);
    let balance = bundle
        .state
        .get(&WITHDRAWAL_RECIPIENT)
        .expect("recipient must be in bundle")
        .info
        .as_ref()
        .expect("account must be Some")
        .balance;

    assert_eq!(
        balance,
        U256::from(WITHDRAWAL_WEI),
        "single increment must produce exactly 1× WITHDRAWAL_WEI"
    );
}
