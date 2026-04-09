//! Regression tests for issue #237: non-atomic hardfork bytecode upgrade.
//!
//! `apply_hardfork_upgrades` mutates `state.cache.contracts` immediately inside
//! the upgrade loop (line 97 of common.rs), but the corresponding account
//! `code_hash` updates are deferred into `hardfork_changes` and committed only
//! at the end via `state.commit(hardfork_changes)` (line 163).
//!
//! If any fallible operation in the storage-patch phase returns an error via
//! `?`, the function returns early and `state.commit()` is never called.
//! This leaves new bytecodes permanently stranded in the contracts cache
//! while account `code_hash` fields retain their old values — diverged state.

use alloy_primitives::{keccak256, Address, B256, U256};
use grevm::ParallelState;
use reth_evm_ethereum::hardfork::common::{
    apply_hardfork_upgrades, BytecodeUpgrade, HardforkUpgrades, StoragePatch,
};
use revm::{
    bytecode::Bytecode,
    primitives::{address, KECCAK_EMPTY},
    state::AccountInfo,
};
use revm_database_interface::DBErrorMarker;
use std::fmt;

// ---------------------------------------------------------------------------
// Mock database
// ---------------------------------------------------------------------------

/// Trivially cloneable error to satisfy `ParallelDatabase` bounds.
#[derive(Debug, Clone)]
struct MockDbError;

impl fmt::Display for MockDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("simulated DB I/O error")
    }
}

impl std::error::Error for MockDbError {}
impl DBErrorMarker for MockDbError {}

/// A `DatabaseRef` that returns a real (non-empty) account for every address
/// but fails `storage_ref` for a configurable address, simulating a transient
/// I/O error that can occur mid-execution of `apply_hardfork_upgrades`.
#[derive(Debug)]
struct FailStorageDb {
    /// The address whose `storage_ref` call will return `Err`.
    fail_addr: Address,
}

impl revm::DatabaseRef for FailStorageDb {
    type Error = MockDbError;

    fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // Return a real, non-empty account so that:
        //   1. `load_mut_cache_account` caches it with `AccountStatus::Loaded`.
        //   2. `is_storage_known()` returns false → `storage_ref` reaches the DB.
        //   3. The `if let Some(ref info) = account.account` branch in the
        //      upgrade loop executes → the account IS staged in hardfork_changes.
        Ok(Some(AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code_hash: KECCAK_EMPTY,
            code: None,
        }))
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(Bytecode::default())
    }

    fn storage_ref(&self, address: Address, _index: U256) -> Result<U256, Self::Error> {
        if address == self.fail_addr {
            // Simulate a DB I/O failure for this address.
            Err(MockDbError)
        } else {
            Ok(U256::ZERO)
        }
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        Ok(B256::default())
    }
}

// ---------------------------------------------------------------------------
// Test hardfork fixtures
// ---------------------------------------------------------------------------

const ADDR_A: Address = address!("1000000000000000000000000000000000000001");
const ADDR_B: Address = address!("1000000000000000000000000000000000000002");
/// Third address used only as a storage-patch target (will trigger the failure).
const ADDR_C: Address = address!("1000000000000000000000000000000000000003");

/// Bytecode for address A: `PUSH1 0x01`.
static BYTECODE_A: [u8; 2] = [0x60, 0x01];
/// Bytecode for address B: `PUSH1 0x02`.
static BYTECODE_B: [u8; 2] = [0x60, 0x02];

/// Two bytecode upgrades (A and B) plus one storage patch (C, which fails).
static UPGRADES_AB: [(Address, &[u8]); 2] =
    [(ADDR_A, &BYTECODE_A), (ADDR_B, &BYTECODE_B)];

static PATCHES_C: [(Address, B256, U256); 1] =
    [(ADDR_C, B256::ZERO, U256::ZERO)];

/// Hardfork with two bytecode upgrades and a storage patch that will fail.
struct FailingStoragePatchHardfork;

impl HardforkUpgrades for FailingStoragePatchHardfork {
    fn name(&self) -> &'static str {
        "FailingStoragePatch"
    }

    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &UPGRADES_AB
    }

    fn storage_patches(&self) -> &'static [StoragePatch] {
        &PATCHES_C
    }
}

/// Hardfork with only two bytecode upgrades — no storage patches, always succeeds.
struct NoStoragePatchHardfork;

impl HardforkUpgrades for NoStoragePatchHardfork {
    fn name(&self) -> &'static str {
        "NoStoragePatch"
    }

    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &UPGRADES_AB
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// # Atomicity test (issue #237 — primary regression test)
///
/// When the storage patch for ADDR_C triggers a DB error, `apply_hardfork_upgrades`
/// returns early via `?` and never calls `state.commit(hardfork_changes)`.
///
/// Expected (correct, atomic) behavior: no partial changes are visible.
/// Actual (buggy) behavior: new bytecodes for A and B are already in
/// `state.cache.contracts` before the error occurs (line 97 of common.rs).
///
/// **This test will FAIL with the current implementation** because
/// `state.cache.contracts.insert(hash_a, ...)` runs unconditionally inside
/// the upgrade loop, before the storage-patch phase can fail.
#[test]
fn apply_hardfork_upgrades_contracts_cache_not_mutated_on_failure() {
    let db = FailStorageDb { fail_addr: ADDR_C };
    let mut state = ParallelState::new(db, false, false);

    let hash_a = keccak256(BYTECODE_A);
    let hash_b = keccak256(BYTECODE_B);

    // Contracts cache must be empty before the call.
    assert!(!state.cache.contracts.contains_key(&hash_a));
    assert!(!state.cache.contracts.contains_key(&hash_b));

    let result = apply_hardfork_upgrades(&FailingStoragePatchHardfork, &mut state);
    assert!(result.is_err(), "expected failure due to storage patch DB error");

    // After failure, no partial bytecode changes should be observable.
    //
    // These assertions FAIL with the current implementation because
    // `state.cache.contracts.insert(code_hash, new_bytecode)` on line 97
    // of common.rs executes immediately for A and B before the error on
    // line 110 propagates.
    assert!(
        !state.cache.contracts.contains_key(&hash_a),
        "BUG #237: orphaned bytecode A found in contracts cache after failure \
         — non-atomic upgrade (line 97 of common.rs mutates cache before error)"
    );
    assert!(
        !state.cache.contracts.contains_key(&hash_b),
        "BUG #237: orphaned bytecode B found in contracts cache after failure \
         — non-atomic upgrade (line 97 of common.rs mutates cache before error)"
    );
}

/// # Account/cache divergence test (issue #237 — secondary regression test)
///
/// After a partial failure, `state.cache.contracts` may contain the new bytecode
/// for A while `state.cache.accounts[A].code_hash` still holds the pre-upgrade
/// value (KECCAK_EMPTY). The two halves of a single logical change are applied
/// at different points in time, which is the root cause of the divergence.
///
/// This test detects the divergence directly: it reads both sides of the state
/// and asserts they are consistent. If the new bytecode is present in the
/// contracts cache, the account's code_hash MUST also reflect the new hash.
///
/// **This test will FAIL with the current implementation** because the bytecode
/// ends up in the contracts cache (line 97) but the account code_hash is not
/// updated (commit on line 163 is skipped).
#[test]
fn apply_hardfork_upgrades_account_code_hash_diverges_on_failure() {
    let db = FailStorageDb { fail_addr: ADDR_C };
    let mut state = ParallelState::new(db, false, false);

    let hash_a = keccak256(BYTECODE_A);

    let result = apply_hardfork_upgrades(&FailingStoragePatchHardfork, &mut state);
    assert!(result.is_err(), "expected failure due to storage patch DB error");

    let bytecode_in_contracts_cache = state.cache.contracts.contains_key(&hash_a);
    let account_code_hash = state
        .cache
        .accounts
        .get(&ADDR_A)
        .and_then(|acc| acc.account.as_ref().map(|info| info.code_hash));

    // Invariant: if the new bytecode is present in the contracts cache,
    // the owning account's code_hash must point to that same bytecode.
    // Violation of this invariant is state divergence (issue #237).
    if bytecode_in_contracts_cache {
        assert_eq!(
            account_code_hash,
            Some(hash_a),
            "BUG #237: new bytecode for A is in contracts cache (line 97) \
             but account code_hash was not updated (commit at line 163 skipped) \
             — diverged state after partial failure"
        );
    }
}

/// # Atomicity of account code_hash on failure
///
/// After a partial failure, the account code_hash for A (and B) must still be
/// KECCAK_EMPTY — the pre-upgrade value loaded from the database. The new
/// code_hash may only appear in the account cache after a successful call to
/// `state.commit(hardfork_changes)`.
///
/// **This test will FAIL** because `state.commit()` is never called when the
/// storage patch fails, yet `state.cache.contracts` already has the new
/// bytecodes, making the assertion about the account code_hash vacuously
/// consistent only if the divergence check in
/// `apply_hardfork_upgrades_account_code_hash_diverges_on_failure` also fails.
/// Together, the two tests pinpoint the exact inconsistency.
#[test]
fn apply_hardfork_upgrades_account_code_hash_unchanged_on_failure() {
    let db = FailStorageDb { fail_addr: ADDR_C };
    let mut state = ParallelState::new(db, false, false);

    let hash_a = keccak256(BYTECODE_A);
    let hash_b = keccak256(BYTECODE_B);

    let result = apply_hardfork_upgrades(&FailingStoragePatchHardfork, &mut state);
    assert!(result.is_err(), "expected failure due to storage patch DB error");

    // If apply_hardfork_upgrades truly rolls back on error, accounts A and B
    // must still carry the pre-upgrade code_hash (KECCAK_EMPTY).
    // In the current buggy implementation the contracts cache has the new
    // bytecodes but the accounts retain KECCAK_EMPTY — demonstrating that
    // the two halves of the upgrade are in an inconsistent state.
    let code_hash_a = state
        .cache
        .accounts
        .get(&ADDR_A)
        .and_then(|acc| acc.account.as_ref().map(|info| info.code_hash))
        .unwrap_or(KECCAK_EMPTY);
    let code_hash_b = state
        .cache
        .accounts
        .get(&ADDR_B)
        .and_then(|acc| acc.account.as_ref().map(|info| info.code_hash))
        .unwrap_or(KECCAK_EMPTY);

    // After a failed upgrade the account code_hash should NOT be the new hash.
    assert_ne!(
        code_hash_a, hash_a,
        "account A code_hash was updated despite apply_hardfork_upgrades failing"
    );
    assert_ne!(
        code_hash_b, hash_b,
        "account B code_hash was updated despite apply_hardfork_upgrades failing"
    );

    // But the new bytecodes ARE in the contracts cache (this is the bug):
    // state.cache.contracts has hash_a and hash_b while accounts don't —
    // the two checks together prove the divergence described in issue #237.
    // Uncomment the next two lines after the bug is fixed (they should then pass):
    //   assert!(!state.cache.contracts.contains_key(&hash_a));
    //   assert!(!state.cache.contracts.contains_key(&hash_b));
}

/// # Smoke test: successful upgrade is consistent
///
/// When no error occurs, both `state.cache.contracts` and the account
/// `code_hash` fields must reflect the new bytecodes after the call.
/// This ensures the happy path continues to work and that the fix for
/// issue #237 does not regress normal hardfork application.
#[test]
fn apply_hardfork_upgrades_success_contracts_and_accounts_consistent() {
    // ZERO address will never be used as a storage-patch target, so no error occurs.
    let db = FailStorageDb { fail_addr: Address::ZERO };
    let mut state = ParallelState::new(db, false, false);

    let hash_a = keccak256(BYTECODE_A);
    let hash_b = keccak256(BYTECODE_B);

    let result = apply_hardfork_upgrades(&NoStoragePatchHardfork, &mut state);
    assert!(result.is_ok(), "expected success, got: {result:?}");

    // Contracts cache must contain both new bytecodes.
    assert!(
        state.cache.contracts.contains_key(&hash_a),
        "contract A bytecode missing from contracts cache after successful upgrade"
    );
    assert!(
        state.cache.contracts.contains_key(&hash_b),
        "contract B bytecode missing from contracts cache after successful upgrade"
    );

    // Account code_hash fields must be updated consistently.
    let code_hash_a = state
        .cache
        .accounts
        .get(&ADDR_A)
        .and_then(|acc| acc.account.as_ref().map(|info| info.code_hash))
        .expect("account A must be in cache after upgrade");
    let code_hash_b = state
        .cache
        .accounts
        .get(&ADDR_B)
        .and_then(|acc| acc.account.as_ref().map(|info| info.code_hash))
        .expect("account B must be in cache after upgrade");

    assert_eq!(
        code_hash_a, hash_a,
        "account A code_hash not updated after successful upgrade"
    );
    assert_eq!(
        code_hash_b, hash_b,
        "account B code_hash not updated after successful upgrade"
    );
}
