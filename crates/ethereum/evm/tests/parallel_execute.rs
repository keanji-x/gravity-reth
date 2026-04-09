//! Tests for GrevmExecutor state-restoration bug (issue #236).
//!
//! # Bug
//! `GrevmExecutor::execute_transactions` calls `self.state.take()` before constructing the
//! `Scheduler`, leaving `self.state = None`. If `parallel_execute` returns `Err`, the early
//! return via `?` exits the method before `self.state = Some(state)` is reached.
//!
//! Every subsequent call (`execute_one`, `size_hint`, `take_bundle`, `transact_system_txn`)
//! then panics at the next `.unwrap()` on `self.state`.
//!
//! # How the tests trigger the failure
//! Grevm bypasses EVM nonce checks during parallel execution by setting `tx_env.nonce = None`.
//! In the commit phase it re-enables strict nonce validation. A transaction whose on-chain
//! nonce is greater than the account's current nonce causes
//! `StateAsyncCommit::commit` to record `NonceTooHigh`, which propagates out of
//! `parallel_execute` as `Err`.  This is a clean, deterministic path that does NOT panic the
//! async-commit thread (the EVM execution itself succeeds; only the commit-phase check fails).

use alloy_consensus::{Header, TxLegacy};
use alloy_primitives::{TxKind, U256};
use reth_chainspec::{ChainSpecBuilder, MAINNET};
use reth_ethereum_primitives::{Block, BlockBody, Transaction};
use reth_evm::parallel_execute::ParallelExecutor;
use reth_evm_ethereum::{parallel_execute::GrevmExecutor, EthEvmConfig};
use reth_primitives_traits::{crypto::secp256k1::public_key_to_address, Block as _};
use reth_testing_utils::generators::{self, sign_tx_with_key_pair};
use revm::{
    database::{CacheDB, EmptyDB},
    primitives::address,
    state::AccountInfo,
};
use std::sync::Arc;

/// Build a chain spec with Shanghai (but not Cancun/Prague) activated at genesis.
///
/// This gives a completely stable EVM environment:
/// - No EIP-4788 beacon-root pre-execution call (requires Cancun).
/// - No EIP-2935 block-hash history call (requires Prague).
/// - No EIP-6110 deposit receipts (requires Prague).
/// - London EIP-1559 is active, but `base_fee_per_gas: None` in the header is
///   handled by `unwrap_or_default()` → basefee = 0, which is compatible with
///   `gas_price = 0` in the test transaction.
fn shanghai_chain_spec() -> Arc<reth_chainspec::ChainSpec> {
    Arc::new(ChainSpecBuilder::from(&*MAINNET).shanghai_activated().build())
}

/// Build a `CacheDB<EmptyDB>` with a single funded sender account.
///
/// * `nonce` – the on-chain nonce to store for the sender.
/// * Returns `(db, sender_address, key_pair)`.
fn funded_sender_db(
    nonce: u64,
) -> (CacheDB<EmptyDB>, alloy_primitives::Address, secp256k1::Keypair) {
    let mut rng = generators::rng();
    let key_pair = generators::generate_key(&mut rng);
    let address = public_key_to_address(key_pair.public_key());

    let mut db = CacheDB::new(EmptyDB::default());
    db.insert_account_info(
        address,
        AccountInfo {
            nonce,
            // Enough balance to cover gas cost (gas_price = 0, so cost = 0 anyway).
            balance: U256::from(1_000_000_000_000_000_u128),
            ..Default::default()
        },
    );
    (db, address, key_pair)
}

/// Create a signed legacy transaction from `key_pair` with the given nonce.
///
/// gas_price = 0 is compatible with base_fee = 0 (header.base_fee_per_gas = None → 0).
fn signed_legacy_tx(
    key_pair: secp256k1::Keypair,
    chain_id: u64,
    nonce: u64,
) -> reth_ethereum_primitives::TransactionSigned {
    sign_tx_with_key_pair(
        key_pair,
        Transaction::Legacy(TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: TxKind::Call(address!("0000000000000000000000000000000000000001")),
            value: U256::ZERO,
            input: Default::default(),
        }),
    )
}

/// Construct a minimal recovered block containing a single transaction.
fn single_tx_block(
    tx: reth_ethereum_primitives::TransactionSigned,
) -> reth_primitives_traits::RecoveredBlock<Block> {
    Block {
        header: Header {
            number: 1,
            timestamp: 1,
            gas_limit: 1_000_000,
            // base_fee_per_gas = None → EVM basefee = 0; compatible with gas_price = 0.
            ..Header::default()
        },
        body: BlockBody {
            transactions: vec![tx],
            // Shanghai blocks may carry a withdrawals list; None is accepted by the executor.
            withdrawals: None,
            ..Default::default()
        },
    }
    .try_into_recovered()
    .expect("transaction signature must be valid")
}

// ---------------------------------------------------------------------------
// Test 1 – main bug: state is permanently None after parallel_execute error
// ---------------------------------------------------------------------------

/// Verify that `GrevmExecutor` keeps its `state` intact after a failed block execution.
///
/// ## Expected behaviour (after the fix)
/// 1. `execute_one` returns `Err` – execution failed because grevm's commit-phase nonce
///    check sees `tx.nonce(5) > account.nonce(0)` → `NonceTooHigh`.
/// 2. A second call to `size_hint()` must NOT panic – `self.state` must have been
///    restored before the error was propagated.
///
/// ## Current behaviour (bug present)
/// Step 2 panics with `called Option::unwrap() on a None value` because
/// `self.state` was never restored after the early `?` return in
/// `execute_transactions`.
#[test]
fn test_grevm_state_is_some_after_parallel_execute_error() {
    let chain_spec = shanghai_chain_spec();
    // Sender's on-chain nonce = 0.  Transaction nonce = 5 > 0 → NonceTooHigh at commit time.
    let (db, _sender, key_pair) = funded_sender_db(0);
    let tx = signed_legacy_tx(key_pair, chain_spec.chain.id(), 5);
    let block = single_tx_block(tx);

    let evm_config = EthEvmConfig::new(chain_spec.clone());
    let mut executor = GrevmExecutor::new(chain_spec, &evm_config, db);

    // First execution: grevm commit-phase nonce check must reject the transaction.
    let first_result = executor.execute_one(&block);
    assert!(
        first_result.is_err(),
        "execute_one should return Err when grevm rejects the transaction at commit time \
         (NonceTooHigh: tx.nonce=5, account.nonce=0)"
    );

    // After the error the executor must still be usable.
    // With the bug this panics: "called Option::unwrap() on a None value"
    // (self.state was taken by execute_transactions but never restored on the error path).
    let _hint = executor.size_hint();
    // Reaching here means self.state is Some — the bug is fixed.
}

// ---------------------------------------------------------------------------
// Test 2 – executor remains usable for a retry after the failed execution
// ---------------------------------------------------------------------------

/// Verify that `execute_one` can be called a second time after a first failure.
///
/// This directly exercises the use-case the issue describes: an engine that retries
/// a block after a failed execution attempt.  With the bug the retry panics; with the
/// fix it returns a new (possibly different) error or succeeds.
#[test]
fn test_grevm_executor_retry_after_parallel_execute_error() {
    let chain_spec = shanghai_chain_spec();
    // Sender's on-chain nonce = 0.  Transaction nonce = 5 > 0 → NonceTooHigh at commit time.
    let (db, _sender, key_pair) = funded_sender_db(0);
    let tx = signed_legacy_tx(key_pair, chain_spec.chain.id(), 5);
    let block = single_tx_block(tx);

    let evm_config = EthEvmConfig::new(chain_spec.clone());
    let mut executor = GrevmExecutor::new(chain_spec, &evm_config, db);

    // First attempt – expected to fail.
    let first = executor.execute_one(&block);
    assert!(first.is_err(), "first execute_one must fail (NonceTooHigh)");

    // Second attempt on the same block.
    // With the bug this panics because self.state is None.
    // After the fix it must return Err again (the nonce mismatch is still present)
    // without panicking.
    let second = executor.execute_one(&block);
    assert!(
        second.is_err(),
        "second execute_one must return Err (not panic) – the executor must be \
         re-usable after a previous failure"
    );
}

// ---------------------------------------------------------------------------
// Test 3 – take_bundle must not panic after a failed execution
// ---------------------------------------------------------------------------

/// Verify that `take_bundle` does not panic when called after a failed execution.
///
/// The engine pipeline calls `take_bundle` to extract the accumulated state diff.
/// With the bug, if a prior `execute_one` failed, `take_bundle` panics because
/// `self.state` is None.
#[test]
fn test_grevm_take_bundle_after_parallel_execute_error() {
    let chain_spec = shanghai_chain_spec();
    let (db, _sender, key_pair) = funded_sender_db(0);
    let tx = signed_legacy_tx(key_pair, chain_spec.chain.id(), 5);
    let block = single_tx_block(tx);

    let evm_config = EthEvmConfig::new(chain_spec.clone());
    let mut executor = GrevmExecutor::new(chain_spec, &evm_config, db);

    let result = executor.execute_one(&block);
    assert!(result.is_err(), "execute_one must fail (NonceTooHigh)");

    // With the bug this panics: "called Option::unwrap() on a None value"
    let _bundle = executor.take_bundle();
}
