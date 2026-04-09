//! Regression tests for issue #225.
//!
//! ## Bug
//!
//! In the `pipe_run_inner` execution path (the default when
//! `disable_pipe_execution = false`), `make_executed_block_canonical` calls
//! `make_canonical` and panics on any `ProviderError` via
//! `unwrap_or_else(|err| panic!(...))` at `tree/mod.rs:545-549`.
//!
//! This terminates the engine OS thread. The tokio-side
//! `EngineApiRequestHandler` continues accepting CL requests and forwards them
//! via `to_tree.send()`, all of which silently fail (`let _ = …` at
//! `engine.rs:200`), making the node appear alive to the CL while actually
//! processing nothing.
//!
//! ## Tests
//!
//! 1. `test_issue_225_make_executed_block_canonical_panics_on_provider_error`
//!    — directly exercises the `unwrap_or_else(|err| panic!(...))` site by
//!    constructing a reorg scenario where `canonical_block_by_hash` propagates
//!    `ProviderError::HeaderNotFound`.
//!
//! 2. `test_issue_225_engine_handler_silently_discards_after_tree_death`
//!    — demonstrates that `EngineApiRequestHandler::on_event` returns `()`
//!    with no error indication even when the tree receiver has been dropped.

use super::*;
use crate::engine::{EngineApiEvent, EngineApiRequestHandler, EngineRequestHandler, FromEngine};
use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use reth_chain_state::test_utils::TestBlockBuilder;
use reth_chainspec::MAINNET;
use reth_engine_primitives::{EngineApiValidator, NoopInvalidBlockHook, PayloadValidator};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm_ethereum::MockEvmConfig;
use reth_payload_primitives::NewPayloadError;
use reth_primitives_traits::{Block as _, RecoveredBlock};
use reth_provider::test_utils::MockEthProvider;
use std::{panic, sync::Arc};
use tokio::sync::mpsc::unbounded_channel;

// ---------------------------------------------------------------------------
// Minimal engine validator used only in this test module.
// (Can't re-use the private `MockEngineValidator` from `tests.rs`.)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct IssueValidator;

impl PayloadValidator<EthEngineTypes> for IssueValidator {
    type Block = Block;

    fn ensure_well_formed_payload(
        &self,
        payload: alloy_rpc_types_engine::ExecutionData,
    ) -> Result<RecoveredBlock<Self::Block>, NewPayloadError> {
        let block = reth_ethereum_primitives::Block::try_from(payload.payload)
            .map_err(|e| NewPayloadError::Other(format!("{e:?}").into()))?;
        let sealed = block.seal_slow();
        sealed.try_recover().map_err(|e| NewPayloadError::Other(e.into()))
    }
}

impl EngineApiValidator<EthEngineTypes> for IssueValidator {
    fn validate_version_specific_fields(
        &self,
        _version: reth_payload_primitives::EngineApiMessageVersion,
        _payload_or_attrs: reth_payload_primitives::PayloadOrAttributes<
            '_,
            alloy_rpc_types_engine::ExecutionData,
            alloy_rpc_types_engine::PayloadAttributes,
        >,
    ) -> Result<(), reth_payload_primitives::EngineObjectValidationError> {
        Ok(())
    }

    fn ensure_well_formed_attributes(
        &self,
        _version: reth_payload_primitives::EngineApiMessageVersion,
        _attributes: &alloy_rpc_types_engine::PayloadAttributes,
    ) -> Result<(), reth_payload_primitives::EngineObjectValidationError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper: build a bare EngineApiTreeHandler using MockEthProvider.
// ---------------------------------------------------------------------------

type TestTree = EngineApiTreeHandler<
    EthPrimitives,
    MockEthProvider,
    EthEngineTypes,
    BasicEngineValidator<MockEthProvider, MockEvmConfig, IssueValidator>,
    MockEvmConfig,
>;

fn build_tree() -> TestTree {
    let chain_spec = MAINNET.clone();
    let (action_tx, _action_rx) = std::sync::mpsc::channel();
    let persistence_handle = PersistenceHandle::new(action_tx);
    let consensus = Arc::new(EthBeaconConsensus::new(chain_spec.clone()));
    let provider = MockEthProvider::default();
    let (from_tree_tx, _from_tree_rx) = unbounded_channel();
    let header = chain_spec.genesis_header().clone();
    let header = SealedHeader::seal_slow(header);
    let engine_api_tree_state =
        EngineApiTreeState::new(10, 10, header.num_hash(), EngineApiKind::Ethereum);
    let canonical_in_memory_state = CanonicalInMemoryState::with_head(header, None, None);
    let (to_payload_service, _payload_command_rx) = unbounded_channel();
    let payload_builder = PayloadBuilderHandle::new(to_payload_service);
    let evm_config = MockEvmConfig::default();
    let engine_validator = BasicEngineValidator::new(
        provider.clone(),
        consensus.clone(),
        evm_config.clone(),
        IssueValidator,
        TreeConfig::default(),
        Box::new(NoopInvalidBlockHook::default()),
    );

    EngineApiTreeHandler::new(
        provider,
        consensus,
        engine_validator,
        from_tree_tx,
        engine_api_tree_state,
        canonical_in_memory_state,
        persistence_handle,
        PersistenceState::default(),
        payload_builder,
        TreeConfig::default().with_legacy_state_root(false).with_has_enough_parallelism(true),
        EngineApiKind::Ethereum,
        evm_config,
    )
}

// ---------------------------------------------------------------------------
// Test 1 — panic in make_executed_block_canonical on ProviderError
// ---------------------------------------------------------------------------

/// Issue #225: `make_executed_block_canonical` panics instead of propagating
/// `ProviderError`.
///
/// **Setup** — a reorg where the old canonical head is NOT in memory and
/// `MockEthProvider::sealed_block_with_senders` always returns `Ok(None)`:
///
/// ```
/// old canonical chain:  genesis → block_1a   (hash_1a in current_canonical_head)
/// fork chain:           genesis → block_1b   (hash_1b, parent ≠ hash_1a)
/// ```
///
/// When `make_executed_block_canonical(block_1b)` runs, `on_new_head`
/// detects a reorg and calls `canonical_block_by_hash(hash_1a)`.  Because
/// `hash_1a` is not in `blocks_by_hash` and the mock provider returns
/// `Ok(None)`, `canonical_block_by_hash` converts this to
/// `Err(ProviderError::HeaderNotFound)`.  `make_canonical` propagates the
/// error, and the bug causes `make_executed_block_canonical` to **panic**
/// instead of returning gracefully.
///
/// **Expected (fixed)**: no panic; the function should return an error or
/// handle it gracefully.
///
/// **Actual (buggy)**: panics with "Failed to make canonical, …".
#[test]
fn test_issue_225_make_executed_block_canonical_panics_on_provider_error() {
    let mut block_builder = TestBlockBuilder::eth().with_chain_spec((**MAINNET).clone());

    // Build block_1a to act as the established (in-memory) canonical head.
    let block_1a = block_builder.get_executed_blocks(1..2).next().unwrap();
    let hash_1a = block_1a.recovered_block.hash();

    let mut tree = build_tree();

    // Point current_canonical_head at block_1a but do NOT add block_1a to
    // blocks_by_hash.  This simulates the block having been persisted and
    // evicted from the in-memory cache — a normal production scenario.
    tree.state.tree_state.current_canonical_head = BlockNumHash { number: 1, hash: hash_1a };
    assert!(
        tree.state.tree_state.blocks_by_hash.is_empty(),
        "blocks_by_hash must be empty so canonical_block_by_hash falls through to the provider"
    );

    // Create a fork block at the same height (number = 1) with a fixed
    // parent hash that is guaranteed to differ from hash_1a, so that
    // on_new_head takes the reorg branch.
    let fork_parent = B256::from([0xABu8; 32]);
    let fork_block = block_builder.get_executed_block_with_number(1, fork_parent);
    let fork_hash = fork_block.recovered_block.hash();

    // Sanity checks: the fork block must NOT be a simple extension of block_1a.
    assert_ne!(fork_hash, hash_1a, "fork block hash must differ from canonical head hash");
    assert_ne!(
        fork_block.recovered_block.parent_hash(),
        hash_1a,
        "fork block parent must not equal canonical head hash; \
         otherwise on_new_head treats it as a chain extension and no reorg occurs"
    );

    // When make_executed_block_canonical runs:
    //   1. insert_executed(fork_block) — adds fork_block to blocks_by_hash.
    //   2. make_canonical(fork_hash) → on_new_head detects reorg and calls
    //      canonical_block_by_hash(hash_1a).
    //   3. canonical_block_by_hash: hash_1a not in blocks_by_hash → falls
    //      through to MockEthProvider::sealed_block_with_senders → Ok(None)
    //      → .ok_or_else(|| ProviderError::HeaderNotFound(…))? → Err.
    //   4. make_canonical returns Err.
    //   5. BUG: unwrap_or_else(|err| panic!(…)) fires.
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        tree.make_executed_block_canonical(fork_block);
    }));

    // The assertion CONFIRMS the bug is present: the call panicked.
    // Once the bug is fixed, result will be Ok(()) and this assertion must
    // be inverted (or the test rewritten to check graceful error handling).
    assert!(
        result.is_err(),
        "Bug confirmed (issue #225): make_executed_block_canonical panicked on ProviderError. \
         The fix should remove the panic! and propagate the error gracefully."
    );

    // Optionally verify the panic message contains the expected text.
    if let Err(payload) = result {
        let msg = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("<non-string panic payload>");
        assert!(
            msg.contains("Failed to make canonical"),
            "Panic message should contain 'Failed to make canonical', got: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 2 — EngineApiRequestHandler::on_event silently discards after tree death
// ---------------------------------------------------------------------------

/// Issue #225 (secondary): `EngineApiRequestHandler::on_event` silently
/// discards `SendError` when the tree thread has died.
///
/// The bug at `engine.rs:200`:
/// ```rust
/// let _ = self.to_tree.send(event);   // error silently discarded
/// ```
///
/// After the tree thread panics (see Test 1), the `Sender`'s paired
/// `Receiver` is dropped.  Every subsequent call to `on_event` returns `()`
/// with no indication of failure, so the node appears alive to the CL while
/// processing nothing.
///
/// **Expected (fixed)**: `on_event` should detect the dead channel and surface
/// a fatal-error signal (e.g. return `Result`, set an atomic flag, or panic
/// with a clear message so the process can restart).
///
/// **Actual (buggy)**: returns `()` silently; the caller cannot distinguish
/// "message delivered" from "tree thread is dead".
#[test]
fn test_issue_225_engine_handler_silently_discards_after_tree_death() {
    use crate::chain::FromOrchestrator;

    let (to_tree_tx, to_tree_rx) =
        std::sync::mpsc::channel::<FromEngine<EngineApiRequest<EthEngineTypes, EthPrimitives>, Block>>();
    let (_from_tree_tx, from_tree_rx) = unbounded_channel::<EngineApiEvent>();

    let mut handler = EngineApiRequestHandler::<
        EngineApiRequest<EthEngineTypes, EthPrimitives>,
        EthPrimitives,
    >::new(to_tree_tx.clone(), from_tree_rx);

    // Simulate tree thread death: drop the receiving end of the channel.
    drop(to_tree_rx);

    // Confirm the channel is genuinely dead: a direct send now returns an error.
    let probe_result = to_tree_tx.send(FromEngine::Event(FromOrchestrator::BackfillSyncStarted));
    assert!(
        probe_result.is_err(),
        "Pre-condition: channel must be dead before calling on_event"
    );

    // BUG: on_event uses `let _ = self.to_tree.send(event)`, so this call
    // returns () even though the message is irrecoverably lost.
    // A correct implementation would detect the SendError and signal a fatal
    // condition so that the engine loop can shut down rather than silently
    // accepting further CL requests.
    handler.on_event(FromEngine::Event(FromOrchestrator::BackfillSyncStarted));

    // The on_event call above did not panic or return an error — the silent
    // failure documented in issue #225.  There is no observable side-effect
    // to assert on from the caller's perspective, which is precisely the bug:
    // the caller cannot determine whether the send succeeded or failed.
    //
    // Once fixed, this test should be updated to assert on the error signal
    // exposed by the corrected implementation (e.g. an Err return value or
    // an observable flag).
}
