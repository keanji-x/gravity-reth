/// Tests for issue #223: `PayloadStatus::Valid` returned to the CL before `MakeCanonical`
/// executes, creating an EL/CL state divergence when `MakeCanonical` subsequently fails.
///
/// # Scenario
///
/// ```text
/// CL → engine_newPayload(block_B)
///   engine:  on_new_payload(B)  → VALID + MakeCanonical event
///   engine:  tx.send(VALID)          ← CL records B as VALID  ← BUG: sent too early
///   engine:  make_canonical(B)  → ProviderError::HeaderNotFound  → engine thread exits
///   CL:      receives VALID for B  (B is never committed to canonical EL chain)
/// ```
///
/// The correct order is: make_canonical first, then (and only then) send VALID.
use super::*;
use crate::persistence::PersistenceAction;
use alloy_primitives::{Bytes, B256};
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{ExecutionData, ExecutionPayloadSidecar, ExecutionPayloadV1};
use reth_chain_state::{ExecutedBlock, ExecutedBlockWithTrieUpdates};
use reth_chainspec::HOLESKY;
use reth_engine_primitives::{EngineApiValidator, ForkchoiceStatus, NoopInvalidBlockHook};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm_ethereum::MockEvmConfig;
use reth_primitives_traits::Block as _;
use reth_provider::{test_utils::MockEthProvider, ExecutionOutcome};
use reth_trie::HashedPostState;
use std::{str::FromStr, sync::mpsc::channel};
use tokio::sync::oneshot;

/// Minimal mock validator — identical to the one in `tests.rs`, duplicated here so we do not
/// need to modify that file.
#[derive(Debug, Clone)]
struct MockEngineValidator;

impl reth_engine_primitives::PayloadValidator<EthEngineTypes> for MockEngineValidator {
    type Block = reth_ethereum_primitives::Block;

    fn ensure_well_formed_payload(
        &self,
        payload: ExecutionData,
    ) -> Result<
        reth_primitives_traits::RecoveredBlock<Self::Block>,
        reth_payload_primitives::NewPayloadError,
    > {
        let block =
            reth_ethereum_primitives::Block::try_from(payload.payload).map_err(|e| {
                reth_payload_primitives::NewPayloadError::Other(format!("{e:?}").into())
            })?;
        let sealed = block.seal_slow();
        sealed
            .try_recover()
            .map_err(|e| reth_payload_primitives::NewPayloadError::Other(e.into()))
    }
}

impl EngineApiValidator<EthEngineTypes> for MockEngineValidator {
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

/// Minimal test harness — only what is needed for NewPayload ordering tests.
struct OrderingTestHarness {
    tree: EngineApiTreeHandler<
        EthPrimitives,
        MockEthProvider,
        EthEngineTypes,
        BasicEngineValidator<MockEthProvider, MockEvmConfig, MockEngineValidator>,
        MockEvmConfig,
    >,
    _action_rx: std::sync::mpsc::Receiver<PersistenceAction>,
}

impl OrderingTestHarness {
    fn new() -> Self {
        let chain_spec = HOLESKY.clone();
        let (action_tx, action_rx) = channel();
        let persistence_handle = PersistenceHandle::new(action_tx);
        let consensus = Arc::new(EthBeaconConsensus::new(chain_spec.clone()));
        let provider = MockEthProvider::default();
        let payload_validator = MockEngineValidator;
        let (from_tree_tx, _from_tree_rx) = unbounded_channel();
        let header = chain_spec.genesis_header().clone();
        let header = SealedHeader::seal_slow(header);
        let engine_api_tree_state =
            EngineApiTreeState::new(10, 10, header.num_hash(), EngineApiKind::Ethereum);
        let canonical_in_memory_state =
            CanonicalInMemoryState::with_head(header, None, None);
        let (to_payload_service, _payload_command_rx) = unbounded_channel();
        let payload_builder = PayloadBuilderHandle::new(to_payload_service);
        let evm_config = MockEvmConfig::default();
        let engine_validator = BasicEngineValidator::new(
            provider.clone(),
            consensus.clone(),
            evm_config.clone(),
            payload_validator,
            TreeConfig::default(),
            Box::new(NoopInvalidBlockHook::default()),
        );
        let tree = EngineApiTreeHandler::new(
            provider,
            consensus,
            engine_validator,
            from_tree_tx,
            engine_api_tree_state,
            canonical_in_memory_state,
            persistence_handle,
            PersistenceState::default(),
            payload_builder,
            TreeConfig::default()
                .with_legacy_state_root(false)
                .with_has_enough_parallelism(true),
            EngineApiKind::Ethereum,
            evm_config,
        );
        Self { tree, _action_rx: action_rx }
    }
}

/// Returns the holesky block-1 data from the on-disk test fixture.
fn holesky_block_1() -> (reth_ethereum_primitives::Block, B256) {
    let s = include_str!("../../test-data/holesky/1.rlp");
    let data = Bytes::from_str(s).unwrap();
    let block: Block = Block::decode(&mut data.as_ref()).unwrap();
    let hash = block.clone().seal_slow().hash();
    (block, hash)
}

// ───────────────────────────────────────────────────────────────────────────
// Test 1 — Expected (correct) behaviour: VALID must not reach the CL when
//           make_canonical fails.  This test FAILS under the buggy code,
//           and will PASS once the ordering is fixed.
// ───────────────────────────────────────────────────────────────────────────

/// Asserts the **correct** invariant: if `make_canonical` fails with a `ProviderError`,
/// the CL must NOT have received `PayloadStatus::Valid`.
///
/// Under the current (buggy) implementation this test **fails** because `tx.send(VALID)`
/// is called at line 1528-1538 of `mod.rs` *before* `on_maybe_tree_event` at line 1541,
/// so the CL response channel already contains `VALID` by the time the engine returns
/// an error.
///
/// Fixing the bug (send VALID only after `make_canonical` succeeds) makes this test pass.
#[tokio::test]
async fn test_valid_not_sent_to_cl_when_make_canonical_fails() {
    let (block, b2_hash) = holesky_block_1();
    let sealed = block.clone().seal_slow();
    let payload_v1 =
        ExecutionPayloadV1::from_block_unchecked(b2_hash, &sealed.clone().into_block());

    let mut harness = OrderingTestHarness::new();

    // ── Step 1: Insert block B2 into the in-memory tree state.
    //
    // `sealed_header_by_hash(b2_hash)` will find it here, causing
    // `insert_block_or_payload` to return `AlreadySeen(BlockStatus::Valid)`.
    let b2_number = sealed.number;
    let recovered = sealed.clone().try_recover().expect("holesky block-1 must be recoverable");
    harness.tree.state.tree_state.insert_executed(ExecutedBlockWithTrieUpdates {
        block: ExecutedBlock {
            recovered_block: Arc::new(recovered),
            execution_output: Arc::new(ExecutionOutcome::default()),
            hashed_state: Arc::new(HashedPostState::default()),
        },
        trie: ExecutedTrieUpdates::empty(),
        triev2: Default::default(),
    });

    // ── Step 2: Set canonical head to a *different* block B1 at the same height.
    //
    // B1 is not in the in-memory tree state and not in the mock provider
    // (`MockEthProvider::sealed_block_with_senders` always returns `Ok(None)`).
    //
    // Consequence: when `make_canonical(B2)` calls `on_new_head(B2)`, it detects a
    // reorg (canonical = B1, new head = B2), then calls
    // `canonical_block_by_hash(B1)` which falls through to the provider, gets
    // `Ok(None)`, and converts it to `Err(ProviderError::HeaderNotFound(B1))`.
    let b1_hash = B256::random(); // unknown to tree-state and provider
    harness
        .tree
        .state
        .tree_state
        .set_canonical_head(BlockNumHash { number: b2_number, hash: b1_hash });

    // ── Step 3: Register B2 as the current sync target head.
    //
    // `is_sync_target_head(B2)` checks `forkchoice_state_tracker.sync_target_state()`.
    // Setting it here mirrors what would happen after a prior
    // `engine_forkchoiceUpdated` that returned SYNCING for B2.
    harness.tree.state.forkchoice_state_tracker.set_latest(
        alloy_rpc_types_engine::ForkchoiceState {
            head_block_hash: b2_hash,
            safe_block_hash: b2_hash,
            finalized_block_hash: B256::ZERO,
        },
        ForkchoiceStatus::Syncing,
    );

    // ── Step 4: Deliver the NewPayload message.
    let (tx, mut rx) = oneshot::channel();
    let engine_result =
        harness.tree.on_engine_message(FromEngine::Request(
            BeaconEngineMessage::NewPayload {
                payload: ExecutionData {
                    payload: payload_v1.into(),
                    sidecar: ExecutionPayloadSidecar::none(),
                },
                tx,
            }
            .into(),
        ));

    // ── Assertion A: the engine must have returned an error.
    //
    // `on_maybe_tree_event` propagates the `ProviderError` from `make_canonical`
    // via `?`, which is converted to `InsertBlockFatalError::Provider(...)`.
    assert!(
        engine_result.is_err(),
        "Expected on_engine_message to return Err (make_canonical ProviderError), got Ok. \
         This means make_canonical did not fail as expected — check test setup."
    );

    // ── Assertion B (the invariant that should hold after fixing the bug):
    //    the CL response channel must NOT contain VALID when make_canonical failed.
    //
    // Under the buggy implementation `tx.send(VALID)` is called *before*
    // `on_maybe_tree_event`, so `rx.try_recv()` succeeds here — which is WRONG.
    // A correct implementation would either:
    //   (a) send the response only after make_canonical completes, or
    //   (b) downgrade the status to SYNCING/INVALID before sending.
    //
    // This assertion FAILS under the current code, demonstrating issue #223.
    let cl_received_valid = rx
        .try_recv()
        .ok()
        .and_then(|r| r.ok())
        .map(|s| s.is_valid())
        .unwrap_or(false);

    assert!(
        !cl_received_valid,
        "BUG (issue #223): CL received PayloadStatus::Valid for block {b2_hash} even though \
         make_canonical subsequently failed with a ProviderError. \
         The VALID response was sent at mod.rs:1528-1538 BEFORE on_maybe_tree_event at line 1541. \
         Fix: defer tx.send until after on_maybe_tree_event completes successfully."
    );

    // ── Assertion C: canonical chain must remain at B1 (make_canonical did not commit).
    assert_eq!(
        harness.tree.state.tree_state.canonical_block_hash(),
        b1_hash,
        "Canonical head must not have changed because make_canonical failed"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Test 2 — Documents the current (buggy) ordering to prevent regression once fixed.
//
// This test characterises what actually happens today so that:
//   (a) it can be run immediately to confirm the bug is reproducible, and
//   (b) when the fix lands, this test is deleted or inverted.
// ───────────────────────────────────────────────────────────────────────────

/// Reproduces the exact sequence described in issue #223:
///
/// 1. `tx.send(VALID)` is executed (CL gets VALID).
/// 2. `on_maybe_tree_event` fails with `ProviderError` and the engine returns `Err`.
/// 3. The canonical chain is *not* updated.
///
/// This test passes under the buggy implementation and is intended to be deleted
/// (or the assertions flipped) once the ordering is corrected.
#[tokio::test]
async fn test_current_buggy_ordering_valid_sent_before_make_canonical() {
    let (block, b2_hash) = holesky_block_1();
    let sealed = block.clone().seal_slow();
    let payload_v1 =
        ExecutionPayloadV1::from_block_unchecked(b2_hash, &sealed.clone().into_block());

    let mut harness = OrderingTestHarness::new();

    // Insert B2 into tree state.
    let b2_number = sealed.number;
    let recovered = sealed.clone().try_recover().expect("holesky block-1 must be recoverable");
    harness.tree.state.tree_state.insert_executed(ExecutedBlockWithTrieUpdates {
        block: ExecutedBlock {
            recovered_block: Arc::new(recovered),
            execution_output: Arc::new(ExecutionOutcome::default()),
            hashed_state: Arc::new(HashedPostState::default()),
        },
        trie: ExecutedTrieUpdates::empty(),
        triev2: Default::default(),
    });

    // Set canonical head to an unknown B1 (triggers HeaderNotFound in make_canonical).
    let b1_hash = B256::random();
    harness
        .tree
        .state
        .tree_state
        .set_canonical_head(BlockNumHash { number: b2_number, hash: b1_hash });

    // Register B2 as sync target.
    harness.tree.state.forkchoice_state_tracker.set_latest(
        alloy_rpc_types_engine::ForkchoiceState {
            head_block_hash: b2_hash,
            safe_block_hash: b2_hash,
            finalized_block_hash: B256::ZERO,
        },
        ForkchoiceStatus::Syncing,
    );

    let (tx, mut rx) = oneshot::channel();
    let engine_result =
        harness.tree.on_engine_message(FromEngine::Request(
            BeaconEngineMessage::NewPayload {
                payload: ExecutionData {
                    payload: payload_v1.into(),
                    sidecar: ExecutionPayloadSidecar::none(),
                },
                tx,
            }
            .into(),
        ));

    // The engine returns Err because make_canonical hit ProviderError::HeaderNotFound.
    assert!(
        engine_result.is_err(),
        "Engine must return Err when make_canonical fails with HeaderNotFound"
    );

    // --- BUG MANIFESTATION ---
    // Despite the engine error, the CL oneshot already contains VALID because
    // tx.send() was called at mod.rs:1528-1538 BEFORE on_maybe_tree_event at line 1541.
    let cl_response = (&mut rx)
        .try_recv()
        .expect("VALID was already delivered to the CL before make_canonical ran (issue #223)");
    let status = cl_response.expect("response must be Ok");
    assert!(
        status.is_valid(),
        "The CL received VALID status even though make_canonical failed: {status:?}"
    );

    // The canonical chain was NOT updated — B1 is still the head.
    assert_eq!(
        harness.tree.state.tree_state.canonical_block_hash(),
        b1_hash,
        "Canonical head must still be B1 — make_canonical did not commit"
    );
}
