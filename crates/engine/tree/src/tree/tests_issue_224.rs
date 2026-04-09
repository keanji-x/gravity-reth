//! Tests for Issue #224: FCU response never sent to CL when `on_maybe_tree_event` fails fatally.
//!
//! ## Bug Description
//!
//! In the `BeaconEngineMessage::ForkchoiceUpdated` branch of `on_engine_message`
//! (`mod.rs` around the FCU handler), the code has a critical ordering flaw:
//!
//! ```text
//! // (1) State mutated — set_latest runs
//! self.state.forkchoice_state_tracker.set_latest(state, res.outcome.forkchoice_status());
//!
//! // (2) Fatal exit possible here — prevents tx.send() below
//! self.on_maybe_tree_event(res.event.take())?;
//!
//! // (3) NEVER REACHED if step 2 returns Err (? propagated upward)
//! if let Err(err) = tx.send(...) { ... }
//! ```
//!
//! If `on_maybe_tree_event` returns `Err`, the `?` operator propagates the error out of
//! `on_engine_message`, skipping `tx.send()`. The CL's awaiting oneshot gets
//! `RecvError::Closed`. Internal state (`forkchoice_state_tracker`) has already been
//! mutated to show the FCU as valid/syncing, but the CL has no acknowledgment.
//!
//! ## Contrast with NewPayload (the correct pattern)
//!
//! The `NewPayload` handler correctly sends the response BEFORE processing tree events:
//! ```text
//! // (1) Response sent FIRST
//! if let Err(err) = tx.send(output.map(...)) { ... }
//!
//! // (2) Tree event processed AFTER — if this fails, CL already has its response
//! self.on_maybe_tree_event(maybe_event)?;
//! ```
//!
//! ## Test Strategy
//!
//! Four layers of validation:
//!
//! **Layer 1 — Code-ordering structural test** (`test_fcu_ordering_tx_send_must_precede_tree_event`):
//!   Reads the `mod.rs` source and asserts that `tx.send()` appears **before**
//!   `on_maybe_tree_event?` in the FCU branch.
//!   - **CURRENTLY FAILS** (bug present in current code)
//!   - Will PASS after the fix
//!
//! **Layer 2 — Component failure proof** (`test_on_tree_event_make_canonical_returns_provider_error`):
//!   Proves that `on_tree_event(MakeCanonical)` can fail with `ProviderError::HeaderNotFound`
//!   when the canonical chain walk hits a missing block. This confirms the failure mode is real.
//!
//! **Layer 3 — Bug symptom simulation** (`test_fcu_ordering_bug_simulation`):
//!   Manually injects a `MakeCanonical` event into the FCU result and runs the BUGGY
//!   ordering. Directly observes: tracker mutated + oneshot closed (CL hangs).
//!   Documents the exact symptom described in issue #224.
//!
//! **Layer 4 — Regression tests** (remaining tests):
//!   Validate correct current behavior for all reachable FCU paths.

use super::*;
use crate::persistence::PersistenceAction;
use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use alloy_rpc_types_engine::ForkchoiceState;
use reth_chain_state::test_utils::TestBlockBuilder;
use reth_chainspec::MAINNET;
use reth_engine_primitives::{BeaconEngineMessage, EngineApiValidator, NoopInvalidBlockHook};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm_ethereum::MockEvmConfig;
use reth_payload_primitives::EngineApiMessageVersion;
use reth_primitives_traits::Block as _;
use reth_provider::test_utils::MockEthProvider;
use std::sync::{mpsc::channel, Arc};
use tokio::sync::{mpsc::unbounded_channel, oneshot};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FcuTestValidator;

impl reth_engine_primitives::PayloadValidator<EthEngineTypes> for FcuTestValidator {
    type Block = Block;

    fn ensure_well_formed_payload(
        &self,
        payload: alloy_rpc_types_engine::ExecutionData,
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

impl EngineApiValidator<EthEngineTypes> for FcuTestValidator {
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

type FcuTestHandler = EngineApiTreeHandler<
    EthPrimitives,
    MockEthProvider,
    EthEngineTypes,
    BasicEngineValidator<MockEthProvider, MockEvmConfig, FcuTestValidator>,
    MockEvmConfig,
>;

fn build_fcu_test_handler() -> (
    FcuTestHandler,
    std::sync::mpsc::Receiver<PersistenceAction>,
    tokio::sync::mpsc::UnboundedReceiver<EngineApiEvent<EthPrimitives>>,
    MockEthProvider,
) {
    let chain_spec = MAINNET.clone();
    let (action_tx, action_rx) = channel();
    let persistence_handle = PersistenceHandle::new(action_tx);

    let consensus = Arc::new(EthBeaconConsensus::new(chain_spec.clone()));
    let provider = MockEthProvider::default();
    let (from_tree_tx, from_tree_rx) = unbounded_channel();

    let header = chain_spec.genesis_header().clone();
    let header = reth_primitives_traits::SealedHeader::seal_slow(header);
    let engine_api_tree_state =
        EngineApiTreeState::new(10, 10, header.num_hash(), EngineApiKind::Ethereum);
    let canonical_in_memory_state =
        reth_chain_state::CanonicalInMemoryState::with_head(header, None, None);

    let (to_payload_service, _payload_command_rx) = unbounded_channel();
    let payload_builder = reth_payload_builder::PayloadBuilderHandle::new(to_payload_service);

    let evm_config = MockEvmConfig::default();
    let engine_validator = BasicEngineValidator::new(
        provider.clone(),
        consensus.clone(),
        evm_config.clone(),
        FcuTestValidator,
        TreeConfig::default(),
        Box::new(NoopInvalidBlockHook::default()),
    );

    let tree = EngineApiTreeHandler::new(
        provider.clone(),
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

    (tree, action_rx, from_tree_rx, provider)
}

// ---------------------------------------------------------------------------
// Layer 1: Code-ordering structural test
// ---------------------------------------------------------------------------

/// **CURRENTLY FAILING TEST** — validates the fix for issue #224.
///
/// This test reads the source of `on_engine_message` (in `mod.rs`) and asserts that
/// `tx.send()` appears **before** `on_maybe_tree_event?` in the FCU branch.
///
/// ### Why this test currently fails
///
/// In the current (buggy) code, the FCU branch has this ordering:
/// ```text
/// self.state.forkchoice_state_tracker.set_latest(...);  // (1) state mutated
/// self.on_maybe_tree_event(res.event.take())?;           // (2) can exit early via ?
/// tx.send(output.map(...));                              // (3) NEVER REACHED if (2) fails
/// ```
///
/// `tx.send()` appears at a later byte-offset than `on_maybe_tree_event` in the FCU branch,
/// so the assertion `tx_send_pos < tree_event_pos` **fails**.
///
/// ### Why this test will pass after the fix
///
/// The fix moves `tx.send()` BEFORE `on_maybe_tree_event?` (mirroring the NewPayload pattern):
/// ```text
/// self.state.forkchoice_state_tracker.set_latest(...);  // (1) state mutated
/// tx.send(output.map(...));                             // (2) response sent first
/// self.on_maybe_tree_event(res.event.take())?;          // (3) safe to exit here
/// ```
///
/// After this reordering, `tx.send()` appears at a smaller byte-offset than
/// `on_maybe_tree_event` in the FCU branch, and the assertion passes.
#[test]
fn test_fcu_ordering_tx_send_must_precede_on_maybe_tree_event() {
    let source = include_str!("mod.rs");

    // Locate the FCU arm: starts after the ForkchoiceUpdated variant header
    let fcu_arm_start = source
        .find("BeaconEngineMessage::ForkchoiceUpdated {")
        .expect("FCU arm not found in mod.rs — has the code been restructured?");

    // The FCU arm ends where the NewPayload arm begins
    let fcu_arm_end = source[fcu_arm_start..]
        .find("BeaconEngineMessage::NewPayload")
        .map(|offset| fcu_arm_start + offset)
        .expect("NewPayload arm not found after FCU arm in mod.rs");

    let fcu_arm = &source[fcu_arm_start..fcu_arm_end];

    // Find the positions of the three critical operations within the FCU arm
    let set_latest_pos = fcu_arm
        .find("set_latest(")
        .expect("set_latest() not found in FCU arm — tracker mutation removed?");

    let on_maybe_tree_event_pos = fcu_arm
        .find("on_maybe_tree_event(")
        .expect("on_maybe_tree_event() not found in FCU arm — event handling removed?");

    // "tx.send(output" uniquely identifies the FCU tx.send (not the NewPayload one)
    let tx_send_pos = fcu_arm
        .find("tx.send(output")
        .expect("tx.send(output) not found in FCU arm — response sending removed?");

    // INVARIANT (the fix for issue #224):
    // tx.send() MUST appear BEFORE on_maybe_tree_event? in the FCU arm.
    //
    // If tx.send appears AFTER on_maybe_tree_event, a ProviderError from
    // on_maybe_tree_event propagates via ? and exits on_engine_message before
    // tx.send() is reached — the CL's oneshot is closed with no response.
    //
    // This assertion CURRENTLY FAILS (bug present):
    //   tx_send_pos (line ~1513) > on_maybe_tree_event_pos (line ~1510)
    //
    // This assertion PASSES after the fix:
    //   tx_send_pos < on_maybe_tree_event_pos
    assert!(
        tx_send_pos < on_maybe_tree_event_pos,
        "ISSUE #224 BUG DETECTED: In the FCU arm, tx.send(output) is at byte-offset {} \
         but on_maybe_tree_event() is at byte-offset {}. \
         tx.send() appears AFTER on_maybe_tree_event?, meaning a ProviderError from \
         on_maybe_tree_event would propagate via ? and exit on_engine_message before \
         tx.send() is reached. The CL's oneshot would be closed with no response while \
         forkchoice_state_tracker has already been mutated (set_latest at offset {}). \
         Fix: move tx.send() BEFORE on_maybe_tree_event? to mirror the NewPayload pattern.",
        tx_send_pos,
        on_maybe_tree_event_pos,
        set_latest_pos,
    );

    // Secondary invariant: set_latest must still run before tx.send
    // (both inside the if-let-Ok block)
    assert!(
        set_latest_pos < tx_send_pos,
        "set_latest() should appear before tx.send() — state must be updated before responding"
    );
}

// ---------------------------------------------------------------------------
// Layer 2: Component failure proof
// ---------------------------------------------------------------------------

/// Proves that `on_tree_event(MakeCanonical)` CAN return `ProviderError::HeaderNotFound`.
///
/// This test directly demonstrates that `on_tree_event` with a `MakeCanonical` event
/// fails when the canonical chain walk hits a block that is missing from both the
/// in-memory tree state and the provider.
///
/// ### Why this matters for issue #224
///
/// `on_maybe_tree_event` calls `on_tree_event`, which calls `make_canonical`, which calls
/// `on_new_head`, which walks the canonical chain calling `canonical_block_by_hash`.
/// When that call returns `Ok(None)` for a missing block, `on_new_head` returns
/// `Err(ProviderError::HeaderNotFound)`.
///
/// The `?` at `on_maybe_tree_event(res.event.take())?` in the FCU branch converts this
/// into an early exit from `on_engine_message`, skipping `tx.send()`.
///
/// ### Test setup
///
/// - A side-chain block at height 3 is inserted into the tree state (reachable by hash).
/// - The canonical head is set to a fake hash at height 5 that is NOT in memory or provider.
/// - When `MakeCanonical { sync_target_head: side_block }` is processed:
///   - `on_new_head` finds the side block (height 3)
///   - Canonical head is at height 5 > 3, so it tries to walk old chain first
///   - `canonical_block_by_hash(fake_canonical)` → not in tree state → falls to provider
///   - `MockEthProvider` returns `Ok(None)` → `Err(ProviderError::HeaderNotFound)`
#[test]
fn test_on_tree_event_make_canonical_returns_provider_error() {
    let (mut tree, _action_rx, _from_tree_rx, _provider) = build_fcu_test_handler();
    let mut block_builder = TestBlockBuilder::eth();

    // Insert a side-chain block at height 3 (reachable by hash lookup)
    let side_block = block_builder.get_executed_block_with_number(3, B256::random());
    let sync_target_head = side_block.recovered_block().hash();
    tree.state.tree_state.insert_executed(side_block);

    // Set canonical head to a hash at height 5 that is NOT in memory or provider.
    // This forces the reorg walk in on_new_head to call canonical_block_by_hash,
    // which fails with ProviderError::HeaderNotFound.
    let fake_canonical_hash = B256::random();
    tree.state.tree_state.set_canonical_head(BlockNumHash { hash: fake_canonical_hash, number: 5 });

    let event_result = tree.on_tree_event(TreeEvent::TreeAction(TreeAction::MakeCanonical {
        sync_target_head,
    }));

    assert!(
        event_result.is_err(),
        "Expected on_tree_event(MakeCanonical) to fail with ProviderError when \
         canonical_block_by_hash hits a missing block. Got Ok — test setup may be wrong."
    );

    let err = event_result.unwrap_err();
    assert!(
        matches!(err, ProviderError::HeaderNotFound(_)),
        "Expected ProviderError::HeaderNotFound for the missing canonical block, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Layer 3: Bug symptom simulation
// ---------------------------------------------------------------------------

/// **Bug simulation** — manually demonstrates the exact symptom of issue #224.
///
/// This test simulates what would happen if the FCU code path triggered a
/// `MakeCanonical` event that failed. It injects a `MakeCanonical` event into
/// the result of `on_forkchoice_updated` (which currently only returns `None` or
/// `Download` events), then manually runs the BUGGY ordering.
///
/// ### What the bug looks like (the current code path for a hypothetical future trigger)
///
/// ```text
/// // (1) on_forkchoice_updated returns Ok with MakeCanonical event
/// let mut output = self.on_forkchoice_updated(...);
///
/// if let Ok(res) = &mut output {
///     // (2) State mutated — no going back
///     self.state.forkchoice_state_tracker.set_latest(state, res.outcome.forkchoice_status());
///
///     // (3) FATAL: on_maybe_tree_event(MakeCanonical) returns Err(ProviderError)
///     self.on_maybe_tree_event(res.event.take())?;
///     //                                          ^
///     //                                          exits on_engine_message here!
///
///     // (4) NEVER REACHED: CL's oneshot gets RecvError::Closed
///     tx.send(output.map(|o| o.outcome).map_err(Into::into));
/// }
/// ```
///
/// ### Expected result after fix
///
/// `tx.send()` is moved BEFORE `on_maybe_tree_event?`. The CL always gets a response,
/// even when the tree event fails fatally.
///
/// ### Why this test always demonstrates the bug
///
/// This test manually reproduces the buggy ordering. It is a documentation test
/// that shows the symptom regardless of whether the fix has been applied to
/// `on_engine_message`. Use `test_fcu_ordering_tx_send_must_precede_on_maybe_tree_event`
/// (Layer 1) to detect whether the fix was actually applied.
#[test]
fn test_fcu_ordering_bug_simulation() {
    let (mut tree, _action_rx, _from_tree_rx, _) = build_fcu_test_handler();
    let mut block_builder = TestBlockBuilder::eth();

    // === Set up state so on_tree_event(MakeCanonical) will fail ===
    //
    // Insert a side-chain block at height 3. This is the block we'll use as the
    // sync_target_head for the injected MakeCanonical event.
    let side_block = block_builder.get_executed_block_with_number(3, B256::random());
    let injected_sync_target = side_block.recovered_block().hash();
    tree.state.tree_state.insert_executed(side_block);

    // Set canonical head to a missing block at height 5. When make_canonical runs,
    // on_new_head walks the old canonical chain starting from this hash, hits the
    // provider, and returns ProviderError::HeaderNotFound.
    let fake_canonical = B256::random();
    tree.state.tree_state.set_canonical_head(BlockNumHash { hash: fake_canonical, number: 5 });

    // === Step 1: Call on_forkchoice_updated with a syncing FCU (unknown head) ===
    //
    // This produces a real Ok(Syncing) outcome with a Download event.
    let unknown_head = B256::random();
    let fcu_state = ForkchoiceState {
        head_block_hash: unknown_head,
        safe_block_hash: unknown_head,
        finalized_block_hash: B256::ZERO,
    };

    let mut output = tree
        .on_forkchoice_updated(fcu_state, None, EngineApiMessageVersion::default())
        .expect("on_forkchoice_updated should return Ok for an unknown head (syncing path)");

    // === Step 2: INJECT a MakeCanonical event ===
    //
    // Replace the Download event with a MakeCanonical event pointing to our
    // side-chain block. This simulates what would happen if on_forkchoice_updated
    // were to emit a MakeCanonical event (e.g., after a future code change).
    output.event = Some(TreeEvent::TreeAction(TreeAction::MakeCanonical {
        sync_target_head: injected_sync_target,
    }));

    // === Step 3: Simulate the BUGGY on_engine_message FCU ordering ===
    //
    // Create the oneshot that represents the CL waiting for a response.
    let (tx, mut rx) = oneshot::channel::<reth_errors::RethResult<OnForkChoiceUpdated>>();

    // (a) set_latest runs FIRST — state is mutated before the potential failure point
    tree.state
        .forkchoice_state_tracker
        .set_latest(fcu_state, output.outcome.forkchoice_status());

    let tracker_was_mutated = !tree.state.forkchoice_state_tracker.is_empty();
    assert!(tracker_was_mutated, "Tracker must be mutated by set_latest before the failure point");

    // (b) on_maybe_tree_event(MakeCanonical) FAILS with ProviderError
    let event_result = tree.on_maybe_tree_event(output.event.take());
    assert!(
        event_result.is_err(),
        "on_maybe_tree_event(MakeCanonical) must fail — this confirms the failure mode. \
         Got Ok — the test setup may be wrong."
    );

    // (c) In the BUGGY code, the `?` at on_maybe_tree_event(...)? propagates this error
    //     upward, exiting on_engine_message before tx.send() is ever called.
    //     We simulate this by dropping tx WITHOUT sending:
    drop(tx);

    // === Observe the invariant violation ===
    //
    // The CL's oneshot is now CLOSED — the CL would hang indefinitely waiting for a
    // response that will never arrive, while the engine's forkchoice_state_tracker
    // already shows the new FCU head as the sync target.
    let cl_result = rx.try_recv();
    assert!(
        matches!(cl_result, Err(oneshot::error::TryRecvError::Closed)),
        "Expected oneshot to be CLOSED (CL hangs). Got: {cl_result:?}"
    );

    // CONFIRMED BUG: tracker was mutated AND oneshot is closed
    // This is the exact symptom described in issue #224:
    //   - forkchoice_state_tracker shows new head as sync target (mutated)
    //   - CL gets RecvError::Closed instead of a response (oneshot dropped)
    assert!(
        tracker_was_mutated,
        "ISSUE #224 CONFIRMED: forkchoice_state_tracker was mutated (set_latest ran at \
         the point BEFORE the failure) but the CL's oneshot was dropped without a response. \
         Fix: move tx.send() BEFORE on_maybe_tree_event? in the FCU branch."
    );
}

// ---------------------------------------------------------------------------
// Layer 4: Regression tests — correct behavior for all reachable FCU paths
// ---------------------------------------------------------------------------

/// FCU syncing path — response is always sent when head is unknown (Download event).
///
/// When the FCU head is unknown, `on_forkchoice_updated` returns `Ok` with a `Download`
/// event. `on_maybe_tree_event(Download)` succeeds (emits to channel, no Err).
/// `tx.send()` is reached and the CL gets a Syncing response.
///
/// This is the only currently-reachable FCU path where `if let Ok(res)` succeeds AND
/// an event is generated. The Download event cannot fail, so the bug is latent here.
#[tokio::test]
async fn test_fcu_response_sent_in_syncing_path() {
    let (mut tree, _action_rx, _from_tree_rx, _provider) = build_fcu_test_handler();

    assert!(tree.state.forkchoice_state_tracker.is_empty(), "tracker empty before first FCU");

    let unknown_head = B256::random();
    let fcu_state = ForkchoiceState {
        head_block_hash: unknown_head,
        safe_block_hash: unknown_head,
        finalized_block_hash: B256::ZERO,
    };

    let (tx, mut rx) = oneshot::channel();
    let result = tree.on_engine_message(FromEngine::Request(
        BeaconEngineMessage::ForkchoiceUpdated {
            state: fcu_state,
            payload_attrs: None,
            tx,
            version: EngineApiMessageVersion::default(),
        }
        .into(),
    ));

    assert!(result.is_ok(), "on_engine_message must not return Err for syncing FCU: {result:?}");

    // CL must receive a response — the oneshot must be populated
    let channel_result = rx.try_recv();
    assert!(
        channel_result.is_ok(),
        "FCU oneshot was dropped without a response (CL would hang). \
         Issue #224 regression: {channel_result:?}"
    );

    let fcu_response = channel_result.unwrap().unwrap().await.unwrap();
    assert!(
        fcu_response.payload_status.is_syncing(),
        "Expected Syncing payload status, got: {:?}",
        fcu_response.payload_status
    );

    // Tracker must be updated (set_latest ran before tx.send)
    assert!(!tree.state.forkchoice_state_tracker.is_empty(), "tracker must be updated after FCU");
    assert_eq!(
        tree.state.forkchoice_state_tracker.sync_target_state(),
        Some(fcu_state),
        "sync_target_state must reflect the FCU state"
    );
}

/// FCU with canonical head — response is always sent with Valid status.
///
/// When the head is already canonical, `on_forkchoice_updated` returns Valid with no event.
/// `on_maybe_tree_event(None)` → `Ok(())`. `tx.send()` is reached.
#[tokio::test]
async fn test_fcu_response_sent_for_canonical_head() {
    let (mut tree, _action_rx, _from_tree_rx, _provider) = build_fcu_test_handler();

    let canonical_head = tree.state.tree_state.current_canonical_head.hash;
    let fcu_state = ForkchoiceState {
        head_block_hash: canonical_head,
        safe_block_hash: B256::ZERO,
        finalized_block_hash: B256::ZERO,
    };

    let (tx, mut rx) = oneshot::channel();
    let result = tree.on_engine_message(FromEngine::Request(
        BeaconEngineMessage::ForkchoiceUpdated {
            state: fcu_state,
            payload_attrs: None,
            tx,
            version: EngineApiMessageVersion::default(),
        }
        .into(),
    ));

    assert!(result.is_ok(), "on_engine_message returned Err for canonical head FCU: {result:?}");

    let channel_result = rx.try_recv();
    assert!(
        channel_result.is_ok(),
        "FCU oneshot dropped for canonical head — CL would hang: {channel_result:?}"
    );

    let fcu_response = channel_result.unwrap().unwrap().await.unwrap();
    assert!(
        fcu_response.payload_status.is_valid(),
        "Expected Valid payload status for canonical head, got: {:?}",
        fcu_response.payload_status
    );
}

/// FCU where `on_forkchoice_updated` itself fails — response is still sent (as Err).
///
/// This demonstrates the NON-BUG path: when `on_forkchoice_updated` returns `Err`,
/// `output = Err(...)`, the `if let Ok(res)` guard skips `set_latest` and
/// `on_maybe_tree_event`, and `tx.send(Err(...))` IS called.
///
/// Key distinction from issue #224:
/// - This path: error from `on_forkchoice_updated` → `if-let-Ok` guard prevents mutation
///   → `tx.send(Err)` called → no bug
/// - Issue #224 path: error from `on_maybe_tree_event` inside `if-let-Ok` block →
///   mutation already happened → `?` exits before `tx.send()` → bug
#[test]
fn test_fcu_when_forkchoice_updated_fails_response_is_still_sent() {
    let (mut tree, _action_rx, _from_tree_rx, _provider) = build_fcu_test_handler();
    let mut block_builder = TestBlockBuilder::eth();

    // Insert a block in tree state at height 3 with an unreachable canonical ancestor.
    let in_memory_block = block_builder.get_executed_block_with_number(3, B256::random());
    let block_hash = in_memory_block.recovered_block().hash();
    tree.state.tree_state.insert_executed(in_memory_block);

    // Set canonical head to a missing block at height 5 > 3.
    // on_forkchoice_updated → on_new_head → reorg walk → provider fails → Err
    let fake_canonical = B256::random();
    tree.state.tree_state.set_canonical_head(BlockNumHash { hash: fake_canonical, number: 5 });

    assert!(tree.state.forkchoice_state_tracker.is_empty(), "tracker empty before FCU");

    let (tx, mut rx) = oneshot::channel();
    let result = tree.on_engine_message(FromEngine::Request(
        BeaconEngineMessage::ForkchoiceUpdated {
            state: ForkchoiceState {
                head_block_hash: block_hash,
                safe_block_hash: block_hash,
                finalized_block_hash: B256::ZERO,
            },
            payload_attrs: None,
            tx,
            version: EngineApiMessageVersion::default(),
        }
        .into(),
    ));

    // on_engine_message returns Ok — error was encoded in the oneshot, not propagated
    assert!(
        result.is_ok(),
        "on_engine_message should return Ok when on_forkchoice_updated fails \
         (error sent via tx, not propagated). Got: {result:?}"
    );

    // The oneshot MUST have a response — error is delivered via oneshot, not dropped
    let channel_result = rx.try_recv();
    assert!(
        channel_result.is_ok(),
        "FCU tx was not sent when on_forkchoice_updated failed. CL would hang: {channel_result:?}"
    );

    // The response encodes the provider error
    assert!(
        channel_result.unwrap().is_err(),
        "Expected Err in oneshot when on_forkchoice_updated fails with ProviderError"
    );

    // CRITICAL: tracker must NOT be mutated — if-let-Ok guard protected it.
    // If mutated, engine state would diverge (showing head as valid/syncing when FCU failed).
    assert!(
        tree.state.forkchoice_state_tracker.is_empty(),
        "Tracker must NOT be mutated when on_forkchoice_updated itself returns Err. \
         The if-let-Ok guard should have prevented set_latest from running."
    );
}
