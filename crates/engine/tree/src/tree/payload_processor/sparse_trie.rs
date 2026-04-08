//! Sparse Trie task related functionality.

use crate::tree::payload_processor::multiproof::{MultiProofTaskMetrics, SparseTrieUpdate};
use alloy_primitives::B256;
use rayon::iter::{ParallelBridge, ParallelIterator};
use reth_trie::{updates::TrieUpdates, Nibbles};
use reth_trie_parallel::root::ParallelStateRootError;
use reth_trie_sparse::{
    errors::{SparseStateTrieResult, SparseTrieErrorKind},
    provider::{TrieNodeProvider, TrieNodeProviderFactory},
    ClearedSparseStateTrie, SerialSparseTrie, SparseStateTrie, SparseTrieInterface,
};
use smallvec::SmallVec;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};
use tracing::{debug, trace, trace_span};

/// A task responsible for populating the sparse trie.
pub(super) struct SparseTrieTask<BPF, A = SerialSparseTrie, S = SerialSparseTrie>
where
    BPF: TrieNodeProviderFactory + Send + Sync,
    BPF::AccountNodeProvider: TrieNodeProvider + Send + Sync,
    BPF::StorageNodeProvider: TrieNodeProvider + Send + Sync,
{
    /// Receives updates from the state root task.
    pub(super) updates: mpsc::Receiver<SparseTrieUpdate>,
    /// `SparseStateTrie` used for computing the state root.
    pub(super) trie: SparseStateTrie<A, S>,
    pub(super) metrics: MultiProofTaskMetrics,
    /// Trie node provider factory.
    blinded_provider_factory: BPF,
}

impl<BPF, A, S> SparseTrieTask<BPF, A, S>
where
    BPF: TrieNodeProviderFactory + Send + Sync + Clone,
    BPF::AccountNodeProvider: TrieNodeProvider + Send + Sync,
    BPF::StorageNodeProvider: TrieNodeProvider + Send + Sync,
    A: SparseTrieInterface + Send + Sync + Default,
    S: SparseTrieInterface + Send + Sync + Default + Clone,
{
    /// Creates a new sparse trie, pre-populating with a [`ClearedSparseStateTrie`].
    pub(super) fn new_with_cleared_trie(
        updates: mpsc::Receiver<SparseTrieUpdate>,
        blinded_provider_factory: BPF,
        metrics: MultiProofTaskMetrics,
        sparse_state_trie: ClearedSparseStateTrie<A, S>,
    ) -> Self {
        Self { updates, metrics, trie: sparse_state_trie.into_inner(), blinded_provider_factory }
    }

    /// Runs the sparse trie task to completion.
    ///
    /// This waits for new incoming [`SparseTrieUpdate`].
    ///
    /// This concludes once the last trie update has been received.
    ///
    /// # Returns
    ///
    /// - State root computation outcome.
    /// - `SparseStateTrie` that needs to be cleared and reused to avoid reallocations.
    pub(super) fn run(
        mut self,
    ) -> (Result<StateRootComputeOutcome, ParallelStateRootError>, SparseStateTrie<A, S>) {
        // run the main loop to completion
        let result = self.run_inner();
        (result, self.trie)
    }

    /// Inner function to run the sparse trie task to completion.
    ///
    /// See [`Self::run`] for more information.
    fn run_inner(&mut self) -> Result<StateRootComputeOutcome, ParallelStateRootError> {
        let now = Instant::now();

        let mut num_iterations = 0;

        while let Ok(mut update) = self.updates.recv() {
            num_iterations += 1;
            let mut num_updates = 1;
            while let Ok(next) = self.updates.try_recv() {
                update.extend(next);
                num_updates += 1;
            }

            debug!(
                target: "engine::root",
                num_updates,
                account_proofs = update.multiproof.account_subtree.len(),
                storage_proofs = update.multiproof.storages.len(),
                "Updating sparse trie"
            );

            let elapsed =
                update_sparse_trie(&mut self.trie, update, &self.blinded_provider_factory)
                    .map_err(|e| {
                        ParallelStateRootError::Other(format!(
                            "could not calculate state root: {e:?}"
                        ))
                    })?;
            self.metrics.sparse_trie_update_duration_histogram.record(elapsed);
            trace!(target: "engine::root", ?elapsed, num_iterations, "Root calculation completed");
        }

        debug!(target: "engine::root", num_iterations, "All proofs processed, ending calculation");

        let start = Instant::now();
        let (state_root, trie_updates) =
            self.trie.root_with_updates(&self.blinded_provider_factory).map_err(|e| {
                ParallelStateRootError::Other(format!("could not calculate state root: {e:?}"))
            })?;

        self.metrics.sparse_trie_final_update_duration_histogram.record(start.elapsed());
        self.metrics.sparse_trie_total_duration_histogram.record(now.elapsed());

        Ok(StateRootComputeOutcome { state_root, trie_updates })
    }
}

/// Outcome of the state root computation, including the state root itself with
/// the trie updates.
#[derive(Debug)]
pub struct StateRootComputeOutcome {
    /// The state root.
    pub state_root: B256,
    /// The trie updates.
    pub trie_updates: TrieUpdates,
}

/// Updates the sparse trie with the given proofs and state, and returns the elapsed time.
pub(crate) fn update_sparse_trie<BPF, A, S>(
    trie: &mut SparseStateTrie<A, S>,
    SparseTrieUpdate { mut state, multiproof }: SparseTrieUpdate,
    blinded_provider_factory: &BPF,
) -> SparseStateTrieResult<Duration>
where
    BPF: TrieNodeProviderFactory + Send + Sync,
    BPF::AccountNodeProvider: TrieNodeProvider + Send + Sync,
    BPF::StorageNodeProvider: TrieNodeProvider + Send + Sync,
    A: SparseTrieInterface + Send + Sync + Default,
    S: SparseTrieInterface + Send + Sync + Default + Clone,
{
    trace!(target: "engine::root::sparse", "Updating sparse trie");
    let started_at = Instant::now();

    // Reveal new accounts and storage slots.
    trie.reveal_decoded_multiproof(multiproof)?;
    let reveal_multiproof_elapsed = started_at.elapsed();
    trace!(
        target: "engine::root::sparse",
        ?reveal_multiproof_elapsed,
        "Done revealing multiproof"
    );

    // Update storage slots with new values and calculate storage roots.
    let (tx, rx) = mpsc::channel();
    state
        .storages
        .into_iter()
        .map(|(address, storage)| (address, storage, trie.take_storage_trie(&address)))
        .par_bridge()
        .map(|(address, storage, storage_trie)| {
            let span = trace_span!(target: "engine::root::sparse", "Storage trie", ?address);
            let _enter = span.enter();
            trace!(target: "engine::root::sparse", "Updating storage");
            let storage_provider = blinded_provider_factory.storage_node_provider(address);
            let mut storage_trie = storage_trie.ok_or(SparseTrieErrorKind::Blind)?;

            if storage.wiped {
                trace!(target: "engine::root::sparse", "Wiping storage");
                storage_trie.wipe()?;
            }

            // Defer leaf removals until after updates/additions, so that we don't delete an
            // intermediate branch node during a removal and then re-add that branch back during a
            // later leaf addition. This is an optimization, but also a requirement inherited from
            // multiproof generating, which can't know the order that leaf operations happen in.
            let mut removed_slots = SmallVec::<[Nibbles; 8]>::new();

            for (slot, value) in storage.storage {
                let slot_nibbles = Nibbles::unpack(slot);

                if value.is_zero() {
                    removed_slots.push(slot_nibbles);
                    continue;
                }

                trace!(target: "engine::root::sparse", ?slot_nibbles, "Updating storage slot");
                storage_trie.update_leaf(
                    slot_nibbles,
                    alloy_rlp::encode_fixed_size(&value).to_vec(),
                    &storage_provider,
                )?;
            }

            for slot_nibbles in removed_slots {
                trace!(target: "engine::root::sparse", ?slot_nibbles, "Removing storage slot");
                storage_trie.remove_leaf(&slot_nibbles, &storage_provider)?;
            }

            storage_trie.root();

            SparseStateTrieResult::Ok((address, storage_trie))
        })
        .for_each_init(
            || tx.clone(),
            |tx, result| {
                let _ = tx.send(result);
            },
        );
    drop(tx);

    // Defer leaf removals until after updates/additions, so that we don't delete an intermediate
    // branch node during a removal and then re-add that branch back during a later leaf addition.
    // This is an optimization, but also a requirement inherited from multiproof generating, which
    // can't know the order that leaf operations happen in.
    let mut removed_accounts = Vec::new();

    // Update account storage roots
    for result in rx {
        let (address, storage_trie) = result?;
        trie.insert_storage_trie(address, storage_trie);

        if let Some(account) = state.accounts.remove(&address) {
            // If the account itself has an update, remove it from the state update and update in
            // one go instead of doing it down below.
            trace!(target: "engine::root::sparse", ?address, "Updating account and its storage root");
            if !trie.update_account(
                address,
                account.unwrap_or_default(),
                blinded_provider_factory,
            )? {
                removed_accounts.push(address);
            }
        } else if trie.is_account_revealed(address) {
            // Otherwise, if the account is revealed, only update its storage root.
            trace!(target: "engine::root::sparse", ?address, "Updating account storage root");
            if !trie.update_account_storage_root(address, blinded_provider_factory)? {
                removed_accounts.push(address);
            }
        }
    }

    // Update accounts
    for (address, account) in state.accounts {
        trace!(target: "engine::root::sparse", ?address, "Updating account");
        if !trie.update_account(address, account.unwrap_or_default(), blinded_provider_factory)? {
            removed_accounts.push(address);
        }
    }

    // Remove accounts
    for address in removed_accounts {
        trace!(target: "trie::sparse", ?address, "Removing account");
        let nibbles = Nibbles::unpack(address);
        trie.remove_account_leaf(&nibbles, blinded_provider_factory)?;
    }

    let elapsed_before = started_at.elapsed();
    trace!(
        target: "engine::root::sparse",
        "Calculating subtries"
    );
    trie.calculate_subtries();

    let elapsed = started_at.elapsed();
    let below_level_elapsed = elapsed - elapsed_before;
    trace!(
        target: "engine::root::sparse",
        ?below_level_elapsed,
        "Intermediate nodes calculated"
    );

    Ok(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::payload_processor::multiproof::SparseTrieUpdate;
    use alloy_primitives::B256;
    use reth_trie::{DecodedMultiProof, HashedPostState, HashedStorage};
    use reth_trie_sparse::{
        provider::DefaultTrieNodeProviderFactory, SerialSparseTrie, SparseTrie, SparseStateTrie,
    };

    /// Directly demonstrates the mechanism behind issue #168:
    /// storage tries removed via `take_storage_trie` that are never re-inserted
    /// are permanently lost, corrupting the trie.
    ///
    /// This test simulates the early-return path in `update_sparse_trie` where
    /// `result?` returns before all storage try results are consumed from the channel.
    #[test]
    fn storage_trie_taken_without_reinsert_is_permanently_lost() {
        let mut trie = SparseStateTrie::<SerialSparseTrie>::default();

        let addr_a = B256::from([0xAAu8; 32]);
        let addr_c = B256::from([0xCCu8; 32]);

        // Pre-populate the trie with revealed storage tries for A and C.
        trie.insert_storage_trie(addr_a, SparseTrie::revealed_empty());
        trie.insert_storage_trie(addr_c, SparseTrie::revealed_empty());

        assert!(trie.storage_trie_ref(&addr_a).is_some(), "A should be present before");
        assert!(trie.storage_trie_ref(&addr_c).is_some(), "C should be present before");

        // Simulate what update_sparse_trie does: take ALL storage tries out via the
        // sequential .map() before par_bridge().
        let trie_a = trie.take_storage_trie(&addr_a);
        let trie_c = trie.take_storage_trie(&addr_c);

        // After take, both are removed from the trie.
        assert!(trie.storage_trie_ref(&addr_a).is_none(), "A should be taken out");
        assert!(trie.storage_trie_ref(&addr_c).is_none(), "C should be taken out");

        // Simulate the early-return path in the `for result in rx` loop:
        // A's result is consumed (re-inserted), then B's Err causes `result?` to return early.
        // C's result is never consumed — its storage trie is dropped.
        trie.insert_storage_trie(addr_a, trie_a.unwrap());
        drop(trie_c); // C is lost — never re-inserted due to the early return

        // A is correctly back.
        assert!(trie.storage_trie_ref(&addr_a).is_some(), "A should be re-inserted");

        // CRITICAL BUG (issue #168): C's storage trie is permanently gone.
        // The trie is now corrupted: it has no subtrie for C, so any subsequent
        // read/write to C's storage will operate on a blind node, silently producing
        // wrong state roots for every block that follows.
        assert!(
            trie.storage_trie_ref(&addr_c).is_none(),
            "BUG confirmed: C storage trie is permanently missing after simulated early return"
        );
    }

    /// Verifies that `update_sparse_trie` returns an error when a storage entry references
    /// an address whose storage trie has not been revealed (is blind).
    ///
    /// This is the trigger condition for the corruption in issue #168.
    #[test]
    fn update_sparse_trie_errors_on_blind_storage_trie() {
        let provider = DefaultTrieNodeProviderFactory;
        let mut trie = SparseStateTrie::<SerialSparseTrie>::default();

        // blind_addr has a storage update but NO storage trie in the sparse trie.
        let blind_addr = B256::from([0xBBu8; 32]);

        let mut state = HashedPostState::default();
        state.storages.insert(
            blind_addr,
            HashedStorage { wiped: false, storage: Default::default() },
        );

        let update = SparseTrieUpdate { state, multiproof: DecodedMultiProof::default() };
        let result = update_sparse_trie(&mut trie, update, &provider);

        assert!(
            result.is_err(),
            "update_sparse_trie must return Err when storage trie is missing (blind)"
        );
    }

    /// Tests the invariant that after `update_sparse_trie` returns an error,
    /// ALL storage tries that were present before the call are still present after.
    ///
    /// With the bug in issue #168, storage tries are removed via `take_storage_trie`
    /// before parallel processing, but when any worker returns an error the re-insertion
    /// loop exits early via `result?`, dropping the remaining tries.
    ///
    /// NOTE: Whether corruption is observed depends on the order in which the parallel
    /// workers deliver their results. This test records which tries are present before
    /// and after, and asserts that none were lost. With the bug, this assertion will
    /// fail whenever the blind address is NOT last in the channel ordering.
    #[test]
    fn update_sparse_trie_must_not_lose_storage_tries_on_error() {
        let provider = DefaultTrieNodeProviderFactory;
        let mut trie = SparseStateTrie::<SerialSparseTrie>::default();

        // These addresses have storage tries in the trie.
        let addr_a = B256::from([0xAAu8; 32]);
        let addr_c = B256::from([0xCCu8; 32]);
        let addr_d = B256::from([0xDDu8; 32]);

        // This address has a storage update but NO trie entry → causes Blind error.
        let addr_b = B256::from([0xBBu8; 32]);

        // Pre-populate the trie with revealed storage tries for A, C, D.
        for addr in [addr_a, addr_c, addr_d] {
            trie.insert_storage_trie(addr, SparseTrie::revealed_empty());
        }

        // Build the state update: A, B (blind), C, D all have storage changes.
        let mut state = HashedPostState::default();
        for addr in [addr_a, addr_b, addr_c, addr_d] {
            state.storages.insert(
                addr,
                HashedStorage { wiped: false, storage: Default::default() },
            );
        }

        let update = SparseTrieUpdate { state, multiproof: DecodedMultiProof::default() };
        let result = update_sparse_trie(&mut trie, update, &provider);

        // The function MUST return an error because addr_b has no storage trie.
        assert!(result.is_err(), "expected Err due to blind storage trie for addr_b");

        // INVARIANT (violated by issue #168):
        // All storage tries that existed before the call must still exist after the call,
        // even on the error path. The trie must not be partially mutated.
        let a_present = trie.storage_trie_ref(&addr_a).is_some();
        let c_present = trie.storage_trie_ref(&addr_c).is_some();
        let d_present = trie.storage_trie_ref(&addr_d).is_some();

        assert!(
            a_present && c_present && d_present,
            "BUG (issue #168): storage tries lost after error — \
             addr_a present: {a_present}, addr_c present: {c_present}, \
             addr_d present: {d_present}. \
             `take_storage_trie` removes all tries before par_bridge, \
             but `result?` early-return prevents re-insertion of unconsumed results, \
             leaving the trie permanently incomplete."
        );
    }

    /// Tests that `SparseTrieTask::run()` on error returns a trie that still contains
    /// all storage subtries (none must have been permanently dropped).
    ///
    /// With the bug, `run()` returns `(Err(...), trie)` where `trie` may be missing
    /// storage subtries. Putting this trie back into `cleared_sparse_trie` (as mod.rs
    /// line 417 does unconditionally) then corrupts every subsequent block.
    #[test]
    fn sparse_trie_task_run_trie_retains_storage_tries_on_error() {
        use std::sync::mpsc;

        let provider = DefaultTrieNodeProviderFactory;

        // Addresses with storage tries that will be in the trie.
        let addr_a = B256::from([0xAAu8; 32]);
        let addr_c = B256::from([0xCCu8; 32]);
        // This address has no storage trie → triggers Blind error.
        let addr_b = B256::from([0xBBu8; 32]);

        // Build the initial trie.
        let mut trie = SparseStateTrie::<SerialSparseTrie>::default();
        trie.insert_storage_trie(addr_a, SparseTrie::revealed_empty());
        trie.insert_storage_trie(addr_c, SparseTrie::revealed_empty());

        // Build the state update with all three addresses.
        let mut state = HashedPostState::default();
        for addr in [addr_a, addr_b, addr_c] {
            state.storages.insert(
                addr,
                HashedStorage { wiped: false, storage: Default::default() },
            );
        }

        // Create the SparseTrieTask channel and send one update that will error.
        let (tx, rx) = mpsc::channel();
        let update = SparseTrieUpdate { state, multiproof: DecodedMultiProof::default() };
        tx.send(update).unwrap();
        drop(tx); // close the channel so run() terminates after the update

        let metrics = crate::tree::payload_processor::multiproof::MultiProofTaskMetrics::default();
        let cleared = reth_trie_sparse::ClearedSparseStateTrie::from_state_trie(trie);
        let task = SparseTrieTask::<DefaultTrieNodeProviderFactory, SerialSparseTrie, SerialSparseTrie>::new_with_cleared_trie(
            rx,
            provider,
            metrics,
            cleared,
        );

        let (result, returned_trie) = task.run();

        // run() must report an error.
        assert!(result.is_err(), "expected run() to return Err on blind storage trie");

        // INVARIANT (violated by issue #168):
        // The returned trie must still contain all storage tries that were present
        // before the update was applied. If any are missing, recycling this trie
        // (as mod.rs line 417 does) will corrupt all subsequent blocks.
        let a_present = returned_trie.storage_trie_ref(&addr_a).is_some();
        let c_present = returned_trie.storage_trie_ref(&addr_c).is_some();

        assert!(
            a_present && c_present,
            "BUG (issue #168): returned trie is corrupted — \
             addr_a present: {a_present}, addr_c present: {c_present}. \
             Recycling this trie (mod.rs:417) will cause silent wrong state roots \
             for all subsequent blocks."
        );
    }
}
