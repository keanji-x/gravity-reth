//! Caching and prewarming related functionality.
//!
//! Prewarming executes transactions in parallel before the actual block execution
//! to populate the execution cache with state that will likely be accessed during
//! block processing.
//!
//! ## How Prewarming Works
//!
//! 1. Incoming transactions are split into two streams: one for prewarming (executed in parallel)
//!    and one for actual execution (executed sequentially)
//! 2. Prewarming tasks execute transactions in parallel using shared caches
//! 3. When actual block execution happens, it benefits from the warmed cache

use crate::tree::{
    cached_state::{CachedStateProvider, SavedCache},
    payload_processor::{
        executor::WorkloadExecutor, multiproof::MultiProofMessage,
        ExecutionCache as PayloadExecutionCache,
    },
    precompile_cache::{CachedPrecompile, PrecompileCacheMap},
    ExecutionEnv, StateProviderBuilder,
};
use alloy_consensus::transaction::TxHashRef;
use alloy_evm::Database;
use alloy_primitives::{keccak256, map::B256Set, B256};
use metrics::{Counter, Gauge, Histogram};
use reth_evm::{execute::ExecutableTxFor, ConfigureEvm, Evm, EvmFor, SpecFor};
use reth_metrics::Metrics;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{BlockReader, StateProviderFactory, StateReader};
use reth_revm::{database::StateProviderDatabase, db::BundleState, state::EvmState};
use reth_trie::MultiProofTargets;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, channel, Receiver, Sender},
        Arc,
    },
    time::Instant,
};
use tracing::{debug, trace};

/// A task that is responsible for caching and prewarming the cache by executing transactions
/// individually in parallel.
///
/// Note: This task runs until cancelled externally.
pub(super) struct PrewarmCacheTask<N, P, Evm>
where
    N: NodePrimitives,
    Evm: ConfigureEvm<Primitives = N>,
{
    /// The executor used to spawn execution tasks.
    executor: WorkloadExecutor,
    /// Shared execution cache.
    execution_cache: PayloadExecutionCache,
    /// Context provided to execution tasks
    ctx: PrewarmContext<N, P, Evm>,
    /// How many transactions should be executed in parallel
    max_concurrency: usize,
    /// Sender to emit evm state outcome messages, if any.
    to_multi_proof: Option<Sender<MultiProofMessage>>,
    /// Receiver for events produced by tx execution
    actions_rx: Receiver<PrewarmTaskEvent>,
}

impl<N, P, Evm> PrewarmCacheTask<N, P, Evm>
where
    N: NodePrimitives,
    P: BlockReader + StateProviderFactory + StateReader + Clone + 'static,
    Evm: ConfigureEvm<Primitives = N> + 'static,
{
    /// Initializes the task with the given transactions pending execution
    pub(super) fn new(
        executor: WorkloadExecutor,
        execution_cache: PayloadExecutionCache,
        ctx: PrewarmContext<N, P, Evm>,
        to_multi_proof: Option<Sender<MultiProofMessage>>,
    ) -> (Self, Sender<PrewarmTaskEvent>) {
        let (actions_tx, actions_rx) = channel();
        (
            Self {
                executor,
                execution_cache,
                ctx,
                max_concurrency: 64,
                to_multi_proof,
                actions_rx,
            },
            actions_tx,
        )
    }

    /// Spawns all pending transactions as blocking tasks by first chunking them.
    fn spawn_all(
        &self,
        pending: mpsc::Receiver<impl ExecutableTxFor<Evm> + Send + 'static>,
        actions_tx: Sender<PrewarmTaskEvent>,
    ) {
        let executor = self.executor.clone();
        let ctx = self.ctx.clone();
        let max_concurrency = self.max_concurrency;

        self.executor.spawn_blocking(move || {
            let mut handles = Vec::with_capacity(max_concurrency);
            let (done_tx, done_rx) = mpsc::channel();
            let mut executing = 0;
            while let Ok(executable) = pending.recv() {
                let task_idx = executing % max_concurrency;

                if handles.len() <= task_idx {
                    let (tx, rx) = mpsc::channel();
                    let sender = actions_tx.clone();
                    let ctx = ctx.clone();
                    let done_tx = done_tx.clone();

                    executor.spawn_blocking(move || {
                        ctx.transact_batch(rx, sender, done_tx);
                    });

                    handles.push(tx);
                }

                let _ = handles[task_idx].send(executable);

                executing += 1;
            }

            // drop handle and wait for all tasks to finish and drop theirs
            drop(done_tx);
            drop(handles);
            while done_rx.recv().is_ok() {}

            let _ = actions_tx
                .send(PrewarmTaskEvent::FinishedTxExecution { executed_transactions: executing });
        });
    }

    /// If configured and the tx returned proof targets, emit the targets the transaction produced
    fn send_multi_proof_targets(&self, targets: Option<MultiProofTargets>) {
        if let Some((proof_targets, to_multi_proof)) = targets.zip(self.to_multi_proof.as_ref()) {
            let _ = to_multi_proof.send(MultiProofMessage::PrefetchProofs(proof_targets));
        }
    }

    /// This method calls `ExecutionCache::update_with_guard` which requires exclusive access.
    /// It should only be called after ensuring that:
    /// 1. All prewarming tasks have completed execution
    /// 2. No other concurrent operations are accessing the cache
    ///
    /// Saves the warmed caches back into the shared slot after prewarming completes.
    ///
    /// This consumes the `SavedCache` held by the task, which releases its usage guard and allows
    /// the new, warmed cache to be inserted.
    ///
    /// This method is called from `run()` only after all execution tasks are complete.
    fn save_cache(self, state: BundleState) {
        let start = Instant::now();

        let Self { execution_cache, ctx: PrewarmContext { env, metrics, saved_cache, .. }, .. } =
            self;
        let hash = env.hash;

        // Perform all cache operations atomically under the lock
        execution_cache.update_with_guard(|cached| {

            // consumes the `SavedCache` held by the prewarming task, which releases its usage guard
            let (caches, cache_metrics) = saved_cache.split();
            let new_cache = SavedCache::new(hash, caches, cache_metrics);

            // Insert state into cache while holding the lock
            if new_cache.cache().insert_state(&state).is_err() {
                // Clear the cache on error to prevent having a polluted cache
                *cached = None;
                debug!(target: "engine::caching", "cleared execution cache on update error");
                return;
            }

            new_cache.update_metrics();
            debug!(target: "engine::caching", parent_hash=?new_cache.executed_block_hash(), "Updated execution cache");

            // Replace the shared cache with the new one; the previous cache (if any) is dropped.
            *cached = Some(new_cache);
        });

        metrics.cache_saving_duration.set(start.elapsed().as_secs_f64());
    }

    /// Executes the task.
    ///
    /// This will execute the transactions until all transactions have been processed or the task
    /// was cancelled.
    pub(super) fn run(
        self,
        pending: mpsc::Receiver<impl ExecutableTxFor<Evm> + Send + 'static>,
        actions_tx: Sender<PrewarmTaskEvent>,
    ) {
        // spawn execution tasks.
        self.spawn_all(pending, actions_tx);

        let mut final_block_output = None;
        let mut finished_execution = false;
        while let Ok(event) = self.actions_rx.recv() {
            match event {
                PrewarmTaskEvent::TerminateTransactionExecution => {
                    // stop tx processing
                    self.ctx.terminate_execution.store(true, Ordering::Relaxed);
                }
                PrewarmTaskEvent::Outcome { proof_targets } => {
                    // completed executing a set of transactions
                    self.send_multi_proof_targets(proof_targets);
                }
                PrewarmTaskEvent::Terminate { block_output } => {
                    trace!(target: "engine::tree::prewarm", "Received termination signal");
                    final_block_output = Some(block_output);

                    if finished_execution {
                        // all tasks are done, we can exit, which will save caches and exit
                        break
                    }
                }
                PrewarmTaskEvent::FinishedTxExecution { executed_transactions } => {
                    trace!(target: "engine::tree::prewarm", "Finished prewarm execution signal");
                    self.ctx.metrics.transactions.set(executed_transactions as f64);
                    self.ctx.metrics.transactions_histogram.record(executed_transactions as f64);

                    finished_execution = true;

                    if final_block_output.is_some() {
                        // all tasks are done, we can exit, which will save caches and exit
                        break
                    }
                }
            }
        }

        trace!(target: "engine::tree::prewarm", "Completed prewarm execution");

        // save caches and finish
        if let Some(Some(state)) = final_block_output {
            self.save_cache(state);
        }
    }
}

/// Context required by tx execution tasks.
#[derive(Debug, Clone)]
pub(super) struct PrewarmContext<N, P, Evm>
where
    N: NodePrimitives,
    Evm: ConfigureEvm<Primitives = N>,
{
    pub(super) env: ExecutionEnv<Evm>,
    pub(super) evm_config: Evm,
    pub(super) saved_cache: SavedCache,
    /// Provider to obtain the state
    pub(super) provider: StateProviderBuilder<N, P>,
    pub(super) metrics: PrewarmMetrics,
    /// An atomic bool that tells prewarm tasks to not start any more execution.
    pub(super) terminate_execution: Arc<AtomicBool>,
    pub(super) precompile_cache_disabled: bool,
    pub(super) precompile_cache_map: PrecompileCacheMap<SpecFor<Evm>>,
}

impl<N, P, Evm> PrewarmContext<N, P, Evm>
where
    N: NodePrimitives,
    P: BlockReader + StateProviderFactory + StateReader + Clone + 'static,
    Evm: ConfigureEvm<Primitives = N> + 'static,
{
    /// Splits this context into an evm, an evm config, metrics, and the atomic bool for terminating
    /// execution.
    fn evm_for_ctx(self) -> Option<(EvmFor<Evm, impl Database>, PrewarmMetrics, Arc<AtomicBool>)> {
        let Self {
            env,
            evm_config,
            saved_cache,
            provider,
            metrics,
            terminate_execution,
            precompile_cache_disabled,
            mut precompile_cache_map,
        } = self;

        let state_provider = match provider.build() {
            Ok(provider) => provider,
            Err(err) => {
                trace!(
                    target: "engine::tree",
                    %err,
                    "Failed to build state provider in prewarm thread"
                );
                return None
            }
        };

        // Use the caches to create a new provider with caching
        let caches = saved_cache.cache().clone();
        let cache_metrics = saved_cache.metrics().clone();
        let state_provider =
            CachedStateProvider::new_with_caches(state_provider, caches, cache_metrics);

        let state_provider = StateProviderDatabase::new(state_provider);

        let mut evm_env = env.evm_env;

        // we must disable the nonce check so that we can execute the transaction even if the nonce
        // doesn't match what's on chain.
        evm_env.cfg_env.disable_nonce_check = true;

        // create a new executor and disable nonce checks in the env
        let spec_id = *evm_env.spec_id();
        let mut evm = evm_config.evm_with_env(state_provider, evm_env);

        if !precompile_cache_disabled {
            // Only cache pure precompiles to avoid issues with stateful precompiles
            evm.precompiles_mut().map_pure_precompiles(|address, precompile| {
                CachedPrecompile::wrap(
                    precompile,
                    precompile_cache_map.cache_for_address(*address),
                    spec_id,
                    None, // No metrics for prewarm
                )
            });
        }

        Some((evm, metrics, terminate_execution))
    }

    /// Accepts an [`mpsc::Receiver`] of transactions and a handle to prewarm task. Executes
    /// transactions and streams [`PrewarmTaskEvent::Outcome`] messages for each transaction.
    ///
    /// Returns `None` if executing the transactions failed to a non Revert error.
    /// Returns the touched+modified state of the transaction.
    ///
    /// Note: There are no ordering guarantees; this does not reflect the state produced by
    /// sequential execution.
    fn transact_batch(
        self,
        txs: mpsc::Receiver<impl ExecutableTxFor<Evm>>,
        sender: Sender<PrewarmTaskEvent>,
        done_tx: Sender<()>,
    ) {
        let Some((mut evm, metrics, terminate_execution)) = self.evm_for_ctx() else { return };

        while let Ok(tx) = txs.recv() {
            // If the task was cancelled, stop execution, send an empty result to notify the task,
            // and exit.
            if terminate_execution.load(Ordering::Relaxed) {
                let _ = sender.send(PrewarmTaskEvent::Outcome { proof_targets: None });
                break
            }

            // create the tx env
            let start = Instant::now();
            let res = match evm.transact(&tx) {
                Ok(res) => res,
                Err(err) => {
                    trace!(
                        target: "engine::tree::prewarm",
                        %err,
                        tx_hash=%tx.tx().tx_hash(),
                        sender=%tx.signer(),
                        "Error when executing prewarm transaction",
                    );
                    // Track transaction execution errors
                    metrics.transaction_errors.increment(1);
                    // skip error because we can ignore these errors and continue with the next tx
                    continue
                }
            };
            metrics.execution_duration.record(start.elapsed());

            let (targets, storage_targets) = multiproof_targets_from_state(res.state);
            metrics.prefetch_storage_targets.record(storage_targets as f64);
            metrics.total_runtime.record(start.elapsed());

            let _ = sender.send(PrewarmTaskEvent::Outcome { proof_targets: Some(targets) });
        }

        // send a message to the main task to flag that we're done
        let _ = done_tx.send(());
    }
}

/// Returns a set of [`MultiProofTargets`] and the total amount of storage targets, based on the
/// given state.
fn multiproof_targets_from_state(state: EvmState) -> (MultiProofTargets, usize) {
    let mut targets = MultiProofTargets::with_capacity(state.len());
    let mut storage_targets = 0;
    for (addr, account) in state {
        // if the account was not touched, or if the account was selfdestructed, do not
        // fetch proofs for it
        //
        // Since selfdestruct can only happen in the same transaction, we can skip
        // prefetching proofs for selfdestructed accounts
        //
        // See: https://eips.ethereum.org/EIPS/eip-6780
        if !account.is_touched() || account.is_selfdestructed() {
            continue
        }

        let mut storage_set =
            B256Set::with_capacity_and_hasher(account.storage.len(), Default::default());
        for (key, slot) in account.storage {
            // do nothing if unchanged
            if !slot.is_changed() {
                continue
            }

            storage_set.insert(keccak256(B256::new(key.to_be_bytes())));
        }

        storage_targets += storage_set.len();
        targets.insert(keccak256(addr), storage_set);
    }

    (targets, storage_targets)
}

/// The events the pre-warm task can handle.
pub(super) enum PrewarmTaskEvent {
    /// Forcefully terminate all remaining transaction execution.
    TerminateTransactionExecution,
    /// Forcefully terminate the task on demand and update the shared cache with the given output
    /// before exiting.
    Terminate {
        /// The final block state output.
        block_output: Option<BundleState>,
    },
    /// The outcome of a pre-warm task
    Outcome {
        /// The prepared proof targets based on the evm state outcome
        proof_targets: Option<MultiProofTargets>,
    },
    /// Finished executing all transactions
    FinishedTxExecution {
        /// Number of transactions executed
        executed_transactions: usize,
    },
}

/// Metrics for transactions prewarming.
#[derive(Metrics, Clone)]
#[metrics(scope = "sync.prewarm")]
pub(crate) struct PrewarmMetrics {
    /// The number of transactions to prewarm
    pub(crate) transactions: Gauge,
    /// A histogram of the number of transactions to prewarm
    pub(crate) transactions_histogram: Histogram,
    /// A histogram of duration per transaction prewarming
    pub(crate) total_runtime: Histogram,
    /// A histogram of EVM execution duration per transaction prewarming
    pub(crate) execution_duration: Histogram,
    /// A histogram for prefetch targets per transaction prewarming
    pub(crate) prefetch_storage_targets: Histogram,
    /// A histogram of duration for cache saving
    pub(crate) cache_saving_duration: Gauge,
    /// Counter for transaction execution errors during prewarming
    pub(crate) transaction_errors: Counter,
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    /// Reproduces the core of the bug at `spawn_all` lines 123–125:
    ///
    /// ```text
    /// let _ = handles[task_idx].send(executable);   // Err silently dropped
    /// executing += 1;                               // always incremented
    /// ```
    ///
    /// When a worker's receiver is dropped (e.g., `evm_for_ctx()` returned `None`),
    /// every send to that slot returns `Err(SendError)`.  The current code swallows
    /// the error with `let _` and then unconditionally bumps `executing`, so
    /// `FinishedTxExecution { executed_transactions }` is over-reported.
    ///
    /// **Expected (correct) behaviour:** `executing` must NOT be incremented when
    /// the send returns `Err`.
    /// **Actual (buggy) behaviour:** `executing` IS incremented — this test will
    /// FAIL against the unfixed code, proving the bug exists.
    #[test]
    fn test_executing_counter_must_not_increment_on_failed_send() {
        let (tx, rx) = mpsc::channel::<u32>();

        // Simulate worker death: `transact_batch` returned early because
        // `evm_for_ctx()` returned `None`, dropping the Receiver.
        drop(rx);

        let mut executing = 0usize;

        // --- BEGIN: verbatim buggy pattern from spawn_all lines 123-125 ---
        let _ = tx.send(42u32); // send to dead channel → Err(SendError), ignored
        executing += 1; // unconditionally incremented — the bug
        // --- END ---

        // With the bug: executing == 1 even though zero transactions were delivered.
        // A correct implementation would leave executing == 0.
        assert_eq!(
            executing, 0,
            "BUG: `executing` was incremented to {executing} even though the send \
             failed — `FinishedTxExecution::executed_transactions` would over-report \
             by {executing}"
        );
    }

    /// Simulates the full round-robin dispatch loop from `spawn_all` with
    /// `max_concurrency = 2`, where one of the two worker channels is dead.
    ///
    /// Dispatching 3 transactions should yield:
    ///   - slot 0 (alive): receives tx[0] and tx[2]  → 2 successful sends
    ///   - slot 1 (dead):  send for tx[1] returns Err → 0 deliveries from that slot
    ///
    /// With the bug `executing` reaches 3 (all attempts counted).
    /// A correct implementation would report 2 (only successful sends).
    ///
    /// This test **FAILS** with the current code, demonstrating the over-counting.
    #[test]
    fn test_spawn_all_loop_overcounts_when_one_worker_is_dead() {
        let (tx_alive, rx_alive) = mpsc::channel::<u32>();

        // Second worker dies immediately (models evm_for_ctx() → None).
        let (tx_dead, rx_dead) = mpsc::channel::<u32>();
        drop(rx_dead);

        let handles = vec![tx_alive, tx_dead];
        let max_concurrency = 2usize;
        let mut executing: usize = 0;

        let items = [10u32, 20u32, 30u32];

        for item in items {
            let task_idx = executing % max_concurrency;

            // Verbatim buggy logic from spawn_all:
            let _ = handles[task_idx].send(item);
            executing += 1;
        }

        // item 10 → slot 0 (alive)  → ok  → executing = 1
        // item 20 → slot 1 (dead)   → Err → executing = 2  ← BUG: should not count
        // item 30 → slot 0 (alive)  → ok  → executing = 3

        let actually_delivered = rx_alive.try_iter().count();
        assert_eq!(actually_delivered, 2, "alive worker should have received exactly 2 items");

        // `executing` is now 3, but only 2 transactions were actually delivered.
        // The FinishedTxExecution event would report 3 — one phantom transaction.
        assert_eq!(
            executing,
            actually_delivered,
            "BUG: `executing` ({executing}) does not match actually delivered \
             transactions ({actually_delivered}); over-count = {}",
            executing.saturating_sub(actually_delivered)
        );
    }

    /// Confirms that a dead worker channel (dropped Receiver) makes every
    /// subsequent send return `Err(SendError)`.
    ///
    /// This is the precondition for the bug: `evm_for_ctx()` returning `None`
    /// causes `transact_batch` to return early, dropping the `Receiver`, and
    /// all sends to that slot fail silently.
    #[test]
    fn test_dead_worker_receiver_causes_send_errors() {
        let (tx, rx) = mpsc::channel::<u32>();

        // Worker exits early (evm_for_ctx returned None), dropping receiver.
        drop(rx);

        for i in 0..4u32 {
            assert!(
                tx.send(i).is_err(),
                "send #{i} to dropped receiver must return Err(SendError)"
            );
        }
    }

    /// Shows what the corrected counting logic should look like:
    /// `executing` is only incremented when `send` succeeds.
    ///
    /// This test PASSES and documents the intended fix.
    #[test]
    fn test_correct_counting_only_increments_on_successful_send() {
        let (tx_alive, rx_alive) = mpsc::channel::<u32>();

        let (tx_dead, rx_dead) = mpsc::channel::<u32>();
        drop(rx_dead);

        let handles = vec![tx_alive, tx_dead];
        let max_concurrency = 2usize;

        // Use a separate index for round-robin advancement so that dead slots
        // are still advanced, but the count only reflects successes.
        let mut round_robin_idx: usize = 0;
        let mut successful_sends: usize = 0;

        let items = [10u32, 20u32, 30u32];

        for item in items {
            let task_idx = round_robin_idx % max_concurrency;

            // Fixed logic: only count when the send actually succeeds.
            if handles[task_idx].send(item).is_ok() {
                successful_sends += 1;
            }
            round_robin_idx += 1; // advance round-robin regardless
        }

        let actually_delivered = rx_alive.try_iter().count();

        // With the fix, successful_sends == actually_delivered (both == 2).
        assert_eq!(
            successful_sends, actually_delivered,
            "correct implementation: successful_sends ({successful_sends}) must equal \
             actually_delivered ({actually_delivered})"
        );
        assert_eq!(successful_sends, 2);
    }
}
