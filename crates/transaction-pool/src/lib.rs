//! Reth's transaction pool implementation.
//!
//! This crate provides a generic transaction pool implementation.
//!
//! ## Functionality
//!
//! The transaction pool is responsible for
//!
//!    - recording incoming transactions
//!    - providing existing transactions
//!    - ordering and providing the best transactions for block production
//!    - monitoring memory footprint and enforce pool size limits
//!    - storing blob data for transactions in a separate blobstore on insertion
//!
//! ## Assumptions
//!
//! ### Transaction type
//!
//! The pool expects certain ethereum related information from the generic transaction type of the
//! pool ([`PoolTransaction`]), this includes gas price, base fee (EIP-1559 transactions), nonce
//! etc. It makes no assumptions about the encoding format, but the transaction type must report its
//! size so pool size limits (memory) can be enforced.
//!
//! ### Transaction ordering
//!
//! The pending pool contains transactions that can be mined on the current state.
//! The order in which they're returned are determined by a `Priority` value returned by the
//! `TransactionOrdering` type this pool is configured with.
//!
//! This is only used in the _pending_ pool to yield the best transactions for block production. The
//! _base pool_ is ordered by base fee, and the _queued pool_ by current distance.
//!
//! ### Validation
//!
//! The pool itself does not validate incoming transactions, instead this should be provided by
//! implementing `TransactionsValidator`. Only transactions that the validator returns as valid are
//! included in the pool. It is assumed that transaction that are in the pool are either valid on
//! the current state or could become valid after certain state changes. Transactions that can never
//! become valid (e.g. nonce lower than current on chain nonce) will never be added to the pool and
//! instead are discarded right away.
//!
//! ### State Changes
//!
//! Once a new block is mined, the pool needs to be updated with a changeset in order to:
//!
//!   - remove mined transactions
//!   - update using account changes: balance changes
//!   - base fee updates
//!
//! ## Implementation details
//!
//! The `TransactionPool` trait exposes all externally used functionality of the pool, such as
//! inserting, querying specific transactions by hash or retrieving the best transactions.
//! In addition, it enables the registration of event listeners that are notified of state changes.
//! Events are communicated via channels.
//!
//! ### Architecture
//!
//! The final `TransactionPool` is made up of two layers:
//!
//! The lowest layer is the actual pool implementations that manages (validated) transactions:
//! [`TxPool`](crate::pool::txpool::TxPool). This is contained in a higher level pool type that
//! guards the low level pool and handles additional listeners or metrics: [`PoolInner`].
//!
//! The transaction pool will be used by separate consumers (RPC, P2P), to make sharing easier, the
//! [`Pool`] type is just an `Arc` wrapper around `PoolInner`. This is the usable type that provides
//! the `TransactionPool` interface.
//!
//!
//! ## Blob Transactions
//!
//! Blob transaction can be quite large hence they are stored in a separate blobstore. The pool is
//! responsible for inserting blob data for new transactions into the blobstore.
//! See also [`ValidTransaction`](validate::ValidTransaction)
//!
//!
//! ## Examples
//!
//! Listen for new transactions and print them:
//!
//! ```
//! use reth_chainspec::MAINNET;
//! use reth_storage_api::StateProviderFactory;
//! use reth_tasks::TokioTaskExecutor;
//! use reth_chainspec::ChainSpecProvider;
//! use reth_transaction_pool::{TransactionValidationTaskExecutor, Pool, TransactionPool};
//! use reth_transaction_pool::blobstore::InMemoryBlobStore;
//! use reth_chainspec::EthereumHardforks;
//! async fn t<C>(client: C)  where C: ChainSpecProvider<ChainSpec: EthereumHardforks> + StateProviderFactory + Clone + 'static{
//!     let blob_store = InMemoryBlobStore::default();
//!     let pool = Pool::eth_pool(
//!         TransactionValidationTaskExecutor::eth(client, blob_store.clone(), TokioTaskExecutor::default()),
//!         blob_store,
//!         Default::default(),
//!     );
//!   let mut transactions = pool.pending_transactions_listener();
//!   tokio::task::spawn( async move {
//!      while let Some(tx) = transactions.recv().await {
//!          println!("New transaction: {:?}", tx);
//!      }
//!   });
//!
//!   // do something useful with the pool, like RPC integration
//!
//! # }
//! ```
//!
//! Spawn maintenance task to keep the pool updated
//!
//! ```
//! use futures_util::Stream;
//! use reth_chain_state::CanonStateNotification;
//! use reth_chainspec::{MAINNET, ChainSpecProvider, ChainSpec};
//! use reth_storage_api::{BlockReaderIdExt, StateProviderFactory};
//! use reth_tasks::TokioTaskExecutor;
//! use reth_tasks::TaskSpawner;
//! use reth_tasks::TaskManager;
//! use reth_transaction_pool::{TransactionValidationTaskExecutor, Pool};
//! use reth_transaction_pool::blobstore::InMemoryBlobStore;
//! use reth_transaction_pool::maintain::{maintain_transaction_pool_future};
//!
//!  async fn t<C, St>(client: C, stream: St)
//!    where C: StateProviderFactory + BlockReaderIdExt + ChainSpecProvider<ChainSpec = ChainSpec> + Clone + 'static,
//!     St: Stream<Item = CanonStateNotification> + Send + Unpin + 'static,
//!     {
//!     let blob_store = InMemoryBlobStore::default();
//!     let rt = tokio::runtime::Runtime::new().unwrap();
//!     let manager = TaskManager::new(rt.handle().clone());
//!     let executor = manager.executor();
//!     let pool = Pool::eth_pool(
//!         TransactionValidationTaskExecutor::eth(client.clone(), blob_store.clone(), executor.clone()),
//!         blob_store,
//!         Default::default(),
//!     );
//!
//!   // spawn a task that listens for new blocks and updates the pool's transactions, mined transactions etc..
//!   tokio::task::spawn(maintain_transaction_pool_future(client, pool, stream, executor.clone(), Default::default()));
//!
//! # }
//! ```
//!
//! ## Feature Flags
//!
//! - `serde` (default): Enable serde support
//! - `test-utils`: Export utilities for testing

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub use crate::{
    blobstore::{BlobStore, BlobStoreError},
    config::{
        LocalTransactionConfig, PoolConfig, PriceBumpConfig, SubPoolLimit, DEFAULT_PRICE_BUMP,
        DEFAULT_TXPOOL_ADDITIONAL_VALIDATION_TASKS, MAX_NEW_PENDING_TXS_NOTIFICATIONS,
        REPLACE_BLOB_PRICE_BUMP, TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER,
        TXPOOL_SUBPOOL_MAX_SIZE_MB_DEFAULT, TXPOOL_SUBPOOL_MAX_TXS_DEFAULT,
    },
    error::PoolResult,
    ordering::{CoinbaseTipOrdering, Priority, TransactionOrdering},
    pool::{
        blob_tx_priority, fee_delta, state::SubPool, AllTransactionsEvents, FullTransactionEvent,
        NewTransactionEvent, TransactionEvent, TransactionEvents, TransactionListenerKind,
    },
    traits::*,
    validate::{
        EthTransactionValidator, TransactionValidationOutcome, TransactionValidationTaskExecutor,
        TransactionValidator, ValidPoolTransaction,
    },
};
use crate::{identifier::TransactionId, pool::PoolInner};
use alloy_eips::{
    eip4844::{BlobAndProofV1, BlobAndProofV2},
    eip7594::BlobTransactionSidecarVariant,
};
use alloy_primitives::{Address, TxHash, B256, U256};
use aquamarine as _;
use reth_chainspec::{ChainSpecProvider, EthereumHardforks};
use reth_eth_wire_types::HandleMempoolData;
use reth_execution_types::ChangedAccount;
use reth_primitives_traits::{Block, Recovered};
use reth_storage_api::StateProviderFactory;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU8},
        Arc,
    },
};
use tokio::sync::{mpsc::Receiver, Mutex};
use tracing::{instrument, trace};

pub mod error;
pub mod maintain;
pub mod metrics;
pub mod noop;
pub mod pool;
pub mod validate;

pub mod blobstore;
mod config;
pub mod identifier;
mod ordering;
mod traits;

#[cfg(any(test, feature = "test-utils"))]
/// Common test helpers for mocking a pool
pub mod test_utils;

/// Type alias for default ethereum transaction pool
pub type EthTransactionPool<Client, S> = Pool<
    TransactionValidationTaskExecutor<EthTransactionValidator<Client, EthPooledTransaction>>,
    CoinbaseTipOrdering<EthPooledTransaction>,
    S,
>;

/// A shareable, generic, customizable `TransactionPool` implementation.
#[derive(Debug)]
pub struct Pool<V, T: TransactionOrdering, S> {
    /// Arc'ed instance of the pool internals
    pool: Arc<PoolInner<V, T, S>>,
    batch_insert_task_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    batch_insert_task_running: Arc<AtomicBool>,
}

static ENABLE_BATCH_INSERT: AtomicU8 = AtomicU8::new(2);
fn get_enable_batch_insert() -> bool {
    let val = ENABLE_BATCH_INSERT.load(std::sync::atomic::Ordering::Acquire);
    if val == 2 {
        let env = std::env::var("RETH_TXPOOL_BATCH_INSERT").unwrap_or("false".to_string());
        ENABLE_BATCH_INSERT.store(env.parse().unwrap_or(0), std::sync::atomic::Ordering::Release);
        return ENABLE_BATCH_INSERT.load(std::sync::atomic::Ordering::Acquire) == 1;
    }
    val == 1
}

// === impl Pool ===

impl<V, T, S> Pool<V, T, S>
where
    V: TransactionValidator + 'static,
    T: TransactionOrdering<Transaction = <V as TransactionValidator>::Transaction>,
    S: BlobStore,
{
    /// Create a new transaction pool instance.
    pub fn new(validator: V, ordering: T, blob_store: S, config: PoolConfig) -> Self {
        let pool = Arc::new(PoolInner::new(validator, ordering, blob_store, config));
        Self {
            pool,
            batch_insert_task_handle: Arc::new(Mutex::new(None)),
            batch_insert_task_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the wrapped pool.
    pub(crate) fn inner(&self) -> &PoolInner<V, T, S> {
        &self.pool
    }

    /// Returns the unix timestamp in milliseconds when the transaction was inserted into the pool.
    pub fn txn_insert_time(&self, txn_hash: TxHash) -> Option<u64> {
        self.inner().txn_insert_time.get(&txn_hash).map(|t| *t)
    }

    /// Get the config the pool was configured with.
    pub fn config(&self) -> &PoolConfig {
        self.inner().config()
    }

    /// Returns future that validates all transactions in the given iterator.
    ///
    /// This returns the validated transactions in the iterator's order.
    async fn validate_all(
        &self,
        origin: TransactionOrigin,
        transactions: impl IntoIterator<Item = V::Transaction> + Send,
    ) -> Vec<(TxHash, TransactionValidationOutcome<V::Transaction>)> {
        self.pool
            .validator()
            .validate_transactions(transactions.into_iter().map(|tx| (origin, tx)).collect())
            .await
            .into_iter()
            .map(|tx| (tx.tx_hash(), tx))
            .collect()
    }

    /// Validates the given transaction
    async fn validate(
        &self,
        origin: TransactionOrigin,
        transaction: V::Transaction,
    ) -> (TxHash, TransactionValidationOutcome<V::Transaction>) {
        let hash = *transaction.hash();

        let outcome = self.pool.validator().validate_transaction(origin, transaction).await;
        (hash, outcome)
    }

    /// Number of transactions in the entire pool
    pub fn len(&self) -> usize {
        self.pool.len()
    }

    /// Whether the pool is empty
    pub fn is_empty(&self) -> bool {
        self.pool.is_empty()
    }

    /// Returns whether or not the pool is over its configured size and transaction count limits.
    pub fn is_exceeded(&self) -> bool {
        self.pool.is_exceeded()
    }

    /// Returns the configured blob store.
    pub fn blob_store(&self) -> &S {
        self.pool.blob_store()
    }
}

impl<Client, S> EthTransactionPool<Client, S>
where
    Client:
        ChainSpecProvider<ChainSpec: EthereumHardforks> + StateProviderFactory + Clone + 'static,
    S: BlobStore,
{
    /// Returns a new [`Pool`] that uses the default [`TransactionValidationTaskExecutor`] when
    /// validating [`EthPooledTransaction`]s and ords via [`CoinbaseTipOrdering`]
    ///
    /// # Example
    ///
    /// ```
    /// use reth_chainspec::MAINNET;
    /// use reth_storage_api::StateProviderFactory;
    /// use reth_tasks::TokioTaskExecutor;
    /// use reth_chainspec::ChainSpecProvider;
    /// use reth_transaction_pool::{
    ///     blobstore::InMemoryBlobStore, Pool, TransactionValidationTaskExecutor,
    /// };
    /// use reth_chainspec::EthereumHardforks;
    /// # fn t<C>(client: C)  where C: ChainSpecProvider<ChainSpec: EthereumHardforks> + StateProviderFactory + Clone + 'static {
    /// let blob_store = InMemoryBlobStore::default();
    /// let pool = Pool::eth_pool(
    ///     TransactionValidationTaskExecutor::eth(
    ///         client,
    ///         blob_store.clone(),
    ///         TokioTaskExecutor::default(),
    ///     ),
    ///     blob_store,
    ///     Default::default(),
    /// );
    /// # }
    /// ```
    pub fn eth_pool(
        validator: TransactionValidationTaskExecutor<
            EthTransactionValidator<Client, EthPooledTransaction>,
        >,
        blob_store: S,
        config: PoolConfig,
    ) -> Self {
        Self::new(validator, CoinbaseTipOrdering::default(), blob_store, config)
    }
}

/// implements the `TransactionPool` interface for various transaction pool API consumers.
impl<V, T, S> TransactionPool for Pool<V, T, S>
where
    V: TransactionValidator + 'static,
    <V as TransactionValidator>::Transaction: EthPoolTransaction,
    T: TransactionOrdering<Transaction = <V as TransactionValidator>::Transaction>,
    S: BlobStore,
{
    type Transaction = T::Transaction;

    fn pool_size(&self) -> PoolSize {
        self.pool.size()
    }

    fn block_info(&self) -> BlockInfo {
        self.pool.block_info()
    }

    async fn add_transaction_and_subscribe(
        &self,
        origin: TransactionOrigin,
        transaction: Self::Transaction,
    ) -> PoolResult<TransactionEvents> {
        let (_, tx) = self.validate(origin, transaction).await;
        self.pool.add_transaction_and_subscribe(origin, tx)
    }

    async fn add_transaction(
        &self,
        origin: TransactionOrigin,
        transaction: Self::Transaction,
    ) -> PoolResult<TxHash> {
        let start = std::time::Instant::now();
        let pool = self.pool.clone();
        if self.batch_insert_task_running.load(std::sync::atomic::Ordering::Acquire) {
            return self.pool.send_transaction(origin, transaction).await
        }
        if get_enable_batch_insert() {
            {
                let mut handle = self.batch_insert_task_handle.lock().await;
                if handle.is_none() {
                    tracing::info!("Batch insert task started");
                    *handle = Some(std::thread::spawn(move || {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(pool.batch_add_transactions_task());
                    }));
                }
                self.batch_insert_task_running.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        };
        let (_, tx) = self.validate(origin, transaction).await;
        self.pool.blob_store_metrics.txn_validation_time.record(start.elapsed().as_millis() as f64);
        let mut results = self.pool.add_transactions(origin, std::iter::once(tx));
        self.pool
            .blob_store_metrics
            .txn_val_insertion_time
            .record(start.elapsed().as_millis() as f64);
        results.pop().expect("result length is the same as the input")
    }

    async fn add_transactions(
        &self,
        origin: TransactionOrigin,
        transactions: Vec<Self::Transaction>,
    ) -> Vec<PoolResult<TxHash>> {
        if transactions.is_empty() {
            return Vec::new()
        }
        let validated = self.validate_all(origin, transactions).await;
        self.pool.add_transactions(origin, validated.into_iter().map(|(_, tx)| tx))
    }

    fn transaction_event_listener(&self, tx_hash: TxHash) -> Option<TransactionEvents> {
        self.pool.add_transaction_event_listener(tx_hash)
    }

    fn all_transactions_event_listener(&self) -> AllTransactionsEvents<Self::Transaction> {
        self.pool.add_all_transactions_event_listener()
    }

    fn pending_transactions_listener_for(&self, kind: TransactionListenerKind) -> Receiver<TxHash> {
        self.pool.add_pending_listener(kind)
    }

    fn blob_transaction_sidecars_listener(&self) -> Receiver<NewBlobSidecar> {
        self.pool.add_blob_sidecar_listener()
    }

    fn new_transactions_listener_for(
        &self,
        kind: TransactionListenerKind,
    ) -> Receiver<NewTransactionEvent<Self::Transaction>> {
        self.pool.add_new_transaction_listener(kind)
    }

    fn pooled_transaction_hashes(&self) -> Vec<TxHash> {
        self.pool.pooled_transactions_hashes()
    }

    fn pooled_transaction_hashes_max(&self, max: usize) -> Vec<TxHash> {
        self.pooled_transaction_hashes().into_iter().take(max).collect()
    }

    fn pooled_transactions(&self) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.pooled_transactions()
    }

    fn pooled_transactions_max(
        &self,
        max: usize,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.pooled_transactions_max(max)
    }

    fn get_pooled_transaction_elements(
        &self,
        tx_hashes: Vec<TxHash>,
        limit: GetPooledTransactionLimit,
    ) -> Vec<<<V as TransactionValidator>::Transaction as PoolTransaction>::Pooled> {
        self.pool.get_pooled_transaction_elements(tx_hashes, limit)
    }

    fn get_pooled_transaction_element(
        &self,
        tx_hash: TxHash,
    ) -> Option<Recovered<<<V as TransactionValidator>::Transaction as PoolTransaction>::Pooled>>
    {
        self.pool.get_pooled_transaction_element(tx_hash)
    }

    fn best_transactions(
        &self,
    ) -> Box<dyn BestTransactions<Item = Arc<ValidPoolTransaction<Self::Transaction>>>> {
        Box::new(self.pool.best_transactions())
    }

    fn best_transactions_with_attributes(
        &self,
        best_transactions_attributes: BestTransactionsAttributes,
    ) -> Box<dyn BestTransactions<Item = Arc<ValidPoolTransaction<Self::Transaction>>>> {
        self.pool.best_transactions_with_attributes(best_transactions_attributes)
    }

    fn pending_transactions(&self) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.pending_transactions()
    }

    fn pending_transactions_max(
        &self,
        max: usize,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.pending_transactions_max(max)
    }

    fn queued_transactions(&self) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.queued_transactions()
    }

    fn all_transactions(&self) -> AllPoolTransactions<Self::Transaction> {
        self.pool.all_transactions()
    }

    fn remove_transactions(
        &self,
        hashes: Vec<TxHash>,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.remove_transactions(hashes)
    }

    fn remove_transactions_and_descendants(
        &self,
        hashes: Vec<TxHash>,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.remove_transactions_and_descendants(hashes)
    }

    fn remove_transactions_by_sender(
        &self,
        sender: Address,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.remove_transactions_by_sender(sender)
    }

    fn retain_unknown<A>(&self, announcement: &mut A)
    where
        A: HandleMempoolData,
    {
        self.pool.retain_unknown(announcement)
    }

    fn get(&self, tx_hash: &TxHash) -> Option<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.inner().get(tx_hash)
    }

    fn get_all(&self, txs: Vec<TxHash>) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.inner().get_all(txs)
    }

    fn on_propagated(&self, txs: PropagatedTransactions) {
        self.inner().on_propagated(txs)
    }

    fn get_transactions_by_sender(
        &self,
        sender: Address,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_transactions_by_sender(sender)
    }

    fn get_pending_transactions_with_predicate(
        &self,
        predicate: impl FnMut(&ValidPoolTransaction<Self::Transaction>) -> bool,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.pending_transactions_with_predicate(predicate)
    }

    fn get_pending_transactions_by_sender(
        &self,
        sender: Address,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_pending_transactions_by_sender(sender)
    }

    fn get_queued_transactions_by_sender(
        &self,
        sender: Address,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_queued_transactions_by_sender(sender)
    }

    fn get_highest_transaction_by_sender(
        &self,
        sender: Address,
    ) -> Option<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_highest_transaction_by_sender(sender)
    }

    fn get_highest_consecutive_transaction_by_sender(
        &self,
        sender: Address,
        on_chain_nonce: u64,
    ) -> Option<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_highest_consecutive_transaction_by_sender(sender, on_chain_nonce)
    }

    fn get_transaction_by_sender_and_nonce(
        &self,
        sender: Address,
        nonce: u64,
    ) -> Option<Arc<ValidPoolTransaction<Self::Transaction>>> {
        let transaction_id = TransactionId::new(self.pool.get_sender_id(sender), nonce);

        self.inner().get_pool_data().all().get(&transaction_id).map(|tx| tx.transaction.clone())
    }

    fn get_transactions_by_origin(
        &self,
        origin: TransactionOrigin,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_transactions_by_origin(origin)
    }

    /// Returns all pending transactions filtered by [`TransactionOrigin`]
    fn get_pending_transactions_by_origin(
        &self,
        origin: TransactionOrigin,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        self.pool.get_pending_transactions_by_origin(origin)
    }

    fn unique_senders(&self) -> HashSet<Address> {
        self.pool.unique_senders()
    }

    fn get_blob(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        self.pool.blob_store().get(tx_hash)
    }

    fn get_all_blobs(
        &self,
        tx_hashes: Vec<TxHash>,
    ) -> Result<Vec<(TxHash, Arc<BlobTransactionSidecarVariant>)>, BlobStoreError> {
        self.pool.blob_store().get_all(tx_hashes)
    }

    fn get_all_blobs_exact(
        &self,
        tx_hashes: Vec<TxHash>,
    ) -> Result<Vec<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        self.pool.blob_store().get_exact(tx_hashes)
    }

    fn get_blobs_for_versioned_hashes_v1(
        &self,
        versioned_hashes: &[B256],
    ) -> Result<Vec<Option<BlobAndProofV1>>, BlobStoreError> {
        self.pool.blob_store().get_by_versioned_hashes_v1(versioned_hashes)
    }

    fn get_blobs_for_versioned_hashes_v2(
        &self,
        versioned_hashes: &[B256],
    ) -> Result<Option<Vec<BlobAndProofV2>>, BlobStoreError> {
        self.pool.blob_store().get_by_versioned_hashes_v2(versioned_hashes)
    }
}

impl<V, T, S> TransactionPoolExt for Pool<V, T, S>
where
    V: TransactionValidator + 'static,
    <V as TransactionValidator>::Transaction: EthPoolTransaction,
    T: TransactionOrdering<Transaction = <V as TransactionValidator>::Transaction>,
    S: BlobStore,
{
    #[instrument(skip(self), target = "txpool")]
    fn set_block_info(&self, info: BlockInfo) {
        trace!(target: "txpool", "updating pool block info");
        self.pool.set_block_info(info)
    }

    fn on_canonical_state_change<B>(&self, update: CanonicalStateUpdate<'_, B>)
    where
        B: Block,
    {
        self.pool.on_canonical_state_change(update);
    }

    fn update_accounts(&self, accounts: Vec<ChangedAccount>) {
        self.pool.update_accounts(accounts);
    }

    fn delete_blob(&self, tx: TxHash) {
        self.pool.delete_blob(tx)
    }

    fn delete_blobs(&self, txs: Vec<TxHash>) {
        self.pool.delete_blobs(txs)
    }

    fn cleanup_blobs(&self) {
        self.pool.cleanup_blobs()
    }
}

impl<V: 'static, T: TransactionOrdering, S> Clone for Pool<V, T, S> {
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            batch_insert_task_handle: self.batch_insert_task_handle.clone(),
            batch_insert_task_running: self.batch_insert_task_running.clone(),
        }
    }
}
