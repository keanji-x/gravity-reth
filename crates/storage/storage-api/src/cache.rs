use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{Address, B256, U256};
use core::{
    ops::Deref,
    sync::atomic::{AtomicU64, Ordering},
};
use dashmap::DashMap;
use metrics::Gauge;
use metrics_derive::Metrics;
use once_cell::sync::Lazy;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use reth_primitives_traits::Account;
use reth_trie_common::{nested_trie::Node, updates::TrieUpdatesV2, Nibbles};
use revm_bytecode::Bytecode;
use revm_database::{states::StorageSlot, BundleAccount, OriginalValuesKnown};
use std::{
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Metrics)]
#[metrics(scope = "storage")]
struct CacheMetrics {
    /// Block cache hit ratio
    block_cache_hit_ratio: Gauge,
    /// Trie cache hit ratio
    trie_cache_hit_ratio: Gauge,
    /// Number of cached items
    cache_num_items: Gauge,
    /// Lastest pre-merged block number
    latest_merged_block_number: Gauge,
    /// Latest stored block number
    latest_stored_block_number: Gauge,
}

#[derive(Default)]
struct CacheMetricsReporter {
    block_cache_hit_record: HitRecorder,
    trie_cache_hit_record: HitRecorder,
    cached_items: AtomicU64,
    merged_block_number: AtomicU64,
    stored_block_number: AtomicU64,
    metrics: CacheMetrics,
}

#[derive(Default)]
struct HitRecorder {
    not_hit_cnt: AtomicU64,
    hit_cnt: AtomicU64,
}

impl HitRecorder {
    fn not_hit(&self) {
        self.not_hit_cnt.fetch_add(1, Ordering::Relaxed);
    }

    fn hit(&self) {
        self.hit_cnt.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) -> Option<f64> {
        let not_hit_cnt = self.not_hit_cnt.swap(0, Ordering::Relaxed);
        let hit_cnt = self.hit_cnt.swap(0, Ordering::Relaxed);
        let visit_cnt = not_hit_cnt + hit_cnt;
        if visit_cnt > 0 {
            Some(hit_cnt as f64 / visit_cnt as f64)
        } else {
            None
        }
    }
}

impl CacheMetricsReporter {
    fn report(&self) {
        if let Some(hit_ratio) = self.block_cache_hit_record.report() {
            self.metrics.block_cache_hit_ratio.set(hit_ratio);
        }
        if let Some(hit_ratio) = self.trie_cache_hit_record.report() {
            self.metrics.trie_cache_hit_ratio.set(hit_ratio);
        }
        let cached_items = self.cached_items.load(Ordering::Relaxed) as f64;
        self.metrics.cache_num_items.set(cached_items);
        self.metrics
            .latest_merged_block_number
            .set(self.merged_block_number.load(Ordering::Relaxed) as f64);
        self.metrics
            .latest_stored_block_number
            .set(self.stored_block_number.load(Ordering::Relaxed) as f64);
    }
}

struct ValueWithTip<V> {
    value: V,
    block_number: u64,
}

impl<V> ValueWithTip<V> {
    fn new(value: V, block_number: u64) -> Self {
        Self { value, block_number }
    }
}

#[derive(Default)]
pub struct PersistBlockCacheInner {
    accounts: DashMap<Address, ValueWithTip<Account>>,
    storage: DashMap<Address, DashMap<U256, ValueWithTip<U256>>>,
    contracts: DashMap<B256, Bytecode>,
    account_trie: DashMap<Nibbles, ValueWithTip<Node>>,
    storage_trie: DashMap<B256, DashMap<Nibbles, ValueWithTip<Node>>>,
    persist_block_number: Mutex<Option<u64>>,
    metrics: CacheMetricsReporter,
    daemon_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug)]
pub struct PersistBlockCache(Arc<PersistBlockCacheInner>);

impl Deref for PersistBlockCache {
    type Target = PersistBlockCacheInner;

    fn deref(&self) -> &Self::Target {
        &self.0.as_ref()
    }
}

impl std::fmt::Debug for PersistBlockCacheInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistBlockCache")
            .field("num_cached", &self.metrics.cached_items.load(Ordering::Relaxed))
            .finish()
    }
}

impl PersistBlockCacheInner {
    fn entry_count(&self) -> usize {
        let mut num_items = self.accounts.len();
        num_items += self.storage.iter().map(|s| s.len()).sum::<usize>();
        num_items += self.account_trie.len();
        num_items += self.storage_trie.iter().map(|s| s.len()).sum::<usize>();
        num_items
    }
}

pub static PERSIST_BLOCK_CACHE: Lazy<PersistBlockCache> = Lazy::new(|| PersistBlockCache::new());

impl PersistBlockCache {
    pub fn new() -> Self {
        let inner = Arc::new(PersistBlockCacheInner::default());

        let weak_inner = Arc::downgrade(&inner);
        let handle = thread::spawn(move || {
            let interval = Duration::from_secs(15); // 15s
            loop {
                thread::sleep(interval);
                if let Some(inner) = weak_inner.upgrade() {
                    let num_items = inner.entry_count();
                    inner.metrics.cached_items.store(num_items as u64, Ordering::Release);
                    inner.metrics.report();
                } else {
                    break;
                }
            }
        });
        inner.daemon_handle.lock().unwrap().replace(handle);

        Self(inner)
    }

    pub fn basic_account(&self, address: &Address) -> Option<Account> {
        if let Some(value) = self.accounts.get(address) {
            self.metrics.block_cache_hit_record.hit();
            Some(value.value.clone())
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    pub fn bytecode_by_hash(&self, code_hash: &B256) -> Option<Bytecode> {
        if let Some(value) = self.contracts.get(code_hash) {
            self.metrics.block_cache_hit_record.hit();
            Some(value.clone())
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    pub fn storage(&self, address: &Address, slot: &U256) -> Option<U256> {
        if let Some(storage) = self.storage.get(address) {
            if let Some(value) = storage.get(slot) {
                self.metrics.block_cache_hit_record.hit();
                Some(value.value.clone())
            } else {
                self.metrics.block_cache_hit_record.not_hit();
                None
            }
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    pub fn trie_account(&self, nibbles: &Nibbles) -> Option<Node> {
        if let Some(value) = self.account_trie.get(nibbles) {
            self.metrics.trie_cache_hit_record.hit();
            Some(value.value.clone())
        } else {
            self.metrics.trie_cache_hit_record.not_hit();
            None
        }
    }

    pub fn trie_storage(&self, hash_address: &B256, nibbles: &Nibbles) -> Option<Node> {
        if let Some(storage) = self.storage_trie.get(hash_address) {
            if let Some(value) = storage.get(nibbles) {
                self.metrics.trie_cache_hit_record.hit();
                Some(value.value.clone())
            } else {
                self.metrics.trie_cache_hit_record.not_hit();
                None
            }
        } else {
            self.metrics.trie_cache_hit_record.not_hit();
            None
        }
    }

    pub fn persist_tip(&self, block_number: u64) {
        self.metrics.stored_block_number.store(block_number, Ordering::Relaxed);
        let mut guard = self.persist_block_number.lock().unwrap();
        if let Some(ref mut persist_block_number) = *guard {
            if block_number != *persist_block_number + 1 {
                panic!(
                    "Persist uncontinuous block, expect: {}, actual: {}",
                    *persist_block_number + 1,
                    block_number
                );
            }
            *persist_block_number = block_number;
        } else {
            *guard = Some(block_number);
        }
    }

    pub fn write_state_changes<'a>(
        &self,
        block_number: u64,
        is_value_known: OriginalValuesKnown,
        state: impl IntoParallelIterator<Item = (&'a Address, &'a BundleAccount)>,
        contracts: impl IntoParallelIterator<Item = (&'a B256, &'a Bytecode)>,
    ) {
        self.metrics.merged_block_number.store(block_number, Ordering::Relaxed);

        // Write bytecode
        contracts
            .into_par_iter()
            .filter(|(b, _)| **b != KECCAK_EMPTY)
            .map(|(b, code)| (*b, code.clone()))
            .for_each(|(hash, bytecode)| {
                self.contracts.insert(hash, bytecode);
            });

        state.into_par_iter().for_each(|(address, account)| {
            // Append account info if it is changed.
            let was_destroyed = account.was_destroyed();
            if is_value_known.is_not_known() || account.is_info_changed() {
                // write account to database.
                let info = account.info.clone();
                if let Some(info) = info {
                    self.accounts.insert(*address, ValueWithTip::new(info.into(), block_number));
                } else {
                    self.accounts.remove(address);
                }
            }

            if was_destroyed {
                self.storage.remove(address);
            }
            let write_slot = |kv: (U256, StorageSlot)| {
                let (slot, slot_value) = kv;
                // If storage was destroyed that means that storage was wiped.
                // In that case we need to check if present storage value is different then ZERO.
                let destroyed_and_not_zero = was_destroyed && !slot_value.present_value.is_zero();

                // If account is not destroyed check if original values was changed,
                // so we can update it.
                let not_destroyed_and_changed = !was_destroyed && slot_value.is_changed();

                if is_value_known.is_not_known() ||
                    destroyed_and_not_zero ||
                    not_destroyed_and_changed
                {
                    let value = slot_value.present_value;
                    if value.is_zero() {
                        // delete slot
                        if let Some(storage) = self.storage.get(address) {
                            storage.remove(&slot);
                        }
                    } else {
                        if let Some(storage) = self.storage.get(address) {
                            storage.insert(slot, ValueWithTip::new(value, block_number));
                        } else {
                            match self.storage.entry(*address) {
                                dashmap::Entry::Occupied(entry) => {
                                    entry
                                        .get()
                                        .insert(slot, ValueWithTip::new(value, block_number));
                                }
                                dashmap::Entry::Vacant(entry) => {
                                    let data = DashMap::new();
                                    data.insert(slot, ValueWithTip::new(value, block_number));
                                    entry.insert(data);
                                }
                            }
                        }
                    }
                }
            };

            // Append storage changes
            // Note: Assumption is that revert is going to remove whole plain storage from
            // database so we can check if plain state was wiped or not.
            if account.storage.len() > 256 {
                account.storage.par_iter().map(|(k, v)| (*k, *v)).for_each(write_slot);
            } else {
                for kv in account.storage.iter().map(|(k, v)| (*k, *v)) {
                    write_slot(kv);
                }
            }
        })
    }

    pub fn write_trie_updates(&self, input: &TrieUpdatesV2, block_number: u64) {
        input.removed_nodes.par_iter().for_each(|path| {
            self.account_trie.remove(path);
        });
        input.account_nodes.par_iter().for_each(|(path, node)| {
            self.account_trie
                .insert(path.clone(), ValueWithTip::new(node.clone().reset(), block_number));
        });

        let write_slot = |data: &DashMap<Nibbles, ValueWithTip<Node>>, kv: (&Nibbles, &Node)| {
            data.insert(kv.0.clone(), ValueWithTip::new(kv.1.clone().reset(), block_number));
        };
        input.storage_tries.par_iter().for_each(|(hashed_address, storage_trie_update)| {
            if storage_trie_update.is_deleted {
                self.storage_trie.remove(hashed_address);
            } else {
                if let Some(storage) = self.storage_trie.get(hashed_address) {
                    if storage_trie_update.removed_nodes.len() > 256 {
                        storage_trie_update.removed_nodes.par_iter().for_each(|path| {
                            storage.remove(path);
                        });
                    } else {
                        for path in &storage_trie_update.removed_nodes {
                            storage.remove(path);
                        }
                    }
                }

                if let Some(storage) = self.storage_trie.get(hashed_address) {
                    if storage_trie_update.storage_nodes.len() > 256 {
                        storage_trie_update.storage_nodes.par_iter().for_each(|kv| {
                            write_slot(storage.value(), kv);
                        });
                    } else {
                        for kv in &storage_trie_update.storage_nodes {
                            write_slot(storage.value(), kv);
                        }
                    }
                } else {
                    match self.storage_trie.entry(*hashed_address) {
                        dashmap::Entry::Occupied(entry) => {
                            if storage_trie_update.storage_nodes.len() > 256 {
                                storage_trie_update.storage_nodes.par_iter().for_each(|kv| {
                                    write_slot(entry.get(), kv);
                                });
                            } else {
                                for kv in &storage_trie_update.storage_nodes {
                                    write_slot(entry.get(), kv);
                                }
                            }
                        }
                        dashmap::Entry::Vacant(entry) => {
                            let data = DashMap::new();
                            if storage_trie_update.storage_nodes.len() > 256 {
                                storage_trie_update.storage_nodes.par_iter().for_each(|kv| {
                                    write_slot(&data, kv);
                                });
                            } else {
                                for kv in &storage_trie_update.storage_nodes {
                                    write_slot(&data, kv);
                                }
                            }
                            entry.insert(data);
                        }
                    }
                }
            }
        });
    }
}
