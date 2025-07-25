use alloc::vec::Vec;
use alloy_primitives::{map::B256Map, Address, Bytes, B256};
use reth_db_api::DatabaseError;
use reth_storage_errors::provider::ProviderResult;
use reth_trie_common::{
    updates::{StorageTrieUpdates, TrieUpdates, TrieUpdatesV2},
    AccountProof, HashedPostState, HashedStorage, MultiProof, MultiProofTargets, StorageMultiProof,
    StorageProof, TrieInput,
};
use std::sync::Arc;

/// A type that can compute the state root of a given post state.
#[auto_impl::auto_impl(&, Box, Arc)]
pub trait StateRootProvider: Send + Sync {
    /// Returns the state root of the `BundleState` on top of the current state.
    ///
    /// # Note
    ///
    /// It is recommended to provide a different implementation from
    /// `state_root_with_updates` since it affects the memory usage during state root
    /// computation.
    fn state_root(&self, hashed_state: HashedPostState) -> ProviderResult<B256>;

    /// Returns the state root of the `HashedPostState` on top of the current state but reuses the
    /// intermediate nodes to speed up the computation. It's up to the caller to construct the
    /// prefix sets and inform the provider of the trie paths that have changes.
    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256>;

    /// Returns the state root of the `HashedPostState` on top of the current state with trie
    /// updates to be committed to the database.
    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)>;

    /// Returns state root and trie updates.
    /// See [`StateRootProvider::state_root_from_nodes`] for more info.
    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)>;

    fn state_root_with_updates_v2(
        &self,
        state: HashedPostState,
        hashed_state_vec: Vec<Arc<HashedPostState>>,
        trie_updates_vec: Vec<Arc<TrieUpdates>>,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        let mut input = TrieInput::from_state(state);
        let mut state = HashedPostState::default();
        let mut nodes = TrieUpdates::default();
        hashed_state_vec.iter().for_each(|hashed_state| {
            state.extend_ref(hashed_state.as_ref());
        });
        trie_updates_vec.iter().for_each(|trie_updates| {
            nodes.extend_ref(trie_updates.as_ref());
        });
        input.prepend_cached(nodes, state);
        self.state_root_from_nodes_with_updates(input)
    }
}

/// A type that can compute the storage root for a given account.
#[auto_impl::auto_impl(&, Box, Arc)]
pub trait StorageRootProvider: Send + Sync {
    /// Returns the storage root of the `HashedStorage` for target address on top of the current
    /// state.
    fn storage_root(&self, address: Address, hashed_storage: HashedStorage)
        -> ProviderResult<B256>;

    /// Returns the storage proof of the `HashedStorage` for target slot on top of the current
    /// state.
    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageProof>;

    /// Returns the storage multiproof for target slots.
    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof>;
}

/// A type that can generate state proof on top of a given post state.
#[auto_impl::auto_impl(&, Box, Arc)]
pub trait StateProofProvider: Send + Sync {
    /// Get account and storage proofs of target keys in the `HashedPostState`
    /// on top of the current state.
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof>;

    /// Generate [`MultiProof`] for target hashed account and corresponding
    /// hashed storage slot keys.
    fn multiproof(
        &self,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof>;

    /// Get trie witness for provided state.
    fn witness(&self, input: TrieInput, target: HashedPostState) -> ProviderResult<Vec<Bytes>>;
}

/// Trie Writer
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait TrieWriter: Send + Sync {
    /// Writes trie updates to the database.
    ///
    /// Returns the number of entries modified.
    fn write_trie_updates(&self, trie_updates: &TrieUpdates) -> ProviderResult<usize>;
}

/// Storage Trie Writer
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait StorageTrieWriter: Send + Sync {
    /// Writes storage trie updates from the given storage trie map.
    ///
    /// First sorts the storage trie updates by the hashed address key, writing in sorted order.
    ///
    /// Returns the number of entries modified.
    fn write_storage_trie_updates(
        &self,
        storage_tries: &B256Map<StorageTrieUpdates>,
    ) -> ProviderResult<usize>;

    /// Writes storage trie updates for the given hashed address.
    fn write_individual_storage_trie_updates(
        &self,
        hashed_address: B256,
        updates: &StorageTrieUpdates,
    ) -> ProviderResult<usize>;
}

pub trait TrieWriterV2 {
    fn write(&self, input: &TrieUpdatesV2) -> Result<usize, DatabaseError>;
}
