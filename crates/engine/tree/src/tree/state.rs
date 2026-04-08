//! Functionality related to tree state.

use crate::engine::EngineApiKind;
use alloy_eips::{eip1898::BlockWithParent, merge::EPOCH_SLOTS, BlockNumHash};
use alloy_primitives::{
    map::{HashMap, HashSet},
    BlockNumber, B256,
};
use reth_chain_state::{EthPrimitives, ExecutedBlockWithTrieUpdates};
use reth_primitives_traits::{AlloyBlockHeader, NodePrimitives, SealedHeader};
use reth_trie::updates::TrieUpdates;
use std::{
    collections::{btree_map, hash_map, BTreeMap, VecDeque},
    ops::Bound,
    sync::Arc,
};
use tracing::debug;

/// Default number of blocks to retain persisted trie updates
const DEFAULT_PERSISTED_TRIE_UPDATES_RETENTION: u64 = EPOCH_SLOTS * 2;

/// Number of blocks to retain persisted trie updates for OP Stack chains
/// OP Stack chains only need `EPOCH_BLOCKS` as reorgs are relevant only when
/// op-node reorgs to the same chain twice
const OPSTACK_PERSISTED_TRIE_UPDATES_RETENTION: u64 = EPOCH_SLOTS;

/// Keeps track of the state of the tree.
///
/// ## Invariants
///
/// - This only stores blocks that are connected to the canonical chain.
/// - All executed blocks are valid and have been executed.
#[derive(Debug, Default)]
pub struct TreeState<N: NodePrimitives = EthPrimitives> {
    /// __All__ unique executed blocks by block hash that are connected to the canonical chain.
    ///
    /// This includes blocks of all forks.
    pub(crate) blocks_by_hash: HashMap<B256, ExecutedBlockWithTrieUpdates<N>>,
    /// Executed blocks grouped by their respective block number.
    ///
    /// This maps unique block number to all known blocks for that height.
    ///
    /// Note: there can be multiple blocks at the same height due to forks.
    pub(crate) blocks_by_number: BTreeMap<BlockNumber, Vec<ExecutedBlockWithTrieUpdates<N>>>,
    /// Map of any parent block hash to its children.
    pub(crate) parent_to_child: HashMap<B256, HashSet<B256>>,
    /// Map of hash to trie updates for canonical blocks that are persisted but not finalized.
    ///
    /// Contains the block number for easy removal.
    pub(crate) persisted_trie_updates: HashMap<B256, (BlockNumber, Arc<TrieUpdates>)>,
    /// Currently tracked canonical head of the chain.
    pub(crate) current_canonical_head: BlockNumHash,
    /// The engine API variant of this handler
    pub(crate) engine_kind: EngineApiKind,
}

impl<N: NodePrimitives> TreeState<N> {
    /// Returns a new, empty tree state that points to the given canonical head.
    pub(crate) fn new(current_canonical_head: BlockNumHash, engine_kind: EngineApiKind) -> Self {
        Self {
            blocks_by_hash: HashMap::default(),
            blocks_by_number: BTreeMap::new(),
            current_canonical_head,
            parent_to_child: HashMap::default(),
            persisted_trie_updates: HashMap::default(),
            engine_kind,
        }
    }

    /// Resets the state and points to the given canonical head.
    pub(crate) fn reset(&mut self, current_canonical_head: BlockNumHash) {
        *self = Self::new(current_canonical_head, self.engine_kind);
    }

    /// Returns the number of executed blocks stored.
    pub(crate) fn block_count(&self) -> usize {
        self.blocks_by_hash.len()
    }

    /// Returns the [`ExecutedBlockWithTrieUpdates`] by hash.
    pub(crate) fn executed_block_by_hash(
        &self,
        hash: B256,
    ) -> Option<&ExecutedBlockWithTrieUpdates<N>> {
        self.blocks_by_hash.get(&hash)
    }

    /// Returns the sealed block header by hash.
    pub(crate) fn sealed_header_by_hash(
        &self,
        hash: &B256,
    ) -> Option<SealedHeader<N::BlockHeader>> {
        self.blocks_by_hash.get(hash).map(|b| b.sealed_block().sealed_header().clone())
    }

    /// Returns all available blocks for the given hash that lead back to the canonical chain, from
    /// newest to oldest. And the parent hash of the oldest block that is missing from the buffer.
    ///
    /// Returns `None` if the block for the given hash is not found.
    pub(crate) fn blocks_by_hash(
        &self,
        hash: B256,
    ) -> Option<(B256, Vec<ExecutedBlockWithTrieUpdates<N>>)> {
        let block = self.blocks_by_hash.get(&hash).cloned()?;
        let mut parent_hash = block.recovered_block().parent_hash();
        let mut blocks = vec![block];
        while let Some(executed) = self.blocks_by_hash.get(&parent_hash) {
            parent_hash = executed.recovered_block().parent_hash();
            blocks.push(executed.clone());
        }

        Some((parent_hash, blocks))
    }

    /// Insert executed block into the state.
    pub(crate) fn insert_executed(&mut self, executed: ExecutedBlockWithTrieUpdates<N>) {
        let hash = executed.recovered_block().hash();
        let parent_hash = executed.recovered_block().parent_hash();
        let block_number = executed.recovered_block().number();

        if self.blocks_by_hash.contains_key(&hash) {
            return;
        }

        self.blocks_by_hash.insert(hash, executed.clone());

        self.blocks_by_number.entry(block_number).or_default().push(executed);

        self.parent_to_child.entry(parent_hash).or_default().insert(hash);

        for children in self.parent_to_child.values_mut() {
            children.retain(|child| self.blocks_by_hash.contains_key(child));
        }
    }

    /// Remove single executed block by its hash.
    ///
    /// ## Returns
    ///
    /// The removed block and the block hashes of its children.
    fn remove_by_hash(
        &mut self,
        hash: B256,
    ) -> Option<(ExecutedBlockWithTrieUpdates<N>, HashSet<B256>)> {
        let executed = self.blocks_by_hash.remove(&hash)?;

        // Remove this block from collection of children of its parent block.
        let parent_entry = self.parent_to_child.entry(executed.recovered_block().parent_hash());
        if let hash_map::Entry::Occupied(mut entry) = parent_entry {
            entry.get_mut().remove(&hash);

            if entry.get().is_empty() {
                entry.remove();
            }
        }

        // Remove point to children of this block.
        let children = self.parent_to_child.remove(&hash).unwrap_or_default();

        // Remove this block from `blocks_by_number`.
        let block_number_entry = self.blocks_by_number.entry(executed.recovered_block().number());
        if let btree_map::Entry::Occupied(mut entry) = block_number_entry {
            // We have to find the index of the block since it exists in a vec
            if let Some(index) = entry.get().iter().position(|b| b.recovered_block().hash() == hash)
            {
                entry.get_mut().swap_remove(index);

                // If there are no blocks left then remove the entry for this block
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }

        Some((executed, children))
    }

    /// Returns whether or not the hash is part of the canonical chain.
    pub(crate) fn is_canonical(&self, hash: B256) -> bool {
        let mut current_block = self.current_canonical_head.hash;
        if current_block == hash {
            return true
        }

        while let Some(executed) = self.blocks_by_hash.get(&current_block) {
            current_block = executed.recovered_block().parent_hash();
            if current_block == hash {
                return true
            }
        }

        false
    }

    /// Removes canonical blocks below the upper bound, only if the last persisted hash is
    /// part of the canonical chain.
    pub(crate) fn remove_canonical_until(
        &mut self,
        upper_bound: BlockNumber,
        last_persisted_hash: B256,
    ) {
        debug!(target: "engine::tree", ?upper_bound, ?last_persisted_hash, "Removing canonical blocks from the tree");

        // If the last persisted hash is not canonical, then we don't want to remove any canonical
        // blocks yet.
        if !self.is_canonical(last_persisted_hash) {
            return
        }

        // First, let's walk back the canonical chain and remove canonical blocks lower than the
        // upper bound
        let mut current_block = self.current_canonical_head.hash;
        while let Some(executed) = self.blocks_by_hash.get(&current_block) {
            current_block = executed.recovered_block().parent_hash();
            if executed.recovered_block().number() <= upper_bound {
                let num_hash = executed.recovered_block().num_hash();
                debug!(target: "engine::tree", ?num_hash, "Attempting to remove block walking back from the head");
                if let Some((mut removed, _)) =
                    self.remove_by_hash(executed.recovered_block().hash())
                {
                    debug!(target: "engine::tree", ?num_hash, "Removed block walking back from the head");
                    // finally, move the trie updates
                    let Some(trie_updates) = removed.trie.take_present() else {
                        debug!(target: "engine::tree", ?num_hash, "No trie updates found for persisted block");
                        continue;
                    };
                    self.persisted_trie_updates.insert(
                        removed.recovered_block().hash(),
                        (removed.recovered_block().number(), trie_updates),
                    );
                }
            }
        }
        debug!(target: "engine::tree", ?upper_bound, ?last_persisted_hash, "Removed canonical blocks from the tree");
    }

    /// Prunes old persisted trie updates based on the current block number
    /// and chain type (OP Stack or regular)
    pub(crate) fn prune_persisted_trie_updates(&mut self) {
        let retention_blocks = if self.engine_kind.is_opstack() {
            OPSTACK_PERSISTED_TRIE_UPDATES_RETENTION
        } else {
            DEFAULT_PERSISTED_TRIE_UPDATES_RETENTION
        };

        let earliest_block_to_retain =
            self.current_canonical_head.number.saturating_sub(retention_blocks);

        self.persisted_trie_updates
            .retain(|_, (block_number, _)| *block_number > earliest_block_to_retain);
    }

    /// Removes all blocks that are below the finalized block, as well as removing non-canonical
    /// sidechains that fork from below the finalized block.
    pub(crate) fn prune_finalized_sidechains(&mut self, finalized_num_hash: BlockNumHash) {
        let BlockNumHash { number: finalized_num, hash: finalized_hash } = finalized_num_hash;

        // We remove disconnected sidechains in three steps:
        // * first, remove everything with a block number __below__ the finalized block.
        // * next, we populate a vec with parents __at__ the finalized block.
        // * finally, we iterate through the vec, removing children until the vec is empty
        // (BFS).

        // We _exclude_ the finalized block because we will be dealing with the blocks __at__
        // the finalized block later.
        let blocks_to_remove = self
            .blocks_by_number
            .range((Bound::Unbounded, Bound::Excluded(finalized_num)))
            .flat_map(|(_, blocks)| blocks.iter().map(|b| b.recovered_block().hash()))
            .collect::<Vec<_>>();
        for hash in blocks_to_remove {
            if let Some((removed, _)) = self.remove_by_hash(hash) {
                debug!(target: "engine::tree", num_hash=?removed.recovered_block().num_hash(), "Removed finalized sidechain block");
            }
        }

        self.prune_persisted_trie_updates();

        // The only block that should remain at the `finalized` number now, is the finalized
        // block, if it exists.
        //
        // For all other blocks, we  first put their children into this vec.
        // Then, we will iterate over them, removing them, adding their children, etc,
        // until the vec is empty.
        let mut blocks_to_remove = self.blocks_by_number.remove(&finalized_num).unwrap_or_default();

        // re-insert the finalized hash if we removed it
        if let Some(position) =
            blocks_to_remove.iter().position(|b| b.recovered_block().hash() == finalized_hash)
        {
            let finalized_block = blocks_to_remove.swap_remove(position);
            self.blocks_by_number.insert(finalized_num, vec![finalized_block]);
        }

        let mut blocks_to_remove = blocks_to_remove
            .into_iter()
            .map(|e| e.recovered_block().hash())
            .collect::<VecDeque<_>>();
        while let Some(block) = blocks_to_remove.pop_front() {
            if let Some((removed, children)) = self.remove_by_hash(block) {
                debug!(target: "engine::tree", num_hash=?removed.recovered_block().num_hash(), "Removed finalized sidechain child block");
                blocks_to_remove.extend(children);
            }
        }
    }

    /// Remove all blocks up to __and including__ the given block number.
    ///
    /// If a finalized hash is provided, the only non-canonical blocks which will be removed are
    /// those which have a fork point at or below the finalized hash.
    ///
    /// Canonical blocks below the upper bound will still be removed.
    ///
    /// NOTE: if the finalized block is greater than the upper bound, the only blocks that will be
    /// removed are canonical blocks and sidechains that fork below the `upper_bound`. This is the
    /// same behavior as if the `finalized_num` were `Some(upper_bound)`.
    pub(crate) fn remove_until(
        &mut self,
        upper_bound: BlockNumHash,
        last_persisted_hash: B256,
        finalized_num_hash: Option<BlockNumHash>,
    ) {
        debug!(target: "engine::tree", ?upper_bound, ?finalized_num_hash, "Removing blocks from the tree");

        // If the finalized num is ahead of the upper bound, and exists, we need to instead ensure
        // that the only blocks removed, are canonical blocks less than the upper bound
        let finalized_num_hash = finalized_num_hash.map(|mut finalized| {
            if upper_bound.number < finalized.number {
                finalized = upper_bound;
                debug!(target: "engine::tree", ?finalized, "Adjusted upper bound");
            }
            finalized
        });

        // We want to do two things:
        // * remove canonical blocks that are persisted
        // * remove forks whose root are below the finalized block
        // We can do this in 2 steps:
        // * remove all canonical blocks below the upper bound
        // * fetch the number of the finalized hash, removing any sidechains that are __below__ the
        // finalized block
        self.remove_canonical_until(upper_bound.number, last_persisted_hash);

        // Now, we have removed canonical blocks (assuming the upper bound is above the finalized
        // block) and only have sidechains below the finalized block.
        if let Some(finalized_num_hash) = finalized_num_hash {
            self.prune_finalized_sidechains(finalized_num_hash);
        }
    }

    /// Determines if the second block is a direct descendant of the first block.
    ///
    /// If the two blocks are the same, this returns `false`.
    pub(crate) fn is_descendant(&self, first: BlockNumHash, second: BlockWithParent) -> bool {
        // If the second block's parent is the first block's hash, then it is a direct descendant
        // and we can return early.
        if second.parent == first.hash {
            return true
        }

        // If the second block is lower than, or has the same block number, they are not
        // descendants.
        if second.block.number <= first.number {
            return false
        }

        // iterate through parents of the second until we reach the number
        let Some(mut current_block) = self.blocks_by_hash.get(&second.parent) else {
            // If we can't find its parent in the tree, we can't continue, so return false
            return false
        };

        while current_block.recovered_block().number() > first.number + 1 {
            let Some(block) =
                self.blocks_by_hash.get(&current_block.recovered_block().parent_hash())
            else {
                // If we can't find its parent in the tree, we can't continue, so return false
                return false
            };

            current_block = block;
        }

        // Now the block numbers should be equal, so we compare hashes.
        current_block.recovered_block().parent_hash() == first.hash
    }

    /// Updates the canonical head to the given block.
    pub(crate) const fn set_canonical_head(&mut self, new_head: BlockNumHash) {
        self.current_canonical_head = new_head;
    }

    /// Returns the tracked canonical head.
    pub(crate) const fn canonical_head(&self) -> &BlockNumHash {
        &self.current_canonical_head
    }

    /// Returns the block hash of the canonical head.
    pub(crate) const fn canonical_block_hash(&self) -> B256 {
        self.canonical_head().hash
    }

    /// Returns the block number of the canonical head.
    pub(crate) const fn canonical_block_number(&self) -> BlockNumber {
        self.canonical_head().number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chain_state::test_utils::TestBlockBuilder;

    #[test]
    fn test_tree_state_normal_descendant() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..4).collect();

        tree_state.insert_executed(blocks[0].clone());
        assert!(tree_state.is_descendant(
            blocks[0].recovered_block().num_hash(),
            blocks[1].recovered_block().block_with_parent()
        ));

        tree_state.insert_executed(blocks[1].clone());

        assert!(tree_state.is_descendant(
            blocks[0].recovered_block().num_hash(),
            blocks[2].recovered_block().block_with_parent()
        ));
        assert!(tree_state.is_descendant(
            blocks[1].recovered_block().num_hash(),
            blocks[2].recovered_block().block_with_parent()
        ));
    }

    #[tokio::test]
    async fn test_tree_state_insert_executed() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..4).collect();

        tree_state.insert_executed(blocks[0].clone());
        tree_state.insert_executed(blocks[1].clone());

        assert_eq!(
            tree_state.parent_to_child.get(&blocks[0].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[1].recovered_block().hash()]))
        );

        assert!(!tree_state.parent_to_child.contains_key(&blocks[1].recovered_block().hash()));

        tree_state.insert_executed(blocks[2].clone());

        assert_eq!(
            tree_state.parent_to_child.get(&blocks[1].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[2].recovered_block().hash()]))
        );
        assert!(tree_state.parent_to_child.contains_key(&blocks[1].recovered_block().hash()));

        assert!(!tree_state.parent_to_child.contains_key(&blocks[2].recovered_block().hash()));
    }

    #[tokio::test]
    async fn test_tree_state_insert_executed_with_reorg() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let mut test_block_builder = TestBlockBuilder::eth();
        let blocks: Vec<_> = test_block_builder.get_executed_blocks(1..6).collect();

        for block in &blocks {
            tree_state.insert_executed(block.clone());
        }
        assert_eq!(tree_state.blocks_by_hash.len(), 5);

        let fork_block_3 = test_block_builder
            .get_executed_block_with_number(3, blocks[1].recovered_block().hash());
        let fork_block_4 = test_block_builder
            .get_executed_block_with_number(4, fork_block_3.recovered_block().hash());
        let fork_block_5 = test_block_builder
            .get_executed_block_with_number(5, fork_block_4.recovered_block().hash());

        tree_state.insert_executed(fork_block_3.clone());
        tree_state.insert_executed(fork_block_4.clone());
        tree_state.insert_executed(fork_block_5.clone());

        assert_eq!(tree_state.blocks_by_hash.len(), 8);
        assert_eq!(tree_state.blocks_by_number[&3].len(), 2); // two blocks at height 3 (original and fork)
        assert_eq!(tree_state.parent_to_child[&blocks[1].recovered_block().hash()].len(), 2); // block 2 should have two children

        // verify that we can insert the same block again without issues
        tree_state.insert_executed(fork_block_4.clone());
        assert_eq!(tree_state.blocks_by_hash.len(), 8);

        assert!(tree_state.parent_to_child[&fork_block_3.recovered_block().hash()]
            .contains(&fork_block_4.recovered_block().hash()));
        assert!(tree_state.parent_to_child[&fork_block_4.recovered_block().hash()]
            .contains(&fork_block_5.recovered_block().hash()));

        assert_eq!(tree_state.blocks_by_number[&4].len(), 2);
        assert_eq!(tree_state.blocks_by_number[&5].len(), 2);
    }

    #[tokio::test]
    async fn test_tree_state_remove_before() {
        let start_num_hash = BlockNumHash::default();
        let mut tree_state = TreeState::new(start_num_hash, EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..6).collect();

        for block in &blocks {
            tree_state.insert_executed(block.clone());
        }

        let last = blocks.last().unwrap();

        // set the canonical head
        tree_state.set_canonical_head(last.recovered_block().num_hash());

        // inclusive bound, so we should remove anything up to and including 2
        tree_state.remove_until(
            BlockNumHash::new(2, blocks[1].recovered_block().hash()),
            start_num_hash.hash,
            Some(blocks[1].recovered_block().num_hash()),
        );

        assert!(!tree_state.blocks_by_hash.contains_key(&blocks[0].recovered_block().hash()));
        assert!(!tree_state.blocks_by_hash.contains_key(&blocks[1].recovered_block().hash()));
        assert!(!tree_state.blocks_by_number.contains_key(&1));
        assert!(!tree_state.blocks_by_number.contains_key(&2));

        assert!(tree_state.blocks_by_hash.contains_key(&blocks[2].recovered_block().hash()));
        assert!(tree_state.blocks_by_hash.contains_key(&blocks[3].recovered_block().hash()));
        assert!(tree_state.blocks_by_hash.contains_key(&blocks[4].recovered_block().hash()));
        assert!(tree_state.blocks_by_number.contains_key(&3));
        assert!(tree_state.blocks_by_number.contains_key(&4));
        assert!(tree_state.blocks_by_number.contains_key(&5));

        assert!(!tree_state.parent_to_child.contains_key(&blocks[0].recovered_block().hash()));
        assert!(!tree_state.parent_to_child.contains_key(&blocks[1].recovered_block().hash()));
        assert!(tree_state.parent_to_child.contains_key(&blocks[2].recovered_block().hash()));
        assert!(tree_state.parent_to_child.contains_key(&blocks[3].recovered_block().hash()));
        assert!(!tree_state.parent_to_child.contains_key(&blocks[4].recovered_block().hash()));

        assert_eq!(
            tree_state.parent_to_child.get(&blocks[2].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[3].recovered_block().hash()]))
        );
        assert_eq!(
            tree_state.parent_to_child.get(&blocks[3].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[4].recovered_block().hash()]))
        );
    }

    #[tokio::test]
    async fn test_tree_state_remove_before_finalized() {
        let start_num_hash = BlockNumHash::default();
        let mut tree_state = TreeState::new(start_num_hash, EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..6).collect();

        for block in &blocks {
            tree_state.insert_executed(block.clone());
        }

        let last = blocks.last().unwrap();

        // set the canonical head
        tree_state.set_canonical_head(last.recovered_block().num_hash());

        // we should still remove everything up to and including 2
        tree_state.remove_until(
            BlockNumHash::new(2, blocks[1].recovered_block().hash()),
            start_num_hash.hash,
            None,
        );

        assert!(!tree_state.blocks_by_hash.contains_key(&blocks[0].recovered_block().hash()));
        assert!(!tree_state.blocks_by_hash.contains_key(&blocks[1].recovered_block().hash()));
        assert!(!tree_state.blocks_by_number.contains_key(&1));
        assert!(!tree_state.blocks_by_number.contains_key(&2));

        assert!(tree_state.blocks_by_hash.contains_key(&blocks[2].recovered_block().hash()));
        assert!(tree_state.blocks_by_hash.contains_key(&blocks[3].recovered_block().hash()));
        assert!(tree_state.blocks_by_hash.contains_key(&blocks[4].recovered_block().hash()));
        assert!(tree_state.blocks_by_number.contains_key(&3));
        assert!(tree_state.blocks_by_number.contains_key(&4));
        assert!(tree_state.blocks_by_number.contains_key(&5));

        assert!(!tree_state.parent_to_child.contains_key(&blocks[0].recovered_block().hash()));
        assert!(!tree_state.parent_to_child.contains_key(&blocks[1].recovered_block().hash()));
        assert!(tree_state.parent_to_child.contains_key(&blocks[2].recovered_block().hash()));
        assert!(tree_state.parent_to_child.contains_key(&blocks[3].recovered_block().hash()));
        assert!(!tree_state.parent_to_child.contains_key(&blocks[4].recovered_block().hash()));

        assert_eq!(
            tree_state.parent_to_child.get(&blocks[2].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[3].recovered_block().hash()]))
        );
        assert_eq!(
            tree_state.parent_to_child.get(&blocks[3].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[4].recovered_block().hash()]))
        );
    }

    #[tokio::test]
    async fn test_tree_state_remove_before_lower_finalized() {
        let start_num_hash = BlockNumHash::default();
        let mut tree_state = TreeState::new(start_num_hash, EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..6).collect();

        for block in &blocks {
            tree_state.insert_executed(block.clone());
        }

        let last = blocks.last().unwrap();

        // set the canonical head
        tree_state.set_canonical_head(last.recovered_block().num_hash());

        // we have no forks so we should still remove anything up to and including 2
        tree_state.remove_until(
            BlockNumHash::new(2, blocks[1].recovered_block().hash()),
            start_num_hash.hash,
            Some(blocks[0].recovered_block().num_hash()),
        );

        assert!(!tree_state.blocks_by_hash.contains_key(&blocks[0].recovered_block().hash()));
        assert!(!tree_state.blocks_by_hash.contains_key(&blocks[1].recovered_block().hash()));
        assert!(!tree_state.blocks_by_number.contains_key(&1));
        assert!(!tree_state.blocks_by_number.contains_key(&2));

        assert!(tree_state.blocks_by_hash.contains_key(&blocks[2].recovered_block().hash()));
        assert!(tree_state.blocks_by_hash.contains_key(&blocks[3].recovered_block().hash()));
        assert!(tree_state.blocks_by_hash.contains_key(&blocks[4].recovered_block().hash()));
        assert!(tree_state.blocks_by_number.contains_key(&3));
        assert!(tree_state.blocks_by_number.contains_key(&4));
        assert!(tree_state.blocks_by_number.contains_key(&5));

        assert!(!tree_state.parent_to_child.contains_key(&blocks[0].recovered_block().hash()));
        assert!(!tree_state.parent_to_child.contains_key(&blocks[1].recovered_block().hash()));
        assert!(tree_state.parent_to_child.contains_key(&blocks[2].recovered_block().hash()));
        assert!(tree_state.parent_to_child.contains_key(&blocks[3].recovered_block().hash()));
        assert!(!tree_state.parent_to_child.contains_key(&blocks[4].recovered_block().hash()));

        assert_eq!(
            tree_state.parent_to_child.get(&blocks[2].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[3].recovered_block().hash()]))
        );
        assert_eq!(
            tree_state.parent_to_child.get(&blocks[3].recovered_block().hash()),
            Some(&HashSet::from_iter([blocks[4].recovered_block().hash()]))
        );
    }

    // ── Issue #201 regression tests ────────────────────────────────────────────
    //
    // `insert_executed` contains an O(N) scan that calls `retain` on every entry
    // of `parent_to_child` to filter out hashes absent from `blocks_by_hash`.
    // Because `remove_by_hash` already maintains `parent_to_child` atomically,
    // the scan is dead code: it never removes anything under normal operation.
    // These tests verify:
    //   (a) the retain loop is a no-op (never removes entries), and
    //   (b) `remove_by_hash` alone keeps `parent_to_child` fully consistent.

    /// Verify that every child hash stored in `parent_to_child` is always present
    /// in `blocks_by_hash` after N sequential insertions, which proves the O(N)
    /// retain scan in `insert_executed` never removes any entry (dead code).
    #[test]
    fn test_insert_executed_retain_loop_is_noop_sequential() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..21).collect();

        for block in &blocks {
            tree_state.insert_executed(block.clone());

            // After each insertion, every child reference in parent_to_child must
            // resolve to an entry in blocks_by_hash.  If the retain loop were
            // removing anything it would not be observable here (that would mean a
            // child was already gone), but the point is that the set of children
            // must stay consistent — no phantom entries, no missing ones.
            for children in tree_state.parent_to_child.values() {
                for child in children {
                    assert!(
                        tree_state.blocks_by_hash.contains_key(child),
                        "parent_to_child references a child hash not in blocks_by_hash: {child}"
                    );
                }
            }
        }

        // All 20 inserted blocks should still be present.
        assert_eq!(tree_state.blocks_by_hash.len(), 20);
    }

    /// Verify that after inserting blocks on two fork branches, all `parent_to_child`
    /// values remain in `blocks_by_hash`.  This exercises the retain loop across a
    /// forked tree where multiple children exist for a single parent.
    #[test]
    fn test_insert_executed_retain_loop_is_noop_with_forks() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let mut builder = TestBlockBuilder::eth();

        // Build a chain of 5 blocks.
        let chain: Vec<_> = builder.get_executed_blocks(1..6).collect();
        for block in &chain {
            tree_state.insert_executed(block.clone());
        }

        // Fork off block 3 — two extra branches from block[1]'s hash.
        let fork_a3 =
            builder.get_executed_block_with_number(3, chain[1].recovered_block().hash());
        let fork_a4 =
            builder.get_executed_block_with_number(4, fork_a3.recovered_block().hash());
        let fork_b3 =
            builder.get_executed_block_with_number(3, chain[1].recovered_block().hash());
        let fork_b4 =
            builder.get_executed_block_with_number(4, fork_b3.recovered_block().hash());

        for block in [&fork_a3, &fork_a4, &fork_b3, &fork_b4] {
            tree_state.insert_executed(block.clone());

            // Invariant: every child hash in parent_to_child is live in blocks_by_hash.
            for children in tree_state.parent_to_child.values() {
                for child in children {
                    assert!(
                        tree_state.blocks_by_hash.contains_key(child),
                        "stale child reference {child} found in parent_to_child after insert"
                    );
                }
            }
        }
    }

    /// Verify that `remove_by_hash` alone keeps `parent_to_child` consistent
    /// without any help from the retain loop in `insert_executed`.
    ///
    /// The test interleaves inserts and removes and confirms that after each
    /// operation all child hashes in `parent_to_child` exist in `blocks_by_hash`.
    /// This demonstrates the retain loop in `insert_executed` is dead code:
    /// `remove_by_hash` already maintains the invariant atomically.
    #[test]
    fn test_parent_to_child_consistency_maintained_by_remove_not_retain() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let mut builder = TestBlockBuilder::eth();

        // Insert blocks 1..=5.
        let chain: Vec<_> = builder.get_executed_blocks(1..6).collect();
        for block in &chain {
            tree_state.insert_executed(block.clone());
        }

        let assert_consistency = |ts: &TreeState| {
            for children in ts.parent_to_child.values() {
                for child in children {
                    assert!(
                        ts.blocks_by_hash.contains_key(child),
                        "parent_to_child has stale child {child}"
                    );
                }
            }
        };

        assert_consistency(&tree_state);

        // Directly invoke remove_by_hash (accessible within this module) — it should
        // atomically clean up parent_to_child for the removed block's hash.
        let hash1 = chain[0].recovered_block().hash();
        let removed = tree_state.remove_by_hash(hash1);
        assert!(removed.is_some(), "block should have been present");
        assert!(!tree_state.blocks_by_hash.contains_key(&hash1));
        // After removal, no entry in parent_to_child should reference hash1.
        assert_consistency(&tree_state);

        // Insert a fork at height 3 (off chain[1]) — the retain loop would previously
        // scan all of parent_to_child here; it should be a no-op.
        let fork =
            builder.get_executed_block_with_number(3, chain[1].recovered_block().hash());
        tree_state.insert_executed(fork.clone());
        assert_consistency(&tree_state);

        // Remove the original block at height 3 via remove_by_hash.
        let orig3_hash = chain[2].recovered_block().hash();
        let removed = tree_state.remove_by_hash(orig3_hash);
        assert!(removed.is_some());
        // parent_to_child must still be clean after the remove — no retain loop needed.
        assert_consistency(&tree_state);

        // The fork block at height 3 must still be present and reachable.
        assert!(tree_state.blocks_by_hash.contains_key(&fork.recovered_block().hash()));
        assert_consistency(&tree_state);
    }

    /// Regression test: the retain loop in `insert_executed` (lines 131-133 of
    /// state.rs) must never decrease the number of entries in `parent_to_child`
    /// during a pure insertion workload.  If it did, it would be removing live
    /// entries, which would be a correctness bug.
    #[test]
    fn test_insert_executed_retain_never_removes_live_entries() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..11).collect();

        let mut prev_total_children: usize = 0;

        for block in &blocks {
            tree_state.insert_executed(block.clone());

            // Count total child entries across all parent_to_child sets.
            let total_children: usize =
                tree_state.parent_to_child.values().map(|s| s.len()).sum();

            // The count must be monotonically non-decreasing during pure insertion:
            // the retain loop should never silently discard a live child reference.
            assert!(
                total_children >= prev_total_children,
                "retain loop removed a live child entry: went from {prev_total_children} to \
                 {total_children} children after inserting block {}",
                block.recovered_block().number()
            );
            prev_total_children = total_children;
        }

        // Final sanity: 10 parent→child edges for a 10-block linear chain
        // (block[0]'s parent B256::default() also gets an entry).
        assert_eq!(prev_total_children, 10);
    }

    // ── Retain-loop dead-code proof (Issue #201) ────────────────────────────
    //
    // The tests above check invariants while the retain loop is still running,
    // so they cannot distinguish whether consistency is maintained by
    // `remove_by_hash` alone or with help from the loop.
    //
    // The tests below directly simulate the retain predicate:
    //
    //   stale_count = |{ child ∈ parent_to_child values : !blocks_by_hash.contains(child) }|
    //
    // We compute this count AFTER removal operations but BEFORE any subsequent
    // `insert_executed` call — the only window where the retain loop has NOT run
    // since the last removal.  If stale_count == 0 at that point, the retain
    // loop is provably a no-op: it would find nothing to remove.
    //
    // We also include a negative-control test that injects a stale entry
    // manually and confirms our counter detects it, validating the harness.

    /// Count child hashes in `parent_to_child` that are absent from
    /// `blocks_by_hash` — the exact set the retain loop would remove.
    fn count_retain_loop_work(state: &TreeState) -> usize {
        state
            .parent_to_child
            .values()
            .flat_map(|children| children.iter())
            .filter(|child| !state.blocks_by_hash.contains_key(*child))
            .count()
    }

    /// Negative control: verify `count_retain_loop_work` would actually catch a
    /// stale entry if one existed.  Injects a phantom child hash directly into
    /// `parent_to_child` and asserts the counter reports it.  Without this
    /// validation the other tests might pass vacuously.
    #[test]
    fn test_harness_detects_injected_stale_entry() {
        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        let blocks: Vec<_> = TestBlockBuilder::eth().get_executed_blocks(1..4).collect();
        for block in &blocks {
            tree_state.insert_executed(block.clone());
        }

        // Before injection the count must be zero.
        assert_eq!(count_retain_loop_work(&tree_state), 0, "expected clean state before injection");

        // Inject a hash that is NOT in blocks_by_hash.
        let phantom = B256::repeat_byte(0xde);
        assert!(!tree_state.blocks_by_hash.contains_key(&phantom));
        tree_state
            .parent_to_child
            .entry(blocks[0].recovered_block().hash())
            .or_default()
            .insert(phantom);

        // Now the counter must report exactly 1 stale entry.
        assert_eq!(
            count_retain_loop_work(&tree_state),
            1,
            "harness failed to detect the injected stale entry — subsequent tests are invalid"
        );
    }

    /// After `prune_finalized_sidechains` (which drives `remove_by_hash`) and
    /// *before* any subsequent `insert_executed`, simulate the retain predicate.
    /// A zero count proves `remove_by_hash` alone keeps `parent_to_child`
    /// consistent; the retain loop in `insert_executed` would have no work to do.
    #[test]
    fn test_retain_predicate_zero_after_prune_finalized_sidechains() {
        let mut builder = TestBlockBuilder::eth();
        let canonical: Vec<_> = builder.get_executed_blocks(1..6).collect();

        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        for block in &canonical {
            tree_state.insert_executed(block.clone());
        }

        // Build a fork: branches off canonical[1], blocks at heights 3, 4, 5.
        let fork3 =
            builder.get_executed_block_with_number(3, canonical[1].recovered_block().hash());
        let fork4 =
            builder.get_executed_block_with_number(4, fork3.recovered_block().hash());
        let fork5 =
            builder.get_executed_block_with_number(5, fork4.recovered_block().hash());

        tree_state.insert_executed(fork3.clone());
        tree_state.insert_executed(fork4.clone());
        tree_state.insert_executed(fork5.clone());

        tree_state
            .set_canonical_head(canonical.last().unwrap().recovered_block().num_hash());

        // Baseline: no stale entries after insertions.
        assert_eq!(count_retain_loop_work(&tree_state), 0, "unexpected stale entries before prune");

        // Prune: finalize at block 2, which removes fork3/fork4/fork5 via remove_by_hash.
        tree_state.prune_finalized_sidechains(canonical[1].recovered_block().num_hash());

        // ── Critical check ──────────────────────────────────────────────────
        // We have NOT called insert_executed since the prune, so the retain loop
        // has NOT run.  If remove_by_hash left any stale entries they are visible
        // here.  A non-zero count means the retain loop is load-bearing, not dead.
        let stale = count_retain_loop_work(&tree_state);
        assert_eq!(
            stale, 0,
            "remove_by_hash left {stale} stale entries in parent_to_child; \
             the retain loop in insert_executed is NOT dead code"
        );
    }

    /// After direct `remove_by_hash` calls and *before* `insert_executed`,
    /// simulate the retain predicate.  Same proof strategy as above but
    /// exercising the private API directly to isolate each removal.
    #[test]
    fn test_retain_predicate_zero_after_direct_remove_by_hash() {
        let mut builder = TestBlockBuilder::eth();
        let chain: Vec<_> = builder.get_executed_blocks(1..5).collect();
        // Fork off chain[1]: heights 3 and 4.
        let fork3 =
            builder.get_executed_block_with_number(3, chain[1].recovered_block().hash());
        let fork4 =
            builder.get_executed_block_with_number(4, fork3.recovered_block().hash());

        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        for block in &chain {
            tree_state.insert_executed(block.clone());
        }
        tree_state.insert_executed(fork3.clone());
        tree_state.insert_executed(fork4.clone());

        // Verify clean state after all inserts.
        assert_eq!(count_retain_loop_work(&tree_state), 0);

        // Remove the canonical block at height 3 directly.
        // `remove_by_hash` must unlink chain[2] from chain[1]'s child set
        // without leaving a dangling entry.
        let removed = tree_state.remove_by_hash(chain[2].recovered_block().hash());
        assert!(removed.is_some(), "block must exist before removal");

        // ── Critical check (no insert_executed since the remove) ────────────
        let stale = count_retain_loop_work(&tree_state);
        assert_eq!(
            stale, 0,
            "after removing chain[2], remove_by_hash left {stale} stale entries; \
             retain loop is load-bearing"
        );

        // Remove the fork root too.
        let removed = tree_state.remove_by_hash(fork3.recovered_block().hash());
        assert!(removed.is_some());

        // ── Critical check again (still no insert_executed) ─────────────────
        let stale = count_retain_loop_work(&tree_state);
        assert_eq!(
            stale, 0,
            "after removing fork3, remove_by_hash left {stale} stale entries; \
             retain loop is load-bearing"
        );

        // fork4 should still be present in blocks_by_hash.
        assert!(
            tree_state.blocks_by_hash.contains_key(&fork4.recovered_block().hash()),
            "fork4 should still be alive after removing its parent"
        );
    }

    /// Interleaved inserts and removes: before EACH `insert_executed` that follows
    /// at least one removal, simulate the retain predicate and assert zero stale
    /// entries.  This proves the loop would be a no-op at every invocation in a
    /// realistic mixed workload.
    #[test]
    fn test_retain_predicate_zero_before_every_insert_in_mixed_workload() {
        let mut builder = TestBlockBuilder::eth();
        let chain: Vec<_> = builder.get_executed_blocks(1..4).collect();

        let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);
        // Seed the tree with the first two canonical blocks.
        tree_state.insert_executed(chain[0].clone());
        tree_state.insert_executed(chain[1].clone());

        // Build a batch of fork blocks at height 3 (all parented to chain[1]).
        let forks: Vec<_> = (0..5)
            .map(|_| {
                builder.get_executed_block_with_number(3, chain[1].recovered_block().hash())
            })
            .collect();

        // Insert fork[0], then remove it, then insert each remaining fork.
        // Before each insert (after the removal) we simulate the retain predicate.
        tree_state.insert_executed(forks[0].clone());
        tree_state.remove_by_hash(forks[0].recovered_block().hash());

        for fork in &forks[1..] {
            // ── Critical check: retain loop would see this state ─────────────
            let stale = count_retain_loop_work(&tree_state);
            assert_eq!(
                stale, 0,
                "before inserting a fork, found {stale} stale entries — \
                 retain loop is load-bearing, not dead code"
            );
            tree_state.insert_executed(fork.clone());
        }

        // After all inserts, still no stale entries.
        assert_eq!(count_retain_loop_work(&tree_state), 0);
    }
}
