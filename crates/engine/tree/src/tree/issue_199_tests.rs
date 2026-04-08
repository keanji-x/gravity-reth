//! Tests for issue #199: Reorg recovery silently drops missing-trie blocks,
//! creating broken ancestry chains that are never pruned.
//!
//! These tests target the specific code paths and invariant violations described
//! in the issue, using [`TreeState`] directly.

use crate::{engine::EngineApiKind, tree::state::TreeState};
use alloy_eips::BlockNumHash;
use reth_chain_state::{test_utils::TestBlockBuilder, ExecutedTrieUpdates};

// ── Test 1 ────────────────────────────────────────────────────────────────────
//
// `state.rs:223-226` — `remove_canonical_until` silently skips blocks with
// `ExecutedTrieUpdates::Missing` and does NOT add them to `persisted_trie_updates`.
// After removal those blocks are unreachable for reorg recovery (the filter_map
// at `mod.rs:2304-2311` cannot reconstruct them).
//
// Expected (correct) behaviour: every canonical block that is removed should
// be tracked in some way so that a subsequent reorg can reconstruct the full
// old chain without gaps.
//
// Current (buggy) behaviour confirmed by this test: blocks with Missing trie
// are silently absent from `persisted_trie_updates` after `remove_canonical_until`.
#[test]
fn test_remove_canonical_until_missing_trie_blocks_not_in_persisted_trie_updates() {
    let mut builder = TestBlockBuilder::eth();
    let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);

    // Build chain B1(Missing) → B2(Missing) → B3(Present) → B4(Present) → B5(Present).
    let blocks: Vec<_> = builder.get_executed_blocks(1..6).collect();

    // Mark B1 and B2 as Missing trie — simulating blocks executed on a fork
    // that later became canonical but whose trie updates could not be applied.
    let mut b1 = blocks[0].clone();
    b1.trie = ExecutedTrieUpdates::Missing;
    let mut b2 = blocks[1].clone();
    b2.trie = ExecutedTrieUpdates::Missing;
    let b3 = blocks[2].clone(); // Present (empty TrieUpdates from TestBlockBuilder)
    let b4 = blocks[3].clone();
    let b5 = blocks[4].clone();

    for block in [b1.clone(), b2.clone(), b3.clone(), b4.clone(), b5.clone()] {
        tree_state.insert_executed(block);
    }
    tree_state.set_canonical_head(b5.recovered_block().num_hash());

    // Remove all canonical blocks up through B5.
    tree_state.remove_canonical_until(5, b5.recovered_block().hash());

    // Blocks should be gone from blocks_by_hash after removal.
    assert!(
        !tree_state.blocks_by_hash.contains_key(&b1.recovered_block().hash()),
        "B1 must be removed from blocks_by_hash by remove_canonical_until"
    );
    assert!(
        !tree_state.blocks_by_hash.contains_key(&b2.recovered_block().hash()),
        "B2 must be removed from blocks_by_hash by remove_canonical_until"
    );

    // B3, B4, B5 had Present trie updates → they MUST be in persisted_trie_updates.
    assert!(
        tree_state.persisted_trie_updates.contains_key(&b3.recovered_block().hash()),
        "B3 (Present trie) must be in persisted_trie_updates"
    );
    assert!(
        tree_state.persisted_trie_updates.contains_key(&b4.recovered_block().hash()),
        "B4 (Present trie) must be in persisted_trie_updates"
    );
    assert!(
        tree_state.persisted_trie_updates.contains_key(&b5.recovered_block().hash()),
        "B5 (Present trie) must be in persisted_trie_updates"
    );

    // BUG CONFIRMED: B1 and B2 had Missing trie updates, so `remove_canonical_until`
    // skips them (state.rs:223-226 `continue`) and they are absent from
    // `persisted_trie_updates`.  Any reorg recovery filter_map that looks them up
    // will silently drop them, creating a gap in the reinserted old chain.
    assert!(
        !tree_state.persisted_trie_updates.contains_key(&b1.recovered_block().hash()),
        "BUG #199: B1 (Missing trie) is absent from persisted_trie_updates after removal \
         — the reorg recovery filter_map will silently drop it"
    );
    assert!(
        !tree_state.persisted_trie_updates.contains_key(&b2.recovered_block().hash()),
        "BUG #199: B2 (Missing trie) is absent from persisted_trie_updates after removal \
         — the reorg recovery filter_map will silently drop it"
    );
}

// ── Test 2 ────────────────────────────────────────────────────────────────────
//
// `mod.rs:2348` — `reinsert_reorged_blocks` unconditionally inserts every block
// it receives via `insert_executed`, which blindly adds
// `parent_to_child[parent_hash]` entries even when the parent was dropped from
// the reinsert list (because it had a Missing trie).
//
// This creates **dangling** `parent_to_child` edges: the parent hash appears as
// a key but the parent block itself is absent from `blocks_by_hash`.
//
// Expected: no entry in `parent_to_child` should reference a non-existent parent.
// Current (bug): dangling entry exists and is confirmed by this test.
#[test]
fn test_reinsert_reorged_blocks_creates_dangling_parent_to_child_entry() {
    let mut builder = TestBlockBuilder::eth();
    let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);

    // Simulate the post-reorg state: old chain had B1(Missing)→B2(Missing)→B3→B4→B5.
    // B1 and B2 were persisted with Missing trie → they are absent from both
    // blocks_by_hash and persisted_trie_updates.
    // The reorg recovery filter_map reconstructed [B3, B4, B5] from persisted_trie_updates
    // (they had Present trie) and calls reinsert_reorged_blocks([B3, B4, B5]).
    //
    // We replicate that call here by directly calling insert_executed for B3–B5.
    let all: Vec<_> = builder.get_executed_blocks(1..6).collect();
    // B3 = all[2], B4 = all[3], B5 = all[4].  Their parent hashes form the chain:
    // B3.parent = B2.hash, B4.parent = B3.hash, B5.parent = B4.hash.
    let b2_hash = all[1].recovered_block().hash();
    let b3 = all[2].clone();
    let b4 = all[3].clone();
    let b5 = all[4].clone();

    // B1 and B2 are intentionally NOT inserted (they were dropped by filter_map).
    tree_state.insert_executed(b3.clone());
    tree_state.insert_executed(b4.clone());
    tree_state.insert_executed(b5.clone());
    tree_state.set_canonical_head(b5.recovered_block().num_hash());

    // B3, B4, B5 are in blocks_by_hash.
    assert!(tree_state.blocks_by_hash.contains_key(&b3.recovered_block().hash()));
    assert!(tree_state.blocks_by_hash.contains_key(&b4.recovered_block().hash()));
    assert!(tree_state.blocks_by_hash.contains_key(&b5.recovered_block().hash()));

    // B2 is NOT in blocks_by_hash (it was dropped / never reinserted).
    assert!(
        !tree_state.blocks_by_hash.contains_key(&b2_hash),
        "B2 must not be in blocks_by_hash"
    );

    // BUG CONFIRMED: parent_to_child[B2.hash] = {B3.hash} is a DANGLING entry —
    // B2 exists as a parent key but has no corresponding entry in blocks_by_hash.
    let children_of_b2 = tree_state.parent_to_child.get(&b2_hash);
    assert!(
        children_of_b2.is_some(),
        "BUG #199: parent_to_child should not have an entry for B2 when B2 is not in \
         blocks_by_hash, but it does — dangling edge created by reinsert_reorged_blocks"
    );
    assert!(
        children_of_b2
            .unwrap()
            .contains(&b3.recovered_block().hash()),
        "BUG #199: parent_to_child[B2] contains B3, forming a dangling edge: \
         B2 is absent from blocks_by_hash but still referenced as a parent"
    );
}

// ── Test 3 ────────────────────────────────────────────────────────────────────
//
// `state.rs:179-193` — `is_canonical` walks back through `blocks_by_hash` via
// parent pointers using the pattern:
//
//   ```
//   current_block = parent_hash(current_block);
//   if current_block == target { return true; }
//   ```
//
// This means a block hash that is stored as a *parent pointer* in `blocks_by_hash`
// is reported as canonical even though the block itself is absent from
// `blocks_by_hash` — a "phantom canonical" result.
//
// Concretely: if B3 is in blocks_by_hash with parent = B2.hash, then
// `is_canonical(B2.hash)` returns `true` even if B2 was dropped (Missing trie).
//
// This phantom-canonical result causes two problems:
//
//   1. `remove_canonical_until` passes its `is_canonical` guard but then cannot
//      actually remove the phantom block (it is not in blocks_by_hash), so the
//      removal loop stops short — any in-memory descendants that need the phantom
//      as an anchor accumulate permanently.
//
//   2. A hash that is genuinely absent AND not referenced as any parent pointer
//      returns `false` correctly, but callers cannot distinguish "truly missing"
//      from "phantom-canonical", making the guard unreliable.
#[test]
fn test_is_canonical_phantom_canonical_for_missing_block_via_parent_pointer() {
    let mut builder = TestBlockBuilder::eth();
    let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);

    // Full chain B1→B2→B3→B4→B5.  Only B3, B4, B5 are in blocks_by_hash
    // (B1, B2 were dropped, simulating broken reorg recovery after Missing-trie
    // persistence).
    let all: Vec<_> = builder.get_executed_blocks(1..6).collect();
    let b1 = &all[0];
    let b2 = &all[1];
    let b3 = all[2].clone();
    let b4 = all[3].clone();
    let b5 = all[4].clone();

    tree_state.insert_executed(b3.clone());
    tree_state.insert_executed(b4.clone());
    tree_state.insert_executed(b5.clone());
    tree_state.set_canonical_head(b5.recovered_block().num_hash());

    // B5, B4, B3 are in blocks_by_hash and reachable from the head.
    assert!(tree_state.is_canonical(b5.recovered_block().hash()), "B5 (head) must be canonical");
    assert!(tree_state.is_canonical(b4.recovered_block().hash()), "B4 must be canonical");
    assert!(tree_state.is_canonical(b3.recovered_block().hash()), "B3 must be canonical");

    // B2 is NOT in blocks_by_hash but IS the parent of B3 (which is in blocks_by_hash).
    // is_canonical walks: B5 → parent=B4, B4 → parent=B3, B3 → parent=B2.hash.
    // The walk sets current_block = B2.hash and checks current_block == target → TRUE.
    //
    // BUG CONFIRMED: is_canonical returns `true` for a block that is NOT in blocks_by_hash.
    // The block exists only as a phantom parent pointer — it cannot be found, removed, or
    // validated, yet is reported as canonical.
    assert!(
        tree_state.is_canonical(b2.recovered_block().hash()),
        "BUG #199: is_canonical(B2) returns true even though B2 is absent from \
         blocks_by_hash — it is a 'phantom canonical' block reachable only as \
         the parent pointer stored in B3"
    );

    // B1 is absent AND not referenced as any block's parent (since B2 is the phantom parent
    // and B1 is B2's parent — but B2 is not in blocks_by_hash so the walk stops at B2).
    // So B1 correctly returns false.
    assert!(
        !tree_state.is_canonical(b1.recovered_block().hash()),
        "B1 is below the phantom — is_canonical must return false"
    );

    // CONSEQUENCE: remove_canonical_until(5, B2.hash) passes the is_canonical guard
    // (returns true for B2), enters the removal loop, but never finds B2 in blocks_by_hash.
    // Only B3, B4, B5 are found and removed — B2 itself can never be cleaned up.
    // If a caller expected B2 to be removed, they are silently wrong.
    //
    // We demonstrate the removal proceeds without error (no panic), removing B3-B5:
    tree_state.remove_canonical_until(5, b2.recovered_block().hash());

    // B3, B4, B5 are removed (they were in blocks_by_hash and their numbers <= 5).
    assert!(
        !tree_state.blocks_by_hash.contains_key(&b3.recovered_block().hash()),
        "B3 should be removed by remove_canonical_until"
    );
    assert!(
        !tree_state.blocks_by_hash.contains_key(&b4.recovered_block().hash()),
        "B4 should be removed by remove_canonical_until"
    );
    assert!(
        !tree_state.blocks_by_hash.contains_key(&b5.recovered_block().hash()),
        "B5 should be removed by remove_canonical_until"
    );
    // But B2 itself was never in blocks_by_hash, so it was never touched — the phantom
    // simply disappears silently, with no record in persisted_trie_updates either.
    assert!(
        !tree_state.persisted_trie_updates.contains_key(&b2.recovered_block().hash()),
        "BUG #199: B2 (phantom canonical) has no entry in persisted_trie_updates — \
         it was never tracked anywhere and its ancestry is permanently lost"
    );
}

// ── Test 4 ────────────────────────────────────────────────────────────────────
//
// `state.rs:253-304` — `prune_finalized_sidechains` Phase 3 BFS discovers
// non-canonical children by walking `parent_to_child` from the finalized block.
// When there is a gap in `blocks_by_hash` (an ancestor was dropped due to the
// missing-trie bug), sidechain branches rooted *above* the gap are invisible to
// the BFS — they are never enqueued and therefore never garbage-collected.
//
// Setup: main chain B1→B2→B3→B4→B5 (canonical head = B5); sidechain F3→F4
// forking off B2.  B2 was dropped (Missing-trie reorg) so it is absent from
// blocks_by_hash, but B3-B5 and F3-F4 are present.
// After finalization at B2 the BFS should remove F3 and F4, but it cannot
// reach them because `parent_to_child[B2.hash]` may be stale or
// `blocks_by_number` at the finalized height no longer contains B2.
#[test]
fn test_prune_finalized_sidechains_misses_branches_above_missing_ancestor() {
    let mut builder = TestBlockBuilder::eth();
    let mut tree_state = TreeState::new(BlockNumHash::default(), EngineApiKind::Ethereum);

    // Main chain (only B3, B4, B5 are actually inserted — B2 is the gap).
    let main: Vec<_> = builder.get_executed_blocks(1..6).collect();
    let b2 = &main[1];
    let b3 = main[2].clone();
    let b4 = main[3].clone();
    let b5 = main[4].clone();

    // Fork at B2: F3 and F4 branch off B2 and are inserted normally.
    let f3 = builder.get_executed_block_with_number(3, b2.recovered_block().hash());
    let f4 = builder.get_executed_block_with_number(4, f3.recovered_block().hash());

    // Insert B3-B5 (main chain after gap) and F3-F4 (sidechain).
    // B2 is intentionally absent — it was dropped by the reorg-recovery bug.
    tree_state.insert_executed(b3.clone());
    tree_state.insert_executed(b4.clone());
    tree_state.insert_executed(b5.clone());
    tree_state.insert_executed(f3.clone());
    tree_state.insert_executed(f4.clone());
    tree_state.set_canonical_head(b5.recovered_block().num_hash());

    // Sanity: F3 and F4 are currently in blocks_by_hash.
    assert!(tree_state.blocks_by_hash.contains_key(&f3.recovered_block().hash()));
    assert!(tree_state.blocks_by_hash.contains_key(&f4.recovered_block().hash()));

    // Finalize at B2.  Phase 1 of prune_finalized_sidechains removes everything
    // *below* B2's number (nothing here, since our lowest block is B3=number 3
    // and B2 has number 2, so blocks at number 1 would be cleaned).
    // Phase 2 looks for non-finalized blocks *at* B2's number.  Because B2 is NOT
    // in blocks_by_hash, blocks_by_number[2] is empty — no non-canonical roots are
    // detected there.
    // Phase 3 BFS never enqueues F3 / F4 because they would only be reachable from
    // parent_to_child[B2.hash], but B2 itself is not present in blocks_by_number[2].
    tree_state.prune_finalized_sidechains(b2.recovered_block().num_hash());

    // BUG CONFIRMED: F3 and F4 are still in blocks_by_hash after finalization —
    // the BFS could not reach them because their forking ancestor (B2) was absent.
    assert!(
        tree_state.blocks_by_hash.contains_key(&f3.recovered_block().hash()),
        "BUG #199: F3 was NOT pruned by prune_finalized_sidechains — the sidechain \
         branch rooted above the missing B2 ancestor escaped garbage collection"
    );
    assert!(
        tree_state.blocks_by_hash.contains_key(&f4.recovered_block().hash()),
        "BUG #199: F4 was NOT pruned by prune_finalized_sidechains — unbounded \
         memory accumulation in blocks_by_hash"
    );

    // The sidechain blocks also linger in blocks_by_number.
    let blocks_at_3 = tree_state.blocks_by_number.get(&3);
    assert!(
        blocks_at_3
            .map(|v| v.iter().any(|b| b.recovered_block().hash() == f3.recovered_block().hash()))
            .unwrap_or(false),
        "BUG #199: F3 still present in blocks_by_number[3] after finalization"
    );
}
