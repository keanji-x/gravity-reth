//! Collection of traits and types for common storage access.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

// Re-export used error types.
pub use reth_storage_errors as errors;
mod account;
pub use account::*;

mod block;
pub use block::*;

mod block_id;
pub use block_id::*;

mod block_hash;
pub use block_hash::*;

#[cfg(feature = "db-api")]
mod chain;
#[cfg(feature = "db-api")]
pub use chain::*;

mod header;
pub use header::*;

mod prune_checkpoint;
pub use prune_checkpoint::*;

mod receipts;
pub use receipts::*;

mod stage_checkpoint;
pub use stage_checkpoint::*;

mod state;
pub use state::*;

mod storage;
pub use storage::*;

mod transactions;
pub use transactions::*;

mod trie;
pub use trie::*;

mod chain_info;
pub use chain_info::*;

#[cfg(feature = "db-api")]
mod database_provider;
#[cfg(feature = "db-api")]
pub use database_provider::*;

pub mod noop;

#[cfg(feature = "db-api")]
mod history;
#[cfg(feature = "db-api")]
pub use history::*;

#[cfg(feature = "db-api")]
mod hashing;
#[cfg(feature = "db-api")]
pub use hashing::*;

#[cfg(feature = "db-api")]
mod stats;
#[cfg(feature = "db-api")]
pub use stats::*;

mod legacy;
pub use legacy::*;

mod primitives;
pub use primitives::*;

mod block_indices;
pub use block_indices::*;

mod cache;
pub use cache::*;

mod block_writer;
pub use block_writer::*;

mod state_writer;
pub use state_writer::*;

mod header_sync_gap;
pub use header_sync_gap::HeaderSyncGapProvider;

mod full;
pub use full::*;
