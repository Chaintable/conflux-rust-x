//! Extracts state diff data for debank trace_debankBlock.

use super::State;
use crate::state::overlay_account::AccountEntry;
use cfx_statedb::StateDbExt;
use cfx_types::{Space, H256, U256};
use keccak_hash::KECCAK_EMPTY;

/// Raw account diff data for debank.
#[derive(Debug, Clone)]
pub struct DebankAccountDiff {
    pub address: cfx_types::H160,
    pub balance: U256,
    pub nonce: U256,
    pub code_hash: H256,
    pub code: Option<Vec<u8>>,
    pub storage_changes: Vec<(Vec<u8>, U256)>,
    pub is_new: bool,
}

/// Raw state diff collected after block execution.
#[derive(Debug, Clone, Default)]
pub struct DebankStateDiff {
    pub accounts: Vec<DebankAccountDiff>,
    pub deleted_accounts: Vec<cfx_types::H160>,
}

impl State {
    /// Extract state diff data for debank after block re-execution.
    ///
    /// This should be called after `process_epoch_transactions` in dry_run
    /// mode. The cache contains all dirty accounts, and `self.db` still
    /// has the pre-execution state.
    pub fn extract_debank_state_diff(&self) -> DebankStateDiff {
        let cache = self.cache.read();
        let mut diff = DebankStateDiff::default();

        for (addr, entry_with_warm) in cache.iter() {
            // Only process Ethereum space accounts
            if addr.space != Space::Ethereum {
                continue;
            }

            let AccountEntry::Cached(overlay_acc, true) =
                &entry_with_warm.entry
            else {
                continue; // Skip non-dirty entries
            };

            // Check for deleted/selfdestructed accounts
            if overlay_acc.removed_without_update() {
                diff.deleted_accounts.push(addr.address);
                continue;
            }

            // Check if account existed before execution
            let original = self.db.get_account(addr).ok().flatten();
            let is_new = original.is_none();

            // Collect storage changes from write cache
            let mut storage_changes = Vec::new();
            let write_cache = overlay_acc.storage_write_cache().read();
            for (key, item) in write_cache.iter() {
                if let primitives::storage::WriteCacheItem::Write(sv) = item {
                    storage_changes.push((key.clone(), sv.value));
                }
            }

            // Get code if it's a new account with code (contract creation)
            let code_hash = overlay_acc.code_hash();
            let code = if is_new && code_hash != KECCAK_EMPTY {
                // Code is accessible from within crate::state
                overlay_acc.code().map(|c| c.to_vec())
            } else if !is_new {
                // Check if code hash changed
                let orig_code_hash = original
                    .as_ref()
                    .map(|a| a.code_hash)
                    .unwrap_or(KECCAK_EMPTY);
                if code_hash != orig_code_hash && code_hash != KECCAK_EMPTY {
                    overlay_acc.code().map(|c| c.to_vec())
                } else {
                    None
                }
            } else {
                None
            };

            diff.accounts.push(DebankAccountDiff {
                address: addr.address,
                balance: *overlay_acc.balance(),
                nonce: *overlay_acc.nonce(),
                code_hash,
                code,
                storage_changes,
                is_new,
            });
        }

        diff
    }
}
