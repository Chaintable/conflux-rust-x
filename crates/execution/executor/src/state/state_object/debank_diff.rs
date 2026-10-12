//! Extracts the per-epoch state diff for `trace_debankBlock`.

use super::State;
use crate::state::overlay_account::{AccountEntry, OverlayAccount};
use cfx_statedb::{Result as DbResult, StateDbExt};
use cfx_types::{Address, AddressWithSpace, Space, H256, U256};
use keccak_hash::KECCAK_EMPTY;
use std::collections::BTreeMap;

/// Post-execution data of an account modified in the epoch.
#[derive(Debug, Clone)]
pub struct DebankAccountDiff {
    pub address: Address,
    pub balance: U256,
    pub nonce: U256,
    pub code_hash: H256,
    /// Set when the code hash differs from the one before the epoch.
    pub code: Option<Vec<u8>>,
    /// Raw storage keys and their values after the epoch. A zero value means
    /// the slot was deleted.
    pub storage_changes: Vec<(Vec<u8>, U256)>,
}

/// Accounts of one space changed by an epoch.
#[derive(Debug, Clone, Default)]
pub struct DebankStateDiff {
    pub accounts: Vec<DebankAccountDiff>,
    pub deleted_accounts: Vec<Address>,
}

impl State {
    /// Collects the accounts of `space` modified since this `State` was
    /// created. It must be called after all transactions of the epoch have
    /// been executed and before `commit`.
    pub fn extract_debank_state_diff(
        &self, space: Space,
    ) -> DbResult<DebankStateDiff> {
        // Transactions move their dirty accounts to `committed_cache`; entries
        // still in `cache` are newer.
        let mut dirty: BTreeMap<Address, &OverlayAccount> = BTreeMap::new();
        for (addr, entry) in self.committed_cache.iter() {
            if let (true, AccountEntry::Cached(acc, true)) =
                (addr.space == space, entry)
            {
                dirty.insert(addr.address, acc);
            }
        }
        let cache = self.cache.read();
        for (addr, entry) in cache.iter() {
            if let (true, AccountEntry::Cached(acc, true)) =
                (addr.space == space, &entry.entry)
            {
                dirty.insert(addr.address, acc);
            }
        }

        let mut diff = DebankStateDiff::default();
        for (address, acc) in dirty {
            if acc.removed_without_update() {
                diff.deleted_accounts.push(address);
                continue;
            }

            let code_hash = acc.code_hash();
            let code = if code_hash == KECCAK_EMPTY {
                None
            } else {
                let original_code_hash = self
                    .db
                    .get_account(&AddressWithSpace { address, space })?
                    .map(|a| a.code_hash);
                if original_code_hash == Some(code_hash) {
                    None
                } else {
                    acc.code().map(|c| c.to_vec())
                }
            };

            let mut storage_changes = acc.uncommitted_storage_writes();
            storage_changes.sort();

            diff.accounts.push(DebankAccountDiff {
                address,
                balance: *acc.balance(),
                nonce: *acc.nonce(),
                code_hash,
                code,
                storage_changes,
            });
        }
        Ok(diff)
    }
}
