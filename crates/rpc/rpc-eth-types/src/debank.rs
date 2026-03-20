//! Pipeline (Debank) types for `trace_debankBlock` RPC method.
//!
//! These types mirror the Go Pipeline `types/` package and are used for the
//! `trace_debankBlock` RPC output.

use cfx_rpc_primitives::Bytes;
use cfx_types::{H256, U256};
use md5::{Digest as Md5Digest, Md5};
use num_bigint::BigUint;
use rlp_derive::{RlpDecodable, RlpEncodable};
use serde::{Deserialize, Serialize};
use sha1::Sha1;

// ============================================================
// State Diff Types (RLP-encoded for binary transmission)
// ============================================================

/// Complete state diff for a block.
#[derive(Debug, Clone, Default, RlpEncodable, RlpDecodable)]
pub struct BlockStorageDiff {
    pub hash: H256,
    pub parent_hash: H256,
    pub new_accounts: Vec<NewAccount>,
    pub deleted_accounts: Vec<H256>,
    pub storage_diff: Vec<AccountStorageDiff>,
    pub new_codes: Vec<NewCode>,
}

/// A new account created in this block.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct NewAccount {
    pub address_hash: H256,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: H256,
}

/// New contract code deployed in this block.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct NewCode {
    pub code_hash: H256,
    pub code: Vec<u8>,
}

/// Storage diff for a single account.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct AccountStorageDiff {
    pub address_hash: H256,
    pub values: Vec<IndexValuePair>,
}

/// A single storage slot change.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct IndexValuePair {
    pub index: H256,
    pub value: Vec<u8>,
}

// ============================================================
// Block File Types (JSON-serialized for RPC output)
// ============================================================

/// Debank block metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebankBlock {
    pub id: String,
    pub height: u64,
    pub parent_id: String,
    pub base_fee_per_gas: u64,
    pub miner: String,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub process_start_timestamp: i64,
}

/// Debank transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebankTransaction {
    pub id: String,
    pub from_addr: String,
    pub to_addr: String,
    pub gas_limit: u64,
    pub gas_price: u64,
    pub gas_used: u64,
    pub status: bool,
    pub max_fee_per_gas: u64,
    pub max_priority_fee_per_gas: u64,
    pub input: Bytes,
    pub nonce: u64,
    pub idx: i64,
    #[serde(serialize_with = "serialize_u256_hex")]
    pub value: U256,
}

/// Debank trace (call/create/suicide).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebankTrace {
    pub id: String,
    pub from_addr: String,
    pub gas_limit: u64,
    pub input: Bytes,
    pub to_addr: String,
    #[serde(serialize_with = "serialize_u256_hex")]
    pub value: U256,
    pub gas_used: u64,
    pub output: Bytes,
    #[serde(rename = "type")]
    pub call_create_type: String,
    pub call_type: String,
    pub tx_id: String,
    pub parent_trace_id: String,
    pub pos_in_parent_trace: i64,
    pub self_storage_change: bool,
    pub storage_change: bool,
    pub subtraces: i64,
    pub trace_address: Vec<i64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// Debank event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebankEvent {
    pub id: String,
    pub contract_id: String,
    pub selector: String,
    pub topics: Vec<String>,
    pub data: Bytes,
    pub parent_trace_id: String,
    pub pos_in_parent_trace: i64,
    pub idx: i64,
}

/// Block file containing all debank data for one block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockFile {
    pub block: DebankBlock,
    pub txs: Vec<DebankTransaction>,
    pub events: Vec<DebankEvent>,
    pub traces: Vec<DebankTrace>,
    pub error_events: Vec<DebankEvent>,
    pub error_traces: Vec<DebankTrace>,
    pub storage_contracts: Vec<String>,
}

impl BlockFile {
    /// Compute block validation from the block file.
    pub fn validation(&self) -> BlockValidation {
        let mut ids = Vec::new();
        ids.push(self.block.id.clone());
        for tx in &self.txs {
            ids.push(tx.id.clone());
        }
        for event in &self.events {
            ids.push(event.id.clone());
        }
        for trace in &self.traces {
            ids.push(trace.id.clone());
        }
        for event in &self.error_events {
            ids.push(event.id.clone());
        }
        for trace in &self.error_traces {
            ids.push(trace.id.clone());
        }

        BlockValidation {
            validation_hash: calc_validation_hash(&ids),
            is_fork: false,
            txs_count: self.txs.len(),
            events_count: self.events.len(),
            traces_count: self.traces.len(),
            error_events_count: self.error_events.len(),
            error_traces_count: self.error_traces.len(),
            storage_contracts_count: self.storage_contracts.len(),
        }
    }
}

/// Block validation metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockValidation {
    pub validation_hash: i64,
    pub is_fork: bool,
    pub txs_count: usize,
    pub events_count: usize,
    pub traces_count: usize,
    pub error_events_count: usize,
    pub error_traces_count: usize,
    pub storage_contracts_count: usize,
}

/// Final RPC output for `trace_debankBlock`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebankOutPut {
    pub block_file: BlockFile,
    pub header: serde_json::Value,
    pub state_diff: String,
    pub validation_hash: i64,
}

// ============================================================
// ID Generation (MD5, matching Go's util.ToHash)
// ============================================================

/// Generate a debank ID by MD5 hashing the concatenated args.
///
/// Matches Go's `util.ToHash(args ...string)`.
pub fn debank_id(args: &[&str]) -> String {
    let mut hasher = Md5::new();
    for arg in args {
        hasher.update(arg.as_bytes());
    }
    hex::encode(hasher.finalize())
}

// ============================================================
// Validation Hash (SHA1, matching Go's CalcValidationHash)
// ============================================================

/// Calculate the validation hash from a list of IDs.
///
/// Matches Go's `types.CalcValidationHash(ids []string)`:
/// - SHA1 hash each ID
/// - Sum all hashes as big integers
/// - Take the last 6 decimal digits as i64
pub fn calc_validation_hash(ids: &[String]) -> i64 {
    let mut sha1_sum = BigUint::from(0u64);
    for id in ids {
        let mut hasher = Sha1::new();
        hasher.update(id.as_bytes());
        let hash = hex::encode(hasher.finalize());
        if let Some(hash_int) = BigUint::parse_bytes(hash.as_bytes(), 16) {
            sha1_sum += hash_int;
        }
    }
    let s = sha1_sum.to_string();
    let last6 = if s.len() >= 6 {
        &s[s.len() - 6..]
    } else {
        &s
    };
    last6.parse::<i64>().unwrap_or(0)
}

// ============================================================
// Serialization Helpers
// ============================================================

/// Serialize U256 as a hex string with "0x" prefix (matching Go's
/// hexutil.Big).
fn serialize_u256_hex<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    if value.is_zero() {
        serializer.serialize_str("0x0")
    } else {
        serializer.serialize_str(&format!("{:#x}", value))
    }
}

/// Format a cfx_types::H160 address as lowercase hex with "0x" prefix.
pub fn format_address(addr: &cfx_types::H160) -> String {
    format!("0x{}", hex::encode(addr.as_bytes()))
}

/// Format a 20-byte slice as lowercase hex address with "0x" prefix.
/// Use this for alloy_primitives::Address or any 20-byte address.
pub fn format_address_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Format a hash as lowercase hex with "0x" prefix.
pub fn format_hash(hash: &H256) -> String {
    format!("0x{}", hex::encode(hash.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debank_id() {
        let id = debank_id(&["hello", "world"]);
        // MD5 of "helloworld"
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_calc_validation_hash() {
        let ids = vec!["abc".to_string(), "def".to_string()];
        let hash = calc_validation_hash(&ids);
        assert!(hash >= 0);
        assert!(hash < 1_000_000); // at most 6 digits
    }

    #[test]
    fn test_empty_validation_hash() {
        let ids: Vec<String> = vec![];
        let hash = calc_validation_hash(&ids);
        assert_eq!(hash, 0);
    }
}
