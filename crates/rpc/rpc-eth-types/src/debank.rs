//! Types of the `trace_debankBlock` RPC output.
//!
//! They follow the Chaintable pipeline protocol
//! (github.com/Chaintable/pipeline, `types/` and `docs/protocol.md`): the
//! block file is JSON, the state diff is RLP and IDs are MD5 based.

use crate::Header;
use cfx_rpc_primitives::Bytes;
use cfx_types::{H160, H256, U256};
use md5::{Digest as Md5Digest, Md5};
use num_bigint::BigUint;
use rlp_derive::{RlpDecodable, RlpEncodable};
use serde::{Serialize, Serializer};
use sha1::Sha1;

/// State changes of a block, keyed by hashed addresses and slots.
#[derive(Debug, Clone, Default, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockStorageDiff {
    /// State root after the block.
    pub hash: H256,
    /// State root before the block.
    pub parent_hash: H256,
    pub new_accounts: Vec<NewAccount>,
    /// `keccak(address)` of the deleted accounts.
    pub deleted_accounts: Vec<H256>,
    pub storage_diffs: Vec<AccountStorageDiff>,
    pub new_codes: Vec<NewCode>,
}

/// An account created or updated in the block.
#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct NewAccount {
    /// `keccak(address)`
    pub address: H256,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: H256,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct NewCode {
    pub code_hash: H256,
    pub code: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct AccountStorageDiff {
    /// `keccak(address)`
    pub address: H256,
    pub diffs: Vec<IndexValuePair>,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct IndexValuePair {
    /// `keccak(slot)`
    pub index: H256,
    /// Zero means the slot is deleted.
    pub value: U256,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DebankBlock {
    pub id: String,
    pub height: u64,
    pub parent_id: String,
    pub base_fee_per_gas: u128,
    pub miner: String,
    pub gas_limit: u128,
    pub gas_used: u128,
    pub timestamp: u64,
    /// Unix milliseconds.
    pub process_start_timestamp: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DebankTransaction {
    pub id: String,
    pub from_addr: String,
    /// The created contract address for contract creations.
    pub to_addr: String,
    pub gas_limit: u128,
    pub gas_price: u128,
    pub gas_used: u128,
    pub status: bool,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    pub input: Bytes,
    pub nonce: u128,
    pub idx: i64,
    #[serde(serialize_with = "serialize_hex_big")]
    pub value: U256,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DebankTrace {
    pub id: String,
    pub from_addr: String,
    pub gas_limit: u128,
    pub input: Bytes,
    pub to_addr: String,
    #[serde(serialize_with = "serialize_hex_big")]
    pub value: U256,
    pub gas_used: u128,
    pub output: Bytes,
    /// `call`, `create`, `suicide` or `empty`.
    #[serde(rename = "type")]
    pub call_create_type: String,
    /// `call`, `delegatecall`, `staticcall` or `callcode`; empty unless
    /// `type` is `call`.
    pub call_type: String,
    pub tx_id: String,
    pub parent_trace_id: String,
    pub pos_in_parent_trace: i64,
    pub self_storage_change: bool,
    pub storage_change: bool,
    pub subtraces: i64,
    pub trace_address: Vec<i64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DebankEvent {
    pub id: String,
    pub contract_id: String,
    /// `topics[0]`, empty for anonymous logs.
    pub selector: String,
    /// `topics[1..]`, `null` when the log has no topic.
    pub topics: Option<Vec<String>>,
    pub data: Bytes,
    pub parent_trace_id: String,
    pub pos_in_parent_trace: i64,
    /// Block level log index; 0 for error events.
    pub idx: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
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
    /// Matches Go `BlockFile.Validation().ValidationHash`: error events and
    /// error traces are not included.
    pub fn validation_hash(&self) -> i64 {
        let ids = std::iter::once(&self.block.id)
            .chain(self.txs.iter().map(|tx| &tx.id))
            .chain(self.events.iter().map(|e| &e.id))
            .chain(self.traces.iter().map(|t| &t.id));
        calc_validation_hash(ids)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DebankOutPut {
    pub block_file: BlockFile,
    pub header: Header,
    /// RLP encoded [`BlockStorageDiff`].
    pub state_diff: Bytes,
    pub validation_hash: i64,
}

/// Matches Go `util.ToHash`: hex encoded MD5 of the concatenated args.
pub fn debank_id(args: &[&str]) -> String {
    let mut hasher = Md5::new();
    for arg in args {
        hasher.update(arg.as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Matches Go `types.CalcValidationHash`: the last 6 decimal digits of the
/// sum of the SHA1 of each id.
pub fn calc_validation_hash<'a>(ids: impl Iterator<Item = &'a String>) -> i64 {
    let mut sum = BigUint::from(0u64);
    for id in ids {
        let digest = Sha1::digest(id.as_bytes());
        sum += BigUint::from_bytes_be(&digest);
    }
    let s = sum.to_string();
    s[s.len().saturating_sub(6)..].parse().unwrap_or(0)
}

pub fn format_address(address: &H160) -> String { format!("{:?}", address) }

pub fn format_hash(hash: &H256) -> String { format!("{:?}", hash) }

/// Serializes like Go `hexutil.Big`.
fn serialize_hex_big<S: Serializer>(
    value: &U256, serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{:#x}", value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_hash_matches_go() {
        // pipeline types/block_file_test.go
        let file = BlockFile {
            block: DebankBlock {
                id: "abcd".into(),
                ..Default::default()
            },
            txs: vec![DebankTransaction {
                id: "efgh".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(file.validation_hash(), 217265);
    }

    #[test]
    fn debank_id_is_md5_of_concat() {
        assert_eq!(
            debank_id(&["hello", "world"]),
            "fc5e038d38a57032085441e7fe7010b0"
        );
    }

    #[test]
    fn formats_lowercase_hex() {
        let address: H160 = "0x5BF92508B1935488B8B517E32BA473566DA6CF96"
            .parse()
            .unwrap();
        assert_eq!(
            format_address(&address),
            "0x5bf92508b1935488b8b517e32ba473566da6cf96"
        );
        assert_eq!(format!("{:#x}", U256::zero()), "0x0");
    }

    #[test]
    fn state_diff_rlp_round_trip() {
        let diff = BlockStorageDiff {
            hash: H256::repeat_byte(1),
            parent_hash: H256::repeat_byte(2),
            new_accounts: vec![NewAccount {
                address: H256::repeat_byte(3),
                balance: U256::from(10),
                nonce: 1,
                code_hash: H256::repeat_byte(4),
            }],
            deleted_accounts: vec![H256::repeat_byte(5)],
            storage_diffs: vec![AccountStorageDiff {
                address: H256::repeat_byte(3),
                diffs: vec![IndexValuePair {
                    index: H256::repeat_byte(6),
                    value: U256::zero(),
                }],
            }],
            new_codes: vec![NewCode {
                code_hash: H256::repeat_byte(4),
                code: vec![0x60, 0x00],
            }],
        };
        let encoded = rlp::encode(&diff);
        assert_eq!(rlp::decode::<BlockStorageDiff>(&encoded).unwrap(), diff);
    }
}
