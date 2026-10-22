//! Response types of the DeBank pre-execution RPCs: `pre_traceMany` and
//! `eth_multiCall`. The JSON layout matches the v3.0.0-debank-4 nodes that
//! DeBank consumes.

use crate::{trace::LocalizedTrace, Bytes, Log};
use serde::{Deserialize, Serialize};

#[repr(i64)]
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum PreErrorCode {
    UnKnown = 1000,
    InsufficientBalane = 1001,
    Reverted = 1002,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct PreError {
    pub code: i64,
    pub msg: String,
}

/// Result of one transaction in `pre_traceMany`.
#[derive(Debug, Serialize, Default, Clone)]
pub struct PreResult {
    pub trace: Vec<LocalizedTrace>,
    pub logs: Vec<Log>,
    pub error: PreError,
    #[serde(rename = "gasUsed")]
    pub gas_used: u64,
}

/// Response of `eth_multiCall`.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MultiCallResp {
    pub results: Vec<MultiCallResult>,
    pub stats: MultiCallStats,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MultiCallResult {
    pub code: i32,
    pub err: String,
    pub from_cache: bool,
    pub result: Bytes,
    pub gas_used: i64,
    pub time_cost: f64,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MultiCallStats {
    pub block_num: i64,
    pub block_time: i64,
    pub success: bool,
    pub cache_enabled: bool,
}
