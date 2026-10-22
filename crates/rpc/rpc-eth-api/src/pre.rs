use cfx_rpc_eth_types::{BlockId, PreResult, TransactionRequest};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};

/// DeBank pre-execution RPC: runs the requests one after another on the state
/// of the given block and returns each one's traces, logs and gas used.
#[rpc(server, namespace = "pre")]
pub trait PreApi {
    #[method(name = "traceMany")]
    fn call_many(
        &self, request_list: Vec<TransactionRequest>,
        block_number_or_hash: Option<BlockId>,
    ) -> RpcResult<Vec<PreResult>>;
}
