use cfx_execute_helper::estimation::{EstimateExt, EstimateRequest};
use cfx_executor::executive::{
    execution_outcome::ToRepackError, ExecutionError, ExecutionOutcome,
    TxDropError,
};
use cfx_rpc_cfx_types::{PhantomBlock, RpcImplConfiguration};
use cfx_rpc_eth_api::PreApiServer;

use cfx_execute_helper::exec_tracer::ExecTraceKey;
pub use cfx_parity_trace_types::{
    filter::TraceFilter, trace_types::TransactionExecTraces,
};
use cfx_rpc_common_impl::trace::into_eth_localized_traces;
use cfx_rpc_eth_types::{
    BlockId, Bytes, Log, LogData, PreError, PreErrorCode, PreResult,
    TransactionRequest,
};
use cfx_rpc_utils::error::jsonrpsee_error_helpers::{
    invalid_params_rpc_err, invalid_request_msg,
};
use cfx_types::{Space, H256};
use cfx_util_macros::bail;
use cfx_vm_types::Error as VmError;
use cfxcore::{
    errors::Result as CoreResult, ConsensusGraph, SharedConsensusGraph,
};
use cfxcore_errors::ProviderBlockError;
use jsonrpsee::core::RpcResult;

use solidity_abi::string_revert_reason_decode;

pub struct PreApi {
    consensus: SharedConsensusGraph,
    config: RpcImplConfiguration,
}

impl PreApi {
    pub fn new(
        consensus: SharedConsensusGraph, config: RpcImplConfiguration,
    ) -> Self {
        PreApi { config, consensus }
    }

    pub fn consensus_graph(&self) -> &ConsensusGraph { &self.consensus }

    pub fn get_block_epoch_num(&self, block: BlockId) -> Result<u64, String> {
        let num = match block {
            BlockId::Num(block_number) => block_number,
            BlockId::Latest | BlockId::Safe | BlockId::Finalized => {
                let epoch_num = block.try_into().expect("should success");
                self.consensus_graph()
                    .get_height_from_epoch_number(epoch_num)?
            }
            BlockId::Hash {
                hash,
                require_canonical,
            } => self
                .consensus_graph()
                .get_block_epoch_number_with_pivot_check(
                    &hash,
                    require_canonical.unwrap_or_default(),
                )
                .map_err(|err| err.to_string())?,
            _ => return Err("not supported".to_string()),
        };
        Ok(num)
    }

    pub fn get_block(
        &self, block_number: BlockId,
    ) -> CoreResult<Option<PhantomBlock>> {
        // Only the pivot header is used, so traces are not loaded.
        let phantom_block = match block_number {
            BlockId::Hash { hash, .. } => self
                .consensus_graph()
                .get_phantom_block_by_hash(
                    &hash, false, /* include_traces */
                )
                .map_err(|e| invalid_params_rpc_err(e, None::<()>))?,

            _ => match self.consensus_graph().get_phantom_block_by_number(
                block_number.try_into()?,
                None,
                false, /* include_traces */
            ) {
                Ok(pb) => pb,
                Err(ProviderBlockError::Common(e)) => {
                    return Err(invalid_params_rpc_err(e, None::<()>).into())
                }
                Err(ProviderBlockError::EpochNumberTooLarge) => None,
            },
        };

        Ok(phantom_block)
    }

    fn exec_many_transaction(
        &self, mut request_list: Vec<TransactionRequest>, block: BlockId,
    ) -> CoreResult<Vec<CoreResult<(ExecutionOutcome, EstimateExt)>>> {
        let epoch = self.get_block_epoch_num(block)?;
        let consensus_graph = self.consensus_graph();
        let chain_id = consensus_graph.best_chain_id();
        let mut estimate_request_list = vec![];
        let mut tx_list = vec![];
        let max_gas = self.config.max_estimation_gas_limit;
        for request in request_list.drain(..) {
            if request.gas_price.is_some()
                && request.max_priority_fee_per_gas.is_some()
            {
                bail!(invalid_request_msg(
                    "gasPrice and maxPriorityFeePerGas are mutually exclusive"
                ));
            }

            if request.max_fee_per_gas.is_some()
                && request.max_priority_fee_per_gas.is_some()
            {
                if request.max_fee_per_gas.unwrap()
                    < request.max_priority_fee_per_gas.unwrap()
                {
                    bail!(invalid_request_msg(
                        "maxPriorityFeePerGas is larger than maxFeePerGas"
                    ));
                }
            }
            let estimate_request = EstimateRequest {
                has_sender: request.from.is_some(),
                has_gas_limit: request.gas.is_some(),
                has_gas_price: request.gas_price.is_some(),
                has_nonce: request.nonce.is_some(),
                has_storage_limit: false,
                collect_access_list: false,
            };
            estimate_request_list.push(estimate_request);
            let signed_tx =
                request.sign_call(chain_id.in_evm_space(), max_gas)?;
            tx_list.push(signed_tx);
        }
        consensus_graph.call_many_virtual(
            tx_list,
            epoch.into(),
            estimate_request_list,
        )
    }
}

impl PreApiServer for PreApi {
    fn call_many(
        &self, request_list: Vec<TransactionRequest>,
        block_number_or_hash: Option<BlockId>,
    ) -> RpcResult<Vec<PreResult>> {
        let block = block_number_or_hash.unwrap_or(BlockId::Latest);
        let Some(phantom_block) = self.get_block(block.clone())? else {
            return Err(invalid_params_rpc_err(
                format!("block {:?} not found", block),
                None::<()>,
            ));
        };
        let mut res_list = Vec::new();
        let mut exec_res_list = self.exec_many_transaction(
            request_list,
            block_number_or_hash.unwrap_or(BlockId::Latest),
        )?;
        let mut idx = 0;
        for exec_res in exec_res_list.drain(..) {
            match exec_res {
                Ok(exec_res) => match exec_res {
                    (
                        ExecutionOutcome::NotExecutedDrop(
                            TxDropError::OldNonce(expected, got),
                        ),
                        _,
                    ) => {
                        let pre_error = PreError {
                            msg: format!(
                                "nonce is too old expected {:?} got {:?}",
                                expected, got
                            ),
                            code: PreErrorCode::UnKnown as i64,
                        };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                    (
                        ExecutionOutcome::NotExecutedDrop(
                            TxDropError::InvalidRecipientAddress(recipient),
                        ),
                        _,
                    ) => {
                        let pre_error = PreError {
                            msg: format!(
                                "invalid recipient address {:?}",
                                recipient
                            ),
                            code: PreErrorCode::UnKnown as i64,
                        };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                    (
                        ExecutionOutcome::NotExecutedDrop(
                            TxDropError::NotEnoughGasLimit { .. },
                        ),
                        _,
                    ) => {
                        let pre_error = PreError {
                            msg: "not enough gas limit".to_string(),
                            code: PreErrorCode::UnKnown as i64,
                        };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                    (
                        ExecutionOutcome::NotExecutedToReconsiderPacking(e),
                        _,
                    ) => match e {
                        ToRepackError::NotEnoughCashFromSponsor { .. } => {
                            let pre_error = PreError {
                                        msg: format!("cost of transaction (value + gas_price * gas) exceeds current sponsor balance"),
                                        code: PreErrorCode::InsufficientBalane as i64,
                                    };
                            let pre_res = PreResult {
                                error: pre_error,
                                ..Default::default()
                            };
                            res_list.push(pre_res);
                        }
                        _ => {
                            let pre_error = PreError {
                                msg: format!("Transaction can not be executed"),
                                code: PreErrorCode::UnKnown as i64,
                            };
                            let pre_res = PreResult {
                                error: pre_error,
                                ..Default::default()
                            };
                            res_list.push(pre_res);
                        }
                    },
                    (
                        ExecutionOutcome::ExecutionErrorBumpNonce(
                            ExecutionError::VmError(VmError::Reverted),
                            executed,
                        ),
                        _,
                    ) => {
                        let pre_error = PreError {
                            msg: string_revert_reason_decode(&executed.output),
                            code: PreErrorCode::Reverted as i64,
                        };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                    (
                        ExecutionOutcome::ExecutionErrorBumpNonce(
                            ExecutionError::NotEnoughCash { .. },
                            _,
                        ),
                        _,
                    )
                    | (
                        ExecutionOutcome::ExecutionErrorBumpNonce(
                            ExecutionError::VmError(
                                VmError::NotEnoughBalanceForStorage { .. },
                            ),
                            _,
                        ),
                        _,
                    ) => {
                        let pre_error = PreError {
                                msg: format!("cost of transaction (value + gas_price * gas) exceeds current sponsor balance"),
                                code: PreErrorCode::InsufficientBalane as i64,
                            };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                    (ExecutionOutcome::ExecutionErrorBumpNonce(_, _), _) => {
                        let pre_error = PreError {
                            msg: format!("Transaction can not be executed"),
                            code: PreErrorCode::UnKnown as i64,
                        };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                    (ExecutionOutcome::NotExecutedDrop(_), _) => {
                        let pre_error = PreError {
                            msg: format!("nonce"),
                            code: PreErrorCode::UnKnown as i64,
                        };
                        let pre_res = PreResult {
                            error: pre_error,
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }

                    (ExecutionOutcome::Finished(executed), estimation) => {
                        let traces: TransactionExecTraces = executed
                            .ext_result
                            .get::<ExecTraceKey>()
                            .map_or(&vec![], |x| x.as_ref())
                            .clone()
                            .into();
                        let trace_filter =
                            &TraceFilter::space_filter(Space::Ethereum);
                        let traces = trace_filter.filter_traces(traces);
                        if traces.is_err() {
                            let pre_error = PreError {
                                msg: format!(
                                    "filter_trace_pairs error {:?}",
                                    traces.unwrap_err()
                                ),
                                code: PreErrorCode::UnKnown as i64,
                            };
                            let pre_res = PreResult {
                                error: pre_error,
                                ..Default::default()
                            };
                            res_list.push(pre_res);
                            continue;
                        }
                        let block_number = phantom_block.pivot_header.height();
                        let block_hash = phantom_block.pivot_header.hash();
                        let tx_eth_traces = into_eth_localized_traces(
                            &traces.unwrap(),
                            block_number,
                            block_hash,
                            H256::random(),
                            idx,
                        );
                        idx += 1;
                        if tx_eth_traces.is_err() {
                            let pre_error = PreError {
                                msg: format!(
                                    "filter_trace_pairs error {:?}",
                                    tx_eth_traces.unwrap_err()
                                ),
                                code: PreErrorCode::UnKnown as i64,
                            };
                            let pre_res = PreResult {
                                error: pre_error,
                                ..Default::default()
                            };
                            res_list.push(pre_res);
                            continue;
                        }

                        let mut logs = vec![];
                        let mut transaction_log_index: u32 = 0;
                        for ethereum_log in executed.logs {
                            let log = Log {
                                inner: LogData {
                                    address: ethereum_log.address.clone(),
                                    topics: ethereum_log.topics.clone(),
                                    data: Bytes(ethereum_log.data.clone()),
                                },
                                block_hash,
                                block_number: block_number.into(),
                                transaction_hash: H256::zero(),
                                transaction_index: 0u8.into(),
                                log_index: Some(transaction_log_index.into()),
                                transaction_log_index: Some(
                                    transaction_log_index.into(),
                                ),
                                block_timestamp: None,
                                removed: false,
                            };
                            logs.push(log);
                            transaction_log_index += 1;
                        }
                        let pre_res = PreResult {
                            trace: tx_eth_traces.unwrap(),
                            logs,
                            gas_used: estimation.estimated_gas_limit.as_u64(),
                            ..Default::default()
                        };
                        res_list.push(pre_res);
                    }
                },
                Err(e) => {
                    let pre_error = PreError {
                        msg: e.to_string(),
                        code: PreErrorCode::UnKnown as i64,
                    };
                    let pre_res = PreResult {
                        error: pre_error,
                        ..Default::default()
                    };
                    res_list.push(pre_res);
                }
            }
        }
        Ok(res_list)
    }
}
