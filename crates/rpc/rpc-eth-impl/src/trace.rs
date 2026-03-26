use std::vec;

use cfx_addr::Network;
use cfx_parity_trace_types::{
    action_types::Outcome as ParityOutcome, trace_types::TransactionExecTraces,
    Action,
};
use cfx_rpc_cfx_impl::TraceHandler;
use cfx_rpc_cfx_types::PhantomBlock;
use cfx_rpc_common_impl::trace::{
    into_eth_localized_traces, primitive_traces_to_eth_localized_traces,
};
use cfx_rpc_eth_api::TraceApiServer;
use cfx_rpc_eth_types::{
    debank::{
        self, BlockFile, BlockStorageDiff, DebankBlock, DebankEvent,
        DebankOutPut, DebankTrace, DebankTransaction,
    },
    trace::{LocalizedSetAuthTrace, LocalizedTrace as EthLocalizedTrace},
    BlockNumber, Index, LocalizedTrace, TraceFilter,
};
use cfx_types::{H256, U256};
use cfx_util_macros::unwrap_option_or_return_result_none as unwrap_or_return;
use cfxcore::{errors::Result as CoreResult, SharedConsensusGraph};
use geth_tracer::{CallTraceArena, LogCallOrder};
use jsonrpc_core::Error as RpcError;
use jsonrpsee::{core::RpcResult, types::ErrorObjectOwned};
use log::warn;
use primitives::EpochNumber;
pub struct TraceApi {
    trace_handler: TraceHandler,
}

impl TraceApi {
    pub fn new(consensus: SharedConsensusGraph, network: Network) -> TraceApi {
        let trace_handler = TraceHandler::new(network, consensus);
        TraceApi { trace_handler }
    }

    pub fn get_block(
        &self, block_number: BlockNumber,
    ) -> CoreResult<Option<PhantomBlock>> {
        let phantom_block = match block_number {
            BlockNumber::Hash { hash, .. } => self
                .trace_handler
                .consensus_graph()
                .get_phantom_block_by_hash(
                    &hash, true, /* include_traces */
                )
                .map_err(RpcError::invalid_params)?,

            _ => self
                .trace_handler
                .consensus_graph()
                .get_phantom_block_by_number(
                    block_number.try_into()?,
                    None,
                    true, /* include_traces */
                )
                .map_err(RpcError::invalid_params)?,
        };

        Ok(phantom_block)
    }

    pub fn block_traces(
        &self, block_number: BlockNumber,
    ) -> CoreResult<Option<Vec<LocalizedTrace>>> {
        let phantom_block = self.get_block(block_number)?;

        unwrap_or_return!(phantom_block);

        let mut eth_traces = Vec::new();
        let block_number = phantom_block.pivot_header.height();
        let block_hash = phantom_block.pivot_header.hash();

        for (idx, tx_traces) in phantom_block.traces.into_iter().enumerate() {
            let tx_hash = phantom_block.transactions[idx].hash();
            let tx_eth_traces = into_eth_localized_traces(
                &tx_traces.0,
                block_number,
                block_hash,
                tx_hash,
                idx,
            )
            .map_err(|e| {
                warn!("Internal error on trace reconstruction: {}", e);
                RpcError::internal_error()
            })?;
            eth_traces.extend(tx_eth_traces);
        }

        Ok(Some(eth_traces))
    }

    pub fn block_set_auth_traces(
        &self, block_number: BlockNumber,
    ) -> CoreResult<Option<Vec<LocalizedSetAuthTrace>>> {
        let phantom_block = self.get_block(block_number)?;

        unwrap_or_return!(phantom_block);

        let mut eth_traces = Vec::new();
        let block_number = phantom_block.pivot_header.height();
        let block_hash = phantom_block.pivot_header.hash();

        for (idx, tx_traces) in phantom_block.traces.into_iter().enumerate() {
            let tx_hash = phantom_block.transactions[idx].hash();

            let tx_eth_traces: Vec<LocalizedSetAuthTrace> = tx_traces
                .0
                .iter()
                .filter_map(|trace| match trace.action {
                    Action::SetAuth(ref set_auth) => {
                        Some(LocalizedSetAuthTrace::new(
                            set_auth,
                            idx,
                            tx_hash,
                            block_number,
                            block_hash,
                        ))
                    }
                    _ => None,
                })
                .collect();

            eth_traces.extend(tx_eth_traces);
        }

        Ok(Some(eth_traces))
    }

    pub fn filter_traces(
        &self, filter: TraceFilter,
    ) -> CoreResult<Vec<LocalizedTrace>> {
        let primitive_filter = filter.into_primitive()?;

        let Some(primitive_traces) = self
            .trace_handler
            .filter_primitives_traces_impl(primitive_filter)?
        else {
            return Ok(vec![]);
        };

        let traces =
            primitive_traces_to_eth_localized_traces(&primitive_traces)
                .map_err(|e| {
                    warn!("Internal error on trace reconstruction: {}", e);
                    RpcError::internal_error()
                })?;
        Ok(traces)
    }

    pub fn trace_debank_block(
        &self, block_number: BlockNumber,
    ) -> CoreResult<Option<DebankOutPut>> {
        let phantom_block = self.get_block(block_number.clone())?;

        unwrap_or_return!(phantom_block);

        let block_height = phantom_block.pivot_header.height();
        let block_hash = phantom_block.pivot_header.hash();
        let parent_hash = *phantom_block.pivot_header.parent_hash();
        let timestamp = phantom_block.pivot_header.timestamp();
        let gas_limit = phantom_block.total_gas_limit;
        let miner = phantom_block.pivot_header.author().clone();
        let base_fee = phantom_block
            .pivot_header
            .base_price()
            .map(|bp| bp[cfx_types::Space::Ethereum])
            .unwrap_or_default();

        // Genesis block (height 0) - return early with empty data
        if block_height == 0 {
            let debank_block = DebankBlock {
                id: debank::format_hash(&block_hash),
                height: 0,
                parent_id: debank::format_hash(&parent_hash),
                base_fee_per_gas: 0,
                miner: debank::format_address(&miner),
                gas_limit: 0,
                gas_used: 0,
                timestamp,
                process_start_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            };
            let block_file = BlockFile {
                block: debank_block,
                txs: vec![],
                events: vec![],
                traces: vec![],
                error_events: vec![],
                error_traces: vec![],
                storage_contracts: vec![],
            };
            let validation = block_file.validation();
            let header = serde_json::json!({
                "number": "0x0",
                "hash": debank::format_hash(&block_hash),
                "parentHash": debank::format_hash(&parent_hash),
            });
            let empty_diff = BlockStorageDiff::default();
            let state_diff_rlp = rlp::encode(&empty_diff);
            return Ok(Some(DebankOutPut {
                block_file,
                header,
                state_diff: format!("0x{}", hex::encode(&state_diff_rlp)),
                validation_hash: validation.validation_hash,
            }));
        }

        // Build DebankBlock
        let debank_block = DebankBlock {
            id: debank::format_hash(&block_hash),
            height: block_height,
            parent_id: debank::format_hash(&parent_hash),
            base_fee_per_gas: base_fee.as_u64(),
            miner: debank::format_address(&miner),
            gas_limit: gas_limit.as_u64(),
            gas_used: phantom_block
                .receipts
                .last()
                .map(|r| r.accumulated_gas_used.as_u64())
                .unwrap_or(0),
            timestamp,
            process_start_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        };

        // Build DebankTransactions from receipts
        let mut debank_txs = Vec::new();
        let mut cumulative_gas = 0u64;
        for (idx, tx) in phantom_block.transactions.iter().enumerate() {
            let receipt = &phantom_block.receipts[idx];
            let gas_used = receipt
                .accumulated_gas_used
                .as_u64()
                .saturating_sub(cumulative_gas);
            cumulative_gas = receipt.accumulated_gas_used.as_u64();

            let gas_price = tx.gas_price().as_u64();
            let effective_gas_price =
                if let Some(bp) = phantom_block.pivot_header.base_price() {
                    let base = bp[cfx_types::Space::Ethereum];
                    let tip = tx.max_priority_gas_price().as_u64();
                    std::cmp::min(gas_price, base.as_u64() + tip)
                } else {
                    gas_price
                };

            let to_addr = match tx.action() {
                primitives::Action::Call(addr) => debank::format_address(&addr),
                primitives::Action::Create => String::from("0x"),
            };

            let sender_h160: cfx_types::H160 = tx.sender;

            debank_txs.push(DebankTransaction {
                id: debank::format_hash(&tx.hash()),
                from_addr: debank::format_address(&sender_h160),
                to_addr,
                gas_limit: tx.gas_limit().as_u64(),
                gas_price: effective_gas_price,
                gas_used,
                status: receipt.outcome_status
                    == primitives::receipt::TransactionStatus::Success,
                max_fee_per_gas: tx.gas_price().as_u64(),
                max_priority_fee_per_gas: tx.max_priority_gas_price().as_u64(),
                input: cfx_rpc_primitives::Bytes::from(tx.data().to_vec()),
                nonce: tx.nonce().as_u64(),
                idx: idx as i64,
                value: *tx.value(),
            });
        }

        // Collect debank traces via re-execution + phantom trace mapping
        let mut all_traces = Vec::new();
        let mut all_error_traces = Vec::new();
        let mut all_events = Vec::new();
        let mut all_error_events = Vec::new();
        let mut storage_contracts = Vec::new();

        let mut raw_state_diff = None;
        if !phantom_block.transactions.is_empty() {
            match self
                .trace_handler
                .consensus_graph()
                .collect_epoch_debank_trace(block_height)
            {
                Ok((raw_traces, state_diff)) => {
                    // Build set of addresses with storage changes
                    // from state diff (for storage_change flags)
                    let storage_change_addrs: std::collections::HashSet<
                        String,
                    > = state_diff
                        .accounts
                        .iter()
                        .filter(|a| !a.storage_changes.is_empty())
                        .map(|a| debank::format_address(&a.address))
                        .collect();

                    // Build HashMap: tx_hash → CallTraceArena for
                    // eSpace TXs only
                    let mut arena_map = std::collections::HashMap::new();
                    for raw in &raw_traces {
                        if raw.space == cfx_types::Space::Ethereum {
                            arena_map.insert(raw.tx_hash, &raw.arena);
                        }
                    }

                    let mut global_log_index: i64 = 0;

                    // Iterate PhantomBlock transactions (ordered
                    // eSpace view)
                    for (idx, tx) in
                        phantom_block.transactions.iter().enumerate()
                    {
                        let tx_hash = tx.hash();
                        let tx_id = debank::format_hash(&tx_hash);

                        if let Some(arena) = arena_map.get(&tx_hash) {
                            // Pure eSpace TX: use full CallTraceArena
                            let (
                                traces,
                                err_traces,
                                events,
                                err_events,
                                contracts,
                            ) = build_debank_traces_from_arena(
                                &tx_id,
                                arena,
                                &mut global_log_index,
                                &storage_change_addrs,
                            );
                            all_traces.extend(traces);
                            all_error_traces.extend(err_traces);
                            all_events.extend(events);
                            all_error_events.extend(err_events);
                            storage_contracts.extend(contracts);
                        } else if idx < phantom_block.traces.len() {
                            // Phantom TX: convert parity traces
                            let parity_traces = &phantom_block.traces[idx];
                            let (traces, err_traces, events, err_events) =
                                build_debank_traces_from_parity(
                                    &tx_id,
                                    parity_traces,
                                    &mut global_log_index,
                                );
                            all_traces.extend(traces);
                            all_error_traces.extend(err_traces);
                            all_events.extend(events);
                            all_error_events.extend(err_events);
                        }
                    }
                    raw_state_diff = Some(state_diff);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        // Build BlockFile
        let block_file = BlockFile {
            block: debank_block,
            txs: debank_txs,
            events: all_events,
            traces: all_traces,
            error_events: all_error_events,
            error_traces: all_error_traces,
            storage_contracts,
        };

        let validation = block_file.validation();

        // Build header as JSON
        let header = serde_json::json!({
            "number": format!("0x{:x}", block_height),
            "hash": debank::format_hash(&block_hash),
            "parentHash": debank::format_hash(&parent_hash),
            "stateRoot": debank::format_hash(&phantom_block.pivot_header.deferred_state_root().clone()),
            "miner": debank::format_address(&miner),
            "gasLimit": format!("0x{:x}", gas_limit),
            "gasUsed": format!("0x{:x}", block_file.block.gas_used),
            "timestamp": format!("0x{:x}", timestamp),
            "baseFeePerGas": format!("0x{:x}", base_fee),
        });

        // Build state diff from raw diff data
        let storage_diff = if let Some(raw_diff) = raw_state_diff {
            build_block_storage_diff(&block_hash, &parent_hash, raw_diff)
        } else {
            BlockStorageDiff {
                hash: block_hash,
                parent_hash,
                ..Default::default()
            }
        };
        let state_diff_rlp = rlp::encode(&storage_diff);
        let state_diff = format!("0x{}", hex::encode(&state_diff_rlp));

        Ok(Some(DebankOutPut {
            block_file,
            header,
            state_diff,
            validation_hash: validation.validation_hash,
        }))
    }

    pub fn transaction_traces(
        &self, tx_hash: H256,
    ) -> CoreResult<Option<Vec<EthLocalizedTrace>>> {
        let tx_index = self
            .trace_handler
            .data_man
            .transaction_index_by_hash(&tx_hash, false /* update_cache */);

        unwrap_or_return!(tx_index);

        let epoch_num = self
            .trace_handler
            .consensus
            .get_block_epoch_number(&tx_index.block_hash);

        unwrap_or_return!(epoch_num);

        let phantom_block = self
            .trace_handler
            .consensus_graph()
            .get_phantom_block_by_number(
                EpochNumber::Number(epoch_num),
                None,
                true, /* include_traces */
            )
            .map_err(RpcError::invalid_params)?;

        unwrap_or_return!(phantom_block);

        // find tx corresponding to `tx_hash`
        let id = phantom_block
            .transactions
            .iter()
            .position(|tx| tx.hash() == tx_hash);

        unwrap_or_return!(id);

        let tx = &phantom_block.transactions[id];
        let tx_traces = phantom_block.traces[id].clone();

        let eth_traces = into_eth_localized_traces(
            &tx_traces.0,
            epoch_num,
            phantom_block.pivot_header.hash(),
            tx.hash(),
            id,
        )
        .map_err(|e| {
            warn!("Internal error on trace reconstruction: {}", e);
            RpcError::internal_error()
        })?;

        Ok(Some(eth_traces))
    }
}

#[async_trait::async_trait]
impl TraceApiServer for TraceApi {
    async fn block_traces(
        &self, block_number: BlockNumber,
    ) -> RpcResult<Option<Vec<LocalizedTrace>>> {
        self.block_traces(block_number).map_err(|err| err.into())
    }

    async fn filter_traces(
        &self, filter: TraceFilter,
    ) -> RpcResult<Vec<LocalizedTrace>> {
        self.filter_traces(filter).map_err(|err| err.into())
    }

    async fn transaction_traces(
        &self, tx_hash: H256,
    ) -> RpcResult<Option<Vec<EthLocalizedTrace>>> {
        self.transaction_traces(tx_hash).map_err(|err| err.into())
    }

    async fn block_set_auth_traces(
        &self, block_number: BlockNumber,
    ) -> RpcResult<Option<Vec<LocalizedSetAuthTrace>>> {
        self.block_set_auth_traces(block_number)
            .map_err(|err| err.into())
    }

    async fn trace_get(
        &self, tx_hash: H256, indices: Vec<Index>,
    ) -> RpcResult<Option<LocalizedTrace>> {
        if indices.is_empty() {
            return Ok(None);
        }
        let Some(traces) = self
            .transaction_traces(tx_hash)
            .map_err(|err| ErrorObjectOwned::from(err))?
        else {
            return Ok(None);
        };
        let index = indices[0].value();
        Ok(traces.get(index).cloned())
    }

    async fn trace_debank_block(
        &self, block_number: BlockNumber,
    ) -> RpcResult<Option<DebankOutPut>> {
        self.trace_debank_block(block_number)
            .map_err(|err| err.into())
    }
}

// ============================================================
// Debank trace conversion helpers
// ============================================================

/// Convert a CallTraceArena to debank traces and events.
fn build_debank_traces_from_arena(
    tx_id: &str, arena: &CallTraceArena, global_log_index: &mut i64,
    storage_change_addrs: &std::collections::HashSet<String>,
) -> (
    Vec<DebankTrace>,
    Vec<DebankTrace>,
    Vec<DebankEvent>,
    Vec<DebankEvent>,
    Vec<String>,
) {
    let mut traces = Vec::new();
    let mut error_traces = Vec::new();
    let mut events = Vec::new();
    let mut error_events = Vec::new();
    let mut storage_contracts = Vec::new();

    if arena.arena.is_empty() {
        return (
            traces,
            error_traces,
            events,
            error_events,
            storage_contracts,
        );
    }

    // Walk the arena tree starting from root (index 0)
    walk_trace_node(
        tx_id,
        arena,
        0,   // root node index
        "",  // no parent trace id for root
        0,   // pos in parent
        &[], // empty trace address for root
        global_log_index,
        &mut traces,
        &mut error_traces,
        &mut events,
        &mut error_events,
        &mut storage_contracts,
        storage_change_addrs,
    );

    (
        traces,
        error_traces,
        events,
        error_events,
        storage_contracts,
    )
}

fn walk_trace_node(
    tx_id: &str, arena: &CallTraceArena, node_idx: usize,
    parent_trace_id: &str, pos_in_parent: i64, trace_address: &[i64],
    global_log_index: &mut i64, traces: &mut Vec<DebankTrace>,
    error_traces: &mut Vec<DebankTrace>, events: &mut Vec<DebankEvent>,
    error_events: &mut Vec<DebankEvent>, storage_contracts: &mut Vec<String>,
    storage_change_addrs: &std::collections::HashSet<String>,
) -> bool {
    let node = &arena.arena[node_idx];
    let trace = &node.trace;

    // Generate trace ID
    let trace_id = debank::debank_id(&[
        tx_id,
        parent_trace_id,
        &pos_in_parent.to_string(),
    ]);

    // Determine call type
    let (call_create_type, call_type) = match trace.kind {
        geth_tracer::CallKind::Call => ("call".to_string(), "call".to_string()),
        geth_tracer::CallKind::StaticCall => {
            ("call".to_string(), "staticcall".to_string())
        }
        geth_tracer::CallKind::DelegateCall => {
            ("call".to_string(), "delegatecall".to_string())
        }
        geth_tracer::CallKind::CallCode => {
            ("call".to_string(), "callcode".to_string())
        }
        geth_tracer::CallKind::Create | geth_tracer::CallKind::Create2 => {
            ("create".to_string(), "create".to_string())
        }
    };

    // Check if this trace's execution address has storage changes
    // (inferred from block-level state diff)
    let exec_addr =
        debank::format_address_bytes(node.execution_address().as_slice());
    let self_storage_change = storage_change_addrs.contains(&exec_addr);

    if self_storage_change && !storage_contracts.contains(&exec_addr) {
        storage_contracts.push(exec_addr);
    }

    // Process children and collect their storage_change status
    let mut child_has_storage = false;
    let mut child_call_pos: i64 = 0;
    let mut child_log_pos: i64 = 0;

    // Walk through ordering to maintain correct log/call interleaving
    for order in &node.ordering {
        match order {
            LogCallOrder::Call(child_local_idx) => {
                let child_arena_idx = node.children[*child_local_idx];
                let mut child_trace_addr = trace_address.to_vec();
                child_trace_addr.push(child_call_pos);

                let child_storage = walk_trace_node(
                    tx_id,
                    arena,
                    child_arena_idx,
                    &trace_id,
                    child_call_pos,
                    &child_trace_addr,
                    global_log_index,
                    traces,
                    error_traces,
                    events,
                    error_events,
                    storage_contracts,
                    storage_change_addrs,
                );
                child_has_storage |= child_storage;
                child_call_pos += 1;
            }
            LogCallOrder::Log(log_idx) => {
                let log = &node.logs[*log_idx];
                let event_id =
                    debank::debank_id(&[&trace_id, &child_log_pos.to_string()]);

                let selector = log
                    .topics()
                    .first()
                    .map(|t| format!("0x{}", hex::encode(t.as_slice())))
                    .unwrap_or_default();

                let topics: Vec<String> = log
                    .topics()
                    .iter()
                    .map(|t| format!("0x{}", hex::encode(t.as_slice())))
                    .collect();

                let event = DebankEvent {
                    id: event_id,
                    contract_id: debank::format_address_bytes(
                        node.execution_address().as_slice(),
                    ),
                    selector,
                    topics,
                    data: cfx_rpc_primitives::Bytes::from(log.data.to_vec()),
                    parent_trace_id: trace_id.clone(),
                    pos_in_parent_trace: child_log_pos,
                    idx: *global_log_index,
                };
                *global_log_index += 1;
                child_log_pos += 1;

                if trace.is_error() {
                    error_events.push(event);
                } else {
                    events.push(event);
                }
            }
        }
    }

    let storage_change = self_storage_change || child_has_storage;

    // Build error message
    let error = if trace.is_error() {
        match trace.status {
            revm_interpreter::InstructionResult::Revert => {
                "execution reverted".to_string()
            }
            revm_interpreter::InstructionResult::OutOfGas => {
                "out of gas".to_string()
            }
            _ => format!("{:?}", trace.status),
        }
    } else {
        String::new()
    };

    let from_addr = debank::format_address_bytes(trace.caller.as_slice());
    let to_addr = debank::format_address_bytes(trace.address.as_slice());

    let debank_trace = DebankTrace {
        id: trace_id.clone(),
        from_addr,
        gas_limit: trace.gas_limit,
        input: cfx_rpc_primitives::Bytes::from(trace.data.to_vec()),
        to_addr,
        value: U256::from_big_endian(&trace.value.to_be_bytes::<32>()),
        gas_used: trace.gas_used,
        output: cfx_rpc_primitives::Bytes::from(trace.output.to_vec()),
        call_create_type,
        call_type,
        tx_id: tx_id.to_string(),
        parent_trace_id: parent_trace_id.to_string(),
        pos_in_parent_trace: pos_in_parent,
        self_storage_change,
        storage_change,
        subtraces: child_call_pos,
        trace_address: trace_address.to_vec(),
        error,
    };

    if trace.is_error() {
        error_traces.push(debank_trace);
    } else {
        traces.push(debank_trace);
    }

    // Handle selfdestruct
    if node.is_selfdestruct() {
        if let Some(refund_target) = trace.selfdestruct_refund_target {
            let sd_pos = child_call_pos;
            let sd_id =
                debank::debank_id(&[tx_id, &trace_id, &sd_pos.to_string()]);
            let mut sd_trace_addr = trace_address.to_vec();
            sd_trace_addr.push(sd_pos);

            let sd_trace = DebankTrace {
                id: sd_id,
                from_addr: debank::format_address_bytes(
                    trace.address.as_slice(),
                ),
                gas_limit: 0,
                input: Default::default(),
                to_addr: debank::format_address_bytes(refund_target.as_slice()),
                value: U256::from_big_endian(&trace.value.to_be_bytes::<32>()),
                gas_used: 0,
                output: Default::default(),
                call_create_type: "suicide".to_string(),
                call_type: "suicide".to_string(),
                tx_id: tx_id.to_string(),
                parent_trace_id: trace_id.clone(),
                pos_in_parent_trace: sd_pos,
                self_storage_change: false,
                storage_change: false,
                subtraces: 0,
                trace_address: sd_trace_addr,
                error: String::new(),
            };
            traces.push(sd_trace);
        }
    }

    storage_change
}

/// Convert raw state diff data to BlockStorageDiff.
fn build_block_storage_diff(
    block_hash: &H256, parent_hash: &H256,
    raw_diff: cfx_executor::state::debank_diff::DebankStateDiff,
) -> BlockStorageDiff {
    use cfx_rpc_eth_types::debank::{
        AccountStorageDiff, IndexValuePair, NewAccount, NewCode,
    };
    use keccak_hash::keccak;

    let mut new_accounts = Vec::new();
    let mut storage_diffs = Vec::new();
    let mut new_codes = Vec::new();
    let mut seen_code_hashes = std::collections::HashSet::new();

    for acc_diff in &raw_diff.accounts {
        let addr_hash = keccak(&acc_diff.address);

        if acc_diff.is_new {
            new_accounts.push(NewAccount {
                address_hash: addr_hash,
                balance: acc_diff.balance,
                nonce: acc_diff.nonce.as_u64(),
                code_hash: acc_diff.code_hash,
            });
        }

        // Collect storage changes
        if !acc_diff.storage_changes.is_empty() {
            let values: Vec<IndexValuePair> = acc_diff
                .storage_changes
                .iter()
                .map(|(key, value)| {
                    let key_hash = keccak(key);
                    let mut val_bytes = [0u8; 32];
                    value.to_big_endian(&mut val_bytes);
                    // Trim leading zeros
                    let trimmed = val_bytes
                        .iter()
                        .position(|&b| b != 0)
                        .map(|i| val_bytes[i..].to_vec())
                        .unwrap_or_default();
                    IndexValuePair {
                        index: key_hash,
                        value: trimmed,
                    }
                })
                .collect();
            storage_diffs.push(AccountStorageDiff {
                address_hash: addr_hash,
                values,
            });
        }

        // Collect new code
        if let Some(ref code) = acc_diff.code {
            if seen_code_hashes.insert(acc_diff.code_hash) {
                new_codes.push(NewCode {
                    code_hash: acc_diff.code_hash,
                    code: code.clone(),
                });
            }
        }
    }

    let deleted_accounts = raw_diff
        .deleted_accounts
        .iter()
        .map(|addr| keccak(addr))
        .collect();

    BlockStorageDiff {
        hash: *block_hash,
        parent_hash: *parent_hash,
        new_accounts,
        deleted_accounts,
        storage_diff: storage_diffs,
        new_codes,
    }
}

/// Convert parity-format traces (from PhantomBlock) to debank traces.
/// Used for phantom transactions (cross-space calls).
fn build_debank_traces_from_parity(
    tx_id: &str, parity_traces: &TransactionExecTraces,
    _global_log_index: &mut i64,
) -> (
    Vec<DebankTrace>,
    Vec<DebankTrace>,
    Vec<DebankEvent>,
    Vec<DebankEvent>,
) {
    let mut all_traces: Vec<DebankTrace> = Vec::new();
    let mut error_traces = Vec::new();
    let events = Vec::new();
    let error_events = Vec::new();

    // Reconstruct trace tree from flat parity trace list.
    // Use a stack of indices into all_traces to match Call/Create with
    // their Result.
    let mut trace_address: Vec<i64> = Vec::new();
    let mut parent_ids: Vec<String> = vec![String::new()];
    let mut child_pos: Vec<i64> = vec![0];
    // Stack of indices into all_traces for matching Result to Call/Create
    let mut call_stack: Vec<usize> = Vec::new();

    for exec_trace in &parity_traces.0 {
        match &exec_trace.action {
            Action::Call(call) => {
                let parent_id = parent_ids.last().cloned().unwrap_or_default();
                let pos = *child_pos.last().unwrap_or(&0);
                let trace_id =
                    debank::debank_id(&[tx_id, &parent_id, &pos.to_string()]);

                let (call_create_type, call_type) = match call.call_type {
                    cfx_vm_types::CallType::Call => ("call", "call"),
                    cfx_vm_types::CallType::StaticCall => {
                        ("call", "staticcall")
                    }
                    cfx_vm_types::CallType::DelegateCall => {
                        ("call", "delegatecall")
                    }
                    cfx_vm_types::CallType::CallCode => ("call", "callcode"),
                    cfx_vm_types::CallType::None => ("call", "call"),
                };

                let idx = all_traces.len();
                all_traces.push(DebankTrace {
                    id: trace_id.clone(),
                    from_addr: debank::format_address(&call.from),
                    gas_limit: call.gas.as_u64(),
                    input: cfx_rpc_primitives::Bytes::from(call.input.clone()),
                    to_addr: debank::format_address(&call.to),
                    value: call.value,
                    gas_used: 0,
                    output: Default::default(),
                    call_create_type: call_create_type.to_string(),
                    call_type: call_type.to_string(),
                    tx_id: tx_id.to_string(),
                    parent_trace_id: parent_id,
                    pos_in_parent_trace: pos,
                    self_storage_change: false,
                    storage_change: false,
                    subtraces: 0,
                    trace_address: trace_address.clone(),
                    error: String::new(),
                });

                call_stack.push(idx);
                trace_address.push(pos);
                parent_ids.push(trace_id);
                child_pos.push(0);
            }
            Action::CallResult(result) => {
                trace_address.pop();
                parent_ids.pop();
                child_pos.pop();

                if let Some(pos) = child_pos.last_mut() {
                    *pos += 1;
                }

                // Match with the corresponding Call via stack
                if let Some(matched_idx) = call_stack.pop() {
                    let t = &mut all_traces[matched_idx];
                    t.gas_used =
                        t.gas_limit.saturating_sub(result.gas_left.as_u64());
                    t.output = cfx_rpc_primitives::Bytes::from(
                        result.return_data.clone(),
                    );
                    if result.outcome != ParityOutcome::Success {
                        t.error = match result.outcome {
                            ParityOutcome::Reverted => {
                                "execution reverted".to_string()
                            }
                            ParityOutcome::Fail => {
                                "internal failure".to_string()
                            }
                            _ => "unknown error".to_string(),
                        };
                    }
                } else {
                    warn!("Parity trace CallResult without matching Call");
                }
            }
            Action::Create(create) => {
                let parent_id = parent_ids.last().cloned().unwrap_or_default();
                let pos = *child_pos.last().unwrap_or(&0);
                let trace_id =
                    debank::debank_id(&[tx_id, &parent_id, &pos.to_string()]);

                let idx = all_traces.len();
                all_traces.push(DebankTrace {
                    id: trace_id.clone(),
                    from_addr: debank::format_address(&create.from),
                    gas_limit: create.gas.as_u64(),
                    input: cfx_rpc_primitives::Bytes::from(create.init.clone()),
                    to_addr: String::from("0x"),
                    value: create.value,
                    gas_used: 0,
                    output: Default::default(),
                    call_create_type: "create".to_string(),
                    call_type: "create".to_string(),
                    tx_id: tx_id.to_string(),
                    parent_trace_id: parent_id,
                    pos_in_parent_trace: pos,
                    self_storage_change: false,
                    storage_change: false,
                    subtraces: 0,
                    trace_address: trace_address.clone(),
                    error: String::new(),
                });

                call_stack.push(idx);
                trace_address.push(pos);
                parent_ids.push(trace_id);
                child_pos.push(0);
            }
            Action::CreateResult(result) => {
                trace_address.pop();
                parent_ids.pop();
                child_pos.pop();

                if let Some(pos) = child_pos.last_mut() {
                    *pos += 1;
                }

                if let Some(matched_idx) = call_stack.pop() {
                    let t = &mut all_traces[matched_idx];
                    t.gas_used =
                        t.gas_limit.saturating_sub(result.gas_left.as_u64());
                    t.to_addr = debank::format_address(&result.addr);
                    t.output = cfx_rpc_primitives::Bytes::from(
                        result.return_data.clone(),
                    );
                    if result.outcome != ParityOutcome::Success {
                        t.error = match result.outcome {
                            ParityOutcome::Reverted => {
                                "execution reverted".to_string()
                            }
                            _ => "unknown error".to_string(),
                        };
                    }
                } else {
                    warn!("Parity trace CreateResult without matching Create");
                }
            }
            _ => {}
        }
    }

    // Update subtraces count using O(n) parent lookup via trace_address
    let mut subtraces_map: std::collections::HashMap<Vec<i64>, i64> =
        std::collections::HashMap::new();
    for t in &all_traces {
        if !t.trace_address.is_empty() {
            let parent_addr = &t.trace_address[..t.trace_address.len() - 1];
            *subtraces_map.entry(parent_addr.to_vec()).or_insert(0) += 1;
        }
    }
    for t in &mut all_traces {
        if let Some(&count) = subtraces_map.get(&t.trace_address) {
            t.subtraces = count;
        }
    }

    // Separate error traces from success traces
    let mut traces = Vec::new();
    for t in all_traces {
        if t.error.is_empty() {
            traces.push(t);
        } else {
            error_traces.push(t);
        }
    }

    (traces, error_traces, events, error_events)
}
