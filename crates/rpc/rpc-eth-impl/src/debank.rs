//! `trace_debankBlock`: re-executes an eSpace block (a Conflux epoch) and
//! converts the result to the Chaintable pipeline format.
//!
//! The conversion follows the Go pipeline `callTracer`
//! (github.com/Chaintable/pipeline, `tracer/call_tracer.go`) so that IDs,
//! positions and failure flags are identical to the other chains.

use std::collections::{HashMap, HashSet, VecDeque};

use cfx_execute_helper::observer::debank_tracer::{
    DebankFrame, DebankFrameItem, DebankFrameKind, DebankLog,
};
use cfx_executor::{
    internal_contract::{is_call_create_sig, is_withdraw_sig},
    state::debank_diff::DebankStateDiff,
};
use cfx_parameters::internal_contract_addresses::CROSS_SPACE_CONTRACT_ADDRESS;
use cfx_rpc_cfx_types::PhantomBlock;
use cfx_rpc_eth_types::{
    debank::{
        debank_id, format_address, format_hash, AccountStorageDiff, BlockFile,
        BlockStorageDiff, DebankBlock, DebankEvent, DebankOutPut, DebankTrace,
        DebankTransaction, IndexValuePair, NewAccount, NewCode,
    },
    Header, Transaction,
};
use cfx_types::{Address, Space, H256, U256};
use cfx_util_macros::bail;
use cfx_vm_types::{CallType, CreateType};
use cfxcore::{
    consensus::consensus_inner::consensus_executor::debank_live::DebankEpochTrace,
    errors::Result as CoreResult, ConsensusGraph,
};
use keccak_hash::keccak;
use primitives::{receipt::TransactionStatus, Action, SignedTransaction};

const PARENT_CALL_FAILED: &str = "parent call failed";

/// Re-executes the epoch of `pb` and builds its pipeline output.
pub fn trace_debank_block(
    consensus: &ConsensusGraph, pb: PhantomBlock,
) -> CoreResult<DebankOutPut> {
    let height = pb.pivot_header.height();
    let pivot_hash = pb.pivot_header.hash();

    let epoch = if height == 0 {
        DebankEpochTrace {
            traces: vec![],
            state_diff: DebankStateDiff::default(),
            state_root: consensus.epoch_state_root_hash(&pivot_hash)?,
            parent_state_root: H256::zero(),
        }
    } else {
        consensus.collect_epoch_debank_trace(height, pivot_hash)?
    };
    build_debank_output(&pb, epoch)
}

/// Builds the pipeline output of an epoch from its eSpace block and the call
/// trees and state diff recorded while executing it.
pub fn build_debank_output(
    pb: &PhantomBlock, epoch: DebankEpochTrace,
) -> CoreResult<DebankOutPut> {
    let height = pb.pivot_header.height();
    let pivot_hash = pb.pivot_header.hash();
    let DebankEpochTrace {
        traces: raw_traces,
        state_diff,
        state_root,
        parent_state_root,
    } = epoch;

    let mut header = Header::from_phantom(pb);
    // The eSpace header carries the deferred state root of epoch
    // `height - 5`; consumers key the state diff by `header.stateRoot`, so it
    // must be the state root of this epoch.
    header.state_root = state_root;

    let mut eth_traces: HashMap<H256, DebankFrame> = HashMap::new();
    let mut phantom_slots = VecDeque::new();
    for raw in raw_traces {
        let Some(root) = raw.trace.root else { continue };
        match raw.space {
            Space::Ethereum => {
                eth_traces.entry(raw.tx_hash).or_insert(root);
            }
            Space::Native => collect_phantom_slots(&root, &mut phantom_slots),
        }
    }

    let mut builder = BlockFileBuilder::default();
    let mut prior_log_index = 0usize;
    for (idx, tx) in pb.transactions.iter().enumerate() {
        let receipt = &pb.receipts[idx];
        let tx_id = format_hash(&tx.hash());

        builder.txs.push(build_transaction(pb, idx, tx));

        // Phantom transactions are the only eSpace transactions without gas
        // price (see `PhantomTransaction::into_eip155`).
        let is_phantom = tx.gas_price().is_zero() && tx.gas_limit().is_zero();
        let root = if is_phantom {
            phantom_root(tx, phantom_slots.pop_front(), &tx_id)?
        } else {
            eth_traces.remove(&tx.hash())
        };
        if let Some(root) = root {
            let logs = builder.add_tx_trace(&tx_id, root, prior_log_index);
            if logs != receipt.logs.len() {
                bail!(
                    "debank trace of tx {:?} has {} logs, receipt has {}",
                    tx.hash(),
                    logs,
                    receipt.logs.len()
                );
            }
        }
        prior_log_index += receipt.logs.len();
    }
    if !phantom_slots.is_empty() {
        bail!(
            "{} cross-space calls without phantom transaction",
            phantom_slots.len()
        );
    }

    let block_file = BlockFile {
        block: DebankBlock {
            id: format_hash(&pivot_hash),
            height,
            parent_id: format_hash(&header.parent_hash),
            base_fee_per_gas: header
                .base_fee_per_gas
                .map_or(0, |fee| fee.low_u128()),
            miner: format_address(&header.miner),
            gas_limit: header.gas_limit.low_u128(),
            gas_used: header.gas_used.low_u128(),
            timestamp: pb.pivot_header.timestamp(),
            process_start_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        },
        txs: builder.txs,
        events: builder.events,
        traces: builder.traces,
        error_events: builder.error_events,
        error_traces: builder.error_traces,
        storage_contracts: builder.storage_contracts,
    };
    let validation_hash = block_file.validation_hash();

    let state_diff =
        build_block_storage_diff(state_root, parent_state_root, state_diff);

    Ok(DebankOutPut {
        block_file,
        header,
        state_diff: rlp::encode(&state_diff).to_vec().into(),
        validation_hash,
    })
}

fn build_transaction(
    pb: &PhantomBlock, idx: usize, tx: &SignedTransaction,
) -> DebankTransaction {
    let receipt = &pb.receipts[idx];
    let prior_gas_used = if idx == 0 {
        U256::zero()
    } else {
        pb.receipts[idx - 1].accumulated_gas_used
    };
    let gas_used = receipt.accumulated_gas_used.saturating_sub(prior_gas_used);

    let gas_price = match pb.pivot_header.base_price() {
        Some(base_price) => {
            let base_price = base_price[Space::Ethereum];
            if *tx.gas_price() < base_price {
                *tx.gas_price()
            } else {
                tx.effective_gas_price(&base_price)
            }
        }
        None => *tx.gas_price(),
    };

    // Like geth, only dynamic fee transactions report fee caps.
    let (max_fee, max_priority_fee) = match tx.type_id() {
        0 | 1 => (U256::zero(), U256::zero()),
        _ => (*tx.gas_price(), *tx.max_priority_gas_price()),
    };

    let to = match tx.action() {
        Action::Call(to) => to,
        Action::Create => {
            Transaction::deployed_contract_address(tx).unwrap_or_default()
        }
    };

    DebankTransaction {
        id: format_hash(&tx.hash()),
        from_addr: format_address(&tx.sender().address),
        to_addr: format_address(&to),
        gas_limit: tx.gas_limit().low_u128(),
        gas_price: gas_price.low_u128(),
        gas_used: gas_used.low_u128(),
        status: receipt.outcome_status == TransactionStatus::Success,
        max_fee_per_gas: max_fee.low_u128(),
        max_priority_fee_per_gas: max_priority_fee.low_u128(),
        input: tx.data().clone().into(),
        nonce: tx.nonce().low_u128(),
        idx: idx as i64,
        value: *tx.value(),
    }
}

/// A cross-space operation of a Core space transaction, in the order the
/// phantom transactions are derived from its receipt logs.
enum PhantomSlot {
    /// The value transfer preceding a `callEVM`/`createEVM`.
    Transfer,
    /// The eSpace frame of a `callEVM`/`createEVM`.
    Call(Option<DebankFrame>),
    /// `withdrawFromMapped`.
    Withdraw,
}

/// Walks a Core space call tree and records its effective cross-space
/// operations, mirroring `recover_phantom_traces`.
fn collect_phantom_slots(root: &DebankFrame, out: &mut VecDeque<PhantomSlot>) {
    fn walk(frame: &DebankFrame, out: &mut VecDeque<PhantomSlot>) {
        if frame.failed() {
            // Reverted operations emit no phantom transaction.
            return;
        }
        let is_cross_space_call = frame.space == Space::Native
            && frame.kind == DebankFrameKind::Call(CallType::Call)
            && frame.to == Some(CROSS_SPACE_CONTRACT_ADDRESS)
            && frame.input.len() >= 4;
        if is_cross_space_call && is_call_create_sig(&frame.input[0..4]) {
            out.push_back(PhantomSlot::Transfer);
            let espace_frame =
                frame.calls().find(|f| f.space == Space::Ethereum).cloned();
            out.push_back(PhantomSlot::Call(espace_frame));
            return;
        }
        if is_cross_space_call && is_withdraw_sig(&frame.input[0..4]) {
            out.push_back(PhantomSlot::Withdraw);
            return;
        }
        for child in frame.calls() {
            if child.space == Space::Native {
                walk(child, out);
            }
        }
    }
    walk(root, out)
}

/// Builds the root frame of a phantom transaction.
fn phantom_root(
    tx: &SignedTransaction, slot: Option<PhantomSlot>, tx_id: &str,
) -> CoreResult<Option<DebankFrame>> {
    let transfer = || DebankFrame {
        space: Space::Ethereum,
        kind: DebankFrameKind::Call(CallType::Call),
        from: tx.sender().address,
        to: Some(match tx.action() {
            Action::Call(to) => to,
            Action::Create => Address::zero(),
        }),
        value: *tx.value(),
        gas: U256::zero(),
        gas_used: U256::zero(),
        input: tx.data().to_vec(),
        output: vec![],
        error: None,
        self_storage_change: false,
        items: vec![],
    };
    let from_zero = tx.sender().address.is_zero();
    let to_zero = tx.action() == Action::Call(Address::zero());
    match slot {
        Some(PhantomSlot::Transfer) if from_zero => Ok(Some(transfer())),
        Some(PhantomSlot::Withdraw) if to_zero => Ok(Some(transfer())),
        Some(PhantomSlot::Call(frame)) if !from_zero => {
            Ok(frame.map(|mut frame| {
                // Phantom transactions consume no gas.
                zero_gas(&mut frame);
                frame
            }))
        }
        Some(_) => bail!("cross-space call mismatches phantom tx {}", tx_id),
        None => bail!("no cross-space call found for phantom tx {}", tx_id),
    }
}

fn zero_gas(frame: &mut DebankFrame) {
    frame.gas = U256::zero();
    frame.gas_used = U256::zero();
    for item in frame.items.iter_mut() {
        if let DebankFrameItem::Call(child) = item {
            zero_gas(child);
        }
    }
}

#[derive(Default)]
struct BlockFileBuilder {
    txs: Vec<DebankTransaction>,
    events: Vec<DebankEvent>,
    traces: Vec<DebankTrace>,
    error_events: Vec<DebankEvent>,
    error_traces: Vec<DebankTrace>,
    storage_contracts: Vec<String>,
    seen_storage_contracts: HashSet<Address>,
}

/// A frame with the flags derived after the transaction finished.
struct Node<'a> {
    frame: &'a DebankFrame,
    parent_failed: bool,
    storage_change: bool,
    /// Block level log index of each log item; `None` for failed frames.
    log_indexes: Vec<Option<usize>>,
    children: Vec<Node<'a>>,
}

impl<'a> Node<'a> {
    fn failed(&self) -> bool { self.frame.failed() }

    fn new(
        frame: &'a DebankFrame, parent_failed: bool, next_log_index: &mut usize,
    ) -> Self {
        let failed = frame.failed() || parent_failed;
        let mut log_indexes = vec![];
        let mut children = vec![];
        // Logs of successful frames are numbered in emission order, which is
        // the pre-order of the items.
        for item in frame.items.iter() {
            match item {
                DebankFrameItem::Call(child) => {
                    children.push(Node::new(child, failed, next_log_index))
                }
                DebankFrameItem::Log(_) if failed => log_indexes.push(None),
                DebankFrameItem::Log(_) => {
                    log_indexes.push(Some(*next_log_index));
                    *next_log_index += 1;
                }
            }
        }
        let storage_change = frame.self_storage_change
            || children.iter().any(|c| c.storage_change && !c.failed());
        Node {
            frame,
            parent_failed,
            storage_change,
            log_indexes,
            children,
        }
    }
}

impl BlockFileBuilder {
    /// Adds the traces and events of a transaction. Returns the number of
    /// logs kept in the receipt.
    fn add_tx_trace(
        &mut self, tx_id: &str, root: DebankFrame, first_log_index: usize,
    ) -> usize {
        let mut next_log_index = first_log_index;
        let root = Node::new(&root, false, &mut next_log_index);

        self.collect_storage_contracts(&root);

        let root_id = debank_id(&[tx_id, "", "0"]);
        let root_trace = to_trace(&root, tx_id, &root_id, "", 0, vec![]);
        self.push_trace(root_trace, root.failed());
        self.add_trace_and_log(tx_id, &root, &root_id, &[]);

        next_log_index - first_log_index
    }

    fn collect_storage_contracts(&mut self, node: &Node) {
        let frame = node.frame;
        if frame.self_storage_change {
            let contract = match frame.kind {
                DebankFrameKind::Call(CallType::DelegateCall) => {
                    Some(frame.from)
                }
                _ => frame.to,
            };
            if let Some(contract) = contract {
                if self.seen_storage_contracts.insert(contract) {
                    self.storage_contracts.push(format_address(&contract));
                }
            }
        }
        for child in node.children.iter() {
            self.collect_storage_contracts(child);
        }
    }

    /// Go `callTracer.addTraceAndLog`.
    fn add_trace_and_log(
        &mut self, tx_id: &str, node: &Node, trace_id: &str,
        trace_address: &[i64],
    ) {
        let positions = item_positions(node.frame);

        let child_ids: Vec<String> = positions
            .calls
            .iter()
            .map(|pos| debank_id(&[tx_id, trace_id, &pos.to_string()]))
            .collect();

        for (i, child) in node.children.iter().enumerate() {
            let address = child_address(trace_address, i);
            self.add_trace_and_log(tx_id, child, &child_ids[i], &address);
        }

        let failed = node.failed() || node.parent_failed;
        for (i, log) in positions.logs.iter().enumerate() {
            let (pos, log) = (log.0, log.1);
            let event = to_event(
                log,
                trace_id,
                pos,
                node.log_indexes[i].unwrap_or(0) as i64,
            );
            if failed {
                self.error_events.push(event);
            } else {
                self.events.push(event);
            }
        }

        for (i, child) in node.children.iter().enumerate() {
            let trace = to_trace(
                child,
                tx_id,
                &child_ids[i],
                trace_id,
                positions.calls[i],
                child_address(trace_address, i),
            );
            self.push_trace(trace, child.failed() || child.parent_failed);
        }
    }

    fn push_trace(&mut self, trace: DebankTrace, failed: bool) {
        if failed {
            self.error_traces.push(trace);
        } else {
            self.traces.push(trace);
        }
    }
}

struct ItemPositions<'a> {
    /// Position of each child call among the frame items.
    calls: Vec<i64>,
    /// Position of each log among the frame items.
    logs: Vec<(i64, &'a DebankLog)>,
}

/// Calls and logs of a frame share one position counter.
fn item_positions(frame: &DebankFrame) -> ItemPositions<'_> {
    let mut positions = ItemPositions {
        calls: vec![],
        logs: vec![],
    };
    for (pos, item) in frame.items.iter().enumerate() {
        match item {
            DebankFrameItem::Call(_) => positions.calls.push(pos as i64),
            DebankFrameItem::Log(log) => positions.logs.push((pos as i64, log)),
        }
    }
    positions
}

fn child_address(parent: &[i64], index: usize) -> Vec<i64> {
    let mut address = parent.to_vec();
    address.push(index as i64);
    address
}

/// Go `callTracer.ToTrace`.
fn to_trace(
    node: &Node, tx_id: &str, id: &str, parent_trace_id: &str, pos: i64,
    trace_address: Vec<i64>,
) -> DebankTrace {
    let frame = node.frame;
    let (call_create_type, call_type) = match frame.kind {
        DebankFrameKind::Call(call_type) => (
            "call",
            match call_type {
                CallType::Call | CallType::None => "call",
                CallType::CallCode => "callcode",
                CallType::DelegateCall => "delegatecall",
                CallType::StaticCall => "staticcall",
            },
        ),
        DebankFrameKind::Create(CreateType::CREATE)
        | DebankFrameKind::Create(CreateType::CREATE2)
        | DebankFrameKind::Create(CreateType::None) => ("create", ""),
        DebankFrameKind::SelfDestruct => ("suicide", ""),
    };
    let error = match &frame.error {
        Some(error) => error.clone(),
        None if node.parent_failed => PARENT_CALL_FAILED.to_string(),
        None => String::new(),
    };
    DebankTrace {
        id: id.to_string(),
        from_addr: format_address(&frame.from),
        gas_limit: frame.gas.low_u128(),
        input: frame.input.clone().into(),
        to_addr: format_address(&frame.to.unwrap_or_default()),
        value: frame.value,
        gas_used: frame.gas_used.low_u128(),
        output: frame.output.clone().into(),
        call_create_type: call_create_type.to_string(),
        call_type: call_type.to_string(),
        tx_id: tx_id.to_string(),
        parent_trace_id: parent_trace_id.to_string(),
        pos_in_parent_trace: pos,
        self_storage_change: frame.self_storage_change,
        storage_change: node.storage_change,
        subtraces: node.children.len() as i64,
        trace_address,
        error,
    }
}

fn to_event(
    log: &DebankLog, parent_trace_id: &str, pos: i64, idx: i64,
) -> DebankEvent {
    let mut topics = log.topics.iter().map(format_hash);
    let selector = topics.next().unwrap_or_default();
    let topics = (!log.topics.is_empty()).then(|| topics.collect());
    DebankEvent {
        id: debank_id(&[parent_trace_id, &pos.to_string()]),
        contract_id: format_address(&log.address),
        selector,
        topics,
        data: log.data.clone().into(),
        parent_trace_id: parent_trace_id.to_string(),
        pos_in_parent_trace: pos,
        idx,
    }
}

fn build_block_storage_diff(
    hash: H256, parent_hash: H256, diff: DebankStateDiff,
) -> BlockStorageDiff {
    let mut out = BlockStorageDiff {
        hash,
        parent_hash,
        ..Default::default()
    };
    let mut seen_codes = HashSet::new();
    for account in diff.accounts {
        let address = keccak(account.address);
        out.new_accounts.push(NewAccount {
            address,
            balance: account.balance,
            nonce: account.nonce.low_u64(),
            code_hash: account.code_hash,
        });
        if !account.storage_changes.is_empty() {
            out.storage_diffs.push(AccountStorageDiff {
                address,
                diffs: account
                    .storage_changes
                    .into_iter()
                    .map(|(key, value)| IndexValuePair {
                        index: keccak(key),
                        value,
                    })
                    .collect(),
            });
        }
        if let Some(code) = account.code {
            if seen_codes.insert(account.code_hash) {
                out.new_codes.push(NewCode {
                    code_hash: account.code_hash,
                    code,
                });
            }
        }
    }
    out.deleted_accounts =
        diff.deleted_accounts.iter().map(|a| keccak(a)).collect();
    out
}
