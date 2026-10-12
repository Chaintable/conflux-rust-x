//! Call-frame recorder for `trace_debankBlock`.
//!
//! It mirrors the frame model of the Go pipeline `callTracer`
//! (github.com/Chaintable/pipeline, `tracer/call_tracer.go`): calls, logs and
//! selfdestructs of a frame share one position counter, an `SSTORE` executed
//! inside a frame marks it as `self_storage_change`, and failed frames keep
//! their logs so they can be reported as error events.

use cfx_executor::{
    executive_observer::{
        CallTracer, CheckpointTracer, DrainTrace, InternalTransferTracer,
        OpcodeTracer, SetAuthTracer, StorageTracer,
    },
    stack::{FrameResult, FrameReturn},
};
use cfx_types::{Address, Space, H256, U256};
use cfx_vm_types::{ActionParams, CallType, CreateType, InterpreterInfo};
use typemap::ShareDebugMap;

const SSTORE: u8 = 0x55;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebankFrameKind {
    Call(CallType),
    Create(CreateType),
    SelfDestruct,
}

#[derive(Debug, Clone)]
pub struct DebankLog {
    pub address: Address,
    pub topics: Vec<H256>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum DebankFrameItem {
    Call(DebankFrame),
    Log(DebankLog),
}

#[derive(Debug, Clone)]
pub struct DebankFrame {
    pub space: Space,
    pub kind: DebankFrameKind,
    pub from: Address,
    /// `None` for a failed contract creation.
    pub to: Option<Address>,
    pub value: U256,
    pub gas: U256,
    pub gas_used: U256,
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    /// Geth style error message, `None` if the frame succeeded.
    pub error: Option<String>,
    pub self_storage_change: bool,
    /// Child calls and logs in execution order.
    pub items: Vec<DebankFrameItem>,
}

impl DebankFrame {
    pub fn failed(&self) -> bool { self.error.is_some() }

    pub fn calls(&self) -> impl Iterator<Item = &DebankFrame> {
        self.items.iter().filter_map(|item| match item {
            DebankFrameItem::Call(frame) => Some(frame),
            DebankFrameItem::Log(_) => None,
        })
    }
}

/// The recorded call tree of one transaction.
#[derive(Debug, Clone, Default)]
pub struct DebankTxTrace {
    pub root: Option<DebankFrame>,
}

pub struct DebankTraceKey;

impl typemap::Key for DebankTraceKey {
    type Value = DebankTxTrace;
}

#[derive(Default)]
pub struct DebankTracer {
    stack: Vec<DebankFrame>,
    root: Option<DebankFrame>,
}

impl DebankTracer {
    fn enter(&mut self, frame: DebankFrame) { self.stack.push(frame); }

    fn exit(&mut self, result: &FrameResult, create_address: bool) {
        let Some(mut frame) = self.stack.pop() else {
            warn!("debank tracer: frame result without frame");
            return;
        };

        match result {
            Ok(FrameReturn {
                gas_left,
                apply_state,
                return_data,
                create_address: created,
                ..
            }) => {
                frame.gas_used = frame.gas.saturating_sub(*gas_left);
                let output = return_data.to_vec();
                if *apply_state {
                    if create_address {
                        frame.to = created.or(frame.to);
                    }
                    frame.output = output;
                } else {
                    frame.error = Some(revert_error_message(&output));
                    frame.output = output;
                    if create_address {
                        frame.to = None;
                    }
                }
            }
            Err(err) => {
                frame.gas_used = frame.gas;
                frame.error = Some(vm_error_message(err));
                if create_address {
                    frame.to = None;
                }
            }
        }

        self.nest(frame);
    }

    fn nest(&mut self, frame: DebankFrame) {
        match self.stack.last_mut() {
            Some(parent) => parent.items.push(DebankFrameItem::Call(frame)),
            None => self.root = Some(frame),
        }
    }
}

impl CallTracer for DebankTracer {
    fn record_call(&mut self, params: &ActionParams) {
        let (from, to) = match params.call_type {
            CallType::DelegateCall | CallType::CallCode => {
                (params.address, params.code_address)
            }
            _ => (params.sender, params.address),
        };
        self.enter(DebankFrame {
            space: params.space,
            kind: DebankFrameKind::Call(params.call_type),
            from,
            to: Some(to),
            value: params.value.value(),
            gas: params.gas,
            gas_used: U256::zero(),
            input: params.data.clone().unwrap_or_default(),
            output: vec![],
            error: None,
            self_storage_change: false,
            items: vec![],
        });
    }

    fn record_call_result(&mut self, result: &FrameResult) {
        self.exit(result, false);
    }

    fn record_create(&mut self, params: &ActionParams) {
        self.enter(DebankFrame {
            space: params.space,
            kind: DebankFrameKind::Create(params.create_type),
            from: params.sender,
            to: Some(params.address),
            value: params.value.value(),
            gas: params.gas,
            gas_used: U256::zero(),
            input: params.data.clone().unwrap_or_default(),
            output: vec![],
            error: None,
            self_storage_change: false,
            items: vec![],
        });
    }

    fn record_create_result(&mut self, result: &FrameResult) {
        self.exit(result, true);
    }
}

impl OpcodeTracer for DebankTracer {
    fn do_trace_opcode(&self, enabled: &mut bool) { *enabled = true; }

    fn step(&mut self, interp: &dyn InterpreterInfo) {
        if interp.current_opcode() == SSTORE {
            if let Some(frame) = self.stack.last_mut() {
                frame.self_storage_change = true;
            }
        }
    }

    fn log(&mut self, address: &Address, topics: &Vec<H256>, data: &[u8]) {
        if let Some(frame) = self.stack.last_mut() {
            frame.items.push(DebankFrameItem::Log(DebankLog {
                address: *address,
                topics: topics.clone(),
                data: data.to_vec(),
            }));
        }
    }

    fn selfdestruct(
        &mut self, space: Space, contract: &Address, target: &Address,
        value: U256,
    ) {
        // Geth reports a selfdestruct as an empty child frame of the
        // destructed contract.
        let frame = DebankFrame {
            space,
            kind: DebankFrameKind::SelfDestruct,
            from: *contract,
            to: Some(*target),
            value,
            gas: U256::zero(),
            gas_used: U256::zero(),
            input: vec![],
            output: vec![],
            error: None,
            self_storage_change: false,
            items: vec![],
        };
        self.nest(frame);
    }
}

impl CheckpointTracer for DebankTracer {}

impl InternalTransferTracer for DebankTracer {}

impl StorageTracer for DebankTracer {}

impl SetAuthTracer for DebankTracer {}

impl DrainTrace for DebankTracer {
    fn drain_trace(self, map: &mut ShareDebugMap) {
        map.insert::<DebankTraceKey>(DebankTxTrace { root: self.root });
    }
}

/// Error message of a reverted frame, matching geth's
/// `execution reverted[: <reason>]`.
fn revert_error_message(output: &[u8]) -> String {
    match decode_revert_reason(output) {
        Some(reason) => format!("execution reverted: {}", reason),
        None => "execution reverted".to_string(),
    }
}

/// Decodes a solidity `Error(string)` revert payload.
fn decode_revert_reason(output: &[u8]) -> Option<String> {
    const ERROR_SELECTOR: [u8; 4] = [0x08, 0xc3, 0x79, 0xa0];
    if output.len() < 4 + 64 || output[..4] != ERROR_SELECTOR {
        return None;
    }
    let data = &output[4..];
    let offset = U256::from_big_endian(&data[..32]);
    if offset > U256::from(data.len()) {
        return None;
    }
    let offset = offset.as_usize();
    let len_end = offset.checked_add(32)?;
    if len_end > data.len() {
        return None;
    }
    let len = U256::from_big_endian(&data[offset..len_end]);
    if len > U256::from(data.len()) {
        return None;
    }
    let end = len_end.checked_add(len.as_usize())?;
    if end > data.len() {
        return None;
    }
    String::from_utf8(data[len_end..end].to_vec()).ok()
}

/// Maps a Conflux VM error to the closest geth error message.
fn vm_error_message(err: &cfx_vm_types::Error) -> String {
    use cfx_vm_types::Error::*;
    match err {
        OutOfGas => "out of gas".to_string(),
        BadJumpDestination { .. } => "invalid jump destination".to_string(),
        BadInstruction { instruction } => {
            format!("invalid opcode: opcode {:#x} not defined", instruction)
        }
        StackUnderflow { .. } => "stack underflow".to_string(),
        OutOfStack { .. } => "stack limit reached".to_string(),
        MutableCallInStaticContext => "write protection".to_string(),
        OutOfBounds => "return data out of bounds".to_string(),
        Reverted => "execution reverted".to_string(),
        ConflictAddress(_) => "contract address collision".to_string(),
        CreateContractStartingWithEF => {
            "invalid code: must not begin with 0xef".to_string()
        }
        CreateInitCodeSizeLimit => "max initcode size exceeded".to_string(),
        NonceOverflow(_) => "nonce uint64 overflow".to_string(),
        other => other.to_string(),
    }
}
