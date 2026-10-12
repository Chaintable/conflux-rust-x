//! Produces `trace_debankBlock` data while epochs are executed, the Conflux
//! counterpart of the Go pipeline live tracer
//! (github.com/Chaintable/pipeline, `tracer/pipeline_tracer.go`).
//!
//! The executor records the call trees and the eSpace state diff of every
//! pivot epoch it executes and hands them to a [`DebankLiveSink`] before the
//! epoch is marked as executed. A node that crashes in between re-executes the
//! epoch after restart, so no epoch is lost.

use super::epoch_execution::DebankTxRawTrace;
use cfx_executor::state::debank_diff::DebankStateDiff;
use cfx_rpc_cfx_types::PhantomBlock;
use cfx_types::H256;

/// Call trees and state diff of an executed epoch.
pub struct DebankEpochTrace {
    /// Call trees of the executed transactions, in execution order.
    pub traces: Vec<DebankTxRawTrace>,
    /// eSpace accounts changed by the epoch.
    pub state_diff: DebankStateDiff,
    /// State root hash after the epoch.
    pub state_root: H256,
    /// State root hash after the parent epoch.
    pub parent_state_root: H256,
}

/// An executed pivot epoch with everything needed to build its pipeline
/// output.
pub struct DebankLiveEpoch {
    /// The eSpace block of the epoch, without parity traces.
    pub phantom_block: PhantomBlock,
    pub trace: DebankEpochTrace,
}

/// Receives the epochs executed on the local pivot chain.
///
/// It is called on the execution thread in execution order. A pivot chain
/// reorg re-executes the epochs of the new branch, so an epoch height may be
/// received again with a different hash.
pub trait DebankLiveSink: Send + Sync {
    /// The first epoch to produce. Earlier epochs are executed without
    /// tracing.
    fn start_epoch(&self) -> u64;

    /// Whether an epoch that was executed before still has to be produced.
    /// The executor skips such epochs (e.g. after a restart or when the pivot
    /// chain switches back to a known branch) and replays them only if this
    /// returns true.
    fn needs_epoch(&self, height: u64, hash: &H256) -> bool;

    /// Persists the epoch. The epoch is marked as executed only after this
    /// returns, so a failure must not be ignored.
    fn on_epoch_executed(&self, epoch: DebankLiveEpoch) -> Result<(), String>;
}
