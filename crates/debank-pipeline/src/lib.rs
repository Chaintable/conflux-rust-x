//! Chaintable pipeline writer for Conflux eSpace.
//!
//! The executor hands every executed pivot epoch to [`DebankPipeline`], which
//! builds the `trace_debankBlock` output and durably spools it. A background
//! uploader puts the artifacts to S3 and announces the blocks on Kafka, as the
//! Go pipeline live tracer does for geth based chains.

pub mod chain;
pub mod spool;
pub mod uploader;

use std::{path::PathBuf, sync::Arc, thread};

use cfx_rpc_eth_impl::debank::build_debank_output;
use cfx_types::H256;
use cfxcore::consensus::consensus_inner::consensus_executor::debank_live::{
    DebankLiveEpoch, DebankLiveSink,
};
use log::{info, warn};
use parking_lot::Mutex;

use crate::{
    chain::AnnouncedChain,
    spool::{Spool, SpoolRecord},
    uploader::{gzip, UploadConfig, Uploader},
};

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The first epoch to produce.
    pub start_epoch: u64,
    /// The first block announced on Kafka.
    pub genesis_epoch: u64,
    pub spool_dir: PathBuf,
    /// eSpace chain id, the first segment of the S3 keys.
    pub chain_id: u64,
    pub version: String,
    pub max_fork_depth: u64,
    /// Without it the produced blocks stay in the spool directory.
    pub upload: Option<UploadConfig>,
}

pub struct DebankPipeline {
    config: PipelineConfig,
    spool: Arc<Spool>,
    chain: Arc<Mutex<AnnouncedChain>>,
}

impl DebankPipeline {
    /// Opens the spool and starts the uploader, after the announced chain is
    /// restored from Kafka.
    pub fn start(config: PipelineConfig) -> Result<Arc<Self>, String> {
        let spool = Arc::new(Spool::open(&config.spool_dir)?);
        let chain = Arc::new(Mutex::new(AnnouncedChain::new(
            config.genesis_epoch,
            config.max_fork_depth,
        )));
        let prefix = if config.version.is_empty() {
            config.chain_id.to_string()
        } else {
            format!("{}/{}", config.chain_id, config.version)
        };

        match config.upload.clone() {
            Some(upload) => {
                if upload.kafka.is_none() {
                    warn!(
                        "debank pipeline: no Kafka configured, produced \
                         blocks are uploaded to S3 but not announced"
                    );
                }
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                let spool = spool.clone();
                let chain = chain.clone();
                let max_messages = config.max_fork_depth as i64;
                thread::Builder::new()
                    .name("debank-pipeline".into())
                    .spawn(move || {
                        let runtime =
                            tokio::runtime::Builder::new_multi_thread()
                                .worker_threads(4)
                                .enable_all()
                                .build()
                                .expect("tokio runtime");
                        runtime.block_on(async move {
                            let uploader = match Uploader::new(
                                spool, chain, upload, prefix,
                            )
                            .await
                            .and_then(|u| {
                                u.resume_from_kafka(max_messages).map(|_| u)
                            }) {
                                Ok(u) => u,
                                Err(e) => {
                                    let _ = ready_tx.send(Err(e));
                                    return;
                                }
                            };
                            let _ = ready_tx.send(Ok(()));
                            uploader.run().await;
                        })
                    })
                    .map_err(|e| format!("spawn uploader: {}", e))?;
                ready_rx
                    .recv()
                    .map_err(|_| "uploader exited".to_string())??;
            }
            None => warn!(
                "debank pipeline: no S3/Kafka configured, produced blocks \
                 stay in {:?}",
                config.spool_dir
            ),
        }
        info!("debank pipeline started: {:?}", config);
        Ok(Arc::new(Self {
            config,
            spool,
            chain,
        }))
    }
}

impl DebankLiveSink for DebankPipeline {
    fn start_epoch(&self) -> u64 { self.config.start_epoch }

    fn needs_epoch(&self, height: u64, hash: &H256) -> bool {
        if self.chain.lock().is_announced(height, hash) {
            return false;
        }
        // On error the epoch is produced again, the uploader skips
        // duplicates.
        !self.spool.contains(height, hash).unwrap_or(false)
    }

    fn on_epoch_executed(&self, epoch: DebankLiveEpoch) -> Result<(), String> {
        let state_root = epoch.trace.state_root;
        let parent_state_root = epoch.trace.parent_state_root;
        let output = build_debank_output(&epoch.phantom_block, epoch.trace)
            .map_err(|e| e.to_string())?;
        let record = SpoolRecord {
            hash: output.header.hash,
            parent_hash: output.header.parent_hash,
            number: output.block_file.block.height,
            timestamp: output.block_file.block.timestamp,
            state_root,
            parent_state_root,
            header: gzip_json(&output.header)?,
            block_file: gzip_json(&output.block_file)?,
            validation: gzip_json(&output.block_file.validation())?,
            state_diff: output.state_diff.0,
        };
        self.spool.push(&record)?;
        Ok(())
    }
}

/// Gzipped JSON of a value, as uploaded by the Go pipeline.
fn gzip_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let data = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    Ok(gzip(&data))
}
