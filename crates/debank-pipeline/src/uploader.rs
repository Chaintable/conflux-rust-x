//! Uploads spooled blocks to S3 and announces them on Kafka in production
//! order, like the Go pipeline `PushProcessor` and the `etl` of
//! background-tracer. Without Kafka the blocks are only uploaded.

use crate::{
    chain::{
        merge, AnnouncedChain, BlockChangeNotification, BlockContext, Plan,
    },
    spool::{Spool, SpoolEntry, SpoolRecord},
};
use aws_sdk_s3::{primitives::ByteStream, Client as S3Client};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use log::{error, info, warn};
use parking_lot::Mutex;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::{
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

/// Blocks uploaded concurrently before they are announced.
const BATCH: usize = 64;
const RETRY_DELAY: Duration = Duration::from_secs(1);
const IDLE_DELAY: Duration = Duration::from_millis(200);

#[derive(Debug, Clone)]
pub struct UploadConfig {
    pub region: String,
    pub nodex_bucket: String,
    pub chain_table_bucket: String,
    pub kafka: Option<KafkaConfig>,
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
}

pub struct Uploader {
    spool: Arc<Spool>,
    chain: Arc<Mutex<AnnouncedChain>>,
    s3: S3Client,
    producer: Option<FutureProducer>,
    config: UploadConfig,
    /// `<chain_id>` or `<chain_id>/<version>`.
    prefix: String,
}

impl Uploader {
    pub async fn new(
        spool: Arc<Spool>, chain: Arc<Mutex<AnnouncedChain>>,
        config: UploadConfig, prefix: String,
    ) -> Result<Self, String> {
        let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .load()
            .await;
        let producer = match &config.kafka {
            Some(kafka) => Some(
                ClientConfig::new()
                    .set("bootstrap.servers", &kafka.brokers)
                    .set("message.timeout.ms", "10000")
                    .set("acks", "all")
                    .set("enable.idempotence", "true")
                    .create::<FutureProducer>()
                    .map_err(|e| format!("create kafka producer: {}", e))?,
            ),
            None => None,
        };
        Ok(Self {
            spool,
            chain,
            s3: S3Client::new(&aws),
            producer,
            config,
            prefix,
        })
    }

    /// Rebuilds the announced chain from the last Kafka messages.
    pub fn resume_from_kafka(&self, max_messages: i64) -> Result<(), String> {
        let Some(kafka) = &self.config.kafka else {
            return Ok(());
        };
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &kafka.brokers)
            .set("group.id", format!("debank-pipeline-{}", self.prefix))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .create()
            .map_err(|e| format!("create kafka consumer: {}", e))?;
        let partition = 0;
        let (low, high) = consumer
            .fetch_watermarks(&kafka.topic, partition, Duration::from_secs(10))
            .map_err(|e| format!("fetch kafka watermarks: {}", e))?;
        info!(
            "debank pipeline: kafka topic {} offsets {}..{}",
            kafka.topic, low, high
        );
        if high <= low {
            return Ok(());
        }
        let start = (high - max_messages).max(low);
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &kafka.topic,
            partition,
            Offset::Offset(start),
        )
        .map_err(|e| e.to_string())?;
        consumer.assign(&tpl).map_err(|e| e.to_string())?;

        let mut chain = self.chain.lock();
        let mut read = start;
        while read < high {
            match consumer.poll(Duration::from_secs(10)) {
                Some(Ok(msg)) => {
                    read = msg.offset() + 1;
                    let payload = msg.payload().unwrap_or_default();
                    let notification: BlockChangeNotification =
                        serde_json::from_slice(&gunzip(payload)?).map_err(
                            |e| format!("decode notification: {}", e),
                        )?;
                    chain.apply(&notification);
                }
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
                Some(Err(e)) => return Err(format!("read kafka: {}", e)),
                None => return Err("read kafka: timeout".into()),
            }
        }
        info!(
            "debank pipeline: resumed from kafka, head {:?}",
            chain.head()
        );
        Ok(())
    }

    pub async fn run(self) {
        loop {
            match self.run_batch().await {
                Ok(0) => tokio::time::sleep(IDLE_DELAY).await,
                Ok(_) => {}
                Err(e) => {
                    error!("debank pipeline: {}", e);
                    tokio::time::sleep(RETRY_DELAY * 10).await;
                }
            }
        }
    }

    /// Uploads and announces the next pending blocks, returns how many were
    /// handled.
    async fn run_batch(&self) -> Result<usize, String> {
        let entries: Vec<SpoolEntry> =
            self.spool.list()?.into_iter().take(BATCH).collect();
        if entries.is_empty() {
            return Ok(0);
        }
        let Some(producer) = &self.producer else {
            return self.upload_only(entries).await;
        };

        // Plan on a copy in production order; the shared chain only moves
        // once Kafka acknowledged the announcement.
        let mut draft = self.chain.lock().clone();
        let mut planned = vec![];
        let mut gap = None;
        for entry in entries {
            let record = self.spool.read(&entry)?;
            let block = BlockContext {
                hash: record.hash,
                parent_hash: record.parent_hash,
                block_number: record.number,
                timestamp: record.timestamp,
            };
            match draft.plan(&block) {
                Plan::Skip => planned.push((entry, record, None)),
                Plan::Announce(n) => {
                    draft.apply(&n);
                    planned.push((entry, record, Some(n)));
                }
                Plan::Gap(reason) => {
                    gap = Some(reason);
                    break;
                }
            }
        }
        if planned.is_empty() {
            if let Some(reason) = gap {
                return Err(format!("cannot announce block: {}", reason));
            }
        }

        futures::future::join_all(
            planned
                .iter()
                .filter(|(_, _, n)| n.is_some())
                .map(|(_, record, _)| self.upload(record)),
        )
        .await;

        let mut notifications: Vec<BlockChangeNotification> = vec![];
        for (_, _, n) in &planned {
            if let Some(n) = n {
                merge(&mut notifications, n.clone());
            }
        }
        for notification in &notifications {
            self.announce(producer, notification).await;
            self.chain.lock().apply(notification);
        }
        for (entry, _, _) in &planned {
            self.spool.remove(entry)?;
        }
        if let Some(b) = notifications.last().and_then(|n| n.new_blocks.last())
        {
            info!(
                "debank pipeline: announced up to block {} {:?}",
                b.block_number, b.hash
            );
        }
        Ok(planned.len())
    }

    /// Uploads the blocks, forks included, without announcing them.
    async fn upload_only(
        &self, entries: Vec<SpoolEntry>,
    ) -> Result<usize, String> {
        let records = entries
            .iter()
            .map(|e| self.spool.read(e))
            .collect::<Result<Vec<_>, _>>()?;
        futures::future::join_all(records.iter().map(|r| self.upload(r))).await;
        for entry in &entries {
            self.spool.remove(entry)?;
        }
        let last = records.last().expect("not empty");
        info!(
            "debank pipeline: uploaded {} blocks up to {} {:?}",
            records.len(),
            last.number,
            last.hash
        );
        Ok(entries.len())
    }

    async fn upload(&self, record: &SpoolRecord) {
        let id = format!("{:?}", record.hash);
        let mut puts = vec![
            self.put(
                &self.config.chain_table_bucket,
                format!("{}/{}", self.prefix, id),
                &record.block_file,
                true,
            ),
            // The consistency checker rewrites `is_fork` in place, so an
            // existing validation object is never overwritten.
            self.put(
                &self.config.chain_table_bucket,
                format!("{}/{}/{}", self.prefix, record.number, id),
                &record.validation,
                false,
            ),
            self.put(
                &self.config.nodex_bucket,
                format!("{}/{}/block", self.prefix, id),
                &record.header,
                true,
            ),
        ];
        if record.state_root != record.parent_state_root {
            puts.push(self.put(
                &self.config.nodex_bucket,
                format!("{}/{:?}/stateDiff", self.prefix, record.state_root),
                &record.state_diff,
                true,
            ));
        }
        futures::future::join_all(puts).await;
    }

    /// Retries until the object is stored.
    async fn put(
        &self, bucket: &str, key: String, data: &[u8], overwrite: bool,
    ) {
        loop {
            let mut req = self
                .s3
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from(data.to_vec()));
            if !overwrite {
                req = req.if_none_match("*");
            }
            match req.send().await {
                Ok(_) => return,
                Err(e)
                    if e.raw_response()
                        .is_some_and(|r| r.status().as_u16() == 412) =>
                {
                    return
                }
                Err(e) => {
                    warn!(
                        "debank pipeline: put s3://{}/{} failed, retrying: {:?}",
                        bucket, key, e
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    /// Retries until Kafka acknowledges the notification.
    async fn announce(
        &self, producer: &FutureProducer,
        notification: &BlockChangeNotification,
    ) {
        let payload =
            gzip(&serde_json::to_vec(notification).expect("serializable"));
        loop {
            let topic = &self.config.kafka.as_ref().expect("announcing").topic;
            let record =
                FutureRecord::to(topic).key("NewBlock").payload(&payload);
            match producer.send(record, Duration::from_secs(10)).await {
                Ok(_) => return,
                Err((e, _)) => {
                    warn!(
                        "debank pipeline: kafka send failed, retrying: {}",
                        e
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }
}

pub fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("write to vec");
    encoder.finish().expect("write to vec")
}

fn gunzip(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = vec![];
    GzDecoder::new(data)
        .read_to_end(&mut out)
        .map_err(|e| format!("gunzip: {}", e))?;
    Ok(out)
}
