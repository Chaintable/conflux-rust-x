//! Tracks the blocks announced on Kafka and turns produced blocks into
//! `BlockChangeNotification`s, matching the Go pipeline
//! (`types/block_notification.go`, `processor/push.go`).

use cfx_types::H256;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockContext {
    pub hash: H256,
    pub parent_hash: H256,
    pub block_number: u64,
    #[serde(default)]
    pub timestamp: u64,
}

/// `changeType` 1 announces new blocks, 2 a reorg that drops `dropBlocks`
/// first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockChangeNotification {
    pub change_type: u64,
    #[serde(default, deserialize_with = "null_as_default")]
    pub new_blocks: Vec<BlockContext>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub drop_blocks: Vec<BlockContext>,
}

pub const CHANGE_NEW: u64 = 1;
pub const CHANGE_FORK: u64 = 2;

/// Go encodes empty slices as `null`.
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, PartialEq, Eq)]
pub enum Plan {
    /// Already announced, or before the genesis block.
    Skip,
    Announce(BlockChangeNotification),
    /// The parent block was never announced, the block cannot be placed.
    Gap(String),
}

/// The recently announced canonical chain.
#[derive(Clone)]
pub struct AnnouncedChain {
    genesis: u64,
    max_fork_depth: u64,
    blocks: BTreeMap<u64, BlockContext>,
}

impl AnnouncedChain {
    pub fn new(genesis: u64, max_fork_depth: u64) -> Self {
        Self {
            genesis,
            max_fork_depth,
            blocks: BTreeMap::new(),
        }
    }

    pub fn head(&self) -> Option<&BlockContext> {
        self.blocks.last_key_value().map(|(_, b)| b)
    }

    pub fn contains(&self, number: u64, hash: &H256) -> bool {
        self.blocks.get(&number).is_some_and(|b| b.hash == *hash)
    }

    /// Whether the block is known to be announced already: it is on the
    /// tracked chain, or older than the tracked window.
    pub fn is_announced(&self, number: u64, hash: &H256) -> bool {
        match self.blocks.first_key_value() {
            None => false,
            Some((oldest, _)) if number < *oldest => true,
            Some(_) => self.contains(number, hash),
        }
    }

    pub fn plan(&self, block: &BlockContext) -> Plan {
        let Some(head) = self.head() else {
            return if block.block_number == self.genesis {
                Plan::Announce(new_blocks(vec![block.clone()]))
            } else if block.block_number < self.genesis {
                Plan::Skip
            } else {
                Plan::Gap(format!(
                    "block {} before genesis block {} is announced",
                    block.block_number, self.genesis
                ))
            };
        };

        if self.is_announced(block.block_number, &block.hash) {
            return Plan::Skip;
        }
        if block.parent_hash == head.hash {
            return Plan::Announce(new_blocks(vec![block.clone()]));
        }
        let parent_number = match block.block_number.checked_sub(1) {
            Some(n) => n,
            None => return Plan::Gap("fork at block 0".into()),
        };
        if self.contains(parent_number, &block.parent_hash) {
            let drop_blocks = self
                .blocks
                .range(block.block_number..)
                .map(|(_, b)| b.clone())
                .collect();
            return Plan::Announce(BlockChangeNotification {
                change_type: CHANGE_FORK,
                new_blocks: vec![block.clone()],
                drop_blocks,
            });
        }
        Plan::Gap(format!(
            "parent {:?} of block {} {:?} is not announced, head is {} {:?}",
            block.parent_hash,
            block.block_number,
            block.hash,
            head.block_number,
            head.hash
        ))
    }

    pub fn apply(&mut self, notification: &BlockChangeNotification) {
        for block in &notification.drop_blocks {
            if self.contains(block.block_number, &block.hash) {
                self.blocks.remove(&block.block_number);
            }
        }
        for block in &notification.new_blocks {
            // A fork drops everything above the fork point.
            let stale: Vec<u64> = self
                .blocks
                .range(block.block_number..)
                .map(|(n, _)| *n)
                .collect();
            for n in stale {
                self.blocks.remove(&n);
            }
            self.blocks.insert(block.block_number, block.clone());
        }
        if let Some(head) = self.head().map(|b| b.block_number) {
            let min = head.saturating_sub(self.max_fork_depth);
            self.blocks = self.blocks.split_off(&min);
        }
    }
}

fn new_blocks(blocks: Vec<BlockContext>) -> BlockChangeNotification {
    BlockChangeNotification {
        change_type: CHANGE_NEW,
        new_blocks: blocks,
        drop_blocks: vec![],
    }
}

/// Appends `next` to `batch` when both only announce new blocks, so that
/// consecutive blocks are announced in one Kafka message.
pub fn merge(
    batch: &mut Vec<BlockChangeNotification>, next: BlockChangeNotification,
) {
    if let Some(last) = batch.last_mut() {
        if last.change_type == CHANGE_NEW && next.change_type == CHANGE_NEW {
            last.new_blocks.extend(next.new_blocks);
            return;
        }
    }
    batch.push(next);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: u64, id: u8, parent: u8) -> BlockContext {
        BlockContext {
            hash: H256::repeat_byte(id),
            parent_hash: H256::repeat_byte(parent),
            block_number: number,
            timestamp: number,
        }
    }

    fn announce(chain: &mut AnnouncedChain, b: &BlockContext) -> u64 {
        match chain.plan(b) {
            Plan::Announce(n) => {
                chain.apply(&n);
                n.change_type
            }
            other => panic!("unexpected plan {:?}", other),
        }
    }

    #[test]
    fn starts_at_genesis() {
        let chain = AnnouncedChain::new(10, 100);
        assert_eq!(chain.plan(&block(9, 9, 8)), Plan::Skip);
        assert!(matches!(chain.plan(&block(11, 11, 10)), Plan::Gap(_)));
        assert!(matches!(chain.plan(&block(10, 10, 9)), Plan::Announce(_)));
    }

    #[test]
    fn extends_and_forks() {
        let mut chain = AnnouncedChain::new(0, 100);
        announce(&mut chain, &block(0, 1, 0));
        announce(&mut chain, &block(1, 2, 1));
        announce(&mut chain, &block(2, 3, 2));
        announce(&mut chain, &block(3, 4, 3));
        // duplicate
        assert_eq!(chain.plan(&block(2, 3, 2)), Plan::Skip);
        // fork replacing blocks 2 and 3
        let fork = block(2, 30, 2);
        match chain.plan(&fork) {
            Plan::Announce(n) => {
                assert_eq!(n.change_type, CHANGE_FORK);
                let dropped: Vec<u64> =
                    n.drop_blocks.iter().map(|b| b.block_number).collect();
                assert_eq!(dropped, vec![2, 3]);
                chain.apply(&n);
            }
            other => panic!("unexpected plan {:?}", other),
        }
        assert_eq!(chain.head().unwrap().hash, H256::repeat_byte(30));
        assert_eq!(announce(&mut chain, &block(3, 40, 30)), CHANGE_NEW);
        // unknown parent
        assert!(matches!(chain.plan(&block(5, 60, 50)), Plan::Gap(_)));
    }

    #[test]
    fn prunes_old_blocks() {
        let mut chain = AnnouncedChain::new(0, 2);
        for n in 0..10u8 {
            announce(&mut chain, &block(n as u64, n + 1, n));
        }
        assert_eq!(chain.blocks.len(), 3);
        assert!(chain.is_announced(1, &H256::repeat_byte(99)));
    }

    #[test]
    fn merges_new_blocks() {
        let mut batch = vec![];
        merge(&mut batch, new_blocks(vec![block(1, 2, 1)]));
        merge(&mut batch, new_blocks(vec![block(2, 3, 2)]));
        merge(
            &mut batch,
            BlockChangeNotification {
                change_type: CHANGE_FORK,
                new_blocks: vec![block(2, 30, 2)],
                drop_blocks: vec![block(2, 3, 2)],
            },
        );
        merge(&mut batch, new_blocks(vec![block(3, 40, 30)]));
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].new_blocks.len(), 2);
    }

    #[test]
    fn decodes_go_notification() {
        let json = r#"{"changeType":1,"newBlocks":[{"hash":"0x0101010101010101010101010101010101010101010101010101010101010101","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000","blockNumber":1,"timestamp":5}],"dropBlocks":null}"#;
        let n: BlockChangeNotification = serde_json::from_str(json).unwrap();
        assert_eq!(n.new_blocks[0].block_number, 1);
        assert!(n.drop_blocks.is_empty());
    }
}
