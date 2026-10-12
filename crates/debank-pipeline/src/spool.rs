//! Local queue of produced blocks, the counterpart of the Go pipeline
//! `s3_temp_dir`.
//!
//! Each produced epoch is one file named `<seq>-<height>-<hash>.rlp`. `seq`
//! increases in production order, which is the order the blocks must be
//! announced in: a pivot chain reorg produces the new branch again with
//! lower heights.

use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use cfx_types::H256;

/// The uploaded artifacts of one block, encoded as they are uploaded.
#[derive(Debug, Clone, PartialEq)]
pub struct SpoolRecord {
    pub hash: H256,
    pub parent_hash: H256,
    pub number: u64,
    pub timestamp: u64,
    pub state_root: H256,
    pub parent_state_root: H256,
    /// Gzipped JSON of the eth header.
    pub header: Vec<u8>,
    /// Gzipped JSON of the block file.
    pub block_file: Vec<u8>,
    /// Gzipped JSON of the block validation.
    pub validation: Vec<u8>,
    /// RLP of the block storage diff.
    pub state_diff: Vec<u8>,
}

// Not derived: `rlp_derive` encodes `Vec<u8>` fields as lists of bytes.
impl Encodable for SpoolRecord {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(10)
            .append(&self.hash)
            .append(&self.parent_hash)
            .append(&self.number)
            .append(&self.timestamp)
            .append(&self.state_root)
            .append(&self.parent_state_root)
            .append(&self.header)
            .append(&self.block_file)
            .append(&self.validation)
            .append(&self.state_diff);
    }
}

impl Decodable for SpoolRecord {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if rlp.item_count()? != 10 {
            return Err(DecoderError::RlpIncorrectListLen);
        }
        Ok(Self {
            hash: rlp.val_at(0)?,
            parent_hash: rlp.val_at(1)?,
            number: rlp.val_at(2)?,
            timestamp: rlp.val_at(3)?,
            state_root: rlp.val_at(4)?,
            parent_state_root: rlp.val_at(5)?,
            header: rlp.val_at(6)?,
            block_file: rlp.val_at(7)?,
            validation: rlp.val_at(8)?,
            state_diff: rlp.val_at(9)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SpoolEntry {
    pub seq: u64,
    pub number: u64,
    pub hash: H256,
    pub path: PathBuf,
}

pub struct Spool {
    dir: PathBuf,
    tmp_dir: PathBuf,
    next_seq: AtomicU64,
}

impl Spool {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, String> {
        let dir = dir.into();
        let tmp_dir = dir.join("tmp");
        fs::create_dir_all(&tmp_dir)
            .map_err(|e| format!("create {:?}: {}", tmp_dir, e))?;
        let spool = Self {
            dir,
            tmp_dir,
            next_seq: AtomicU64::new(0),
        };
        let next_seq = spool.list()?.last().map_or(0, |e| e.seq + 1);
        spool.next_seq.store(next_seq, Ordering::SeqCst);
        Ok(spool)
    }

    pub fn dir(&self) -> &Path { &self.dir }

    /// Durably stores a record and returns its entry.
    pub fn push(&self, record: &SpoolRecord) -> Result<SpoolEntry, String> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let name =
            format!("{:020}-{}-{:x}.rlp", seq, record.number, record.hash);
        let tmp = self.tmp_dir.join(&name);
        let path = self.dir.join(&name);
        let data = rlp::encode(record);
        let write = || -> std::io::Result<()> {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&data)?;
            file.sync_all()?;
            fs::rename(&tmp, &path)
        };
        write().map_err(|e| format!("write {:?}: {}", path, e))?;
        Ok(SpoolEntry {
            seq,
            number: record.number,
            hash: record.hash,
            path,
        })
    }

    /// Pending entries in production order.
    pub fn list(&self) -> Result<Vec<SpoolEntry>, String> {
        let mut entries = vec![];
        let dir = fs::read_dir(&self.dir)
            .map_err(|e| format!("read {:?}: {}", self.dir, e))?;
        for item in dir {
            let item =
                item.map_err(|e| format!("read {:?}: {}", self.dir, e))?;
            let path = item.path();
            if let Some(entry) = parse_name(&path) {
                entries.push(entry);
            }
        }
        entries.sort();
        Ok(entries)
    }

    pub fn contains(&self, number: u64, hash: &H256) -> Result<bool, String> {
        Ok(self
            .list()?
            .iter()
            .any(|e| e.number == number && e.hash == *hash))
    }

    pub fn read(&self, entry: &SpoolEntry) -> Result<SpoolRecord, String> {
        let data = fs::read(&entry.path)
            .map_err(|e| format!("read {:?}: {}", entry.path, e))?;
        rlp::decode(&data)
            .map_err(|e| format!("decode {:?}: {}", entry.path, e))
    }

    pub fn remove(&self, entry: &SpoolEntry) -> Result<(), String> {
        fs::remove_file(&entry.path)
            .map_err(|e| format!("remove {:?}: {}", entry.path, e))
    }
}

fn parse_name(path: &Path) -> Option<SpoolEntry> {
    let name = path.file_name()?.to_str()?.strip_suffix(".rlp")?;
    let mut parts = name.splitn(3, '-');
    let seq = parts.next()?.parse().ok()?;
    let number = parts.next()?.parse().ok()?;
    let hash = parts.next()?.parse().ok()?;
    Some(SpoolEntry {
        seq,
        number,
        hash,
        path: path.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(number: u64, id: u8) -> SpoolRecord {
        SpoolRecord {
            hash: H256::repeat_byte(id),
            parent_hash: H256::repeat_byte(id - 1),
            number,
            timestamp: 1,
            state_root: H256::repeat_byte(2),
            parent_state_root: H256::repeat_byte(3),
            header: vec![1],
            block_file: vec![2, 3],
            validation: vec![],
            state_diff: vec![4],
        }
    }

    #[test]
    fn keeps_production_order_across_reopen() {
        let dir = std::env::temp_dir()
            .join(format!("debank-spool-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let spool = Spool::open(&dir).unwrap();
        spool.push(&record(5, 10)).unwrap();
        spool.push(&record(6, 11)).unwrap();
        spool.push(&record(5, 12)).unwrap();
        let spool = Spool::open(&dir).unwrap();
        let entry = spool.push(&record(6, 13)).unwrap();
        assert_eq!(entry.seq, 3);
        let entries = spool.list().unwrap();
        let numbers: Vec<u64> = entries.iter().map(|e| e.number).collect();
        assert_eq!(numbers, vec![5, 6, 5, 6]);
        assert_eq!(spool.read(&entries[2]).unwrap(), record(5, 12));
        // byte fields are rlp strings, not lists
        let raw = fs::read(&entries[2].path).unwrap();
        assert!(Rlp::new(&raw).at(7).unwrap().is_data());
        assert!(spool.contains(6, &H256::repeat_byte(11)).unwrap());
        spool.remove(&entries[0]).unwrap();
        assert_eq!(spool.list().unwrap().len(), 3);
        fs::remove_dir_all(&dir).unwrap();
    }
}
