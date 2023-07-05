//! An in-memory filestore.

use serde_json::Deserializer;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::prelude::*;

use super::{KvsEngine, Op};
use crate::err::KvsError;

/// The maximum redundant space(in bytes) before the log needs to be compacted.
const REDUNDANT_SIZE_LIMIT: usize = 1024 * 1024;

/// The store.
pub struct KvStore {
    /// The path to the logfile.
    fp: std::path::PathBuf,
    /// The handle to the logfile.
    fh: File,
    /// An index mapping a key to the start and end offset of its last `set` op.
    index: BTreeMap<String, Offset>,
    /// The size(in bytes) taken up by redundant entries.
    redundant_size: usize,
}

struct Offset {
    start: usize,
    end: usize,
}

fn new_offset(start: usize, end: usize) -> Offset {
    Offset { start, end }
}

impl Offset {
    pub fn len(&self) -> usize {
        self.end - self.start
    }
}

impl KvStore {
    const LOG_LOCATION: &str = "kvstore-logs";

    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<std::path::PathBuf>) -> crate::Result<Self> {
        let mut path: std::path::PathBuf = path.into();
        path.push(Self::LOG_LOCATION);

        let fh = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path.clone())?;

        let mut stream = Deserializer::from_reader(&fh).into_iter::<Op>();
        let mut index = BTreeMap::new();

        let mut start = stream.byte_offset();
        assert!(start == 0);
        let mut redundant_size = 0;
        while let Some(op) = stream.next() {
            let end = stream.byte_offset();
            match op? {
                Op::Set { key, .. } => {
                    if let Some(offset) = index.insert(key, new_offset(start, end)) {
                        redundant_size += offset.len();
                    }
                }
                Op::Rm { key } => {
                    if let Some(offset) = index.remove(&key) {
                        redundant_size += offset.len();
                    }

                    redundant_size += end - start;
                }
            }
            start = end;
        }

        Ok(KvStore {
            fp: path,
            fh,
            index,
            redundant_size,
        })
    }

    fn compact(&mut self) -> crate::Result<()> {
        self.fh.rewind()?;

        let stream = Deserializer::from_reader(&mut self.fh).into_iter::<Op>();
        let mut keep = HashMap::new();
        for op in stream {
            let op = op?;
            match op.clone() {
                Op::Set { key, .. } => _ = keep.insert(key, op),
                Op::Rm { key } => _ = keep.remove(&key),
            }
        }

        let mut nfh = File::options()
            .truncate(true)
            .read(true)
            .write(true)
            .open(&self.fp)?;

        for (_, op) in keep {
            nfh.write_all(serde_json::to_string(&op)?.as_bytes())?;
        }

        self.fh = nfh;
        self.redundant_size = 0;

        Ok(())
    }

    fn needs_compaction(&self) -> bool {
        self.redundant_size > REDUNDANT_SIZE_LIMIT
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        let op = Op::set(key.clone(), value);

        let start = self.fh.stream_position()?;
        self.fh.write_all(serde_json::to_string(&op)?.as_bytes())?;
        let end = self.fh.stream_position()?;

        if let Some(offset) = self
            .index
            .insert(key, new_offset(start as usize, end as usize))
        {
            self.redundant_size += offset.len();
        }

        if self.needs_compaction() {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        match self.index.remove(&key) {
            Some(offset) => {
                self.redundant_size += offset.len();
                let op = Op::rm(key);
                self.fh.write_all(serde_json::to_string(&op)?.as_bytes())?;

                if self.needs_compaction() {
                    self.compact()?;
                }
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }

    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        match self.index.get(&key) {
            Some(pos) => {
                let fh_ref = std::io::Read::by_ref(&mut self.fh);
                fh_ref.seek(std::io::SeekFrom::Start(pos.start as u64))?;
                let mut stream = Deserializer::from_reader(fh_ref).into_iter::<Op>();
                let op = stream.next().ok_or(KvsError::Serde(None))?;
                match op? {
                    Op::Set { value, .. } => Ok(Some(value)),
                    Op::Rm { .. } => {
                        unreachable!();
                    }
                }
            }
            None => Ok(None),
        }
    }
}
