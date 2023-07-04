//! An in-memory filestore.

use serde_json::Deserializer;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::prelude::*;

use super::{KvsEngine, Op};
use crate::err::KvsError;

/// The number of ops that can be performed before another log-compaction
/// process needs to be done.
const SIZE_LIMIT: usize = 10_000;

/// The store.
pub struct KvStore {
    /// The path to the logfile.
    fp: std::path::PathBuf,
    /// The handle to the logfile.
    fh: File,
    /// An index mapping a key to the file offset of its last `set` op.
    index: BTreeMap<String, u64>,
    /// The number of operations that have been made since the store last
    /// underwent compaction.
    uncmp: usize,
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

        //let contents = std::fs::read_to_string(path.clone())?;
        //println!("Kvstore. Contents: {:#?}", contents);

        let mut stream = Deserializer::from_reader(&fh).into_iter::<Op>();
        let mut index = BTreeMap::new();

        let mut offset = stream.byte_offset();
        while let Some(op) = stream.next() {
            match op? {
                Op::Set { key, .. } => {
                    index.insert(key, offset as u64);
                }
                Op::Rm { key } => {
                    index.remove(&key);
                }
            }
            offset = stream.byte_offset();
        }

        Ok(KvStore {
            fp: path,
            fh,
            index,
            uncmp: 0,
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

        let mut nf = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(self.fp.clone())?;

        for (_, op) in keep {
            nf.write_all(serde_json::to_string_pretty(&op)?.as_bytes())?;
        }
        self.uncmp = 0;
        Ok(())
    }

    fn needs_compaction(&self) -> bool {
        self.uncmp == SIZE_LIMIT
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        let op = Op::set(key.clone(), value);
        self.index.insert(key, self.fh.stream_position()?);
        self.fh
            .write_all(serde_json::to_string_pretty(&op)?.as_bytes())?;
        self.uncmp += 1;

        if self.needs_compaction() {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        return match self.index.get(&key) {
            Some(_) => {
                self.index.remove(&key).expect("unreachable!");
                let op = Op::rm(key);
                self.fh
                    .write_all(serde_json::to_string_pretty(&op)?.as_bytes())?;
                self.uncmp += 1;

                if self.needs_compaction() {
                    self.compact()?;
                }
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        };
    }

    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        match self.index.get(&key) {
            Some(pos) => {
                let fh_ref = std::io::Read::by_ref(&mut self.fh);
                fh_ref.seek(std::io::SeekFrom::Start(*pos))?;
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
