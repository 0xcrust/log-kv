//! An in-memory filestore.

use super::{KvsEngine, Op};
use crate::err::KvsError;
use serde_json::Deserializer;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, BufWriter, prelude::*},
    sync::{Arc, Mutex},
};

/// The maximum redundant space(in bytes) before the log needs to be compacted.
const REDUNDANT_SIZE_LIMIT: usize = 1024 * 1024;

pub struct KvStore(Arc<Mutex<KvStoreInner>>);

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore(Arc::clone(&self.0))
    }
}

/// The store.
pub struct KvStoreInner {
    /// The path to the logfile.
    fp: std::path::PathBuf,
    /// The handle to the logfile.
    fh: File,
    /// An index mapping a key to the start and end offset of its last `set` op.
    index: BTreeMap<String, Offset>,
    /// The size(in bytes) taken up by redundant entries.
    redundant_size: usize,
}

#[derive(Copy, Clone)]
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

        let inner = KvStoreInner {
            fp: path,
            fh,
            index,
            redundant_size,
        };

        Ok(KvStore(Arc::new(Mutex::new(inner))))
    }

    fn compact(&self) -> crate::Result<()> {
        let mut store = self.0.lock().unwrap();
        let path = store.fp.to_owned();
        store.fh.rewind()?;

        let offsets = store
            .index
            .iter()
            .map(|(s, o)| (s.to_owned(), o.to_owned()))
            .collect::<Vec<_>>();
        let mut keep = vec![];
        for (key, offset) in offsets {
            store
                .fh
                .seek(std::io::SeekFrom::Start(offset.start as u64))?;
            let mut stream = Deserializer::from_reader(&mut store.fh).into_iter::<Op>();
            let op = stream.next().ok_or(KvsError::Serde(None))??;
            keep.push((key, op));
        }

        let mut new_index = BTreeMap::new();
        let mut nfh = File::options()
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;

        for (key, op) in keep {
            let start = nfh.stream_position()?;
            nfh.write_all(serde_json::to_string(&op)?.as_bytes())?;
            let end = nfh.stream_position()?;
            let res = new_index.insert(key, new_offset(start as usize, end as usize));
            assert!(res.is_none());
        }

        store.fh = nfh;
        store.redundant_size = 0;
        store.index = new_index;

        drop(store);

        Ok(())
    }

    fn needs_compaction(&self) -> bool {
        self.0.lock().unwrap().redundant_size > REDUNDANT_SIZE_LIMIT
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> crate::Result<()> {
        let op = Op::set(key.clone(), value);

        let mut store = self.0.lock().unwrap();
        store.fh.seek(std::io::SeekFrom::End(0)).unwrap();
        let start = store.fh.stream_position()?;
        store.fh.write_all(serde_json::to_string(&op)?.as_bytes())?;
        let end = store.fh.stream_position()?;

        if let Some(offset) = store
            .index
            .insert(key, new_offset(start as usize, end as usize))
        {
            store.redundant_size += offset.len();
        }
        drop(store);

        if self.needs_compaction() {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        let mut store = self.0.lock().unwrap();
        match store.index.remove(&key) {
            Some(offset) => {
                store.redundant_size += offset.len();
                let op = Op::rm(key);
                store.fh.seek(std::io::SeekFrom::End(0)).unwrap();
                store.fh.write_all(serde_json::to_string(&op)?.as_bytes())?;
                drop(store);

                if self.needs_compaction() {
                    self.compact()?;
                }
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }

    fn get(&self, key: String) -> crate::Result<Option<String>> {
        let store = self.0.lock().unwrap();
        let path = store.fp.to_owned();
        match store.index.get(&key) {
            Some(pos) => {
                let mut reader = File::options().read(true).open(path)?;
                reader.seek(std::io::SeekFrom::Start(pos.start as u64))?;

                let mut stream = Deserializer::from_reader(reader).into_iter::<Op>();
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
