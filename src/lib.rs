//! An in-memory filestore.

pub mod err;
mod op;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;

pub use crate::err::{KvsError, Result};
use crate::op::Op;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

/// The store.
pub struct KvStore {
    /// The handle to the logfile.
    fh: std::fs::File,
    /// An index mapping a key to the file offset
    /// of its last `set` value.
    index: BTreeMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
pub struct Log(Vec<Op>);

impl KvStore {
    /// Set a key-value pair.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Op::set(key.clone(), value);
        self.index.insert(key, self.fh.stream_position()?);
        self.fh
            .write_all(serde_json::to_string_pretty(&op)?.as_bytes())?;
        self.add_op(op)?;

        Ok(())
    }

    /// Remove a key-value pair by its key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        return match self.index.get(&key) {
            Some(_) => {
                self.index.remove(&key).expect("unreachable!");
                let op = Op::rm(key);
                self.fh
                    .write_all(serde_json::to_string_pretty(&op)?.as_bytes())?;
                Ok(())
            }
            None => {
                println!("Key not found");
                Err(KvsError::KeyNotFound)
            }
        };
    }

    /// Get a value by its key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(pos) => {
                let fh_ref = std::io::Read::by_ref(&mut self.fh);
                fh_ref.seek(std::io::SeekFrom::Start(*pos))?;
                let mut stream = Deserializer::from_reader(fh_ref).into_iter::<Op>();
                let op = stream.next().ok_or(KvsError::DeSer(None))?;
                match op? {
                    Op::Set { value, .. } => Ok(Some(value)),
                    Op::Rm { .. } => {
                        unreachable!();
                    }
                }
            }
            None => {
                println!("Key not found");
                Ok(None)
            }
        }
    }

    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<KvStore> {
        let mut path: std::path::PathBuf = path.into();
        path.push("logs.json");

        let fh = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let mut stream = Deserializer::from_reader(&fh).into_iter::<Op>();
        let mut index = BTreeMap::new();

        let mut offset = stream.byte_offset();
        //println!("start offset: {:?}", offset);
        while let Some(op) = stream.next() {
            //println!("item: {:?}", item);
            //println!("offset: {:?}", offset);
            match op? {
                Op::Set { key, .. } => {
                    //test_vec.push(op.clone());
                    index.insert(key, offset as u64);
                }
                Op::Rm { key } => {
                    index.remove(&key);
                }
            }
            offset = stream.byte_offset();
        }
        //println!("ops gotten: {:?}", test_vec);

        Ok(KvStore { fh, index })
    }

    fn add_op(&mut self, op: Op) -> Result<()> {
        self.fh
            .write_all(serde_json::to_string_pretty(&op)?.as_bytes())?;
        Ok(())
    }
}
