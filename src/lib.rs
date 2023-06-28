//! An in-memory filestore.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

/// A result type generic over any error
pub type Result<T> = std::result::Result<T, KvsError>;

/// The store.
pub struct KvStore {
    log_fh: std::fs::File,
    log: Vec<Op>,
}

impl KvStore {
    /// Set a key-value pair.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Op::set(key.clone(), value.clone());
        self.add_op(op)?;

        Ok(())
    }

    fn add_op(&mut self, op: Op) -> Result<()> {
        self.log.push(op.clone());
        let log = serde_json::to_string_pretty(&self.log)?;
        self.log_fh.rewind()?;
        self.log_fh.write(log.as_bytes())?;

        Ok(())
    }

    fn build_from_path(&mut self) -> Result<HashMap<String, String>> {
        let mut map = HashMap::new();
        let ops = self.fetch_ops()?;
        recursive_apply(&mut map, &ops);
        self.log = ops;
        Ok(map)
    }

    fn fetch_ops(&mut self) -> Result<Vec<Op>> {
        let mut contents: String = String::new();
        self.log_fh.rewind()?;
        self.log_fh.read_to_string(&mut contents)?;
        let ops = if !contents.is_empty() {
            serde_json::from_str::<Vec<Op>>(&contents)?
        } else {
            Vec::new()
        };

        self.log = ops.clone();
        Ok(ops)
    }

    /// Get a value by its key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let map = self.build_from_path()?;
        if let Some(v) = map.get(&key) {
            Ok(Some(v.to_owned()))
        } else {
            println!("Key not found");
            Ok(None)
        }
    }

    /// Remove a key-value pair by its key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let mut map = self.build_from_path()?;
        use std::collections::hash_map::Entry;
        return match map.entry(key.clone()) {
            Entry::Occupied(_) => {
                let op = Op::rm(key);
                self.add_op(op)?;
                Ok(())
            }
            Entry::Vacant(_) => {
                println!("Key not found");
                Err(KvsError::KeyNotFound)
            }
        };
    }

    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<KvStore> {
        let mut path: std::path::PathBuf = path.into();
        path.push("logs.json");

        let log_fh = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let log = Vec::new();
        let mut kvs = KvStore { log_fh, log };
        kvs.build_from_path()?;

        Ok(kvs)
    }
}

fn recursive_apply(map: &mut HashMap<String, String>, ops: &Vec<Op>) {
    for op in ops {
        apply(map, op.clone());
    }
}

fn apply(map: &mut HashMap<String, String>, op: Op) {
    match op {
        Op::Set { key, value } => {
            map.insert(key, value);
        }
        Op::Rm { key } => {
            map.remove(&key);
        }
    }
}

/// Serializable write operations on the Kvstore.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Op {
    /// Set
    Set {
        key: String,
        value: String,
    },
    // Remove
    Rm {
        key: String,
    },
}

impl Op {
    pub fn set(key: String, value: String) -> Self {
        Op::Set { key, value }
    }

    pub fn rm(key: String) -> Self {
        Op::Rm { key }
    }
}

/// Custom error
//#[derive(Debug)]
pub enum KvsError {
    DeSer(serde_json::Error),
    Io(std::io::Error),
    KeyNotFound,
}

impl std::fmt::Debug for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            KvsError::KeyNotFound => write!(f, "Key not found"),
            _ => write!(f, "you dey play"),
        }
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        KvsError::DeSer(e)
    }
}
impl From<std::io::Error> for KvsError {
    fn from(e: std::io::Error) -> Self {
        KvsError::Io(e)
    }
}

impl std::fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            KvsError::KeyNotFound => write!(f, "Key not found"),
            _ => write!(f, "{:?}", self),
        }
        //write!(f, "{:?}", self)
    }
}

impl std::error::Error for KvsError {}
