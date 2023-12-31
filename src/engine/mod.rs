mod kvs;
mod sled_engine;

pub use kvs::KvStore;
pub use sled_engine::SledEngine;

use crate::err::Result;
use serde::{Deserialize, Serialize};

pub trait KvsEngine: Clone + Send + 'static {
    /// Set a key-value pair.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get a value by its key.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove a key-value pair by its key.
    fn remove(&self, key: String) -> Result<()>;
}

/// Serializable write operations on the Kvstore.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) enum Op {
    Set { key: String, value: String },
    Rm { key: String },
}

impl Op {
    pub fn set(key: String, value: String) -> Self {
        Op::Set { key, value }
    }

    pub fn rm(key: String) -> Self {
        Op::Rm { key }
    }
}
