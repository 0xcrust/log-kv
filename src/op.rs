use serde::{Deserialize, Serialize};

/// Serializable write operations on the Kvstore.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
