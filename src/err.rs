//use crate::network::NetworkError;

/// A Result type generic over a [KvsError]
pub type Result<T> = std::result::Result<T, KvsError>;

/// Different variants of a KVS Error.
pub enum KvsError {
    Serde(Option<serde_json::Error>),
    Io(std::io::Error),
    KeyNotFound,
    Sled(sled::Error),
    StrConvert(std::string::FromUtf8Error),
}
impl std::fmt::Debug for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvsError::Serde(e) => write!(f, "Error during serialization/deserialization: {:?}", e),
            KvsError::Io(e) => write!(f, "Io: {:?}", e),
            KvsError::KeyNotFound => write!(f, "Key not found."),
            KvsError::Sled(e) => write!(f, "Sled: {:?}", e),
            KvsError::StrConvert(e) => write!(f, "str convert: {:?}", e),
        }
    }
}
impl std::fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for KvsError {}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        KvsError::Serde(Some(e))
    }
}
impl From<std::io::Error> for KvsError {
    fn from(e: std::io::Error) -> Self {
        KvsError::Io(e)
    }
}
impl From<sled::Error> for KvsError {
    fn from(e: sled::Error) -> Self {
        KvsError::Sled(e)
    }
}
impl From<std::string::FromUtf8Error> for KvsError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        KvsError::StrConvert(e)
    }
}
