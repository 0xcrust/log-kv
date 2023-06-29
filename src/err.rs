/// A Result type generic over a [KvsError]
pub type Result<T> = std::result::Result<T, KvsError>;

/// Custom error
#[derive(Debug)]
pub enum KvsError {
    DeSer(Option<serde_json::Error>),
    Io(std::io::Error),
    KeyNotFound,
}

impl std::fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for KvsError {}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        KvsError::DeSer(Some(e))
    }
}
impl From<std::io::Error> for KvsError {
    fn from(e: std::io::Error) -> Self {
        KvsError::Io(e)
    }
}
