mod client;
mod server;
use crate::err::KvsError;
use serde::{Deserialize, Serialize};

pub use client::KvsClient;
pub use server::KvsServer;

#[derive(Clone, Debug, Serialize, Deserialize)]
/// A command sent from the client to a KvsEngine server.
struct NetRequest {
    id: u64,
    command: Command,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// The response sent from the KvsEngine server to the client.
struct NetResponse {
    id: u64,
    response: Response,
}

impl NetResponse {
    pub fn err(req: &NetRequest, e: ServerError) -> Self {
        NetResponse {
            id: req.id,
            response: Response::Err(format!("{:?}", e)),
        }
    }
    pub fn success(req: &NetRequest, res: Option<String>) -> Self {
        NetResponse {
            id: req.id,
            response: Response::Success(res),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Response types.
enum Response {
    /// Error response containing the error message.
    Err(String),
    /// Success response expected to only contain a `Some(_)` for get requests.
    Success(Option<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Serializable commands for the network protocol.
enum Command {
    Get { key: String },
    Rm { key: String },
    Set { key: String, value: String },
}

pub enum ServerError {
    Core(KvsError),
    Io(std::io::Error),
    Serde(serde_json::Error),
}

#[derive(Debug)]
pub enum ClientError {
    Error(String),
}

impl std::fmt::Debug for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Io(e) => write!(f, "Network io error: {:?}", e),
            ServerError::Serde(e) => {
                write!(f, "Network serialization/deserialization error: {:?}", e)
            }
            ServerError::Core(e) => write!(f, "Core error: {:?}", e),
        }
    }
}
impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for ServerError {}

impl From<KvsError> for ServerError {
    fn from(e: KvsError) -> Self {
        ServerError::Core(e)
    }
}
impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError::Io(e)
    }
}
impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        ServerError::Serde(e)
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for ClientError {}

impl From<String> for ClientError {
    fn from(s: String) -> ClientError {
        ClientError::Error(s)
    }
}
impl From<std::io::Error> for ClientError {
    fn from(s: std::io::Error) -> ClientError {
        ClientError::Error(s.to_string())
    }
}
impl From<serde_json::Error> for ClientError {
    fn from(s: serde_json::Error) -> ClientError {
        ClientError::Error(s.to_string())
    }
}
