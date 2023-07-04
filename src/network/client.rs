use super::{ClientError, Command, NetRequest, NetResponse, Response};
use std::io::prelude::*;
use std::io::BufWriter;
use std::net::{SocketAddr, TcpStream};

// Used internally by this module.
type Result<T> = std::result::Result<T, ClientError>;

/// Represents a client connection to a kvs server.
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    pub fn connect(server_addr: SocketAddr) -> Result<Self> {
        //println!("tcp connect");
        let stream = TcpStream::connect(server_addr)?;
        //println!("so it didn't even get here");
        Ok(KvsClient { stream })
    }

    fn send_request(&mut self, req: NetRequest) -> Result<NetResponse> {
        let writer = BufWriter::new(&self.stream);

        serde_json::to_writer(writer, &req)?;
        //log::info!("Sent request: {:#?}", req);

        let mut buf = [0u8; 4096];
        let nbytes = self.stream.read(&mut buf)?;
        let response: NetResponse = serde_json::from_slice(&buf[..nbytes])?;

        //log::info!("Got response: {:#?}", response);
        if response.id != req.id {
            return Err("Invalid response".to_string().into());
        }

        Ok(response)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let response = self.send_request(new_get_req(key))?;

        match response.response {
            Response::Err(e) => Err(e.into()),
            Response::Success(None) => Ok(None),
            Response::Success(Some(value)) => Ok(Some(value)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let response = self.send_request(new_set_req(key, value))?;
        match response.response {
            Response::Err(e) => Err(e.into()),
            Response::Success(_) => Ok(()),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let response = self.send_request(new_rm_req(key))?;
        match response.response {
            Response::Err(e) => Err(e.into()),
            Response::Success(_) => Ok(()),
        }
    }
}

fn new_get_req(key: String) -> NetRequest {
    NetRequest {
        id: rand::random::<u64>(),
        command: Command::Get { key },
    }
}
fn new_set_req(key: String, value: String) -> NetRequest {
    NetRequest {
        id: rand::random::<u64>(),
        command: Command::Set { key, value },
    }
}
fn new_rm_req(key: String) -> NetRequest {
    NetRequest {
        id: rand::random::<u64>(),
        command: Command::Rm { key },
    }
}
