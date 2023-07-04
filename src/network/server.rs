use super::{Command, NetRequest, NetResponse, ServerError};
use crate::engine::KvsEngine;
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener};

// Used internally by this module.
type Result<T> = std::result::Result<T, ServerError>;

/// The KVS server.
pub struct KvsServer<Engine> {
    /// A TCP listener for receiving wire messages.
    net: TcpListener,
    /// The kvstore instance for this server.
    engine: Engine,
}

impl<Engine: KvsEngine> KvsServer<Engine> {
    pub fn bind(bind_addr: SocketAddr, engine: Engine) -> Result<Self> {
        let net = TcpListener::bind(bind_addr)?;
        Ok(KvsServer { net, engine })
    }

    pub fn run(mut self) -> Result<()> {
        for incoming in self.net.incoming() {
            let stream = incoming?;
            //log::debug!("received new connection from {:?}", stream.peer_addr());
            let reader = BufReader::new(&stream);
            let mut writer = BufWriter::new(&stream);

            let requests = serde_json::Deserializer::from_reader(reader).into_iter::<NetRequest>();
            for req in requests {
                let req = req?;
                //log::debug!("Received request: {:?}", req);
                let response = match &req.command {
                    Command::Get { key } => {
                        let res = self.engine.get(key.clone());
                        match res {
                            Err(e) => NetResponse::err(&req, e.into()),
                            Ok(None) => NetResponse::success(&req, None),
                            Ok(some_value) => NetResponse::success(&req, some_value),
                        }
                    }
                    Command::Rm { key } => {
                        let res = self.engine.remove(key.clone());
                        match res {
                            Ok(()) => NetResponse::success(&req, None),
                            Err(e) => NetResponse::err(&req, e.into()),
                        }
                    }
                    Command::Set { key, value } => {
                        let res = self.engine.set(key.clone(), value.clone());
                        match res {
                            Ok(()) => NetResponse::success(&req, None),
                            Err(e) => NetResponse::err(&req, e.into()),
                        }
                    }
                };

                //log::debug!("responding: {:?}", response);
                let response = serde_json::to_vec(&response)?;
                writer.write_all(&response)?;
                writer.flush()?;
            }
        }

        Ok(())
    }
}
