use super::{Command, NetRequest, NetResponse, ServerError};
use crate::engine::KvsEngine;
use crate::thread_pool::ThreadPool;
use crossbeam::channel::{self, Receiver, Sender};
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};

// Used internally by this module.
type Result<T> = std::result::Result<T, ServerError>;

/// The KVS server.
pub struct KvsServer<Engine, Tp> {
    /// A TCP listener for receiving wire messages.
    listener: TcpListener,
    /// The kvstore instance for this server.
    engine: Engine,
    /// The threadpool for servicing stream requests.
    thread_pool: Tp,
    shutdown_init_rx: Receiver<()>,
}

pub struct ShutdownHandle(Sender<()>);

impl ShutdownHandle {
    pub fn shutdown(self) -> Result<()> {
        self.0.send(()).map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

impl<Engine: KvsEngine, Tp: ThreadPool + 'static> KvsServer<Engine, Tp> {
    pub fn bind(
        bind_addr: SocketAddr,
        engine: Engine,
        thread_pool: Tp,
    ) -> Result<(Self, ShutdownHandle)> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true).unwrap();

        let (shutdown_init_tx, shutdown_init_rx) = channel::bounded::<()>(1);

        let server = KvsServer {
            listener,
            engine,
            thread_pool,
            shutdown_init_rx,
        };
        let shutdown = ShutdownHandle(shutdown_init_tx);
        Ok((server, shutdown))
    }

    pub fn run(self) -> Result<()> {
        loop {
            match self.shutdown_init_rx.try_recv() {
                Ok(_) => {
                    log::debug!("Received shutdown signal. shutting down");
                    break;
                }
                Err(e) => {
                    log::debug!("Shutdown error: {e}");
                }
            }

            match self.listener.accept() {
                Ok((stream, addr)) => {
                    log::debug!("New connection from {addr}");
                    let engine = self.engine.clone();

                    self.thread_pool.spawn(move || {
                        if let Err(err) = run(engine, stream) {
                            log::error!("run error: {err}");
                        }
                    });
                }
                Err(e) => log::debug!("Accept error: {e}"),
            }
        }
        log::debug!("waiting for streams shutdown");

        Ok(())
    }
}

fn run<T: KvsEngine>(engine: T, stream: TcpStream) -> Result<()> {
    log::debug!(
        "received new connection from {:?}",
        stream.peer_addr().unwrap()
    );
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let requests = serde_json::Deserializer::from_reader(reader).into_iter::<NetRequest>();
    for request in requests {
        let req = request?;
        log::debug!("Received request: {:?}", req);
        let response = match &req.command {
            Command::Get { key } => {
                let res = engine.get(key.clone());
                match res {
                    Err(e) => NetResponse::err(&req, e.into()),
                    Ok(None) => NetResponse::success(&req, None),
                    Ok(some_value) => NetResponse::success(&req, some_value),
                }
            }
            Command::Rm { key } => {
                let res = engine.remove(key.clone());
                match res {
                    Ok(()) => NetResponse::success(&req, None),
                    Err(e) => NetResponse::err(&req, e.into()),
                }
            }
            Command::Set { key, value } => {
                let res = engine.set(key.clone(), value.clone());
                match res {
                    Ok(()) => NetResponse::success(&req, None),
                    Err(e) => NetResponse::err(&req, e.into()),
                }
            }
        };

        log::debug!("responding: {:?}", response);
        let response = serde_json::to_vec(&response)?;
        writer.write_all(&response)?;
        writer.flush()?;
    }
    Ok(())
}
