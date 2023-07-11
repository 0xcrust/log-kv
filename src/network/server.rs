use super::{Command, NetRequest, NetResponse, ServerError};
use crate::engine::KvsEngine;
use crate::thread_pool::ThreadPool;
use crossbeam::channel::{self, Receiver, Sender, TryRecvError};
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};

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
}

pub struct ShutdownHandle {
    shutdown_init_tx: Sender<()>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ShutdownHandle {
    pub fn shutdown(mut self) -> Result<()> {
        self.shutdown_init_tx
            .send(())
            .map_err(|e| anyhow::anyhow!(e));
        Option::take(&mut self.handle).map(|jh| jh.join().unwrap());
        Ok(())
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        Option::take(&mut self.handle).map(|jh| jh.join().unwrap());
    }
}

impl<Engine: KvsEngine, Tp: ThreadPool + 'static> KvsServer<Engine, Tp> {
    pub fn bind(bind_addr: SocketAddr, engine: Engine, thread_pool: Tp) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true).unwrap();
        Ok(KvsServer {
            listener,
            engine,
            thread_pool,
        })
    }

    pub fn run(self) -> Result<ShutdownHandle> {
        // The channel used to send the shutdown message to the server.
        let (shutdown_init_tx, shutdown_init_rx) = channel::bounded::<()>(1);
        // The channel used to send a shutdown message to each stream.
        let (stream_shutdown_tx, stream_shutdown_rx) = channel::unbounded::<()>();
        // The channel used to wait for shutdown to be acknowledged by all streams.
        let (wait_for_shutdown_tx, wait_for_shutdown_rx) = channel::unbounded::<()>();

        let handle = std::thread::spawn(move || {
            loop {
                match shutdown_init_rx.try_recv() {
                    Ok(_) => {
                        log::debug!("Received shutdown signal. shutting down");
                        _ = stream_shutdown_tx.send(());
                        break;
                    }
                    Err(e) => log::debug!("Shutdown error: {e}"),
                }

                let shutdown_rx = stream_shutdown_rx.clone();
                let wait_tx = wait_for_shutdown_tx.clone();
                match self.listener.accept() {
                    Ok((stream, addr)) => {
                        log::debug!("New connection from {addr}");
                        let engine = self.engine.clone();

                        self.thread_pool.spawn(move || {
                            if let Err(err) = run(engine, stream, shutdown_rx, wait_tx) {
                                log::error!("{err}");
                            }
                        });
                    }
                    Err(e) => log::debug!("Accept error: {e}"),
                }
            }
            log::debug!("waiting for streams shutdown");
            wait_for_shutdown_rx.recv();
        });

        Ok(ShutdownHandle {
            shutdown_init_tx,
            handle: Some(handle),
        })
    }
}

fn run<T: KvsEngine>(
    engine: T,
    stream: TcpStream,
    stream_shutdown_rx: Receiver<()>,
    _dropped: Sender<()>,
) -> Result<()> {
    log::debug!("received new connection from {:?}", stream.peer_addr());
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let mut requests = serde_json::Deserializer::from_reader(reader).into_iter::<NetRequest>();
    loop {
        match stream_shutdown_rx.try_recv() {
            Ok(()) => {
                stream.shutdown(Shutdown::Both);
                break Ok(());
            }
            Err(e) if e == TryRecvError::Empty => {}
            Err(e) => return Err(anyhow::anyhow!(e).into()),
        };

        match requests.next() {
            Some(request) => {
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
            None => {
                break Ok(());
            }
        }
    }
}
