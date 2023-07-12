use clap::Parser;
use env_logger::Target;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsServer, SledEngine};
use log::*;
use std::net::SocketAddr;

fn main() -> anyhow::Result<()> {
    env_logger::Builder::new().target(Target::Stderr).build();
    env_logger::init();

    let cli = Cli::parse();
    info!("version {}", env!("CARGO_PKG_VERSION"));

    let cwd = std::env::current_dir()?;
    let engine_lock_path = cwd.join("engine.lock");
    let existing_engine = if engine_lock_path.exists() {
        let engine = std::fs::read_to_string(&engine_lock_path)?;
        Some(StorageEngine::try_from_string(engine)?)
    } else {
        None
    };

    let socket_addr = cli.socket_addr.parse::<SocketAddr>()?;
    info!("bind address: {}", socket_addr);

    let engine = match (cli.engine, existing_engine) {
        // If no persistence and no specified engine, use kvs:
        (None, None) => StorageEngine::Kvs,
        // If persistence and no specified engine, use the existing engine:
        (None, Some(engine)) => engine,
        // If persistence and specified engine but they differ, panic:
        (Some(new_specified_engine), Some(existing_engine))
            if new_specified_engine != existing_engine.to_str() =>
        {
            panic!("Specified engine differs from persisting engine!")
        }
        // Use the specified engine if:
        // * the `persistent engine` is None.
        // * the `persistent engine` is Some(_) but is equal to the `specified engine` since
        //   it wasn't caught by the branch above.
        // An invalid storage engine name is caught here:
        (Some(any), _) => StorageEngine::try_from_string(any)?,
    };
    info!("loading {} engine", engine.to_str());
    std::fs::write(&engine_lock_path, engine.to_str())?;

    let pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
    match engine {
        StorageEngine::Kvs => {
            let db = KvStore::open(cwd)?;
            let (server, _) = KvsServer::bind(socket_addr, db, pool)?;
            server.run()?;
        }
        StorageEngine::Sled => {
            let db = SledEngine::open(cwd)?;
            let (server, _) = KvsServer::bind(socket_addr, db, pool)?;
            server.run()?;
        }
    }

    Ok(())
}

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[arg(id = "addr", short, long, default_value = "127.0.0.1:4000")]
    socket_addr: String,
    #[arg(short, long, help = "kvs/sled: the engine to bind to")]
    engine: Option<String>,
}

#[derive(Eq, PartialEq)]
pub enum StorageEngine {
    Kvs,
    Sled,
}

impl StorageEngine {
    pub fn to_str(&self) -> &str {
        match self {
            StorageEngine::Kvs => "kvs",
            StorageEngine::Sled => "sled",
        }
    }

    pub fn try_from_string<T>(s: T) -> anyhow::Result<StorageEngine>
    where
        T: AsRef<str>,
    {
        let s = s.as_ref();

        match s {
            "kvs" => Ok(StorageEngine::Kvs),
            "sled" => Ok(StorageEngine::Sled),
            _ => Err(anyhow::anyhow!("Invalid storage engine name")),
        }
    }
}
