use clap::{Parser, Subcommand};
use kvs::KvsClient;
use std::net::SocketAddr;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let socket_addr = cli.addr.parse::<SocketAddr>()?;
    let mut client = KvsClient::connect(socket_addr)?;

    match cli.command {
        Command::Get { key } => match client.get(key)? {
            Some(val) => println!("{val}"),
            None => println!("Key not found"),
        },
        Command::Rm { key } => client.remove(key)?,
        Command::Set { key, value } => client.set(key, value)?,
    }

    Ok(())
}

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
    #[clap(
        help = "The socket address to bind to",
        long,
        default_value = "127.0.0.1:4000",
        global = true
    )]
    addr: String,
}

#[derive(Subcommand)]
pub enum Command {
    Set {
        #[arg(help = "The key of the object to be inserted")]
        key: String,
        #[arg(help = "The object to be inserted")]
        value: String,
    },
    Get {
        #[arg(help = "The key of the object we want to get")]
        key: String,
    },
    Rm {
        #[arg(help = "The key of the object we want to remove")]
        key: String,
    },
}
