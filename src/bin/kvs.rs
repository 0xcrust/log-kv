use clap::{Parser, Subcommand};
use kvs::{KvStore, Result};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir().expect("couldn't get cwd");
    let mut kvs = KvStore::open(cwd)?;

    match cli.commands {
        Command::Get { key } => {
            if let Some(val) = kvs.get(key)? {
                println!("{}", val);
            }
        }
        Command::Set { key, value } => {
            kvs.set(key, value)?;
        }
        Command::Rm { key } => {
            kvs.remove(key)?;
        }
    }
    Ok(())
}

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    commands: Command,
}

#[derive(Subcommand)]
enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}
