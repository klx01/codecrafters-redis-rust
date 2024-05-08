use std::ffi::OsString;
use std::path::PathBuf;
use clap::Parser;
use std::os::unix::ffi::OsStringExt;
use crate::server::{Config, run_master, run_slave};

mod resp;
mod storage;
mod handshake;
mod handlers;
mod server;
mod command;
mod connection;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, num_args = 2, value_names=["master_host", "master_port"])]
    replicaof: Vec<String>,
    /// the path to the directory where the RDB file is stored
    #[arg(long)]
    dir: Option<PathBuf>,
    /// the name of the RDB file
    #[arg(long)]
    dbfilename: Option<OsString>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let port = cli.port.unwrap_or(6379);

    let dir = cli.dir;
    let dbfilename = cli.dbfilename;
    
    let mut config = Config::default();
    if let Some(dir) = dir {
        config.insert("dir", dir.into_os_string().into_vec());
    }
    if let Some(dbfilename) = dbfilename {
        config.insert("dbfilename", dbfilename.into_vec());
    }

    if cli.replicaof.len() > 0 {
        let master_addr = format!("{}:{}", cli.replicaof[0], cli.replicaof[1]);
        run_slave(port, config, &master_addr).await;
    } else {
        run_master(port, config).await;
    };
}
