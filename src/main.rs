use clap::Parser;
use crate::master::run_master;
use crate::slave::run_slave;

mod resp;
mod storage;
mod handshake;
mod commands;
mod master;
mod misc;
mod slave;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, num_args = 2, value_names=["master_host", "master_port"])]
    replicaof: Vec<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let port = cli.port.unwrap_or(6379);

    if cli.replicaof.len() > 0 {
        let master_addr = format!("{}:{}", cli.replicaof[0], cli.replicaof[1]);
        run_slave(port, &master_addr).await;
    } else {
        run_master(port).await;
    };
}
