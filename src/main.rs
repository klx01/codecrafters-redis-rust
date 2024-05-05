use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
use tokio::io::BufReader;
use tokio::net::{lookup_host, TcpListener, TcpStream};
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use crate::commands::handle_command;
use crate::handshake::master_handshake;
use crate::resp::*;
use crate::storage::*;

mod resp;
mod storage;
mod handshake;
mod commands;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, num_args = 2, value_names=["master_host", "master_port"])]
    replicaof: Vec<String>,
}

struct ServerInfo {
    is_slave: bool,
    replication_id: String,
    replication_offset: usize,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let port = cli.port.unwrap_or(6379);
    let is_slave = if cli.replicaof.len() > 0 {
        let master_addr = format!("{}:{}", cli.replicaof[0], cli.replicaof[1]);
        let master_socket = lookup_host(&master_addr).await
            .expect(format!("Failed to lookup the address of master host {master_addr}").as_str())
            .next()
            .expect(format!("No addresses found for master host {master_addr}").as_str());
        let mut master_stream = TcpStream::connect(master_socket).await
            .expect("failed to connect to master");
        master_handshake(&mut master_stream, port).await;
        true
    }  else {
        false
    };
    let info = ServerInfo {
        is_slave,
        replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        replication_offset: 0,
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await
        .expect(format!("Failed to bind to the port {port}").as_str());

    let storage = Arc::new(Storage::default());
    let info = Arc::new(info);

    loop {
        let (stream, _addr) = listener.accept().await
            .expect("Failed to accept connection");
        let storage = Arc::clone(&storage);
        let info = Arc::clone(&info);
        tokio::spawn(async move {
            handle_connection(stream, storage, info).await
        });
    }
}

async fn exec_with_timeout<R>(future: impl Future<Output = R>) -> Result<R, Elapsed> {
    timeout(Duration::from_millis(1000), future).await
}

async fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>, info: Arc<ServerInfo>) -> Option<()> {
    loop {
        let command = exec_with_timeout(
            read_command(&mut BufReader::new(&mut stream))
        ).await;
        let command = match command {
            Ok(x) => x?,
            Err(_) => {
                eprintln!("read timed out");
                return None;
            }
        };
        if command.len() == 0 {
            eprintln!("received a command of size 0");
            continue;
        }
        let (command_name, command_params) = command.split_first().unwrap();
        let command_name = match std::str::from_utf8(command_name) {
            Ok(x) => x,
            Err(error) => {
                eprintln!("failed to parse the received command name: {error}");
                continue;
            }
        };
        let command_name = command_name.to_ascii_uppercase();

        let write_result = exec_with_timeout(
            handle_command(&mut stream, &command_name, &command_params, &storage, &info)
        ).await;
        match write_result {
            Ok(x) => x?,
            Err(_) => {
                eprintln!("response timed out");
                return None;
            }
        }
    }
}

