use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use clap::Parser;
use tokio::io::BufReader;
use tokio::net::{lookup_host, TcpListener, TcpStream};
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use crate::handshake::master_handshake;
use crate::resp::*;
use crate::storage::*;

mod resp;
mod storage;
mod handshake;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, num_args = 2, value_names=["master_host", "master_port"])]
    replicaof: Vec<String>,
}

struct Info {
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
    let info = Info{
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

async fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>, info: Arc<Info>) -> Option<()> {
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
            handle_command(&mut stream, &command_name, command_params, &storage, &info)
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

async fn handle_command(stream: &mut TcpStream, command_name: &str, command_params: &[Vec<u8>], storage: &Storage, info: &Info) -> Option<()> {
    match command_name {
        "PING" => handle_ping(stream).await,
        "ECHO" => handle_echo(stream, command_params).await,
        "GET" => handle_get(stream, command_params, storage).await,
        "SET" => handle_set(stream, command_params, storage).await,
        "INFO" => handle_info(stream, command_params, info, storage).await,
        _ => {
            eprintln!("received unknown command {command_name}");
            Some(())
        },
    }
}

async fn handle_ping(stream: &mut TcpStream) -> Option<()> {
    write_simple_string(stream, "PONG").await
}

async fn handle_echo(stream: &mut TcpStream, params: &[Vec<u8>]) -> Option<()> {
    if params.len() < 1 {
        eprintln!("echo command is missing arguments");
        return Some(());
    }
    write_binary_string(stream, &params[0]).await
}

async fn handle_get(stream: &mut TcpStream, params: &[Vec<u8>], storage: &Storage) -> Option<()> {
    if params.len() < 1 {
        eprintln!("get command is missing arguments");
        return Some(());
    }
    let result = storage.get(&params[0]);
    write_binary_string_or_null(stream, result).await
}

async fn handle_set(stream: &mut TcpStream, params: &[Vec<u8>], storage: &Storage) -> Option<()> {
    if params.len() < 2 {
        eprintln!("get command is missing arguments");
        return Some(());
    }
    let (key, params) = params.split_first().unwrap();
    let (value, params) = params.split_first().unwrap();
    let item = StorageItem {
        value: value.clone(),
        expires_at: get_expiry(params)?,
    };
    storage.set(key.clone(), item);
    write_simple_string(stream, "OK").await
}

fn get_expiry(params: &[Vec<u8>]) -> Option<Option<Instant>> {
    if params.len() == 0 {
        return Some(None);
    }
    let expiry_index = params.iter().position(|x| x.to_ascii_lowercase() == b"px");
    let Some(expiry_index) = expiry_index else {
        return Some(None);
    };
    let expiry_value = match params.get(expiry_index + 1) {
        Some(x) => x,
        None => {
            eprintln!("No value found for the expiry param");
            return None;
        }
    };
    let expiry_value = match std::str::from_utf8(expiry_value) {
        Ok(x) => x,
        Err(err) => {
            eprintln!("Expiry value is not a valid string {err}");
            return None;
        }
    };
    let expiry_value = match expiry_value.parse() {
        Ok(x) => x,
        Err(err) => {
            eprintln!("Expiry value is not a valid number {err}");
            return None;
        }
    };
    let expires_at = Instant::now() + Duration::from_millis(expiry_value);
    Some(Some(expires_at))
}

async fn handle_info(stream: &mut TcpStream, params: &[Vec<u8>], info: &Info, _storage: &Storage) -> Option<()> {
    for section in params {
        match section.as_slice() {
            b"replication" => info_replication(stream, info).await,
            _ => {
                eprintln!("Unknown section {:?}", std::str::from_utf8(section));
                Some(())
            }
        }?;
    }
    Some(())
}

async fn info_replication(stream: &mut TcpStream, info: &Info) -> Option<()> {
    let result = format!(
"# Replication
role:{}
master_replid:{}
master_repl_offset:{}
",
        if info.is_slave { "slave" } else { "master" },
        info.replication_id,
        info.replication_offset,
    );
    write_binary_string(stream, result).await
}
