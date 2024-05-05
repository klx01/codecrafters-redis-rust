use std::sync::Arc;
use std::time::{Duration, Instant};
use clap::Parser;
use tokio::io::BufReader;
use tokio::net::{lookup_host, TcpListener, TcpStream};
use tokio::time::timeout;
use crate::resp::*;
use crate::storage::*;

mod resp;
mod storage;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, num_args = 2, value_names=["master_host", "master_port"])]
    replicaof: Vec<String>,
}

struct Info {
    is_slave: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let is_slave = if cli.replicaof.len() > 0 {
        let master_addr = format!("{}:{}", cli.replicaof[0], cli.replicaof[1]);
        let _master_socket = lookup_host(&master_addr).await
            .expect(format!("Failed to lookup the address of master host {master_addr}").as_str())
            .next()
            .expect(format!("No addresses found for master host {master_addr}").as_str());
        true
    }  else {
        false
    };
    let info = Info{is_slave};

    let port = cli.port.unwrap_or(6379);
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

async fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>, info: Arc<Info>) -> Option<()> {
    loop {
        let command = timeout(
            Duration::from_millis(1000),
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

        let can_continue = timeout(
            Duration::from_millis(1000),
            handle_command(&mut stream, &command_name, command_params, &storage, &info)
        ).await;
        match can_continue {
            Ok(true) => {},
            Ok(false) => return None,
            Err(_) => {
                eprintln!("response timed out");
                return None;
            }
        }
    }
}

async fn handle_command(stream: &mut TcpStream, command_name: &str, command_params: &[Vec<u8>], storage: &Storage, info: &Info) -> bool {
    match command_name {
        "PING" => handle_ping(stream).await,
        "ECHO" => handle_echo(stream, command_params).await,
        "GET" => handle_get(stream, command_params, storage).await,
        "SET" => handle_set(stream, command_params, storage).await,
        "INFO" => handle_info(stream, command_params, info, storage).await,
        _ => {
            eprintln!("received unknown command {command_name}");
            true
        },
    }
}

async fn handle_ping(stream: &mut TcpStream) -> bool {
    write_simple_string(stream, "PONG").await
}

async fn handle_echo(stream: &mut TcpStream, params: &[Vec<u8>]) -> bool {
    if params.len() < 1 {
        eprintln!("echo command is missing arguments");
        return true;
    }
    write_binary_string(stream, &params[0]).await
}

async fn handle_get(stream: &mut TcpStream, params: &[Vec<u8>], storage: &Storage) -> bool {
    if params.len() < 1 {
        eprintln!("get command is missing arguments");
        return true;
    }
    let result = storage.get(&params[0]);
    write_binary_string_or_null(stream, result).await
}

async fn handle_set(stream: &mut TcpStream, params: &[Vec<u8>], storage: &Storage) -> bool {
    if params.len() < 2 {
        eprintln!("get command is missing arguments");
        return true;
    }
    let (key, params) = params.split_first().unwrap();
    let (value, params) = params.split_first().unwrap();
    let Some(expires_at) = get_expiry(params) else {
        return false;
    };
    let item = StorageItem {
        value: value.clone(),
        expires_at,
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

async fn handle_info(stream: &mut TcpStream, params: &[Vec<u8>], info: &Info, _storage: &Storage) -> bool {
    for section in params {
        let can_continue = match section.as_slice() {
            b"replication" => info_replication(stream, info).await,
            _ => {
                eprintln!("Unknown section {:?}", std::str::from_utf8(section));
                true
            }
        };
        if !can_continue {
            return false;
        }
    }
    true
}

async fn info_replication(stream: &mut TcpStream, info: &Info) -> bool {
    let role = if info.is_slave {
        "role:slave"
    } else {
        "role:master"
    };
    write_binary_string(stream, role).await
}
