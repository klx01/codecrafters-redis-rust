use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use crate::resp::*;

mod resp;

type Storage = HashMap<Vec<u8>, Vec<u8>>;
type LockStorage = RwLock<Storage>;
type ArcStorage = Arc<LockStorage>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await
        .expect("Failed to bind to the port");

    let storage: ArcStorage = Default::default();

    loop {
        let (stream, _addr) = listener.accept().await
            .expect("Failed to accept connection");
        let storage = Arc::clone(&storage);
        tokio::spawn(async move {
            handle_connection(stream, storage).await
        });
    }
}

async fn handle_connection(mut stream: TcpStream, storage: ArcStorage) -> Option<()> {
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
            handle_command(&mut stream, &command_name, command_params, &storage)
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

async fn handle_command(mut stream: &mut TcpStream, command_name: &str, command_params: &[Vec<u8>], storage: &LockStorage) -> bool {
    match command_name {
        "PING" => handle_ping(&mut stream).await,
        "ECHO" => handle_echo(&mut stream, command_params).await,
        "GET" => handle_get(&mut stream, command_params, storage).await,
        "SET" => handle_set(&mut stream, command_params, storage).await,
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

async fn handle_get(stream: &mut TcpStream, params: &[Vec<u8>], storage: &LockStorage) -> bool {
    if params.len() < 1 {
        eprintln!("get command is missing arguments");
        return true;
    }
    let result = do_get(storage, &params[0]);
    match result {
        Some(value) => write_binary_string(stream, &value).await,
        None => write_null(stream).await,
    }
}

fn do_get(storage: &LockStorage, key: &Vec<u8>) -> Option<Vec<u8>> {
    storage.read().expect("got poisoned lock, can't handle that")
        .get(key).map(|x| x.clone())
}

async fn handle_set(stream: &mut TcpStream, params: &[Vec<u8>], storage: &LockStorage) -> bool {
    if params.len() < 2 {
        eprintln!("get command is missing arguments");
        return true;
    }
    do_set(storage, params[0].clone(), params[1].clone());
    write_simple_string(stream, "OK").await
}

fn do_set(storage: &LockStorage, key: Vec<u8>, value: Vec<u8>) {
    storage.write().expect("got poisoned lock, can't handle that")
        .insert(key, value);
}

