use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use crate::ServerInfo;
use crate::resp::{write_binary_string, write_binary_string_or_null, write_simple_string};
use crate::storage::{Storage, StorageItem};

const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub(crate) async fn handle_command(stream: &mut TcpStream, command_name: &str, command_params: &[Vec<u8>], storage: &Storage, server_info: &ServerInfo) -> Option<()> {
    let command_name = command_name.to_ascii_uppercase();
    match command_name.as_str() {
        "PING" => ping(stream).await,
        "ECHO" => echo(stream, command_params).await,
        "GET" => get(stream, command_params, storage).await,
        "SET" => set(stream, command_params, storage).await,
        "INFO" => info(stream, command_params, server_info, storage).await,
        "REPLCONF" => repl_conf(stream, command_params).await,
        "PSYNC" => psync(stream, command_params, server_info).await,
        _ => {
            eprintln!("received unknown command {command_name}");
            Some(())
        },
    }
}

async fn ping(stream: &mut TcpStream) -> Option<()> {
    write_simple_string(stream, "PONG").await
}

async fn echo(stream: &mut TcpStream, params: &[Vec<u8>]) -> Option<()> {
    if params.len() < 1 {
        eprintln!("echo command is missing arguments");
        return Some(());
    }
    write_binary_string(stream, &params[0], true).await
}

async fn get(stream: &mut TcpStream, params: &[Vec<u8>], storage: &Storage) -> Option<()> {
    if params.len() < 1 {
        eprintln!("get command is missing arguments");
        return Some(());
    }
    let result = storage.get(&params[0]);
    write_binary_string_or_null(stream, result).await
}

async fn set(stream: &mut TcpStream, params: &[Vec<u8>], storage: &Storage) -> Option<()> {
    if params.len() < 2 {
        eprintln!("set command is missing arguments");
        return Some(());
    }
    let (key, params) = params.split_first().unwrap();
    let (value, params) = params.split_first().unwrap();
    let item = StorageItem {
        value: value.clone(),
        expires_at: parse_expiry(params)?,
    };
    storage.set(key.clone(), item);
    write_simple_string(stream, "OK").await
}

fn parse_expiry(params: &[Vec<u8>]) -> Option<Option<Instant>> {
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

async fn info(stream: &mut TcpStream, params: &[Vec<u8>], info: &ServerInfo, _storage: &Storage) -> Option<()> {
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

async fn info_replication(stream: &mut TcpStream, info: &ServerInfo) -> Option<()> {
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
    write_binary_string(stream, result, true).await
}

async fn repl_conf(stream: &mut TcpStream, params: &[Vec<u8>]) -> Option<()> {
    if params.len() < 2 {
        eprintln!("replconf command is missing arguments");
        return Some(());
    }
    match params[0].as_slice() {
        b"capa" => if params[1].as_slice() == b"psync2" {
            write_simple_string(stream, "OK").await
        } else {
            eprintln!("unexpected replconf capabilities");
            Some(())
        },
        b"listening-port" => {
            let port = match std::str::from_utf8(&params[1]) {
                Ok(x) => x,
                Err(error) => {
                    eprintln!("slave port is not a valid string {error}");
                    return Some(());
                }
            };
            match port.parse::<u16>() {
                Ok(_) => write_simple_string(stream, "OK").await,
                Err(error) => {
                    eprintln!("slave port is not a valid int {error}");
                    Some(())
                }
            }
        }
        _ => {
            eprintln!("unexpected replconf argument");
            return Some(());
        }
    }
}

async fn psync(stream: &mut TcpStream, params: &[Vec<u8>], info: &ServerInfo) -> Option<()> {
    if params.len() < 2 {
        eprintln!("psync command is missing arguments");
        return Some(());
    }
    if params[0].as_slice() != b"?"{
        eprintln!("unexpected psync id argument");
        return Some(());
    }
    if params[1].as_slice() != b"-1"{
        eprintln!("unexpected psync offset argument");
        return Some(());
    }
    write_simple_string(stream, format!("FULLRESYNC {} 0", info.replication_id)).await?;
    let file_contents = hex::decode(EMPTY_RDB_FILE_HEX).unwrap();
    write_binary_string(stream, file_contents, false).await
}
