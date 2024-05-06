use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use crate::misc::ServerInfo;
use crate::resp::*;
use crate::storage::{Storage, StorageItem};

const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum HandlingMode {
    ServerMasterConnectionExternal,
    ServerMasterConnectionSlave,
    ServerSlaveConnectionExternal,
    ServerSlaveConnectionMaster,
}

pub(crate) async fn handle_command(stream: &mut (impl AsyncWriteExt + Unpin), command: &Command, storage: &Storage, server_info: &ServerInfo, mode: HandlingMode, bytes_processed: usize) -> Option<()> {
    let write_result = exec_with_timeout(
        handle_command_inner(stream, &command, &storage, &server_info, mode, bytes_processed)
    ).await;
    match write_result {
        Ok(x) => x,
        Err(_) => {
            eprintln!("response timed out");
            return None;
        }
    }
}

async fn handle_command_inner(stream: &mut (impl AsyncWriteExt + Unpin), (command_name, command_params): &Command, storage: &Storage, server_info: &ServerInfo, mode: HandlingMode, bytes_processed: usize) -> Option<()> {
    match command_name.as_str() {
        "PING" => ping(stream, mode).await,
        "ECHO" => echo(stream, command_params).await,
        "GET" => get(stream, command_params, storage).await,
        "SET" => set(stream, command_params, storage, mode).await,
        "INFO" => info(stream, command_params, server_info, storage).await,
        "REPLCONF" => repl_conf(stream, command_params, server_info, bytes_processed).await,
        "PSYNC" => psync(stream, command_params, server_info).await,
        _ => {
            eprintln!("received unknown command {command_name}");
            Some(())
        },
    }
}

async fn ping(stream: &mut (impl AsyncWriteExt + Unpin), mode: HandlingMode) -> Option<()> {
    if mode == HandlingMode::ServerSlaveConnectionMaster {
        Some(())
    } else {
        write_simple_string(stream, "PONG").await
    }
}

async fn echo(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>]) -> Option<()> {
    if params.len() < 1 {
        eprintln!("echo command is missing arguments");
        return Some(());
    }
    write_binary_string(stream, &params[0], true).await
}

async fn get(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], storage: &Storage) -> Option<()> {
    if params.len() < 1 {
        eprintln!("get command is missing arguments");
        return Some(());
    }
    let result = storage.get(&params[0]);
    write_binary_string_or_null(stream, result).await
}

async fn set(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], storage: &Storage, mode: HandlingMode) -> Option<()> {
    if mode == HandlingMode::ServerSlaveConnectionExternal {
        eprintln!("set command was called for slave by non-master");
        return Some(());
    }
    if mode == HandlingMode::ServerMasterConnectionSlave {
        eprintln!("set command was called for master by slave");
        return Some(());
    }
    let Some((key, params)) = params.split_first() else {
        eprintln!("set command is missing a key parameter");
        return Some(());
    };
    let Some((value, params)) = params.split_first() else {
        eprintln!("set command is missing a value parameter");
        return Some(());
    };
    let Some(expiry) = parse_expiry(params) else {
        return Some(());
    };
    let item = StorageItem {
        value: value.clone(),
        expires_at: expiry,
    };
    storage.set(key.clone(), item);
    if mode == HandlingMode::ServerSlaveConnectionMaster {
        Some(())
    } else {
        write_simple_string(stream, "OK").await
    }
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
    let expiry_value = parse_value(expiry_value)?;
    let expires_at = Instant::now() + Duration::from_millis(expiry_value);
    Some(Some(expires_at))
}

async fn info(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo, _storage: &Storage) -> Option<()> {
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

async fn info_replication(stream: &mut (impl AsyncWriteExt + Unpin), info: &ServerInfo) -> Option<()> {
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

async fn repl_conf(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo, bytes_processed: usize) -> Option<()> {
    let Some((subcommand, params)) = split_and_parse_str(params) else {
        return Some(());
    };
    let subcommand = subcommand.to_ascii_uppercase();
    match subcommand.as_str() {
        "CAPA" => repl_conf_capa(stream, params, info).await,
        "LISTENING-PORT" => repl_conf_port(stream, params, info).await,
        "GETACK" => repl_conf_get_ack(stream, params, info, bytes_processed).await,
        "ACK" => repl_conf_ack(stream, params, info).await,
        _ => {
            eprintln!("unknown replconf subcommand {subcommand}");
            return Some(());
        }
    }
}

async fn repl_conf_capa(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo) -> Option<()> {
    if info.is_slave {
        eprintln!("received replconf capa as a slave");
        return Some(());
    }
    let Some(_) = split_and_assert_value(params, b"psync2") else {
        return Some(());
    };
    write_simple_string(stream, "OK").await
}

async fn repl_conf_port(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo) -> Option<()> {
    if info.is_slave {
        eprintln!("received replconf listening-port as a slave");
        return Some(());
    }
    let Some(_) = split_and_parse_value::<u16>(params) else {
        return Some(());
    };
    write_simple_string(stream, "OK").await
}

async fn repl_conf_get_ack(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo, bytes_processed: usize) -> Option<()> {
    if !info.is_slave {
        eprintln!("received replconf getack as master");
        return Some(());
    }
    let Some(_) = split_and_assert_value(params, b"*") else {
        return Some(());
    };
    write_array_of_strings(stream, ["REPLCONF", "ACK", bytes_processed.to_string().as_str()]).await
}

async fn repl_conf_ack(_stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo) -> Option<()> {
    if info.is_slave {
        eprintln!("received replconf ack as a slave");
        return Some(());
    }
    let Some(_) = split_and_parse_value::<usize>(params) else {
        return Some(());
    };
    Some(())
}

async fn psync(stream: &mut (impl AsyncWriteExt + Unpin), params: &[Vec<u8>], info: &ServerInfo) -> Option<()> {
    if info.is_slave {
        eprintln!("received psync command as a slave");
        return Some(());
    }
    let Some(params) = split_and_assert_value(params, b"?") else {
        return Some(());
    };
    let Some(_) = split_and_assert_value(params, b"-1") else {
        return Some(());
    };
    write_simple_string(stream, format!("FULLRESYNC {} 0", info.replication_id)).await?;
    let file_contents = hex::decode(EMPTY_RDB_FILE_HEX).unwrap();
    write_binary_string(stream, file_contents, false).await
}

fn split_and_parse_value<T: FromStr>(params: &[Vec<u8>]) -> Option<(T, &[Vec<u8>])> {
    let Some((value, params)) = params.split_first() else {
        eprintln!("missing parameter");
        return None;
    };
    let val = parse_value(value)?;
    Some((val, params))
}

fn split_and_parse_str(params: &[Vec<u8>]) -> Option<(&str, &[Vec<u8>])> {
    let Some((value, params)) = params.split_first() else {
        eprintln!("missing parameter");
        return None;
    };
    let val = parse_str(value)?;
    Some((val, params))
}

fn split_and_assert_value<'a>(params: &'a [Vec<u8>], expected: &[u8]) -> Option<&'a [Vec<u8>]> {
    let Some((value, params)) = params.split_first() else {
        eprintln!("missing parameter");
        return None;
    };
    if value != expected {
        eprintln!("unexpected parameter value");
        return None;
    }
    Some(params)
}

fn parse_value<T: FromStr>(value: &[u8]) -> Option<T> {
    let value = parse_str(value)?;
    match value.parse::<T>() {
        Ok(x) => Some(x),
        Err(_) => {
            eprintln!("value is not valid: {value}");
            None
        }
    }
}

fn parse_str(value: &[u8]) -> Option<&str> {
    match std::str::from_utf8(value) {
        Ok(x) => Some(x),
        Err(error) => {
            eprintln!("value is not a valid string {error}");
            return None;
        }
    }
}
