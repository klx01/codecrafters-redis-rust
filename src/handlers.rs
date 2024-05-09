use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;
use crate::command::{Command, normalize_name};
use crate::connection::{Connection, ConnectionKind};
use crate::resp::*;
use crate::storage::{ExpiryTs, now_ts, StorageItem, StorageKey};

const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub(crate) enum HandleError {
    InvalidArgs,
    ResponseFailed,
}
type HandleResult<T> = Result<T, HandleError>;

pub(crate) async fn psync(connection: &mut Connection, command: Command) -> HandleResult<Receiver<Command>> {
    let args = command.get_args();
    let args = split_and_assert_value(args, b"?")?;
    let _ = split_and_assert_value(args, b"-1")?;
    write_simple_string(&mut connection.stream, format!("FULLRESYNC {} 0", connection.server.replication_id)).await
        .ok_or(HandleError::ResponseFailed)?;
    let file_contents = hex::decode(EMPTY_RDB_FILE_HEX).expect("hardcoded db file should be decodable");
    write_binary_string(&mut connection.stream, file_contents, false).await
        .ok_or(HandleError::ResponseFailed)?;
    let repl_rx = connection.server.replication.read().expect("got poisoned lock").subscribe();
    return Ok(repl_rx);
}

pub(crate) async fn handle_command_ignore_invalid(connection: &mut Connection, command: Command) -> Option<()> {
    let res = handle_command(connection, command).await;
    match res {
        Ok(x) => Some(x),
        Err(HandleError::InvalidArgs) => Some(()),
        Err(HandleError::ResponseFailed) => None,
    }
}

pub(crate) async fn handle_command(connection: &mut Connection, command: Command) -> HandleResult<()> {
    match command.name.as_str() {
        "PING" => ping(connection).await,
        "ECHO" => echo(connection, command).await,
        "GET" => get(connection, command).await,
        "SET" => set(connection, command).await,
        "INFO" => info(connection, command).await,
        "REPLCONF" => repl_conf(connection, command).await,
        "WAIT" => wait(connection, command).await,
        "CONFIG" => config(connection, command).await,
        _ => {
            eprintln!("received unknown command {}", command.name);
            Err(HandleError::InvalidArgs)
        },
    }
}

async fn ping(connection: &mut Connection) -> HandleResult<()> {
    if matches!(connection.kind, ConnectionKind::ServerSlaveConnectionMaster{..}) {
        Ok(())
    } else {
        write_simple_string(&mut connection.stream, "PONG").await
            .ok_or(HandleError::ResponseFailed)
    }
}

async fn echo(connection: &mut Connection, command: Command) -> HandleResult<()> {
    let (value, _) = split_arg(command.get_args())?;
    write_binary_string(&mut connection.stream, value, true).await
        .ok_or(HandleError::ResponseFailed)
}

async fn get(connection: &mut Connection, command: Command) -> HandleResult<()> {
    let (key, _) = split_arg(command.get_args())?;
    let result = connection.server.storage.get(key);
    write_binary_string_or_null(&mut connection.stream, result).await
        .ok_or(HandleError::ResponseFailed)
}

async fn set(connection: &mut Connection, command: Command) -> HandleResult<()> {
    if !connection.can_replicate() {
        eprintln!("set command was called via readonly connection");
        return Err(HandleError::InvalidArgs);
    }
    let (key, item) = parse_set_args(command.get_args())?;
    do_set(connection, key, item, command);
    if connection.server.is_slave {
        Ok(())
    } else {
        write_simple_string(&mut connection.stream, "OK").await
            .ok_or(HandleError::ResponseFailed)
    }
}

fn parse_set_args(args: &[Vec<u8>]) -> HandleResult<(StorageKey, StorageItem)> {
    let (key, args) = split_arg(args)?;
    let (value, args) = split_arg(args)?;
    let item = StorageItem {
        value: value.clone(),
        expires_at: parse_expiry(args)?,
    };
    Ok((key.clone(), item))
}

fn parse_expiry(args: &[Vec<u8>]) -> HandleResult<Option<ExpiryTs>> {
    if args.len() == 0 {
        return Ok(None);
    }
    let expiry_index = args.iter().position(|x| x.to_ascii_lowercase() == b"px");
    let Some(expiry_index) = expiry_index else {
        return Ok(None);
    };
    let expiry_value = match args.get(expiry_index + 1) {
        Some(x) => x,
        None => {
            eprintln!("No value found for the expiry param");
            return Err(HandleError::InvalidArgs);
        }
    };
    let expiry_value = parse_value::<u128>(expiry_value)?;
    let expires_at = now_ts() + expiry_value;
    Ok(Some(expires_at))
}

fn do_set(connection: &mut Connection, key: StorageKey, item: StorageItem, command: Command) {
    /*
    We need to ensure that replicas have exactly the same state as master,
    so if there are concurrent updates to the same key, replicas need to receive them in the same order as they were applied in master,
    so sending commands to replicas should be done under the same lock as the updates.
     */
    let guard = connection.server.storage.set(key, item);
    connection.replicate(command);
    drop(guard); // guard is unused, it just needs to exist until the end of scope
}

async fn info(connection: &mut Connection, command: Command) -> HandleResult<()> {
    for section in command.get_args() {
        match section.as_slice() {
            b"replication" => info_replication(connection).await?,
            _ => eprintln!("Unknown section {:?}", std::str::from_utf8(&section)),
        };
    }
    Ok(())
}

async fn info_replication(connection: &mut Connection) -> HandleResult<()> {
    let result = format!(
        "# Replication
role:{}
master_replid:{}
master_repl_offset:{}
",
        if connection.server.is_slave { "slave" } else { "master" },
        connection.server.replication_id,
        connection.server.slave_read_offset.load(Ordering::Acquire),
    );
    write_binary_string(&mut connection.stream, result, true).await
        .ok_or(HandleError::ResponseFailed)
}

async fn repl_conf(connection: &mut Connection, command: Command) -> HandleResult<()> {
    let args = command.get_args();
    let (subcommand, args) = split_subcommand(args)?;
    match subcommand.as_str() {
        "CAPA" => repl_conf_capa(connection, args).await,
        "LISTENING-PORT" => repl_conf_port(connection, args).await,
        "GETACK" => repl_conf_get_ack(connection, args).await,
        "ACK" => repl_conf_ack(connection, args).await,
        _ => {
            eprintln!("unknown replconf subcommand {subcommand}");
            Err(HandleError::InvalidArgs)
        }
    }
}

async fn repl_conf_capa(connection: &mut Connection, args: &[Vec<u8>]) -> HandleResult<()> {
    split_and_assert_value(args, b"psync2")?;
    write_simple_string(&mut connection.stream, "OK").await
        .ok_or(HandleError::ResponseFailed)
}

async fn repl_conf_port(connection: &mut Connection, args: &[Vec<u8>]) -> HandleResult<()> {
    split_and_parse_value::<u16>(args)?;
    write_simple_string(&mut connection.stream, "OK").await
        .ok_or(HandleError::ResponseFailed)
}

async fn repl_conf_get_ack(connection: &mut Connection, args: &[Vec<u8>]) -> HandleResult<()> {
    if !connection.server.is_slave {
        eprintln!("received replconf getack as master");
        return Err(HandleError::InvalidArgs);
    }
    split_and_assert_value(args, b"*")?;
    let offset = connection.server.slave_read_offset.load(Ordering::Acquire).to_string();
    write_array_of_strings(&mut connection.stream, ["REPLCONF", "ACK", offset.as_str()]).await
        .ok_or(HandleError::ResponseFailed)
}

async fn repl_conf_ack(connection: &mut Connection, args: &[Vec<u8>]) -> HandleResult<()> {
    let (offset, _) = split_and_parse_value::<usize>(args)?;
    let success = connection.update_acknowledged_offset(offset);
    if !success {
        return Err(HandleError::InvalidArgs);
    }
    Ok(())
}

async fn wait(connection: &mut Connection, command: Command) -> HandleResult<()> {
    if !matches!(connection.kind, ConnectionKind::ServerMasterConnectionExternal{..}) {
        eprintln!("wait command was called via readonly connection");
        return Err(HandleError::InvalidArgs);
    }
    let args = command.get_args();
    let (need_count, args) = split_and_parse_value::<usize>(args)?;
    let (timeout, _) = split_and_parse_value(args)?;
    if timeout > 600000 {
        eprintln!("timeout is too long");
        return Err(HandleError::InvalidArgs);
    }

    let need_offset = connection.get_replicated_offset();
    let (acked_count, waiting_count) = connection.check_acknowledged_replicas(need_offset);
    let acked_count = if (waiting_count > 0) && (acked_count < need_count) {
        // todo: refactor this
        let command = Command{
            byte_size: 37,
            name: "REPLCONF".to_string(),
            raw: vec![
                b"REPLCONF".to_vec(),
                b"GETACK".to_vec(),
                b"*".to_vec()
            ],
        };
        connection.replicate(command);
        sleep(Duration::from_millis(timeout)).await;
        let (acked_count, _) = connection.check_acknowledged_replicas(need_offset);
        acked_count
    } else {
        acked_count
    };
    
    write_int(&mut connection.stream, acked_count as i64).await
        .ok_or(HandleError::ResponseFailed)
}

async fn config(connection: &mut Connection, command: Command) -> HandleResult<()> {
    let args = command.get_args();
    let (subcommand, args) = split_subcommand(args)?;
    match subcommand.as_str() {
        "GET" => config_get(connection, args).await,
        _ => {
            eprintln!("unknown config subcommand {subcommand}");
            Err(HandleError::InvalidArgs)
        }
    }
}

async fn config_get(connection: &mut Connection, args: &[Vec<u8>]) -> HandleResult<()> {
    let (key, _) = split_and_parse_str(args)?;
    match connection.server.config.get(key) {
        Some(value) => write_array_of_strings(&mut connection.stream, [key.as_bytes(), value]).await,
        None => write_null(&mut connection.stream).await,
    }.ok_or(HandleError::ResponseFailed)
}

fn split_subcommand(args: &[Vec<u8>]) -> HandleResult<(String, &[Vec<u8>])> {
    let (subcommand, args) = split_arg(args)?;
    let Some(subcommand) = normalize_name(subcommand) else {
        eprintln!("invalid subcommand name");
        return Err(HandleError::InvalidArgs);
    };
    Ok((subcommand, args))
}

fn split_and_parse_value<T: FromStr>(args: &[Vec<u8>]) -> HandleResult<(T, &[Vec<u8>])> {
    let (value, args) = split_arg(args)?;
    let value = parse_value(value)?;
    Ok((value, args))
}

fn split_and_parse_str(args: &[Vec<u8>]) -> HandleResult<(&str, &[Vec<u8>])> {
    let (value, args) = split_arg(args)?;
    let value = parse_str(value)?;
    Ok((value, args))
}

fn split_arg(args: &[Vec<u8>]) -> HandleResult<(&Vec<u8>, &[Vec<u8>])> {
    let Some((value, args)) = args.split_first() else {
        eprintln!("missing parameter");
        return Err(HandleError::InvalidArgs);
    };
    Ok((value, args))
}

fn parse_value<T: FromStr>(value: &[u8]) -> HandleResult<T> {
    let value = parse_str(value)?;
    match value.parse::<T>() {
        Ok(x) => Ok(x),
        Err(_) => {
            eprintln!("value is not valid: {value}");
            Err(HandleError::InvalidArgs)
        }
    }
}

fn parse_str(value: &[u8]) -> HandleResult<&str> {
    match std::str::from_utf8(value) {
        Ok(x) => Ok(x),
        Err(error) => {
            eprintln!("value is not a valid string {error}");
            Err(HandleError::InvalidArgs)
        }
    }
}

fn split_and_assert_value<'a>(args: &'a [Vec<u8>], expected: &[u8]) -> HandleResult<&'a [Vec<u8>]> {
    let (value, args) = split_arg(args)?;
    if value != expected {
        eprintln!("unexpected parameter value");
        return Err(HandleError::InvalidArgs);
    }
    Ok(args)
}
