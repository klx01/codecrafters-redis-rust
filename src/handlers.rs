use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;
use crate::command::{Command, normalize_name};
use crate::connection::{Connection, ConnectionKind};
use crate::resp::*;
use crate::storage::{ExpiryTs, now_ts, StorageItemSimple, StorageKey, StreamEntry, SimpleValue};
use crate::transaction::QueuedCommand;

const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

const INVALID_ARGS_DEFAULT: HandleError = HandleError::InvalidArgs(ArgsError::Generic);

pub(crate) enum HandleError {
    InvalidArgs(ArgsError),
    ResponseFailed,
}
#[derive(Default)]
pub(crate) enum ArgsError {
    #[default]
    Generic,
    CanNotIncrementThisValue,
}
impl ArgsError {
    pub(crate) fn get_message(&self) -> &'static str {
        match self {
            ArgsError::Generic => "Invalid args",
            ArgsError::CanNotIncrementThisValue => "ERR value is not an integer or out of range",
        }
    }
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
        Err(HandleError::InvalidArgs(_)) => Some(()),
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
        "KEYS" => keys(connection, command).await,
        "TYPE" => handle_type(connection, command).await,
        "XADD" => xadd(connection, command).await,
        "INCR" => incr(connection, command).await,
        "MULTI" => multi(connection).await,
        _ => {
            eprintln!("received unknown command {} {:?}", command.name, command.raw);
            Err(INVALID_ARGS_DEFAULT)
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
    let result = connection.server.storage.get_simple(key);
    let stream = &mut connection.stream;
    match result {
        None => write_null(stream).await,
        Some(SimpleValue::String(data)) => write_binary_string(stream, data, true).await,
        Some(SimpleValue::Int(data)) => write_binary_string(stream, data.to_string(), true).await,
    }.ok_or(HandleError::ResponseFailed)
}

async fn set(connection: &mut Connection, command: Command) -> HandleResult<()> {
    if !connection.can_replicate() {
        eprintln!("set command was called via readonly connection");
        return Err(INVALID_ARGS_DEFAULT);
    }
    let (key, item) = parse_set_args(command.get_args())?;
    if let Some(transaction) = connection.get_transaction_mut() {
        if transaction.started {
            transaction.queue.push(QueuedCommand::Set{key, item});
            return write_simple_string(&mut connection.stream, "QUEUED").await
                .ok_or(HandleError::ResponseFailed);
        }
    }
    do_set(connection, key, item, command);
    if connection.server.is_slave {
        Ok(())
    } else {
        write_simple_string(&mut connection.stream, "OK").await
            .ok_or(HandleError::ResponseFailed)
    }
}

fn parse_set_args(args: &[Vec<u8>]) -> HandleResult<(StorageKey, StorageItemSimple)> {
    let (key, args) = split_arg(args)?;
    let (value, args) = split_arg(args)?;
    let expiry = parse_expiry(args)?;
    let item = StorageItemSimple::from_data(value.clone(), expiry);
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
            return Err(INVALID_ARGS_DEFAULT);
        }
    };
    let expiry_value = parse_value::<u128>(expiry_value)?;
    let expires_at = now_ts() + expiry_value;
    Ok(Some(expires_at))
}

fn do_set(connection: &mut Connection, key: StorageKey, item: StorageItemSimple, command: Command) {
    /*
    We need to ensure that replicas have exactly the same state as master,
    so if there are concurrent updates to the same key, replicas need to receive them in the same order as they were applied in master,
    so sending commands to replicas should be done under the same lock as the updates.
     */
    let guard = connection.server.storage.set_string(key, item);
    connection.replicate(command);
    drop(guard); // guard is unused, it just needs to exist until the end of scope
}

async fn info(connection: &mut Connection, command: Command) -> HandleResult<()> {
    for section in command.get_args() {
        match section.as_slice() {
            b"replication" => info_replication(connection).await?,
            b"SERVER" => {
                write_binary_string(&mut connection.stream, "# Server\n", true).await
                    .ok_or(HandleError::ResponseFailed)?
            },
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
            Err(INVALID_ARGS_DEFAULT)
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
        return Err(INVALID_ARGS_DEFAULT);
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
        return Err(INVALID_ARGS_DEFAULT);
    }
    Ok(())
}

async fn wait(connection: &mut Connection, command: Command) -> HandleResult<()> {
    if !matches!(connection.kind, ConnectionKind::ServerMasterConnectionExternal{..}) {
        eprintln!("wait command was called via readonly connection");
        return Err(INVALID_ARGS_DEFAULT);
    }
    let args = command.get_args();
    let (need_count, args) = split_and_parse_value::<usize>(args)?;
    let (timeout, _) = split_and_parse_value(args)?;
    if timeout > 600000 {
        eprintln!("timeout is too long");
        return Err(INVALID_ARGS_DEFAULT);
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
            Err(INVALID_ARGS_DEFAULT)
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

async fn keys(connection: &mut Connection, command: Command) -> HandleResult<()> {
    let args = command.get_args();
    split_and_assert_value(args, b"*")?;
    let keys = connection.server.storage.keys();
    write_array_of_strings(&mut connection.stream, keys).await
        .ok_or(HandleError::ResponseFailed)
}

async fn handle_type(connection: &mut Connection, command: Command) -> HandleResult<()> {
    let args = command.get_args();
    let (key, _) = split_arg(args)?;
    let kind = connection.server.storage.get_value_kind(key);
    write_simple_string(&mut connection.stream, kind).await
        .ok_or(HandleError::ResponseFailed)
}

async fn xadd(connection: &mut Connection, command: Command) -> HandleResult<()> {
    if !connection.can_replicate() {
        eprintln!("xadd command was called via readonly connection");
        return Err(INVALID_ARGS_DEFAULT);
    }
    let (key, item) = parse_xadd_args(command.get_args())?;
    if let Some(transaction) = connection.get_transaction_mut() {
        if transaction.started {
            transaction.queue.push(QueuedCommand::Xadd{key, item});
            return write_simple_string(&mut connection.stream, "QUEUED").await
                .ok_or(HandleError::ResponseFailed);
        }
    }
    let id = item.id.clone(); // todo: would it be possible not to clone it?
    do_xadd(connection, key, item, command)?;
    if connection.server.is_slave {
        Ok(())
    } else {
        write_binary_string(&mut connection.stream, id, true).await
            .ok_or(HandleError::ResponseFailed)
    }
}
fn parse_xadd_args(args: &[Vec<u8>]) -> HandleResult<(StorageKey, StreamEntry)> {
    let (key, args) = split_arg(args)?;
    let (item_id, args) = split_arg(args)?;
    let mut args = args;
    let mut entry_data = HashMap::new();
    while !args.is_empty() {
        let args2 = args;
        let (entry_key, args2) = split_arg(args2)?;
        let (entry_value, args2) = split_arg(args2)?;
        args = args2;
        entry_data.insert(entry_key.clone(), entry_value.clone());
    }
    let item = StreamEntry {
        id: item_id.clone(),
        data: entry_data,
    };
    Ok((key.clone(), item))
}
fn do_xadd(connection: &mut Connection, key: StorageKey, item: StreamEntry, command: Command) -> HandleResult<()> {
    /*
    We need to ensure that replicas have exactly the same state as master,
    so if there are concurrent updates to the same key, replicas need to receive them in the same order as they were applied in master,
    so sending commands to replicas should be done under the same lock as the updates.
     */
    let Some(guard) = connection.server.storage.append_to_stream(key, item) else {
        eprintln!("can't do xadd when key is not a stream");
        return Err(INVALID_ARGS_DEFAULT);
    };
    connection.replicate(command);
    drop(guard); // guard is unused, it just needs to exist until the end of scope
    Ok(())
}

async fn incr(connection: &mut Connection, command: Command) -> HandleResult<()> {
    if !connection.can_replicate() {
        eprintln!("incr command was called via readonly connection");
        return Err(INVALID_ARGS_DEFAULT);
    }
    let (key, _args) = split_arg(command.get_args())?;
    let key = key.clone();
    if let Some(transaction) = connection.get_transaction_mut() {
        if transaction.started {
            transaction.queue.push(QueuedCommand::Incr{key});
            return write_simple_string(&mut connection.stream, "QUEUED").await
                .ok_or(HandleError::ResponseFailed);
        }
    }
    let new_value = do_incr(connection, key, command)?;
    if connection.server.is_slave {
        Ok(())
    } else {
        write_int(&mut connection.stream, new_value).await
            .ok_or(HandleError::ResponseFailed)
    }
}

fn do_incr(connection: &mut Connection, key: StorageKey, command: Command) -> HandleResult<i64> {
    /*
    We need to ensure that replicas have exactly the same state as master,
    so if there are concurrent updates to the same key, replicas need to receive them in the same order as they were applied in master,
    so sending commands to replicas should be done under the same lock as the updates.
     */
    let Some((guard, value)) = connection.server.storage.increment(key) else {
        eprintln!("can't do incr when key is not an int");
        return Err(HandleError::InvalidArgs(ArgsError::CanNotIncrementThisValue));
    };
    connection.replicate(command);
    drop(guard); // guard is unused, it just needs to exist until the end of scope
    Ok(value)
}

async fn multi(connection: &mut Connection) -> HandleResult<()> {
    let Some(transaction) = connection.get_transaction_mut() else {
        eprintln!("multi command was called on a wrong type of connection");
        return Err(INVALID_ARGS_DEFAULT);
    };
    transaction.started = true;
    write_simple_string(&mut connection.stream, "OK").await
        .ok_or(HandleError::ResponseFailed)
}

fn split_subcommand(args: &[Vec<u8>]) -> HandleResult<(String, &[Vec<u8>])> {
    let (subcommand, args) = split_arg(args)?;
    let Some(subcommand) = normalize_name(subcommand) else {
        eprintln!("invalid subcommand name");
        return Err(INVALID_ARGS_DEFAULT);
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
        return Err(INVALID_ARGS_DEFAULT);
    };
    Ok((value, args))
}

fn parse_value<T: FromStr>(value: &[u8]) -> HandleResult<T> {
    let value = parse_str(value)?;
    match value.parse::<T>() {
        Ok(x) => Ok(x),
        Err(_) => {
            eprintln!("value is not valid: {value}");
            Err(INVALID_ARGS_DEFAULT)
        }
    }
}

fn parse_str(value: &[u8]) -> HandleResult<&str> {
    match std::str::from_utf8(value) {
        Ok(x) => Ok(x),
        Err(error) => {
            eprintln!("value is not a valid string {error}");
            Err(INVALID_ARGS_DEFAULT)
        }
    }
}

fn split_and_assert_value<'a>(args: &'a [Vec<u8>], expected: &[u8]) -> HandleResult<&'a [Vec<u8>]> {
    let (value, args) = split_arg(args)?;
    if value != expected {
        eprintln!("unexpected parameter value");
        return Err(INVALID_ARGS_DEFAULT);
    }
    Ok(args)
}
