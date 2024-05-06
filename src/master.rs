use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::broadcast::error::RecvError;
use crate::commands::{handle_command, HandlingMode};
use crate::misc::{make_info, make_listener, ServerInfo};
use crate::resp::{Command, read_command, write_command};
use crate::storage::Storage;

pub(crate) async fn run_master(port: u16) {
    let listener = make_listener(port).await;
    let storage = Arc::new(Storage::default());
    let info = make_info(false);
    let (replication_tx, _) = channel(100);
    loop {
        let (stream, _addr) = listener.accept().await
            .expect("Failed to accept connection");
        let storage = Arc::clone(&storage);
        let info = Arc::clone(&info);
        let replication_tx = replication_tx.clone();
        tokio::spawn(async move {
            handle_connection(BufReader::new(stream), storage, info, replication_tx).await
        });
    }
}

/*
    Handling loop:
        None is returned if there were any errors in the message format.
        Because we are most likely not going to be able to recover from that.
        If the format is correct, but data is unexpected - we might be able to recover, and in that case we just skip the message.
        Format errors also include both read and write timeouts.
        Because that might mean that some data was lost.

    Replication:
        We are using broadcast channels for replication, because we want each slave to have their own queue of messages,
        so that slow slaves do not affect fast slaves;
        For master server, normal connections should have a transmitter side of the channel, while slave connections should have a receiver side of the channel.
        We can't detect that a connection is a slave until the handshake is completed,
        so we start as a normal channel with a transmitter, and then convert to a slave with a receiver.
 */
async fn handle_connection(mut stream: impl AsyncBufReadExt + AsyncWriteExt + Unpin, storage: Arc<Storage>, info: Arc<ServerInfo>, repl_transmitter: Sender<Command>) -> Option<()> {
    let mut repl_receiver = loop {
        let (command, _) = read_command(&mut stream).await?;
        let Some(command) = command else {
            // todo: return error replies instead of just logging errors
            continue;
        };
        handle_command(&mut stream, &command, &storage, &info, HandlingMode::ServerMasterConnectionExternal, 0).await?;
        match command.0.as_str() {
            "PSYNC" => {
                // ideally receiver should be created at the moment when the initial dump is created, so that all messages after the dump are queued
                break repl_transmitter.subscribe();
            },
            "SET" => {
                // if we want to ensure the order of the messages, we should send this while we are still holding the write lock
                let _ = repl_transmitter.send(command);
            },
            _ => {},
        }
    };
    drop(repl_transmitter);
    //println!("Slave is connected");
    let res = handle_slave(stream, storage, info, &mut repl_receiver).await;
    //println!("Slave is disconnected");
    res
}

async fn handle_slave(mut stream: impl AsyncBufReadExt + AsyncWriteExt + Unpin, storage: Arc<Storage>, info: Arc<ServerInfo>, repl_receiver: &mut Receiver<Command>) -> Option<()> {
    loop {
        let mut reader = BufReader::new(&mut stream);
        select! {
            command = read_command(&mut reader) => {
                let (command, _) = command?;
                if let Some(command) = command {
                    handle_command(&mut stream, &command, &storage, &info, HandlingMode::ServerMasterConnectionSlave, 0).await?;
                } else {
                    // todo: return error replies instead of just logging errors
                };
            },
            replicated_command = repl_receiver.recv() => {
                match replicated_command {
                    Ok(command) => {
                        write_command(&mut stream, command).await?;
                    },
                    Err(RecvError::Lagged(_)) => {
                        // we have dropped some messages that should have been replicated, can't continue
                        eprintln!("a slave connection has lagged too much");
                        return None;
                    }
                    Err(RecvError::Closed) => {
                        unreachable!("main loop should always have a copy of a transmitter");
                    }
                }
            },
        }
    }
}
