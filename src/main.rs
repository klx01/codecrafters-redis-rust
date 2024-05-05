use std::sync::Arc;
use clap::Parser;
use tokio::io::BufReader;
use tokio::net::{lookup_host, TcpListener, TcpStream};
use tokio::select;
use tokio::sync::broadcast::{channel, Sender};
use tokio::sync::broadcast::error::RecvError;
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
    let master_stream = if cli.replicaof.len() > 0 {
        let master_addr = format!("{}:{}", cli.replicaof[0], cli.replicaof[1]);
        let master_socket = lookup_host(&master_addr).await
            .expect(format!("Failed to lookup the address of master host {master_addr}").as_str())
            .next()
            .expect(format!("No addresses found for master host {master_addr}").as_str());
        let mut master_stream = TcpStream::connect(master_socket).await
            .expect("failed to connect to master");
        master_handshake(&mut master_stream, port).await;
        Some(master_stream)
    }  else {
        None
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await
        .expect(format!("Failed to bind to the port {port}").as_str());
    
    let is_slave = master_stream.is_some();
    let info = ServerInfo {
        is_slave,
        replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        replication_offset: 0,
    };
    let info = Arc::new(info);

    let storage = Arc::new(Storage::default());
    let replication_tx = if is_slave {
        None
    } else {
        let (tx, _) = channel(100);
        Some(tx)
    };
    
    if let Some(master_stream) = master_stream {
        let storage = Arc::clone(&storage);
        let info = Arc::clone(&info);
        tokio::spawn(async move {
            handle_connection(master_stream, storage, info, None, true).await
        });
    }

    loop {
        let (stream, _addr) = listener.accept().await
            .expect("Failed to accept connection");
        let storage = Arc::clone(&storage);
        let info = Arc::clone(&info);
        let replication_tx = replication_tx.clone();
        tokio::spawn(async move {
            handle_connection(stream, storage, info, replication_tx, false).await
        });
    }
}

async fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>, info: Arc<ServerInfo>, repl_transmitter: Option<Sender<Vec<Vec<u8>>>>, is_connection_with_master: bool) -> Option<()> {
    let mut repl_receiver = loop {
        /*
        None is returned if there were any errors in the message format.
        Because we are most likely not going to be able to recover from that.
        If the format is correct, but data is unexpected - we might be able to recover, and in that case we just skip the message.
        Format errors also include both read and write timeouts.
        Because that might mean that some data was lost.
         */
        let command = read_command(&mut BufReader::new(&mut stream)).await?;
        handle_command(&mut stream, &command, &storage, &info, is_connection_with_master).await?;
        /*
        We are using broadcast channels for replication, because we want each slave to have their own queue of messages,
        so that slow slaves do not affect fast slaves;
        Normal connections should have a transmitter side of the channel, while slave connections should have a receiver side of the channel.
        We can't detect that a connection is a slave until the handshake is completed,
        so we start as a normal channel with a transmitter, and then convert to a slave with a receiver.
        If the server is a slave, it does not have a replication transmitter.
         */
        if let Some(repl_transmitter) = &repl_transmitter {
            match command[0].as_slice() {
                b"PSYNC" => {
                    // ideally receiver should be created at the moment when the initial dump is created, so that all messages after the dump are queued  
                    break repl_transmitter.subscribe();
                },
                b"SET" => {
                    // if we want to ensure the order of the messages, we should send this while we are still holding the write lock
                    let _ = repl_transmitter.send(command);
                },
                _ => {},
            }
        }
    };
    drop(repl_transmitter);
    loop {
        let mut reader = BufReader::new(&mut stream);
        select! {
            command = read_command(&mut reader) => {
                let command = command?;
                handle_command(&mut stream, &command, &storage, &info, is_connection_with_master).await?;
            },
            replicated_command = repl_receiver.recv() => {
                match replicated_command {
                    Ok(command) => {
                        write_array_of_strings(&mut stream, command).await?;
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

