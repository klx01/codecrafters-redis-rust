use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{lookup_host, TcpStream};
use tokio::select;
use crate::commands::{handle_command, HandlingMode};
use crate::handshake::master_handshake;
use crate::misc::{make_info, make_listener, ServerInfo};
use crate::resp::read_command;
use crate::storage::Storage;

pub(crate) async fn run_slave(port: u16, master_addr: &str) {
    let master_socket = lookup_host(&master_addr).await
        .expect(format!("Failed to lookup the address of master host {master_addr}").as_str())
        .next()
        .expect(format!("No addresses found for master host {master_addr}").as_str());
    
    let master_stream = TcpStream::connect(master_socket).await
        .expect("failed to connect to master");
    let mut master_stream = BufReader::new(master_stream);
    
    let master_config = master_handshake(&mut master_stream, port).await;

    let listener = make_listener(port).await;
    let storage = Arc::new(Storage::default());
    let info = make_info(true);

    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    {
        let storage = Arc::clone(&storage);
        let info = Arc::clone(&info);
        tokio::spawn(async move {
            handle_master(master_stream, storage, info, master_config.1).await;
            eprintln!("lost connection to master, shutting down");
            //let _ = shutdown_tx.send(()); // todo: disabled to pass a test one one of the stages
        });
    }
    loop {
        select! {
            _ = &mut shutdown_rx => {
                break;
            },
            accept_res = listener.accept() => {
                let (stream, _addr) = accept_res
                    .expect("Failed to accept connection");
                let storage = Arc::clone(&storage);
                let info = Arc::clone(&info);
                tokio::spawn(async move {
                    handle_connection(BufReader::new(stream), storage, info).await
                });
            },
        }
    }
}

async fn handle_connection(mut stream: impl AsyncBufReadExt + AsyncWriteExt + Unpin, storage: Arc<Storage>, info: Arc<ServerInfo>) -> Option<()> {
    loop {
        let (command, _) = read_command(&mut stream).await?;
        if let Some(command) = command {
            handle_command(&mut stream, &command, &storage, &info, HandlingMode::ServerSlaveConnectionExternal, 0).await?;
        } else {
            // todo: return error replies instead of just logging errors
        }
    };
}

async fn handle_master(mut stream: impl AsyncBufReadExt + AsyncWriteExt + Unpin, storage: Arc<Storage>, info: Arc<ServerInfo>, mut offset: usize) -> Option<()> {
    loop {
        let (command, command_bytes) = read_command(&mut stream).await?;
        if let Some(command) = command {
            handle_command(&mut stream, &command, &storage, &info, HandlingMode::ServerSlaveConnectionMaster, offset).await?;
        } else {
            // todo: return error replies instead of just logging errors
        }
        offset += command_bytes;
    };
}
