use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use crate::command::Command;
use crate::handlers::{handle_command, handle_command_ignore_invalid, psync};
use crate::resp::{read_command, write_command};
use crate::server::Server;

pub(crate) struct Connection {
    pub stream: BufReader<TcpStream>,
    pub server: Arc<Server>,
    pub kind: ConnectionKind,
}
impl Connection {
    pub fn can_replicate(&self) -> bool {
        self.replicated_offset_ref().is_some()
    }
    fn replicated_offset_ref(&self) -> Option<&Cell<usize>> {
        match &self.kind {
            ConnectionKind::ServerMasterConnectionExternal { replicated_offset } => Some(replicated_offset),
            ConnectionKind::ServerSlaveConnectionMaster { replicated_offset } => Some(replicated_offset),
            _ => None,
        }
    }
    pub fn replicate(&self, command: Command) {
        let offset_store = self.replicated_offset_ref()
            .expect(format!("we should not send anything to replication from connection kind {:?}", self.kind).as_str());
        let offset_value = self.server.replication.write().expect("got a poisoned lock, can't handle it")
            .send(command);
        offset_store.set(offset_value)
    }
    pub fn convert_to_slave(mut self) -> Self {
        assert!(
            self.is_external(),
            "trying to convert wrong connection to slave {:?}", self.kind
        );
        let slave_id = self.server.slave_state.write().expect("got poisoned lock")
            .connect();
        self.kind = ConnectionKind::ServerMasterConnectionSlave{slave_id};
        self
    }
    pub fn is_external(&self) -> bool {
        matches!(self.kind, ConnectionKind::ServerMasterConnectionExternal{..})
            || matches!(self.kind, ConnectionKind::ServerSlaveConnectionExternal)
    }
    pub fn update_acknowledged_offset(&mut self, offset: usize) -> bool {
        let slave_id = match self.kind {
            ConnectionKind::ServerMasterConnectionSlave { slave_id } => slave_id,
            _ => {
                eprintln!("received update offset command for a wrong kind of connection {:?}", self.kind);
                return false;
            },
        };
        let is_correct= self.server.slave_state.write().expect("got poisoned lock")
            .update_offset(slave_id, offset);
        if !is_correct {
            eprintln!("received offset that is smaller than a previously acknowledged one");
            return false;
        }
        true
    }
    pub fn check_acknowledged_replicas(&self) -> (usize, usize) {
        let offset_store = self.replicated_offset_ref()
            .expect(format!("can't check acknowledged replicas for connection kind {:?}", self.kind).as_str());
        let offset = offset_store.get();
        self.server.slave_state.read().expect("got poisoned lock")
            .check_acknowledged(offset)
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        match self.kind {
            ConnectionKind::ServerMasterConnectionSlave { slave_id } => {
                self.server.slave_state.write().expect("got poisoned lock")
                    .disconnect(slave_id);
            },
            _ => {},
        };
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionKind {
    ServerMasterConnectionExternal{replicated_offset: Cell<usize>},
    ServerMasterConnectionSlave{slave_id: usize},
    ServerSlaveConnectionMaster{replicated_offset: Cell<usize>},
    ServerSlaveConnectionExternal,
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
pub(crate) async fn handle_external(stream: TcpStream, server: Arc<Server>) -> Option<(Connection, Receiver<Command>)> {
    let kind = if server.is_slave {
        ConnectionKind::ServerSlaveConnectionExternal
    } else {
        ConnectionKind::ServerMasterConnectionExternal { replicated_offset: Default::default() }
    };
    let mut connection = Connection {
        stream: BufReader::new(stream),
        server,
        kind,
    };
    loop {
        let command_raw = read_command(&mut connection.stream).await?;
        let Some(command) = Command::new(command_raw) else {
            // todo: return error replies instead of just logging errors
            continue;
        };
        if command.name == "PSYNC" {
            if let Ok(rx) = psync(&mut connection, command).await {
                return Some((connection, rx));
            }
        } else {
            handle_command_ignore_invalid(&mut connection, command).await?;
        }
    };
}

pub(crate) async fn handle_slave(connection: Connection, mut repl_receiver: Receiver<Command>) -> Option<()> {
    let mut connection = connection.convert_to_slave();
    loop {
        select! {
            command = read_command(&mut connection.stream) => {
                let command_raw = command?;
                if let Some(command) = Command::new(command_raw) {
                    handle_command_ignore_invalid(&mut connection, command).await?;
                };
            },
            replicated_command = (&mut repl_receiver).recv() => {
                match replicated_command {
                    Ok(command) => {
                        write_command(&mut connection.stream, command).await?;
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

pub(crate) async fn handle_master(stream: BufReader<TcpStream>, server: Arc<Server>) -> Option<()> {
    let mut connection = Connection {
        stream,
        server,
        kind: ConnectionKind::ServerSlaveConnectionMaster{ replicated_offset: Default::default() },
    };
    loop {
        let command_raw = read_command(&mut connection.stream).await?;
        let Some(command) = Command::new(command_raw) else {
            // if we are unable to process master's command, we can't acknowledge that we've consumed the offset
            eprintln!("got a weird command from master, can't process it, shutting down the connection");
            continue;
        };
        let command_size = command.byte_size;
        let res = handle_command(&mut connection, command).await;
        if res.is_err() {
            // if we are unable to process master's command, we can't acknowledge that we've consumed the offset
            eprintln!("failed to process master's command, shutting down the connection");
            continue;
        }
        connection.server.slave_read_offset.fetch_add(command_size, Ordering::AcqRel);
    };
}