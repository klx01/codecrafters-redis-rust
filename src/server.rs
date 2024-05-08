use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;
use tokio::io::BufReader;
use tokio::net::{lookup_host, TcpListener, TcpStream};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use crate::command::Command;
use crate::connection::{handle_external, handle_master, handle_slave};
use crate::handshake::master_handshake;
use crate::storage::Storage;

pub(crate) struct Server {
    pub is_slave: bool,
    pub replication_id: String,
    pub slave_read_offset: AtomicUsize,
    pub storage: Storage,
    pub replication: RwLock<Replication>,
    pub slave_state: RwLock<SlaveState>,
}
impl Server {
    fn new(master_config: Option<(String, usize)>) -> Self {
        let (repl_tx, _) = channel(100);
        let (is_slave, replication_id, offset) = match master_config {
            Some(x) => (true, x.0, x.1),
            None => (false, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), 0),
        };
        Self {
            is_slave,
            replication_id,
            slave_read_offset: offset.into(),
            storage: Default::default(),
            replication: RwLock::new(Replication {
                sender: repl_tx,
                master_written_offset: 0,
            }),
            slave_state: Default::default(),
        }
    }
    fn new_arc(master_config: Option<(String, usize)>) -> Arc<Self> {
        Arc::new(Self::new(master_config))
    }
}

/*
We need to know which offset corresponds to which command.
This is needed for 2 things:
when replicas connect, they can tell which offset they are on, and master should be able to start replicating from that offset (not a part of this challenge)
and when replicas acknowledge some offset, we need to know if this includes a specific command or not.
The simplest way to implement this is to ensure that all commands are replicated to all slaves in exactly the same order, and each command has a known offset.
Which requires a global lock.
Alternatively we could try keeping a separate replication log for each replica, but getting an offset for a specific command would be way too complicated.
 */
pub(crate) struct Replication {
    sender: Sender<Command>,
    master_written_offset: usize,
}
impl Replication {
    pub fn send(&mut self, command: Command) -> usize {
        self.master_written_offset += command.byte_size;
        let _ = self.sender.send(command);
        self.master_written_offset
    }
    pub fn subscribe(&self) -> Receiver<Command> {
        self.sender.subscribe()
    }
}

#[derive(Default)]
pub(crate) struct SlaveState {
    offsets: HashMap<usize, usize>,
    next_id: usize,
}
impl SlaveState {
    pub fn connect(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.offsets.insert(id, 0);
        id
    }
    pub fn update_offset(&mut self, id: usize, offset: usize) -> bool {
        let old_value = self.offsets.get(&id)
            .expect("update should only happen for valid ids");
        if *old_value > offset {
            return false;
        }
        self.offsets.insert(id, offset);
        return true;
    }
    pub fn disconnect(&mut self, id: usize) {
        self.offsets.remove(&id);
    }
    pub fn check_acknowledged(&self, offset: usize) -> (usize, usize) {
        let count_acknowledged = self.offsets
            .iter()
            .filter(|(_, &v)| {
                v >= offset
            })
            .count();
        (count_acknowledged, self.offsets.len() - count_acknowledged)
    }
}

pub(crate) async fn run_master(port: u16) {
    serve_external_connections(port, Server::new_arc(None)).await
}

pub(crate) async fn run_slave(port: u16, master_addr: &str) {
    let master_socket = lookup_host(&master_addr).await
        .expect(format!("Failed to lookup the address of master host {master_addr}").as_str())
        .next()
        .expect(format!("No addresses found for master host {master_addr}").as_str());

    let master_stream = TcpStream::connect(master_socket).await
        .expect("failed to connect to master");
    let mut master_stream = BufReader::new(master_stream);
    let master_config = master_handshake(&mut master_stream, port).await;

    let server = Server::new_arc(Some(master_config));

    {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            handle_master(master_stream, server).await;
            eprintln!("lost connection to master!");
        });
    }

    serve_external_connections(port, server).await
}

async fn serve_external_connections(port: u16, server: Arc<Server>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await
        .expect(format!("Failed to bind to the port {port}").as_str());
    loop {
        let (stream, _addr) = listener.accept().await
            .expect("Failed to accept connection");
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let (connection, repl_rx) = handle_external(stream, server).await?;
            handle_slave(connection, repl_rx).await
        });
    }
}
