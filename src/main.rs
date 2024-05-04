use std::net::SocketAddr;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use crate::resp::*;

mod resp;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await
        .expect("Failed to bind to the port");

    loop {
        let (stream, addr) = listener.accept().await
            .expect("Failed to accept connection");
        tokio::spawn(async move {
            handle_connection(stream, addr).await
        });
    }
}

async fn handle_connection(mut stream: TcpStream, _addr: SocketAddr) -> Option<()> {
    loop {
        let command = read_command(&mut BufReader::new(&mut stream)).await?;
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
        let can_continue = match command_name.as_str() {
            "PING" => handle_ping(&mut stream).await,
            "ECHO" => handle_echo(&mut stream, command_params).await,
            _ => {
                eprintln!("received unknown command {command_name}");
                true
            },
        };
        if !can_continue {
            return None;
        }
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

