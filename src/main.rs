use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
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
        let command = timeout(
            Duration::from_millis(1000),
            read_command(&mut BufReader::new(&mut stream))
        ).await;
        let command = match command {
            Ok(x) => x?,
            Err(_) => {
                eprintln!("read timed out");
                return None;
            }
        };
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

        let can_continue = timeout(
            Duration::from_millis(1000),
            handle_command(&mut stream, &command_name, command_params)
        ).await;
        match can_continue {
            Ok(true) => {},
            Ok(false) => return None,
            Err(_) => {
                eprintln!("response timed out");
                return None;
            }
        }
    }
}

async fn handle_command(mut stream: &mut TcpStream, command_name: &str, command_params: &[Vec<u8>]) -> bool {
    match command_name {
        "PING" => handle_ping(&mut stream).await,
        "ECHO" => handle_echo(&mut stream, command_params).await,
        _ => {
            eprintln!("received unknown command {command_name}");
            true
        },
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

