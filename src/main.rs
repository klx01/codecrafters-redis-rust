use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

async fn handle_connection(mut stream: TcpStream, addr: SocketAddr) {
    println!("accepted connection from {addr}");
    let mut buf= [0u8; 512];
    loop {
        let read_size = match stream.read(&mut buf).await {
            Ok(x) => x,
            Err(err) => {
                eprintln!("failed to read data from stream for {addr}: {err}");
                break;
            }
        };
        if read_size == 0 {
            println!("EOF for {addr}");
            break;
        }
        let read_data = std::str::from_utf8(&buf[..read_size]);
        println!("message from {addr}: {read_data:?}");

        let result = stream.write_all(b"+PONG\r\n").await;
        if let Err(error) = result {
            eprintln!("failed to write response: {error}");
            break;
        }
    }

}
