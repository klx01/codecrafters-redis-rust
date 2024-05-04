use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
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

async fn handle_connection(mut stream: TcpStream, _addr: SocketAddr) -> Option<()> {
    loop {
        let command = read_command(&mut stream).await?;
        if command.len() == 0 {
            eprintln!("received a command of size 0");
            return None;
        }
        let (command_name, command_params) = command.split_first().unwrap();
        let command_name = match std::str::from_utf8(command_name) {
            Ok(x) => x,
            Err(error) => {
                eprintln!("failed to parse the received command name: {error}");
                return None;
            }
        };
        let command_name = command_name.to_ascii_uppercase();
        match command_name.as_str() {
            "PING" => handle_ping(&mut stream).await?,
            _ => {
                eprintln!("received unknown command {command_name}");
            },
        }
    }
}

async fn read_command(stream: &mut TcpStream) -> Option<Vec<Vec<u8>>> {
    let mut buf_reader = BufReader::new(stream);
    let array_size = read_command_array_size(&mut buf_reader).await?;
    let mut command = Vec::with_capacity(array_size);
    for _ in 0..array_size {
        command.push(read_binary_string(&mut buf_reader).await?)
    }
    Some(command)
}

async fn read_command_array_size(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<usize> {
    return read_int(reader, "*", true, 100).await
}

async fn read_int(reader: &mut (impl AsyncBufReadExt + Unpin), expected_type_prefix: &'static str, is_eof_expected: bool, max: usize) -> Option<usize> {
    let mut buf = String::new();
    let res = reader.take(10).read_line(&mut buf).await;
    if let Err(err) = res {
        eprintln!("failed to read integer line {err}");
        return None;
    }
    if buf.len() == 0 {
        if !is_eof_expected {
            eprintln!("unexpected end of file when reading integer");
        }
        // EOF
        return None;
    }
    if buf.len() < 4 {
        eprintln!("invalid format, integer line is too small, len {}, val {buf}", buf.len());
        return None;
    }
    let (data_type, tail) = buf.split_at(1);
    if data_type != expected_type_prefix {
        eprintln!("invalid format, expected type {expected_type_prefix}, got {data_type}");
        return None;
    }
    let (size_string, delimiter) = tail.split_at(tail.len() - 2);
    if delimiter != "\r\n" {
        eprintln!("invalid format, integer string is too large {buf}");
        return None;
    }
    let int = match size_string.parse() {
        Ok(x) => x,
        Err(err) => {
            eprintln!("failed to parse integer from {size_string}: {err}");
            return None;
        }
    };
    if int > max {
        eprintln!("integer {int} larger than max allowed {max}");
        return None;
    }
    Some(int)
}

async fn read_binary_string(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<Vec<u8>> {
    let size = read_binary_string_size(reader).await?;
    read_binary_string_body(reader, size).await
}

async fn read_binary_string_size(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<usize> {
    return read_int(reader, "$", true, 300).await
}

async fn read_binary_string_body(reader: &mut (impl AsyncBufReadExt + Unpin), expected_size: usize) -> Option<Vec<u8>> {
    const DELIMITER: &[u8] = b"\r\n";
    let mut result = vec![0; expected_size + DELIMITER.len()];
    let res = reader.read_exact(&mut result).await;
    if let Err(err) = res {
        eprintln!("failed to read string line {err}");
        return None;
    }
    if !result.ends_with(DELIMITER) {
        eprintln!("invalid format, string is missing the delimiter");
        return None;
    }
    result.truncate(result.len() - DELIMITER.len());
    Some(result)
}

async fn handle_ping(stream: &mut TcpStream) -> Option<()> {
    let result = stream.write_all(b"+PONG\r\n").await;
    if let Err(error) = result {
        eprintln!("failed to write response: {error}");
        return None;
    }
    Some(())
}
