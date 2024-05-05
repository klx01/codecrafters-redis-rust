use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use crate::resp::{read_simple_string, write_array_of_strings};

pub(crate) async fn master_handshake(stream: &mut TcpStream, my_port: u16) -> (String, usize) {
    let buf = &mut [0u8; 512];
    write(stream, ["PING"]).await;
    read_expect(stream, buf, "+PONG\r\n").await;
    write(stream, ["REPLCONF", "listening-port", my_port.to_string().as_str()]).await;
    read_expect(stream, buf, "+OK\r\n").await;
    write(stream, ["REPLCONF", "capa", "psync2"]).await;
    read_expect(stream, buf, "+OK\r\n").await;
    write(stream, ["PSYNC", "?", "-1"]).await;
    let master_config = exec_with_timeout(read_simple_string(&mut BufReader::new(stream), 100))
        .await
        .expect("timeout when reading during handshake")
        .unwrap();
    parse_master_config(&master_config)
}

async fn write<S: AsRef<[u8]>>(stream: &mut TcpStream, message: impl AsRef<[S]>) {
    exec_with_timeout(write_array_of_strings(stream, message))
        .await
        .expect("timeout when writing during handshake with master")
        .expect("failed to write message during handshake with master");
}

async fn read<'a>(stream: &mut TcpStream, buf: &'a mut [u8]) -> &'a [u8] {
    let read_size = exec_with_timeout(stream.read(buf))
        .await
        .expect("timeout when reading during handshake")
        .expect("failed to read message during handshake");
    if read_size == 0 {
        panic!("got EOF from master during handshake");
    }
    &buf[..read_size]
}

async fn read_expect(stream: &mut TcpStream, buf: &mut [u8], expected: &str) {
    let response = read(stream, buf).await;
    if response != expected.as_bytes() {
        panic!(
            "unexpected response from master: expected {expected}, got {:?}",
            std::str::from_utf8(response)
        );
    }
}

fn parse_master_config(buf: &str) -> (String, usize) {
    let buf = buf.strip_prefix("FULLRESYNC ")
        .expect("Missing prefix in master config response");
    let (id, offset) = buf.split_once(' ')
        .expect("Failed to split the master config response");
    assert_eq!(id.len(), 40, "Invalid length of master id");
    let offset = offset.parse()
        .expect("Failed to parse master offset");
    (id.to_string(), offset)
}

async fn exec_with_timeout<R>(future: impl Future<Output = R>) -> Result<R, Elapsed> {
    timeout(Duration::from_millis(1000), future).await
}