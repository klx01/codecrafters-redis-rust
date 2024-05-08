use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use crate::resp::*;

pub(crate) async fn master_handshake(stream: &mut (impl AsyncBufReadExt + AsyncWriteExt + Unpin), my_port: u16) -> (String, usize) {
    let buf = &mut [0u8; 512];
    write(stream, ["PING"]).await;
    read_expect(stream, buf, "+PONG\r\n").await;
    write(stream, ["REPLCONF", "listening-port", my_port.to_string().as_str()]).await;
    read_expect(stream, buf, "+OK\r\n").await;
    write(stream, ["REPLCONF", "capa", "psync2"]).await;
    read_expect(stream, buf, "+OK\r\n").await;
    write(stream, ["PSYNC", "?", "-1"]).await;
    let master_config = read_simple_string(stream, 100).await
        .expect("failed to get config from master");
    let result = parse_master_config(&master_config);
    read_binary_string(stream, false).await
        .expect("failed to get file from master");
    result
}

async fn write<S: AsRef<[u8]>>(stream: &mut (impl AsyncWriteExt + Unpin), message: impl AsRef<[S]>) {
    write_array_of_strings(stream, message)
        .await
        .expect("failed to write message during handshake with master");
}

async fn read<'a>(stream: &mut (impl AsyncBufReadExt + Unpin), buf: &'a mut [u8]) -> &'a [u8] {
    let read_size = timeout(
        Duration::from_millis(1000), 
        stream.read(buf)
    )
        .await
        .expect("timeout when reading during handshake")
        .expect("failed to read message during handshake");
    if read_size == 0 {
        panic!("got EOF from master during handshake");
    }
    &buf[..read_size]
}

async fn read_expect(stream: &mut (impl AsyncBufReadExt + Unpin), buf: &mut [u8], expected: &str) {
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
