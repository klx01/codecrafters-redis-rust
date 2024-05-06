use std::sync::Arc;
use tokio::net::TcpListener;

pub(crate) struct ServerInfo {
    pub is_slave: bool,
    pub replication_id: String,
    pub replication_offset: usize,
}

pub(crate) async fn make_listener(port: u16) -> TcpListener {
    TcpListener::bind(format!("127.0.0.1:{port}")).await
        .expect(format!("Failed to bind to the port {port}").as_str())
}

pub(crate) fn make_info(is_slave: bool) -> Arc<ServerInfo> {
    let info = ServerInfo {
        is_slave,
        replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        replication_offset: 0,
    };
    let info = Arc::new(info);
    info
}
