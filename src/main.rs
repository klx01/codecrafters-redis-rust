use std::io::Write;
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .expect("Failed to bind to the port");

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(x) => x,
            Err(error) => {
                eprintln!("error in incoming stream: {error}");
                continue;
            }
        };

        let result = stream.write_all(b"+PONG\r\n");
        if let Err(error) = result {
            eprintln!("failed to write response: {error}");
        }
    }
}
