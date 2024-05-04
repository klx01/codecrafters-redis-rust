use std::io::{BufRead, BufReader, Write};
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

        loop {
            let mut buf_read = BufReader::new(&stream);
            let mut read_data = vec![];
            let line = buf_read.read_until('\n' as u8, &mut read_data);
            let read_size = match line {
                Ok(x) => x,
                Err(error) => {
                    eprintln!("failed to read data from stream: {error}");
                    break;
                }
            };
            if read_size == 0 {
                break;
            }
            let result = stream.write_all(b"+PONG\r\n");
            if let Err(error) = result {
                eprintln!("failed to write response: {error}");
                break;
            }
        }
    }
}
