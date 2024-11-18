use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use crate::command::{Command, CommandRaw};

const DELIMITER_STR: &str = "\r\n";
const DELIMITER_BYTES: &[u8] = DELIMITER_STR.as_bytes();

pub(crate) async fn read_command(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<CommandRaw> {
    /*
    Only the very first read does not have a timeout.
    Because we are reading only a very small amounts of data,
    and because pauses between messages are expected.
    Later reads have a timeout, to protect against clients that would make us allocate memory and hold it
    and because pauses there are not expected.
     */
    let (array_size, mut read_bytes) = read_command_array_size(reader).await?;
    exec_with_timeout(async move {
        let mut command = Vec::with_capacity(array_size);
        for _ in 0..array_size {
            let (param, param_bytes) = read_binary_string_with_size(reader, true).await?;
            read_bytes += param_bytes;
            command.push(param)
        }
        Some((command, read_bytes))
    }).await
}

async fn read_command_array_size(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<(usize, usize)> {
    return read_int(reader, "*", true, 100).await
}

async fn read_int(reader: &mut (impl AsyncBufReadExt + Unpin), expected_type_prefix: &'static str, is_eof_expected: bool, max: usize) -> Option<(usize, usize)> {
    let mut buf = String::new();
    let res = reader.take(10).read_line(&mut buf).await;
    if let Err(err) = res {
        eprintln!("failed to read integer line {err}");
        return None;
    }
    let read_bytes = buf.len();
    if read_bytes == 0 {
        if !is_eof_expected {
            eprintln!("unexpected end of file when reading integer");
        }
        // EOF
        return None;
    }
    let Some(buf) = buf.strip_prefix(expected_type_prefix) else {
        eprintln!("invalid format, did not find prefix {expected_type_prefix} in {buf}");
        return None;
    };
    let Some(buf) = buf.strip_suffix(DELIMITER_STR) else {
        eprintln!("invalid format, delimiter is missing or string is too large {buf}");
        return None;
    };
    let int = match buf.parse() {
        Ok(x) => x,
        Err(err) => {
            eprintln!("failed to parse integer from {buf}: {err}");
            return None;
        }
    };
    if int > max {
        eprintln!("integer {int} larger than max allowed {max}");
        return None;
    }
    Some((int, read_bytes))
}

pub(crate) async fn read_binary_string(reader: &mut (impl AsyncBufReadExt + Unpin), with_delimiter: bool) -> Option<Vec<u8>> {
    exec_with_timeout(
        read_binary_string_with_size(reader, with_delimiter)
    ).await.map(|x| x.0)
}

async fn read_binary_string_with_size(reader: &mut (impl AsyncBufReadExt + Unpin), with_delimiter: bool) -> Option<(Vec<u8>, usize)> {
    let (size, size_bytes) = read_binary_string_size(reader).await?;
    let (string, string_bytes) = read_binary_string_body(reader, size, with_delimiter).await?;
    Some((string, size_bytes + string_bytes))
}

async fn read_binary_string_size(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<(usize, usize)> {
    return read_int(reader, "$", false, 300).await
}

async fn read_binary_string_body(reader: &mut (impl AsyncBufReadExt + Unpin), expected_size: usize, with_delimiter: bool) -> Option<(Vec<u8>, usize)> {
    let mut buffer_size = expected_size;
    if with_delimiter {
        buffer_size += DELIMITER_BYTES.len();
    }
    let mut result = vec![0; buffer_size];
    let res = reader.read_exact(&mut result).await;
    if let Err(err) = res {
        eprintln!("failed to read string line {err}");
        return None;
    }
    if with_delimiter {
        if !result.ends_with(DELIMITER_BYTES) {
            eprintln!("invalid format, string is missing the delimiter");
            return None;
        }
        result.truncate(result.len() - DELIMITER_BYTES.len());
    }
    Some((result, buffer_size))
}

pub(crate) async fn read_simple_string(reader: &mut (impl AsyncBufReadExt + Unpin), max_size: u64) -> Option<String> {
    exec_with_timeout(async move {
        let mut buf = String::new();
        let result = reader.take(max_size).read_line(&mut buf).await;
        if let Err(error) = result {
            eprintln!("Failed to read simple string {error}");
            return None;
        }
        if buf.len() == 0 {
            eprintln!("Got EOF when reading a simple string");
            return None;
        }
        let Some(buf) = buf.strip_prefix('+') else {
            eprintln!("Missing the simple string prefix");
            return None;
        };
        let Some(buf) = buf.strip_suffix(DELIMITER_STR) else {
            eprintln!("Missing the delimiter for simple string");
            return None;
        };
        Some(buf.to_string())
    }).await
}

pub(crate) async fn write_simple_string(stream: &mut (impl AsyncWriteExt + Unpin), string: impl AsRef<str>) -> Option<()> {
    exec_with_timeout(async move {
        let string = string.as_ref();
        let result = stream.write_all(format!("+{string}{DELIMITER_STR}").as_bytes()).await;
        if let Err(error) = result {
            eprintln!("failed to write simple string: {error}");
            return None;
        }
        Some(())
    }).await
}

#[allow(dead_code)]
pub(crate) async fn write_binary_string_or_null(stream: &mut (impl AsyncWriteExt + Unpin), string: Option<impl AsRef<[u8]>>) -> Option<()> {
    match string {
        Some(value) => write_binary_string(stream, &value, true).await,
        None => write_null(stream).await,
    }
}

pub(crate) async fn write_binary_string(stream: &mut (impl AsyncWriteExt + Unpin), string: impl AsRef<[u8]>, with_delimiter: bool) -> Option<()> {
    exec_with_timeout(async move {
        let string = string.as_ref();
        let result = stream.write_all(format!("${}{DELIMITER_STR}", string.len()).as_bytes()).await;
        if let Err(error) = result {
            eprintln!("failed to write binary string size: {error}");
            return None;
        }
        let result = stream.write_all(string).await;
        if let Err(error) = result {
            eprintln!("failed to write binary string body: {error}");
            return None;
        }
        if with_delimiter {
            let result = stream.write_all(DELIMITER_BYTES).await;
            if let Err(error) = result {
                eprintln!("failed to write binary string delimiter: {error}");
                return None;
            }
        }
        Some(())
    }).await
}

pub(crate) async fn write_null(stream: &mut (impl AsyncWriteExt + Unpin)) -> Option<()> {
    exec_with_timeout(async move {
        let result = stream.write_all(format!("$-1{DELIMITER_STR}").as_bytes()).await;
        if let Err(error) = result {
            eprintln!("failed to write simple string: {error}");
            return None;
        }
        Some(())
    }).await
}

pub(crate) async fn write_int(stream: &mut (impl AsyncWriteExt + Unpin), value: i64) -> Option<()> {
    exec_with_timeout(async move {
        let result = stream.write_all(format!(":{value}{DELIMITER_STR}").as_bytes()).await;
        if let Err(error) = result {
            eprintln!("failed to write integer: {error}");
            return None;
        }
        Some(())
    }).await
}

pub(crate) async fn write_array_of_strings<S: AsRef<[u8]>>(stream: &mut (impl AsyncWriteExt + Unpin), strings: impl AsRef<[S]>) -> Option<()> {
    exec_with_timeout(async move {
        let strings = strings.as_ref();
        let result = stream.write_all(format!("*{}{DELIMITER_STR}", strings.len()).as_bytes()).await;
        if let Err(error) = result {
            eprintln!("failed to write array size: {error}");
            return None;
        }
        for string in strings {
            write_binary_string(stream, string, true).await?;
        }
        Some(())
    }).await
}

pub(crate) async fn write_command(stream: &mut (impl AsyncWriteExt + Unpin), command: Command) -> Option<()> {
    write_array_of_strings(stream, command.raw).await
}

async fn exec_with_timeout<R>(future: impl Future<Output = Option<R>>) -> Option<R> {
    let res = timeout(Duration::from_millis(1000), future).await;
    match res {
        Ok(x) => x,
        Err(_) => {
            eprintln!("operation timed out");
            return None;
        }
    }
}
