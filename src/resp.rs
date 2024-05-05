use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

const DELIMITER_STR: &str = "\r\n";
const DELIMITER_BYTES: &[u8] = DELIMITER_STR.as_bytes();

pub(crate) async fn read_command(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<Vec<Vec<u8>>> {
    let array_size = read_command_array_size(reader).await?;
    let mut command = Vec::with_capacity(array_size);
    for _ in 0..array_size {
        command.push(read_binary_string(reader).await?)
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
    if buf.len() < expected_type_prefix.len() + DELIMITER_STR.len() + 1 {
        eprintln!("invalid format, integer line is too small, len {}, val {buf}", buf.len());
        return None;
    }
    let (data_type, tail) = buf.split_at(expected_type_prefix.len());
    if data_type != expected_type_prefix {
        eprintln!("invalid format, expected type {expected_type_prefix}, got {data_type}");
        return None;
    }
    let (size_string, delimiter) = tail.split_at(tail.len() - DELIMITER_STR.len());
    if delimiter != DELIMITER_STR {
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
    let mut result = vec![0; expected_size + DELIMITER_BYTES.len()];
    let res = reader.read_exact(&mut result).await;
    if let Err(err) = res {
        eprintln!("failed to read string line {err}");
        return None;
    }
    if !result.ends_with(DELIMITER_BYTES) {
        eprintln!("invalid format, string is missing the delimiter");
        return None;
    }
    result.truncate(result.len() - DELIMITER_BYTES.len());
    Some(result)
}

pub(crate) async fn write_simple_string(stream: &mut (impl AsyncWriteExt + Unpin), string: impl AsRef<str>) -> Option<()> {
    let string = string.as_ref();
    let result = stream.write_all(format!("+{string}{DELIMITER_STR}").as_bytes()).await;
    if let Err(error) = result {
        eprintln!("failed to write simple string: {error}");
        return None;
    }
    Some(())
}

pub(crate) async fn write_binary_string_or_null(stream: &mut (impl AsyncWriteExt + Unpin), string: Option<impl AsRef<[u8]>>) -> Option<()> {
    match string {
        Some(value) => write_binary_string(stream, &value).await,
        None => write_null(stream).await,
    }
}

pub(crate) async fn write_binary_string(stream: &mut (impl AsyncWriteExt + Unpin), string: impl AsRef<[u8]>) -> Option<()> {
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
    let result = stream.write_all(DELIMITER_BYTES).await;
    if let Err(error) = result {
        eprintln!("failed to write binary string delimiter: {error}");
        return None;
    }
    Some(())
}

pub(crate) async fn write_null(stream: &mut (impl AsyncWriteExt + Unpin)) -> Option<()> {
    let result = stream.write_all(format!("$-1{DELIMITER_STR}").as_bytes()).await;
    if let Err(error) = result {
        eprintln!("failed to write simple string: {error}");
        return None;
    }
    Some(())
}

pub(crate) async fn write_array_of_strings<S: AsRef<[u8]>>(stream: &mut (impl AsyncWriteExt + Unpin), strings: impl AsRef<[S]>) -> Option<()> {
    let strings = strings.as_ref();
    let result = stream.write_all(format!("*{}{DELIMITER_STR}", strings.len()).as_bytes()).await;
    if let Err(error) = result {
        eprintln!("failed to write array size: {error}");
        return None;
    }
    for string in strings {
        write_binary_string(stream, string).await?;
    }
    Some(())
}
