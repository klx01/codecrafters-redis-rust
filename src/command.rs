pub(crate) type CommandRaw = (Vec<Vec<u8>>, usize);

#[derive(Clone, Debug)]
pub(crate) struct Command {
    pub byte_size: usize,
    pub name: String,
    pub raw: Vec<Vec<u8>>,
}
impl Command {
    pub fn new((raw, byte_size): CommandRaw) -> Option<Self> {
        if raw.len() == 0 {
            eprintln!("received a command of size 0");
            return None;
        }
        let name = normalize_name(&raw[0])?;
        let res = Command{ byte_size, name, raw };
        Some(res)
    }
    pub fn get_args(&self) -> &[Vec<u8>] {
        &self.raw[1..]
    }
}

pub(crate) fn normalize_name(name: &[u8]) -> Option<String> {
    let mut name = match String::from_utf8(name.to_vec()) {
        Ok(x) => x,
        Err(error) => {
            eprintln!("failed to parse the received command name: {error}");
            return None;
        }
    };
    name.make_ascii_uppercase();
    Some(name)
}
