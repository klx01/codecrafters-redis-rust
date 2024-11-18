use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use nom::branch::alt;
use nom::bytes::complete::{tag, take};
use nom::combinator::opt;
use nom::error::{ErrorKind, make_error, VerboseError};
use nom::{IResult, Parser};
use nom::multi::{many0, many_till};
use nom::number::complete::{le_i16, le_i32, le_i8, le_u8, le_u32, le_u64};
use nom::sequence::Tuple;
use crate::storage::{ExpiryTs, StorageInner, StorageItem, StorageItemString, StorageKey};

const STRING_CONTROL_BITMASK: u8 = 0b11000000;

type FileParseError<I> = VerboseError<I>;
type FileParseResult<I, O> = IResult<I, O, FileParseError<I>>;

pub(crate) fn load_file(path: &PathBuf) -> Option<StorageInner> {
    let mut file = match File::open(path) {
        Ok(x) => x,
        Err(err) => {
            eprintln!("Failed to open file {path:?} {err}");
            return None;
        }
    };
    let mut contents = Vec::new();
    if let Err(err) = file.read_to_end(&mut contents) {
        eprintln!("Failed to read file {path:?} {err}");
        return None;
    }

    let (data, databases) = match parse_file(&contents) {
        Ok(x) => x,
        Err(err) => {
            eprintln!("Failed to parse file {path:?} {err}");
            return None;
        }
    };
    if data.len() > 0 {
        eprintln!("some data is remaining after end {}", data.len());
        return None;
    }
    if databases.len() == 0 {
        eprintln!("no databases found in the file");
        return None;
    };
    if databases.len() > 1 {
        eprintln!("found more than 1 database in the file, using only the first one");
    }
    Some(databases[0].clone())
}

fn parse_file(data: &[u8]) -> FileParseResult<&[u8], Vec<StorageInner>> {
    let (data, (_, _, _, (databases, _), _, _)) = (
        tag(b"REDIS"),
        take(4usize), // version
        many0(auxiliary),
        many_till(database, tag([0xFF])),
        le_u64, // checksum
        opt(take(1usize)), // for some reason codecrafters' file has one extra byte in the end
    ).parse(data)?;
    Ok((data, databases))
}

fn auxiliary(tail: &[u8]) -> FileParseResult<&[u8], (Vec<u8>, Vec<u8>)> {
    let (tail, (_, key, value)) = (
        tag([0xFA]),
        length_encoded_string,
        length_encoded_string,
    ).parse(tail)?;
    Ok((tail, (key, value)))
}

fn database(tail: &[u8]) -> FileParseResult<&[u8], StorageInner> {
    let (tail, (database_num, _, pairs)) = (
        db_selector,
        db_size,
        many0(key_value),
    ).parse(tail)?;
    let mut storage = StorageInner::default();
    for (key, item) in pairs {
        let existing = storage.insert(key, item);
        if existing.is_some() {
            eprintln!("duplicate key found in database {database_num:?}");
            return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
        }
    }
    Ok((tail, storage))
}

fn db_selector(tail: &[u8]) -> FileParseResult<&[u8], i32> {
    let (tail, (_, db_number)) = (
        tag([0xFE]),
        length_encoded_int, // database number
    ).parse(tail)?;
    Ok((tail, db_number))
}

fn db_size(tail: &[u8]) -> FileParseResult<&[u8], (i32, i32)> {
    let (tail, (_, size, expiry_size)) = (
        tag([0xFB]),
        length_encoded_int, // Database hash table size
        length_encoded_int, // Expiry hash table size
    ).parse(tail)?;
    Ok((tail, (size, expiry_size)))
}

fn key_value(tail: &[u8]) -> FileParseResult<&[u8], (StorageKey, StorageItem)> {
    let (tail, (expires_at, kind, key)) = (
       opt(expiry),
       value_kind,
       length_encoded_string,
    ).parse(tail)?;
    let (tail, item) = value(tail, kind, expires_at)?;
    Ok((tail, (key, item)))
}

fn expiry(tail: &[u8]) -> FileParseResult<&[u8], ExpiryTs> {
    let res = alt((
        expiry_sec,
        expiry_milli,
    )).parse(tail)?;
    Ok(res)
}

fn expiry_sec(tail: &[u8]) -> FileParseResult<&[u8], ExpiryTs> {
    (tag([0xFD]), le_u32).parse(tail)
        .map(|(tail, (_, val))| (tail, val as ExpiryTs * 1000))
}

fn expiry_milli(tail: &[u8]) -> FileParseResult<&[u8], ExpiryTs> {
    (tag([0xFC]), le_u64).parse(tail)
        .map(|(tail, (_, val))| (tail, val as ExpiryTs))
}

#[derive(Copy, Clone, Debug)]
enum ValueKind {
    String = 0,
    List = 1,
    Set = 2,
    SortedSet = 3,
    Hash = 4,
    ZipMap = 9,
    ZipList = 10,
    IntSet = 11,
    SortedSetZipList = 12,
    HashMapZipList = 13,
    QuickList = 14
}
impl TryFrom<u8> for ValueKind {
    type Error = ();
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let res = match value {
            x if x == Self::String as u8 => Self::String,
            x if x == Self::List as u8 => Self::List,
            x if x == Self::Set as u8 => Self::Set,
            x if x == Self::SortedSet as u8 => Self::SortedSet,
            x if x == Self::Hash as u8 => Self::Hash,
            x if x == Self::ZipMap as u8 => Self::ZipMap,
            x if x == Self::ZipList as u8 => Self::ZipList,
            x if x == Self::IntSet as u8 => Self::IntSet,
            x if x == Self::SortedSetZipList as u8 => Self::SortedSetZipList,
            x if x == Self::HashMapZipList as u8 => Self::HashMapZipList,
            x if x == Self::QuickList as u8 => Self::QuickList,
            _ => return Err(()),
        };
        Ok(res)
    }
}

fn value_kind(tail: &[u8]) -> FileParseResult<&[u8], ValueKind> {
    let (tail, kind) = le_u8(tail)?;
    let Ok(kind) = kind.try_into() else {
        if (kind != 0xFF) && (kind != 0xFE) { // this can happen after we parse the last key-value, and the next section begins
            eprintln!("unexpected value kind {kind}");
        }
        return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
    };
    Ok((tail, kind))
}

fn value(tail: &[u8], kind: ValueKind, expires_at: Option<ExpiryTs>) -> FileParseResult<&[u8], StorageItem> {
    match kind {
        ValueKind::String => length_encoded_string(tail)
            .map(|(tail, value)| (tail, StorageItem::String(StorageItemString{value, expires_at}))),
        _ => {
            eprintln!("parsing value kind {kind:?} is not implemented yet");
            return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
        },
    }
}

fn length_encoded_string(tail: &[u8]) -> FileParseResult<&[u8], Vec<u8>> {
    let (tail, (kind, value)) = length_encoding_control(tail)?;
    let res = match kind {
        0b11 => string_special(tail, value),
        _ => string_normal(tail, kind, value),
    }?;
    Ok(res)
}

fn length_encoded_int(tail: &[u8]) -> FileParseResult<&[u8], i32> {
    let (tail, (kind, value)) = length_encoding_control(tail)?;
    match kind {
        0b00 => Ok((tail, value.into())),
        0b11 => integer(tail, value),
        _ => {
            eprintln!("unexpected kind value for integer {kind}");
            return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
        }
    }
}

fn length_encoding_control(tail: &[u8]) -> FileParseResult<&[u8], (u8, u8)> {
    let (tail, first) = le_u8(tail)?;
    let kind = (first & STRING_CONTROL_BITMASK) >> 6;
    let value = first & !STRING_CONTROL_BITMASK;
    Ok((tail, (kind, value)))
}

fn string_normal(tail: &[u8], kind: u8, value: u8) -> FileParseResult<&[u8], Vec<u8>> {
    let (tail, length) = match kind {
        0b00 => (tail, value.into()),
        0b01 => {
            let (tail, next) = le_u8(tail)?;
            let value = u16::from_le_bytes([value, next]);
            (tail, value.into())
        },
        0b10 => le_u32(tail)?,
        _ => unreachable!(),
    };
    let (tail, string) = take(length)(tail)?;
    Ok((tail, string.to_owned()))
}

fn string_special(tail: &[u8], control: u8) -> FileParseResult<&[u8], Vec<u8>> {
    match control {
        x if x < 3  => {
            let (tail, value) = integer(tail, control)?;
            Ok((tail, value.to_string().into_bytes()))
        },
        3 => {
            eprintln!("parsing of compressed strings is not implemented yet");
            return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
        },
        _ => {
            eprintln!("unexpected value of length-encoded string {control}");
            return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
        },
    }
}

fn integer(tail: &[u8], control: u8) -> FileParseResult<&[u8], i32> {
    let res = match control {
        0 => le_i8(tail)
            .map(|(tail, val)| (tail, val.into())),
        1 => le_i16(tail)
            .map(|(tail, val)| (tail, val.into())),
        2 => le_i32(tail),
        _ => {
            eprintln!("unexpected control value for integer {control}");
            return Err(nom::Err::Error(make_error(tail, ErrorKind::Verify)));
        }
    }?;
    Ok(res)
}
