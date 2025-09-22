use std::io;

use io::Read;

use std::collections::BTreeSet;

use arrow::array::FixedSizeBinaryArray;

pub fn set2array(b: BTreeSet<[u8; 16]>) -> Result<FixedSizeBinaryArray, io::Error> {
    FixedSizeBinaryArray::try_from_iter(b.into_iter()).map_err(io::Error::other)
}

pub fn iter2set<I>(uuids: I) -> Result<BTreeSet<[u8; 16]>, io::Error>
where
    I: Iterator<Item = Result<[u8; 16], io::Error>>,
{
    uuids.collect()
}

/// Creates the array of unique uuids from the iterator.
pub fn iter2uniq2array<I>(uuids: I) -> Result<FixedSizeBinaryArray, io::Error>
where
    I: Iterator<Item = Result<[u8; 16], io::Error>>,
{
    let s: BTreeSet<_> = iter2set(uuids)?;
    set2array(s)
}

/// Converts the reader of raw bytes to an array of uuids.
pub fn rdr2iter_raw<R>(mut rdr: R) -> impl Iterator<Item = Result<[u8; 16], io::Error>>
where
    R: Read,
{
    let mut buf: [u8; 16] = [0; 16];

    std::iter::from_fn(move || {
        let rslt = rdr.read_exact(&mut buf);
        match rslt {
            Ok(_) => Some(Ok(buf)),
            Err(e) => match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            },
        }
    })
}

pub fn line2uuid(line: &str) -> Result<uuid::Uuid, io::Error> {
    uuid::Uuid::parse_str(line).map_err(io::Error::other)
}

/// Converts the lines to uuids.
pub fn lines2iter<I>(lines: I) -> impl Iterator<Item = Result<[u8; 16], io::Error>>
where
    I: Iterator<Item = Result<String, io::Error>>,
{
    lines
        .map(|rline| rline.and_then(|line: String| line2uuid(&line)))
        .map(|ruuid| ruuid.map(|u| u.as_u128().to_be_bytes()))
}

pub fn rdr2lines2iter<R>(rdr: R) -> impl Iterator<Item = Result<[u8; 16], io::Error>>
where
    R: io::BufRead,
{
    let lines = rdr.lines();
    lines2iter(lines)
}

/// Creates an array of unique uuids parsed from the reader.
pub fn rdr2lines2iter2uniq2array<R>(rdr: R) -> Result<FixedSizeBinaryArray, io::Error>
where
    R: io::BufRead,
{
    let uuids = rdr2lines2iter(rdr);
    iter2uniq2array(uuids)
}

/// Creates an array of unique uuids read from the bytes from the reader.
pub fn rdr2iter_raw2uuids2uniq2array<R>(rdr: R) -> Result<FixedSizeBinaryArray, io::Error>
where
    R: io::BufRead,
{
    let uuids = rdr2iter_raw(rdr);
    iter2uniq2array(uuids)
}

/// Creates an array of unique uuids parsed from stdin.
pub fn stdin2lines2uuids2uniq2array() -> Result<FixedSizeBinaryArray, io::Error> {
    rdr2lines2iter2uniq2array(io::stdin().lock())
}

/// Creates an array of unique uuids read from the bytes from stdin.
pub fn stdin2iter_raw2uuids2uniq2array() -> Result<FixedSizeBinaryArray, io::Error> {
    rdr2iter_raw2uuids2uniq2array(io::stdin().lock())
}
