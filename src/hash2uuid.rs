use std::io;

use io::Read;

use sha2::Digest;

use sha2::digest;

use digest::generic_array;

use generic_array::GenericArray;

use arrow::array::FixedSizeBinaryArray;

pub fn bytes2sha512(s: &[u8]) -> [u8; 64] {
    let ga: GenericArray<_, _> = sha2::Sha512::digest(s);
    ga.into()
}

pub fn bytes2sha256(s: &[u8]) -> [u8; 32] {
    let ga: GenericArray<_, _> = sha2::Sha256::digest(s);
    ga.into()
}

/// Gets the first 16 bytes from the array.
pub fn array2uuid32(b: [u8; 32]) -> [u8; 16] {
    let src: &[u8] = &b[..16];
    let mut dst: [u8; 16] = [0; 16];
    dst.copy_from_slice(src);
    dst
}

/// Gets the first 16 bytes from the array.
pub fn array2uuid64(b: [u8; 64]) -> [u8; 16] {
    let src: &[u8] = &b[..16];
    let mut dst: [u8; 16] = [0; 16];
    dst.copy_from_slice(src);
    dst
}

/// Generates the "uuid" from the slice using sha2-512.
pub fn bytes2hash2array2uuid512(s: &[u8]) -> [u8; 16] {
    let a: [u8; 64] = bytes2sha512(s);
    array2uuid64(a)
}

/// Generates the "uuid" from the slice using sha2-256.
pub fn bytes2hash2array2uuid256(s: &[u8]) -> [u8; 16] {
    let a: [u8; 32] = bytes2sha256(s);
    array2uuid32(a)
}

/// Creates an iterator of arrays from the reader.
pub fn rdr2fixed2iter<R, const N: usize>(
    mut rdr: R,
) -> impl Iterator<Item = Result<[u8; N], io::Error>>
where
    R: Read,
{
    let mut buf: [u8; N] = [0; N];
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

/// Creates an iterator of uuids from the reader using sha2-512.
pub fn rdr2fixed2iter2uuids512<R, const N: usize>(
    rdr: R,
) -> impl Iterator<Item = Result<[u8; 16], io::Error>>
where
    R: Read,
{
    let arrays = rdr2fixed2iter(rdr);
    arrays.map(|ritem| ritem.map(|item: [u8; N]| bytes2hash2array2uuid512(&item)))
}

/// Creates an iterator of uuids from the reader using sha2-256.
pub fn rdr2fixed2iter2uuids256<R, const N: usize>(
    rdr: R,
) -> impl Iterator<Item = Result<[u8; 16], io::Error>>
where
    R: Read,
{
    let arrays = rdr2fixed2iter(rdr);
    arrays.map(|ritem| ritem.map(|item: [u8; N]| bytes2hash2array2uuid256(&item)))
}

/// Creates an array of unique uuids from the reader using sha2-256.
pub fn rdr2fixed2iter2uuids2array256<R, const N: usize>(
    rdr: R,
) -> Result<FixedSizeBinaryArray, io::Error>
where
    R: Read,
{
    let uuids = rdr2fixed2iter2uuids256::<_, N>(rdr);
    super::core::iter2uniq2array(uuids)
}

/// Creates an array of unique uuids from the reader using sha2-512.
pub fn rdr2fixed2iter2uuids2array512<R, const N: usize>(
    rdr: R,
) -> Result<FixedSizeBinaryArray, io::Error>
where
    R: Read,
{
    let uuids = rdr2fixed2iter2uuids512::<_, N>(rdr);
    super::core::iter2uniq2array(uuids)
}

/// Creates an array of unique uuids from stdin using sha2-512.
pub fn stdin2fixed2iter2uuids2array512<const N: usize>() -> Result<FixedSizeBinaryArray, io::Error>
{
    rdr2fixed2iter2uuids2array512::<_, N>(io::stdin().lock())
}

/// Creates an array of unique uuids from stdin using sha2-256.
pub fn stdin2fixed2iter2uuids2array256<const N: usize>() -> Result<FixedSizeBinaryArray, io::Error>
{
    rdr2fixed2iter2uuids2array256::<_, N>(io::stdin().lock())
}

/// Creates an iterator of uuids from stdin using sha2-256.
pub fn stdin2fixed2iter2uuids256<const N: usize>()
-> impl Iterator<Item = Result<[u8; 16], io::Error>> {
    rdr2fixed2iter2uuids256::<_, N>(io::stdin().lock())
}

/// Creates an iterator of uuids from stdin using sha2-512.
pub fn stdin2fixed2iter2uuids512<const N: usize>()
-> impl Iterator<Item = Result<[u8; 16], io::Error>> {
    rdr2fixed2iter2uuids512::<_, N>(io::stdin().lock())
}
