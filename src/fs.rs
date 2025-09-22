use std::fs::File;
use std::io;
use std::path::Path;

use io::BufReader;

use arrow::array::FixedSizeBinaryArray;

pub fn file2iter_raw2uuids2uniq2array(f: File) -> Result<FixedSizeBinaryArray, io::Error> {
    let br = BufReader::new(f);
    super::core::rdr2iter_raw2uuids2uniq2array(br)
}

pub fn file2lines2iter2uniq2array(f: File) -> Result<FixedSizeBinaryArray, io::Error> {
    let br = BufReader::new(f);
    super::core::rdr2lines2iter2uniq2array(br)
}

pub fn filename2lines2iter2uniq2array<P>(filename: P) -> Result<FixedSizeBinaryArray, io::Error>
where
    P: AsRef<Path>,
{
    let f = File::open(filename)?;
    file2lines2iter2uniq2array(f)
}

pub fn filename2iter_raw2uuids2uniq2array<P>(filename: P) -> Result<FixedSizeBinaryArray, io::Error>
where
    P: AsRef<Path>,
{
    let f = File::open(filename)?;
    file2iter_raw2uuids2uniq2array(f)
}

#[cfg(feature = "hash2uuid")]
pub mod fs_hash {
    use std::io;

    use io::BufReader;

    use arrow::array::FixedSizeBinaryArray;
    use std::fs::File;
    use std::path::Path;

    /// Creates an array of unique uuids from the file using sha2-256.
    pub fn file2fixed2iter2uuids2array256<const N: usize>(
        f: File,
    ) -> Result<FixedSizeBinaryArray, io::Error> {
        let br = BufReader::new(f);
        crate::hash2uuid::rdr2fixed2iter2uuids2array256::<_, N>(br)
    }

    /// Creates an array of unique uuids from the file using sha2-512.
    pub fn file2fixed2iter2uuids2array512<const N: usize>(
        f: File,
    ) -> Result<FixedSizeBinaryArray, io::Error> {
        let br = BufReader::new(f);
        crate::hash2uuid::rdr2fixed2iter2uuids2array512::<_, N>(br)
    }

    /// Creates an array of unique uuids from the file using sha2-256.
    pub fn filename2fixed2iter2uuids2array256<P, const N: usize>(
        filename: P,
    ) -> Result<FixedSizeBinaryArray, io::Error>
    where
        P: AsRef<Path>,
    {
        file2fixed2iter2uuids2array256::<N>(File::open(filename)?)
    }

    /// Creates an array of unique uuids from the file using sha2-512.
    pub fn filename2fixed2iter2uuids2array512<P, const N: usize>(
        filename: P,
    ) -> Result<FixedSizeBinaryArray, io::Error>
    where
        P: AsRef<Path>,
    {
        file2fixed2iter2uuids2array512::<N>(File::open(filename)?)
    }
}
