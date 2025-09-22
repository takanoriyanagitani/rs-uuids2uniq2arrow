use std::io;
use std::process::ExitCode;

use rs_uuids2uniq2arrow::arrow;

use arrow::array::FixedSizeBinaryArray;

use rs_uuids2uniq2arrow::hash2uuid::stdin2fixed2iter2uuids2array256;

fn sub() -> Result<(), io::Error> {
    let fsba: FixedSizeBinaryArray = stdin2fixed2iter2uuids2array256::<4>()?;
    println!("{fsba:?}");
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
