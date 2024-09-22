use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::FileExt;
use std::u16;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);

            let mut page_header = [0; 8];
            file.read_exact(&mut page_header)?;
            println!("page is leaf: {}", page_header[0] == 13);
            let number_of_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

            let mut cell_pointer_bytes = vec![0; (number_of_cells * 2).into()];
            file.read_exact(&mut cell_pointer_bytes)?;

            println!("number of tables: {}", number_of_cells);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
