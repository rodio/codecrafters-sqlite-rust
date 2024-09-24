mod page;
mod util;

use anyhow::{bail, Result};
use page::FirstPage;
use std::fs::File;

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

            let page = FirstPage::from_file(&mut file)?;
            println!("database page size: {}", page.db_header.page_size);
            println!("page type: {:#?}", page.page_header.page_type);
            println!("number of tables: {}", page.page_header.num_cells);
            println!("cell pointer array: {:#?}", page.cell_pointer_array);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;

            let page = FirstPage::from_file(&mut file)?;
            //println!("database page size: {}", page.db_header.page_size);
            //println!("page type: {:#?}", page.page_header.page_type);
            //println!("number of pages: {}", page.page_header.num_cells);
            //println!("cell pointer array: {:#?}", page.cell_pointer_array);

            for c in page.cells {
                if let Some(table) = c.record_body.strings.get(2) {
                    if table != "sqlite_sequence" {
                        print!("{table} ");
                    }
                }
            }
            println!();
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
