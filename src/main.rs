mod page;
mod util;

use anyhow::{bail, Result};
use page::{Column, FirstPage, Page};
use regex::Regex;
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

            let first_page = FirstPage::from_file(&mut file)?;
            println!("database page size: {}", first_page.db_header.page_size);
            println!("page type: {:#?}", first_page.page.page_header.page_type);
            println!(
                "number of tables: {}",
                first_page.page.page_header.num_cells
            );
            println!(
                "cell pointer array: {:#?}",
                first_page.page.cell_pointer_array
            );
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;

            let first_page = FirstPage::from_file(&mut file)?;
            //println!("database page size: {}", page.db_header.page_size);
            //println!("page type: {:#?}", page.page_header.page_type);
            //println!("number of pages: {}", page.page_header.num_cells);
            //println!("cell pointer array: {:#?}", page.cell_pointer_array);
            //println!("cells: {:#?}", first_page.page.cells);

            for c in &first_page.page.cells {
                if let Some(table) = c.record_body.columns.get(2) {
                    if *table != Column::Str(String::from("sqlite_sequence")) {
                        print!("{} ", table);
                    }
                }
            }
            println!();
        }
        s if s.to_lowercase().starts_with("select count") => {
            let query = &args[2];
            let table_name = query.split(" ").last().unwrap();
            let mut file = File::open(&args[1])?;
            let first_page = FirstPage::from_file(&mut file)?;

            for c in first_page.page.cells {
                let table = c.record_body.columns.get(2);
                if let Some(Column::Str(table)) = table {
                    if table == table_name {
                        let root_page = c.record_body.columns.get(3);
                        if let Some(Column::I8(root_page)) = root_page {
                            let page = Page::from_file(
                                &mut file,
                                (*root_page - 1) as u64 * first_page.db_header.page_size as u64,
                            );
                            println!("{}", page.unwrap().page_header.num_cells);
                        } else {
                            todo!("can't get root page");
                        }
                    }
                } else {
                    todo!("can't get table name");
                }
            }
        }
        s if s.to_lowercase().starts_with("select")
            && !s.to_lowercase().starts_with("select count") =>
        {
            let re = Regex::new(r"(?i)SELECT (?P<column>\w+) FROM (?P<table>\w+)").unwrap();
            let caps = re.captures(s).unwrap();

            let table_name = caps["table"].to_string();
            let column_name = caps["column"].to_string();

            let mut file = File::open(&args[1])?;
            let first_page = FirstPage::from_file(&mut file)?;

            let table_info = first_page.table_infos.get(&table_name).unwrap();
            let column_order = table_info.column_orders.get(&column_name).unwrap();

            let page = Page::from_file(
                &mut file,
                (table_info.root_page_num - 1) as u64 * first_page.db_header.page_size as u64,
            )?;

            for cell in page.cells {
                if let Some(Column::Str(val)) = cell.record_body.columns.get(*column_order) {
                    println!("{val}");
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
