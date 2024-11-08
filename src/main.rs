mod db;
mod page;
mod query;
mod util;

use anyhow::{bail, Result};
use db::Db;
use page::{InteriorIdxPage, Page};
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
        ".test" => {
            let file = File::open(&args[1])?;
            let db = Db::new(file)?;
            dbg!(&db.idx_infos);
            let root_offset = (db.idx_infos.get("companies").unwrap().root_page_num - 1) as u64
                * db.header.page_size as u64;

            let root_page = db.get_page(root_offset, None)?;
            //dbg!(&root_page);

            if let Page::InteriorIdx(interior_page) = root_page {
                let left_child_offset = interior_page.cells.first().unwrap().left_child_page_num
                    as u64
                    * db.header.page_size as u64;
                let roots_first_child = db.get_page(left_child_offset, None)?;
                //dbg!(&roots_first_child);

                if let Page::InteriorIdx(interior_roots_first_child) = roots_first_child {
                    let roots_first_grandkid_offset = interior_roots_first_child
                        .cells
                        .first()
                        .unwrap()
                        .left_child_page_num
                        as u64
                        * db.header.page_size as u64;
                    let roots_first_grandkid = db.get_page(roots_first_grandkid_offset, None)?;

                    // grand grand
                    if let Page::InteriorIdx(roots_first_grandkid) = roots_first_grandkid {
                        let roots_first_grand_grandkid_offset = roots_first_grandkid
                            .cells
                            .first()
                            .unwrap()
                            .left_child_page_num
                            as u64
                            * db.header.page_size as u64;
                        let roots_first_grand_grandkid =
                            db.get_page(roots_first_grand_grandkid_offset, None)?;
                        //dbg!(roots_first_grand_grandkid);
                    }
                }
            }
        }
        ".dbinfo" => {
            let file = File::open(&args[1])?;
            let db = Db::new(file)?;

            println!("database page size: {}", db.header.page_size);
            println!("number of tables: {}", db.num_cells,);
        }

        ".tables" => {
            let file = File::open(&args[1])?;
            let db = Db::new(file)?;

            for k in db.table_infos.keys() {
                if k != "sqlite_sequence" {
                    print!("{} ", k);
                }
            }

            println!();
        }

        s if s.to_lowercase().starts_with("select count") => {
            let query = &args[2];
            let queried_table_name = query.split(" ").last().unwrap();

            let file = File::open(&args[1])?;
            let db = Db::new(file)?;

            for (table_name, table_info) in &db.table_infos {
                if table_name == queried_table_name {
                    let root_page = table_info.root_page_num;
                    let page =
                        db.get_page((root_page - 1) as u64 * db.header.page_size as u64, None)?;
                    let leaf_page = match page {
                        Page::LeafTable(p) => p,
                        _ => todo!(),
                    };
                    println!("{}", leaf_page.page_header.num_cells);
                }
            }
        }
        s if s.to_lowercase().starts_with("select")
            && !s.to_lowercase().starts_with("select count") =>
        {
            let select_query = query::SelectQuery::from_query_string(s)?;

            let file = File::open(&args[1])?;
            let db = Db::new(file)?;

            match db.execute_select(select_query) {
                Ok(rows) => {
                    for row in rows {
                        for (i, column) in row.iter().enumerate() {
                            print!("{}", column);
                            if i != row.len() - 1 {
                                print!("|");
                            } else {
                                println!();
                            }
                        }
                    }
                }
                Err(e) => bail!(e),
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
