mod db;
mod page;
mod query;
mod util;

use anyhow::{bail, Result};
use db::Db;
use page::Page;
use query::SelectQuery;
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

            let table_info = db.table_infos.get("companies").unwrap();
            let query = SelectQuery::from_query_string(
                "SELECT id, name, country FROM companies WHERE country = mongolia",
            )?;

            //dbg!(&query);

            let rowids = db
                .query_idx(&query.table_name, &query.where_value.clone().unwrap())?
                .unwrap();
            //dbg!(&rowids);

            let root_offset = (db.table_infos.get(&query.table_name).unwrap().root_page_num - 1)
                as u64
                * db.header.page_size as u64;
            let root_page = db.get_page(root_offset, None)?;

            let mut res = Vec::new();
            for rowid in &rowids {
                let r = db.get_row(&root_page, *rowid, table_info, &query)?;
                if !r.is_empty() {
                    res.push(r);
                }
            }

            dbg!(rowids.len(), res.len());

            //dbg!(&root_page);

            //if let Page::InteriorIdx(root_page) = root_page {
            //    for c in root_page.cells {
            //        dbg!(&c);
            //        dbg!(&root_page.page_header.rightmost_pointer);
            //        let child_page = db.get_page(
            //            ((c.left_child_page_num - 1) * db.header.page_size as u32).into(),
            //            None,
            //        )?;
            //        if let Page::InteriorIdx(child_page) = child_page {
            //            //maybe skip:
            //            let first_cell_key: String = match &child_page
            //                .cells
            //                .first()
            //                .unwrap()
            //                .record_body
            //                .columns
            //                .first()
            //                .unwrap()
            //            {
            //                Column::Str(key) => key.to_string(),
            //                Column::I8(_) => todo!(),
            //                Column::I16(_) => todo!(),
            //                Column::I24(_) => todo!(),
            //                Column::One => todo!(),
            //                Column::Null => todo!(),
            //            };
            //
            //            let last_cell_key: String = match &child_page
            //                .cells
            //                .last()
            //                .unwrap()
            //                .record_body
            //                .columns
            //                .first()
            //                .unwrap()
            //            {
            //                Column::Str(key) => key.to_string(),
            //                Column::I8(_) => todo!(),
            //                Column::I16(_) => todo!(),
            //                Column::I24(_) => todo!(),
            //                Column::One => todo!(),
            //                Column::Null => todo!(),
            //            };
            //
            //            dbg!(&first_cell_key);
            //            dbg!(&last_cell_key);
            //            if last_cell_key < country {
            //                dbg!("could skip 1", &child_page);
            //            }
            //
            //            if first_cell_key > country {
            //                dbg!("could skip 2", &child_page);
            //            }
            //
            //            for c in &child_page.cells {
            //                if let Column::Str(idx_column) = c.record_body.columns.first().unwrap()
            //                {
            //                    let idx_column2 = idx_column.clone();
            //                    if country == *idx_column {
            //                        dbg!(&c.record_body.columns);
            //                    }
            //                    if idx_column2 > country {
            //                        break;
            //                    }
            //                } else {
            //                    todo!()
            //                }
            //            }
            //        }
            //    }
            //
            //    // rightmost
            //    let rightmost_page = db.get_page(
            //        ((root_page.page_header.rightmost_pointer.unwrap() - 1)
            //            * db.header.page_size as u32)
            //            .into(),
            //        None,
            //    )?;
            //
            //    let rightmost_page = match rightmost_page {
            //        Page::InteriorIdx(page) => page,
            //        _ => todo!(),
            //    };
            //
            //    for c in rightmost_page.cells {
            //        let child_page = db.get_page(
            //            ((c.left_child_page_num - 1) * db.header.page_size as u32).into(),
            //            None,
            //        )?;
            //        if let Page::LeafIndex(child_page) = child_page {
            //            //maybe skip:
            //            let first_cell_key: String = match &child_page
            //                .cells
            //                .first()
            //                .unwrap()
            //                .record_body
            //                .columns
            //                .first()
            //                .unwrap()
            //            {
            //                Column::Str(key) => key.to_string(),
            //                Column::I8(_) => todo!(),
            //                Column::I16(_) => todo!(),
            //                Column::I24(_) => todo!(),
            //                Column::One => todo!(),
            //                Column::Null => todo!(),
            //            };
            //
            //            let last_cell_key: String = match &child_page
            //                .cells
            //                .last()
            //                .unwrap()
            //                .record_body
            //                .columns
            //                .first()
            //                .unwrap()
            //            {
            //                Column::Str(key) => key.to_string(),
            //                Column::I8(_) => todo!(),
            //                Column::I16(_) => todo!(),
            //                Column::I24(_) => todo!(),
            //                Column::One => todo!(),
            //                Column::Null => todo!(),
            //            };
            //
            //            dbg!(&first_cell_key);
            //            dbg!(&last_cell_key);
            //            if last_cell_key < country {
            //                dbg!("could skip 1", &child_page);
            //            }
            //
            //            if first_cell_key > country {
            //                dbg!("could skip 2", &child_page);
            //            }
            //
            //            for c in &child_page.cells {
            //                if let Column::Str(idx_column) = c.record_body.columns.first().unwrap()
            //                {
            //                    let idx_column2 = idx_column.clone();
            //                    if country == *idx_column {
            //                        dbg!(&c.record_body.columns);
            //                    }
            //                    if idx_column2 > country {
            //                        break;
            //                    }
            //                } else {
            //                    todo!()
            //                }
            //            }
            //        }
            //    }
            //    // rightmost
            //}
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
