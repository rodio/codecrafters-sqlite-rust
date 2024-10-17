use std::{collections::HashMap, fs::File, os::unix::fs::FileExt};

use crate::{
    page::{
        Cell, Column, ColumnType, FirstPage, Page, PageHeader, PageType, RecordBody, RecordHeader,
        TableInfo,
    },
    query::SelectQuery,
    util::{get_content_size_type, read_varint},
};
use anyhow::{anyhow, Result};
use regex::Regex;

pub struct Db {
    file: File,
    pub header: DbHeader,
    pub table_infos: HashMap<String, TableInfo>, // TableName->TableInfo
    pub num_cells: usize,                        // number of cells in the first page for now
}

impl Db {
    pub fn new(file: File) -> Result<Self> {
        let mut db_header_bytes = [0; 100];
        file.read_exact_at(&mut db_header_bytes, 0)
            .map_err(|e| anyhow!("can't read 100 db header bytes from file: {e}"))?;
        let page_size = u16::from_be_bytes([db_header_bytes[16], db_header_bytes[17]]);
        //let text_encoding = u32::from_be_bytes([
        //    db_header_bytes[56],
        //    db_header_bytes[57],
        //    db_header_bytes[58],
        //    db_header_bytes[59],
        //]);
        let header = DbHeader { page_size };
        let first_page = Self::get_first_page(&file)?;
        Ok(Db {
            file,
            header,
            table_infos: first_page.table_infos,
            num_cells: first_page.page.cells.len(),
        })
    }

    fn get_first_page(file: &File) -> Result<FirstPage> {
        let page = match Self::_get_page(file, 0, Some(100)) {
            Ok(p) => p,
            Err(e) => return Err(anyhow!("error reading first page from file: {e}")),
        };

        let mut table_infos = HashMap::new();
        for cell in &page.cells {
            let page_name_col = cell
                .record_body
                .columns
                .get(2)
                .ok_or(anyhow!("can't get page name from cell 2"))?;
            let table_name = match page_name_col {
                Column::Str(s) => s.to_string(),
                _ => return Err(anyhow!("wrong format of page name column")),
            };

            let root_page_number_col = cell
                .record_body
                .columns
                .get(3)
                .ok_or(anyhow!("can't get root page num from cell 3"))?;
            let root_page_num = match root_page_number_col {
                Column::I8(i) => *i,
                _ => return Err(anyhow!("wrong format of root page column")),
            };

            let sql_col = cell
                .record_body
                .columns
                .get(4)
                .ok_or(anyhow!("can't get sql num from cell 4"))?;
            let sql = match sql_col {
                Column::Str(s) => s,
                _ => return Err(anyhow!("wrong format of sql column")),
            };

            let re =
                Regex::new(r#"CREATE TABLE \"?\w+\"?\n?\s?\(\n?(?P<columns>(?:\n|.)+)\)"#).unwrap();
            let caps = re
                .captures(sql)
                .ok_or(anyhow!("can't parse columns from {}", sql))?;
            let columns = &caps["columns"];
            let mut column_orders = HashMap::new();
            for (i, mut c) in columns.split(",").enumerate() {
                c = c
                    .trim()
                    .split(" ")
                    .next()
                    .ok_or(anyhow!("bad format of the column {c}"))?;

                column_orders.insert(c.to_string(), i);
            }
            let table_info = TableInfo {
                root_page_num,
                column_orders,
            };
            table_infos.insert(table_name, table_info);
        }
        Ok(FirstPage { page, table_infos })
    }

    pub fn get_page(&self, page_offset: u64, page_header_offset: Option<u64>) -> Result<Page> {
        return Self::_get_page(&self.file, page_offset, page_header_offset);
    }

    pub fn _get_page(
        file: &File,
        page_offset: u64,
        page_header_offset: Option<u64>,
    ) -> Result<Page> {
        let page_header_offset = page_header_offset.unwrap_or(0);
        let page_header = Self::get_page_header(file, page_offset + page_header_offset)
            .map_err(|e| anyhow!("can't read page header from file at page offset {page_offset}, page header offset {page_header_offset}: {e}"))?;

        let page_data_offset = match page_header.page_type {
            PageType::LeafTable | PageType::LeafIndex => page_offset + page_header_offset + 8,
            PageType::InteriorTable | PageType::InteriorIndex => {
                page_offset + page_header_offset + 12
            }
        };

        let mut cell_offset = page_data_offset;
        let mut cell_pointer_array = Vec::with_capacity(page_header.num_cells.into());
        for i in 0..page_header.num_cells {
            let mut buf = [0_u8; 2];
            file.read_exact_at(&mut buf, cell_offset)
                .map_err(|e| anyhow!("can't read cell {i} at offset {cell_offset}: {e}"))?;
            cell_pointer_array.push(u16::from_be_bytes(buf));
            cell_offset += 2;
        }

        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            pointer += page_offset;
            let mut buf = [0_u8, 9]; // for varints

            // size:
            file.read_exact_at(&mut buf, pointer)
                .map_err(|e| anyhow!("can't read cell size: {e} at pointer {pointer}"))?;
            let (size, mut varint_offset) = read_varint(&buf);

            // rowid:
            file.read_exact_at(&mut buf, pointer + varint_offset as u64)
                .map_err(|e| anyhow!("can't read cell rowid: {e} at pointer {pointer}"))?;
            let (rowid, o) = read_varint(&buf);
            varint_offset += o;

            // header_size:
            file.read_exact_at(&mut buf, pointer + varint_offset as u64)
                .map_err(|e| anyhow!("can't read cell header size: {e} at pointer {pointer}"))?;
            let (record_header_size, record_header_size_bytes) = read_varint(&buf);
            varint_offset += record_header_size_bytes;

            // record header
            let mut record_header = RecordHeader::new(record_header_size);

            // cell types
            let mut bytes_read = 0;
            while bytes_read < record_header.size - record_header_size_bytes as i64 {
                file.read_exact_at(&mut buf, pointer + varint_offset as u64)?;
                let (column_type, o) = read_varint(&buf);
                varint_offset += o;
                bytes_read += o as i64;

                record_header.column_types.push(column_type);
            }

            let mut record_body = RecordBody::new();
            for t in &record_header.column_types {
                let (size, typ) = get_content_size_type(*t);
                let mut buf: Vec<u8> = vec![0; size];
                file.read_exact_at(buf.as_mut_slice(), pointer + varint_offset as u64)?;
                varint_offset += size;
                match typ {
                    ColumnType::Str => {
                        let s = String::from_utf8(buf).unwrap();
                        record_body.columns.push(Column::Str(s));
                    }
                    ColumnType::I8 => {
                        let val = i8::from_be_bytes([buf.as_slice()[0]]);
                        record_body.columns.push(Column::I8(val));
                    }
                    ColumnType::Null => record_body.columns.push(Column::Null),
                }
            }

            let cell = Cell {
                size,
                rowid,
                record_header,
                record_body,
            };

            cells.push(cell)
        }

        Ok(Page {
            page_header,
            //cell_pointer_array,
            cells,
        })
    }

    fn get_page_header(file: &File, offset: u64) -> Result<PageHeader> {
        let mut page_header = [0; 8];
        file.read_exact_at(&mut page_header, offset)
            .map_err(|e| anyhow!("cant read 8 bytes of page header from file: {e}"))?;
        let page_type_byte = page_header[0];
        let page_type = match page_type_byte {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            _ => return Err(anyhow!("wrong page type byte {}", page_type_byte)),
        };

        let num_cells = u16::from_be_bytes([page_header[3], page_header[4]]);
        Ok(PageHeader {
            page_type,
            num_cells,
        })
    }

    pub fn execute_select(&self, query: SelectQuery) -> Result<Vec<Vec<String>>> {
        let table_info = self
            .table_infos
            .get(&query.table_name)
            .ok_or(anyhow!("no such table: {}", &query.table_name))?;

        let page = self.get_page(
            (table_info.root_page_num - 1) as u64 * self.header.page_size as u64,
            None,
        )?;

        let mut result = Vec::new();

        for cell in &page.cells {
            let mut row = Vec::new();

            let mut write_row = true;
            if query.where_column.is_some() {
                write_row = false;
            }
            for column_name in &query.columns {
                let order = table_info.column_orders[column_name];
                let column = &cell.record_body.columns[order];

                let column_value = match column {
                    Column::Str(s) => s.to_string(),
                    Column::I8(i) => i.to_string(),
                    Column::Null => cell.rowid.to_string(),
                };
                if query.where_column == Some(column_name.to_string())
                    && query.where_value == Some(column_value.to_string())
                {
                    write_row = true;
                }
                row.push(column_value);
            }
            if write_row {
                result.push(row);
            }
        }

        Ok(result)
    }
}

pub struct DbHeader {
    pub page_size: u16,
}
