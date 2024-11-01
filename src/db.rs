use std::{collections::BTreeMap, fs::File, os::unix::fs::FileExt};

use crate::{
    page::{
        Column, ColumnType, FirstPage, InteriorTablePage, LeafTableCell, LeafTablePage, Page,
        PageHeader, PageType, RecordBody, RecordHeader, TableInfo, TableInteriorCell,
    },
    query::SelectQuery,
    util::{get_content_size_type, read_varint},
};
use anyhow::{anyhow, Result};
use regex::Regex;

pub struct Db {
    file: File,
    pub header: DbHeader,
    pub table_infos: BTreeMap<String, TableInfo>, // TableName->TableInfo
    pub num_cells: usize,                         // number of cells in the first page for now
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
            Ok(p) => match p {
                Page::LeafTable(leaf) => leaf,
                _ => todo!("first page is not a leaf table page"),
            },
            Err(e) => return Err(anyhow!("error reading first page from file: {e}")),
        };

        let mut table_infos = BTreeMap::new();
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
                Column::I8(i) => *i as i32,
                Column::I24(i) => *i,
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

            if sql.starts_with("CREATE INDEX") {
                //CREATE INDEX (?P<index_name>.+)\s+on (?P<table>.+) ((?P<columns>.+))
                continue;
            }

            let re =
                Regex::new(r#"CREATE TABLE \"?\w+\"?\n?\s?\(\n?(?P<columns>(?:\n|.)+)\)"#).unwrap();
            let caps = re
                .captures(sql)
                .ok_or(anyhow!("can't parse columns from {}", sql))?;
            let columns = &caps["columns"];
            let mut column_orders = BTreeMap::new();
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
        Self::_get_page(&self.file, page_offset, page_header_offset)
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

        match page_header.page_type {
            PageType::LeafTable => {
                let cells =
                    Self::get_leaf_cells(cell_pointer_array, &page_header, file, page_offset)?;

                Ok(Page::LeafTable(LeafTablePage {
                    page_header,
                    //cell_pointer_array,
                    cells,
                }))
            }
            PageType::InteriorIndex => Ok(Page::InteriorIndex),
            PageType::InteriorTable => {
                let cells =
                    Self::get_interior_cells(cell_pointer_array, &page_header, file, page_offset)?;
                Ok(Page::InteriorTable(InteriorTablePage {
                    page_header,
                    cells,
                }))
            }
            PageType::LeafIndex => Ok(Page::LeafIndex),
        }
    }

    fn get_interior_cells(
        cell_pointer_array: Vec<u16>,
        page_header: &PageHeader,
        file: &File,
        page_offset: u64,
    ) -> Result<Vec<TableInteriorCell>> {
        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            pointer += page_offset;
            let mut buf_u32 = [0_u8; 4]; // for varints
            let mut buf_varint = [0_u8; 9]; // for varints

            file.read_exact_at(&mut buf_u32, pointer)
                .map_err(|e| anyhow!("can't read cell size: {e} at pointer {pointer}"))?;
            let left_child_page_num = u32::from_be_bytes(buf_u32);

            // rowid:
            file.read_exact_at(&mut buf_varint, pointer)
                .map_err(|e| anyhow!("can't read cell rowid: {e} at pointer {pointer}"))?;
            let (rowid, _) = read_varint(&buf_varint);

            cells.push(TableInteriorCell {
                left_child_page_num,
                rowid,
            })
        }

        Ok(cells)
    }

    fn get_leaf_cells(
        cell_pointer_array: Vec<u16>,
        page_header: &PageHeader,
        file: &File,
        page_offset: u64,
    ) -> Result<Vec<LeafTableCell>> {
        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            pointer += page_offset;
            let mut buf = [0_u8; 9]; // for varints

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
                // todo: tightly couple sizes and types
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
                    ColumnType::I16 => {
                        let val = i16::from_be_bytes([buf[0], buf[1]]);
                        record_body.columns.push(Column::I16(val));
                    }
                    ColumnType::I24 => {
                        let val = i32::from_be_bytes([0, buf[0], buf[1], buf[2]]);
                        record_body.columns.push(Column::I24(val));
                    }
                    ColumnType::One => {
                        record_body.columns.push(Column::One);
                    }
                    ColumnType::Null => record_body.columns.push(Column::Null),
                }
            }

            let cell = LeafTableCell {
                size,
                rowid,
                record_header,
                record_body,
            };

            cells.push(cell)
        }

        Ok(cells)
    }

    fn get_page_header(file: &File, offset: u64) -> Result<PageHeader> {
        let mut page_header = [0; 12];
        file.read_exact_at(&mut page_header, offset)
            .map_err(|e| anyhow!("can't read 8 bytes of page header from file: {e}"))?;
        let page_type_byte = page_header[0];
        let page_type = match page_type_byte {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            _ => {
                return Err(anyhow!(
                    "wrong page type byte {} for page at offset {offset}",
                    page_type_byte
                ))
            }
        };

        let mut rightmost_pointer = None;
        if page_type == PageType::InteriorTable {
            let b: [u8; 4] = page_header[8..12].try_into().unwrap();
            rightmost_pointer = Some(u32::from_be_bytes(b));
        }

        let num_cells = u16::from_be_bytes([page_header[3], page_header[4]]);
        Ok(PageHeader {
            page_type,
            num_cells,
            rightmost_pointer,
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

        match page {
            Page::LeafTable(p) => Self::query_leaf_page(&p, &query, table_info),
            Page::InteriorTable(p) => self.query_interior_page(&p, &query, table_info),
            _ => todo!(),
        }
    }

    fn query_interior_page(
        &self,
        interior_page: &InteriorTablePage,
        query: &SelectQuery,
        table_info: &TableInfo,
    ) -> Result<Vec<Vec<String>>> {
        let mut res = Vec::new();
        for cell in &interior_page.cells {
            let pointer =
                ((cell.left_child_page_num - 1) * u32::from(self.header.page_size)).into();
            let child = self.get_page(pointer, None)?;
            match child {
                Page::LeafTable(leaf) => {
                    let mut r = Self::query_leaf_page(&leaf, query, table_info)?;
                    res.append(&mut r);
                }
                Page::InteriorTable(interior_child) => {
                    let mut r = self.query_interior_page(&interior_child, query, table_info)?;
                    res.append(&mut r);
                }
                _ => {
                    //dbg!("other type");
                }
            }
        }

        //todo: make something with rightmost?
        //let rightmost = self.get_page(
        //    interior_page.page_header.rightmost_pointer.unwrap().into(),
        //    None,
        //)?;
        //dbg!(rightmost);

        Ok(res)
    }

    fn query_leaf_page(
        leaf_page: &LeafTablePage,
        query: &SelectQuery,
        table_info: &TableInfo,
    ) -> Result<Vec<Vec<String>>> {
        let mut result = Vec::new();

        for cell in &leaf_page.cells {
            let mut row = vec![String::from(""); query.columns.len()];

            let mut write_row = true;
            if query.where_column.is_some() {
                write_row = false;
            }

            for column_name in table_info.column_orders.keys() {
                let order = table_info.column_orders[column_name];
                let column = &cell.record_body.columns[order];

                let column_value = match column {
                    Column::Str(s) => s.to_string(),
                    Column::I8(i) => i.to_string(),
                    Column::I16(i) => i.to_string(),
                    Column::I24(i) => i.to_string(),
                    Column::One => String::from("1"),
                    Column::Null => cell.rowid.to_string(),
                };
                if query.where_column == Some(column_name.to_string())
                    && query.where_value == Some(column_value.to_string())
                {
                    write_row = true;
                }

                if query.columns.contains_key(column_name) {
                    row[*query.columns.get(column_name).unwrap()] = column_value;
                }
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
