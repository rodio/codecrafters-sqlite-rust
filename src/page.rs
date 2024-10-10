use anyhow::{anyhow, Result};
use regex::Regex;
use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::{fmt::Display, fs::File};

use crate::util::{get_content_size_type, read_varint};

pub struct DbHeader {
    pub page_size: u16,
}

impl DbHeader {
    fn from_file(file: &mut File) -> Result<DbHeader> {
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
        Ok(DbHeader { page_size })
    }
}

#[derive(Debug)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub num_cells: u16,
}

impl PageHeader {
    fn from_file(file: &mut File, offset: u64) -> Result<PageHeader> {
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
}

#[derive(Debug)]
pub struct Page {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<Cell>,
}

impl Page {
    pub fn from_file(
        file: &mut File,
        page_offset: u64,
        page_header_offset: Option<u64>,
    ) -> Result<Self> {
        let page_header_offset = page_header_offset.unwrap_or(0);
        let page_header = PageHeader::from_file(file, page_offset + page_header_offset)
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
                    ColumnType::Null => (),
                }
            }

            cells.push(Cell {
                size,
                rowid,
                record_header,
                record_body,
            })
        }

        Ok(Page {
            page_header,
            //cell_pointer_array,
            cells,
        })
    }
}

#[derive(Debug)]
pub struct TableInfo {
    pub root_page_num: I8,
    // column_name -> order
    pub column_orders: HashMap<String, usize>,
}

pub struct FirstPage {
    pub db_header: DbHeader,
    pub page: Page,
    pub table_infos: HashMap<String, TableInfo>, // TableName->TableInfo
}

impl FirstPage {
    pub fn from_file(file: &mut File) -> Result<FirstPage> {
        let db_header = DbHeader::from_file(file)?;
        let page = match Page::from_file(file, 0, Some(100)) {
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

            let re = Regex::new(r"CREATE TABLE \w+\n?\s?\(\n?(?P<columns>(?:\n|.)+)\)").unwrap();
            let caps = re.captures(sql).ok_or(anyhow!("can't parse columns"))?;
            let columns = &caps["columns"];
            let mut column_orders = HashMap::new();
            for (i, mut c) in columns.split(",").enumerate() {
                c = c
                    .trim()
                    .split(" ")
                    .next()
                    .ok_or(anyhow!("bad format of the column {c}"))?;

                column_orders.insert(c.to_string(), i - 1);
            }
            let table_info = TableInfo {
                root_page_num,
                column_orders,
            };
            table_infos.insert(table_name, table_info);
        }
        Ok(Self {
            db_header,
            page,
            table_infos,
        })
    }

    //pub fn get_root_page(&self, table_name: String, mut file: &mut File) -> Result<Page> {
    //    let value = Column::Str(table_name);
    //    for c in &self.page.cells {
    //        let page_name = c.record_body.columns.get(2).unwrap();
    //        if *page_name == value {
    //            let root_page_number = c.record_body.columns.get(3);
    //            if let Some(Column::I8(root_page_number)) = root_page_number {
    //                let page = Page::from_file(
    //                    &mut file,
    //                    (*root_page_number - 1) as u64 * self.db_header.page_size as u64,
    //                )?;
    //                return Ok(page);
    //            }
    //        }
    //    }
    //
    //    Err(anyhow!("no such table"))
    //}

    //pub fn get_column_order(&self, table_name: String, column: String) -> Result<usize> {
    //    //println!("parsing {sql} {column}");
    //    let re = Regex::new(r"CREATE TABLE \w+\n?\s?\(\n?(?P<columns>(?:\n|.)+)\)").unwrap();
    //
    //    let value = Column::Str(table_name);
    //    for c in &self.page.cells {
    //        let page_name = c.record_body.columns.get(2).unwrap();
    //        if *page_name == value {
    //            if let Some(Column::Str(sql)) = c.record_body.columns.get(4) {
    //                let caps = re.captures(sql).ok_or(anyhow!("can't parse columns"))?;
    //                let columns = &caps["columns"];
    //                for (i, mut c) in columns.split(",").enumerate() {
    //                    c = c
    //                        .trim()
    //                        .split(" ")
    //                        .next()
    //                        .ok_or(anyhow!("bad format of the column {c}"))?;
    //
    //                    if c == column {
    //                        return Ok(i - 1);
    //                    }
    //                }
    //            }
    //        }
    //    }
    //
    //    Err(anyhow!("no such column"))
    //}
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Cell {
    pub size: i64,
    pub rowid: i64,
    pub record_header: RecordHeader,
    pub record_body: RecordBody,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub size: i64,
    pub column_types: Vec<i64>,
}

impl RecordHeader {
    fn new(size: i64) -> Self {
        Self {
            size,
            column_types: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct RecordBody {
    pub columns: Vec<Column>,
}

impl RecordBody {
    fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }
}

pub type Str = String;
pub type I8 = i8;

#[derive(Debug)]
pub enum ColumnType {
    Str,
    I8,
    Null,
}

#[derive(Debug, PartialEq)]
pub enum Column {
    Str(Str),
    I8(I8),
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Str(s) => write!(f, "{}", s),
            Column::I8(i) => write!(f, "{}", i),
        }
    }
}
