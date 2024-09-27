use anyhow::Result;
use std::os::unix::fs::FileExt;
use std::{fmt::Display, fs::File};

use crate::util::{get_content_size_type, read_varint};

pub struct DbHeader {
    pub page_size: u16,
}

impl DbHeader {
    fn from_file(file: &mut File) -> Result<DbHeader> {
        let mut db_header_bytes = [0; 100];
        file.read_exact_at(&mut db_header_bytes, 0)?;
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
        file.read_exact_at(&mut page_header, offset)?;
        let page_type_byte = page_header[0];
        let page_type = match page_type_byte {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            _ => return Err(anyhow::anyhow!("wrong page type")),
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
    pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<Cell>,
}

impl Page {
    pub fn from_file(file: &mut File, page_offset: u64) -> Result<Self> {
        let page_header = PageHeader::from_file(file, page_offset)?;

        let mut page_data_offset = match page_header.page_type {
            PageType::LeafTable | PageType::LeafIndex => page_offset + 8,
            PageType::InteriorTable | PageType::InteriorIndex => page_offset + 12,
        };

        let mut cell_pointer_array = Vec::with_capacity(page_header.num_cells.into());
        for _ in 0..page_header.num_cells {
            let mut buf = [0_u8; 2];
            file.read_exact_at(&mut buf, page_data_offset)?;
            cell_pointer_array.push(u16::from_be_bytes(buf));
            page_data_offset += 2;
        }

        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            if page_offset != 100 {
                // todo
                // not the first page
                pointer += page_offset;
            }
            let mut buf = [0_u8, 9]; // for varints

            // size:
            file.read_exact_at(&mut buf, pointer)?;
            let (size, mut varint_offset) = read_varint(&buf);

            // rowid:
            file.read_exact_at(&mut buf, pointer + varint_offset as u64)?;
            let (rowid, o) = read_varint(&buf);
            varint_offset += o;

            // header_size:
            file.read_exact_at(&mut buf, pointer + varint_offset as u64)?;
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
                if *t == 0 {
                    continue;
                }
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
                        let byte = [buf.as_slice()[0]];
                        let val = i8::from_be_bytes(byte);
                        record_body.columns.push(Column::I8(val));
                    }
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
            cell_pointer_array,
            cells,
        })
    }
}

pub struct FirstPage {
    pub db_header: DbHeader,
    pub page: Page, //pub page_header: PageHeader,
                    //pub cell_pointer_array: Vec<u16>,
                    //pub cells: Vec<Cell>,
}

impl FirstPage {
    pub fn from_file(file: &mut File) -> Result<FirstPage> {
        let db_header = DbHeader::from_file(file)?;
        let page = Page::from_file(file, 100)?;

        Ok(Self { db_header, page })
    }
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
