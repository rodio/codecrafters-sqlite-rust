use anyhow::Result;
use std::os::unix::fs::FileExt;
use std::usize;
use std::{fs::File, i64};

use crate::util::{get_content_size, read_varint};

pub struct DbHeader {
    pub page_size: u16,
}

impl DbHeader {
    fn from_file(file: &mut File) -> Result<DbHeader> {
        let mut db_header_bytes = [0; 100];
        file.read_exact_at(&mut db_header_bytes, 0)?;
        let page_size = u16::from_be_bytes([db_header_bytes[16], db_header_bytes[17]]);
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

pub struct PageHeader {
    pub page_type: PageType,
    pub num_cells: u16,
}

impl PageHeader {
    fn from_file(file: &mut File) -> Result<PageHeader> {
        let mut page_header = [0; 8];
        file.read_exact_at(&mut page_header, 100)?;
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

pub struct FirstPage {
    pub db_header: DbHeader,
    pub page_header: PageHeader,
    pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<Cell>,
}

impl FirstPage {
    pub fn from_file(file: &mut File) -> Result<FirstPage> {
        let db_header = DbHeader::from_file(file)?;
        let page_header = PageHeader::from_file(file)?;

        let mut offset = match page_header.page_type {
            PageType::LeafTable | PageType::LeafIndex => 100 + 8,
            PageType::InteriorTable | PageType::InteriorIndex => 100 + 12,
        };

        let mut cell_pointer_array = Vec::with_capacity(page_header.num_cells.into());
        for _ in 0..page_header.num_cells {
            let mut buf = [0_u8; 2];
            file.read_exact_at(&mut buf, offset)?;
            cell_pointer_array.push(u16::from_be_bytes(buf));
            offset += 2;
        }

        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let pointer = *pointer as u64;
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
                let size = get_content_size(*t);
                let mut buf: Vec<u8> = vec![0; size];
                file.read_exact_at(buf.as_mut_slice(), pointer + varint_offset as u64)?;
                varint_offset += size;
                record_body.strings.push(String::from_utf8(buf).unwrap());
            }

            cells.push(Cell {
                size,
                rowid,
                record_header,
                record_body,
            })
        }

        Ok(Self {
            db_header,
            page_header,
            cell_pointer_array,
            cells,
        })
    }
}

#[derive(Debug)]
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
    pub strings: Vec<String>,
}

impl RecordBody {
    fn new() -> Self {
        Self {
            strings: Vec::new(),
        }
    }
}
