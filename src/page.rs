use anyhow::anyhow;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;

use crate::util::{get_content_size_type, read_varint};
use std::{fs::File, os::unix::fs::FileExt};

#[derive(Debug, PartialEq)]
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
    #[allow(dead_code)]
    pub rightmost_pointer: Option<u32>,
}

#[derive(Debug)]
pub enum Page {
    LeafTable(LeafTablePage),
    InteriorTable(InteriorTablePage),
    LeafIndex(LeafIdxPage),
    InteriorIdx(InteriorIdxPage),
}

#[derive(Debug)]
pub struct LeafTablePage {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<LeafTableCell>,
}

#[derive(Debug)]
pub struct InteriorTablePage {
    #[allow(dead_code)]
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<TableInteriorCell>,
}

#[derive(Debug)]
pub struct InteriorIdxPage {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<IdxInteriorCell>,
}

#[derive(Debug)]
pub struct LeafIdxPage {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<IdxLeafCell>,
}

#[derive(Debug)]
pub struct TableInfo {
    pub root_page_num: u32,
    // column_name -> order
    pub column_orders: BTreeMap<String, usize>,
}

#[derive(Debug)]
pub struct IdxInfo {
    pub root_page_num: u32,
    pub idx_name: String,
    pub columns: HashSet<String>,
}

#[derive(Debug)]
pub struct FirstPage {
    //pub db_header: DbHeader,
    pub page: LeafTablePage,
    pub table_infos: BTreeMap<String, TableInfo>, // TableName->TableInfo
    pub idx_infos: BTreeMap<String, IdxInfo>,     // TableName->IdxInfo
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct LeafTableCell {
    pub size: i64,
    pub rowid: i64,
    pub record_header: RecordHeader,
    pub record_body: RecordBody,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TableInteriorCell {
    pub left_child_page_num: u32,
    pub rowid: i64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct IdxInteriorCell {
    pub left_child_page_num: u32,
    //pub key_payload_size: i64,
    pub record_header: RecordHeader,
    pub record_body: IdxRecordBody,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct IdxLeafCell {
    //pub key_payload_size: i64,
    pub record_header: RecordHeader,
    pub record_body: IdxRecordBody,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub column_types: Vec<i64>,
}

impl RecordHeader {
    pub fn from_file(file: &File, pointer: u64) -> anyhow::Result<(Self, u64)> {
        let mut buf_varint = [0_u8; 9];
        let mut current_offset = 0;
        // header_size:
        file.read_exact_at(&mut buf_varint, pointer + current_offset)
            .map_err(|e| anyhow!("can't read cell header size: {e} at offset {current_offset}"))?;
        let (record_header_size, record_header_size_bytes) = read_varint(&buf_varint);
        current_offset += record_header_size_bytes as u64;

        let mut column_types = Vec::new();
        // column types
        let mut bytes_read = 0;
        while bytes_read < record_header_size - record_header_size_bytes as i64 {
            file.read_exact_at(&mut buf_varint, pointer + current_offset)?;
            let (column_type, o) = read_varint(&buf_varint);
            current_offset += o as u64;
            bytes_read += o as i64;

            column_types.push(column_type);
        }
        Ok((Self { column_types }, current_offset))
    }

    pub fn read_columns(&self, file: &File, pointer: u64) -> anyhow::Result<(Vec<Column>, u64)> {
        let mut current_offset = 0_u64;
        let mut columns = Vec::new();
        for t in &self.column_types {
            // todo: tightly couple sizes and types
            let (size, typ) = get_content_size_type(*t);
            let mut buf: Vec<u8> = vec![0; size.try_into().unwrap()];
            file.read_exact_at(buf.as_mut_slice(), pointer + current_offset)?;
            current_offset += size;
            match typ {
                ColumnType::Str => {
                    let s = String::from_utf8(buf).unwrap();
                    columns.push(Column::Str(s));
                }
                ColumnType::I8 => {
                    let val = i8::from_be_bytes([buf.as_slice()[0]]);
                    columns.push(Column::I8(val));
                }
                ColumnType::I16 => {
                    let val = i16::from_be_bytes([buf[0], buf[1]]);
                    columns.push(Column::I16(val));
                }
                ColumnType::I24 => {
                    let val = i32::from_be_bytes([0, buf[0], buf[1], buf[2]]);
                    columns.push(Column::I24(val));
                }
                ColumnType::One => {
                    columns.push(Column::One);
                }
                ColumnType::Null => columns.push(Column::Null),
            }
        }

        Ok((columns, current_offset))
    }
}

#[derive(Debug)]
pub struct RecordBody {
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct IdxRecordBody {
    pub columns: Vec<Column>,
    pub rowid: i64,
}

impl RecordBody {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }
}

pub type Str = String;
pub type I8 = i8;
pub type I16 = i16;
pub type I24 = i32;

#[derive(Debug)]
pub enum ColumnType {
    Str,
    I8,
    I16,
    I24,
    One,
    Null,
}

#[derive(Debug, PartialEq)]
pub enum Column {
    Str(Str),
    I8(I8),
    I16(I16),
    I24(I24),
    One,
    Null,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Str(s) => write!(f, "{}", s),
            Column::I8(i) => write!(f, "{}", i),
            Column::I16(i) => write!(f, "{}", i),
            Column::I24(i) => write!(f, "{}", i),
            Column::One => write!(f, "1"),
            Column::Null => write!(f, "NULL"),
        }
    }
}
