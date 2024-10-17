use std::collections::HashMap;
use std::fmt::Display;

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

#[derive(Debug)]
pub struct Page {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<Cell>,
}

#[derive(Debug)]
pub struct TableInfo {
    pub root_page_num: I8,
    // column_name -> order
    pub column_orders: HashMap<String, usize>,
}

pub struct FirstPage {
    //pub db_header: DbHeader,
    pub page: Page,
    pub table_infos: HashMap<String, TableInfo>, // TableName->TableInfo
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
    pub fn new(size: i64) -> Self {
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
    pub fn new() -> Self {
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
    Null,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Str(s) => write!(f, "{}", s),
            Column::I8(i) => write!(f, "{}", i),
            Column::Null => write!(f, "NULL"),
        }
    }
}
