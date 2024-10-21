use std::collections::BTreeMap;
use std::fmt::Display;

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
    pub rightmost_pointer: Option<u32>,
}

#[derive(Debug)]
pub enum Page {
    LeafTable(LeafTablePage),
    InteriorTable(InteriorTablePage),
    LeafIndex,
    InteriorIndex,
}

#[derive(Debug)]
pub struct LeafTablePage {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<LeafTableCell>,
    pub offset: u64,
}

#[derive(Debug)]
pub struct InteriorTablePage {
    pub page_header: PageHeader,
    //pub cell_pointer_array: Vec<u16>,
    pub cells: Vec<TableInteriorCell>,
    pub offset: u64,
}

#[derive(Debug)]
pub struct TableInfo {
    pub root_page_num: I8,
    // column_name -> order
    pub column_orders: BTreeMap<String, usize>,
}

pub struct FirstPage {
    //pub db_header: DbHeader,
    pub page: LeafTablePage,
    pub table_infos: BTreeMap<String, TableInfo>, // TableName->TableInfo
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
pub type I16 = i16;

#[derive(Debug)]
pub enum ColumnType {
    Str,
    I8,
    I16,
    One,
    Null,
}

#[derive(Debug, PartialEq)]
pub enum Column {
    Str(Str),
    I8(I8),
    I16(I16),
    One,
    Null,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Str(s) => write!(f, "{}", s),
            Column::I8(i) => write!(f, "{}", i),
            Column::I16(i) => write!(f, "{}", i),
            Column::One => write!(f, "1"),
            Column::Null => write!(f, "NULL"),
        }
    }
}
