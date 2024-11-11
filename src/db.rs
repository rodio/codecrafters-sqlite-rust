use core::panic;
use std::{collections::BTreeMap, fs::File, os::unix::fs::FileExt};

use crate::{
    page::{
        Column, FirstPage, IdxInfo, IdxInteriorCell, IdxLeafCell, InteriorIdxPage,
        InteriorIdxRecordBody, InteriorTablePage, LeafIdxPage, LeafIdxRecordBody, LeafTableCell,
        LeafTablePage, Page, PageHeader, PageType, RecordBody, RecordHeader, TableInfo,
        TableInteriorCell,
    },
    query::{CreateQuery, SelectQuery},
    util::read_varint,
};
use anyhow::{anyhow, Result};

pub struct Db {
    file: File,
    pub header: DbHeader,
    pub table_infos: BTreeMap<String, TableInfo>, // TableName->TableInfo
    pub idx_infos: BTreeMap<String, IdxInfo>,     // TableName->TableInfo
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
            idx_infos: first_page.idx_infos,
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
        let mut idx_infos = BTreeMap::new();
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
                Column::I8(i) => *i as u32,
                Column::I24(i) => (*i).try_into().unwrap(),
                _ => return Err(anyhow!("wrong format of root page column")),
            };

            let sql_col = cell
                .record_body
                .columns
                .get(4)
                .ok_or(anyhow!("can't get sql for table {table_name} from cell 4"))?;
            let sql = match sql_col {
                Column::Str(s) => s,
                _ => return Err(anyhow!("wrong format of sql column")),
            };

            match CreateQuery::from_sql(sql)? {
                CreateQuery::CreateIdx(query) => {
                    let idx_info = IdxInfo {
                        root_page_num,
                        idx_name: query.idx_name,
                        columns: query.columns,
                    };
                    idx_infos.insert(table_name, idx_info);
                }
                CreateQuery::CreateTable(query) => {
                    let table_info = TableInfo {
                        root_page_num,
                        column_orders: query.column_orders,
                    };
                    table_infos.insert(table_name, table_info);
                }
            }
        }
        Ok(FirstPage {
            page,
            table_infos,
            idx_infos,
        })
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
                let cells = Self::get_leaf_table_cells(
                    cell_pointer_array,
                    &page_header,
                    file,
                    page_offset,
                )?;

                Ok(Page::LeafTable(LeafTablePage { page_header, cells }))
            }
            PageType::InteriorIndex => {
                let cells = Self::get_interior_idx_cells(
                    cell_pointer_array,
                    &page_header,
                    file,
                    page_offset,
                )?;
                Ok(Page::InteriorIdx(InteriorIdxPage { page_header, cells }))
            }
            PageType::InteriorTable => {
                let cells = Self::get_interior_table_cells(
                    cell_pointer_array,
                    &page_header,
                    file,
                    page_offset,
                )?;
                Ok(Page::InteriorTable(InteriorTablePage {
                    page_header,
                    cells,
                }))
            }
            PageType::LeafIndex => {
                let cells =
                    Self::get_leaf_idx_cells(cell_pointer_array, &page_header, file, page_offset)
                        .map_err(|e| anyhow!("can't get leaf idx cells: {e}"))?;
                Ok(Page::LeafIndex(LeafIdxPage { page_header, cells }))
            }
        }
    }

    fn get_leaf_idx_cells(
        cell_pointer_array: Vec<u16>,
        page_header: &PageHeader,
        file: &File,
        page_offset: u64,
    ) -> Result<Vec<IdxLeafCell>> {
        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        let mut buf_varint = [0_u8; 9];
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            pointer += page_offset;
            let mut current_offset = 0_u64;

            // payload size, skipping
            file.read_exact_at(&mut buf_varint, pointer + current_offset)
                .map_err(|e| anyhow!("can't read number of bytes of payload of leaf idx cell: {e} at pointer {pointer}"))?;
            let (_payload_size, o) = read_varint(&buf_varint);
            current_offset += o as u64;

            // record header
            let (record_header, o) = RecordHeader::from_file(file, pointer + current_offset)
                .map_err(|e| anyhow!("can't read record header of leaf idx page: {e}"))?;
            current_offset += o;

            // columns
            let (columns, _) = record_header
                .read_columns(file, pointer + current_offset)
                .map_err(|e| anyhow!("can't read columns of leaf idx page {e} "))?;

            cells.push(IdxLeafCell {
                record_header,
                record_body: LeafIdxRecordBody { columns },
            })
        }

        Ok(cells)
    }

    fn get_interior_idx_cells(
        cell_pointer_array: Vec<u16>,
        page_header: &PageHeader,
        file: &File,
        page_offset: u64,
    ) -> Result<Vec<IdxInteriorCell>> {
        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            pointer += page_offset;
            let mut buf_u32 = [0_u8; 4];
            let mut buf_varint = [0_u8; 9];

            // left child
            file.read_exact_at(&mut buf_u32, pointer)
                .map_err(|e| anyhow!("can't read page number of left child of interior idx cell: {e} at pointer {pointer}"))?;
            let left_child_page_num = u32::from_be_bytes(buf_u32);
            let mut current_offset = 4;

            // payload size, skipping
            file.read_exact_at(&mut buf_varint, pointer + current_offset)
                .map_err(|e| anyhow!("can't read number of bytes of payload of interior idx cell: {e} at pointer {pointer}"))?;
            let (_payload_size, o) = read_varint(&buf_varint);
            current_offset += o as u64;

            // record header
            let (record_header, o) = RecordHeader::from_file(file, pointer + current_offset)?;
            current_offset += o;

            let (columns, o) = record_header.read_columns(file, pointer + current_offset)?;
            current_offset += o;

            file.read_exact_at(&mut buf_varint, pointer + current_offset)?;
            let (rowid, _) = read_varint(&buf_varint);

            cells.push(IdxInteriorCell {
                left_child_page_num,
                record_header,
                record_body: InteriorIdxRecordBody { columns, rowid },
            })
        }

        Ok(cells)
    }

    fn get_interior_table_cells(
        cell_pointer_array: Vec<u16>,
        page_header: &PageHeader,
        file: &File,
        page_offset: u64,
    ) -> Result<Vec<TableInteriorCell>> {
        let mut cells = Vec::with_capacity(page_header.num_cells.into());
        for pointer in &cell_pointer_array {
            let mut pointer = *pointer as u64;
            pointer += page_offset;
            let mut buf_u32 = [0_u8; 4]; // for integers
            let mut buf_varint = [0_u8; 9]; // for varints

            file.read_exact_at(&mut buf_u32, pointer)
                .map_err(|e| anyhow!("can't read cell size: {e} at pointer {pointer}"))?;
            let left_child_page_num = u32::from_be_bytes(buf_u32);

            // rowid:
            file.read_exact_at(&mut buf_varint, pointer + 4)
                .map_err(|e| anyhow!("can't read cell rowid: {e} at pointer {pointer}"))?;
            let (rowid, _) = read_varint(&buf_varint);

            cells.push(TableInteriorCell {
                left_child_page_num,
                rowid,
            })
        }

        Ok(cells)
    }

    fn get_leaf_table_cells(
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

            let mut current_offset = 0_u64;
            // size:
            file.read_exact_at(&mut buf, pointer)
                .map_err(|e| anyhow!("can't read cell size: {e} at pointer {pointer}"))?;
            let (size, o) = read_varint(&buf);
            current_offset += o as u64;

            // rowid:
            file.read_exact_at(&mut buf, pointer + current_offset)
                .map_err(|e| anyhow!("can't read cell rowid: {e} at pointer {pointer}"))?;
            let (rowid, o) = read_varint(&buf);
            current_offset += o as u64;

            let (record_header, o) = RecordHeader::from_file(file, pointer + current_offset)?;
            current_offset += o;

            let (columns, _) = record_header.read_columns(file, pointer + current_offset)?;

            let cell = LeafTableCell {
                size,
                rowid,
                record_header,
                record_body: RecordBody { columns },
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
        if page_type == PageType::InteriorTable || page_type == PageType::InteriorIndex {
            let b: [u8; 4] = page_header[8..12].try_into().unwrap();
            rightmost_pointer = Some(u32::from_be_bytes(b));
        }

        let num_cells = u16::from_be_bytes([page_header[3], page_header[4]]);
        Ok(PageHeader {
            page_type,
            num_cells,
            rightmost_pointer,
            page_offset: offset,
        })
    }

    pub fn execute_select(&self, query: SelectQuery) -> Result<Vec<Vec<String>>> {
        let idx_info = self.idx_infos.get(&query.table_name);
        if idx_info.is_some() {
            let table_name = &query.table_name;

            let table_info = self.table_infos.get(table_name).unwrap();

            let rowids = self
                .query_idx(table_name, &query.where_value.clone().unwrap())?
                .unwrap();

            let root_offset = (table_info.root_page_num - 1) as u64 * self.header.page_size as u64;
            let root_page = self.get_page(root_offset, None)?;

            let mut res: Vec<Vec<String>> = Vec::new();
            for rowid in &rowids {
                let r = self.get_row(&root_page, *rowid, table_info, &query)?;
                if !r.is_empty() {
                    res.push(r);
                }
            }

            return Ok(res);
        }

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
            let pointer = (cell.left_child_page_num - 1) as u64 * self.header.page_size as u64;
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

        let rightmost = self.get_page(
            (interior_page.page_header.rightmost_pointer.unwrap() - 1) as u64
                * self.header.page_size as u64,
            None,
        )?;
        match rightmost {
            Page::LeafTable(leaf) => {
                let mut r = Self::query_leaf_page(&leaf, query, table_info)?;
                res.append(&mut r);
            }
            Page::InteriorTable(interior_child) => {
                let mut r = self.query_interior_page(&interior_child, query, table_info)?;
                res.append(&mut r);
            }
            _ => {
                dbg!("other type");
            }
        }

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
                    Column::Zero => String::from("0"),
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

    pub fn query_idx(&self, table_name: &str, looking_for: &String) -> Result<Option<Vec<i64>>> {
        let idx_info = self
            .idx_infos
            .get(table_name)
            .ok_or(anyhow!("no index for {table_name}"))?;

        let root_page = self.get_page(
            (idx_info.root_page_num - 1) as u64 * self.header.page_size as u64,
            None,
        )?;

        self._query_idx(root_page, looking_for)
    }

    fn _query_idx(&self, root_page: Page, looking_for: &String) -> Result<Option<Vec<i64>>> {
        match root_page {
            Page::LeafTable(_) => panic!("index root page is LeafTablePage"),
            Page::InteriorTable(_) => panic!("index root page is InteriorTablePage"),
            Page::LeafIndex(leaf_idx_page) => self.query_leaf_idx(leaf_idx_page, looking_for),
            Page::InteriorIdx(interior_idx_page) => {
                self.query_interior_idx(interior_idx_page, looking_for)
            }
        }
    }

    fn query_interior_idx(
        &self,
        page: InteriorIdxPage,
        looking_for: &String,
    ) -> Result<Option<Vec<i64>>> {
        if page.cells.is_empty() {
            panic!("page has no cells");
        };

        if page.cells.first().unwrap().record_body.columns.is_empty() {
            panic!("no keys in idx");
        };

        if page.cells.first().unwrap().record_body.columns.len() != 2 {
            todo!("more than one key in index");
        };

        let mut res = Vec::new();

        let first_key = page
            .cells
            .first()
            .unwrap()
            .record_body
            .columns
            .first()
            .unwrap();

        let last_key = page
            .cells
            .last()
            .unwrap()
            .record_body
            .columns
            .first()
            .unwrap();

        if *first_key <= Column::Str(looking_for.clone())
            && *last_key >= Column::Str(looking_for.clone())
        {
            for cell in page.cells {
                if cell.record_body.columns.is_empty() {
                    todo!("no keys in cell");
                }

                if cell.record_body.columns.len() != 2 {
                    todo!("more than one key in index");
                }

                let key = cell.record_body.columns.first().unwrap();

                if *key == Column::Str(looking_for.clone()) {
                    let rowid = match cell.record_body.columns.last().unwrap() {
                        Column::I8(i) => *i as i64,
                        Column::I16(i) => *i as i64,
                        Column::I24(i) => *i as i64,
                        _ => panic!("rowid is not int"),
                    };
                    res.push(rowid);
                }

                let child_page = self.get_page(
                    (cell.left_child_page_num - 1) as u64 * self.header.page_size as u64,
                    None,
                )?;

                if let Some(mut from_children) = self._query_idx(child_page, looking_for)? {
                    res.append(&mut from_children);
                }
            }
        } else {
            let offset = (page.page_header.rightmost_pointer.unwrap() - 1) as u64
                * self.header.page_size as u64;
            let rightmost_page = self
                .get_page(offset, None)
                .map_err(|e| anyhow!("can't get rightmost page: {e}"))?;

            if let Some(mut from_rightmost) = self._query_idx(rightmost_page, looking_for)? {
                res.append(&mut from_rightmost);
            }
        }

        Ok(Some(res))
    }

    fn query_leaf_idx(&self, page: LeafIdxPage, looking_for: &String) -> Result<Option<Vec<i64>>> {
        if page.cells.is_empty() {
            panic!("page has no cells");
        };

        if page.cells.first().unwrap().record_body.columns.is_empty() {
            panic!("no keys in idx");
        };

        if page.cells.first().unwrap().record_body.columns.len() != 2 {
            todo!("more than one key in index");
        };

        let mut res = Vec::new();

        //let first_key = page.cells.first().unwrap();
        //let first_key = first_key.record_body.columns.first().unwrap();
        //let first_key = match first_key {
        //    Column::Str(s) => s,
        //    k => todo!("key is not str: {k}"),
        //};
        //
        //if first_key > looking_for {
        //    return Ok(None);
        //};
        //
        //let last_key = match page
        //    .cells
        //    .last()
        //    .unwrap()
        //    .record_body
        //    .columns
        //    .first()
        //    .unwrap()
        //{
        //    Column::Str(s) => s,
        //    _ => todo!("key is not str"),
        //};
        //
        //if (first_key > looking_for && last_key > looking_for) || last_key < looking_for {
        //    return Ok(None);
        //};

        for cell in &page.cells {
            if cell.record_body.columns.is_empty() {
                todo!("no keys in cell");
            }

            if cell.record_body.columns.len() != 2 {
                todo!("more than one key in index");
            }

            let key = cell.record_body.columns.first().unwrap();

            if *key == Column::Str(looking_for.clone()) {
                let rowid = match cell.record_body.columns.last().unwrap() {
                    Column::I8(i) => *i as i64,
                    Column::I16(i) => *i as i64,
                    Column::I24(i) => *i as i64,
                    _ => panic!("rowid is not int"),
                };
                res.push(rowid);
            }
        }

        Ok(Some(res))
    }

    pub fn get_row(
        &self,
        page: &Page,
        rowid: i64,
        table_info: &TableInfo,
        query: &SelectQuery,
    ) -> Result<Vec<String>> {
        match page {
            Page::LeafTable(leaf_page) => self.get_row_leaf(leaf_page, rowid, table_info, query),
            Page::InteriorTable(interior_page) => {
                self.get_row_interior(interior_page, rowid, table_info, query)
            }
            _ => panic!("can't get row from an index page"),
        }
    }

    fn get_row_interior(
        &self,
        page: &InteriorTablePage,
        rowid: i64,
        table_info: &TableInfo,
        query: &SelectQuery,
    ) -> Result<Vec<String>> {
        //let first_rowid = page.cells.first().unwrap().rowid;
        let last_rowid = page.cells.last().unwrap().rowid;

        if last_rowid >= rowid {
            for cell in &page.cells {
                if rowid <= cell.rowid {
                    let page = self.get_page(
                        (cell.left_child_page_num - 1) as u64 * self.header.page_size as u64,
                        None,
                    )?;
                    return self.get_row(&page, rowid, table_info, query);
                }
            }
        } else {
            let page = self.get_page(
                (page.page_header.rightmost_pointer.unwrap() - 1) as u64
                    * self.header.page_size as u64,
                None,
            )?;
            return self.get_row(&page, rowid, table_info, query);
        }

        Ok(vec![])
    }

    fn get_row_leaf(
        &self,
        page: &LeafTablePage,
        rowid: i64,
        table_info: &TableInfo,
        query: &SelectQuery,
    ) -> Result<Vec<String>> {
        let mut row = vec![String::from(""); query.columns.len()];
        for cell in &page.cells {
            if cell.rowid == rowid {
                for column_name in table_info.column_orders.keys() {
                    let order = table_info.column_orders[column_name];
                    let column = &cell.record_body.columns[order];

                    let column_value = match column {
                        Column::Str(s) => s.to_string(),
                        Column::I8(i) => i.to_string(),
                        Column::I16(i) => i.to_string(),
                        Column::I24(i) => i.to_string(),
                        Column::Zero => String::from("0"),
                        Column::One => String::from("1"),
                        Column::Null => cell.rowid.to_string(),
                    };

                    if query.columns.contains_key(column_name) {
                        row[*query.columns.get(column_name).unwrap()] = column_value;
                    }
                }
                return Ok(row);
            }
        }

        Ok(vec![])
    }
}

pub struct DbHeader {
    pub page_size: u16,
}
