use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Result};
use regex::Regex;

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: HashMap<String, usize>,
    pub where_column: Option<String>,
    pub where_value: Option<String>,
}

impl SelectQuery {
    pub fn from_query_string(query_string: &str) -> Result<SelectQuery> {
        let re = Regex::new(r"(?i)SELECT (?P<columns>[,|\s|\w]+) FROM (?P<table>\w+)(?: WHERE(?P<condition>[\s|\w]+=['\s|\w]+))?").unwrap();
        let caps = re.captures(query_string).unwrap();

        let table_name = caps
            .name("table")
            .ok_or(anyhow!("can't get table name from query string"))?
            .as_str()
            .to_string();

        let column_caps = caps
            .name("columns")
            .ok_or(anyhow!("can't get table name from query string"))?
            .as_str()
            .to_string();

        let mut columns: HashMap<String, usize> = Default::default();
        for (i, c) in column_caps
            .split(",")
            .map(|c| c.trim().to_string())
            .enumerate()
        {
            columns.insert(c, i);
        }

        let condition = caps.name("condition");

        let mut where_column = None;
        let mut where_value = None;
        if condition.is_some() {
            let mut parts = condition.unwrap().as_str().split('=');
            where_column = Some(
                parts
                    .next()
                    .ok_or(anyhow!("can't parse where_column from condition"))?
                    .trim()
                    .trim_matches('\'')
                    .to_string(),
            );
            where_value = Some(
                parts
                    .next()
                    .ok_or(anyhow!("can't parse where_value from condition"))?
                    .trim()
                    .trim_matches('\'')
                    .to_string(),
            );
        };

        Ok(Self {
            table_name,
            columns,
            where_column,
            where_value,
        })
    }
}

#[derive(Debug)]
pub struct CreateTableQuery {
    pub column_orders: BTreeMap<String, usize>,
}

impl CreateTableQuery {
    pub fn from_sql(sql: &str) -> anyhow::Result<CreateTableQuery> {
        let re =
            Regex::new(r#"CREATE TABLE \"?\w+\"?\n?\s?\(\n?(?P<columns>(?:\n|.)+)\)"#).unwrap();
        let caps = re
            .captures(sql)
            .ok_or(anyhow!("can't parse columns from {}", sql))?;
        let columns = &caps["columns"];
        let mut column_orders = BTreeMap::new();
        for (i, mut c) in columns.split(",").enumerate() {
            c = c.trim();
            if c.starts_with('"') {
                c = c
                    .split('"')
                    .nth(1)
                    .ok_or(anyhow!("bad format of the column {c}"))?;
                column_orders.insert(c.to_string(), i);
                continue;
            }
            c = c
                .trim()
                .split(" ")
                .next()
                .ok_or(anyhow!("bad format of the column {c}"))?;

            column_orders.insert(c.to_string(), i);
        }
        Ok(CreateTableQuery { column_orders })
    }
}

#[derive(Debug)]
pub struct CreateIdxQuery {
    pub idx_name: String,
    #[allow(dead_code)]
    pub table_name: String,
    pub columns: HashSet<String>,
}

impl CreateIdxQuery {
    pub fn from_sql(sql: &str) -> anyhow::Result<CreateIdxQuery> {
        let re = Regex::new(
            r#"CREATE INDEX (?P<idx_name>.+)\s+on (?P<table_name>.+) ((?P<columns>.+))"#,
        )
        .unwrap();

        let caps = re
            .captures(sql)
            .ok_or(anyhow!("can't parse create index query from {}", sql))?;
        let idx_name = caps["idx_name"].to_string();
        let table_name = caps["table_name"].to_string();
        let columns_str = caps["columns"].trim_matches('(').trim_matches(')');

        let mut columns = HashSet::new();
        for c in columns_str.split(',') {
            columns.insert(c.to_string());
        }

        Ok(CreateIdxQuery {
            idx_name,
            table_name,
            columns,
        })
    }
}

pub enum CreateQuery {
    CreateIdx(CreateIdxQuery),
    CreateTable(CreateTableQuery),
}

impl CreateQuery {
    pub fn from_sql(sql: &str) -> anyhow::Result<CreateQuery> {
        match sql {
            s if s.starts_with("CREATE TABLE") => {
                CreateTableQuery::from_sql(sql).map(CreateQuery::CreateTable)
            }
            s if s.starts_with("CREATE INDEX") => {
                CreateIdxQuery::from_sql(sql).map(CreateQuery::CreateIdx)
            }
            _ => todo!("can't parse create query {sql}"),
        }
    }
}
