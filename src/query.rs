use anyhow::{anyhow, Result};
use regex::Regex;

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
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

        let columns = caps
            .name("columns")
            .ok_or(anyhow!("can't get table name from query string"))?
            .as_str()
            .to_string();

        let columns: Vec<String> = columns.split(",").map(|c| c.trim().to_string()).collect();

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
