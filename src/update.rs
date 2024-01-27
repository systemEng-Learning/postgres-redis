use pgrx::prelude::*;
use regex::Regex;
use std::ffi::CStr;

pub fn handle_update(query_desc: &PgBox<pg_sys::QueryDesc>, expectedtable_name: &str) {
    let c_str = unsafe { CStr::from_ptr(query_desc.sourceText) };
    let rust_str = c_str.to_str().expect("Failed to convert CStr to str");

    if let Some(table_name) = extract_table_name(rust_str) {
        if table_name == expectedtable_name {
            let s = format!(
                "PostgresRedis > Hello World!!! here is your update query: {}",
                rust_str
            );
            ereport!(
                PgLogLevel::NOTICE,
                PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                s
            );
        }
    }
}

fn extract_table_name(query_text: &str) -> Option<&str> {
    let re = Regex::new(r"\b(?i)FROM\s+(\w+)|\b(?i)UPDATE\s+(\w+)").unwrap();
    if let Some(captures) = re.captures(query_text) {
        if let Some(table_name) = captures.get(1).or(captures.get(2)) {
            return Some(table_name.as_str());
        }
    }
    None
}
