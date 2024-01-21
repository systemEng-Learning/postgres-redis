use pgrx::prelude::*;
use std::ffi::CStr;

pub fn handle_select(query_desc: &PgBox<pg_sys::QueryDesc>) {
    let raw_query_string = unsafe { CStr::from_ptr(query_desc.sourceText) };
    let query_string = raw_query_string
        .to_str()
        .expect("Failed to convert Postgres query string for rust");

    let s = format!("PostgresRedis > Hello World!!! Here's your select query {query_string}");
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        s
    )
}
