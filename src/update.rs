use pgrx::prelude::*;
use std::ffi::CStr;

pub fn handle_update(query_desc: &PgBox<pg_sys::QueryDesc>) {
    let c_str = unsafe { CStr::from_ptr(query_desc.sourceText) };
    let rust_str = c_str.to_str().expect("Failed to convert CStr to str");

    let s = format!(
        "PostgresRedis > Hello World!!! here is your update query: {}",
        rust_str
    );
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        s
    )
}
