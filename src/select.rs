use pgrx::{
    pg_sys::{self, rt_fetch, RELKIND_RELATION},
    prelude::*,
};
use std::ffi::CStr;

pub fn handle_select(query_desc: &PgBox<pg_sys::QueryDesc>, expected_table_name: &str) {
    let raw_query_string = unsafe { CStr::from_ptr(query_desc.sourceText) };
    let query_string = raw_query_string
        .to_str()
        .expect("Failed to convert Postgres query string for rust");

    let mut expected_table = false;
    let mut single_row = false;
    unsafe {
        let p = *(query_desc.plannedstmt);
        let estate = *(query_desc.estate);
        if estate.es_processed == 1 {
            single_row = true;
        }

        let table_lists = p.rtable;
        let mut length = 0;
        if !table_lists.is_null() {
            length = table_lists.as_ref().unwrap().length;
        }
        for i in 1..=length {
            let table_entry = *rt_fetch(i as u32, table_lists);
            if table_entry.relkind as u8 != RELKIND_RELATION {
                continue;
            }
            let table_data = *table_entry.eref;
            let name = CStr::from_ptr(table_data.aliasname);
            let name = name
                .to_str()
                .expect("Failed to convert Postgres query string for rust");
            if name == expected_table_name {
                expected_table = true;
                break;
            }
        }
    }
    if expected_table {
        let s = format!("PostgresRedis > Hello World for selecting from {expected_table_name}, Here's your select query {query_string}");
        ereport!(
            PgLogLevel::NOTICE,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            s
        );
    }

    if single_row {
        let s = format!("PostgresRedis > Hello World for selecting only one row");
        ereport!(
            PgLogLevel::NOTICE,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            s
        );
    }
}
