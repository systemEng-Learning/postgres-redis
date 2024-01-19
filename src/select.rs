use pgrx::prelude::*;

pub fn handle_select(query_desc: &PgBox<pg_sys::QueryDesc>) {
    let s = format!("PostgresRedis > Hello Select statement with operation number {}", query_desc.operation);
    ereport!(PgLogLevel::NOTICE, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, s)
}