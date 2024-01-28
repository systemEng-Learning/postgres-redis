use pgrx::pg_sys::{CmdType_CMD_SELECT, CmdType_CMD_UPDATE};
use pgrx::{prelude::*, register_hook, PgHooks};

pub mod select;
pub mod update;

pgrx::pg_module_magic!();

struct PRHook;

impl PgHooks for PRHook {
    fn executor_end(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        prev_hook: fn(query_desc: PgBox<pg_sys::QueryDesc>) -> pgrx::HookResult<()>,
    ) -> pgrx::HookResult<()> {
        let op = query_desc.operation;
        if op == CmdType_CMD_SELECT {
            select::handle_select(&query_desc, "test");
        } else if op == CmdType_CMD_UPDATE {
            update::handle_update(&query_desc);
        }
        prev_hook(query_desc)
    }
}

static mut HOOK: PRHook = PRHook {};

#[pg_extern]
fn hello_postgres_redis() -> &'static str {
    "Hello, postgres_redis"
}
unsafe fn init_hook() {
    register_hook(&mut HOOK);
}

#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    init_hook();
}

extension_sql_file!("../sql/test.sql");

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_postgres_redis() {
        assert_eq!("Hello, postgres_redis", crate::hello_postgres_redis());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
