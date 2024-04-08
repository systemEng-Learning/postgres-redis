use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::{CmdType_CMD_SELECT, CmdType_CMD_UPDATE, DestReceiver};
use pgrx::{prelude::*, register_hook, HookResult, PgHooks};
use prshmem::{add_item, init_redis_buffer, move_redis_data, Info};
use select::{create_custom_dest_receiver, CustomDestReceiver};
use update::UpdateDestReceiver;
pub mod gucs;
pub mod prshmem;
pub mod select;
pub mod update;
pub mod utils;

pgrx::pg_module_magic!();

struct PRHook {
    custom_receiver: Option<CustomDestReceiver>,
    update_receiver: Option<UpdateDestReceiver>,
    where_clause_receiver: Option<(String, String)>,
    table: Option<String>,
    key_column: Option<String>,
    value_column: Option<String>,
    keep_running: bool,
}

impl PgHooks for PRHook {
    fn planner(
        &mut self,
        parse: PgBox<pg_sys::Query>,
        query_string: *const std::os::raw::c_char,
        cursor_options: i32,
        bound_params: PgBox<pg_sys::ParamListInfoData>,
        prev_hook: fn(
            parse: PgBox<pg_sys::Query>,
            query_string: *const std::os::raw::c_char,
            cursor_options: i32,
            bound_params: PgBox<pg_sys::ParamListInfoData>,
        ) -> HookResult<*mut pg_sys::PlannedStmt>,
    ) -> HookResult<*mut pg_sys::PlannedStmt> {
        self.keep_running = utils::is_contain_table(parse.rtable, self.table.as_ref().unwrap());
        if !self.keep_running {
            return prev_hook(parse, query_string, cursor_options, bound_params);
        }
        unsafe {
            self.where_clause_receiver = utils::get_where_object(
                parse.jointree,
                parse.rtable,
                self.table.as_ref().unwrap(),
                self.key_column.as_ref().unwrap(),
            );
        }
        if self.where_clause_receiver.is_none() {
            self.keep_running = false;
        }
        prev_hook(parse, query_string, cursor_options, bound_params)
    }
    fn executor_start(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        eflags: i32,
        prev_hook: fn(query_desc: PgBox<pg_sys::QueryDesc>, eflags: i32) -> HookResult<()>,
    ) -> HookResult<()> {
        prev_hook(query_desc, eflags)
    }
    fn executor_run(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        direction: pg_sys::ScanDirection,
        count: u64,
        execute_once: bool,
        prev_hook: fn(
            query_desc: PgBox<pg_sys::QueryDesc>,
            direction: pg_sys::ScanDirection,
            count: u64,
            execute_once: bool,
        ) -> pgrx::HookResult<()>,
    ) -> pgrx::HookResult<()> {
        let op = query_desc.operation;
        if op == CmdType_CMD_SELECT && self.keep_running {
            let mut custom_receiver: CustomDestReceiver =
                create_custom_dest_receiver(self.value_column.as_ref().unwrap());
            custom_receiver.original_dest = Some(query_desc.dest);
            let new_query_desc;
            unsafe {
                let d = *query_desc.dest;
                custom_receiver.mydest = d.mydest;
                let s = &mut custom_receiver as *mut CustomDestReceiver;
                let t = s as *mut DestReceiver;
                let q = query_desc.into_pg();
                (*q).dest = t;
                new_query_desc = PgBox::from_pg(q);
            }
            prev_hook(new_query_desc, direction, count, execute_once);
            self.custom_receiver = Some(custom_receiver);
        } else {
            prev_hook(query_desc, direction, count, execute_once);
        }
        HookResult::new(())
    }

    fn executor_end(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        prev_hook: fn(query_desc: PgBox<pg_sys::QueryDesc>) -> pgrx::HookResult<()>,
    ) -> pgrx::HookResult<()> {
        if !self.keep_running {
            return prev_hook(query_desc);
        }
        let op = query_desc.operation;
        if op == CmdType_CMD_UPDATE {
            let mut new_update_receiver = UpdateDestReceiver {
                value: None,
                column: String::new(),
            };
            update::handle_update(
                &query_desc,
                self.value_column.as_ref().unwrap(),
                &mut new_update_receiver,
            );
            self.update_receiver = Some(new_update_receiver);
        }
        prev_hook(query_desc)
    }

    fn commit(&mut self) {
        if self.custom_receiver.is_some() {
            let custom_receiver = self.custom_receiver.as_ref().unwrap();
            let key_string = &(self.where_clause_receiver.as_ref().unwrap().1);
            if custom_receiver.value.is_some() {
                let t = custom_receiver.value.as_ref().unwrap();
                add_item(Info::new(key_string, t));
            }
            self.custom_receiver = None;
        } else if self.update_receiver.is_some() {
            let update_receiver = self.update_receiver.as_ref().unwrap();
            let key_string = &(self.where_clause_receiver.as_ref().unwrap().1);
            if update_receiver.value.is_some() {
                let t = update_receiver.value.as_ref().unwrap();
                add_item(Info::new(key_string, t));
            }
            self.update_receiver = None;
        }

        if self.where_clause_receiver.is_some() {
            self.where_clause_receiver = None;
        }
        self.keep_running = true;
    }

    fn abort(&mut self) {
        if self.custom_receiver.is_some() {
            self.custom_receiver = None;
        }

        if self.update_receiver.is_some() {
            self.update_receiver = None;
        }

        if self.where_clause_receiver.is_some() {
            self.where_clause_receiver = None;
        }
        self.keep_running = true;
    }
}

static mut HOOK: PRHook = PRHook {
    custom_receiver: None,
    update_receiver: None,
    where_clause_receiver: None,
    table: None,
    key_column: None,
    value_column: None,
    keep_running: true,
};

#[pg_extern]
fn hello_postgres_redis() -> &'static str {
    "Hello, postgres_redis"
}

unsafe fn init_hook() {
    if gucs::PGD_REDIS_TABLE.get().is_none() {
        log!("Table name is not set");
        return;
    }
    if gucs::PGD_KEY_COLUMN.get().is_none() {
        log!("Table key column is not set");
        return;
    }
    if gucs::PGD_VALUE_COLUMN.get().is_none() {
        log!("Table value column is not set");
        return;
    }
    let table_name = gucs::PGD_REDIS_TABLE
        .get()
        .unwrap()
        .to_str()
        .expect("table name extraction failed");

    let key_column = gucs::PGD_KEY_COLUMN
        .get()
        .unwrap()
        .to_str()
        .expect("key column extraction failed");

    let value_column = gucs::PGD_VALUE_COLUMN
        .get()
        .unwrap()
        .to_str()
        .expect("value column extraction failed");

    HOOK.table = Some(table_name.to_string());
    HOOK.key_column = Some(key_column.to_string());
    HOOK.value_column = Some(value_column.to_string());
    register_hook(&mut HOOK);
}

#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    gucs::init();
    init_redis_buffer();
    init_hook();
    BackgroundWorkerBuilder::new("PGRedis Experiment")
        .set_function("postgres_redis_background")
        .set_library("postgres_redis")
        .enable_shmem_access(None)
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn postgres_redis_background() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    log!(
        "Hello from inside the {} BGWorker",
        BackgroundWorker::get_name()
    );
    if gucs::PGD_REDIS_URL.get().is_none() {
        log!("Redis URL is not set");
        return;
    }
    let url = gucs::PGD_REDIS_URL
        .get()
        .unwrap()
        .to_str()
        .expect("URL extraction failed");
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_connection().unwrap();
    let mut pipe = redis::pipe();

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        let results = move_redis_data();
        for i in results.iter() {
            let key: String = i.key[0..i.key_length as usize].iter().collect();
            let value: String = i.value[0..i.value_length as usize].iter().collect();
            log!("From bg: {key} => {value}");
            pipe.set(key, value).ignore();
        }
        if results.len() > 0 {
            let _: () = pipe.query(&mut connection).unwrap();
            pipe.clear();
        }
    }
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
