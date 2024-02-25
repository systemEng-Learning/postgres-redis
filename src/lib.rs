use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::{CmdType_CMD_SELECT, CmdType_CMD_UPDATE, DestReceiver};
use pgrx::{prelude::*, register_hook, HookResult, PgHooks};
use prshmem::{add_item, init_redis_buffer, move_redis_data, Info};
use select::{create_custom_dest_receiver, CustomDestReceiver};
use update::UpdateDestReceiver;

pub mod prshmem;
pub mod select;
pub mod update;

pgrx::pg_module_magic!();

struct PRHook {
    custom_receiver: Option<CustomDestReceiver>,
    table: Option<String>,
    update_receiver: Option<UpdateDestReceiver>,
}

impl PgHooks for PRHook {
    fn executor_start(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        eflags: i32,
        prev_hook: fn(query_desc: PgBox<pg_sys::QueryDesc>, eflags: i32) -> HookResult<()>,
    ) -> HookResult<()> {
        self.table = Some(String::from("test"));
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
        if op == CmdType_CMD_SELECT
            && select::is_contain_table(&query_desc, self.table.as_ref().unwrap())
        {
            let mut custom_receiver: CustomDestReceiver =
                create_custom_dest_receiver("description");
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
        let op = query_desc.operation;
        if op == CmdType_CMD_SELECT {
            select::handle_select(&query_desc, "test", &self.custom_receiver);
        } else if op == CmdType_CMD_UPDATE {
            let mut new_update_receiver = UpdateDestReceiver {
                values: vec![],
                column: String::new(),
            };
            update::handle_update(&query_desc, "test", "description", &mut new_update_receiver);
            self.update_receiver = Some(new_update_receiver);
        }
        prev_hook(query_desc)
    }

    fn commit(&mut self) {
        if !self.custom_receiver.is_none() {
            let custom_receiver = self.custom_receiver.as_ref().unwrap();
            if custom_receiver.values.len() > 0 {
                let t = custom_receiver.values.join(", ");
                let s = format!(
                    "PostgresRedis > The values of column {} in table {} are {t}",
                    custom_receiver.column,
                    self.table.as_ref().unwrap()
                );
                let mut content = [' '; 255];
                for (i, c) in s.chars().enumerate() {
                    content[i] = c;
                }
                let info = Info {
                    content,
                    length: s.len() as u8,
                };
                add_item(info);
            }
            self.custom_receiver = None;
        } else if !self.update_receiver.is_none() {
            let update_receiver = self.update_receiver.as_ref().unwrap();
            if update_receiver.values.len() > 0 {
                let t = update_receiver.values.join(", ");
                let s = format!(
                    "PostgresRedis > The values updated of column {} in table {} are {t}",
                    update_receiver.column,
                    self.table.as_ref().unwrap()
                );
                let mut content = [' '; 255];
                for (i, c) in s.chars().enumerate() {
                    content[i] = c;
                }
                let info = Info {
                    content,
                    length: s.len() as u8,
                };
                add_item(info);
            }
            self.update_receiver = None;
        }
    }

    fn abort(&mut self) {
        if self.custom_receiver.is_some() {
            self.custom_receiver = None;
        } else if self.update_receiver.is_some() {
            self.update_receiver = None;
        }
    }
}

static mut HOOK: PRHook = PRHook {
    custom_receiver: None,
    table: None,
    update_receiver: None,
};

#[pg_extern]
fn hello_postgres_redis() -> &'static str {
    "Hello, postgres_redis"
}
unsafe fn init_hook() {
    register_hook(&mut HOOK);
}

#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
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

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        let results = move_redis_data();
        for i in results.iter() {
            let s = &i.content[0..i.length as usize];
            let s2: String = s.iter().collect();
            log!("From bg: {s2}");
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
