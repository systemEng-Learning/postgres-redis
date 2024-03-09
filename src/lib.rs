use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::is_a;
use pgrx::pg_sys::{
    eval_const_expressions, getTypeOutputInfo, get_attname, nodeToString, rt_fetch,
    CmdType_CMD_SELECT, CmdType_CMD_UPDATE, DestReceiver, Node, Oid, OidOutputFunctionCall, OpExpr,
    PgNode,
};
use pgrx::{prelude::*, register_hook, void_ptr, HookResult, PgHooks};
use prshmem::{add_item, init_redis_buffer, move_redis_data, Info};
use select::{create_custom_dest_receiver, CustomDestReceiver};
use std::ffi::CStr;
use update::UpdateDestReceiver;
pub mod prshmem;
pub mod select;
pub mod update;
use pgrx::pg_sys::NodeTag::T_OpExpr;

pgrx::pg_module_magic!();

struct PRHook {
    custom_receiver: Option<CustomDestReceiver>,
    table: Option<String>,
    update_receiver: Option<UpdateDestReceiver>,
    where_cluase_receiver: Option<UpdateDestReceiver>,
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
        unsafe {
            let jointree = *(parse.jointree);

            let quals: *mut pg_sys::Node = (jointree.quals);
            let quals_node = eval_const_expressions(std::ptr::null_mut(), quals.cast());
            if is_a(quals_node.cast(), T_OpExpr) {
                let op_expr_pointer = quals_node.cast::<OpExpr>();
                let op_expr = *op_expr_pointer;
                let op_number = Oid::from(416);

                if op_expr.opno == op_number {
                    let args = op_expr.args;
                    let argg = args.as_ref().unwrap();
                    let first_cell = argg.elements.add(0);
                    let first_value = first_cell.as_ref().unwrap().ptr_value;
                    let second_cell = argg.elements.add(1);
                    let second_value = second_cell.as_ref().unwrap().ptr_value;
                    let first_node = first_value.cast::<Node>();
                    let second_node = second_value.cast::<Node>();

                    if is_a(first_node.cast(), pg_sys::NodeTag::T_Var)
                        && is_a(second_node.cast(), pg_sys::NodeTag::T_Const)
                    {
                        let var: *mut pg_sys::Var = first_node.cast::<pg_sys::Var>();
                        let constt: *mut pg_sys::Const = second_node.cast::<pg_sys::Const>();
                        let var_attid: i16 = var.as_ref().unwrap().varattno;
                        let varno = var.as_ref().unwrap().varno;

                        let rte = rt_fetch(varno, parse.rtable);
                        let rte_relid = rte.as_ref().unwrap().relid;
                        let rte_name = get_attname(rte_relid, var_attid, true);
                        let rte_name_str = CStr::from_ptr(rte_name).to_str().unwrap();

                        let consstt = constt.as_ref().unwrap();
                        let const_cons = consstt.constvalue;
                        let const_type = consstt.consttype;
                        let mut foutoid: Oid = Oid::default();
                        let mut typisvarlena: bool = false;
                        getTypeOutputInfo(const_type, &mut foutoid, &mut typisvarlena);
                        let const_type_output = OidOutputFunctionCall(foutoid, const_cons);
                        let qual_value = CStr::from_ptr(const_type_output)
                            .to_str()
                            .expect("Failed to convert Postgres query string for rust");
                        let s = format!(
                            "PostgresRedis > The query qual is  {rte_name_str} =  {qual_value}"
                        );
                        ereport!(
                            PgLogLevel::NOTICE,
                            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                            s
                        );

                        // save in where_cluase_receiver for later use
                        let where_cluase_receiver = UpdateDestReceiver {
                            values: vec![qual_value.to_string()],
                            column: String::from(rte_name_str),
                        };

                        self.where_cluase_receiver = Some(where_cluase_receiver);
                    }
                }
            }
        }
        prev_hook(parse, query_string, cursor_options, bound_params)
    }
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
    where_cluase_receiver: None,
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
