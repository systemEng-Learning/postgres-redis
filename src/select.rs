use pgrx::{
    pg_sys::{self, rt_fetch, CommandDest, CommandDest_DestNone, DestReceiver, TupleDesc, TupleTableSlot, RELKIND_RELATION},
    prelude::*,
};
use std::ffi::CStr;
use std::os::raw::c_int;

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

pub type CustomDestReceiver = _CustomDestReceiver;
#[repr(C)]
#[allow(non_snake_case)]
#[derive(Debug, Copy, Clone)]
pub struct _CustomDestReceiver {
    pub receiveSlot: Option<unsafe extern "C" fn(_: *mut TupleTableSlot, _: *mut DestReceiver) -> bool>,
    pub rStartup: Option<
        unsafe extern "C" fn(
            self_: *mut DestReceiver,
            operation: c_int,
            typeinfo: TupleDesc,
        ),
    >,
    pub rShutdown: Option<unsafe extern "C" fn(self_: *mut DestReceiver)>,
    pub rDestroy: Option<unsafe extern "C" fn(self_: *mut DestReceiver)>,
    pub mydest: CommandDest,
    pub original_dest: Option<*mut DestReceiver>,
    pub wow: u64,
}

pub const fn create_custom_dest_receiver() -> CustomDestReceiver {
    CustomDestReceiver {
        receiveSlot: Some(receive),
        rStartup: Some(startup),
        rShutdown: Some(shutdown),
        rDestroy: Some(destroy),
        mydest: CommandDest_DestNone,
        original_dest: None,
        wow: 98
    }
}

pub extern "C" fn receive(slot: *mut TupleTableSlot, receiver: *mut DestReceiver) -> bool {
    let s = format!("PostgresRedis > Receiving");
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        s
    );
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = *custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.receiveSlot {
            r(slot, custom_receiver.original_dest.unwrap());
        }
    }
    true
}

pub extern "C" fn startup(receiver: *mut DestReceiver, operation: c_int, typeinfo: TupleDesc) {
    let s = format!("PostgresRedis > Starting");
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        s
    );
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = *custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.rStartup {
            r(custom_receiver.original_dest.unwrap(), operation, typeinfo);
        }
    }
}

pub extern "C" fn shutdown(receiver: *mut DestReceiver) {
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = *custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.rShutdown {
            r(custom_receiver.original_dest.unwrap());
        }
    }
}

pub extern "C" fn destroy(receiver: *mut DestReceiver) {
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = *custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.rDestroy {
            r(custom_receiver.original_dest.unwrap());
        }
    }
}


