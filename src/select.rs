use pgrx::{
    pg_sys::{self, getTypeOutputInfo, rt_fetch, slot_getsomeattrs_int, CommandDest, CommandDest_DestNone, Datum, DestReceiver, Oid, OidOutputFunctionCall, TupleDesc, TupleTableSlot, RELKIND_RELATION},
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

#[repr(C)]
#[allow(non_snake_case)]
#[derive(Debug, Copy, Clone)]
pub struct CustomDestReceiver {
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
}

pub const fn create_custom_dest_receiver() -> CustomDestReceiver {
    CustomDestReceiver {
        receiveSlot: Some(receive),
        rStartup: Some(startup),
        rShutdown: Some(shutdown),
        rDestroy: Some(destroy),
        mydest: CommandDest_DestNone,
        original_dest: None,
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
        let typeinfo = (*slot).tts_tupleDescriptor;
        let tinfo = &(*typeinfo);
        let nattrs = tinfo.natts as usize;
        let attrs = tinfo.attrs.as_slice(nattrs);
        let mut typoutput: Oid = Oid::default();
        let mut typvarlena: bool = false;
        for i in 0..nattrs {
            let attr = slot_getattr(slot, i+1);
            if attr.is_none() {
                continue;
            }
            let attr = attr.unwrap();
            getTypeOutputInfo(attrs[i].atttypid, 
                &mut typoutput as *mut Oid, &mut typvarlena as *mut bool);
            let value = OidOutputFunctionCall(typoutput, attr);
            let value = CStr::from_ptr(value);
            let value = value
                .to_str()
                .expect("Failed to convert Postgres query string for rust");
            println!("r\t{i}: = \"{value}\" {}\t(typeid = {}, len = {}, typmod = {}, byval = {})", 
                    attrs[i].name(), attrs[i].atttypid.as_u32(),
                    attrs[i].attlen, attrs[i].type_mod(), attrs[i].attbyval);
        }
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
        let tinfo = &(*typeinfo);
        let nattrs = tinfo.natts as usize;
        let attrs = tinfo.attrs.as_slice(nattrs);
        for i in 0..nattrs {
            println!("s\t{i}: {}\t(typeid = {}, len = {}, typmod = {}, byval = {})", 
                    attrs[i].name(), attrs[i].atttypid.as_u32(),
                    attrs[i].attlen, attrs[i].type_mod(), attrs[i].attbyval);
        }
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

unsafe fn slot_getattr(slot: *mut TupleTableSlot, attnum: usize) -> Option<Datum> {
    let real_slot = &(*slot);
    if attnum as i16 > real_slot.tts_nvalid  {
        slot_getsomeattrs_int(slot, attnum as i32);
    }

    let is_null = *real_slot.tts_isnull.offset(attnum as isize-1);

    if is_null {
        return None;
    }
	let datum = *real_slot.tts_values.offset(attnum as isize - 1);
    Some(datum)
}
