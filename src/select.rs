use pgrx::pg_sys::{
    getTypeOutputInfo, slot_getsomeattrs_int, CommandDest, CommandDest_DestNone, Datum,
    DestReceiver, Oid, OidOutputFunctionCall, TupleDesc, TupleTableSlot,
};
use std::ffi::CStr;
use std::os::raw::c_int;

/// This struct is an implementation of the postgres DestReceiver. It includes all the
/// required members (including functions). It includes additional members like the
/// original destreceiver from the querydesc, the required column, value that will hold
/// the extracted tuple value and a flag to detect if multiple or single rows are returned.
///
/// The original destreceiver is needed so that any calls to this destreceiver in the execution
/// run stage will ensure that the same call is made to the original one. This ensures that this
/// plugin does not change the normal execution mode.
#[repr(C)]
#[allow(non_snake_case)]
#[derive(Debug, Clone)]
pub struct CustomDestReceiver {
    pub receiveSlot:
        Option<unsafe extern "C" fn(_: *mut TupleTableSlot, _: *mut DestReceiver) -> bool>,
    pub rStartup: Option<
        unsafe extern "C" fn(self_: *mut DestReceiver, operation: c_int, typeinfo: TupleDesc),
    >,
    pub rShutdown: Option<unsafe extern "C" fn(self_: *mut DestReceiver)>,
    pub rDestroy: Option<unsafe extern "C" fn(self_: *mut DestReceiver)>,
    pub mydest: CommandDest,
    pub original_dest: Option<*mut DestReceiver>,
    pub column: String,
    pub value: Option<String>,
    pub is_single: bool,
}

pub fn create_custom_dest_receiver(column: &str) -> CustomDestReceiver {
    CustomDestReceiver {
        receiveSlot: Some(receive),
        rStartup: Some(startup),
        rShutdown: Some(shutdown),
        rDestroy: Some(destroy),
        mydest: CommandDest_DestNone,
        original_dest: None,
        column: String::from(column),
        value: None,
        is_single: true,
    }
}

/// This receives a tuple from the select query executor and extracts the value of the receiver
/// `column` member. The single flag ensures that this receiver extracts the tuple value if
/// the query returns a single row. If multiple rows are returned, the previous value is discarded
/// and extraction never happens again.
///
/// Once extraction is carried out, the tuple is sent to the `receive` function of the original
/// destreceiver.
pub extern "C" fn receive(slot: *mut TupleTableSlot, receiver: *mut DestReceiver) -> bool {
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = &mut *custom_receiver;
        if custom_receiver.value.is_some() {
            custom_receiver.value = None;
            custom_receiver.is_single = false;
        }
        if custom_receiver.is_single {
            let typeinfo = (*slot).tts_tupleDescriptor;
            let tinfo = &(*typeinfo);
            let nattrs = tinfo.natts as usize;
            let attrs = tinfo.attrs.as_slice(nattrs);
            let mut typoutput: Oid = Oid::default();
            let mut typvarlena: bool = false;
            for i in 0..nattrs {
                let attr = slot_getattr(slot, i + 1);
                if attr.is_none() {
                    continue;
                }
                let attr = attr.unwrap();
                getTypeOutputInfo(
                    attrs[i].atttypid,
                    &mut typoutput as *mut Oid,
                    &mut typvarlena as *mut bool,
                );
                let value = OidOutputFunctionCall(typoutput, attr);
                let value = CStr::from_ptr(value);
                let value = value
                    .to_str()
                    .expect("Failed to convert Postgres query string for rust");
                if attrs[i].name() == custom_receiver.column {
                    custom_receiver.value = Some(value.to_string());
                }
            }
        }
        let custom_receiver = &*custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.receiveSlot {
            r(slot, custom_receiver.original_dest.unwrap());
        }
    }
    true
}

/// Run the startup function of the original destreceiver.
pub extern "C" fn startup(receiver: *mut DestReceiver, operation: c_int, typeinfo: TupleDesc) {
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = &*custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.rStartup {
            r(custom_receiver.original_dest.unwrap(), operation, typeinfo);
        }
    }
}

/// Run the shutdown function of the original destreceiver.
pub extern "C" fn shutdown(receiver: *mut DestReceiver) {
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = &*custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.rShutdown {
            r(custom_receiver.original_dest.unwrap());
        }
    }
}

/// Run the destroy function of the original destreceiver.
pub extern "C" fn destroy(receiver: *mut DestReceiver) {
    let custom_receiver = receiver as *mut CustomDestReceiver;
    unsafe {
        let custom_receiver = &*custom_receiver;
        let original_receiver = *(custom_receiver.original_dest.unwrap());
        if let Some(r) = original_receiver.rDestroy {
            r(custom_receiver.original_dest.unwrap());
        }
    }
}

/// A Rust port of the same named Postgres function
/// https://github.com/postgres/postgres/blob/422041542f313f23ca66cad26e9b2b99c4d1999a/src/include/executor/tuptable.h#L396
unsafe fn slot_getattr(slot: *mut TupleTableSlot, attnum: usize) -> Option<Datum> {
    let real_slot = &(*slot);
    if attnum as i16 > real_slot.tts_nvalid {
        slot_getsomeattrs_int(slot, attnum as i32);
    }

    let is_null = *real_slot.tts_isnull.offset(attnum as isize - 1);

    if is_null {
        return None;
    }
    let datum = *real_slot.tts_values.offset(attnum as isize - 1);
    Some(datum)
}
