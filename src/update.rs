use pgrx::{
    pg_sys::{self, getTypeOutputInfo, rt_fetch, Oid, OidOutputFunctionCall, RELKIND_RELATION},
    prelude::*,
    PgTupleDesc,
};
use regex::Regex;
use std::ffi::CStr;

pub fn handle_update2(query_desc: &PgBox<pg_sys::QueryDesc>, expectedtable_name: &str) {
    let c_str = unsafe { CStr::from_ptr(query_desc.sourceText) };
    let rust_str = c_str.to_str().expect("Failed to convert CStr to str");

    if let Some(table_name) = extract_table_name(rust_str) {
        if table_name == expectedtable_name {
            let s = format!(
                "PostgresRedis > Hello World!!! here is your update query: {}",
                rust_str
            );
            ereport!(
                PgLogLevel::NOTICE,
                PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                s
            );
        }
    }
}

fn extract_table_name(query_text: &str) -> Option<&str> {
    let re = Regex::new(r"\b(?i)FROM\s+(\w+)|\b(?i)UPDATE\s+(\w+)").unwrap();
    if let Some(captures) = re.captures(query_text) {
        if let Some(table_name) = captures.get(1).or(captures.get(2)) {
            return Some(table_name.as_str());
        }
    }
    None
}

pub fn handle_update(query_desc: &PgBox<pg_sys::QueryDesc>, expected_table_name: &str) {
    let c_str = unsafe { CStr::from_ptr(query_desc.sourceText) };
    let rust_str = c_str.to_str().expect("Failed to convert CStr to str");

    let mut expected_table = false;
    let mut single_row = false;

    unsafe {
        let desc_pointer = *(query_desc.plannedstmt);
        let estate = *(query_desc.estate);
        let result_rel_info = estate.es_result_relations;

        if !result_rel_info.is_null() {
            let relation_rel = *result_rel_info;
            let relation_desc = relation_rel.ri_RelationDesc;

            // let tuple_old_slot: *mut pgrx::prelude::pg_sys::TupleTableSlot =
            //     ((*relation_rel).ri_oldTupleSlot);

            let tuple_new_slot = *(relation_rel.ri_TrigNewSlot);

            if relation_desc.is_null() {
                let s = format!("PostgresRedis > relation_desc is null");
                ereport!(
                    PgLogLevel::NOTICE,
                    PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                    s
                );
            }

            let relation_descp: pgrx::prelude::pg_sys::RelationData = *relation_desc;
            let tuple_desc = PgTupleDesc::from_pg_unchecked(relation_descp.rd_att);
            let natts = tuple_desc.natts;
            let s = format!("PostgresRedis > natts is {}", natts);
            ereport!(
                PgLogLevel::NOTICE,
                PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                s
            );

            for i in 0..natts {
                let is_null = *tuple_new_slot.tts_isnull.add(i as usize);
                if is_null {
                    //rough work
                    // let attrname = CStr::from_ptr(*tuple_desc.attrs.add(i as usize).attname);
                    let desc_pointer = tuple_desc.attrs.as_ptr();
                    let desc_attr = *desc_pointer.add(i as usize);
                    let attr = desc_attr.name();
                    let s = format!("PostgresRedis > Column {} is updated to NULL ", attr);
                    ereport!(
                        PgLogLevel::NOTICE,
                        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                        s
                    );
                } else {
                    let desc_pointer = tuple_desc.attrs.as_ptr();
                    let desc_attr = *desc_pointer.add(i as usize);
                    let attr = desc_attr.name();
                    let value_pointer = *tuple_new_slot.tts_values.add(i as usize);
                    let mut foutoid: Oid = Oid::default();
                    let mut typisvarlena: bool = false;
                    let typoid: Oid = desc_attr.atttypid;
                    getTypeOutputInfo(typoid, &mut foutoid, &mut typisvarlena);
                    let output = OidOutputFunctionCall(foutoid, value_pointer);
                    let output_value = CStr::from_ptr(output);

                    let s = format!(
                        "PostgresRedis > Column {} is updated to  {}",
                        attr,
                        output_value.to_str().unwrap()
                    );
                    ereport!(
                        PgLogLevel::NOTICE,
                        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                        s
                    );
                }
            }
        }

        if estate.es_processed == 1 {
            single_row = true;
        }

        let table_lists = desc_pointer.rtable;
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
            let name = name.to_str().expect("Failed to ocnvert string to rust");
            if name == expected_table_name {
                expected_table = true;
                break;
            }
        }
    }
    if expected_table {
        let s = format!(
            "PostgresRedis > Hello World!!! updated table {expected_table_name} using this query {rust_str}"
        );
        ereport!(
            PgLogLevel::NOTICE,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            s
        );
    }

    if single_row {
        let s = format!("PostgresRedis > Hello World for updating only one row");
        ereport!(
            PgLogLevel::NOTICE,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            s
        );
    }
}
