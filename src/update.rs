use pgrx::{
    pg_sys::{self, getTypeOutputInfo, Oid, OidOutputFunctionCall},
    prelude::*,
    PgTupleDesc,
};
use std::ffi::CStr;

#[derive(Debug)]
pub struct UpdateDestReceiver {
    pub value: Option<String>,
    pub column: String,
}

impl UpdateDestReceiver {
    pub fn update_values_column(&mut self, column: &str) {
        self.value = None;
        self.column = String::from(column);
    }
}

pub fn handle_update(
    query_desc: &PgBox<pg_sys::QueryDesc>,
    expected_column: &str,
    update_receiver: &mut UpdateDestReceiver,
) {

    unsafe {
        let estate = *(query_desc.estate);

        let result_rel_info = estate.es_result_relations;

        if !result_rel_info.is_null() {
            let relation_rel = *result_rel_info;
            let relation_desc = (*relation_rel).ri_RelationDesc;

            let tuple_new_slot = *((*relation_rel).ri_newTupleSlot);

            if !relation_desc.is_null() {
                let relation_descp: pgrx::prelude::pg_sys::RelationData = *relation_desc;
                let tuple_desc = PgTupleDesc::from_pg_unchecked(relation_descp.rd_att);
                let natts = tuple_desc.natts;
                for i in 0..natts {
                    let is_null = *tuple_new_slot.tts_isnull.add(i as usize);
                    if !is_null {
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
                        let output_value = output_value
                            .to_str()
                            .expect("Failed to convert Postgres query string for rust");

                        if attr == expected_column {
                            update_receiver.value = Some(output_value.to_string());
                            update_receiver.column = attr.to_string();
                        }
                    }
                }
            }
        }
    }
}
