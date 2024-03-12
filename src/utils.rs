use std::ffi::CStr;

use pgrx::{
    is_a, notice,
    pg_sys::{
        self, eval_const_expressions, getTypeOutputInfo, get_attname, rt_fetch, FromExpr, List,
        Node, NodeTag, Oid, OidOutputFunctionCall, OpExpr, TextEqualOperator,
    },
};

pub unsafe fn get_where_object(
    jointree: *mut FromExpr,
    range_table: *mut List,
) -> Option<(String, String)> {
    let jointree = *jointree;
    let quals: *mut pg_sys::Node = jointree.quals;
    let quals_node = eval_const_expressions(std::ptr::null_mut(), quals.cast());
    if is_a(quals_node.cast(), NodeTag::T_OpExpr) {
        let op_expr_pointer = quals_node.cast::<OpExpr>();
        let op_expr = *op_expr_pointer;
        let op_number = Oid::from(416);

        if op_expr.opno == op_number || op_expr.opno == Oid::from(TextEqualOperator) {
            let args = op_expr.args;
            let argg = args.as_ref().unwrap();
            let first_cell = argg.elements.add(0);
            let first_value = first_cell.as_ref().unwrap().ptr_value;
            let second_cell = argg.elements.add(1);
            let second_value = second_cell.as_ref().unwrap().ptr_value;
            let mut first_node = first_value.cast::<Node>();
            let second_node = second_value.cast::<Node>();
            if is_a(first_node.cast(), pg_sys::NodeTag::T_RelabelType) {
                let relabel = first_node.cast::<pg_sys::RelabelType>();
                first_node = (*relabel).arg.cast::<pg_sys::Node>();
            }

            if is_a(first_node.cast(), pg_sys::NodeTag::T_Var)
                && is_a(second_node.cast(), pg_sys::NodeTag::T_Const)
            {
                let var: *mut pg_sys::Var = first_node.cast::<pg_sys::Var>();
                let constt: *mut pg_sys::Const = second_node.cast::<pg_sys::Const>();
                let var_attid: i16 = var.as_ref().unwrap().varattno;
                let varno = var.as_ref().unwrap().varno;

                let rte = rt_fetch(varno, range_table);
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
                let s =
                    format!("PostgresRedis > The query qual is  {rte_name_str} =  {qual_value}");
                notice!("{s}");

                return Some((String::from(rte_name_str), qual_value.to_string()));
            }
        }
    }
    None
}
