use std::ffi::CStr;

use pgrx::{
    is_a, list,
    pg_sys::{
        self, eval_const_expressions, getTypeOutputInfo, get_attname, rt_fetch, BoolExpr, FromExpr,
        List, Node, NodeTag, Oid, OidOutputFunctionCall, OpExpr, TextEqualOperator,
        RELKIND_RELATION,
    },
};

/// This function returns the column name and value of the WHERE clause of the query.
/// This column name is the `key_column_name` argument. This function only extracts
/// the needed result if the filter is an equal filter. It does not work for like filter.
///
/// There are only 2 types of sql expressions that this function can work with. The first
/// one is an `opexpr` e.g a = b. This one is straightforward to process. The function gets
/// b if a is the `key_column_name`. The second expression is a `boolexpr` e.g a = b or c = d
/// and e = f. This one contains multiple `opexpr`. All other expressions are ignored.
pub unsafe fn get_where_object(
    jointree: *mut FromExpr,
    range_table: *mut List,
    table_name: &str,
    key_column_name: &str,
) -> Option<(String, String)> {
    let jointree = *jointree;
    let quals: *mut pg_sys::Node = jointree.quals;
    let quals_node = eval_const_expressions(std::ptr::null_mut(), quals.cast());
    let mut opexprs = vec![];
    let mut boolexprs = vec![];
    let mut result = None;
    if is_a(quals_node.cast(), NodeTag::T_OpExpr) {
        opexprs.push(quals_node.cast::<OpExpr>());
    }

    if is_a(quals_node.cast(), NodeTag::T_BoolExpr) {
        boolexprs.push(quals_node.cast::<BoolExpr>());
    }

    // A boolexpr can contain multiple opexprs and boolexprs. Processing the content
    // of a boolexpr can be recursive. This section uses a stack related method of
    // solving a recursive problem. It processes all the boolexpr in the
    // boolexprs vector. Each expression in the currently processed boolexpr is checked
    // for its type. If its type is an opexpr, it is added to the opexprs vector. If its
    // type is a boolexpr, it is added to the boolexprs vector to be processed later.
    while let Some(boolexpr) = boolexprs.pop() {
        let args = (*boolexpr).args;
        let l = &(*args);
        for i in 0..l.length {
            let f = node_fetch(args, i as usize);
            let t = f.cast::<Node>();
            if is_a(t.cast(), NodeTag::T_OpExpr) {
                opexprs.push(t.cast::<OpExpr>());
            } else if is_a(t.cast(), NodeTag::T_BoolExpr) {
                boolexprs.push(t.cast::<BoolExpr>());
            }
        }
    }

    // This processes all the expressions in the opexprs vector. It sets result to the
    // a tuple of the var part and const part of the expression if the var part is
    // the `key_column_name` parameter.
    for node in opexprs {
        let op_expr_pointer = node.cast::<OpExpr>();
        let op_expr = *op_expr_pointer;
        let op_number = Oid::from(416);

        // Only process if the op is an `equal` sign. The operation number is different
        // for number comparisons and text comparisons.
        if op_expr.opno == op_number || op_expr.opno == Oid::from(TextEqualOperator) {
            let args = op_expr.args;
            let argg = args.as_ref().unwrap();
            let first_cell = argg.elements.add(0);
            let first_value = first_cell.as_ref().unwrap().ptr_value;
            let second_cell = argg.elements.add(1);
            let second_value = second_cell.as_ref().unwrap().ptr_value;
            let mut first_node = first_value.cast::<Node>();
            let second_node = second_value.cast::<Node>();

            // Get argument of the first_node if it's of the relabel type.
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

                if (*rte).relkind as u8 != RELKIND_RELATION {
                    continue;
                }
                let tbl_data = *(*rte).eref;
                let tbl_name = CStr::from_ptr(tbl_data.aliasname);
                let tbl_name = tbl_name
                    .to_str()
                    .expect("Failed to convert Postgres query string for rust");

                if table_name != tbl_name {
                    continue;
                }

                let rte_relid = rte.as_ref().unwrap().relid;
                let col_name = get_attname(rte_relid, var_attid, true);
                let col_name_str = CStr::from_ptr(col_name).to_str().unwrap();

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

                if col_name_str == key_column_name {
                    result = Some((String::from(col_name_str), qual_value.to_string()));
                    break;
                }
            }
        }
    }
    result
}

/// Get the nth node from a list of nodes. The `rt_fetch` is the template for this
/// function.
unsafe fn node_fetch(nodes: *mut List, index: usize) -> *mut Node {
    list::List::<*mut core::ffi::c_void>::downcast_ptr(nodes)
        .expect("node_fetch used on non-ptr List")
        .get(index)
        .expect("node_fetch used out-of-bounds")
        .cast()
}

/// This function returns true if the `expected_table_name` is in the list of tables.
pub fn is_contain_table(table_lists: *mut List, expected_table_name: &str) -> bool {
    let mut result = false;
    unsafe {
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
                result = true;
                break;
            }
        }
    }
    result
}
