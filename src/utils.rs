use pgrx::{list, PgBox, nodes::is_a};
use pgrx::pg_sys::{self, cstring_to_text, expression_tree_walker, get_tle_by_resno, makeConst, nodeToString, pgrx_list_nth, rt_fetch, AppendState, BitmapAndState, BitmapIndexScan, BitmapOrState, BoolExpr, Const, Expr, HashJoin, IndexOnlyScan, IndexScan, List, MergeAppendState, MergeJoin, NestLoop, Node, NodeTag, Oid, OpExpr, PlanState, RelabelType, SubPlanState, SubqueryScanState, Var, INDEX_VAR, INNER_VAR, OUTER_VAR, RELKIND_RELATION, TEXTOID};
use std::ffi::{c_void, CStr};
use std::ptr;

struct Context<'a> {
    planstate: *mut PlanState,
    table_lists: *mut List,
    table_columns: &'a Vec<String>,
    outer_tlist: *mut List,
    inner_tlist: *mut List,
    index_tlist: *mut List,
    outer_planstate: *mut PlanState,
    inner_planstate: *mut PlanState
}

pub fn is_valid(query_desc: &PgBox<pg_sys::QueryDesc>, expected_table_name: &String, table_columns: &Vec<String>) -> bool {
    let mut result = false;
    unsafe {
        let p = *(query_desc.plannedstmt);

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
                result = true;
                break;
            }
        }

        if !result {
            return false;
        }
        let mut context = Context {planstate: query_desc.planstate, table_lists, table_columns, 
            outer_tlist: ptr::null_mut(), inner_tlist: ptr::null_mut(), index_tlist: ptr::null_mut(),
            outer_planstate: ptr::null_mut(), inner_planstate: ptr::null_mut()};
        let context_ptr: *mut Context = &mut context as *mut Context;
        let context_ptr: *mut c_void = context_ptr as *mut c_void;
        handle_plan_states(query_desc.planstate, context_ptr);
    }
    result
}

unsafe fn handle_plan_states(planstate_ptr: *mut PlanState, context_ptr: *mut c_void) {
    let planstate = *planstate_ptr;
    let original_plan = planstate.plan;
    let plan = *original_plan;
    let mut quals: *mut List = ptr::null_mut();
    let mut indexquals: *mut List = ptr::null_mut();
    match plan.type_ {
        pg_sys::NodeTag::T_IndexOnlyScan => {
            let index_plan = original_plan as *mut IndexOnlyScan;
            indexquals = (*index_plan).indexqual;
            quals = plan.qual;
        },
        pg_sys::NodeTag::T_IndexScan => {
            let index_plan = original_plan as *mut IndexScan;
            indexquals = (*index_plan).indexqualorig;
            quals = plan.qual;
        },
        pg_sys::NodeTag::T_BitmapIndexScan => {
            let index_plan = original_plan as *mut BitmapIndexScan;
            indexquals = (*index_plan).indexqualorig;
            quals = plan.qual;
        },
        pg_sys::NodeTag::T_CteScan | pg_sys::NodeTag::T_SeqScan | pg_sys::NodeTag::T_BitmapHeapScan | pg_sys::NodeTag::T_TidScan | 
        pg_sys::NodeTag::T_SubqueryScan | pg_sys::NodeTag::T_FunctionScan | pg_sys::NodeTag::T_ValuesScan | pg_sys::NodeTag::T_WorkTableScan | 
        pg_sys::NodeTag::T_ForeignScan | pg_sys::NodeTag::T_ModifyTable => {
            quals = plan.qual;
        },
        pg_sys::NodeTag::T_NestLoop => {
            let join_plan = original_plan as *mut NestLoop;
            quals = (*join_plan).join.joinqual;
        },
        pg_sys::NodeTag::T_MergeJoin => {
            let join_plan = original_plan as *mut MergeJoin;
            quals = (*join_plan).join.joinqual;
        },
        pg_sys::NodeTag::T_HashJoin => {
            let join_plan = original_plan as *mut HashJoin;
            quals = (*join_plan).join.joinqual;
        },
        _ => {}
    }

    // Add the where clause walker to indexquals and quals
    // if indexquals.is_null() 
    // whereclause(indexquals as *mut Node, table_lists, table_columns);

    if !indexquals.is_null() {
        expression_tree_walker(indexquals as *mut Node, Some(whereclause_processor), context_ptr);
    }

    if !quals.is_null() {
        expression_tree_walker(quals as *mut Node, Some(whereclause_processor), context_ptr);
    }
    // 
    // if quals.is_null() 
    // whereclause(quals as *mut Node, table_lists, table_columns);
    
    let other_plans = planstate.initPlan;
    if !other_plans.is_null() {
        let length = other_plans.as_ref().unwrap().length as usize;
        for i in 0..length {
            let sps: *mut PlanState = list::List::<*mut c_void>::downcast_ptr(other_plans).unwrap().get(i).unwrap().cast();
            let sps = sps as *mut SubPlanState;
            let sps = *sps;
            handle_plan_states(sps.planstate, context_ptr);
        }
    }

    if !planstate.lefttree.is_null() {
        let outer_tree = planstate.lefttree;
        handle_plan_states(outer_tree, context_ptr);
    }

    if !planstate.righttree.is_null() {
        let inner_tree = planstate.righttree;
        handle_plan_states(inner_tree, context_ptr);
    }

    match plan.type_ {
        pg_sys::NodeTag::T_Append => {
            let other_state = planstate_ptr as *mut AppendState;
            let other_state = *other_state;
            handle_plans_states(other_state.as_nplans as usize, other_state.appendplans, context_ptr)
        },
        pg_sys::NodeTag::T_MergeAppend => {
            let other_state = planstate_ptr as *mut MergeAppendState;
            let other_state = *other_state;
            handle_plans_states(other_state.ms_nplans as usize, other_state.mergeplans, context_ptr)
        },
        pg_sys::NodeTag::T_BitmapAnd => {
            let other_state = planstate_ptr as *mut BitmapAndState;
            let other_state = *other_state;
            handle_plans_states(other_state.nplans as usize, other_state.bitmapplans, context_ptr);
        },
        pg_sys::NodeTag::T_BitmapOr => {
            let other_state = planstate_ptr as *mut BitmapOrState;
            let other_state = *other_state;
            handle_plans_states(other_state.nplans as usize, other_state.bitmapplans, context_ptr);
        },
        pg_sys::NodeTag::T_SubqueryScan => {
            let other_state = planstate_ptr as *mut SubqueryScanState;
            let other_state = *other_state;
            handle_plan_states(other_state.subplan, context_ptr);
        },
        _ => {}
    }

    let other_plans = planstate.subPlan;
    if !other_plans.is_null() {
        let length = other_plans.as_ref().unwrap().length as usize;
        for i in 0..length {
            let sps: *mut PlanState = list::List::<*mut c_void>::downcast_ptr(other_plans).unwrap().get(i).unwrap().cast();
            let sps = sps as *mut SubPlanState;
            let sps = *sps;
            handle_plan_states(sps.planstate, context_ptr);
        }
    }
}

unsafe fn handle_plans_states(nplans: usize, plan_states: *mut *mut PlanState, context_ptr: *mut c_void) {
    for i in 0..nplans {
        let plan_state = (*plan_states).add(i);
        handle_plan_states(plan_state, context_ptr);
    }
}

unsafe extern "C" fn whereclause_processor(node: *mut Node, context_ptr: *mut c_void) -> bool {
    let node_obj = *node;
    match node_obj.type_ {
        pg_sys::NodeTag::T_BoolExpr => {
            let boolexpr = node as *mut BoolExpr;
            let boolexpr = *boolexpr;
            expression_tree_walker(boolexpr.args as *mut Node, Some(whereclause_processor), context_ptr);
        },
        pg_sys::NodeTag::T_OpExpr => {
            process_opexpr(node as *mut OpExpr, context_ptr);
        },
        pg_sys::NodeTag::T_ScalarArrayOpExpr | pg_sys::NodeTag::T_BooleanTest => {},
        _ => {
            expression_tree_walker(node, Some(whereclause_processor), context_ptr);
        }
    }
    return false;
}

unsafe fn process_opexpr(expr: *mut OpExpr, context_ptr: *mut c_void) {
    let opexpr = *expr;
    let args = opexpr.args;
    if args.is_null() {
        return;
    }
    let length = args.as_ref().unwrap().length as usize;
    if length != 2 {
        return;
    }
    let opid = opexpr.opno;
    let mut node: *mut Node = pgrx_list_nth(args, 0).cast();
    let mut var: *mut Var = ptr::null_mut();
    let mut constant: *mut Const = ptr::null_mut();
    let mut lrellid = 0;
    let mut rrellid = 0;
    let mut lattnum = 0;
    let mut rattnum = 0;
    let mut srelid = &mut lrellid as *mut i32;
    let mut sattnum = &mut lattnum as *mut i32;

    for step in 0..2 {
        if is_a(node, pg_sys::NodeTag::T_RelabelType) {
            let relabel_node = node as *mut RelabelType;
            node =  (*relabel_node).arg as *mut Node;
        }

        if is_a(node, pg_sys::NodeTag::T_Var) {
            node = resolve_var(node as *mut Var, context_ptr) as *mut Node;
        }

        let realnode = *node;
        match realnode.type_ {
            NodeTag::T_Var => {
                var = node as *mut Var;
            },
            _ => {}
        }
    }

}

unsafe fn resolve_var(node: *mut Var, context_ptr: *mut c_void) -> *mut Expr {
    let context = context_ptr as *mut Context;
    let context = &(*context);
    let mut tlist: *mut List = ptr::null_mut();
    let planstate = context.planstate;
    set_planstate(planstate, context_ptr as *mut Context);
    let mut node = node;
    let var = *node;
    match var.varno {
        INNER_VAR => {
            tlist = context.inner_tlist;
        },
        OUTER_VAR => {
            tlist = context.outer_tlist;
        },
        INDEX_VAR => {
            tlist = context.index_tlist;
        },
        _ => { return node as *mut Expr; }
    }

    if !tlist.is_null() {
        let entry = get_tle_by_resno(tlist, var.varattno);
        if !entry.is_null() {
            let newvar = (*entry).expr as *mut Var;
            if var.varno == OUTER_VAR {
                set_planstate(context.outer_planstate, context_ptr as *mut Context);
            } else if var.varno == INNER_VAR {
                set_planstate(context.inner_planstate, context_ptr as *mut Context);
            }
            node = resolve_var(newvar, context_ptr) as *mut Var;
        }
    }

    if !is_a(node as *mut Node, NodeTag::T_Var) {
        let n = nodeToString(node as *mut c_void);
        let consttest = makeConst(TEXTOID, -1, Oid::INVALID, -1, Into::into(cstring_to_text(n)), false, false);
        node = consttest as *mut Var;
    }
    set_planstate(planstate, context_ptr as *mut Context);
    node as *mut Expr
}

unsafe fn set_planstate(planstate_ptr: *mut PlanState, context_ptr: *mut Context) {
    let context = context_ptr.as_mut().unwrap();
    (*context).inner_tlist = ptr::null_mut();
    (*context).outer_tlist = ptr::null_mut();
    (*context).index_tlist = ptr::null_mut();
    (*context).inner_planstate = ptr::null_mut();
    (*context).outer_planstate = ptr::null_mut();
    (*context).planstate = planstate_ptr;
    let planstate = *planstate_ptr;
    if planstate.type_ != pg_sys::NodeTag::T_AppendState && planstate.type_ != pg_sys::NodeTag::T_MergeAppendState {
        (*context).outer_planstate = planstate.lefttree;
    }
    if !context.outer_planstate.is_null() {
        let outer_planstate = *(context.outer_planstate);
        (*context).outer_tlist = outer_planstate.plan.as_ref().unwrap().targetlist;
    }

    if planstate.type_ != pg_sys::NodeTag::T_SubqueryScanState && planstate.type_ != pg_sys::NodeTag::T_CteScanState {
        (*context).inner_planstate = planstate.righttree;
    }

    if !context.inner_planstate.is_null() {
        let inner_planstate = *(context.inner_planstate);
        (*context).inner_tlist = inner_planstate.plan.as_ref().unwrap().targetlist;
    }

    if planstate.type_ == pg_sys::NodeTag::T_IndexOnlyScan {
        let index_planstate = *(planstate.plan as *mut IndexOnlyScan);
        (*context).index_tlist = index_planstate.indextlist;
    }
}
