use std::ffi::c_char;
use polars::prelude::*;
use crate::types::*;
use polars::lazy::dsl::UnpivotArgsDSL;

// ==========================================
// 宏定义
// ==========================================

/// 模式 A: LazyFrame -> Vec<Expr> -> LazyFrame
/// 适用: select, with_columns
macro_rules! gen_lazy_vec_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext,
            exprs_ptr: *const *mut ExprContext,
            len: usize
        ) -> *mut LazyFrameContext {
            ffi_try!({
                // 1. 拿回 LazyFrame 所有权 (Consume)
                // 链式调用的核心：上一步的输出是这一步的输入，旧壳子丢弃
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                
                // 2. 拿回 Exprs 所有权
                let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

                // 3. 执行转换
                let new_lf = lf_ctx.inner.$method(exprs);

                // 4. 返回新壳子
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// 模式 B: LazyFrame -> 单个 Expr -> LazyFrame
/// 适用: filter
macro_rules! gen_lazy_single_expr_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            expr_ptr: *mut ExprContext
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
                
                let new_lf = lf_ctx.inner.$method(expr_ctx.inner);
                
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// 模式 C: LazyFrame -> 标量参数 -> LazyFrame
/// 适用: limit (u32), head (u32, 只要类型匹配)
macro_rules! gen_lazy_scalar_op {
    ($func_name:ident, $method:ident, $arg_type:ty) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            val: $arg_type
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let new_lf = lf_ctx.inner.$method(val); 
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

// ==========================================
// 3. 宏应用 (标准 API)
// ==========================================

// --- Select / WithColumns ---
gen_lazy_vec_op!(pl_lazy_select, select);
gen_lazy_vec_op!(pl_lazy_with_columns, with_columns);

// --- Filter ---
gen_lazy_single_expr_op!(pl_lazy_filter, filter);

// --- Limit ---
// limit 在 Polars 中通常接受 IdxSize (u32)
gen_lazy_scalar_op!(pl_lazy_limit, limit, u32);
// 也可以加个 tail
gen_lazy_scalar_op!(pl_lazy_tail, tail, u32);

// ==========================================
// Sort
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sort(
    lf_ptr: *mut LazyFrameContext,
    expr_ptr: *mut ExprContext,
    descending: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // 构建排序选项
        let options = SortMultipleOptions::default()
            .with_order_descending(descending);

        // Polars 0.50: sort_by_exprs 接受 Vec<Expr>
        let new_lf = lf_ctx.inner.sort_by_exprs(
            vec![expr_ctx.inner], 
            options
        );
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// GroupBy
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_groupby_agg(
    lf_ptr: *mut LazyFrameContext,
    keys_ptr: *const *mut ExprContext, keys_len: usize,
    aggs_ptr: *const *mut ExprContext, aggs_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let keys = unsafe { consume_exprs_array(keys_ptr, keys_len) };
        let aggs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        // 链式调用
        let new_lf = lf_ctx.inner.group_by(keys).agg(aggs);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explode(
    lf_ptr: *mut LazyFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

        if exprs.is_empty() {
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf_ctx.inner })));
        }

        let mut iter = exprs.into_iter();
        
        // 1. 处理第一个
        let first_expr = iter.next().unwrap();
        // [修复] 处理 Option: 如果转换失败，抛出错误
        let mut final_selector = first_expr.into_selector()
            .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;

        // 2. 处理剩下的
        for e in iter {
            let s = e.into_selector()
                .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;
            
            final_selector = final_selector | s; // Union
        }

        let new_lf = lf_ctx.inner.explode(final_selector);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Collect (出口：LazyFrame -> DataFrame)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // 去掉了 println!，保持库函数的纯洁性。
        // 如果想看日志，可以在 F# 端调用 explain 或者 check schema。
        // 这里的 ? 会捕获 PolarsError 并转给 ffi_try
        let df = lf_ctx.inner.collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect_streaming(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Polars 0.50+ 写法: with_streaming(true).collect()
        let df = lf_ctx.inner
            .with_new_streaming(true)
            .collect()?;
            
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// Unpivot
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_unpivot(
    lf_ptr: *mut LazyFrameContext,
    id_vars_ptr: *const *const c_char, id_len: usize,
    val_vars_ptr: *const *const c_char, val_len: usize,
    variable_name_ptr: *const c_char,
    value_name_ptr: *const c_char
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // 1. 辅助：把 C字符串数组 转为 Vec<PlSmallStr>
        // 因为 cols() 和 exclude() 都接受 IntoVec<PlSmallStr>
        let to_pl_strs = |ptr, len| unsafe {
            let mut v = Vec::with_capacity(len);
            for &p in std::slice::from_raw_parts(ptr, len) {
                let s = ptr_to_str(p).unwrap();
                v.push(PlSmallStr::from_str(s));
            }
            v
        };

        let index_names = to_pl_strs(id_vars_ptr, id_len);
        let on_names = to_pl_strs(val_vars_ptr, val_len);

        // 2. 构造 Selector
        // index: 直接指定列名
        let index_selector = cols(index_names.clone()); // clone 一份给 index 使用

        // on: 如果为空，则默认选取 "所有非 index 的列" (模仿 pandas/polars 默认行为)
        let on_selector = if on_names.is_empty() {
            all().exclude_cols(index_names) // 这里用掉了 index_names
        } else {
            cols(on_names)
        };

        // 3. 处理重命名
        let variable_name = if variable_name_ptr.is_null() { 
            None 
        } else { 
            Some(PlSmallStr::from_str(ptr_to_str(variable_name_ptr).unwrap())) 
        };
        
        let value_name = if value_name_ptr.is_null() { 
            None 
        } else { 
            Some(PlSmallStr::from_str(ptr_to_str(value_name_ptr).unwrap())) 
        };

        // 4. 构建参数 (UnpivotArgs)
        let args = UnpivotArgsDSL {
            index: index_selector, // 必须是 Selector
            on: on_selector,       // 必须是 Selector
            variable_name,
            value_name,
        };

        let new_lf = lf_ctx.inner.unpivot(args);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// Concat
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_concat(
    lfs_ptr: *const *mut LazyFrameContext, // 指针数组
    len: usize,
    rechunk: bool,
    parallel: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        // 1. 转换指针数组为 Vec<LazyFrame>
        // 注意：这里我们需要消费掉所有的输入 LazyFrame (拿走所有权)
        let mut lfs = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(lfs_ptr, len) };
        
        for &p in slice {
            // Box::from_raw 会拿回所有权，循环结束后如果不 move 就会 drop
            // 所以我们需要把 inner 拿出来放入 Vec
            let lf_ctx = unsafe { Box::from_raw(p) };
            lfs.push(lf_ctx.inner);
        }

        // 2. 构建参数
        let args = UnionArgs {
            rechunk,
            parallel,
            ..Default::default()
        };

        // 3. 调用 concat
        // polars::prelude::concat 接受 Vec<LazyFrame>
        let new_lf = concat(lfs, args)?;
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// Join & Join As of
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_join(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    left_on_ptr: *const *mut ExprContext, left_on_len: usize,
    right_on_ptr: *const *mut ExprContext, right_on_len: usize,
    how_code: i32 // 复用 PlJoinType 枚举
) -> *mut LazyFrameContext {
    ffi_try!({
        // 1. 消费左右 LazyFrame
        let left_ctx = unsafe { Box::from_raw(left_ptr) };
        let right_ctx = unsafe { Box::from_raw(right_ptr) };

        // 2. 消费连接键表达式
        let left_on = unsafe { consume_exprs_array(left_on_ptr, left_on_len) };
        let right_on = unsafe { consume_exprs_array(right_on_ptr, right_on_len) };

        // 3. 映射 JoinType
        let how = map_jointype(how_code);
        let args = JoinArgs::new(how);

        // 4. 执行 Lazy Join
        let new_lf = left_ctx.inner.join(right_ctx.inner, left_on, right_on, args);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
fn exprs_to_names(exprs: &[Expr]) -> PolarsResult<Vec<PlSmallStr>> {
    let mut names = Vec::new();
    for e in exprs {
        // e.meta().root_names() 返回 Vec<PlSmallStr>
        let roots = e.clone().meta().root_names();
        
        // 将所有找到的根列名都加入到结果列表中
        // extend_from_slice 会把 Vec 里的元素一个个加进去
        names.extend_from_slice(&roots);
    }
    Ok(names)
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_join_asof(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    left_on_ptr: *mut ExprContext,
    right_on_ptr: *mut ExprContext,
    by_left_ptr: *const *mut ExprContext, by_left_len: usize,
    by_right_ptr: *const *mut ExprContext, by_right_len: usize,
    strategy_ptr: *const c_char,
    tolerance_ptr: *const c_char 
) -> *mut LazyFrameContext {
    ffi_try!({
        let left = unsafe { Box::from_raw(left_ptr) };
        let right = unsafe { Box::from_raw(right_ptr) };
        let left_on = unsafe { Box::from_raw(left_on_ptr) };
        let right_on = unsafe { Box::from_raw(right_on_ptr) };
        
        let by_left_exprs = unsafe { consume_exprs_array(by_left_ptr, by_left_len) };
        let by_right_exprs = unsafe { consume_exprs_array(by_right_ptr, by_right_len) };

        // 将 Expr 列表转换为列名列表 (PlSmallStr)
        let left_by_names = if by_left_exprs.is_empty() { None } else { Some(exprs_to_names(&by_left_exprs)?) };
        let right_by_names = if by_right_exprs.is_empty() { None } else { Some(exprs_to_names(&by_right_exprs)?) };

        // 策略
        let strategy_str = ptr_to_str(strategy_ptr).unwrap_or("backward");
        let strategy = match strategy_str {
            "forward" => AsofStrategy::Forward,
            "nearest" => AsofStrategy::Nearest,
            _ => AsofStrategy::Backward,
        };

        // [修复] 容差解析: 字符串 -> (Scalar?, String?)
        let tol_str = if tolerance_ptr.is_null() { "" } else { ptr_to_str(tolerance_ptr).unwrap() };
        
        let (tolerance, tolerance_str_val) = if tol_str.is_empty() {
            (None, None)
        } else if let Ok(v) = tol_str.parse::<i64>() {
            // 纯整数 -> Scalar(Int64)
            (Some(Scalar::new(DataType::Int64, AnyValue::Int64(v))), None)
        } else if let Ok(v) = tol_str.parse::<f64>() {
            // 浮点数 -> Scalar(Float64)
            (Some(Scalar::new(DataType::Float64, AnyValue::Float64(v))), None)
        } else {
            // 否则认为是时间字符串 ("2h")，直接传给 Polars
            (None, Some(PlSmallStr::from_str(tol_str)))
        };

        // 构建 Options
        let options = AsOfOptions {
            strategy,
            tolerance,      // Option<Scalar>
            tolerance_str: tolerance_str_val, // Option<PlSmallStr>
            left_by: left_by_names,
            right_by: right_by_names,
            allow_eq: true, // 默认允许相等匹配
            check_sortedness: true, // 默认检查排序
        };

        let new_lf = left.inner.join_builder()
            .with(right.inner)
            .left_on([left_on.inner])
            .right_on([right_on.inner])
            .how(JoinType::AsOf(Box::new(options)))
            .finish();

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// 5. 实用功能
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_schema(lf_ptr: *mut LazyFrameContext) -> *mut c_char {
    ffi_try!({
        // 借用 LazyFrame (注意：collect_schema 需要 &mut self，因为它会缓存 plan)
        // 所以这里我们必须用 &mut *lf_ptr
        let ctx = unsafe { &mut *lf_ptr };
        
        // 调用 collect_schema 获取 SchemaRef
        let schema = ctx.inner.collect_schema()?;
        
        // 方案 A: 转 JSON (推荐，如果开启了 serde feature)
        // let json = serde_json::to_string(&schema).unwrap();
        
        // 方案 B: 简单 Debug 字符串 (如果不依赖 serde)
        // 格式类似: Schema { fields: [Field { name: "a", dtype: Int64 }, ...] }
        // 这对 C# 来说比较难解析。
        
        // 方案 C (最佳折衷): 手动构建一个简化的 JSON 字符串
        // {"name": "Int64", "age": "Utf8"}
        let mut json_parts = Vec::new();
        for (name, dtype) in schema.iter() {
            let dtype_str = dtype.to_string();
            json_parts.push(format!("\"{}\": \"{}\"", name, dtype_str));
        }
        let json = format!("{{ {} }}", json_parts.join(", "));
        
        Ok(std::ffi::CString::new(json).unwrap().into_raw())
    })
}

// 不执行计算，只查看计划
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explain(lf_ptr: *mut LazyFrameContext, optimized: bool) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*lf_ptr };
        
        let plan_str = ctx.inner.explain(optimized)?;
        
        Ok(std::ffi::CString::new(plan_str).unwrap().into_raw())
    })
}

// 释放字符串 (配合 pl_lazy_explain 使用)
#[unsafe(no_mangle)]
pub extern "C" fn pl_free_string(ptr: *mut std::os::raw::c_char) {
    if !ptr.is_null() {
        unsafe { let _ = std::ffi::CString::from_raw(ptr); }
    }
}

// 克隆逻辑计划
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_clone(lf_ptr: *mut LazyFrameContext) -> *mut LazyFrameContext {
    // 注意：这里用 &*lf_ptr 借用，而不是 Box::from_raw 消费
    let ctx = unsafe { &*lf_ptr };
    
    // LazyFrame 的 clone 只是复制查询计划，非常快
    let new_lf = ctx.inner.clone();
    
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_frame_free(ptr: *mut LazyFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
