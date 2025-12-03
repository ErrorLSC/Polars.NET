use polars::prelude::*;
use polars_arrow::ffi;
use crate::types::{ExprContext, map_datatype};
use std::sync::Arc;
use polars_arrow::datatypes::Field as ArrowField;
use std::ffi::{CStr,c_void};

// 1. 定义清理回调的签名
// 参数: user_data (这里我们将传入 C# GCHandle 的 IntPtr)
type CleanupCallback = extern "C" fn(*mut c_void);

// 当 Polars 执行完查询，销毁表达式树时，会自动调用 drop
impl Drop for CSharpUdf {
    fn drop(&mut self) {
        // 调用 C# 传过来的清理函数，把 GCHandle 传回去
        (self.cleanup)(self.user_data);
    }
}

// 定义回调函数的签名
// 参数 1 (输入): ArrowArray 指针
// 参数 2 (输入): ArrowSchema 指针
// 参数 3 (输出): ArrowArray 指针 (由 C# 填充)
// 参数 4 (输出): ArrowSchema 指针 (由 C# 填充)
// 新增参数: msg_buf (用于接收错误信息)
// 返回值: i32 (0=Ok, 1=Error)
type UdfCallback = extern "C" fn(
    *const ffi::ArrowArray, 
    *const ffi::ArrowSchema, 
    *mut ffi::ArrowArray, 
    *mut ffi::ArrowSchema,
    *mut std::os::raw::c_char
) -> i32;

// 定义一个 Wrapper 结构体来持有这个函数指针
// 必须实现 Send + Sync 因为 Polars 是多线程执行的
#[derive(Clone)]
struct CSharpUdf {
    callback: UdfCallback,
    cleanup: CleanupCallback, // 析构函数指针
    user_data: *mut c_void,   // GCHandle 的原始指针
}

unsafe impl Send for CSharpUdf {}
unsafe impl Sync for CSharpUdf {}

impl CSharpUdf {
    fn call(&self, s: Series) -> PolarsResult<Option<Series>> {
        // A. 准备输入数据
        let array = s.to_arrow(0, CompatLevel::newest());
        
        // [修复 1 & 2]
        // 1. 使用 .dtype() 而不是 .data_type()
        // 2. 使用 ArrowField (polars_arrow::datatypes::Field)
        // 3. ArrowField::new 需要 3 个参数：名字, 类型,是否可为空(通常设为 true)
        let field = ArrowField::new("".into(), array.dtype().clone(), true);
        
        // 这里的 export_field_to_c 需要 &ArrowField
        let c_array_in = ffi::export_array_to_c(array);
        let c_schema_in = ffi::export_field_to_c(&field);

        // B. 准备输出容器
        let mut c_array_out = ffi::ArrowArray::empty();
        let mut c_schema_out = ffi::ArrowSchema::empty();

        // 2. 准备一个 1KB 的错误缓冲区
        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;
        // C. 调用 C#
        let status = (self.callback)(&c_array_in, &c_schema_in, &mut c_array_out, &mut c_schema_out, error_ptr);
        // 4. 检查状态码
        if status != 0 {
            // 如果失败，读取缓冲区里的错误信息
            let msg = unsafe { CStr::from_ptr(error_ptr).to_string_lossy().into_owned() };
            // 返回给 Polars 引擎，这会停止查询并抛出异常
            return Err(PolarsError::ComputeError(format!("C# UDF Failed: {}", msg).into()));
        }
        // D. 导回结果
        // import_field_from_c 返回的就是 ArrowField，所以这里的 field 类型是对的
        let out_field = unsafe { ffi::import_field_from_c(&c_schema_out).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };
        let out_array = unsafe { ffi::import_array_from_c(c_array_out, out_field.dtype.clone()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };

        // E. 重建 Series
        let out_series = Series::try_from((s.name().clone(), out_array))?;
        
        Ok(Some(out_series))
    }
}

/// 4. 导出给 C# 的 API
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_map(
    expr_ptr: *mut ExprContext,
    callback: UdfCallback,
    return_type_code: i32,
    cleanup: CleanupCallback,
    user_data: *mut c_void // 接收 C# 的 GCHandle.ToIntPtr()
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let udf = Arc::new(CSharpUdf { callback,cleanup,user_data });
        let target_dtype = map_datatype(return_type_code);
        // [核心逻辑] 构建 GetOutput
        let output_type = match target_dtype {
            // 如果是 Unknown (0)，说明用户没指定，我们假设输出类型 == 输入类型
            DataType::Unknown(UnknownKind::Any) => GetOutput::map_field(|f| Ok(f.clone())),
            
            // 否则，指定具体的返回类型 (如 String, Float64)
            _ => GetOutput::from_type(target_dtype),
        };

        let new_expr = ctx.inner.map(
            move |c| {
                let s = c.take_materialized_series();
                let res_series = udf.call(s)?;
                Ok(res_series.map(|s| s.into_column()))
            }, 
            output_type
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}