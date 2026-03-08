use polars::prelude::*;
use polars_arrow::array::Array;
use polars_arrow::ffi::{ArrowArrayStream, export_iterator};
use polars_arrow::datatypes::Field;

use crate::types::DataFrameContext;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_export_to_stream(
    df_ptr: *mut DataFrameContext,
    out_stream: *mut ArrowArrayStream,
) -> i32 {
    ffi_try_c_int!({
        if df_ptr.is_null() || out_stream.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed".into()));
        }

        let ctx = unsafe {&*df_ptr};
        let df = &ctx.df;

        // 1. 将 DataFrame 融合为一个大 Struct
        let struct_series = df.clone().into_struct("".into()).into_series();
        
        // 2. 提取物理层的 Data Type
        let dtype = struct_series.dtype().to_arrow(CompatLevel::newest());
        let root_field = Field::new("".into(), dtype, false);

        // 3. ✨ 关键修复：斩断生命周期！
        // 把切片借用（&[Box<dyn Array>]）转换为拥有完全所有权的 Owned Vec。
        // 这里的 cloned() 只是极速增加底层 Buffer 的引用计数，真·零拷贝。
        let owned_chunks: Vec<Box<dyn Array>> = struct_series
            .chunks()
            .iter()
            .cloned()
            .collect();

        // 4. 调用 into_iter()，迭代器将完全拿走 owned_chunks 的所有权。
        // 此时的 chunks_iter 不再借用任何局部变量，完美符合 'static 约束！
        let chunks_iter = owned_chunks.into_iter().map(Ok);

        // 5. 调用官方方法，生成 C 结构体
        let exported_stream_struct = export_iterator(Box::new(chunks_iter), root_field);

        // 5. 将生成的 C 结构体写入调用方 (C#) 提供的内存地址
       unsafe{ std::ptr::write(out_stream, exported_stream_struct)};

        Ok(0)
    });
    
    1
}