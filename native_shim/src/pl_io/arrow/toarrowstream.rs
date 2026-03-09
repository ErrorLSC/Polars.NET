use polars::prelude::*;
// use polars_arrow::array::Array;
use polars_arrow::ffi::{ArrowArrayStream, export_iterator};
use polars_arrow::datatypes::Field;
use polars_core::utils::Container;

use crate::types::DataFrameContext;

// #[unsafe(no_mangle)]
// pub unsafe extern "C" fn pl_dataframe_export_to_stream(
//     df_ptr: *mut DataFrameContext,
//     out_stream: *mut ArrowArrayStream,
// ) -> i32 {
//     ffi_try_c_int!({
//         if df_ptr.is_null() || out_stream.is_null() {
//             return Err(PolarsError::ComputeError("Null pointer passed".into()));
//         }

//         let ctx = unsafe {&*df_ptr};
//         let df = &ctx.df;

//         let struct_series = df.clone().into_struct("".into()).into_series();
        
//         let dtype = struct_series.dtype().to_arrow(CompatLevel::newest());
//         let root_field = Field::new("".into(), dtype, false);

//         let owned_chunks: Vec<Box<dyn polars_arrow::array::Array>> = struct_series
//             .chunks()
//             .iter()
//             .cloned()
//             .collect();

//         let chunks_iter = owned_chunks.into_iter().map(Ok);

//         let exported_stream_struct = export_iterator(Box::new(chunks_iter), root_field);

//         unsafe { std::ptr::write(out_stream, exported_stream_struct) };

//         Ok(0)
//     })
// }

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_export_to_stream(
    df_ptr: *mut DataFrameContext,
    out_stream: *mut ArrowArrayStream,
) -> i32 {
    ffi_try_c_int!({
        if df_ptr.is_null() || out_stream.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed".into()));
        }

        let ctx = unsafe { &*df_ptr };
        let mut df = ctx.df.clone();
        
        // 1. 内存整理：强行将所有列对齐并压缩成一个连续的 Chunk
        // 这不仅提升 FFI 传输速度，还能顺手抹平一些导入时的内存碎片
        df.align_chunks_par(); 

        let compat = CompatLevel::newest();
        
        // 2. 构建纯正的“逻辑” Arrow Schema
        let fields: Vec<Field> = df.schema()
            .iter_fields()
            .map(|f| f.to_arrow(compat))
            .collect();
            
        let root_dtype = polars_arrow::datatypes::ArrowDataType::Struct(fields);
        let root_field = polars_arrow::datatypes::Field::new("".into(), root_dtype.clone(), false);

        let n_chunks = df.n_chunks();
        let mut owned_chunks: Vec<Box<dyn polars_arrow::array::Array>> = Vec::with_capacity(n_chunks);

        // 3. 手搓 StructArray：完美避开 into_struct 的物理类型陷阱！
        for i in 0..n_chunks {
            let mut chunk_arrays = Vec::with_capacity(df.width());
            
            for series in df.columns() {
                // 🔥 核心魔法：to_arrow 会自动处理 FFI 边界的类型转换 (Int64 -> Timestamp)
                let logical_arrow_array = series.as_materialized_series().to_arrow(i, compat);
                chunk_arrays.push(logical_arrow_array);
            }
            let chunk_len = chunk_arrays.first().map_or(0, |arr| arr.len());
            // 此时 Schema (root_dtype) 和 Data (chunk_arrays) 的类型绝对完美匹配
            let struct_arr = polars_arrow::array::StructArray::new(root_dtype.clone(),chunk_len,chunk_arrays, None);
            owned_chunks.push(Box::new(struct_arr));
        }

        let chunks_iter = owned_chunks.into_iter().map(Ok);
        let exported_stream_struct = export_iterator(Box::new(chunks_iter), root_field);

        unsafe { std::ptr::write(out_stream, exported_stream_struct) };

        Ok(0)
    })
}