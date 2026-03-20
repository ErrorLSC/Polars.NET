use polars::prelude::*;
use polars_arrow::ffi::{ArrowArrayStream, export_iterator};
use polars_arrow::datatypes::Field;
use polars_core::utils::Container;

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

        let ctx = unsafe { &*df_ptr };
        let mut df = ctx.df.clone();
        
        df.align_chunks_par(); 

        let compat = CompatLevel::newest();
        
        // Build Pure Logical Arrow Schema
        let fields: Vec<Field> = df.schema()
            .iter_fields()
            .map(|f| f.to_arrow(compat))
            .collect();
            
        let root_dtype = polars_arrow::datatypes::ArrowDataType::Struct(fields);
        let root_field = polars_arrow::datatypes::Field::new("".into(), root_dtype.clone(), false);

        let n_chunks = df.n_chunks();
        let mut owned_chunks: Vec<Box<dyn polars_arrow::array::Array>> = Vec::with_capacity(n_chunks);

        // Build Struct Array with correct schema
        for i in 0..n_chunks {
            let mut chunk_arrays = Vec::with_capacity(df.width());
            
            for series in df.columns() {
                let logical_arrow_array = series.as_materialized_series().to_arrow(i, compat);
                chunk_arrays.push(logical_arrow_array);
            }
            let chunk_len = chunk_arrays.first().map_or(0, |arr| arr.len());

            let struct_arr = polars_arrow::array::StructArray::new(root_dtype.clone(),chunk_len,chunk_arrays, None);
            owned_chunks.push(Box::new(struct_arr));
        }

        let chunks_iter = owned_chunks.into_iter().map(Ok);
        let exported_stream_struct = export_iterator(Box::new(chunks_iter), root_field);

        unsafe { std::ptr::write(out_stream, exported_stream_struct) };

        Ok(0)
    })
}