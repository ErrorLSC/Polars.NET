use polars::prelude::*;
use polars_arrow::ffi::{ArrowArrayStream, export_iterator};
use polars_arrow::datatypes::Field;

use crate::types::DataFrameContext;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_export_to_stream(
    df_ptr: *mut DataFrameContext,
    out_stream: *mut ArrowArrayStream,
    col_indices: *const i32, 
    num_cols: usize,         
    shuffle_seed: *const u64, 
) -> i32 {
    ffi_try_c_int!({
        if df_ptr.is_null() || out_stream.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed".into()));
        }

        let ctx = unsafe { &*df_ptr };
        let mut df = ctx.df.clone();

        // Projection Pushdown
        if !col_indices.is_null() && num_cols > 0 {
            let indices = unsafe { std::slice::from_raw_parts(col_indices, num_cols) };

            let all_columns = df.columns(); 
            
            let selected_columns: Vec<_> = indices
                .iter()
                .map(|&idx| all_columns[idx as usize].clone())
                .collect();
            
            df = DataFrame::new_infer_height(selected_columns)?;
        }

        df.align_chunks_par(); 

        let compat = CompatLevel::newest();
        
        // Build Pure Logical Arrow Schema
        let fields: Vec<Field> = df.schema()
            .iter_fields()
            .map(|f| f.to_arrow(compat))
            .collect();
            
        let root_dtype = polars_arrow::datatypes::ArrowDataType::Struct(fields);
        let root_field = polars_arrow::datatypes::Field::new("".into(), root_dtype.clone(), false);

        // The frame's chunks as Polars lays them out: scalar and dictionary columns follow the series columns' chunks
        // (since Polars 2.x they keep no chunks of their own, so chunk `i` of each column is not the batch's).
        let mut owned_chunks: Vec<Box<dyn polars_arrow::array::Array>> = Vec::new();
        for batch in df.try_iter_chunks(compat, false) {
            let batch = batch?;
            let chunk_len = batch.height();
            let struct_arr = polars_arrow::array::StructArray::new(root_dtype.clone(), chunk_len, batch.into_arrays(), None);
            owned_chunks.push(Box::new(struct_arr));
        }

        if !shuffle_seed.is_null() {
            let seed_val = unsafe { *shuffle_seed };
            
            use rand::seq::SliceRandom;
            use rand::SeedableRng;
            
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed_val);
            
            owned_chunks.shuffle(&mut rng);
        }

        let chunks_iter = owned_chunks.into_iter().map(Ok);
        let exported_stream_struct = export_iterator(Box::new(chunks_iter), root_field);

        unsafe { std::ptr::write(out_stream, exported_stream_struct) };

        Ok(0)
    })
}