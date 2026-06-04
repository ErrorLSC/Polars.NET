use polars::prelude::*;
use polars_arrow::array::{Array,ListArray};
use polars_core::utils::Wrap;
use std::ffi::{CStr, CString, c_int};
use std::hash::{DefaultHasher, Hasher};
use std::os::raw::c_char;
use std::slice::from_raw_parts;
use crate::pl_io::arrow::ArrowArrayContext;
use crate::types::{DataFrameContext, DataTypeContext, SeriesContext};
use polars_arrow::datatypes::ArrowDataType;
use polars_buffer::Buffer;
use polars::chunked_array::cast::CastOptions;

// ==========================================
// Constructors 
// ==========================================


// macro_rules! gen_series_new_128 {
//     ($func_name:ident, $rs_type:ty, $pl_type:ty) => {
//         #[unsafe(no_mangle)]
//         pub unsafe extern "C" fn $func_name(
//             name: *const c_char,
//             ptr: *const u64, 
//             validity: *const u8, 
//             len: usize,
//         ) -> *mut SeriesContext {
//             ffi_try!({
//                 let name = unsafe {CStr::from_ptr(name).to_string_lossy()};
                
//                 let slice_u64 = unsafe { std::slice::from_raw_parts(ptr, len * 2) };
                
//                 let values_vec: Vec<$rs_type> = slice_u64
//                     .chunks_exact(2)
//                     .map(|chunk| {
//                         let low = chunk[0];
//                         let high = chunk[1];
//                         ((high as $rs_type) << 64) | (low as $rs_type)
//                     })
//                     .collect();

//                 let values_buffer = Buffer::from(values_vec);

//                 let validity_bitmap = if validity.is_null() {
//                     None
//                 } else {
//                     let bytes_len = (len + 7) / 8;
//                     let v_slice =unsafe { std::slice::from_raw_parts(validity, bytes_len)};
//                     let v_vec = v_slice.to_vec(); 
//                     Some(Bitmap::try_new(v_vec, len).unwrap())
//                 };

//                 let arrow_dtype = <$pl_type as PolarsDataType>::get_static_dtype().to_arrow(CompatLevel::newest());
                
//                 let arrow_array = PrimitiveArray::new(
//                     arrow_dtype,
//                     values_buffer,
//                     validity_bitmap
//                 );

//                 let ca = ChunkedArray::<$pl_type>::with_chunk(
//                     PlSmallStr::from_str(name.as_ref()), 
//                     arrow_array,
//                 );
                
//                 Ok(Box::into_raw(Box::new(SeriesContext { series: ca.into_series() })))
//             })
//         }
//     };
// }

// gen_series_new_128!(pl_series_new_i128, i128, Int128Type);
// gen_series_new_128!(pl_series_new_u128, u128, UInt128Type);



#[unsafe(no_mangle)]
pub extern "C" fn pl_series_clone(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let new_series = ctx.series.clone();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: new_series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_rechunk(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };

        let contiguous_series = ctx.series.rechunk();
        
        Ok(Box::into_raw(Box::new(SeriesContext {
            series: contiguous_series,
        })))
    })
}



#[unsafe(no_mangle)]
pub extern "C" fn pl_series_approx_n_unique(
    series_ptr: *mut SeriesContext,
    out_count: *mut u32,
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*series_ptr };
        
        let count = ctx.series.approx_n_unique()?;
        
        unsafe { *out_count = count as u32 };
        
        Ok(())
    })
}

// ==========================================
// Methods
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_free(ptr: *mut SeriesContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_name(ptr: *mut SeriesContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let name = ctx.series.name().as_str(); 
        
        let c_str = CString::new(name)
            .map_err(|e| polars_err!(ComputeError: "Series name contains null byte: {}", e))?;
            
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_rename(ptr: *mut SeriesContext, name: *const c_char) -> bool {
    ffi_bool_try!({

        if ptr.is_null() {
            polars_bail!(ComputeError: "Series pointer is null");
        }
        if name.is_null() {
            polars_bail!(ComputeError: "Cannot rename Series to a null string");
        }

        let ctx = unsafe { &mut *ptr };
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        ctx.series.rename(name_str.into());

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_string(s_ptr: *mut SeriesContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let mut s = std::string::ToString::to_string(&ctx.series); // Native Display
        if s.contains('\0') {
            s = s.replace('\0', "␀"); 
        }
        let c_str = CString::new(s).map_err(|e| polars_err!(ComputeError: "Series contains null byte: {}", e))?;
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_slice(series: *mut Series, offset: i64, length: usize) -> *mut Series {
    ffi_try!({
        let s = unsafe { &*series };
        let new_s = s.slice(offset, length);
        Ok(Box::into_raw(Box::new(new_s)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_take(
    series_ptr: *mut SeriesContext,
    indices_ptr: *mut SeriesContext,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*series_ptr };
        let indices_series = unsafe { &(*indices_ptr).series };

        let idx_series = indices_series.cast(&DataType::UInt32)?;
        let idx_ca = idx_series.u32()?;

        let taken_series = ctx.series.take(idx_ca)?;

        Ok(Box::into_raw(Box::new(SeriesContext { series: taken_series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_dtype_str(s_ptr: *mut SeriesContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let dtype_str = ctx.series.dtype().to_string();
        let c_str = CString::new(dtype_str)
            .map_err(|e| polars_err!(ComputeError: "dtype string contains null byte: {}", e))?;
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_arrow(ptr: *mut SeriesContext) -> *mut ArrowArrayContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let contiguous_series = ctx.series.rechunk();
        let arr = contiguous_series.to_arrow(0, CompatLevel::newest());
        Ok(Box::into_raw(Box::new(ArrowArrayContext { array: arr })))
    })
}

pub fn upgrade_to_large_list(array: Box<dyn Array>) -> Box<dyn Array> {
    match array.dtype() {
        ArrowDataType::List(inner_field) => {
            // Convert to ListArray<i32>
            let list_array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();

            // Convert Offsets (i32 -> i64)
            let offsets_i32 = list_array.offsets();
            let offsets_i64: Vec<i64> = offsets_i32.iter().map(|&x| x as i64).collect();
            
            // Convert Arrow Buffer
            let raw_buffer = Buffer::from(offsets_i64);
            let offsets_buffer = polars_arrow::offset::OffsetsBuffer::try_from(raw_buffer).unwrap();

            // Deal Values Recursively
            let values = list_array.values().clone();
            let new_values = upgrade_to_large_list(values);

            // Build new DataType (LargeList)
            let new_inner_dtype = new_values.dtype().clone();
            let new_field = inner_field.as_ref().clone().with_dtype(new_inner_dtype);
            let new_dtype = ArrowDataType::LargeList(Box::new(new_field));

            // Build New LargeListArray
            // new(data_type, offsets, values, validity)
            let large_list = ListArray::<i64>::new(
                new_dtype,
                offsets_buffer.into(),
                new_values,
                list_array.validity().cloned(),
            );

            Box::new(large_list)
        },
        
        ArrowDataType::LargeList(inner_field) => {
             let list_array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
             
             let values = list_array.values().clone();
             let new_values = upgrade_to_large_list(values.clone());
             
             if new_values.dtype() == values.dtype() {
                 return array;
             }

             let new_inner_dtype = new_values.dtype().clone();
             let new_field = inner_field.as_ref().clone().with_dtype(new_inner_dtype);
             let new_dtype = ArrowDataType::LargeList(Box::new(new_field));
             
             let large_list = ListArray::<i64>::new(
                new_dtype,
                list_array.offsets().clone(),
                new_values,
                list_array.validity().cloned(),
            );
            Box::new(large_list)
        },
        ArrowDataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            
            let new_values: Vec<Box<dyn Array>> = struct_array
                .values()
                .iter()
                .map(|v| upgrade_to_large_list(v.clone())) 
                .collect();

            let mut changed = false;
            for (old, new) in struct_array.values().iter().zip(new_values.iter()) {
                if old.dtype() != new.dtype() {
                    changed = true;
                    break;
                }
            }

            if !changed {
                return array;
            }

            let new_fields: Vec<ArrowField> = fields
                .iter()
                .zip(new_values.iter())
                .map(|(f, v)| {
                    f.clone().with_dtype(v.dtype().clone())
                })
                .collect();
            
            let new_dtype = ArrowDataType::Struct(new_fields);

            let new_struct = StructArray::new(
                new_dtype,
                struct_array.len(),
                new_values,
                struct_array.validity().cloned(),
            );

            Box::new(new_struct)
        },
        _ => array,
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_arrow_to_series(
    name: *const c_char,
    ptr_array: *mut polars_arrow::ffi::ArrowArray,
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_str().map_err(|e| polars_err!(ComputeError: "Name contains null byte: {}", e))? };
        let field = unsafe { polars_arrow::ffi::import_field_from_c(&*ptr_schema)? };

        let array_val = unsafe { std::ptr::read(ptr_array) };
        let mut array = unsafe { polars_arrow::ffi::import_array_from_c(array_val, field.dtype)? };
       
        array = upgrade_to_large_list(array);

        let series = Series::from_arrow(name_str.into(), array)?;
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_cast(
    ptr: *mut SeriesContext, 
    dtype_ptr: *mut DataTypeContext,
    strict: bool,
    wrap_numerical: bool
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let target_dtype = unsafe { &(*dtype_ptr).dtype };
        
        let options = match (strict, wrap_numerical) {
            (true, _) => CastOptions::Strict,
            (false, true) => CastOptions::Overflowing,
            (false, false) => CastOptions::NonStrict,
        };

        let s = ctx.series.cast_with_options(target_dtype, options)?;
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_null_at(
    s_ptr: *mut SeriesContext, 
    idx: usize,
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        if s_ptr.is_null() {
            polars_bail!(ComputeError: "Series pointer is null");
        }
        
        let ctx = unsafe { &*s_ptr };
        let len = ctx.series.len();
        
        if idx >= len { 
            polars_bail!(OutOfBounds: "Index {} is out of bounds for Series of length {}", idx, len);
        }
        
        let is_null = match unsafe { ctx.series.get_unchecked(idx) } {
            AnyValue::Null => true,
            _ => false 
        };
        
        unsafe { *out_is_null = is_null; }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_drop_nulls(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.drop_nulls();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

// Unique
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_unique(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &*ptr }.series.clone();
        let out = s.unique()?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: out })))
    })
}

// UniqueStable
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_unique_stable(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &*ptr }.series.clone();
        let out = s.unique_stable()?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: out })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_n_unique(
    ptr: *mut SeriesContext,
    out_count: *mut usize,
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*ptr };
        
        let count = ctx.series.n_unique()?; 
        
        unsafe { *out_count = count };
        
        Ok(())
    })
}
// --- Scalar Access ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_i64(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i64,
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Int64(v) => unsafe { *out_val = v; *out_is_null = false; },
            AnyValue::Int32(v) => unsafe { *out_val = v as i64; *out_is_null = false; },
            AnyValue::Int16(v) => unsafe { *out_val = v as i64; *out_is_null = false; },
            AnyValue::Int8(v)  => unsafe { *out_val = v as i64; *out_is_null = false; },
            AnyValue::UInt64(v) => unsafe { *out_val = v as i64; *out_is_null = false; }, 
            AnyValue::UInt32(v) => unsafe { *out_val = v as i64; *out_is_null = false; },
            AnyValue::Null => unsafe { *out_is_null = true; },
            other => polars_bail!(ComputeError: "Expected Integer, got DataType: {:?}", other.dtype()),
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_i128(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i128, 
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Int128(v) => unsafe { *out_val = v; *out_is_null = false; },
            AnyValue::Int64(v)  => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::Int32(v)  => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::Int16(v)  => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::Int8(v)   => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::UInt64(v) => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::UInt32(v) => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::UInt16(v) => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::UInt8(v)  => unsafe { *out_val = v as i128; *out_is_null = false; },
            AnyValue::Null      => unsafe { *out_is_null = true; },
            other => polars_bail!(ComputeError: "Expected Integer, got DataType: {:?}", other.dtype()),
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_u128(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut u128, 
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::UInt128(v) => unsafe { *out_val = v; *out_is_null = false; },
            AnyValue::UInt64(v)  => unsafe { *out_val = v as u128; *out_is_null = false; },
            AnyValue::UInt32(v)  => unsafe { *out_val = v as u128; *out_is_null = false; },
            AnyValue::UInt16(v)  => unsafe { *out_val = v as u128; *out_is_null = false; },
            AnyValue::UInt8(v)   => unsafe { *out_val = v as u128; *out_is_null = false; },
            AnyValue::Int64(v) if v >= 0 => unsafe { *out_val = v as u128; *out_is_null = false; },
            AnyValue::Null       => unsafe { *out_is_null = true; },
            other => polars_bail!(ComputeError: "Expected Unsigned Integer, got DataType: {:?}", other.dtype()),
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_f64(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut f64, 
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Float64(v) => { 
                unsafe { *out_val = v; *out_is_null = false; } 
            },
            AnyValue::Float32(v) => { 
                unsafe { *out_val = v as f64; *out_is_null = false; } 
            },
            AnyValue::Float16(v) => { 
                unsafe { *out_val = v.0.to_f64(); *out_is_null = false; } 
            },
            AnyValue::Null => {
                unsafe { *out_is_null = true; }
            },
            other => {
                polars_bail!(ComputeError: "Expected Float type, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_bool(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut bool,
    out_is_null: *mut bool 
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds for Series of length {}", idx, ctx.series.len());
        }

        match ctx.series.get(idx)? {
            AnyValue::Boolean(v) => {
                unsafe { 
                    *out_val = v; 
                    *out_is_null = false;
                }
            },
            AnyValue::Null => {
                unsafe { 
                    *out_val = false; 
                    *out_is_null = true;
                }
            },
            other => {
                polars_bail!(ComputeError: "Expected Boolean, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_str(s_ptr: *mut SeriesContext, idx: usize) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::String(s) => {
                let c_str = CString::new(s)
                    .map_err(|e| polars_err!(ComputeError: "String at index {} contains null byte: {}", idx, e))?;
                Ok(c_str.into_raw())
            },
            AnyValue::Null => {
                Ok(std::ptr::null_mut())
            },
            other => {
                polars_bail!(ComputeError: "Expected String, got DataType: {:?}", other.dtype());
            }
        }
    })
}
// Decimal 
// out_val: i128 value
// out_scale: scale 
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_decimal(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i128, 
    out_precision: *mut usize, 
    out_scale: *mut usize,
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Decimal(v, precision, scale) => {
                unsafe {
                    *out_val = v;
                    *out_precision = precision;
                    *out_scale = scale;
                    *out_is_null = false;
                }
            },
            AnyValue::Null => {
                unsafe { 
                    *out_val = 0; 
                    *out_precision = 0;
                    *out_scale = 0;
                    *out_is_null = true; 
                }
            },
            other => {
                polars_bail!(ComputeError: "Expected Decimal, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_date(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i32,
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Date(v) => { 
                unsafe { 
                    *out_val = v; 
                    *out_is_null = false;
                } 
            },
            AnyValue::Null => {
                unsafe { *out_is_null = true; }
            },
            other => {
                polars_bail!(ComputeError: "Expected Date, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_time(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i64,
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Time(v) => { 
                unsafe { 
                    *out_val = v; 
                    *out_is_null = false;
                } 
            },
            AnyValue::Null => {
                unsafe { *out_is_null = true; }
            },
            other => {
                polars_bail!(ComputeError: "Expected Time, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_datetime(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i64, 
    out_time_unit: *mut u8,      
    out_timezone: *mut *mut c_char, 
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Datetime(v, tu, tz) => {
                let tu_val = match tu {
                    TimeUnit::Nanoseconds => 0,
                    TimeUnit::Microseconds => 1,
                    TimeUnit::Milliseconds => 2,
                };

                let tz_ptr = match tz {
                    Some(tz_str) => {
                        let c_str = CString::new(tz_str.as_str())
                            .map_err(|e| polars_err!(ComputeError: "TimeZone contains null byte: {}", e))?;
                        c_str.into_raw()
                    },
                    None => std::ptr::null_mut(),
                };

                unsafe { 
                    *out_val = v; 
                    *out_time_unit = tu_val;
                    *out_timezone = tz_ptr;
                    *out_is_null = false; 
                }
            },
            AnyValue::Null => {
                unsafe { 
                    *out_is_null = true; 
                    *out_timezone = std::ptr::null_mut(); 
                }
            },
            other => {
                polars_bail!(ComputeError: "Expected Datetime, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_duration(
    s_ptr: *mut SeriesContext, 
    idx: usize, 
    out_val: *mut i64, 
    out_time_unit: *mut u8, 
    out_is_null: *mut bool
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };
        
        if idx >= ctx.series.len() {
            polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
        }

        match ctx.series.get(idx)? {
            AnyValue::Duration(v, tu) => {
                let tu_val = match tu {
                    TimeUnit::Nanoseconds => 0,
                    TimeUnit::Microseconds => 1,
                    TimeUnit::Milliseconds => 2,
                };
                unsafe { 
                    *out_val = v; 
                    *out_time_unit = tu_val;
                    *out_is_null = false; 
                }
            },
            AnyValue::Null => {
                unsafe { *out_is_null = true; }
            },
            other => {
                polars_bail!(ComputeError: "Expected Duration, got DataType: {:?}", other.dtype());
            }
        }
        
        Ok(())
    })
}

// ==========================================
// Arithmetic Ops 
// ==========================================

macro_rules! impl_series_arithmetic_op {
    ($func_name:ident, $op:tt) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let s1_ref = unsafe { &(*s1).series };
                let s2_ref = unsafe { &(*s2).series };
                let res = s1_ref $op s2_ref; 
                Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
            })
        }
    };
}

impl_series_arithmetic_op!(pl_series_add, +);
impl_series_arithmetic_op!(pl_series_sub, -);
impl_series_arithmetic_op!(pl_series_mul, *);
impl_series_arithmetic_op!(pl_series_div, /);
impl_series_arithmetic_op!(pl_series_rem, %);

// ==========================================
// Comparison Ops 
// ==========================================

macro_rules! impl_series_comparison_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let s1_ref = unsafe { &(*s1).series };
                let s2_ref = unsafe { &(*s2).series };
                let res = s1_ref.$method(s2_ref)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                    .into_series();
                Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
            })
        }
    };
}

impl_series_comparison_op!(pl_series_eq, equal);
impl_series_comparison_op!(pl_series_eq_missing, equal_missing);
impl_series_comparison_op!(pl_series_neq, not_equal);
impl_series_comparison_op!(pl_series_neq_missing, not_equal_missing);
impl_series_comparison_op!(pl_series_gt, gt);
impl_series_comparison_op!(pl_series_gt_eq, gt_eq);
impl_series_comparison_op!(pl_series_lt, lt);
impl_series_comparison_op!(pl_series_lt_eq, lt_eq);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_get_dtype(ptr: *mut Series) -> *mut DataType {
    ffi_try!({
        let s = unsafe {&*ptr};
        Ok(Box::into_raw(Box::new(s.dtype().clone())))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_not(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        
        // Downcast to BooleanChunked. If it's not a boolean series, this will automatically 
        // return a PolarsError which ffi_try! will catch and pass to the C# side safely.
        let bool_ca = ctx.series.bool()?;
        
        // Apply logical NOT operation and convert back to Series
        let res = (!bool_ca).into_series();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

// ==========================================
// Operations
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_sort(
    series_ptr: *mut SeriesContext,
    descending: bool,
    nulls_last: bool,
    multithreaded: bool,
    maintain_order: bool
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*series_ptr };
        
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded,
            maintain_order,
            limit: None, 
        };

        let out = ctx.series.sort(options)?;
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: out })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_reshape(
    series_ptr: *const Series,
    dims_ptr: *const i64,
    dims_len: usize,
) -> *mut Series {
    ffi_try!({
        let s = unsafe { &*series_ptr };
        
        let raw_dims = unsafe { from_raw_parts(dims_ptr, dims_len) };
        
        let mut dimensions = Vec::with_capacity(dims_len);
        for &d in raw_dims {
            if d == -1 {
                dimensions.push(ReshapeDimension::Infer);
            } else if d > 0 {
                dimensions.push(ReshapeDimension::Specified(Dimension::new(d as u64)));
            } else {
                polars_bail!(InvalidOperation: "dimension size must be > 0 or -1 (infer)");
            }
}
        
        let reshaped = s.reshape_array(&dimensions)?;
        
        Ok(Box::into_raw(Box::new(reshaped)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_struct_unnest(series_ptr: *mut SeriesContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*series_ptr };
        let s = &ctx.series;

        let ca = s.struct_()?;

        let df = ca.clone().unnest();

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_append(s_ptr: *mut SeriesContext, other_ptr: *mut SeriesContext) -> bool {
    ffi_bool_try!({
        if s_ptr.is_null() {
            polars_bail!(ComputeError: "Target Series pointer is null");
        }
        if other_ptr.is_null() {
            polars_bail!(ComputeError: "Series to append pointer is null");
        }

        let target_ctx = unsafe { &mut *s_ptr };
        let other_ctx = unsafe { &*other_ptr };
        
        target_ctx.series.append(&other_ctx.series)?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_shrink_to_fit(ptr: *mut SeriesContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            let ctx = unsafe { &mut *ptr };
            // Shrink the memory footprint of the Series in-place
            ctx.series.shrink_to_fit();
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_extend(s_ptr: *mut SeriesContext, other_ptr: *mut SeriesContext) -> bool {
    ffi_bool_try!({
        if s_ptr.is_null() {
            polars_bail!(ComputeError: "Target Series pointer is null");
        }
        if other_ptr.is_null() {
            polars_bail!(ComputeError: "Series to extend pointer is null");
        }

        let target_ctx = unsafe { &mut *s_ptr };
        let other_ctx = unsafe { &*other_ptr };
        
        target_ctx.series.extend(&other_ctx.series)?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_sorted_flags(
    series_ptr: *mut SeriesContext,
    out_flags: *mut u8,
) -> c_int {
    ffi_try_c_int!({
        let ctx = unsafe { &*series_ptr };
        
        let mut flags: u8 = 0;
        match ctx.series.is_sorted_flag() {
            polars::series::IsSorted::Ascending => {
                flags |= 1; // IsSorted
            },
            polars::series::IsSorted::Descending => {
                flags |= 1; // IsSorted
                flags |= 2; // Descending
            },
            _ => {}
        }
        // TODO for 0.54: 
        // if ctx.series.nulls_last_flag() { flags |= 4; }

        if !out_flags.is_null() {
            unsafe { *out_flags = flags };
        }
        
        Ok(0)
    })
}

/// Set the sorted flag of the Series.
/// Returns a new SeriesContext.
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_set_sorted_flag(
    series_ptr: *mut SeriesContext,
    descending: bool,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*series_ptr };
        
        let mut s = ctx.series.clone();
        
        let sorted_flag = if descending {
            polars::series::IsSorted::Descending
        } else {
            polars::series::IsSorted::Ascending
        };
        
        s.set_sorted_flag(sorted_flag);

        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_set_with_mask(
    s_ptr: *mut SeriesContext,
    mask_ptr: *mut SeriesContext,
    value_ptr: *mut SeriesContext
) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        let mask = unsafe { &(*mask_ptr).series };
        let value_s = unsafe { &(*value_ptr).series };

        let mask_bool = mask.bool().map_err(|_| {
            polars_err!(ComputeError: "Mask must be a boolean series")
        })?;
        
        let casted_value = value_s.cast(s.dtype())?;
        
        let broadcasted_value = if casted_value.len() == 1 && s.len() > 1 {
            casted_value.new_from_index(0, s.len())
        } else {
            casted_value.clone()
        };

        let result = broadcasted_value.zip_with(mask_bool, s)?;
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: result })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_series_scatter_indices(
    s_ptr: *mut SeriesContext,
    idx_ptr: *mut SeriesContext,
    value_ptr: *mut SeriesContext
) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        let idx_s = unsafe { &(*idx_ptr).series };
        let value_s = unsafe { &(*value_ptr).series };

        let idx_u32 = idx_s.cast(&DataType::UInt32)?;
        let idx_ca = idx_u32.u32()?;
        
        let casted_value = value_s.cast(s.dtype())?;
        let is_scalar = casted_value.len() == 1;

        if !is_scalar && casted_value.len() != idx_ca.len() {
            polars_bail!(ComputeError: "Value length must match indices length, or be a scalar");
        }

        let result = if is_scalar {
            let mut mask_vec = vec![false; s.len()];
            for opt_idx in idx_ca.iter() {
                if let Some(idx) = opt_idx {
                    let idx_usize = idx as usize;
                    if idx_usize < s.len() {
                        mask_vec[idx_usize] = true;
                    } else {
                        polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
                    }
                }
            }
            let mask_bool = BooleanChunked::from_slice("mask".into(), &mask_vec);
            
            let broadcasted_value = casted_value.new_from_index(0, s.len());
            broadcasted_value.zip_with(&mask_bool, s)?
        } else {
            let mut s_vec: Vec<AnyValue> = s.iter().collect();
            for (i, opt_idx) in idx_ca.iter().enumerate() {
                if let Some(idx) = opt_idx {
                    let idx_usize = idx as usize;
                    if idx_usize < s.len() {
                        s_vec[idx_usize] = casted_value.get(i)?;
                    } else {
                        polars_bail!(OutOfBounds: "Index {} is out of bounds", idx);
                    }
                }
            }
            Series::new(s.name().clone(), &s_vec).cast(s.dtype())?
        };
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: result })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_zip_with(
    s_ptr: *mut SeriesContext,
    mask_ptr: *mut SeriesContext,
    other_ptr: *mut SeriesContext,
) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        let mask_series = unsafe { &(*mask_ptr).series };
        let other = unsafe { &(*other_ptr).series };

        // Convert the mask Series to a BooleanChunked
        // The ? operator will safely propagate an error to C# if the mask is not boolean
        let mask = mask_series.bool()?;
        
        // Execute zip_with
        let res = s.zip_with(mask, other)?;

        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_dummies(
    s_ptr: *mut SeriesContext,
    separator: *const c_char,
    drop_first: bool,
    drop_nulls: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        
        let sep_str = if separator.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(separator).to_str().unwrap() })
        };

        let df = ctx.series.to_dummies(sep_str, drop_first, drop_nulls)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_from_index(
    s_ptr: *mut SeriesContext,
    index:usize,
    length:usize
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        
        let s = ctx.series.new_from_index(index,length);
        
        Ok(Box::into_raw(Box::new(SeriesContext { series:s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_equals(
    ptr1: *mut SeriesContext,
    ptr2: *mut SeriesContext,
    out: *mut bool,
) -> i32 {
    ffi_try_c_int!({
        let ctx1 = unsafe { &*ptr1 };
        let ctx2 = unsafe { &*ptr2 };
        
        unsafe {
            *out = ctx1.series.equals_missing(&ctx2.series);
        }
        
        Ok(0) 
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_hash(
    ptr: *mut SeriesContext,
    out: *mut u64,
) -> i32 {
    ffi_try_c_int!({
        let ctx = unsafe { &*ptr };
        
        let mut hasher = DefaultHasher::new();
        
        std::hash::Hash::hash(&Wrap(ctx.series.clone()), &mut hasher);
        
        unsafe {
            *out = hasher.finish();
        }
        
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_clear(
    s_ptr: *mut SeriesContext,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };

        let s = ctx.series.clear();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series:s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_physical(
    s_ptr: *mut SeriesContext,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };

        let s = ctx.series.to_physical_repr();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series:s.into_owned() })))
    })
}