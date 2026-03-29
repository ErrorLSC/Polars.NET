use polars::prelude::*;
use polars_arrow::array::{Array,FixedSizeListArray,ListArray, Utf8ViewArray, View};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use crate::pl_io::arrow::ArrowArrayContext;
use crate::types::{DataFrameContext, DataTypeContext, SeriesContext};
use polars_arrow::datatypes::ArrowDataType;
use polars_buffer::Buffer;
use polars_arrow::array::PrimitiveArray;
use polars_arrow::array::BooleanArray;
use polars_arrow::bitmap::Bitmap;
use crate::datatypes::parse_timeunit;

// ==========================================
// Constructors 
// ==========================================
macro_rules! gen_series_new {
    ($func_name:ident, $rs_type:ty, $pl_type:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $func_name(
            name: *const c_char,
            ptr: *const $rs_type,
            validity: *const u8, 
            len: usize,
        ) -> *mut SeriesContext {
            ffi_try!({
                let name = unsafe {CStr::from_ptr(name).to_string_lossy()};
                
                // 1. Values: Convert to Vec 
                // slice.to_vec() is memcpy
                let slice = unsafe {std::slice::from_raw_parts(ptr, len)};
                let values_vec = slice.to_vec(); 
                let values_buffer = Buffer::from(values_vec);

                // 2. Validity: Convert to Vec<u8> 
                let validity_bitmap = if validity.is_null() {
                    None
                } else {
                    let bytes_len = (len + 7) / 8;
                    let v_slice =unsafe { std::slice::from_raw_parts(validity, bytes_len)};
                    let v_vec = v_slice.to_vec(); 
                    
                    Some(Bitmap::try_new(v_vec, len).unwrap())
                };

                // 3. Assemble
                let arrow_dtype = <$pl_type as PolarsDataType>::get_static_dtype().to_arrow(CompatLevel::newest());
                
                let arrow_array = PrimitiveArray::new(
                    arrow_dtype,
                    values_buffer,
                    validity_bitmap
                );

                let ca = ChunkedArray::<$pl_type>::with_chunk(
                    PlSmallStr::from_str(name.as_ref()), 
                    arrow_array,
                );
                
                Ok(Box::into_raw(Box::new(SeriesContext { series: ca.into_series() })))
            })
        }
    };
}
gen_series_new!(pl_series_new_i8,  i8,  Int8Type);
gen_series_new!(pl_series_new_u8,  u8,  UInt8Type);
gen_series_new!(pl_series_new_i16, i16, Int16Type);
gen_series_new!(pl_series_new_u16, u16, UInt16Type);
gen_series_new!(pl_series_new_i32, i32, Int32Type);
gen_series_new!(pl_series_new_u32, u32, UInt32Type);
gen_series_new!(pl_series_new_i64, i64, Int64Type);
gen_series_new!(pl_series_new_u64, u64, UInt64Type);
gen_series_new!(pl_series_new_f16, pf16, Float16Type);
gen_series_new!(pl_series_new_f32, f32, Float32Type);
gen_series_new!(pl_series_new_f64, f64, Float64Type);
gen_series_new!(pl_series_new_i128, i128, Int128Type);
gen_series_new!(pl_series_new_u128, u128, UInt128Type);

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
pub unsafe extern "C" fn pl_series_new_bool(
    name: *const c_char,
    ptr: *const u8,     
    validity: *const u8,
    len: usize,
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let bytes_len = (len + 7) / 8;

        let slice = unsafe {std::slice::from_raw_parts(ptr, bytes_len)};
        let values_vec = slice.to_vec();
        let values_bitmap = Bitmap::try_new(values_vec, len).unwrap();

        let validity_bitmap = if validity.is_null() {
            None
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, bytes_len)};
            let v_vec = v_slice.to_vec();
            Some(Bitmap::try_new(v_vec, len).unwrap())
        };

        let arrow_array = BooleanArray::new(
            ArrowDataType::Boolean, 
            values_bitmap, 
            validity_bitmap
        );

        let ca = BooleanChunked::with_chunk(PlSmallStr::from_str(name.as_ref()), arrow_array);
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: ca.into_series() })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_str_simd(
    name: *const c_char,
    values_ptr: *const u8,   // dataBuffer from Host language
    values_len: usize,       // dataBuffer len
    offsets_ptr: *const i64, // ArrowStringView array
    validity_ptr: *const u8, // Validity Bitmap
    len: usize               // logic length
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

        // Views Buffer: i64* => View*
        let views_ptr = offsets_ptr as *const View;
        let views_slice = unsafe { std::slice::from_raw_parts(views_ptr, len) };
        let views_vec = views_slice.to_vec();
        let views_buffer = Buffer::from(views_vec); 

        // Data Buffers 
        let mut data_buffers_vec = Vec::new();
        if !values_ptr.is_null() && values_len > 0 {
            let data_slice = unsafe { std::slice::from_raw_parts(values_ptr, values_len) };
            let data_vec = data_slice.to_vec();
            data_buffers_vec.push(Buffer::from(data_vec));
        }
        let buffers = Buffer::from(data_buffers_vec); 

        // Validity (Bitmap)
        let validity = if validity_ptr.is_null() {
            None
        } else {
            let bytes_len = (len + 7) / 8;
            let v_slice = unsafe { std::slice::from_raw_parts(validity_ptr, bytes_len) };
            let v_vec = v_slice.to_vec();
            Some(Bitmap::try_new(v_vec, len).expect("Invalid validity bitmap"))
        };

        // Build Arrow Utf8ViewArray
        let array = Utf8ViewArray::try_new(
            ArrowDataType::Utf8View,
            views_buffer,
            buffers,
            validity
        ).expect("Failed to build Utf8ViewArray from C# buffers");

        // Convert to Series
        let series = Series::from_arrow(
            PlSmallStr::from_str(name_str.as_ref()), 
            Box::new(array)
        ).expect("Failed to create Series");

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_datetime(
    name: *const c_char,
    ptr: *const i64,       
    validity: *const u8,   // Bitmap
    len: usize,
    unit: u8,   // "ms", "us", "ns"
    zone: *const c_char    // null is Naive (No timezone), or we need "Asia/Shanghai" and etc.
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        // Build Int64 ChunkedArray
        let bytes_len = (len + 7) / 8;
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        let vec = slice.to_vec(); // Copy from C# to Rust Heap
        
        let validity_bitmap = if validity.is_null() {
            None
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, bytes_len) };
            let v_vec = v_slice.to_vec();
            Some(Bitmap::try_new(v_vec, len).unwrap())
        };

        // Use Arrow Interface to build
        let arrow_array = PrimitiveArray::new(
            ArrowDataType::Int64, 
            vec.into(), 
            validity_bitmap
        );
        
        // Generate Int64Chunked
        let ca_i64 = Int64Chunked::with_chunk(PlSmallStr::from_str(name_str.as_ref()), arrow_array);

        // Parse TimeUnit
        let time_unit = parse_timeunit(unit);

        // Parse TimeZone
        let pl_tz = if zone.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(zone).to_str().unwrap() };
            
            Some(unsafe { TimeZone::new_unchecked(s) })
        };

        // Logical Cast
        let ca_dt = ca_i64.into_datetime(time_unit,pl_tz);

        Ok(Box::into_raw(Box::new(SeriesContext { series: ca_dt.into_series() })))
    })
}

macro_rules! create_physical_ca {
    ($name:ident, $ptr:ident, $len:ident, $validity:ident, $phys_ty:ty, $arrow_ty:expr, $ca_ty:ident) => {{
        let name_str = unsafe { CStr::from_ptr($name).to_string_lossy() };
        let bytes_len = ($len + 7) / 8;

        // 1. Data Copy (C# -> Rust Heap)
        let slice = unsafe { std::slice::from_raw_parts($ptr, $len) };
        let vec = slice.to_vec();

        // 2. Validity Bitmap Copy
        let validity_bitmap = if $validity.is_null() {
            None
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts($validity, bytes_len) };
            let v_vec = v_slice.to_vec();
            Some(Bitmap::try_new(v_vec, $len).unwrap()) // unwrap is safe if C# logic is correct
        };

        // 3. Arrow Array Construction
        let arrow_array = PrimitiveArray::new(
            $arrow_ty,
            vec.into(),
            validity_bitmap
        );

        // 4. Polars ChunkedArray (Physical)
        $ca_ty::with_chunk(PlSmallStr::from_str(name_str.as_ref()), arrow_array)
    }}
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_date(
    name: *const c_char,
    ptr: *const i32,      // Days since epoch
    validity: *const u8,
    len: usize,
) -> *mut SeriesContext {
    ffi_try!({
        // Build Int32Chunked
        let ca = create_physical_ca!(name, ptr, len, validity, i32, ArrowDataType::Int32, Int32Chunked);
        
        // Convert -> Date
        let ca_date = ca.into_date();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: ca_date.into_series() })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_time(
    name: *const c_char,
    ptr: *const i64,      // Nanoseconds since midnight
    validity: *const u8,
    len: usize,
) -> *mut SeriesContext {
    ffi_try!({
        let ca = create_physical_ca!(name, ptr, len, validity, i64, ArrowDataType::Int64, Int64Chunked);
        
        let ca_time = ca.into_time();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: ca_time.into_series() })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_duration(
    name: *const c_char,
    ptr: *const i64,      
    validity: *const u8,
    len: usize,
    unit: u8,             // 0=ns, 1=us, 2=ms
) -> *mut SeriesContext {
    ffi_try!({
        let ca = create_physical_ca!(name, ptr, len, validity, i64, ArrowDataType::Int64, Int64Chunked);
        
        let time_unit = parse_timeunit(unit);

        let ca_duration = ca.into_duration(time_unit);
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: ca_duration.into_series() })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_decimal(
    name: *const c_char,
    ptr: *const i128,      // physical data: Int128
    validity: *const u8,   // Bitmap
    len: usize,
    precision: usize,      
    scale: usize           
) -> *mut SeriesContext {
    
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let bytes_len = (len + 7) / 8;

        // 1. Data Copy (i128)
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        let vec = slice.to_vec();

        // 2. Validity
        let validity_bitmap = if validity.is_null() {
            None
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, bytes_len) };
            let v_vec = v_slice.to_vec();
            Some(Bitmap::try_new(v_vec, len).unwrap())
        };

        // 3. Build Arrow Decimal Array
        let data_type = ArrowDataType::Decimal(
            if precision == 0 { 38 } else { precision }, 
            scale
        );
        
        let arrow_array = PrimitiveArray::new(
            data_type,
            vec.into(),
            validity_bitmap
        );

        // 4. Wrap into Series
        let series = Series::from_arrow(PlSmallStr::from_str(name_str.as_ref()), Box::new(arrow_array)).unwrap();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

macro_rules! impl_fixed_list_ffi {
    ($func_name:ident, $rust_ty:ty, $arrow_ty:expr) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $func_name(
            name: *const c_char,
            flat_ptr: *const $rust_ty,  
            flat_len: usize,
            validity: *const u8,
            parent_len: usize,
            width: usize,
        ) -> *mut SeriesContext {
            ffi_try!({
                let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

                // Build Inner Child (Primitive Array)
                let slice = unsafe { std::slice::from_raw_parts(flat_ptr, flat_len) };
                let vec = slice.to_vec();
                
                // PrimitiveArray::new is generic type
                let inner_array = PrimitiveArray::new(
                    $arrow_ty,
                    vec.into(),
                    None
                );

                // Build Validity
                let validity_bitmap = if validity.is_null() {
                    None
                } else {
                    let bytes_len = (parent_len + 7) / 8;
                    let v_slice = unsafe { std::slice::from_raw_parts(validity, bytes_len) };
                    Some(Bitmap::try_new(v_slice.to_vec(), parent_len).unwrap())
                };

                // Construct FixedSizeList
                let inner_field = polars_arrow::datatypes::Field::new("item".into(), $arrow_ty,false);
                let list_dtype = ArrowDataType::FixedSizeList(
                    Box::new(inner_field),
                    width
                );

                let list_array = FixedSizeListArray::new(
                    list_dtype,
                    parent_len,
                    Box::new(inner_array),
                    validity_bitmap,
                );

                // 4. Series Wrap
                let s = Series::from_arrow(PlSmallStr::from_str(name_str.as_ref()), Box::new(list_array)).unwrap();
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            })
        }
    };
}

// ============================================================================
// FixedSizeList (Array) Generators
// ============================================================================

// Int
impl_fixed_list_ffi!(pl_series_new_array_i8,  i8,  ArrowDataType::Int8);
impl_fixed_list_ffi!(pl_series_new_array_i16, i16, ArrowDataType::Int16);
impl_fixed_list_ffi!(pl_series_new_array_i32, i32, ArrowDataType::Int32);
impl_fixed_list_ffi!(pl_series_new_array_i64, i64, ArrowDataType::Int64);
impl_fixed_list_ffi!(pl_series_new_array_i128, i128, ArrowDataType::Int128);

// UInt
impl_fixed_list_ffi!(pl_series_new_array_u8,  u8,  ArrowDataType::UInt8);
impl_fixed_list_ffi!(pl_series_new_array_u16, u16, ArrowDataType::UInt16);
impl_fixed_list_ffi!(pl_series_new_array_u32, u32, ArrowDataType::UInt32);
impl_fixed_list_ffi!(pl_series_new_array_u64, u64, ArrowDataType::UInt64);
impl_fixed_list_ffi!(pl_series_new_array_u128, u128, ArrowDataType::UInt128);

// Float
impl_fixed_list_ffi!(pl_series_new_array_f16, pf16, ArrowDataType::Float16);
impl_fixed_list_ffi!(pl_series_new_array_f32, f32, ArrowDataType::Float32);
impl_fixed_list_ffi!(pl_series_new_array_f64, f64, ArrowDataType::Float64);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_new_array_decimal(
    name: *const c_char,
    flat_ptr: *const u64,       
    flat_len: usize,
    validity: *const u8,
    parent_len: usize,
    width: usize,
    scale: usize,               
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

        // 1. Build Inner Child (PrimitiveArray<i128>)
        let slice_u64 = unsafe { std::slice::from_raw_parts(flat_ptr, flat_len * 2) };
        let vec_i128: Vec<i128> = slice_u64
            .chunks_exact(2)
            .map(|chunk| {
                let low = chunk[0];
                let high = chunk[1];
                ((high as i128) << 64) | (low as i128)
            })
            .collect();

        let decimal_dtype = ArrowDataType::Decimal(38, scale);

        let inner_array = PrimitiveArray::new(
            decimal_dtype.clone(), 
            vec_i128.into(), // Vec<i128> -> Buffer
            None // Inner validity (assuming dense flat array implies no inner nulls for now)
        );

        // 2. Build Validity Bitmap (Parent List Validity)
        let validity_bitmap = if validity.is_null() {
            None
        } else {
            let bytes_len = (parent_len + 7) / 8;
            let v_slice = unsafe { std::slice::from_raw_parts(validity, bytes_len) };
            Some(Bitmap::try_new(v_slice.to_vec(), parent_len).unwrap())
        };

        // 3. Construct FixedSizeListArray
        // Inner Field must also be Decimal
        let inner_field = polars_arrow::datatypes::Field::new("item".into(), decimal_dtype, false);
        
        let list_dtype = ArrowDataType::FixedSizeList(
            Box::new(inner_field),
            width
        );

        let list_array = FixedSizeListArray::new(
            list_dtype,
            parent_len,
            Box::new(inner_array),
            validity_bitmap,
        );

        // 4. Wrap in Series
        let s = Series::from_arrow(PlSmallStr::from_str(name_str.as_ref()), Box::new(list_array)).unwrap();
        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_struct(
    name: *const c_char,
    fields_ptrs: *const *mut SeriesContext, 
    len: usize,                             
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        // Get Series Ptr
        let ptrs = unsafe { std::slice::from_raw_parts(fields_ptrs, len) };
        
        // Collect Series
        let fields: Vec<Series> = ptrs
            .iter()
            .map(|&ptr| unsafe { &*ptr }.series.clone())
            .collect();

        // Calc Struct Height
        let struct_height = fields.first().map(|s| s.len()).unwrap_or(0);

        // Call StructChunked::from_series
        let ca = StructChunked::from_series(
            PlSmallStr::from_str(name_str.as_ref()), // &str -> PlSmallStr
            struct_height, 
            fields.iter()
        )?;

        let s = ca.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}

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
pub extern "C" fn pl_series_chunk_count(ptr: *mut SeriesContext,out_count: *mut u32) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*ptr };

        let count = ctx.series.n_chunks();

        unsafe { *out_count = count as u32 };

        Ok(())
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
pub extern "C" fn pl_series_len(ptr: *mut SeriesContext,out_count: *mut u32) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*ptr };

        let count = ctx.series.len();

        unsafe { *out_count = count as u32 };
        
        Ok(())
    })
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
    dtype_ptr: *mut DataTypeContext
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let target_dtype = unsafe { &(*dtype_ptr).dtype };
        
        let s = ctx.series.cast(target_dtype)?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_null(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.is_null().into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_not_null(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.is_not_null().into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
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
pub extern "C" fn pl_series_null_count(s_ptr: *mut SeriesContext,out_count: *mut usize) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };

        let count = ctx.series.null_count();
        
        unsafe { *out_count = count };
            
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
impl_series_comparison_op!(pl_series_neq, not_equal);
impl_series_comparison_op!(pl_series_gt, gt);
impl_series_comparison_op!(pl_series_gt_eq, gt_eq);
impl_series_comparison_op!(pl_series_lt, lt);
impl_series_comparison_op!(pl_series_lt_eq, lt_eq);
// ==========================================
// Aggregations
// ==========================================

macro_rules! impl_series_reduce_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let s = unsafe { &(*s_ptr).series };
                let res = s.$method()?.into_series(s.name().clone());
                Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
            })
        }
    };
}

impl_series_reduce_op!(pl_series_sum, sum_reduce);
impl_series_reduce_op!(pl_series_min, min_reduce);
impl_series_reduce_op!(pl_series_max, max_reduce);
impl_series_reduce_op!(pl_series_mean, mean_reduce);

macro_rules! impl_series_unary_bool_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let s = unsafe { &(*s_ptr).series }; 
                let res = s.$method()?.into_series();
                Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
            })
        }
    };
}

impl_series_unary_bool_op!(pl_series_is_nan, is_nan);
impl_series_unary_bool_op!(pl_series_is_not_nan, is_not_nan);
impl_series_unary_bool_op!(pl_series_is_finite, is_finite);
impl_series_unary_bool_op!(pl_series_is_infinite, is_infinite);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_get_dtype(ptr: *mut Series) -> *mut DataType {
    ffi_try!({
        let s = unsafe {&*ptr};
        Ok(Box::into_raw(Box::new(s.dtype().clone())))
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
pub extern "C" fn pl_series_value_counts(
    s_ptr: *mut SeriesContext,
    sort: bool,
    parallel: bool,
    name: *const c_char,
    normalize: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        
        let c_str = unsafe { CStr::from_ptr(name) };
        let name_str = c_str.to_str().map_err(|e| polars_err!(ComputeError: "Series name contains null byte: {}", e))?;
        let pl_name = PlSmallStr::from_str(name_str); 

        let df = ctx.series.value_counts(sort, parallel, pl_name, normalize)?;
        
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