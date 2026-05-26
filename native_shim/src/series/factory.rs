use polars::prelude::*;
use polars_arrow::array::{FixedSizeListArray, Utf8ViewArray, View};
use std::ffi::CStr;
use std::os::raw::c_char;
use crate::types::DataTypeContext;
use crate::types::SeriesContext;
use polars_arrow::datatypes::ArrowDataType;
use polars_buffer::Buffer;
use polars_arrow::array::PrimitiveArray;
use polars_arrow::array::BooleanArray;
use polars_arrow::bitmap::Bitmap;
use crate::datatypes::parse_timeunit;

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
pub extern "C" fn pl_series_new_empty(
    name: *const c_char,
    dtype_ptr: *const DataTypeContext
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let dtype = unsafe {&*dtype_ptr};
        
        let s = Series::new_empty(name_str.into(), &dtype.dtype);
        
        Ok(Box::into_raw(Box::new(SeriesContext { series:s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_null(
    name: *const c_char,
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        let s = Series::new_null(name_str.into(), len);
        
        Ok(Box::into_raw(Box::new(SeriesContext { series:s })))
    })
}