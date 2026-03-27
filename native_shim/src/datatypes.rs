use std::{ffi::{CStr, CString, c_char}, panic::{AssertUnwindSafe, catch_unwind}};
use crate::{error::set_error, types::DataTypeContext, utils::ptr_to_str};
use polars::prelude::*;

macro_rules! define_pl_datatype_kind {
    (
        $(#[$meta:meta])*
        pub enum PlDataTypeKind {
            // Format: Variant = Discriminant <=> MatchPattern => Constructor
            $($Variant:ident = $Val:literal <=> $MatchPat:pat => $Constructor:expr),* $(,)?
        }
    ) => {
        // 1. Generate Enum Defination
        #[repr(i32)]
        $(#[$meta])*
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum PlDataTypeKind {
            $($Variant = $Val),*
        }

        impl PlDataTypeKind {
            // Helper：Convert i32 to Enum
            pub fn from_i32(code: i32) -> Option<Self> {
                match code {
                    $($Val => Some(PlDataTypeKind::$Variant)),*,
                    _ => None,
                }
            }

            pub fn to_default_datatype(self) -> DataType {
                match self {
                    $(PlDataTypeKind::$Variant => $Constructor),*
                }
            }
        }

        pub fn map_dtype_to_kind(dtype: &DataType) -> PlDataTypeKind {
            match dtype {
                $($MatchPat => PlDataTypeKind::$Variant),*,
                _ => PlDataTypeKind::Unknown,
            }
        }
    };
}

// === Marco here ===
define_pl_datatype_kind! {
    pub enum PlDataTypeKind {
        Unknown     = 0  <=> DataType::Unknown(_)      => DataType::Unknown(Default::default()),
        Boolean     = 1  <=> DataType::Boolean         => DataType::Boolean,
        Int8        = 2  <=> DataType::Int8            => DataType::Int8,
        Int16       = 3  <=> DataType::Int16           => DataType::Int16,
        Int32       = 4  <=> DataType::Int32           => DataType::Int32,
        Int64       = 5  <=> DataType::Int64           => DataType::Int64,
        UInt8       = 6  <=> DataType::UInt8           => DataType::UInt8,
        UInt16      = 7  <=> DataType::UInt16          => DataType::UInt16,
        UInt32      = 8  <=> DataType::UInt32          => DataType::UInt32,
        UInt64      = 9  <=> DataType::UInt64          => DataType::UInt64,
        Float32     = 10 <=> DataType::Float32         => DataType::Float32,
        Float64     = 11 <=> DataType::Float64         => DataType::Float64,
        String      = 12 <=> DataType::String          => DataType::String,
        Date        = 13 <=> DataType::Date            => DataType::Date,
        Datetime    = 14 <=> DataType::Datetime(_, _)  => DataType::Datetime(TimeUnit::Microseconds, None),
        Time        = 15 <=> DataType::Time            => DataType::Time,
        Duration    = 16 <=> DataType::Duration(_)     => DataType::Duration(TimeUnit::Microseconds),
        Binary      = 17 <=> DataType::Binary          => DataType::Binary,
        Null        = 18 <=> DataType::Null            => DataType::Null,
        Struct      = 19 <=> DataType::Struct(_)       => DataType::Struct(vec![]),
        List        = 20 <=> DataType::List(_)         => DataType::List(Box::new(DataType::Null)),
        Categorical = 21 <=> DataType::Categorical(_, _) => DataType::Categorical(Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32),Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32).mapping()),
        Decimal     = 22 <=> DataType::Decimal(_, _)   => DataType::Decimal(38, 0),
        Array       = 23 <=> DataType::Array(_, _)     => DataType::Array(Box::new(DataType::Null), 0),
        Int128      = 24  <=> DataType::Int128        => DataType::Int128,
        UInt128     = 25  <=> DataType::UInt128        => DataType::UInt128,
        Float16     = 26  <=> DataType::Float16       => DataType:: Float16
    }
}
// --- Constructors ---

// Primitive Type
// 0=Bool, 1=Int8, ... (Same as C# defined enum)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_new_primitive(code: i32) -> *mut DataType {
    // 1. Convert code to Kind
    if let Some(kind) = PlDataTypeKind::from_i32(code) {
        // 2. Convert Kind to DataType
        let dtype = kind.to_default_datatype();
        Box::into_raw(Box::new(dtype))
    } else {
        let dtype = DataType::Unknown(Default::default());
        Box::into_raw(Box::new(dtype))
    }
}

// Decimal 
// precision: 0 for None (auto detect), >0 for real precision
// scale: decimal places
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_decimal(precision: usize, scale: usize) -> *mut DataTypeContext {
    let prec = if precision == 0 { 38 } else { precision };
    let dtype = DataType::Decimal(prec, scale);
    Box::into_raw(Box::new(DataTypeContext { dtype }))
}

// Categorical 
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_categorical() -> *mut DataTypeContext {
    let cats = Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32);

    let mapping = cats.mapping();

    let dtype = DataType::Categorical(cats, mapping);
    
    Box::into_raw(Box::new(DataTypeContext { dtype }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_list(inner_ptr: *mut DataTypeContext) -> *mut DataTypeContext {
    assert!(!inner_ptr.is_null());
    
    // Get innertype reference
    let inner_ctx = unsafe { &*inner_ptr };
    
    // Build List Type
    // DataType::List needs Box<DataType>
    let list_dtype = DataType::List(Box::new(inner_ctx.dtype.clone()));
    
    Box::into_raw(Box::new(DataTypeContext { dtype: list_dtype }))
}

pub fn parse_timeunit(unit: u8) -> TimeUnit {
    let time_unit = match unit {
        0 => TimeUnit::Nanoseconds,
        1 => TimeUnit::Microseconds,
        2 => TimeUnit::Milliseconds,
        _ => TimeUnit::Microseconds, 
    };
    time_unit
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_duration(unit: u8) -> *mut DataTypeContext {
    let time_unit = parse_timeunit(unit);
    let dt = DataType::Duration(time_unit);
    Box::into_raw(Box::new(DataTypeContext { dtype:dt }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_datetime(
    unit_code: u8,     // 0=ns, 1=us, 2=ms
    tz_ptr: *const c_char // null=Naive, string=Aware
) -> *mut DataTypeContext {
    let time_unit = parse_timeunit(unit_code);

    let timezone = if tz_ptr.is_null() {
        None
    } else {
        unsafe { 
            let c_str = ptr_to_str(tz_ptr).unwrap();
            Some(TimeZone::from_static(c_str))
        }
    };

    let dtype = DataType::Datetime(time_unit, timezone);
    Box::into_raw(Box::new(DataTypeContext { dtype }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_array(
    inner_ptr: *mut DataTypeContext, 
    width: usize
) -> *mut DataTypeContext {
    // Check null pointer
    if inner_ptr.is_null() {
        return std::ptr::null_mut();
    }
    
    let inner_ctx = unsafe { &*inner_ptr };
    
    let array_dtype = DataType::Array(Box::new(inner_ctx.dtype.clone()), width);
    
    Box::into_raw(Box::new(DataTypeContext { dtype: array_dtype }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_struct(
    names: *const *const c_char,      
    types: *const *mut DataTypeContext, 
    len: usize
) -> *mut DataTypeContext {
    ffi_try!({
        let mut fields = Vec::with_capacity(len);
        
        let name_slice = unsafe { std::slice::from_raw_parts(names, len) };
        let type_slice = unsafe { std::slice::from_raw_parts(types, len) };

        for i in 0..len {
            let name_cstr = unsafe { CStr::from_ptr(name_slice[i]) };
            let name = name_cstr.to_str().unwrap().to_string();
            
            let dt_ptr = type_slice[i];
            let dt_box = unsafe { Box::from_raw(dt_ptr) };
            let dtype = dt_box.dtype;

            fields.push(Field::new(name.into(), dtype));
        }

        let dt = DataType::Struct(fields);
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype: dt })))
    })
}

fn dtype_to_string_verbose(dt: &DataType) -> String {
    match dt {
        // Struct：Concat "struct[name: type, ...]"
        DataType::Struct(fields) => {
            let content: Vec<String> = fields.iter()
                .map(|f| format!("{}: {}", f.name, dtype_to_string_verbose(&f.dtype)))
                .collect();
            format!("struct[{}]", content.join(", "))
        },
        
        // List：open innertype recursively
        DataType::List(inner) => {
            format!("list[{}]", dtype_to_string_verbose(inner))
        },
        
        DataType::Array(inner, width) => {
             format!("array[{}; {}]", dtype_to_string_verbose(inner), width)
        },
        
        _ => dt.to_string()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_to_string(dt_ptr: *mut DataTypeContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*dt_ptr };
        let s = dtype_to_string_verbose(&ctx.dtype);
        let c_str = CString::new(s).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        Ok(c_str.into_raw())
    })
}

// --- Destructor ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_free(ptr: *mut DataTypeContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_clone(ptr: *mut DataTypeContext) -> *mut DataTypeContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let new_dt = ctx.dtype.clone();
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype:new_dt})))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_get_kind(ptr: *mut DataType) -> i32 {
    if ptr.is_null() { return 0; }
    let dtype = unsafe {&*ptr};
    map_dtype_to_kind(dtype) as i32
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_get_time_unit(ptr: *mut DataType) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() { return -1; }
        let dtype = unsafe {&*ptr};
        match dtype {
            DataType::Datetime(u, _) | DataType::Duration(u) => match u {
                TimeUnit::Nanoseconds => 0,
                TimeUnit::Microseconds => 1,
                TimeUnit::Milliseconds => 2,
            },
            _ => -1
        }
    }));

    match result {
        Ok(val) => val,
        Err(_) => {
            set_error("Panic in pl_datatype_get_time_unit".to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_timezone(ptr: *mut DataType) -> *mut c_char {
    ffi_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }

        let dtype = unsafe { &*ptr };
        
        if let DataType::Datetime(_, Some(tz)) = dtype {
            let c_str = CString::new(tz.as_str())
                .map_err(|e| polars_err!(ComputeError: "TimeZone contains null byte: {}", e))?;
            
            Ok(c_str.into_raw())
        } else {
            Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_get_decimal_info(
    ptr: *mut DataType, 
    precision: *mut i32, 
    scale: *mut i32
) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() { return; }
        let dtype = unsafe {&*ptr};
        if let DataType::Decimal(p, s) = dtype {
            unsafe {*precision = *p as i32};
            unsafe {*scale = *s as i32};
        } else {
            unsafe {*precision = 0};
            unsafe {*scale = 0};
        }
    }));
    
    if result.is_err() {
        set_error("Panic in pl_datatype_get_decimal_info".to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_inner(ptr: *mut DataType) -> *mut DataType {
    ffi_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }

        let dtype = unsafe { &*ptr };
        
        match dtype {
            DataType::List(inner) | DataType::Array(inner, _) => {
                Ok(Box::into_raw(Box::new(*inner.clone())))
            },
            _ => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_array_width(
    ptr: *mut DataTypeContext, 
    out_width: *mut usize
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataTypeContext pointer is null");
        }
        
        let ctx = unsafe { &*ptr };
        
        match &ctx.dtype {
            DataType::Array(_, width) => {
                unsafe { *out_width = *width };
            },
            _ => {
                unsafe { *out_width = 0 };
            }
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_struct_len(
    ptr: *mut DataType,
    out_len: *mut usize
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }
        
        let dtype = unsafe { &*ptr };
        
        if let DataType::Struct(fields) = dtype {
            unsafe { *out_len = fields.len() };
            Ok(())
        } else {
            polars_bail!(ComputeError: "Expected Struct DataType, but got: {:?}", dtype);
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_struct_field(
    ptr: *mut DataType, 
    index: usize, 
    name_out: *mut *mut c_char, 
    type_out: *mut *mut DataType
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }
        
        let dtype = unsafe { &*ptr };
        
        if let DataType::Struct(fields) = dtype {
            if index >= fields.len() {
                polars_bail!(OutOfBounds: "Index {} is out of bounds for Struct DataType with {} fields", index, fields.len());
            }
            
            let field = &fields[index];
            
            let c_name = CString::new(field.name.as_str())
                .map_err(|e| polars_err!(ComputeError: "Struct field name contains null byte: {}", e))?;
            
            unsafe {
                *name_out = c_name.into_raw();
                *type_out = Box::into_raw(Box::new(field.dtype.clone()));
            }
            Ok(())
        } else {
            polars_bail!(ComputeError: "Expected Struct DataType, but got: {:?}", dtype);
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_export_arrow_schema(
    ptr: *mut DataType,
    out_schema: *mut polars_arrow::ffi::ArrowSchema, 
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }
        if out_schema.is_null() {
            polars_bail!(ComputeError: "Output ArrowSchema pointer is null");
        }

        let dtype = unsafe { &*ptr };
        
        let arrow_type = dtype.to_arrow(CompatLevel::newest());
        let field = polars_arrow::datatypes::Field::new("value".into(), arrow_type, true);

        let schema = polars_arrow::ffi::export_field_to_c(&field);
        
        unsafe { std::ptr::write(out_schema, schema); }
        
        Ok(())
    })
}