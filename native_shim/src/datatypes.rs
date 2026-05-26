use std::{ffi::{CStr, CString, c_char}, os::raw::c_int};
use crate::types::{CategoriesContext, DataTypeContext, FrozenCategoriesContext};
use polars::prelude::{extension::get_extension_type_or_generic, *};

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
        Float16     = 26  <=> DataType::Float16       => DataType:: Float16,
        Enum = 27 <=> DataType::Enum(_, _) => {
                    let frozen = FrozenCategories::new(std::iter::empty::<&str>()).unwrap();
                    DataType::Enum(frozen.clone(), frozen.mapping().clone())
                },
        Extension = 28 <=> DataType::Extension(_, _) => {
            let storage = DataType::Null;
            let ext_inst = get_extension_type_or_generic("", &storage, None);
            DataType::Extension(ext_inst, Box::new(storage))
        }
    }
}
// --- Constructors ---

// Primitive Type
// 0=Bool, 1=Int8, ... (Same as C# defined enum)
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_primitive(code: i32) -> *mut DataType {
    ffi_try!({
        // Convert code to Kind
        if let Some(kind) = PlDataTypeKind::from_i32(code) {
            // Convert Kind to DataType
            let dtype = kind.to_default_datatype();
            Ok(Box::into_raw(Box::new(dtype)))
        } else {
            polars_bail!(ComputeError: "Invalid primitive DataType code: {}", code);
        }
    })
}

// Decimal 
// precision: 0 for None (auto detect), >0 for real precision
// scale: decimal places
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_decimal(precision: usize, scale: usize) -> *mut DataTypeContext {
    ffi_try!({
        let prec = if precision == 0 { 38 } else { precision };
        let dtype = DataType::Decimal(prec, scale);
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype })))
    })
}

// Categorical 
// #[unsafe(no_mangle)]
// pub extern "C" fn pl_datatype_new_categorical() -> *mut DataTypeContext {
//     ffi_try!({
//         let cats = Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32);
//         let mapping = cats.mapping();
//         let dtype = DataType::Categorical(cats, mapping);
        
//         Ok(Box::into_raw(Box::new(DataTypeContext { dtype })))
//     })
// }
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_categorical(
    cats_ptr: *mut CategoriesContext
) -> *mut DataTypeContext {
    ffi_try!({
        let cats = if cats_ptr.is_null() {
            Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32)
        } else {
            let ctx = unsafe { &*cats_ptr };
            ctx.inner.clone()
        };

        let mapping = cats.mapping();
        let dtype = DataType::Categorical(cats, mapping);
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_enum(
    frozen_ptr: *mut FrozenCategoriesContext
) -> *mut DataTypeContext {
    ffi_try!({
        let frozen = if frozen_ptr.is_null() {
            FrozenCategories::new(std::iter::empty::<&str>()).unwrap()
        } else {
            let ctx = unsafe { &*frozen_ptr };
            ctx.inner.clone() 
        };

        let mapping = frozen.mapping().clone();
        
        let dtype = DataType::Enum(frozen, mapping);
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_extension(
    name: *const c_char,
    inner_dtype_ptr: *mut DataTypeContext,
    metadata: *const c_char,
) -> *mut DataTypeContext {
    ffi_try!({
        let name_str = if name.is_null() {
            ""
        } else {
            unsafe { CStr::from_ptr(name).to_str().unwrap() }
        };

        let inner = if inner_dtype_ptr.is_null() {
            DataType::Null
        } else {
            let inner_ctx = unsafe { &*inner_dtype_ptr };
            inner_ctx.dtype.clone()
        };

        let meta_str = if metadata.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(metadata).to_str().unwrap() })
        };

        let ext_inst = get_extension_type_or_generic(
            name_str,
            &inner,
            meta_str
        );

        let dtype = DataType::Extension(ext_inst, Box::new(inner));

        Ok(Box::into_raw(Box::new(DataTypeContext { dtype })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_list(inner_ptr: *mut DataTypeContext) -> *mut DataTypeContext {
    ffi_try!({
        if inner_ptr.is_null() {
            polars_bail!(ComputeError: "Inner DataTypeContext pointer is null for List creation");
        }
        
        let inner_ctx = unsafe { &*inner_ptr };
        let list_dtype = DataType::List(Box::new(inner_ctx.dtype.clone()));
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype: list_dtype })))
    })
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
    ffi_try!({
        let time_unit = parse_timeunit(unit);
        let dt = DataType::Duration(time_unit);
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype: dt })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_datetime(
    unit_code: u8,     
    tz_ptr: *const c_char 
) -> *mut DataTypeContext {
    ffi_try!({
        let time_unit = parse_timeunit(unit_code);

        let timezone = if tz_ptr.is_null() {
            None
        } else {
            let c_str = unsafe { CStr::from_ptr(tz_ptr) }
                .to_str()
                .map_err(|e| polars_err!(ComputeError: "Invalid UTF-8 in timezone string: {}", e))?;

            unsafe {Some(TimeZone::from_static(c_str))}
        };

        let dtype = DataType::Datetime(time_unit, timezone);
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_array(
    inner_ptr: *mut DataTypeContext,
    width_or_shape_ptr: *const usize,  
    ndim: usize                         
) -> *mut DataTypeContext {
    ffi_try!({
        if inner_ptr.is_null() {
            polars_bail!(ComputeError: "Inner DataTypeContext pointer is null");
        }
        if width_or_shape_ptr.is_null() && ndim > 0 {
            polars_bail!(ComputeError: "Shape pointer is null but ndim > 0");
        }
        
        let inner_ctx = unsafe { &*inner_ptr };
        let mut current_dtype = inner_ctx.dtype.clone();
        
        if ndim == 0 {
            return Ok(Box::into_raw(Box::new(DataTypeContext { 
                dtype: current_dtype 
            })));
        }
        
        let widths = unsafe { std::slice::from_raw_parts(width_or_shape_ptr, ndim) };
        
        for &width in widths.iter().rev() {
            current_dtype = DataType::Array(
                Box::new(current_dtype), 
                width
            );
        }
        
        Ok(Box::into_raw(Box::new(DataTypeContext { 
            dtype: current_dtype 
        })))
    })
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
pub extern "C" fn pl_datatype_get_kind(
    ptr: *mut DataType,
    out_kind: *mut i32
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }
        
        let dtype = unsafe { &*ptr };
        
        unsafe { 
            *out_kind = map_dtype_to_kind(dtype) as i32; 
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_time_unit(
    ptr: *mut DataType,
    out_unit: *mut u8
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }
        
        let dtype = unsafe { &*ptr };
        
        match dtype {
            DataType::Datetime(u, _) | DataType::Duration(u) => {
                let unit_val = match u {
                    TimeUnit::Nanoseconds => 0,
                    TimeUnit::Microseconds => 1,
                    TimeUnit::Milliseconds => 2,
                };
                unsafe { *out_unit = unit_val };
                Ok(())
            },
            _ => {
                polars_bail!(ComputeError: "Expected Datetime or Duration DataType, but got: {:?}", dtype);
            }
        }
    })
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
pub extern "C" fn pl_datatype_get_decimal_info(
    ptr: *mut DataType, 
    out_precision: *mut i32, 
    out_scale: *mut i32
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataType pointer is null");
        }
        
        let dtype = unsafe { &*ptr };
        
        if let DataType::Decimal(precision, scale) = dtype {
            unsafe {
                *out_precision = *precision as i32 ; 
                *out_scale = *scale as i32; 
            }
            Ok(())
        } else {
            polars_bail!(ComputeError: "Expected Decimal DataType, but got: {:?}", dtype);
        }
    })
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
pub extern "C" fn pl_datatype_get_array_shape(
    ctx: *const DataTypeContext,
    out_shape: *mut *const usize,
    out_len: *mut usize,
) -> bool {
    ffi_bool_try!({
        if ctx.is_null() || out_shape.is_null() || out_len.is_null() {
            polars_bail!(ComputeError: "null pointer passed to pl_datatype_get_array_shape");
        }

        let ctx = unsafe { &*ctx };
        let shape_opt = ctx.dtype.get_shape(); 

        match shape_opt {
            Some(shape) if !shape.is_empty() => {
                let len = shape.len();
                let ptr = shape.as_ptr();
                std::mem::forget(shape);
                unsafe {
                    *out_shape = ptr;
                    *out_len = len;
                }
                Ok(())
            }
            _ => {
                unsafe {
                    *out_shape = std::ptr::null();
                    *out_len = 0;
                }
                Ok(())
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_shape(data: *mut usize, len: usize) {
    if !data.is_null() && len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(data, len, len);
        }
    }
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_eq(
    a_ptr: *mut DataTypeContext,
    b_ptr: *mut DataTypeContext,
    out_eq: *mut bool,
) -> std::os::raw::c_int {
    ffi_try_c_int!({
        let a = unsafe { &(*a_ptr).dtype };
        let b = unsafe { &(*b_ptr).dtype };
        
        unsafe {
            *out_eq = a == b;
        }
        
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_categories(
    ptr: *mut DataTypeContext
) -> *mut CategoriesContext {
    ffi_try!({
        if ptr.is_null() { return Ok(std::ptr::null_mut()); }
        let ctx = unsafe { &*ptr };
        
        match &ctx.dtype {
            DataType::Categorical(cats, _) => {
                Ok(Box::into_raw(Box::new(CategoriesContext { inner: cats.clone() })))
            },
            _ => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_enum_categories(
    ptr: *mut DataTypeContext
) -> *mut FrozenCategoriesContext {
    ffi_try!({
        if ptr.is_null() { return Ok(std::ptr::null_mut()); }
        let ctx = unsafe { &*ptr };
        
        match &ctx.dtype {
            DataType::Enum(frozen, _) => {
                Ok(Box::into_raw(Box::new(FrozenCategoriesContext { inner: frozen.clone() })))
            },
            _ => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_extension_name(
    dt_ptr: *const DataTypeContext,
    out_name: *mut *mut c_char,
) -> c_int {
    ffi_try_c_int!({
        let dt = unsafe { &(*dt_ptr).dtype };
        unsafe { *out_name = std::ptr::null_mut() };
        
        if let DataType::Extension(instance, _) = dt {
            let c_str = CString::new(instance.name().as_ref())
                .map_err(|e| PolarsError::ComputeError(format!("Invalid UTF-8 in Extension Name: {}", e).into()))?;
            
            unsafe { *out_name = c_str.into_raw() };
        }
        
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_extension_metadata(
    dt_ptr: *const DataTypeContext,
    out_metadata: *mut *mut c_char,
) -> c_int {
    ffi_try_c_int!({
        let dt = unsafe { &(*dt_ptr).dtype };
        unsafe { *out_metadata = std::ptr::null_mut() };
        
        if let DataType::Extension(instance, _) = dt {
            if let Some(metadata_cow) = instance.serialize_metadata() {
                let c_str = CString::new(metadata_cow.as_ref())
                    .map_err(|e| PolarsError::ComputeError(format!("Invalid UTF-8 in Extension Metadata: {}", e).into()))?;
                    
                unsafe { *out_metadata = c_str.into_raw() };
            }
        }
        
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_get_extension_storage(
    dt_ptr: *const DataTypeContext,
    out_storage: *mut *mut DataTypeContext,
) -> c_int {
    ffi_try_c_int!({
        let dt = unsafe { &(*dt_ptr).dtype };
        unsafe { *out_storage = std::ptr::null_mut() };
        
        if let DataType::Extension(_, storage) = dt {
            let storage_ctx = DataTypeContext {
                dtype: *storage.clone(), 
            };
            
            unsafe { *out_storage = Box::into_raw(Box::new(storage_ctx)) };
        }
        
        Ok(0)
    })
}