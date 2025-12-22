use std::{ffi::{CStr, CString, c_char}, panic::{AssertUnwindSafe, catch_unwind}};
use crate::{error::set_error, types::ptr_to_str};
use polars::prelude::*;

// 包装 DataType，因为我们需要传递它给 cast 函数
pub struct DataTypeContext {
    pub dtype: DataType,
}

macro_rules! define_pl_datatype_kind {
    (
        $(#[$meta:meta])*
        pub enum PlDataTypeKind {
            // 格式: Variant = Discriminant <=> MatchPattern => Constructor
            $($Variant:ident = $Val:literal <=> $MatchPat:pat => $Constructor:expr),* $(,)?
        }
    ) => {
        // 1. 生成枚举定义
        #[repr(i32)]
        $(#[$meta])*
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum PlDataTypeKind {
            $($Variant = $Val),*
        }

        impl PlDataTypeKind {
            // 辅助：从 i32 安全转换为 Enum
            pub fn from_i32(code: i32) -> Option<Self> {
                match code {
                    $($Val => Some(PlDataTypeKind::$Variant)),*,
                    _ => None,
                }
            }

            // 核心：Kind -> Default DataType
            pub fn to_default_datatype(self) -> DataType {
                match self {
                    $(PlDataTypeKind::$Variant => $Constructor),*
                }
            }
        }

        // 核心：DataType -> Kind
        pub fn map_dtype_to_kind(dtype: &DataType) -> PlDataTypeKind {
            match dtype {
                $($MatchPat => PlDataTypeKind::$Variant),*,
                // 兜底处理：如果在宏里没定义的类型，统一返回 Unknown
                _ => PlDataTypeKind::Unknown,
            }
        }
    };
}

// === 使用宏定义所有类型 ===
// 这里是唯一的维护点！
define_pl_datatype_kind! {
    pub enum PlDataTypeKind {
        Unknown     = 0  <=> DataType::Unknown(_)      => DataType::Unknown(Default::default()), // 注意：UnknownKind::Any 在新版可能是 Default
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
        // Datetime 比较特殊：匹配所有 Datetime，但构造时给默认值
        Datetime    = 14 <=> DataType::Datetime(_, _)  => DataType::Datetime(TimeUnit::Microseconds, None),
        Time        = 15 <=> DataType::Time            => DataType::Time,
        Duration    = 16 <=> DataType::Duration(_)     => DataType::Duration(TimeUnit::Microseconds),
        Binary      = 17 <=> DataType::Binary          => DataType::Binary,
        Null        = 18 <=> DataType::Null            => DataType::Null,
        Struct      = 19 <=> DataType::Struct(_)       => DataType::Struct(vec![]),
        List        = 20 <=> DataType::List(_)         => DataType::List(Box::new(DataType::Null)),
        // Categorical 逻辑较复杂，直接内联你的构造代码
        Categorical = 21 <=> DataType::Categorical(_, _) => DataType::Categorical(Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32),Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32).mapping()),
        Decimal     = 22 <=> DataType::Decimal(_, _)   => DataType::Decimal(None, None),
    }
}
// --- Constructors ---

// 1. 基础类型 (通过枚举值创建)
// 0=Bool, 1=Int8, ... (与 C# 定义的 enum 对应)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_new_primitive(code: i32) -> *mut DataType {
    // 1. 尝试将 code 转为 Kind
    if let Some(kind) = PlDataTypeKind::from_i32(code) {
        // 2. 将 Kind 转为 DataType
        let dtype = kind.to_default_datatype();
        Box::into_raw(Box::new(dtype))
    } else {
        // 处理无效 code，返回 Unknown
        let dtype = DataType::Unknown(Default::default());
        Box::into_raw(Box::new(dtype))
    }
}

// 2. Decimal 类型
// precision: 0 代表 None (自动推断), >0 代表具体精度
// scale: 小数位数
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_decimal(precision: usize, scale: usize) -> *mut DataTypeContext {
    let prec = if precision == 0 { None } else { Some(precision) };
    let dtype = DataType::Decimal(prec, Some(scale));
    Box::into_raw(Box::new(DataTypeContext { dtype }))
}

// 3. Categorical 类型
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_categorical() -> *mut DataTypeContext {
    // 根据源码 Categories::random(namespace, physical) -> Arc<Self>
    // 1. 创建一个新的、独立的 Categories 上下文。
    //    Namespace 设为空，Physical 类型设为默认的 U32。
    let cats = Categories::random(PlSmallStr::EMPTY, CategoricalPhysical::U32);

    // 2. 获取对应的 Mapping。
    //    根据源码：pub fn mapping(&self) -> Arc<CategoricalMapping>
    //    如果不存在会自动创建一个新的。
    let mapping = cats.mapping();

    // 3. 构造 DataType::Categorical
    //    现在我们有了两个合法的 Arc 对象
    let dtype = DataType::Categorical(cats, mapping);
    
    Box::into_raw(Box::new(DataTypeContext { dtype }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_list(inner_ptr: *mut DataTypeContext) -> *mut DataTypeContext {
    assert!(!inner_ptr.is_null());
    
    // 1. 获取内部类型的引用
    let inner_ctx = unsafe { &*inner_ptr };
    
    // 2. 构造 List 类型
    // 注意：DataType::List 需要一个 Box<DataType>
    // 我们这里 Clone 内部类型，这样 C# 端释放 inner_ptr 不会影响新的 List 类型
    let list_dtype = DataType::List(Box::new(inner_ctx.dtype.clone()));
    
    // 3. 返回新的 Context
    Box::into_raw(Box::new(DataTypeContext { dtype: list_dtype }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_duration(unit: i32) -> *mut DataTypeContext {
    let time_unit = match unit {
        0 => TimeUnit::Nanoseconds,
        1 => TimeUnit::Microseconds,
        2 => TimeUnit::Milliseconds,
        _ => TimeUnit::Microseconds, // 默认
    };
    let dt = DataType::Duration(time_unit);
    Box::into_raw(Box::new(DataTypeContext { dtype:dt }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_datetime(
    unit_code: i32,     // 0=ns, 1=us, 2=ms
    tz_ptr: *const c_char // null=Naive, string=Aware
) -> *mut DataTypeContext {
    // 解析时间单位
    let unit = match unit_code {
        0 => TimeUnit::Nanoseconds,
        1 => TimeUnit::Microseconds,
        2 => TimeUnit::Milliseconds,
        _ => TimeUnit::Microseconds,
    };

    // 解析时区
    let timezone = if tz_ptr.is_null() {
        None
    } else {
        unsafe { 
            let c_str = ptr_to_str(tz_ptr).unwrap();
            // 将 C String 转为 Rust String (PlSmallStr)
            Some(TimeZone::from_static(c_str))
        }
    };

    let dtype = DataType::Datetime(unit, timezone);
    Box::into_raw(Box::new(DataTypeContext { dtype }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_struct(
    names: *const *const c_char,      // 字段名数组
    types: *const *mut DataTypeContext, // 类型句柄数组
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

            // 构造 Field
            fields.push(Field::new(name.into(), dtype));
        }

        let dt = DataType::Struct(fields);
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype: dt })))
    })
}

fn dtype_to_string_verbose(dt: &DataType) -> String {
    match dt {
        // 针对 Struct：手动拼接 "struct[name: type, ...]"
        DataType::Struct(fields) => {
            let content: Vec<String> = fields.iter()
                .map(|f| format!("{}: {}", f.name, dtype_to_string_verbose(&f.dtype)))
                .collect();
            format!("struct[{}]", content.join(", "))
        },
        
        // 针对 List：递归展开内部类型
        DataType::List(inner) => {
            format!("list[{}]", dtype_to_string_verbose(inner))
        },
        
        // 其他类型：使用 Polars 默认的 Display
        _ => dt.to_string()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_to_string(dt_ptr: *mut DataTypeContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*dt_ptr };
        // Polars 的 Display 实现非常详细，包含了时区、单位、Struct 字段等
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
        // 1. 借用 (&*ptr) 而不是消费 (Box::from_raw)
        let ctx = unsafe { &*ptr };
        
        // 2. Clone (Deep copy of the logical plan/structure, data is COW)
        let new_dt = ctx.dtype.clone();
        
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype:new_dt})))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_get_kind(ptr: *mut DataType) -> i32 {
    if ptr.is_null() { return 0; }
    let dtype = unsafe {&*ptr};
    // 直接调用宏生成的函数
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
pub unsafe extern "C" fn pl_datatype_get_timezone(ptr: *mut DataType) -> *mut c_char {
    // 展开你的宏逻辑
    let closure = || -> PolarsResult<*mut c_char> {
        if ptr.is_null() {
            return Ok(std::ptr::null_mut());
        }
        let dtype = unsafe {&*ptr};
        if let DataType::Datetime(_, Some(tz)) = dtype {
            let c_str = CString::new(tz.as_str()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            return Ok(c_str.into_raw());
        }
        Ok(std::ptr::null_mut())
    };

    // 这里手动模拟你的 ffi_try! 展开，或者直接调用你定义的宏
    // ffi_try!(closure()) 
    
    // 为了演示完整性，我写全：
    let result = catch_unwind(AssertUnwindSafe(closure));
    match result {
        Ok(inner) => match inner {
            Ok(ptr) => ptr,
            Err(e) => {
                set_error(e.to_string());
                std::ptr::null_mut()
            }
        },
        Err(_) => {
            set_error("Panic in pl_datatype_get_timezone".to_string());
            std::ptr::null_mut()
        }
    }
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
             // Polars 中 None 通常意味着 infer，这里给个默认值
            unsafe {*precision = p.map(|v| v as i32).unwrap_or(38)};
            unsafe {*scale = s.map(|v| v as i32).unwrap_or(9)};
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
pub unsafe extern "C" fn pl_datatype_get_inner(ptr: *mut DataType) -> *mut DataType {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() { return std::ptr::null_mut(); }
        let dtype = unsafe {&*ptr};
        match dtype {
            DataType::List(inner) => {
                // Clone inner type and box it
                Box::into_raw(Box::new(*inner.clone())) 
            },
            _ => std::ptr::null_mut() // Not a list
        }
    }));
    result.unwrap_or(std::ptr::null_mut())
}

// 获取 Struct 的字段数量
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_get_struct_len(ptr: *mut DataType) -> usize {
    let dtype = unsafe {&*ptr};
    if let DataType::Struct(fields) = dtype {
        fields.len()
    } else {
        0
    }
}

// 获取 Struct 指定索引的字段名和类型
// name_ptr: 用于返回 CString, type_ptr: 用于返回 DataTypeHandle
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_datatype_get_struct_field(
    ptr: *mut DataType, 
    index: usize, 
    name_out: *mut *mut c_char, 
    type_out: *mut *mut DataType
) {
    let dtype = unsafe {&*ptr};
    if let DataType::Struct(fields) = dtype {
        if index < fields.len() {
            let field = &fields[index]; // Field { name, dtype }
            
            // 1. 设置 Name
            unsafe {*name_out = CString::new(field.name.as_str()).unwrap().into_raw()};
            
            // 2. 设置 Type (Clone handle)
            unsafe {*type_out = Box::into_raw(Box::new(field.dtype.clone()))};
        }
    }
}