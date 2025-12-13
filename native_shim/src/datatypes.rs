use polars::prelude::*;

// 包装 DataType，因为我们需要传递它给 cast 函数
pub struct DataTypeContext {
    pub dtype: DataType,
}

// --- Constructors ---

// 1. 基础类型 (通过枚举值创建)
// 0=Bool, 1=Int8, ... (与 C# 定义的 enum 对应)
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_new_primitive(code: i32) -> *mut DataTypeContext {
    let dtype = match code {
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::UInt8,
        7 => DataType::UInt16,
        8 => DataType::UInt32,
        9 => DataType::UInt64,
        10 => DataType::Float32,
        11 => DataType::Float64,
        12 => DataType::String,
        13 => DataType::Date,
        14 => DataType::Datetime(TimeUnit::Microseconds, None), // 默认无时区
        15 => DataType::Time,
        16 => DataType::Duration(TimeUnit::Microseconds),
        17 => DataType::Binary,
        18 => DataType::Null,
        19 => DataType::Struct(vec![]),
        _ => DataType::Unknown(UnknownKind::Any),
    };
    Box::into_raw(Box::new(DataTypeContext { dtype }))
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

// --- Destructor ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_free(ptr: *mut DataTypeContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}