use polars_arrow::ffi::ArrowArray;
use polars_arrow::ffi::{export_array_to_c,export_field_to_c};
use polars::prelude::ArrowSchema;
use polars_arrow::datatypes::Field;

pub struct ArrowArrayContext {
    pub array: Box<dyn polars_arrow::array::Array>, 
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_array_free(ptr: *mut ArrowArrayContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_array_export(
    ptr: *mut ArrowArrayContext,
    out_c_array: *mut ArrowArray // C# 传来的未初始化的结构体指针
) {
    assert!(!ptr.is_null());
    assert!(!out_c_array.is_null());

    let ctx = unsafe { &*ptr };
    
    // 1. 克隆 Array (Box<dyn Array>)
    // Arrow Array 是 Arc 的，所以这里是浅拷贝，开销很小
    let array = ctx.array.clone(); 

    // 2. 调用 Polars 的 export_array_to_c
    // 这会返回一个 ArrowArray (Rust RAII Wrapper)
    let rust_arrow_array = export_array_to_c(array);

    // 3. 提取内部的 C 结构体 (FFI_ArrowArray) 并写入 C# 提供的指针
    unsafe {
        std::ptr::write(out_c_array, rust_arrow_array);
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_schema_export(
    ptr: *mut ArrowArrayContext,
    out_c_schema: *mut ArrowSchema
) {
    assert!(!ptr.is_null());
    assert!(!out_c_schema.is_null());

    let ctx = unsafe { &*ptr };
    
    // 1. 获取 Array 的 DataType
    let dtype = ctx.array.dtype().clone();

    // 2. 构造一个 Field (名字随意，因为 ImportArray 主要看类型，Series 名字通常在上层管理)
    // 但为了调试方便，我们叫它 "exported"
    let field = Field::new("".into(), dtype, true);

    // 3. 导出 Schema
    let rust_arrow_schema = export_field_to_c(&field);

    // 4. 写入 C# 指针
    unsafe {
        std::ptr::write(out_c_schema as *mut _, rust_arrow_schema);
    }
}