use polars::prelude::*;
// ==========================================
// 1. 定义“不透明”容器
// ==========================================
// 这是一个壳，专门用来在 C# 和 Rust 之间传递 DataFrame 的所有权
pub struct DataFrameContext {
    pub df: DataFrame,
}
// 定义 Expr 的壳子
pub struct ExprContext {
    pub inner: Expr,
}

pub struct SelectorContext {
    pub inner: Selector,
}

// 定义 LazyFrame 壳子
pub struct LazyFrameContext {
    pub inner: LazyFrame,
}

// 包装结构体
pub struct SeriesContext {
    pub series: Series,
}

// 包装 DataType，因为我们需要传递它给 cast 函数
pub struct DataTypeContext {
    pub dtype: DataType,
}

pub struct SchemaContext {
    pub schema: SchemaRef, // 使用 Arc<Schema>
}
