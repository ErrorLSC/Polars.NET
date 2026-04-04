use polars::prelude::*;
pub struct DataFrameContext {
    pub df: DataFrame,
}
pub struct ExprContext {
    pub inner: Expr,
}

pub struct SelectorContext {
    pub inner: Selector,
}

pub struct LazyFrameContext {
    pub inner: LazyFrame,
}

pub struct SeriesContext {
    pub series: Series,
}

pub struct DataTypeContext {
    pub dtype: DataType,
}

pub struct SchemaContext {
    pub schema: SchemaRef, 
}

pub struct DataTypeExprContext {
    pub inner: DataTypeExpr, 
}
