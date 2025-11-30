namespace PolarsFSharp

open System
open Polars.Native
open Apache.Arrow

// ==========================================
// Expr 类型封装
// ==========================================
type Expr(handle: ExprHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr(handle)
    // 运算符重载, Compare
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Lt(lhs.Handle, rhs.Handle))
    static member (.>=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.GtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.LtEq(lhs.Handle, rhs.Handle))
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.Handle, rhs.Handle))
    static member (.!=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Neq(lhs.Handle, rhs.Handle))
    // 运算符重载, Arithmetic
    static member (+) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Add(lhs.Handle, rhs.Handle))
    static member (-) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Sub(lhs.Handle, rhs.Handle))
    static member (*) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.Handle, rhs.Handle))
    static member (/) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Div(lhs.Handle, rhs.Handle))
    static member (%) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Rem(lhs.Handle, rhs.Handle))
    // --- 逻辑运算符 ---
    // 使用 .&& 和 .|| 避免与 F# 的短路逻辑 && 冲突
    static member (.&&) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.And(lhs.Handle, rhs.Handle))
    static member (.||) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Or(lhs.Handle, rhs.Handle))
    // 逻辑非 (unary not) -> !expr
    // F# 中一元非可以用 ~~ 或者自定义
    static member (!!) (e: Expr) = new Expr(PolarsWrapper.Not(e.Handle))
    // 方法
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(handle, name))
    member this.Sum() = new Expr(PolarsWrapper.Sum(handle))
    member this.Mean() = new Expr(PolarsWrapper.Mean(handle))
    member this.Max() = new Expr(PolarsWrapper.Max(handle))
    member this.Min() = new Expr(PolarsWrapper.Min(handle))
    // FillNull (填充空值)
    // 
    member this.FillNull(fillValue: Expr) = 
        new Expr(PolarsWrapper.FillNull(this.CloneHandle(), fillValue.CloneHandle()))

    // IsNull (检查是否为空)
    member this.IsNull() = 
        new Expr(PolarsWrapper.IsNull(this.CloneHandle()))

    // IsNotNull
    member this.IsNotNull() = 
        new Expr(PolarsWrapper.IsNotNull(this.CloneHandle()))

    // IsBetween
    member this.IsBetween(lower: Expr, upper: Expr) =
        new Expr(PolarsWrapper.IsBetween(this.CloneHandle(), lower.CloneHandle(), upper.CloneHandle()))

    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        new Expr(PolarsWrapper.Map(this.CloneHandle(), func))
    member this.Map(func: Func<IArrowArray, IArrowArray>, outputType: PlDataType) =
        new Expr(PolarsWrapper.Map(this.CloneHandle(), func, outputType))
    member this.Dt = new DtOps(handle)
    member this.Str = new StringOps(this.CloneHandle())
and DtOps(handle: ExprHandle) =
    member _.Year() = new Expr(PolarsWrapper.DtYear(handle))
and StringOps(handle: ExprHandle) =
    // 内部帮助函数
    let wrap op = new Expr(op handle)
    
    // 大小写
    member _.ToUpper() = wrap PolarsWrapper.StrToUpper
    member _.ToLower() = wrap PolarsWrapper.StrToLower
    
    // 长度
    member _.Len() = wrap PolarsWrapper.StrLenBytes
    
    // 切片
    // F# uint64 = C# ulong
    member _.Slice(offset: int64, length: uint64) = 
        new Expr(PolarsWrapper.StrSlice(handle, offset, length))
        
    // 替换 (Replace All)
    member _.ReplaceAll(pattern: string, value: string) =
        new Expr(PolarsWrapper.StrReplaceAll(handle, pattern, value))

    // 包含 (之前做的)
    member _.Contains(pat: string) = 
        new Expr(PolarsWrapper.StrContains(handle, pat))


// DataFrame 封装
type DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    
    member _.Handle = handle
    
    // 依然保留，用于 Show 或者用户真的需要 Arrow 数据时
    member this.ToArrow() = PolarsWrapper.Collect(handle)

    // 零拷贝获取行数
    member _.Rows = PolarsWrapper.DataFrameHeight(handle)

    // 零拷贝获取列数
    member _.Columns = PolarsWrapper.DataFrameWidth(handle)
    member _.ColumnNames = PolarsWrapper.GetColumnNames(handle) |> Array.toList

    member this.Item 
        with get(colName: string, rowIndex: int) =
            // 默认返回 float (double)，因为最通用。
            // 如果需要其他类型，可以使用下面的专用方法
            PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)

    // 专用取值方法
    member this.Int(colName: string, rowIndex: int) : int64 option = 
        let nullableVal = PolarsWrapper.GetInt(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None

    member this.Float(colName: string, rowIndex: int) : float option = 
        let nullableVal = PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.String(colName: string, rowIndex: int) = PolarsWrapper.GetString(handle, colName, int64 rowIndex) |> Option.ofObj

// LazyFrame 封装
// 它依赖 DataFrame (Collect 返回 DataFrame)，所以必须定义在 DataFrame 后面
type LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneLazy(handle)
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect(handle)
        new DataFrame(dfHandle)