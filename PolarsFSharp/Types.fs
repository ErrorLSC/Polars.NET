namespace PolarsFSharp

open System
open Polars.Native

// ==========================================
// Expr 类型封装
// ==========================================
type Expr(handle: ExprHandle) =
    member _.Handle = handle

    // 运算符重载
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.Handle, rhs.Handle))
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.Handle, rhs.Handle))
    static member (.*) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.Handle, rhs.Handle))
    
    // 方法
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(handle, name))
    member this.Sum() = new Expr(PolarsWrapper.Sum(handle))
    member this.Mean() = new Expr(PolarsWrapper.Mean(handle))
    member this.Max() = new Expr(PolarsWrapper.Max(handle))
    member this.Min() = new Expr(PolarsWrapper.Min(handle))
    member this.StrContains(pattern: string) = new Expr(PolarsWrapper.StrContains(handle, pattern))

    member this.Dt = new DtOps(handle)

and DtOps(handle: ExprHandle) =
    member _.Year() = new Expr(PolarsWrapper.DtYear(handle))

// DataFrame 封装
type DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    
    member _.Handle = handle
    
    // 这里的 ToArrow 只是为了方便内部逻辑，
    // 具体拿数据的逻辑放在这里没问题
    member this.ToArrow() = PolarsWrapper.Collect(handle)
    // 此处以后应修改为从rust拿数
    member this.Rows = 
        use batch = this.ToArrow()
        batch.Length
    // 此处以后应修改为从rust拿数
    member this.Columns = 
        use batch = this.ToArrow()
        batch.ColumnCount

// LazyFrame 封装
// 它依赖 DataFrame (Collect 返回 DataFrame)，所以必须定义在 DataFrame 后面
type LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect(handle)
        new DataFrame(dfHandle)