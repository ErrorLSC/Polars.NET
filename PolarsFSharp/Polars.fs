namespace PolarsFSharp

open System
open Apache.Arrow
open Polars.Native

module Polars =
    
    // --- 积木工厂 ---
    let col (name: string) = new Expr(PolarsWrapper.Col(name))
    let alias (name: string) (expr: Expr) = expr.Alias(name)

    // --- 黑魔法：万能 lit ---
    type LitMechanism = LitMechanism with
        static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: DateTime) = new Expr(PolarsWrapper.Lit(v))

    let inline lit (value: ^T) : Expr = 
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))

    // --- IO ---
    let readCsv (path: string) (tryParseDates: bool option): DataFrame =
        let parseDates = defaultArg tryParseDates true
        let handle = PolarsWrapper.ReadCsv(path, parseDates)
        new DataFrame(handle)

    let readParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet(path))

    let writeCsv (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteCsv(df.Handle, path)
        df 

    let writeParquet (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteParquet(df.Handle, path)
        df
    // --- Expr Helpers ---
    // [新增] count/len
    let count () = new Expr(PolarsWrapper.Len())
    let len () = new Expr(PolarsWrapper.Len())
    // --- Eager Ops ---
    let withColumnsEager (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.WithColumns(df.Handle, handles)
        new DataFrame(h)
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Filter(df.Handle, expr.Handle)
        new DataFrame(h)

    let select (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.Select(df.Handle, handles)
        new DataFrame(h)

    let sort (expr: Expr) (desc: bool) (df: DataFrame) : DataFrame =
        // Clone Handle (因为 Eager 操作不应消耗 Expr 的原始引用，虽然底层消耗了 handle)
        // 这里的逻辑稍微有点绕：Wrapper.Sort 会消耗 ExprHandle。
        // 为了让 F# 的 Expr 对象可复用，我们需要 CloneHandle。
        let h = PolarsWrapper.Sort(df.Handle, expr.CloneHandle(), desc)
        new DataFrame(h)

    // 保留 orderBy 别名
    let orderBy (expr: Expr) (desc: bool) (df: DataFrame) = sort expr desc df

    let groupBy (keys: Expr list) (aggs: Expr list) (df: DataFrame) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.Handle) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.GroupByAgg(df.Handle, kHandles, aHandles)
        new DataFrame(h)

    let join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: string) (left: DataFrame) : DataFrame =
        let lHandles = leftOn |> List.map (fun e -> e.Handle) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.Join(left.Handle, other.Handle, lHandles, rHandles, how)
        new DataFrame(h)

    let head (n: int) (df: DataFrame) : DataFrame =
        // 这里的 n 转 uint，PolarsWrapper 接收 uint
        let h = PolarsWrapper.Head(df.Handle, uint n)
        new DataFrame(h)
    let explode (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Explode(df.Handle, handles)
        new DataFrame(h)

    let sum (e: Expr) = e.Sum()
    let mean (e: Expr) = e.Mean()
    let max (e: Expr) = e.Max()
    let min (e: Expr) = e.Min()
    let abs (e: Expr) = e.Abs()
    let fillNull (fillValue: Expr) (e: Expr) = e.FillNull(fillValue)
    
    let isNull (e: Expr) = e.IsNull()
    
    let isNotNull (e: Expr) = e.IsNotNull()
    // Math Helpers
    let pow (exponent: Expr) (baseExpr: Expr) = baseExpr.Pow(exponent)
    let sqrt (e: Expr) = e.Sqrt()
    let exp (e: Expr) = e.Exp()

    // cols(["a", "b"])
    // 这是一个 Expr 工厂方法
    let cols (names: string list) =
        let arr = List.toArray names
        new Expr(PolarsWrapper.Cols(arr))

    // --- Lazy API ---
    let scanCsv (path: string) (tryParseDates: bool option) = 
        let parseDates = defaultArg tryParseDates true
        new LazyFrame(PolarsWrapper.ScanCsv(path, parseDates))

    let scanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet(path))

    // 1. Filter
    let filterLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        // 关键点：
        // 1. 克隆 lf (因为 Rust 会消耗它)
        // 2. 克隆 expr (因为 Rust 也会消耗它，而用户可能想复用 expr)
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)

    // 2. Select
    let selectLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        // 列表里的每个 Expr 都要克隆
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)

    // 3. Sort
    let sortLazy (expr: Expr) (desc: bool) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        let h = PolarsWrapper.LazySort(lfClone, exprClone, desc)
        new LazyFrame(h)

    // 别名
    let orderByLazy (expr: Expr) (desc: bool) (lf: LazyFrame) = sortLazy expr desc lf

    // 4. Limit
    let limit (n: uint) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)

    // 5. WithColumn
    let withColumn (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] // 使用克隆的 handle
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)

    let withColumns (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        // 1. 克隆 LazyFrame
        let lfClone = lf.CloneHandle()
        
        // 2. 克隆列表里的每一个 Expr
        // 注意：这里必须用 CloneHandle()，否则原来的 Expr 列表也会失效
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        // 3. 调用 C# Wrapper (传入的全是副本)
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    // 6. GroupBy
    // 用法: lf |> Polars.groupByLazy [col "a"] [count()]
    let groupByLazy (keys: Expr list) (aggs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazyGroupByAgg(lfClone, kHandles, aHandles)
        new LazyFrame(h)
    // 7. Collect (触发执行)
    let collect (lf: LazyFrame) : DataFrame = 
        // Collect 也会消耗 LazyFrame，所以也要克隆！
        // 这样你可以 collect 多次 (例如一次 show，一次 save)
        let lfClone = lf.CloneHandle()
        let dfHandle = PolarsWrapper.LazyCollect(lfClone)
        new DataFrame(dfHandle)
    // all() 现在返回 Selector
    let all () = new Selector(PolarsWrapper.SelectorAll())
    let asExpr (s: Selector) = s.ToExpr()
    // exclude 专门针对 Selector
    let exclude (names: string list) (s: Selector) = s.Exclude(names)
    // [新增] asStruct (将多列打包成 Struct)
    // 用法: asStruct [col "a"; col "b"]
    let asStruct (exprs: Expr list) =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        new Expr(PolarsWrapper.AsStruct(handles))
    // --- Show / Helper ---
    // 为了保持文件整洁，formatValue 可以设为 private
    let private formatValue (col: IArrowArray) (index: int) : string =
        if col.IsNull(index) then "(null)"
        else
            match col with
            | :? Int32Array as arr -> string (arr.GetValue(index))
            | :? Int64Array as arr -> string (arr.GetValue(index))
            | :? DoubleArray as arr -> string (arr.GetValue(index))
            | :? StringArray as arr -> arr.GetString(index)
            | :? StringViewArray as arr -> arr.GetString(index)
            | :? LargeStringArray as arr -> arr.GetString(index)
            | :? BooleanArray as arr -> string (arr.GetValue(index))
            | :? Date32Array as arr -> 
                let v = arr.GetValue(index)
                if v.HasValue then DateTime(1970, 1, 1).AddDays(float v.Value).ToString("yyyy-MM-dd")
                else "(null)"
            | :? TimestampArray as arr -> 
                let v = arr.GetValue(index) // Nullable<long>
                if v.HasValue then v.Value.ToString() else "(null)"
            | _ -> sprintf "[%s]" (col.GetType().Name)

    let show (rows: int) (df: DataFrame) : DataFrame =
        // 1. 获取总行数 (零拷贝，瞬间完成)
        let totalRows = df.Rows
        
        // 2. 决定实际要切多少行 (不能超过总行数)
        let n = Math.Min(int64 rows, totalRows)
        
        // 3. 切片并转换
        // 只有这 n 行会被拷贝
        let previewDf = df |> head (int n)
        use batch = previewDf.ToArrow()

        printfn "\n--- Polars DataFrame (Showing %d / %d rows) ---" batch.Length totalRows
        let fields = batch.Schema.FieldsList
        
        for field in fields do
            let col = batch.Column(field.Name)
            let typeName = field.DataType.Name
            
            printfn "[%s: %s]" field.Name typeName
            
            for i in 0 .. batch.Length - 1 do
                // 这里的 formatValue 就是你之前写的那个 helper
                printfn "  %s" (formatValue col i)
        
        printfn "--------------------------------------------"
        
        // 返回原始 df
        df