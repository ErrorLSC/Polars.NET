namespace PolarsFSharp

open System
open Apache.Arrow
open Polars.Native

module Polars =
    open Apache.Arrow.Types
    
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

    let sinkParquet (path: string) (lf: LazyFrame) : unit =
        let lfClone = lf.CloneHandle()
        PolarsWrapper.SinkParquet(lfClone, path)
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

    // --- Reshaping (Eager) ---

    // 1. Pivot
    // 参数顺序：index -> columns -> values -> agg -> df
    let pivot (index: string list) (columns: string list) (values: string list) (aggFn: string) (df: DataFrame) : DataFrame =
        let iArr = List.toArray index
        let cArr = List.toArray columns
        let vArr = List.toArray values
        new DataFrame(PolarsWrapper.Pivot(df.Handle, iArr, cArr, vArr, aggFn))

    // 2. Unpivot (Melt)
    let unpivot (index: string list) (on: string list) (variableName: string option) (valueName: string option) (df: DataFrame) : DataFrame =
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = defaultArg variableName null
        let valN = defaultArg valueName null
        new DataFrame(PolarsWrapper.Unpivot(df.Handle, iArr, oArr, varN, valN))

    // 别名
    let melt = unpivot    

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
    let explain (lf: LazyFrame) = lf.Explain(true)
    let explainUnoptimized (lf: LazyFrame) = lf.Explain(false)
    let schema (lf: LazyFrame) = lf.Schema
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
    // Lazy Unpivot
    let unpivotLazy (index: string list) (on: string list) (variableName: string option) (valueName: string option) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle() // 必须 Clone
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = defaultArg variableName null
        let valN = defaultArg valueName null
        new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, iArr, oArr, varN, valN))

    let meltLazy = unpivotLazy
    // --- Concatenation ---

    // [新增] Eager Concat
    let concat (dfs: DataFrame list) : DataFrame =
        // 这里的 CloneHandle 是为了用户体验：用户可能不希望 concat 之后原表就废了
        // 所以我们手动 Clone 一份传给底层去消费
        let handles = dfs |> List.map (fun df -> df.CloneHandle()) |> List.toArray
        
        // 注意：Wrapper.Concat 会 SetInvalid，所以这里的 dfs 列表里的对象之后就不能用了
        // 这是符合线性类型逻辑的。如果想支持 Copy，需要在 Rust 加 pl_dataframe_clone
        new DataFrame(PolarsWrapper.Concat(handles))
    let concatLazy (lfs: LazyFrame list) : LazyFrame =
        // 同样，LazyFrame 支持 CloneHandle (我们之前加过)
        // 这里我们可以选择自动 Clone，保持 Functional 的不可变感觉
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        
        new LazyFrame(PolarsWrapper.LazyConcat(handles))
    // Streaming Collect
    let collectStreaming (lf: LazyFrame) : DataFrame =
        let lfClone = lf.CloneHandle()
        new DataFrame(PolarsWrapper.CollectStreaming(lfClone))

    // Collect (触发执行)
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
        
    // [新增] over
    // 用法: col "salary" |> sum |> over [col "dept"]
    let over (partitionBy: Expr list) (e: Expr) = e.Over(partitionBy)


    // --- Show / Helper ---

    let rec private formatValue (col: IArrowArray) (index: int) : string =
        if col.IsNull(index) then "null"
        else
            match col with
            // --- 基础整数 ---
            | :? Int8Array as arr -> string (arr.GetValue(index))
            | :? Int16Array as arr -> string (arr.GetValue(index))
            | :? Int32Array as arr -> string (arr.GetValue(index))
            | :? Int64Array as arr -> string (arr.GetValue(index))
            | :? UInt8Array as arr -> string (arr.GetValue(index))
            | :? UInt16Array as arr -> string (arr.GetValue(index))
            | :? UInt32Array as arr -> string (arr.GetValue(index))
            | :? UInt64Array as arr -> string (arr.GetValue(index))
            
            // --- 浮点数 ---
            | :? FloatArray as arr -> string (arr.GetValue(index))
            | :? DoubleArray as arr -> string (arr.GetValue(index))
            
            // --- 字符串 ---
            | :? StringArray as arr -> arr.GetString(index)
            | :? StringViewArray as arr -> arr.GetString(index)
            | :? BooleanArray as arr -> string (arr.GetValue(index)) // True/False
            
            // --- 时间日期 ---
            | :? Date32Array as arr -> 
                let v = arr.GetValue(index).Value
                DateTime(1970, 1, 1).AddDays(float v).ToString("yyyy-MM-dd")
            | :? TimestampArray as arr ->
                let v = arr.GetValue(index).Value
                // Polars 默认微秒 (us)
                try DateTime.UnixEpoch.AddTicks(v * 10L).ToString("yyyy-MM-dd HH:mm:ss")
                with _ -> string v

            // --- 嵌套类型 ---
            | :? ListArray as arr ->
                let start = arr.ValueOffsets.[index]
                let end_ = arr.ValueOffsets.[index + 1]
                let items = [ for i in start .. end_ - 1 -> formatValue arr.Values i ]
                sprintf "[%s]" (String.Join(", ", items))

            | :? LargeListArray as arr -> // Polars 默认是用这个
                let start = int (arr.ValueOffsets.[index])
                let end_ = int (arr.ValueOffsets.[index + 1])
                let items = [ for i in start .. end_ - 1 -> formatValue arr.Values i ]
                sprintf "[%s]" (String.Join(", ", items))

            | :? StructArray as arr ->
                let structType = arr.Data.DataType :?> StructType
                let fields = 
                    structType.Fields 
                    |> Seq.mapi (fun i field -> 
                        let childCol = arr.Fields.[i]
                        sprintf "%s: %s" field.Name (formatValue childCol index)
                    )
                sprintf "{%s}" (String.Join(", ", fields))

            | _ -> sprintf "<%s>" (col.GetType().Name)

    /// <summary>
    /// [底层实现] 显式指定显示多少行。
    /// 会先在 Rust 侧切片 (Head)，性能安全。
    /// </summary>
    let showRows (rows: int) (df: DataFrame) : DataFrame =
        // 1. 获取元数据 (零拷贝)
        let totalRows = df.Rows
        let n = Math.Min(int64 rows, totalRows)
        
        // 2. Rust 侧切片 (只拷贝 n 行数据到 C#)
        // 如果 totalRows 很大，这步至关重要
        let previewDf = df |> head (int n)
        use batch = previewDf.ToArrow()

        printfn "\n--- Polars DataFrame (Showing %d / %d rows) ---" batch.Length totalRows
        let fields = batch.Schema.FieldsList
        
        for field in fields do
            let col = batch.Column(field.Name)
            // 直接从 Schema 获取类型名，更准确
            let typeName = field.DataType.Name 
            
            printfn "[%s: %s]" field.Name typeName
            
            // 打印值
            for i in 0 .. batch.Length - 1 do
                printfn "  %s" (formatValue col i)
            
            if totalRows > int64 rows then
                printfn "  ..."
        
        printfn "--------------------------------------------"
        df

    /// <summary>
    /// [快捷方式] 显示默认行数 (10行)。
    /// 适合 REPL 或快速调试。
    /// </summary>
    let show (df: DataFrame) : DataFrame =
        showRows 10 df