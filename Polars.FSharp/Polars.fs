namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.Native
/// <summary>
/// The main entry point for Polars operations in F#.
/// </summary>
module Polars =
    open System.Threading.Tasks
    
    // --- Factories ---
    /// <summary> Reference a column by name. </summary>
    let col (name: string) = new Expr(PolarsWrapper.Col name)
    /// <summary> Select multiple columns (returns a Wildcard Expression). </summary>
    let cols (names: string list) =
        let arr = List.toArray names
        new Expr(PolarsWrapper.Cols arr)
    /// <summary> Select all columns (returns a Selector). </summary>
    let all () = new Selector(PolarsWrapper.SelectorAll())

    // --- Lit (SRTP) ---
    type LitMechanism = LitMechanism with
        static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit(v))
        static member ($) (LitMechanism, v: DateTime) = new Expr(PolarsWrapper.Lit(v))

    /// <summary> Create a literal expression from a value. </summary>
    let inline lit (value: ^T) : Expr = 
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))

    // --- IO ---
    /// <summary> Read a CSV file into a DataFrame (Eager). </summary>
    let readCsv (path: string) (tryParseDates: bool option): DataFrame =
        let parseDates = defaultArg tryParseDates true
        let handle = PolarsWrapper.ReadCsv(path, parseDates)
        new DataFrame(handle)
    /// <summary> Read a parquet file into a DataFrame (Eager). </summary>
    let readParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet path)
    /// <summary> Scan a CSV file into a LazyFrame. </summary>
    let scanCsv (path: string) (tryParseDates: bool option) = 
        let parseDates = defaultArg tryParseDates true
        new LazyFrame(PolarsWrapper.ScanCsv(path, parseDates))
    /// <summary> Scan a parquet file into a LazyFrame. </summary>
    let scanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet path)
    /// <summary> Read a JSON file into a DataFrame (Eager). </summary>
    let readJson (path: string) : DataFrame =
        new DataFrame(PolarsWrapper.ReadJson path)
    /// <summary> Scan a JSON file into a LazyFrame. </summary>
    let scanNdjson (path: string) : LazyFrame =
        new LazyFrame(PolarsWrapper.ScanNdjson path)
    /// <summary> Read an IPC file into a DataFrame (Eager). </summary>
    let readIpc (path: string) = new DataFrame(PolarsWrapper.ReadIpc path)
    /// <summary> Scan an IPC file into a LazyFrame. </summary>
    let scanIpc (path: string) = new LazyFrame(PolarsWrapper.ScanIpc path)
    /// <summary> Write DataFrame to CSV. </summary>
    let writeCsv (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteCsv(df.Handle, path)
        df 
    /// <summary> Write DataFrame to Parquet. </summary>
    let writeParquet (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteParquet(df.Handle, path)
        df
    /// <summary> Write LazyFrame execution result to Parquet (Streaming). </summary>
    let sinkParquet (path: string) (lf: LazyFrame) : unit =
        let lfClone = lf.CloneHandle()
        PolarsWrapper.SinkParquet(lfClone, path)
    /// <summary> Write LazyFrame execution result to IPC (Streaming). </summary>
    let sinkIpc (path: string) (lf: LazyFrame) = 
        let lfClone = lf.CloneHandle()
        PolarsWrapper.SinkIpc(lfClone, path)
    /// <summary> Transform RecordBatch into DataFrame </summary>
    let fromArrow (batch: Apache.Arrow.RecordBatch) : DataFrame =
        new DataFrame(PolarsWrapper.FromArrow batch)
    // --- Expr Helpers ---
    /// <summary> Cast an expression to a different data type. </summary>
    let cast (dtype: DataType) (e: Expr) = e.Cast dtype
    /// Common DataTypes
    let boolean = DataType.Boolean
    let int32 = DataType.Int32
    let float64 = DataType.Float64
    let string = DataType.String
    /// <summary> Count the number of elements in an expression. </summary>
    let count () = new Expr(PolarsWrapper.Len())
    /// Alias for count
    let len = count
    /// <summary> Alias an expression with a new name. </summary>
    let alias (name: string) (expr: Expr) = expr.Alias name
    /// <summary> Collect LazyFrame into DataFrame (Eager execution). </summary>
    let collect (lf: LazyFrame) : DataFrame = 
        let lfClone = lf.CloneHandle()
        let dfHandle = PolarsWrapper.LazyCollect(lfClone)
        new DataFrame(dfHandle)
    /// <summary> Convert Selector to Expr. </summary>
    let asExpr (s: Selector) = s.ToExpr()
    /// <summary> Exclude columns from Selector. </summary>
    let exclude (names: string list) (s: Selector) = s.Exclude names
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    let asStruct (exprs: Expr list) =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        new Expr(PolarsWrapper.AsStruct(handles))
    // --- Eager Ops ---
    /// <summary> Add or replace columns. </summary>
    let withColumn (expr: Expr) (df: DataFrame) : DataFrame =
        let exprHandle = expr.CloneHandle()
        let h = PolarsWrapper.WithColumns(df.Handle, [| exprHandle |])
        new DataFrame(h)
    /// <summary> Add or replace multiple columns. </summary>
    let withColumns (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.WithColumns(df.Handle, handles)
        new DataFrame(h)
    /// <summary> Filter rows based on a boolean expression. </summary>
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Filter(df.Handle, expr.CloneHandle())
        new DataFrame(h)
    /// <summary> Select columns from DataFrame. </summary>
    let select (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Select(df.Handle, handles)
        new DataFrame(h)
    /// <summary> Sort (Order By) the DataFrame. </summary>
    let sort (expr: Expr) (desc: bool) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Sort(df.Handle, expr.CloneHandle(), desc)
        new DataFrame(h)
    let orderBy (expr: Expr) (desc: bool) (df: DataFrame) = sort expr desc df
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupBy (keys: Expr list) (aggs: Expr list) (df: DataFrame) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.GroupByAgg(df.Handle, kHandles, aHandles)
        new DataFrame(h)
    /// <summary> Perform a join between two DataFrames. </summary>
    let join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) (left: DataFrame) : DataFrame =
        let lHandles = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Join(left.Handle, other.Handle, lHandles, rHandles, how.ToNative())
        new DataFrame(h)
    /// <summary> Concatenate multiple DataFrames vertically. </summary>
    let concat (dfs: DataFrame list) : DataFrame =
        let handles = dfs |> List.map (fun df -> df.CloneHandle()) |> List.toArray
        new DataFrame(PolarsWrapper.Concat (handles,PlConcatType.Vertical))
    /// <summary> Concatenate multiple DataFrames horizontally (hstack). </summary>
    let concatHorizontal (dfs: DataFrame list) : DataFrame =
        let handles = dfs |> List.map (fun df -> df.CloneHandle()) |> List.toArray
        new DataFrame(PolarsWrapper.Concat(handles, PlConcatType.Horizontal))
    /// <summary> 
    /// Concatenate multiple DataFrames diagonally. 
    /// Columns are aligned by name; missing columns are filled with nulls.
    /// </summary>
    let concatDiagonal (dfs: DataFrame list) : DataFrame =
        let handles = dfs |> List.map (fun df -> df.CloneHandle()) |> List.toArray
        new DataFrame(PolarsWrapper.Concat(handles, PlConcatType.Diagonal))
    /// <summary> Get the first n rows of the DataFrame. </summary>
    let head (n: int) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Head(df.Handle, uint n)
        new DataFrame(h)
    /// <summary> Get the last n rows of the DataFrame. </summary>
    let tail (n: int) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Tail(df.Handle, uint n)
        new DataFrame(h)
    /// <summary> Explode list-like columns into multiple rows. </summary>
    let explode (exprs: Expr list) (df: DataFrame) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Explode(df.Handle, handles)
        new DataFrame(h)

    // --- Reshaping (Eager) ---

    /// <summary> Pivot the DataFrame from long to wide format. </summary>
    let pivot (index: string list) (columns: string list) (values: string list) (aggFn: PivotAgg) (df: DataFrame) : DataFrame =
        let iArr = List.toArray index
        let cArr = List.toArray columns
        let vArr = List.toArray values
        new DataFrame(PolarsWrapper.Pivot(df.Handle, iArr, cArr, vArr, aggFn.ToNative()))

    /// <summary> Unpivot (Melt) the DataFrame from wide to long format. </summary>
    let unpivot (index: string list) (on: string list) (variableName: string option) (valueName: string option) (df: DataFrame) : DataFrame =
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = Option.toObj variableName 
        let valN = Option.toObj valueName 
        new DataFrame(PolarsWrapper.Unpivot(df.Handle, iArr, oArr, varN, valN))
    /// Alias for unpivot
    let melt = unpivot    
    /// Aggregation Helpers
    let sum (e: Expr) = e.Sum()
    let mean (e: Expr) = e.Mean()
    let max (e: Expr) = e.Max()
    let min (e: Expr) = e.Min()
    // Fill Helpers
    let fillNull (fillValue: Expr) (e: Expr) = e.FillNull fillValue
    let isNull (e: Expr) = e.IsNull()
    let isNotNull (e: Expr) = e.IsNotNull()
    // Math Helpers
    let abs (e: Expr) = e.Abs()
    let pow (exponent: Expr) (baseExpr: Expr) = baseExpr.Pow exponent
    let sqrt (e: Expr) = e.Sqrt()
    let exp (e: Expr) = e.Exp()

    // --- Lazy API ---

    /// <summary> Explain the LazyFrame execution plan. </summary>
    let explain (lf: LazyFrame) = lf.Explain true
    /// <summary> Explain the unoptimized LazyFrame execution plan. </summary>
    let explainUnoptimized (lf: LazyFrame) = lf.Explain false
    /// <summary> Get the schema of the LazyFrame. </summary>
    let schema (lf: LazyFrame) = lf.Schema
    /// <summary> Filter rows based on a boolean expression. </summary>
    let filterLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)

    /// <summary> Select columns from LazyFrame. </summary>
    let selectLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)

    /// <summary> Sort (Order By) the LazyFrame. </summary>
    let sortLazy (expr: Expr) (desc: bool) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        let h = PolarsWrapper.LazySort(lfClone, exprClone, desc)
        new LazyFrame(h)
    /// <summary> Alias for sortLazy </summary>
    let orderByLazy (expr: Expr) (desc: bool) (lf: LazyFrame) = sortLazy expr desc lf

    /// <summary> Limit the number of rows in the LazyFrame. </summary>
    let limit (n: uint) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)
    /// <summary> Add or replace columns in the LazyFrame. </summary>
    let withColumnLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] // 使用克隆的 handle
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary> Add or replace multiple columns in the LazyFrame. </summary>
    let withColumnsLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupByLazy (keys: Expr list) (aggs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazyGroupByAgg(lfClone, kHandles, aHandles)
        new LazyFrame(h)
    /// <summary> Unpivot (Melt) the LazyFrame from wide to long format. </summary>
    let unpivotLazy (index: string list) (on: string list) (variableName: string option) (valueName: string option) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle() // 必须 Clone
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName 
        new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, iArr, oArr, varN, valN))
    /// Alias for unpivotLazy
    let meltLazy = unpivotLazy
    /// <summary> Perform a join between two LazyFrames. </summary>
    let joinLazy (other: LazyFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) (lf: LazyFrame) : LazyFrame =
        let lClone = lf.CloneHandle()
        let rClone = other.CloneHandle()
        
        let lOnArr = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rOnArr = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        new LazyFrame(PolarsWrapper.Join(lClone, rClone, lOnArr, rOnArr, how.ToNative()))
    /// <summary> Perform an As-Of Join (time-series join). </summary>
    let joinAsOf (other: LazyFrame) 
                 (leftOn: Expr) (rightOn: Expr) 
                 (byLeft: Expr list) (byRight: Expr list) 
                 (strategy: string option) 
                 (tolerance: string option) 
                 (lf: LazyFrame) : LazyFrame =
        
        let lClone = lf.CloneHandle()
        let rClone = other.CloneHandle()
        
        let lOn = leftOn.CloneHandle()
        let rOn = rightOn.CloneHandle()
        
        // 处理分组列 (Clone List)
        let lByArr = byLeft |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rByArr = byRight |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        // 处理可选参数
        let strat = defaultArg strategy "backward"
        let tol = Option.toObj tolerance // 转为 string 或 null

        let h = PolarsWrapper.JoinAsOf(
            lClone, rClone, 
            lOn, rOn, 
            lByArr, rByArr,
            strat, tol
        )
        new LazyFrame(h)
    /// <summary> Concatenate multiple LazyFrames vertically. </summary>
    let concatLazy (lfs: LazyFrame list) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        // 默认 rechunk=false, parallel=true (Lazy 的常见默认值)
        new LazyFrame(PolarsWrapper.LazyConcat(handles, PlConcatType.Vertical, false, true))
    /// <summary> 
    /// Lazily concatenate multiple LazyFrames horizontally.
    /// Note: Duplicate column names will cause an error during collection.
    /// </summary>
    let concatLazyHorizontal (lfs: LazyFrame list) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, PlConcatType.Horizontal, false, false))
    /// <summary> 
    /// Lazily concatenate multiple LazyFrames diagonally. 
    /// </summary>
    let concatLazyDiagonal (lfs: LazyFrame list) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, PlConcatType.Diagonal, false, true))
    /// <summary> Collect LazyFrame into DataFrame (Streaming execution). </summary>
    let collectStreaming (lf: LazyFrame) : DataFrame =
        let lfClone = lf.CloneHandle()
        new DataFrame(PolarsWrapper.CollectStreaming lfClone)
    /// <summary> Define a window over which to perform an aggregation. </summary>
    let over (partitionBy: Expr list) (e: Expr) = e.Over partitionBy
    /// <summary> Create a SQL context for executing SQL queries on LazyFrames. </summary>
    let sqlContext () = new SqlContext()
    /// <summary> Execute a SQL query against the provided LazyFrames. </summary>
    let ifElse (predicate: Expr) (ifTrue: Expr) (ifFalse: Expr) : Expr =
        let p = predicate.CloneHandle()
        let t = ifTrue.CloneHandle()
        let f = ifFalse.CloneHandle()
        
        new Expr(PolarsWrapper.IfElse(p, t, f))
    // --- Async IO ---

    /// <summary> Asynchronously read a CSV file into a DataFrame. </summary>
    let readCsvAsync (path: string) (tryParseDates: bool option) : Async<DataFrame> =
        async {
            let parseDates = defaultArg tryParseDates true
            let! handle = PolarsWrapper.ReadCsvAsync(path,parseDates) |> Async.AwaitTask
            return new DataFrame(handle)
        }
    // --- Async Execution ---

    /// <summary> 
    /// Asynchronously execute the LazyFrame query plan. 
    /// Useful for keeping UI responsive during heavy calculations.
    /// </summary>
    let collectAsync (lf: LazyFrame) : Async<DataFrame> =
        async {
            // 这里必须 CloneHandle！
            // 因为 collectAsync 会立即返回一个 Async 对象，
            // 原始的 lf 可能会在 Async 还没真正运行前就被 dispose 或者修改（虽然 LazyFrame 是不可变的）。
            // 最安全的方式是让后台线程持有一个独立的 Handle。
            let lfClone = lf.CloneHandle()
            
            let! dfHandle = 
                Task.Run(fun () -> PolarsWrapper.LazyCollect lfClone) 
                |> Async.AwaitTask
                
            return new DataFrame(dfHandle)
        }

    [<AutoOpen>]
    module Printing =
        open Apache.Arrow.Types
        
        // 1. 格式化辅助函数
        let private formatBytes (bytes: byte[]) =
            let hex = BitConverter.ToString(bytes).Replace("-", "").ToLower()
            if hex.Length > 20 then sprintf "x'%s...'" (hex.Substring(0, 20)) else sprintf "x'%s'" hex

        let private formatTimestamp (arr: TimestampArray) (index: int) =
            let v = arr.GetValue(index).Value
            let unit = (arr.Data.DataType :?> TimestampType).Unit
            try
                let dt = 
                    match unit with
                    | TimeUnit.Nanosecond -> DateTime.UnixEpoch.AddTicks(v / 100L)
                    | TimeUnit.Microsecond -> DateTime.UnixEpoch.AddTicks(v * 10L)
                    | TimeUnit.Millisecond -> DateTime.UnixEpoch.AddMilliseconds(float v)
                    | TimeUnit.Second -> DateTime.UnixEpoch.AddSeconds(float v)
                    | _ -> DateTime.UnixEpoch
                dt.ToString "yyyy-MM-dd HH:mm:ss.ffffff"
            with _ -> v.ToString()

        // 2. 核心值格式化 (递归，补全了 Decimal 和 Categorical)
        let rec formatValue (col: IArrowArray) (index: int) : string =
            if col.IsNull(index) then "null"
            else
                match col with
                // --- Base numbers ---
                | :? Int8Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? Int16Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? Int32Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? Int64Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? UInt8Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? UInt16Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? UInt32Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? UInt64Array as arr -> (arr.GetValue(index).Value).ToString()
                | :? FloatArray as arr -> (arr.GetValue(index).Value).ToString()
                | :? DoubleArray as arr -> (arr.GetValue(index).Value).ToString()
                
                // --- [新增] Decimal ---
                | :? Decimal128Array as arr -> (arr.GetValue(index).Value).ToString()

                // --- String ---
                | :? StringArray as arr -> sprintf "\"%s\"" (arr.GetString index)
                | :? StringViewArray as arr -> sprintf "\"%s\"" (arr.GetString index)
                
                // --- Boolean ---
                | :? BooleanArray as arr -> if arr.GetValue(index).Value then "true" else "false"
                
                // --- [新增] Categorical (Dictionary) ---
                | :? DictionaryArray as arr ->
                    // 1. 获取索引
                    let indices = arr.Indices
                    let key = 
                        match indices with
                        | :? UInt32Array as idx -> int (idx.GetValue(index).Value)
                        | :? Int32Array as idx -> idx.GetValue(index).Value
                        | :? Int8Array as idx -> int (idx.GetValue(index).Value)
                        | _ -> -1 // Should not happen
                    
                    if key < 0 then "null"
                    else
                        // 2. 递归去字典里查值
                        formatValue arr.Dictionary key

                // --- Binary ---
                | :? BinaryArray as arr -> formatBytes (arr.GetBytes(index).ToArray())
                | :? LargeBinaryArray as arr -> formatBytes (arr.GetBytes(index).ToArray())

                // --- Temporal ---
                | :? Date32Array as arr -> 
                    DateTime(1970, 1, 1).AddDays(float (arr.GetValue(index).Value)).ToString("yyyy-MM-dd")
                | :? TimestampArray as arr -> formatTimestamp arr index
                | :? Time32Array as arr -> (TimeSpan.FromMilliseconds(float (arr.GetValue(index).Value))).ToString() // 简化处理
                | :? Time64Array as arr -> (TimeSpan.FromTicks(int64 (arr.GetValue(index).Value) * 10L)).ToString()

                // --- Nested ---
                | :? ListArray as arr ->
                    let start = arr.ValueOffsets.[index]
                    let end_ = arr.ValueOffsets.[index + 1]
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

        // 3. 打印表格逻辑 (从原来的 showRows 升级而来)
        let printTable (df: DataFrame) (nRows: int) =
            let totalRows = df.Rows
            let limit = Math.Min(int64 nRows, totalRows)
            
            // 1. 获取数据
            let previewDf = if totalRows > limit then head (int limit) df else df
            use batch = previewDf.ToArrow()
            
            let schema = batch.Schema
            let columns = [ for field in schema.FieldsList -> batch.Column field.Name ]
            let headers = [ for field in schema.FieldsList -> field.Name ]
            
            // 简化的类型名称 (类似 Polars 官方)
            let getShortTypeName (n: string) =
                match n with
                | "Int32" -> "i32" | "Int64" -> "i64" | "Double" -> "f64" | "Float" -> "f32"
                | "String" | "StringView" -> "str" | "Boolean" -> "bool"
                | "Date32" -> "date" | "Timestamp" -> "time"
                | _ -> n
            
            let types = [ for field in schema.FieldsList -> getShortTypeName field.DataType.Name ]
            
            // 2. 预生成数据矩阵
            let dataRows = 
                [ for r in 0 .. int batch.Length - 1 -> 
                    [ for c in 0 .. columns.Length - 1 -> formatValue columns.[c] r ] 
                ]
                
            // 3. 计算列宽 (内容 + 2空格Padding)
            let colWidths = Array.zeroCreate<int> columns.Length
            for i in 0 .. headers.Length - 1 do
                let typeLen = types.[i].Length
                // 宽度 = Max(表头, 类型, 最长数据) + 2 (Padding)
                let maxContent = 
                    if dataRows.Length > 0 then
                        let maxData = dataRows |> List.map (fun row -> row.[i].Length) |> List.max
                        Math.Max(headers.[i].Length, Math.Max(typeLen, maxData))
                    else
                        Math.Max(headers.[i].Length, typeLen)
                colWidths.[i] <- maxContent + 2

            // 4. 绘图辅助函数
            let sb = System.Text.StringBuilder()
            
            // 画分割线: +------+------+
            let appendSeparator (borderChar: char) =
                sb.Append("+") |> ignore
                for w in colWidths do
                    sb.Append(String(borderChar, w)) |> ignore
                    sb.Append("+") |> ignore
                sb.AppendLine() |> ignore

            // 画内容行: | val1 | val2 |
            let appendRow (items: string list) =
                sb.Append("|") |> ignore
                for i in 0 .. items.Length - 1 do
                    // 默认左对齐 (PadRight)
                    // 左右各空一格: " " + text + " " (利用 PadRight 补齐剩余)
                    let text = " " + items.[i]
                    sb.Append(text.PadRight(colWidths.[i])) |> ignore
                    sb.Append("|") |> ignore
                sb.AppendLine() |> ignore

            // 5. 开始绘制
            sb.AppendLine(sprintf "shape: (%d, %d)" totalRows df.Columns) |> ignore
            
            appendSeparator '-'
            appendRow headers
            
            // 类型行 (灰色/不同风格，这里简单处理)
            let paddedTypes = types |> List.map (fun t -> sprintf "<%s>" t) // <i32> 风格
            // 这里为了对齐，我们需要稍微处理一下类型行的宽度逻辑，或者直接打印
            // 简单起见，我们直接打印类型行
            // appendRow types // 可选：打印类型行

            appendSeparator '=' // 表头和数据的强分隔符
            
            for row in dataRows do
                appendRow row
                
            appendSeparator '-' // 底部封口

            if totalRows > limit then 
                sb.AppendLine(sprintf "... with %d more rows" (totalRows - limit)) |> ignore
            Console.WriteLine(sb.ToString())

    // ==========================================
    // Public API (保持简单，返回 DataFrame 以支持管道)
    // ==========================================

    /// <summary>
    /// Print the DataFrame to Console (Table format).
    /// </summary>
    let show (df: DataFrame) : DataFrame =
        Printing.printTable df 10
        df

    /// <summary>
    /// Print the Series to Console.
    /// </summary>
    let showSeries (s: Series) : Series =
        // 临时转为 DataFrame 打印，最省事
        let h = PolarsWrapper.SeriesToFrame(s.Handle)
        use df = new DataFrame(h)
        Printing.printTable df 10
        s
