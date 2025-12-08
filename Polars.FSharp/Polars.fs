namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.Native
/// <summary>
/// The main entry point for Polars operations in F#.
/// </summary>
module Polars =
    open Apache.Arrow.Types
    
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
        new DataFrame(PolarsWrapper.FromArrow(batch))
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

    // --- Show / Helper ---
    /// <summary>
    /// Format a value from an Arrow column for display.
    /// </summary>
    let rec formatValue (col: IArrowArray) (index: int) : string =
        if col.IsNull index then "null"
        else
            match col with
            // --- Base numbers ---
            | :? Int8Array as arr -> arr.GetValue(index).Value.ToString()
            | :? Int16Array as arr -> arr.GetValue(index).Value.ToString()
            | :? Int32Array as arr -> arr.GetValue(index).Value.ToString()
            | :? Int64Array as arr -> arr.GetValue(index).Value.ToString()
            | :? UInt8Array as arr -> arr.GetValue(index).Value.ToString()
            | :? UInt16Array as arr -> arr.GetValue(index).Value.ToString()
            | :? UInt32Array as arr -> arr.GetValue(index).Value.ToString()
            | :? UInt64Array as arr -> arr.GetValue(index).Value.ToString()
            | :? FloatArray as arr -> arr.GetValue(index).Value.ToString()
            | :? DoubleArray as arr -> arr.GetValue(index).Value.ToString()
            
            // --- String ---
            | :? StringArray as arr -> sprintf "\"%s\"" (arr.GetString index)
            | :? StringViewArray as arr -> sprintf "\"%s\"" (arr.GetString index)
            
            // --- Boolean ---
            | :? BooleanArray as arr -> arr.GetValue(index).Value.ToString().ToLower()
            
            // --- Binary ---
            | :? BinaryArray as arr -> 
                let bytes = arr.GetBytes(index).ToArray()
                let hex = BitConverter.ToString(bytes).Replace("-", "").ToLower()
                if hex.Length > 20 then sprintf "x'%s...'" (hex.Substring(0, 20))
                else sprintf "x'%s'" hex
            | :? LargeBinaryArray as arr ->
                let bytes = arr.GetBytes(index).ToArray()
                let hex = BitConverter.ToString(bytes).Replace("-", "").ToLower()
                if hex.Length > 20 then sprintf "x'%s...'" (hex.Substring(0, 20))
                else sprintf "x'%s'" hex

            // --- Date) ---
            | :? Date32Array as arr -> 
                let v = arr.GetValue(index).Value
                DateTime(1970, 1, 1).AddDays(float v).ToString "yyyy-MM-dd"
            
            // --- Timestamp ---
            | :? TimestampArray as arr ->
                let v = arr.GetValue(index).Value
                let unit = (arr.Data.DataType :?> TimestampType).Unit
                let ticks = 
                    match unit with
                    | TimeUnit.Nanosecond -> v / 100L 
                    | TimeUnit.Microsecond -> v * 10L 
                    | TimeUnit.Millisecond -> v * 10000L 
                    | TimeUnit.Second -> v * 10000000L 
                    | _ -> v
                
                try DateTime.UnixEpoch.AddTicks(ticks).ToString "yyyy-MM-dd HH:mm:ss.ffffff"
                with _ -> v.ToString()

            // --- Time ---
            | :? Time32Array as arr ->
                let v = arr.GetValue(index).Value
                let unit = (arr.Data.DataType :?> Time32Type).Unit
                let span = 
                    match unit with
                    | TimeUnit.Millisecond -> TimeSpan.FromMilliseconds(float v)
                    | _ -> TimeSpan.FromSeconds(float v)
                span.ToString()

            | :? Time64Array as arr ->
                let v = arr.GetValue(index).Value
                let unit = (arr.Data.DataType :?> Time64Type).Unit
                let ticks = 
                    match unit with
                    | TimeUnit.Nanosecond -> v / 100L
                    | _ -> v * 10L
                TimeSpan.FromTicks(ticks).ToString()

            // --- Duration ---
            | :? DurationArray as arr ->
                let v = arr.GetValue(index).Value
                let unit = (arr.Data.DataType :?> DurationType).Unit
                let suffix = 
                    match unit with
                    | TimeUnit.Nanosecond -> "ns"
                    | TimeUnit.Microsecond -> "us"
                    | TimeUnit.Millisecond -> "ms"
                    | TimeUnit.Second -> "s"
                    | _ -> ""
                sprintf "%d%s" v suffix

            // --- List and Struct ---
            | :? ListArray as arr ->
                let start = arr.ValueOffsets.[index]
                let end_ = arr.ValueOffsets.[index + 1]
                // 递归调用 formatValue 处理子元素
                let items = [ for i in start .. end_ - 1 -> formatValue arr.Values i ]
                sprintf "[%s]" (String.Join(", ", items))

            | :? LargeListArray as arr -> 
                let start = int arr.ValueOffsets.[index]
                let end_ = int arr.ValueOffsets.[index + 1]
                let items = [ for i in start .. end_ - 1 -> formatValue arr.Values i ]
                sprintf "[%s]" (String.Join(", ", items))

            | :? StructArray as arr ->
                let structType = arr.Data.DataType :?> StructType
                let fields = 
                    structType.Fields 
                    |> Seq.mapi (fun i field -> 
                        let childCol = arr.Fields.[i]
                        // 递归调用
                        sprintf "%s: %s" field.Name (formatValue childCol index)
                    )
                sprintf "{%s}" (String.Join(", ", fields))

            | _ -> sprintf "<%s>" (col.GetType().Name)

    /// <summary>
    /// Show first N lines of DataFrame
    /// </summary>
    let showRows (rows: int) (df: DataFrame) : DataFrame =
        let totalRows = df.Rows
        let n = Math.Min(int64 rows, totalRows)

        let previewDf = df |> head (int n)
        use batch = previewDf.ToArrow()

        printfn "\n--- Polars DataFrame (Showing %d / %d rows) ---" batch.Length totalRows
        let fields = batch.Schema.FieldsList
        
        for field in fields do
            let col = batch.Column field.Name
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
    /// Show first 10 lines of DataFrame
    /// </summary>
    let show (df: DataFrame) : DataFrame =
        showRows 10 df