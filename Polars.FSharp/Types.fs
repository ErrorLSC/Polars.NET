namespace Polars.FSharp

open System
open Polars.Native
open Apache.Arrow
/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
type DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float32 | Float64
    | String
    | Date | Datetime | Time
    | Duration
    | Binary
    | Categorical
    | Decimal of precision: int option * scale: int
    | Unknown

    // 转换 helper
    member internal this.CreateHandle() =
        match this with
        | Boolean -> PolarsWrapper.NewPrimitiveType 0
        | Int8 -> PolarsWrapper.NewPrimitiveType 1
        | Int16 -> PolarsWrapper.NewPrimitiveType 2
        | Int32 -> PolarsWrapper.NewPrimitiveType 3
        | Int64 -> PolarsWrapper.NewPrimitiveType 4
        | UInt8 -> PolarsWrapper.NewPrimitiveType 5
        | UInt16 -> PolarsWrapper.NewPrimitiveType 6
        | UInt32 -> PolarsWrapper.NewPrimitiveType 7
        | UInt64 -> PolarsWrapper.NewPrimitiveType 8
        | Float32 -> PolarsWrapper.NewPrimitiveType 9
        | Float64 -> PolarsWrapper.NewPrimitiveType 10
        | String -> PolarsWrapper.NewPrimitiveType 11
        | Date -> PolarsWrapper.NewPrimitiveType 12
        | Datetime -> PolarsWrapper.NewPrimitiveType 13
        | Time -> PolarsWrapper.NewPrimitiveType 14
        | Duration -> PolarsWrapper.NewPrimitiveType 15
        | Binary -> PolarsWrapper.NewPrimitiveType 16
        | Unknown -> PolarsWrapper.NewPrimitiveType -1
        | Categorical -> PolarsWrapper.NewCategoricalType()
        | Decimal (p, s) -> 
            let prec = defaultArg p 0 // 0 means None in Rust shim
            PolarsWrapper.NewDecimalType(prec, s)

/// <summary>
/// Represents the type of join operation to perform.
/// </summary>
type JoinType =
    | Inner
    | Left
    | Outer
    | Cross
    | Semi
    | Anti
    
    // 内部转换 helper
    member internal this.ToNative() =
        match this with
        | Inner -> PlJoinType.Inner
        | Left -> PlJoinType.Left
        | Outer -> PlJoinType.Outer
        | Cross -> PlJoinType.Cross
        | Semi -> PlJoinType.Semi
        | Anti -> PlJoinType.Anti

/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
type PivotAgg =
    | First | Sum | Min | Max | Mean | Median | Count | Last
    
    member internal this.ToNative() =
        match this with
        | First -> PlPivotAgg.First
        | Sum -> PlPivotAgg.Sum
        | Min -> PlPivotAgg.Min
        | Max -> PlPivotAgg.Max
        | Mean -> PlPivotAgg.Mean
        | Median -> PlPivotAgg.Median
        | Count -> PlPivotAgg.Count
        | Last -> PlPivotAgg.Last
/// <summary>
/// Represents a Polars Expression, which can be a column reference, a literal value, or a computation.
/// </summary>
type Expr(handle: ExprHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr handle

    // --- Namespaces ---
    /// <summary> Access naming operations (prefix/suffix). </summary>
    member this.Name = new NameOps(this.CloneHandle())
    /// <summary> Access list operations. </summary>
    member this.List = new ListOps(this.CloneHandle())
    /// <summary> Access struct operations. </summary>
    member this.Struct = new StructOps(this.CloneHandle())
    /// <summary> Access temporal (date/time) operations. </summary>
    member this.Dt = new DtOps(handle)
    /// <summary> Access string manipulation operations. </summary>
    member this.Str = new StringOps(this.CloneHandle())


    // --- Helpers ---
    member this.Round(decimals: int) = new Expr(PolarsWrapper.Round(this.CloneHandle(), uint decimals))

    // --- Operators ---
    /// <summary> Greater than. </summary>
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.Handle, rhs.Handle))
    /// <summary> Less than. </summary>
    static member (.<) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Lt(lhs.Handle, rhs.Handle))
    /// <summary> Greater than or equal to. </summary>
    static member (.>=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.GtEq(lhs.Handle, rhs.Handle))
    /// <summary> Less than or equal to. </summary>
    static member (.<=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.LtEq(lhs.Handle, rhs.Handle))
    /// <summary> Equal to. </summary>
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.Handle, rhs.Handle))
    /// <summary> Not equal to. </summary>
    static member (.!=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Neq(lhs.Handle, rhs.Handle))
    // 运算符重载, Arithmetic
    static member ( + ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Add(lhs.Handle, rhs.Handle))
    static member ( - ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Sub(lhs.Handle, rhs.Handle))
    static member ( * ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.Handle, rhs.Handle))
    static member ( / ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Div(lhs.Handle, rhs.Handle))
    static member ( % ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Rem(lhs.Handle, rhs.Handle))
    /// <summary> Power / Exponentiation. </summary>
    static member (.**) (baseExpr: Expr, exponent: Expr) = baseExpr.Pow(exponent)
    /// <summary> Logical AND. </summary>
    static member (.&&) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.And(lhs.Handle, rhs.Handle))
    /// <summary> Logical OR. </summary>
    static member (.||) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Or(lhs.Handle, rhs.Handle))
    /// <summary> Logical NOT. </summary>
    static member (!!) (e: Expr) = new Expr(PolarsWrapper.Not e.Handle)
    // --- Methods ---
    /// <summary> Rename the output column. </summary>
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(handle, name))

    /// <summary> Cast the expression to a different data type. </summary>
    member this.Cast(dtype: DataType, ?strict: bool) =
        let isStrict = defaultArg strict false
        
        // 1. 创建 Type Handle
        use typeHandle = dtype.CreateHandle()
        
        // 2. 调用更新后的 Wrapper
        // 注意：CloneHandle 是必须的，因为 Expr 是不可变的，操作产生新 Expr
        let newHandle = PolarsWrapper.ExprCast(this.CloneHandle(), typeHandle, isStrict)
        
        new Expr(newHandle)
    // Aggregations
    member this.Sum() = new Expr(PolarsWrapper.Sum handle)
    member this.Mean() = new Expr(PolarsWrapper.Mean handle)
    member this.Max() = new Expr(PolarsWrapper.Max handle)
    member this.Min() = new Expr(PolarsWrapper.Min handle)
    // Math
    member this.Abs() = new Expr(PolarsWrapper.Abs handle)
    member this.Sqrt() = new Expr(PolarsWrapper.Sqrt(this.CloneHandle()))
    member this.Exp() = new Expr(PolarsWrapper.Exp(this.CloneHandle()))
    member this.Pow(exponent: Expr) = 
        new Expr(PolarsWrapper.Pow(this.CloneHandle(), exponent.CloneHandle()))
    member this.Pow(exponent: double) = 
        this.Pow(PolarsWrapper.Lit exponent |> fun h -> new Expr(h))
    member this.Pow(exponent: int) = 
        this.Pow(PolarsWrapper.Lit exponent |> fun h -> new Expr(h))
    /// <summary> Calculate the logarithm with the given base. </summary>
    member this.Log(baseVal: double) = 
        new Expr(PolarsWrapper.Log(this.CloneHandle(), baseVal))
    member this.Log(baseExpr: Expr) = 
        this.Ln() / baseExpr.Ln()
    /// <summary> Calculate the natural logarithm (base e). </summary>
    member this.Ln() = 
        this.Log Math.E

    // Logic
    /// <summary> Check if the value is between lower and upper bounds (inclusive). </summary>
    member this.IsBetween(lower: Expr, upper: Expr) =
        new Expr(PolarsWrapper.IsBetween(this.CloneHandle(), lower.CloneHandle(), upper.CloneHandle()))
    member this.FillNull(fillValue: Expr) = 
        new Expr(PolarsWrapper.FillNull(this.CloneHandle(), fillValue.CloneHandle()))
    member this.IsNull() = 
        new Expr(PolarsWrapper.IsNull(this.CloneHandle()))
    member this.IsNotNull() = 
        new Expr(PolarsWrapper.IsNotNull(this.CloneHandle()))
    // UDF
    /// <summary>
    /// Apply a custom C# function (UDF) to the expression.
    /// The function receives an Apache Arrow Array and returns an Arrow Array.
    /// </summary>
    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        new Expr(PolarsWrapper.Map(this.CloneHandle(), func))
    member this.Map(func: Func<IArrowArray, IArrowArray>, outputType: PlDataType) =
        new Expr(PolarsWrapper.Map(this.CloneHandle(), func, outputType))
    /// Advanced
    /// <summary> Explode a list column into multiple rows. </summary>
    member this.Explode() = new Expr(PolarsWrapper.Explode(this.CloneHandle()))
    /// <summary> Apply a window function over specific partition columns. </summary>
    member this.Over(partitionBy: Expr list) =
        let mainHandle = this.CloneHandle()
        let partHandles = partitionBy |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        new Expr(PolarsWrapper.Over(mainHandle, partHandles))

    member this.Over(partitionCol: Expr) =
        this.Over [partitionCol]
    // Shift
    member this.Shift(n: int64) = new Expr(PolarsWrapper.Shift(this.CloneHandle(), n))
    // Default shift 1
    member this.Shift() = this.Shift(1L)

    // Diff
    member this.Diff(n: int64) = new Expr(PolarsWrapper.Diff(this.CloneHandle(), n))
    // Default diff 1
    member this.Diff() = this.Diff 1L

    // Fill
    // limit: 0 means fill infinitely
    member this.ForwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.ForwardFill(this.CloneHandle(), uint l))

    member this.BackwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.BackwardFill(this.CloneHandle(), uint l))
    
    member this.FillNullStrategy(strategy: string) =
        match strategy.ToLower() with
        | "forward" | "ffill" -> this.ForwardFill()
        | "backward" | "bfill" -> this.BackwardFill()
        | _ -> failwith "Unsupported strategy"

    member this.RollingMin(windowSize: string) = 
        new Expr(PolarsWrapper.RollingMin(this.CloneHandle(), windowSize))
        
    member this.RollingMax(windowSize: string) = 
        new Expr(PolarsWrapper.RollingMax(this.CloneHandle(), windowSize))

    member this.RollingMean(windowSize: string) = 
        new Expr(PolarsWrapper.RollingMean(this.CloneHandle(), windowSize))
        
    member this.RollingSum(windowSize: string) = 
        new Expr(PolarsWrapper.RollingSum(this.CloneHandle(), windowSize))
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMeanBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingMeanBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))

    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingSumBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingMaxBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))

    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: string) =
        let c = defaultArg closed "left"
        new Expr(PolarsWrapper.RollingMinBy(this.CloneHandle(), windowSize, by.CloneHandle(), c))

// --- Namespace Helpers ---

and DtOps(handle: ExprHandle) =
    let wrap op = new Expr(op handle)
    member _.Year() = wrap PolarsWrapper.DtYear
    member _.Month() = wrap PolarsWrapper.DtMonth
    member _.Day() = wrap PolarsWrapper.DtDay
    member _.Hour() = wrap PolarsWrapper.DtHour
    member _.Minute() = wrap PolarsWrapper.DtMinute
    member _.Second() = wrap PolarsWrapper.DtSecond
    member _.Millisecond() = wrap PolarsWrapper.DtMillisecond
    member _.Microsecond() = wrap PolarsWrapper.DtMicrosecond
    member _.Nanosecond() = wrap PolarsWrapper.DtNanosecond
    member _.OrdinalDay() = wrap PolarsWrapper.DtOrdinalDay
    member _.Weekday() = wrap PolarsWrapper.DtWeekday
    member _.Date() = wrap PolarsWrapper.DtDate
    member _.Time() = wrap PolarsWrapper.DtTime

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        new Expr(PolarsWrapper.DtToString(handle, format)) // 注意这里 handle 是 Clone 进来的，Wrapper 会消耗它

    // col("date").Dt.ToString()
    member this.ToString() = 
        // 这是一个常见的 ISO 格式，或者你可以选择其他默认值
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"

and StringOps(handle: ExprHandle) =
    let wrap op = new Expr(op handle)
    
    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = wrap PolarsWrapper.StrToUpper
    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = wrap PolarsWrapper.StrToLower
    /// <summary> Get length in bytes. </summary>
    member _.Len() = wrap PolarsWrapper.StrLenBytes
    // F# uint64 = C# ulong
    member _.Slice(offset: int64, length: uint64) = 
        new Expr(PolarsWrapper.StrSlice(handle, offset, length))
    member _.ReplaceAll(pattern: string, value: string, ?useRegex: bool) =
        let regex = defaultArg useRegex false
        new Expr(PolarsWrapper.StrReplaceAll(handle, pattern, value,regex))
    member _.Extract(pattern: string, groupIndex: int) =
        new Expr(PolarsWrapper.StrExtract(handle, pattern, uint groupIndex))
    member _.Contains(pat: string) = 
        new Expr(PolarsWrapper.StrContains(handle, pat))
    member _.Split(separator: string) = new Expr(PolarsWrapper.StrSplit(handle, separator))
and NameOps(handle: ExprHandle) =
    let wrap op arg = new Expr(op(handle, arg))
    member _.Prefix(p: string) = wrap PolarsWrapper.Prefix p
    member _.Suffix(s: string) = wrap PolarsWrapper.Suffix s

and ListOps(handle: ExprHandle) =
    member _.First() = new Expr(PolarsWrapper.ListFirst(handle))
    member _.Get(index: int) = new Expr(PolarsWrapper.ListGet(handle, int64 index))
    member _.Join(separator: string) = new Expr(PolarsWrapper.ListJoin(handle, separator))
    member _.Len() = new Expr(PolarsWrapper.ListLen(handle))
    // Aggregations within list
    member _.Sum() = new Expr(PolarsWrapper.ListSum(handle))
    member _.Min() = new Expr(PolarsWrapper.ListMin(handle))
    member _.Max() = new Expr(PolarsWrapper.ListMax(handle))
    member _.Mean() = new Expr(PolarsWrapper.ListMean(handle))
    member _.Sort(descending: bool) = new Expr(PolarsWrapper.ListSort(handle, descending))
    // Contains
    member _.Contains(item: Expr) : Expr = 
        new Expr(PolarsWrapper.ListContains(handle, item.CloneHandle()))
    member _.Contains(item: int) = 
        let itemHandle = PolarsWrapper.Lit(item)
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr(handle), itemHandle))
    member _.Contains(item: string) = 
        let itemHandle = PolarsWrapper.Lit(item)
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr(handle), itemHandle))

and StructOps(handle: ExprHandle) =
    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, name))

/// <summary>
/// A column selection strategy (e.g., all columns, or specific columns).
/// </summary>
type Selector(handle: SelectorHandle) =
    member _.Handle = handle
    
    member internal this.CloneHandle() = 
        PolarsWrapper.CloneSelector(handle)

    /// <summary> Exclude columns from a wildcard selection (col("*")). </summary>
    member this.Exclude(names: string list) =
        let arr = List.toArray names
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))

    member this.ToExpr() =
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))
/// --- Series ---
/// <summary>
/// An eager Series holding a single column of data.
/// </summary>
type Series(handle: SeriesHandle) =
    interface IDisposable with member _.Dispose() = handle.Dispose()
    member _.Handle = handle

    member _.Name = PolarsWrapper.SeriesName handle
    member _.Length = PolarsWrapper.SeriesLen handle
    
    member this.Rename(name: string) = 
        PolarsWrapper.SeriesRename(handle, name)
        this

    /// <summary>
    /// Convert the Series to an Apache Arrow Array.
    /// This operation involves zero-copy if possible, but may rechunk data internally.
    /// </summary>
    member this.ToArrow() : IArrowArray =
        PolarsWrapper.SeriesToArrow(handle)

    // ==========================================
    // Static Constructors
    // ==========================================
    
    // --- Int32 ---
    static member create(name: string, data: int seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: int option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<int> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- 0; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))

    // --- Int64 ---
    static member create(name: string, data: int64 seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: int64 option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- 0L; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))
        
    // --- Float64 ---
    static member create(name: string, data: double seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: double option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<double> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- Double.NaN; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))

    // --- Boolean ---
    static member create(name: string, data: bool seq) = 
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data, null))

    static member create(name: string, data: bool option seq) = 
        let arr = Seq.toArray data
        let vals = Array.zeroCreate<bool> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some v -> vals.[i] <- v; valid.[i] <- true
            | None -> vals.[i] <- false; valid.[i] <- false
        new Series(PolarsWrapper.SeriesNew(name, vals, valid))

    // --- String ---
    static member create(name: string, data: string seq) = 
        // 这里的 string seq 本身可能包含 null (如果源是 C#), 或者 F# string (不可空)
        // 为了安全，我们转为 string[] 即可
        new Series(PolarsWrapper.SeriesNew(name, Seq.toArray data))

    static member create(name: string, data: string option seq) = 
        let arr = Seq.toArray data
        // 将 Option 转换为 string array (None -> null)
        let vals = arr |> Array.map (fun opt -> match opt with Some s -> s | None -> null)
        new Series(PolarsWrapper.SeriesNew(name, vals))

    // ==========================================
    // Interop with DataFrame
    // ==========================================
    member this.ToFrame() : DataFrame =
        let h = PolarsWrapper.SeriesToFrame handle
        new DataFrame(h)

    member this.Cast(dtype: DataType) : Series =
        use typeHandle = dtype.CreateHandle()
        let newHandle = PolarsWrapper.SeriesCast(handle, typeHandle)
        new Series(newHandle)
// --- Frames ---

/// <summary>
/// An eager DataFrame holding data in memory.
/// </summary>
and DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    member this.Clone() = new DataFrame(PolarsWrapper.CloneDataFrame handle)
    member internal this.CloneHandle() = PolarsWrapper.CloneDataFrame handle
    member _.Handle = handle
    // Interop
    member this.ToArrow() = PolarsWrapper.Collect handle
    member _.Rows = PolarsWrapper.DataFrameHeight handle
    member _.Columns = PolarsWrapper.DataFrameWidth handle
    member _.ColumnNames = PolarsWrapper.GetColumnNames handle |> Array.toList
    member this.Item 
        with get(colName: string, rowIndex: int) =
            PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
    member this.Int(colName: string, rowIndex: int) : int64 option = 
        let nullableVal = PolarsWrapper.GetInt(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.Float(colName: string, rowIndex: int) : float option = 
        let nullableVal = PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.String(colName: string, rowIndex: int) = PolarsWrapper.GetString(handle, colName, int64 rowIndex) |> Option.ofObj
    member this.StringList(colName: string, rowIndex: int) : string list option =
        use colHandle = PolarsWrapper.Select(handle, [| PolarsWrapper.Col colName |])
        use tempDf = new DataFrame(colHandle)
        use arrowBatch = tempDf.ToArrow()
        
        let col = arrowBatch.Column colName
        
        let extractStrings (valuesArr: IArrowArray) (startIdx: int) (endIdx: int) =
            match valuesArr with
            | :? StringArray as sa ->
                [ for i in startIdx .. endIdx - 1 -> sa.GetString i ]
            | :? StringViewArray as sva ->
                [ for i in startIdx .. endIdx - 1 -> sva.GetString i ]
            | _ -> [] 

        match col with
        // Case A: Arrow.ListArray 
        | :? Apache.Arrow.ListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                let start = listArr.ValueOffsets.[rowIndex]
                let end_ = listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        // Case B: Large List (64-bit offsets) 
        | :? Apache.Arrow.LargeListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                // Offset 是 long，强转 int (单行 List 长度通常不会超过 20 亿)
                let start = int listArr.ValueOffsets.[rowIndex]
                let end_ = int listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        | _ -> 
            // System.Console.WriteLine($"[Debug] Mismatched Array Type: {col.GetType().Name}")
            None
    member this.Column(name: string) : Series =
    // 我们假设 Rust 端有 pl_dataframe_get_column
        let h = PolarsWrapper.DataFrameGetColumn(this.Handle, name)
        new Series(h)
    member this.Column(index: int) : Series =
        let h = PolarsWrapper.DataFrameGetColumnAt(this.Handle, index)
        new Series(h)
        
    member this.Item 
        with get(name: string) = this.Column name
    
    member this.Item 
        with get(index: int) = this.Column index

    member this.GetSeries() : Series list =
        [ for i in 0 .. int this.Columns - 1 -> this.Column i ]

/// <summary>
/// A LazyFrame represents a logical plan of operations that will be optimized and executed only when collected.
/// </summary>
type LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.LazyClone handle
    /// <summary> Execute the plan and return a DataFrame. </summary>
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect handle
        new DataFrame(dfHandle)
    /// <summary> Get the schema string of the LazyFrame without executing it. </summary>
    member _.SchemaRaw = PolarsWrapper.GetSchemaString handle

    /// <summary> Get the schema of the LazyFrame without executing it. </summary>
    member _.Schema = 
        let dict = PolarsWrapper.GetSchema handle
        dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

    /// <summary> Print the query plan. </summary>
    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)
/// <summary>
/// SQL Context for executing SQL queries on registered LazyFrames.
/// </summary>
type SqlContext() =
    let handle = PolarsWrapper.SqlContextNew()
    
    interface IDisposable with
        member _.Dispose() = handle.Dispose()

    /// <summary> Register a LazyFrame as a table for SQL querying. </summary>
    member _.Register(name: string, lf: LazyFrame) =
        PolarsWrapper.SqlRegister(handle, name, lf.CloneHandle())

    /// <summary> Execute a SQL query and return a LazyFrame. </summary>
    member _.Execute(query: string) =
        new LazyFrame(PolarsWrapper.SqlExecute(handle, query))