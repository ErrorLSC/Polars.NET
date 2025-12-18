namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
open System.Collections.Generic
open Polars.NET.Core.Arrow
/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
type DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float16 | Float32 | Float64
    | String
    | Date | Datetime | Time
    | Duration
    | Binary
    | Categorical
    | Struct
    | Decimal of precision: int option * scale: int
    | Unknown | SameAsInput | Null

    // 转换 helper
    member internal this.CreateHandle() =
        match this with
        | SameAsInput -> PolarsWrapper.NewPrimitiveType 0
        | Boolean -> PolarsWrapper.NewPrimitiveType 1
        | Int8 -> PolarsWrapper.NewPrimitiveType 2
        | Int16 -> PolarsWrapper.NewPrimitiveType 3
        | Int32 -> PolarsWrapper.NewPrimitiveType 4
        | Int64 -> PolarsWrapper.NewPrimitiveType 5
        | UInt8 -> PolarsWrapper.NewPrimitiveType 6
        | UInt16 -> PolarsWrapper.NewPrimitiveType 7
        | UInt32 -> PolarsWrapper.NewPrimitiveType 8
        | UInt64 -> PolarsWrapper.NewPrimitiveType 9
        | Float32 -> PolarsWrapper.NewPrimitiveType 10
        | Float64 -> PolarsWrapper.NewPrimitiveType 11
        | String -> PolarsWrapper.NewPrimitiveType 12
        | Date -> PolarsWrapper.NewPrimitiveType 13
        | Datetime -> PolarsWrapper.NewPrimitiveType 14
        | Time -> PolarsWrapper.NewPrimitiveType 15
        | Duration -> PolarsWrapper.NewPrimitiveType 16
        | Binary -> PolarsWrapper.NewPrimitiveType 17
        | Null -> PolarsWrapper.NewPrimitiveType 18
        | Struct -> PolarsWrapper.NewPrimitiveType 19
        | Float16 -> PolarsWrapper.NewPrimitiveType 20
        | Unknown -> PolarsWrapper.NewPrimitiveType 0
        | Categorical -> PolarsWrapper.NewCategoricalType()
        | Decimal (p, s) -> 
            let prec = defaultArg p 0 // 0 means None in Rust shim
            PolarsWrapper.NewDecimalType(prec, s)
    static member Parse(str: string) =
        match str with
        | "bool" -> Boolean
        | "i8" -> Int8
        | "i16" -> Int16
        | "i32" -> Int32
        | "i64" -> Int64
        | "u8" -> UInt8
        | "u16" -> UInt16
        | "u32" -> UInt32
        | "u64" -> UInt64
        | "f16" -> Float16
        | "f32" -> Float32
        | "f64" -> Float64
        | "str" | "String" -> String // 兼容一下旧版
        | "date" -> Date
        | "time" -> Time
        | "null" -> Null
        | "struct" -> Struct
        | s when s.StartsWith "datetime" -> Datetime // 处理 "datetime[μs]" 这种带参数的
        | s when s.StartsWith "duration" -> Duration
        | s when s.StartsWith "decimal" -> 
            // 简单处理 decimal，暂不解析具体精度
            Decimal(None, 0)
        | "cat" -> Categorical
        | _ -> Unknown
    member this.IsNumeric =
        match this with
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int8 | Int16 | Int32 | Int64
        | Float16 | Float32 | Float64 
        | Decimal _ -> true
        | _ -> false

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
    // Stats
    /// <summary>
    /// Count the number of valid (non-null) values.
    /// </summary>
    member this.Count() = new Expr(PolarsWrapper.Count handle)
    member this.Std(?ddof: int) = 
        let d = defaultArg ddof 1 // Default sample std dev
        new Expr(PolarsWrapper.Std(handle, d))
        
    member this.Var(?ddof: int) = 
        let d = defaultArg ddof 1
        new Expr(PolarsWrapper.Var(handle, d))
        
    member this.Median() = new Expr(PolarsWrapper.Median handle)
    
    member this.Quantile(q: float, ?interpolation: string) =
        let method = defaultArg interpolation "linear"
        new Expr(PolarsWrapper.Quantile(this.CloneHandle(), q, method))
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
    member this.Map(func: Func<IArrowArray, IArrowArray>, outputType: DataType) =
        use typeHandle = outputType.CreateHandle()
        let newHandle = PolarsWrapper.Map(this.CloneHandle(), func, typeHandle)
        new Expr(newHandle)
    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        this.Map(func, DataType.SameAsInput)
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

    member this.RollingMin(windowSize: string, ?minPeriod: int) =
        let m = defaultArg minPeriod 1
        new Expr(PolarsWrapper.RollingMin(this.CloneHandle(), windowSize,m))
        
    member this.RollingMax(windowSize: string, ?minPeriod: int) =
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMax(this.CloneHandle(), windowSize,m))

    member this.RollingMean(windowSize: string, ?minPeriod: int) = 
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMean(this.CloneHandle(), windowSize, m))
        
    member this.RollingSum(windowSize: string, ?minPeriod: int) =
        let m = defaultArg minPeriod 1  
        new Expr(PolarsWrapper.RollingSum(this.CloneHandle(), windowSize, m))
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMeanBy(windowSize: string, by: Expr,?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1
        new Expr(PolarsWrapper.RollingMeanBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))

    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingSumBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: string, ?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMaxBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))

    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: string, ?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMinBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))

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
    // --- Manipulation ---

    /// <summary>
    /// Truncate dates to the specified interval (e.g., "1d", "1h", "15m").
    /// </summary>
    member _.Truncate(every: string) = 
        new Expr(PolarsWrapper.DtTruncate(handle, every))

    /// <summary>
    /// Round dates to the nearest interval.
    /// </summary>
    member _.Round(every: string) = 
        new Expr(PolarsWrapper.DtRound(handle, every))

    /// <summary>
    /// Offset the date by a given duration string (e.g., "1d", "-2h").
    /// </summary>
    member _.OffsetBy(duration: string) =
        // 自动创建 lit(duration)
        let durExpr = PolarsWrapper.Lit(duration) // 假设 Lit(string) wrapper 存在
        new Expr(PolarsWrapper.DtOffsetBy(handle, durExpr))

    /// <summary>
    /// Offset the date by a duration expression (dynamic offset).
    /// </summary>
    member _.OffsetBy(duration: Expr) =
        new Expr(PolarsWrapper.DtOffsetBy(handle, duration.CloneHandle()))

    // --- Conversion ---

    /// <summary>
    /// Convert to integer timestamp (Microseconds).
    /// </summary>
    member _.TimestampMicros() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, 1))

    /// <summary>
    /// Convert to integer timestamp (Milliseconds).
    /// </summary>
    member _.TimestampMillis() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, 2))
    /// <summary>
    /// Convert the datetime to a different time zone.
    /// The underlying physical value (UTC timestamp) remains the same, but the display time changes.
    /// </summary>
    member _.ConvertTimeZone(timeZone: string) =
        new Expr(PolarsWrapper.DtConvertTimeZone(handle, timeZone))

    /// <summary>
    /// Replace the time zone of a datetime.
    /// Use None (null) to make it TimeZone-Naive.
    /// ambiguous: Strategy for DST overlaps ("raise", "earliest", "latest", "null").
    /// nonExistent: Strategy for missing DST times ("raise", "null").
    /// </summary>
    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: string, ?nonExistent: string) =
        let tz = Option.toObj timeZone
        let amb = Option.toObj ambiguous
        let ne = Option.toObj nonExistent
        new Expr(PolarsWrapper.DtReplaceTimeZone(handle, tz, amb, ne))

    /// <summary>
    /// Helper: Replace time zone with a specific string.
    /// </summary>
    member this.ReplaceTimeZone(timeZone: string, ?ambiguous: string, ?nonExistent: string) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent)
        
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
    /// <summary>
    /// Remove leading and trailing characters.
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.Strip(?matches: string) = 
        // Option.toObj: None -> null, Some s -> s
        new Expr(PolarsWrapper.StrStripChars(handle, Option.toObj matches))

    /// <summary>
    /// Remove leading characters (Left Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.LStrip(?matches: string) = 
        new Expr(PolarsWrapper.StrStripCharsStart(handle, Option.toObj matches))

    /// <summary>
    /// Remove trailing characters (Right Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.RStrip(?matches: string) = 
        new Expr(PolarsWrapper.StrStripCharsEnd(handle, Option.toObj matches))

    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    member _.StripPrefix(prefix: string) = 
        new Expr(PolarsWrapper.StrStripPrefix(handle, prefix))

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    member _.StripSuffix(suffix: string) = 
        new Expr(PolarsWrapper.StrStripSuffix(handle, suffix))

    /// <summary>
    /// Check if string starts with a specific prefix.
    /// </summary>
    member _.StartsWith(prefix: string) = 
        new Expr(PolarsWrapper.StrStartsWith(handle, prefix))

    /// <summary>
    /// Check if string ends with a specific suffix.
    /// </summary>
    member _.EndsWith(suffix: string) = 
        new Expr(PolarsWrapper.StrEndsWith(handle, suffix))

    /// <summary>
    /// Parse string to Date using a format string (e.g., "%Y-%m-%d").
    /// </summary>
    member _.ToDate(format: string) = 
        new Expr(PolarsWrapper.StrToDate(handle, format))

    /// <summary>
    /// Parse string to Datetime using a format string.
    /// </summary>
    member _.ToDatetime(format: string) = 
        new Expr(PolarsWrapper.StrToDatetime(handle, format))
and NameOps(handle: ExprHandle) =
    let wrap op arg = new Expr(op(handle, arg))
    member _.Prefix(p: string) = wrap PolarsWrapper.Prefix p
    member _.Suffix(s: string) = wrap PolarsWrapper.Suffix s

and ListOps(handle: ExprHandle) =
    member _.First() = new Expr(PolarsWrapper.ListFirst handle)
    member _.Get(index: int) = new Expr(PolarsWrapper.ListGet(handle, int64 index))
    member _.Join(separator: string) = new Expr(PolarsWrapper.ListJoin(handle, separator))
    member _.Len() = new Expr(PolarsWrapper.ListLen handle)
    // Aggregations within list
    member _.Sum() = new Expr(PolarsWrapper.ListSum handle)
    member _.Min() = new Expr(PolarsWrapper.ListMin handle)
    member _.Max() = new Expr(PolarsWrapper.ListMax handle)
    member _.Mean() = new Expr(PolarsWrapper.ListMean handle)
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
    member this.Slice(offset: int64, length: int64) =
        new Series(PolarsWrapper.SeriesSlice(handle, offset, length))
    /// <summary>
    /// Get the string representation of the Series Data Type (e.g., "Int64", "String").
    /// </summary>
    member _.DtypeStr = PolarsWrapper.GetSeriesDtypeString handle

    /// <summary>
    /// Returns a boolean Series indicating which values are null.
    /// </summary>
    member this.IsNull() : Series = 
        new Series(PolarsWrapper.SeriesIsNull handle)

    /// <summary>
    /// Returns a boolean Series indicating which values are not null.
    /// </summary>
    member this.IsNotNull() : Series = 
        new Series(PolarsWrapper.SeriesIsNotNull handle)
    /// <summary>
    /// Check if the value at the specified index is null.
    /// This is faster than retrieving the value and checking for Option.None.
    /// </summary>
    member _.IsNullAt(index: int) : bool =
        PolarsWrapper.SeriesIsNullAt(handle, int64 index)
    /// <summary>
    /// Get the number of null values in the Series.
    /// This is an O(1) operation (metadata access).
    /// </summary>
    member _.NullCount : int64 = 
        PolarsWrapper.SeriesNullCount handle
    /// <summary> Check if floating point values are NaN. </summary>
    member this.IsNan() = new Series(PolarsWrapper.SeriesIsNan handle)

    /// <summary> Check if floating point values are not NaN. </summary>
    member this.IsNotNan() = new Series(PolarsWrapper.SeriesIsNotNan handle)

    /// <summary> Check if floating point values are finite (not NaN and not Inf). </summary>
    member this.IsFinite() = new Series(PolarsWrapper.SeriesIsFinite handle)

    /// <summary> Check if floating point values are infinite. </summary>
    member this.IsInfinite() = new Series(PolarsWrapper.SeriesIsInfinite handle)
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
    // --- DateTime ---
    static member create(name: string, data: DateTime seq) = 
        let arr = Seq.toArray data
        let longs = Array.zeroCreate<int64> arr.Length
        let epoch = 621355968000000000L
        
        for i in 0 .. arr.Length - 1 do
            // 转换为 Unix Microseconds
            longs.[i] <- (arr.[i].Ticks - epoch) / 10L

        // 1. 创建 Int64 Series
        let s = Series.create(name, longs)
        // 2. 转换为 Datetime 类型 (Microseconds, No Timezone)
        s.Cast DataType.Datetime

    static member create(name: string, data: DateTime option seq) = 
        let arr = Seq.toArray data
        let longs = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        let epoch = 621355968000000000L
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some dt -> 
                longs.[i] <- (dt.Ticks - epoch) / 10L
                valid.[i] <- true
            | None -> 
                longs.[i] <- 0L
                valid.[i] <- false

        // 直接调用底层 Wrapper 创建带 Validity 的 Int64 Series
        let s = new Series(PolarsWrapper.SeriesNew(name, longs, valid))
        s.Cast DataType.Datetime

    // --- Decimal ---
    /// <summary>
    /// Create a Decimal Series.
    /// scale: The number of decimal places (e.g., 2 for currency).
    /// </summary>
    static member create(name: string, data: decimal seq, scale: int) = 
        new Series(PolarsWrapper.SeriesNewDecimal(name, Seq.toArray data, null, scale))

    static member create(name: string, data: decimal option seq, scale: int) = 
        let arr = Seq.toArray data // decimal option[]
        // 转换逻辑稍复杂，我们在 Wrapper 里处理了 nullable 数组转换
        // 这里我们需要把 seq<decimal option> 转为 decimal?[] (Nullable<decimal>[]) 传给 C#
        let nullableArr = 
            arr |> Array.map (function Some v -> System.Nullable(v) | None -> System.Nullable())
            
        new Series(PolarsWrapper.SeriesNewDecimal(name, nullableArr, scale))
    // ==========================================
    // Temporal Types Creation
    // ==========================================

    // --- DateOnly (Polars Date: i32 days) ---
    static member create(name: string, data: DateOnly seq) =
        let arr = Seq.toArray data
        let days = Array.zeroCreate<int> arr.Length
        let epochOffset = 719162 // 0001-01-01 to 1970-01-01
        
        for i in 0 .. arr.Length - 1 do
            days.[i] <- arr.[i].DayNumber - epochOffset
            
        let s = Series.create(name, days)
        s.Cast DataType.Date

    static member create(name: string, data: DateOnly option seq) =
        let arr = Seq.toArray data
        let days = Array.zeroCreate<int> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        let epochOffset = 719162
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some d -> 
                days.[i] <- d.DayNumber - epochOffset
                valid.[i] <- true
            | None -> 
                days.[i] <- 0
                valid.[i] <- false
                
        // 调用底层 int32 (SeriesNew)
        let s = new Series(PolarsWrapper.SeriesNew(name, days, valid))
        s.Cast(DataType.Date)

    // --- TimeOnly (Polars Time: i64 nanoseconds) ---
    static member create(name: string, data: TimeOnly seq) =
        let arr = Seq.toArray data
        let nanos = Array.zeroCreate<int64> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            // Ticks = 100ns -> * 100 = ns
            nanos.[i] <- arr.[i].Ticks * 100L
            
        let s = Series.create(name, nanos)
        s.Cast(DataType.Time)

    static member create(name: string, data: TimeOnly option seq) =
        let arr = Seq.toArray data
        let nanos = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some t -> 
                nanos.[i] <- t.Ticks * 100L
                valid.[i] <- true
            | None -> 
                nanos.[i] <- 0L
                valid.[i] <- false
                
        let s = new Series(PolarsWrapper.SeriesNew(name, nanos, valid))
        s.Cast DataType.Time

    // --- TimeSpan (Polars Duration: i64 microseconds) ---
    // 为了和 Datetime(us) 兼容，Duration 也默认用 us
    static member create(name: string, data: TimeSpan seq) =
        let arr = Seq.toArray data
        let micros = Array.zeroCreate<int64> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            // Ticks = 100ns -> / 10 = us
            micros.[i] <- arr.[i].Ticks / 10L
            
        let s = Series.create(name, micros)
        s.Cast(DataType.Duration)

    static member create(name: string, data: TimeSpan option seq) =
        let arr = Seq.toArray data
        let micros = Array.zeroCreate<int64> arr.Length
        let valid = Array.zeroCreate<bool> arr.Length
        
        for i in 0 .. arr.Length - 1 do
            match arr.[i] with
            | Some t -> 
                micros.[i] <- t.Ticks / 10L
                valid.[i] <- true
            | None -> 
                micros.[i] <- 0L
                valid.[i] <- false
                
        let s = new Series(PolarsWrapper.SeriesNew(name, micros, valid))
        s.Cast DataType.Duration
    /// <summary>
    /// Smart Constructor:
    /// 1. Handles primitive types (int, double...).
    /// 2. Handles Option types (int option...) by forwarding to ofOptionSeq.
    /// 3. Handles Decimal types (decimal, decimal option) by inferring scale.
    /// </summary>
    static member ofOptionSeq<'T>(name: string, data: seq<'T option>) : Series =
        let t = typeof<'T>
        if t = typeof<int> then Series.create(name, data |> Seq.cast<int option>)
        else if t = typeof<int64> then Series.create(name, data |> Seq.cast<int64 option>)
        else if t = typeof<double> then Series.create(name, data |> Seq.cast<double option>)
        else if t = typeof<bool> then Series.create(name, data |> Seq.cast<bool option>)
        else if t = typeof<string> then Series.create(name, data |> Seq.cast<string option>)
        else if t = typeof<DateTime> then Series.create(name, data |> Seq.cast<DateTime option>)
        else if t = typeof<DateOnly> then Series.create(name, data |> Seq.cast<DateOnly option>)
        else if t = typeof<TimeOnly> then Series.create(name, data |> Seq.cast<TimeOnly option>)
        else if t = typeof<TimeSpan> then Series.create(name, data |> Seq.cast<TimeSpan option>)
        else failwithf "Unsupported type for Series.ofOptionSeq: %A" t

    // --- Scalar Access ---
    
    /// <summary> Get value as Int64 Option. Handles Int32/Int64 etc. </summary>
    member _.Int(index: int) : int64 option = 
        PolarsWrapper.SeriesGetInt(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as Double Option. Handles Float32/Float64. </summary>
    member _.Float(index: int) : float option = 
        PolarsWrapper.SeriesGetDouble(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as String Option. </summary>
    member _.String(index: int) : string option = 
        PolarsWrapper.SeriesGetString(handle, int64 index) |> Option.ofObj

    /// <summary> Get value as Boolean Option. </summary>
    member _.Bool(index: int) : bool option = 
        PolarsWrapper.SeriesGetBool(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as Decimal Option. </summary>
    member _.Decimal(index: int) : decimal option = 
        PolarsWrapper.SeriesGetDecimal(handle, int64 index) |> Option.ofNullable

    // 时间类型
    member _.Date(index: int) : DateOnly option = 
        PolarsWrapper.SeriesGetDate(handle, int64 index) |> Option.ofNullable

    member _.Time(index: int) : TimeOnly option = 
        PolarsWrapper.SeriesGetTime(handle, int64 index) |> Option.ofNullable

    member _.Datetime(index: int) : DateTime option = 
        PolarsWrapper.SeriesGetDatetime(handle, int64 index) |> Option.ofNullable

    member _.Duration(index: int) : TimeSpan option = 
        PolarsWrapper.SeriesGetDuration(handle, int64 index) |> Option.ofNullable
    // --- Aggregations (Returning Series of len 1) ---
    // 返回 Series 而不是 scalar，是为了支持链式计算 (s.Sum() / s.Count())
    // 最终要取值时，用户可以调 s.Int(0) 或 s.Float(0)
    
    member this.Sum() = new Series(PolarsWrapper.SeriesSum handle)
    member this.Mean() = new Series(PolarsWrapper.SeriesMean handle)
    member this.Min() = new Series(PolarsWrapper.SeriesMin handle)
    member this.Max() = new Series(PolarsWrapper.SeriesMax handle)

    // --- Operators (Arithmetic) ---

    static member (+) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesAdd(lhs.Handle, rhs.Handle))
    static member (-) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesSub(lhs.Handle, rhs.Handle))
    static member (*) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesMul(lhs.Handle, rhs.Handle))
    static member (/) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesDiv(lhs.Handle, rhs.Handle))

    // --- Operators (Comparison) ---

    static member (.=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesEq(lhs.Handle, rhs.Handle))
    static member (!=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesNeq(lhs.Handle, rhs.Handle))
    static member (.>) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLt(lhs.Handle, rhs.Handle))

    static member (.>=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLtEq(lhs.Handle, rhs.Handle))

    // --- Broadcasting Helpers (Scalar Ops) ---
    // 允许 s + 1, s * 2.5 等操作
    // 我们创建一个临时的长度为1的 Series 传给 Rust，Rust 会处理广播

    static member (+) (lhs: Series, rhs: int) = lhs + Series.create("lit", [rhs])
    static member (+) (lhs: Series, rhs: double) = lhs + Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: int) = lhs - Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: double) = lhs - Series.create("lit", [rhs])
    
    static member (*) (lhs: Series, rhs: int) = lhs * Series.create("lit", [rhs])
    static member (*) (lhs: Series, rhs: double) = lhs * Series.create("lit", [rhs])
    
    static member (/) (lhs: Series, rhs: int) = lhs / Series.create("lit", [rhs])
    static member (/) (lhs: Series, rhs: double) = lhs / Series.create("lit", [rhs])

    // Comparison with Scalar
    static member (.>) (lhs: Series, rhs: int) = lhs .> Series.create("lit", [rhs])
    static member (.>) (lhs: Series, rhs: double) = lhs .> Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: int) = lhs .< Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: double) = lhs .< Series.create("lit", [rhs])
    static member (.>=) (lhs: Series, rhs: int) = lhs .>= Series.create("lit", [rhs])
    static member (.<=) (lhs: Series, rhs: double) = lhs .<= Series.create("lit", [rhs])
    
    static member (.=) (lhs: Series, rhs: int) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: double) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: string) = lhs .= Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: int) = lhs != Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: string) = lhs != Series.create("lit", [rhs])
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
    member this.ToArrow() : IArrowArray =
        PolarsWrapper.SeriesToArrow handle

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
    static member ReadCsv (path: string, 
                               ?schema: Map<string, DataType>, // 注意逗号
                               ?hasHeader: bool,               // 注意逗号
                               ?separator: char,
                               ?skipRows: int,
                               ?tryParseDates: bool) : DataFrame =
        
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        let h = PolarsWrapper.ReadCsv(path, schemaDict, header, sep, uint64 skip, dates)
        new DataFrame(h)
    /// <summary> Asynchronously read a CSV file into a DataFrame. </summary>
    static member ReadCsvAsync(path: string, 
                               ?schema: Map<string, DataType>,
                               ?hasHeader: bool,
                               ?separator: char,
                               ?skipRows: int,
                               ?tryParseDates: bool) : Async<DataFrame> =
        
        // 1. 在主线程准备参数 (参数解析是非常快的)
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        // 2. 转换 Schema (Map -> Dictionary)
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                // 记得使用 CreateHandle()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        // 3. 进入 Async 工作流
        async {
            // 调用 C# Wrapper 的 Async 方法 (返回 Task<DataFrameHandle>)
            // 使用 Async.AwaitTask 等待 C# Task 完成
            let! handle = 
                PolarsWrapper.ReadCsvAsync(
                    path, 
                    schemaDict, 
                    header, 
                    sep, 
                    uint64 skip, 
                    dates
                ) |> Async.AwaitTask

            return new DataFrame(handle)
        }

    /// <summary> Read a parquet file into a DataFrame (Eager). </summary>
    static member ReadParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet path)
    /// <summary> Read a JSON file into a DataFrame (Eager). </summary>
    static member ReadJson (path: string) : DataFrame =
        new DataFrame(PolarsWrapper.ReadJson path)
    /// <summary> Read an IPC file into a DataFrame (Eager). </summary>
    static member ReadIpc (path: string) = new DataFrame(PolarsWrapper.ReadIpc path)

    static member ofSeqStream<'T>(data: seq<'T>, ?batchSize: int) : DataFrame =
        let size = defaultArg batchSize 100_000

        // 1. 构建批次流 (Lazy Batch Stream)
        // 使用 Seq.chunk 实现分块，然后惰性映射到 RecordBatch
        let batchStream = 
            data
            |> Seq.chunkBySize size
            |> Seq.map (fun chunk -> 
                // 调用 Core 层的转换器，把一块数据转成 Arrow RecordBatch
                ArrowFfiBridge.BuildRecordBatch chunk
            )

        // 获取枚举器 (此时还没开始转换数据)
        let enumerator = batchStream.GetEnumerator()

        // 2. 探测首帧 (Peek First Batch)
        if not (enumerator.MoveNext()) then
            // 空序列处理：释放枚举器，返回空 DataFrame
            enumerator.Dispose()
            DataFrame.create []
        else
            let firstBatch = enumerator.Current
            let schema = firstBatch.Schema

            // 3. 缝合迭代器 (Stitch)
            // 使用 C# Core 定义的 PrependEnumerator 把探测出来的第一帧缝回去
            // 这样 Interop 层能读到完整的数据流
            let combinedEnumerator = new PrependEnumerator(firstBatch, enumerator)

            // 4. 调用 Interop 层 (Zero-Copy Import)
            // ImportEager 会接管 combinedEnumerator 的生命周期 (包括 Dispose)
            // Rust 端会通过 C Stream Interface 拉取数据
            let handle = ArrowStreamInterop.ImportEager(combinedEnumerator, schema)

            new DataFrame(handle)
    static member FromArrow (batch: Apache.Arrow.RecordBatch) : DataFrame =
        new DataFrame(PolarsWrapper.FromArrow batch)
    /// <summary> Write DataFrame to CSV. </summary>
    member this.WriteCsv (path: string) = 
        PolarsWrapper.WriteCsv(this.Handle, path)
        this 
    /// <summary> Write DataFrame to Parquet. </summary>
    member this.WriteParquet (path: string) = 
        PolarsWrapper.WriteParquet(this.Handle, path)
        this
    /// <summary>
    /// Write DataFrame to an Arrow IPC (Feather) file.
    /// This is a fast, zero-copy binary format.
    /// </summary>
    member this.WriteIpc(path: string)=
        PolarsWrapper.WriteIpc(this.Handle, path)
        this
    /// <summary>
    /// Write DataFrame to a JSON file (standard array format).
    /// </summary>
    member this.WriteJson(path: string) =
        PolarsWrapper.WriteJson(this.Handle, path)
        this 
    
    /// <summary>
    /// Get the schema as Map<ColumnName, DataType>.
    /// </summary>
    member this.Schema : Map<string, DataType> =
        let json = PolarsWrapper.GetDataFrameSchemaString handle
        if String.IsNullOrEmpty json then Map.empty
        else
            try
                // 依赖 System.Text.Json
                let dict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(json)
                dict 
                |> Seq.map (fun kv -> kv.Key, DataType.Parse kv.Value) 
                |> Map.ofSeq
            with _ ->
                Map.empty
    member this.Lazy() : LazyFrame =
        let lfHandle = PolarsWrapper.DataFrameToLazy handle
        new LazyFrame(lfHandle)
    /// <summary>
    /// Print schema in a readable format.
    /// </summary>
    member this.PrintSchema() =
        printfn "--- DataFrame Schema ---"
        this.Schema |> Map.iter (fun name dtype -> 
            // 简单的反射打印 DU 名称
            printfn "%-15s | %A" name dtype
        )
        printfn "------------------------"
    static member create(series: Series list) : DataFrame =
        let handles = 
            series 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
            
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    
    // 重载：允许变长参数 (df = DataFrame.create(s1, s2, s3))
    static member create([<System.ParamArray>] series: Series[]) : DataFrame =
        let handles = series |> Array.map (fun s -> s.Handle)
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    /// <summary>
    /// Remove a column by name. Returns a new DataFrame.
    /// </summary>
    member this.Drop(name: string) : DataFrame =
        new DataFrame(PolarsWrapper.Drop(handle, name))

    /// <summary>
    /// Rename a column. Returns a new DataFrame.
    /// </summary>
    member this.Rename(oldName: string, newName: string) : DataFrame =
        new DataFrame(PolarsWrapper.Rename(handle, oldName, newName))

    /// <summary>
    /// Drop rows containing any null values.
    /// subset: Optional list of column names to consider.
    /// </summary>
    member this.DropNulls(?subset: string list) : DataFrame =
        let s = subset |> Option.map List.toArray |> Option.toObj
        new DataFrame(PolarsWrapper.DropNulls(handle, s))

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    member this.Sample(n: int, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        // n 必须 >= 0
        new DataFrame(PolarsWrapper.SampleN(handle, uint64 n, replace, shuff, s))

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    member this.Sample(frac: double, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        new DataFrame(PolarsWrapper.SampleFrac(handle, frac, replace, shuff, s))
    // Interop
    member this.ToArrow() = ArrowFfiBridge.ExportDataFrame handle
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
    member this.Decimal(col: string, row: int) : decimal option =
        use s = this.Column col
        s.Decimal row
    // 1. Boolean
    member this.Bool(col: string, row: int) : bool option =
        use s = this.Column col
        s.Bool row
    // 2. Date (DateOnly)
    member this.Date(col: string, row: int) : DateOnly option =
        use s = this.Column col
        s.Date row

    // 3. Time (TimeOnly)
    member this.Time(col: string, row: int) : TimeOnly option =
        use s = this.Column col
        s.Time row

    // 4. Datetime (DateTime)
    member this.Datetime(col: string, row: int) : DateTime option =
        use s = this.Column col
        s.Datetime row

    // 5. Duration (TimeSpan)
    member this.Duration(col: string, row: int) : TimeSpan option =
        use s = this.Column col
        s.Duration row
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
    /// Check if the value at the specified column and row is null.
    /// </summary>
    member this.IsNullAt(col: string, row: int) : bool =
        use s = this.Column col
        s.IsNullAt row
    /// <summary>
    /// Get the number of null values in a specific column.
    /// </summary>
    member this.NullCount(colName: string) : int64 =
        use s = this.Column colName
        s.NullCount
    member this.IsNan(col: string) =
        use s = this.Column col
        s.IsNan()
    member this.IsNotNan (col:string) =
        use s = this.Column col
        s.IsNotNan()
    member this.IsFinite (col:string) =
        use s = this.Column col
        s.IsFinite()
    member this.IsInfinite (col:string) =
        use s = this.Column col
        s.IsInfinite()
/// <summary>
/// A LazyFrame represents a logical plan of operations that will be optimized and executed only when collected.
/// </summary>
and LazyFrame(handle: LazyFrameHandle) =
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
    /// Lazily scan a CSV file into a LazyFrame.
    /// </summary>
    static member ScanCsv(path: string,
                          ?schema: Map<string, DataType>,
                          ?hasHeader: bool,
                          ?separator: char,
                          ?skipRows: int,
                          ?tryParseDates: bool) : LazyFrame =
        
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        let h = PolarsWrapper.ScanCsv(path, schemaDict, header, sep, uint64 skip, dates)
        new LazyFrame(h)
    /// <summary> Scan a parquet file into a LazyFrame. </summary>
    static member ScanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet path)
    /// <summary> Scan a JSON file into a LazyFrame. </summary>
    static member ScanNdjson (path: string) : LazyFrame =
        new LazyFrame(PolarsWrapper.ScanNdjson path)
    /// <summary> Scan an IPC file into a LazyFrame. </summary>
    static member ScanIpc (path: string) = new LazyFrame(PolarsWrapper.ScanIpc path)
    
    // ==========================================
    // Streaming Scan (Lazy)
    // ==========================================

    /// <summary>
    /// Lazily scan a sequence of objects using Apache Arrow Stream Interface.
    /// This supports predicate pushdown and streaming execution.
    /// Data is pulled from the sequence only when needed.
    /// </summary>
    /// <param name="data">The data source sequence.</param>
    /// <param name="batchSize">Rows per Arrow batch (default: 100,000).</param>
    static member scanSeq<'T>(data: seq<'T>, ?batchSize: int) : LazyFrame =
        let size = defaultArg batchSize 100_000

        // 1. 静态推断 Schema (无需触碰数据流)
        // 我们利用 BuildRecordBatch 的反射能力，传入空序列即可得到正确的 Schema
        let dummyBatch = ArrowFfiBridge.BuildRecordBatch Seq.empty<'T>
        let schema = dummyBatch.Schema
        
        // 2. 准备上下文 (Context)
        // 创建 GCHandle，封装数据源和 Schema
        let ctxPtr = ArrowStreamInterop.CreateScanContext(data, size, schema)

        // 3. 准备 C Schema 结构体 (用于传给 Rust 做校验)
        let cSchema = Apache.Arrow.C.CArrowSchema.Create()
        Apache.Arrow.C.CArrowSchemaExporter.ExportSchema(schema, cSchema)

        try
            // 4. 调用 Rust 
            // 传入：Schema, Factory回调, Destroy回调, Context指针
            let handle = PolarsWrapper.LazyFrameScanStream(
                cSchema,
                ArrowStreamInterop.GetFactoryCallback(),
                ArrowStreamInterop.GetDestroyCallback(),
                ctxPtr
            )
            new LazyFrame(handle)
        finally
            // Rust 会复制 Schema，所以我们可以释放这边的 C 结构体
            // 注意：ctxPtr 不能释放，它的所有权已经移交给 Rust，会通过 DestroyCallback 释放
            Apache.Arrow.C.CArrowSchema.Free cSchema

    /// <summary> Write LazyFrame execution result to Parquet (Streaming). </summary>
    member this.SinkParquet (path: string) : unit =
        PolarsWrapper.SinkParquet(this.CloneHandle(), path)
    /// <summary> Write LazyFrame execution result to IPC (Streaming). </summary>
    member this.SinkIpc (path: string) = 
        PolarsWrapper.SinkIpc(this.CloneHandle(), path)
    /// <summary>
    /// Join with another LazyFrame.
    /// </summary>
    member this.Join(other: LazyFrame, leftOn: Expr seq, rightOn: Expr seq, how: PlJoinType) : LazyFrame =
        // 1. 准备 Left On 表达式数组 (Clone Handle, Move 语义)
        let lOnArr = leftOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        // 2. 准备 Right On 表达式数组
        let rOnArr = rightOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        // 3. 准备 LazyFrame Handles
        // Join 算子会消耗左右两个 LazyFrame，所以我们需要传入 Clone 的 Handle
        let lHandle = this.CloneHandle()
        let rHandle = other.CloneHandle()

        // 4. 调用 Wrapper (假设 Wrapper 接受 int 枚举作为 JoinType)
        // 注意：Wrapper 签名通常是 (Left, Right, LeftExprs, RightExprs, JoinType)
        let newHandle = PolarsWrapper.Join(lHandle, rHandle, lOnArr, rOnArr, how)
        
        new LazyFrame(newHandle)
    
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