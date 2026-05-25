#nowarn "3391"
namespace Polars.FSharp

open System
open Polars.NET.Core
open System.Threading.Tasks
open Polars.NET.Core.Helpers

type CjkColumnOptions = {
    Chinese       : bool
    Japanese      : bool
    Korean        : bool
    IncludeDigits : bool
    IncludeLetters: bool
    IgnoreSpaces  : bool
}

/// <summary>
/// Intermediate F# state holder after calling 'pl.when''.
/// </summary>
type FSharpWhen (condition: Expr) =
    member internal _.Condition = condition

/// <summary>
/// Intermediate F# state holder that collects branch pairs.
/// </summary>
type FSharpThen (conditions: Expr list, statements: Expr list) =
    member internal _.Conditions = conditions
    member internal _.Statements = statements

    /// <summary>
    /// Chain another condition block (equivalent to Python/C# multi-branch .when()).
    /// </summary>
    member this.when'(condition: Expr) =
        if box condition = null then raise (ArgumentNullException(nameof(condition)))
        FSharpChainedWhen(this, condition)

    /// <summary>
    /// Terminal operator that provides the default fallback value and compiles to the underlying Ternary Expr tree.
    /// </summary>
    member this.otherwise(statement: Expr) =
        if box statement = null then raise (ArgumentNullException(nameof(statement)))
        
        // Unroll branches in reverse order to correctly nest the Ternary Expression tree
        let mutable currentExpr = statement
        let condArr = List.toArray conditions
        let stmtArr = List.toArray statements
        
        for i = condArr.Length - 1 downto 0 do
            currentExpr <- Expr.Ternary(condArr.[i], stmtArr.[i], currentExpr)
            
        currentExpr

/// <summary>
/// Intermediate F# state holder after chaining an extra .when() onto a Then block.
/// </summary>
and FSharpChainedWhen (parent: FSharpThen, condition: Expr) =
    member this.then'(statement: Expr) =
        if box statement = null then raise (ArgumentNullException(nameof(statement)))
        // Append the new condition and statement to the accumulated branch lists
        FSharpThen(parent.Conditions @ [condition], parent.Statements @ [statement])

/// <summary>
/// The main entry point for Polars.NET F# API.
/// <para>Contains factories for Expressions (pl.col, pl.lit), shortcuts for DataFrame operations, and types.</para>
/// </summary>
module pl =

    /// <summary>   
    /// Create an expression representing a column with the given name.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    let col (name: string) = Expr.Col name
    /// <summary>
    /// Create an expression representing multiple columns (Wildcard).
    /// </summary>
    /// <example>
    /// <code>
    /// pl.cols ["A"; "B"]
    /// </code>
    /// </example>
    let cols (names: seq<string>) =
        Expr.Col names
    /// <summary>
    /// Select all columns.
    /// Equivalent to `pl.col("*")`.
    /// </summary>
    let all() = Expr.Col "*"
    /// <summary>
    /// Return the lines count of current context.
    /// </summary>
    let len = new Expr(PolarsWrapper.Len())
    /// <summary>
    /// Alias for an element being evaluated in an eval or filter expression.
    /// </summary>
    let element = col ""
    
    /// <summary>
    /// Create a literal expression from a value.
    /// <para>Supported types: int, float, bool, string, DateTime,decimal,list,option list,array.</para>
    /// </summary>
    /// <example>
    /// <code>
    /// df.Filter(pl.col("Age") .> pl.lit(18))
    /// </code>
    /// </example>
    let inline lit (value: ^T) : Expr = 
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))
    /// <summary>
    /// Create a literal expression from a Series.
    /// </summary>
    let litSeries (series: Series) = 
        let h = PolarsWrapper.CloneSeries series.Handle 
        new Expr(PolarsWrapper.Lit h)
    // -------------------------------------------------------------------------
    // Struct Literals
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a Struct Expression from a single anonymous record or class instance.
    /// <para>Example: <c>pl.litStruct {| A = 1; B = "hi" |}</c></para>
    /// </summary>
    /// <param name="value">The object to pack into a struct.</param>
    let litStruct (value: 'T when 'T : not struct) =
        // C#'s StructPacker.Pack expects an array
        let sHandle = StructPacker.Pack("literal", [| value |])
        new Expr(PolarsWrapper.Lit sHandle)

    /// <summary>
    /// Create a Struct Expression from a sequence of objects.
    /// <para>The properties of the objects become the fields of the struct.</para>
    /// </summary>
    /// <param name="values">The sequence of objects to pack.</param>
    let litStructs (values: seq<'T>) =
        // Convert to array for the C# StructPacker
        let arr = values |> Seq.toArray
        let sHandle = StructPacker.Pack("literal", arr)
        new Expr(PolarsWrapper.Lit sHandle)
    /// <summary>
    /// Creates a scalar Binary Literal Expression from a byte array.
    /// </summary>
    let litBinary(value:byte[]) =
        lit(value).Implode().Cast(DataType.Binary)
    // --- Aggregation ---
    /// <summary>
    /// Evaluate a bitwise AND operation on the specified columns.
    /// This function is syntactic sugar for col(names).all().
    /// </summary>
    /// <param name="names">The names of the columns to aggregate.</param>
    /// <returns>A boolean aggregation expression.</returns>
    let allOf(names:seq<string>)(ignoreNulls:bool)=
        cols(names).All(ignoreNulls)
    /// <summary>
    /// Compute the logical AND horizontally across columns.
    /// <para>Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.</para>
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation.</param>
    let allHorizontal(exprs:seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.ExprAllHorizontal(handles))
    /// <summary>
    /// Evaluate a bitwise OR operation.
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    /// <param name="ignoreNulls">If set to True (default), null values are ignored. If there are no non-null values, the output is False.
    /// If set to False, Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.</param>
    let any(exprs:seq<string>)(ignoreNulls:bool) =
        cols(exprs).Any(ignoreNulls)
    /// <summary>
    /// Compute the logical OR horizontally across columns.
    /// <para>Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.</para>
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    let anyHorizontal(exprs:seq<Expr>)=
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.ExprAnyHorizontal(handles))
    /// <summary>
    /// Get the maximum value.
    /// Syntactic sugar for Col(names).Max().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    let max(names:seq<string>) =
        cols(names).Max()
    /// <summary>
    /// Get the maximum value horizontally across columns.
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation.</param>
    let maxHorizontal(exprs:seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.ExprMaxHorizontal(handles))
    /// <summary>
    /// Get the Sum value.
    /// Syntactic sugar for Col(names).Sum().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    let sum(names:seq<string>) =
        cols(names).Sum()
    /// <summary>
    /// Sum all values horizontally across columns.
    /// </summary>
    /// <param name="exprs">An iterable of expressions</param>
    /// <param name="ignoreNulls">Whether to ignore null values</param>
    let sumHorizontal(exprs:seq<Expr>)(ignoreNulls:bool)=
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.ExprSumHorizontal(handles,ignoreNulls))
    /// <summary>
    /// Get the Mean value.
    /// Syntactic sugar for Col(names).Mean().
    /// </summary>
    /// <param name="names">Name(s) of the columns to use in the aggregation.</param>
    let mean(names:seq<string>) =
        cols(names).Mean()
    /// <summary>
    /// Compute the mean of all values horizontally across columns.
    /// </summary>
    /// <param name="exprs">Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <param name="ignoreNulls">Ignore null values (default). If set to False, any null value in the input will lead to a null output.</param>
    let meanHorizontal(exprs:seq<Expr>)(ignoreNulls:bool)=
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.ExprMeanHorizontal(handles,ignoreNulls))
    /// <summary>
    /// Get the median value.This function is syntactic sugar for Pl.Col(columns).Median().
    /// </summary>
    /// <param name="names">One or more column names.</param>
    let median(names:seq<string>) =
        cols(names).Median()
    /// <summary>
    /// Return the number of non-null values in the column.
    /// </summary>
    let count(names:seq<string>) =
        cols(names).Count()
   /// <summary>
    /// Cumulatively sum all values.
    /// Syntactic sugar for Col(names).CumSum().
    /// </summary>
    /// <param name="columns">Name(s) of the columns to use in the aggregation.</param>
    let cumSum(names:seq<string>) =
        cols(names).CumSum()
    /// <summary>
    /// Return the cumulative count of the non-null values in the column.This function is syntactic sugar for Col(column).CumCount().
    /// </summary>
    /// <param name="column">Name of the columns to use.</param>
    /// <param name="reverse">reverse the operation</param>
    let cumCount(column:string)(reverse:bool) =
        col(column).CumCount(reverse)
    /// <summary>
    /// Count unique values.This function is syntactic sugar for pl.col(columns).NUnique().
    /// </summary>
    /// <param name="columns">One or more column names.</param>
    let nUnique(names:seq<string>)=
        cols(names).NUnique()
    /// <summary>
    /// Syntactic sugar for pl.Col("foo").Quantile(..).
    /// </summary>
    /// <param name="column">Column name.</param>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method.</param>
    let quantile(column:string)(quantile:float)(interpolation:QuantileMethod) =
        col(column).Quantile(quantile,interpolation)
    /// <summary>
    /// Get the first column.
    /// </summary>
    let firstCol() =
        Selector.ByIndex(ReadOnlySpan<int64> [|0L|],true)
    /// <summary>
    /// Get the first value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the first value.</returns>
    let firstValue(columns:seq<string>,ignoreNulls:bool) =
        cols(columns).First ignoreNulls
    /// <summary>
    /// Get the last column.
    /// </summary>
    let lastCol() =
        Selector.ByIndex(ReadOnlySpan<int64> [|-1L|],true)
    /// <summary>
    /// Get the last value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the first value.</returns>
    let lastValue(columns:seq<string>,ignoreNulls:bool) =
        cols(columns).Last ignoreNulls
    /// <summary>
    /// Get the standard deviation.
    /// This function is syntactic sugar for pl.col(column).Std(ddof).
    /// </summary>
    /// <param name="column">Column name.</param>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    let std(column:string)(ddof:byte) =
        col(column).Std(ddof)
    /// <summary>
    /// Get the variance.
    /// This function is syntactic sugar for Pl.Col(column).Var(ddof).
    /// </summary>
    /// <param name="column">Column name.</param>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    let var(column:string)(ddof:byte) =
        col(column).Var(ddof)
    /// <summary>
    /// Compute the covariance between two columns/ expressions.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    let cov(a:Expr)(b:Expr)(ddof:byte) =
        new Expr(PolarsWrapper.Cov(a.CloneHandle(),b.CloneHandle(),ddof))
    let covAsSeries(a:Expr)(b:Expr)(ddof:byte) =
        Series.ofExpr(cov a b ddof)
    /// <summary>
    /// Compute the Pearson’s correlation between two columns.
    /// </summary>
    let corrPearson(a:Expr)(b:Expr) =
        let ae = a.CloneHandle()
        let be = b.CloneHandle()
        new Expr(PolarsWrapper.PearsonCorr(ae,be))
    let corrPearsonAsSeries(a:Expr)(b:Expr) =
        Series.ofExpr(corrPearson a b)
    /// <summary>
    /// Compute the Spearman rank correlation between two columns.
    /// </summary>
    let corrSpearman (a:Expr)(b:Expr)(propagateNans:bool) =
        let ae = a.CloneHandle()
        let be = b.CloneHandle()
        new Expr(PolarsWrapper.SpearmanRankCorr(ae,be,propagateNans))
    let corrSpearmanAsSeries(a:Expr)(b:Expr)(propagateNans:bool)  =
        Series.ofExpr(corrSpearman a b propagateNans)
    /// <summary>
    /// Compute the rolling correlation between two columns/ expressions.
    /// The window at a given row includes the row itself and the window_size - 1 elements before it.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="windowSize">The length of the window.</param>
    /// <param name="minSamples">The number of values in the window that should be non-null before computing a result. If None, it will be set equal to window size.</param>
    let rollingCorr(a:Expr)(b:Expr)(windowSize:uint)(minSamples:uint option) =
        let min = 
            match minSamples with
            | Some m -> m
            | None -> windowSize
        new Expr(PolarsWrapper.RollingCorr(a.CloneHandle(),b.CloneHandle(),windowSize,min))
    /// <summary>
    /// Compute the rolling covariance between two columns/ expressions.
    /// The window at a given row includes the row itself and the window_size - 1 elements before it.
    /// </summary>
    /// <param name="a">Column name or Expression.</param>
    /// <param name="b">Column name or Expression.</param>
    /// <param name="windowSize">The length of the window.</param>
    /// <param name="minSamples">The number of values in the window that should be non-null before computing a result. If None, it will be set equal to window size.</param>
    /// <param name="ddof">Delta degrees of freedom. The divisor used in calculations is N - ddof, where N represents the number of elements.</param>
    let rollingCov(a:Expr)(b:Expr)(windowSize:uint)(minSamples:uint option)(ddof:byte) =
        let min = 
            match minSamples with
            | Some m -> m
            | None -> windowSize
        new Expr(PolarsWrapper.RollingCov(a.CloneHandle(),b.CloneHandle(),windowSize,min,ddof))
    /// <summary>
    /// Return the row indices that would sort the column(s).
    /// </summary>
    /// <param name="expr">Column(s) to arg sort by. Accepts expression input. Strings are parsed as column names.</param>
    /// <param name="descending">Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans.</param>
    /// <param name="nullsLast">Place null values last.</param>
    /// <param name="multithreaded">Sort using multiple threads.</param>
    /// <param name="maintainOrder">Whether the order should be maintained if elements are equal.</param>
    let argSortBy(exprs:seq<Expr>) (descending:seq<bool>) (nullsLast:seq<bool>) (multithreaded:bool) (maintainOrder:bool) =
        let handles = exprs |> Seq.map (fun e-> e.CloneHandle()) |> Seq.toArray
        let nul = nullsLast |> Seq.toArray
        let des = descending |> Seq.toArray
        new Expr(PolarsWrapper.ArgSortBy(handles,des,nul,multithreaded,maintainOrder))
    /// <summary>
    /// Return indices where condition evaluates True.
    /// </summary>
    /// <param name="condition">Boolean expression/Series to evaluate</param>
    let argWhere(condition:Expr) =
        new Expr(PolarsWrapper.ArgWhere(condition.CloneHandle()))
    let argWhereAsSeries(condition:Expr) = 
        Series.ofExpr(argWhere condition)
    // --- Range ---
    /// <summary>
    /// Generate a range of integers as an Expression.
    /// </summary>
    /// <param name="start">Start of the range (inclusive).</param>
    /// <param name="end">End of the range (exclusive). If set to Null (default), the value of start is used and start is set to 0.</param>
    /// <param name="step">Step size of the range.</param>
    /// <returns>A Literal Expression containing the integer series.</returns>
    let intRange<'T>(start:int64) (endRange:int64) (step:int64) =
        let dtexpr = DataType.FromNetType<'T>().ToDataTypeExpr().handle
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        (new Expr(PolarsWrapper.IntRange(st,ed,step,dtexpr))).SetSorted(step<0)
    let intRangeAsSeries<'T>(name:string) (start:int64) (endRange:int64) (step:int64) =
        let exp = intRange<'T> start endRange step
        Series.ofExpr(exp).Rename(name).SetSorted(step < 0)
    /// <summary>
    /// Generate a range of integers for each row of the input columns.
    /// Resulting column is of dtype List(dtype).
    /// </summary>
    let intRanges<'T>(start:int64)(endRange:int64)(step:int64) =
        let dtexpr = DataType.FromNetType<'T>().ToDataTypeExpr().handle
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let stp = (lit step).Handle
        new Expr(PolarsWrapper.IntRanges(st,ed,stp,dtexpr))
    let intRangesAsSeries<'T>(name:string) (start:int64) (endRange:int64) (step:int64) =
        let expr = intRanges<'T> start endRange step
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Generate a date range.
    /// </summary>
    /// <param name="start">Lower bound of the date range.</param>
    /// <param name="end">Upper bound of the date range.</param>
    /// <param name="interval">Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day.</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <returns>Column of data type Date</returns>
    let dateRange(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedWindow) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.DateRange(st,ed,ine,null,closed.ToNative()))
    let dateRangeAsSeries (name:string)(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedWindow) =
        let expr = dateRange start endRange interval closed
        Series.ofExpr(expr).Rename name
    /// <summary>
    /// Create a column of date ranges. DataType will be a list of dates
    /// </summary>
    /// <param name="start">Lower bound of the date range.</param>
    /// <param name="end">Upper bound of the date range.</param>
    /// <param name="interval">Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day.</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <returns>Column of data type Date</returns>
    let dateRanges(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedWindow) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.DateRanges(st,ed,ine,null,closed.ToNative()))
    let dateRangesAsSeries(name:string)(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedWindow) =
        let expr = dateRanges start endRange interval closed
        Series.ofExpr(expr).Rename name
    /// <summary>
    /// Generate a datetime range.
    /// </summary>
    /// <param name="start">Lower bound of the datetime range.</param>
    /// <param name="end">Upper bound of the datetime range.</param>
    /// <param name="interval">Interval of the range periods</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <param name="unit">Time unit of the resulting Datetime data type.</param>
    /// <param name="timeZone">Time zone of the resulting Datetime data type.</param>
    let datetimeRange(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedWindow)(unit:TimeUnit)(timeZone:string option) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        let tz =
            match timeZone with
            | Some tz -> tz
            | None -> null
        new Expr(PolarsWrapper.DatetimeRange(st,ed,ine,null,closed.ToNative(),unit.ToNative(),tz))
    let datetimeRangeAsSeries(name:string)(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedWindow)(unit:TimeUnit)(timeZone:string option) =
        let expr = datetimeRange start endRange interval closed unit timeZone
        Series.ofExpr(expr).Rename(name)
    let datetimeRanges(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedWindow)(unit:TimeUnit)(timeZone:string option) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        let tz =
            match timeZone with
            | Some tz -> tz
            | None -> null
        new Expr(PolarsWrapper.DatetimeRanges(st,ed,ine,null,closed.ToNative(),unit.ToNative(),tz))
    let datetimeRangesAsSeries(name:string)(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedWindow)(unit:TimeUnit)(timeZone:string option) =
        let expr = datetimeRanges start endRange interval closed unit timeZone
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Generate a time range.
    /// </summary>
    /// <param name="start">Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue</param>
    /// <param name="end">Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue</param>
    /// <param name="interval">Interval of the range periods</param>
    /// <param name="closed">Define which sides of the range are closed.</param>
    let timeRange(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedWindow) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.TimeRange(st,ed,ine,closed.ToNative()))
    let timeRangeAsSeries(name:string)(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedWindow) =
        let expr = timeRange start endRange interval closed
        Series.ofExpr(expr).Rename(name)
    let timeRanges(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedWindow) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.TimeRanges(st,ed,ine,closed.ToNative()))
    let timeRangesAsSeries(name:string)(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedWindow) =
        let expr = timeRanges start endRange interval closed
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Generate a series of equally-spaced points.
    /// </summary>
    /// <param name="start">Lower bound of the linear space.</param>
    /// <param name="end">Upper bound of the linear space.</param>
    /// <param name="numSamples">Number of samples to generate.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    let linearSpace(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedWindow) =
        let st = start.CloneHandle()
        let en = endRange.CloneHandle()
        let nu = (lit numSamples).Handle
        new Expr(PolarsWrapper.LinearSpace(st,en,nu,closed.ToNative()))
    let linearSpaceAsSeries(name:string)(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedWindow) =
        let expr = linearSpace start endRange numSamples closed
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Create a column of linearly-spaced sequences for each row.
    /// </summary>
    /// <param name="start">Lower bound.</param>
    /// <param name="end">Upper bound.</param>
    /// <param name="numSamples">Number of samples.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    /// <param name="asArray">If true, returns an Array dtype instead of List. Requires numSamples to be a constant.</param>
    let linearSpaces(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedWindow)(asArray:bool) =
        let st = start.CloneHandle()
        let en = endRange.CloneHandle()
        let nu = (lit numSamples).Handle
        new Expr(PolarsWrapper.LinearSpaces(st,en,nu,closed.ToNative(),asArray))
    let linearSpacesAsSeries(name:string)(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedWindow)(asArray:bool) =
        let expr = linearSpaces start endRange numSamples closed asArray
        Series.ofExpr(expr).Rename(name)
    // --- Expr Helpers ---
    /// <summary> Cast an expression to a different data type. </summary>
    let cast (dtype: DataType) (e: Expr) = e.Cast dtype
    let castWithNetType<'T> (e: Expr) = e.Cast<'T>()
    let series<'T>(name:string)(data:seq<'T>) =
        Series.create(name,data)
    let dataframe(series:seq<Series>) = DataFrame.create(series)
    /// <summary> Boolean data type. </summary>
    let boolean = DataType.Boolean
    let int8 = DataType.Int8
    let uint8 = DataType.UInt8
    let int16 = DataType.Int16
    let uint16 = DataType.UInt16
    /// <summary> 32-bit Integer data type. </summary>
    let int32 = DataType.Int32
    let uint32 = DataType.UInt32
    let uint64 = DataType.UInt64
    /// <summary> 64-bit Integer data type. </summary>
    let int64 = DataType.Int64
    let int128 = DataType.Int128
    let decimal precision scale = DataType.Decimal(precision,scale)
    let float16 = DataType.Float16
    let float32 = DataType.Float32
    /// <summary> 64-bit Floating point data type. </summary>
    let float64 = DataType.Float64
    /// <summary> String data type (UTF-8). </summary>
    let string = DataType.String
    /// <summary> alias of String</summary>
    let utf8 = string
    /// <summary> Date data type (no time). </summary>
    let date = DataType.Date
    /// <summary> Datetime data type. </summary>
    let datetime(unit:TimeUnit)(timeZone:string option) = 
        DataType.Datetime(unit,?tz=timeZone) 
    /// <summary> Duration (TimeSpan) data type. </summary>
    let duration(unit:TimeUnit) = DataType.Duration unit
    /// <summary> Time data type (no date). </summary>
    let time = DataType.Time
    let list(inner:DataType) = DataType.List inner
    let array(inner:DataType)(shape:uint[]) = DataType.Array(inner,shape)
    let structType(fields:seq<Field>) = DataType.Struct(fields)
    /// <summary>
    /// Create an Extension data type
    /// </summary>
    /// <param name="name">The registered name of the extension type (e.g. "geoarrow.wkb")</param>
    /// <param name="inner">The physical storage data type</param>
    /// <param name="metadata">Optional metadata string</param>
    let extensionType(name:string)(inner:DataType)(metadata:string option) =
        DataType.Extension(name,inner,?metadata=metadata)
    /// <summary>
    /// Register the extension type for the given extension name.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    /// <param name="factory">The factory function deserialize the extension type.</param>
    let registerExtensionType(extName:string)(factory: ExtensionFactory) =
        ExtensionRegistry.Register(extName, ExtensionRegistration.AsType factory)
    /// <summary>
    /// Register the extension type to be passed through purely as physical storage.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    let registerExtensionAsStorage (extName: string) =
        ExtensionRegistry.Register(extName, ExtensionRegistration.AsStorage)
    /// <summary>
    /// Unregister the extension type for the given extension name.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    let unregisterExtensionType(extName:string) = 
        ExtensionRegistry.Unregister extName
    /// <summary>
    /// Get the extension type registration info for the given extension name.
    /// </summary>
    /// <param name="extName">The registered name.</param>
    let tryGetExtensionType (extName: string) : ExtensionRegistration option =
        ExtensionRegistry.TryResolve extName
    let field(name:string)(dtype:DataType) = {Field.Name=name;Field.DataType=dtype}
    let categories(name:string option)(nameSpace:string option)(physical:CategoricalPhysical option) =
        new Categories(?name=name,?nameSpace=nameSpace,?physical=physical)
    let categorical(categories:Categories option) = DataType.Categorical(?categories=categories)
    let enumType(categories:Categories) = DataType.Enum(categories.Freeze())
    let nullType = DataType.Null
    let unknownType = DataType.Unknown
    let sameAsInputType = unknownType
    let schema(fields:seq<Field>) = new PolarsSchema(fields)
    let emptySchema = new PolarsSchema()
    /// <summary>
    /// Gets the DataType of an expression.
    /// Equivalent to Python's polars.dtype_of()
    /// </summary>
    let dataTypeOf(expr:Expr) = new DataTypeExpr(PolarsWrapper.DataTypeExprDtypeOf(expr.CloneHandle()))
    /// <summary>
    /// Represents the intrinsic data type of the current element.
    /// Equivalent to Python's polars.self_dtype()
    /// </summary>
    let selfDataType() = new DataTypeExpr(PolarsWrapper.DataTypeExprSelfDtype())
    /// <summary>
    /// (Lazy) Evaluates the arguments in order and returns the first non-null value.
    /// </summary>
    /// <param name="exprs">Expressions to evaluate. Strings, Literals, Series are automatically converted.</param>
    /// <returns>A new expression.</returns>
    let coalesce(exprs:seq<#IColumnExpr>) =
        let exprHandles = 
            exprs
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle())
            |> Seq.toArray

        new Expr(PolarsWrapper.Coalesce exprHandles)
    /// <summary>
    /// (Eager) Evaluates the arguments eagerly and returns the first non-null value as a Series.
    /// </summary>
    /// <param name="exprs">Expressions or Series to evaluate.</param>
    let coalesceAsSeries(exprs:seq<#IColumnExpr>) =
        Series.ofExpr(coalesce exprs)
    // [Temporal]
    /// <summary>
    /// Combine a Date expression and a Time expression into a Datetime expression.
    /// Usage: pl.col("date") |> pl.combineDateAndTime (pl.col("time"))
    /// </summary>
    let combineDateAndTime (time: Expr) (date: Expr) = date.Dt.Combine time
    /// <summary>
    /// Combine a Date expression and a Time expression with a specific TimeUnit.
    /// Usage: pl.col("date") |> pl.combineDateAndTimeUnit (pl.col("time")) TimeUnit.Milliseconds
    /// </summary>
    let combineDateAndTimeUnit (time: Expr) (tu: TimeUnit) (date: Expr) = date.Dt.Combine(time, tu)
    /// <summary>
    /// Count the number of business days between start and end (not including end).
    /// </summary>
    /// <param name="start">Start dates.</param>
    /// <param name="end">End dates.</param>
    /// <param name="weekMask">Which days of the week to count. The default is Monday to Friday. If you wanted to count only Monday to Thursday, you would pass (True, True, True, True, False, False, False).</param>
    /// <param name="holidays">Holidays to exclude from the count.</param>
    let businessDayCount(start:Expr) (endDay:Expr) (weekMask:seq<bool>) (holidays:Series option) =
        let st = start.CloneHandle()
        let ed = endDay.CloneHandle()
        let wm = weekMask |> Seq.toArray
        let ho = 
            match holidays with
            | Some s -> s.DropNulls().ToPhysical().ToArray<int>()
            | None -> System.Array.Empty<int>()
        new Expr(PolarsWrapper.DtBusinessDayCount(st,ed,wm,ho))
        
    /// <summary> Create a Polars Expr from a SQL string. </summary>
    /// <param name="sql">The SQL expression string.</param>
    /// <returns>A Polars Expr representing the SQL logic.</returns>
    /// <exception cref="T:System.ArgumentException">Thrown when the provided SQL string is null or whitespace.</exception>
    let sqlExpr(sql:string) = Expr.SqlExpr sql
    /// <summary> Create an array of Polars Exprs from a collection of SQL strings. </summary>
    /// <param name="sqls">The collection of SQL expression strings.</param>
    /// <returns>An array of Polars Expr objects.</returns>
    let sqlExprs(sqls: seq<string>) = Expr.SqlExprs sqls
    /// <summary> Alias an expression with a new name. </summary>
    let alias (name: string) (expr: Expr) = expr.Alias name
    /// <summary> Collect LazyFrame into DataFrame (Eager execution). </summary>
    let collect (lf: LazyFrame) : DataFrame = 
        lf.Collect()
    /// <summary>
    /// Collect multiple LazyFrames concurrently.
    /// </summary>
    let collectAll(engine:Engine) (frames:seq<LazyFrame>) : DataFrame[] =
        let lfs = frames |> Seq.toArray
        if lfs.Length = 0 then
            [||]
        else 
            let handles = frames |> Seq.map (fun l -> l.Handle) |> Seq.toArray
            let dfhandles = PolarsWrapper.LazyCollectAll(handles,engine.ToNative())
            dfhandles |> Array.map (fun e-> new DataFrame(e)) 
    /// <summary>
    /// Collect multiple LazyFrames concurrently and asynchronously.
    /// </summary>
    let collectAllAsync (engine: Engine) (frames: seq<LazyFrame>) : Async<DataFrame[]> =
        async {
            let lfs = Seq.toArray frames
            if lfs.Length = 0 then return [||]
            else
                let handles = lfs |> Array.map (fun lf -> lf.Handle)
                let! dfHandles = 
                    PolarsWrapper.LazyCollectAllAsync(handles, engine.ToNative())
                    |> Async.AwaitTask
                return dfHandles |> Array.map (fun h -> new DataFrame(h))
        }
    /// <summary>
    /// Align a sequence of frames using common values from one or more columns as a key.
    /// </summary>
    /// <param name="frames">Sequence of DataFrames or LazyFrames.</param>
    /// <param name="on">One or more columns whose unique values will be used to align the frames.</param>
    /// <param name="how">Join strategy; defaults to outer join.</param>
    /// <param name="select">Optional post-alignment column select.</param>
    /// <param name="descending">Sort the alignment column values in descending order.</param>
    let alignLazyFrames
        (on: seq<Expr>)
        (how: JoinType)
        (select: seq<Expr> option)
        (descending: bool)
        (frames: seq<LazyFrame>)
        : LazyFrame[] =
        let lfs = Seq.toArray frames

        if lfs.Length = 0 then [||]
        elif lfs.Length = 1 then lfs
        else
            // Expressions used for alignment (cloned for repeated use)
            let baseAlignExprs = on |> Seq.map (fun e -> e.Clone()) |> Seq.toArray

            // Build a single joined frame incorporating all input frames with index suffix
            let seed = lfs.[0]
            let onCol = baseAlignExprs |> Array.map (fun e -> e.Clone())
            let joinedFrame =
                (seed, lfs |> Array.skip 1 |> Array.mapi (fun i lf -> (i + 1, lf)))
                ||> Array.fold (fun acc (idx, lf) ->
                    acc.Join(
                        lf,
                        onCol,
                        how = how,
                        suffix = $":{idx}",
                        nullsEqual = true,
                        coalesce = JoinCoalesce.CoalesceColumns
                    ))

            // Sort the joined frame by the key columns
            let joinedFrame =
                joinedFrame.Sort(
                    columns = (baseAlignExprs |> Array.map (fun e -> e.Clone())),
                    descending = descending,
                    nullsLast = false,
                    maintainOrder = true
                )

            // Schema of the joined frame to determine which columns are suffixed
            let masterSchemaCols = joinedFrame.CollectSchema().Names |> Set.ofSeq

            // For each original frame, extract the relevant columns (renaming suffixed back)
            let selectExprs =
                match select with
                | Some cols -> cols |> Seq.map (fun e -> e.Clone()) |> Seq.toArray
                | None -> Array.empty

            let alignedFrames =
                lfs
                |> Array.mapi (fun i lf ->
                    let sfx = $":{i}"
                    let currentNames = lf.CollectSchema().Names

                    let componentCols =
                        currentNames
                        |> Seq.map (fun colName ->
                            let suffixedCol = $"{colName}{sfx}"
                            if masterSchemaCols.Contains suffixedCol then
                                col suffixedCol |> alias(colName)
                            else
                                col colName)
                        |> Seq.toArray

                    let mutable aligned = joinedFrame.Select componentCols
                    if selectExprs.Length > 0 then
                        aligned <- aligned.Select(selectExprs)
                    aligned)

            alignedFrames
    let alignDataFrames
        (on: seq<Expr>)
        (how: JoinType)
        (select: seq<Expr> option)
        (descending: bool) 
        (frames: seq<DataFrame>) =
        frames 
        |> Seq.map (fun df -> df.Lazy()) 
        |> alignLazyFrames on how select descending 
        |> collectAll Engine.Auto
    let alignDataFramesAsync
        (on: seq<Expr>)
        (how: JoinType)
        (select: seq<Expr> option)
        (descending: bool) 
        (frames: seq<DataFrame>) =
        frames
        |> Seq.map (fun df -> df.Lazy())
        |> alignLazyFrames on how select descending
        |> collectAllAsync Engine.Auto
    /// <summary>
    /// Explain multiple LazyFrames as if passed to collect_all.
    /// Common Subplan Elimination is applied on the combined plan, meaning that diverging queries will run only once.
    /// </summary>
    /// <param name="lazyFrames">A list of LazyFrames to collect.</param>
    /// <returns>Explained plan.</returns>
    let explainAll(lazyFrames:seq<LazyFrame>) =
        PolarsWrapper.LazyExplainAll(
            lazyFrames 
            |> Seq.map (fun e->e.CloneHandle()) 
            |> Seq.toArray
        )
    /// <summary>
    /// Run polars expressions without a context.
    /// This is syntactic sugar for running lf.select on an empty LazyFrame.
    /// </summary>
    let selectExprsLazy (exprs: seq<Expr>) : LazyFrame =
        (DataFrame.create()).Lazy().Select exprs
    /// <summary>
    /// Run polars expressions without a context.
    /// This is syntactic sugar for running df.select on an empty DataFrame.
    /// </summary>
    let selectExprsEager (exprs: seq<Expr>) : DataFrame =
        (DataFrame.create()).Select exprs
    /// <summary> Convert Selector to Expr. </summary>
    let asExpr (s: Selector) = s.ToExpr()
    /// <summary>
    /// Represent all columns except for the given columns.
    /// Syntactic sugar for pl.all().Exclude(columns).
    /// </summary>
    let excludeCols (columns:seq<string>) =
        all().Exclude(columns)
    /// <summary>
    /// Represent all columns except for the given datatypes.
    /// Syntactic sugar for pl.all().Exclude(dtypes).
    /// </summary>
    let excludeDataTypes(dtypes:seq<DataType>) =
        all().Exclude(dtypes)
    /// <summary> Exclude columns from Selector. </summary>
    let exclude (names: seq<string>) (s: Selector) = s.Exclude names
    /// <summary>
    /// Aggregate all column values into a list.
    /// This function is syntactic sugar for pl.col(name).Implode().
    /// </summary>
    /// <param name="column">Column name</param>
    /// <param name="maintainOrder">Whether to preserve the order of elements in the list. Setting this to False can improve performance, especially within GroupBy.</param>
    let implode (column:string) (maintainOrder:bool) = col(column).Implode()
    /// <summary>
    /// Get the nth column(s) of the context.
    /// </summary>
    /// <param name="indices">One or more indices representing the columns to retrieve.</param>
    /// <param name="strict">By default, all specified indices must be valid; if any index is out of bounds, an error is raised. If set to False, out-of-bounds indices are ignored.</param>
    let nth (indices:seq<int64>) (strict:bool) = 
        let indSpan = ReadOnlySpan<int64> (indices |> Seq.toArray)
        Selector.ByIndex(indSpan,strict)
    /// <summary>
    /// Escapes string regex meta characters.
    /// </summary>
    /// <param name="s">The string to escape.</param>
    /// <returns>A string with regex meta characters escaped.</returns>
    let escapeRegex(s:string) =
        if String.IsNullOrEmpty s then
            s
        else System.Text.RegularExpressions.Regex.Escape(s)
    /// <summary>
    /// Construct a column of length n filled with the given value.
    /// </summary>
    /// <param name="value">Value to repeat.</param>
    /// <param name="n">Length of the resulting column.</param>
    /// <param name="dtype">Data type of the resulting column. If set to None (default), data type is inferred from the given value. 
    /// Defaults to Int32 for integer values, unless Int64 is required to fit the given value. Defaults to Float64 for float values.</param>
    let repeat(value:Expr)(n:int)(dtype:DataType option) =
        let va = value.CloneHandle()
        let nh = (lit n).Handle
        let expr = new Expr(PolarsWrapper.ExprRepeat(va,nh))
        match dtype with
        | Some d -> expr.Cast d
        | None -> expr
    let repeatAsSeries(value:Expr)(n:int)(dtype:DataType option) =
        Series.ofExpr(repeat value n dtype)
    /// <summary>
    /// Construct a column of length n filled with zeros.
    /// This is syntactic sugar for the repeat function.
    /// </summary>
    /// <param name="n">Length of the resulting column.</param>
    /// <param name="dtype">Data type of the resulting column. Defaults to Float64.</param>
    let zeros(n:int) (dtype:DataType option) =
        match dtype with
        | Some d -> repeat (lit 0) n (Some d)
        | None -> repeat (lit 0) n (Some DataType.Float64)
    let zerosAsSeries (n:int) (dtype:DataType option) =
        Series.ofExpr(zeros n dtype)
    /// <summary>
    /// Construct a column of length n filled with ones.
    /// This is syntactic sugar for the repeat function.
    /// </summary>
    /// <param name="n">Length of the resulting column.</param>
    /// <param name="dtype">Data type of the resulting column. Defaults to Float64.</param>
    let ones(n:int) (dtype:DataType option) =
        match dtype with
        | Some d -> repeat (lit 1) n (Some d)
        | None -> repeat (lit 1) n (Some DataType.Float64)
    let onesAsSeries (n:int) (dtype:DataType option) =
        Series.ofExpr(ones n dtype)
    /// <summary>
    /// Parses an integer column (seconds, milliseconds, etc.) into a Datetime or Date expression.
    /// </summary>
    let fromEpoch(column:Expr) (timeUnit:EpochTimeUnit) =
        match timeUnit with
        | EpochTimeUnit.Day -> column |> castWithNetType<DateOnly>
        | EpochTimeUnit.Second -> column * lit 1_000_000L |> cast(datetime TimeUnit.Microseconds None)
        | EpochTimeUnit.Milliseconds -> column * lit 1_000L |> cast(datetime TimeUnit.Microseconds None)
        | EpochTimeUnit.Microseconds -> column |> cast(datetime TimeUnit.Microseconds None)
        | EpochTimeUnit.Nanoseconds -> column |> cast(datetime TimeUnit.Nanoseconds None)
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    let asStruct (exprs: seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.AsStruct handles)
    /// <summary>
    /// Collect several expressions and combine them into a single Struct Series
    /// </summary>
    let structSeries(exprs: seq<Expr>) =
        Series.ofExpr(asStruct exprs)
    // --- Eager Ops ---
    /// <summary> Add or replace a single column in the DataFrame. </summary>
    let withColumn (expr: Expr) (df: DataFrame) : DataFrame =
        df.WithColumns expr
    /// <summary> Add or replace multiple columns in the DataFrame. </summary>
    let withColumns (exprs: seq<Expr>) (df: DataFrame) : DataFrame =
        df.WithColumns exprs

    /// <summary> Filter rows based on a boolean expression. </summary>
    let filter (expr: IColumnExpr) (df: DataFrame) : DataFrame =
        df.Filter (expr.ToExprs().[0]) 
    /// <summary> Select columns from the DataFrame. </summary>
    let select (exprs: seq<#IColumnExpr>) (df: DataFrame) : DataFrame =
        df.Select exprs
    /// <summary> Sort (Order By) the DataFrame. </summary>
    let sort (columns:seq<IColumnExpr>)(desc: bool)(df: DataFrame) : DataFrame =
        df.Sort(columns,descending=desc)
    let orderBy (columns: seq<IColumnExpr>) (desc: bool) (df: DataFrame) = sort columns desc df
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupBy (keys: seq<Expr>)(df: DataFrame) : GroupBy =
        df.GroupBy(keys)
    let having(predicate: Expr) (builder: GroupBy) = builder.Having(predicate)
    let agg(aggs:seq<Expr>)(builder: GroupBy) = builder.Agg(aggs)
    /// <summary> Perform a join between two DataFrames. </summary>
    let join (other: DataFrame) (leftOn: seq<Expr>) (rightOn:seq<Expr>) (how: JoinType) (left: DataFrame) : DataFrame =
        left.Join (other, leftOn, rightOn, how)
    let joinOn(other: DataFrame) (on:seq<Expr>) (how: JoinType) (left: DataFrame) : DataFrame =
        left.Join(other,on,how)
    /// <summary>
    /// Vertically concat DataFrames (Standard concat).
    /// </summary>
    let concat (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatVertical dfs

    /// <summary>
    /// Horizontally concat DataFrames.
    /// </summary>
    let concatHorizontal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatHorizontal(dfs, checkDuplicates=true)

    /// <summary>
    /// Horizontally concat DataFrames (Allow duplicates).
    /// </summary>
    let concatHorizontalNoCheck (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatHorizontal(dfs, checkDuplicates=false)
    /// <summary>
    /// Diagonally concat DataFrames
    /// </summary>
    let concatDiagonal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.ConcatDiagonal dfs
    /// <summary>
    /// Combine multiple expressions horizontally into a List element.
    /// Supports Selectors (e.g. pl.concatList([pl.cs.numeric()])).
    /// </summary>
    let concatList (columns: seq<#IColumnExpr>) =
        let exprHandles = 
            columns
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle())
            |> Seq.toArray

        new Expr(PolarsWrapper.ConcatList exprHandles)
    /// <summary>
    /// Combine multiple expressions horizontally into an array element.
    /// </summary>
    let concatArray (columns: seq<#IColumnExpr>) =
        let exprHandles = 
            columns
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle())
            |> Seq.toArray

        new Expr(PolarsWrapper.ConcatArray exprHandles)
    /// <summary>
    /// Horizontally concatenate columns into a single string column.
    /// </summary>
    /// <param name="separator">String that will be used to separate the values of each column.</param>
    /// <param name="ignoreNulls">Ignore null values.
    /// If set to False, null values will be propagated. if the row contains any null values, the output is null.</param>
    /// <param name="exprs">Columns to concatenate into a single string column.</param>
    let concatString (exprs: seq<#IColumnExpr>)(separator:string)(ignoreNulls:bool) =
        let handles =
            exprs
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle())
            |> Seq.toArray
        new Expr(PolarsWrapper.ConcatString(handles,separator,ignoreNulls))
    /// <summary>
    /// Concat multiple expressions into a single expression.
    /// </summary>
    let concatExpr(exprs:seq<#IColumnExpr>)(rechunk:bool) =
        let handles =
            exprs
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle())
            |> Seq.toArray
        new Expr(PolarsWrapper.ConcatExprs(handles,rechunk))
    /// <summary>
    /// Format expressions as a string.
    /// </summary>
    let format (format:string)(exprs:seq<#IColumnExpr>) =
        let handles =
            exprs
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle())
            |> Seq.toArray
        new Expr(PolarsWrapper.FormatString(format,handles))
    /// <summary> Get the first n rows of the DataFrame. </summary>
    let head (n: int) (df: DataFrame) : DataFrame =
        df.Head n
    /// <summary> Get the last n rows of the DataFrame. </summary>
    let tail (n: int) (df: DataFrame) : DataFrame =
        df.Tail n
    /// <summary> Explode list-like columns into multiple rows. </summary>
    let explode (columns: seq<string>) (df: DataFrame) : DataFrame =
        df.Explode columns
    /// <summary> Decompose multiple struct columns. </summary>
    let unnestColumns(columns: seq<string>) (df:DataFrame) : DataFrame =
        df.UnnestColumns columns
    let unnestColumnsLazy(columns: seq<string>) (lf:LazyFrame) =
        lf.Unnest columns
    // --- Reshaping (Eager) ---

    /// <summary> Pivot the DataFrame from long to wide format. </summary>
    let pivot (index: string list) (columns: string list) (values: string list) (aggFn: PivotAgg) (df: DataFrame) : DataFrame =
        df.Pivot(index,columns,values,aggFn)

    /// <summary>
    /// Unpivot (Melt) the DataFrame.
    /// Supports pipelining: df |> Frame.unpivot ...
    /// </summary>
    let unpivot (index: seq<string>) (on: seq<string>) (variableName: string option) (valueName: string option) (df: DataFrame) =
        df.Unpivot(index, on, variableName, valueName)
    /// <summary>
    /// Unpivot (Melt) the DataFrame by selector.
    /// Supports pipelining: df |> Frame.unpivot ...
    /// </summary>
    let unpivotSel (index: Selector) (on: Selector) (variableName: string option) (valueName: string option) (df: DataFrame) =
        df.Unpivot(index, on, variableName, valueName)
    /// Alias for unpivot
    let melt = unpivot    
    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// </summary>
    let hstack (columns: Series list) (df: DataFrame) : DataFrame =
        df.HStack columns

    /// <summary>
    /// Vertically stack another DataFrame to this one.
    /// </summary>
    let vstack (other: DataFrame) (df: DataFrame) : DataFrame =
        df.VStack other
    // Fill Helpers
    /// <summary> Fill null values with a specific value. </summary>
    let fillNull (fillValue: Expr) (e: Expr) = e.FillNull fillValue
    /// <summary> Check for null values. </summary>
    let isNull (e: Expr) = e.IsNull()
    /// <summary> Check for non-null values. </summary>
    let isNotNull (e: Expr) = e.IsNotNull()
    // unique and duplicated helpers
    /// <summary> Get unique values. </summary>
    let inline unique (e: Expr) = e.Unique()
    /// <summary> Check if values are unique. </summary>
    let inline isUnique (e: Expr) = e.IsUnique()
    /// <summary> Check if values are duplicated. </summary>
    let inline isDuplicated (e: Expr) = e.IsDuplicated()
    // Math Helpers
    /// <summary> Absolute value. </summary>
    let abs (e: Expr) = e.Abs()
    /// <summary> Power. </summary>
    let pow (exponent: Expr) (baseExpr: Expr) = baseExpr.Pow exponent
    /// <summary> Square root. </summary>
    let sqrt (e: Expr) = e.Sqrt()
    /// <summary> Exponential (e^x). </summary>
    let exp (e: Expr) = e.Exp()
    /// <summary> True division. </summary>
    let inline truediv (other: Expr) (e: Expr) = e.Truediv other
    /// <summary> Floor division (integer result). </summary>
    let inline floorDiv (other: Expr) (e: Expr) = e.FloorDiv other
    /// <summary> Modulo (remainder). </summary>
    let inline mod_ (other: Expr) (e: Expr) = e.Mod other
    /// <summary> Cube root. </summary>
    let inline cbrt (e: Expr) = e.Cbrt()
    /// <summary> Sign of the value (-1, 0, 1). </summary>
    let inline sign (e: Expr) = e.Sign()
    /// <summary> Ceiling (round up). </summary>
    let inline ceil (e: Expr) = e.Ceil()
    /// <summary> Floor (round down). </summary>
    let inline floor (e: Expr) = e.Floor()

    // Trig
    let inline sin (e: Expr) = e.Sin()
    let inline cos (e: Expr) = e.Cos()
    let inline tan (e: Expr) = e.Tan()
    let inline cot (e:Expr) = e.Cot()
    let inline arcsin (e: Expr) = e.ArcSin()
    let inline arccos (e: Expr) = e.ArcCos()
    let inline arctan (e: Expr) = e.ArcTan()
    
    // Hyperbolic
    let inline sinh (e: Expr) = e.Sinh()
    let inline cosh (e: Expr) = e.Cosh()
    let inline tanh (e: Expr) = e.Tanh()
    let inline arcsinh (e: Expr) = e.ArcSinh()
    let inline arccosh (e: Expr) = e.ArcCosh()
    let inline arctanh (e: Expr) = e.ArcTanh()
    /// <summary>
    /// Compute two argument arctan in radians.
    /// Returns the angle (in radians) in the plane between the positive x-axis and the ray from the origin to (x,y).
    /// </summary>
    let arctan2(y:Expr) (x:Expr) = new Expr(PolarsWrapper.ArcTan2(y.CloneHandle(),x.CloneHandle()))
    
    // --- Lazy API ---
    let asLazy(df:DataFrame) = df.Lazy()
    /// <summary> Explain the LazyFrame execution plan. </summary>
    let explain (lf: LazyFrame) = lf.Explain true
    /// <summary> Explain the unoptimized LazyFrame execution plan. </summary>
    let explainUnoptimized (lf: LazyFrame) = lf.Explain false
    /// <summary> Get the schema of the LazyFrame. </summary>
    let collectSchema (lf: LazyFrame) = lf.Schema
    /// <summary> Filter rows based on a boolean expression. </summary>
    let filterLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        lf.Filter expr
    /// <summary> Select columns from LazyFrame. </summary>
    let selectLazy (exprs: seq<Expr>) (lf: LazyFrame) : LazyFrame =
        lf.Select exprs
    /// <summary> Sort (Order By) the LazyFrame. </summary>
    let sortLazy (exprs: seq<Expr>) (desc: bool) (lf: LazyFrame) : LazyFrame =
        lf.Sort (exprs,desc)
    /// <summary> Alias for sortLazy </summary>
    let orderByLazy (expr: seq<Expr>) (desc: bool) (lf: LazyFrame) = sortLazy expr desc lf
    /// <summary> Add or replace columns in the LazyFrame. </summary>
    let withColumnLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        lf.WithColumns expr
    /// <summary> Add or replace multiple columns in the LazyFrame. </summary>
    let withColumnsLazy (exprs: seq<Expr>) (lf: LazyFrame) : LazyFrame =
        lf.WithColumns exprs
    /// <summary> Group by keys. </summary>
    let groupByLazy (keys: seq<Expr>)(lf: LazyFrame) :LazyGroupBy =
        lf.GroupBy(keys)
    let havingLazy (predicate: Expr) (builder: LazyGroupBy) = builder.Having(predicate)
    let aggLazy (aggs: seq<Expr>) (builder: LazyGroupBy) = builder.Agg(aggs)
    /// <summary>
    /// Unpivot (Melt) the LazyFrame.
    /// Usage: lf |> LazyFrame.unpivot ["ID"] ["Val"] None None
    /// </summary>
    let unpivotLazy (index: seq<string>) (on: seq<string>) (variableName: string option) (valueName: string option) (lf: LazyFrame) : LazyFrame =
        lf.Unpivot(index, on, variableName, valueName)
    /// <summary>
    /// Unpivot (Melt) the LazyFrame by selector.
    /// Usage: lf |> LazyFrame.unpivot ["ID"] ["Val"] None None
    /// </summary>
    let unpivotLazySel (index: Selector) (on: Selector) (variableName: string option) (valueName: string option) (lf: LazyFrame) : LazyFrame =
        lf.Unpivot(index, on, variableName, valueName)
    /// Alias for unpivotLazy
    let meltLazy = unpivotLazy
    /// <summary> Perform a join between two LazyFrames. </summary>
    let joinOnLazy (other: LazyFrame) (on: Expr seq) (how: JoinType) (lf: LazyFrame) : LazyFrame =
        lf.Join(other,on,how)
    let joinLazy(other:LazyFrame)(leftOn:Expr seq)(rightOn: Expr seq)(how: JoinType) (lf: LazyFrame) : LazyFrame =
        lf.Join(other,leftOn,rightOn,how)
    /// <summary> Concatenate multiple LazyFrames. </summary>
    let concatLazy (lfs: LazyFrame list) (how: ConcatType) : LazyFrame =
        LazyFrame.Concat(lfs,how)
    /// <summary> Define a window over which to perform an aggregation. </summary>
    let over (partitionBy: Expr seq) (e: Expr) = e.Over partitionBy
    /// <summary> Create a SQL context for executing SQL queries on LazyFrames. </summary>
    let sqlContext () = new SqlContext()
    let ifElse (predicate: Expr) (ifTrue: Expr) (ifFalse: Expr) : Expr =
        let p = predicate.CloneHandle()
        let t = ifTrue.CloneHandle()
        let f = ifFalse.CloneHandle()
        
        new Expr(PolarsWrapper.IfElse(p, t, f))


    // --- Async Execution ---

    /// <summary> 
    /// Asynchronously execute the LazyFrame query plan. 
    /// Useful for keeping UI responsive during heavy calculations.
    /// </summary>
    let collectAsync (lf: LazyFrame) : Async<DataFrame> =
        async {
            let lfClone = lf.CloneHandle()
            
            let! dfHandle = 
                Task.Run(fun () -> PolarsWrapper.LazyCollect(lfClone,PlEngine.Auto,true)) 
                |> Async.AwaitTask
                
            return new DataFrame(dfHandle)
        }
    /// --- Config ---
    let setEnvVar (key:string) (value:string) = 
        PolarsWrapper.SetEnvVar(key,value)
    let setEnvVarPrefixKey suffix value =
        setEnvVar ("POLARS_" + suffix) value
    let setEnvVarAll vars =
        vars |> Seq.iter (fun (k, v) -> PolarsWrapper.SetEnvVar(k, v))
    
    /// <summary> Accumulate over multiple columns horizontally/row-wise. </summary>
    let fold (f: Expr -> Expr -> Expr) (acc: Expr) (exprs: seq<Expr>) : Expr =
        Seq.fold f acc exprs

    /// <summary> Reduce multiple columns horizontally/row-wise. </summary>
    let reduce (f: Expr -> Expr -> Expr) (exprs: seq<Expr>) : Expr =
        Seq.reduce f exprs

    /// <summary>
    /// Print the DataFrame to Console (Table format).
    /// </summary>
    let show (df: DataFrame) : DataFrame =
        df.Show()
        df

    /// <summary>
    /// Print the Series to Console.
    /// </summary>
    let showSeries (s: Series) : Series =
        s.Show()
        s
    /// <summary>
    /// Starts a conditional when-then-otherwise expression branch logic natively in F#.
    /// </summary>
    /// <param name="condition">The initial filter condition expression.</param>
    /// <returns>An intermediate FSharpWhen state object.</returns>
    let when' (condition: Expr) =
        if box condition = null then raise (ArgumentNullException(nameof(condition)))
        FSharpWhen(condition)

    /// <summary>
    /// Connects a statement to the preceding when' condition.
    /// </summary>
    /// <param name="statement">The expression to evaluate if the condition is true.</param>
    /// <param name="whenBlock">The FSharpWhen block built by pl.when'.</param>
    /// <returns>A new FSharpThen collector block.</returns>
    let then' (statement: Expr) (whenBlock: FSharpWhen) =
        if box statement = null then raise (ArgumentNullException(nameof(statement)))
        if box whenBlock = null then raise (ArgumentNullException(nameof(whenBlock)))
        FSharpThen([whenBlock.Condition], [statement])
    // ==========================================
    // Column Selectors (pl.cs)
    // ==========================================
    module cs =

        /// <summary>
        /// Select a single column by name.
        /// </summary>
        let inline byName (name: string) =
            new Selector(PolarsWrapper.SelectorCols [| name |])

        /// <summary>
        /// Select columns by their index with strictness control.
        /// </summary>
        let inline byIndex(indices:ReadOnlySpan<int64>) (strict:bool) = new Selector(PolarsWrapper.SelectorByIndex(indices, strict))
        /// <summary>
        /// Select columns by their index. 
        /// Usage: cs.byIndex(0L, 2L, 4L)
        /// </summary>
        let inline byIndexStrict(indices:ReadOnlySpan<int64>) = byIndex indices true
        
        /// <summary> Select all columns. </summary>
        let inline all () = 
            new Selector(PolarsWrapper.SelectorAll())
        /// <summary>
        /// Select all columns EXCEPT the specified Selectors.
        /// </summary>
        let exclude(selectors:seq<Selector>) = all().Exclude selectors

        /// <summary>
        /// Select all columns EXCEPT the specified Data Types.
        /// </summary>
        let excludeDataType(dtypes:seq<DataType>) = all().Exclude dtypes
        
        /// <summary> Select columns by DataType. </summary>
        let inline byType (dt: DataType) = 
            let code = dt.Code
            let kind = enum<PlDataType> code
            
            new Selector(PolarsWrapper.SelectorByDtype kind)
        /// <summary> 
        /// Select columns by Generic Type.
        /// Usage: pl.cs.byType<int option>() or pl.cs.byType<DateTime>()
        /// </summary>
        let inline byNetType<'T> () =
            let dtype = DataType.FromNetType<'T>()
            byType dtype

        /// <summary> Select columns starting with a pattern. </summary>
        let inline startsWith (pattern: string) = 
            new Selector(PolarsWrapper.SelectorStartsWith pattern)
        
        /// <summary> Select columns ending with a pattern. </summary>
        let inline endsWith (pattern: string) = 
            new Selector(PolarsWrapper.SelectorEndsWith pattern)
        
        /// <summary> Select columns containing a pattern. </summary>
        let inline contains (pattern: string) = 
            new Selector(PolarsWrapper.SelectorContains pattern)
        
        /// <summary> Select columns matching a regex pattern. </summary>
        let inline matches (regex: string) = 
            new Selector(PolarsWrapper.SelectorMatch regex)

        /// <summary>
        /// Select the first column.
        /// </summary>
        let first() = byIndex ([|0L|].AsSpan())

        /// <summary>
        /// Select the last column.
        /// </summary>
        let last() = byIndex ([|-1L|].AsSpan())
        /// <summary> Select numeric columns (Int, Float, Decimal). </summary>
        let inline numeric() = 
            new Selector(PolarsWrapper.SelectorNumeric())
        /// <summary> Select string columns.</summary>
        let inline string() = byType DataType.String 

        let inline date() = new Selector(PolarsWrapper.SelectorByDtype(PlDataType.Date));
        let inline boolean() = new Selector(PolarsWrapper.SelectorByDtype(PlDataType.Boolean));
        let inline binary() = byType DataType.Binary
        let inline empty() = new Selector(PolarsWrapper.SelectorEmpty());
        let inline integer() = new Selector(PolarsWrapper.SelectorInteger());
        let inline unsignedInteger() = new Selector(PolarsWrapper.SelectorUnsignedInteger());
        let inline signedInteger() = new Selector(PolarsWrapper.SelectorSignedInteger());
        let inline float() = Selector.Float();
        let inline decimal() = new Selector(PolarsWrapper.SelectorDecimal());
        let inline enum() = new Selector(PolarsWrapper.SelectorEnum());
        let inline nested() = new Selector(PolarsWrapper.SelectorNested());
        let inline structType() = new Selector(PolarsWrapper.SelectorStruct());
        let inline temporal() = new Selector(PolarsWrapper.SelectorTemporal());
        /// <summary>
        /// Select list columns. Optionally filter by the inner data type.
        /// Example: pl.cs.list(Some(pl.cs.numeric()))
        /// </summary>
        let list (inner: Selector option) =
            let innerHandle = 
                match inner with
                | Some s -> s.CloneHandle()
                | None -> null
                
            new Selector(PolarsWrapper.SelectorList innerHandle)
        /// <summary>
        /// Select all list columns.
        /// </summary>
        let listAll() = list None
        let private getNativeTimeUnit (unit: TimeUnit option) : PlTimeUnit =
            match unit with
            | Some u -> u.ToNative()
            | None -> PlTimeUnit.All

        let private datetimeInternal (timeUnit: TimeUnit option) (tzString: string option) =
            let tu = getNativeTimeUnit timeUnit
            let tz = 
                match tzString with
                | Some t -> t
                | None -> null
            new Selector(PolarsWrapper.SelectorDatetime(tu, tz))
        /// <summary>
        /// Select array columns. Optionally filter by inner data type and fixed width.
        /// Example: pl.cs.array (Some(pl.cs.numeric())) (Some 3L)
        /// </summary>
        let array (inner: Selector option) (width: int64 option) =
            let innerHandle = 
                match inner with
                | Some s -> s.CloneHandle()
                | None -> null
            let w = Option.toNullable width
            new Selector(PolarsWrapper.SelectorArray(innerHandle, w))
        /// <summary>
        /// Select all array columns.
        /// </summary>
        let arrayAll() = array None None
        /// <summary>
        /// Select all datetime columns (both with and without timezones).
        /// </summary>
        let datetime (timeUnit: TimeUnit) =
            datetimeInternal (Some timeUnit) None

        /// <summary>
        /// Select ONLY timezone-naive datetime columns (no timezone set).
        /// </summary>
        let datetimeNaive (timeUnit: TimeUnit) =
            datetimeInternal (Some timeUnit) (Some "")

        /// <summary>
        /// Select ONLY timezone-aware datetime columns (any timezone).
        /// </summary>
        let datetimeAware (timeUnit: TimeUnit) =
            datetimeInternal (Some timeUnit) (Some "*")

        /// <summary>
        /// Select datetime columns matching a specific timezone (e.g., "UTC", "Asia/Shanghai").
        /// </summary>
        let datetimeExact (timeZone: string) (timeUnit: TimeUnit) =
            if System.String.IsNullOrEmpty timeZone then
                invalidArg "timeZone" "timeZone cannot be null or empty"
                
            datetimeInternal (Some timeUnit) (Some timeZone)
        /// <summary>
        /// Select all duration columns. Optionally match a specific TimeUnit.
        /// </summary>
        let duration (timeUnit: TimeUnit) =
            new Selector(PolarsWrapper.SelectorDuration(getNativeTimeUnit (Some timeUnit)))
        /// <summary>
        /// Select all columns with alphabetic names.
        /// </summary>
        let alpha (asciiOnly: bool) (ignoreSpaces: bool ) =
            let mutable charClass = if asciiOnly then "a-zA-Z" else @"\p{L}"
            if ignoreSpaces then charClass <- charClass + " "
            
            matches (sprintf "^[%s]+$" charClass)

        /// <summary>
        /// <para>[EN] Select columns whose names consist of CJK scripts, Unicode digits (\p{N}),
        /// and optionally ASCII/full-width Latin letters.</para>
        /// <para>[ZH] 选择列名由中日韩字符、数字（\p{N}，含全/半角）以及可选英文字母（全/半角）组成的列。</para>
        /// <para>[JA] 列名がCJK文字・数字（\p{N}、全角/半角）および英字（全角/半角）で構成される列を選択します。</para>
        /// <para>[KO] 열 이름이 CJK 문자, 숫자(\p{N}, 전각/반각) 및 영문자(전각/반각)로 구성된 열을 선택합니다.</para>
        /// </summary>
        let cjkCols (opts: CjkColumnOptions) =
            if not opts.Chinese && not opts.Japanese && not opts.Korean then
                invalidArg (nameof opts) "At least one CJK script must be enabled."

            let charClass = ResizeArray<string>()

            if opts.IncludeDigits then
                charClass.Add @"\p{N}"

            if opts.IncludeLetters then
                charClass.Add @"a-zA-ZＡ-Ｚａ-ｚ"

            if opts.Chinese then charClass.Add @"\p{Han}"
            if opts.Japanese then charClass.Add @"\p{Hiragana}\p{Katakana}"
            if opts.Korean then charClass.Add @"\p{Hangul}"
            if opts.IgnoreSpaces then charClass.Add " "

            let pattern = sprintf "^[%s]+$" (String.concat "" charClass)
            matches pattern 
        /// <summary>
        /// Select all columns with alphanumeric names.
        /// </summary>
        let alphanumeric (asciiOnly: bool ) (ignoreSpaces: bool) =
            
            let mutable charClass = if asciiOnly then "a-zA-Z0-9" else @"\p{L}\p{N}"
            if ignoreSpaces then charClass <- charClass + " "

            matches (sprintf "^[%s]+$" charClass)
        /// <summary>
        /// Expand a Selector against a DataFrame to get the matched column names.
        /// </summary>
        let expandDf (selector: Selector) (target: DataFrame) : string array =
            use emptyDf = target.Clear()
            use result = emptyDf.Select [selector]
            result.Columns |> Seq.toArray

        /// <summary>
        /// Expand an Expr against a DataFrame to get the matched column names.
        /// </summary>
        let expandDfExpr (expr: Expr) (target: DataFrame) : string array =
            use emptyDf = target.Clear()
            use result = emptyDf.Select expr
            result.Columns |> Seq.toArray

        /// <summary>
        /// Expand a Selector against a LazyFrame to get the matched column names.
        /// </summary>
        let expandLf (selector: Selector) (target: LazyFrame) : string array =
            target.Select(selector.ToExpr()).Schema.Names |> Seq.toArray

[<AutoOpen>]
module PolarsAutoOpen =
    let inline col name = pl.col name
    let inline lit value = pl.lit value
    let inline alias column = pl.alias column    
    /// <summary>
    /// Upcast operator: Converts Expr or Selector to IColumnExpr interface.
    /// Helps mixing types in a list.
    /// </summary>
    let inline (!>) (x: #IColumnExpr) = x :> IColumnExpr