#nowarn "3391"
namespace Polars.FSharp

open System
open Polars.NET.Core
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
/// Intermediate F# state holder representing an active conditional branch awaiting its statement.
/// </summary>
type WhenCondition internal (conditions: Expr list, statements: Expr list, currentCondition: Expr) =
    member internal _.Conditions = conditions
    member internal _.Statements = statements
    member internal _.CurrentCondition = currentCondition

    /// <summary>
    /// Connects a statement expression to the preceding condition.
    /// </summary>
    /// <param name="statement">The expression to evaluate if true.</param>
    /// <returns>A ThenBranch state holder allowing further chaining, piping, or terminal fallback.</returns>
    member this.Then(statement: Expr) : ThenBranch =
        if box statement = null then raise (ArgumentNullException(nameof statement))
        ThenBranch(this.CurrentCondition :: this.Conditions, statement :: this.Statements)

    /// <summary>
    /// Connects a scalar statement to the preceding condition using SRTP resolution.
    /// </summary>
    member inline this.Then(value: ^T) : ThenBranch =
        this.Then((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))

/// <summary>
/// Intermediate F# state holder representing accumulated conditional branches.
/// </summary>
and ThenBranch internal (conditions: Expr list, statements: Expr list) =
    member internal _.Conditions = conditions
    member internal _.Statements = statements

    /// <summary>
    /// Chains an additional conditional branch.
    /// </summary>
    /// <param name="condition">Condition expression for the next branch.</param>
    /// <returns>A WhenCondition state holder awaiting the next statement.</returns>
    member this.When(condition: Expr) : WhenCondition =
        if box condition = null then raise (ArgumentNullException(nameof condition))
        WhenCondition(this.Conditions, this.Statements, condition)

    /// <summary>
    /// Terminal operator that provides the fallback expression and compiles the ternary tree.
    /// </summary>
    /// <param name="fallback">The fallback expression if all conditions evaluate to false.</param>
    /// <returns>Compiled ternary expression.</returns>
    member this.Otherwise(fallback: Expr) : Expr =
        if box fallback = null then raise (ArgumentNullException(nameof fallback))
        
        // Branches were accumulated in reverse order (head-first), so folding rightward
        // naturally folds from innermost fallback to the outermost condition.
        (fallback, List.zip this.Conditions this.Statements)
        ||> List.fold (fun acc (cond, stmt) -> Expr.Ternary(cond, stmt, acc))

    /// <summary>
    /// Terminal operator that provides a default fallback scalar value resolved via SRTP.
    /// </summary>
    member inline this.Otherwise(value: ^T) : Expr =
        this.Otherwise((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))

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
    let len() = new Expr(PolarsWrapper.Len())
    /// <summary>
    /// Alias for an element being evaluated in an eval or filter expression.
    /// </summary>
    let element = col ""

    /// <summary>
    /// Create a literal expression from a value.
    /// </summary>
    /// <example>
    /// <code>
    /// df.Filter(pl.col("Age") .> pl.lit(18))
    /// </code>
    /// </example>
    let inline lit (value: ^T) : Expr =
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))
    /// <summary>
    /// Create a literal expression for null value.
    /// </summary>
    let litNull() = Expr.LitNull()
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
    let cumCount(column:string) =
        col(column).CumCount(false)
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
    let firstValue(columns:seq<string>) =
        cols(columns).First
    /// <summary>
    /// Get the last column.
    /// </summary>
    let lastCol() =
        Selector.ByIndex(ReadOnlySpan<int64> [|-1L|],true)
    /// <summary>
    /// Get the last value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the first value.</returns>
    let lastValue(columns:seq<string>) =
        cols(columns).Last
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
        let dtexpr = DataType.FromNetType<'T>().ToDataTypeExpr().Handle
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
        let dtexpr = DataType.FromNetType<'T>().ToDataTypeExpr().Handle
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
    let dateRange(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedInterval) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.DateRange(st,ed,ine,null,closed.ToNative()))
    let dateRangeAsSeries (name:string)(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedInterval) =
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
    let dateRanges(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedInterval) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.DateRanges(st,ed,ine,null,closed.ToNative()))
    let dateRangesAsSeries(name:string)(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedInterval) =
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
    let datetimeRange(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedInterval)(unit:TimeUnit)(timeZone:string option) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        let tz =
            match timeZone with
            | Some tz -> tz
            | None -> null
        new Expr(PolarsWrapper.DatetimeRange(st,ed,ine,null,closed.ToNative(),unit.ToNative(),tz))
    /// <summary>
    /// Converts a datetime range expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Start datetime of the range.</param>
    /// <param name="endRange">End datetime of the range.</param>
    /// <param name="interval">Interval between datetime values.</param>
    /// <param name="closed">Closed window of the range.</param>
    /// <param name="unit">Time unit of the resulting Datetime data type.</param>
    /// <param name="timeZone">Time zone of the resulting Datetime data type.</param>
    let datetimeRangeAsSeries(name:string)(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedInterval)(unit:TimeUnit)(timeZone:string option) =
        let expr = datetimeRange start endRange interval closed unit timeZone
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Creates a datetime range expression from the specified start, end, interval, closed window, unit, and time zone.
    /// </summary>
    let datetimeRanges(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedInterval)(unit:TimeUnit)(timeZone:string option) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        let tz =
            match timeZone with
            | Some tz -> tz
            | None -> null
        new Expr(PolarsWrapper.DatetimeRanges(st,ed,ine,null,closed.ToNative(),unit.ToNative(),tz))
    /// <summary>
    /// Converts a datetime range expression to a Series with the specified name.
    /// </summary>
    let datetimeRangesAsSeries(name:string)(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedInterval)(unit:TimeUnit)(timeZone:string option) =
        let expr = datetimeRanges start endRange interval closed unit timeZone
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Generate a time range.
    /// </summary>
    /// <param name="start">Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue</param>
    /// <param name="end">Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue</param>
    /// <param name="interval">Interval of the range periods</param>
    /// <param name="closed">Define which sides of the range are closed.</param>
    let timeRange(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedInterval) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.TimeRange(st,ed,ine,closed.ToNative()))
    /// <summary>
    /// Converts a time range expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Start time of the range.</param>
    /// <param name="endRange">End time of the range.</param>
    /// <param name="interval">Interval between time values.</param>
    /// <param name="closed">Closed window of the range.</param>
    let timeRangeAsSeries(name:string)(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedInterval) =
        let expr = timeRange start endRange interval closed
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Creates a time range expression from the specified start, end, interval, and closed window.
    /// </summary>
    /// <param name="start">Start time of the range.</param>
    /// <param name="endRange">End time of the range.</param>
    /// <param name="interval">Interval between time values.</param>
    /// <param name="closed">Closed window of the range.</param>
    let timeRanges(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedInterval) =
        let st = (lit start).Handle
        let ed = (lit endRange).Handle
        let ine = Dur.consume interval
        new Expr(PolarsWrapper.TimeRanges(st,ed,ine,closed.ToNative()))
    /// <summary>
    /// Converts a time range expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Start time of the range.</param>
    /// <param name="endRange">End time of the range.</param>
    /// <param name="interval">Interval between time values.</param>
    /// <param name="closed">Closed window of the range.</param>
    let timeRangesAsSeries(name:string)(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedInterval) =
        let expr = timeRanges start endRange interval closed
        Series.ofExpr(expr).Rename(name)
    /// <summary>
    /// Generate a series of equally-spaced points.
    /// </summary>
    /// <param name="start">Lower bound of the linear space.</param>
    /// <param name="end">Upper bound of the linear space.</param>
    /// <param name="numSamples">Number of samples to generate.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    let linearSpace(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedInterval) =
        let st = start.CloneHandle()
        let en = endRange.CloneHandle()
        let nu = (lit numSamples).Handle
        new Expr(PolarsWrapper.LinearSpace(st,en,nu,closed.ToNative()))
    /// <summary>
    /// Converts a linear space expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Lower bound of the linear space.</param>
    /// <param name="endRange">Upper bound of the linear space.</param>
    /// <param name="numSamples">Number of samples to generate.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    let linearSpaceAsSeries(name:string)(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedInterval) =
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
    let linearSpaces(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedInterval)(asArray:bool) =
        let st = start.CloneHandle()
        let en = endRange.CloneHandle()
        let nu = (lit numSamples).Handle
        new Expr(PolarsWrapper.LinearSpaces(st,en,nu,closed.ToNative(),asArray))
    /// <summary>
    /// Converts a linear spaces expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Lower bound.</param>
    /// <param name="endRange">Upper bound.</param>
    /// <param name="numSamples">Number of samples.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    /// <param name="asArray">If true, returns an Array dtype instead of List. Requires numSamples to be a constant.</param>
    let linearSpacesAsSeries(name:string)(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedInterval)(asArray:bool) =
        let expr = linearSpaces start endRange numSamples closed asArray
        Series.ofExpr(expr).Rename(name)
    // // --- Expr Helpers ---
    // /// <summary> Cast an expression to a different data type. </summary>
    // let cast (dtype: DataType) (e: Expr) = e.Cast dtype
    // /// <summary> Cast an expression to a .NET data type. </summary>
    // let castWithNetType<'T> (e: Expr) = e.Cast<'T>()
    /// <summary> Create a Series from a sequence of values. </summary>
    let series<'T>(name:string)(data:seq<'T>) =
        Series.create(name,data)
    /// <summary> Create a DataFrame from a sequence of Series. </summary>
    let dataframe(series:seq<Series>) = DataFrame.create(series)
    /// <summary> Boolean data type. </summary>
    let boolean = DataType.Boolean
    /// <summary> 8-bit Integer data type. </summary>
    let int8 = DataType.Int8
    /// <summary> 8-bit Unsigned Integer data type. </summary>
    let uint8 = DataType.UInt8
    /// <summary> 16-bit Integer data type. </summary>
    let int16 = DataType.Int16
    /// <summary> 16-bit Unsigned Integer data type. </summary>
    let uint16 = DataType.UInt16
    /// <summary> 32-bit Integer data type. </summary>
    let int32 = DataType.Int32
    /// <summary> 32-bit Unsigned Integer data type. </summary>
    let uint32 = DataType.UInt32
    /// <summary> 64-bit Unsigned Integer data type. </summary>
    let uint64 = DataType.UInt64
    /// <summary> 64-bit Integer data type. </summary>
    let int64 = DataType.Int64
    /// <summary> 128-bit Integer data type. </summary>
    let int128 = DataType.Int128
    /// <summary> Decimal data type. </summary>
    let decimal precision scale = DataType.Decimal(precision,scale)
    /// <summary> 16-bit Floating point data type. </summary>
    let float16 = DataType.Float16
    /// <summary> 32-bit Floating point data type. </summary>
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
    /// <summary> List data type. </summary>
    let list(inner:DataType) = DataType.List inner
    /// <summary> Array data type.(Also known as fixed length list) </summary>
    let array(inner:DataType)(shape:uint[]) = DataType.Array(inner,shape)
    /// <summary> Struct data type. </summary>
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
    /// <summary>
    /// Create a Struct Field with the given name and data type.
    /// </summary>
    /// <param name="name">The field name.</param>
    /// <param name="dtype">The data type.</param>
    let field(name:string)(dtype:DataType) = {Field.Name=name;Field.DataType=dtype}
    /// <summary>
    /// Create a Categories object with the given name, name space, and physical type.
    /// </summary>
    /// <param name="name">The category name.</param>
    /// <param name="nameSpace">The category name space.</param>
    /// <param name="physical">The physical type.</param>
    let categories(name:string option)(nameSpace:string option)(physical:CategoricalPhysical option) =
        new Categories(?name=name,?nameSpace=nameSpace,?physical=physical)
    /// <summary>
    /// Create a Categorical data type with the given categories.
    /// </summary>
    /// <param name="categories">The categories.</param>
    let categorical(categories:Categories option) = DataType.Categorical(?categories=categories)
    /// <summary>
    /// Create an Enum data type with the given categories.
    /// </summary>
    /// <param name="categories">The categories.</param>
    let enumType(categories:Categories) = DataType.Enum(categories.Freeze())
    /// <summary>
    /// Create a Null data type.
    /// </summary>
    let nullType = DataType.Null
    /// <summary>
    /// Create an Unknown data type.(Same as SameAsInputType)
    /// </summary>
    let unknownType = DataType.Unknown
    /// <summary>
    /// Create a SameAsInputType data type.
    /// </summary>
    let sameAsInputType = unknownType
    /// <summary>
    /// Create a schema from a sequence of fields.
    /// </summary>
    let schema(fields:seq<Field>) = new PolarsSchema(fields)
    /// <summary>
    /// Create an empty schema.
    /// </summary>
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
        let dateExpr =
            match holidays with
            | Some ho -> litSeries ho
            | None -> Series.create("__Date__",[||]).Cast<DateOnly>().Implode() |> litSeries
        new Expr(PolarsWrapper.DtBusinessDayCount(st,ed,wm,dateExpr.Handle))

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
    let collectWithEngine (engine:Engine) (lf:LazyFrame) = lf.Collect(engine)
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
                (seed, lfs |> Array.skip 1 |> Array.mapi (fun i lf -> i + 1, lf))
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
    /// <summary>
    /// Align a sequence of DataFrames on a common schema.
    /// </summary>
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
    /// <summary>
    /// Align a sequence of DataFrames on a common schema asynchronously.
    /// </summary>
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
    // /// <summary> Convert Selector to Expr. </summary>
    // let asExpr (s: Selector) = s.ToExpr()
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
    // /// <summary> Exclude columns from Selector. </summary>
    // let exclude (names: seq<string>) (s: Selector) = s.Exclude names
    /// <summary>
    /// Aggregate all column values into a list.
    /// This function is syntactic sugar for pl.col(name).Implode().
    /// </summary>
    /// <param name="column">Column name</param>
    let implode (column:string) = col(column).Implode()
    /// <summary>
    /// Get the nth column(s) of the context.
    /// </summary>
    /// <param name="indices">One or more indices representing the columns to retrieve.</param>
    let nth (indices:seq<int64>)=
        let indSpan = ReadOnlySpan<int64> (indices |> Seq.toArray)
        Selector.ByIndex(indSpan,false)
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
    let zeros<'T>(n:int) =
        let dtype = DataType.FromNetType<'T>()
        repeat (lit 0) n (Some dtype)
    let zerosAsSeries<'T> (n:int) =
        Series.ofExpr(zeros<'T> n)
    /// <summary>
    /// Construct a column of length n filled with ones.
    /// This is syntactic sugar for the repeat function.
    /// </summary>
    /// <param name="n">Length of the resulting column.</param>
    let ones<'T>(n:int) =
        let dtype = DataType.FromNetType<'T>()
        repeat (lit 1) n (Some dtype)
    let onesAsSeries<'T> (n:int) =
        Series.ofExpr(ones<'T> n)
    /// <summary>
    /// Parses an integer column (seconds, milliseconds, etc.) into a Datetime or Date expression.
    /// </summary>
    let fromEpoch(column:Expr) (timeUnit:EpochTimeUnit) =
        match timeUnit with
        | EpochTimeUnit.Day -> column.Cast<DateOnly>()
        | EpochTimeUnit.Second -> (column * lit 1_000_000L).Cast(datetime TimeUnit.Microseconds None)
        | EpochTimeUnit.Milliseconds -> (column * lit 1_000L).Cast(datetime TimeUnit.Microseconds None)
        | EpochTimeUnit.Microseconds -> column.Cast(datetime TimeUnit.Microseconds None)
        | EpochTimeUnit.Nanoseconds -> column.Cast(datetime TimeUnit.Nanoseconds None)
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    let asStruct (exprs: seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.AsStruct handles)
    /// <summary>
    /// Collect several expressions and combine them into a single Struct Series
    /// </summary>
    let structSeries(exprs: seq<Expr>) =
        Series.ofExpr(asStruct exprs)
    /// <summary>
    /// Concat DataFrames
    /// </summary>
    let concatDataFrame(how:ConcatType) (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs,how)
    /// <summary>
    /// Concat LazyFrames
    /// </summary>
    let concatLazyFrame(how:ConcatType) (lfs: seq<LazyFrame>) : LazyFrame =
        LazyFrame.Concat(lfs,how)
    /// <summary>
    /// Concatenates a sequence of Series into a single Series.
    /// Preserves caller immutability by creating a cloned accumulator.
    /// </summary>
    let concatSeries (series: seq<Series>) : Series =
        use enumerator = series.GetEnumerator()
        if not (enumerator.MoveNext()) then
            invalidArg (nameof series) "Cannot concatenate an empty sequence of Series."
        
        // Clone the first Series to act as our private accumulator
        let acc = enumerator.Current.Clone()
        while enumerator.MoveNext() do
            acc.AppendInplace enumerator.Current
        acc
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
    /// <summary>
    /// Compute two argument arctan in radians.
    /// Returns the angle (in radians) in the plane between the positive x-axis and the ray from the origin to (x,y).
    /// </summary>
    let arctan2(y:Expr) (x:Expr) = new Expr(PolarsWrapper.ArcTan2(y.CloneHandle(),x.CloneHandle()))

    let asLazy(df:DataFrame) = df.Lazy()
    /// <summary> Create a SQL context for executing SQL queries on LazyFrames. </summary>
    let sqlContext() = new SqlContext()
    /// <summary>
    /// Create an if-else expression.
    /// </summary>
    let ifElse (predicate: Expr) (ifTrue: Expr) (ifFalse: Expr) : Expr =
        let p = predicate.CloneHandle()
        let t = ifTrue.CloneHandle()
        let f = ifFalse.CloneHandle()

        new Expr(PolarsWrapper.IfElse(p, t, f))

    /// --- Config ---
    let setEnvVar (key:string) (value:string) =
        Config.set key value
    let threadPoolSize() =
        CoreConfig.ThreadPoolSize
        |> Option.ofNullable
        |> Option.map int
        |> Option.defaultWith (fun () -> Environment.ProcessorCount)
    /// <summary> Accumulate over multiple columns horizontally/row-wise. </summary>
    let fold (f: Expr -> Expr -> Expr) (acc: Expr) (exprs: seq<Expr>) : Expr =
        Seq.fold f acc exprs

    /// <summary> Reduce multiple columns horizontally/row-wise. </summary>
    let reduce (f: Expr -> Expr -> Expr) (exprs: seq<Expr>) : Expr =
        Seq.reduce f exprs
    /// <summary>
    /// Starts a conditional when-then-otherwise expression branch natively in F#.
    /// </summary>
    /// <param name="condition">The initial filter condition expression.</param>
    /// <returns>An intermediate WhenCondition state object.</returns>
    let when' (condition: Expr) : WhenCondition =
        if box condition = null then raise (ArgumentNullException(nameof condition))
        WhenCondition([], [], condition)

    /// <summary>
    /// Connects a statement expression to the preceding when' condition.
    /// </summary>
    /// <param name="statement">The expression to evaluate if the condition is true.</param>
    /// <param name="whenBlock">The WhenCondition block built by pl.when'.</param>
    /// <returns>A new ThenBranch collector block.</returns>
    let then' (statement: Expr) (whenBlock: WhenCondition) : ThenBranch =
        if box whenBlock = null then raise (ArgumentNullException(nameof whenBlock))
        whenBlock.Then statement

    /// <summary>
    /// Terminal operator that provides the default fallback expression for a when-then-otherwise chain.
    /// </summary>
    /// <param name="fallback">The fallback expression.</param>
    /// <param name="thenBlock">The ThenBranch collector block.</param>
    /// <returns>The compiled ternary expression tree.</returns>
    let otherwise (fallback: Expr) (thenBlock: ThenBranch) : Expr =
        if box thenBlock = null then raise (ArgumentNullException(nameof thenBlock))
        thenBlock.Otherwise fallback

    // ==========================================
    // Column Selectors (pl.cs)
    // ==========================================
    module cs =

        /// <summary>
        /// Select a single column by name.
        /// </summary>
        let inline byName (names: string seq) =
            new Selector(PolarsWrapper.SelectorCols (names |> Seq.toArray))

        /// <summary>
        /// Select columns by their index with.
        /// </summary>
        let inline byIndex(indices:ReadOnlySpan<int64>) = new Selector(PolarsWrapper.SelectorByIndex(indices, false))
        /// <summary>
        /// Select columns by their index.
        /// Usage: cs.byIndex(0L, 2L, 4L)
        /// </summary>
        let inline byIndexStrict(indices:ReadOnlySpan<int64>) = new Selector(PolarsWrapper.SelectorByIndex(indices, true))

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
        /// <summary> Select date columns.</summary>
        let inline date() = new Selector(PolarsWrapper.SelectorByDtype(PlDataType.Date));
        /// <summary> Select boolean columns.</summary>
        let inline boolean() = new Selector(PolarsWrapper.SelectorByDtype(PlDataType.Boolean));
        /// <summary> Select binary columns.</summary>
        let inline binary() = byType DataType.Binary
        /// <summary> Select empty columns.</summary>
        let inline empty() = new Selector(PolarsWrapper.SelectorEmpty());
        /// <summary> Select integer columns.</summary>
        let inline integer() = new Selector(PolarsWrapper.SelectorInteger());
        /// <summary> Select unsigned integer columns.</summary>
        let inline unsignedInteger() = new Selector(PolarsWrapper.SelectorUnsignedInteger());
        /// <summary> Select signed integer columns.</summary>
        let inline signedInteger() = new Selector(PolarsWrapper.SelectorSignedInteger());
        /// <summary> Select float columns.</summary>
        let inline float() = Selector.Float();
        /// <summary> Select decimal columns.</summary>
        let inline decimal() = new Selector(PolarsWrapper.SelectorDecimal());
        /// <summary> Select enum columns.</summary>
        let inline enum() = new Selector(PolarsWrapper.SelectorEnum());
        /// <summary> Select nested columns.</summary>
        let inline nested() = new Selector(PolarsWrapper.SelectorNested());
        /// <summary> Select struct columns.</summary>
        let inline structType() = new Selector(PolarsWrapper.SelectorStruct());
        /// <summary> Select temporal columns.</summary>
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
        let expandDf (selector: IColumnExpr) (target: DataFrame) : string array =
            use emptyDf = target.Clear()
            use result = emptyDf.Select [selector]
            result.Columns |> Seq.toArray
        /// <summary>
        /// Expand a Selector against a LazyFrame to get the matched column names.
        /// </summary>
        let expandLf (selector: IColumnExpr) (target: LazyFrame) : string array =
            target.Select([selector]).Schema.Names |> Seq.toArray

[<AutoOpen>]
module PolarsAutoOpen =
    /// <summary>
    /// Select a column by name.
    /// </summary>
    let inline col name = pl.col name
    /// <summary>
    /// Create a literal value expression.
    /// </summary>
    let inline lit value = pl.lit value
    /// <summary>
    /// Create an alias expression.
    /// </summary>
    let inline alias column = pl.alias column
    /// <summary>
    /// Upcast operator: Converts Expr or Selector to IColumnExpr interface.
    /// Helps mixing types in a list.
    /// </summary>
    let inline (!>) (x: #IColumnExpr) = x :> IColumnExpr
