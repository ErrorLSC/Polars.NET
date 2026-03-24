namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
open System.Collections.Generic
open Polars.NET.Core.Arrow
open Polars.NET.Core.Data
open Polars.NET.Core.Helpers
open System.Data
open System.Threading.Tasks
open System.Collections.Concurrent
open System.Collections
open System.Reflection
open System.Text
open System.IO
open System.Threading
open Apache.Arrow.Adbc
open Apache.Arrow.Ipc
open System.Data.Common
open System.Threading.Channels
open Polars.NET.Core.TensorInterop
open System.Numerics.Tensors
/// --- Series ---
/// <summary>
/// An eager Series holding a single column of data.
/// </summary>
type Series(handle: SeriesHandle) =

    member this.Dispose() = handle.Dispose()
    
    interface IDisposable with 
        member this.Dispose() = this.Dispose()

    interface IPolarsSeries with
        member this.ToFrame() = 
            this.ToFrame() :> IPolarsDataFrame
            
        member this.DataType = 
            this.DataType :> IPolarsDataType
            
        member this.Name
            with get () = 
                this.Name 
            and set (value: string) = 
                this.Rename value |> ignore
    member _.Handle = handle
    member _.Name = PolarsWrapper.SeriesName handle
    member _.Length = PolarsWrapper.SeriesLen handle
    member _.Len = PolarsWrapper.SeriesLen handle
    member _.NullCount : int64 = PolarsWrapper.SeriesNullCount handle

    // ==========================================
    // Expression Composition (The "ApplyExpr" Pattern)
    // ==========================================

    /// <summary>
    /// Internal Helper: Wrap this Series in a temporary DataFrame, run an Expr, and extract the result.
    /// This allows Series to directly use the full power of the Expression engine without duplicating logic.
    /// </summary>
    member internal this.ApplyExpr(expr: Expr) : Series =
        use dfHandle = PolarsWrapper.SeriesToFrame handle
        use df = new DataFrame(dfHandle)

        use dfRes = df.Select [expr]

        dfRes.[0]
    // ==========================================
    // Binary Op Helper (The "ApplyBinaryExpr" Pattern)
    // ==========================================

    /// <summary>
    /// Internal Helper: Apply a binary expression using two Series.
    /// Handles name collision by creating a temporary renamed series if necessary.
    /// </summary>
    member internal this.ApplyBinaryExpr(other: Series, op: Expr -> Expr -> Expr) : Series =
        let leftName = this.Name
        let rightNameRaw = other.Name
        
        let rightName, rightSeries, tempToDispose =
            if leftName = rightNameRaw then
                let newName = "__other_temp__"
                let cloneHandle = PolarsWrapper.CloneSeries other.Handle 
                let clone = (new Series(cloneHandle)).Rename newName
                newName, clone, Some clone
            else
                rightNameRaw, other, None

        try
            let handles = [| this.Handle; rightSeries.Handle |]
            use dfHandle = PolarsWrapper.DataFrameNew handles
            use df = new DataFrame(dfHandle)

            let expr = op (Expr.Col leftName) (Expr.Col rightName)
            
            use resDf = df.Select [expr]

            resDf.[0]

        finally
            match tempToDispose with
            | Some s -> s.Handle.Dispose()
            | None -> ()
    /// <summary> Access temporal (Date/Time) operations. </summary>
    member this.Dt = SeriesDtNameSpace this
    /// <summary> Access string manipulation operations. </summary>
    member this.Str = SeriesStrNameSpace this
    /// <summary> Access list operations. </summary>
    member this.List = SeriesListNameSpace this
    /// <summary> Access array (fixed-size list) operations. </summary>
    member this.Array = SeriesArrayNameSpace this
    /// <summary> Access struct operations. </summary>
    member this.Struct = SeriesStructNameSpace this
    // --- Basic Operations ---

    /// <summary> Rename the Series in-place. Returns self. </summary>    
    member this.Rename(name: string) = 
        PolarsWrapper.SeriesRename(handle, name)
        this
    /// <summary> Slice the Series. Returns a new Series. </summary>
    /// <param name="offset">Start index.</param>
    /// <param name="length">Length of the slice.</param>
    member this.Slice(offset: int64, length: int64) =
        new Series(PolarsWrapper.SeriesSlice(handle, offset, length))
    /// <summary>
    /// Sort this Series. Returns a new Series.
    /// </summary>
    /// <param name="descending">Sort in descending order (default: false).</param>
    /// <param name="nullsLast">Place null values last (default: false).</param>
    /// <param name="maintainOrder">Maintain the order of equal elements (Stable sort) (default: false).</param>
    /// <param name="multithreaded">Use multiple threads (default: true).</param>
    member this.Sort(
        ?descending: bool,
        ?nullsLast: bool,
        ?maintainOrder: bool,
        ?multithreaded: bool
    ) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        let stable = defaultArg maintainOrder false
        let multi = defaultArg multithreaded true

        new Series(PolarsWrapper.SeriesSort(handle, desc, nLast, multi, stable))
    /// <summary>
    /// Sort this Series in ascending order.
    /// </summary>
    member this.Sort() =
        this.Sort false
    /// <summary>
    /// Explode a list column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    member this.Explode(?emptyAsNull: bool, ?keepNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).Explode(?emptyAsNull=emptyAsNull,?keepNulls=keepNulls))
    /// <summary>
    /// Aggregate values into a list.
    /// Result is a Series with 1 row containing a List of all values.
    /// </summary>
    member this.Implode() =
        this.ApplyExpr(Expr.Col(this.Name).Implode())
    /// <summary>
    /// Unnest a Struct column into a DataFrame.
    /// Shortcut for <see cref="SeriesStructOps.Unnest"/>.
    /// </summary>
    member this.Unnest() =
        this.Struct.Unnest()
    /// <summary>
    /// Get the string representation of the Series Data Type (e.g., "Int64", "String").
    /// </summary>
    member _.DtypeStr = PolarsWrapper.GetSeriesDtypeString handle
    /// <summary> Get the DataType of the Series. </summary>
    member this.DataType : DataType =
        use typeHandle = PolarsWrapper.GetSeriesDataType handle
        
        DataType.FromHandle typeHandle
    // ==========================================
    // Indexing & Searching (Forwarded to Expr)
    // ==========================================

    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index expression.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    member this.Get(index: Expr, ?nullOnOutOfBounds: bool) =
        this.ApplyExpr(Expr.Col(this.Name).Get(index, ?nullOnOutOfBounds = nullOnOutOfBounds))

    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index number.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    member this.Get(index: uint64, ?nullOnOutOfBounds: bool) =
        this.ApplyExpr(Expr.Col(this.Name).Get(index, ?nullOnOutOfBounds = nullOnOutOfBounds))

    /// <summary>
    /// Gather values by an index expression.
    /// </summary>
    member this.Gather(indices: Expr) =
        this.ApplyExpr(Expr.Col(this.Name).Gather indices)

    /// <summary>
    /// LINQ-like alias for Gather.
    /// </summary>
    member this.Take(indices: Expr) = 
        this.ApplyExpr(Expr.Col(this.Name).Take indices)

    /// <summary>
    /// Take every nth value starting from an offset.
    /// </summary>
    member this.GatherEvery(n: uint64, ?offset: uint64) =
        this.ApplyExpr(Expr.Col(this.Name).GatherEvery(n, ?offset = offset))

    /// <summary>
    /// Get the index of the unique values.
    /// </summary>
    member this.ArgUnique() =
        this.ApplyExpr(Expr.Col(this.Name).ArgUnique())

    /// <summary>
    /// Get the index of the maximum value.
    /// </summary>
    member this.ArgMax() =
        this.ApplyExpr(Expr.Col(this.Name).ArgMax())

    /// <summary>
    /// Get the index of the minimum value.
    /// </summary>
    member this.ArgMin() =
        this.ApplyExpr(Expr.Col(this.Name).ArgMin())

    /// <summary>
    /// Get the index values that would sort this expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">If true, place null values last. Default is false.</param>
    member this.ArgSort(?descending: bool, ?nullsLast: bool) =
        this.ApplyExpr(Expr.Col(this.Name).ArgSort(?descending = descending, ?nullsLast = nullsLast))

    /// <summary>
    /// Find the index of the first occurrence of a specific value.
    /// </summary>
    /// <param name="element">The element expression to search for.</param>
    member this.IndexOf(element: Expr) =
        this.ApplyExpr(Expr.Col(this.Name).IndexOf element)

    /// <summary>
    /// Find indices where elements should be inserted to maintain order (Binary Search).
    /// </summary>
    /// <param name="element">The element expression to insert/search.</param>
    /// <param name="side">The insertion side (Any, Left, Right). Default is Any.</param>
    /// <param name="descending">Whether the target column is sorted in descending order. Default is false.</param>
    member this.SearchSorted(element: Expr, ?side: SearchSortedSide, ?descending: bool) =
        this.ApplyExpr(Expr.Col(this.Name).SearchSorted(element, ?side = side, ?descending = descending))
    // ==========================================
    // Missing Data Handling (FillNull & FillNan)
    // ==========================================

    // --- 1. Fill with Scalar (ApplyExpr) ---

    /// <summary> Fill null values with a literal integer. </summary>
    member this.FillNull(fillValue: int) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))
    /// <summary> Fill null values with a literal double. </summary>
    member this.FillNull(fillValue: double) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))
    /// <summary> Fill null values with a literal string. </summary>
    member this.FillNull(fillValue: string) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))
    /// <summary>
    /// Interpolate intermediate values. The interpolation method can be configured.
    /// <para>Nulls at the beginning and end of the series remain null.</para>
    /// </summary>
    /// <param name="method">Interpolation method (Linear or Nearest).</param>
    member this.Interpolate(?method:InterpolationMethod) = 
        this.ApplyExpr(Expr.Col(this.Name).Interpolate(?method=method))
    member this.InterpolateBy(by:Series) = 
        this.ApplyBinaryExpr(by, fun l r -> l.InterpolateBy r)
    /// <summary> Fill null values with a literal boolean. </summary>
    member this.FillNull(fillValue: bool) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))

    /// <summary> Fill floating point NaN values with a literal value. </summary>
    member this.FillNan(fillValue: double) =
        this.ApplyExpr(Expr.Col(this.Name).FillNan(new Expr(PolarsWrapper.Lit fillValue)))

    // --- 2. Fill with Series (ApplyBinaryExpr) ---

    /// <summary>
    /// Fill null values with values from another Series.
    /// Useful for coalescing.
    /// </summary>
    member this.FillNull(fillValue: Series) =
        this.ApplyBinaryExpr(fillValue, fun l r -> l.FillNull r)

    /// <summary>
    /// Fill NaN values with values from another Series.
    /// </summary>
    member this.FillNan(fillValue: Series) =
        this.ApplyBinaryExpr(fillValue, fun l r -> l.FillNan r)
    
    // --- 3. Fill with Expr (Advanced) ---
    
    /// <summary>
    /// Fill nulls using an expression (mostly for internal use or complex literals).
    /// </summary>
    member this.FillNull(expr: Expr) =
        this.ApplyExpr(Expr.Col(this.Name).FillNull expr)
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
    /// Drop null values.
    /// </summary>
    member this.DropNulls() : Series =
        new Series(PolarsWrapper.SeriesDropNulls handle)
    /// <summary>
    /// Drop nan values.
    /// </summary>
    member this.DropNans() : Series =
        let expr = Expr.Col(this.Name).DropNans()
        this.ApplyExpr expr
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

    /// <summary> Check if floating point values are NaN. </summary>
    member this.IsNan() = new Series(PolarsWrapper.SeriesIsNan handle)

    /// <summary> Check if floating point values are not NaN. </summary>
    member this.IsNotNan() = new Series(PolarsWrapper.SeriesIsNotNan handle)

    /// <summary> Check if floating point values are finite (not NaN and not Inf). </summary>
    member this.IsFinite() = new Series(PolarsWrapper.SeriesIsFinite handle)

    /// <summary> Check if floating point values are infinite. </summary>
    member this.IsInfinite() = new Series(PolarsWrapper.SeriesIsInfinite handle)
    // ==========================================
    // Uniqueness & Boolean Masl
    // ==========================================

    /// <summary>
    /// Get unique values (distinct).
    /// </summary>
    member this.Unique() =
        new Series(PolarsWrapper.SeriesUnique handle)

    /// <summary>
    /// Get unique values (distinct), maintaining original order.
    /// </summary>
    member this.UniqueStable() =
        new Series(PolarsWrapper.SeriesUniqueStable handle)

    /// <summary>
    /// Count the number of unique values.
    /// </summary>
    member this.NUnique = 
        PolarsWrapper.SeriesNUnique handle
    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// Implemented via Expression engine.
    /// </summary>
    member this.IsUnique() =
        // col(Name).IsUnique()
        let expr = Expr.Col(this.Name).IsUnique()
        this.ApplyExpr expr

    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// Implemented via Expression engine.
    /// </summary>
    member this.IsDuplicated() =
        let expr = Expr.Col(this.Name).IsDuplicated()
        this.ApplyExpr expr
    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    member this.IsBetween(lower:Expr, upper:Expr) = 
        this.ApplyExpr(Expr.Col(this.Name).IsBetween(lower,upper))
    /// <summary>
    /// Check if the value is in given collection.
    /// </summary>
    member this.IsIn(other:Expr, ?nullsEqual:bool) =
        this.ApplyExpr(Expr.Col(this.Name).IsIn(other=other,?nullsEqual=nullsEqual))
    /// <summary>
    /// Filter a series.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new series with filtered values.</returns>
    member this.Filter(predicate:Expr) = 
        this.ApplyExpr(Expr.Col(this.Name).Filter predicate)
    // ==========================================
    // UDF / Map (Apply Custom C# / F# Functions)
    // ==========================================

    /// <summary>
    /// Apply a custom function (UDF) to the Series.
    /// Uses Apache Arrow arrays for high-performance data transfer.
    /// </summary>
    /// <param name="func">The compiled UDF (created via Udf.map or Udf.mapOption).</param>
    /// <param name="returnType">The expected output DataType. Required for Polars query planning.</param>
    member this.Map(func: Func<IArrowArray, IArrowArray>, returnType: DataType) =
        // col(Name).Map(func, returnType)
        this.ApplyExpr(Expr.Col(this.Name).Map(func, returnType))

    /// <summary>
    /// Apply a custom function (UDF) assuming the output type is the same as the input.
    /// </summary>
    /// <param name="func">The compiled UDF.</param>
    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        this.Map(func, DataType.SameAsInput)
        
    // ==========================================
    // Optional: High-Level F# Overloads (Sugar)
    // ==========================================
    /// <summary>
    /// Map values using a standard F# function.
    /// Automatically wraps it using Udf.map.
    /// </summary>
    member this.Map<'T, 'U>(f: 'T -> 'U, returnType: DataType) =
        let udf = Udf.map f
        this.Map(udf, returnType)

    /// <summary>
    /// Map values using an F# function that handles Options.
    /// Automatically wraps it using Udf.mapOption.
    /// </summary>
    member this.MapOption<'T, 'U>(f: 'T option -> 'U option, returnType: DataType) =
        let udf = Udf.mapOption f
        this.Map(udf, returnType)
    /// <summary>
    /// Map values using an F# function that handles Value Options.
    /// Automatically wraps it using Udf.mapValueOption.
    /// </summary>
    member this.MapValueOption<'T, 'U>(f: 'T voption -> 'U voption, returnType: DataType) =
        let udf = Udf.mapValueOption f
        this.Map(udf, returnType)
    // ==========================================
    // Math Operations (Forwarding to Expr)
    // ==========================================

    // --- 1. Unary Operations (Scalar / Self) ---

    /// <summary> Round to given decimals. </summary>
    member this.Round(decimals: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Round decimals)

    /// <summary> Round up to the nearest integer. </summary>
    member this.Ceil() = this.ApplyExpr(Expr.Col(this.Name).Ceil())

    /// <summary> Round down to the nearest integer. </summary>
    member this.Floor() = this.ApplyExpr(Expr.Col(this.Name).Floor())

    /// <summary> Absolute value. </summary>
    member this.Abs() = this.ApplyExpr(Expr.Col(this.Name).Abs())

    /// <summary> Element-wise sign. </summary>
    member this.Sign() = this.ApplyExpr(Expr.Col(this.Name).Sign())

    /// <summary> Square root. </summary>
    member this.Sqrt() = this.ApplyExpr(Expr.Col(this.Name).Sqrt())

    /// <summary> Cube root. </summary>
    member this.Cbrt() = this.ApplyExpr(Expr.Col(this.Name).Cbrt())

    /// <summary> Exponential (e^x). </summary>
    member this.Exp() = this.ApplyExpr(Expr.Col(this.Name).Exp())

    /// <summary> Natural logarithm (ln). </summary>
    member this.Ln() = this.ApplyExpr(Expr.Col(this.Name).Ln())

    // --- 2. Binary Operations with Scalar (Treated as Unary Expr) ---

    /// <summary> Power with scalar exponent. </summary>
    member this.Pow(exponent: double) = 
        this.ApplyExpr(Expr.Col(this.Name).Pow exponent)

    /// <summary> Power with integer exponent. </summary>
    member this.Pow(exponent: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Pow exponent)

    /// <summary> Logarithm with scalar base. </summary>
    member this.Log(baseVal: double) = 
        this.ApplyExpr(Expr.Col(this.Name).Log baseVal)
    /// <summary> Bitwise left shift. </summary>
    member this.BitLeftShift(n: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BitLeftShift n)
    /// <summary> Bitwise right shift. </summary>
    member this.BitRightShift(n: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BitRightShift n)

    // --- 3. Binary Operations with Series (Using ApplyBinaryExpr) ---

    /// <summary> Power with Series exponent. </summary>
    member this.Pow(exponent: Series) = 
        this.ApplyBinaryExpr(exponent, fun l r -> l.Pow r)

    /// <summary> Logarithm with Series base. </summary>
    member this.Log(baseVal: Series) = 
        this.ApplyBinaryExpr(baseVal, fun l r -> l.Log r)
    member this.Dot(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.Dot r)
    /// <summary> True division (float result). </summary>
    member this.Truediv(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.Truediv r)
    
    /// <summary> True division (scalar). </summary>
    member this.Truediv(other: double) = 
        this.ApplyExpr(Expr.Col(this.Name).Truediv(new Expr(PolarsWrapper.Lit other)))

    /// <summary> Floor division (integer result). </summary>
    member this.FloorDiv(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.FloorDiv(r))

    /// <summary> Floor division (scalar). </summary>
    member this.FloorDiv(other: int) = 
        this.ApplyExpr(Expr.Col(this.Name).FloorDiv(new Expr(PolarsWrapper.Lit other)))

    /// <summary> Modulo (remainder). </summary>
    member this.Mod(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.Mod r)

    /// <summary> Modulo (scalar). </summary>
    member this.Mod(other: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Mod(new Expr(PolarsWrapper.Lit other)))

    // Alias for Mod
    member this.Rem(other: Series) = this.Mod other
    member this.Rem(other: int) = this.Mod other
    // ==========================================
    // Math: Trigonometry
    // ==========================================

    /// <summary> Compute the element-wise sine. </summary>
    member this.Sin() = this.ApplyExpr(Expr.Col(this.Name).Sin())

    /// <summary> Compute the element-wise cosine. </summary>
    member this.Cos() = this.ApplyExpr(Expr.Col(this.Name).Cos())

    /// <summary> Compute the element-wise tangent. </summary>
    member this.Tan() = this.ApplyExpr(Expr.Col(this.Name).Tan())

    /// <summary> Compute the element-wise inverse sine. </summary>
    member this.ArcSin() = this.ApplyExpr(Expr.Col(this.Name).ArcSin())

    /// <summary> Compute the element-wise inverse cosine. </summary>
    member this.ArcCos() = this.ApplyExpr(Expr.Col(this.Name).ArcCos())

    /// <summary> Compute the element-wise inverse tangent. </summary>
    member this.ArcTan() = this.ApplyExpr(Expr.Col(this.Name).ArcTan())

    // ==========================================
    // Math: Hyperbolic
    // ==========================================

    /// <summary> Compute the element-wise hyperbolic sine. </summary>
    member this.Sinh() = this.ApplyExpr(Expr.Col(this.Name).Sinh())

    /// <summary> Compute the element-wise hyperbolic cosine. </summary>
    member this.Cosh() = this.ApplyExpr(Expr.Col(this.Name).Cosh())

    /// <summary> Compute the element-wise hyperbolic tangent. </summary>
    member this.Tanh() = this.ApplyExpr(Expr.Col(this.Name).Tanh())

    /// <summary> Compute the element-wise inverse hyperbolic sine. </summary>
    member this.ArcSinh() = this.ApplyExpr(Expr.Col(this.Name).ArcSinh())

    /// <summary> Compute the element-wise inverse hyperbolic cosine. </summary>
    member this.ArcCosh() = this.ApplyExpr(Expr.Col(this.Name).ArcCosh())

    /// <summary> Compute the element-wise inverse hyperbolic tangent. </summary>
    member this.ArcTanh() = this.ApplyExpr(Expr.Col(this.Name).ArcTanh())
    // ==========================================
    // Shift, Diff & Fill
    // ==========================================

    /// <summary>
    /// Shift the values by a given period.
    /// </summary>
    member this.Shift(n: int64) = 
        this.ApplyExpr(Expr.Col(this.Name).Shift n)

    member this.Shift(n: int) = this.Shift(int64 n)
    
    /// <summary> Shift by 1. </summary>
    member this.Shift() = this.Shift(1L)

    /// <summary>
    /// Calculate the difference with a given period.
    /// </summary>
    member this.Diff(n: int64) = 
        this.ApplyExpr(Expr.Col(this.Name).Diff n)

    member this.Diff(n: int) = this.Diff(int64 n)
    
    /// <summary> Diff by 1. </summary>
    member this.Diff() = this.Diff(1L)

    /// <summary>
    /// Fill null values with the previous non-null value.
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill.</param>
    member this.ForwardFill(?limit: int) =
        this.ApplyExpr(Expr.Col(this.Name).ForwardFill(?limit=limit))

    /// <summary>
    /// Fill null values with the next non-null value.
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill.</param>
    member this.BackwardFill(?limit: int) =
        this.ApplyExpr(Expr.Col(this.Name).BackwardFill(?limit=limit))

    // Alias
    member this.FFill ?limit = this.ForwardFill(?limit=limit)
    member this.BFill ?limit = this.BackwardFill(?limit=limit)

    // ==========================================
    // Rolling Window Functions
    // ==========================================

    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    member this.RollingMin(windowSize: string, ?minPeriod: int,?weights: float[], ?center: bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    member this.RollingMin(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    member this.RollingMax(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    member this.RollingMax(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling average (moving average) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling average.</returns>
    member this.RollingMean(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling average (moving average) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling average.</returns>
    member this.RollingMean(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
    member this.RollingSum(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
    member this.RollingSum(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    member this.RollingMedian(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedian(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    member this.RollingMedian(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedian(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    member this.RollingStd(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStd(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    member this.RollingStd(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStd(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    member this.RollingVar(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVar(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?ddof=ddof))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    member this.RollingVar(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVar(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?ddof=ddof))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingSkew(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSkew(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?bias=bias))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingSkew(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSkew(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?bias=bias))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingKurtosis(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool,?fisher:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingKurtosis(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?fisher=fisher,?bias=bias))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingKurtosis(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool,?fisher:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingKurtosis(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?fisher=fisher,?bias=bias))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    member this.RollingRank(windowSize: string, ?minPeriod: int,?method:RankMethod,?seed:uint64,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRank(windowSize, ?minPeriod=minPeriod,?method=method,?seed=seed,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    member this.RollingRank(windowSize: TimeSpan, ?minPeriod: int,?method:RankMethod,?seed:uint64,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRank(windowSize, ?minPeriod=minPeriod,?method=method,?seed=seed,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling quantile over a fixed window.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    /// <param name="windowSize">
    /// The size of the window. 
    /// <para>Format: <c>"3i"</c> (3 rows) or just a number string <c>"3"</c>.</para>
    /// <para>For time-based windows (e.g. "2h"), use <see cref="RollingQuantileBy(double,QuantileMethod,string,Expr,int,ClosedWindow)"/> instead.</para>
    /// </param>
    /// <param name="weights">
    /// Optional weights for the window. The length must match the parsed window size.
    /// <para>If <c>null</c>, equal weights are used.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new series representing the rolling quantile.</returns>
    member this.RollingQuantile(quantile:float,method:QuantileMethod,windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantile(quantile,method,windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling quantile over a fixed window.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="weights">
    /// Optional weights for the window. The length must match the parsed window size.
    /// <para>If <c>null</c>, equal weights are used.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new series representing the rolling quantile.</returns>
    member this.RollingQuantile(quantile:float,method:QuantileMethod,windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantile(quantile,method,windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    // ==========================================
    // Rolling ... By
    // ==========================================

    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling mean.</returns>
    member this.RollingMeanBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMeanBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling mean.</returns>
    member this.RollingMeanBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMeanBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))

    /// <summary>
    /// Apply a rolling sum (moving sum) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling sum.</returns>
    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSumBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling sum (moving sum) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling sum.</returns>
    member this.RollingSumBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSumBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))

    /// <summary>
    /// Apply a rolling min (moving min) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling min.</returns>
    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMinBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling min (moving min) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling min.</returns>
    member this.RollingMinBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMinBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))

    /// <summary>
    /// Apply a rolling max (moving max) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling max.</returns>
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMaxBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling median (moving median) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling median.</returns>
    member this.RollingMedianBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedianBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling median (moving median) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling max.</returns>
    member this.RollingMedianBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedianBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling max (moving max) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling max.</returns>
    member this.RollingMaxBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMaxBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling standard deviation.</returns>
    member this.RollingStdBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStdBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling standard deviation.</returns>
    member this.RollingStdBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStdBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <returns>A new series representing the dynamic rolling variance.</returns>
    member this.RollingVarBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVarBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod,?ddof=ddof))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <returns>A new series representing the dynamic rolling variance.</returns>
    member this.RollingVarBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVarBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod,?ddof=ddof))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="method">The method used to assign ranks to tied elements.
    /// </param>
    /// <param name="seed">Seed for the random method (only relevant when method is Random).
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling rank.</returns>
    member this.RollingRankBy(windowSize: string, by: Expr, ?method:RollingRankMethod,?seed:uint64,?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRankBy(windowSize, by,?method=method,?seed=seed, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="method">The method used to assign ranks to tied elements.
    /// </param>
    /// <param name="seed">Seed for the random method (only relevant when method is Random).
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling rank.</returns>
    member this.RollingRankBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRankBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling quantile (moving quantile) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).
    /// </param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling quantile.</returns>
    member this.RollingQuantileBy(quantile: float, method: QuantileMethod, windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantileBy(quantile, method, windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling quantile (moving quantile) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).
    /// </param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling quantile.</returns>
    member this.RollingQuantileBy(quantile: float, method: QuantileMethod, windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantileBy(quantile, method, windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    // ==========================================
    // TopK / BottomK
    // ==========================================
    /// <summary>
    /// Get the k largest elements.
    /// Result is sorted descending.
    /// </summary>
    member this.TopK(k: int) = 
        this.ApplyExpr(Expr.Col(this.Name).TopK k)
    /// <summary>
    /// Get the k smallest elements.
    /// Result is sorted ascending.
    /// </summary>
    member this.BottomK(k: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BottomK k)

    /// <summary>
    /// Get top k elements of this Series, sorted by another Series.
    /// </summary>
    member this.TopKBy(k: int, by: Series, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyBinaryExpr(by, fun me other -> me.TopKBy(k, other, r))
    /// <summary>
    /// Get top k elements of this Series, sorted by another Expr.
    /// </summary>
    member this.TopKBy(k: int, by: Expr, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyExpr(Expr.Col(this.Name).TopKBy(k, by, r))

    member this.TopKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        this.ApplyExpr(Expr.Col(this.Name).TopKBy(k, by, ?reverse=reverse))

    /// <summary>
    /// Get bottom k elements of this Series, sorted by another Series.
    /// </summary>
    member this.BottomKBy(k: int, by: Series, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyBinaryExpr(by, fun me other -> me.BottomKBy(k, other, r))
    /// <summary>
    /// Get bottom k elements of this Series, sorted by another Expr.
    /// </summary>
    member this.BottomKBy(k: int, by: Expr, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyExpr(Expr.Col(this.Name).BottomKBy(k, by, r))
    member this.BottomKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        this.ApplyExpr(Expr.Col(this.Name).BottomKBy(k, by, ?reverse=reverse))
    // ==========================================
    // Static Constructors
    // ==========================================
    /// <summary>
    /// Supports:
    /// - Primitives ('T)
    /// - Option types ('T option)
    /// - ValueOption types ('T voption)
    /// Create a Series directly from a ReadOnlySpan.
    /// No memory allocation, pure FFI zero-copy!
    /// </summary>
    static member create(name: string, data: ReadOnlySpan<'T>) =
        let handle = SeriesFactory.CreateSpan(name, data)
        new Series(handle)

    /// <summary>
    /// Create a Series directly from a Span.
    /// </summary>
    static member create(name: string, data: Span<'T>) =
        let roSpan = Span<'T>.op_Implicit data
        
        let handle = SeriesFactory.CreateSpan(name, roSpan)
        new Series(handle)

    /// <summary>
    /// Create a Series from array
    /// </summary>
    static member create(name: string, data: 'T[]) =
        let handle = SeriesFactory.CreateSpan(name, ReadOnlySpan<'T> data)
        new Series(handle)

    /// <summary>
    /// Create a Series from any sequence (List, Seq, etc.).
    /// This will allocate memory via Seq.toArray.
    /// </summary>
    static member create(name: string, data: seq<'T>) =
        let arr = Seq.toArray data
        let handle = SeriesFactory.CreateSpan(name, ReadOnlySpan<'T>(arr))
        new Series(handle)
    
    // -------------------------------------------------------------------------
    // Fixed Size List / Array (Matrix)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a FixedSizeList Series from a 2D Array (Matrix).
    /// Shape: [Rows, Width] -> Array[Width]
    /// Supported Types: Primitives, Decimal, Int128
    /// </summary>
    static member ofArray2D<'T
        when 'T : struct 
        and 'T : unmanaged
        and 'T :> ValueType   
        and 'T : (new : unit -> 'T)> 
        (name: string, data: 'T[,]) =
            new Series(PolarsWrapper.SeriesNewFixedArray(name, data))
    // ========================================================================
    // Unified Entry Points (Delegating to SeriesFactory)
    // ========================================================================
    /// <summary>
    /// High-performance creation from any sequence. 
    /// Supports nested lists, structs, and F# Options.
    /// </summary>
    static member ofSeq<'T>(name: string, data: seq<'T>) : Series =
        let arrowArray = ArrowConverter.Build data
        
        let handle = ArrowFfiBridge.ImportSeries(name, arrowArray)
        
        new Series(handle)

    /// <summary>
    /// Convert Series to a typed sequence of Options.
    /// Uses high-performance Arrow reader (Zero-Copy).
    /// Supports: Primitives, String, DateTime, DateOnly, TimeOnly, List, Struct.
    /// </summary>
    member this.AsSeq<'T>() : seq<'T option> =
        seq {
            use cArray = PolarsWrapper.SeriesToArrow this.Handle
            
            let accessor = ArrowReader.GetSeriesAccessor<'T> cArray
            let len = cArray.Length

            for i in 0 .. len - 1 do
                let valObj = accessor.Invoke i
                if isNull valObj then 
                    None 
                else 
                    Some(unbox<'T> valObj)
        }
    /// <summary>
    /// Get values as a list (forces evaluation).
    /// </summary>
    member this.ToList<'T>() = this.AsSeq<'T>() |> Seq.toList
    /// <summary>
    /// Create a Series from a sequence of Options (F# style nullables).
    /// Automatically handles all supported types (int, float, string, datetime, etc.)
    /// </summary>
    static member ofOptionSeq<'T>(name: string, data: seq<'T option>) : Series =
        Series.create(name, data)

    /// <summary>
    /// Create a Series from a sequence of ValueOptions (Struct nullables).
    /// Automatically handles all supported types.
    /// </summary>
    static member ofVOptionSeq<'T>(name: string, data: seq<'T voption>) : Series =
        Series.create(name, data)

    // --- Scalar Access ---
    
    /// <summary> Get value as Int64 Option. Handles Int32/Int64 etc. </summary>
    member _.Int(index: int) : int64 option = 
        PolarsWrapper.SeriesGetInt(handle, int64 index) |> Option.ofNullable

    member _.Int128(index: int) : Int128 option = 
        PolarsWrapper.SeriesGetInt128(handle, int64 index) |> Option.ofNullable

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

    // Temporal Type
    member _.Date(index: int) : DateOnly option = 
        PolarsWrapper.SeriesGetDate(handle, int64 index) |> Option.ofNullable

    member _.Time(index: int) : TimeOnly option = 
        PolarsWrapper.SeriesGetTime(handle, int64 index) |> Option.ofNullable

    member _.DateTime(index: int) : DateTime option = 
        PolarsWrapper.SeriesGetDatetime(handle, int64 index) |> Option.ofNullable

    member _.Duration(index: int) : TimeSpan option = 
        PolarsWrapper.SeriesGetDuration(handle, int64 index) |> Option.ofNullable
    // --- Aggregations (Returning Series of len 1) ---
    member this.First() = this.ApplyExpr(Expr.Col(this.Name).First())
    member this.Last() = this.ApplyExpr(Expr.Col(this.Name).Last())
    member this.Sum() = new Series(PolarsWrapper.SeriesSum handle)
    member this.Mean() = new Series(PolarsWrapper.SeriesMean handle)
    member this.Min() = new Series(PolarsWrapper.SeriesMin handle)
    member this.Max() = new Series(PolarsWrapper.SeriesMax handle)
    member this.Product() = this.ApplyExpr(Expr.Col(this.Name).Product())
    // ==========================================
    // Statistical Ops
    // ==========================================
    member this.Count() = this.ApplyExpr(Expr.Col(this.Name).Count())
    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    /// <returns>A new <see cref="Series"/> containing the Std (length 1).</returns>
    member this.Std(?ddof: int) = 
        let d = defaultArg ddof 1
        this.ApplyExpr(Expr.Col(this.Name).Std d)

    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    /// <returns>A new <see cref="Series"/> containing the Var (length 1).</returns>
    member this.Var(?ddof: int) = 
        let d = defaultArg ddof 1
        this.ApplyExpr(Expr.Col(this.Name).Var d)

    /// <summary>
    /// Get the median.
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the Median (length 1).</returns>
    member this.Median() = 
        this.ApplyExpr(Expr.Col(this.Name).Median())
    /// <summary>
    /// Get the mode.
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the Mode (length 1).</returns>
    member this.Mode() = 
        this.ApplyExpr(Expr.Col(this.Name).Mode()) 
    /// <summary>
    /// Get the Skew.
    /// </summary>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> containing the Skew (length 1).</returns>
    member this.Skew(?bias:bool) = 
        let b = defaultArg bias true
        this.ApplyExpr(Expr.Col(this.Name).Skew b)
    /// <summary>
    /// Get the Kurtosis.
    /// </summary>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> containing the Skew (length 1).</returns>
    member this.Kurtosis(?fisher:bool,?bias:bool) = 
        let b = defaultArg bias true
        let f = defaultArg fisher true
        this.ApplyExpr(Expr.Col(this.Name).Kurtosis(f,b))
    /// <summary>
    /// Get the quantile.
    /// </summary>
    /// <param name="q">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method ("nearest", "higher", "lower", "midpoint", "linear"). Default "linear".</param>
    member this.Quantile(q: float, ?interpolation: QuantileMethod) =
        this.ApplyExpr(Expr.Col(this.Name).Quantile(q, ?interpolation=interpolation))
    /// <summary>
    /// Computes percentage change between values. 
    /// Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element. 
    /// Computes the change from the previous row by default.
    /// </summary>
    /// <param name="n">Periods to shift for forming percent change.Default:1</param>
    /// <returns>A new <see cref="Series"/> containing the Var (length 1).</returns>
    member this.PctChange(?n: int) = 
        let nd = defaultArg n 1
        this.ApplyExpr(Expr.Col(this.Name).PctChange nd)
    /// <summary>
    /// Assign ranks to data, dealing with ties appropriately.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="descending">Rank in descending order.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    /// <returns></returns>
    member this.Rank(?method: RankMethod, ?descending: bool, ?seed: uint64) = 
        this.ApplyExpr(Expr.Col(this.Name).Rank(?method=method, ?descending=descending, ?seed=seed))
    /// <summary>
    /// Count the occurrences of unique values.
    /// Similar to SQL `GROUP BY val COUNT(*)`.
    /// </summary>
    /// <param name="sort">Sort the output by count in descending order. Default is true.</param>
    /// <param name="parallel">Execute in parallel. Default is true.</param>
    /// <param name="name">The name of the count column. Default is "count".</param>
    /// <param name="normalize">If true, the count column will contain probabilities instead of counts. Default is false.</param>
    member this.ValueCounts(?sort: bool, ?paralleling: bool, ?name: string, ?normalize: bool) =
        let sort = defaultArg sort true
        let paralleling = defaultArg paralleling true
        let name = defaultArg name "count"
        let normalize = defaultArg normalize false
        
        let dfHandle = PolarsWrapper.SeriesValueCounts(this.Handle, sort, paralleling, name, normalize)
        new DataFrame(dfHandle)
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// Get an array with the cumulative sum computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumSum(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumSum(?reverse=reverse))
    /// <summary>
    /// Get an array with the cumulative min computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumMin(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumMin(?reverse=reverse))
    /// <summary>
    /// Get an array with the cumulative max computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumMax(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumMax(?reverse=reverse))
    /// <summary>
    /// Get an array with the cumulative prod computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumProd(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumProd(?reverse=reverse))    
    /// <summary>
    /// Get an array with the cumulative count computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumCount(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumCount(?reverse=reverse)) 
    // ==========================================
    // EWM Functions
    // ==========================================
    /// <summary>
    /// Compute exponentially-weighted moving average.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM mean.</returns>
    member this.EwmMean(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).EwmMean(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Compute exponentially-weighted moving standard deviation.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM standard deviation.</returns>
    member this.EwmStd(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).EwmStd(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Compute exponentially-weighted moving variance.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM variance.</returns>
    member this.EwmVar(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).EwmVar(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Compute exponentially-weighted moving average based on a temporal or index column.
    /// </summary>
    /// <param name="by">
    /// The column used to determine the distance between observations.
    /// <para>Supported data types: <c>Date</c>, <c>DateTime</c>, <c>UInt64</c>, <c>UInt32</c>, <c>Int64</c>, or <c>Int32</c>.</para>
    /// </param>
    /// <param name="halfLife">
    /// The unit over which an observation decays to half its value.
    /// <para>Supported string formats:</para>
    /// <list type="bullet">
    ///     <item><term>Time units</term><description><c>ns</c> (nanosecond), <c>us</c> (microsecond), <c>ms</c> (millisecond), <c>s</c> (second), <c>m</c> (minute), <c>h</c> (hour), <c>d</c> (day), <c>w</c> (week).</description></item>
    ///     <item><term>Index units</term><description><c>i</c> (index count). Example: <c>"2i"</c> means decay by half every 2 index steps.</description></item>
    ///     <item><term>Compound</term><description>Example: <c>"3d12h4m25s"</c>.</description></item>
    /// </list>
    /// <para>
    /// <b>Warning:</b> <paramref name="halfLife"/> is treated as a constant duration. 
    /// Calendar durations such as months (<c>mo</c>) or years (<c>y</c>) are <b>NOT</b> supported because they vary in length. 
    /// Please express such durations in hours (e.g. use <c>'730h'</c> instead of <c>'1mo'</c>).
    /// </para>
    /// </param>
    /// <returns>A new expression representing the time/index-based EWM mean.</returns>
    member this.EwmMeanBy(by:Expr,halfLife:string) =
        this.ApplyExpr(Expr.Col(this.Name).EwmMeanBy(by=by,halfLife=halfLife))

    // ==========================================
    // Operators (Arithmetic) 
    // ==========================================

    static member (+) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesAdd(lhs.Handle, rhs.Handle))
    static member (-) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesSub(lhs.Handle, rhs.Handle))
    static member (*) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesMul(lhs.Handle, rhs.Handle))
    static member (/) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesDiv(lhs.Handle, rhs.Handle))
    static member (%) (lhs: Series, rhs: Series) = lhs.Mod rhs

    // --- Operators (Comparison) ---

    static member (.=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesEq(lhs.Handle, rhs.Handle))
    static member (!=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesNeq(lhs.Handle, rhs.Handle))
    static member (.>) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLt(lhs.Handle, rhs.Handle))

    static member (.>=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLtEq(lhs.Handle, rhs.Handle))

    // --- Broadcasting Helpers (Scalar Ops) ---
    static member (+) (lhs: Series, rhs: int) = lhs + Series.create("lit", [rhs])
    static member (+) (lhs: Series, rhs: double) = lhs + Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: int) = lhs - Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: double) = lhs - Series.create("lit", [rhs])
    
    static member (*) (lhs: Series, rhs: int) = lhs * Series.create("lit", [rhs])
    static member (*) (lhs: Series, rhs: double) = lhs * Series.create("lit", [rhs])
    
    static member (/) (lhs: Series, rhs: int) = lhs / Series.create("lit", [rhs])
    static member (/) (lhs: Series, rhs: double) = lhs / Series.create("lit", [rhs])
    static member (%) (lhs: Series, rhs: int) = lhs.Mod rhs
    static member (<<<) (lhs: Series, rhs: int) = lhs.BitLeftShift rhs
    static member (>>>) (lhs: Series, rhs: int) = lhs.BitRightShift rhs

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
    // Unified Accessor (Fast Path + Universal Path)
    // ==========================================

    /// <summary>
    /// Get an item at the specified index.
    /// Supports primitives (int, float, bool, string) via fast native path,
    /// and complex types (Struct, List, DateTime) via Arrow infrastructure.
    /// </summary>
    member this.GetValue<'T>(index: int64) : 'T =
        let len = this.Length
        if index < 0L || index >= len then
            raise (IndexOutOfRangeException(sprintf "Index %d is out of bounds for Series length %d." index len))

        // Consistent Null Check
        if PolarsWrapper.SeriesIsNullAt(handle, index) then
            Unchecked.defaultof<'T>
        else
            // 2. Getvalue
            let t = typeof<'T>
            
            // --- Integer Family ---
            if t = typeof<int> || t = typeof<int option> || t = typeof<Nullable<int>> then
                let v = int (PolarsWrapper.SeriesGetInt(handle, index).Value)
                if t = typeof<int option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<int64> || t = typeof<int64 option> || t = typeof<Nullable<int64>> then
                let v = PolarsWrapper.SeriesGetInt(handle, index).Value
                if t = typeof<int64 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<Int128> || t = typeof<Int128 option> || t = typeof<Nullable<Int128>> then
                let v = PolarsWrapper.SeriesGetInt128(handle, index).Value
                if t = typeof<Int128 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Float Family ---
            else if t = typeof<double> || t = typeof<double option> || t = typeof<Nullable<double>> then
                let v = PolarsWrapper.SeriesGetDouble(handle, index).Value
                if t = typeof<double option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<float32> || t = typeof<float32 option> || t = typeof<Nullable<float32>> then
                let v = float32 (PolarsWrapper.SeriesGetDouble(handle, index).Value)
                if t = typeof<float32 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<Half> || t = typeof<Half> || t = typeof<Nullable<Half>> then
                let v = PolarsWrapper.SeriesGetDouble(handle, index).Value
                if t = typeof<Half option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Boolean ---
            else if t = typeof<bool> || t = typeof<bool option> || t = typeof<Nullable<bool>> then
                let v = PolarsWrapper.SeriesGetBool(handle, index).Value
                if t = typeof<bool option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- String ---
            else if t = typeof<string> || t = typeof<string option> then
                let v = PolarsWrapper.SeriesGetString(handle, index)
                if t = typeof<string option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Decimal ---
            else if t = typeof<decimal> || t = typeof<decimal option> || t = typeof<Nullable<decimal>> then
                let v = PolarsWrapper.SeriesGetDecimal(handle, index).Value
                if t = typeof<decimal option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Temporal ---
            else if t = typeof<DateOnly> || t = typeof<DateOnly option> || t = typeof<Nullable<DateOnly>> then
                let v = PolarsWrapper.SeriesGetDate(handle, index).Value
                if t = typeof<DateOnly option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<TimeOnly> || t = typeof<TimeOnly option> || t = typeof<Nullable<TimeOnly>> then
                let v = PolarsWrapper.SeriesGetTime(handle, index).Value
                if t = typeof<TimeOnly option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>
                
            else if t = typeof<TimeSpan> || t = typeof<TimeSpan option> || t = typeof<Nullable<TimeSpan>> then
                let v = PolarsWrapper.SeriesGetDuration(handle, index).Value
                if t = typeof<TimeSpan option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Complex Types (Arrow Fallback) ---
            else
                use slicedHandle = PolarsWrapper.SeriesSlice(handle, index, 1L)
                use dfHandle = PolarsWrapper.SeriesToFrame slicedHandle
                use batch = ArrowFfiBridge.ExportDataFrame dfHandle
                let column = batch.Column(0)
                ArrowReader.ReadItem<'T>(column, 0)
    /// <summary>
    /// Get a value as an F# List ('T list).
    /// Automatically handles conversion from .NET List (ResizeArray).
    /// </summary>
    member this.GetList<'Elem>(index: int64) : 'Elem list =
        let netList = this.GetValue<ResizeArray<'Elem>> index
        
        if isNull netList then 
            []
        else 
            netList |> List.ofSeq
                
    /// <summary>
    /// [Indexer] Access value at specific index as boxed object.
    /// Syntax: series.[index]
    /// </summary>
    member this.Item (index: int) : obj =
        let idx = int64 index
        
        match this.DataType with
        | DataType.Boolean -> box (this.GetValue<bool option> idx) 
        
        | DataType.Int8 -> box (this.GetValue<int8 option> idx)
        | DataType.Int16 -> box (this.GetValue<int16 option> idx)
        | DataType.Int32 -> box (this.GetValue<int32 option> idx)
        | DataType.Int64 -> box (this.GetValue<int64 option> idx)
        | DataType.Int128 -> box (this.GetValue<Int128 option> idx)
        
        | DataType.UInt8 -> box (this.GetValue<uint8 option> idx)
        | DataType.UInt16 -> box (this.GetValue<uint16 option> idx)
        | DataType.UInt32 -> box (this.GetValue<uint32 option> idx)
        | DataType.UInt64 -> box (this.GetValue<uint64 option> idx)
        | DataType.UInt128 -> box (this.GetValue<UInt128 option> idx)

        | DataType.Float16 -> box (this.GetValue<Half option> idx)
        | DataType.Float32 -> box (this.GetValue<float32 option> idx)
        | DataType.Float64 -> box (this.GetValue<double option> idx)
        
        | DataType.Decimal _ -> box (this.GetValue<decimal option> idx)
        
        | DataType.String -> box (this.GetValue<string option> idx)
        
        | DataType.Date -> box (this.GetValue<DateOnly option> idx)
        | DataType.Time -> box (this.GetValue<TimeOnly option> idx)
        | DataType.Datetime _ -> box (this.GetValue<DateTime option> idx)
        | DataType.Duration _ -> box (this.GetValue<TimeSpan option> idx)
        
        | DataType.Binary -> box (this.GetValue<byte[] option> idx)

        // Complex Type
        | DataType.List _ -> this.GetValue<obj> idx
        | DataType.Struct _ -> this.GetValue<obj> idx
        | DataType.Array _ -> this.GetValue<obj> idx
        
        | _ -> failwithf "Indexer not fully implemented for type: %A" this.DataType
    /// <summary>
    /// Get an item as an F# Option.
    /// Ideal for safe handling of nulls in Polars series.
    /// </summary>
    member this.GetValueOption<'T>(index: int64) : 'T option =
        this.GetValue<'T option> index
    // ==========================================
    // Interop 
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
    member this.FromArrow(name:string,arrowArray:IArrowArray) : Series = 
        new Series(ArrowFfiBridge.ImportSeries(name,arrowArray))
    member this.ToArray<'T>() =
        let col = this.ToArrow()
        ArrowReader.ReadColumn<'T> col
    /// <summary>
    /// Generate zero-copy ReadOnlySpan from a numeric Series.
    /// </summary>
    member this.AsReadOnlySpan<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>() : ReadOnlySpan<'T> =
        
        let checkDataType () =
            let dataType = this.DataType
            match dataType with
            | DataType.String | DataType.Categorical ->
                raise (InvalidOperationException(
                    $"Cannot create Tensor/Span from a {dataType} Series. \
                    Machine learning models and Spans require numeric inputs. \
                    Please use Polars string manipulation (.Str) or categorical \
                    casting to encode your strings into numbers first."
                ))
            | _ -> ()
            
        checkDataType ()

        ArrowTensorInterop.AsReadOnlySpan<'T> this.Handle
    /// <summary>
    /// Generates a zero-copy Tensor representation from the Series.
    /// Automatically infers the shape: 
    /// - 1D [Rows] for flat numeric columns.
    /// - 2D [Rows, ListSize] for List/Array columns (Perfect for Embeddings or Feature Matrices).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only (e.g., float, int)</typeparam>
    /// <returns>A ReadOnlyTensorSpan representing the underlying Arrow memory.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the Series contains nulls or is not numeric.</exception>
    member this.AsTensorSpan<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>() =  
        ArrowTensorInterop.AsTensorSpan<'T> this.Handle
    /// <summary>
    /// Generates a zero-copy N-Dimensional Tensor representation from the Series.
    /// Allows explicit reshaping of the underlying memory (e.g., for 3D/4D image matrices).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only</typeparam>
    /// <param name="shape">The desired dimensions. Total elements must match the underlying memory.</param>
    /// <returns>A reshaped ReadOnlyTensorSpan representing the underlying Arrow memory.</returns>
    /// <exception cref="ArgumentException">Thrown if the shape's total elements do not match the Series data.</exception>
    member this.AsTensorSpan<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>(shape:ReadOnlySpan<nativeint>) =
        ArrowTensorInterop.AsTensorSpan<'T>(this.Handle, shape);
    /// <summary>
    /// Generates a zero-copy, transposed 2D Tensor representation from a List or Array Series.
    /// By manipulating memory strides, it returns an (N x M) view of an (M x N) matrix without moving bytes.
    /// </summary>
    /// <remarks>
    /// This is useful when bridging with external Machine Learning libraries 
    /// (like ONNX or native C++ math backends) that expect Column-Major order or specifically require a transposed matrix.
    /// </remarks>
    /// <typeparam name="T">Unmanaged Type Only (e.g., float, double, int).</typeparam>
    /// <returns>A transposed <see cref="ReadOnlyTensorSpan{T}"/> pointing to the original Arrow memory.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the Series is not a 2D List/Array, contains null values, or if the generic type <typeparamref name="T"/> 
    /// does not match the underlying physical Arrow memory type.
    /// </exception>
    member this.AsTransposedTensorSpan<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>() = 
        ArrowTensorInterop.AsTransposedTensorSpan<'T> this.Handle
    /// <summary>
    /// Deep copies the underlying Arrow memory into a managed .NET <see cref="Tensor{T}"/> on the garbage-collected heap.
    /// Automatically infers the rank and dimensions: 1D data is promoted to a [N, 1] column vector, 
    /// while 2D nested data becomes an [N, M] matrix.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="AsTensorSpan{T}()"/>, this method allocates memory and performs a safe physical copy. 
    /// The returned Tensor is completely independent of the Polars engine, making it safe to pass across threads 
    /// or await inside asynchronous (async/Task) workflows.
    /// </remarks>
    /// <typeparam name="T">The unmanaged primitive type of the tensor (e.g., float, int).</typeparam>
    /// <returns>A new managed <see cref="Tensor{T}"/> containing the copied data.</returns>
    member this.AsTensor<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>() =
        ArrowTensorInterop.AsTensor<'T> this.Handle;

    /// <summary>
    /// Deep copies the underlying Arrow memory into a managed .NET <see cref="Tensor{T}"/> with an explicit N-dimensional shape.
    /// </summary>
    /// <remarks>
    /// This method performs a safe physical copy to the managed heap. It is highly useful when reshaping 
    /// flat or nested Arrow arrays into higher-dimensional structures (e.g., 3D/4D image batches like [Batch, Channel, Height, Width]) 
    /// required by Machine Learning models.
    /// </remarks>
    /// <typeparam name="T">The unmanaged primitive type of the tensor.</typeparam>
    /// <param name="shape">The target dimensions. The total number of elements must match the underlying Arrow memory length.</param>
    /// <returns>A new managed <see cref="Tensor{T}"/> reshaped to the specified dimensions.</returns>
    /// <exception cref="ArgumentException">Thrown if the total elements required by the shape do not match the data length.</exception>
    member this.AsTensor<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>(shape:ReadOnlySpan<nativeint>) =
        ArrowTensorInterop.AsTensor<'T>(this.Handle, shape);

    /// <summary>
    /// Extracts the raw unmanaged pointer and shape metadata of the underlying physical Arrow memory.
    /// Designed for zero-copy integrations with native C++ Machine Learning backends like ONNX Runtime or TorchSharp.
    /// </summary>
    /// <remarks>
    /// DANGER (LIFECYCLE WARNING): This is a zero-copy operation. The returned <see cref="IntPtr"/> points 
    /// directly to native memory managed by the Rust Polars engine. It is ONLY valid as long as this <see cref="Series"/> 
    /// instance remains alive. You MUST ensure the Series is not garbage collected or explicitly disposed 
    /// while the native pointer is still in use by an external FFI library, otherwise it will result in a fatal segmentation fault.
    /// </remarks>
    /// <typeparam name="T">The unmanaged primitive type.</typeparam>
    /// <returns>A tuple containing the raw <see cref="IntPtr"/> to the first element, and a <c>long[]</c> representing the tensor shape.</returns>
    member this.AsUnmanagedTensor<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>() =
        ArrowTensorInterop.GetNativePointers<'T> this.Handle;
    /// <summary>
    /// Converts an N-Dimensional .NET Tensor into a Polars Series.
    /// Automatically infers the rank and dynamically wraps the data into Polars native types:
    /// - 1D Tensors map to flat primitive columns (e.g., Float32, Int32).
    /// - N-D Tensors map to nested Array columns (e.g., Array[f32, (H, W)]).
    /// </summary>
    /// <typeparam name="T">The unmanaged primitive type of the tensor (e.g., float, int).</typeparam>
    /// <param name="name">The column name for the newly created Series.</param>
    /// <param name="tensor">The source TensorSpan. Supports sliced and transposed views safely.</param>
    /// <returns>A new Polars Series instance encapsulating the tensor data.</returns>
    /// <remarks>
    /// For memory safety against non-contiguous tensor views (like transpositions), 
    /// this method performs a memory materialization (Flatten) before mapping to Arrow's C Data Interface. 
    /// </remarks>
    static member ofTensor<'T when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)>(name:string,tensor: ReadOnlyTensorSpan<'T> ) = 
        new Series(ArrowTensorInterop.ImportTensor(name, tensor))
    
    member this.Show() =
        this.ToFrame().Show()
and SeriesDtNameSpace(parent: Series) =
    
    // Helper: col("Name").Dt.Op(...)
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Extraction ---
    
    member _.Year() = apply (fun e -> e.Dt.Year())
    member _.Quarter() = apply (fun e -> e.Dt.Quarter())
    member _.Month() = apply (fun e -> e.Dt.Month())
    member _.Day() = apply (fun e -> e.Dt.Day())
    member _.Hour() = apply (fun e -> e.Dt.Hour())
    member _.Minute() = apply (fun e -> e.Dt.Minute())
    member _.Second() = apply (fun e -> e.Dt.Second())
    member _.Millisecond() = apply (fun e -> e.Dt.Millisecond())
    member _.Microsecond() = apply (fun e -> e.Dt.Microsecond())
    member _.Nanosecond() = apply (fun e -> e.Dt.Nanosecond())
    member _.OrdinalDay() = apply (fun e -> e.Dt.OrdinalDay())
    member _.Weekday() = apply (fun e -> e.Dt.Weekday())
    member _.Date() = apply (fun e -> e.Dt.Date())
    member _.Time() = apply (fun e -> e.Dt.Time())

    // --- Formatting ---

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        apply (fun e -> e.Dt.ToString format)

    /// <summary> Default ISO format. </summary>
    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"

    // --- Manipulation ---

    member _.Truncate(every: string) = 
        apply (fun e -> e.Dt.Truncate every)

    member _.Round(every: string) = 
        apply (fun e -> e.Dt.Round every)

    member _.OffsetBy(duration: string) =
        apply (fun e -> e.Dt.OffsetBy duration)
    member _.OffsetBy(duration: Expr) =
        apply (fun e -> e.Dt.OffsetBy duration)

    // --- Conversion ---

    member _.TimestampMicros() = apply (fun e -> e.Dt.TimestampMicros())
    member _.TimestampMillis() = apply (fun e -> e.Dt.TimestampMillis())
    member _.Combine(time:Expr,timeUnit:TimeUnit) = apply (fun e -> e.Dt.Combine(time,timeUnit))

    // --- TimeZone ---

    member _.ConvertTimeZone(timeZone: string) =
        apply (fun e -> e.Dt.ConvertTimeZone timeZone)

    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: string, ?nonExistent: string) =
        apply (fun e -> e.Dt.ReplaceTimeZone(timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent))

    member this.ReplaceTimeZone(timeZone: string, ?ambiguous, ?nonExistent) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent)

    // --- Business Days ---

    /// <summary>
    /// Add business days (using integer).
    /// </summary>
    member _.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        apply (fun e -> 
            e.Dt.AddBusinessDays(
                n, 
                ?weekMask=weekMask, 
                ?holidays=holidays, 
                ?roll=roll
            )
        )

    /// <summary>
    /// Is Business Day check.
    /// </summary>
    member _.IsBusinessDay(?weekMask, ?holidays) =
        apply (fun e -> 
            e.Dt.IsBusinessDay(?weekMask=weekMask, ?holidays=holidays)
        )

and SeriesStrNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = apply (fun e -> e.Str.ToUpper())

    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = apply (fun e -> e.Str.ToLower())

    /// <summary> Get length in bytes. </summary>
    member _.Len() = apply (fun e -> e.Str.Len())

    /// <summary> Slice the string. </summary>
    member _.Slice(offset: int64, length: uint64) = 
        apply (fun e -> e.Str.Slice(offset, length))

    /// <summary> Replace all occurrences of a pattern. </summary>
    member _.ReplaceAll(pattern: string, value: string, ?useRegex: bool) =
        apply (fun e -> e.Str.ReplaceAll(pattern, value, ?useRegex=useRegex))

    /// <summary> Extract the target capture group from regex pattern. </summary>
    member _.Extract(pattern: string, groupIndex: int) =
        apply (fun e -> e.Str.Extract(pattern, groupIndex))

    /// <summary> Check if string contains pattern. </summary>
    member _.Contains(pat: string) = 
        apply (fun e -> e.Str.Contains pat)

    /// <summary> Split string by separator. Returns a List column. </summary>
    member _.Split(separator: string) = 
        apply (fun e -> e.Str.Split separator)

    // --- Trimming ---

    /// <summary> Remove leading and trailing characters. </summary>
    member _.Strip(?matches: string) = 
        apply (fun e -> e.Str.Strip(?matches=matches))

    /// <summary> Remove leading characters. </summary>
    member _.LStrip(?matches: string) = 
        apply (fun e -> e.Str.LStrip(?matches=matches))

    /// <summary> Remove trailing characters. </summary>
    member _.RStrip(?matches: string) = 
        apply (fun e -> e.Str.RStrip(?matches=matches))

    member _.StripPrefix(prefix: string) = 
        apply (fun e -> e.Str.StripPrefix prefix)

    member _.StripSuffix(suffix: string) = 
        apply (fun e -> e.Str.StripSuffix suffix)

    // --- Checks ---

    member _.StartsWith(prefix: string) = 
        apply (fun e -> e.Str.StartsWith prefix)

    member _.EndsWith(suffix: string) = 
        apply (fun e -> e.Str.EndsWith suffix)

    // --- Parsing ---

    /// <summary> Parse string to Date. </summary>
    member _.ToDate(format: string) = 
        apply (fun e -> e.Str.ToDate format)

    /// <summary> Parse string to Datetime. </summary>
    member _.ToDatetime(format: string) = 
        apply (fun e -> e.Str.ToDatetime format)

and SeriesListNameSpace(parent: Series) =
    
    // Unary helper
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Unary Ops (Forward to Expr.List) ---
    
    member _.First() = apply (fun e -> e.List.First())
    member _.Get(index: int) = apply (fun e -> e.List.Get index)
    member _.Join(sep: string) = apply (fun e -> e.List.Join sep)
    member _.Len() = apply (fun e -> e.List.Len())
    member _.Sum() = apply (fun e -> e.List.Sum())
    member _.Min() = apply (fun e -> e.List.Min())
    member _.Max() = apply (fun e -> e.List.Max())
    member _.Mean() = apply (fun e -> e.List.Mean())
    member _.Reverse() = apply (fun e -> e.List.Reverse())
    
    member _.Sort(?descending, ?nullsLast, ?maintainOrder) =
        apply (fun e -> e.List.Sort(?descending=descending, ?nullsLast=nullsLast, ?maintainOrder=maintainOrder))

    // --- Binary Ops ---

    /// <summary>
    /// Concat with another Series.
    /// Logic: [this_val, other_val]
    /// </summary>
    member _.Concat(other: Series) =
        parent.ApplyBinaryExpr(other, (fun l r -> l.List.Concat r))

    // --- Search ---

    member _.Contains(item: int) = apply (fun e -> e.List.Contains item)
    member _.Contains(item: string) = apply (fun e -> e.List.Contains item)

and SeriesArrayNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Aggregations ---
    member _.Sum() = apply (fun e -> e.Array.Sum())
    member _.Min() = apply (fun e -> e.Array.Min())
    member _.Max() = apply (fun e -> e.Array.Max())
    member _.Mean() = apply (fun e -> e.Array.Mean())
    member _.Median() = apply (fun e -> e.Array.Median())
    
    
    member _.Std(?ddof: int) = 
        apply (fun e -> e.Array.Std(?ddof=ddof))
    
    member _.Var(?ddof: int) = 
        apply (fun e -> e.Array.Var(?ddof=ddof))

    // --- Boolean / Search ---

    member _.Any() = apply (fun e -> e.Array.Any())
    member _.All() = apply (fun e -> e.Array.All())

    /// <summary> Check if array contains an Item (Expr). </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    /// <summary> Check if array contains a literal string. </summary>
    member _.Contains(item: string, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    /// <summary> Check if array contains a literal int. </summary>
    member _.Contains(item: int, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    // --- Operations ---

    member _.Unique(?stable: bool) = 
        apply (fun e -> e.Array.Unique(?stable=stable))

    member _.Join(separator: string, ?ignoreNulls: bool) =
        apply (fun e -> e.Array.Join(separator, ?ignoreNulls=ignoreNulls))

    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        apply (fun e -> 
            e.Array.Sort(
                ?descending=descending, 
                ?nullsLast=nullsLast, 
                ?maintainOrder=maintainOrder
            )
        )

    member _.Reverse() = apply (fun e -> e.Array.Reverse())

    member _.ArgMin() = apply (fun e -> e.Array.ArgMin())
    member _.ArgMax() = apply (fun e -> e.Array.ArgMax())

    member _.Explode(?emptyAsNull:bool,?keepNulls:bool) = apply (fun e -> e.Array.Explode(?emptyAsNull=emptyAsNull,?keepNulls=keepNulls))

    // --- Indexing ---

    member _.Get(index: Expr, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob=nullOnOob))

    member _.Get(index: int, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob=nullOnOob))

    // --- Conversion ---

    /// <summary> Convert to variable length List. </summary>
    member _.ToList() = apply (fun e -> e.Array.ToList())

    /// <summary> Convert to Struct. </summary>
    member _.ToStruct() = apply (fun e -> e.Array.ToStruct())

and SeriesStructNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        apply (fun e -> e.Struct.Field name)

    /// <summary> Retrieve a field from the struct by index. </summary>
    member _.Field(index: int) = 
        apply (fun e -> e.Struct.Field index)

    /// <summary> Rename the fields of the struct. </summary>
    member _.RenameFields(names: string list) =
        apply (fun e -> e.Struct.RenameFields names)

    /// <summary> Convert struct to JSON string. </summary>
    member _.JsonEncode() = 
        apply (fun e -> e.Struct.JsonEncode())
    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    member _.Unnest() =
        let dfHandle = PolarsWrapper.SeriesStructUnnest parent.Handle
        new DataFrame(dfHandle)
// --- Frames ---

/// <summary>
/// An eager DataFrame holding data in memory.
/// <para>
/// DataFrames are 2D tabular data structures with named columns of potentially different types.
/// </para>
/// </summary>
and DataFrame(handle: DataFrameHandle) =
    let backingResources = ResizeArray<IDisposable>()
    member this.Clone() = new DataFrame(PolarsWrapper.CloneDataFrame handle)
    member internal this.CloneHandle() = PolarsWrapper.CloneDataFrame handle
    member _.Handle = handle
    
    member internal this.HoldResource(resource: IDisposable) =
        if not (isNull resource) then
            backingResources.Add resource

    member this.Dispose() =
        if not this.Handle.IsInvalid then 
            this.Handle.Dispose()

        for res in backingResources do
            res.Dispose()
            
        backingResources.Clear()
        GC.SuppressFinalize this

    interface IDisposable with
        member this.Dispose() = 
            this.Dispose()
    /// <summary> Create a DataFrame from a list of Series. </summary>
    static member create(series: Series list) : DataFrame =
        let handles = 
            series 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
            
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    /// <summary> Create a DataFrame from an array of Series. </summary>
    static member create([<ParamArray>] series: Series[]) : DataFrame =
        let handles = series |> Array.map (fun s -> s.Handle)
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    // ---------------------------------------------------------
    // Read CSV (File / Cloud / Glob)
    // ---------------------------------------------------------

    /// <summary>
    /// Read a DataFrame from a CSV file.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanCsv and collects the result. 
    /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanCsv directly.
    /// </para>
    /// </summary>
    static member ReadCsv(
        path: string,
        ?columns: seq<string>,
        ?hasHeader: bool,
        ?separator: char,
        ?quoteChar: char,
        ?eolChar: char,
        ?ignoreErrors: bool,
        ?tryParseDates: bool,
        ?lowMemory: bool,
        ?skipRows: uint64,
        ?nRows: uint64,
        ?inferSchemaLength: uint64,
        ?schema: PolarsSchema,
        ?dtypeOverride:PolarsSchema,
        ?encoding: CsvEncoding,
        ?nullValues: seq<string>,
        ?missingIsNull: bool,
        ?commentPrefix: string,
        ?decimalComma: bool,
        ?truncateRaggedLines: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint64,
        ?cloudOptions: CloudOptions
    ) : DataFrame =
        
        let mutable lf = LazyFrame.ScanCsv(
            path,
            ?schema = schema,
            ?dtypeOverride = dtypeOverride,
            ?hasHeader = hasHeader,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?eolChar = eolChar,
            ?ignoreErrors = ignoreErrors,
            ?tryParseDates = tryParseDates,
            ?lowMemory = lowMemory,
            ?skipRows = skipRows,
            ?nRows = nRows,
            ?inferSchemaLength = inferSchemaLength,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?encoding = encoding,
            ?nullValues = nullValues,
            ?missingIsNull = missingIsNull,
            ?commentPrefix = commentPrefix,
            ?decimalComma = decimalComma,
            ?truncateRaggedLines = truncateRaggedLines,
            ?cloudOptions = cloudOptions
        )

        match columns with
        | Some cols -> 
            let colArray = cols |> Seq.toArray
            if colArray.Length > 0 then
                use sel = new Selector(PolarsWrapper.SelectorCols colArray)
                lf <- lf.Select [sel :> IColumnExpr]
        | None -> ()

        lf.Collect()

    // ---------------------------------------------------------
    // Read CSV (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read a DataFrame from a CSV memory buffer.
    /// </summary>
    static member ReadCsv(
        buffer: byte[],
        ?columns: seq<string>,
        ?hasHeader: bool,
        ?separator: char,
        ?quoteChar: char,
        ?eolChar: char,
        ?ignoreErrors: bool,
        ?tryParseDates: bool,
        ?lowMemory: bool,
        ?skipRows: uint64,
        ?nRows: uint64,
        ?inferSchemaLength: uint64,
        ?schema: PolarsSchema,
        ?dtypeOverride:PolarsSchema,
        ?encoding: CsvEncoding,
        ?nullValues: seq<string>,
        ?missingIsNull: bool,
        ?commentPrefix: string,
        ?decimalComma: bool,
        ?truncateRaggedLines: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint64
    ) : DataFrame =
        
        let mutable lf = LazyFrame.ScanCsv(
            buffer,
            ?schema = schema,
            ?dtypeOverride = dtypeOverride,
            ?hasHeader = hasHeader,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?eolChar = eolChar,
            ?ignoreErrors = ignoreErrors,
            ?tryParseDates = tryParseDates,
            ?lowMemory = lowMemory,
            ?skipRows = skipRows,
            ?nRows = nRows,
            ?inferSchemaLength = inferSchemaLength,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?encoding = encoding,
            ?nullValues = nullValues ,
            ?missingIsNull = missingIsNull,
            ?commentPrefix = commentPrefix,
            ?decimalComma = decimalComma,
            ?truncateRaggedLines = truncateRaggedLines
        )

        match columns with
        | Some cols -> 
            let colArray = cols |> Seq.toArray
            if colArray.Length > 0 then
                use sel = new Selector(PolarsWrapper.SelectorCols colArray)
                lf <- lf.Select [sel :> IColumnExpr]
        | None -> ()

        lf.Collect()

    // ---------------------------------------------------------
    // Read CSV (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read a DataFrame from a CSV memory stream.
    /// </summary>
    static member ReadCsv(
        stream: System.IO.Stream,
        ?columns: seq<string>,
        ?hasHeader: bool,
        ?separator: char,
        ?quoteChar: char,
        ?eolChar: char,
        ?ignoreErrors: bool,
        ?tryParseDates: bool,
        ?lowMemory: bool,
        ?skipRows: uint64,
        ?nRows: uint64,
        ?inferSchemaLength: uint64,
        ?schema: PolarsSchema,
        ?encoding: CsvEncoding,
        ?nullValues: seq<string>,
        ?missingIsNull: bool,
        ?commentPrefix: string,
        ?decimalComma: bool,
        ?truncateRaggedLines: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint64
    ) : DataFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()

        DataFrame.ReadCsv(
            bytes,
            ?columns = columns,
            ?hasHeader = hasHeader,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?eolChar = eolChar,
            ?ignoreErrors = ignoreErrors,
            ?tryParseDates = tryParseDates,
            ?lowMemory = lowMemory,
            ?skipRows = skipRows,
            ?nRows = nRows,
            ?inferSchemaLength = inferSchemaLength,
            ?schema = schema,
            ?encoding = encoding,
            ?nullValues = nullValues,
            ?missingIsNull = missingIsNull,
            ?commentPrefix = commentPrefix,
            ?decimalComma = decimalComma,
            ?truncateRaggedLines = truncateRaggedLines,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset
        )
    /// <summary>
    /// Read a CSV file asynchronously into a DataFrame.
    /// </summary>
    static member ReadCsvAsync
        (
            path: string,
            ?columns: seq<string>,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?skipRows: uint64,
            ?nRows: uint64,
            ?inferSchemaLength: uint64,
            ?schema: PolarsSchema,
            ?encoding: CsvEncoding,
            ?nullValues: seq<string>,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,
            ?cloudOptions: CloudOptions
        ) =
        task {
             return DataFrame.ReadCsv(
                path,
                ?columns = columns,
                ?schema = schema,
                ?hasHeader = hasHeader,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?eolChar = eolChar,
                ?ignoreErrors = ignoreErrors,
                ?tryParseDates = tryParseDates,
                ?lowMemory = lowMemory,
                ?skipRows = skipRows,
                ?nRows = nRows,
                ?inferSchemaLength = inferSchemaLength,
                ?encoding = encoding,
                ?nullValues = nullValues,
                ?missingIsNull = missingIsNull,
                ?commentPrefix = commentPrefix,
                ?decimalComma = decimalComma,
                ?truncateRaggedLines = truncateRaggedLines,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?cloudOptions = cloudOptions
            )
        }
    /// <summary>
    /// [Eager] Create a DataFrame from an IDataReader (e.g. SqlDataReader).
    /// <para>
    /// Uses high-performance streaming ingestion via Apache Arrow.
    /// </para>
    /// </summary>
    /// <param name="reader">The open IDataReader instance.</param>
    /// <param name="batchSize">Number of rows per Arrow batch (default 50,000).</param>
    static member ReadDb(reader: IDataReader, ?batchSize: int) : DataFrame =
        let schema = reader.GetArrowSchema()

        let size = defaultArg batchSize 50_000
        
        let batchStream = reader.ToArrowBatches(size).Prefetch()
        
        let handle = ArrowStreamInterop.ImportEager(batchStream,schema)
        
        if handle.IsInvalid then

            let emptyBatch = new RecordBatch(schema, System.Array.Empty<IArrowArray>(), 0)

            let safeHandle = ArrowFfiBridge.ImportDataFrame emptyBatch
            new DataFrame(safeHandle)
        else
            new DataFrame(handle)
    /// <summary>
    /// Stream F# sequences (or C# enumerables) into Polars.
    /// </summary>
    /// <param name="data">The input sequence of objects.</param>
    /// <param name="batchSize">Optional batch size for Arrow chunks. Defaults to 100,000.</param>
    /// <param name="providedSchema">Optional Arrow Schema. Inferred via reflection if not provided.</param>
    static member ReadSeq<'T>(data: seq<'T>, ?batchSize: int, ?providedSchema: Schema) : DataFrame =

        if isNull data then 
            invalidArg "data" "Data sequence cannot be null."

        let actualBatchSize = defaultArg batchSize 100_000

        let schema = 
            match providedSchema with
            | Some s -> s
            | None -> ArrowConverter.GetSchemaFromType<'T>()

        let stream = data.ToArrowBatches actualBatchSize

        let handle = ArrowStreamInterop.ImportEager(stream, schema)

        new DataFrame(handle)
    // ---------------------------------------------------------
    // Read Parquet (File / Cloud / Glob)
    // ---------------------------------------------------------

    /// <summary>
    /// Read a parquet file into a DataFrame (Eager).
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanParquet and collects the result.
    /// </para>
    /// </summary>
    static member ReadParquet(
        path: string,
        ?columns: seq<string>,
        ?nRows: uint64,
        ?parallelStrategy: ParallelStrategy,
        ?lowMemory: bool,
        ?useStatistics: bool,
        ?glob: bool,
        ?allowMissingColumns: bool,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool,
        ?cloudOptions: CloudOptions
    ) : DataFrame =
        
        let mutable (lf: LazyFrame) = LazyFrame.ScanParquet(
            path,
            ?nRows = nRows,
            ?parallelStrategy = parallelStrategy,
            ?lowMemory = lowMemory,
            ?useStatistics = useStatistics,
            ?glob = glob,
            ?allowMissingColumns = allowMissingColumns,
            ?rechunk = rechunk,
            ?cache = cache,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?schema = schema,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates,
            ?cloudOptions = cloudOptions
        )

        match columns with
        | Some cols -> 
            let mutable colList = cols |> Seq.toList
            if not (List.isEmpty colList) then
                match rowIndexName with
                | Some rName when not (List.contains rName colList) -> colList <- colList @ [rName]
                | _ -> ()

                match includePathColumn with
                | Some pName when not (List.contains pName colList) -> colList <- colList @ [pName]
                | _ -> ()

                use sel = new Selector(PolarsWrapper.SelectorCols (colList |> List.toArray))
                lf <- lf.Select [sel :> IColumnExpr]
        | None -> ()

        lf.Collect()

    // ---------------------------------------------------------
    // Read Parquet (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read Parquet from an in-memory byte array.
    /// </summary>
    static member ReadParquet(
        buffer: byte[],
        ?columns: seq<string>,
        ?nRows: uint64,
        ?parallelStrategy: ParallelStrategy,
        ?lowMemory: bool,
        ?useStatistics: bool,
        ?allowMissingColumns: bool,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning:bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool
    ) : DataFrame =
        
        let mutable (lf: LazyFrame) = LazyFrame.ScanParquet(
            buffer,
            ?nRows = nRows,
            ?parallelStrategy = parallelStrategy,
            ?lowMemory = lowMemory,
            ?useStatistics = useStatistics,
            ?allowMissingColumns = allowMissingColumns,
            ?rechunk = rechunk,
            ?cache = cache,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?schema = schema,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates
        )

        match columns with
        | Some cols -> 
            let mutable colList = cols |> Seq.toList
            if not (List.isEmpty colList) then
                match rowIndexName with
                | Some rName when not (List.contains rName colList) -> colList <- colList @ [rName]
                | _ -> ()

                match includePathColumn with
                | Some pName when not (List.contains pName colList) -> colList <- colList @ [pName]
                | _ -> ()

                use sel = new Selector(PolarsWrapper.SelectorCols(colList |> List.toArray))
                lf <- lf.Select [sel :> IColumnExpr]
        | None -> ()

        lf.Collect()

    // ---------------------------------------------------------
    // Read Parquet (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read Parquet from a Stream.
    /// </summary>
    static member ReadParquet(
        stream: System.IO.Stream,
        ?columns: seq<string>,
        ?nRows: uint64,
        ?parallelStrategy: ParallelStrategy,
        ?lowMemory: bool,
        ?useStatistics: bool,
        ?allowMissingColumns: bool,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning:bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool
    ) : DataFrame =
        
        use ms = new System.IO.MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()

        DataFrame.ReadParquet(
            buffer = bytes,
            ?columns = columns,
            ?nRows = nRows,
            ?parallelStrategy = parallelStrategy,
            ?lowMemory = lowMemory,
            ?useStatistics = useStatistics,
            ?allowMissingColumns = allowMissingColumns,
            ?rechunk = rechunk,
            ?cache = cache,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?schema = schema,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates
        ) 

    /// <summary> Asynchronously read a Parquet file. </summary>
    static member ReadParquetAsync (        
            path: string,
            ?columns: seq<string>,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
            ?allowMissingColumns: bool,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning:bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool,
            ?cloudOptions: CloudOptions) = 
        task {
            return DataFrame.ReadParquet(
                path,
                ?columns = columns,
                ?nRows = nRows,
                ?parallelStrategy = parallelStrategy,
                ?lowMemory = lowMemory,
                ?useStatistics = useStatistics,
                ?glob = glob,
                ?allowMissingColumns = allowMissingColumns,
                ?rechunk = rechunk,
                ?cache = cache,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?includePathColumn = includePathColumn,
                ?schema = schema,
                ?hivePartitioning = hivePartitioning,
                ?hivePartitionSchema = hivePartitionSchema,
                ?tryParseHiveDates = tryParseHiveDates,
                ?cloudOptions = cloudOptions
            )
        }

    /// <summary>
    /// Read a JSON file into a DataFrame.
    /// </summary>
    static member ReadJson(path: string, 
                           ?columns: string seq, 
                           ?schema: PolarsSchema, 
                           ?inferSchemaLen: uint64, 
                           ?batchSize: uint64, 
                           ?ignoreErrors: bool,
                           ?jsonFormat: JsonFormat) : DataFrame =
        
        let cols = columns |> Option.map Seq.toArray |> Option.toObj
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        
        let ignoreErr = defaultArg ignoreErrors false
        
        let fmt = defaultArg jsonFormat JsonFormat.Json

        let h = PolarsWrapper.ReadJson(path, cols, schemaHandle, inferLen, batch, ignoreErr, fmt.ToNative())
        new DataFrame(h)

    /// <summary>
    /// Read JSON from in-memory bytes.
    /// </summary>
    static member ReadJson(buffer: byte[], 
                           ?columns: string seq, 
                           ?schema: PolarsSchema, 
                           ?inferSchemaLen: uint64, 
                           ?batchSize: uint64, 
                           ?ignoreErrors: bool,
                           ?jsonFormat: JsonFormat) : DataFrame =
        
        let cols = columns |> Option.map Seq.toArray |> Option.toObj
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        let ignoreErr = defaultArg ignoreErrors false
        
        let fmt = defaultArg jsonFormat JsonFormat.Json

        let h = PolarsWrapper.ReadJson(buffer, cols, schemaHandle, inferLen, batch, ignoreErr, fmt.ToNative())
        new DataFrame(h)

    /// <summary>
    /// Read JSON from a Stream.
    /// </summary>
    static member ReadJson(stream: Stream, 
                           ?columns: string seq, 
                           ?schema: PolarsSchema, 
                           ?inferSchemaLen: uint64, 
                           ?batchSize: uint64, 
                           ?ignoreErrors: bool,
                           ?jsonFormat: JsonFormat) : DataFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()
        
        DataFrame.ReadJson(
            bytes, 
            ?columns=columns, 
            ?schema=schema, 
            ?inferSchemaLen=inferSchemaLen, 
            ?batchSize=batchSize, 
            ?ignoreErrors=ignoreErrors, 
            ?jsonFormat=jsonFormat
        )
    // ---------------------------------------------------------
    // Read IPC (File / Cloud / Glob)
    // ---------------------------------------------------------

    /// <summary>
    /// Read an Arrow IPC (Feather v2) file into a DataFrame.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanIpc and collects the result.
    /// </para>
    /// </summary>
    static member ReadIpc(
        path: string,
        ?columns: seq<string>,
        ?nRows: uint64,
        ?rechunk: bool,
        ?cache: bool,
        ?glob: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool,
        ?cloudOptions: CloudOptions
    ) : DataFrame =
        
        let mutable lf = LazyFrame.ScanIpc(
            path,
            ?nRows = nRows,
            ?rechunk = rechunk,
            ?cache = cache,
            ?glob = glob,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?schema = schema,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates,
            ?cloudOptions = cloudOptions
        )

        match columns with
        | Some cols -> 
            let mutable colList = cols |> Seq.toList
            if not (List.isEmpty colList) then
                match rowIndexName with
                | Some rName when not (List.contains rName colList) -> colList <- colList @ [rName]
                | _ -> ()

                match includePathColumn with
                | Some pName when not (List.contains pName colList) -> colList <- colList @ [pName]
                | _ -> ()

                use sel = new Selector(PolarsWrapper.SelectorCols(colList |> List.toArray))
                lf <- lf.Select [sel :> IColumnExpr]
        | None -> ()

        lf.Collect()

    // ---------------------------------------------------------
    // Read IPC (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read IPC from in-memory bytes.
    /// </summary>
    static member ReadIpc(
        buffer: byte[],
        ?columns: seq<string>,
        ?nRows: uint64,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool
    ) : DataFrame =
        
        let mutable lf = LazyFrame.ScanIpc(
            buffer,
            ?nRows = nRows,
            ?rechunk = rechunk,
            ?cache = cache,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?schema = schema,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates
        )

        match columns with
        | Some cols -> 
            let mutable colList = cols |> Seq.toList
            if not (List.isEmpty colList) then
                match rowIndexName with
                | Some rName when not (List.contains rName colList) -> colList <- colList @ [rName]
                | _ -> ()

                match includePathColumn with
                | Some pName when not (List.contains pName colList) -> colList <- colList @ [pName]
                | _ -> ()

                use sel = new Selector(PolarsWrapper.SelectorCols(colList |> List.toArray))
                lf <- lf.Select [sel :> IColumnExpr]
        | None -> ()

        lf.Collect()

    // ---------------------------------------------------------
    // Read IPC (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read IPC from a Stream.
    /// </summary>
    static member ReadIpc(
        stream: System.IO.Stream,
        ?columns: seq<string>,
        ?nRows: uint64,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool
    ) : DataFrame =
        
        use ms = new System.IO.MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()

        DataFrame.ReadIpc(
            bytes,
            ?columns = columns,
            ?nRows = nRows,
            ?rechunk = rechunk,
            ?cache = cache,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?schema = schema,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates
        )
    /// <summary>
    /// Read an Excel file (.xlsx) into a DataFrame using the high-performance Rust 'calamine' engine.
    /// </summary>
    /// <param name="path">Path to the .xlsx file.</param>
    /// <param name="sheetName">Name of the sheet to read. If specified, takes precedence over sheetIndex.</param>
    /// <param name="sheetIndex">Index of the sheet to read (0-based). Default is 0.</param>
    /// <param name="schema">Optional schema overrides to enforce specific column types.</param>
    /// <param name="hasHeader">Indicates if the first row contains header names. Default is true.</param>
    /// <param name="inferSchemaLen">Number of rows to use for schema inference. Default is 100.</param>
    /// <param name="dropEmptyRows">If true, drop rows where all cells are empty/null. Default is true.</param>
    /// <param name="raiseIfEmpty">If true, raises an error if the sheet is empty. Default is true.</param>
    static member ReadExcel(path: string,
                            ?sheetName: string,
                            ?sheetIndex: uint64,
                            ?schema: PolarsSchema,
                            ?hasHeader: bool,
                            ?inferSchemaLen: uint64,
                            ?dropEmptyRows: bool,
                            ?raiseIfEmpty: bool) : DataFrame =
        
        let sName = Option.toObj sheetName
        let sIdx = defaultArg sheetIndex 0UL
        
        let sHandle = 
            match schema with 
            | Some s -> s.Handle 
            | None -> null

        let header = defaultArg hasHeader true
        let infer = defaultArg inferSchemaLen 100UL
        let dropEmpty = defaultArg dropEmptyRows true
        let raiseEmpty = defaultArg raiseIfEmpty true

        let h = PolarsWrapper.ReadExcel(
            path, 
            sName, 
            sIdx, 
            sHandle, 
            header, 
            infer, 
            dropEmpty, 
            raiseEmpty
        )
        
        new DataFrame(h)
    // ---------------------------------------------------------
    // Read Avro
    // ---------------------------------------------------------

    /// <summary>
    /// Read an Avro file into a DataFrame.
    /// </summary>
    /// <param name="path">The path to the Avro file.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    static member ReadAvro(
        path: string, 
        ?nRows: uint64, 
        ?columns: seq<string>, 
        ?projection: seq<int>
    ) =
        let pNRows = Option.toNullable nRows
        let pCols = match columns with Some c -> Seq.toArray c | None -> null
        let pProj = match projection with Some p -> Seq.toArray p | None -> null
        
        let handle = PolarsWrapper.ReadAvro(path, pNRows, pCols, pProj)
        new DataFrame(handle)

    /// <summary>
    /// Read an Avro memory buffer into a DataFrame.
    /// </summary>
    /// <param name="buffer">The byte array containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    static member ReadAvro(
        buffer: byte[], 
        ?nRows: uint64, 
        ?columns: seq<string>, 
        ?projection: seq<int>
    ) =
        let pNRows = Option.toNullable nRows
        let pCols = match columns with Some c -> Seq.toArray c | None -> null
        let pProj = match projection with Some p -> Seq.toArray p | None -> null
        
        let handle = PolarsWrapper.ReadAvro(buffer, pNRows, pCols, pProj)
        new DataFrame(handle)

    /// <summary>
    /// Read an Avro memory Stream into a DataFrame.
    /// </summary>
    /// <param name="stream">The stream containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    static member ReadAvro(
        stream: System.IO.Stream, 
        ?nRows: uint64, 
        ?columns: seq<string>, 
        ?projection: seq<int>
    ) =
        use ms = new System.IO.MemoryStream()
        stream.CopyTo(ms)
        
        let pNRows = Option.toNullable nRows
        let pCols = match columns with Some c -> Seq.toArray c | None -> null
        let pProj = match projection with Some p -> Seq.toArray p | None -> null
        
        let handle = PolarsWrapper.ReadAvro(ms.ToArray(), pNRows, pCols, pProj)
        new DataFrame(handle)
    /// <summary>
    /// Create a DataFrame by reading a Delta Lake table.
    /// </summary>
    /// <param name="path">Path to the Delta Lake table (folder containing _delta_log).</param>
    /// <param name="version">The version of the table to read (e.g., 0L, 1L). Mutually exclusive with <paramref name="datetime"/>.</param>
    /// <param name="datetime">The timestamp to read (ISO-8601 string, e.g., "2026-02-09T12:00:00Z"). Mutually exclusive with <paramref name="version"/>.</param>
    /// <returns>A new LazyFrame.</returns>
    static member ReadDelta(
        path: string,
        ?version: int64,
        ?datetime: string,
        ?nRows: uint64,
        ?parallelStrategy: ParallelStrategy,
        ?lowMemory: bool,
        ?useStatistics: bool,
        ?glob: bool,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool,
        ?cloudOptions: CloudOptions
    ) : DataFrame =
        let lf = LazyFrame.ScanDelta(
            path,
            ?version=version,
            ?datetime=datetime,
            ?nRows=nRows,
            ?parallelStrategy=parallelStrategy,
            ?lowMemory=lowMemory,
            ?useStatistics=useStatistics,
            ?glob=glob,
            ?rechunk=rechunk,
            ?cache=cache,
            ?rowIndexName=rowIndexName,
            ?rowIndexOffset=rowIndexOffset,
            ?includePathColumn=includePathColumn,
            ?schema=schema,
            ?hivePartitioning=hivePartitioning,
            ?hivePartitionSchema=hivePartitionSchema,
            ?tryParseHiveDates=tryParseHiveDates,
            ?cloudOptions=cloudOptions
        )
        lf.Collect()
    /// <summary> Create a DataFrame from a sequence of objects using Arrow streaming. </summary>
    static member ofSeqStream<'T>(data: seq<'T>, ?batchSize: int) : DataFrame =
        let size = defaultArg batchSize 100_000

        let schema = ArrowConverter.GetSchemaFromType<'T>()
        let batchStream = 
            data
            |> Seq.chunkBySize size
            |> Seq.map ArrowFfiBridge.BuildRecordBatch

        let handle = ArrowStreamInterop.ImportEager(batchStream,schema)

        if handle.IsInvalid then
            let emptyBatch = new RecordBatch(schema, System.Array.Empty<Apache.Arrow.IArrowArray>(), 0)
            let safeHandle = ArrowFfiBridge.ImportDataFrame emptyBatch
            new DataFrame(safeHandle)
        else
            new DataFrame(handle)
    /// <summary>
    /// [ToRecords] Transform DataFrame to F# Records
    /// </summary>
    member this.ToRecords<'T>() : seq<'T> =
        use batch = ArrowFfiBridge.ExportDataFrame this.Handle
        
        ArrowReader.ReadRecordBatch<'T> batch |> Seq.toList |> List.toSeq

    // ==========================================
    // High-Performance Record Converter
    // ==========================================
    /// <summary>
    /// Check if a type is supported by the Fast Columnar Transposition path.
    /// Primitives, Strings, Dates, and their Option/VOption variants, or Arrays with non-null primitive data types are supported.
    /// Lists, Arrays with nullable or option type, and Nested Records must fallback to Arrow.
    /// </summary>
    static member private IsSupportedFastType (t: Type) =
        // 1. Unwrap Option/VOption/Nullable
        let coreType = 
            if t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<option<_>> || t.GetGenericTypeDefinition() = typedefof<voption<_>> || t.GetGenericTypeDefinition() = typedefof<Nullable<_>>) then
                t.GetGenericArguments().[0]
            else
                t

        if t.IsArray then false 
        else
            if coreType.IsPrimitive then true
            else if coreType = typeof<string> then true
            else if coreType = typeof<decimal> then true
            else if coreType = typeof<DateTime> then true
            else if coreType = typeof<DateOnly> then true
            else if coreType = typeof<TimeOnly> then true
            else if coreType = typeof<TimeSpan> then true
            else if coreType = typeof<DateTimeOffset> then true
            else if coreType = typeof<Int128> then true
            else if coreType = typeof<UInt128> then true
            else false
    /// <summary>
    /// [Internal] Worker method to transpose a single column from Record[] to Series.
    /// This is generic to avoid boxing during array population.
    /// </summary>
    static member private CreateSeriesFromColumn<'Rec, 'Field>(data: 'Rec[], name: string, prop: PropertyInfo) : Series =
        // 1. Create Fast Getter (Delegate)
        let getterMethod = prop.GetGetMethod()
        let getter = Delegate.CreateDelegate(typeof<Func<'Rec, 'Field>>, getterMethod) :?> Func<'Rec, 'Field>
        
        // 2. Transpose: Row-Oriented -> Column-Oriented
        //    (Allocation happens here: O(N))
        let len = data.Length
        let colData = Array.zeroCreate<'Field> len
        
        for i = 0 to len - 1 do
            colData.[i] <- getter.Invoke(data.[i])
            
        // 3. Delegate to C# SeriesFactory (The Magic Step!)
        Series.create(name, colData)

    /// <summary>
    /// Create a DataFrame from a sequence of records.
    /// <para>
    /// Strategy:
    /// 1. Inspects types. If all are simple primitives/strings/dates, uses Fast Columnar Transposition (Zero-Arrow).
    /// 2. If any complex types (Lists, Arrays, Nested Records) are found, falls back to ArrowFfiBridge.
    /// </para>
    /// </summary>
    static member ofRecords<'T>(data: seq<'T>) : DataFrame =
        let recordType = typeof<'T>
        let props = recordType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

        // 1. Check Eligibility for Fast Path
        // We only use Fast Path if ALL columns are supported.
        let useFastPath = 
            props 
            |> Array.forall (fun p -> DataFrame.IsSupportedFastType p.PropertyType)

        if useFastPath then
            // ==================================================
            // PATH A: High-Performance Columnar Transposition
            // ==================================================
            let records = Seq.toArray data
            
            // Helper Cache
            let helperMethodDef = 
                typeof<DataFrame>.GetMethod("CreateSeriesFromColumn", BindingFlags.NonPublic ||| BindingFlags.Static)

            let seriesList = 
                props
                |> Array.map (fun prop ->
                    let fieldType = prop.PropertyType
                    let specificHelper = helperMethodDef.MakeGenericMethod(recordType, fieldType)
                    try 
                        specificHelper.Invoke(null, [| records; prop.Name; prop |]) :?> Series
                    with ex ->
                        failwithf "Failed to create series for column '%s': %s" prop.Name ex.InnerException.Message
                )
            DataFrame.create seriesList

        else
            // ==================================================
            // PATH B: Arrow Fallback (The Old Way)
            // Supports Lists, Structs, and complex nesting
            // ==================================================
            let batch = ArrowFfiBridge.BuildRecordBatch data
            let handle = ArrowFfiBridge.ImportDataFrame batch
            new DataFrame(handle)
    /// <summary> Create a DataFrame directly from an Apache Arrow RecordBatch. </summary>
    static member FromArrow (batch: RecordBatch) : DataFrame =
        new DataFrame(PolarsWrapper.FromArrow batch)
    /// <summary>
    /// Write DataFrame to a comma-separated values (CSV) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    member this.WriteCsv(
        path: string,
        ?includeHeader: bool,
        ?includeBom: bool,
        ?separator: char,
        ?quoteChar: char,
        ?quoteStyle: QuoteStyle,
        ?nullValue: string,
        ?lineTerminator: string,
        ?floatScientific: bool,
        ?floatPrecision: int,
        ?decimalComma: bool,
        ?dateFormat: string,
        ?timeFormat: string,
        ?datetimeFormat: string,
        ?checkExtension: bool,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?batchSize: int,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkCsv(
            path,
            ?includeHeader = includeHeader,
            ?includeBom = includeBom,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?quoteStyle = quoteStyle,
            ?nullValue = nullValue,
            ?lineTerminator = lineTerminator,
            ?floatScientific = floatScientific,
            ?floatPrecision = floatPrecision,
            ?decimalComma = decimalComma,
            ?dateFormat = dateFormat,
            ?timeFormat = timeFormat,
            ?datetimeFormat = datetimeFormat,
            ?checkExtension = checkExtension,
            ?compression = compression,
            ?compressionLevel = compressionLevel,
            ?maintainOrder = maintainOrder,
            ?syncOnClose = syncOnClose,
            ?mkdir = mkdir,
            ?batchSize = batchSize,
            ?cloudOptions = cloudOptions
        )
        this
    /// <summary>
    /// Write DataFrame to a partitioned comma-separated values (CSV) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    member this.WriteCsvPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?includeHeader: bool,
        ?includeBom: bool,
        ?separator: char,
        ?quoteChar: char,
        ?quoteStyle: QuoteStyle,
        ?nullValue: string,
        ?lineTerminator: string,
        ?floatScientific: bool,
        ?floatPrecision: int,
        ?decimalComma: bool,
        ?dateFormat: string,
        ?timeFormat: string,
        ?datetimeFormat: string,
        ?checkExtension: bool,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?batchSize: int,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkCsvPartitioned(
            path,
            partitionBy,
            ?includeKeys = includeKeys,
            ?keysPreGrouped = keysPreGrouped,
            ?maxRowsPerFile = maxRowsPerFile,
            ?approxBytesPerFile = approxBytesPerFile,
            ?includeHeader = includeHeader,
            ?includeBom = includeBom,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?quoteStyle = quoteStyle,
            ?nullValue = nullValue,
            ?lineTerminator = lineTerminator,
            ?floatScientific = floatScientific,
            ?floatPrecision = floatPrecision,
            ?decimalComma = decimalComma,
            ?dateFormat = dateFormat,
            ?timeFormat = timeFormat,
            ?datetimeFormat = datetimeFormat,
            ?checkExtension = checkExtension,
            ?compression = compression,
            ?compressionLevel = compressionLevel,
            ?maintainOrder = maintainOrder,
            ?syncOnClose = syncOnClose,
            ?mkdir = mkdir,
            ?batchSize = batchSize,
            ?cloudOptions = cloudOptions
        )
        this
    /// <summary>
    /// Write DataFrame to a CSV format in memory.
    /// </summary>
    /// <returns>A byte array containing the serialized CSV data.</returns>
    member this.WriteCsvMemory(
        ?includeBom: bool,
        ?includeHeader: bool,
        ?batchSize: int,
        ?checkExtension: bool,
        ?compressionCode: ExternalCompression,
        ?compressionLevel: int,
        ?dateFormat: string,
        ?timeFormat: string,
        ?datetimeFormat: string,
        ?floatScientific: int, 
        ?floatPrecision: int,
        ?decimalComma: bool,
        ?separator: byte,
        ?quoteChar: byte,
        ?nullValue: string,
        ?lineTerminator: string,
        ?quoteStyle: QuoteStyle,
        ?maintainOrder: bool
    ) : byte[] =
        this.Lazy().SinkCsvMemory(
            ?includeBom = includeBom,
            ?includeHeader = includeHeader,
            ?batchSize = batchSize,
            ?checkExtension = checkExtension,
            ?compressionCode = compressionCode,
            ?compressionLevel = compressionLevel,
            ?dateFormat = dateFormat,
            ?timeFormat = timeFormat,
            ?datetimeFormat = datetimeFormat,
            ?floatScientific = floatScientific,
            ?floatPrecision = floatPrecision,
            ?decimalComma = decimalComma,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?nullValue = nullValue,
            ?lineTerminator = lineTerminator,
            ?quoteStyle = quoteStyle,
            ?maintainOrder = maintainOrder
        )
        
    /// <summary>
    /// Write DataFrame to a Parquet file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming, statistics, and cloud storage.
    /// </para>
    /// </summary>
    member this.WriteParquet(
        path: string,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkParquet(
            path,
            ?compression = compression,
            ?compressionLevel = compressionLevel,
            ?statistics = statistics,
            ?rowGroupSize = rowGroupSize,
            ?dataPageSize = dataPageSize,
            ?compatLevel = compatLevel,
            ?maintainOrder = maintainOrder,
            ?syncOnClose = syncOnClose,
            ?mkdir = mkdir,
            ?cloudOptions = cloudOptions
        )
        this
    /// <summary>
    /// Write DataFrame to a partitioned Parquet file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    member this.WriteParquetPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkParquetPartitioned(
            path,
            partitionBy,
            ?includeKeys = includeKeys,
            ?keysPreGrouped = keysPreGrouped,
            ?maxRowsPerFile = maxRowsPerFile,
            ?approxBytesPerFile = approxBytesPerFile,
            ?compression = compression,
            ?compressionLevel = compressionLevel,
            ?statistics = statistics,
            ?rowGroupSize = rowGroupSize,
            ?dataPageSize = dataPageSize,
            ?compatLevel = compatLevel,
            ?maintainOrder = maintainOrder,
            ?syncOnClose = syncOnClose,
            ?mkdir = mkdir,
            ?cloudOptions = cloudOptions
        )
        this
    /// <summary>
    /// Write DataFrame to a Parquet format in memory.
    /// </summary>
    /// <returns>A byte array containing the serialized Parquet data.</returns>
    member this.WriteParquetMemory(
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?compatLevel: int,
        ?maintainOrder: bool
    ) : byte[] =
        this.Lazy().SinkParquetMemory(
            ?compression = compression,
            ?compressionLevel = compressionLevel,
            ?statistics = statistics,
            ?rowGroupSize = rowGroupSize,
            ?dataPageSize = dataPageSize,
            ?compatLevel = compatLevel,
            ?maintainOrder = maintainOrder
        )
    /// <summary>
    /// Write DataFrame to an IPC (Arrow/Feather) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    member this.WriteIpc(
        path: string,
        ?compression: IpcCompression,
        ?compatLevel: int,
        ?recordBatchSize: int,
        ?recordBatchStatistics: bool,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkIpc(
            path,
            ?compression = compression,
            ?compatLevel = compatLevel,
            ?recordBatchSize = recordBatchSize,
            ?recordBatchStatistics = recordBatchStatistics,
            ?maintainOrder = maintainOrder,
            ?syncOnClose = syncOnClose,
            ?mkdir = mkdir,
            ?cloudOptions = cloudOptions
        ) |> ignore
        
        this

    /// <summary>
    /// Write DataFrame to a partitioned IPC (Arrow/Feather) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    member this.WriteIpcPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: IpcCompression,
        ?compatLevel: int,
        ?recordBatchSize: int,
        ?recordBatchStatistics: bool,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkIpcPartitioned(
            path,
            partitionBy,
            ?includeKeys = includeKeys,
            ?keysPreGrouped = keysPreGrouped,
            ?maxRowsPerFile = maxRowsPerFile,
            ?approxBytesPerFile = approxBytesPerFile,
            ?compression = compression,
            ?compatLevel = compatLevel,
            ?recordBatchSize = recordBatchSize,
            ?recordBatchStatistics = recordBatchStatistics,
            ?maintainOrder = maintainOrder,
            ?syncOnClose = syncOnClose,
            ?mkdir = mkdir,
            ?cloudOptions = cloudOptions
        ) |> ignore
        
        this

    /// <summary>
    /// Write DataFrame to an IPC (Arrow/Feather) format in memory.
    /// </summary>
    /// <returns>A byte array containing the serialized IPC data.</returns>
    member this.WriteIpcMemory(
        ?compression: IpcCompression,
        ?compatLevel: int,
        ?recordBatchSize: int,
        ?recordBatchStatistics: bool,
        ?maintainOrder: bool
    ) : byte[] =
        this.Lazy().SinkIpcMemory(
            ?compression = compression,
            ?compatLevel = compatLevel,
            ?recordBatchSize = recordBatchSize,
            ?recordBatchStatistics = recordBatchStatistics,
            ?maintainOrder = maintainOrder
        )
    /// <summary>   
    /// Write DataFrame to a JSON file.
    /// </summary>
    member this.WriteJson(path: string, ?format: JsonFormat) =
        let format = defaultArg format JsonFormat.Json
        PolarsWrapper.WriteJson(this.Handle, path, format.ToNative())
        this
    /// <summary>
    /// Write DataFrame to a NDJSON (JsonLines) file.
    /// </summary>
    member this.WriteNdJson(path: string) =
        this.WriteJson(path, JsonFormat.JsonLines)
    // ---------------------------------------------------------
    // Write Excel (Export)
    // ---------------------------------------------------------
    
    /// <summary>
    /// Write the DataFrame to an Excel file (.xlsx) using the native high-performance engine.
    /// <para>UInt64, Int128, UInt128 are automatically written as Text to preserve precision.</para>
    /// </summary>
    /// <param name="path">Destination file path.</param>
    /// <param name="sheetName">Optional sheet name (default: "Sheet1").</param>
    /// <param name="dateFormat">Optional Excel format string for Date columns (e.g. "yyyy-mm-dd").</param>
    /// <param name="datetimeFormat">Optional Excel format string for Datetime columns (e.g. "yyyy-mm-dd hh:mm:ss").</param>
    member this.WriteExcel(path: string, 
                           ?sheetName: string, 
                           ?dateFormat: string, 
                           ?datetimeFormat: string) =
        
        // F# Option -> C# null handling
        let sName = Option.toObj sheetName
        let dFmt = Option.toObj dateFormat
        let dtFmt = Option.toObj datetimeFormat
        
        PolarsWrapper.WriteExcel(this.Handle, path, sName, dFmt, dtFmt)
    // ---------------------------------------------------------
    // Write Avro
    // ---------------------------------------------------------

    /// <summary>
    /// Write the DataFrame to an Apache Avro file.
    /// </summary>
    /// <param name="path">The file path to write to.</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    member this.WriteAvro(
        path: string, 
        ?compression: AvroCompression, 
        ?name: string
    ) =
        let pComp = defaultArg compression AvroCompression.Uncompressed
        let pName = defaultArg name ""
        PolarsWrapper.WriteAvro(this.Handle, path, pComp.ToNative(), pName)

    /// <summary>
    /// Write the DataFrame to an Apache Avro memory buffer.
    /// </summary>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    /// <returns>A byte array containing the Avro data.</returns>
    member this.WriteAvroMemory(
        ?compression: AvroCompression, 
        ?name: string
    ) : byte[] =
        let pComp = defaultArg compression AvroCompression.Uncompressed
        let pName = defaultArg name ""
        PolarsWrapper.WriteAvroToMemory(this.Handle, pComp.ToNative(), pName)
    // ---- ADBC ----
    static member FromArrowStream(stream:IArrowArrayStream) =
    
        ArgumentNullException.ThrowIfNull stream

        let handle = ArrowStreamInterop.ImportForeignStream stream;
        
        let df = new DataFrame(handle)
        df.HoldResource(stream); 
        df
    
    /// <summary>
    /// Generate DataFrame from ADBC query results
    /// </summary>
    /// <param name="statement"></param>
    /// <exception cref="InvalidOperationException"></exception>
    static member ReadAdbc(statement: AdbcStatement) : DataFrame =
        ArgumentNullException.ThrowIfNull statement

        let result = statement.ExecuteQuery()

        if isNull result.Stream then
            raise (InvalidOperationException "ADBC query executed, but returned a null Arrow stream.")
            
        DataFrame.FromArrowStream result.Stream


    /// <summary>
    /// Executes a SQL query directly against an ADBC connection and reads the result into a zero-copy Polars DataFrame.
    /// Pure syntactic sugar: automatically manages the creation and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="sqlQuery">The SQL query string to execute.</param>
    static member ReadAdbc(connection: AdbcConnection, sqlQuery: string) : DataFrame =
        ArgumentNullException.ThrowIfNull connection
        
        if String.IsNullOrWhiteSpace sqlQuery then
            invalidArg "sqlQuery" "SQL query cannot be null or whitespace."

        use statement = connection.CreateStatement()
        
        statement.SqlQuery <- sqlQuery

        DataFrame.ReadAdbc statement
    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database (e.g., DuckDB, SQLite).
    /// </summary>
    /// <param name="statement">An AdbcStatement configured with ingest options (e.g., target table).</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    member this.WriteToAdbc(statement: AdbcStatement) : UpdateResult =
        ArgumentNullException.ThrowIfNull statement

        try
            // Delegate all unsafe pointer handling, FFI bindings, and execution to the Core layer.
            // This ensures no raw pointers leak into the managed high-level API.
            AdbcInterop.ExecuteIngest(statement, this.Handle)
        finally
            // Crucial: Pin the DataFrame to prevent the Garbage Collector from 
            // reclaiming the underlying Rust memory while the ADBC C++ engine is actively pulling data.
            GC.KeepAlive this

    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database table.
    /// Pure syntactic sugar: automatically manages the creation, configuration, and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="tableName">The name of the target table to ingest data into.</param>
    /// <param name="ingestMode">The ingestion mode. Defaults to Create.</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    member this.WriteToAdbc(connection: AdbcConnection, tableName: string, ?ingestMode: AdbcIngestMode) : UpdateResult =
        ArgumentNullException.ThrowIfNull(connection)
        
        if String.IsNullOrWhiteSpace tableName then
            invalidArg "tableName" "Target table name cannot be null or whitespace."

        let mode = defaultArg ingestMode AdbcIngestMode.Create

        // Let the framework handle the Statement lifecycle
        use statement = connection.CreateStatement()
        
        // Configure ADBC bulk ingest options automatically
        statement.SetOption("adbc.ingest.target_table", tableName)
        
        let modeString = 
            match mode with
            | AdbcIngestMode.Create  -> "adbc.ingest.mode.create"
            | AdbcIngestMode.Append  -> "adbc.ingest.mode.append"
            | AdbcIngestMode.Replace -> "adbc.ingest.mode.replace"

        statement.SetOption("adbc.ingest.mode", modeString)

        // Route to the core execution method
        this.WriteToAdbc statement
    /// <summary>
    /// Write the DataFrame to a Delta Lake table with partition discovery.
    /// <para>
    /// This operation performs a "blind write" of partitioned Parquet files (Hive-style) 
    /// and then commits a transaction to the Delta Log, registering the new files.
    /// </para>
    /// </summary>
    /// <param name="path">
    /// Path to the root of the Delta Table. Can be local (e.g. "./data/table") 
    /// or remote (e.g. "s3://bucket/table").
    /// </param>
    /// <param name="partitionBy">
    /// The selector(s) to partition the data by. 
    /// Directories will be created in the format "col=value".
    /// </param>
    /// <param name="mode">
    /// Save mode (Append, Overwrite, ErrorIfExists, Ignore). Default is Append.
    /// </param>
    /// <param name="includeKeys">
    /// Whether to include the partition keys in the Parquet files themselves. 
    /// Default is true (recommended for Delta Lake compatibility).
    /// </param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <param name="compression">Compression codec to use (Snappy, Zstd, etc.).</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">
    /// Write statistics to the Parquet file. 
    /// Delta Lake uses these stats for data skipping, so 'true' is highly recommended.
    /// </param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
    member this.WriteDelta(
        path: string,
        ?partitionBy: Selector,
        ?mode: DeltaSaveMode,
        ?canEvolve: bool,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: uint32,
        ?dataPageSize: uint32,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().SinkDelta(
            path,
            
            // --- Delta Options ---
            ?partitionBy=partitionBy,
            ?mode=mode,
            ?canEvolve=canEvolve,
            // --- Partition Params ---
            ?includeKeys=includeKeys,
            ?keysPreGrouped=keysPreGrouped,
            ?maxRowsPerFile=maxRowsPerFile,
            ?approxBytesPerFile=approxBytesPerFile,
            
            // --- Parquet Options ---
            ?compression=compression,
            ?compressionLevel=compressionLevel,
            ?statistics=statistics,
            ?rowGroupSize=rowGroupSize,  
            ?dataPageSize=dataPageSize,
            ?compatLevel=compatLevel,
            
            // --- Unified Options ---
            ?maintainOrder=maintainOrder,
            ?syncOnClose=syncOnClose,
            ?mkdir=mkdir,
            
            // --- Cloud Params ---
            ?cloudOptions=cloudOptions
        )
    /// <summary>
    /// Merge a DataFrame into a Delta Lake table with full SQL MERGE semantics.
    /// Provides fine-grained control over Update, Insert, and Delete behaviors.
    /// Notice: In this method, Delete > Update > Insert > Ignore.
    /// If you need other orders, please use DataFrame.MergeDeltaOrdered
    /// </summary>
    /// <param name="path">Uri to the Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The column names to join on (must exist in both Source and Target).</param>
    /// <param name="matchedUpdateCond">
    /// Condition for 'WHEN MATCHED THEN UPDATE'. 
    /// If null, defaults to true (always update when matched).
    /// </param>
    /// <param name="matchedDeleteCond">
    /// Condition for 'WHEN MATCHED THEN DELETE'. 
    /// If null, defaults to false (never delete when matched).
    /// </param>
    /// <param name="notMatchedInsertCond">
    /// Condition for 'WHEN NOT MATCHED THEN INSERT'. 
    /// If null, defaults to true (always insert new rows).
    /// </param>
    /// <param name="notMatchedBySourceDeleteCond">
    /// Condition for 'WHEN NOT MATCHED BY SOURCE THEN DELETE' (Target rows not in Source). 
    /// If null, defaults to false (retain target-only rows).
    /// </param>
    /// <param name="canEvolve">Allow schema evolution during the merge.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    [<Obsolete("This method is deprecated because its execution order of matched/not-matched actions is hardcoded and may lead to silent data corruption in complex scenarios. Please use 'MergeDeltaOrdered(...)' combined with the '.WhenMatched...()' chaining methods to ensure strict SQL MERGE semantics.")>]
    member this.MergeDelta(
        path: string,
        mergeKeys: seq<string>,
        ?matchedUpdateCond: Expr,
        ?matchedDeleteCond: Expr,
        ?notMatchedInsertCond: Expr,
        ?notMatchedBySourceDeleteCond: Expr,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.Lazy().MergeDelta(
            path,
            mergeKeys,
            ?matchedUpdateCond=matchedUpdateCond,
            ?matchedDeleteCond=matchedDeleteCond,
            ?notMatchedInsertCond=notMatchedInsertCond,
            ?notMatchedBySourceDeleteCond=notMatchedBySourceDeleteCond,
            ?canEvolve=canEvolve,
            ?cloudOptions=cloudOptions
        )
    /// <summary>
    /// Export the DataFrame as a stream of Arrow RecordBatches (Zero-Copy).
    /// Calls 'onBatch' for each chunk in the DataFrame.
    /// Useful for custom eager sinks (e.g. WriteDatabase).
    /// </summary>
    member this.ExportBatches(onBatch: Action<RecordBatch>) : unit =
        PolarsWrapper.ExportBatches(this.Handle, onBatch)

    /// <summary>
    /// Stream the DataFrame directly to a database or other IDataReader consumer.
    /// <para>
    /// Uses a producer-consumer pattern. This method blocks until the consumer finishes reading.
    /// Ideal for <c>SqlBulkCopy.WriteToServer</c> or <c>NpgsqlBinaryImporter</c>.
    /// </para>
    /// </summary>
    /// <param name="writerAction">Callback that receives an IDataReader.</param>
    /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
    /// <param name="typeOverrides">Dictionary to force specific C# types for columns (optional).</param>
    member this.WriteTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
        let capacity = defaultArg bufferSize 5
        
        use buffer = new BlockingCollection<RecordBatch>(capacity)

        let consumerTask = Task.Run(fun () ->
            let stream = buffer.GetConsumingEnumerable()
            
            let overrides = 
                match typeOverrides with 
                | Some d -> new Dictionary<string, Type>(d) 
                | None -> null
            
            use reader = new ArrowToDbStream(stream, overrides)
            
            writerAction.Invoke reader
        )

        try
            try
                this.ExportBatches(fun batch -> buffer.Add batch)
            finally
                buffer.CompleteAdding()
        with
        | _ -> 
            reraise()

        try
            consumerTask.Wait()
        with
        | :? AggregateException as aggEx ->
            raise (aggEx.Flatten().InnerException)
    
    /// <summary>
    /// Get the schema as Map<ColumnName, DataType>.
    /// </summary>
    member this.Schema =
        let h = PolarsWrapper.GetDataFrameSchema this.Handle 
        new PolarsSchema(h)
    member this.Lazy() : LazyFrame =
        let lfHandle = PolarsWrapper.DataFrameToLazy handle
        new LazyFrame(lfHandle)
    /// <summary>
    /// Print schema in a readable format.
    /// </summary>
    member this.PrintSchema() =
        printfn "--- DataFrame Schema ---"
        
        use sc = this.Schema

        sc.ToMap() 
        |> Map.iter (fun name dtype -> 
            printfn "%-15s | %O" name dtype
        )
        
        printfn "------------------------"
    // ==========================================
    // Eager Ops
    // ==========================================
    /// <summary> Add or replace columns using expressions. </summary>
    member this.WithColumns (exprs:Expr list) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.WithColumns(this.Handle,handles)
        new DataFrame(h)
    /// <summary> Add or replace columns using generic column expressions (Expr or Selectors). </summary>
    member this.WithColumns (columns:seq<#IColumnExpr>) =
        let exprs = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        
        this.WithColumns exprs
    /// <summary> Add a single column. </summary>
    member this.WithColumn (expr: Expr) : DataFrame =
        let handle = expr.CloneHandle()
        let h = PolarsWrapper.WithColumns(this.Handle,[| handle |])
        new DataFrame(h)
    /// <summary> Select columns using expressions. </summary>
    member this.Select(exprs: Expr list) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Select(this.Handle, handles)
        new DataFrame(h)

    /// <summary> Select columns using generic column expressions (Expr or Selectors). </summary>
    member this.Select(columns: seq<#IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.Select exprs
    /// <summary> 
    /// Select a single column using an expression.
    /// Usage: df.Select(pl.col("A"))
    /// </summary>
    member this.Select(expr: Expr) =
        this.Select [expr]

    /// <summary> Filter rows based on a boolean expression (predicate). </summary>
    member this.Filter (expr: Expr) : DataFrame = 
        let h = PolarsWrapper.Filter(this.Handle,expr.CloneHandle())
        new DataFrame(h)
    /// <summary>
    /// Sort the DataFrame.
    /// </summary>
    /// <param name="columns">the column which needs to be sorted (Expr/Selector)。</param>
    /// <param name="descending">sort direction (true=descending).Length must be 1 (broadcasting) or same with columns.</param>
    /// <param name="nullsLast">null value position (true=last).Length must be 1 (broadcasting) or same with columns</param>
    /// <param name="maintainOrder">Stable Sort option</param>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        descending: seq<bool>,
        nullsLast: seq<bool>,
        ?maintainOrder: bool
    ) =
        let exprHandles = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = descending |> Seq.toArray
        let nullsArr = nullsLast |> Seq.toArray
        let stable = defaultArg maintainOrder false

        let h = PolarsWrapper.DataFrameSort(this.Handle, exprHandles, descArr, nullsArr, stable)
        new DataFrame(h)

    /// <summary> Sort with simple broadcasting options. </summary>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        ?descending: bool,
        ?nullsLast: bool,
        ?maintainOrder: bool
    ) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        
        this.Sort(columns, [| desc |], [| nLast |], ?maintainOrder = maintainOrder)
    /// <summary> Sort by a single expression. </summary>
    member this.Sort(expr: Expr, ?descending: bool, ?nullsLast: bool) =
        this.Sort([expr], ?descending=descending, ?nullsLast=nullsLast)
    /// <summary> Sort by a single column name. </summary>

    member this.Sort(colName: string, ?descending: bool, ?nullsLast: bool) =
        this.Sort([Expr.Col colName], ?descending=descending, ?nullsLast=nullsLast)
    /// <summary> Alias for Sort. </summary>
    member this.Orderby (expr: Expr,desc :bool) : DataFrame =
        this.Sort(expr,desc)
    /// <summary> Group by keys and apply aggregate expressions. </summary>
    member this.GroupBy (keys: Expr list,aggs: Expr list) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.GroupByAgg(this.Handle, kHandles, aHandles)
        new DataFrame(h)
    /// <summary> Group by keys and apply aggregations (Supports Selectors). </summary>
    member this.GroupBy(keys: seq<#IColumnExpr>, aggs: seq<#IColumnExpr>) =
        let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
        let aExprs = aggs |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
        this.GroupBy (kExprs, aExprs)
    /// <summary> Join with another DataFrame. </summary>
    member this.Join (other: DataFrame,
                      leftOn: Expr list,
                      rightOn: Expr list,
                      how: JoinType,
                      // --- New Optional Parameters ---
                      ?suffix: string,
                      ?validation: JoinValidation,
                      ?coalesce: JoinCoalesce,
                      ?maintainOrder: JoinMaintainOrder,
                      ?joinSide: JoinSide,
                      ?nullsEqual: bool,
                      ?sliceOffset: int64,
                      ?sliceLen: uint64) : DataFrame =
        
        let lHandles = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        // Handle Defaults
        let suff = defaultArg suffix null // Pass null to let Rust use default ("_right")
        let valid = defaultArg validation JoinValidation.ManyToMany
        let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
        let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
        let js = defaultArg joinSide JoinSide.LetPolarsDecide
        let ne = defaultArg nullsEqual false
        
        // Slice logic
        let so = Option.toNullable sliceOffset
        let sl = defaultArg sliceLen 0UL

        let h = PolarsWrapper.Join(
            this.Handle, 
            other.Handle, 
            lHandles, 
            rHandles, 
            how.ToNative(),
            suff,
            valid.ToNative(),
            coal.ToNative(),
            mo.ToNative(),
            js.ToNative(),
            ne,
            so,
            sl
        )
        new DataFrame(h)
    member internal this.JoinAsOfInternal(other: DataFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         // --- Optional Parameters ---
                         ?byLeft: Expr list, 
                         ?byRight: Expr list, 
                         ?strategy: AsofStrategy, 
                         ?tolerance: string,      // String
                         ?toleranceInt: int64,    // Int
                         ?toleranceFloat: float,  // Float
                         ?allowEq: bool,
                         ?checkSorted: bool,
                         ?suffix: string,
                         ?validation: JoinValidation,
                         ?coalesce: JoinCoalesce,
                         ?maintainOrder: JoinMaintainOrder,
                         ?nullsEqual: bool,
                         ?sliceOffset: int64,
                         ?sliceLen: uint64) : DataFrame =
        
        // 1. Convert to Lazy
        let lfSelf = this.Lazy()
        let lfOther = other.Lazy()

        // 2. Delegate to LazyFrame.JoinAsOfInternal
        // F# allows passing optional arguments directly via ?arg=val
        let resLf = lfSelf.JoinAsOfInternal(
            lfOther, leftOn, rightOn,
            ?byLeft = byLeft, 
            ?byRight = byRight, 
            ?strategy = strategy, 
            ?tolerance = tolerance, 
            ?toleranceInt = toleranceInt, 
            ?toleranceFloat = toleranceFloat, 
            ?allowEq = allowEq, 
            ?checkSorted = checkSorted, 
            ?suffix = suffix, 
            ?validation = validation, 
            ?coalesce = coalesce, 
            ?maintainOrder = maintainOrder, 
            ?nullsEqual = nullsEqual, 
            ?sliceOffset = sliceOffset, 
            ?sliceLen = sliceLen
        )

        // 3. Collect back to DataFrame
        resLf.Collect()

    // ==========================================
    // Public Overloads (Facade)
    // ==========================================

    // 1. String Tolerance
    /// <summary>
    /// Join with tolerance as string (e.g. "2h", "10s").
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: string, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolerance, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    // 2. TimeSpan Tolerance
    /// <summary>
    /// Join with tolerance as TimeSpan.
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: System.TimeSpan, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        let tolStr = DurationFormatter.ToPolarsString tolerance
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolStr, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    // 3. Int64 Tolerance
    /// <summary>
    /// Join with tolerance as integer (e.g. timestamp or simple counter).
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: int64, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceInt = tolerance, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    // 4. Float Tolerance
    /// <summary>
    /// Join with tolerance as float.
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: float, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceFloat = tolerance, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )
    /// <summary>
    /// General Concat method.
    /// checkDuplicates is only used when how = ConcatType.Horizontal.
    /// </summary>
    static member internal Concat (dfs: seq<DataFrame>, how: ConcatType, ?checkDuplicates: bool,?strict: bool, ?unitLengthAsScalar: bool) : DataFrame =
        let handles = dfs |> Seq.map (fun df -> df.CloneHandle()) |> Seq.toArray
        
        let check = defaultArg checkDuplicates true
        let st = defaultArg strict true
        let uni = defaultArg unitLengthAsScalar false
        let h = PolarsWrapper.Concat(handles, how.ToNative(), check, st, uni)
        new DataFrame(h)

    /// <summary>
    /// Horizontal concatenation (Index alignment).
    /// </summary>
    /// <param name="strict">For Horizontal: if true, error on height mismatch.</param>
    /// <param name="unitLengthAsScalar">For Horizontal: if true, broadcast length-1 DataFrames to match height.</param>
    static member ConcatHorizontal (dfs: seq<DataFrame>, ?checkDuplicates: bool,?strict: bool, ?unitLengthAsScalar: bool) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Horizontal, ?checkDuplicates = checkDuplicates, ?strict=strict,?unitLengthAsScalar=unitLengthAsScalar)

    /// <summary>
    /// Vertical concatenation (Column alignment).
    /// </summary>
    static member ConcatVertical (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Vertical)

    /// <summary>
    /// Diagonal concatenation.
    /// </summary>
    static member ConcatDiagonal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Diagonal)
    /// <summary> Get the first n rows. </summary>
    member this.Head (?rows: int) : DataFrame  =
        let n = defaultArg rows 5
        let h = PolarsWrapper.Head(this.Handle, uint n) 
        new DataFrame(h)
    /// <summary> Get the last n rows. </summary>
    member this.Tail (?n: int) : DataFrame =
        let rows = defaultArg n 5
        let h = PolarsWrapper.Tail(this.Handle, uint rows) 
        new DataFrame(h)

    /// <summary> 
    /// Explode list columns to rows using a Selector.
    /// </summary>
    member this.Explode(selector: Selector,?emptyAsNull:bool,?keepNulls:bool) : DataFrame =
        let sh = selector.CloneHandle()
        let ean = defaultArg emptyAsNull true
        let kn = defaultArg keepNulls true
        let h = PolarsWrapper.Explode(this.Handle, sh,ean,kn)
        new DataFrame(h)

    /// <summary> 
    /// Explode list columns to rows using column names.
    /// </summary>
    member this.Explode(columns: seq<string>,?emptyAsNull:bool,?keepNulls:bool) =
        let names = Seq.toArray columns
        let h = PolarsWrapper.SelectorCols names
        let sel = new Selector(h)
        this.Explode(sel,?emptyAsNull=emptyAsNull,?keepNulls=keepNulls) 

    /// <summary>Explode a single column by name. </summary>
    member this.Explode(column: string,?emptyAsNull:bool,?keepNulls:bool) =
        this.Explode([column],?emptyAsNull=emptyAsNull,?keepNulls=keepNulls)                          
    /// <summary> Decompose a struct column into multiple columns. </summary>
    member this.UnnestColumn(column: string, ?separator: string) : DataFrame =
        let cols = [| column |]
        let sep = defaultArg separator null
        let newHandle = PolarsWrapper.Unnest(this.Handle, cols, sep)
        new DataFrame(newHandle)
    /// <summary> Decompose multiple struct columns. </summary>
    member this.UnnestColumns(columns: string list, ?separator: string) : DataFrame =
        let cArr = List.toArray columns
        let sep = defaultArg separator null
        let newHandle = PolarsWrapper.Unnest(this.Handle, cArr, sep)
        new DataFrame(newHandle)
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// </summary>
    /// <param name="index">Selector for the index column(s) (the rows).</param>
    /// <param name="columns">Selector for the column(s) to pivot (the new column headers).</param>
    /// <param name="values">Selector for the value column(s) to populate the cells.</param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="sortColumns">Sort the pivoted columns.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    member this.Pivot(
        index: Selector,
        columns: Selector,
        values: Selector,
        ?aggregateExpr: Expr,
        ?aggregateFunction: PivotAgg,
        ?sortColumns: bool,
        ?maintainOrder: bool,
        ?separator: string
    ) =
        // 1. Resolve Defaults
        let aggFunc = defaultArg aggregateFunction PivotAgg.First
        let sort = defaultArg sortColumns false
        let mo = defaultArg maintainOrder true
        let sep = Option.toObj separator

        // 2. Clone Handles
        use indexH = index.CloneHandle()
        use columnsH = columns.CloneHandle()
        use valuesH = values.CloneHandle()
        use aggExprH = 
            match aggregateExpr with
            | Some e -> e.CloneHandle()
            | None -> null

        // 3. Native Call
        let h = PolarsWrapper.Pivot(
            this.Handle,
            indexH,
            columnsH,
            valuesH,
            aggExprH,
            aggFunc.ToNative(),
            sort,
            mo,
            sep
        )
        new DataFrame(h)

    /// <summary>
    /// Pivot the DataFrame using column names.
    /// </summary>
    member this.Pivot(
        index: seq<string>,
        columns: seq<string>,
        values: seq<string>,
        ?aggFn: PivotAgg,
        ?sortColumns: bool,
        ?maintainOrder: bool,
        ?separator: string
    ) =
        use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
        use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
        use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

        this.Pivot(
            sIndex,
            sColumns,
            sValues,
            ?aggregateFunction = aggFn,
            ?sortColumns = sortColumns,
            ?maintainOrder = maintainOrder,
            ?separator = separator
        )

    /// <summary>
    /// Pivot the DataFrame using column names and a custom aggregation expression.
    /// </summary>
    member this.Pivot(
        index: seq<string>,
        columns: seq<string>,
        values: seq<string>,
        aggExpr: Expr,
        ?sortColumns: bool,
        ?maintainOrder: bool,
        ?separator: string
    ) =        
        use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
        use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
        use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

        this.Pivot(
            sIndex,
            sColumns,
            sValues,
            aggregateExpr = aggExpr,
            ?sortColumns = sortColumns,
            ?maintainOrder = maintainOrder,
            ?separator = separator
        )
    /// <summary> 
    /// Unpivot (Melt) the DataFrame from wide to long format using Selectors.
    /// This is the primary implementation backed by native binding.
    /// </summary>
    /// <param name="index">Selector for ID variables (columns to keep)</param>
    /// <param name="on">Selector for Value variables (columns to melt)</param>
    /// <param name="variableName">Name for the variable column (default: "variable")</param>
    /// <param name="valueName">Name for the value column (default: "value")</param>
    member this.Unpivot (index: Selector,on: Selector,variableName: string option,valueName: string option) : DataFrame =
        let hIndex = index.CloneHandle()
        let hOn = on.CloneHandle()
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName
        
        new DataFrame(PolarsWrapper.Unpivot(this.Handle, hIndex, hOn, varN, valN))

    /// <summary> 
    /// Unpivot (Melt) overload for simple string lists.
    /// Auto-converts string lists to Column Selectors.
    /// </summary>
    member this.Unpivot (index: seq<string>,on: seq<string>,variableName: string option,valueName: string option) =
        // 1. Index Selector
        let idxArr = Seq.toArray index
        let sIndex = new Selector(PolarsWrapper.SelectorCols idxArr)

        // 2. On (Value) Selector
        let onArr = Seq.toArray on
        let sOn = new Selector(PolarsWrapper.SelectorCols onArr)

        this.Unpivot(sIndex,sOn,variableName,valueName)
    member this.Unpivot (index: string list,on: string list) =
        this.Unpivot(index,on,None,None)
    /// <summary> Alias for Unpivot. </summary>
    member this.Melt(index: Selector, on: Selector, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: seq<string>, on: seq<string>, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: string list, on: string list) =
        this.Unpivot(index, on)
    /// <summary>
    /// Slice the DataFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint64) = 
        new DataFrame(PolarsWrapper.Slice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)
    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// Returns a new DataFrame with the new columns appended.
    /// </summary>
    member this.HStack(columns: Series list) : DataFrame =
        let handles = 
            columns 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
        
        new DataFrame(PolarsWrapper.HStack(this.Handle, handles))

    /// <summary>
    /// Vertically stack another DataFrame to this one.
    /// Checks that the schema matches.
    /// </summary>
    member this.VStack(other: DataFrame) : DataFrame =
        new DataFrame(PolarsWrapper.VStack(this.Handle, other.Handle))

    // ==========================================
    // Printing / String Representation
    // ==========================================

    /// <summary>
    /// Returns the native Polars string representation of the DataFrame.
    /// Includes shape, header, and truncated data.
    /// </summary>
    override this.ToString() =
        PolarsWrapper.DataFrameToString handle

    /// <summary>
    /// Print the DataFrame to Console (Stdout).
    /// </summary>
    member this.Show() =
        printfn "%s" (this.ToString())
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
    // Properties
    member _.Rows = PolarsWrapper.DataFrameHeight handle
    member _.Height = PolarsWrapper.DataFrameHeight handle
    member _.Len = PolarsWrapper.DataFrameHeight handle
    member _.Width = PolarsWrapper.DataFrameWidth handle
    /// <summary>
    /// Returns the shape of the DataFrame as (Height, Width).
    /// </summary>
    member this.Shape = this.Len,this.Width
    member _.ColumnNames = PolarsWrapper.GetColumnNames handle
    member _.Columns = PolarsWrapper.GetColumnNames handle
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
        | :? ListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                let start = listArr.ValueOffsets.[rowIndex]
                let end_ = listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        // Case B: Large List (64-bit offsets) 
        | :? LargeListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
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

    // 4. DateTime (DateTime)
    member this.DateTime(col: string, row: int) : DateTime option =
        use s = this.Column col
        s.DateTime row

    // 5. Duration (TimeSpan)
    member this.Duration(col: string, row: int) : TimeSpan option =
        use s = this.Column col
        s.Duration row
    member this.Column(name: string) : Series =
        let h = PolarsWrapper.DataFrameGetColumn(this.Handle, name)
        new Series(h)
    member this.Column(index: int) : Series =
        let h = PolarsWrapper.DataFrameGetColumnAt(this.Handle, index)
        new Series(h)

    member this.GetSeries() : Series list =
        [ for i in 0 .. int this.Width - 1 -> this.Column i ]
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
    /// Helper to get a cell value as an F# List directly.
    /// </summary>
    member this.CellList<'T>(colName: string,row:int) : 'T list =
        let s = this.Column colName
        s.GetList<'T>(int64 row)
    // ==========================================
    // Indexers (Syntax Sugar)
    // ==========================================
    member this.Item (columnName: string) : Series =
        this.Column columnName
    
    member this.Item (columnIndex: int) : Series =
        this.Column columnIndex
    /// <summary>
    /// [Indexer] Access cell value by Row Index and Column Name.
    /// Syntax: df.[rowIndex, "colName"]
    /// </summary>
    member this.Item (rowIndex: int, columnName: string) : obj =
        let series = this.Column columnName
        series.[rowIndex]

    /// <summary>
    /// [Indexer] Access cell value by Row Index and Column Index.
    /// Syntax: df.[rowIndex, colIndex]
    /// </summary>
    member this.Item (rowIndex: int, columnIndex: int) : obj =
        let series = this.Column columnIndex
        series.[rowIndex]
    /// <summary>
    /// Get a value from the DataFrame using a generic type argument.
    /// Eliminates the need for unbox, but throws if type mismatches.
    /// </summary>
    member this.Cell<'T>(colName: string ,rowIndex: int) : 'T =
        let s = this.Column colName
        s.GetValue<'T>(int64 rowIndex)
    /// <summary>
    /// Get a value from the DataFrame using a generic type argument.
    /// Eliminates the need for unbox, but throws if type mismatches.
    /// </summary>
    member this.Cell<'T>(rowIndex: int,colName: string ) : 'T =
        let s = this.Column colName
        s.GetValue<'T>(int64 rowIndex)

    // ==========================================
    // Row Access
    // ==========================================

    /// <summary>
    /// Get data for a specific row as an object array.
    /// Similar to DataTable.Rows[i].ItemArray.
    /// </summary>
    member this.Row (index: int) : obj[] =
        let h = int64 this.Rows
        if int64 index < 0L || int64 index >= h then
            raise (IndexOutOfRangeException(sprintf "Row index %d is out of bounds. Height: %d" index h))

        let w = this.Columns.Length
        let rowData = Array.zeroCreate<obj> w

        for i in 0 .. w - 1 do
            rowData.[i] <- this.[index, i]

        rowData

    /// <summary>
    /// Export DataFrame As DbDataReader (Zero-Copy Enabled)
    /// </summary>
    member this.AsDataReader(?bufferSize: int, ?typeOverrides: Dictionary<string, Type>) : DbDataReader =

        let bufferSize = defaultArg bufferSize 5
        let overrides = defaultArg typeOverrides null

        let options = BoundedChannelOptions(bufferSize, 
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = true
        )

        let channel = Channel.CreateBounded<RecordBatch> options
        let cts = new CancellationTokenSource()

        let producerTask = Task.Run(fun () ->
            try
                this.ExportBatches(fun batch ->
                    channel.Writer.WriteAsync(batch, cts.Token).AsTask().Wait cts.Token
                )
                channel.Writer.Complete()
            with
            | :? OperationCanceledException -> 
                channel.Writer.Complete()
            | ex -> 
                channel.Writer.Complete ex
        )

        let stream = channel.Reader.ReadAllAsync(cts.Token).ToBlockingEnumerable cts.Token
        
        let innerReader = new ArrowToDbStream(stream, overrides)

        new PolarsDataReader(innerReader, cts, producerTask) :> DbDataReader
    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream.
    /// Supports zero-copy and lazy chunked reading.
    /// </summary>
    /// <param name="seed">Optional seed for Native Global Shuffle.</param>
    /// <returns>Standard IArrowArrayStream</returns>
    member this.ToArrowStream(?seed: uint64) : IArrowArrayStream = 
        let nullableSeed = Option.toNullable seed
        ArrowStreamInterop.ExportToStream(this.Handle, ReadOnlySpan<int>.Empty, nullableSeed)

    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream with Column Pruning.
    /// </summary>
    /// <param name="columnIndices">Column indices to prune the export (Projection Pushdown).</param>
    /// <param name="seed">Optional seed for Native Global Shuffle.</param>
    member this.ToArrowStream(columnIndices: ReadOnlySpan<int>, ?seed: uint64) : IArrowArrayStream = 
        let nullableSeed = Option.toNullable seed
        ArrowStreamInterop.ExportToStream(this.Handle, columnIndices, nullableSeed)

    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream with Column Pruning (Array friendly).
    /// </summary>
    /// <param name="columnIndices">Column indices array to prune the export (Projection Pushdown).</param>
    /// <param name="seed">Optional seed for Native Global Shuffle.</param>
    member this.ToArrowStream(columnIndices: int array, ?seed: uint64) : IArrowArrayStream = 
        let span = if isNull columnIndices then ReadOnlySpan<int>.Empty else ReadOnlySpan<int>(columnIndices)
        let nullableSeed = Option.toNullable seed
        ArrowStreamInterop.ExportToStream(this.Handle, span, nullableSeed)

    // ==========================================
    // IEnumerable<Series> Support
    // ==========================================
    interface IEnumerable<Series> with
        member this.GetEnumerator() : IEnumerator<Series> =
            let seq = seq {
                let w = this.Columns.Length
                for i in 0 .. w - 1 do
                    yield this.Column i
            }
            seq.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<Series>).GetEnumerator() :> IEnumerator
    
    interface IPolarsDataFrame with
        
        member this.Height = 
            int64 this.Height 
            
        member this.Schema = 
            this.Schema :> IPolarsSchema
        member this.Show(): unit = 
            this.Show()    
        member this.ToArrow() = 
            this.ToArrow()
        member this.WriteToAdbc(statement:AdbcStatement) = 
            this.WriteToAdbc statement
        member this.Column(index:int) = 
            this.Column index
        member this.ToArrowStream(columnIndices: ReadOnlySpan<int>, seed: Nullable<uint64>) =
            ArrowStreamInterop.ExportToStream(this.Handle, columnIndices, seed)
/// <summary>
/// A LazyFrame represents a logical plan of operations that will be optimized and executed only when collected.
/// <para>
/// Operations on LazyFrame are not executed immediately. Instead, they build a query plan.
/// Use <c>Collect()</c> to execute the plan and get a DataFrame.
/// </para>
/// </summary>
and LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    abstract member Dispose : unit -> unit
    default x.Dispose() = 
        handle.Dispose()

    interface IDisposable with
        member x.Dispose() = x.Dispose()

    interface IPolarsLazyFrame with
        
        member this.Collect(useStreaming: bool) = 
            this.Collect useStreaming :> IPolarsDataFrame
            
        member this.Schema = 
            this.Schema :> IPolarsSchema
            
        member this.Explain(optimized: bool) = 
            this.Explain optimized
        member this.CollectAsync(useStreaming: bool, cancellationToken: CancellationToken) =
            task {
                let! df = this.CollectAsync(useStreaming, cancellationToken)
                
                return df :> IPolarsDataFrame
            }
    member internal this.CloneHandle() = PolarsWrapper.LazyClone handle
    /// <summary> Execute the plan and return a DataFrame. </summary>
    member this.Collect(?streaming:bool) = 
        let stream = defaultArg streaming false
        let dfHandle = PolarsWrapper.LazyCollect(handle,stream)
        new DataFrame(dfHandle)
    member this.CollectAsync
        (
            ?useStreaming: bool, 
            ?cancellationToken: CancellationToken
        ) : Task<DataFrame> =
        let us = defaultArg useStreaming false
        let cct = defaultArg cancellationToken CancellationToken.None
        
        task {
            cct.ThrowIfCancellationRequested()

            let! dfHandle = PolarsWrapper.LazyCollectAsync(handle, us,cct)
            
            return new DataFrame(dfHandle)
        }
    /// <summary> Execute the plan using the streaming engine. </summary>
    member _.CollectStreaming() =
        let dfHandle = PolarsWrapper.CollectStreaming handle
        new DataFrame(dfHandle)

    /// <summary>
    /// Get the schema of the LazyFrame without executing it.
    /// Uses Zero-Copy native introspection.
    /// </summary>
    member this.Schema =
        let h = PolarsWrapper.GetLazySchema this.Handle 
        new PolarsSchema(h)
    member this.PrintSchema() =
        printfn "--- LazyFrame Schema ---"
        
        use sc = this.Schema

        sc.ToMap() 
        |> Map.iter (fun name dtype -> 
            printfn "%-15s | %O" name dtype
        )
        
        printfn "------------------------"

    /// <summary> Print the query plan. </summary>
    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)
    // ---------------------------------------------------------
    // Scan CSV (File / Cloud / Glob)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily scans a CSV file into a LazyFrame.
    /// <para>
    /// This allows for query optimization (predicate pushdown, projection pushdown) 
    /// and streaming processing of datasets larger than memory.
    /// </para>
    /// </summary>
    static member ScanCsv(
        path: string,
        ?schema: PolarsSchema,
        ?dtypeOverride:PolarsSchema,
        ?hasHeader: bool,
        ?separator: char,
        ?quoteChar: char,
        ?eolChar: char,
        ?ignoreErrors: bool,
        ?tryParseDates: bool,
        ?lowMemory: bool,
        ?cache: bool,
        ?glob: bool,
        ?rechunk: bool,
        ?raiseIfEmpty: bool,
        ?skipRows: uint64,
        ?skipRowsAfterHeader: uint64,
        ?skipLines: uint64,
        ?nRows: uint64,
        ?inferSchemaLength: uint64,
        ?nThreads: uint64,
        ?chunkSize: uint64,
        ?rowIndexName: string,
        ?rowIndexOffset: uint64,
        ?includeFilePaths: string,
        ?encoding: CsvEncoding,
        ?nullValues: seq<string>,
        ?missingIsNull: bool,
        ?commentPrefix: string,
        ?decimalComma: bool,
        ?truncateRaggedLines: bool,
        ?cloudOptions: CloudOptions
    ) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let dtypeOverrideHandle = match dtypeOverride with Some s -> s.Handle | None -> null
        let pHasHdr = defaultArg hasHeader true
        let pSep = defaultArg separator ','
        let pQuote = match quoteChar with Some c -> System.Nullable c | None -> System.Nullable '"'
        let pEol = defaultArg eolChar '\n'
        let pIgnoreErr = defaultArg ignoreErrors false
        let pTryDates = defaultArg tryParseDates true
        let pLowMem = defaultArg lowMemory false
        let pCache = defaultArg cache true
        let pGlob = defaultArg glob true
        let pRechunk = defaultArg rechunk false
        let pRaiseEmpty = defaultArg raiseIfEmpty true
        let pSkipR = defaultArg skipRows 0UL
        let pSkipRAH = defaultArg skipRowsAfterHeader 0UL
        let pSkipL = defaultArg skipLines 0UL
        let pNRows = Option.toNullable nRows
        let pInferLen = match inferSchemaLength with Some v -> System.Nullable v | None -> System.Nullable 100UL
        let pNTh = Option.toNullable nThreads
        let pChunkSz = Option.toNullable chunkSize
        let pRowIdxName = Option.toObj rowIndexName
        let pRowIdxOff = defaultArg rowIndexOffset 0UL
        let pIncPaths = Option.toObj includeFilePaths
        let pEnc = defaultArg encoding CsvEncoding.UTF8
        let pNullVals = nullValues |> Option.map Seq.toArray |> Option.toObj
        let pMissNull = defaultArg missingIsNull true
        let pComment = Option.toObj commentPrefix
        let pDecComma = defaultArg decimalComma false
        let pTruncRagged = defaultArg truncateRaggedLines false

        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals =
            CloudOptions.ParseCloudOptions cloudOptions

        let h = PolarsWrapper.ScanCsv(
            path,
            schemaHandle,
            dtypeOverrideHandle,
            pHasHdr,
            pSep,
            pQuote,
            pEol,
            pIgnoreErr,
            pTryDates,
            pLowMem,
            pCache,
            pGlob,
            pRechunk,
            pRaiseEmpty,
            pSkipR,
            pSkipRAH,
            pSkipL,
            pNRows,
            pInferLen,
            pNTh,
            pChunkSz,
            pRowIdxName,
            pRowIdxOff,
            pIncPaths,
            pEnc.ToNative(),
            pNullVals,
            pMissNull,
            pComment,
            pDecComma,
            pTruncRagged,
            cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals
        )
        new LazyFrame(h)

    // ---------------------------------------------------------
    // Scan CSV (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily scans a CSV from an in-memory byte array.
    /// </summary>
    static member ScanCsv(
        buffer: byte[],
        ?schema: PolarsSchema,
        ?dtypeOverride:PolarsSchema,
        ?hasHeader: bool,
        ?separator: char,
        ?quoteChar: char,
        ?eolChar: char,
        ?ignoreErrors: bool,
        ?tryParseDates: bool,
        ?lowMemory: bool,
        ?cache: bool,
        ?glob: bool,
        ?rechunk: bool,
        ?raiseIfEmpty: bool,
        ?skipRows: uint64,
        ?skipRowsAfterHeader: uint64,
        ?skipLines: uint64,
        ?nRows: uint64,
        ?inferSchemaLength: uint64,
        ?nThreads: uint64,
        ?chunkSize: uint64,
        ?rowIndexName: string,
        ?rowIndexOffset: uint64,
        ?includeFilePaths: string,
        ?encoding: CsvEncoding,
        ?nullValues: seq<string>,
        ?missingIsNull: bool,
        ?commentPrefix: string,
        ?decimalComma: bool,
        ?truncateRaggedLines: bool
    ) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let dtypeOverrideHandle = match dtypeOverride with Some s -> s.Handle | None -> null
        let pHasHdr = defaultArg hasHeader true
        let pSep = defaultArg separator ','
        let pQuote = match quoteChar with Some c -> System.Nullable c | None -> System.Nullable '"'
        let pEol = defaultArg eolChar '\n'
        let pIgnoreErr = defaultArg ignoreErrors false
        let pTryDates = defaultArg tryParseDates true
        let pLowMem = defaultArg lowMemory false
        let pCache = defaultArg cache true
        let pGlob = defaultArg glob true
        let pRechunk = defaultArg rechunk false
        let pRaiseEmpty = defaultArg raiseIfEmpty true
        let pSkipR = defaultArg skipRows 0UL
        let pSkipRAH = defaultArg skipRowsAfterHeader 0UL
        let pSkipL = defaultArg skipLines 0UL
        let pNRows = Option.toNullable nRows
        let pInferLen = match inferSchemaLength with Some v -> System.Nullable v | None -> System.Nullable 100UL
        let pNTh = Option.toNullable nThreads
        let pChunkSz = Option.toNullable chunkSize
        let pRowIdxName = Option.toObj rowIndexName
        let pRowIdxOff = defaultArg rowIndexOffset 0UL
        let pIncPaths = Option.toObj includeFilePaths
        let pEnc = defaultArg encoding CsvEncoding.UTF8
        let pNullVals = nullValues |> Option.map Seq.toArray |> Option.toObj
        let pMissNull = defaultArg missingIsNull true
        let pComment = Option.toObj commentPrefix
        let pDecComma = defaultArg decimalComma false
        let pTruncRagged = defaultArg truncateRaggedLines false

        let h = PolarsWrapper.ScanCsv(
            buffer,
            schemaHandle,
            dtypeOverrideHandle,
            pHasHdr,
            pSep,
            pQuote,
            pEol,
            pIgnoreErr,
            pTryDates,
            pLowMem,
            pCache,
            pGlob,
            pRechunk,
            pRaiseEmpty,
            pSkipR,
            pSkipRAH,
            pSkipL,
            pNRows,
            pInferLen,
            pNTh,
            pChunkSz,
            pRowIdxName,
            pRowIdxOff,
            pIncPaths,
            pEnc.ToNative(),
            pNullVals,
            pMissNull,
            pComment,
            pDecComma,
            pTruncRagged
        )
        new LazyFrame(h)

    // ---------------------------------------------------------
    // Scan CSV (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily scans a CSV from a Stream.
    /// <para>
    /// This reads the stream fully into memory to construct the Lazy execution plan.
    /// </para>
    /// </summary>
    static member ScanCsv(
        stream: System.IO.Stream,
        ?schema: PolarsSchema,
        ?hasHeader: bool,
        ?separator: char,
        ?quoteChar: char,
        ?eolChar: char,
        ?ignoreErrors: bool,
        ?tryParseDates: bool,
        ?lowMemory: bool,
        ?cache: bool,
        ?glob: bool,
        ?rechunk: bool,
        ?raiseIfEmpty: bool,
        ?skipRows: uint64,
        ?skipRowsAfterHeader: uint64,
        ?skipLines: uint64,
        ?nRows: uint64,
        ?inferSchemaLength: uint64,
        ?nThreads: uint64,
        ?chunkSize: uint64,
        ?rowIndexName: string,
        ?rowIndexOffset: uint64,
        ?includeFilePaths: string,
        ?encoding: CsvEncoding,
        ?nullValues: seq<string>,
        ?missingIsNull: bool,
        ?commentPrefix: string,
        ?decimalComma: bool,
        ?truncateRaggedLines: bool
    ) : LazyFrame =
        
        use ms = new System.IO.MemoryStream()
        stream.CopyTo(ms)
        let bytes = ms.ToArray()

        LazyFrame.ScanCsv(
            bytes,
            ?schema = schema,
            ?hasHeader = hasHeader,
            ?separator = separator,
            ?quoteChar = quoteChar,
            ?eolChar = eolChar,
            ?ignoreErrors = ignoreErrors,
            ?tryParseDates = tryParseDates,
            ?lowMemory = lowMemory,
            ?cache = cache,
            ?glob = glob,
            ?rechunk = rechunk,
            ?raiseIfEmpty = raiseIfEmpty,
            ?skipRows = skipRows,
            ?skipRowsAfterHeader = skipRowsAfterHeader,
            ?skipLines = skipLines,
            ?nRows = nRows,
            ?inferSchemaLength = inferSchemaLength,
            ?nThreads = nThreads,
            ?chunkSize = chunkSize,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includeFilePaths = includeFilePaths,
            ?encoding = encoding,
            ?nullValues = nullValues,
            ?missingIsNull = missingIsNull,
            ?commentPrefix = commentPrefix,
            ?decimalComma = decimalComma,
            ?truncateRaggedLines = truncateRaggedLines
        )    /// <summary> Helper: Scan CSV with default settings </summary>
    static member ScanCsv(path: string) = 
        LazyFrame.ScanCsv(path, hasHeader=true)
    /// <summary> Scan a parquet file into a LazyFrame. </summary>
    /// <summary>
    /// Lazily read from a parquet file or a common cloud store (S3, GCS, Azure, etc.).
    /// </summary>
    /// <param name="path">Path to file or cloud location.</param>
    /// <param name="nRows">Stop reading after n rows.</param>
    /// <param name="parallel">Parallel strategy (Auto, Columns, RowGroups, None).</param>
    /// <param name="lowMemory">Reduce memory pressure at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to prune row groups.</param>
    /// <param name="glob">Expand path using globbing rules.</param>
    /// <param name="allowMissingColumns">If true, do not fail if columns are missing.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks when reading. (default: false)</param>
    /// <param name="cache">Cache the result after reading. (default: true)</param>
    /// <param name="rowIndexName">If provided, add a row index column with this name.</param>
    /// <param name="rowIndexOffset">Start index for the row index column.</param>
    /// <param name="includePathColumn">If provided, add a column with the path of the file.</param>
    /// <param name="schema">Overwrite the schema of the dataset.</param>
    /// <param name="hivePartitionSchema">The schema of the hive partitions.</param>
    /// <param name="tryParseHiveDates">Attempt to parse hive values as Date/Datetime.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    static member ScanParquet
        (
            path: string,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
            ?allowMissingColumns: bool,
            ?rechunk:bool,
            ?cache:bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool,
            ?cloudOptions: CloudOptions
        ) =
        // Defaults
        let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
        let pLowMem = defaultArg lowMemory false
        let pStats = defaultArg useStatistics true
        let pGlob = defaultArg glob true
        let pAllowMissing = defaultArg allowMissingColumns false
        let pRowIndexOffset = defaultArg rowIndexOffset 0u
        let pTryHive = defaultArg tryParseHiveDates false
        let pRechunk = defaultArg rechunk false
        let pCache = defaultArg cache false
        let pHivePartitioning = defaultArg hivePartitioning false

        // F# Types -> C# Interop Types
        
        // nRows: uint64 option -> ulong? (Nullable<ulong>)
        let pNRows = 
            nRows 
            |> Option.toNullable

        // Schema: Schema Object -> SchemaHandle (Raw Pointer holder)
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        let hHiveSchema = 
            hivePartitionSchema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        // Cloud Options Unwrapping Logic
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // Call Wrapper
        let handle = PolarsWrapper.ScanParquet(
            path,
            pNRows,
            pParallel.ToNative(),
            pLowMem,
            pStats,
            pGlob,
            pAllowMissing,
            pRechunk,
            pCache,
            Option.toObj rowIndexName,
            pRowIndexOffset,
            Option.toObj includePathColumn,
            hSchema,
            pHivePartitioning,
            hHiveSchema,
            pTryHive,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )
        
        new LazyFrame(handle)
    /// <summary>
    /// [Memory] Lazily read parquet from a byte array (in-memory buffer).
    /// </summary>
    /// <param name="buffer">The byte array containing parquet data.</param>
    /// <param name="nRows">Stop reading after n rows.</param>
    /// <param name="parallel">Parallel strategy (Auto, Columns, RowGroups, None).</param>
    /// <param name="lowMemory">Reduce memory pressure at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to prune row groups.</param>
    /// <param name="glob">Globbing patterns (usually irrelevant for memory scan,always false).</param>
    /// <param name="allowMissingColumns">If true, do not fail if columns are missing.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks when reading. (default: false)</param>
    /// <param name="cache">Cache the result after reading. (default: true)</param>
    /// <param name="rowIndexName">If provided, add a row index column with this name.</param>
    /// <param name="rowIndexOffset">Start index for the row index column.</param>
    /// <param name="includePathColumn">If provided, add a column with the path (usually irrelevant for memory).</param>
    /// <param name="schema">Overwrite the schema of the dataset.</param>
    /// <param name="hivePartitionSchema">The schema of the hive partitions.</param>
    /// <param name="tryParseHiveDates">Attempt to parse hive values as Date/Datetime.</param>
    static member ScanParquet
        (
            buffer: byte[],
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
            ?allowMissingColumns: bool,
            ?rechunk:bool,
            ?cache:bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,       
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,   
            ?tryParseHiveDates: bool
        ) =
        let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
        let pLowMem = defaultArg lowMemory false
        let pStats = defaultArg useStatistics true
        let _pGlob = defaultArg glob true
        let pAllowMissing = defaultArg allowMissingColumns false
        let pRowIndexOffset = defaultArg rowIndexOffset 0u
        let pTryHive = defaultArg tryParseHiveDates false
        let pRechunk = defaultArg rechunk false
        let pCache = defaultArg cache false
        let pHivePartitioning = defaultArg hivePartitioning false

        // 2. Type Conversions
        let pNRows = 
            nRows 
            |> Option.toNullable

        // Extract Handle from PolarsSchema wrapper
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        let hHiveSchema = 
            hivePartitionSchema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        // 3. Call C# Wrapper (Memory Overload)
        let handle = PolarsWrapper.ScanParquet(
            buffer,
            pNRows,
            pParallel.ToNative(),
            pLowMem,
            pStats,
            false,
            pAllowMissing,
            pRechunk,
            pCache,
            Option.toObj rowIndexName,
            pRowIndexOffset,
            Option.toObj includePathColumn,
            hSchema,
            pHivePartitioning,
            hHiveSchema,
            pTryHive
        )

        new LazyFrame(handle)
    /// <summary>
    /// Lazily read a NDJSON file.
    /// </summary>
    static member ScanNdjson(path: string,
                             ?schema: PolarsSchema,
                             ?inferSchemaLen: uint64,
                             ?batchSize: uint64,
                             ?nRows: uint64,
                             ?lowMemory: bool,
                             ?rechunk: bool,
                             ?ignoreErrors: bool,
                             ?rowIndexName: string,
                             ?rowIndexOffset: uint32,
                             ?includePathColumn: string,
                             ?cloudOptions: CloudOptions) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        let rows = Option.toNullable nRows
        
        let lowMem = defaultArg lowMemory false
        let rechk = defaultArg rechunk false
        let ignoreErr = defaultArg ignoreErrors false
        
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let pathCol = Option.toObj includePathColumn

        // Parse Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        let h = PolarsWrapper.ScanNdjson(
            path, 
            schemaHandle, 
            batch, 
            inferLen, 
            rows, 
            lowMem, 
            rechk, 
            ignoreErr, 
            idxName, 
            idxOffset, 
            pathCol,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )
        new LazyFrame(h)

    /// <summary>
    /// Lazily read NDJSON from in-memory bytes.
    /// </summary>
    static member ScanNdjson(buffer: byte[],
                             ?schema: PolarsSchema,
                             ?inferSchemaLen: uint64,
                             ?batchSize: uint64,
                             ?nRows: uint64,
                             ?lowMemory: bool,
                             ?rechunk: bool,
                             ?ignoreErrors: bool,
                             ?rowIndexName: string,
                             ?rowIndexOffset: uint32) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        let rows = Option.toNullable nRows
        
        let lowMem = defaultArg lowMemory false
        let rechk = defaultArg rechunk false
        let ignoreErr = defaultArg ignoreErrors false
        
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        
        let h = PolarsWrapper.ScanNdjson(
            buffer, 
            schemaHandle, 
            batch, 
            inferLen, 
            rows, 
            lowMem, 
            rechk, 
            ignoreErr, 
            idxName, 
            idxOffset, 
            null 
        )
        new LazyFrame(h)

    /// <summary>
    /// Lazily read NDJSON from a Stream.
    /// </summary>
    static member ScanNdjson(stream: Stream,
                             ?schema: PolarsSchema,
                             ?inferSchemaLen: uint64,
                             ?batchSize: uint64,
                             ?nRows: uint64,
                             ?lowMemory: bool,
                             ?rechunk: bool,
                             ?ignoreErrors: bool,
                             ?rowIndexName: string,
                             ?rowIndexOffset: uint32) : LazyFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()

        LazyFrame.ScanNdjson(
            bytes,
            ?schema=schema,
            ?inferSchemaLen=inferSchemaLen,
            ?batchSize=batchSize,
            ?nRows=nRows,
            ?lowMemory=lowMemory,
            ?rechunk=rechunk,
            ?ignoreErrors=ignoreErrors,
            ?rowIndexName=rowIndexName,
            ?rowIndexOffset=rowIndexOffset
        )
    // ---------------------------------------------------------
    // Scan IPC (File / Cloud / Glob)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read an Arrow IPC (Feather v2) file, multiple files via glob patterns, or cloud storage.
    /// </summary>
    static member ScanIpc(
        path: string,
        ?schema: PolarsSchema,
        ?nRows: uint64,
        ?rechunk: bool,
        ?cache: bool,
        ?glob: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool,
        ?cloudOptions: CloudOptions
    ) : LazyFrame =
        
        // Resolve Optional Handles
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null
        
        // Resolve Defaults
        let rows = Option.toNullable nRows
        let rechk = defaultArg rechunk false
        let useCache = defaultArg cache true 
        let useGlob = defaultArg glob true
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let pathCol = Option.toObj includePathColumn
        let hive = defaultArg hivePartitioning false
        let hiveDates = defaultArg tryParseHiveDates true

        // Parse Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        let h = PolarsWrapper.ScanIpc(
            path, 
            rows, 
            rechk, 
            useCache, 
            useGlob,
            idxName, 
            idxOffset, 
            pathCol, 
            schemaHandle,
            hive,
            hiveSchemaHandle,
            hiveDates,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )
        new LazyFrame(h)

    // ---------------------------------------------------------
    // Scan IPC (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from in-memory bytes.
    /// </summary>
    static member ScanIpc(
        buffer: byte[],
        ?schema: PolarsSchema,
        ?nRows: uint64,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool
    ) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null
        
        let rows = Option.toNullable nRows
        let rechk = defaultArg rechunk false
        let useCache = defaultArg cache true
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let pathCol = Option.toObj includePathColumn
        let hive = defaultArg hivePartitioning false
        let hiveDates = defaultArg tryParseHiveDates false

        let h = PolarsWrapper.ScanIpc(
            buffer, 
            rows, 
            rechk, 
            useCache, 
            idxName, 
            idxOffset, 
            pathCol,
            schemaHandle, 
            hive,
            hiveSchemaHandle,
            hiveDates
        )
        new LazyFrame(h)

    // ---------------------------------------------------------
    // Scan IPC (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from a Stream.
    /// </summary>
    /// <remarks>
    /// This reads the stream fully into memory to construct the Lazy execution plan.
    /// </remarks>
    static member ScanIpc(
        stream: System.IO.Stream,
        ?schema: PolarsSchema,
        ?nRows: uint64,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool
    ) : LazyFrame =
        
        use ms = new System.IO.MemoryStream()
        stream.CopyTo(ms)
        let bytes = ms.ToArray()

        LazyFrame.ScanIpc(
            bytes,
            ?schema = schema,
            ?nRows = nRows,
            ?rechunk = rechunk,
            ?cache = cache,
            ?rowIndexName = rowIndexName,
            ?rowIndexOffset = rowIndexOffset,
            ?includePathColumn = includePathColumn,
            ?hivePartitioning = hivePartitioning,
            ?hivePartitionSchema = hivePartitionSchema,
            ?tryParseHiveDates = tryParseHiveDates
        )
    // ---------------------------------------------------------
    // Scan Delta Lake
    // ---------------------------------------------------------

    /// <summary>
    /// Create a LazyFrame by scanning a Delta Lake table.
    /// </summary>
    /// <param name="path">Path to the Delta Lake table (folder containing _delta_log).</param>
    /// <param name="version">The version of the table to read (e.g., 0L, 1L). Mutually exclusive with <paramref name="datetime"/>.</param>
    /// <param name="datetime">The timestamp to read (ISO-8601 string, e.g., "2026-02-09T12:00:00Z"). Mutually exclusive with <paramref name="version"/>.</param>
    /// <returns>A new LazyFrame.</returns>
    static member ScanDelta(
        path: string,
        ?version: int64,
        ?datetime: string,
        ?nRows: uint64,
        ?parallelStrategy: ParallelStrategy,
        ?lowMemory: bool,
        ?useStatistics: bool,
        ?glob: bool,
        ?rechunk: bool,
        ?cache: bool,
        ?rowIndexName: string,
        ?rowIndexOffset: uint32,
        ?includePathColumn: string,
        ?schema: PolarsSchema,
        ?hivePartitioning: bool,
        ?hivePartitionSchema: PolarsSchema,
        ?tryParseHiveDates: bool,
        ?cloudOptions: CloudOptions
    ) : LazyFrame =
        
        // Time Travel Mutual Exclusivity Check
        if version.IsSome && datetime.IsSome then
            invalidArg "version/datetime" "Cannot specify both 'version' and 'datetime' for Delta Time Travel."

        // Resolve Defaults & Nullables
        let pVersion = Option.toNullable version
        let pDatetime = Option.toObj datetime
        let pNRows = Option.toNullable nRows
        
        let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
        let pLowMem = defaultArg lowMemory false
        let pUseStats = defaultArg useStatistics true
        let pGlob = defaultArg glob true
        let pRechunk = defaultArg rechunk false
        let pCache = defaultArg cache true
        
        let pRowIdxName = Option.toObj rowIndexName
        let pRowIdxOff = defaultArg rowIndexOffset 0u
        let pIncPathCol = Option.toObj includePathColumn
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        
        // Hive Defaults (Delta typically defaults to Hive partitioning = true)
        let pHivePart = defaultArg hivePartitioning true
        let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null
        let pTryHiveDates = defaultArg tryParseHiveDates true

        // Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        let h = PolarsWrapper.ScanDelta(
            path,
            pVersion,
            pDatetime,
            pNRows,
            pParallel.ToNative(),
            pLowMem,
            pUseStats,
            pGlob,
            pRechunk,
            pCache,
            pRowIdxName,
            pRowIdxOff,
            pIncPathCol,
            schemaHandle,
            pHivePart,
            hiveSchemaHandle,
            pTryHiveDates,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

        new LazyFrame(h)
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
    /// <param name="useBuffered">Choose whether disk buffer file needed (for big data) <param>
    static member scanSeq<'T>(data: seq<'T>, ?batchSize: int, ?useBuffered: bool) : LazyFrame =
            let size = defaultArg batchSize 100_000
            let buffered = defaultArg useBuffered false

            // =========================================================
            // 1. Buffered Mode (Disk IPC)
            // =========================================================
            if buffered then
                let scope = new IpcStreamService.TempIpcScope<'T>(data, size)
                
                // Get FileHandle
                let handle = LazyFrame.ScanIpc(scope.FilePath).Handle
                
                { new LazyFrame(handle) with
                    member this.Dispose() =
                        base.Dispose()
                        scope.Dispose()
                }

            // =========================================================
            // 2. Streaming Mode (Memory Safety & Lazy Fallback)
            // =========================================================
            else
                let schema = ArrowConverter.GetSchemaFromType<'T>()

                let streamFactory = Func<IEnumerable<RecordBatch>>(fun () ->
                    seq {
                        let mutable hasYielded = false
                        
                        let batches = ArrowConverter.ToArrowBatches(data, size).Prefetch()

                        for batch in batches do
                            hasYielded <- true
                            yield batch
                            batch.Dispose()
                        
                        if not hasYielded then
                            let emptyBatch = ArrowConverter.GetEmptyBatch<'T>()
                            yield emptyBatch
                            emptyBatch.Dispose()
                    }
                )

                let handle = ArrowStreamInterop.ScanStream(streamFactory, schema)
                new LazyFrame(handle)

    /// <summary>
    /// Scan a database query lazily.
    /// Requires a factory function to create new IDataReaders for potential multi-pass scans.
    /// </summary>
    static member scanDb(readerFactory: unit -> IDataReader, ?batchSize: int, ?useBuffered: bool) : LazyFrame =
        let size = defaultArg batchSize 50_000
        let buffered = defaultArg useBuffered false

        // =========================================================
        // 1. Buffered Mode (Disk IPC)
        // =========================================================
        if buffered then
            let runBuffer () =
                use reader = readerFactory()
                new IpcStreamService.TempIpcScopeReader(reader, size)

            let scope = runBuffer()
            let handle = LazyFrame.ScanIpc(scope.FilePath).Handle

            { new LazyFrame(handle) with
                member this.Dispose() =
                    base.Dispose()
                    scope.Dispose()
            }

        // =========================================================
        // 2. Streaming Mode (Memory)
        // =========================================================
        else
            // Probe Schema
            let schema = 
                use reader = readerFactory()
                ArrowTypeResolver.GetSchemaFromDataReader reader

            // Stream Factory
            let factory = Func<IEnumerable<RecordBatch>>(fun () ->
                seq {
                    use reader = readerFactory()
                    let batches = DbToArrowStream.ToArrowBatches(reader, size).Prefetch()
                    
                    for batch in batches do
                        yield batch
                        batch.Dispose()
                }
            )

            let handle = ArrowStreamInterop.ScanStream(factory, schema)
            new LazyFrame(handle)

    /// <summary>
    /// [Lazy][Streaming] Scan a database DataReader directly.
    /// <para>Upgraded to pure memory Streaming Mode using Arrow C-Data FFI!</para>
    /// </summary>
    static member scanDb(reader: IDataReader, ?batchSize: int) : LazyFrame =
        let size = defaultArg batchSize 50_000
        let schema = reader.GetArrowSchema()
        
        let stream = reader.ToArrowBatches(size).Prefetch() 
        
        let factory = Func<IEnumerable<RecordBatch>>(fun () -> stream)
        let handle = ArrowStreamInterop.ScanStream(factory, schema)
        
        new LazyFrame(handle)
    /// <summary>
    /// Execute the LazyFrame and sink the result to a CSV file.
    /// <para>
    /// This operation allows processing datasets larger than memory by streaming results 
    /// directly to the file system.
    /// </para>
    /// </summary>
    member this.SinkCsv(
        path: string,
        ?includeHeader: bool,
        ?includeBom: bool,
        ?separator: char,
        ?quoteChar: char,
        ?quoteStyle: QuoteStyle,
        ?nullValue: string,
        ?lineTerminator: string,
        ?floatScientific: bool,
        ?floatPrecision: int,
        ?decimalComma: bool,
        ?dateFormat: string,
        ?timeFormat: string,
        ?datetimeFormat: string,
        ?checkExtension: bool,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?batchSize: int,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let incHdr = defaultArg includeHeader true
        let incBom = defaultArg includeBom false
        let sep = defaultArg separator ','
        let qChar = defaultArg quoteChar '"'
        let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
        let nVal = defaultArg nullValue null
        let lTerm = defaultArg lineTerminator "\n"
        let fSci = Option.toNullable floatScientific
        let fPrec = Option.toNullable floatPrecision
        let dComma = defaultArg decimalComma false
        let dFmt = defaultArg dateFormat null
        let tFmt = defaultArg timeFormat null
        let dtFmt = defaultArg datetimeFormat null
        let chkExt = defaultArg checkExtension true
        let comp = defaultArg compression ExternalCompression.Uncompressed
        let compLvl = defaultArg compressionLevel -1
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false
        let bSize = defaultArg batchSize 0

        // 2. Unpack Cloud Options via external static helper
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 3. Native Call
        PolarsWrapper.SinkCsv(
            this.CloneHandle(),
            path,
            incHdr,
            incBom,
            bSize,
            chkExt,
            comp.ToNative(),
            compLvl,
            sep,
            qChar,
            qStyle.ToNative(),
            nVal,
            lTerm,
            dFmt,
            tFmt,
            dtFmt,
            fSci,
            fPrec,
            dComma,
            mo,
            sync.ToNative(),
            mkd,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Execute the LazyFrame and sink the result to a CSV file, partitioned by the given selector.
    /// </summary>
    member this.SinkCsvPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?includeHeader: bool,
        ?includeBom: bool,
        ?separator: char,
        ?quoteChar: char,
        ?quoteStyle: QuoteStyle,
        ?nullValue: string,
        ?lineTerminator: string,
        ?floatScientific: bool,
        ?floatPrecision: int,
        ?decimalComma: bool,
        ?dateFormat: string,
        ?timeFormat: string,
        ?datetimeFormat: string,
        ?checkExtension: bool,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?batchSize: int,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults & Limits
        let incKeys = defaultArg includeKeys true
        let preGrouped = defaultArg keysPreGrouped false
        let maxRows = defaultArg maxRowsPerFile 0
        let approxBytes = defaultArg approxBytesPerFile 0L

        let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
        let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

        let incHdr = defaultArg includeHeader true
        let incBom = defaultArg includeBom false
        let sep = defaultArg separator ','
        let qChar = defaultArg quoteChar '"'
        let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
        let nVal = defaultArg nullValue null
        let lTerm = defaultArg lineTerminator "\n"
        let fSci = Option.toNullable floatScientific
        let fPrec = Option.toNullable floatPrecision
        let dComma = defaultArg decimalComma false
        let dFmt = defaultArg dateFormat null
        let tFmt = defaultArg timeFormat null
        let dtFmt = defaultArg datetimeFormat null
        let chkExt = defaultArg checkExtension true
        let comp = defaultArg compression ExternalCompression.Uncompressed
        let compLvl = defaultArg compressionLevel -1
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false
        let bSize = defaultArg batchSize 0

        // 2. Unpack Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 3. Native Call
        PolarsWrapper.SinkCsvPartitioned(
            this.CloneHandle(),
            path,
            partitionBy.CloneHandle(),
            incKeys,
            preGrouped,
            maxRowsNuint,
            approxBytesUlong,
            incHdr,
            incBom,
            bSize,
            chkExt,
            comp.ToNative(),
            compLvl,
            sep,
            qChar,
            qStyle.ToNative(),
            nVal,
            lTerm,
            dFmt,
            tFmt,
            dtFmt,
            fSci,
            fPrec,
            dComma,
            mo,
            sync.ToNative(),
            mkd,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sink the LazyFrame to a CSV format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    /// <returns>A byte array containing the serialized CSV data.</returns>
    member this.SinkCsvMemory(
        ?includeBom: bool,
        ?includeHeader: bool,
        ?batchSize: int,
        ?checkExtension: bool,
        ?compressionCode: ExternalCompression,
        ?compressionLevel: int,
        ?dateFormat: string,
        ?timeFormat: string,
        ?datetimeFormat: string,
        ?floatScientific: int, 
        ?floatPrecision: int,
        ?decimalComma: bool,
        ?separator: byte,
        ?quoteChar: byte,
        ?nullValue: string,
        ?lineTerminator: string,
        ?quoteStyle: QuoteStyle,
        ?maintainOrder: bool
    ) =
        let incBom = defaultArg includeBom false
        let incHdr = defaultArg includeHeader true
        let bSize = defaultArg batchSize 1024
        let chkExt = defaultArg checkExtension false
        let comp = defaultArg compressionCode ExternalCompression.Uncompressed
        let compLvl = defaultArg compressionLevel 0
        let dFmt = defaultArg dateFormat null
        let tFmt = defaultArg timeFormat null
        let dtFmt = defaultArg datetimeFormat null
        let fSci = defaultArg floatScientific -1
        let fPrec = defaultArg floatPrecision -1
        let dComma = defaultArg decimalComma false
        let sep = defaultArg separator (byte ',')
        let qChar = defaultArg quoteChar (byte '"')
        let nVal = defaultArg nullValue null
        let lTerm = defaultArg lineTerminator "\n"
        let qStyle = defaultArg quoteStyle QuoteStyle.Necessary
        let mo = defaultArg maintainOrder true

        PolarsWrapper.SinkCsvMemory(
            this.CloneHandle(),
            incBom,
            incHdr,
            bSize,
            chkExt,
            comp.ToNative(),
            compLvl,
            dFmt,
            tFmt,
            dtFmt,
            fSci,
            fPrec,
            dComma,
            sep,
            qChar,
            nVal,
            lTerm,
            qStyle.ToNative(),
            mo
        )
    /// <summary>
    /// Sink the LazyFrame to a Parquet file.
    /// <para>
    /// This allows for streaming execution, processing the data in chunks and writing it to the file
    /// without loading the entire dataset into memory.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the output file.</param>
    /// <param name="compression">Compression codec to use.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Write statistics to the parquet file.</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    member this.SinkParquet(
        path: string,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let comp = defaultArg compression ParquetCompression.Snappy
        let compLevel = defaultArg compressionLevel -1
        let stats = defaultArg statistics false
        let rgs = defaultArg rowGroupSize 0
        let dps = defaultArg dataPageSize 0
        let compat = defaultArg compatLevel -1
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false

        // 2. Unpack Cloud Options via external static helper
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 3. Native Call
        PolarsWrapper.SinkParquet(
            this.CloneHandle(), 
            path,
            comp.ToNative(),
            compLevel,
            stats,
            rgs,
            dps,
            compat,
            mo,
            sync.ToNative(),
            mkd,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sink the LazyFrame to a set of Parquet files, partitioned by the specified selector.
    /// <para>
    /// This writes the dataset to a directory, splitting the data into multiple files based on the
    /// partition key(s) defined in <paramref name="partitionBy"/>.
    /// </para>
    /// </summary>
    member this.SinkParquetPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let incKeys = defaultArg includeKeys true
        let preGrouped = defaultArg keysPreGrouped false
        let maxRows = defaultArg maxRowsPerFile 0
        let approxBytes = defaultArg approxBytesPerFile 0L

        let comp = defaultArg compression ParquetCompression.Snappy
        let compLevel = defaultArg compressionLevel -1
        let stats = defaultArg statistics false
        let rgs = defaultArg rowGroupSize 0
        let dps = defaultArg dataPageSize 0
        let compat = defaultArg compatLevel -1
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false

        // 2. Type Conversions for Limits
        let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
        let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

        // 3. Unpack Cloud Options via external static helper
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 4. Native Call
        PolarsWrapper.SinkParquetPartitioned(
            this.CloneHandle(), 
            path,
            partitionBy.CloneHandle(), // Pass native handle of Selector
            incKeys,
            preGrouped,
            maxRowsNuint,
            approxBytesUlong,
            comp.ToNative(),
            compLevel,
            stats,
            rgs,
            dps,
            compat,
            mo,
            sync.ToNative(),
            mkd,
            // Cloud Params
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sink the LazyFrame to a Parquet format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    /// <returns>A byte array containing the serialized Parquet data.</returns>
    member this.SinkParquetMemory(
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?compatLevel: int,
        ?maintainOrder: bool
    ) : byte[] =
        // Note: For Memory Sink, ZSTD and Level 3 are the typical defaults used in the C# wrapper
        let comp = defaultArg compression ParquetCompression.Zstd
        let compLevel = defaultArg compressionLevel 3
        let stats = defaultArg statistics true
        let rgs = defaultArg rowGroupSize 0
        let dps = defaultArg dataPageSize 0
        let compat = defaultArg compatLevel -1
        let mo = defaultArg maintainOrder true

        PolarsWrapper.SinkParquetMemory(
            this.CloneHandle(),
            comp.ToNative(),
            compLevel,
            stats,
            rgs,
            dps,
            compat,
            mo
        )
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd).</param>
    /// <param name="compressionLevel">Compression level.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    member this.SinkJson(
        path: string,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?checkExtension: bool,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let comp = defaultArg compression ExternalCompression.Uncompressed
        let compLevel = defaultArg compressionLevel -1
        let chkExt = defaultArg checkExtension true
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false

        // 2. Unpack Cloud Options via external static helper
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 3. Native Call
        PolarsWrapper.SinkJson(
            this.CloneHandle(), 
            path,
            comp.ToNative(),
            compLevel,
            chkExt,
            mo,
            sync.ToNative(),
            mkd,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file, partitioned by the given selector.
    /// </summary>
    member this.SinkJsonPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?checkExtension: bool,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        let incKeys = defaultArg includeKeys true
        let preGrouped = defaultArg keysPreGrouped false
        let maxRows = defaultArg maxRowsPerFile 0
        let approxBytes = defaultArg approxBytesPerFile 0L
        let comp = defaultArg compression ExternalCompression.Uncompressed
        let compLevel = defaultArg compressionLevel -1
        let chkExt = defaultArg checkExtension true
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false

        let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
        let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.SinkJsonPartitioned(
            this.CloneHandle(), 
            path,
            partitionBy.CloneHandle(),
            incKeys,
            preGrouped,
            maxRowsNuint,
            approxBytesUlong,
            comp.ToNative(),
            compLevel,
            chkExt,
            mo,
            sync.ToNative(),
            mkd,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Alias for SinkJson (Lazily evaluated JSON is always NDJSON/JsonLines).
    /// </summary>
    member this.SinkNdJson(
        path: string,
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?checkExtension: bool,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        this.SinkJson(
            path, 
            ?compression = compression, 
            ?compressionLevel = compressionLevel,
            ?checkExtension = checkExtension,
            ?maintainOrder = maintainOrder, 
            ?syncOnClose = syncOnClose, 
            ?mkdir = mkdir,
            ?cloudOptions = cloudOptions
        )

    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in memory.
    /// </summary>
    /// <returns>A byte array containing the serialized NDJSON data.</returns>
    member this.SinkJsonMemory(
        ?compression: ExternalCompression,
        ?compressionLevel: int,
        ?checkExtension: bool,
        ?maintainOrder: bool
    ) : byte[] =
        let comp = defaultArg compression ExternalCompression.Uncompressed
        let compLevel = defaultArg compressionLevel -1
        let chkExt = defaultArg checkExtension true
        let mo = defaultArg maintainOrder true

        PolarsWrapper.SinkJsonMemory(
            this.CloneHandle(),
            comp.ToNative(),
            compLevel,
            chkExt,
            mo
        )
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) file.
    /// <para>
    /// This allows for streaming execution.
    /// </para>
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (NoCompression, LZ4, ZSTD). Defaults to NoCompression.</param>
    /// <param name="compatLevel">Arrow compatibility level. -1 means newest. Defaults to -1.</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header. Defaults to true.</param>
    /// <param name="maintainOrder">Whether to maintain the order of the data. Defaults to true.</param>
    /// <param name="syncOnClose">File synchronization behavior on close. Defaults to None.</param>
    /// <param name="mkdir">Recursively create the directory if it does not exist. Defaults to false.</param>
    /// <param name="cloudOptions">Options for cloud storage.</param>
    member this.SinkIpc(
        path: string, 
        ?compression: IpcCompression, 
        ?compatLevel: int,
        ?recordBatchSize: int,
        ?recordBatchStatistics: bool,
        ?maintainOrder: bool, 
        ?syncOnClose: SyncOnClose, 
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let comp = defaultArg compression IpcCompression.NoCompression
        let compat = defaultArg compatLevel -1
        let batchSize = defaultArg recordBatchSize 0
        let batchStats = defaultArg recordBatchStatistics true
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false

        // 2. Unpack Cloud Options via Helper
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 3. Native Call
        PolarsWrapper.SinkIpc(
            this.CloneHandle(), 
            path,
            comp.ToNative(),
            compat,
            batchSize,
            batchStats,
            mo,
            sync.ToNative(),
            mkd,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) file, partitioned by the given selector.
    /// </summary>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    member this.SinkIpcPartitioned(
        path: string,
        partitionBy: Selector,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: IpcCompression,
        ?compatLevel: int,
        ?recordBatchSize: int,
        ?recordBatchStatistics: bool,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let incKeys = defaultArg includeKeys true
        let preGrouped = defaultArg keysPreGrouped false
        let maxRows = defaultArg maxRowsPerFile 0
        let approxBytes = defaultArg approxBytesPerFile 0L
        let comp = defaultArg compression IpcCompression.NoCompression
        let compat = defaultArg compatLevel -1
        let batchSize = defaultArg recordBatchSize 0
        let batchStats = defaultArg recordBatchStatistics true
        let mo = defaultArg maintainOrder true
        let sync = defaultArg syncOnClose SyncOnClose.NoSync
        let mkd = defaultArg mkdir false

        // 2. Type Conversions for Limits
        let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
        let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

        // 3. Unpack Cloud Options via external static helper
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 4. Native Call
        PolarsWrapper.SinkIpcPartitioned(
            this.CloneHandle(), 
            path,
            partitionBy.CloneHandle(), // Pass native handle of Selector
            incKeys,
            preGrouped,
            maxRowsNuint,
            approxBytesUlong,
            comp.ToNative(),
            compat,
            batchSize,
            batchStats,
            mo,
            sync.ToNative(),
            mkd,
            // Cloud
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    member this.SinkIpcMemory(
        ?compression: IpcCompression,
        ?compatLevel: int,
        ?recordBatchSize: int,
        ?recordBatchStatistics: bool,
        ?maintainOrder: bool
    ) : byte[] =
    
        let comp = defaultArg compression IpcCompression.NoCompression
        let compat = defaultArg compatLevel -1
        let batchSize = defaultArg recordBatchSize 0
        let batchStats = defaultArg recordBatchStatistics true
        let mo = defaultArg maintainOrder true

        PolarsWrapper.SinkIpcMemory(
            this.CloneHandle(),
            comp.ToNative(),
            compat,
            batchSize,
            batchStats,
            mo
        )
    /// <summary>
    /// Sink the LazyFrame to a Delta Lake table with partition discovery.
    /// <para>
    /// This operation performs a "blind write" of partitioned Parquet files (Hive-style) 
    /// and then commits a transaction to the Delta Log, registering the new files.
    /// </para>
    /// </summary>
    /// <param name="path">
    /// Path to the root of the Delta Table. Can be local (e.g. "./data/table") 
    /// or remote (e.g. "s3://bucket/table").
    /// </param>
    /// <param name="partitionBy">
    /// The selector(s) to partition the data by. 
    /// Directories will be created in the format "col=value".
    /// </param>
    /// <param name="mode">
    /// Save mode (Append, Overwrite, ErrorIfExists, Ignore). Default is Append.
    /// </param>
    /// <param name="includeKeys">
    /// Whether to include the partition keys in the Parquet files themselves. 
    /// Default is true (recommended for Delta Lake compatibility).
    /// </param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <param name="compression">Compression codec to use (Snappy, Zstd, etc.).</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">
    /// Write statistics to the Parquet file. 
    /// Delta Lake uses these stats for data skipping, so 'true' is highly recommended.
    /// </param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
    member this.SinkDelta(
        path: string,
        ?partitionBy: Selector,
        ?mode: DeltaSaveMode,
        ?canEvolve: bool,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: uint32,
        ?dataPageSize: uint32,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults
        let pMode = defaultArg mode DeltaSaveMode.Append
        let pEvolve = defaultArg canEvolve false
        let pIncKeys = defaultArg includeKeys true
        let pPreGrouped = defaultArg keysPreGrouped false
        let pMaxRows = defaultArg maxRowsPerFile 0
        let pApproxBytes = defaultArg approxBytesPerFile 0L
        
        let pComp = defaultArg compression ParquetCompression.Snappy
        let pCompLevel = defaultArg compressionLevel -1
        let pStats = defaultArg statistics true
        let pRowGrpSz = defaultArg rowGroupSize 0u
        let pDataPgSz = defaultArg dataPageSize 0u
        let pCompat = defaultArg compatLevel -1
        
        let pMaintain = defaultArg maintainOrder true
        let pSync = defaultArg syncOnClose SyncOnClose.NoSync
        let pMkdir = defaultArg mkdir false

        // 2. Type Conversions for Limits (handling zero-checks safely)
        let maxRowsNuint = if pMaxRows > 0 then unativeint pMaxRows else 0un
        let approxBytesUlong = if pApproxBytes > 0L then uint64 pApproxBytes else 0UL
        let rowGrpSzNuint = unativeint pRowGrpSz
        let dataPgSzNuint = unativeint pDataPgSz

        // 3. Unpack Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 4. Safe Handle Binding for Optional Selector
        use partitionByH = 
            match partitionBy with
            | Some s -> s.CloneHandle()
            | None -> null

        // 5. Native Call
        PolarsWrapper.SinkDelta(
            this.CloneHandle(),
            path,
            
            // --- Delta Options ---
            pMode.ToNative(),
            pEvolve,
            
            // --- Partition Params ---
            partitionByH,
            pIncKeys,
            pPreGrouped,
            maxRowsNuint,
            approxBytesUlong,
            
            // --- Parquet Options ---
            pComp.ToNative(),
            pCompLevel,
            pStats,
            rowGrpSzNuint,  
            dataPgSzNuint,
            pCompat,
            
            // --- Unified Options ---
            pMaintain,
            pSync.ToNative(),
            pMkdir,
            
            // --- Cloud Params ---
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )
    /// <summary>
    /// Merge a LazyFrame into a Delta Lake table with full SQL MERGE semantics.
    /// Provides fine-grained control over Update, Insert, and Delete behaviors.
    /// Notice: In this method, Delete > Update > Insert > Ignore.
    /// If you need other orders, please use LazyFrame.MergeDeltaOrdered
    /// </summary>
    /// <param name="path">Uri to the Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The column names to join on (must exist in both Source and Target).</param>
    /// <param name="matchedUpdateCond">
    /// Condition for 'WHEN MATCHED THEN UPDATE'. 
    /// If null, defaults to true (always update when matched).
    /// </param>
    /// <param name="matchedDeleteCond">
    /// Condition for 'WHEN MATCHED THEN DELETE'. 
    /// If null, defaults to false (never delete when matched).
    /// </param>
    /// <param name="notMatchedInsertCond">
    /// Condition for 'WHEN NOT MATCHED THEN INSERT'. 
    /// If null, defaults to true (always insert new rows).
    /// </param>
    /// <param name="notMatchedBySourceDeleteCond">
    /// Condition for 'WHEN NOT MATCHED BY SOURCE THEN DELETE' (Target rows not in Source). 
    /// If null, defaults to false (retain target-only rows).
    /// </param>
    /// <param name="canEvolve">Allow schema evolution during the merge.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    [<Obsolete("This method is deprecated because its execution order of matched/not-matched actions is hardcoded and may lead to silent data corruption in complex scenarios. Please use 'MergeDeltaOrdered(...)' combined with the '.WhenMatched...()' chaining methods to ensure strict SQL MERGE semantics.")>]
    member this.MergeDelta(
        path: string,
        mergeKeys: seq<string>,
        ?matchedUpdateCond: Expr,
        ?matchedDeleteCond: Expr,
        ?notMatchedInsertCond: Expr,
        ?notMatchedBySourceDeleteCond: Expr,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Resolve Defaults & Sequences
        let pEvolve = defaultArg canEvolve false
        let keysArr = mergeKeys |> Seq.toArray

        // 2. Parse Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 3. Clone Handles (safely disposing them at the end of the scope)
        use clonedLf = this.CloneHandle()
        
        use hUpdate = 
            match matchedUpdateCond with
            | Some e -> e.CloneHandle()
            | None -> null
            
        use hDelete = 
            match matchedDeleteCond with
            | Some e -> e.CloneHandle()
            | None -> null
            
        use hInsert = 
            match notMatchedInsertCond with
            | Some e -> e.CloneHandle()
            | None -> null
            
        use hSrcDelete = 
            match notMatchedBySourceDeleteCond with
            | Some e -> e.CloneHandle()
            | None -> null

        // 4. Native Call
        PolarsWrapper.DeltaMerge(
            clonedLf,
            path,
            keysArr,
            hUpdate,
            hDelete,
            hInsert,
            hSrcDelete,
            pEvolve,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )
    // ==========================================
    // Streaming Sink (Lazy)
    // ==========================================
    /// <summary>
    /// Stream the query result in batches.
    /// This executes the query and calls 'onBatch' for each RecordBatch produced.
    /// </summary>
    member this.SinkBatches(onBatch: Action<RecordBatch>) : unit =
        let newHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), onBatch)
        
        let lfRes = new LazyFrame(newHandle)
        use _ = lfRes.Collect()
        () 
    /// <summary>
    /// Stream query results directly to a database or other IDataReader consumer.
    /// Uses a producer-consumer pattern with bounded capacity for memory efficiency.
    /// </summary>
    /// <param name="writerAction">Callback to consume the IDataReader (e.g., using SqlBulkCopy).</param>
    /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
    /// <param name="typeOverrides">Force specific C# types for columns (e.g. map Date32 to DateTime).</param>
    member this.SinkTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
        let capacity = defaultArg bufferSize 5
        
        use buffer = new BlockingCollection<RecordBatch>(boundedCapacity = capacity)

        let consumerTask = Task.Run(fun () ->
            let stream = buffer.GetConsumingEnumerable()
            
            let overrides = 
                    match typeOverrides with 
                    | Some d -> new Dictionary<string, Type>(d) 
                    | None -> null
            
            use reader = new ArrowToDbStream(stream, overrides)
            
            writerAction.Invoke reader
        )

        try
            try
                this.SinkBatches(fun batch -> buffer.Add batch)
            finally
                buffer.CompleteAdding()
        with
        | _ -> 
            reraise()

        try
            consumerTask.Wait()
        with
        | :? AggregateException as aggEx ->
            raise (aggEx.Flatten().InnerException)
    
    /// <summary>
    /// Join with another LazyFrame.
    /// </summary>
    member this.Join(other: LazyFrame, 
                     leftOn: Expr seq, 
                     rightOn: Expr seq, 
                     how: JoinType,
                     // --- New Optional Parameters ---
                     ?suffix: string,
                     ?validation: JoinValidation,
                     ?coalesce: JoinCoalesce,
                     ?maintainOrder: JoinMaintainOrder,
                     ?joinSide: JoinSide,
                     ?nullsEqual: bool,
                     ?sliceOffset: int64,
                     ?sliceLen: uint64) : LazyFrame =

        let lOnArr = leftOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        let rOnArr = rightOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        let lHandle = this.CloneHandle()
        let rHandle = other.CloneHandle()

        // Handle Defaults
        let suff = defaultArg suffix null
        let valid = defaultArg validation JoinValidation.ManyToMany
        let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
        let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
        let ne = defaultArg nullsEqual false
        let js = defaultArg joinSide JoinSide.LetPolarsDecide
        
        let so = Option.toNullable sliceOffset
        let sl = defaultArg sliceLen 0UL

        let newHandle = PolarsWrapper.Join(
            lHandle, 
            rHandle, 
            lOnArr, 
            rOnArr, 
            how.ToNative(),
            suff,
            valid.ToNative(),
            coal.ToNative(),
            mo.ToNative(),
            js.ToNative(),
            ne,
            so,
            sl
        )
        
        new LazyFrame(newHandle)
    
    member this.Filter (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)
    
    member this.Select (exprs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)
    member this.Select(columns: seq<IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.Select exprs
    /// <summary>
    /// Sort with Parameters
    /// </summary>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        descending: seq<bool>,
        nullsLast: seq<bool>,
        ?maintainOrder: bool
    ) =
        let exprHandles = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = descending |> Seq.toArray
        let nullsArr = nullsLast |> Seq.toArray
        let stable = defaultArg maintainOrder false

        let lfHandle = this.CloneHandle()

        let h = PolarsWrapper.LazyFrameSort(lfHandle, exprHandles, descArr, nullsArr, stable)
        new LazyFrame(h)

    /// <summary>
    /// Sort with simple options
    /// </summary>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        ?descending: bool,
        ?nullsLast: bool,
        ?maintainOrder: bool
    ) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        
        this.Sort(columns, [| desc |], [| nLast |], ?maintainOrder = maintainOrder)
    member this.Sort(expr: Expr, ?descending: bool, ?nullsLast: bool) =
        this.Sort([expr], ?descending=descending, ?nullsLast=nullsLast)

    member this.Sort(colName: string, ?descending: bool, ?nullsLast: bool) =
        this.Sort([Expr.Col colName], ?descending=descending, ?nullsLast=nullsLast)
    // Alias
    member this.OrderBy(columns: seq<#IColumnExpr>, ?descending: bool, ?nullsLast: bool) = 
        this.Sort(columns, ?descending=descending, ?nullsLast=nullsLast)
    // ==========================================
    // TopK / BottomK
    // ==========================================

    /// <summary>
    /// Get the top k rows based on the given columns.
    /// This is often faster than a full sort followed by a head.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">Columns to sort by.</param>
    /// <param name="reverse">Sort direction per column. Default is false (no reverse).</param>
    member this.TopK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let exprHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = 
            match reverse with
            | Some d -> d |> Seq.toArray
            | None -> [| false |] 

        let lfHandle = this.CloneHandle()

        let h = PolarsWrapper.LazyFrameTopK(lfHandle, uint k, exprHandles, descArr)
        new LazyFrame(h)

    /// <summary>
    /// Get the bottom k rows based on the given columns.
    /// </summary>
    member this.BottomK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let exprHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = 
            match reverse with
            | Some d -> d |> Seq.toArray
            | None -> [| false |]

        let lfHandle = this.CloneHandle()
        let h = PolarsWrapper.LazyFrameBottomK(lfHandle, uint k, exprHandles, descArr)
        new LazyFrame(h)

    // [Overload] Sugar for single boolean reversing
    member this.TopK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
        this.TopK(k, by, [| reverse |])
    
    member this.BottomK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
        this.BottomK(k, by, [| reverse |])


    // ==========================================
    // Unnest
    // ==========================================

    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// </summary>
    member this.Unnest(selector: Selector,?separator: string) =
        let lfHandle = this.CloneHandle()
        
        let selHandle = selector.CloneHandle()

        let sep = defaultArg separator null

        let h = PolarsWrapper.LazyFrameUnnest(lfHandle, selHandle, sep)
        new LazyFrame(h)

    /// <summary>
    /// Helper: Unnest columns by name.
    /// </summary>
    member this.Unnest(columns: string list,?separator: string) =
        let columnsArray = columns|> List.toArray
        let handle = PolarsWrapper.SelectorCols columnsArray
        let sel = new Selector(handle)
        this.Unnest (sel,?separator=separator)

    /// <summary>
    /// Helper: Unnest a single column by name.
    /// </summary>
    member this.Unnest(column: string, ?separator: string) =
        this.Unnest ([column], ?separator=separator)
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// This is an optimization hint that pushes down the limit to the scan if possible.
    /// </summary>
    /// <param name="n">Maximum number of rows to return.</param>
    member this.Limit (n: uint) : LazyFrame =
        let lfClone = this.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace a single column in the LazyFrame.
    /// </summary>
    /// <param name="expr">The expression defining the new column.</param>
    member this.WithColumn (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] 
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace multiple columns in the LazyFrame.
    /// </summary>
    /// <param name="exprs">List of expressions defining the new columns.</param>
    member this.WithColumns (exprs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace columns using generic column expressions (Expr or Selectors).
    /// </summary>
    member this.WithColumns (columns:seq<#IColumnExpr>) =
        let exprs = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        
        this.WithColumns exprs
    /// <summary>
    /// Group by keys and apply aggregate expressions.
    /// </summary>
    /// <param name="keys">Grouping keys.</param>
    /// <param name="aggs">Aggregation expressions to apply per group.</param>
    member this.GroupBy (keys: Expr list,aggs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyGroupByAgg(lfClone, kHandles, aHandles)
        new LazyFrame(h)
    /// <summary>
    /// Group by keys and apply aggregations (Supports Selectors).
    /// </summary>
    member this.GroupBy(keys: seq<#IColumnExpr>, aggs: seq<#IColumnExpr>) =
            let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
            let aExprs = aggs |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
            this.GroupBy(kExprs, aExprs)
    /// <summary>
    /// Pivot the LazyFrame.
    /// <para>
    /// <b>Important:</b> Lazy pivot requires an eager <paramref name="onColumns"/> DataFrame 
    /// to determine the output schema (column names) during the planning phase.
    /// </para>
    /// </summary>
    /// <param name="index">Selector for the index column(s) (the rows).</param>
    /// <param name="columns">Selector for the column(s) to pivot (the new column headers).</param>
    /// <param name="values">Selector for the value column(s) to populate the cells.</param>
    /// <param name="onColumns">
    /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
    /// <br/>This is strictly used for schema inference.
    /// </param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    /// <returns>A new LazyFrame with the pivot operation applied.</returns>
    member this.Pivot(
        index: Selector,
        columns: Selector,
        values: Selector,
        onColumns: DataFrame,
        ?aggregateExpr: Expr,
        ?aggregateFunction: PivotAgg,
        ?maintainOrder: bool,
        ?separator: string
    ) =
        let aggFunc = defaultArg aggregateFunction PivotAgg.First
        let mo = defaultArg maintainOrder true
        let sep = Option.toObj separator

        use indexH = index.CloneHandle()
        use columnsH = columns.CloneHandle()
        use valuesH = values.CloneHandle()
        use aggExprH = 
            match aggregateExpr with
            | Some e -> e.CloneHandle()
            | None -> null

        let h = PolarsWrapper.LazyPivot(
            this.CloneHandle(),
            columnsH,           // on (columns selector)
            onColumns.Handle,   // onColumns (Eager DF Handle - passed directly, not cloned/disposed here)
            indexH,             // index
            valuesH,            // values
            aggExprH,           // aggExpr
            aggFunc.ToNative(), // aggregateFunction mapping
            mo,                 // maintainOrder
            sep                 // separator
        )

        new LazyFrame(h)
    /// <summary>
    /// Pivot the LazyFrame using column names.
    /// </summary>
    /// <param name="index">Column names to use as the index.</param>
    /// <param name="columns">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values.</param>
    /// <param name="onColumns">
    /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
    /// </param>
    /// <param name="aggregateFunction">Aggregation function. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator for generated column names.</param>
    member this.Pivot(
        index: seq<string>,
        columns: seq<string>,
        values: seq<string>,
        onColumns: DataFrame,
        ?aggregateFunction: PivotAgg,
        ?maintainOrder: bool,
        ?separator: string
    ) =
        use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
        use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
        use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

        this.Pivot(
            sIndex,
            sColumns,
            sValues,
            onColumns,
            ?aggregateFunction = aggregateFunction,
            ?maintainOrder = maintainOrder,
            ?separator = separator
        )

    /// <summary>
    /// Pivot the LazyFrame using column names and a custom aggregation expression.
    /// </summary>
    member this.Pivot(
        index: seq<string>,
        columns: seq<string>,
        values: seq<string>,
        onColumns: DataFrame,
        aggregateExpr: Expr,
        ?maintainOrder: bool,
        ?separator: string
    ) =
        use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
        use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
        use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

        this.Pivot(
            sIndex,
            sColumns,
            sValues,
            onColumns,
            aggregateExpr = aggregateExpr,
            // aggregateFunction is ignored when Expr is provided, but we pass default to match
            ?maintainOrder = maintainOrder,
            ?separator = separator
        )
    /// <summary>
    /// Unpivot (Melt) the LazyFrame using Selectors.
    /// Primary overload backed by native binding.
    /// </summary>
    member this.Unpivot(index: Selector, on: Selector, variableName: string option, valueName: string option) : LazyFrame =
        let lfClone = this.CloneHandle()
        
        let hIndex = index.CloneHandle()
        let hOn = on.CloneHandle()
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName
        
        new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, hIndex, hOn, varN, valN))

    /// <summary>
    /// Unpivot (Melt) overload for simple string lists.
    /// Auto-converts to Selectors.
    /// </summary>
    member this.Unpivot(index: seq<string>, on: seq<string>, variableName: string option, valueName: string option) =
        // 1. Convert Index strings to Selector
        let idxArr = Seq.toArray index
        let sIndex = new Selector(PolarsWrapper.SelectorCols idxArr)

        // 2. Convert On strings to Selector
        let onArr = Seq.toArray on
        let sOn = new Selector(PolarsWrapper.SelectorCols onArr)

        // 3. Route to main logic
        this.Unpivot(sIndex, sOn, variableName, valueName)

    member this.Unpivot(index: string list, on: string list) =
        this.Unpivot(index, on, None, None)

    // ==========================================
    // Aliases (Melt)
    // ==========================================
    
    member this.Melt(index: Selector, on: Selector, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: seq<string>, on: seq<string>, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: string list, on: string list) =
        this.Unpivot(index, on)
    member this.Explode(selector: Selector,?emptyAsNull:bool,?keepNulls:bool) : LazyFrame =
        let lfClone = this.CloneHandle()
        let sh = selector.CloneHandle()
        let ean = defaultArg emptyAsNull true
        let kn = defaultArg keepNulls true
        new LazyFrame(PolarsWrapper.LazyExplode(lfClone, sh, ean, kn))

    member this.Explode(columns: seq<string>) =
        let names = Seq.toArray columns
        let h = PolarsWrapper.SelectorCols names
        let sel = new Selector(h)
        this.Explode sel

    member this.Explode(column: string) = 
        this.Explode [column]

    /// <summary>
    /// JoinAsOf with string tolerance (e.g., "2d", "1h").
    /// </summary>
    member internal this.JoinAsOfInternal(other: LazyFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         // --- Optional Parameters ---
                         ?byLeft: Expr list, 
                         ?byRight: Expr list, 
                         ?strategy: AsofStrategy, 
                         ?tolerance: string,      // String (e.g. "2h")
                         ?toleranceInt: int64,    // Int (e.g. timestamp)
                         ?toleranceFloat: float,  // Float
                         ?allowEq: bool,
                         ?checkSorted: bool,
                         ?suffix: string,
                         ?validation: JoinValidation,
                         ?coalesce: JoinCoalesce,
                         ?maintainOrder: JoinMaintainOrder,
                         ?joinSide: JoinSide,
                         ?nullsEqual: bool,
                         ?sliceOffset: int64,
                         ?sliceLen: uint64) : LazyFrame =
        
        // 1. Clone Handles (Mandatory)
        let lClone = this.CloneHandle()
        let rClone = other.CloneHandle()
        let lOn = leftOn.CloneHandle()
        let rOn = rightOn.CloneHandle()
        
        // 2. Handle 'By' keys (Optional List -> Handle Array)
        let toHandleArr (exprs: Expr list option) =
            match exprs with
            | Some es -> es |> List.map (fun e -> e.CloneHandle()) |> List.toArray
            | None -> [||]

        let lByArr = toHandleArr byLeft
        let rByArr = toHandleArr byRight

        // 3. Handle Enums & Defaults
        let strat = defaultArg strategy AsofStrategy.Backward
        let valid = defaultArg validation JoinValidation.ManyToMany
        let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
        let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
        let js = defaultArg joinSide JoinSide.LetPolarsDecide
        
        // 4. Handle Bools & Strings
        let ae = defaultArg allowEq true
        let cs = defaultArg checkSorted true
        let ne = defaultArg nullsEqual false
        let suff = defaultArg suffix null // Rust default is "_right"
        
        // 5. Handle Nullables (Tolerances & Slice)
        // Option.toObj converts string option -> string (null if None)
        let tolStr = Option.toObj tolerance 
        // Option.toNullable converts int option -> Nullable<int>
        let tolInt = Option.toNullable toleranceInt
        let tolFloat = Option.toNullable toleranceFloat
        let sOff = Option.toNullable sliceOffset
        let sLen = defaultArg sliceLen 0UL

        // 6. Call Wrapper
        let h = PolarsWrapper.JoinAsOf(
            lClone, rClone, 
            [| lOn |], [| rOn |], // Wrapper expects arrays
            lByArr, rByArr,
            strat.ToNative(),     // Enum -> PlAsofStrategy
            tolStr,
            tolInt,
            tolFloat,
            ae,
            cs,
            suff,
            valid.ToNative(),
            coal.ToNative(),
            mo.ToNative(),
            js.ToNative(),
            ne,
            sOff,
            sLen
        )
        
        new LazyFrame(h)

    /// <summary>
    /// Join with tolerance as string (e.g. "2h", "10s").
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: string, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolerance, // String
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    /// <summary>
    /// Join with tolerance as TimeSpan.
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: System.TimeSpan, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        let tolStr = DurationFormatter.ToPolarsString(tolerance)
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolStr, // Converted String
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    /// <summary>
    /// Join with tolerance as integer (e.g. timestamp or simple counter).
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: int64, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceInt = tolerance, // Int64
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    /// <summary>
    /// Join with tolerance as float.
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: float, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceFloat = tolerance, // Float
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )
    /// <summary>
    /// Slice the LazyFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint32) = 
        new LazyFrame(PolarsWrapper.LazySlice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)
    /// <summary>
    /// Rename the lazyframe columns
    /// Example: lf.Rename ["colA", "col1"; "colB", "col2"]
    /// </summary>
    /// <param name="strict">
    /// If <c>true</c>, an error is raised if any column in <paramref name="existing"/> is not found in the schema. 
    /// If <c>false</c>, columns that are not found are silently ignored. Default is <c>true</c>.
    /// </param>
    member this.Rename(mapping: (string * string) list, ?strict: bool) =
        let oldNames, newNames = List.unzip mapping
        let isStrict = defaultArg strict true
        let oldNames, newNames = List.unzip mapping
        let pOldNames = List.toArray oldNames
        let pNewNames = List.toArray newNames
        let pStrict = defaultArg strict true

        let handle = PolarsWrapper.LazyRename(
            this.Handle, 
            pOldNames, 
            pNewNames, 
            pStrict
        )
        
        new LazyFrame(handle)
    static member Concat  (lfs: LazyFrame list) (how: ConcatType) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), false, true))
    /// <summary>
    /// Perform a dynamic group-by (rolling window) and aggregation in one step.
    /// </summary>
    /// <param name="indexCol">Name of the time index column.</param>
    /// <param name="every">Period of the window (step size).</param>
    /// <param name="aggs">List of aggregation expressions to apply.</param>
    /// <param name="period">Size of the window. Defaults to 'every'.</param>
    /// <param name="offset">Offset of the window. Defaults to 0.</param>
    /// <param name="by">Additional grouping keys (e.g. ID, Category).</param>
    /// <param name="label">Label of the window (Left, Right, DataPoint).</param>
    /// <param name="includeBoundaries">Whether to include the window boundaries.</param>
    /// <param name="closedWindow">Which side of the window is closed.</param>
    /// <param name="startBy">Strategy to determine the start of the first window.</param>
    member this.GroupByDynamic(
        indexCol: string,
        every: TimeSpan,
        aggs: seq<#IColumnExpr>, 
        ?period: TimeSpan,
        ?offset: TimeSpan,
        ?by: seq<#IColumnExpr>, 
        ?label: Label,
        ?includeBoundaries: bool,
        ?closedWindow: ClosedWindow,
        ?startBy: StartBy
    ) : LazyFrame =
        let periodVal = defaultArg period every
        let offsetVal = defaultArg offset TimeSpan.Zero
        let labelVal = defaultArg label Label.Left
        let includeBoundariesVal = defaultArg includeBoundaries false
        let closedWindowVal = defaultArg closedWindow ClosedWindow.Left
        let startByVal = defaultArg startBy StartBy.WindowBound

        let everyStr = DurationFormatter.ToPolarsString every
        let periodStr = DurationFormatter.ToPolarsString periodVal
        let offsetStr = DurationFormatter.ToPolarsString offsetVal

        let keyExprs = 
            match by with
            | Some cols -> cols |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
            | None -> []
        let keyHandles = keyExprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        let aggExprs = 
            aggs 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        let aggHandles = aggExprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        let lfHandle = this.CloneHandle()

        let newH = PolarsWrapper.LazyGroupByDynamic(
            lfHandle,
            indexCol,
            everyStr,
            periodStr,
            offsetStr,
            labelVal.ToNative(),
            includeBoundariesVal,
            closedWindowVal.ToNative(),
            startByVal.ToNative(),
            keyHandles,
            aggHandles
        )

        new LazyFrame(newH)

/// <summary>
/// Polars Schema definition (Name -> DataType).
/// </summary>
and PolarsSchema (handle: SchemaHandle) =
    
    // --- Property ---
  member val Handle = handle

    // --- Constructors ---
    static member private CreateHandleFromFields(fields: seq<string * DataType>) =
            let names = fields |> Seq.map fst |> Seq.toArray
            let typeHandles = fields |> Seq.map (fun (_, t) -> t.CreateHandle()) |> Seq.toArray
            
            try
                PolarsWrapper.NewSchema(names, typeHandles)
            finally
                for th in typeHandles do th.Dispose()
    /// <summary> Create an empty schema </summary>
    new () = new PolarsSchema(PolarsWrapper.SchemaCreate())

    /// <summary> Create schema from field definitions </summary>
    new (fields: seq<string * DataType>) =
        new PolarsSchema(PolarsSchema.CreateHandleFromFields fields)
    /// <summary>
    /// Create a Schema directly from a .NET type (e.g., a record or class).
    /// </summary>
    /// <typeparam name="T">The record or class type.</typeparam>
    /// <returns>A PolarsSchema mapped from the type's properties.</returns>
    static member FromRecord<'T>() =
        let handle = PolarsWrapper.NewSchemaFromType(typeof<'T>)
        new PolarsSchema(handle)

    static member ofMap (m: Map<string, DataType>) = new PolarsSchema(m |> Map.toSeq)
    static member ofList (fields: (string * DataType) list) = new PolarsSchema(fields)

    // --- Inspection API (Alignment with C#) ---

    member this.Len() = PolarsWrapper.GetSchemaLen this.Handle

    /// <summary> Get column name and type at specific index </summary>
    member private this.GetFieldAt(index: uint64) =
        let mutable name = Unchecked.defaultof<string>
        let mutable typeHandle = Unchecked.defaultof<DataTypeHandle>
        
        PolarsWrapper.GetSchemaFieldAt(this.Handle, index, &name, &typeHandle)
        
        try
            let dt = DataType.FromHandle typeHandle
            name, dt
        finally
            if not typeHandle.IsInvalid then 
                typeHandle.Dispose()

    /// <summary> Get all column names </summary>
    member this.Names =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) -> 
            let mutable name = Unchecked.defaultof<string>
            let mutable _th = Unchecked.defaultof<DataTypeHandle>
            PolarsWrapper.GetSchemaFieldAt(this.Handle, i, &name, &_th)
            if not _th.IsInvalid then _th.Dispose() 
            name 
        ]

    /// <summary> Convert to F# Map </summary>
    member this.ToMap() =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) do
            yield this.GetFieldAt(i) 
        ] |> Map.ofList

    /// <summary> Convert to Dictionary </summary>
    member this.ToDictionary() =
        let len = this.Len()
        let dict = Dictionary<string, DataType>(int len)
        for i in 0UL .. (len - 1UL) do
            let name, dtype = this.GetFieldAt(i)
            dict.[name] <- dtype
        dict

    /// <summary> Indexer: schema["col_name"] </summary>
    member this.Item 
        with get(name: string) =
            let len = this.Len()
            let rec find i =
                if i >= len then raise (KeyNotFoundException $"Column '{name}' not found in Schema.")
                else
                    let colName, dtype = this.GetFieldAt(i)
                    if colName = name then dtype
                    else 
                        find (i + 1UL)
            find 0UL

    // --- Display ---
    
    override this.ToString() =
        if this.Handle.IsInvalid then "Schema: {}"
        else
            let sb = StringBuilder "Schema: {"
            let len = this.Len()
            for i in 0UL .. (len - 1UL) do
                let name, dtype = this.GetFieldAt(i)
                sb.Append $"{name}: {dtype}" |> ignore
                if i < len - 1UL then sb.Append(", ") |> ignore
            sb.Append "}" |> ignore
            sb.ToString()

    // --- Interface ---
    
    interface IDisposable with
        member this.Dispose() = 
            if not (isNull (box this.Handle)) && not this.Handle.IsInvalid then
                this.Handle.Dispose()

    interface IPolarsSchema with
        member this.Length = 
            int (this.Len())
            
        member this.ColumnNames = 
            List<string> this.Names
            
        member this.Item
            with get (name: string) = 
                this.[name] :> IPolarsDataType
        member this.ToDictionary() =
            let dict = Dictionary<string, IPolarsDataType>()
            for kvp in this.ToDictionary() do
                dict.[kvp.Key] <- kvp.Value :> IPolarsDataType
            dict

/// <summary>
/// SQL Context for executing SQL queries on registered LazyFrames.
/// </summary>
type SqlContext() as this =
    let handle = PolarsWrapper.SqlContextNew()
    
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    
    interface IPolarsSqlContext with
        member _.Register(tableName: string, df: IPolarsDataFrame) =
            this.Register(tableName, df :?> DataFrame)

        member _.Register(tableName: string, lf: IPolarsLazyFrame) =
            this.Register(tableName, lf :?> LazyFrame)

        member _.Execute(sql: string) =
            this.Execute sql :> IPolarsLazyFrame

    /// <summary> Register a LazyFrame as a table for SQL querying. </summary>
    member _.Register(name: string, lf: LazyFrame) =
        PolarsWrapper.SqlRegister(handle, name, lf.CloneHandle())

    /// <summary> Register a DataFrame as a table for SQL querying. </summary>
    member _.Register(name: string, df: DataFrame) =
        let lf = df.Lazy()
        PolarsWrapper.SqlRegister(handle, name, lf.Handle)

    member _.UnRegister(name: string) = 
        PolarsWrapper.SqlUnRegister(handle,name)

    /// <summary> Get the names of all registered tables, in sorted order. </summary>
    member _.GetTables() =
        PolarsWrapper.SqlGetTables(handle)

    /// <summary> Execute a SQL query and return a LazyFrame. </summary>
    member _.Execute(query: string) =
        new LazyFrame(PolarsWrapper.SqlExecute(handle, query))

