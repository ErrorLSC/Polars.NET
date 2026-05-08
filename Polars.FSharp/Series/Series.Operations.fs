namespace Polars.FSharp

[<AutoOpen>]
module SeriesOperationExtensions =
    open Polars.NET.Core
    open System
    open Apache.Arrow
    type Series with

        /// <summary> Slice the Series. Returns a new Series. </summary>
        /// <param name="offset">Start index.</param>
        /// <param name="length">Length of the slice.</param>
        member this.Slice(offset: int64, length: uint64) =
            new Series(PolarsWrapper.SeriesSlice(this.Handle, offset, length))
        /// <summary>
        /// Append a Series to this one.
        /// The resulting series will consist of multiple chunks.
        /// </summary>
        /// <param name="other">Series to append.</param>
        member this.Append(other:Series) = 
            PolarsWrapper.SeriesAppend(this.Handle,other.Handle)
        /// <summary>
        /// Extend the memory backed by this Series with the values from another.
        /// Different from append, which adds the chunks from other to the chunks of this series, extend appends the data from other to the underlying memory locations and thus may cause a reallocation (which is expensive).
        /// If this does not cause a reallocation, the resulting data structure will not have any extra chunks and thus will yield faster queries.
        /// </summary>
        /// <param name="other">Series to extend the series with.</param>
        member this.Extend(other:Series) = 
            PolarsWrapper.SeriesExtend(this.Handle,other.Handle)
            this

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
            let dfHandle = PolarsWrapper.SeriesStructUnnest this.Handle
            new DataFrame(dfHandle)
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
            new Series(PolarsWrapper.SeriesIsNull this.Handle)
        /// <summary>
        /// Returns a boolean Series indicating which values are not null.
        /// </summary>
        member this.IsNotNull() : Series = 
            new Series(PolarsWrapper.SeriesIsNotNull this.Handle)
        /// <summary>
        /// Drop null values.
        /// </summary>
        member this.DropNulls() : Series =
            new Series(PolarsWrapper.SeriesDropNulls this.Handle)
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
        member this.IsNullAt(index: int) : bool =
            PolarsWrapper.SeriesIsNullAt(this.Handle, int64 index)
        member this.IsNullAt(index: int64) : bool =
            PolarsWrapper.SeriesIsNullAt(this.Handle, index)
        /// <summary>
        /// Get the number of null values in the Series.
        /// This is an O(1) operation (metadata access).
        /// </summary>

        /// <summary> Check if floating point values are NaN. </summary>
        member this.IsNan() = new Series(PolarsWrapper.SeriesIsNan this.Handle)

        /// <summary> Check if floating point values are not NaN. </summary>
        member this.IsNotNan() = new Series(PolarsWrapper.SeriesIsNotNan this.Handle)

        /// <summary> Check if floating point values are finite (not NaN and not Inf). </summary>
        member this.IsFinite() = new Series(PolarsWrapper.SeriesIsFinite this.Handle)

        /// <summary> Check if floating point values are infinite. </summary>
        member this.IsInfinite() = new Series(PolarsWrapper.SeriesIsInfinite this.Handle)
        // ==========================================
        // Uniqueness & Boolean Masl
        // ==========================================

        /// <summary>
        /// Get unique values (distinct).
        /// </summary>
        member this.Unique() = new Series(PolarsWrapper.SeriesUnique this.Handle)

        /// <summary>
        /// Get unique values (distinct), maintaining original order.
        /// </summary>
        member this.UniqueStable() = new Series(PolarsWrapper.SeriesUniqueStable this.Handle)

        /// <summary>
        /// Count the number of unique values.
        /// </summary>
        member this.NUnique() = PolarsWrapper.SeriesNUnique this.Handle
        /// <summary>
        /// Get an approximation of the number of unique values in this Series.
        /// Uses HyperLogLog algorithm for fast, memory-efficient counting.
        /// </summary>
        /// <returns>Approximate count of unique values.</returns>
        member this.ApproxNUnique() = PolarsWrapper.SeriesApproxNUnique this.Handle
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
        member this.Round(decimals: uint,?mode: RoundMode) =
            let mo = defaultArg mode RoundMode.HalfToEven 
            this.ApplyExpr(Expr.Col(this.Name).Round(decimals,mo))

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


        // --- Scalar Access ---
        
        /// <summary> Get value as Int64 Option. Handles Int32/Int64 etc. </summary>
        member this.Int(index: int) : int64 option = 
            PolarsWrapper.SeriesGetInt(this.Handle, int64 index) |> Option.ofNullable

        member this.Int128(index: int) : Int128 option = 
            PolarsWrapper.SeriesGetInt128(this.Handle, int64 index) |> Option.ofNullable

        /// <summary> Get value as Double Option. Handles Float32/Float64. </summary>
        member this.Float(index: int) : float option = 
            PolarsWrapper.SeriesGetDouble(this.Handle, int64 index) |> Option.ofNullable

        /// <summary> Get value as String Option. </summary>
        member this.String(index: int) : string option = 
            PolarsWrapper.SeriesGetString(this.Handle, int64 index) |> Option.ofObj

        /// <summary> Get value as Boolean Option. </summary>
        member this.Bool(index: int) : bool option = 
            PolarsWrapper.SeriesGetBool(this.Handle, int64 index) |> Option.ofNullable

        /// <summary> Get value as Decimal Option. </summary>
        member this.Decimal(index: int) : decimal option = 
            PolarsWrapper.SeriesGetDecimal(this.Handle, int64 index) |> Option.ofNullable

        // Temporal Type
        member this.Date(index: int) : DateOnly option = 
            PolarsWrapper.SeriesGetDate(this.Handle, int64 index) |> Option.ofNullable

        member this.Time(index: int) : TimeOnly option = 
            PolarsWrapper.SeriesGetTime(this.Handle, int64 index) |> Option.ofNullable
        member this.DateTime(index: int) : DateTime option = 
            let result = PolarsWrapper.SeriesGetDatetime(this.Handle, int64 index)
            if result.HasValue then
                let struct (dt, _) = result.Value 
                Some dt
            else
                None
        /// <summary>
        /// Gets the Datetime value and its TimeZone string at the specified index.
        /// Returns None if the value is null.
        /// </summary>
        member this.DateTimeWithZone(index: int) : struct (DateTime * string) option = 
            PolarsWrapper.SeriesGetDatetime(this.Handle, int64 index)
            |> Option.ofNullable

        member this.Duration(index: int) : TimeSpan option = 
            PolarsWrapper.SeriesGetDuration(this.Handle, int64 index) |> Option.ofNullable
        // --- Aggregations (Returning Series of len 1) ---
        member this.First() = this.ApplyExpr(Expr.Col(this.Name).First())
        member this.Last() = this.ApplyExpr(Expr.Col(this.Name).Last())
        member this.Sum() = new Series(PolarsWrapper.SeriesSum this.Handle)
        member this.Mean() = new Series(PolarsWrapper.SeriesMean this.Handle)
        member this.Min() = new Series(PolarsWrapper.SeriesMin this.Handle)
        member this.Max() = new Series(PolarsWrapper.SeriesMax this.Handle)
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
