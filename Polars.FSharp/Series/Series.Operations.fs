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
        /// Get the index values that would sort this expression.
        /// </summary>
        /// <param name="descending">If true, sort in descending order. Default is false.</param>
        /// <param name="nullsLast">If true, place null values last. Default is false.</param>
        member this.ArgSort(?descending: bool, ?nullsLast: bool) =
            this.ApplyExpr(Expr.Col(this.Name).ArgSort(?descending = descending, ?nullsLast = nullsLast))

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

        // ==========================================
        // Uniqueness & Boolean Masl
        // ==========================================
        /// <summary>
        /// Check if values are between lower and upper bounds.
        /// </summary>
        member this.IsBetween(lower:Expr, upper:Expr) = 
            this.ApplyExpr(Expr.Col(this.Name).IsBetween(lower,upper))

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
        // ==========================================
        // Statistical Ops
        // ==========================================

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


