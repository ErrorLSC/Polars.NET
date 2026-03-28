namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
open Polars.NET.Core.Helpers
/// <summary>
/// Interface for types that can be converted to one or more Polars Expressions.
/// </summary>
type IColumnExpr =
    abstract member ToExprs : unit -> Expr list

/// <summary>
/// Represents a Polars Expression (lazy evaluation).
/// Expressions are the building blocks of Polars queries, representing columns, literals, or transformations.
/// </summary>
/// <example>
/// <code>
/// // Select column "A", multiply by 2, and alias as "B"
/// let e = pl.col("A") * pl.lit(2) |> pl.alias "B"
/// </code>
/// </example>
and Expr(handle: ExprHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr handle
    member this.Clone() = new Expr(this.CloneHandle())
    interface IDisposable with member _.Dispose() = handle.Dispose()

    interface IColumnExpr with
        member this.ToExprs() = [this]

    // --- Namespaces ---
    /// <summary> Access naming operations (prefix/suffix). </summary>
    member this.Name = new NameOps(this.CloneHandle())
    /// <summary> Access list operations. </summary>
    member this.List = new ListOps(this.CloneHandle())
    /// <summary> Access struct operations. </summary>
    member this.Struct = new StructOps(this.CloneHandle())
    /// <summary> Access temporal (date/time) operations. </summary>
    member this.Dt = new DtOps(this.CloneHandle())
    /// <summary> Access string manipulation operations. </summary>
    member this.Str = new StringOps(this.CloneHandle())
    member this.Array = new ArrayOps(this.CloneHandle())
    member this.Meta = new MetaOps(this.Handle)

    // --- Column ---
    /// <summary>
    /// Create an expression representing a column in a DataFrame.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    static member Col (name: string) = new Expr(PolarsWrapper.Col name)
    /// <summary>
    /// Create an expression representing multiple columns (Wildcard).
    /// </summary>
    /// <example>
    /// <code>
    /// pl.cols ["A"; "B"]
    /// </code>
    /// </example>
    static member Cols (names: seq<string>) =
        let arr = Seq.toArray names
        new Expr(PolarsWrapper.Cols arr)
    
    /// <summary>
    /// Create an expression representing all columns. Same as Col("*").
    /// </summary>
    static member All() = 
        Expr.Col "*"
    /// <summary>
    /// Create a single chunk of memory for this Series.
    /// </summary>
    /// <returns></returns>
    member this.Rechunk() = new Expr(PolarsWrapper.Rechunk(this.CloneHandle()))

    // --- Rounding & Sign ---
    /// <summary> Round the underlying floating point data to the given number of decimals. </summary>
    member this.Round(decimals: int) = new Expr(PolarsWrapper.Round(this.CloneHandle(), uint decimals))
    /// <summary> Compute the element-wise sign (-1, 0, 1). </summary>
    member this.Sign() = new Expr(PolarsWrapper.Sign(this.CloneHandle()))
    /// <summary> Round up to the nearest integer. </summary>
    member this.Ceil() = new Expr(PolarsWrapper.Ceil(this.CloneHandle()))

    /// <summary> Round down to the nearest integer. </summary>
    member this.Floor() = new Expr(PolarsWrapper.Floor(this.CloneHandle()))
    // ==========================================
    // Bitwise Operations (Custom Extension)
    // ==========================================

    /// <summary>
    /// Bitwise left shift (<<).
    /// </summary>
    member this.BitLeftShift(n: int) = 
        new Expr(PolarsWrapper.BitLeftShift(this.CloneHandle(), n))

    /// <summary>
    /// Bitwise right shift (>>).
    /// </summary>
    member this.BitRightShift(n: int) = 
        new Expr(PolarsWrapper.BitRightShift(this.CloneHandle(), n))

    // --- Operators ---
    /// <summary> Greater than comparison. </summary>
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Less than comparison. </summary>
    static member (.<) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Lt(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Greater than or equal comparison. </summary>
    static member (.>=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.GtEq(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Less than or equal comparison. </summary>
    static member (.<=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.LtEq(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Equal comparison. </summary>
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Not equal comparison. </summary>
    static member (.!=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Neq(lhs.CloneHandle(), rhs.CloneHandle()))
    // Arithmetic
    static member ( + ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Add(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( - ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Sub(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( * ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( / ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Div(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( % ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Rem(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Power / Exponentiation. </summary>
    static member (.**) (baseExpr: Expr, exponent: Expr) = baseExpr.Pow exponent
    /// <summary> Logical AND. </summary>
    static member (.&&) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.And(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Logical OR. </summary>
    static member (.||) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Or(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Logical NOT. </summary>
    static member (!!) (e: Expr) = new Expr(PolarsWrapper.Not (e.CloneHandle()))
    static member (.^) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Xor(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Bitwise left shift operator (expr <<< n). </summary>
    static member (<<<) (lhs: Expr, rhs: int) = lhs.BitLeftShift rhs

    /// <summary> Bitwise right shift operator (expr >>> n). </summary>
    static member (>>>) (lhs: Expr, rhs: int) = lhs.BitRightShift rhs
    // --- Methods ---
    /// <summary> Rename the output column. </summary>
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(this.CloneHandle(), name))
    // --- SQL Expressions ---

    /// <summary> Create a Polars Expr from a SQL string. </summary>
    /// <param name="sql">The SQL expression string.</param>
    /// <returns>A Polars Expr representing the SQL logic.</returns>
    /// <exception cref="T:System.ArgumentException">Thrown when the provided SQL string is null or whitespace.</exception>
    static member SqlExpr(sql: string) =
        if String.IsNullOrWhiteSpace sql then
            invalidArg "sql" "SQL expression can not be null or whitespace."
            
        new Expr(PolarsWrapper.SqlExpr sql)
    /// <summary> Create an array of Polars Exprs from a collection of SQL strings. </summary>
    /// <param name="sqls">The collection of SQL expression strings.</param>
    /// <returns>An array of Polars Expr objects.</returns>
    static member SqlExprs(sqls: seq<string>) =
        sqls 
        |> Seq.map Expr.SqlExpr 
        |> Seq.toArray
    /// <summary>
    /// Cast the expression to a different data type.
    /// </summary>
    /// <param name="dtype">Target Polars DataType.</param>
    /// <param name="strict">If true, raise error on invalid cast. If false, convert to null.</param>
    member this.Cast(dtype: DataType, ?strict: bool) =
        let isStrict = defaultArg strict false
        use typeHandle = dtype.CreateHandle()
        let newHandle = PolarsWrapper.ExprCast(this.CloneHandle(), typeHandle, isStrict)
        new Expr(newHandle)
    // Aggregations
    /// <summary>
    /// Get the first n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    member this.Head(?n:int) =
        let n10 = defaultArg n 10
        new Expr(PolarsWrapper.Head(this.CloneHandle(),n10));
    /// <summary>
    /// Get the last n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    member this.Tail(?n:int) =
        let n10 = defaultArg n 10
        new Expr(PolarsWrapper.Tail(this.CloneHandle(),n10));
    member this.First(?ignoreNulls:bool) = 
        let ign = defaultArg ignoreNulls false
        new Expr(PolarsWrapper.First(this.CloneHandle(),ign))
    member this.Last(?ignoreNulls:bool) =
        let ign = defaultArg ignoreNulls false 
        new Expr(PolarsWrapper.Last(this.CloneHandle(),ign))
    member this.All(?ignoreNulls:bool) = 
        let ignore = defaultArg ignoreNulls false
        new Expr(PolarsWrapper.All(this.CloneHandle(),ignore))
    member this.Any(?ignoreNulls:bool) = 
        let ignore = defaultArg ignoreNulls false
        new Expr(PolarsWrapper.Any(this.CloneHandle(),ignore))
    member this.Item(?allowEmpty:bool) = 
        let allow = defaultArg allowEmpty true
        new Expr(PolarsWrapper.Item(this.CloneHandle(),allow))
    member this.Sum() = new Expr(PolarsWrapper.Sum (this.CloneHandle()))
    member this.Mean() = new Expr(PolarsWrapper.Mean (this.CloneHandle()))
    member this.Mode() =new Expr(PolarsWrapper.Mode (this.CloneHandle()))
    member this.Max() = new Expr(PolarsWrapper.Max (this.CloneHandle()))
    member this.Min() = new Expr(PolarsWrapper.Min (this.CloneHandle()))
    member this.NullCount() = new Expr(PolarsWrapper.NullCount (this.CloneHandle()))
    member this.NUnique() = new Expr(PolarsWrapper.NUnique (this.CloneHandle()))
    member this.ApproxNUnique() = new Expr(PolarsWrapper.ApproxNUnique (this.CloneHandle()))
    member this.Product() = new Expr(PolarsWrapper.Product (this.CloneHandle()))
    
    // Math
    member this.Abs() = new Expr(PolarsWrapper.Abs (this.CloneHandle()))
    member this.Sqrt() = new Expr(PolarsWrapper.Sqrt(this.CloneHandle()))
    member this.Cbrt() = new Expr(PolarsWrapper.Cbrt(this.CloneHandle()))
    member this.Exp() = new Expr(PolarsWrapper.Exp(this.CloneHandle()))
    member this.Dot(other:Expr) = new Expr(PolarsWrapper.Dot(this.CloneHandle(),other.CloneHandle()))
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

    /// <summary>
    /// Divide this expression by another.
    /// Result is always a float (True Division).
    /// </summary>
    member this.Truediv(other: Expr) =
        new Expr(PolarsWrapper.Div(this.CloneHandle(), other.CloneHandle()))

    /// <summary>
    /// Integer division (floor division).
    /// </summary>
    member this.FloorDiv(other: Expr) =
        new Expr(PolarsWrapper.FloorDiv(this.CloneHandle(), other.CloneHandle()))

    /// <summary>
    /// Modulo operator (remainder).
    /// </summary>
    member this.Mod(other: Expr) =
        new Expr(PolarsWrapper.Rem(this.CloneHandle(), other.CloneHandle()))
    member this.Rem(other: Expr) = 
        this.Mod other
        
    // ==========================================
    // Math: Trigonometry
    // ==========================================
    member this.Sin() = new Expr(PolarsWrapper.Sin(this.CloneHandle()))
    member this.Cos() = new Expr(PolarsWrapper.Cos(this.CloneHandle()))
    member this.Tan() = new Expr(PolarsWrapper.Tan(this.CloneHandle()))
    
    member this.ArcSin() = new Expr(PolarsWrapper.ArcSin(this.CloneHandle()))
    member this.ArcCos() = new Expr(PolarsWrapper.ArcCos(this.CloneHandle()))
    member this.ArcTan() = new Expr(PolarsWrapper.ArcTan(this.CloneHandle()))

    // ==========================================
    // Math: Hyperbolic
    // ==========================================

    member this.Sinh() = new Expr(PolarsWrapper.Sinh(this.CloneHandle()))
    member this.Cosh() = new Expr(PolarsWrapper.Cosh(this.CloneHandle()))
    member this.Tanh() = new Expr(PolarsWrapper.Tanh(this.CloneHandle()))
    
    member this.ArcSinh() = new Expr(PolarsWrapper.ArcSinh(this.CloneHandle()))
    member this.ArcCosh() = new Expr(PolarsWrapper.ArcCosh(this.CloneHandle()))
    member this.ArcTanh() = new Expr(PolarsWrapper.ArcTanh(this.CloneHandle()))
    // ==========================================
    // Indexing & Searching (Get / Gather / Arg / Index)
    // ==========================================

    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index expression.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    member this.Get(index: Expr, ?nullOnOutOfBounds: bool) =
        let nullOnOutOfBounds = defaultArg nullOnOutOfBounds false
        new Expr(PolarsWrapper.Get(this.CloneHandle(), index.CloneHandle(), nullOnOutOfBounds))

    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index number.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    member this.Get(index: uint64, ?nullOnOutOfBounds: bool) =
        let nullOnOutOfBounds = defaultArg nullOnOutOfBounds false
        new Expr(PolarsWrapper.Get(this.CloneHandle(), PolarsWrapper.Lit index, nullOnOutOfBounds))

    /// <summary>
    /// Gather values by an index expression.
    /// </summary>
    member this.Gather(indices: Expr) =
        new Expr(PolarsWrapper.Gather(this.CloneHandle(), indices.CloneHandle()))

    /// <summary>
    /// LINQ-like alias for Gather.
    /// </summary>
    member this.Take(indices: Expr) = 
        this.Gather indices

    /// <summary>
    /// Take every nth value starting from an offset.
    /// </summary>
    member this.GatherEvery(n: uint64, ?offset: uint64) =
        let offset = defaultArg offset 0UL
        new Expr(PolarsWrapper.GatherEvery(this.CloneHandle(), unativeint n, unativeint offset))

    /// <summary>
    /// Get the index of the unique values.
    /// </summary>
    member this.ArgUnique() =
        new Expr(PolarsWrapper.ArgUnique(this.CloneHandle()))

    /// <summary>
    /// Get the index of the maximum value.
    /// </summary>
    member this.ArgMax() =
        new Expr(PolarsWrapper.ArgMax(this.CloneHandle()))

    /// <summary>
    /// Get the index of the minimum value.
    /// </summary>
    member this.ArgMin() =
        new Expr(PolarsWrapper.ArgMin(this.CloneHandle()))

    /// <summary>
    /// Get the index values that would sort this expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">If true, place null values last. Default is false.</param>
    member this.ArgSort(?descending: bool, ?nullsLast: bool) =
        let descending = defaultArg descending false
        let nullsLast = defaultArg nullsLast false
        new Expr(PolarsWrapper.ArgSort(this.CloneHandle(), descending, nullsLast))

    /// <summary>
    /// Find the index of the first occurrence of a specific value.
    /// </summary>
    /// <param name="element">The element expression to search for.</param>
    member this.IndexOf(element: Expr) =
        new Expr(PolarsWrapper.IndexOf(this.CloneHandle(), element.CloneHandle()))

    /// <summary>
    /// Find indices where elements should be inserted to maintain order (Binary Search).
    /// </summary>
    /// <param name="element">The element expression to insert/search.</param>
    /// <param name="side">The insertion side (Any, Left, Right). Default is Any.</param>
    /// <param name="descending">Whether the target column is sorted in descending order. Default is false.</param>
    member this.SearchSorted(element: Expr, ?side: SearchSortedSide, ?descending: bool) =
        let side = defaultArg side SearchSortedSide.Any
        let descending = defaultArg descending false
        new Expr(PolarsWrapper.SearchSorted(this.CloneHandle(), element.CloneHandle(), side.ToNative(), descending))
    // ------ Stats ------
    /// <summary>
    /// Count the number of valid (non-null) values.
    /// </summary>
    member this.Count() = new Expr(PolarsWrapper.Count(this.CloneHandle()))
    /// <summary>
    /// Return the number of elements in the column.
    /// Null values count towards the total.
    /// </summary>
    member this.Len() = new Expr(PolarsWrapper.ExprLen(this.CloneHandle()))
    /// <summary> Return the number of rows in the context. </summary>
    static member Len() = new Expr(PolarsWrapper.Len())
    /// <summary>
    /// Get the standard deviation value.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    member this.Std(?ddof: int) = 
        let d = defaultArg ddof 1 // Default sample std dev
        new Expr(PolarsWrapper.Std(this.CloneHandle(), d))
    /// <summary>
    /// Get the variance value.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    member this.Var(?ddof: int) = 
        let d = defaultArg ddof 1
        new Expr(PolarsWrapper.Var(this.CloneHandle(), d))
    /// <summary>
    /// Get the median value.
    /// </summary>
    member this.Median() = new Expr(PolarsWrapper.Median (this.CloneHandle()))
    /// <summary>
    /// Compute the sample skewness of a data set.
    /// </summary>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    member this.Skew(?bias: bool) = 
        let b = defaultArg bias true
        new Expr(PolarsWrapper.Skew(this.CloneHandle(), b))
    /// <summary>
    /// Compute the kurtosis (Fisher or Pearson) of a dataset.
    /// </summary>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    member this.Kurtosis(?fisher: bool, ?bias: bool) = 
        let f = defaultArg fisher true
        let b = defaultArg bias true
        new Expr(PolarsWrapper.Kurtosis(this.CloneHandle(), f,b))
    /// <summary>
    /// Get the quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    member this.Quantile(q: float, ?interpolation: QuantileMethod) =
        let method = defaultArg interpolation QuantileMethod.Linear
        new Expr(PolarsWrapper.Quantile(this.CloneHandle(), q, method.ToNative()))
    /// <summary>
    /// Computes percentage change between values.
    /// Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element.
    /// Computes the change from the previous row by default.
    /// </summary>
    /// <param name="n">periods to shift for forming percent change.</param>
    member this.PctChange(?n: int) = 
        let nd = defaultArg n 1
        new Expr(PolarsWrapper.PctChange(this.CloneHandle(), nd))
    /// <summary>
    /// Assign ranks to data, dealing with ties appropriately.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="descending">Rank in descending order.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    member this.Rank(?method: RankMethod, ?descending: bool,?seed: uint64) = 
        let rm = defaultArg method RankMethod.Average
        let des = defaultArg descending false
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.Rank(this.CloneHandle(), rm.ToNative(),des,sd))
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// Get an array with the cumulative sum computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumSum(?reverse: bool) = 
        let r = defaultArg reverse true
        new Expr(PolarsWrapper.CumSum(this.CloneHandle(), r))
    /// <summary>
    /// Get an array with the cumulative max computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumMax(?reverse: bool) = 
        let r = defaultArg reverse true
        new Expr(PolarsWrapper.CumMax(this.CloneHandle(), r))
    /// <summary>   
    /// Get an array with the cumulative min computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumMin(?reverse: bool) = 
        let r = defaultArg reverse true
        new Expr(PolarsWrapper.CumMin(this.CloneHandle(), r))
    /// <summary>
    /// Get an array with the cumulative prod computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumProd(?reverse: bool) = 
        let r = defaultArg reverse true
        new Expr(PolarsWrapper.CumProd(this.CloneHandle(), r))        
    /// <summary>
    /// Get an array with the cumulative count computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumCount(?reverse: bool) = 
        let r = defaultArg reverse true
        new Expr(PolarsWrapper.CumCount(this.CloneHandle(), r))
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
    member this.EwmMean(alpha: float,?adjust: bool,?bias:bool,?minPeriods:int, ?ignoreNulls:bool) = 
        let adj = defaultArg adjust true
        let b = defaultArg bias true
        let ig = defaultArg ignoreNulls false
        let min = defaultArg minPeriods 1
        new Expr(PolarsWrapper.EwmMean(this.CloneHandle(),alpha,adj,b,min,ig))
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
    member this.EwmStd(alpha: float,?adjust: bool,?bias:bool,?minPeriods:int, ?ignoreNulls:bool) = 
        let adj = defaultArg adjust true
        let b = defaultArg bias true
        let ig = defaultArg ignoreNulls false
        let min = defaultArg minPeriods 1
        new Expr(PolarsWrapper.EwmStd(this.CloneHandle(),alpha,adj,b,min,ig))
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
    member this.EwmVar(alpha: float,?adjust: bool,?bias:bool,?minPeriods:int, ?ignoreNulls:bool) = 
        let adj = defaultArg adjust true
        let b = defaultArg bias true
        let ig = defaultArg ignoreNulls false
        let min = defaultArg minPeriods 1
        new Expr(PolarsWrapper.EwmVar(this.CloneHandle(),alpha,adj,b,min,ig))
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
        new Expr(PolarsWrapper.EwmMeanBy(this.CloneHandle(),by.CloneHandle(),halfLife))
    // ==========================================
    // Logic / Comparison
    // ==========================================
    /// <summary> Check if the value is between lower and upper bounds (inclusive). </summary>
    member this.IsBetween(lower: Expr, upper: Expr) =
        new Expr(PolarsWrapper.IsBetween(this.CloneHandle(), lower.CloneHandle(), upper.CloneHandle()))
    /// <summary>
    /// Check if the value is in given collection.
    /// </summary>
    member this.IsIn(other: Expr,?nullsEqual: bool) : Expr = 
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.IsIn(this.CloneHandle(), other.CloneHandle(),nE))
    /// <summary>
    /// Filter a single column.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new expression with filtered values.</returns>
    member this.Filter(predicate:Expr) : Expr =
        new Expr(PolarsWrapper.Filter(this.CloneHandle(),predicate.CloneHandle()))
    member this.FillNull(fillValue: Expr) = 
        new Expr(PolarsWrapper.FillNull(this.CloneHandle(), fillValue.CloneHandle()))
    /// <summary>
    /// Interpolate intermediate values. The interpolation method can be configured.
    /// <para>Nulls at the beginning and end of the series remain null.</para>
    /// </summary>
    /// <param name="method">Interpolation method (Linear or Nearest).</param>
    member this.Interpolate(?method:InterpolationMethod) = 
        let met = defaultArg method InterpolationMethod.Linear
        new Expr(PolarsWrapper.Interpolate(this.CloneHandle(), met.ToNative()))
    /// <summary>
    /// Interpolate intermediate values based on the values of another column.
    /// <para>
    /// This is useful when the data is not equally spaced, for example when interpolating based on a timestamp column.
    /// </para>
    /// </summary>
    /// <param name="by">The column to use for interpolation (e.g. a timestamp column).</param>
    /// <returns>A new expression with interpolated values.</returns>
    member this.InterpolateBy(by:Expr) = 
        new Expr(PolarsWrapper.InterpolateBy(this.CloneHandle(), by.CloneHandle()))
    member this.FillNan(fillValue:Expr) =
        new Expr(PolarsWrapper.FillNan(this.CloneHandle(), fillValue.CloneHandle()));
    member this.IsNull() = 
        new Expr(PolarsWrapper.IsNull(this.CloneHandle()))
    member this.IsNotNull() = 
        new Expr(PolarsWrapper.IsNotNull(this.CloneHandle()))
    member this.DropNulls() =
        new Expr(PolarsWrapper.DropNulls(this.CloneHandle()))
    member this.DropNans() =
        new Expr(PolarsWrapper.DropNans(this.CloneHandle()))
    // UDF
    /// <summary>
    /// Apply a custom C#/F# function (UDF) to the expression.
    /// The function receives an Apache Arrow Array and returns an Arrow Array.
    /// </summary>
    /// <param name="func">Function mapping IArrowArray -> IArrowArray.</param>
    /// <param name="outputType">The expected output DataType (optional).</param>
    member this.Map(func: Func<IArrowArray, IArrowArray>, outputType: DataType) =
        use typeHandle = outputType.CreateHandle()
        let newHandle = PolarsWrapper.Map(this.CloneHandle(), func, typeHandle)
        new Expr(newHandle)
    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        this.Map(func, DataType.SameAsInput)
    /// Advanced
    /// <summary> Explode a list column into multiple rows. </summary>
    member this.Explode(?emptyAsNull: bool, ?keepNulls: bool) = 
        let emp = defaultArg emptyAsNull true
        let kn = defaultArg keepNulls true
        new Expr(PolarsWrapper.Explode(this.CloneHandle(),emp,kn))
    /// <summary> Implode multiple rows to a list. </summary>
    member this.Implode() = new Expr(PolarsWrapper.Implode(this.CloneHandle()))
    // ==========================================
    // TopK / BottomK
    // ==========================================

    /// <summary>
    /// Get the k largest elements.
    /// Result is sorted descending.
    /// </summary>
    member this.TopK(k: int) = 
        new Expr(PolarsWrapper.TopK(this.CloneHandle(), uint k))

    /// <summary>
    /// Get the k smallest elements.
    /// Result is sorted ascending.
    /// </summary>
    member this.BottomK(k: int) = 
        new Expr(PolarsWrapper.BottomK(this.CloneHandle(), uint k))

    // ==========================================
    // TopKBy / BottomKBy
    // ==========================================

    /// <summary>
    /// Get the top k elements determined by the values in the 'by' columns.
    /// </summary>
    /// <param name="k">Number of elements to return.</param>
    /// <param name="by">Columns to sort by.</param>
    /// <param name="reverse">Reverse the sort order for each by column. Default false.</param>
    member this.TopKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let byHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let revArr = 
            match reverse with
            | Some r -> r |> Seq.toArray
            | None -> [| false |] // C# 端会广播

        new Expr(PolarsWrapper.TopKBy(this.CloneHandle(), uint k, byHandles, revArr))

    /// <summary>
    /// Get the bottom k elements determined by the values in the 'by' columns.
    /// </summary>
    member this.BottomKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let byHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let revArr = 
            match reverse with
            | Some r -> r |> Seq.toArray
            | None -> [| false |]

        new Expr(PolarsWrapper.BottomKBy(this.CloneHandle(), uint k, byHandles, revArr))

    // --- Sugar Overloads (Single Column By) ---

    member this.TopKBy(k: int, by: #IColumnExpr, reverse: bool) =
        this.TopKBy(k, [by], [| reverse |])
    
    member this.TopKBy(k: int, by: #IColumnExpr) =
        this.TopKBy(k, [by], [| false |])

    member this.BottomKBy(k: int, by: #IColumnExpr, reverse: bool) =
        this.BottomKBy(k, [by], [| reverse |])

    member this.BottomKBy(k: int, by: #IColumnExpr) =
        this.BottomKBy(k, [by], [| false |])
    /// <summary> 
    /// Apply a window function over specific partition columns. 
    /// </summary>
    /// <example>
    /// <code>
    /// // Calculate sum of "Value" per "Group"
    /// pl.col("Value").Sum().Over(pl.col("Group"))
    /// </code>
    /// </example>
    member this.Over(partitionBy: Expr list) =
        let mainHandle = this.CloneHandle()
        let partHandles = partitionBy |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        new Expr(PolarsWrapper.Over(mainHandle, partHandles))

    member this.Over(partitionCol: Expr) =
        this.Over [partitionCol]
    /// <summary>
    /// Shift values by the given number of indices.
    /// Positive values shift downstream, negative values shift upstream.
    /// </summary>
    member this.Shift(n: int64) = new Expr(PolarsWrapper.Shift(this.CloneHandle(), n))
    // Default shift 1
    member this.Shift() = this.Shift 1L

    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// Null values are propagated.
    /// </summary>
    member this.Diff(n: int64) = new Expr(PolarsWrapper.Diff(this.CloneHandle(), n))
    // Default diff 1
    member this.Diff() = this.Diff 1L

    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    member this.ForwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.ForwardFill(this.CloneHandle(), uint l))
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    member this.BackwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.BackwardFill(this.CloneHandle(), uint l))

    // ==========================================
    // Uniqueness & Duplication
    // ==========================================

    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// </summary>
    member this.IsUnique() =
        new Expr(PolarsWrapper.ExprIsUnique(this.CloneHandle()))

    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// </summary>
    member this.IsDuplicated() =
        new Expr(PolarsWrapper.ExprIsDuplicated(this.CloneHandle()))

    /// <summary>
    /// Get unique values of this expression.
    /// </summary>
    member this.Unique() =
        new Expr(PolarsWrapper.ExprUnique(this.CloneHandle()))

    /// <summary>
    /// Get unique values of this expression, maintaining order (Stable).
    /// </summary>
    member this.UniqueStable() =
        new Expr(PolarsWrapper.ExprUniqueStable(this.CloneHandle()))
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
    /// <returns>A new <see cref="Expr"/> with the rolling minimum.</returns>
    member this.RollingMin(windowSize: string, ?minPeriod: int,?weights: float[],?center:bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingMin(this.CloneHandle(), windowSize,m,w,c))
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
    /// <returns>A new <see cref="Expr"/> with the rolling minimum.</returns>
    member this.RollingMin(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool) =
        this.RollingMin(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)
        
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
    /// <returns>A new <see cref="Expr"/> with the rolling maximum.</returns>
    member this.RollingMax(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingMax(this.CloneHandle(), windowSize, m, w, c))
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
    /// <returns>A new <see cref="Expr"/> with the rolling maximum.</returns>
    member this.RollingMax(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool) =
        this.RollingMax(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)
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
    /// <returns>A new <see cref="Expr"/> with the rolling average.</returns>
    member this.RollingMean(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingMean(this.CloneHandle(), windowSize, m, w, c))
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
    /// <returns>A new <see cref="Expr"/> with the rolling average.</returns>
    member this.RollingMean(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool) =
        this.RollingMean(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)

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
    /// <returns>A new <see cref="Expr"/> with the rolling sum.</returns>
    member this.RollingSum(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingSum(this.CloneHandle(), windowSize, m, w, c))
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
    /// <returns>A new <see cref="Expr"/> with the rolling sum.</returns>
    member this.RollingSum(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool) =
        this.RollingSum(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)
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
    /// <returns>A new <see cref="Expr"/> with the rolling standard deviation.</returns>
    member this.RollingStd(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingStd(this.CloneHandle(), windowSize, m, w, c))
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
    /// <returns>A new <see cref="Expr"/> with the rolling standard deviation.</returns>
    member this.RollingStd(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool) =
        this.RollingStd(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)
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
    /// <returns>A new <see cref="Expr"/> with the rolling variance.</returns>
    member this.RollingVar(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool,?ddof:uint8) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        let d = defaultArg ddof 1uy
        new Expr(PolarsWrapper.RollingVar(this.CloneHandle(), windowSize, m, w, c,d))
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
    /// <returns>A new <see cref="Expr"/> with the rolling variance.</returns>
    member this.RollingVar(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool,?ddof: uint8) =
        this.RollingVar(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center, ?ddof = ddof)
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
    /// <returns>A new <see cref="Expr"/> with the rolling median.</returns>
    member this.RollingMedian(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingMedian(this.CloneHandle(), windowSize, m, w, c))
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
    /// <returns>A new <see cref="Expr"/> with the rolling median.</returns>
    member this.RollingMedian(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool) =
        this.RollingMedian(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)
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
    /// <returns>A new <see cref="Expr"/> with the rolling skew.</returns>
    member this.RollingSkew(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool,?bias: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        let b = defaultArg bias true
        new Expr(PolarsWrapper.RollingSkew(this.CloneHandle(), windowSize, m, w, c,b))
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
    /// <returns>A new <see cref="Expr"/> with the rolling skew.</returns>
    member this.RollingSkew(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool,?bias: bool) =
        this.RollingSkew(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center, ?bias = bias)
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
    /// <returns>A new <see cref="Expr"/> with the rolling skew.</returns>
    member this.RollingKurtosis(windowSize: string, ?minPeriod: int, ?weights: float[], ?center: bool,?fisher:bool,?bias: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        let b = defaultArg bias true
        let f = defaultArg fisher true
        new Expr(PolarsWrapper.RollingKurtosis(this.CloneHandle(), windowSize, m, w, c,f,b))
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
    /// <returns>A new <see cref="Expr"/> with the rolling skew.</returns>
    member this.RollingKurtosis(windowSize: TimeSpan, ?minPeriod: int, ?weights: float[], ?center: bool,?fisher:bool,?bias: bool) =
        this.RollingKurtosis(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center,?fisher=fisher, ?bias = bias)
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
    member this.RollingRank(windowSize: string, ?minPeriod: int, ?method: RankMethod,?seed:uint64 ,?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        let met = defaultArg method RankMethod.Average
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.RollingRank(this.CloneHandle(), windowSize, m, met.ToNative(), sd,w,c))
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
    member this.RollingRank(windowSize: TimeSpan, ?minPeriod: int,?method: RankMethod,?seed:uint64 , ?weights: float[], ?center: bool) =
        this.RollingRank(DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod,?method=method,?seed=seed, ?weights=weights, ?center=center)
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
    /// <returns>A new Expr representing the rolling quantile.</returns>
    member this.RollingQuantile(quantile:float,method: QuantileMethod,windowSize: string, ?minPeriod: int ,?weights: float[], ?center: bool) =
        let m = defaultArg minPeriod 1
        let w = match weights with Some arr -> arr | None -> null
        let c = defaultArg center false
        new Expr(PolarsWrapper.RollingQuantile(this.CloneHandle(),quantile,method.ToNative(), windowSize, m,w,c))
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
    /// <returns>A new Expr representing the rolling quantile.</returns>
    member this.RollingQuantile(quantile:float,method: QuantileMethod,windowSize: TimeSpan, ?minPeriod: int ,?weights: float[], ?center: bool) =
        this.RollingQuantile(quantile,method,DurationFormatter.ToPolarsString windowSize, ?minPeriod=minPeriod, ?weights=weights, ?center=center)
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
    /// <returns>A new Expr representing the dynamic rolling mean.</returns>
    member this.RollingMeanBy(windowSize: string, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        new Expr(PolarsWrapper.RollingMeanBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling mean.</returns>
    member this.RollingMeanBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingMeanBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
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
    /// <returns>A new Expr representing the dynamic rolling sum.</returns>
    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingSumBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling sum.</returns>
    member this.RollingSumBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingSumBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
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
    /// <returns>A new Expr representing the dynamic rolling max.</returns>
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMaxBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling median.</returns>
    member this.RollingMaxBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingMaxBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
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
    /// <returns>A new Expr representing the dynamic rolling min.</returns>
    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMinBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling min.</returns>
    member this.RollingMinBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingMinBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
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
    /// <returns>A new Expr representing the dynamic rolling standard deviation.</returns>
    member this.RollingStdBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingStdBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling standard deviation.</returns>
    member this.RollingStdBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingStdBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
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
    /// <returns>A new Expr representing the dynamic rolling variance.</returns>
    member this.RollingVarBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int,?ddof:uint8) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        let d = defaultArg ddof 1uy
        new Expr(PolarsWrapper.RollingVarBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative(),d))
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
    /// <returns>A new Expr representing the dynamic rolling variance.</returns>
    member this.RollingVarBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int,?ddof:uint8) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingVarBy(DurationFormatter.ToPolarsString windowSize,by,c,m,?ddof=ddof)
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
    /// <returns>A new Expr representing the dynamic rolling max.</returns>
    member this.RollingMedianBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMedianBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling max.</returns>
    member this.RollingMedianBy(windowSize: TimeSpan, by: Expr,?closed: ClosedWindow,?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1
        this.RollingMedianBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
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
    /// <returns>A new Expr representing the dynamic rolling rank.</returns>
    member this.RollingRankBy(windowSize: string, by: Expr, ?method:RollingRankMethod,?seed:uint64,?closed: ClosedWindow, ?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let met = defaultArg method RollingRankMethod.Average
        let m = defaultArg minPeriod 1 
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.RollingRankBy(this.CloneHandle(), windowSize, by.CloneHandle(),met.ToNative(),sd,m, c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling rank.</returns>
    member this.RollingRankBy(windowSize: TimeSpan, by: Expr, ?method:RollingRankMethod,?seed:uint64,?closed: ClosedWindow, ?minPeriod: int) =
        this.RollingRankBy(DurationFormatter.ToPolarsString windowSize,by,?method=method,?seed=seed,?closed=closed,?minPeriod=minPeriod)
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
    /// <returns>A new Expr representing the dynamic rolling quantile.</returns>
    member this.RollingQuantileBy(quantile:float,method:QuantileMethod, windowSize: string, by: Expr,?closed: ClosedWindow, ?minPeriod: int) =
        let c = defaultArg closed ClosedWindow.Left
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingQuantileBy(this.CloneHandle(),quantile,method.ToNative(), windowSize,m, by.CloneHandle(), c.ToNative()))
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
    /// <returns>A new Expr representing the dynamic rolling quantile.</returns>
    member this.RollingQuantileBy(quantile:float,method:QuantileMethod,windowSize: TimeSpan, by: Expr,?closed: ClosedWindow, ?minPeriod: int) =
        this.RollingQuantileBy(quantile,method,DurationFormatter.ToPolarsString windowSize,by,?closed=closed,?minPeriod=minPeriod)
    /// <summary>
    /// Concat multiple string expressions into a single string expression.
    /// </summary>
    static member ConcatString(exprs: seq<Expr>,?separator: string, ?ignoreNulls: bool) =
        let sep = defaultArg separator ","
        let ignNulls = defaultArg ignoreNulls false
        
        let handles = 
            exprs 
            |> Seq.map (fun e -> PolarsWrapper.CloneExpr e.Handle)
            |> Seq.toArray
            
        new Expr(PolarsWrapper.ConcatString(handles, sep, ignNulls))

    /// <summary>
    /// Format multiple string expressions into a single formated string expression.
    /// </summary>
    static member FormatString(format: string, exprs: seq<Expr>) =
        let handles = 
            exprs 
            |> Seq.map (fun e -> PolarsWrapper.CloneExpr(e.Handle))
            |> Seq.toArray
            
        new Expr(PolarsWrapper.FormatString(format, handles))

    // ==========================================
    // Concat Exprs
    // ==========================================
    
    /// <summary>
    /// Concat multiple expressions into a single expression.
    /// </summary>
    static member ConcatExpr(exprs: seq<Expr>,?rechunk: bool) =
        let rchk = defaultArg rechunk false
        let handles = 
            exprs 
            |> Seq.map (fun e -> PolarsWrapper.CloneExpr e.Handle)
            |> Seq.toArray
            
        new Expr(PolarsWrapper.ConcatExprs(handles, rchk))

    member this.ToSelector() =
        new Selector(PolarsWrapper.ToSelector(this.CloneHandle()))

// --- Namespace Helpers ---

and [<Struct>] DtOps(handle: ExprHandle) =
    /// <summary>Get the year from the underlying date/datetime.</summary>
    member _.Year() = new Expr(PolarsWrapper.DtYear handle)
    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    member _.Quarter() = new Expr(PolarsWrapper.DtQuarter handle)
    /// <summary>Get the month from the underlying date/datetime.</summary>
    member _.Month() = new Expr(PolarsWrapper.DtMonth handle)
    /// <summary>Get the day from the underlying date/datetime.</summary>
    member _.Day() = new Expr(PolarsWrapper.DtDay handle)
    /// <summary>Get the ordinal day(day of year) from the underlying date/datetime.</summary>
    member _.OrdinalDay() = new Expr(PolarsWrapper.DtOrdinalDay handle)
    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    member _.Weekday() = new Expr(PolarsWrapper.DtWeekday handle)
    /// <summary>Get the hour from the underlying datetime.</summary>
    member _.Hour() = new Expr(PolarsWrapper.DtHour handle)
    /// <summary>Get the minute from the underlying datetime.</summary>
    member _.Minute() = new Expr(PolarsWrapper.DtMinute handle)
    /// <summary>Get the second from the underlying datetime.</summary>
    member _.Second() = new Expr(PolarsWrapper.DtSecond handle)
    /// <summary>Get the millisecond from the underlying datetime.</summary>
    member _.Millisecond() = new Expr(PolarsWrapper.DtMillisecond handle)
    /// <summary>Get the microsecond from the underlying datetime.</summary>
    member _.Microsecond() = new Expr(PolarsWrapper.DtMicrosecond handle)
    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    member _.Nanosecond() = new Expr(PolarsWrapper.DtNanosecond handle)
    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    member _.Date() = new Expr(PolarsWrapper.DtDate handle)
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    member _.Time() = new Expr(PolarsWrapper.DtTime handle)

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        new Expr(PolarsWrapper.DtToString(handle, format))

    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"
    // --- Manipulation ---

    /// <summary>
    /// Truncate dates to the specified interval (e.g., "1d", "1h", "15m").
    /// </summary>
    member _.Truncate(every: string) = 
        new Expr(PolarsWrapper.DtTruncate(handle, every))
    member _.Truncate(every: TimeSpan) = 
        new Expr(PolarsWrapper.DtTruncate(handle, DurationFormatter.ToPolarsString every))
    /// <summary>
    /// Round dates to the nearest interval.
    /// </summary>
    member _.Round(every: string) = 
        new Expr(PolarsWrapper.DtRound(handle, every))
    member _.Round(every: TimeSpan) = 
        new Expr(PolarsWrapper.DtRound(handle, DurationFormatter.ToPolarsString every))
    /// <summary>
    /// Offset the date by a given duration string (e.g., "1d", "-2h").
    /// </summary>
    member _.OffsetBy(duration: string) =
        let durExpr = PolarsWrapper.Lit duration
        new Expr(PolarsWrapper.DtOffsetBy(handle, durExpr))

    /// <summary>
    /// Offset the date by a duration expression (dynamic offset).
    /// </summary>
    member _.OffsetBy(duration: Expr) =
        new Expr(PolarsWrapper.DtOffsetBy(handle, duration.CloneHandle()))
    member _.OffsetBy(duration: TimeSpan) =
        let durExpr = PolarsWrapper.Lit(DurationFormatter.ToPolarsString duration)
        new Expr(PolarsWrapper.DtOffsetBy(handle, durExpr))
    // --- Conversion ---

    /// <summary>
    /// Convert to integer timestamp (Microseconds).
    /// </summary>
    member _.TimestampMicros() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, TimeUnit.Microseconds.ToNative()))

    /// <summary>
    /// Convert to integer timestamp (Milliseconds).
    /// </summary>
    member _.TimestampMillis() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, TimeUnit.Milliseconds.ToNative()))
    /// <summary>
    /// Combine the date from the underlying date/datetime with the time from another expression.
    /// <para>The resulting Series will have the specified TimeUnit.</para>
    /// </summary>
    /// <param name="time">An expression yielding the Time component.</param>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    member _.Combine(time:Expr, ?timeUnit:TimeUnit ) = 
        let unit = defaultArg timeUnit TimeUnit.Microseconds
        let hTime = PolarsWrapper.CloneExpr time.Handle;
        new Expr(PolarsWrapper.DtCombine(handle, hTime, unit.ToNative()));

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
    /// <summary>
    /// Add business days to a date column.
    /// </summary>
    /// <param name="n">Number of business days to add.</param>
    /// <param name="weekMask">Array of 7 bools (Mon-Sun) indicating business days. Default: [true, true, true, true, true, false, false].</param>
    /// <param name="holidays">List of holidays to skip.</param>
    /// <param name="roll">Strategy for handling non-business start dates. Default: Raise.</param>
    member this.AddBusinessDays(
        n: Expr, 
        ?weekMask: bool[], 
        ?holidays: seq<DateOnly>, 
        ?roll: Roll
    ) =
        let mask = defaultArg weekMask [| true; true; true; true; true; false; false |]
        let r = defaultArg roll Roll.Raise
        
        let epoch = DateOnly(1970, 1, 1).DayNumber
        let holidayInts = 
            match holidays with
            | Some hols -> hols |> Seq.map (fun d -> d.DayNumber - epoch) |> Seq.toArray
            | None -> [||]

        new Expr(PolarsWrapper.DtAddBusinessDays(
            handle, 
            n.CloneHandle(), 
            mask, 
            holidayInts, 
            r.ToNative()
        ))

    /// <summary>
    /// Overload: Add business days using an integer literal.
    /// </summary>
    member this.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        let expr = new Expr(PolarsWrapper.Lit n)
        this.AddBusinessDays(
            expr, 
            ?weekMask = weekMask, 
            ?holidays = holidays, 
            ?roll = roll
        )

    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    /// <param name="weekMask">Array of 7 bools (Mon-Sun). Default: Mon-Fri are business days.</param>
    /// <param name="holidays">List of holidays.</param>
    member this.IsBusinessDay(?weekMask: bool[], ?holidays: seq<DateOnly>) =
        let mask = defaultArg weekMask [| true; true; true; true; true; false; false |]
        
        let epoch = DateOnly(1970, 1, 1).DayNumber
        let holidayInts = 
            match holidays with
            | Some hols -> hols |> Seq.map (fun d -> d.DayNumber - epoch) |> Seq.toArray
            | None -> [||]

        new Expr(PolarsWrapper.DtIsBusinessDay(
            handle,
            mask,
            holidayInts
        ))
        
and [<Struct>] StringOps(handle: ExprHandle) =
    
    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = new Expr(PolarsWrapper.StrToUpper handle)
    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = new Expr(PolarsWrapper.StrToLower handle)
    /// <summary> Get length in bytes. </summary>
    member _.Len() = new Expr(PolarsWrapper.StrLenBytes handle)
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

and [<Struct>] NameOps(handle: ExprHandle) =
    // let wrap op arg = new Expr(op(handle, arg))
    member _.Prefix(p: string) = new Expr(PolarsWrapper.Prefix(handle,p))
    member _.Suffix(s: string) = new Expr(PolarsWrapper.Suffix(handle,s))

and [<Struct>] ListOps(handle: ExprHandle) =
    /// <summary> Get the first element of the list. </summary>
    member _.First() = new Expr(PolarsWrapper.ListFirst handle)
    /// <summary> Get element at index. </summary>
    member _.Get(index: int) = new Expr(PolarsWrapper.ListGet(handle, int64 index))
    /// <summary> Join list elements with separator. </summary>
    member _.Join(separator: string) = new Expr(PolarsWrapper.ListJoin(handle, separator))
    /// <summary> Get list length. </summary>
    member _.Len() = new Expr(PolarsWrapper.ListLen handle)
    /// <summary> Reverse the list. </summary>
    member _.Reverse() = new Expr(PolarsWrapper.ListReverse handle)
    // Aggregations within list
    member _.Sum() = new Expr(PolarsWrapper.ListSum handle)
    member _.Min() = new Expr(PolarsWrapper.ListMin handle)
    member _.Max() = new Expr(PolarsWrapper.ListMax handle)
    member _.Mean() = new Expr(PolarsWrapper.ListMean handle)
    /// <summary> Sort the list. </summary>
    member _.Sort(?descending: bool, ?nullsLast:bool,?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nullsLastOption = defaultArg nullsLast false 
        let maintainOrderOption = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ListSort(handle, desc,nullsLastOption,maintainOrderOption))
    /// <summary>
    /// Combine the current expression with other expressions into a List.
    /// Result: [parent_val, other_val_1, other_val_2, ...]
    /// Equivalent to: pl.concatList([parent, others...])
    /// </summary>
    member _.Concat(others: seq<#IColumnExpr>) =
        let currentHandle = handle
        let handles = 
            seq {
                yield currentHandle
                
                yield! others 
                       |> Seq.collect (fun x -> x.ToExprs()) 
                       |> Seq.map (fun e -> e.CloneHandle())
            }
            |> Seq.toArray

        new Expr(PolarsWrapper.ConcatList handles)

    /// <summary>
    /// Overload: Concat a single expression/column.
    /// </summary>
    member this.Concat(other: #IColumnExpr) =
        this.Concat [other]
    // Contains
    member _.Contains(item: Expr,?nullsEqual: bool) : Expr = 
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.ListContains(handle, item.CloneHandle(),nE))
    member _.Contains(item: int,?nullsEqual: bool) = 
        let itemHandle = PolarsWrapper.Lit item
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle,nE))
    member _.Contains(item: string,?nullsEqual:bool) =
        let nE = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle, nE))

and [<Struct>] ArrayOps(handle: ExprHandle) = 
    // --- Aggregations ---

    /// <summary> Compute the sum of the values in the array. </summary>
    member _.Sum() = new Expr(PolarsWrapper.ArraySum handle)

    /// <summary> Compute the minimum value in the array. </summary>
    member _.Min() = new Expr(PolarsWrapper.ArrayMin handle)

    /// <summary> Compute the maximum value in the array. </summary>
    member _.Max() = new Expr(PolarsWrapper.ArrayMax handle)

    /// <summary> Compute the mean value in the array. </summary>
    member _.Mean() = new Expr(PolarsWrapper.ArrayMean handle)

    /// <summary> Compute the median value in the array. </summary>
    member _.Median() = new Expr(PolarsWrapper.ArrayMedian handle)

    /// <summary> Compute the standard deviation of the values in the array. </summary>
    member _.Std(?ddof: int) = 
        let d = defaultArg ddof 1 |> byte
        new Expr(PolarsWrapper.ArrayStd(handle, d))

    /// <summary> Compute the variance of the values in the array. </summary>
    member _.Var(?ddof: int) = 
        let d = defaultArg ddof 1 |> byte
        new Expr(PolarsWrapper.ArrayVar(handle, d))

    // --- Boolean / Search ---

    /// <summary> Check if any value in the array is true. </summary>
    member _.Any() = new Expr(PolarsWrapper.ArrayAny handle)

    /// <summary> Check if all values in the array are true. </summary>
    member _.All() = new Expr(PolarsWrapper.ArrayAll handle)

    /// <summary> Check if the array contains a specific item. </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        let eq = defaultArg nullsEqual false
        // Wrapper consumes item ownership
        new Expr(PolarsWrapper.ArrayContains(handle, item.CloneHandle(), eq))

    /// <summary> Check if the array contains a literal value. </summary>
    member this.Contains(item: string, ?nullsEqual) =
        let eq = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ArrayContains(handle, itemHandle, eq))
    member this.Contains(item: int, ?nullsEqual) =
        let eq = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ArrayContains(handle, itemHandle, eq))
    // --- Operations ---

    /// <summary> Get unique values in the array. </summary>
    member _.Unique(?stable: bool) = 
        let s = defaultArg stable false
        new Expr(PolarsWrapper.ArrayUnique(handle, s))

    /// <summary> Join array elements into a string. </summary>
    member _.Join(separator: string, ?ignoreNulls: bool) =
        let ign = defaultArg ignoreNulls true
        new Expr(PolarsWrapper.ArrayJoin(handle, separator, ign))

    /// <summary> Sort the array. </summary>
    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        let stable = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ArraySort(handle, desc, nLast, stable))

    /// <summary> Reverse the array. </summary>
    member _.Reverse() = new Expr(PolarsWrapper.ArrayReverse handle)

    /// <summary> Get the index of the minimum value. </summary>
    member _.ArgMin() = new Expr(PolarsWrapper.ArrayArgMin handle)

    /// <summary> Get the index of the maximum value. </summary>
    member _.ArgMax() = new Expr(PolarsWrapper.ArrayArgMax handle)

    /// <summary> Explode the array to rows. </summary>
    member _.Explode(?emptyAsNull:bool,?keepNulls:bool) = 
        let emp = defaultArg emptyAsNull true
        let kn = defaultArg keepNulls true
        new Expr(PolarsWrapper.ArrayExplode(handle,emp,kn))

    // --- Indexing ---

    /// <summary> Get value at index. </summary>
    member _.Get(index: Expr, ?nullOnOob: bool) =
        let oob = defaultArg nullOnOob false
        new Expr(PolarsWrapper.ArrayGet(handle, index.CloneHandle(), oob))

    /// <summary> Get value at integer index. </summary>
    member this.Get(index: int, ?nullOnOob) =
        let oob = defaultArg nullOnOob false
        let indexHandle = PolarsWrapper.Lit index
        new Expr(PolarsWrapper.ArrayGet(handle, indexHandle, oob))

    /// <summary>
    /// Combine the current expression with other expressions into an Array.
    /// Result: [parent_val, other_val_1, other_val_2, ...]
    /// Equivalent to: pl.concatArray([parent, others...])
    /// </summary>
    member _.Concat(others: seq<#IColumnExpr>) =
        let currentHandle = handle
        let handles = 
            seq {
                yield currentHandle
                
                yield! others 
                       |> Seq.collect (fun x -> x.ToExprs()) 
                       |> Seq.map (fun e -> e.CloneHandle())
            }
            |> Seq.toArray

        new Expr(PolarsWrapper.ConcatArray handles)

    // --- Conversion ---

    /// <summary> Convert Array to List (variable length). </summary>
    member _.ToList() = new Expr(PolarsWrapper.ArrayToList handle)

    /// <summary> Convert Array to Struct. </summary>
    member _.ToStruct() = new Expr(PolarsWrapper.ArrayToStruct handle)

and [<Struct>] StructOps(handle: ExprHandle) =
    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, name))
    member _.Field(index: int) = 
        new Expr(PolarsWrapper.StructFieldByIndex(handle, index))
    member _.RenameFields(names: string list) =
        let cArr = List.toArray names
        new Expr(PolarsWrapper.StructRenameFields(handle, cArr));
    member _.JsonEncode() = 
        new Expr(PolarsWrapper.StructJsonEncode handle);

and MetaOps(handle: ExprHandle) = 
    member this.OutputName() =
        PolarsWrapper.ExprGetOutputName handle

/// <summary>
/// A column selection strategy (e.g., all columns, or specific columns).
/// </summary>
and Selector(handle: SelectorHandle) =
    member _.Handle = handle
    
    member internal this.CloneHandle() = 
        PolarsWrapper.CloneSelector handle
    member this.Clone() = new Selector(this.CloneHandle())

    // ==========================================
    // Methods
    // ==========================================

    /// <summary> Exclude columns from a wildcard selection (col("*")). </summary>
    member this.Exclude(names: seq<string>) =
        let arr = Seq.toArray names
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))
        
    /// <summary>
    /// Convert the Selector to an Expression.
    /// Selectors are essentially dynamic Expressions that expand to column names.
    /// </summary>
    member this.ToExpr() =
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))

    interface IColumnExpr with
        member this.ToExprs() = [this.ToExpr()]

    interface IDisposable with member _.Dispose() = handle.Dispose()

    // ==========================================
    // Operators
    // ==========================================

    /// <summary> NOT operator: ~selector </summary>
    /// <example> ~~~pl.cs.numeric() </example>
    static member (~~~) (s: Selector) = 
        new Selector(PolarsWrapper.SelectorNot(s.CloneHandle()))

    /// <summary> AND operator: s1 &&& s2 (Intersection) </summary>
    /// <example> pl.cs.numeric() &&& pl.cs.matches("Val") </example>
    static member (&&&) (l: Selector, r: Selector) = 
        new Selector(PolarsWrapper.SelectorAnd(l.CloneHandle(), r.CloneHandle()))

    /// <summary> OR operator: s1 ||| s2 (Union) </summary>
    /// <example> pl.cs.startsWith("A") ||| pl.cs.endsWith("Z") </example>
    static member (|||) (l: Selector, r: Selector) = 
        new Selector(PolarsWrapper.SelectorOr(l.CloneHandle(), r.CloneHandle()))

    /// <summary> subtraction operator: s1 - s2 (Difference) </summary>
    /// <remarks> Some Polars versions support this as a shorthand for Exclude or Difference </remarks>
    static member (-) (l: Selector, r: Selector) =
         new Selector(PolarsWrapper.SelectorAnd(l.CloneHandle(), PolarsWrapper.SelectorNot(r.CloneHandle())))

/// <summary>
/// Let Expr & Selector in same line possible
/// </summary>
type ColumnExpr =
    /// <summary> Expr </summary>
    | Plain of Expr
    
    /// <summary> Selector </summary>
    | Select of Selector
    
    /// <summary> Selector with Map </summary>
    /// <example> Map(pl.cs.numeric(), fun e -> e * pl.lit(2)) </example>
    | MapCols of Selector * (Expr -> Expr)

    interface IColumnExpr with
        member this.ToExprs() =
            match this with
            | Plain e -> [ e ]
            
            | Select s -> [ s.ToExpr() ]
            
            | MapCols (s, mapper) -> 
                let wildcard = s.ToExpr()
                let mappedExpr = mapper wildcard
                [ mappedExpr ]