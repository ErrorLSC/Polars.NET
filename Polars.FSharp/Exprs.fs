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
    static member Col (names: seq<string>) =
        let arr = Seq.toArray names
        let sel: Selector = Selector.ByName arr
        sel.ToExpr()
    
    /// <summary>
    /// Create an expression representing all columns. Same as Col("*").
    /// </summary>
    static member All() = 
        Expr.Col "*"
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    static member AsStruct (exprs: seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.AsStruct handles)
    /// <summary>
    /// Create a single chunk of memory for this Series.
    /// </summary>
    /// <returns></returns>
    member this.Rechunk() = new Expr(PolarsWrapper.Rechunk(this.CloneHandle()))

    // --- Rounding & Sign ---
    /// <summary> Round the underlying floating point data to the given number of decimals. </summary>
    member this.Round(decimals: uint) = new Expr(PolarsWrapper.Round(this.CloneHandle(), uint decimals))
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
    member this.Cast(dtype: DataTypeExpr, ?strict: bool, ?wrapNumerical :bool) =
        let isStrict = defaultArg strict false
        let wn = defaultArg wrapNumerical false
        use typeHandle = dtype.CloneHandle()
        let newHandle = PolarsWrapper.ExprCast(this.CloneHandle(), typeHandle, isStrict, wn)
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
        new Expr(PolarsWrapper.Log(this.CloneHandle(), PolarsWrapper.Lit baseVal))
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

    static member ByName([<ParamArray>]columns: string array) =
        new Selector(PolarsWrapper.SelectorCols columns)

    /// <summary> Exclude columns from a wildcard selection (col("*")). </summary>
    member this.Exclude(names: seq<string>) =
        let arr = Seq.toArray names
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))
    /// <summary>
    /// Exclude columns matching any of the specified Selectors.
    /// </summary>
    member this.Exclude([<ParamArray>] selectors: ReadOnlySpan<Selector>) =
        if selectors.Length = 0 then 
            this
        else
            let mutable toExclude = selectors.[0]
            for i = 1 to selectors.Length - 1 do
                toExclude <- toExclude ||| selectors.[i]
                
            this - toExclude

    /// <summary>
    /// Exclude columns matching any of the specified Data Types.
    /// </summary>
    member this.Exclude([<ParamArray>] dtypes: ReadOnlySpan<DataType>) =
        if dtypes.Length = 0 then 
            this
        else
            let createSelector (dt: DataType) =
                new Selector(PolarsWrapper.SelectorByDtype(enum<PlDataType> dt.Code))
            
            let mutable toExclude = createSelector dtypes.[0]
            for i = 1 to dtypes.Length - 1 do
                toExclude <- toExclude ||| createSelector dtypes.[i]
                
            this - toExclude
    /// <summary>
    /// Convert the Selector to an Expression.
    /// Selectors are essentially dynamic Expressions that expand to column names.
    /// </summary>
    member this.ToExpr() =
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))
    static member Float() = new Selector(PolarsWrapper.SelectorFloat());
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
    static member (^^) (l: Selector, r: Selector) =
         new Selector(PolarsWrapper.SelectorXor(l.CloneHandle(), r.CloneHandle()))

and DataTypeExpr =
    val private handle : DataTypeExprHandle
    internal new (handle: DataTypeExprHandle) = { handle = handle }

    interface IDisposable with
        member this.Dispose() =
            if not (isNull (box this.handle)) && not this.handle.IsInvalid then
                this.handle.Dispose()
            GC.SuppressFinalize(this)

    member internal this.CloneHandle() = 
        PolarsWrapper.DataTypeExprClone(this.handle)

    /// <summary>Clone this DataTypeExpr.</summary>
    member this.Clone() = 
        new DataTypeExpr(this.CloneHandle())

    /// <summary>
    /// Convert DataTypeExpr into a DataType literal.
    /// Returns None if the expression is not a literal.
    /// </summary>
    member this.ToLiteral() : DataType option =
        let cloned = this.CloneHandle()
        try
            let literalHandle = PolarsWrapper.DataTypeExprIntoLiteral(cloned)
            if literalHandle.IsInvalid then
                literalHandle.Dispose()
                None
            else
                Some (DataType.FromHandle literalHandle)
        finally
            if not cloned.IsInvalid then cloned.Dispose()

        /// <summary>
    /// Get a default value of a specific type.
    /// </summary>
    /// <param name="n">Number of types you want the value</param>
    /// <param name="numericToOne">Use 1 instead of 0 as the default value for numeric types</param>
    /// <param name="numListValues">The amount of values a list contains</param>
    member this.DefaultValue(?n: int, ?numericToOne: bool, ?numListValues: int) =
        let n = defaultArg n 1
        let numericToOne = defaultArg numericToOne false
        let numListValues = defaultArg numListValues 0
        let handle = PolarsWrapper.DataTypeExprDefaultValue(this.CloneHandle(), n, numericToOne, numListValues)
        new Expr(handle)

    /// <summary>
    /// Get a formatted version of the output DataType.
    /// </summary>
    member this.Display() =
        let handle = PolarsWrapper.DataTypeExprDisplay(this.CloneHandle())
        new Expr(handle)

    /// <summary>
    /// Get the inner DataType of a List or Array.
    /// </summary>
    member this.InnerDataType() =
        let handle = PolarsWrapper.DataTypeExprInnerDtype(this.CloneHandle())
        new DataTypeExpr(handle)

    /// <summary>
    /// Get whether the output DataType matches a certain selector.
    /// </summary>
    member this.Matches(selector: Selector) =
        let handle = PolarsWrapper.DataTypeExprMatches(this.CloneHandle(), selector.CloneHandle())
        new Expr(handle)

    /// <summary>
    /// Get the DataType wrapped in a list.
    /// </summary>
    member this.WrapInList() =
        let handle = PolarsWrapper.DataTypeExprWrapInList(this.CloneHandle())
        new DataTypeExpr(handle)

    /// <summary>
    /// Get the DataType wrapped in an array.
    /// </summary>
    /// <param name="width">Array Width</param>
    member this.WrapInArray(width: uint) =
        let handle = PolarsWrapper.DataTypeExprWrapInArray(this.CloneHandle(), width)
        new DataTypeExpr(handle)

    /// <summary>
    /// Get the signed integer version of the same bitsize.
    /// </summary>
    member this.ToSignedInteger() =
        let handle = PolarsWrapper.DataTypeExprIntToSigned(this.CloneHandle())
        new DataTypeExpr(handle)

    /// <summary>
    /// Get the unsigned integer version of the same bitsize.
    /// </summary>
    member this.ToUnsignedInteger() =
        let handle = PolarsWrapper.DataTypeExprIntToUnsigned(this.CloneHandle())
        new DataTypeExpr(handle)

    member this.List = ListNameSpace(this)
    member this.Array = ArrayNameSpace(this)
    member this.Struct = StructNameSpace(this)
    
/// <summary>Namespace for list-related operations on a DataTypeExpr.</summary>
and ListNameSpace (parent: DataTypeExpr) =
    /// <summary>Get the inner DataType of a list.</summary>
    member this.InnerDataType() =
        let handle = PolarsWrapper.DataTypeExprListInnerDtype(parent.CloneHandle())
        new DataTypeExpr(handle)

/// <summary>Namespace for array-related operations on a DataTypeExpr.</summary>
and ArrayNameSpace (parent: DataTypeExpr) =
    /// <summary>Get the inner DataType of an array.</summary>
    member this.InnerDataType() =
        let handle = PolarsWrapper.DataTypeExprArrayInnerDtype(parent.CloneHandle())
        new DataTypeExpr(handle)

    /// <summary>Get the array width.</summary>
    member this.Width() =
        let handle = PolarsWrapper.DataTypeExprArrayWidth(parent.CloneHandle())
        new Expr(handle)

    /// <summary>Get the array shape.</summary>
    member this.Shape() =
        let handle = PolarsWrapper.DataTypeExprArrayShape(parent.CloneHandle())
        new Expr(handle)

/// <summary>Namespace for struct-related operations on a DataTypeExpr.</summary>
and StructNameSpace (parent: DataTypeExpr) =
    /// <summary>Get the field names in a struct as a list.</summary>
    member this.FieldNames() =
        let handle = PolarsWrapper.DataTypeExprStructFieldNames(parent.CloneHandle())
        new Expr(handle)

    /// <summary>Get the DataType of a field by name.</summary>
    member this.FieldDataType(name: string) =
        let handle = PolarsWrapper.DataTypeExprStructFieldDtypeByName(parent.CloneHandle(), name)
        new DataTypeExpr(handle)

    /// <summary>Get the DataType of a field by index (int64).</summary>
    member this.FieldDataType(index: int64) =
        let handle = PolarsWrapper.DataTypeExprStructFieldDtypeByIndex(parent.CloneHandle(), index)
        new DataTypeExpr(handle)

    /// <summary>Indexer: get DataType by int64 index.</summary>
    member this.Item with get(index: int64) = this.FieldDataType(index)

    /// <summary>Indexer: get DataType by int index.</summary>
    member this.Item with get(index: int) = this.FieldDataType(int64 index)

    /// <summary>Indexer: get DataType by field name.</summary>
    member this.Item with get(name: string) = this.FieldDataType(name)

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