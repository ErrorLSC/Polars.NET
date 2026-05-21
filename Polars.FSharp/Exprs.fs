namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
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
    static member internal Col (name: string) = new Expr(PolarsWrapper.Col name)
    /// <summary>
    /// Create an expression representing multiple columns (Wildcard).
    /// </summary>
    /// <example>
    /// <code>
    /// pl.cols ["A"; "B"]
    /// </code>
    /// </example>
    static member internal Col (names: seq<string>) =
        let arr = Seq.toArray names
        let sel: Selector = Selector.ByName arr
        sel.ToExpr()
    
    /// <summary>
    /// Create an expression representing all columns. Same as Col("*").
    /// </summary>
    static member internal All() = 
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
    member this.Round(decimals: uint,?mode:RoundMode) = 
        let mo = defaultArg mode RoundMode.HalfToEven
        new Expr(PolarsWrapper.Round(this.CloneHandle(), uint decimals,mo.ToNative()))
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
    // Aggregations

    member this.Item(?allowEmpty:bool) = 
        let allow = defaultArg allowEmpty true
        new Expr(PolarsWrapper.Item(this.CloneHandle(),allow))

    // Math
    member this.Pow(exponent: Expr) = 
        new Expr(PolarsWrapper.Pow(this.CloneHandle(), exponent.CloneHandle()))
    member this.Pow(exponent: double) = 
        this.Pow(PolarsWrapper.Lit exponent |> fun h -> new Expr(h))
    member this.Pow(exponent: int) = 
        this.Pow(PolarsWrapper.Lit exponent |> fun h -> new Expr(h))

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
    /// Get the index values that would sort this expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">If true, place null values last. Default is false.</param>
    member this.ArgSort(?descending: bool, ?nullsLast: bool) =
        let descending = defaultArg descending false
        let nullsLast = defaultArg nullsLast false
        new Expr(PolarsWrapper.ArgSort(this.CloneHandle(), descending, nullsLast))
    // ------ Stats ------

    /// <summary> Return the number of rows in the context. </summary>
    static member internal Len() = new Expr(PolarsWrapper.Len())

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
    // Logic / Comparison
    // ==========================================

    /// <summary>
    /// Filter a single column.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new expression with filtered values.</returns>
    member this.Filter(predicate:Expr) : Expr =
        new Expr(PolarsWrapper.Filter(this.CloneHandle(),predicate.CloneHandle()))
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
            | None -> [| false |] 

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
    /// Calculate sum of "Value" per "Group"
    /// pl.col("Value").Sum().Over(pl.col("Group"))
    /// </code>
    /// </example>
    member this.Over(
        ?partitionBy: seq<Expr>,
        ?orderBy: seq<Expr>,
        ?descending: bool,
        ?nullsLast: bool,
        ?multithreaded: bool,
        ?maintainOrder: bool,
        ?mapping: WindowMappingStrategy
    ) =
        let partitionBy = defaultArg partitionBy Seq.empty
        let orderBy = defaultArg orderBy Seq.empty
        let descending = defaultArg descending false
        let nullsLast = defaultArg nullsLast false
        let multithreaded = defaultArg multithreaded true
        let maintainOrder = defaultArg maintainOrder false
        let mapping = defaultArg mapping WindowMappingStrategy.GroupsToRows

        let partArr = partitionBy |> Seq.toArray
        let orderArr = orderBy |> Seq.toArray

        if partArr.Length = 0 && orderArr.Length = 0 then
            this
        else
            let partHandles = partArr |> Array.map (fun e -> e.CloneHandle())
            let orderHandles = orderArr |> Array.map (fun e -> e.CloneHandle())
            let mainHandle = this.CloneHandle()  

            new Expr(PolarsWrapper.Over(
                mainHandle,
                partHandles,
                orderHandles,
                descending,
                nullsLast,
                multithreaded,
                maintainOrder,
                mapping.ToNative()
            ))

    member this.Over(partitionCol: Expr) =
        this.Over [partitionCol]
    /// <summary>
    /// Shift values by the given number of indices.
    /// Positive values shift downstream, negative values shift upstream.
    /// </summary>
    member this.Shift(n: int64) = new Expr(PolarsWrapper.Shift(this.CloneHandle(), PolarsWrapper.Lit n))
    // Default shift 1
    member this.Shift() = this.Shift 1L

    member this.FillNull(value:Expr) = new Expr(PolarsWrapper.FillNull(this.CloneHandle(),value.CloneHandle())) 
    member this.FillNull(strategy: FillNullStrategy, ?limit: int) =
        let l = defaultArg limit 0
        let nullableLimit = System.Nullable<uint32>(uint32 l)
        let exprHandle = this.CloneHandle() 
        new Expr(PolarsWrapper.FillNullWithStrategy(exprHandle, strategy.ToNative(), nullableLimit))
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    member this.ForwardFill(?limit: int) = 
        let l = defaultArg limit 0
        this.FillNull(FillNullStrategy.Forward, l)
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    member this.BackwardFill(?limit: int) = 
        let l = defaultArg limit 0
        this.FillNull(FillNullStrategy.Backward,l)
    // ==========================================
    // Uniqueness & Duplication
    // ==========================================
   
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

    static member internal ByName([<ParamArray>]columns: string array) =
        new Selector(PolarsWrapper.SelectorCols columns)
    static member internal ByDtype(dtype:DataType) = 
        new Selector(PolarsWrapper.SelectorByDtype(dtype.ToPlDataType()))
       
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
    val internal handle : DataTypeExprHandle
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
and [<Struct>] ListNameSpace (parent: DataTypeExpr) =
    /// <summary>Get the inner DataType of a list.</summary>
    member this.InnerDataType() =
        let handle = PolarsWrapper.DataTypeExprListInnerDtype(parent.CloneHandle())
        new DataTypeExpr(handle)

/// <summary>Namespace for array-related operations on a DataTypeExpr.</summary>
and [<Struct>] ArrayNameSpace (parent: DataTypeExpr) =
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
and [<Struct>] StructNameSpace (parent: DataTypeExpr) =
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

[<AutoOpen>]
module DataTypeExtension =
    type DataType with
        /// <summary>
        /// Convert this DataType to a DataTypeExpr literal.
        /// Equivalent to Python's polars.DataType.to_dtype_expr()
        /// </summary>
        member this.ToDataTypeExpr() =
            let handle = PolarsWrapper.DataTypeExprFromDataType(this.Handle)
            new DataTypeExpr(handle)

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