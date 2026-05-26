namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
/// <summary>
/// Interface for types that can be converted to one or more Polars Expressions.
/// </summary>
type IColumnExpr =
    abstract member ToExprs : unit -> Expr list
    abstract member ToSelector : unit -> Selector

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
    member internal _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr handle
    member this.Clone() = new Expr(this.CloneHandle())
    interface IDisposable with member _.Dispose() = handle.Dispose()

    interface IColumnExpr with
        member this.ToExprs() = [this]
        member this.ToSelector (): Selector = this.ToSelector()
    interface IEquatable<Expr> with
        member this.Equals(other: Expr) =
            if box other = null then 
                false
            elif Object.ReferenceEquals(this, other) then 
                true
            else
                PolarsWrapper.ExprEquals(this.Handle, other.Handle)

    /// <summary>
    /// Overrides the standard object equality check.
    /// </summary>
    override this.Equals(obj: obj) =
        match obj with
        | :? Expr as other -> (this :> IEquatable<Expr>).Equals(other)
        | _ -> false

    /// <summary>
    /// Get hashcode based on the dependencies (root column names) of the expression.
    /// </summary>
    override this.GetHashCode() =
        if this.Handle.IsInvalid then 
            0
        else
            // Inline of Meta.RootNames() logic to break compilation order dependencies
            let rootsArray = PolarsWrapper.RootNames(this.Handle)
            
            if box rootsArray = null || rootsArray.Length = 0 then
                0
            else
                let rootsStr = String.concat "," rootsArray
                rootsStr.GetHashCode()
    override this.ToString (): string = 
        if (this.Handle.IsInvalid) then 
            "Expr (Disposed)"
        else PolarsWrapper.ExprToString(this.Handle)

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
    static member internal LitNull() = new Expr(PolarsWrapper.LitNull())
    member this.Exclude(names:seq<string>):Expr =
        let sel: Selector = this.ToSelector()
        let ns: Selector = sel.Exclude(names)
        ns.ToExpr()
    member this.Exclude(dtypes: seq<DataType>):Expr = 
        let sel: Selector = this.ToSelector()
        let ns: Selector = sel.Exclude(dtypes)
        ns.ToExpr()
    /// <summary>
    /// Sort the expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">Whether to place null values last. Default is false.</param>
    /// <param name="multithreaded">If true, sort in multiple threads. Default is true.</param>
    /// <param name="maintainOrder">If true, maintain the order of equal elements. Default is false.</param>
    /// <param name="limit">Limit the sort output (for optimization purposes).</param>
    member this.Sort(?descending,?nullsLast,?multithreaded,?maintainOrder,?limit:uint32) = 
        let des = defaultArg descending false
        let nul = defaultArg nullsLast false
        let mul = defaultArg multithreaded true
        let mai = defaultArg maintainOrder false
        let lim = limit |> Option.toNullable
        new Expr(PolarsWrapper.Sort(this.CloneHandle(),des,nul,mul,mai,lim))
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    static member AsStruct (exprs: seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.AsStruct handles)
    /// <summary>
    /// Reverse the selection.
    /// <para>This is useful in a GroupBy context to reverse the order of the group.</para>
    /// </summary>
    /// <returns>A new expression with the order reversed.</returns>
    member this.Reverse() = new Expr(PolarsWrapper.Reverse(this.CloneHandle()))
    /// <summary>
    /// Create a single chunk of memory for this Series.
    /// </summary>
    /// <returns></returns>
    member this.Rechunk() = new Expr(PolarsWrapper.Rechunk(this.CloneHandle()))
    /// <summary>
    /// Calculate the lower bound.
    /// Returns a unit Series with the lowest value possible for the dtype of this expression.
    /// </summary>
    member this.LowerBound() = new Expr(PolarsWrapper.LowerBound(this.CloneHandle()))
    /// <summary>
    /// Calculate the upper bound.
    /// Returns a unit Series with the highest value possible for the dtype of this expression.
    /// </summary>
    member this.UpperBound() = new Expr(PolarsWrapper.UpperBound(this.CloneHandle()))
    /// <summary>
    /// Cast to physical representation of the logical dtype.
    /// </summary>
    member this.ToPhysical() = new Expr(PolarsWrapper.ExprToPhysical(this.CloneHandle()))
    // ==========================================
    // Random
    // ==========================================
    /// <summary>
    /// Shuffle the contents of this expression.Note this is shuffled independently of any other column or Expression. If you want each row to stay the same use df.sample(shuffle=True)
    /// </summary>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated each time the shuffle is called.</param>
    member this.Shuffle(?seed:uint64) = 
        let se = seed |> Option.toNullable
        new Expr(PolarsWrapper.ExprShuffle(this.CloneHandle(),se))
    /// <summary>
    /// Sample from this expression.
    /// </summary>
    /// <param name="n">Number of items to return. Default to 1</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">Shuffle the order of sampled data points.</param>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation.</param>
    member _.SampleN(?n:Expr,?withReplacement:bool,?shuffle:bool,?seed:uint64) =
        let num = 
            match n with
            | Some n -> n.CloneHandle()
            | None -> PolarsWrapper.Lit 1
        let wr = defaultArg withReplacement false
        let sh = defaultArg shuffle false
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.ExprSampleN(handle,num,wr,sh,sd))
    /// <summary>
    /// Sample from this expression.
    /// </summary>
    /// <param name="fraction">Fraction of items to return.</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">Shuffle the order of sampled data points.</param>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation.</param>
    member _.SampleFrac(fraction:Expr,?withReplacement:bool,?shuffle:bool,?seed:uint64) =
        let wr = defaultArg withReplacement false
        let sh = defaultArg shuffle false
        let sd = seed |> Option.toNullable
        new Expr(PolarsWrapper.ExprSampleFrac(handle,fraction.CloneHandle(),wr,sh,sd))
    // ==========================================
    // Rounding & Sign
    // ==========================================
    /// <summary>
    /// Round underlying floating point data by decimals digits.
    /// </summary>
    /// <param name="decimals">Number of decimals to round by.</param>
    /// <param name="mode">The rounding strategy used. A “rounded value” is a value with at most decimals decimal places (e.g. integers when decimals=0, multiples of 0.1 when decimals=1, 0.01 when decimals=2, and so on).
    /// Strategies that start with half_ round all values to the nearest rounded value, only using the strategy to break ties when a value falls exactly between two rounded values (e.g. 0.5 when decimals=0, 0.05 when decimals=1). 
    /// Other rounding strategies specify explicitly which rounded value is chosen and always apply (not just for tiebreaks).</param>
    member this.Round(?decimals: uint32,?mode:RoundMode) = 
        let de = defaultArg decimals 0u
        let mo = defaultArg mode RoundMode.HalfToEven
        new Expr(PolarsWrapper.Round(this.CloneHandle(), uint de,mo.ToNative()))
    /// <summary>
    /// Round to a number of significant figures.
    /// </summary>
    /// <param name="digits">Number of significant figures to round to.</param>
    member this.RoundSigFigs(digits:int) = new Expr(PolarsWrapper.RoundSigFigs(this.CloneHandle(),digits))
    /// <summary> Compute the element-wise sign (-1, 0, 1). </summary>
    member this.Sign() = new Expr(PolarsWrapper.Sign(this.CloneHandle()))
    /// <summary> Round up to the nearest integer. </summary>
    member this.Ceil() = new Expr(PolarsWrapper.Ceil(this.CloneHandle()))

    /// <summary> Round down to the nearest integer. </summary>
    member this.Floor() = new Expr(PolarsWrapper.Floor(this.CloneHandle()))
    /// <summary>
    /// Return indices where expression evaluates True.
    /// </summary>
    /// <returns>Expression of data type UInt32.</returns>
    member this.ArgTrue() = new Expr(PolarsWrapper.ArgWhere(this.CloneHandle()))
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
    /// <summary>
    /// Check if this expression is NOT equal to another, treating nulls as valid values.
    /// (e.g., Null != Null is False, 5 != Null is True)
    /// </summary>
    member this.NeqMissing(other:Expr) =
        new Expr(PolarsWrapper.NeqMissing(this.CloneHandle(),other.CloneHandle()))
    /// <summary>
    /// Check if this expression is equal to another, treating nulls as valid values.
    /// (e.g., Null == Null is True, 5 == Null is False)
    /// </summary>
    member this.EqMissing(other:Expr) =
        new Expr(PolarsWrapper.EqMissing(this.CloneHandle(),other.CloneHandle()))
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
    /// Compress the column data using run-length encoding.
    /// Run-length encoding (RLE) encodes data by storing each run of identical values as a single value and its length.
    /// </summary>
    /// <returns>Expression/Series of data type Struct with fields len of data type UInt32 and value of the original data type.</returns>  
    member this.Rle() = new Expr(PolarsWrapper.Rle(this.CloneHandle()))
    /// <summary>
    /// Get a distinct integer ID for each run of identical values.
    /// The ID starts at 0 and increases by one each time the value of the column changes.
    /// </summary>
    /// <returns>Expression/Series of data type UInt32.</returns>
    member this.RleId() = new Expr(PolarsWrapper.RleId(this.CloneHandle()))
    /// <summary>
    /// Get a boolean mask of the local maximum peaks.
    /// </summary>
    member this.PeakMax() = new Expr(PolarsWrapper.PeakMax(this.CloneHandle()))
    /// <summary>
    /// Get a boolean mask of the local minimum peaks.
    /// </summary>
    member this.PeakMin() = new Expr(PolarsWrapper.PeakMin(this.CloneHandle()))
    /// <summary>
    /// Bin continuous values into discrete categories.
    /// </summary>
    /// <param name="breaks">List of unique cut points.</param>
    /// <param name="labels">Names of the categories. The number of labels must be equal to the number of cut points plus one.</param>
    /// <param name="leftClosed">Set the intervals to be left-closed instead of right-closed.</param>
    /// <param name="includeBreaks">Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from an Enum to a Struct.</param>
    /// <returns>Expression/Series of data type Enum if include_breaks is set to False (default), otherwise an expression of data type Struct.</returns>
    member this.Cut(breaks: seq<double>, ?labels: seq<string>, ?leftClosed: bool, ?includeBreaks: bool) =
            let isLeftClosed = defaultArg leftClosed false
            let isIncludeBreaks = defaultArg includeBreaks false

            let breaksArray = Seq.toArray breaks
            let breaksSpan = ReadOnlySpan<double>(breaksArray)

            let labelsArray = 
                match labels with
                | Some lbls -> Seq.toArray lbls
                | None -> null

            let newHandle = PolarsWrapper.Cut(this.CloneHandle(), breaksSpan, labelsArray, isLeftClosed, isIncludeBreaks)
            new Expr(newHandle)
    /// <summary>
    /// Bin continuous values into discrete categories based on their quantiles.
    /// </summary>
    /// <param name="quantiles">Either a list of quantile probabilities between 0 and 1 or a positive integer determining the number of bins with uniform probability.</param>
    /// <param name="labels">Names of the categories. The number of labels must be equal to the number of categories.</param>
    /// <param name="leftClosed">Set the intervals to be left-closed instead of right-closed.</param>
    /// <param name="allowDuplicates">If set to True, duplicates in the resulting quantiles are dropped, rather than raising a DuplicateError. This can happen even with unique probabilities, depending on the data.</param>
    /// <param name="includeBreaks">Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from a Categorical to a Struct.</param>
    /// <returns>Expression/Series of data type Categorical if include_breaks is set to False (default), otherwise an expression of data type Struct.</returns>
    member this.QCut(quantiles: seq<double>, ?labels: seq<string>, ?leftClosed: bool,?allowDuplicates:bool, ?includeBreaks: bool) = 
            let isLeftClosed = defaultArg leftClosed false
            let isIncludeBreaks = defaultArg includeBreaks false
            let al = defaultArg allowDuplicates false

            let quanArray = quantiles |> Seq.toArray
            let quanSpan = ReadOnlySpan<double> quanArray

            let labelsArray = 
                match labels with
                | Some lbls -> Seq.toArray lbls
                | None -> null

            let newHandle = PolarsWrapper.QCut(this.CloneHandle(), quanSpan, labelsArray, isLeftClosed,al, isIncludeBreaks)
            new Expr(newHandle)
    member this.QCut(quantiles: int, ?labels: seq<string>, ?leftClosed: bool, ?allowDuplicates: bool, ?includeBreaks: bool) =
        let isLeftClosed = defaultArg leftClosed false
        let isAllowDuplicates = defaultArg allowDuplicates false
        let isIncludeBreaks = defaultArg includeBreaks false

        let labelsArray = 
            match labels with
            | Some lbls -> Seq.toArray lbls
            | None -> null

        let newHandle = PolarsWrapper.QCutUniform(this.CloneHandle(), unativeint quantiles, labelsArray, isLeftClosed, isAllowDuplicates, isIncludeBreaks)
        new Expr(newHandle)
    /// <summary>
    /// Replace the given values by different values of the same data type.
    /// </summary>
    /// <param name="old">Value or sequence of values to replace. Accepts expression input. </param>
    /// <param name="newExpr">Value or sequence of values to replace by. Accepts expression input.</param>
    member this.Replace(old:Expr,newExpr:Expr) = 
        new Expr(PolarsWrapper.ExprReplace(this.CloneHandle(),old.CloneHandle(),newExpr.CloneHandle()))
    /// <summary>
    /// Replace all values by different values.
    /// </summary>
    /// <param name="old">Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.</param>
    /// <param name="newExpr">Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. Length must match the length of old or have length 1.</param>
    /// <param name="defaultExpr">Set values that were not replaced to this value. If no default is specified, (default), an error is raised if any values were not replaced. Accepts expression input. Non-expression inputs are parsed as literals.</param>
    /// <param name="returnDataType">The data type of the resulting expression. If set to null (default), the data type is determined automatically based on the other inputs.</param>
    member this.ReplaceStrict(old:Expr,newExpr:Expr,?defaultExpr:Expr,?returnDataType:DataTypeExpr) = 
        let def = 
            match defaultExpr with
            | Some d -> d.CloneHandle()
            | None -> null
        let rdt = 
            match returnDataType with
            | Some r -> r.CloneHandle()
            | None -> null
        new Expr(PolarsWrapper.ExprReplaceStrict(this.CloneHandle(),old.CloneHandle(),newExpr.CloneHandle(),def,rdt))
    /// <summary>
    /// Append expressions.
    /// This is done by adding the chunks of other to this Series.
    /// </summary>
    /// <param name="other">Expression to append.</param>
    /// <param name="execUpcast">Cast both Series to the same supertype.</param>
    member this.Append(other:Expr,?execUpcast:bool) = 
        let exe = defaultArg execUpcast true
        new Expr(PolarsWrapper.Append(this.CloneHandle(),other.CloneHandle(),exe))
    /// <summary>
    /// Extremely fast method for extending the Series with ‘n’ copies of a value.
    /// </summary>
    /// <param name="value">A constant literal value or a unit expression with which to extend the expression result Series; can pass None to extend with nulls.</param>
    /// <param name="n">The number of additional values that will be added.</param>
    member this.ExtendConstant(value:Expr,n:Expr) =
        new Expr(PolarsWrapper.ExtendConstant(this.CloneHandle(),value.CloneHandle(),n.CloneHandle()))
    /// <summary>
    /// Set values outside the given boundaries to the boundary value.
    /// </summary>
    /// <param name="lowerBound">Lower bound. Accepts expression input.</param>
    /// <param name="upperBound">Upper bound. Accepts expression input.</param>
    member this.Clip(?lowerBound: Expr, ?upperBound: Expr) =
            match lowerBound, upperBound with
            | None, None ->
                let msg = "At least one of 'lowerBound' or 'upperBound' must be provided."
                raise (ArgumentException(msg))

            | Some lower, None ->
                // Only lower bound is provided: ClipMin
                let hMin = PolarsWrapper.ClipMin(this.Handle, lower.Handle)
                new Expr(hMin)

            | None, Some upper ->
                // Only upper bound is provided: ClipMax
                let hMax = PolarsWrapper.ClipMax(this.Handle, upper.Handle)
                new Expr(hMax)

            | Some lower, Some upper ->
                // Both bounds are provided: Full Clip
                let hFull = PolarsWrapper.Clip(this.Handle, lower.Handle, upper.Handle)
                new Expr(hFull)
    /// <summary>
    /// Returns the first non-null value between this expression and other expressions.
    /// Syntactic sugar for <c>Pl.Coalesce(this, others)</c>.
    /// </summary>
    /// <param name="others">Fallback expressions, column names, or literals.</param>
    /// <returns>A new coalesced expression.</returns>
    member this.Coalesce(others: seq<Expr>) =
        if box others = null then
            this
        else
            let othersArr = Seq.toArray others
            if othersArr.Length = 0 then
                this
            else
                // Allocate a single unified array for all Expression Handles
                // Total size is current expression (1) + fallback expressions (othersArr.Length)
                let totalCount = othersArr.Length + 1
                let handles = Array.zeroCreate totalCount

                // Populate the first slot with the current expression's handle
                handles.[0] <- this.Handle

                // Extract handles from the remaining fallback expressions
                for i = 0 to othersArr.Length - 1 do
                    handles.[i + 1] <- othersArr.[i].Handle

                let newHandle = PolarsWrapper.Coalesce(handles)
                new Expr(newHandle)
    static member internal Ternary(predicate:Expr,truthy:Expr,falsy:Expr) = 
        new Expr(PolarsWrapper.IfElse(predicate.CloneHandle(),truthy.CloneHandle(),falsy.CloneHandle()))
    /// <summary>
    /// Indicate that the expression is sorted.
    /// This is a hint to the optimizer and does not actually sort the data.
    /// </summary>
    /// <param name="descending">Whether the column is sorted in descending order.</param>
    /// <param name="nullsLast">Whether null values appear last. (Placeholder for Polars 0.54+)</param>
    /// <returns>A new expression with the sorted flag set.</returns>
    member this.SetSorted(?descending,?nullsLast) = 
        let des = defaultArg descending false
        let nul = defaultArg nullsLast false
        new Expr(PolarsWrapper.ExprSetSorted(this.CloneHandle(),des,nul))
    /// <summary>
    /// Reshape the column into a multi-dimensional array.
    /// </summary>
    /// <param name="dimensions">Tuple of the dimension sizes. If a -1 is used, that dimension is inferred.</param>
    member this.Reshape(dimensions:seq<int64>) =
        let dim = dimensions |> Seq.toArray
        let dimSpan = ReadOnlySpan<int64> dim
        new Expr(PolarsWrapper.ExprReshape(this.CloneHandle(),dimSpan))
    member this.Slice(offset:int64,?length:uint64) =
        let len = 
            match length with
            | Some l -> PolarsWrapper.Lit l
            | None -> PolarsWrapper.Lit UInt64.MaxValue
        new Expr(PolarsWrapper.ExprSlice(this.CloneHandle(),PolarsWrapper.Lit offset,len))
    /// <summary>
    /// Print the value that this expression evaluates to and pass on the value.
    /// </summary>
    member this.Inspect(?format) =
        let f = defaultArg format "{}"
        new Expr(PolarsWrapper.ExprInspect(this.CloneHandle(),f))
    /// <summary>
    /// Reinterpret the underlying bits as a signed/unsigned integer.
    /// This operation is only allowed for numeric types of the same size. For lower bits numbers, you can safely use the cast operation.
    /// </summary>
    /// <param name="signed">If True, reinterpret as signed integer. Otherwise, reinterpret as unsigned integer.</param>
    member this.Reinterpret(?signed) =
        let s = defaultArg signed true
        new Expr(PolarsWrapper.ExprReinterpret(this.CloneHandle(),s))
    /// <summary>
    /// Repeat the elements in this Series as specified in the given expression.
    /// The repeated elements are expanded into a List.
    /// </summary>
    /// <param name="by">Numeric column that determines how often the values will be repeated. The column will be coerced to UInt32. Give this dtype to make the coercion a no-op.</param>
    /// <returns>Expression/Series of data type List, where the inner data type is equal to the original data type.</returns>
    member this.RepeatBy(by:Expr) =
        new Expr(PolarsWrapper.ExprRepeatBy(this.CloneHandle(),by.CloneHandle()))
    /// <summary>
    /// Apply a user-defined function to this expression, returning the result of the function.
    /// This allows encapsulating a sequence of Polars expression operations into a reusable function.
    /// </summary>
    /// <typeparam name="T">The return type of the function.</typeparam>
    /// <param name="func">
    /// A function that receives the current expression and returns a value of type <typeparamref name="T"/>.
    /// Typically this function wraps several Polars API calls that operate on the given expression.
    /// </param>
    /// <returns>The result of applying <paramref name="func"/> to this expression.</returns>
    member this.Pipe(func: Expr -> 'T) = func this
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

    override this.ToString() =
        if (this.Handle.IsInvalid) then 
            "Selector (Disposed)"
        else PolarsWrapper.SelectorToString(this.Handle);
    

    // ==========================================
    // Methods
    // ==========================================

    static member internal ByName([<ParamArray>]columns: string array) =
        new Selector(PolarsWrapper.SelectorCols columns)
    static member internal ByDtype(dtype:DataType) = 
        new Selector(PolarsWrapper.SelectorByDtype(dtype.ToPlDataType()))
    static member internal ByIndex(indices:ReadOnlySpan<int64>,strict:bool)=
        new Selector(PolarsWrapper.SelectorByIndex(indices, strict))   
    /// <summary> Exclude columns from a wildcard selection (col("*")). </summary>
    member this.Exclude(names: seq<string>) =
        let arr = Seq.toArray names
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))
    /// <summary>
    /// Exclude columns matching any of the specified Selectors.
    /// </summary>
    member this.Exclude(selectors: seq<Selector>) =
        match selectors with
        | s when Seq.isEmpty s -> this
        | s -> this - (s |> Seq.reduce (fun a b -> a ||| b))

    /// <summary>
    /// Exclude columns matching any of the specified Data Types.
    /// </summary>
    member this.Exclude(dtypes: seq<DataType>) =
        let toSelector (dt: DataType) =
            new Selector(PolarsWrapper.SelectorByDtype(enum<PlDataType> dt.Code))
        match dtypes with
        | d when Seq.isEmpty d -> this
        | d -> this - (d |> Seq.map toSelector |> Seq.reduce (fun a b -> a ||| b))
    /// <summary>
    /// Convert the Selector to an Expression.
    /// Selectors are essentially dynamic Expressions that expand to column names.
    /// </summary>
    member this.ToExpr() =
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))
    static member Float() = new Selector(PolarsWrapper.SelectorFloat());
    interface IColumnExpr with
        member this.ToExprs() = [this.ToExpr()]
        member this.ToSelector() = this

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
