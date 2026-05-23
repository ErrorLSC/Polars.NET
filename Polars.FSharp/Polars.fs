#nowarn "3391"
namespace Polars.FSharp

open System
open Polars.NET.Core
open System.Threading.Tasks
open Polars.NET.Core.Helpers
open Polars.NET.Core.Arrow

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

    // --- Factories ---
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
        let arr = Seq.toArray names
        Expr.Col names
    /// <summary>
    /// Select all columns.
    /// Equivalent to `pl.col("*")`.
    /// </summary>
    let all() = Expr.Col "*"

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
    // --- Expr Helpers ---
    /// <summary> Cast an expression to a different data type. </summary>
    let cast (dtype: DataType) (e: Expr) = e.Cast dtype
    let castWithNetType<'T> (e: Expr) = e.Cast<'T>()
    /// <summary> Boolean data type. </summary>
    let boolean = DataType.Boolean
    /// <summary> 32-bit Integer data type. </summary>
    let int32 = DataType.Int32
    /// <summary> 64-bit Integer data type. </summary>
    let int64 = DataType.Int64
    /// <summary> 64-bit Floating point data type. </summary>
    let float64 = DataType.Float64
    /// <summary> String data type (UTF-8). </summary>
    let string = DataType.String
    /// <summary> Date data type (no time). </summary>
    let date = DataType.Date
    /// <summary> Datetime data type. </summary>
    let datetime = DataType.Datetime
    /// <summary> Duration (TimeSpan) data type. </summary>
    let timeSpan = DataType.Duration
    /// <summary> Time data type (no date). </summary>
    let time = DataType.Time
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
    /// <summary> Count the number of elements in an expression. </summary>
    let count() = new Expr(PolarsWrapper.Len())
    /// Alias for count
    let len = count
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
    /// <summary> Convert Selector to Expr. </summary>
    let asExpr (s: Selector) = s.ToExpr()
    /// <summary> Exclude columns from Selector. </summary>
    let exclude (names: string list) (s: Selector) = s.Exclude names
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    let asStruct (exprs: seq<Expr>) =
        let handles = exprs |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        new Expr(PolarsWrapper.AsStruct handles)
    let struct_ = asStruct
    // --- Eager Ops ---
    /// <summary> Add or replace a single column in the DataFrame. </summary>
    let withColumn (expr: Expr) (df: DataFrame) : DataFrame =
        df.WithColumn expr
    /// <summary> Add or replace multiple columns in the DataFrame. </summary>
    let withColumns (exprs: Expr list) (df: DataFrame) : DataFrame =
        df.WithColumns exprs

    /// <summary> Filter rows based on a boolean expression. </summary>
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        df.Filter expr
    /// <summary> Select columns from the DataFrame. </summary>
    let select (exprs: Expr list) (df: DataFrame) : DataFrame =
        df.Select exprs
    /// <summary> Sort (Order By) the DataFrame. </summary>
    let sort (expr: Expr,desc: bool) (df: DataFrame) : DataFrame =
        df.Sort (expr,desc)
    let orderBy (expr: Expr) (desc: bool) (df: DataFrame) = sort(expr,desc) df
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupBy (keys: Expr list) (aggs: Expr list) (df: DataFrame) : DataFrame =
        use builder = df.GroupBy(keys)
        builder.Agg(aggs)
    /// <summary> Perform a join between two DataFrames. </summary>
    let join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) (left: DataFrame) : DataFrame =
        left.Join (other, leftOn, rightOn, how)
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
    /// <summary> Get the first n rows of the DataFrame. </summary>
    let head (n: int) (df: DataFrame) : DataFrame =
        df.Head n
    /// <summary> Get the last n rows of the DataFrame. </summary>
    let tail (n: int) (df: DataFrame) : DataFrame =
        df.Tail n
    /// <summary> Explode list-like columns into multiple rows. </summary>
    let explode (columns: seq<string>) (df: DataFrame) : DataFrame =
        df.Explode columns
    /// <summary> Decompose a struct column into multiple columns. </summary>
    let unnestColumn(column: string) (df:DataFrame) : DataFrame =
        df.UnnestColumn column
    /// <summary> Decompose multiple struct columns. </summary>
    let unnestColumns(columns: string list) (df:DataFrame) : DataFrame =
        df.UnnestColumns columns

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
    /// Aggregation Helpers
    // <summary> Sum aggregation. </summary>
    let sum (e: Expr) = e.Sum()
    // <summary> Mean aggregation. </summary>
    let mean (e: Expr) = e.Mean()
    // <summary> Max aggregation. </summary>
    let max (e: Expr) = e.Max()
    // <summary> Min aggregation. </summary>
    let min (e: Expr) = e.Min()
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
    
    // --- Lazy API ---

    /// <summary> Explain the LazyFrame execution plan. </summary>
    let explain (lf: LazyFrame) = lf.Explain true
    /// <summary> Explain the unoptimized LazyFrame execution plan. </summary>
    let explainUnoptimized (lf: LazyFrame) = lf.Explain false
    /// <summary> Get the schema of the LazyFrame. </summary>
    let schema (lf: LazyFrame) = lf.Schema
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
        lf.WithColumn expr
    /// <summary> Add or replace multiple columns in the LazyFrame. </summary>
    let withColumnsLazy (exprs: seq<Expr>) (lf: LazyFrame) : LazyFrame =
        lf.WithColumns exprs
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupByLazy (keys: seq<Expr>) (aggs: seq<Expr>) (lf: LazyFrame) : LazyFrame =
        use builder = lf.GroupBy(keys)
        builder.Agg(aggs)
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
    let joinLazy (other: LazyFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) (lf: LazyFrame) : LazyFrame =
        lf.Join(other,leftOn, rightOn, how)
    /// <summary> Perform an As-Of Join (time-series join). </summary>
    /// <summary>
    /// Perform an As-Of Join (Lite version).
    /// For full options (int/float tolerance, suffix, validation, etc.), use the member method: df.JoinAsOf(...)
    /// </summary>
    let joinAsOf (other: LazyFrame) 
                 (leftOn: Expr) 
                 (rightOn: Expr) 
                 (byLeft: Expr list) 
                 (byRight: Expr list) 
                 (strategy: AsofStrategy option) 
                 (tolerance: string option)     
                 (lf: LazyFrame) : LazyFrame =
        
        lf.JoinAsOfInternal(
            other, 
            leftOn, 
            rightOn, 
            byLeft = byLeft,
            byRight = byRight,
            ?strategy = strategy,
            ?tolerance = tolerance
        )
    /// <summary> Concatenate multiple LazyFrames. </summary>
    let concatLazy (lfs: LazyFrame list) (how: ConcatType) : LazyFrame =
        LazyFrame.Concat(lfs,how)
    /// <summary> Define a window over which to perform an aggregation. </summary>
    let over (partitionBy: Expr list) (e: Expr) = e.Over partitionBy
    /// <summary> Create a SQL context for executing SQL queries on LazyFrames. </summary>
    let sqlContext () = new SqlContext()
    /// <summary> Execute a SQL query against the provided LazyFrames. </summary>
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
        let exclude([<ParamArray>] selectors:ReadOnlySpan<Selector>) = all().Exclude selectors

        /// <summary>
        /// Select all columns EXCEPT the specified Data Types.
        /// </summary>
        let excludeDtype([<ParamArray>] dtypes:ReadOnlySpan<DataType>) = all().Exclude dtypes
        
        /// <summary> Select columns by DataType. </summary>
        let inline byType (dt: DataType) = 
            let code = dt.Code
            let kind = enum<PlDataType> code
            
            new Selector(PolarsWrapper.SelectorByDtype kind)
        /// <summary> Select columns by .NET System.Type. </summary>
        let byNetType (t: System.Type) =
            let arrowType = ArrowTypeResolver.GetArrowTypeFromNetType t
            let dt = DataType.FromArrowType arrowType
            byType dt

        /// <summary> 
        /// Select columns by Generic Type.
        /// Usage: pl.cs.byType<int option>() or pl.cs.byType<DateTime>()
        /// </summary>
        let inline byGenericType<'T> () =
            byNetType typeof<'T>
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
        let inline struct_() = new Selector(PolarsWrapper.SelectorStruct());
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
        let private getNativeTimeUnit (unit: TimeUnit option) : PlTimeUnit =
            match unit with
            | Some u -> u.ToNative()
            | None -> PlTimeUnit.All

        let private datetimeInternal (timeUnit: TimeUnit option) (tzString: string option) =
            let tu = getNativeTimeUnit timeUnit
            let tz = Option.toObj tzString
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
        /// Select all datetime columns (both with and without timezones).
        /// </summary>
        let datetime (timeUnit: TimeUnit option) =
            datetimeInternal timeUnit None

        /// <summary>
        /// Select ONLY timezone-naive datetime columns (no timezone set).
        /// </summary>
        let datetimeNaive (timeUnit: TimeUnit option) =
            datetimeInternal timeUnit (Some "")

        /// <summary>
        /// Select ONLY timezone-aware datetime columns (any timezone).
        /// </summary>
        let datetimeAware (timeUnit: TimeUnit option) =
            datetimeInternal timeUnit (Some "*")

        /// <summary>
        /// Select datetime columns matching a specific timezone (e.g., "UTC", "Asia/Shanghai").
        /// </summary>
        let datetimeExact (timeZone: string) (timeUnit: TimeUnit option) =
            if System.String.IsNullOrEmpty timeZone then
                invalidArg "timeZone" "timeZone cannot be null or empty"
                
            datetimeInternal timeUnit (Some timeZone)
        /// <summary>
        /// Select all duration columns. Optionally match a specific TimeUnit.
        /// </summary>
        let duration (timeUnit: TimeUnit option) =
            new Selector(PolarsWrapper.SelectorDuration(getNativeTimeUnit timeUnit))
        /// <summary>
        /// Select all columns with alphabetic names.
        /// </summary>
        let alpha (asciiOnly: bool option) (ignoreSpaces: bool option) =
            let isAscii = defaultArg asciiOnly false
            let isIgnoreSpaces = defaultArg ignoreSpaces false
            
            let mutable charClass = if isAscii then "a-zA-Z" else @"\p{L}"
            if isIgnoreSpaces then charClass <- charClass + " "
            
            matches (sprintf "^[%s]+$" charClass)

        /// <summary>
        /// <para>[EN] Select columns whose names consist entirely of CJK scripts (Han, Hiragana, Katakana, Hangul).
        /// The 'chinese' option enables \p{Han}, which also includes Japanese Kanji and Korean Hanja.</para>
        /// <para>[ZH] 选择列名完全由中日韩字符（Han / 平假名 / 片假名 / 韩文）组成的列。
        /// 注意：'chinese' 实际匹配 \p{Han}，包含日文汉字与韩文汉字。</para>
        /// <para>[JA] 列名がCJK文字（漢字・ひらがな・カタカナ・ハングル）のみで構成される列を選択します。
        /// ※ 'chinese' は \p{Han}（日本・韓国の漢字を含む）を有効にします。</para>
        /// <para>[KO] 열 이름이 CJK 문자(한자, 히라가나, 가타카나, 한글)로만 구성된 열을 선택합니다.
        /// ※ 'chinese'는 \p{Han}을 의미하며 일본/한국 한자도 포함합니다.</para>
        /// </summary>
        let cjk (chinese: bool option) (japanese: bool option) (korean: bool option) (ignoreSpaces: bool option) =
            let isChinese = defaultArg chinese true
            let isJapanese = defaultArg japanese true
            let isKorean = defaultArg korean true
            let isIgnoreSpaces = defaultArg ignoreSpaces false

            if not isChinese && not isJapanese && not isKorean then
                invalidArg "CJK" "At least one CJK script must be enabled."

            let mutable charClass = ""
            if isChinese then charClass <- charClass + @"\p{Han}"
            if isJapanese then charClass <- charClass + @"\p{Hiragana}\p{Katakana}"
            if isKorean then charClass <- charClass + @"\p{Hangul}"
            if isIgnoreSpaces then charClass <- charClass + " "

            matches (sprintf "^[%s]+$" charClass)

        /// <summary>
        /// <para>[EN] Select columns whose names consist of CJK scripts, Unicode digits (\p{N}),
        /// and optionally ASCII/full-width Latin letters.</para>
        /// <para>[ZH] 选择列名由中日韩字符、数字（\p{N}，含全/半角）以及可选英文字母（全/半角）组成的列。</para>
        /// <para>[JA] 列名がCJK文字・数字（\p{N}、全角/半角）および英字（全角/半角）で構成される列を選択します。</para>
        /// <para>[KO] 열 이름이 CJK 문자, 숫자(\p{N}, 전각/반각) 및 영문자(전각/반각)로 구성된 열을 선택합니다.</para>
        /// </summary>
        let cjkAlphanumeric (chinese: bool option) (japanese: bool option) (korean: bool option) (includeLetters: bool option) (ignoreSpaces: bool option) =
            let isChinese = defaultArg chinese true
            let isJapanese = defaultArg japanese true
            let isKorean = defaultArg korean true
            let isIncludeLetters = defaultArg includeLetters true
            let isIgnoreSpaces = defaultArg ignoreSpaces false

            if not isChinese && not isJapanese && not isKorean then
                invalidArg "CJKAlphanumeric" "At least one CJK script must be enabled."

            let mutable charClass = @"\p{N}"
            if isIncludeLetters then charClass <- charClass + "a-zA-ZＡ-Ｚａ-ｚ"
            if isChinese then charClass <- charClass + @"\p{Han}"
            if isJapanese then charClass <- charClass + @"\p{Hiragana}\p{Katakana}"
            if isKorean then charClass <- charClass + @"\p{Hangul}"
            if isIgnoreSpaces then charClass <- charClass + " "

            matches (sprintf "^[%s]+$" charClass)

        /// <summary>
        /// Select all columns with alphanumeric names.
        /// </summary>
        let alphanumeric (asciiOnly: bool option) (ignoreSpaces: bool option) =
            let isAscii = defaultArg asciiOnly false
            let isIgnoreSpaces = defaultArg ignoreSpaces false
            
            let mutable charClass = if isAscii then "a-zA-Z0-9" else @"\p{L}\p{N}"
            if isIgnoreSpaces then charClass <- charClass + " "

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