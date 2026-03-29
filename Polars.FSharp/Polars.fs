namespace Polars.FSharp

open System
open System.ComponentModel
open Polars.NET.Core
open System.Threading.Tasks
open Polars.NET.Core.Helpers

[<EditorBrowsable(EditorBrowsableState.Never)>]
type LitMechanism = LitMechanism with
    static member ($) (LitMechanism, v: int8) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint8) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int16) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint16) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int64) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint64) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: Int128) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: Half) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: float32) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateTime) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateOnly) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: TimeOnly) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateTimeOffset) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: TimeSpan) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: decimal) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: bool) = new Expr(PolarsWrapper.Lit v)
    // --- List ---
    static member ($) (LitMechanism, v: int list)      = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: int64 list)    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: Half list)     = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float list)    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle)) // double
    static member ($) (LitMechanism, v: float32 list)  = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: string list)   = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: bool list)     = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: DateTime list) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: DateOnly list) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))

    // --- Array (High Performance) ---
    static member ($) (LitMechanism, v: int[])      = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
     static member ($) (LitMechanism, v: Half[])    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: int64[])    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float[])    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float32[])  = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: string[])   = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: bool[])     = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: DateTime[]) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))

    // --- Nullable List (Option) ---
    static member ($) (LitMechanism, v: int option list)    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float option list)  = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: string option list) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))

    
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
        Expr.Cols names
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
        df.GroupBy (keys,aggs)
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
    /// <summary> Get unique values (stable). </summary>
    let inline uniqueStable (e: Expr) = e.UniqueStable()
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
    let selectLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        lf.Select exprs
    /// <summary> Sort (Order By) the LazyFrame. </summary>
    let sortLazy (expr: Expr) (desc: bool) (lf: LazyFrame) : LazyFrame =
        lf.Sort (expr,desc)
    /// <summary> Alias for sortLazy </summary>
    let orderByLazy (expr: Expr) (desc: bool) (lf: LazyFrame) = sortLazy expr desc lf
    /// <summary> Limit the number of rows in the LazyFrame. </summary>
    let limit (n: uint) (lf: LazyFrame) : LazyFrame =
        lf.Limit n
    /// <summary> Add or replace columns in the LazyFrame. </summary>
    let withColumnLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        lf.WithColumn expr
    /// <summary> Add or replace multiple columns in the LazyFrame. </summary>
    let withColumnsLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        lf.WithColumns exprs
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupByLazy (keys: Expr list) (aggs: Expr list) (lf: LazyFrame) : LazyFrame =
        lf.GroupBy(keys, aggs)
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
        LazyFrame.Concat lfs how
    /// <summary> Collect LazyFrame into DataFrame (Streaming execution). </summary>
    let collectStreaming (lf: LazyFrame) : DataFrame =
        lf.CollectStreaming()
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
                Task.Run(fun () -> PolarsWrapper.LazyCollect(lfClone,true)) 
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
    // ==========================================
    // Column Selectors (pl.cs)
    // ==========================================
    module cs =

        /// <summary>
        /// Select a single column by name.
        /// </summary>
        let inline col (name: string) =
            new Selector(PolarsWrapper.SelectorCols [| name |])
        
        /// <summary>
        /// Select multiple columns by their names.
        /// Accepts any sequence (list, array, etc.) of strings.
        /// </summary>
        let inline cols (names: seq<string>) = 
            Selector.Cols names
        
        /// <summary> Select all columns. </summary>
        let inline all () = 
            new Selector(PolarsWrapper.SelectorAll())
        
        /// <summary> Select numeric columns (Int, Float, Decimal). </summary>
        let inline numeric () = 
            new Selector(PolarsWrapper.SelectorNumeric())
        
        /// <summary> Select columns by DataType. </summary>
        let inline byType (dt: DataType) = 
            let code = dt.Code
            let kind = enum<PlDataType> code
            
            new Selector(PolarsWrapper.SelectorByDtype kind)
        
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
        
        /// <summary> Invert a selector. </summary>
        let not (s: Selector) = 
            new Selector(PolarsWrapper.SelectorNot(s.CloneHandle()))

        /// <summary> Select all list columns. </summary>
        let list () = 
            let dummy = DataType.List DataType.Null
            byType dummy
            
        /// <summary> Select all struct columns. </summary>
        let struct_ () = 
            let dummy = DataType.Struct []
            byType dummy

    /// <summary> Accumulate over multiple columns horizontally/row-wise. </summary>
    let fold (f: Expr -> Expr -> Expr) (acc: Expr) (exprs: seq<Expr>) : Expr =
        Seq.fold f acc exprs

    /// <summary> Reduce multiple columns horizontally/row-wise. </summary>
    let reduce (f: Expr -> Expr -> Expr) (exprs: seq<Expr>) : Expr =
        Seq.reduce f exprs

    // ==========================================
    // Public API
    // ==========================================

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
        let h = PolarsWrapper.SeriesToFrame s.Handle
        use df = new DataFrame(h)
        df.Show()
        s

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
    let inline (.%) (s: Series) (i: int) : 'T = s.GetValue<'T>(int64 i)