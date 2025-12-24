namespace Polars.FSharp

open System
open System.ComponentModel
open Polars.NET.Core
open System.Threading.Tasks

// =========================================================================
// 1. 核心实现层 (Internal / Private Implementation)
//    这里放所有的逻辑，但不对外直接暴露，防止命名污染
// =========================================================================
[<EditorBrowsable(EditorBrowsableState.Never)>]
type LitMechanism = LitMechanism with
    static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateTime) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: bool) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: float32) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int64) = new Expr(PolarsWrapper.Lit v)
module pl =

    // --- Factories ---
    /// <summary> Reference a column by name. </summary>
    let col (name: string) = new Expr(PolarsWrapper.Col name)
    /// <summary> Select multiple columns (returns a Wildcard Expression). </summary>
    let cols (names: string list) =
        let arr = List.toArray names
        new Expr(PolarsWrapper.Cols arr)
    /// <summary> Select all columns (returns a Selector). </summary>
    let all () = new Selector(PolarsWrapper.SelectorAll())

    /// <summary> Create a literal expression from a value. </summary>
    let inline lit (value: ^T) : Expr = 
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))
    // --- Expr Helpers ---
    /// <summary> Cast an expression to a different data type. </summary>
    let cast (dtype: DataType) (e: Expr) = e.Cast dtype

    let boolean = DataType.Boolean
    let int32 = DataType.Int32
    let int64 = DataType.Int64
    let float64 = DataType.Float64
    let string = DataType.String
    let Date = DataType.Date
    let Datetime = DataType.Datetime
    let TimeSpan = DataType.Duration
    let Time = DataType.Time

    /// <summary> Count the number of elements in an expression. </summary>
    let count () = new Expr(PolarsWrapper.Len())
    /// Alias for count
    let len = count
    /// <summary> Alias an expression with a new name. </summary>
    let alias (name: string) (expr: Expr) = expr.Alias name
    /// <summary> Collect LazyFrame into DataFrame (Eager execution). </summary>
    let collect (lf: LazyFrame) : DataFrame = 
        let lfClone = lf.CloneHandle()
        let dfHandle = PolarsWrapper.LazyCollect lfClone
        new DataFrame(dfHandle)
    /// <summary> Convert Selector to Expr. </summary>
    let asExpr (s: Selector) = s.ToExpr()
    /// <summary> Exclude columns from Selector. </summary>
    let exclude (names: string list) (s: Selector) = s.Exclude names
    /// <summary> Create a Struct expression from a list of expressions. </summary>
    let asStruct (exprs: Expr list) =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        new Expr(PolarsWrapper.AsStruct handles)
    // --- Eager Ops ---
    /// <summary> Add or replace columns. </summary>
    let withColumn (expr: Expr) (df: DataFrame) : DataFrame =
        df.WithColumn expr
    /// <summary> Add or replace multiple columns. </summary>
    let withColumns (exprs: Expr list) (df: DataFrame) : DataFrame =
        df.WithColumns exprs

    /// <summary> Filter rows based on a boolean expression. </summary>
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        df.Filter expr
    /// <summary> Select columns from DataFrame. </summary>
    let select (exprs: Expr list) (df: DataFrame) : DataFrame =
        df.Select exprs
    /// <summary> Sort (Order By) the DataFrame. </summary>
    let sort (expr: Expr) (desc: bool) (df: DataFrame) : DataFrame =
        df.Sort expr desc 
    let orderBy (expr: Expr) (desc: bool) (df: DataFrame) = sort expr desc df
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupBy (keys: Expr list) (aggs: Expr list) (df: DataFrame) : DataFrame =
        df.GroupBy keys aggs
    /// <summary> Perform a join between two DataFrames. </summary>
    let join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) (left: DataFrame) : DataFrame =
        left.Join other leftOn rightOn how
    /// <summary> Concatenate multiple DataFrames. </summary>
    let concat (dfs: DataFrame list) (how:ConcatType) : DataFrame =
        DataFrame.Concat dfs how
    /// <summary> Get the first n rows of the DataFrame. </summary>
    let head (n: int) (df: DataFrame) : DataFrame =
        df.Head n
    /// <summary> Get the last n rows of the DataFrame. </summary>
    let tail (n: int) (df: DataFrame) : DataFrame =
        df.Tail n
    /// <summary> Explode list-like columns into multiple rows. </summary>
    let explode (exprs: Expr list) (df: DataFrame) : DataFrame =
        df.Explode exprs
    let unnestColumn(column: string) (df:DataFrame) : DataFrame =
        df.UnnestColumn column
    let unnestColumns(columns: string list) (df:DataFrame) : DataFrame =
        df.UnnestColumns columns

    // --- Reshaping (Eager) ---

    /// <summary> Pivot the DataFrame from long to wide format. </summary>
    let pivot (index: string list) (columns: string list) (values: string list) (aggFn: PivotAgg) (df: DataFrame) : DataFrame =
        df.Pivot index columns values aggFn

    /// <summary> Unpivot (Melt) the DataFrame from wide to long format. </summary>
    let unpivot (index: string list) (on: string list) (variableName: string option) (valueName: string option) (df: DataFrame) : DataFrame =
        df.Unpivot index on variableName valueName
    /// Alias for unpivot
    let melt = unpivot    
    /// Aggregation Helpers
    let sum (e: Expr) = e.Sum()
    let mean (e: Expr) = e.Mean()
    let max (e: Expr) = e.Max()
    let min (e: Expr) = e.Min()
    // Fill Helpers
    let fillNull (fillValue: Expr) (e: Expr) = e.FillNull fillValue
    let isNull (e: Expr) = e.IsNull()
    let isNotNull (e: Expr) = e.IsNotNull()
    // Math Helpers
    let abs (e: Expr) = e.Abs()
    let pow (exponent: Expr) (baseExpr: Expr) = baseExpr.Pow exponent
    let sqrt (e: Expr) = e.Sqrt()
    let exp (e: Expr) = e.Exp()

    // --- Lazy API ---

    /// <summary> Explain the LazyFrame execution plan. </summary>
    let explain (lf: LazyFrame) = lf.Explain true
    /// <summary> Explain the unoptimized LazyFrame execution plan. </summary>
    let explainUnoptimized (lf: LazyFrame) = lf.Explain false
    /// <summary> Get the schema of the LazyFrame. </summary>
    let schema (lf: LazyFrame) = lf.Schema
    /// <summary> Filter rows based on a boolean expression. </summary>
    let filterLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)

    /// <summary> Select columns from LazyFrame. </summary>
    let selectLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)

    /// <summary> Sort (Order By) the LazyFrame. </summary>
    let sortLazy (expr: Expr) (desc: bool) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        let h = PolarsWrapper.LazySort(lfClone, exprClone, desc)
        new LazyFrame(h)
    /// <summary> Alias for sortLazy </summary>
    let orderByLazy (expr: Expr) (desc: bool) (lf: LazyFrame) = sortLazy expr desc lf

    /// <summary> Limit the number of rows in the LazyFrame. </summary>
    let limit (n: uint) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)
    /// <summary> Add or replace columns in the LazyFrame. </summary>
    let withColumnLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] // 使用克隆的 handle
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary> Add or replace multiple columns in the LazyFrame. </summary>
    let withColumnsLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary> Group by keys and apply aggregations. </summary>
    let groupByLazy (keys: Expr list) (aggs: Expr list) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle()
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazyGroupByAgg(lfClone, kHandles, aHandles)
        new LazyFrame(h)
    /// <summary> Unpivot (Melt) the LazyFrame from wide to long format. </summary>
    let unpivotLazy (index: string list) (on: string list) (variableName: string option) (valueName: string option) (lf: LazyFrame) : LazyFrame =
        let lfClone = lf.CloneHandle() // 必须 Clone
        let iArr = List.toArray index
        let oArr = List.toArray on
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName 
        new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, iArr, oArr, varN, valN))
    /// Alias for unpivotLazy
    let meltLazy = unpivotLazy
    /// <summary> Perform a join between two LazyFrames. </summary>
    let joinLazy (other: LazyFrame) (leftOn: Expr list) (rightOn: Expr list) (how: JoinType) (lf: LazyFrame) : LazyFrame =
        let lClone = lf.CloneHandle()
        let rClone = other.CloneHandle()
        
        let lOnArr = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rOnArr = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        new LazyFrame(PolarsWrapper.Join(lClone, rClone, lOnArr, rOnArr, how.ToNative()))
    /// <summary> Perform an As-Of Join (time-series join). </summary>
    let joinAsOf (other: LazyFrame) 
                 (leftOn: Expr) (rightOn: Expr) 
                 (byLeft: Expr list) (byRight: Expr list) 
                 (strategy: string option) 
                 (tolerance: string option) 
                 (lf: LazyFrame) : LazyFrame =
        
        let lClone = lf.CloneHandle()
        let rClone = other.CloneHandle()
        
        let lOn = leftOn.CloneHandle()
        let rOn = rightOn.CloneHandle()
        
        // 处理分组列 (Clone List)
        let lByArr = byLeft |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rByArr = byRight |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        // 处理可选参数
        let strat = defaultArg strategy "backward"
        let tol = Option.toObj tolerance // 转为 string 或 null

        let h = PolarsWrapper.JoinAsOf(
            lClone, rClone, 
            lOn, rOn, 
            lByArr, rByArr,
            strat, tol
        )
        new LazyFrame(h)
    /// <summary> Concatenate multiple LazyFrames vertically. </summary>
    let concatLazy (lfs: LazyFrame list) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        // 默认 rechunk=false, parallel=true (Lazy 的常见默认值)
        new LazyFrame(PolarsWrapper.LazyConcat(handles, PlConcatType.Vertical, false, true))
    /// <summary> 
    /// Lazily concatenate multiple LazyFrames horizontally.
    /// Note: Duplicate column names will cause an error during collection.
    /// </summary>
    let concatLazyHorizontal (lfs: LazyFrame list) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, PlConcatType.Horizontal, false, false))
    /// <summary> 
    /// Lazily concatenate multiple LazyFrames diagonally. 
    /// </summary>
    let concatLazyDiagonal (lfs: LazyFrame list) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, PlConcatType.Diagonal, false, true))
    /// <summary> Collect LazyFrame into DataFrame (Streaming execution). </summary>
    let collectStreaming (lf: LazyFrame) : DataFrame =
        let lfClone = lf.CloneHandle()
        new DataFrame(PolarsWrapper.CollectStreaming lfClone)
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
                Task.Run(fun () -> PolarsWrapper.LazyCollect lfClone) 
                |> Async.AwaitTask
                
            return new DataFrame(dfHandle)
        }

    // ==========================================
    // Public API (保持简单，返回 DataFrame 以支持管道)
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
        // 临时转为 DataFrame 打印，最省事
        let h = PolarsWrapper.SeriesToFrame(s.Handle)
        use df = new DataFrame(h)
        df.Show()
        s

// =========================================================================
// 3. AutoOpen 层 (The "Magic" Layer)
//    只暴露最核心、最不会冲突的东西到全局
// =========================================================================
[<AutoOpen>]
module PolarsAutoOpen =

    // A. 暴露核心原子 col 和 lit
    // 允许用户直接写: col("A") .> lit(10)
    let inline col name = pl.col name
    let inline lit value = pl.lit value

    let inline alias column = pl.alias column