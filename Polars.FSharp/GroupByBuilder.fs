namespace Polars.FSharp

open System
open Polars.NET.Core

/// <summary>
/// Intermediate builder for LazyGroupBy operations.
/// Holds the LazyFrame handle (ownership transferred to this builder) and grouping keys.
/// </summary>
[<Sealed>]
type LazyGroupBy internal (lfHandle: LazyFrameHandle, keys: Expr[]) =
    
    let mutable disposed = false
    let mutable havingExpr: Expr option = None
    
    let ownedKeyHandles = 
        keys |> Array.map (fun k -> PolarsWrapper.CloneExpr k.Handle)

    member this.Dispose() =
        if not disposed then
            for h in ownedKeyHandles do
                if not (isNull (box h)) && not h.IsInvalid then 
                    h.Dispose()
            
            if not lfHandle.IsClosed then 
                lfHandle.Dispose()
            
            disposed <- true
            
        GC.SuppressFinalize this

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    // --------------------------------------------------------
    // Constructor
    // --------------------------------------------------------
    
    /// <summary>
    /// Filter groups with a list of predicates after aggregation.
    /// Using this method is equivalent to adding the predicates to the aggregation and filtering afterwards.
    /// </summary>
    member this.Having(predicate: Expr) : LazyGroupBy =
        havingExpr <- Some predicate
        this 

    /// <summary>
    /// Apply aggregations to the group.
    /// </summary>
    member this.Agg(aggs: seq<Expr>) : LazyFrame =
        if disposed then raise (ObjectDisposedException "LazyGroupBy")

        let aggArray = Seq.toArray aggs
        let aggHandles = aggArray |> Array.map (fun a -> PolarsWrapper.CloneExpr a.Handle)
        let keysForRust = ownedKeyHandles |> Array.map (fun k -> PolarsWrapper.CloneExpr k)

        let havingHandle = 
            match havingExpr with
            | Some expr -> PolarsWrapper.CloneExpr(expr.Handle)
            | None -> null
            
        let resHandle = PolarsWrapper.LazyGroupByAgg(lfHandle, keysForRust, aggHandles, havingHandle)
        new LazyFrame(resHandle)

    member this.Agg([<ParamArray>] aggs: Expr[]) : LazyFrame =
        this.Agg(aggs :> seq<Expr>)

    // --------------------------------------------------------
    // Syntactic Sugar
    // --------------------------------------------------------

    /// <summary> Count the number of values in each group. </summary>
    member this.Count() = this.Agg [ Expr.All().Count() ]

    /// <summary> Aggregate all columns into lists. </summary>
    member this.All() = this.Agg [ Expr.All() ]

    /// <summary> Aggregate the first values in the group. </summary>
    member this.First(?ignoreNulls: bool) =
        let ig = defaultArg ignoreNulls false
        this.Agg [ Expr.All().First ig ]

    /// <summary> Aggregate the last values in the group. </summary>
    member this.Last(?ignoreNulls: bool) =
        let ig = defaultArg ignoreNulls false
        this.Agg [ Expr.All().Last ig ]

    /// <summary> Get the first n rows of each group. </summary>
    member this.Head(?n: int) =
        let count = defaultArg n 10
        let aggregated = this.Agg [ Expr.All().Head count ]
        
        let keyNames = 
            keys 
            |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
            
        aggregated.Explode(Expr.All().ToSelector().Exclude keyNames )

    /// <summary> Get the last n rows of each group. </summary>
    member this.Tail(?n: int) =
        let count = defaultArg n 10
        let aggregated = this.Agg [ Expr.All().Tail(count) ]
        
        let keyNames = 
            keys 
            |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
            
        aggregated.Explode(Expr.All().ToSelector().Exclude keyNames )

    /// <summary> Return the number of rows in each group. </summary>
    member this.Len(?name: string) =
        let colName = defaultArg name "len"
        this.Agg [ Expr.Len().Alias colName ]

    /// <summary> Reduce the groups to the maximal value. </summary>
    member this.Max() = this.Agg [ Expr.All().Max() ]

    /// <summary> Reduce the groups to the minimal value. </summary>
    member this.Min() = this.Agg [ Expr.All().Min() ]

    /// <summary> Reduce the groups to the median value. </summary>
    member this.Median() = this.Agg [ Expr.All().Median() ]

    /// <summary> Reduce the groups to the mean value. </summary>
    member this.Mean() = this.Agg [ Expr.All().Mean() ]

    /// <summary> Count the unique values per group. </summary>
    member this.NUnique() = this.Agg [ Expr.All().NUnique() ]

    /// <summary> Reduce the groups to the sum. </summary>
    member this.Sum() = this.Agg [ Expr.All().Sum() ]

    /// <summary> Compute the quantile per group. </summary>
    member this.Quantile(quantile: float, ?interpolation: QuantileMethod) =
        let interp = defaultArg interpolation QuantileMethod.Linear
        this.Agg [ Expr.All().Quantile(quantile, interp) ]

[<AutoOpen>]
module LazyGroupByExtensions =

    type LazyFrame with

        /// <summary>
        /// Start a GroupBy operation. Returns a builder to apply syntax sugar aggregations (like .Sum(), .Head()).
        /// </summary>
        member this.GroupBy(keys: seq<Expr>, ?having: Expr) : LazyGroupBy =
            let builder = new LazyGroupBy(this.CloneHandle(), Seq.toArray keys)
            match having with
            | Some h -> builder.Having h 
            | None -> builder

        /// <summary>
        /// Start a GroupBy operation using Selectors/IColumnExpr. 
        /// </summary>
        member this.GroupBy(keys: seq<#IColumnExpr>, ?having: Expr) : LazyGroupBy =
            let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
            let builder = new LazyGroupBy(this.CloneHandle(), kExprs)
            match having with
            | Some h -> builder.Having(h)
            | None -> builder

        member this.GroupBy([<ParamArray>] keys: Expr[]) : LazyGroupBy =
            this.GroupBy(keys :> seq<Expr>)

        /// <summary>
        /// Group by keys and apply aggregate expressions, optionally filtering groups.
        /// </summary>
        member this.GroupBy(keys: seq<Expr>, aggs: seq<Expr>, ?having: Expr) : LazyFrame =
            use builder = this.GroupBy(keys, ?having = having)
            builder.Agg aggs

        /// <summary>
        /// Group by keys and apply aggregations (Supports Selectors), optionally filtering groups.
        /// </summary>
        member this.GroupBy(keys: seq<#IColumnExpr>, aggs: seq<#IColumnExpr>, ?having: Expr) : LazyFrame =
            let aExprs = aggs |> Seq.collect (fun x -> x.ToExprs())
            use builder = this.GroupBy(keys, ?having = having)
            builder.Agg aExprs

/// <summary>
/// Intermediate builder for eager GroupBy operations.
/// Under the hood, this routes through the Lazy engine to maximize performance.
/// </summary>
type GroupBy internal (df: DataFrame, keys: Expr[]) =

    let mutable disposed = false
    
    let mutable havingExpr: Expr option = None

    let ownedKeyHandles = 
        keys |> Array.map (fun k -> PolarsWrapper.CloneExpr(k.Handle))

    /// <summary> Filter groups with a predicate after aggregation. </summary>
    member this.Having(predicate: Expr) =
        havingExpr <- Some predicate
        this

    member this.Dispose() =
        if not disposed then
            for h in ownedKeyHandles do
                if not (isNull (box h)) && not h.IsInvalid then 
                    h.Dispose()
        
            disposed <- true
            
        GC.SuppressFinalize(this)

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    member this.Agg(aggs: seq<Expr>) : DataFrame =
        if disposed then raise (ObjectDisposedException "GroupBy Builder")

        let safeKeys = ownedKeyHandles |> Array.map (fun h -> new Expr(PolarsWrapper.CloneExpr h))
        
        let lazyBuilder = df.Lazy().GroupBy(safeKeys)
        
        let finalBuilder = 
            match havingExpr with
            | Some h -> lazyBuilder.Having(h)
            | None -> lazyBuilder
            
        use lazyResult = finalBuilder.Agg aggs
        
        for k in safeKeys do (k :> IDisposable).Dispose()

        lazyResult.Collect()

    member this.Agg([<ParamArray>] aggs: Expr[]) : DataFrame =
        this.Agg(aggs :> seq<Expr>)

    member this.Count() = this.Agg [ Expr.All().Count() ]
    member this.All() = this.Agg [ Expr.All() ]
    
    member this.First(?ignoreNulls: bool) =
        let ig = defaultArg ignoreNulls false
        this.Agg [ Expr.All().First ig ]
        
    member this.Last(?ignoreNulls: bool) =
        let ig = defaultArg ignoreNulls false
        this.Agg [ Expr.All().Last ig ]
        
    member this.Len(?name: string) =
        let colName = defaultArg name "len"
        this.Agg [ Expr.Len().Alias colName ]

    member this.Max() = this.Agg [ Expr.All().Max() ]
    member this.Min() = this.Agg [ Expr.All().Min() ]
    member this.Median() = this.Agg [ Expr.All().Median() ]
    member this.Mean() = this.Agg [ Expr.All().Mean() ]
    member this.NUnique() = this.Agg [ Expr.All().NUnique() ]
    member this.Sum() = this.Agg [ Expr.All().Sum() ]

    member this.Quantile(quantile: float, ?interpolation: QuantileMethod) =
        let interp = defaultArg interpolation QuantileMethod.Linear
        this.Agg [ Expr.All().Quantile(quantile, interp) ]

    member this.Head(?n: int) =
        let count = defaultArg n 10
        let aggregatedDf = this.Agg [ Expr.All().Head count ]
        
        let keyNames = 
            keys |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
        aggregatedDf.Explode(Expr.All().ToSelector().Exclude keyNames)

    member this.Tail(?n: int) =
        let count = defaultArg n 10
        let aggregatedDf = this.Agg [ Expr.All().Tail count ]
        let keyNames = 
            keys |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
        aggregatedDf.Explode(Expr.All().ToSelector().Exclude keyNames)

[<AutoOpen>]
module DataFrameGroupByExtensions =

    type DataFrame with
        
        /// <summary> Start a GroupBy operation. </summary>
        member this.GroupBy(keys: seq<Expr>) =
            new GroupBy(this, Seq.toArray keys)

        /// <summary> Start a GroupBy operation with Selectors. </summary>
        member this.GroupBy(keys: seq<#IColumnExpr>) =
            let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
            new GroupBy(this, kExprs)

        member this.GroupBy([<ParamArray>] keys: Expr[]) =
            new GroupBy(this, keys)

        /// <summary> Group by keys and apply aggregate expressions. </summary>
        member this.GroupBy(keys: seq<Expr>, aggs: seq<Expr>, ?having: Expr) : DataFrame =
            let builder = this.GroupBy keys
            match having with
            | Some h -> builder.Having(h).Agg aggs
            | None -> builder.Agg aggs
        
        member this.GroupBy(keys: Expr, aggs: seq<Expr>, ?having: Expr) : DataFrame =
            this.GroupBy([keys], aggs, ?having = having)

        member this.GroupBy(keys: seq<Expr>, aggs: Expr, ?having: Expr) : DataFrame =
            this.GroupBy(keys, [aggs], ?having = having)

        member this.GroupBy(keys: Expr, aggs: Expr, ?having: Expr) : DataFrame =
            this.GroupBy([keys], [aggs], ?having = having)