namespace Polars.FSharp

open System
open Polars.NET.Core

/// <summary>
/// Intermediate builder for LazyGroupBy operations.
/// Holds the LazyFrame handle (ownership transferred to this builder) and grouping keys.
/// </summary>
[<Sealed>]
type LazyGroupBy internal (lfHandle: LazyFrameHandle, keys: Expr[]) =
    
    // 1. 内部可变状态
    let mutable disposed = false
    let mutable havingExpr: Expr option = None
    
    // 2. 构造时立刻克隆句柄，保留 keys 的 C# 对象引用
    let ownedKeyHandles = 
        keys |> Array.map (fun k -> PolarsWrapper.CloneExpr(k.Handle))

    // --------------------------------------------------------
    // 生命周期与清理
    // --------------------------------------------------------
    member this.Dispose() =
        if not disposed then
            for h in ownedKeyHandles do
                // 在 F# 中判断 null 和失效
                if not (isNull (box h)) && not h.IsInvalid then 
                    h.Dispose()
            
            if not lfHandle.IsClosed then 
                lfHandle.Dispose()
            
            disposed <- true
            
        GC.SuppressFinalize(this)

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    // --------------------------------------------------------
    // 核心构建器方法
    // --------------------------------------------------------
    
    /// <summary>
    /// Filter groups with a list of predicates after aggregation.
    /// Using this method is equivalent to adding the predicates to the aggregation and filtering afterwards.
    /// </summary>
    member this.Having(predicate: Expr) : LazyGroupBy =
        havingExpr <- Some predicate
        this // 支持链式调用

    /// <summary>
    /// Apply aggregations to the group.
    /// </summary>
    member this.Agg(aggs: seq<Expr>) : LazyFrame =
        if disposed then raise (ObjectDisposedException("LazyGroupBy"))

        let aggArray = Seq.toArray aggs
        let aggHandles = aggArray |> Array.map (fun a -> PolarsWrapper.CloneExpr(a.Handle))
        let keysForRust = ownedKeyHandles |> Array.map (fun k -> PolarsWrapper.CloneExpr(k))

        let havingHandle = 
            match havingExpr with
            | Some expr -> PolarsWrapper.CloneExpr(expr.Handle)
            | None -> null
            
        let resHandle = PolarsWrapper.LazyGroupByAgg(lfHandle, keysForRust, aggHandles, havingHandle)
        new LazyFrame(resHandle)

    // 提供一个 ParamArray 的重载，方便非 seq 的直接调用（兼容性考虑）
    member this.Agg([<ParamArray>] aggs: Expr[]) : LazyFrame =
        this.Agg(aggs :> seq<Expr>)

    // --------------------------------------------------------
    // 语法糖 (Syntactic Sugar)
    // --------------------------------------------------------

    /// <summary> Count the number of values in each group. </summary>
    member this.Count() = this.Agg([ Expr.All().Count() ])

    /// <summary> Aggregate all columns into lists. </summary>
    member this.All() = this.Agg([ Expr.All() ])

    /// <summary> Aggregate the first values in the group. </summary>
    member this.First(?ignoreNulls: bool) =
        let ig = defaultArg ignoreNulls false
        this.Agg([ Expr.All().First(ig) ])

    /// <summary> Aggregate the last values in the group. </summary>
    member this.Last(?ignoreNulls: bool) =
        let ig = defaultArg ignoreNulls false
        this.Agg([ Expr.All().Last(ig) ])

    /// <summary> Get the first n rows of each group. </summary>
    member this.Head(?n: int) =
        let count = defaultArg n 10
        let aggregated = this.Agg([ Expr.All().Head(count) ])
        
        let keyNames = 
            keys 
            |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
            
        // 注意：假设你在 F# 中也有一套 Selectors (比如 Selectors.All() 或 Expr.All())
        // 如果你 F# 的 API 是用 Expr 借道，就写 Expr.All().Exclude(keyNames)
        aggregated.Explode([ Expr.All().Exclude(keyNames) ])

    /// <summary> Get the last n rows of each group. </summary>
    member this.Tail(?n: int) =
        let count = defaultArg n 10
        let aggregated = this.Agg([ Expr.All().Tail(count) ])
        
        let keyNames = 
            keys 
            |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
            
        aggregated.Explode([ Expr.All().Exclude(keyNames) ])

    /// <summary> Return the number of rows in each group. </summary>
    member this.Len(?name: string) =
        let colName = defaultArg name "len"
        this.Agg([ Expr.Len().Alias(colName) ])

    /// <summary> Reduce the groups to the maximal value. </summary>
    member this.Max() = this.Agg([ Expr.All().Max() ])

    /// <summary> Reduce the groups to the minimal value. </summary>
    member this.Min() = this.Agg([ Expr.All().Min() ])

    /// <summary> Reduce the groups to the median value. </summary>
    member this.Median() = this.Agg([ Expr.All().Median() ])

    /// <summary> Reduce the groups to the mean value. </summary>
    member this.Mean() = this.Agg([ Expr.All().Mean() ])

    /// <summary> Count the unique values per group. </summary>
    member this.NUnique() = this.Agg([ Expr.All().NUnique() ])

    /// <summary> Reduce the groups to the sum. </summary>
    member this.Sum() = this.Agg([ Expr.All().Sum() ])

    /// <summary> Compute the quantile per group. </summary>
    member this.Quantile(quantile: float, ?interpolation: QuantileMethod) =
        let interp = defaultArg interpolation QuantileMethod.Linear
        this.Agg([ Expr.All().Quantile(quantile, interp) ])