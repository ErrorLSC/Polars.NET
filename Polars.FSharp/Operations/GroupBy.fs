namespace Polars.FSharp

open System
open Polars.NET.Core

/// <summary>
/// Defines the type of GroupBy operation and stores specific parameters for each strategy.
/// </summary>
type internal GroupByType =
    | Standard of maintainOrder: bool
    | Dynamic of 
        indexColumn: string * every: string * period: string * offset: string * label: Label * includeBoundaries: bool * closedInterval: ClosedWindow * startBy: StartBy
    | Rolling of 
        indexColumn: string * period: string * offset: string * closedInterval: ClosedWindow

/// <summary>
/// A unified builder for Standard, Dynamic, and Rolling GroupBy operations.
/// Holds the LazyFrame handle (ownership transferred to this builder) and grouping keys.
/// </summary>
[<Sealed>]
type LazyGroupBy internal (lfHandle: LazyFrameHandle, groupByType: GroupByType, keys: Expr[]) =
    
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
    // Builder Core Methods
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
    /// Routes the operation to the appropriate PolarsWrapper native call based on GroupByType.
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
            
        // Clone the original lazy frame handle for the native call just like C# wrapper does
        let clonedLf = PolarsWrapper.LazyClone(lfHandle)

        let resHandle = 
            match groupByType with
            | Standard maintainOrder ->
                PolarsWrapper.LazyGroupByAgg(
                    clonedLf, keysForRust, aggHandles, havingHandle, maintainOrder)
                    
            | Dynamic (indexCol, every, period, offset, label, incBoundaries, closedInt, startBy) ->
                PolarsWrapper.LazyGroupByDynamic(
                    clonedLf, indexCol, every, period, offset, 
                    label.ToNative(), incBoundaries, closedInt.ToNative(), startBy.ToNative(), 
                    keysForRust, aggHandles, havingHandle)
                    
            | Rolling (indexCol, period, offset, closedInt) ->
                PolarsWrapper.LazyGroupByRolling(
                    clonedLf, indexCol, period, offset, closedInt.ToNative(), 
                    keysForRust, aggHandles, havingHandle)

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

    // Helper to get excluded key names for Explode in Head/Tail
    member private this.GetExcludedKeyNames() =
        let baseKeys = 
            keys 
            |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            )
            |> Array.toList
            
        // If it's a Dynamic or Rolling GroupBy, we must also exclude the indexColumn
        let allKeys =
            match groupByType with
            | Dynamic (indexCol, _, _, _, _, _, _, _) -> indexCol :: baseKeys
            | Rolling (indexCol, _, _, _) -> indexCol :: baseKeys
            | Standard _ -> baseKeys
            
        allKeys |> List.toArray

    /// <summary> Get the first n rows of each group. </summary>
    member this.Head(?n: int) =
        let count = defaultArg n 10
        let aggregated = this.Agg [ Expr.All().Head count ]
        let keyNames = this.GetExcludedKeyNames()
        aggregated.Explode(Expr.All().ToSelector().Exclude keyNames)

    /// <summary> Get the last n rows of each group. </summary>
    member this.Tail(?n: int) =
        let count = defaultArg n 10
        let aggregated = this.Agg [ Expr.All().Tail count ]
        let keyNames = this.GetExcludedKeyNames()
        aggregated.Explode(Expr.All().ToSelector().Exclude keyNames)

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

/// <summary>
/// Intermediate builder for eager GroupBy operations.
/// Under the hood, this routes through the Lazy engine to maximize performance.
/// </summary>
type GroupBy internal (df: DataFrame, groupByType: GroupByType, keys: Expr[]) =

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
        
        // Key feature: Route the eager GroupByType to the lazy engine builder
        let lazyBuilder = new LazyGroupBy(df.Lazy().CloneHandle(), groupByType, safeKeys)
        
        let finalBuilder = 
            match havingExpr with
            | Some h -> lazyBuilder.Having(h)
            | None -> lazyBuilder
            
        use lazyResult = finalBuilder.Agg aggs
        
        for k in safeKeys do (k :> IDisposable).Dispose()

        lazyResult.Collect()

    member this.Agg([<ParamArray>] aggs: Expr[]) : DataFrame =
        this.Agg(aggs :> seq<Expr>)

    // --- Syntactic Sugar for Eager GroupBy ---
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

    member private this.GetExcludedKeyNames() =
        let baseKeys = 
            keys |> Array.choose (fun expr -> 
                match expr.Meta.OutputName() with
                | null | "" -> None
                | name -> Some name
            ) |> Array.toList
            
        let allKeys =
            match groupByType with
            | Dynamic (indexCol, _, _, _, _, _, _, _) -> indexCol :: baseKeys
            | Rolling (indexCol, _, _, _) -> indexCol :: baseKeys
            | Standard _ -> baseKeys
            
        allKeys |> List.toArray

    member this.Head(?n: int) =
        let count = defaultArg n 10
        let aggregatedDf = this.Agg [ Expr.All().Head count ]
        let keyNames = this.GetExcludedKeyNames()
        aggregatedDf.Explode(Expr.All().ToSelector().Exclude keyNames)

    member this.Tail(?n: int) =
        let count = defaultArg n 10
        let aggregatedDf = this.Agg [ Expr.All().Tail count ]
        let keyNames = this.GetExcludedKeyNames()
        aggregatedDf.Explode(Expr.All().ToSelector().Exclude keyNames)
    // // --------------------------------------------------------
    // // Group Iteration (The Polars Workaround)
    // // --------------------------------------------------------

    // member private this.BuildGroupsDataFrame() : DataFrame * string =
    //     let rowIdxCol = "__POLARS_GB_ROW_INDEX"
    //     let tempCol = "__POLARS_GB_GROUP_INDICES"

    //     let safeKeys = ownedKeyHandles |> Array.map (fun h -> new Expr(PolarsWrapper.CloneExpr h))
        
    //     // 1. 给原数据打上行号标记
    //     use lazyWithIndex = df.Lazy().WithRowIndex(rowIdxCol)
        
    //     // 2. 利用我们之前写的统一化 LazyGroupBy 重新应用同样的分组策略（无缝处理 Standard/Dynamic/Rolling）
    //     let lazyGrouped = new LazyGroupBy(lazyWithIndex.CloneHandle(), groupByType, safeKeys)
        
    //     // 应用 Having 过滤
    //     let finalLazyGrouped = 
    //         match havingExpr with
    //         | Some h -> lazyGrouped.Having(h)
    //         | None -> lazyGrouped

    //     // 3. 将同一组的行号聚合成 List (Implode)
    //     let groupsDf = 
    //         finalLazyGrouped.Agg [ Expr.Col(rowIdxCol).Implode().Alias(tempCol) ]
        
    //     // 清理安全克隆的 Keys
    //     for k in safeKeys do (k :> IDisposable).Dispose()

    //     groupsDf, tempCol

    // /// <summary>
    // /// Allows iteration over the groups of the group by operation. (Strong Typed)
    // /// </summary>
    // member this.GetGroups<'TKey when 'TKey : (new : unit -> 'TKey)>() : seq<'TKey * DataFrame> =
    //     if disposed then raise (ObjectDisposedException "GroupBy Builder")
        
    //     seq {
    //         let groupsDf, tempCol = this.BuildGroupsDataFrame()
            
    //         // F# 序列表达式中的 use 绑定会自动在 IEnumerator 回收时触发 Dispose
    //         use __groupsDf = groupsDf
    //         use indicesCol = groupsDf.[tempCol]
    //         use keysDf = groupsDf.Drop(tempCol)

    //         // 将 Key 映射为强类型
    //         let typedKeys = keysDf.Rows<'TKey>() |> Seq.toArray

    //         for i = 0 to typedKeys.Length - 1 do
    //             use slicedList = indicesCol.Slice(i, 1UL)
    //             use indices = slicedList.Explode()
                
    //             // 利用索引截取原 DataFrame 生成该组的子 DataFrame
    //             let groupDf = df.[indices]
                
    //             yield (typedKeys.[i], groupDf)
    //     }

    // /// <summary>
    // /// Allows iteration over the groups of the group by operation.
    // /// </summary>
    // member this.GetEnumerator() : IEnumerator<obj[] * DataFrame> =
    //     if disposed then raise (ObjectDisposedException "GroupBy Builder")
        
    //     let seqImpl = seq {
    //         let groupsDf, tempCol = this.BuildGroupsDataFrame()

    //         use __groupsDf = groupsDf
    //         use indicesCol = groupsDf.[tempCol]
    //         use keysDf = groupsDf.Drop(tempCol)

    //         let height = int keysDf.Height

    //         for i = 0 to height - 1 do
    //             let keyObjects = keysDf.Row(i)
                
    //             use slicedList = indicesCol.Slice(i, 1UL)
    //             use indices = slicedList.Explode()
                
    //             let groupDf = df.[indices]
                
    //             yield (keyObjects, groupDf)
    //     }
    //     seqImpl.GetEnumerator()

    // // --------------------------------------------------------
    // // Interface Implementations
    // // --------------------------------------------------------
    
    // interface seq<obj[] * DataFrame> with
    //     member this.GetEnumerator() = this.GetEnumerator()
        
    // interface IEnumerable with
    //     member this.GetEnumerator() = (this :> seq<obj[] * DataFrame>).GetEnumerator() :> IEnumerator
        
    // interface IDisposable with
    //     member this.Dispose() = this.Dispose()

[<AutoOpen>]
module LazyGroupByExtensions =
    open Polars.NET.Core.Helpers

    type LazyFrame with
        /// <summary> Start a GroupBy operation. </summary>
        member this.GroupBy(keys: seq<Expr>, ?maintainOrder: bool, ?having: Expr) : LazyGroupBy =
            let maintain = defaultArg maintainOrder false
            let builder = new LazyGroupBy(this.CloneHandle(), GroupByType.Standard maintain, Seq.toArray keys)
            match having with
            | Some h -> builder.Having h 
            | None -> builder

        member this.GroupBy(keys: seq<#IColumnExpr>, ?maintainOrder: bool, ?having: Expr) : LazyGroupBy =
            let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
            let maintain = defaultArg maintainOrder false
            let builder = new LazyGroupBy(this.CloneHandle(), GroupByType.Standard maintain, kExprs)
            match having with
            | Some h -> builder.Having(h)
            | None -> builder

        member this.GroupBy([<ParamArray>] keys: Expr[]) : LazyGroupBy =
            this.GroupBy(keys :> seq<Expr>)

        /// <summary> Start a dynamic group-by (rolling window) operation. Returns a Builder. </summary>
        member this.GroupByDynamic(
            indexCol: string,
            every: TimeSpan,
            ?period: TimeSpan,
            ?offset: TimeSpan,
            ?by: seq<#IColumnExpr>, 
            ?label: Label,
            ?includeBoundaries: bool,
            ?closedWindow: ClosedWindow,
            ?startBy: StartBy
        ) : LazyGroupBy =
            let periodVal = defaultArg period every
            let offsetVal = defaultArg offset TimeSpan.Zero
            let labelVal = defaultArg label Label.Left
            let includeBoundariesVal = defaultArg includeBoundaries false
            let closedWindowVal = defaultArg closedWindow ClosedWindow.Left
            let startByVal = defaultArg startBy StartBy.WindowBound

            let everyStr = DurationFormatter.ToPolarsString every
            let periodStr = DurationFormatter.ToPolarsString periodVal
            let offsetStr = DurationFormatter.ToPolarsString offsetVal

            let keyExprs = 
                match by with
                | Some cols -> cols |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
                | None -> [||]

            let dynType = GroupByType.Dynamic(indexCol, everyStr, periodStr, offsetStr, labelVal, includeBoundariesVal, closedWindowVal, startByVal)
            new LazyGroupBy(this.CloneHandle(), dynType, keyExprs)
        /// <summary> Start a rolling group-by operation. Returns a Builder. </summary>
        member this.GroupByRolling(
            indexCol: string,
            period: TimeSpan,
            ?offset: TimeSpan,
            ?by: seq<#IColumnExpr>, 
            ?closedWindow: ClosedWindow
        ) : LazyGroupBy =
            let offsetVal = defaultArg offset TimeSpan.Zero
            let closedWindowVal = defaultArg closedWindow ClosedWindow.Left

            let periodStr = DurationFormatter.ToPolarsString period
            let offsetStr = DurationFormatter.ToPolarsString offsetVal

            let keyExprs = 
                match by with
                | Some cols -> cols |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
                | None -> [||]

            let rollType = GroupByType.Rolling(indexCol, periodStr, offsetStr, closedWindowVal)
            new LazyGroupBy(this.CloneHandle(), rollType, keyExprs)

[<AutoOpen>]
module DataFrameGroupByExtensions =
    open Polars.NET.Core.Helpers

    type DataFrame with
        member this.GroupBy(keys: seq<Expr>, ?maintainOrder: bool) =
            let maintain = defaultArg maintainOrder false
            new GroupBy(this, GroupByType.Standard maintain, Seq.toArray keys)

        member this.GroupBy(keys: seq<#IColumnExpr>, ?maintainOrder: bool) =
            let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
            let maintain = defaultArg maintainOrder false
            new GroupBy(this, GroupByType.Standard maintain, kExprs)

        member this.GroupBy([<ParamArray>] keys: Expr[]) =
            new GroupBy(this, GroupByType.Standard false, keys)

        /// <summary> Start a dynamic group-by operation. Returns a Builder. </summary>
        member this.GroupByDynamic(
            indexCol: string,
            every: TimeSpan,
            ?period: TimeSpan,
            ?offset: TimeSpan,
            ?by: seq<#IColumnExpr>, 
            ?label: Label,
            ?includeBoundaries: bool,
            ?closedWindow: ClosedWindow,
            ?startBy: StartBy
        ) : GroupBy =
            let periodVal = defaultArg period every
            let offsetVal = defaultArg offset TimeSpan.Zero
            let labelVal = defaultArg label Label.Left
            let includeBoundariesVal = defaultArg includeBoundaries false
            let closedWindowVal = defaultArg closedWindow ClosedWindow.Left
            let startByVal = defaultArg startBy StartBy.WindowBound

            let everyStr = DurationFormatter.ToPolarsString every
            let periodStr = DurationFormatter.ToPolarsString periodVal
            let offsetStr = DurationFormatter.ToPolarsString offsetVal

            let keyExprs = 
                match by with
                | Some cols -> cols |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
                | None -> [||]

            let dynType = GroupByType.Dynamic(indexCol, everyStr, periodStr, offsetStr, labelVal, includeBoundariesVal, closedWindowVal, startByVal)
            new GroupBy(this, dynType, keyExprs)
        /// <summary> Start a rolling group-by operation. Returns a Builder. </summary>
        member this.GroupByRolling(
            indexCol: string,
            period: TimeSpan,
            ?offset: TimeSpan,
            ?by: seq<#IColumnExpr>, 
            ?closedWindow: ClosedWindow
        ) : GroupBy =
            let offsetVal = defaultArg offset TimeSpan.Zero
            let closedWindowVal = defaultArg closedWindow ClosedWindow.Left

            let periodStr = DurationFormatter.ToPolarsString period
            let offsetStr = DurationFormatter.ToPolarsString offsetVal

            let keyExprs = 
                match by with
                | Some cols -> cols |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toArray
                | None -> [||]

            let rollType = GroupByType.Rolling(indexCol, periodStr, offsetStr, closedWindowVal)
            new GroupBy(this, rollType, keyExprs)