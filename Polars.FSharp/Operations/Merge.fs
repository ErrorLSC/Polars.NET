namespace Polars.FSharp
open System

[<RequireQualifiedAccess>]
type SetterSpec =
    | Explicit of Map<string, Expr>
    | Dynamic  of (MergeContext -> Map<string, Expr>)

and MergeAction =
    | MatchedUpdate      of condition: (MergeContext -> Expr) * setters: SetterSpec option
    | MatchedDelete      of condition: (MergeContext -> Expr)
    | NotMatchedInsert   of condition: (MergeContext -> Expr) * setters: SetterSpec option
    | NotMatchedBySourceDelete of condition: (MergeContext -> Expr)

and MergeContext = {
    SourceCol   : string -> Expr
    TargetCol   : string -> Expr
    TargetFrame : LazyFrame
}

[<RequireQualifiedAccess>]
type MergePlan = {
    Target    : LazyFrame
    Source    : LazyFrame
    On        : string[]
    Actions   : MergeAction list
    IncludeNulls : bool
    MaintainOrder : JoinMaintainOrder
    TmpSuffix : string
    ActionCol : string
    Context   : MergeContext
}

module private MergeUtils =
    let generateSuffix() = $"""__TMP_{Guid.NewGuid().ToString("N").Substring(0, 8)}"""
    let createContext (suffix: string) (targetFrame: LazyFrame) = {
        SourceCol   = fun name -> pl.col (name + suffix)
        TargetCol   = pl.col
        TargetFrame = targetFrame
    }
    let actionColName = "__ACTION"
    let targetFlagName = "__TGT"
    let sourceFlagName = "__SRC"

module Merge =
    open MergeUtils
    /// <summary>
    /// Initializes a Merge plan from a target and source LazyFrame, using the provided join key columns.
    /// This is the module-level equivalent of calling <c>myTarget.Merge(source, on)</c>.
    /// </summary>
    /// <param name="target">The target LazyFrame to be updated.</param>
    /// <param name="source">The source LazyFrame containing the updates.</param>
    /// <param name="on">The resolved join key column names.</param>
    /// <returns>A new MergePlan ready for further configuration via <c>Merge.whenXxx</c> functions.</returns>
    let initiate (source: LazyFrame) (on: seq<string>) (target: LazyFrame) : MergePlan =
        let onArray = on |> Seq.toArray
        let sfx = generateSuffix()
        {
            Target    = target
            Source    = source
            On        = onArray
            Actions   = []
            IncludeNulls = false
            MaintainOrder = JoinMaintainOrder.Left
            TmpSuffix = sfx
            ActionCol = actionColName
            Context   = createContext sfx target   
        }

    let private defaultCond : MergeContext -> Expr = fun _ -> pl.lit true
    /// <summary>
    /// Controls whether null values from the source should overwrite existing target values
    /// during a default column update (when no explicit setters are provided).
    /// </summary>
    /// <param name="flag">If true, nulls from the source will overwrite target values.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with the updated IncludeNulls setting.</returns>
    let includeNulls (flag: bool) (plan: MergePlan) = { plan with IncludeNulls = flag }
    /// <summary>
    /// Sets the row order preservation strategy for the join phase.
    /// </summary>
    /// <param name="order">The desired maintain-order behavior from the JoinMaintainOrder enum.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with the updated MaintainOrder setting.</returns>
    let maintainOrder (order: JoinMaintainOrder) (plan: MergePlan) = { plan with MaintainOrder = order }
    /// <summary>
    /// Adds a "WHEN MATCHED UPDATE" action to the merge plan.
    /// If the condition is not provided, all matched rows are updated.
    /// If setters are not provided, all source columns with matching names will overwrite target columns
    /// (respecting <see cref="includeNulls"/>).
    /// If explicit setters or a SetterSpec are given, only those columns are updated.
    /// </summary>
    /// <param name="condition">Optional condition expression builder using the MergeContext (source/target column accessors).</param>
    /// <param name="setters">Optional SetterSpec defining which columns to update and how.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with the UPDATE action appended.</returns>
    let whenMatchedUpdate
        (condition: (MergeContext -> Expr) option)
        (setters: SetterSpec option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.MatchedUpdate(cond, setters) :: plan.Actions }
    /// <summary>
    /// Adds a "WHEN MATCHED DELETE" action to the merge plan.
    /// If the condition is not provided, all matched rows are deleted.
    /// </summary>
    /// <param name="condition">Optional condition expression builder using the MergeContext.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with the DELETE action appended.</returns>
    let whenMatchedDelete
        (condition: (MergeContext -> Expr) option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.MatchedDelete(cond) :: plan.Actions }
    /// <summary>
    /// Adds a "WHEN NOT MATCHED INSERT" action to the merge plan.
    /// If the condition is not provided, all unmatched source rows are inserted.
    /// If setters are not provided, all source columns are inserted as-is.
    /// If setters are given, only those columns are populated (with the rest set to null/default).
    /// </summary>
    /// <param name="condition">Optional condition expression builder using the MergeContext.</param>
    /// <param name="setters">Optional SetterSpec defining which columns to populate in the new row.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with the INSERT action appended.</returns>
    let whenNotMatchedInsert
        (condition: (MergeContext -> Expr) option)
        (setters: SetterSpec option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.NotMatchedInsert(cond, setters) :: plan.Actions }
    /// <summary>
    /// Adds a "WHEN NOT MATCHED BY SOURCE DELETE" action to the merge plan.
    /// This deletes rows from the target that have no matching row in the source,
    /// optionally filtered by a condition.
    /// </summary>
    /// <param name="condition">Optional condition expression builder using the MergeContext.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with the DELETE (by source) action appended.</returns>
    let whenNotMatchedBySourceDelete
        (condition: (MergeContext -> Expr) option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.NotMatchedBySourceDelete(cond) :: plan.Actions }
    /// <summary>
    /// Convenience shorthand for <c>whenMatchedUpdate None None</c> – updates all matched rows
    /// using all source columns (with <see cref="includeNulls"/> controlling null overwrites).
    /// </summary>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with an unconditional, full-row UPDATE action appended.</returns>
    let whenMatchedUpdateAll plan = whenMatchedUpdate None None plan
    /// <summary>
    /// Convenience shorthand for <c>whenMatchedUpdate None (Some setters)</c> – updates all matched rows
    /// but only the columns specified by the <paramref name="setters"/>.
    /// </summary>
    /// <param name="setters">The SetterSpec (built via Set.build) defining which columns to update.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with an unconditional, setter-defined UPDATE action appended.</returns>
    let whenMatchedUpdateSet setters plan = whenMatchedUpdate None (Some setters) plan
    /// <summary>
    /// Convenience shorthand for <c>whenNotMatchedInsert None None</c> – inserts all unmatched source rows
    /// with all source columns.
    /// </summary>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with an unconditional, full-row INSERT action appended.</returns>
    let whenNotMatchedInsertAll plan = whenNotMatchedInsert None None plan
    /// <summary>
    /// Convenience shorthand for <c>whenNotMatchedInsert None (Some setters)</c> – inserts unmatched source rows
    /// but only the columns specified by the <paramref name="setters"/>.
    /// </summary>
    /// <param name="setters">The SetterSpec (built via Set.build) defining which columns to populate.</param>
    /// <param name="plan">The current MergePlan being constructed.</param>
    /// <returns>A new MergePlan with an unconditional, setter-defined INSERT action appended.</returns>
    let whenNotMatchedInsertSet setters plan = whenNotMatchedInsert None (Some setters) plan
    let private validate (plan: MergePlan) =
        let srcSchema = plan.Source.Schema
        let tgtSchema = plan.Target.Schema

        plan.On |> Array.iter (fun key ->
            if not (srcSchema.ContainsKey key) then invalidArg "on" $"Key '{key}' not found in source."
            if not (tgtSchema.ContainsKey key) then invalidArg "on" $"Key '{key}' not found in target."

            if srcSchema.[key] <> tgtSchema.[key] then
                invalidArg "on" $"Key type mismatch for '{key}': source: {srcSchema.[key]}, target: {tgtSchema.[key]}."
        )

        let nullCheckExpr = 
            plan.On
            |> Array.map (fun k -> pl.col k |> pl.isNull)
            |> Array.reduce (fun e1 e2 -> e1 .|| e2)
            |> alias plan.ActionCol

        let nullCheck = 
            plan.Source
                |> pl.selectLazy (plan.On |> Array.map pl.col |> Array.toSeq)
                |> pl.withColumnsLazy [nullCheckExpr]
                |> pl.filterLazy(pl.col plan.ActionCol)
                |> pl.headLazy 1
                |> pl.collect
        if nullCheck.Height > 0 then
            raise (System.IO.InvalidDataException $"""Null values found in merge keys: {String.Join(", ", plan.On)}""")

        let dupCheck = 
            plan.Source
                |> pl.groupByLazy (plan.On |> Array.map pl.col |> Array.toSeq)
                |> pl.aggLazy [pl.len() |> pl.alias "dup_count"]
                |> pl.filterLazy(pl.col "dup_count" .> pl.lit 1)
                |> pl.headLazy 1
                |> pl.collect
        if dupCheck.Height > 0 then
            raise (System.IO.InvalidDataException $"""Duplicate keys found in source for: {String.Join(", ", plan.On)}""")
    let private buildAst (plan: MergePlan) : LazyFrame =
        let sfx = plan.TmpSuffix
        let actionColName = plan.ActionCol
        let tgtValCol = targetFlagName
        let srcValCol = sourceFlagName

        let defaultCond : MergeContext -> Expr = fun _ -> pl.lit true
        let allActions = 
            if plan.Actions.IsEmpty then 
                [ MergeAction.MatchedUpdate(defaultCond, None);
                MergeAction.NotMatchedInsert(defaultCond, None) ]
            else plan.Actions

        let tgt = plan.Target |> pl.withColumnLazy (pl.lit true |> pl.alias tgtValCol)
        let src = plan.Source |> pl.selectLazy([
                        pl.all().Name.Suffix sfx
                        pl.lit true |> pl.alias srcValCol
                    ])

        let leftOn = plan.On |> Array.map pl.col
        let rightOn = plan.On |> Array.map (fun k -> pl.col (k + sfx))

        let hasInsert = 
            allActions |> List.exists (function MergeAction.NotMatchedInsert _ -> true | _ -> false)
        let how = if hasInsert then JoinType.Outer else JoinType.Left

        let isMatched = pl.col tgtValCol |> pl.isNotNull .&& (pl.col srcValCol |> pl.isNotNull)
        let isSourceOnly = pl.col tgtValCol |> pl.isNull .&& (pl.col srcValCol |> pl.isNotNull)
        let isTargetOnly = pl.col tgtValCol |> pl.isNotNull .&& (pl.col srcValCol |> pl.isNull)

        let actionsWithId = allActions |> List.mapi (fun i a -> i + 1, a)
        
        let baseActionExpr = 
            pl.ifElse isTargetOnly (pl.lit 0) (pl.ifElse isMatched (pl.lit 0) (pl.lit -1))

        let actionCol =
            actionsWithId
            |> List.fold (fun accExpr (id, action) ->
                let condFunc =
                    match action with
                    | MergeAction.MatchedUpdate (f, _) | MergeAction.MatchedDelete f
                    | MergeAction.NotMatchedInsert (f, _) | MergeAction.NotMatchedBySourceDelete f -> f
                let condExpr = condFunc plan.Context
                let finalCond =
                    match action with
                    | MergeAction.MatchedUpdate _ | MergeAction.MatchedDelete _ -> isMatched .&& condExpr
                    | MergeAction.NotMatchedInsert _ -> isSourceOnly .&& condExpr
                    | MergeAction.NotMatchedBySourceDelete _ -> isTargetOnly .&& condExpr
                pl.ifElse finalCond (pl.lit id) accExpr
            ) baseActionExpr
        
        let tgtNames = plan.Target.Schema.Names
        let srcNames = plan.Source.Schema.Names
        
        let allColumns = 
            tgtNames @ srcNames
            |> List.distinct

        let whenCondHelper condId thenExpr elseExpr =
            pl.ifElse (pl.col actionColName .== pl.lit condId) thenExpr elseExpr

        let columnUpdateExprs = 
            allColumns |> List.map (fun colName ->
                let tgtCol = if List.contains colName tgtNames then pl.col colName else pl.litNull()
                let srcTmpCol = if List.contains colName srcNames then pl.col (colName + sfx) else pl.litNull()
                let initial = tgtCol
                let finalExpr = 
                    actionsWithId
                    |> List.fold (fun current (id, action) ->
                        match action with
                        | MergeAction.MatchedUpdate(_, settersSpec) 
                        | MergeAction.NotMatchedInsert(_, settersSpec) ->
                            let settersMapOpt =
                                settersSpec |> Option.map (fun spec ->
                                    match spec with
                                    | SetterSpec.Explicit m -> m
                                    | SetterSpec.Dynamic f -> f plan.Context)
                            match settersMapOpt with
                            | Some settersMap ->
                                match Map.tryFind colName settersMap with
                                | Some userExpr -> whenCondHelper id userExpr current
                                | None -> current   
                            | None ->
                                if List.contains colName srcNames then
                                    if plan.IncludeNulls then
                                        whenCondHelper id srcTmpCol current
                                    else
                                        pl.ifElse (pl.col actionColName .== pl.lit id)
                                            (pl.ifElse (srcTmpCol |> pl.isNotNull) srcTmpCol current)
                                            current
                                else current
                        | _ -> current
                    ) initial
                finalExpr |> alias colName
            )

        let deleteIds = 
            actionsWithId 
            |> List.choose (fun (id, action) -> 
                match action with 
                | MergeAction.MatchedDelete _ | MergeAction.NotMatchedBySourceDelete _ -> Some id 
                | _ -> None)
        let keepCond =
            deleteIds 
            |> List.fold (fun cond id -> cond .&& (pl.col actionColName .!= pl.lit id)) (pl.col actionColName .!= pl.lit -1)

        let dropExprs = 
            [ yield! srcNames |> List.map (fun n -> pl.col (n + sfx));
            yield pl.col tgtValCol; yield pl.col srcValCol; yield pl.col actionColName ] 
        
        let readyforDrop =
            tgt.Join(
                        other = src,
                        leftOn = leftOn,
                        rightOn = rightOn,
                        how = how,
                        coalesce = JoinCoalesce.KeepColumns,
                        maintainOrder = plan.MaintainOrder
                    )
            |> pl.withColumnsLazy [actionCol |> pl.alias actionColName]
            |> pl.withColumnsLazy columnUpdateExprs 
            |> pl.filterLazy keepCond
        readyforDrop.Drop dropExprs

    let private replaceSuffix (suffix: string) (s: string) =
        s.Replace(suffix, ".Source")

    let private formatConditionInline (suffix: string) (cond: MergeContext -> Expr) (ctx: MergeContext) =
        let expr = cond ctx
        expr.ToString() |> replaceSuffix suffix

    let private isAlwaysTrueCond (condStr: string) =
        condStr = "true" || condStr = "Literal(true)"

    let private expandSetters (suffix: string) (ctx: MergeContext) (setterSpec: SetterSpec) : Map<string, string> =
        let m = match setterSpec with
                | SetterSpec.Explicit m -> m
                | SetterSpec.Dynamic f -> f ctx
        m |> Map.map (fun _ expr -> expr.ToString() |> replaceSuffix suffix)
    /// <summary>
    /// Generates a human-readable string describing the logical merge plan,
    /// including merge keys, match strategy, actions with conditions and setters,
    /// and the join strategy.
    /// </summary>
    /// <param name="plan">The constructed MergePlan.</param>
    /// <returns>A formatted string representing the merge logic.</returns>
    let toPlanString (plan: MergePlan) =
        let sb = System.Text.StringBuilder()
        let suffix = plan.TmpSuffix
        let ctx = plan.Context

        // 1. MERGE ON
        sb.AppendLine $"""MERGE ON: {String.Join(", ", plan.On)}""" |> ignore
        sb.AppendLine() |> ignore

        // 2. MATCH STRATEGY
        sb.AppendLine "MATCH STRATEGY:" |> ignore
        sb.AppendLine "  First Match Wins (Sequential Evaluation)" |> ignore
        sb.AppendLine() |> ignore

        let actionsToPrint =
            if plan.Actions.IsEmpty then
                [ MergeAction.MatchedUpdate((fun _ -> lit true), None);
                MergeAction.NotMatchedInsert((fun _ -> lit true), None) ]
            else plan.Actions

        let matchedActions =
            actionsToPrint |> List.choose (function
                | MergeAction.MatchedUpdate(c, s) -> Some("UPDATE", c, s)
                | MergeAction.MatchedDelete c -> Some("DELETE", c, None)
                | _ -> None)
        let notMatchedActions =
            actionsToPrint |> List.choose (function
                | MergeAction.NotMatchedInsert(c, s) -> Some("INSERT", c, s)
                | MergeAction.NotMatchedBySourceDelete c -> Some("DELETE (By Source)", c, None)
                | _ -> None)

        let printActionGroup (title: string) (actions: (string * (MergeContext -> Expr) * SetterSpec option) list) =
            if not actions.IsEmpty then
                let lines = [
                    yield title
                    
                    for i, (name, cond, setters) in List.indexed actions do
                        let condStr = formatConditionInline suffix cond ctx
                        
                        if isAlwaysTrueCond condStr then
                            yield $"  [{i+1}] {name}"
                        else
                            yield $"  [{i+1}] {name} WHERE {condStr}"

                        if name = "UPDATE" || name = "INSERT" then
                            match setters with
                            | Some spec ->
                                let expanded = expandSetters suffix ctx spec
                                if not expanded.IsEmpty then
                                    yield $"      SET ({expanded.Count} overrides):"
                                    for kv in expanded do
                                        yield $"        - {kv.Key} = {kv.Value}"
                                else
                                    yield "      SET: (All Source Columns)"
                            | None ->
                                yield "      SET: (All Source Columns)"
                    
                    yield ""
                ]

                lines |> List.iter (fun line -> sb.AppendLine line |> ignore)

        printActionGroup "WHEN MATCHED:" matchedActions
        printActionGroup "WHEN NOT MATCHED:" notMatchedActions

        // JOIN STRATEGY
        let hasInsert = actionsToPrint |> List.exists (function MergeAction.NotMatchedInsert _ -> true | _ -> false)
        let joinType = if hasInsert then "Outer" else "Left"
        let reason = if hasInsert then " (Upgraded to Outer to support INSERT)" else " (Left join sufficient)"
        sb.AppendLine "JOIN STRATEGY:" |> ignore
        sb.AppendLine $"  Type: {joinType}{reason}" |> ignore
        sb.AppendLine $"  MaintainOrder: {plan.MaintainOrder}" |> ignore

        sb.ToString().TrimEnd()
    /// <summary>
    /// Inspects the current merge plan by passing its string representation to the given logger function,
    /// then returns the plan unchanged to continue the pipeline.
    /// </summary>
    /// <param name="logger">A function that receives the plan description string (e.g., printfn "%s").</param>
    /// <param name="plan">The MergePlan to inspect.</param>
    /// <returns>The original MergePlan for further chaining.</returns>
    let inspectPlan (logger: string -> unit) (plan: MergePlan) =
        let planStr = toPlanString plan
        let output = $"""
========== POLARS.NET MERGE PLAN ==========
{planStr}
===========================================
    """
        logger output
        plan
    /// <summary>
    /// Convenience function that prints the merge plan to standard output using printfn.
    /// Equivalent to <c>inspectPlan (printfn "%s") plan</c>.
    /// </summary>
    /// <param name="plan">The MergePlan to inspect.</param>
    /// <returns>The original MergePlan for further chaining.</returns>
    let printPlan (plan: MergePlan) =
        inspectPlan (printfn "%s") plan
    /// <summary>
    /// Returns the optimized execution plan for the merge operation
    /// as a string, using Polars' EXPLAIN mechanism. This is useful for performance tuning.
    /// </summary>
    /// <param name="plan">The constructed MergePlan.</param>
    /// <returns>A string representing the query plan (e.g., optimized logical/physical plan).</returns>
    let explain (plan:MergePlan) = plan |> buildAst |> pl.explain
    /// <summary>
    /// Executes the merge plan lazily, returning a LazyFrame that can be further transformed
    /// or collected later. Validates the plan before building the AST.
    /// </summary>
    /// <param name="plan">The fully constructed MergePlan.</param>
    /// <returns>A LazyFrame representing the merged result (not yet materialized).</returns>
    let execute (plan: MergePlan) =
        validate plan
        buildAst plan 
    /// <summary>
    /// Executes the merge plan eagerly, materializing the result into a DataFrame.
    /// This is the terminal operation for a merge pipeline when a concrete result is needed.
    /// </summary>
    /// <param name="engine">The Polars execution engine to use (e.g., Engine.Auto).</param>
    /// <param name="plan">The fully constructed MergePlan.</param>
    /// <returns>A materialized DataFrame containing the merged data.</returns>
    let executeEager (engine: Engine) (plan: MergePlan) : DataFrame =
        execute plan |> pl.collectWithEngine engine


/// <summary>
/// A module for building column setter specifications used in Merge operations.
/// Use <c>Set.col</c> for exact column assignments, <c>Set.selector</c> for batch assignments via a column selector,
/// and combine them with <c>Set.build</c> to produce a <c>SetterSpec</c>.
/// </summary>
[<RequireQualifiedAccess>]
module Set =
    /// <summary>
    /// Internal specification for a single column or a selector‑based group of columns.
    /// </summary>
    type ColSpec =
        | Exact of string * (MergeContext -> Expr)
        | Select of IColumnExpr * (string -> MergeContext -> Expr)
    /// <summary>
    /// Creates an exact column assignment for a specific column name.
    /// The value function receives the <c>MergeContext</c> and should return the expression to be assigned.
    /// </summary>
    /// <param name="name">The name of the column to set.</param>
    /// <param name="valueFn">A function that, given the MergeContext, returns the Expr to assign to the column.</param>
    /// <returns>A ColSpec representing an exact column mapping.</returns>
    let col (name: string) (valueFn: MergeContext -> Expr) = Exact(name, valueFn)
    /// <summary>
    /// Creates a batch column assignment for all columns matching the given <c>IColumnExpr</c> selector.
    /// The value function receives each matched column name and the <c>MergeContext</c>, and should return the Expr for that column.
    /// </summary>
    /// <param name="sel">The column selector (e.g., <c>pl.cs.startsWith "Stat_"</c>).</param>
    /// <param name="valueFn">A function taking a column name and the MergeContext, returning the Expr for that column.</param>
    /// <returns>A ColSpec representing a batch selector mapping.</returns>
    let selector (sel: IColumnExpr) (valueFn: string -> MergeContext -> Expr) = Select(sel, valueFn)
    /// <summary>
    /// Builds a <c>SetterSpec</c> from a list of column specifications.
    /// Exact columns are mapped directly; selectors are expanded against the target frame to produce multiple column mappings.
    /// The resulting <c>SetterSpec.Dynamic</c> can be passed to <c>Merge.whenMatchedUpdateSet</c>, <c>Merge.whenNotMatchedInsertSet</c>,
    /// or any other setter‑accepting function.
    /// </summary>
    /// <param name="specs">A list of ColSpec values produced by <c>Set.col</c> and <c>Set.selector</c>.</param>
    /// <returns>A SetterSpec representing the complete column update logic.</returns>
    let build (specs: ColSpec list) : SetterSpec =
        SetterSpec.Dynamic(fun ctx ->
            specs
            |> List.collect (fun spec ->
                match spec with
                | Exact(name, fn) -> [name, fn ctx]
                | Select(sel, fn) ->
                    let colNames = pl.cs.expandLf sel ctx.TargetFrame
                    colNames |> Array.map (fun n -> n, fn n ctx) |> Array.toList
            )
            |> Map.ofList
        )


[<AutoOpen>]
module MergeOps =

    type LazyFrame with
        /// <summary>
        /// Initiates a Merge (upsert) plan using this LazyFrame as the target and the given source.
        /// The <paramref name="on"/> selector is expanded against the target to determine the join keys.
        /// </summary>
        /// <param name="source">The source LazyFrame containing updates/inserts.</param>
        /// <param name="on">A column expression (e.g., <c>pl.col "Id"</c>) or selector (e.g., <c>pl.cs.endsWith "Id"</c>)
        /// that resolves to the merge key columns.</param>
        /// <returns>A <c>MergePlan</c> that can be further configured and then executed.</returns>
        member this.Merge(source: LazyFrame, on: IColumnExpr) =
            let onCols = this |> pl.cs.expandLf on 
            this |> Merge.initiate source onCols 

    type DataFrame with
        /// <summary>
        /// Initiates a Merge (upsert) plan using this DataFrame as the target and the given source.
        /// Internally both frames are converted to LazyFrame, so the resulting <c>MergePlan</c> can still be
        /// executed eagerly by piping into <c>Merge.executeEager</c>.
        /// </summary>
        /// <param name="source">The source DataFrame containing updates/inserts.</param>
        /// <param name="on">A column expression or selector that resolves to the merge key columns.</param>
        /// <returns>A <c>MergePlan</c> backed by LazyFrames, ready for further configuration and execution.</returns>
        member this.Merge(source: DataFrame, on: IColumnExpr) =
            let onCols = this |> pl.cs.expandDf on 
            let slf = source |> pl.asLazy 
            this |> pl.asLazy |> Merge.initiate slf onCols 