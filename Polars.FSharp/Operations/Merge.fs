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

    let source (src: LazyFrame) (on: string[]) (tgt: LazyFrame) : MergePlan =
        let sfx = generateSuffix()
        {
            Target    = tgt
            Source    = src
            On        = on
            Actions   = []
            IncludeNulls = false
            MaintainOrder = JoinMaintainOrder.Left
            TmpSuffix = sfx
            ActionCol = actionColName
            Context   = createContext sfx tgt   
        }

    let private defaultCond : MergeContext -> Expr = fun _ -> lit true
    // let private defaultSetters : SetterSpec option = None

    let includeNulls (flag: bool) (plan: MergePlan) = { plan with IncludeNulls = flag }
    let maintainOrder (order: JoinMaintainOrder) (plan: MergePlan) = { plan with MaintainOrder = order }

    let whenMatchedUpdate
        (condition: (MergeContext -> Expr) option)
        (setters: SetterSpec option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.MatchedUpdate(cond, setters) :: plan.Actions }

    let whenMatchedDelete
        (condition: (MergeContext -> Expr) option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.MatchedDelete(cond) :: plan.Actions }

    let whenNotMatchedInsert
        (condition: (MergeContext -> Expr) option)
        (setters: SetterSpec option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.NotMatchedInsert(cond, setters) :: plan.Actions }

    let whenNotMatchedBySourceDelete
        (condition: (MergeContext -> Expr) option)
        (plan: MergePlan) =
        let cond = defaultArg condition defaultCond
        { plan with Actions = MergeAction.NotMatchedBySourceDelete(cond) :: plan.Actions }
    let whenMatchedUpdateAll plan = whenMatchedUpdate None None plan
    let whenMatchedUpdateSet setters plan = whenMatchedUpdate None (Some setters) plan
    let whenNotMatchedInsertAll plan = whenNotMatchedInsert None None plan
    let whenNotMatchedInsertSet setters plan = whenNotMatchedInsert None (Some setters) plan
    let private validate (plan: MergePlan) =
        let srcSchema = plan.Source.Schema
        let tgtSchema = plan.Target.Schema

        for key in plan.On do
            if not (List.contains key srcSchema.Names) then
                invalidArg "on" $"Key '{key}' not found in source."
            if not (List.contains key tgtSchema.Names) then
                invalidArg "on" $"Key '{key}' not found in target."

            let srcType = srcSchema.[key]
            let tgtType = tgtSchema.[key]
            if srcType <> tgtType then
                invalidArg "on" $"Key type mismatch for '{key}': source: {srcType}, target: {tgtType}."

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

        let defaultCond : MergeContext -> Expr = fun _ -> lit true
        let allActions = 
            if plan.Actions.IsEmpty then 
                [ MergeAction.MatchedUpdate(defaultCond, None);
                MergeAction.NotMatchedInsert(defaultCond, None) ]
            else List.rev plan.Actions

        let tgt = plan.Target |> pl.withColumnLazy (pl.lit true |> pl.alias tgtValCol)
        let src = plan.Source |> pl.selectLazy([
                        pl.all().Name.Suffix(sfx)
                        pl.lit true |> pl.alias srcValCol
                    ])

        let leftOn = plan.On |> Array.map col
        let rightOn = plan.On |> Array.map (fun k -> col (k + sfx))

        let hasInsert = 
            allActions |> List.exists (function MergeAction.NotMatchedInsert _ -> true | _ -> false)
        let how = if hasInsert then JoinType.Outer else JoinType.Left

        let joined = tgt.Join(
                        other = src,
                        leftOn = leftOn,
                        rightOn = rightOn,
                        how = how,
                        coalesce = JoinCoalesce.KeepColumns,
                        maintainOrder = plan.MaintainOrder
                    )

        let isMatched = pl.col tgtValCol |> pl.isNotNull .&& (pl.col srcValCol |> pl.isNotNull)
        let isSourceOnly = pl.col tgtValCol |> pl.isNull .&& (pl.col srcValCol |> pl.isNotNull)
        let isTargetOnly = pl.col tgtValCol |> pl.isNotNull .&& (pl.col srcValCol |> pl.isNull)

        let actionsWithId = allActions |> List.mapi (fun i a -> i + 1, a)
        
        let baseActionExpr = 
            pl.ifElse isTargetOnly (pl.lit 0) (pl.ifElse isMatched (pl.lit 0) (pl.lit -1))

        let actionCol =
            actionsWithId
            |> List.rev
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
        
        let joined = joined.WithColumns(actionCol |> alias actionColName)

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
                    |> List.rev
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
                                            (pl.ifElse (srcTmpCol.IsNotNull()) srcTmpCol current)
                                            current
                                else current
                        | _ -> current
                    ) initial
                finalExpr |> alias colName
            )
        let joined = joined |> pl.withColumnsLazy columnUpdateExprs 

        let deleteIds = 
            actionsWithId 
            |> List.choose (fun (id, action) -> 
                match action with 
                | MergeAction.MatchedDelete _ | MergeAction.NotMatchedBySourceDelete _ -> Some id 
                | _ -> None)
        let keepCond =
            deleteIds 
            |> List.fold (fun cond id -> cond .&& (pl.col actionColName .!= pl.lit id)) (pl.col actionColName .!= pl.lit -1)
        let joined = joined |> pl.filterLazy keepCond

        let dropExprs = 
            [ yield! srcNames |> List.map (fun n -> col (n + sfx));
            yield col tgtValCol; yield col srcValCol; yield col actionColName ]
        joined.Drop dropExprs

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

    let toPlanString (plan: MergePlan) =
        let sb = System.Text.StringBuilder()
        let suffix = plan.TmpSuffix
        let ctx = plan.Context

        // 1. MERGE ON
        sb.AppendLine $"""MERGE ON: {String.Join(", ", plan.On)}""" |> ignore
        sb.AppendLine() |> ignore

        // 2. MATCH STRATEGY
        sb.AppendLine("MATCH STRATEGY:") |> ignore
        sb.AppendLine("  First Match Wins (Sequential Evaluation)") |> ignore
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
                sb.AppendLine(title) |> ignore
                actions |> List.iteri (fun i (name, cond, setters) ->
                    let condStr = formatConditionInline suffix cond ctx
                    let alwaysTrue = isAlwaysTrueCond condStr
                    if alwaysTrue then
                        sb.AppendLine($"  [{i+1}] {name}") |> ignore
                    else
                        sb.AppendLine($"  [{i+1}] {name} WHERE {condStr}") |> ignore

                    if name = "UPDATE" || name = "INSERT" then
                        match setters with
                        | Some spec ->
                            let expanded = expandSetters suffix ctx spec
                            if not expanded.IsEmpty then
                                sb.AppendLine($"      SET ({expanded.Count} overrides):") |> ignore
                                for kv in expanded do
                                    sb.AppendLine($"        - {kv.Key} = {kv.Value}") |> ignore
                            else
                                sb.AppendLine("      SET: (All Source Columns)") |> ignore
                        | None ->
                            sb.AppendLine("      SET: (All Source Columns)") |> ignore
                )
                sb.AppendLine() |> ignore

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

    let inspectPlan (logger: string -> unit) (plan: MergePlan) =
        let planStr = toPlanString plan
        let output = $"""
========== POLARS.NET MERGE PLAN ==========
{planStr}
===========================================
    """
        logger output
        plan

    let printPlan (plan: MergePlan) =
        inspectPlan (printfn "%s") plan
    let explain (plan:MergePlan) = plan |> buildAst |> pl.explain
    let execute (plan: MergePlan) =
        validate plan
        buildAst plan 
    let executeEager (engine: Engine) (plan: MergePlan) : DataFrame =
        execute plan |> pl.collectWithEngine engine

[<RequireQualifiedAccess>]
module Set =

    type ColSpec =
        | Exact of string * (MergeContext -> Expr)
        | Select of IColumnExpr * (string -> MergeContext -> Expr)

    let col (name: string) (valueFn: MergeContext -> Expr) = Exact(name, valueFn)

    let selector (sel: IColumnExpr) (valueFn: string -> MergeContext -> Expr) = Select(sel, valueFn)

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
        member this.Merge(source: LazyFrame, on: IColumnExpr) =
            let onCols = this |> pl.cs.expandLf on 
            Merge.source source onCols this

    type DataFrame with
        member this.Merge(source: DataFrame, on: IColumnExpr) =
            let onCols = this |> pl.cs.expandDf on  
            Merge.source (source.Lazy()) onCols (this.Lazy())