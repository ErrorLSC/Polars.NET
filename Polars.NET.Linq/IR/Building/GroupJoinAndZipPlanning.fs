namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq

/// Plans combining a second sequence row by row: a GroupJoin with a result selector, and Zip.
module internal GroupJoinAndZipPlanning =

    /// Disambiguates duplicate column names in a LogicalPlan against an existing set of columns by appending a suffix.
    let private disambiguatePlanColumns
        (existingCols: Set<string>)
        (plan: LogicalPlan)
        (suffix: string)
        : LogicalPlan * Set<string> * Map<string, string> =
        let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
        let duplicates = cols |> List.filter existingCols.Contains
        if not duplicates.IsEmpty then
            let newNames = duplicates |> List.map (fun c -> c + suffix)
            let suffixMap = List.zip duplicates newNames |> Map.ofList
            let renamedPlan = LogicalPlan.Rename(plan, duplicates, newNames)
            renamedPlan, existingCols |> Set.union (Set.ofList cols), suffixMap
        else
            plan, existingCols |> Set.union (Set.ofList cols), Map.empty

    /// Pure symbolic GroupJoin with explicit result selector builder (supporting pre-aggregated group counts and null-filling).
    let buildGroupJoinWithResultPlan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (resSel: LambdaExpression)
        : LogicalPlan option =

        let tryExtractGroupAgg (groupParamName: string) (resLambda: LambdaExpression) : (string * ScalarExpr) option =
            match resLambda.Body with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = extractPascalCaseMemberNames newExpr
                List.zip memberNames args
                |> List.tryPick (fun (memName, arg) ->
                    match arg with
                    | MethodCall(m, _, (:? ParameterExpression as p) :: _) when
                        p.Name = groupParamName && (m.Name = "Count" || m.Name = "LongCount")
                        ->
                        Some(memName, ScalarExpr.Aggregation(AggKind.Len, None, false))
                    | _ -> None)
            | _ -> None

        resolveSequence innerExpr
        |> Option.bind (fun rightPlan ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]
            let leftCols = (SchemaInference.inferPlanSchema basePlan).ColumnNames
            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
            let leftCtx = PipelineStageParser.createContextWithScopeAndCols RowShape.Empty outerParam.Type leftCols outerParam.Name
            let rightCtx = PipelineStageParser.createContextWithScopeAndCols RowShape.Empty innerParam.Type rightCols innerParam.Name

            match
                ExprTranslator.tryTranslateToScalar leftCtx outerKey.Body,
                ExprTranslator.tryTranslateToScalar rightCtx innerKey.Body
            with
            | Some leftKey, Some rightKey ->
                let groupParam = resSel.Parameters.[1]

                let preAggRightPlan, aggColOpt =
                    match tryExtractGroupAgg groupParam.Name resSel with
                    | Some(aggColName, aggScalar) ->
                        LogicalPlan.GroupBy(rightPlan, [ rightKey ], [ (aggColName, aggScalar) ], None, false), Some aggColName
                    | None -> rightPlan, None

                let joinPlan =
                    LogicalPlan.Join(
                        basePlan,
                        preAggRightPlan,
                        [ leftKey ],
                        [ rightKey ],
                        JoinKind.Left,
                        Some LinqConstants.DefaultRightSuffix,
                        JoinKeyCoalescing.Default,
                        false
                    )

                let filledPlan =
                    match aggColOpt with
                    | Some aggCol ->
                        let filledExpr =
                            ScalarExpr.Function(
                                ScalarFunction.FillNull,
                                [ ScalarExpr.Column aggCol; ScalarExpr.Literal(LiteralValue.UInt32 0u) ],
                                LogicalTypes.UInt32
                            )
                        LogicalPlan.WithColumns(joinPlan, [ (aggCol, filledExpr) ])
                    | None -> joinPlan

                let renames = extractBinaryResultRenames resSel None
                let finalPlan =
                    if renames.IsEmpty then filledPlan
                    else LogicalPlan.Rename(filledPlan, renames |> List.map fst, renames |> List.map snd)

                Some finalPlan
            | _ -> None)

    /// Pure symbolic 2-way Zip builder into HorizontalConcat and optional column renames.
    let buildZipPlan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (secondExpr: Expression)
        (resLambdaOpt: LambdaExpression option)
        : LogicalPlan option =
        resolveSequence secondExpr
        |> Option.map (fun otherPlan ->
            let baseCols = (SchemaInference.inferPlanSchema basePlan).ColumnNames |> Set.ofList
            let safeSecondPlan, _, suffixMap = disambiguatePlanColumns baseCols otherPlan "_second"
            let zipPlan = LogicalPlan.HorizontalConcat [ basePlan; safeSecondPlan ]
            match resLambdaOpt with
            | Some resLambda ->
                let renames =
                    extractBinaryResultRenames resLambda None
                    |> List.map (fun (srcCol, destCol) ->
                        suffixMap.TryFind srcCol |> Option.defaultValue srcCol, destCol)
                if renames.IsEmpty then zipPlan
                else LogicalPlan.Rename(zipPlan, renames |> List.map fst, renames |> List.map snd)
            | None -> zipPlan)

    /// Pure symbolic 3-way Zip builder into HorizontalConcat.
    let buildZip3Plan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (secondExpr: Expression)
        (thirdExpr: Expression)
        : LogicalPlan option =
        match
            resolveSequence secondExpr,
            resolveSequence thirdExpr
        with
        | Some secondPlan, Some thirdPlan ->
            let baseCols = (SchemaInference.inferPlanSchema basePlan).ColumnNames |> Set.ofList
            let safeSecondPlan, colsAfter2, _ = disambiguatePlanColumns baseCols secondPlan "_second"
            let safeThirdPlan, _, _ = disambiguatePlanColumns colsAfter2 thirdPlan "_third"
            Some(LogicalPlan.HorizontalConcat [ basePlan; safeSecondPlan; safeThirdPlan ])
        | _ -> None
