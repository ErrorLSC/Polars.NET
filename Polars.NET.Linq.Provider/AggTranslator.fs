namespace Polars.NET.Linq.Provider

open System.Linq.Expressions
open Polars.NET.Core

module AggTranslator =

    /// Strips Unary wraps like Convert or Quote recursively
    let rec private unwrap (e: Expression) =
        match e with
        | null -> null
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) -> unwrap inner
        | other -> other

    /// Checks if expression refers to the group parameter
    let rec private isGroupParam (groupParamName: string) (expr: Expression) =
        match unwrap expr with
        | null -> false
        | :? ParameterExpression as p -> p.Name = groupParamName
        | _ -> false

    /// Maps standard LINQ reduction method names to their corresponding native Polars aggregation expressions.
    /// Exposes internal accessibility so query compiler stages (e.g. GroupBy with ElementSelector) can share the mapping.
    let internal translateReductionOp (methodName: string) (targetColExpr: ExprHandle) : ExprHandle option =
        match methodName with
        | "Sum"     -> Some (PolarsWrapper.Sum targetColExpr)
        | "Average" -> Some (PolarsWrapper.Mean targetColExpr)
        | "Min"     -> Some (PolarsWrapper.Min targetColExpr)
        | "Max"     -> Some (PolarsWrapper.Max targetColExpr)
        | "First"   -> Some (PolarsWrapper.First(targetColExpr, ignoreNulls = false))
        | "Last"    -> Some (PolarsWrapper.Last(targetColExpr, ignoreNulls = false))
        | _         -> None

    /// Attempts to translate group collection operations into Polars ExprHandle
    let rec tryTranslateAgg (groupParamName: string) (expr: Expression) (aliasName: string) : ExprHandle option =
        let cleanExpr = unwrap expr

        match cleanExpr with
        // 1. group.Count() or group.LongCount() -> Len()
        | MethodCall(methodInfo, null, [ targetSeq ]) 
            when methodInfo.Name = "Count" || methodInfo.Name = "LongCount" ->
            let unwrappedTarget = unwrap targetSeq
            match unwrappedTarget with
            | :? ParameterExpression as p when p.Name = groupParamName ->
                let lenExpr = PolarsWrapper.Len()
                Some (PolarsWrapper.Alias(lenExpr, aliasName))
            | _ -> None

        // 2. group.Count(x => predicate) or group.LongCount(x => predicate)
        // Vectorized pushdown: (predicate_expr).Cast(Int64).Sum()
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda (Lambda([ p ], filterBody)) ]) 
            when methodInfo.Name = "Count" || methodInfo.Name = "LongCount" ->
            match unwrap targetSeq with
            | :? ParameterExpression as param when param.Name = groupParamName ->
                match ExprTranslator.tryTranslate p.Name filterBody with
                | Some boolExpr ->
                    let int64Dtype = PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))
                    let castExpr = PolarsWrapper.ExprCast(boolExpr, int64Dtype, strict = false, wrapNumerical = false)
                    let countExpr = PolarsWrapper.Sum(castExpr)
                    Some (PolarsWrapper.Alias(countExpr, aliasName))
                | None -> None
            | _ -> None

        // 3. group.MinBy(x => x.ByCol).TargetCol / group.MaxBy(x => x.ByCol).TargetCol
        | MemberAccess(MethodCall(methodInfo, null, [ targetSeq; CleanLambda (Lambda([ p ], byBody)) ]), memberInfo)
            when (methodInfo.Name = "MinBy" || methodInfo.Name = "MaxBy") && isGroupParam groupParamName targetSeq ->
            match ExprTranslator.tryTranslate p.Name byBody with
            | Some byExpr ->
                let targetExpr = PolarsWrapper.Col memberInfo.Name
                let aggExpr =
                    match methodInfo.Name with
                    | "MinBy" -> Some (PolarsWrapper.MinBy(targetExpr, byExpr))
                    | "MaxBy" -> Some (PolarsWrapper.MaxBy(targetExpr, byExpr))
                    | _       -> None

                aggExpr |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))
            | None -> None

        // 4. group.Select(x => x.Col).First() / group.Select(x => x.Col).Last()
        | MethodCall(outerMethod, null, [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]) ])
            when (outerMethod.Name = "First" || outerMethod.Name = "Last") && selectMethod.Name = "Select" ->
            match unwrap targetSeq with
            | :? ParameterExpression as param when param.Name = groupParamName ->
                ExprTranslator.tryTranslate p.Name body
                |> Option.bind (translateReductionOp outerMethod.Name)
                |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))
            | _ -> None

        // 5. group.Sum(x => x.Field), group.Min(x => x.Field), group.Max(x => x.Field), group.Average(x => x.Field)
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]) 
            when isGroupParam groupParamName targetSeq ->
            ExprTranslator.tryTranslate p.Name body
            |> Option.bind (translateReductionOp methodInfo.Name)
            |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))

        | _ -> None