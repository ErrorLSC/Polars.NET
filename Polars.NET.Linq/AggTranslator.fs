namespace Polars.NET.Linq

open System.Linq.Expressions
open Polars.NET.Core
open System

module AggTranslator =
    open ExprTranslator

    /// Checks if expression refers to the group parameter
    let rec private isGroupParam (groupParamName: string) (expr: Expression) =
        match unwrap expr with
        | null -> false
        | :? ParameterExpression as p -> p.Name = groupParamName
        | _ -> false

    /// Attempts to translate group collection operations into Polars ExprHandle
    let rec tryTranslateAgg (groupParamName: string) (wholeRowStructOpt: ExprHandle option) (expr: Expression) (aliasName: string) : ExprHandle option =
        let cleanExpr = unwrap expr

        match cleanExpr with
        | MethodCall(m, null, [ sepExpr; MethodCall(mSel, _, [ gArg; CleanLambda selLambda ]) ])
            when m.DeclaringType = typeof<string> && m.Name = "Join" && mSel.Name = "Select" &&
                 (match unwrap gArg with :? ParameterExpression as p -> p.Name = groupParamName | _ -> false) ->
            
            match tryEvaluate sepExpr with
            | Some (:? string as sep) ->
                let colExprOpt =
                    match tryResolveColumnName selLambda.Parameters.[0].Name selLambda.Body with
                    | Some cName -> Some (PolarsWrapper.Col cName)
                    | None -> tryTranslate selLambda.Parameters.[0].Name selLambda.Body

                colExprOpt
                |> Option.map (fun cExpr ->
                    let joined = PolarsWrapper.ListJoin(cExpr, sep, ignoreNulls = true)
                    if String.IsNullOrEmpty aliasName then joined
                    else PolarsWrapper.Alias(joined, aliasName)
                )
            | _ -> None
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
                match tryTranslate p.Name filterBody with
                | Some boolExpr ->
                    let int64Dtype = PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))
                    let castExpr = PolarsWrapper.ExprCast(boolExpr, int64Dtype, strict = false, wrapNumerical = false)
                    let countExpr = PolarsWrapper.Sum castExpr
                    Some (PolarsWrapper.Alias(countExpr, aliasName))
                | None -> None
            | _ -> None

        // 3. group.MinBy(x => x.ByCol).TargetCol / group.MaxBy(x => x.ByCol).TargetCol
        | MemberAccess(MethodCall(methodInfo, null, [ targetSeq; CleanLambda (Lambda([ p ], byBody)) ]), memberInfo)
            when (methodInfo.Name = "MinBy" || methodInfo.Name = "MaxBy") && isGroupParam groupParamName targetSeq ->
            match tryTranslate p.Name byBody with
            | Some byExpr ->
                let targetExpr = PolarsWrapper.Col memberInfo.Name
                let aggExpr =
                    match methodInfo.Name with
                    | "MinBy" -> Some (PolarsWrapper.MinBy(targetExpr, byExpr))
                    | "MaxBy" -> Some (PolarsWrapper.MaxBy(targetExpr, byExpr))
                    | _       -> None

                aggExpr |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))
            | None -> None

        // 4. group.Select(x => x.Col).Op()
        | MethodCall(outerMethod, null, [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]) ])
            when selectMethod.Name = "Select" && isGroupParam groupParamName targetSeq ->
            tryTranslate p.Name body
            |> Option.bind (translateReductionOp outerMethod.Name)
            |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))

        // 5. group.Sum(x => x.Field), group.Min(x => x.Field), group.Max(x => x.Field), group.Average(x => x.Field)
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]) 
            when isGroupParam groupParamName targetSeq ->
            tryTranslate p.Name body
            |> Option.bind (translateReductionOp methodInfo.Name)
            |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))

        // 6. Std(x => x.Salary, ddof) / group.Var(x => x.Salary, ddof)
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda (Lambda([ p ], body)); ddofExpr ]) 
            when isGroupParam groupParamName targetSeq ->
            match tryTranslate p.Name body, tryEvaluate ddofExpr with
            | Some targetH, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal
                translateDispersionOp methodInfo.Name ddof targetH
                |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))
            | _ -> None

        // 7. Nested List Aggregation:
        // group.Select(x => x.Col).ToList() / group.Select(x => x.Col).ToArray()
        | MethodCall(outerMethod, null, [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]) ])
            when (outerMethod.Name = "ToList" || outerMethod.Name = "ToArray") 
                && selectMethod.Name = "Select" 
                && isGroupParam groupParamName targetSeq ->
            ExprTranslator.tryTranslate p.Name body
            |> Option.map (fun colH ->
                let listExpr = PolarsWrapper.Implode(colH ,maintainOrder=true)
                PolarsWrapper.Alias(listExpr, aliasName))

        // 8. Whole entity collection: group.ToList() / group.ToArray()
        // Directly turns into pl.struct([colA, colB, ...]).implode().alias(aliasName)
        | MethodCall(methodInfo, null, [ targetSeq ])
            when (methodInfo.Name = "ToList" || methodInfo.Name = "ToArray") 
                && isGroupParam groupParamName targetSeq ->
            match wholeRowStructOpt with
            | Some rowStructExpr ->
                // Clone the struct expression handle to prevent double-free in Rust FFI
                let clonedStruct = PolarsWrapper.CloneExpr rowStructExpr
                let listStructExpr = PolarsWrapper.Implode(clonedStruct,maintainOrder=true)
                Some (PolarsWrapper.Alias(listStructExpr, aliasName))
            | None -> None

        // group.Select(x => x.Col).Quantile(q)
        | MethodCall(outerMethod, null, [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]); qExpr ])
            when outerMethod.Name = "Quantile" && selectMethod.Name = "Select" && isGroupParam groupParamName targetSeq ->
            match tryTranslate p.Name body, tryEvaluate qExpr with
            | Some colH, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    let qExprHandle = PolarsWrapper.Quantile(colH, q, PlQuantileMethod.Nearest)
                    Some (PolarsWrapper.Alias(qExprHandle, aliasName))
                with _ -> None
            | _ -> None

        // group.Select(x => x.Col).Quantile(q, method)
        | MethodCall(outerMethod, null, [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda (Lambda([ p ], body)) ]); qExpr; methodExpr ])
            when outerMethod.Name = "Quantile" && selectMethod.Name = "Select" && isGroupParam groupParamName targetSeq ->
            match tryTranslate p.Name body, tryEvaluate qExpr, tryEvaluate methodExpr with
            | Some colH, Some qVal, Some mVal ->
                try
                    let q = Convert.ToDouble qVal
                    let method = tryExtractQuantileMethod mVal
                    let qExprHandle = PolarsWrapper.Quantile(colH, q, method)
                    Some (PolarsWrapper.Alias(qExprHandle, aliasName))
                with _ -> None
            | _ -> None

        | _ -> None