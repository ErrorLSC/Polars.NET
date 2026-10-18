namespace Polars.NET.Linq

open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers
open System

module AggTranslator =
    open ExprTranslator

    /// Checks if expression refers to the group parameter
    let rec private isGroupParam (groupParamName: string) (expr: Expression) =
        match unwrap expr with
        | null -> false
        | :? ParameterExpression as p -> p.Name = groupParamName
        | _ -> false

    /// Resolves the appropriate TranslationContext for inner group lambdas based on parameter type
    let private getParamContext (scalarColOpt: string option) (p: ParameterExpression) : TranslationContext =
        if PolarsTypeHelper.IsScalarType p.Type || p.Type = typeof<string> then
            match scalarColOpt with
            | Some col when not (String.IsNullOrEmpty col) -> TranslationContext.Scalar(p.Name, col)
            | _ -> TranslationContext.Entity p.Name
        else
            TranslationContext.Entity p.Name

    /// Attempts to translate group collection operations into native Polars ExprHandle
    let rec tryTranslateAggWithScalarCol
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (expr: Expression)
        (aliasName: string)
        : ExprHandle option =

        let cleanExpr = unwrap expr

        match cleanExpr with
        // string.Join(sep, group.Select(x => x.Col))
        | MethodCall(m, null, [ sepExpr; MethodCall(mSel, _, [ gArg; CleanLambda selLambda ]) ]) when
            m.DeclaringType = typeof<string>
            && m.Name = "Join"
            && mSel.Name = "Select"
            && (match unwrap gArg with
                | :? ParameterExpression as p -> p.Name = groupParamName
                | _ -> false)
            ->

            let sepOpt =
                match tryEvaluate sepExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some (string sep)
                | _ -> None

            match sepOpt with
            | Some sep ->
                let p = selLambda.Parameters.[0]
                let ctx = getParamContext scalarColOpt p

                let colExprOpt =
                    match tryResolveColumnNameWithContext ctx selLambda.Body with
                    | Some cName -> Some(PolarsWrapper.Col cName)
                    | None -> tryTranslateWithContext ctx selLambda.Body

                colExprOpt
                |> Option.map (fun cExpr ->
                    let listExpr = PolarsWrapper.Implode(cExpr, maintainOrder = true)
                    let joined = PolarsWrapper.ListJoin(listExpr, sep, ignoreNulls = true)

                    if String.IsNullOrEmpty aliasName then
                        joined
                    else
                        PolarsWrapper.Alias(joined, aliasName))
            | _ -> None

        // string.Join(sep, group) when group elements are scalar string (scalarColOpt is Some col)
        | MethodCall(m, null, [ sepExpr; gArg ]) when
            m.DeclaringType = typeof<string>
            && m.Name = "Join"
            && isGroupParam groupParamName gArg
            && scalarColOpt.IsSome
            ->

            let sepOpt =
                match tryEvaluate sepExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some (string sep)
                | _ -> None

            match sepOpt, scalarColOpt with
            | Some sep, Some cName ->
                let cExpr = PolarsWrapper.Col cName
                let listExpr = PolarsWrapper.Implode(cExpr, maintainOrder = true)
                let joined = PolarsWrapper.ListJoin(listExpr, sep, ignoreNulls = true)

                if String.IsNullOrEmpty aliasName then
                    Some joined
                else
                    Some(PolarsWrapper.Alias(joined, aliasName))
            | _ -> None

        // 1. group.Count() or group.LongCount() -> Len()
        | MethodCall(methodInfo, null, [ targetSeq ]) when methodInfo.Name = "Count" || methodInfo.Name = "LongCount" ->
            let unwrappedTarget = unwrap targetSeq

            match unwrappedTarget with
            | :? ParameterExpression as p when p.Name = groupParamName ->
                let lenExpr = PolarsWrapper.Len()
                Some(PolarsWrapper.Alias(lenExpr, aliasName))
            | _ -> None

        // 2. group.Count(x => predicate) or group.LongCount(x => predicate)
        // Vectorized pushdown: (predicate_expr).Cast(Int64).Sum()
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda(Lambda([ p ], filterBody)) ]) when
            methodInfo.Name = "Count" || methodInfo.Name = "LongCount"
            ->
            match unwrap targetSeq with
            | :? ParameterExpression as param when param.Name = groupParamName ->
                let ctx = getParamContext scalarColOpt p

                match tryTranslateWithContext ctx filterBody with
                | Some boolExpr ->
                    let int64Dtype =
                        PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))

                    let castExpr =
                        PolarsWrapper.ExprCast(boolExpr, int64Dtype, strict = false, wrapNumerical = false)

                    let countExpr = PolarsWrapper.Sum castExpr
                    Some(PolarsWrapper.Alias(countExpr, aliasName))
                | None -> None
            | _ -> None

        // 3. group.MinBy(x => x.ByCol).TargetCol / group.MaxBy(x => x.ByCol).TargetCol
        | MemberAccess(MethodCall(methodInfo, null, [ targetSeq; CleanLambda(Lambda([ p ], byBody)) ]), memberInfo) when
            (methodInfo.Name = "MinBy" || methodInfo.Name = "MaxBy")
            && isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            match tryTranslateWithContext ctx byBody with
            | Some byExpr ->
                let targetExpr = PolarsWrapper.Col memberInfo.Name

                let aggExpr =
                    match methodInfo.Name with
                    | "MinBy" -> Some(PolarsWrapper.MinBy(targetExpr, byExpr))
                    | "MaxBy" -> Some(PolarsWrapper.MaxBy(targetExpr, byExpr))
                    | _ -> None

                aggExpr |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))
            | None -> None

        // 4. group.Select(x => x.Col).Op()
        | MethodCall(outerMethod,
                     null,
                     [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda(Lambda([ p ], body)) ]) ]) when
            selectMethod.Name = "Select" && isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            tryTranslateWithContext ctx body
            |> Option.bind (translateReductionOp outerMethod.Name)
            |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))

        // 5. group.Sum(x => x.Field), group.Min(x => x.Field), group.Max(x => x.Field), group.Average(x => x.Field)
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda(Lambda([ p ], body)) ]) when
            isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            tryTranslateWithContext ctx body
            |> Option.bind (translateReductionOp methodInfo.Name)
            |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))

        // 6. Std(x => x.Salary, ddof) / group.Var(x => x.Salary, ddof)
        | MethodCall(methodInfo, null, [ targetSeq; CleanLambda(Lambda([ p ], body)); ddofExpr ]) when
            isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            match tryTranslateWithContext ctx body, tryEvaluate ddofExpr with
            | Some targetH, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal

                translateDispersionOp methodInfo.Name ddof targetH
                |> Option.map (fun e -> PolarsWrapper.Alias(e, aliasName))
            | _ -> None

        // 7. Nested List Aggregation:
        // group.Select(x => x.Col).ToList() / group.Select(x => x.Col).ToArray()
        | MethodCall(outerMethod,
                     null,
                     [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda(Lambda([ p ], body)) ]) ]) when
            (outerMethod.Name = "ToList" || outerMethod.Name = "ToArray")
            && selectMethod.Name = "Select"
            && isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            tryTranslateWithContext ctx body
            |> Option.map (fun colH ->
                let listExpr = PolarsWrapper.Implode(colH, maintainOrder = true)
                PolarsWrapper.Alias(listExpr, aliasName))

        // 8. Whole entity collection: group.ToList() / group.ToArray()
        // Directly turns into pl.struct([colA, colB, ...]).implode().alias(aliasName)
        | MethodCall(methodInfo, null, [ targetSeq ]) when
            (methodInfo.Name = "ToList" || methodInfo.Name = "ToArray")
            && isGroupParam groupParamName targetSeq
            ->
            match wholeRowStructOpt with
            | Some rowStructExpr ->
                let clonedStruct = PolarsWrapper.CloneExpr rowStructExpr
                let listStructExpr = PolarsWrapper.Implode(clonedStruct, maintainOrder = true)
                Some(PolarsWrapper.Alias(listStructExpr, aliasName))
            | None -> None

        // 9. group.Select(x => x.Col).Quantile(q)
        | MethodCall(outerMethod,
                     null,
                     [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda(Lambda([ p ], body)) ]); qExpr ]) when
            outerMethod.Name = "Quantile"
            && selectMethod.Name = "Select"
            && isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            match tryTranslateWithContext ctx body, tryEvaluate qExpr with
            | Some colH, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    let qExprHandle = PolarsWrapper.Quantile(colH, q, PlQuantileMethod.Nearest)
                    Some(PolarsWrapper.Alias(qExprHandle, aliasName))
                with _ ->
                    None
            | _ -> None

        // 10. group.Select(x => x.Col).Quantile(q, method)
        | MethodCall(outerMethod,
                     null,
                     [ MethodCall(selectMethod, null, [ targetSeq; CleanLambda(Lambda([ p ], body)) ])
                       qExpr
                       methodExpr ]) when
            outerMethod.Name = "Quantile"
            && selectMethod.Name = "Select"
            && isGroupParam groupParamName targetSeq
            ->
            let ctx = getParamContext scalarColOpt p

            match tryTranslateWithContext ctx body, tryEvaluate qExpr, tryEvaluate methodExpr with
            | Some colH, Some qVal, Some mVal ->
                try
                    let q = Convert.ToDouble qVal
                    let method = tryExtractQuantileMethod mVal
                    let qExprHandle = PolarsWrapper.Quantile(colH, q, method)
                    Some(PolarsWrapper.Alias(qExprHandle, aliasName))
                with _ ->
                    None
            | _ -> None

        // 11. Binary arithmetic or logical expressions over group aggregations: e.g. 100.0 * (double)g.Sum(...) / (double)g.Sum(...)
        | Binary(op, left, right) ->
            match
                tryTranslateAggWithScalarCol groupParamName scalarColOpt wholeRowStructOpt left "",
                tryTranslateAggWithScalarCol groupParamName scalarColOpt wholeRowStructOpt right ""
            with
            | Some leftH, Some rightH ->
                let binH = ExprTranslator.translateBinary op leftH rightH

                if String.IsNullOrEmpty aliasName then
                    Some binH
                else
                    Some(PolarsWrapper.Alias(binH, aliasName))
            | _ -> None

        // 12. Unary operations / conversions on aggregations: e.g. (double)g.Sum(...)
        | Unary(op, operand) ->
            tryTranslateAggWithScalarCol groupParamName scalarColOpt wholeRowStructOpt operand ""
            |> Option.bind (fun innerH ->
                match op with
                | ExpressionType.Convert
                | ExpressionType.ConvertChecked ->
                    ExprTranslator.tryTranslateCast cleanExpr.Type innerH
                | _ ->
                    ExprTranslator.translateUnary op innerH)
            |> Option.map (fun uH ->
                if String.IsNullOrEmpty aliasName then
                    uH
                else
                    PolarsWrapper.Alias(uH, aliasName))

        // 13. Constant values inside aggregation arithmetic expressions: e.g. 100.0, 100.00m
        | Constant(v, t) ->
            let litH = ExprTranslator.toLiteralHandle v t

            if String.IsNullOrEmpty aliasName then
                Some litH
            else
                Some(PolarsWrapper.Alias(litH, aliasName))

        | _ -> None

