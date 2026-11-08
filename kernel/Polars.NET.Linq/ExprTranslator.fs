namespace Polars.NET.Linq

open Polars.NET.Linq.IR
open System
open System.Linq.Expressions

[<AutoOpen>]
module internal ExprTranslator =

    /// Root entrypoint for translating LINQ Expression AST nodes to pure, symbolic ScalarExpr trees.
    let rec tryTranslateToScalar (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
        let eval = tryTranslateToScalar
        try
            match expr with
            // 0. Row-level String Split exact index access: s.Split(sep)[idx] -> StrSplitExact + StructFieldByIndex
            | SplitIndexAccess(target, sep, idx) ->
                let n =
                    match CompilationScope.Current.SplitIndexHints with
                    | Some map ->
                        match map.TryFind(target.ToString(), sep) with
                        | Some maxIdx -> maxIdx
                        | None -> idx
                    | None -> idx

                match eval ctx target with
                | Some t ->
                    let splitS = ScalarExpr.Function(ScalarFunction.SplitExact, [ t; ScalarExpr.Literal(LiteralValue.String sep); ScalarExpr.Literal(LiteralValue.Int32 n) ], LogicalType.Unknown)
                    Some(ScalarExpr.Function(ScalarFunction.StructFieldByIndex, [ splitS; ScalarExpr.Literal(LiteralValue.Int32 idx) ], LogicalTypes.String))
                | _ -> None

            // 1. Row-level List / Array operations
            | ArrayIndexOp(target, indexExpr) ->
                match eval ctx target, eval ctx indexExpr with
                | Some t, Some idx -> Some(ScalarExpr.ItemAccess(t, idx))
                | _ -> None

            // 1. Regex operations
            | RegexInvocation invocation -> RegexTranslator.translateRegexInvocation eval ctx invocation

            // 1b. Row-level List / Array method or property invocations
            | ListInvocation invocation -> ListTranslator.translateListInvocation eval ctx invocation

            // 1c. JSON operations (System.Text.Json)
            | JsonInvocation invocation -> JsonTranslator.translateJsonInvocation eval ctx invocation

            // 2. Struct Field Access
            | StructFieldAccess(parentExpr, fieldName) ->
                match eval ctx parentExpr with
                | Some parentS -> Some(ScalarExpr.Function(ScalarFunction.StructFieldByName, [ parentS; ScalarExpr.Literal(LiteralValue.String fieldName) ], LogicalType.Unknown))
                | None -> None

            // 3. Constants and Literals
            | :? ConstantExpression as constExpr ->
                if isNull constExpr.Value then
                    // A null of a known type (`(int?)null`) keeps it.
                    Some(ScalarExpr.Literal(LiteralValue.Null(LogicalTypes.tryOfClrType constExpr.Type |> Option.defaultValue LogicalTypes.Null)))
                else
                    Some(ScalarExprOps.toLiteral constExpr.Value constExpr.Type)

            // 4. Member / Property Access
            | MemberAccess _ as memberExpr -> MemberAccessTranslator.translateMemberAccess eval ctx memberExpr

            // 5. Method Calls (Strings, DateTime, Math, Windows, Reductions, etc.)
            | MethodCall(m, target, args) ->
                match WindowAndMethodTranslator.translateMethodCall eval ctx m target args with
                | Some s -> Some s
                | None -> OperatorAndUdfTranslator.tryTranslateUdfInvocation eval ctx expr

            // 5b. Delegate Invocations (e.g. myFunc(x.A, x.B))
            | :? InvocationExpression -> OperatorAndUdfTranslator.tryTranslateUdfInvocation eval ctx expr

            // 6. Binary operations
            | Binary(op, left, right) -> OperatorAndUdfTranslator.translateBinaryExpr eval ctx op left right expr.Type

            // 7. Unary operations
            | Unary(op, operand) -> OperatorAndUdfTranslator.translateUnaryExpr eval ctx op operand expr.Type

            // 8. Conditional Expression (Ternary: cond ? ifTrue : ifFalse)
            // C# evaluates only the branch the test selects: a UDF in a branch runs only where it is selected.
            | :? ConditionalExpression as cond ->
                let testOpt = eval ctx cond.Test
                let branch (guard: ScalarExpr -> ScalarExpr) (e: Expression) =
                    match testOpt with
                    | Some testS -> OperatorAndUdfTranslator.withUdfGuard ctx (guard testS) (fun () -> eval ctx e)
                    | None -> eval ctx e
                match testOpt, branch id cond.IfTrue, branch (fun t -> ScalarExpr.Unary(UnaryOp.Not, t)) cond.IfFalse with
                | Some testS, Some trueS, Some falseS -> Some(ScalarExpr.Conditional(testS, trueS, falseS))
                | _ ->
                    let hasParam (e: Expression) =
                        let rec check expr =
                            match unwrap expr with
                            | null -> false
                            | :? ParameterExpression as p -> ctx.ContainsParam p.Name
                            | MethodCall(_, target, args) -> not (isNull target) && check target || args |> List.exists check
                            | MemberAccess(target, _) -> not (isNull target) && check target
                            | Unary(_, inner) -> check inner
                            | Binary(_, left, right) -> check left || check right
                            | _ -> false
                        check e

                    let colExprOpt, fallbackExprOpt =
                        if hasParam cond.IfTrue && not (hasParam cond.IfFalse) then
                            Some cond.IfTrue, Some cond.IfFalse
                        elif hasParam cond.IfFalse && not (hasParam cond.IfTrue) then
                            Some cond.IfFalse, Some cond.IfTrue
                        else
                            None, None

                    match colExprOpt, fallbackExprOpt with
                    | Some colExpr, Some fallbackExpr ->
                        match eval ctx colExpr, eval ctx fallbackExpr with
                        | Some colS, Some fallbackS ->
                            Some(ScalarExpr.Function(ScalarFunction.FillNull, [ colS; fallbackS ], LogicalType.Unknown))
                        | _ -> None
                    | _ -> None

            // 9. Parameter mapping: delegate physical column resolution directly to TranslationContext
            | :? ParameterExpression as p ->
                if not (isNull p.Name) && (p.Name.StartsWith "__subquery_agg_" || p.Name.StartsWith "__subquery_col_") then
                    Some(ScalarExpr.Column p.Name)
                elif not (isNull p.Name) && p.Name.StartsWith "__key_col_" then
                    Some(ScalarExpr.Column(p.Name.Substring 10))
                else
                    ctx.TryResolveParameter p.Name

            | _ -> None
        with
        | :? InvalidQueryArgumentException -> reraise ()
        | _ -> None


