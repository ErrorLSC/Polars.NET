namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Linq.Expressions

[<AutoOpen>]
module internal ExprTranslator =

    let internal unwrap e = ColumnResolver.unwrap e
    let internal toLiteralHandle (value: obj) (t: Type) = ColumnResolver.toLiteralHandle value t
    let internal tryTranslateCast targetType operand = ColumnResolver.tryTranslateCast targetType operand
    let internal tryResolveColumnNameWithContext ctx expr = ColumnResolver.tryResolveColumnNameWithContext ctx expr
    let internal translateReductionOp methodName targetColExpr = ColumnResolver.translateReductionOp methodName targetColExpr
    let internal translateDispersionOp methodName ddof targetColExpr = ColumnResolver.translateDispersionOp methodName ddof targetColExpr

    let internal translateBinary nodeType left right = OperatorAndUdfTranslator.translateBinary nodeType left right
    let internal translateUnary nodeType operand = OperatorAndUdfTranslator.translateUnary nodeType operand
    let internal isPolarsEngineMethod m = OperatorAndUdfTranslator.isPolarsEngineMethod m
    let internal isInternalOrLinqMethod m = OperatorAndUdfTranslator.isInternalOrLinqMethod m
    let internal compileSingleArgUdfDelegate argExpr expr = OperatorAndUdfTranslator.compileSingleArgUdfDelegate argExpr expr
    let internal compileMultiArgUdfDelegate dynArgs expr = OperatorAndUdfTranslator.compileMultiArgUdfDelegate dynArgs expr

    /// Root entrypoint for translating LINQ Expression AST nodes to native Polars Expr handles.
    let rec tryTranslateWithContext (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
        let eval = tryTranslateWithContext
        try
            match expr with
            // 0. Row-level String Split exact index access: s.Split(sep)[idx] -> StrSplitExact + StructFieldByIndex
            | SplitIndexAccess(target, sep, idx) ->
                let n =
                    match SplitOptContext.Current with
                    | Some map ->
                        match map.TryFind(target.ToString(), sep) with
                        | Some maxIdx -> maxIdx
                        | None -> idx
                    | None -> idx

                match eval ctx target with
                | Some t ->
                    let splitH = PolarsWrapper.StrSplitExact(t, PolarsWrapper.Lit sep, n, inclusive = false)
                    Some(PolarsWrapper.StructFieldByIndex(splitH, int64 idx))
                | _ -> None

            // 1. Row-level List / Array operations
            | ArrayIndexOp(target, indexExpr) ->
                match eval ctx target, eval ctx indexExpr with
                | Some t, Some idx -> Some(PolarsWrapper.ListGet(t, idx, nullOnOob = true))
                | _ -> None

            // 1. Regex operations
            | RegexInvocation invocation -> RegexTranslator.translateRegexInvocation eval ctx invocation

            // 1b. Row-level List / Array method or property invocations
            | ListInvocation invocation -> ListAndJsonTranslator.translateListInvocation eval ctx invocation

            // 1c. JSON operations (System.Text.Json)
            | JsonInvocation invocation -> ListAndJsonTranslator.translateJsonInvocation eval ctx invocation

            // 2. Struct Field Access
            | StructFieldAccess(parentExpr, fieldName) ->
                match eval ctx parentExpr with
                | Some parentH -> Some(PolarsWrapper.StructFieldByName(parentH, [| fieldName |]))
                | None -> None

            // 3. Constants and Literals
            | :? ConstantExpression as constExpr ->
                if isNull constExpr.Value then
                    Some(PolarsWrapper.LitNull())
                else
                    Some(toLiteralHandle constExpr.Value constExpr.Type)

            // 4. Member / Property Access
            | MemberAccess _ as memberExpr -> MemberAccessTranslator.translateMemberAccess eval ctx memberExpr

            // 5. Method Calls (Strings, DateTime, Math, Windows, Reductions, etc.)
            | MethodCall(m, target, args) ->
                match WindowAndMethodTranslator.translateMethodCall eval ctx m target args with
                | Some h -> Some h
                | None -> OperatorAndUdfTranslator.tryTranslateUdfInvocation eval ctx expr

            // 5b. Delegate Invocations (e.g. myFunc(x.A, x.B))
            | :? InvocationExpression -> OperatorAndUdfTranslator.tryTranslateUdfInvocation eval ctx expr

            // 6. Binary operations
            | Binary(op, left, right) -> OperatorAndUdfTranslator.translateBinaryExpr eval ctx op left right

            // 7. Unary operations
            | Unary(op, operand) -> OperatorAndUdfTranslator.translateUnaryExpr eval ctx op operand expr.Type

            // 8. Conditional Expression (Ternary: cond ? ifTrue : ifFalse)
            | :? ConditionalExpression as cond ->
                match eval ctx cond.Test, eval ctx cond.IfTrue, eval ctx cond.IfFalse with
                | Some testH, Some trueH, Some falseH -> Some(PolarsWrapper.IfElse(testH, trueH, falseH))
                | _ -> None

            // 9. Parameter mapping: delegate physical column resolution directly to TranslationContext
            | :? ParameterExpression as p ->
                if not (isNull p.Name) && (p.Name.StartsWith "__subquery_agg_" || p.Name.StartsWith "__subquery_col_") then
                    Some(PolarsWrapper.Col p.Name)
                elif not (isNull p.Name) && p.Name.StartsWith "__key_col_" then
                    Some(PolarsWrapper.Col(p.Name.Substring 10))
                else
                    ctx.TryResolveParameterHandle p.Name

            | _ -> None
        with _ ->
            None
