namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars
open System
open System.Linq.Expressions

[<AutoOpen>]
module internal ExprTranslator =

    let internal toLiteralHandle (value: obj) (t: Type) = toLiteral value t |> PolarsScalarLowerer.lower

    let internal tryTranslateCast targetType (operand: ExprHandle) =
        ColumnResolver.tryTranslateCast targetType (ScalarExpr.Opaque(operand, [], ""))
        |> Option.map PolarsScalarLowerer.lower

    let internal translateReductionOp methodName (targetColExpr: ExprHandle) =
        ColumnResolver.translateReductionOp methodName (ScalarExpr.Opaque(targetColExpr, [], ""))
        |> Option.map PolarsScalarLowerer.lower

    let internal translateDispersionOp methodName ddof (targetColExpr: ExprHandle) =
        ColumnResolver.translateDispersionOp methodName ddof (ScalarExpr.Opaque(targetColExpr, [], ""))
        |> Option.map PolarsScalarLowerer.lower

    let internal translateBinary nodeType (left: ExprHandle) (right: ExprHandle) : ExprHandle =
        OperatorAndUdfTranslator.translateBinary nodeType (ScalarExpr.Opaque(left, [], "")) (ScalarExpr.Opaque(right, [], ""))
        |> PolarsScalarLowerer.lower

    let internal translateUnary nodeType (operand: ExprHandle) : ExprHandle option =
        translateUnary nodeType (ScalarExpr.Opaque(operand, [], ""))
        |> Option.map PolarsScalarLowerer.lower

    /// Root entrypoint for translating LINQ Expression AST nodes to pure, symbolic ScalarExpr trees.
    let rec tryTranslateToScalar (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
        let eval = tryTranslateToScalar
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
                    let splitS = ScalarExpr.Function(ScalarFunction.SplitExact, [ t; ScalarExpr.Literal(LiteralValue.String sep); ScalarExpr.Literal(LiteralValue.Int32 n) ], PlDataType.Unknown)
                    Some(ScalarExpr.Function(ScalarFunction.StructFieldByIndex, [ splitS; ScalarExpr.Literal(LiteralValue.Int32 idx) ], PlDataType.String))
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
                | Some parentS -> Some(ScalarExpr.Function(ScalarFunction.StructFieldByName, [ parentS; ScalarExpr.Literal(LiteralValue.String fieldName) ], PlDataType.Unknown))
                | None -> None

            // 3. Constants and Literals
            | :? ConstantExpression as constExpr ->
                if isNull constExpr.Value then
                    Some(ScalarExpr.Literal(LiteralValue.Null PlDataType.Null))
                else
                    Some(ColumnResolver.toLiteral constExpr.Value constExpr.Type)

            // 4. Member / Property Access
            | MemberAccess _ as memberExpr -> MemberAccessTranslator.translateMemberAccess eval ctx memberExpr

            // 5. Method Calls (Strings, DateTime, Math, Windows, Reductions, etc.)
            | MethodCall(m, target, args) ->
                match WindowAndMethodTranslator.translateMethodCall eval ctx m target args with
                | Some s -> Some s
                | None -> OperatorAndUdfTranslator.tryTranslateUdfInvocationScalar eval ctx expr

            // 5b. Delegate Invocations (e.g. myFunc(x.A, x.B))
            | :? InvocationExpression -> OperatorAndUdfTranslator.tryTranslateUdfInvocationScalar eval ctx expr

            // 6. Binary operations
            | Binary(op, left, right) -> OperatorAndUdfTranslator.translateBinaryExpr eval ctx op left right

            // 7. Unary operations
            | Unary(op, operand) -> OperatorAndUdfTranslator.translateUnaryExpr eval ctx op operand expr.Type

            // 8. Conditional Expression (Ternary: cond ? ifTrue : ifFalse)
            | :? ConditionalExpression as cond ->
                match eval ctx cond.Test, eval ctx cond.IfTrue, eval ctx cond.IfFalse with
                | Some testS, Some trueS, Some falseS -> Some(ScalarExpr.Conditional(testS, trueS, falseS))
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
        with _ ->
            None

    /// Root entrypoint for translating LINQ Expression AST nodes to native Polars Expr handles.
    /// Pure pipeline: translates Expression -> ScalarExpr -> lowers to ExprHandle.
    let tryTranslateWithContext (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
        tryTranslateToScalar ctx expr
        |> Option.map PolarsScalarLowerer.lower
