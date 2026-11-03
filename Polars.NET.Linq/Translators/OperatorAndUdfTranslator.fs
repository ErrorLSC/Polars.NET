namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Arrow
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars
open System
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal OperatorAndUdfTranslator =

    let internal translateBinary (nodeType: ExpressionType) (left: ScalarExpr) (right: ScalarExpr) : ScalarExpr =
        match nodeType with
        | ExpressionType.Equal -> ScalarExpr.Binary(BinaryOp.Eq, left, right)
        | ExpressionType.NotEqual -> ScalarExpr.Binary(BinaryOp.NotEq, left, right)
        | ExpressionType.GreaterThan -> ScalarExpr.Binary(BinaryOp.Gt, left, right)
        | ExpressionType.GreaterThanOrEqual -> ScalarExpr.Binary(BinaryOp.GtEq, left, right)
        | ExpressionType.LessThan -> ScalarExpr.Binary(BinaryOp.Lt, left, right)
        | ExpressionType.LessThanOrEqual -> ScalarExpr.Binary(BinaryOp.LtEq, left, right)
        | ExpressionType.Add -> ScalarExpr.Binary(BinaryOp.Add, left, right)
        | ExpressionType.Subtract -> ScalarExpr.Binary(BinaryOp.Subtract, left, right)
        | ExpressionType.Multiply -> ScalarExpr.Binary(BinaryOp.Multiply, left, right)
        | ExpressionType.Divide -> ScalarExpr.Binary(BinaryOp.Divide, left, right)
        | ExpressionType.Modulo -> ScalarExpr.Binary(BinaryOp.Modulo, left, right)
        | ExpressionType.Power -> ScalarExpr.Binary(BinaryOp.Power, left, right)
        | ExpressionType.And
        | ExpressionType.AndAlso -> ScalarExpr.Binary(BinaryOp.And, left, right)
        | ExpressionType.Or
        | ExpressionType.OrElse -> ScalarExpr.Binary(BinaryOp.Or, left, right)
        | ExpressionType.ExclusiveOr -> ScalarExpr.Binary(BinaryOp.Xor, left, right)
        | ExpressionType.Coalesce -> ScalarExpr.Coalesce [ left; right ]
        | other -> failwithf "Binary operator '%A' is not currently supported in Polars LINQ" other

    let internal translateUnary (nodeType: ExpressionType) (operand: ScalarExpr) : ScalarExpr option =
        match nodeType with
        | ExpressionType.Not -> Some(ScalarExpr.Unary(UnaryOp.Not, operand))
        | ExpressionType.Negate
        | ExpressionType.NegateChecked -> Some(ScalarExpr.Unary(UnaryOp.Negate, operand))
        | ExpressionType.UnaryPlus -> Some operand
        | _ -> None


    let internal translateBinaryExpr
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (op: ExpressionType)
        (left: Expression)
        (right: Expression)
        : ScalarExpr option =
        let translateNullCheckTarget targetExpr =
            eval ctx targetExpr
            |> Option.orElseWith (fun () -> tryResolveEntityNullTarget ctx targetExpr)

        if op = ExpressionType.NotEqual && isNullConstantExpr right then
            translateNullCheckTarget left |> Option.map (fun x -> ScalarExpr.Unary(UnaryOp.IsNotNull, x))
        elif op = ExpressionType.NotEqual && isNullConstantExpr left then
            translateNullCheckTarget right |> Option.map (fun x -> ScalarExpr.Unary(UnaryOp.IsNotNull, x))
        elif op = ExpressionType.Equal && isNullConstantExpr right then
            translateNullCheckTarget left |> Option.map (fun x -> ScalarExpr.Unary(UnaryOp.IsNull, x))
        elif op = ExpressionType.Equal && isNullConstantExpr left then
            translateNullCheckTarget right |> Option.map (fun x -> ScalarExpr.Unary(UnaryOp.IsNull, x))
        elif op = ExpressionType.LeftShift then
            match eval ctx left, tryEvaluate right with
            | Some leftS, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some(ScalarExpr.Function(ScalarFunction.BitLeftShift, [ leftS; ScalarExpr.Literal(LiteralValue.Int32 n) ], PlDataType.Unknown))
                with _ ->
                    None
            | _ -> None

        elif op = ExpressionType.RightShift then
            match eval ctx left, tryEvaluate right with
            | Some leftS, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some(ScalarExpr.Function(ScalarFunction.BitRightShift, [ leftS; ScalarExpr.Literal(LiteralValue.Int32 n) ], PlDataType.Unknown))
                with _ ->
                    None
            | _ -> None
        // Intercept String concatenation (+) to synthesize native string concatenation
        elif
            op = ExpressionType.Add
            && (left.Type = typeof<string> || right.Type = typeof<string>)
        then
            match eval ctx left, eval ctx right with
            | Some l, Some r -> Some(ScalarExpr.Binary(BinaryOp.StringConcat, l, r))
            | _ -> None
        else
            match eval ctx left, eval ctx right with
            | Some l, Some r -> Some(translateBinary op l r)
            | _ -> None

    let internal translateUnaryExpr
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (op: ExpressionType)
        (operand: Expression)
        (targetType: Type)
        : ScalarExpr option =
        match op with
        | ExpressionType.Convert
        | ExpressionType.ConvertChecked when targetType = operand.Type || Nullable.GetUnderlyingType targetType = operand.Type ->
            eval ctx operand
        | ExpressionType.Convert
        | ExpressionType.ConvertChecked ->
            eval ctx operand |> Option.bind (tryTranslateCast targetType)
        | _ -> eval ctx operand |> Option.bind (translateUnary op)

    // =========================================================================
    // 9b. Managed UDF Callback Translation (Tier 2 Pushdown)
    // =========================================================================

    let rec internal exprDependsOn (paramPredicate: string -> bool) (e: Expression) : bool =
        match e with
        | null -> false
        | :? ParameterExpression as p ->
            if isNull p.Name then false
            else paramPredicate p.Name
        | :? MemberExpression as m -> exprDependsOn paramPredicate m.Expression
        | :? MethodCallExpression as mc ->
            (not (isNull mc.Object) && exprDependsOn paramPredicate mc.Object)
            || (mc.Arguments |> Seq.exists (exprDependsOn paramPredicate))
        | :? BinaryExpression as b ->
            exprDependsOn paramPredicate b.Left || exprDependsOn paramPredicate b.Right
        | :? UnaryExpression as u -> exprDependsOn paramPredicate u.Operand
        | :? ConditionalExpression as c ->
            exprDependsOn paramPredicate c.Test || exprDependsOn paramPredicate c.IfTrue || exprDependsOn paramPredicate c.IfFalse
        | :? InvocationExpression as inv ->
            exprDependsOn paramPredicate inv.Expression || (inv.Arguments |> Seq.exists (exprDependsOn paramPredicate))
        | :? LambdaExpression as l ->
            exprDependsOn paramPredicate l.Body
        | :? ConstantExpression -> false
        | _ -> false

    let internal exprDependsOnParam (paramName: string) (e: Expression) : bool =
        exprDependsOn ((=) paramName) e

    let internal exprDependsOnContext (ctx: TranslationContext) (e: Expression) : bool =
        exprDependsOn ctx.ContainsParam e

    let internal replaceSubtrees (replacements: (Expression * Expression) list) (root: Expression) : Expression =
        let visitor =
            { new ExpressionVisitor() with
                override this.Visit(node: Expression) =
                    match replacements |> List.tryFind (fun (orig, _) -> Object.ReferenceEquals(orig, node)) with
                    | Some(_, repl) -> repl
                    | None -> base.Visit(node) }
        visitor.Visit(root)

    /// Checks whether a method belongs to the internal Polars.NET engine or framework SDKs
    let internal isPolarsEngineMethod (m: MethodInfo) : bool =
        if isNull m || isNull m.DeclaringType then false
        else
            let asm = m.DeclaringType.Assembly
            asm = typeof<ExprHandle>.Assembly
            || asm = typeof<TranslationContext>.Assembly
            || let name = asm.GetName().Name in name = "Polars.CSharp" || name = "Polars.FSharp"

    /// Checks whether a method is an internal LINQ query operator or Polars engine method
    let internal isInternalOrLinqMethod (m: MethodInfo) : bool =
        if isNull m || isNull m.DeclaringType then false
        else
            m.DeclaringType = typeof<System.Linq.Queryable>
            || isPolarsEngineMethod m

    /// Synthesizes and compiles a Func<obj, obj> delegate by replacing a single argument expression with a parameter.
    let internal compileSingleArgUdfDelegate (argExpr: Expression) (expr: Expression) : Func<obj, obj> =
        let inParam = Expression.Parameter(typeof<obj>, "input")
        let typedIn = Expression.Convert(inParam, argExpr.Type)
        let bodyWithParam = replaceSubtrees [ (argExpr, typedIn :> Expression) ] expr
        let boxedBody = Expression.Convert(bodyWithParam, typeof<obj>)
        let lambda = Expression.Lambda<Func<obj, obj>>(boxedBody, inParam)
        lambda.Compile()

    /// Synthesizes and compiles a Func<obj[], obj> delegate by replacing multiple argument expressions with array index accesses.
    let internal compileMultiArgUdfDelegate (dynArgs: (Expression * 'T) list) (expr: Expression) : Func<obj[], obj> =
        let inArrayParam = Expression.Parameter(typeof<obj[]>, "inputs")
        let replacements =
            dynArgs
            |> List.mapi (fun i (argExpr, _) ->
                let arrayIdx = Expression.ArrayIndex(inArrayParam, Expression.Constant(i))
                let typedIn = Expression.Convert(arrayIdx, argExpr.Type)
                argExpr, typedIn :> Expression)

        let bodyWithParams = replaceSubtrees replacements expr
        let boxedBody = Expression.Convert(bodyWithParams, typeof<obj>)
        let lambda = Expression.Lambda<Func<obj[], obj>>(boxedBody, inArrayParam)
        lambda.Compile()

    let internal tryTranslateUdfInvocation (eval: ScalarExprEvaluator) (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
        try
            if CoreConfig.DisallowUdfPushdown || isNull expr || expr.Type = typeof<System.Void> then
                None
            else
                let methodInfoOpt, rawArgs =
                    match expr with
                    | :? MethodCallExpression as mc ->
                        if isInternalOrLinqMethod mc.Method then
                            None, []
                        else
                            let args =
                                if isNull mc.Object then
                                    mc.Arguments |> Seq.toList
                                else
                                    mc.Object :: (mc.Arguments |> Seq.toList)
                            Some mc.Method, args
                    | :? InvocationExpression as inv ->
                        let args = inv.Arguments |> Seq.toList
                        None, args
                    | _ -> None, []

                let methodName =
                    match methodInfoOpt with
                    | Some m -> m.Name
                    | None -> "Invoke"

                if rawArgs.IsEmpty || not (rawArgs |> List.exists (exprDependsOnContext ctx)) then
                    None
                else
                    // Collect all argument expressions that depend on the row context
                    let dependentArgs =
                        rawArgs
                        |> List.filter (exprDependsOnContext ctx)

                    // Every dependent argument must be translatable to a Polars ExprHandle
                    let translatedDependentArgs =
                        dependentArgs
                        |> List.map (fun a ->
                            match eval ctx a |> Option.map PolarsScalarLowerer.lower with
                            | Some h -> Some(a, h)
                            | None -> None)

                    if translatedDependentArgs |> List.exists Option.isNone then
                        None
                    else
                        let outDataTypeOpt =
                            try
                                let h = ArrowUdfBatchAdapter.GetDataTypeHandle(expr.Type)
                                if isNull h || h.IsInvalid then None else Some h
                            with _ ->
                                None

                        match outDataTypeOpt with
                        | None -> None
                        | Some outDataType ->
                            let dynArgs = translatedDependentArgs |> List.choose id
                            let dynCount = dynArgs.Length

                            if dynCount = 1 then
                                let argExpr, handle = dynArgs.[0]
                                let userFunc = compileSingleArgUdfDelegate argExpr expr
                                let arrowCallback = ArrowUdfBatchAdapter.CreateSingle(userFunc, argExpr.Type, expr.Type)
                                ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                                UdfTracker.Record(sprintf "%s: %s" methodName (expr.ToString()))
                                Some(ScalarExpr.Opaque(PolarsWrapper.Map(handle, arrowCallback, outDataType), [], "UDF"))

                            elif dynCount > 1 then
                                let userFunc = compileMultiArgUdfDelegate dynArgs expr
                                let inputTypes = dynArgs |> List.map (fun (a, _) -> a.Type) |> List.toArray
                                let arrowCallback = ArrowUdfBatchAdapter.CreateMulti(userFunc, inputTypes, expr.Type)
                                let mainHandle = snd dynArgs.[0]
                                let otherHandles = dynArgs |> List.tail |> List.map snd |> List.toArray

                                ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                                UdfTracker.Record(sprintf "%s: %s" methodName (expr.ToString()))
                                Some(ScalarExpr.Opaque(PolarsWrapper.MapMany(mainHandle, otherHandles, arrowCallback, outDataType), [], "UDF"))
                            else
                                None
        with _ ->
            None

