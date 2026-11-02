namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal OperatorAndUdfTranslator =

    let internal translateBinary (nodeType: ExpressionType) (left: ExprHandle) (right: ExprHandle) : ExprHandle =
        match nodeType with
        | ExpressionType.Equal -> PolarsWrapper.Eq(left, right)
        | ExpressionType.NotEqual -> PolarsWrapper.Neq(left, right)
        | ExpressionType.GreaterThan -> PolarsWrapper.Gt(left, right)
        | ExpressionType.GreaterThanOrEqual -> PolarsWrapper.GtEq(left, right)
        | ExpressionType.LessThan -> PolarsWrapper.Lt(left, right)
        | ExpressionType.LessThanOrEqual -> PolarsWrapper.LtEq(left, right)
        | ExpressionType.Add -> PolarsWrapper.Add(left, right)
        | ExpressionType.Subtract -> PolarsWrapper.Sub(left, right)
        | ExpressionType.Multiply -> PolarsWrapper.Mul(left, right)
        | ExpressionType.Divide -> PolarsWrapper.Div(left, right)
        | ExpressionType.Modulo -> PolarsWrapper.Rem(left, right)
        | ExpressionType.Power -> PolarsWrapper.Pow(left, right)
        | ExpressionType.And
        | ExpressionType.AndAlso -> PolarsWrapper.And(left, right)
        | ExpressionType.Or
        | ExpressionType.OrElse -> PolarsWrapper.Or(left, right)
        | ExpressionType.ExclusiveOr -> PolarsWrapper.Xor(left, right)
        | ExpressionType.Coalesce -> PolarsWrapper.Coalesce [| left; right |]
        | other -> failwithf "Binary operator '%A' is not currently supported in Polars LINQ" other

    let internal translateUnary (nodeType: ExpressionType) (operand: ExprHandle) : ExprHandle option =
        match nodeType with
        | ExpressionType.Not -> Some(PolarsWrapper.Not operand)
        | ExpressionType.Negate
        | ExpressionType.NegateChecked -> Some(PolarsWrapper.Neg operand)
        | ExpressionType.UnaryPlus -> Some operand
        | _ -> None

    let internal tryResolveEntityNullTarget (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
        let cleanE = unwrap expr
        let scopeOpt = tryExtractScopeName cleanE
        match scopeOpt with
        | Some scope ->
            let findColInScopeMap (scopeMap: Map<string * string, string>) =
                let nonNullablePropCol =
                    if not (isNull cleanE.Type) && not (PolarsTypeHelper.IsScalarType cleanE.Type) then
                        cleanE.Type.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                        |> Array.tryPick (fun prop ->
                            if prop.PropertyType.IsValueType && isNull (Nullable.GetUnderlyingType prop.PropertyType) then
                                scopeMap.TryFind(scope, prop.Name)
                                |> Option.orElseWith (fun () -> scopeMap.TryFind("", prop.Name))
                                |> Option.orElseWith (fun () ->
                                    scopeMap
                                    |> Map.tryPick (fun (_, c) pCol -> if c = prop.Name then Some pCol else None))
                            else
                                None)
                    else
                        None
                match nonNullablePropCol with
                | Some col -> Some(PolarsWrapper.Col col)
                | None ->
                    scopeMap
                    |> Map.toList
                    |> List.tryPick (fun ((s, _), physicalCol) ->
                        if s = scope then Some(PolarsWrapper.Col physicalCol) else None)

            match ctx with
            | TranslationContext.ScopedEntity(_, scopeMap, _)
            | TranslationContext.CorrelatedJoin(_, _, _, _, scopeMap) ->
                findColInScopeMap scopeMap
            | TranslationContext.BinaryJoin(leftParam, rightParam, leftCols, rightSuffix) ->
                if scope = leftParam then
                    leftCols |> Seq.tryHead |> Option.map PolarsWrapper.Col
                elif scope = rightParam then
                    leftCols |> Seq.tryHead |> Option.map (fun c -> PolarsWrapper.Col(c + rightSuffix))
                else None
            | TranslationContext.Merge(target, source, suffix) ->
                if scope = target then Some(PolarsWrapper.Col "")
                elif scope = source then Some(PolarsWrapper.Col suffix)
                else None
            | _ -> None
        | None -> None

    let internal translateBinaryExpr
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (op: ExpressionType)
        (left: Expression)
        (right: Expression)
        : ExprHandle option =
        let translateNullCheckTarget targetExpr =
            eval ctx targetExpr
            |> Option.orElseWith (fun () -> tryResolveEntityNullTarget ctx targetExpr)

        if op = ExpressionType.NotEqual && isNullConstantExpr right then
            translateNullCheckTarget left |> Option.map PolarsWrapper.IsNotNull
        elif op = ExpressionType.NotEqual && isNullConstantExpr left then
            translateNullCheckTarget right |> Option.map PolarsWrapper.IsNotNull
        elif op = ExpressionType.Equal && isNullConstantExpr right then
            translateNullCheckTarget left |> Option.map PolarsWrapper.IsNull
        elif op = ExpressionType.Equal && isNullConstantExpr left then
            translateNullCheckTarget right |> Option.map PolarsWrapper.IsNull
        elif op = ExpressionType.LeftShift then
            match eval ctx left, tryEvaluate right with
            | Some leftH, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some(PolarsWrapper.BitLeftShift(leftH, n))
                with _ ->
                    None
            | _ -> None

        elif op = ExpressionType.RightShift then
            match eval ctx left, tryEvaluate right with
            | Some leftH, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some(PolarsWrapper.BitRightShift(leftH, n))
                with _ ->
                    None
            | _ -> None
        // Intercept String concatenation (+) to synthesize native FormatString
        elif
            op = ExpressionType.Add
            && (left.Type = typeof<string> || right.Type = typeof<string>)
        then
            match eval ctx left, eval ctx right with
            | Some l, Some r -> Some(PolarsWrapper.FormatString("{}{}", [| l; r |]))
            | _ -> None
        else
            match eval ctx left, eval ctx right with
            | Some l, Some r -> Some(translateBinary op l r)
            | _ -> None

    let internal translateUnaryExpr
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (op: ExpressionType)
        (operand: Expression)
        (targetType: Type)
        : ExprHandle option =
        match op with
        | ExpressionType.Convert
        | ExpressionType.ConvertChecked ->
            eval ctx operand |> Option.bind (tryTranslateCast targetType)
        | _ -> eval ctx operand |> Option.bind (translateUnary op)

    // =========================================================================
    // 9b. Managed UDF Callback Translation (Tier 2 Pushdown)
    // =========================================================================

    let rec internal exprDependsOnContext (ctx: TranslationContext) (e: Expression) : bool =
        match e with
        | null -> false
        | :? ParameterExpression as p ->
            if isNull p.Name then false
            else ctx.ContainsParam p.Name
        | :? MemberExpression as m -> exprDependsOnContext ctx m.Expression
        | :? MethodCallExpression as mc ->
            not (isNull mc.Object) && exprDependsOnContext ctx mc.Object
            || mc.Arguments |> Seq.exists (exprDependsOnContext ctx)
        | :? BinaryExpression as b ->
            exprDependsOnContext ctx b.Left || exprDependsOnContext ctx b.Right
        | :? UnaryExpression as u -> exprDependsOnContext ctx u.Operand
        | :? ConditionalExpression as c ->
            exprDependsOnContext ctx c.Test || exprDependsOnContext ctx c.IfTrue || exprDependsOnContext ctx c.IfFalse
        | :? InvocationExpression as inv ->
            exprDependsOnContext ctx inv.Expression || (inv.Arguments |> Seq.exists (exprDependsOnContext ctx))
        | :? ConstantExpression -> false
        | _ -> false

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

    let internal tryTranslateUdfInvocation (eval: ExprEvaluator) (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
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
                            match eval ctx a with
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
                                Some(PolarsWrapper.Map(handle, arrowCallback, outDataType))

                            elif dynCount > 1 then
                                let userFunc = compileMultiArgUdfDelegate dynArgs expr
                                let inputTypes = dynArgs |> List.map (fun (a, _) -> a.Type) |> List.toArray
                                let arrowCallback = ArrowUdfBatchAdapter.CreateMulti(userFunc, inputTypes, expr.Type)
                                let mainHandle = snd dynArgs.[0]
                                let otherHandles = dynArgs |> List.tail |> List.map snd |> List.toArray

                                ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                                UdfTracker.Record(sprintf "%s: %s" methodName (expr.ToString()))
                                Some(PolarsWrapper.MapMany(mainHandle, otherHandles, arrowCallback, outDataType))
                            else
                                None
        with _ ->
            None

