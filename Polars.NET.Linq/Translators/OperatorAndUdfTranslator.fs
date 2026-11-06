namespace Polars.NET.Linq

open Polars.NET.Core.Config
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR
open System
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module internal OperatorAndUdfTranslator =

    /// The ScalarExpr of a binary operator over translated operands, or None for an operator without one.
    let internal tryTranslateBinary (nodeType: ExpressionType) (left: ScalarExpr) (right: ScalarExpr) : ScalarExpr option =
        match nodeType with
        | ExpressionType.Equal -> Some(ScalarExpr.Binary(BinaryOp.Eq, left, right))
        | ExpressionType.NotEqual -> Some(ScalarExpr.Binary(BinaryOp.NotEq, left, right))
        | ExpressionType.GreaterThan -> Some(ScalarExpr.Binary(BinaryOp.Gt, left, right))
        | ExpressionType.GreaterThanOrEqual -> Some(ScalarExpr.Binary(BinaryOp.GtEq, left, right))
        | ExpressionType.LessThan -> Some(ScalarExpr.Binary(BinaryOp.Lt, left, right))
        | ExpressionType.LessThanOrEqual -> Some(ScalarExpr.Binary(BinaryOp.LtEq, left, right))
        | ExpressionType.Add -> Some(ScalarExpr.Binary(BinaryOp.Add, left, right))
        | ExpressionType.Subtract -> Some(ScalarExpr.Binary(BinaryOp.Subtract, left, right))
        | ExpressionType.Multiply -> Some(ScalarExpr.Binary(BinaryOp.Multiply, left, right))
        | ExpressionType.Divide -> Some(ScalarExpr.Binary(BinaryOp.Divide, left, right))
        | ExpressionType.Modulo -> Some(ScalarExpr.Binary(BinaryOp.Modulo, left, right))
        | ExpressionType.Power -> Some(ScalarExpr.Binary(BinaryOp.Power, left, right))
        | ExpressionType.And
        | ExpressionType.AndAlso -> Some(ScalarExpr.Binary(BinaryOp.And, left, right))
        | ExpressionType.Or
        | ExpressionType.OrElse -> Some(ScalarExpr.Binary(BinaryOp.Or, left, right))
        | ExpressionType.ExclusiveOr -> Some(ScalarExpr.Binary(BinaryOp.Xor, left, right))
        | ExpressionType.Coalesce -> Some(ScalarExpr.Coalesce [ left; right ])
        | _ -> None

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
            match eval ctx left, PartialEvaluator.tryEvaluate right with
            | Some leftS, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    let dt = (LogicalTypes.tryOfClrType left.Type) |> Option.defaultValue LogicalType.Unknown
                    Some(ScalarExpr.Function(ScalarFunction.BitLeftShift, [ leftS; ScalarExpr.Literal(LiteralValue.Int32 n) ], dt))
                with ConversionFailure ->
                    None
            | _ -> None

        elif op = ExpressionType.RightShift then
            match eval ctx left, PartialEvaluator.tryEvaluate right with
            | Some leftS, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    let dt = (LogicalTypes.tryOfClrType left.Type) |> Option.defaultValue LogicalType.Unknown
                    Some(ScalarExpr.Function(ScalarFunction.BitRightShift, [ leftS; ScalarExpr.Literal(LiteralValue.Int32 n) ], dt))
                with ConversionFailure ->
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
            | Some l, Some r -> tryTranslateBinary op l r
            | _ -> None

    /// A conversion to or from an enum type. An enum operand becomes its underlying value and an enum target
    /// takes an underlying value, through EnumConvert nodes for enums stored as Polars Enums; EnumResolution later
    /// fits them to the column representation. Boxing keeps the enum itself.
    let private translateEnumConvert (operandExpr: ScalarExpr) (operandType: Type) (targetType: Type) : ScalarExpr option =
        if targetType = typeof<obj> then
            Some operandExpr
        else
            let number, numberType =
                match ScalarExprOps.tryEnumUnderlyingType operandType, ScalarExprOps.tryEnumMapping operandType with
                | Some underlying, Some mapping -> ScalarExpr.EnumConvert(EnumConversion.ToValue, operandExpr, mapping), underlying
                | Some underlying, None -> operandExpr, underlying
                | None, _ -> operandExpr, PolarsTypeHelper.UnwrapCoreType operandType

            match ScalarExprOps.tryEnumUnderlyingType targetType with
            | Some underlying ->
                let asUnderlying =
                    if numberType = underlying then Some number else tryTranslateCast underlying number

                match ScalarExprOps.tryEnumMapping targetType with
                | Some mapping -> asUnderlying |> Option.map (fun v -> ScalarExpr.EnumConvert(EnumConversion.FromValue, v, mapping))
                | None -> asUnderlying
            | None ->
                if PolarsTypeHelper.UnwrapCoreType targetType = numberType then Some number
                else tryTranslateCast targetType number

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
        | ExpressionType.ConvertChecked when
            (ScalarExprOps.tryEnumUnderlyingType operand.Type).IsSome
            || (ScalarExprOps.tryEnumUnderlyingType targetType).IsSome
            ->
            eval ctx operand |> Option.bind (fun x -> translateEnumConvert x operand.Type targetType)
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
            not (isNull mc.Object) && exprDependsOn paramPredicate mc.Object
            || mc.Arguments |> Seq.exists (exprDependsOn paramPredicate)
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
            asm = typeof<Polars.NET.Core.Arrow.ArrowTypeResolver>.Assembly
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

                    // Every dependent argument must be translatable to a symbolic ScalarExpr
                    let translatedDependentArgs =
                        dependentArgs
                        |> List.map (fun a ->
                            match eval ctx a with
                            | Some s -> Some(a, s)
                            | None -> None)

                    if translatedDependentArgs |> List.exists Option.isNone then
                        None
                    else
                        // An enum result leaves the UDF as its underlying value; one stored as a Polars Enum then
                        // takes its category (EnumConvert.FromValue), so an undefined value is an error.
                        let enumUnderlying = ScalarExprOps.tryEnumUnderlyingType expr.Type
                        let isOptionalEnum = enumUnderlying.IsSome && isNull (Nullable.GetUnderlyingType expr.Type) && not expr.Type.IsEnum

                        let isSupportedReturn =
                            match enumUnderlying with
                            | Some _ -> not isOptionalEnum
                            | None -> ScalarExprOps.isUdfReturnTypeSupported expr.Type

                        if not isSupportedReturn then
                            None
                        else
                            let udfReturnType, toUdfResult =
                                match enumUnderlying with
                                | Some underlying ->
                                    let returnType =
                                        if expr.Type.IsEnum then underlying
                                        else typedefof<Nullable<_>>.MakeGenericType underlying
                                    returnType, (fun (v: obj) -> if isNull v then null else Convert.ChangeType(v, underlying))
                                | None -> expr.Type, id

                            let wrapResult (udf: ScalarExpr) =
                                match ScalarExprOps.tryEnumMapping expr.Type with
                                | Some mapping -> ScalarExpr.EnumConvert(EnumConversion.FromValue, udf, mapping)
                                | None -> udf

                            let retPlType =
                                (LogicalTypes.tryOfClrType udfReturnType)
                                |> Option.defaultValue LogicalType.Unknown
                            let dynArgs = translatedDependentArgs |> List.choose id
                            let dynCount = dynArgs.Length

                            if dynCount = 1 then
                                let argExpr, scalarArg = dynArgs.[0]
                                let userFunc = compileSingleArgUdfDelegate argExpr expr
                                let resultFunc = Func<obj, obj>(fun x -> toUdfResult (userFunc.Invoke x))
                                let arrowCallback = ArrowUdfBatchAdapter.CreateSingle(resultFunc, argExpr.Type, udfReturnType)
                                ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                                CompilationScope.Current.RecordUdf(sprintf "%s: %s" methodName (expr.ToString()))
                                Some(wrapResult (ScalarExpr.Udf(methodName, [ scalarArg ], retPlType, UdfInvoker.SingleBatch(box arrowCallback, udfReturnType))))

                            elif dynCount > 1 then
                                let userFunc = compileMultiArgUdfDelegate dynArgs expr
                                let resultFunc = Func<obj array, obj>(fun xs -> toUdfResult (userFunc.Invoke xs))
                                let inputTypes = dynArgs |> List.map (fun (a, _) -> a.Type) |> List.toArray
                                let arrowCallback = ArrowUdfBatchAdapter.CreateMulti(resultFunc, inputTypes, udfReturnType)
                                let scalarArgs = dynArgs |> List.map snd

                                ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                                CompilationScope.Current.RecordUdf(sprintf "%s: %s" methodName (expr.ToString()))
                                Some(wrapResult (ScalarExpr.Udf(methodName, scalarArgs, retPlType, UdfInvoker.MultiBatch(box arrowCallback, udfReturnType))))
                            else
                                None
        with
        | :? InvalidQueryArgumentException -> reraise ()
        // Building the expression tree of the UDF delegate or compiling it.
        | :? ArgumentException
        | :? InvalidOperationException
        | :? NotSupportedException -> None

