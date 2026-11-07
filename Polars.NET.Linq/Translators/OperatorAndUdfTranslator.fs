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


    let private isDecimalClrType (t: Type) = (match Nullable.GetUnderlyingType t with null -> t | u -> u) = typeof<decimal>

    /// An integer type (or Nullable of one), excluding enums.
    let private isIntegralClrType (t: Type) =
        let core = match Nullable.GetUnderlyingType t with null -> t | u -> u
        not core.IsEnum
        && (match Type.GetTypeCode core with
            | TypeCode.SByte | TypeCode.Byte | TypeCode.Int16 | TypeCode.UInt16
            | TypeCode.Int32 | TypeCode.UInt32 | TypeCode.Int64 | TypeCode.UInt64 -> true
            | _ -> false)

    /// Translates with `guard` in effect: a UDF translated by `translate` in context `ctx` runs only on rows where
    /// the guard is true, as C# evaluates the right of `&&` / `||`, a `?:` branch and the right of `??` only when
    /// reached. A guard that aggregates or reads a window is not usable row by row and is not added.
    let internal withUdfGuard (ctx: TranslationContext) (guard: ScalarExpr) (translate: unit -> 'T) : 'T =
        match CompilationScope.TryCurrent with
        | Some scope when not (ScalarExprOps.containsAggregation guard || ScalarExprOps.containsWindow guard) ->
            let saved = scope.UdfGuards
            scope.UdfGuards <- (box ctx, guard) :: saved
            try translate () finally scope.UdfGuards <- saved
        | _ -> translate ()

    /// The conjunction of the guards in effect for context `ctx`, if any.
    let private currentUdfGuard (ctx: TranslationContext) : ScalarExpr option =
        let guards =
            match CompilationScope.TryCurrent with
            | Some scope -> scope.UdfGuards |> List.filter (fun (c, _) -> Object.Equals(c, box ctx)) |> List.map snd |> List.rev
            | None -> []
        match guards with
        | [] -> None
        | first :: rest -> Some(rest |> List.fold (fun acc g -> ScalarExpr.Binary(BinaryOp.And, acc, g)) first)

    /// Combines the translated operands of a binary operator with C# semantics: C# null semantics for equality and
    /// lifted comparisons (NullSemantics), .NET NaN semantics (NaNSemantics), truncating integer division and
    /// decimal division. Shared by every translator that compiles a binary operator from its operands.
    let internal combineBinary
        (op: ExpressionType)
        (leftType: Type, leftS: ScalarExpr option)
        (rightType: Type, rightS: ScalarExpr option)
        (resultType: Type)
        : ScalarExpr option =
        let isComparison =
            match op with
            | ExpressionType.Equal | ExpressionType.NotEqual
            | ExpressionType.LessThan | ExpressionType.LessThanOrEqual
            | ExpressionType.GreaterThan | ExpressionType.GreaterThanOrEqual -> true
            | _ -> false
        if isComparison && resultType = typeof<Nullable<bool>> then
            None
        elif op = ExpressionType.Add && (leftType = typeof<string> || rightType = typeof<string>) then
            match leftS, rightS with
            | Some l, Some r -> Some(ScalarExpr.Binary(BinaryOp.StringConcat, l, r))
            | _ -> None
        else
            match leftS, rightS with
            // A value known only at run time (a captured variable) that is null: C# `==` is a null check.
            | Some(ScalarExpr.Literal(LiteralValue.Null _)), Some other
            | Some other, Some(ScalarExpr.Literal(LiteralValue.Null _)) when op = ExpressionType.Equal || op = ExpressionType.NotEqual ->
                Some(ScalarExpr.Unary((if op = ExpressionType.Equal then UnaryOp.IsNull else UnaryOp.IsNotNull), other))
            // C# null semantics (EF Core's default, see NullSemantics): equality is two-valued, and a lifted
            // ordering comparison is false on a null operand.
            // .NET floating semantics (see NaNSemantics): comparisons involving NaN are false, `!=` true.
            | Some l, Some r when (op = ExpressionType.Equal || op = ExpressionType.NotEqual) && resultType = typeof<bool> ->
                NullSemantics.equality (op = ExpressionType.Equal) (leftType, l) (rightType, r)
                |> NaNSemantics.comparison op (leftType, l) (rightType, r)
                |> Some
            | Some l, Some r when
                op = ExpressionType.LessThan
                || op = ExpressionType.LessThanOrEqual
                || op = ExpressionType.GreaterThan
                || op = ExpressionType.GreaterThanOrEqual
                ->
                tryTranslateBinary op l r
                |> Option.map (NaNSemantics.comparison op (leftType, l) (rightType, r))
                |> Option.map (NullSemantics.liftedComparison resultType [ leftType, l; rightType, r ])
            // .NET integer division truncates toward zero.
            | Some l, Some r when op = ExpressionType.Divide && isIntegralClrType leftType && isIntegralClrType rightType ->
                Some(ScalarExpr.Binary(BinaryOp.TruncatedDivide, l, r))
            // .NET decimal division keeps up to 28 significant digits; a fixed-point engine keeps the dividend's
            // scale, so the dividend is widened to the scale System.Decimal maps to (18).
            // A zero divisor gives null, as for integers (the engine raises on a decimal division by zero).
            | Some l, Some r when op = ExpressionType.Divide && (isDecimalClrType leftType || isDecimalClrType rightType) ->
                let divisor =
                    match r with
                    | ScalarExpr.Literal(LiteralValue.Decimal d) when d <> 0m -> r
                    | _ ->
                        ScalarExpr.Conditional(
                            ScalarExpr.Binary(BinaryOp.Eq, r, ScalarExpr.Literal(LiteralValue.Int32 0)),
                            ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Decimal),
                            r)
                Some(ScalarExpr.Binary(BinaryOp.Divide, ScalarExpr.Cast(l, LogicalTypes.Decimal, false), divisor))
            | Some l, Some r -> tryTranslateBinary op l r
            | _ -> None

    let internal translateBinaryExpr
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (op: ExpressionType)
        (left: Expression)
        (right: Expression)
        (resultType: Type)
        : ScalarExpr option =
        let translateNullCheckTarget targetExpr =
            eval ctx targetExpr
            |> Option.orElseWith (fun () -> tryResolveEntityNullTarget ctx targetExpr)

        let isComparison =
            match op with
            | ExpressionType.Equal | ExpressionType.NotEqual
            | ExpressionType.LessThan | ExpressionType.LessThanOrEqual
            | ExpressionType.GreaterThan | ExpressionType.GreaterThanOrEqual -> true
            | _ -> false

        // A comparison lifted to null (`bool?` result, SQL three-valued logic) comes only from hand-built or
        // VB expression trees; it is not translated natively (see SEMANTICS.md).
        if isComparison && resultType = typeof<Nullable<bool>> then
            None
        elif op = ExpressionType.NotEqual && isNullConstantExpr right then
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
            // C# evaluates the right of `&&` / `||` / `??` only when the left does not decide the result.
            let leftS = eval ctx left
            let rightS =
                match op, leftS with
                | ExpressionType.AndAlso, Some l -> withUdfGuard ctx l (fun () -> eval ctx right)
                | ExpressionType.OrElse, Some l -> withUdfGuard ctx (ScalarExpr.Unary(UnaryOp.Not, l)) (fun () -> eval ctx right)
                | ExpressionType.Coalesce, Some l -> withUdfGuard ctx (ScalarExpr.Unary(UnaryOp.IsNull, l)) (fun () -> eval ctx right)
                | _ -> eval ctx right
            combineBinary op (left.Type, leftS) (right.Type, rightS) resultType

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
        // .NET converts a floating value to an integer by truncating and saturating: NaN is 0, and values beyond
        // the range give MinValue / MaxValue (the engine's cast gives null for them). A type narrower than int is
        // reached through int: saturate to int, then narrow with wraparound ((byte)-2.7 is 254).
        | ExpressionType.Convert when NaNSemantics.isFloatingClrType operand.Type && isIntegralClrType targetType ->
            let target = match Nullable.GetUnderlyingType targetType with null -> targetType | u -> u
            let core = if Runtime.InteropServices.Marshal.SizeOf target < 4 then typeof<int> else target
            let bound (name: string) = core.GetField(name).GetValue(null)
            // The bounds are compared as doubles and produced as the target's own literals (long.MaxValue is not
            // a double, so casting the double bound back would overflow).
            let asDouble (v: obj) = ScalarExpr.Literal(LiteralValue.Float64(Convert.ToDouble v))
            let asTarget (v: obj) = ScalarExprOps.toLiteral v core
            eval ctx operand
            |> Option.bind (fun x ->
                tryTranslateCast core x
                |> Option.map (fun cast ->
                    ScalarExpr.Conditional(
                        ScalarExpr.Unary(UnaryOp.IsNan, x), asTarget (Convert.ChangeType(0, core)),
                        ScalarExpr.Conditional(
                            ScalarExpr.Binary(BinaryOp.GtEq, x, asDouble (bound "MaxValue")), asTarget (bound "MaxValue"),
                            ScalarExpr.Conditional(ScalarExpr.Binary(BinaryOp.LtEq, x, asDouble (bound "MinValue")), asTarget (bound "MinValue"), cast)))))
            |> Option.bind (fun saturated -> if core = target then Some saturated else tryTranslateCast targetType saturated)
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

    /// The row-dependent inputs a UDF over `a` reads. A value that cannot travel through a column (a dictionary)
    /// is not an input itself: the UDF computes it, from the inputs found beneath it.
    let rec private udfInputs (ctx: TranslationContext) (a: Expression) : Expression list =
        if isNull a || not (exprDependsOnContext ctx a) then
            []
        elif not (ListTranslator.isDictionary a.Type) then
            [ a ]
        else
            let children: Expression list =
                match a with
                | :? MethodCallExpression as mc -> (if isNull mc.Object then [] else [ mc.Object ]) @ List.ofSeq mc.Arguments
                | :? InvocationExpression as inv -> inv.Expression :: List.ofSeq inv.Arguments
                | :? MemberExpression as m -> [ m.Expression ]
                | :? UnaryExpression as u -> [ u.Operand ]
                | :? ConditionalExpression as c -> [ c.Test; c.IfTrue; c.IfFalse ]
                | :? BinaryExpression as b -> [ b.Left; b.Right ]
                | _ -> []
            if children.IsEmpty then [ a ] else children |> List.collect (udfInputs ctx)

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
                        |> List.collect (udfInputs ctx)
                        |> List.distinct // Expression equality is reference equality

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
                        // An enum result (plain, Nullable or F# option) leaves the UDF as its underlying value;
                        // one stored as a Polars Enum then takes its category (EnumConvert.FromValue), so an
                        // undefined value is an error.
                        let enumUnderlying = ScalarExprOps.tryEnumUnderlyingType expr.Type

                        let isSupportedReturn =
                            match enumUnderlying with
                            | Some _ -> true
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
                                    let struct (_, optionalKind) = PolarsTypeHelper.GetOptionalInfo expr.Type
                                    returnType,
                                    (fun (v: obj) ->
                                        match PolarsTypeHelper.UnwrapOptionalValue(v, optionalKind) with
                                        | null -> null
                                        | payload -> Convert.ChangeType(payload, underlying))
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

                            match currentUdfGuard ctx with
                            // Under a guard (short-circuit evaluation), the guard is one more input: a row where it
                            // is not true skips the user function and yields null, which the guarding `&&` / `||`
                            // / `?:` / `??` discards.
                            | Some guard when dynCount > 0 ->
                                let userFunc = compileMultiArgUdfDelegate dynArgs expr
                                let resultFunc =
                                    Func<obj array, obj>(fun xs ->
                                        match xs.[dynCount] with
                                        | :? bool as reached when reached -> toUdfResult (userFunc.Invoke xs)
                                        | _ -> null)
                                let inputTypes = (dynArgs |> List.map (fun (a, _) -> a.Type)) @ [ typeof<bool> ] |> List.toArray
                                let arrowCallback = ArrowUdfBatchAdapter.CreateMulti(resultFunc, inputTypes, udfReturnType)
                                let scalarArgs = (dynArgs |> List.map snd) @ [ guard ]

                                ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                                CompilationScope.Current.RecordUdf(sprintf "%s: %s" methodName (expr.ToString()))
                                Some(wrapResult (ScalarExpr.Udf(methodName, scalarArgs, retPlType, UdfInvoker.MultiBatch(box arrowCallback, udfReturnType))))

                            | _ ->
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

