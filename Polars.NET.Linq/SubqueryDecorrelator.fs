namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Helpers
open System
open System.Reflection
open System.Linq.Expressions
open System.Linq

/// Represents the result of attempting subquery decorrelation on a filter expression.
type internal DecorrelationResult =
    {
        /// Execution pipeline operations produced by decorrelation (e.g., Semi-Join, Anti-Join)
        GeneratedOps: QueryOp list
        /// Any remaining pure outer filter expression that must still be applied
        ResidualLambda: LambdaExpression option
    }

/// Dedicated engine for decorrelating subqueries (EXISTS, NOT EXISTS, IN, NOT IN, scalar aggregates,
/// and embedded projection subqueries) into native Polars pushdown operations.
module internal SubqueryDecorrelator =
    open System.Collections.Generic

    // =========================================================================
    // 1. AST Inspection & Reflection Helpers
    // =========================================================================

    /// Checks recursively whether an expression contains references to a specific parameter name.
    let rec containsParameter (paramName: string) (expr: Expression) : bool =
        match expr with
        | null -> false
        | :? ParameterExpression as p -> p.Name = paramName
        | :? NewExpression as ne -> ne.Arguments |> Seq.exists (containsParameter paramName)
        | :? MemberInitExpression as mie ->
            containsParameter paramName mie.NewExpression
            || mie.Bindings
               |> Seq.exists (function
                   | :? MemberAssignment as ma -> containsParameter paramName ma.Expression
                   | _ -> false)
        | :? ConditionalExpression as cond ->
            containsParameter paramName cond.Test
            || containsParameter paramName cond.IfTrue
            || containsParameter paramName cond.IfFalse
        | MethodCall(_, target, args) ->
            not (isNull target) && containsParameter paramName target
            || args |> List.exists (containsParameter paramName)
        | MemberAccess(target, _) -> not (isNull target) && containsParameter paramName target
        | Unary(_, inner) -> containsParameter paramName inner
        | Binary(_, left, right) -> containsParameter paramName left || containsParameter paramName right
        | Lambda(_, body) -> containsParameter paramName body
        | _ -> false

    /// Determines if a type represents a scalar/primitive/string rather than a sequence/collection.
    let isScalarType (t: Type) : bool =
        if isNull t then
            false
        elif t = typeof<string> then
            true
        elif typeof<System.Collections.IEnumerable>.IsAssignableFrom t then
            false
        else
            true

    /// Checks whether an expression contains a queryable source or LINQ reduction call.
    let rec containsSubquery (expr: Expression) : bool =
        match expr with
        | null -> false
        | :? ConstantExpression as c ->
            match c.Value with
            | :? IQueryable
            | :? IPolarsPlanSource -> true
            | _ -> false
        | :? NewExpression as ne -> ne.Arguments |> Seq.exists containsSubquery
        | :? MemberInitExpression as mie ->
            containsSubquery mie.NewExpression
            || mie.Bindings
               |> Seq.exists (function
                   | :? MemberAssignment as ma -> containsSubquery ma.Expression
                   | _ -> false)
        | :? ConditionalExpression as cond ->
            containsSubquery cond.Test
            || containsSubquery cond.IfTrue
            || containsSubquery cond.IfFalse
        | MethodCall(m, target, args) ->
            let isQueryMethod =
                m.DeclaringType = typeof<Queryable>
                || m.DeclaringType = typeof<Enumerable>
                || typeof<IQueryable>.IsAssignableFrom m.ReturnType

            isQueryMethod
            || not (isNull target) && containsSubquery target
            || args |> List.exists containsSubquery
        | MemberAccess(target, _) -> not (isNull target) && containsSubquery target
        | Unary(_, inner) -> containsSubquery inner
        | Binary(_, left, right) -> containsSubquery left || containsSubquery right
        | Lambda(_, body) -> containsSubquery body
        | _ -> false

    /// Flattens nested binary AndAlso expressions into a list of conjunct expressions.
    let rec flattenAnd (expr: Expression) : Expression list =
        match ExprTranslator.unwrap expr with
        | Binary(ExpressionType.AndAlso, left, right) -> flattenAnd left @ flattenAnd right
        | other -> [ other ]

    /// Creates an appropriate TranslationContext based on whether the sequence element type
    /// is a scalar/primitive (bound to the underlying single physical column) or a composite entity.
    let createContextForType (elemType: Type) (lf: LazyFrameHandle) (paramName: string) : TranslationContext =
        if PolarsTypeHelper.IsScalarType elemType || elemType = typeof<string> then
            let colNames = PolarsSchemaExtractor.GetColumnNames lf

            if colNames.Length = 1 then
                TranslationContext.Scalar(paramName, colNames.[0])
            else
                TranslationContext.Entity paramName
        elif
            elemType.IsGenericType
            && (elemType.Name.StartsWith "ValueTuple`2"
                || elemType.Name.StartsWith "Tuple`2"
                || elemType.Name.Contains "AnonymousType"
                   && not (isNull (elemType.GetProperty "Outer"))
                   && not (isNull (elemType.GetProperty "Inner")))
        then
            let genArgs = elemType.GetGenericArguments()
            let tOuter = genArgs.[0]

            let rec getLeftMemberNames (t: Type) : Set<string> =
                let actualT = Nullable.GetUnderlyingType t |> Option.ofObj |> Option.defaultValue t

                if
                    actualT.IsGenericType
                    && (actualT.Name.StartsWith "ValueTuple" || actualT.Name.StartsWith "Tuple")
                then
                    actualT.GetGenericArguments() |> Array.map getLeftMemberNames |> Set.unionMany
                elif PolarsTypeHelper.IsScalarType actualT || actualT = typeof<string> then
                    Set.empty
                else
                    let props =
                        actualT.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                        |> Array.map (fun p -> p.Name)

                    let fields =
                        actualT.GetFields(BindingFlags.Public ||| BindingFlags.Instance)
                        |> Array.map (fun f -> f.Name)

                    Array.append props fields |> Set.ofArray

            let typeCols = getLeftMemberNames tOuter
            let lfCols = PolarsSchemaExtractor.GetColumnNames lf |> Set.ofArray
            let leftCols = Set.union typeCols lfCols
            TranslationContext.TupleJoin(paramName, leftCols, "_right")
        else
            TranslationContext.Entity paramName

    /// Uniformly extracts a LazyFrameHandle from an Expression.
    let resolveToLazyFrameHandle (expr: Expression) : LazyFrameHandle option =
        let rec unwrap (e: Expression) =
            match e with
            | Unary(ExpressionType.Convert, inner)
            | Unary(ExpressionType.ConvertChecked, inner)
            | Unary(ExpressionType.Quote, inner) -> unwrap inner
            | other -> other

        let cleanExpr = unwrap expr

        let rawObjOpt =
            match cleanExpr with
            | :? ConstantExpression as ce -> Some ce.Value
            | other ->
                match tryEvaluate other with
                | Some v -> Some v
                | None ->
                    try
                        let lambda =
                            Expression.Lambda<Func<obj>>(Expression.Convert(other, typeof<obj>)).Compile()

                        Some(lambda.Invoke())
                    with _ ->
                        None

        match rawObjOpt with
        | None -> None
        | Some null -> None
        | Some(:? IPolarsPlanSource as plan) -> Some(plan.GetCompiledLazyFrameHandle())
        | Some(:? LazyFrameHandle as lf) -> Some(PolarsWrapper.LazyClone lf)
        | Some(:? DataFrameHandle as df) -> Some(PolarsWrapper.DataFrameToLazy df)
        | Some value ->
            let valueType = value.GetType()

            let handleProp =
                valueType.GetProperty(
                    "Handle",
                    BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance
                )

            if not (isNull handleProp) then
                match handleProp.GetValue value with
                | :? LazyFrameHandle as lf -> Some(PolarsWrapper.LazyClone lf)
                | :? DataFrameHandle as df -> Some(PolarsWrapper.DataFrameToLazy df)
                | _ -> None
            else
                match PolarsTypeHelper.TryGetEnumerableElementType valueType with
                | null -> None
                | elemType ->
                    let fromRows =
                        typeof<DataFrameBuilder>
                            .GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static)
                            .MakeGenericMethod
                            elemType

                    let dfHandle = fromRows.Invoke(null, [| value |]) :?> DataFrameHandle
                    Some(PolarsWrapper.DataFrameToLazy dfHandle)

    /// Compiles any sub-expression into an isolated LazyFrameHandle.
    let compileSubtreeToLazyFrame (expr: Expression) : LazyFrameHandle option =
        match expr with
        | :? ConstantExpression as c when not (isNull c.Value) ->
            match c.Value with
            | :? IPolarsPlanSource as planSource -> Some(planSource.GetCompiledLazyFrameHandle())
            | _ -> None
        | _ ->
            let rec findRootQueryable (e: Expression) : IQueryable option =
                match e with
                | :? ConstantExpression as c when not (isNull c.Value) ->
                    match c.Value with
                    | :? IQueryable as q -> Some q
                    | _ -> None
                | :? MethodCallExpression as mc -> mc.Arguments |> Seq.tryPick findRootQueryable
                | _ -> None

            match findRootQueryable expr with
            | Some rootQuery ->
                try
                    let subQuery = rootQuery.Provider.CreateQuery expr

                    match subQuery with
                    | :? IPolarsPlanSource as ps -> Some(ps.GetCompiledLazyFrameHandle())
                    | _ -> None
                with _ ->
                    None
            | None ->
                match tryEvaluate expr with
                | Some(:? IPolarsPlanSource as ps) -> Some(ps.GetCompiledLazyFrameHandle())
                | _ -> None

    /// Resolves an inner sequence expression to a LazyFrameHandle by trying direct evaluation,
    /// handle unwrapping, and full subtree queryable compilation.
    let resolveSequenceLazyFrame (expr: Expression) : LazyFrameHandle option =
        resolveToLazyFrameHandle expr
        |> Option.orElseWith (fun () -> compileSubtreeToLazyFrame expr)

    /// Evaluates an uncorrelated expression into a scalar ConstantExpression.
    let tryEvaluateUncorrelatedScalar (expr: Expression) : Expression option =
        try
            let lambda = Expression.Lambda(Expression.Convert(expr, typeof<obj>)).Compile()
            let resultVal = lambda.DynamicInvoke()

            if isNull resultVal then
                Some(Expression.Constant(null, expr.Type) :> Expression)
            else
                Some(Expression.Constant(resultVal, expr.Type) :> Expression)
        with _ ->
            None

    /// Recursively folds any uncorrelated scalar subquery within an expression into an Expression.Constant.
    let rec foldUncorrelatedSubqueries (outerParamName: string) (expr: Expression) : Expression * bool =
        match expr with
        | null -> null, false
        | _ when
            containsSubquery expr
            && not (containsParameter outerParamName expr)
            && isScalarType expr.Type
            ->
            match tryEvaluateUncorrelatedScalar expr with
            | Some foldedConstant -> foldedConstant, true
            | None -> expr, false
        | Binary(op, left, right) ->
            let newLeft, leftChanged = foldUncorrelatedSubqueries outerParamName left
            let newRight, rightChanged = foldUncorrelatedSubqueries outerParamName right

            if leftChanged || rightChanged then
                Expression.MakeBinary(op, newLeft, newRight) :> Expression, true
            else
                expr, false
        | Unary(op, inner) ->
            let newInner, innerChanged = foldUncorrelatedSubqueries outerParamName inner

            if innerChanged then
                Expression.MakeUnary(op, newInner, expr.Type) :> Expression, true
            else
                expr, false
        | MethodCall(m, target, args) ->
            let newTarget, targetChanged =
                if isNull target then
                    null, false
                else
                    foldUncorrelatedSubqueries outerParamName target

            let newArgsWithFlags = args |> List.map (foldUncorrelatedSubqueries outerParamName)
            let anyArgChanged = newArgsWithFlags |> List.exists snd

            if targetChanged || anyArgChanged then
                let newArgs = newArgsWithFlags |> List.map fst

                if isNull target then
                    Expression.Call(m, newArgs |> List.toArray) :> Expression, true
                else
                    Expression.Call(newTarget, m, newArgs |> List.toArray) :> Expression, true
            else
                expr, false
        | :? ConditionalExpression as condExpr ->
            let newTest, testChanged = foldUncorrelatedSubqueries outerParamName condExpr.Test

            let newIfTrue, trueChanged =
                foldUncorrelatedSubqueries outerParamName condExpr.IfTrue

            let newIfFalse, falseChanged =
                foldUncorrelatedSubqueries outerParamName condExpr.IfFalse

            if testChanged || trueChanged || falseChanged then
                Expression.Condition(newTest, newIfTrue, newIfFalse) :> Expression, true
            else
                expr, false
        | MemberAccess(target, m) when not (isNull target) ->
            let newTarget, targetChanged = foldUncorrelatedSubqueries outerParamName target

            if targetChanged then
                Expression.MakeMemberAccess(newTarget, m) :> Expression, true
            else
                expr, false
        | :? NewExpression as ne ->
            let newArgsWithFlags =
                ne.Arguments
                |> Seq.map (foldUncorrelatedSubqueries outerParamName)
                |> Seq.toList

            if newArgsWithFlags |> List.exists snd then
                let newArgs = newArgsWithFlags |> List.map fst

                let newNe =
                    if isNull ne.Members || ne.Members.Count = 0 then
                        Expression.New(ne.Constructor, newArgs |> List.toArray)
                    else
                        Expression.New(ne.Constructor, newArgs |> List.toArray, ne.Members)

                newNe :> Expression, true
            else
                expr, false
        | :? MemberInitExpression as mie ->
            let newNeExpr, neChanged =
                foldUncorrelatedSubqueries outerParamName mie.NewExpression

            let newBindingsWithFlags =
                mie.Bindings
                |> Seq.map (function
                    | :? MemberAssignment as ma ->
                        let newInit, initChanged = foldUncorrelatedSubqueries outerParamName ma.Expression
                        (Expression.Bind(ma.Member, newInit) :> MemberBinding), initChanged
                    | other -> other, false)
                |> Seq.toList

            let anyBindingChanged = newBindingsWithFlags |> List.exists snd

            if neChanged || anyBindingChanged then
                let newBindings = newBindingsWithFlags |> List.map fst
                Expression.MemberInit(newNeExpr :?> NewExpression, newBindings |> List.toArray) :> Expression, true
            else
                expr, false
        | _ -> expr, false

    // =========================================================================
    // 2. Subquery Pattern Matching & Algebraic Deconstruction
    // =========================================================================

    [<RequireQualifiedAccess>]
    type internal AggScale =
        | Identity
        | Multiply of Expression
        | Divide of Expression

    type internal AggReductionCall =
        {
            InnerSeq: Expression
            MethodName: string
            SelectorLambdaOpt: LambdaExpression option
            ExtraWhereLambdaOpt: LambdaExpression option
        }

    let private isAggReductionMethod (name: string) : bool =
        match name with
        | "Average"
        | "Mean"
        | "Min"
        | "Max"
        | "Sum"
        | "Count"
        | "LongCount"
        | "Median"
        | "ToList"
        | "ToArray" -> true
        | _ -> false

    let rec internal unwrapWhere (seqExpr: Expression) : Expression * LambdaExpression list =
        match ExprTranslator.unwrap seqExpr with
        | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" ->
            let rootSrc, lams = unwrapWhere src
            rootSrc, lams @ [ lam ]
        | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" ->
            let rootSrc, lams = unwrapWhere src
            rootSrc, lams @ [ lam ]
        | other -> other, []

    // Pattern 1: Any(innerSeq, [predicate]) or innerSeq.Where(predicate).Any()
    let internal tryMatchAny (e: Expression) : (Expression * LambdaExpression option) option =
        match e with
        | MethodCall(m, null, [ innerSeq; CleanLambda lam ]) when m.Name = "Any" -> Some(innerSeq, Some lam)
        | MethodCall(m, innerSeq, [ CleanLambda lam ]) when m.Name = "Any" -> Some(innerSeq, Some lam)
        | MethodCall(m, null, [ innerSeq ]) when m.Name = "Any" ->
            match ExprTranslator.unwrap innerSeq with
            | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | _ -> Some(innerSeq, None)
        | MethodCall(m, innerSeq, []) when m.Name = "Any" ->
            match ExprTranslator.unwrap innerSeq with
            | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | _ -> Some(innerSeq, None)
        | _ -> None

    // Pattern 2: innerSeq.Contains(keyExpr) or Contains(innerSeq, keyExpr)
    let internal tryMatchContains (e: Expression) : (Expression * LambdaExpression option * Expression) option =
        let matchSeqAndKey (innerSeq: Expression) (keyExpr: Expression) =
            match ExprTranslator.unwrap innerSeq with
            | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when mSel.Name = "Select" ->
                Some(src, Some selLam, keyExpr)
            | MethodCall(mSel, src, [ CleanLambda selLam ]) when mSel.Name = "Select" -> Some(src, Some selLam, keyExpr)
            | otherSeq -> Some(otherSeq, None, keyExpr)

        match e with
        | MethodCall(m, null, [ innerSeq; keyExpr ]) when m.Name = "Contains" -> matchSeqAndKey innerSeq keyExpr
        | MethodCall(m, innerSeq, [ keyExpr ]) when m.Name = "Contains" -> matchSeqAndKey innerSeq keyExpr
        | _ -> None

    // Pattern 3: AggReductionCall (Count, Sum, Avg, Min, Max, etc.)
    let internal tryMatchAggCall (expr: Expression) : AggReductionCall option =
        let unwrapClean (e: Expression) =
            match ExprTranslator.unwrap e with
            | :? MethodCallExpression as mc -> mc
            | _ -> null

        let mc = unwrapClean expr

        if isNull mc || not (isAggReductionMethod mc.Method.Name) then
            None
        else
            let methodName = mc.Method.Name
            let isCount = methodName = "Count" || methodName = "LongCount"

            let extractSeqAndSelector seqExpr =
                match ExprTranslator.unwrap seqExpr with
                | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when mSel.Name = "Select" -> src, Some selLam
                | MethodCall(mSel, src, [ CleanLambda selLam ]) when mSel.Name = "Select" -> src, Some selLam
                | other -> other, None

            match mc.Object, mc.Arguments |> Seq.toList with
            | null, [ innerSeq ] ->
                let src, selLamOpt = extractSeqAndSelector innerSeq

                Some
                    {
                        InnerSeq = src
                        MethodName = methodName
                        SelectorLambdaOpt = selLamOpt
                        ExtraWhereLambdaOpt = None
                    }
            | null, [ innerSeq; CleanLambda lam ] ->
                Some
                    {
                        InnerSeq = innerSeq
                        MethodName = methodName
                        SelectorLambdaOpt = if isCount then None else Some lam
                        ExtraWhereLambdaOpt = if isCount then Some lam else None
                    }
            | target, [] when not (isNull target) ->
                let src, selLamOpt = extractSeqAndSelector target

                Some
                    {
                        InnerSeq = src
                        MethodName = methodName
                        SelectorLambdaOpt = selLamOpt
                        ExtraWhereLambdaOpt = None
                    }
            | target, [ CleanLambda lam ] when not (isNull target) ->
                Some
                    {
                        InnerSeq = target
                        MethodName = methodName
                        SelectorLambdaOpt = if isCount then None else Some lam
                        ExtraWhereLambdaOpt = if isCount then Some lam else None
                    }
            | _ -> None

    let internal tryExtractScaledAgg (expr: Expression) : (Expression * AggScale) option =
        let unwrapClean = ExprTranslator.unwrap expr

        match unwrapClean with
        | Binary(ExpressionType.Multiply, left, right)
        | Binary(ExpressionType.MultiplyChecked, left, right) ->
            if containsSubquery left && not (containsSubquery right) then
                Some(left, AggScale.Multiply right)
            elif containsSubquery right && not (containsSubquery left) then
                Some(right, AggScale.Multiply left)
            else
                None
        | Binary(ExpressionType.Divide, left, right) ->
            if containsSubquery left && not (containsSubquery right) then
                Some(left, AggScale.Divide right)
            else
                None
        | _ when containsSubquery unwrapClean -> Some(unwrapClean, AggScale.Identity)
        | _ -> None

    // =========================================================================
    // 3. Core Reusable Algebraic Building Blocks
    // =========================================================================

    type internal ClassifiedConditions =
        {
            EquiPairs: (Expression * Expression) list
            ThetaConditions: Expression list
            InnerFilters: Expression list
        }

    /// Classifies a list of conjunct conditions into equi-join pairs (outer, inner),
    /// cross-table theta conditions (inequalities), and pure inner filters.
    let internal classifyConditions
        (outerName: string)
        (innerName: string)
        (conditions: Expression list)
        : ClassifiedConditions option =
        let classify (cond: Expression) =
            match ExprTranslator.unwrap cond with
            | Binary(ExpressionType.Equal, left, right) ->
                let cleanL = ExprTranslator.unwrap left
                let cleanR = ExprTranslator.unwrap right
                let lOuter = containsParameter outerName cleanL
                let lInner = containsParameter innerName cleanL
                let rOuter = containsParameter outerName cleanR
                let rInner = containsParameter innerName cleanR

                if lOuter && not lInner && rInner && not rOuter then
                    Choice1Of4(cleanL, cleanR)
                elif rOuter && not rInner && lInner && not lOuter then
                    Choice1Of4(cleanR, cleanL)
                elif not (lOuter || rOuter) && (lInner || rInner) then
                    Choice2Of4 cond
                elif not (lOuter || rOuter || lInner || rInner) then
                    Choice2Of4 cond
                else
                    let hasOuter = lOuter || rOuter
                    let hasInner = lInner || rInner

                    if hasOuter && hasInner then
                        Choice3Of4 cond
                    else
                        Choice4Of4()
            | other ->
                let hasOuter = containsParameter outerName other
                let hasInner = containsParameter innerName other

                if not hasOuter && hasInner then Choice2Of4 other
                elif not hasOuter && not hasInner then Choice2Of4 other
                elif hasOuter && hasInner then Choice3Of4 other
                else Choice4Of4()

        let classified = conditions |> List.map classify

        let hasUnsupported =
            classified
            |> List.exists (function
                | Choice4Of4() -> true
                | _ -> false)

        if hasUnsupported then
            None
        else
            let equiPairs =
                classified
                |> List.choose (function
                    | Choice1Of4 pair -> Some pair
                    | _ -> None)

            let innerFilters =
                classified
                |> List.choose (function
                    | Choice2Of4 f -> Some f
                    | _ -> None)

            let thetaConditions =
                classified
                |> List.choose (function
                    | Choice3Of4 c -> Some c
                    | _ -> None)

            if equiPairs.IsEmpty && thetaConditions.IsEmpty then
                None
            else
                Some
                    {
                        EquiPairs = equiPairs
                        ThetaConditions = thetaConditions
                        InnerFilters = innerFilters
                    }

    type internal CorrelatedParts =
        {
            RootSource: Expression
            InnerParam: ParameterExpression
            EquiPairs: (Expression * Expression) list
            ThetaConditions: Expression list
            InnerFilters: Expression list
        }

    /// Recursively unwraps Where cascades, validates decorrelation conditions against outerParamName,
    /// and extracts root source, inner parameter, equi-pairs, theta conditions, and inner filters.
    let internal tryExtractCorrelatedParts
        (outerParamName: string)
        (innerSeq: Expression)
        (extraWhereLambdaOpt: LambdaExpression option)
        : CorrelatedParts option =
        let rootSource, whereLams = unwrapWhere innerSeq

        let allWhereLams =
            match extraWhereLambdaOpt with
            | Some w -> whereLams @ [ w ]
            | None -> whereLams

        if allWhereLams.IsEmpty || containsParameter outerParamName rootSource then
            None
        else
            let innerParam = allWhereLams.[0].Parameters.[0]
            let conditions = allWhereLams |> List.collect (fun lam -> flattenAnd lam.Body)

            classifyConditions outerParamName innerParam.Name conditions
            |> Option.map (fun c ->
                {
                    RootSource = rootSource
                    InnerParam = innerParam
                    EquiPairs = c.EquiPairs
                    ThetaConditions = c.ThetaConditions
                    InnerFilters = c.InnerFilters
                })

    /// Combines and applies pushdown inner filters onto the right-side LazyFrame.
    let internal tryApplyInnerFilters
        (rawRightLf: LazyFrameHandle)
        (innerParam: ParameterExpression)
        (innerFilters: Expression list)
        : LazyFrameHandle option =
        if innerFilters.IsEmpty then
            Some rawRightLf
        else
            let combinedInner =
                innerFilters |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

            let innerCtx = createContextForType innerParam.Type rawRightLf innerParam.Name

            ExprTranslator.tryTranslateWithContext innerCtx combinedInner
            |> Option.map (fun filterH -> PolarsWrapper.LazyFilter(rawRightLf, filterH))

    type internal TranslatedKeys =
        {
            OuterKeys: ExprHandle[]
            InnerGroupByKeys: ExprHandle[]
            InnerJoinKeys: ExprHandle[]
        }

    /// Safely translates join keys across outer and inner contexts, using PolarsWrapper.CloneExpr
    /// to yield independent inner key handles for GroupBy and Join operations.
    let internal tryTranslateJoinKeys
        (outerCtx: TranslationContext)
        (innerCtx: TranslationContext)
        (equiPairs: (Expression * Expression) list)
        : TranslatedKeys option =
        let translated =
            equiPairs
            |> List.choose (fun (outerE, innerE) ->
                match
                    ExprTranslator.tryTranslateWithContext outerCtx outerE,
                    ExprTranslator.tryTranslateWithContext innerCtx innerE
                with
                | Some lk, Some rk -> Some(lk, rk)
                | _ -> None)

        if translated.Length <> equiPairs.Length then
            None
        else
            let outerKeys = translated |> List.map fst |> List.toArray
            let innerKeys = translated |> List.map snd |> List.toArray
            let innerGroupByKeys = innerKeys |> Array.map PolarsWrapper.CloneExpr

            Some
                {
                    OuterKeys = outerKeys
                    InnerGroupByKeys = innerGroupByKeys
                    InnerJoinKeys = innerKeys
                }

    /// Builds a physical reduction aggregation handle (Len() for Count/Any, or translated reduction op).
    let internal tryBuildAggHandle
        (rightLf: LazyFrameHandle)
        (methodName: string)
        (isAnyOrCount: bool)
        (selectorLambdaOpt: LambdaExpression option)
        : ExprHandle option =
        if isAnyOrCount || methodName = "Count" || methodName = "LongCount" then
            Some(PolarsWrapper.Len())
        elif methodName = "ToList" || methodName = "ToArray" then
            match selectorLambdaOpt with
            | Some selLam ->
                let selParam = selLam.Parameters.[0]
                let selCtx = createContextForType selParam.Type rightLf selParam.Name

                ExprTranslator.tryTranslateWithContext selCtx selLam.Body
                |> Option.map (fun colH -> PolarsWrapper.Implode(colH, maintainOrder = true))
            | None ->
                let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                if rightCols.Length > 0 then
                    Some(PolarsWrapper.Implode(PolarsWrapper.Col rightCols.[0], maintainOrder = true))
                else
                    None
        else
            match selectorLambdaOpt with
            | Some selLam ->
                let selParam = selLam.Parameters.[0]
                let selCtx = createContextForType selParam.Type rightLf selParam.Name

                ExprTranslator.tryTranslateWithContext selCtx selLam.Body
                |> Option.bind (ExprTranslator.translateReductionOp methodName)
            | None ->
                let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                if rightCols.Length > 0 then
                    PolarsWrapper.Col rightCols.[0]
                    |> ExprTranslator.translateReductionOp methodName
                else
                    None

    // =========================================================================
    // 4. Semi/Anti Join Filter Decorrelation (Step 1)
    // =========================================================================

    /// Tries to decorrelate a single conjunct expression relative to outerParam into Semi/Anti join operations.
    let internal tryDecorrelateConjunct
        (currentLf: LazyFrameHandle)
        (outerParam: ParameterExpression)
        (conjunctExpr: Expression)
        : QueryOp list option =
        let outerName = outerParam.Name

        let isNegated, coreExpr =
            match ExprTranslator.unwrap conjunctExpr with
            | Unary(ExpressionType.Not, inner) -> true, ExprTranslator.unwrap inner
            | other -> false, other

        match tryMatchAny coreExpr with
        | Some(innerSource, innerLambdaOpt) ->
            tryExtractCorrelatedParts outerName innerSource innerLambdaOpt
            |> Option.bind (fun parts ->
                resolveSequenceLazyFrame parts.RootSource
                |> Option.bind (fun rawRightLf ->
                    tryApplyInnerFilters rawRightLf parts.InnerParam parts.InnerFilters
                    |> Option.bind (fun rightLf ->
                        let outerCtx = createContextForType outerParam.Type currentLf outerName

                        let innerCtx =
                            createContextForType parts.InnerParam.Type rightLf parts.InnerParam.Name

                        if parts.ThetaConditions.IsEmpty then
                            tryTranslateJoinKeys outerCtx innerCtx parts.EquiPairs
                            |> Option.map (fun keys ->
                                [
                                    QueryOp.Join
                                        {
                                            RightLf = rightLf
                                            LeftOn = keys.OuterKeys
                                            RightOn = keys.InnerJoinKeys
                                            How = if isNegated then PlJoinType.Anti else PlJoinType.Semi
                                            Suffix = None
                                            InvertSides = false
                                            Coalesce = PlJoinCoalesce.JoinSpecific
                                        }
                                ])
                        else
                            let leftCols = PolarsSchemaExtractor.GetColumnNames currentLf |> Set.ofArray
                            let originalCols = PolarsSchemaExtractor.GetColumnNames currentLf

                            let correlatedCtx =
                                TranslationContext.CorrelatedJoin(
                                    outerName,
                                    parts.InnerParam.Name,
                                    leftCols,
                                    "_right"
                                )

                            let combinedThetaExpr =
                                parts.ThetaConditions
                                |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

                            ExprTranslator.tryTranslateWithContext correlatedCtx combinedThetaExpr
                            |> Option.bind (fun thetaFilterH ->
                                let equiKeysOpt =
                                    if parts.EquiPairs.IsEmpty then
                                        Some([||], [||])
                                    else
                                        tryTranslateJoinKeys outerCtx innerCtx parts.EquiPairs
                                        |> Option.map (fun k -> k.OuterKeys, k.InnerJoinKeys)

                                equiKeysOpt
                                |> Option.map (fun (outerKeys, innerKeys) ->
                                    let uid = Guid.NewGuid().ToString "N"
                                    let ridCol = $"__subquery_rid_{uid}"

                                    let outerWithRidLf =
                                        PolarsWrapper.LazyFrameWithRowIndex(
                                            PolarsWrapper.LazyClone currentLf,
                                            ridCol,
                                            Nullable 0
                                        )

                                    let joinedLf =
                                        if outerKeys.Length = 0 then
                                            PolarsWrapper.Join(
                                                outerWithRidLf,
                                                rightLf,
                                                [||],
                                                [||],
                                                PlJoinType.Cross,
                                                "_right",
                                                PlJoinValidation.ManyToMany,
                                                PlJoinCoalesce.KeepColumns,
                                                PlJoinMaintainOrder.None,
                                                PlJoinSide.None,
                                                false,
                                                Nullable(),
                                                0UL
                                            )
                                        else
                                            PolarsWrapper.Join(
                                                outerWithRidLf,
                                                rightLf,
                                                outerKeys |> Array.map PolarsWrapper.CloneExpr,
                                                innerKeys |> Array.map PolarsWrapper.CloneExpr,
                                                PlJoinType.Inner,
                                                "_right",
                                                PlJoinValidation.ManyToMany,
                                                PlJoinCoalesce.JoinSpecific,
                                                PlJoinMaintainOrder.None,
                                                PlJoinSide.None,
                                                false,
                                                Nullable(),
                                                0UL
                                            )

                                    let filteredLf = PolarsWrapper.LazyFilter(joinedLf, thetaFilterH)

                                    let selectedLf =
                                        PolarsWrapper.LazySelect(filteredLf, [| PolarsWrapper.Col ridCol |])

                                    let matchedRidsLf =
                                        PolarsWrapper.LazyUnique(selectedLf, null, PlUniqueKeepStrategy.Any, false)

                                    let withRowIndexOp = QueryOp.WithRowIndex(ridCol, Some 0u)

                                    let semiJoinOp =
                                        QueryOp.Join
                                            {
                                                RightLf = matchedRidsLf
                                                LeftOn = [| PolarsWrapper.Col ridCol |]
                                                RightOn = [| PolarsWrapper.Col ridCol |]
                                                How = if isNegated then PlJoinType.Anti else PlJoinType.Semi
                                                Suffix = None
                                                InvertSides = false
                                                Coalesce = PlJoinCoalesce.JoinSpecific
                                            }

                                    let restoreSchemaOp =
                                        QueryOp.Select(originalCols |> Array.map PolarsWrapper.Col)

                                    [ withRowIndexOp; semiJoinOp; restoreSchemaOp ])))))
        | None ->
            match tryMatchContains coreExpr with
            | Some(innerSource, selLambdaOpt, outerKeyExpr) ->
                if containsParameter outerName innerSource then
                    None
                else
                    resolveSequenceLazyFrame innerSource
                    |> Option.bind (fun rightLf ->
                        let outerCtx = createContextForType outerParam.Type currentLf outerName
                        let leftKeyOpt = ExprTranslator.tryTranslateWithContext outerCtx outerKeyExpr

                        let rightKeyOpt =
                            match selLambdaOpt with
                            | Some selLambda ->
                                let p = selLambda.Parameters.[0]
                                let innerCtx = createContextForType p.Type rightLf p.Name
                                ExprTranslator.tryTranslateWithContext innerCtx selLambda.Body
                            | None ->
                                let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                                if rightCols.Length > 0 then
                                    Some(PolarsWrapper.Col rightCols.[0])
                                else
                                    None

                        match leftKeyOpt, rightKeyOpt with
                        | Some leftKey, Some rightKey ->
                            Some
                                [
                                    QueryOp.Join
                                        {
                                            RightLf = rightLf
                                            LeftOn = [| leftKey |]
                                            RightOn = [| rightKey |]
                                            How = if isNegated then PlJoinType.Anti else PlJoinType.Semi
                                            Suffix = None
                                            InvertSides = false
                                            Coalesce = PlJoinCoalesce.JoinSpecific
                                        }
                                ]
                        | _ -> None)
            | None -> None

    // =========================================================================
    // 5. Correlated Scalar Aggregate Filter Decorrelation (Step 3)
    // =========================================================================

    /// Tries to decorrelate a correlated scalar aggregate expression (e.g. Min, Max, Average, Sum, Count)
    /// within a filter conjunct into a pre-aggregated GroupBy LazyFrame, Left-Join, and pushdown comparison filter.
    let internal tryDecorrelateCorrelatedScalarAgg
        (currentLf: LazyFrameHandle)
        (outerParam: ParameterExpression)
        (conjunctExpr: Expression)
        : QueryOp list option =
        let outerName = outerParam.Name

        let isNegated, coreConjunct =
            match ExprTranslator.unwrap conjunctExpr with
            | Unary(ExpressionType.Not, inner) -> true, ExprTranslator.unwrap inner
            | other -> false, other

        let invertOp =
            function
            | ExpressionType.Equal -> Some ExpressionType.Equal
            | ExpressionType.NotEqual -> Some ExpressionType.NotEqual
            | ExpressionType.LessThan -> Some ExpressionType.GreaterThan
            | ExpressionType.LessThanOrEqual -> Some ExpressionType.GreaterThanOrEqual
            | ExpressionType.GreaterThan -> Some ExpressionType.LessThan
            | ExpressionType.GreaterThanOrEqual -> Some ExpressionType.LessThanOrEqual
            | _ -> None

        let negateOp =
            function
            | ExpressionType.Equal -> Some ExpressionType.NotEqual
            | ExpressionType.NotEqual -> Some ExpressionType.Equal
            | ExpressionType.LessThan -> Some ExpressionType.GreaterThanOrEqual
            | ExpressionType.LessThanOrEqual -> Some ExpressionType.GreaterThan
            | ExpressionType.GreaterThan -> Some ExpressionType.LessThanOrEqual
            | ExpressionType.GreaterThanOrEqual -> Some ExpressionType.LessThan
            | _ -> None

        match coreConjunct with
        | Binary(rawOp, left, right) ->
            let effectiveOpOpt = if isNegated then negateOp rawOp else Some rawOp

            effectiveOpOpt
            |> Option.bind (fun baseOp ->
                if containsParameter outerName left && not (containsSubquery left) then
                    match tryExtractScaledAgg right with
                    | Some(aggExpr, scale) ->
                        let scaleOk =
                            match scale with
                            | AggScale.Identity -> true
                            | AggScale.Multiply m
                            | AggScale.Divide m -> not (containsParameter outerName m)

                        if scaleOk then Some(left, aggExpr, scale, baseOp) else None
                    | None -> None
                elif containsParameter outerName right && not (containsSubquery right) then
                    match tryExtractScaledAgg left with
                    | Some(aggExpr, scale) ->
                        let scaleOk =
                            match scale with
                            | AggScale.Identity -> true
                            | AggScale.Multiply m
                            | AggScale.Divide m -> not (containsParameter outerName m)

                        if scaleOk then
                            invertOp baseOp |> Option.map (fun invOp -> (right, aggExpr, scale, invOp))
                        else
                            None
                    | None -> None
                else
                    None)
            |> Option.bind (fun (outerSideExpr, aggCallExpr, scale, effectiveOp) ->
                match tryMatchAggCall aggCallExpr with
                | None -> None
                | Some aggCall ->
                    tryExtractCorrelatedParts outerName aggCall.InnerSeq aggCall.ExtraWhereLambdaOpt
                    |> Option.bind (fun parts ->
                        if not parts.ThetaConditions.IsEmpty then
                            None
                        else
                            resolveSequenceLazyFrame parts.RootSource
                            |> Option.bind (fun rawRightLf ->
                                tryApplyInnerFilters rawRightLf parts.InnerParam parts.InnerFilters
                                |> Option.bind (fun rightLf ->
                                    let outerCtx = createContextForType outerParam.Type currentLf outerName

                                    let innerCtx =
                                        createContextForType parts.InnerParam.Type rightLf parts.InnerParam.Name

                                    tryTranslateJoinKeys outerCtx innerCtx parts.EquiPairs
                                    |> Option.bind (fun keys ->
                                        let isCount =
                                            aggCall.MethodName = "Count" || aggCall.MethodName = "LongCount"

                                        tryBuildAggHandle
                                            rightLf
                                            aggCall.MethodName
                                            isCount
                                            aggCall.SelectorLambdaOpt
                                        |> Option.bind (fun rawAggH ->
                                            let uid = Guid.NewGuid().ToString "N"
                                            let aggColName = $"__subquery_agg_{uid}"
                                            let aliasedAggH = PolarsWrapper.Alias(rawAggH, aggColName)

                                            let groupedLf =
                                                PolarsWrapper.LazyGroupByAgg(
                                                    rightLf,
                                                    keys.InnerGroupByKeys,
                                                    [| aliasedAggH |],
                                                    null,
                                                    false
                                                )

                                            ExprTranslator.tryTranslateWithContext outerCtx outerSideExpr
                                            |> Option.bind (fun outerSideH ->
                                                let aggColBaseH =
                                                    if isCount then
                                                        PolarsWrapper.FillNull(
                                                            PolarsWrapper.Col aggColName,
                                                            PolarsWrapper.Lit 0u
                                                        )
                                                    else
                                                        PolarsWrapper.Col aggColName

                                                let scaledAggHOpt =
                                                    match scale with
                                                    | AggScale.Identity -> Some aggColBaseH
                                                    | AggScale.Multiply multExpr ->
                                                        match tryEvaluate multExpr with
                                                        | Some v when not (isNull v) ->
                                                            let litH =
                                                                ExprTranslator.toLiteralHandle v multExpr.Type

                                                            Some(PolarsWrapper.Mul(litH, aggColBaseH))
                                                        | _ -> None
                                                    | AggScale.Divide divExpr ->
                                                        match tryEvaluate divExpr with
                                                        | Some v when not (isNull v) ->
                                                            let litH =
                                                                ExprTranslator.toLiteralHandle v divExpr.Type

                                                            Some(PolarsWrapper.Div(aggColBaseH, litH))
                                                        | _ -> None

                                                scaledAggHOpt
                                                |> Option.bind (fun scaledAggH ->
                                                    let cmpHandle =
                                                        match effectiveOp with
                                                        | ExpressionType.Equal ->
                                                            PolarsWrapper.Eq(outerSideH, scaledAggH)
                                                        | ExpressionType.NotEqual ->
                                                            PolarsWrapper.Neq(outerSideH, scaledAggH)
                                                        | ExpressionType.LessThan ->
                                                            PolarsWrapper.Lt(outerSideH, scaledAggH)
                                                        | ExpressionType.LessThanOrEqual ->
                                                            PolarsWrapper.LtEq(outerSideH, scaledAggH)
                                                        | ExpressionType.GreaterThan ->
                                                            PolarsWrapper.Gt(outerSideH, scaledAggH)
                                                        | ExpressionType.GreaterThanOrEqual ->
                                                            PolarsWrapper.GtEq(outerSideH, scaledAggH)
                                                        | _ -> null

                                                    if isNull cmpHandle then
                                                        None
                                                    else
                                                        let joinOp =
                                                            QueryOp.Join
                                                                {
                                                                    RightLf = groupedLf
                                                                    LeftOn = keys.OuterKeys
                                                                    RightOn = keys.InnerJoinKeys
                                                                    How = PlJoinType.Left
                                                                    Suffix = None
                                                                    InvertSides = false
                                                                    Coalesce = PlJoinCoalesce.JoinSpecific
                                                                }

                                                        let filterOp = QueryOp.Filter cmpHandle

                                                        let originalCols =
                                                            PolarsSchemaExtractor.GetColumnNames currentLf

                                                        let ops =
                                                            if originalCols.Length > 0 then
                                                                [
                                                                    joinOp
                                                                    filterOp
                                                                    QueryOp.Select(
                                                                        originalCols
                                                                        |> Array.map PolarsWrapper.Col
                                                                    )
                                                                ]
                                                            else
                                                                [ joinOp; filterOp ]

                                                        Some ops))))))))
        | _ -> None

    /// Attempts to decorrelate one or more subquery expressions within a filter lambda,
    /// emitting corresponding QueryOp pushdown stages and separating residual outer filters.
    let tryDecorrelateFilter
        (currentLf: LazyFrameHandle)
        (filterLambda: LambdaExpression)
        : DecorrelationResult option =
        if filterLambda.Parameters.Count <> 1 then
            None
        else
            let outerParam = filterLambda.Parameters.[0]
            let conjuncts = flattenAnd filterLambda.Body

            let generatedOpsRev, residualConjunctsRev, wasDecorrelated =
                conjuncts
                |> List.fold
                    (fun (opsAcc, resAcc, decorrelatedAny) cond ->
                        match tryDecorrelateConjunct currentLf outerParam cond with
                        | Some ops -> (List.rev ops) @ opsAcc, resAcc, true
                        | None ->
                            match tryDecorrelateCorrelatedScalarAgg currentLf outerParam cond with
                            | Some aggOps -> (List.rev aggOps) @ opsAcc, resAcc, true
                            | None ->
                                let foldedCond, changed = foldUncorrelatedSubqueries outerParam.Name cond

                                if changed then
                                    opsAcc, foldedCond :: resAcc, true
                                else
                                    opsAcc, cond :: resAcc, decorrelatedAny)
                    ([], [], false)

            let generatedOps = List.rev generatedOpsRev
            let residualConjuncts = List.rev residualConjunctsRev

            if generatedOps.IsEmpty && not wasDecorrelated then
                None
            else
                let residualLambda =
                    match residualConjuncts with
                    | [] -> None
                    | [ single ] -> Some(Expression.Lambda(single, outerParam))
                    | first :: rest ->
                        let combined =
                            rest |> List.fold (fun acc c -> Expression.AndAlso(acc, c) :> Expression) first

                        Some(Expression.Lambda(combined, outerParam))

                Some
                    {
                        GeneratedOps = generatedOps
                        ResidualLambda = residualLambda
                    }

    // =========================================================================
    // 6. Projection Subquery Decorrelation & Coalescing (Step 4)
    // =========================================================================

    /// Represents an extracted correlated subquery candidate within a projection expression tree.
    type internal ProjectionSubqueryCandidate =
        {
            OriginalExpr: Expression
            RootSource: Expression
            InnerParam: ParameterExpression
            EquiPairs: (Expression * Expression) list
            ThetaConditions: Expression list
            InnerFilters: Expression list
            MethodName: string
            SelectorLambdaOpt: LambdaExpression option
            IsAny: bool
            TargetType: Type
        }

    /// AST visitor that replaces matched original subquery expressions with their corresponding
    /// physical column parameter or boolean comparison expressions.
    type private SubqueryExpressionReplacer(replacements: (Expression * Expression) list) =
        inherit ExpressionVisitor()

        override this.Visit(node: Expression) =
            if isNull node then
                null
            else
                let matchOpt =
                    replacements |> List.tryFind (fun (orig, _) -> obj.ReferenceEquals(orig, node))

                match matchOpt with
                | Some(_, repl) -> repl
                | None -> base.Visit node

    let private tryExtractProjectionCandidate
        (outerParamName: string)
        (expr: Expression)
        : ProjectionSubqueryCandidate option =
        let matchedOpt =
            match tryMatchAny expr with
            | Some(innerSeq, extraWhereOpt) -> Some(innerSeq, "Any", None, extraWhereOpt, true)
            | None ->
                match tryMatchAggCall expr with
                | Some aggCall ->
                    Some(
                        aggCall.InnerSeq,
                        aggCall.MethodName,
                        aggCall.SelectorLambdaOpt,
                        aggCall.ExtraWhereLambdaOpt,
                        false
                    )
                | None -> None

        match matchedOpt with
        | None -> None
        | Some(innerSeq, methodName, selectorLambdaOpt, extraWhereLambdaOpt, isAny) ->
            tryExtractCorrelatedParts outerParamName innerSeq extraWhereLambdaOpt
            |> Option.map (fun parts ->
                {
                    OriginalExpr = expr
                    RootSource = parts.RootSource
                    InnerParam = parts.InnerParam
                    EquiPairs = parts.EquiPairs
                    ThetaConditions = parts.ThetaConditions
                    InnerFilters = parts.InnerFilters
                    MethodName = methodName
                    SelectorLambdaOpt = selectorLambdaOpt
                    IsAny = isAny
                    TargetType = expr.Type
                })

    let rec private collectProjectionSubqueries
        (outerParamName: string)
        (expr: Expression)
        : ProjectionSubqueryCandidate list =
        if
            isNull expr
            || not (containsSubquery expr)
            || not (containsParameter outerParamName expr)
        then
            []
        else
            match tryExtractProjectionCandidate outerParamName expr with
            | Some cand -> [ cand ]
            | None ->
                match expr with
                | :? NewExpression as ne ->
                    ne.Arguments
                    |> Seq.toList
                    |> List.collect (collectProjectionSubqueries outerParamName)
                | :? MemberInitExpression as mie ->
                    let initArgs =
                        mie.NewExpression.Arguments
                        |> Seq.toList
                        |> List.collect (collectProjectionSubqueries outerParamName)

                    let bindingArgs =
                        mie.Bindings
                        |> Seq.choose (function
                            | :? MemberAssignment as ma -> Some ma.Expression
                            | _ -> None)
                        |> Seq.toList
                        |> List.collect (collectProjectionSubqueries outerParamName)

                    initArgs @ bindingArgs
                | MethodCall(_, target, args) ->
                    let targetSubs =
                        if isNull target then
                            []
                        else
                            collectProjectionSubqueries outerParamName target

                    let argSubs = args |> List.collect (collectProjectionSubqueries outerParamName)
                    targetSubs @ argSubs
                | Binary(_, left, right) ->
                    collectProjectionSubqueries outerParamName left
                    @ collectProjectionSubqueries outerParamName right
                | Unary(_, inner) -> collectProjectionSubqueries outerParamName inner
                | :? ConditionalExpression as cond ->
                    collectProjectionSubqueries outerParamName cond.Test
                    @ collectProjectionSubqueries outerParamName cond.IfTrue
                    @ collectProjectionSubqueries outerParamName cond.IfFalse
                | MemberAccess(target, _) ->
                    if isNull target then
                        []
                    else
                        collectProjectionSubqueries outerParamName target
                | :? InvocationExpression as inv ->
                    let exprSubs = collectProjectionSubqueries outerParamName inv.Expression

                    let argSubs =
                        inv.Arguments
                        |> Seq.toList
                        |> List.collect (collectProjectionSubqueries outerParamName)

                    exprSubs @ argSubs
                | _ -> []

    let private canCoalesce (c1: ProjectionSubqueryCandidate) (c2: ProjectionSubqueryCandidate) : bool =
        let sameSource =
            obj.ReferenceEquals(c1.RootSource, c2.RootSource)
            || c1.RootSource.ToString() = c2.RootSource.ToString()

        let sameKeys =
            c1.EquiPairs.Length = c2.EquiPairs.Length
            && List.forall2
                (fun (o1: Expression, i1: Expression) (o2: Expression, i2: Expression) ->
                    o1.ToString() = o2.ToString() && i1.ToString() = i2.ToString())
                c1.EquiPairs
                c2.EquiPairs

        let sameThetas =
            c1.ThetaConditions.Length = c2.ThetaConditions.Length
            && List.forall2
                (fun (t1: Expression) (t2: Expression) -> t1.ToString() = t2.ToString())
                c1.ThetaConditions
                c2.ThetaConditions

        let sameFilters =
            c1.InnerFilters.Length = c2.InnerFilters.Length
            && List.forall2
                (fun (f1: Expression) (f2: Expression) -> f1.ToString() = f2.ToString())
                c1.InnerFilters
                c2.InnerFilters

        sameSource && sameKeys && sameThetas && sameFilters

    let private groupCandidates (candidates: ProjectionSubqueryCandidate list) : ProjectionSubqueryCandidate list list =
        let rec groupHelper (remaining: ProjectionSubqueryCandidate list) (acc: ProjectionSubqueryCandidate list list) =
            match remaining with
            | [] -> acc
            | head :: tail ->
                let found, newAccRev =
                    acc
                    |> List.fold
                        (fun (matched, grps) grp ->
                            if not matched && canCoalesce head grp.Head then
                                true, (grp @ [ head ]) :: grps
                            else
                                matched, grp :: grps)
                        (false, [])

                if found then
                    groupHelper tail (List.rev newAccRev)
                else
                    groupHelper tail (acc @ [ [ head ] ])

        groupHelper candidates []

    /// Attempts to decorrelate embedded subqueries (e.g. Count, Sum, Average, Min, Max, Any)
    /// found within a Select projection lambda. Groups subqueries against the same inner source table
    /// and keys into unified GroupBy aggregations, emitting Left Outer Joins and null-coalescing operations,
    /// and returns the rewritten projection lambda with native column references.
    let tryDecorrelateProjection
        (currentLf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        : (QueryOp list * LambdaExpression) option =
        if projLambda.Parameters.Count <> 1 then
            None
        elif not (containsSubquery projLambda.Body) then
            None
        else
            let outerParam = projLambda.Parameters.[0]
            let outerParamName = outerParam.Name

            let foldedBody, hasFolded =
                foldUncorrelatedSubqueries outerParamName projLambda.Body

            let candidates = collectProjectionSubqueries outerParamName foldedBody

            if candidates.IsEmpty then
                if hasFolded then
                    Some([], Expression.Lambda(foldedBody, projLambda.Parameters))
                else
                    None
            else
                let groups = groupCandidates candidates
                let outerCtx = createContextForType outerParam.Type currentLf outerParamName

                let compileGroup (grpIdx: int) (grp: ProjectionSubqueryCandidate list) =
                    let head = grp.Head

                    resolveSequenceLazyFrame head.RootSource
                    |> Option.bind (fun rawRightLf ->
                        tryApplyInnerFilters rawRightLf head.InnerParam head.InnerFilters
                        |> Option.bind (fun rightLf ->
                            let innerCtx =
                                createContextForType head.InnerParam.Type rightLf head.InnerParam.Name

                            let sameAggOp (c1: ProjectionSubqueryCandidate) (c2: ProjectionSubqueryCandidate) =
                                c1.MethodName = c2.MethodName
                                && c1.IsAny = c2.IsAny
                                && match c1.SelectorLambdaOpt, c2.SelectorLambdaOpt with
                                   | None, None -> true
                                   | Some s1, Some s2 -> s1.ToString() = s2.ToString()
                                   | _ -> false

                            let rec assignAggCols buildAgg remaining assigned =
                                match remaining with
                                | [] -> Some(List.rev assigned)
                                | (candIdx, cand) :: tail ->
                                    match
                                        assigned |> List.tryFind (fun (prevCand, _, _) -> sameAggOp cand prevCand)
                                    with
                                    | Some(_, prevColName, _) ->
                                        assignAggCols buildAgg tail ((cand, prevColName, None) :: assigned)
                                    | None ->
                                        let colName = $"__subquery_agg_{grpIdx}_{candIdx}"

                                        match buildAgg cand with
                                        | None -> None
                                        | Some rawAggH ->
                                            let aliased = PolarsWrapper.Alias(rawAggH, colName)
                                            assignAggCols buildAgg tail ((cand, colName, Some aliased) :: assigned)

                            let buildFillNullOps
                                (assignments: (ProjectionSubqueryCandidate * string * ExprHandle option) list)
                                =
                                let handledCols = HashSet<string>()

                                let fillExprs =
                                    assignments
                                    |> List.choose (fun (cand, colName, _) ->
                                        let defaultValOpt =
                                            if
                                                cand.IsAny
                                                || cand.MethodName = "Count"
                                                || cand.MethodName = "LongCount"
                                            then
                                                Some(PolarsWrapper.Lit 0u)
                                            elif cand.MethodName = "Sum" then
                                                if cand.TargetType = typeof<double> then
                                                    Some(PolarsWrapper.Lit 0.0)
                                                elif cand.TargetType = typeof<float> then
                                                    Some(PolarsWrapper.Lit 0.0f)
                                                elif cand.TargetType = typeof<int> then
                                                    Some(PolarsWrapper.Lit 0)
                                                elif cand.TargetType = typeof<int64> then
                                                    Some(PolarsWrapper.Lit 0L)
                                                elif cand.TargetType = typeof<decimal> then
                                                    Some(PolarsWrapper.Lit 0.0)
                                                else
                                                    None
                                            else
                                                None

                                        match defaultValOpt with
                                        | Some defVal when handledCols.Add colName ->
                                            let filled = PolarsWrapper.FillNull(PolarsWrapper.Col colName, defVal)
                                            Some(PolarsWrapper.Alias(filled, colName))
                                        | _ -> None)

                                if fillExprs.IsEmpty then
                                    []
                                else
                                    [ QueryOp.WithColumns(fillExprs |> List.toArray) ]

                            let buildReplacements
                                (assignments: (ProjectionSubqueryCandidate * string * ExprHandle option) list)
                                =
                                assignments
                                |> List.map (fun (cand, colName, _) ->
                                    let replExpr =
                                        if cand.IsAny then
                                            Expression.GreaterThan(
                                                Expression.Parameter(typeof<uint32>, colName),
                                                Expression.Constant 0u
                                            )
                                            :> Expression
                                        else
                                            Expression.Parameter(cand.TargetType, colName) :> Expression

                                    cand.OriginalExpr, replExpr)

                            let indexedCandidates = grp |> List.mapi (fun i c -> i, c)

                            if head.ThetaConditions.IsEmpty then
                                tryTranslateJoinKeys outerCtx innerCtx head.EquiPairs
                                |> Option.bind (fun keys ->
                                    let buildAgg (cand: ProjectionSubqueryCandidate) =
                                        tryBuildAggHandle rightLf cand.MethodName cand.IsAny cand.SelectorLambdaOpt

                                    assignAggCols buildAgg indexedCandidates []
                                    |> Option.map (fun assignments ->
                                        let aggExprsToEmit =
                                            assignments
                                            |> List.choose (fun (_, _, exprOpt) -> exprOpt)
                                            |> List.toArray

                                        let groupedLf =
                                            PolarsWrapper.LazyGroupByAgg(
                                                rightLf,
                                                keys.InnerGroupByKeys,
                                                aggExprsToEmit,
                                                null,
                                                false
                                            )

                                        let joinOp =
                                            QueryOp.Join
                                                {
                                                    RightLf = groupedLf
                                                    LeftOn = keys.OuterKeys
                                                    RightOn = keys.InnerJoinKeys
                                                    How = PlJoinType.Left
                                                    Suffix = None
                                                    InvertSides = false
                                                    Coalesce = PlJoinCoalesce.JoinSpecific
                                                }

                                        let fillNullOps = buildFillNullOps assignments
                                        let replacements = buildReplacements assignments
                                        joinOp :: fillNullOps, replacements))
                            else
                                let leftCols = PolarsSchemaExtractor.GetColumnNames currentLf |> Set.ofArray
                                let rightSuffix = "_right"

                                let correlatedCtx =
                                    TranslationContext.CorrelatedJoin(
                                        outerParamName,
                                        head.InnerParam.Name,
                                        leftCols,
                                        rightSuffix
                                    )

                                let combinedThetaExpr =
                                    head.ThetaConditions
                                    |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

                                ExprTranslator.tryTranslateWithContext correlatedCtx combinedThetaExpr
                                |> Option.bind (fun thetaFilterH ->
                                    let equiKeysOpt =
                                        if head.EquiPairs.IsEmpty then
                                            Some([||], [||])
                                        else
                                            tryTranslateJoinKeys outerCtx innerCtx head.EquiPairs
                                            |> Option.map (fun k -> k.OuterKeys, k.InnerJoinKeys)

                                    equiKeysOpt
                                    |> Option.bind (fun (outerKeys, innerKeys) ->
                                        let uid = Guid.NewGuid().ToString "N"
                                        let ridCol = $"__subquery_rid_{grpIdx}_{uid}"

                                        let outerWithRidLf =
                                            PolarsWrapper.LazyFrameWithRowIndex(
                                                PolarsWrapper.LazyClone currentLf,
                                                ridCol,
                                                Nullable 0
                                            )

                                        let joinedLf =
                                            if outerKeys.Length = 0 then
                                                PolarsWrapper.Join(
                                                    outerWithRidLf,
                                                    rightLf,
                                                    [||],
                                                    [||],
                                                    PlJoinType.Cross,
                                                    rightSuffix,
                                                    PlJoinValidation.ManyToMany,
                                                    PlJoinCoalesce.KeepColumns,
                                                    PlJoinMaintainOrder.None,
                                                    PlJoinSide.None,
                                                    false,
                                                    Nullable(),
                                                    0UL
                                                )
                                            else
                                                PolarsWrapper.Join(
                                                    outerWithRidLf,
                                                    rightLf,
                                                    outerKeys |> Array.map PolarsWrapper.CloneExpr,
                                                    innerKeys |> Array.map PolarsWrapper.CloneExpr,
                                                    PlJoinType.Inner,
                                                    rightSuffix,
                                                    PlJoinValidation.ManyToMany,
                                                    PlJoinCoalesce.JoinSpecific,
                                                    PlJoinMaintainOrder.None,
                                                    PlJoinSide.None,
                                                    false,
                                                    Nullable(),
                                                    0UL
                                                )

                                        let filteredLf = PolarsWrapper.LazyFilter(joinedLf, thetaFilterH)

                                        let buildAgg (cand: ProjectionSubqueryCandidate) =
                                            if
                                                cand.IsAny
                                                || cand.MethodName = "Count"
                                                || cand.MethodName = "LongCount"
                                            then
                                                Some(PolarsWrapper.Len())
                                            elif cand.MethodName = "ToList" || cand.MethodName = "ToArray" then
                                                match cand.SelectorLambdaOpt with
                                                | Some selLam ->
                                                    ExprTranslator.tryTranslateWithContext correlatedCtx selLam.Body
                                                    |> Option.map (fun colH ->
                                                        PolarsWrapper.Implode(colH, maintainOrder = true))
                                                | None ->
                                                    let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                                                    if rightCols.Length > 0 then
                                                        let cName =
                                                            if leftCols.Contains rightCols.[0] then
                                                                rightCols.[0] + rightSuffix
                                                            else
                                                                rightCols.[0]

                                                        Some(
                                                            PolarsWrapper.Implode(
                                                                PolarsWrapper.Col cName,
                                                                maintainOrder = true
                                                            )
                                                        )
                                                    else
                                                        None
                                            else
                                                match cand.SelectorLambdaOpt with
                                                | Some selLam ->
                                                    ExprTranslator.tryTranslateWithContext correlatedCtx selLam.Body
                                                    |> Option.bind (
                                                        ExprTranslator.translateReductionOp cand.MethodName
                                                    )
                                                | None ->
                                                    let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                                                    if rightCols.Length > 0 then
                                                        let cName =
                                                            if leftCols.Contains rightCols.[0] then
                                                                rightCols.[0] + rightSuffix
                                                            else
                                                                rightCols.[0]

                                                        PolarsWrapper.Col cName
                                                        |> ExprTranslator.translateReductionOp cand.MethodName
                                                    else
                                                        None

                                        assignAggCols buildAgg indexedCandidates []
                                        |> Option.map (fun assignments ->
                                            let aggExprsToEmit =
                                                assignments
                                                |> List.choose (fun (_, _, exprOpt) -> exprOpt)
                                                |> List.toArray

                                            let groupedMatchesLf =
                                                PolarsWrapper.LazyGroupByAgg(
                                                    filteredLf,
                                                    [| PolarsWrapper.Col ridCol |],
                                                    aggExprsToEmit,
                                                    null,
                                                    false
                                                )

                                            let withRowIndexOp = QueryOp.WithRowIndex(ridCol, Some 0u)

                                            let joinOp =
                                                QueryOp.Join
                                                    {
                                                        RightLf = groupedMatchesLf
                                                        LeftOn = [| PolarsWrapper.Col ridCol |]
                                                        RightOn = [| PolarsWrapper.Col ridCol |]
                                                        How = PlJoinType.Left
                                                        Suffix = None
                                                        InvertSides = false
                                                        Coalesce = PlJoinCoalesce.JoinSpecific
                                                    }

                                            let fillNullOps = buildFillNullOps assignments
                                            let replacements = buildReplacements assignments
                                            [ withRowIndexOp; joinOp ] @ fillNullOps, replacements)))))

                let compiledGroupsOpt =
                    groups
                    |> List.mapi (fun idx grp -> compileGroup idx grp)
                    |> fun listOpts ->
                        if listOpts |> List.exists Option.isNone then
                            None
                        else
                            Some(listOpts |> List.choose id)

                match compiledGroupsOpt with
                | None -> None
                | Some compiledGroups ->
                    let allGeneratedOps = compiledGroups |> List.collect fst
                    let allReplacements = compiledGroups |> List.collect snd

                    let replacer = SubqueryExpressionReplacer allReplacements
                    let rewrittenBody = replacer.Visit foldedBody

                    let rewrittenLambda = Expression.Lambda(rewrittenBody, projLambda.Parameters)

                    Some(allGeneratedOps, rewrittenLambda)
