namespace Polars.NET.Linq

open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR
open System
open System.Reflection
open System.Linq.Expressions
open System.Linq
open System.Collections.Generic

/// Dedicated engine for decorrelating subqueries (EXISTS, NOT EXISTS, IN, NOT IN, scalar aggregates,
/// and embedded projection subqueries) into native Polars pushdown operations.
module internal SubqueryDecorrelator =

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
        | :? NewArrayExpression as nae ->
            nae.Expressions |> Seq.exists (containsParameter paramName)
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

    /// Checks whether an expression contains a queryable source or LINQ reduction call.
    let rec containsSubquery (expr: Expression) : bool =
        match expr with
        | null -> false
        | :? ConstantExpression as c ->
            match c.Value with
            | :? IQueryable
            | :? IPolarsInternalExplainable -> true
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
        match ExpressionPatterns.unwrap expr with
        | Binary(ExpressionType.AndAlso, left, right) -> flattenAnd left @ flattenAnd right
        | other -> [ other ]

    /// Creates an appropriate TranslationContext based on column names and element type.
    let createContextForTypeWithCols (elemType: Type) (colNames: string list) (paramName: string) : TranslationContext =
        if PolarsTypeHelper.IsScalarType elemType then
            if colNames.Length = 1 then
                TranslationContext.Scalar(paramName, colNames.[0])
            else
                TranslationContext.EntityWithCols(paramName, colNames)
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
                elif PolarsTypeHelper.IsScalarType actualT then
                    Set.empty
                else
                    let props =
                        PolarsTypeHelper.GetModelProperties actualT
                        |> Array.map (fun p -> p.Name)

                    let fields =
                        actualT.GetFields(BindingFlags.Public ||| BindingFlags.Instance)
                        |> Array.map (fun f -> f.Name)

                    Array.append props fields |> Set.ofArray

            let typeCols = getLeftMemberNames tOuter
            let lfCols = colNames |> Set.ofList
            let leftCols = Set.union typeCols lfCols
            TranslationContext.TupleJoin(paramName, leftCols, LinqConstants.DefaultRightSuffix)
        else
            TranslationContext.EntityWithCols(paramName, colNames)

    /// Creates an appropriate TranslationContext based on whether the sequence element type
    /// is a scalar/primitive (bound to the underlying single physical column) or a composite entity.
    let createContextForType (elemType: Type) (plan: LogicalPlan) (paramName: string) : TranslationContext =
        let colNames = (SchemaInference.inferPlanSchema plan).ColumnNames
        createContextForTypeWithCols elemType colNames paramName

    /// Evaluates an uncorrelated expression into a scalar ConstantExpression.
    let tryEvaluateUncorrelatedScalar (expr: Expression) : Expression option =
        try
            let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(expr, typeof<obj>)).Compile()
            let resultVal = lambda.Invoke()
            CompilationScope.TryCurrent |> Option.iter (fun scope -> scope.EvaluatedAtCompile <- true)

            if isNull resultVal then
                Some(Expression.Constant(null, expr.Type) :> Expression)
            else
                Some(Expression.Constant(resultVal, expr.Type) :> Expression)
        with _ ->
            None

    /// Recursively folds any uncorrelated scalar subquery within an expression into an Expression.Constant.
    let rec foldUncorrelatedSubqueries (outerNames: Set<string>) (expr: Expression) : Expression * bool =
        match expr with
        | null -> null, false
        | _ when
            containsSubquery expr
            && not (outerNames |> Set.exists (fun name -> containsParameter name expr))
            && PolarsTypeHelper.IsScalarType expr.Type
            ->
            match tryEvaluateUncorrelatedScalar expr with
            | Some foldedConstant -> foldedConstant, true
            | None -> expr, false
        | Binary(op, left, right) ->
            let newLeft, leftChanged = foldUncorrelatedSubqueries outerNames left
            let newRight, rightChanged = foldUncorrelatedSubqueries outerNames right

            if leftChanged || rightChanged then
                Expression.MakeBinary(op, newLeft, newRight) :> Expression, true
            else
                expr, false
        | Unary(op, inner) ->
            let newInner, innerChanged = foldUncorrelatedSubqueries outerNames inner

            if innerChanged then
                Expression.MakeUnary(op, newInner, expr.Type) :> Expression, true
            else
                expr, false
        | MethodCall(m, target, args) ->
            let newTarget, targetChanged =
                if isNull target then
                    null, false
                else
                    foldUncorrelatedSubqueries outerNames target

            let newArgsWithFlags = args |> List.map (foldUncorrelatedSubqueries outerNames)
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
            let newTest, testChanged = foldUncorrelatedSubqueries outerNames condExpr.Test

            let newIfTrue, trueChanged =
                foldUncorrelatedSubqueries outerNames condExpr.IfTrue

            let newIfFalse, falseChanged =
                foldUncorrelatedSubqueries outerNames condExpr.IfFalse

            if testChanged || trueChanged || falseChanged then
                Expression.Condition(newTest, newIfTrue, newIfFalse) :> Expression, true
            else
                expr, false
        | MemberAccess(target, m) when not (isNull target) ->
            let newTarget, targetChanged = foldUncorrelatedSubqueries outerNames target

            if targetChanged then
                Expression.MakeMemberAccess(newTarget, m) :> Expression, true
            else
                expr, false
        | :? NewExpression as ne ->
            let newArgsWithFlags =
                ne.Arguments
                |> Seq.map (foldUncorrelatedSubqueries outerNames)
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
                foldUncorrelatedSubqueries outerNames mie.NewExpression

            let newBindingsWithFlags =
                mie.Bindings
                |> Seq.map (function
                    | :? MemberAssignment as ma ->
                        let newInit, initChanged = foldUncorrelatedSubqueries outerNames ma.Expression
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
            TargetType: Type
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
        match ExpressionPatterns.unwrap seqExpr with
        | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" ->
            let rootSrc, lams = unwrapWhere src
            rootSrc, lams @ [ lam ]
        | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" ->
            let rootSrc, lams = unwrapWhere src
            rootSrc, lams @ [ lam ]
        | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when mSel.Name = "Select" ->
            let rootSrc, lams = unwrapWhere src
            if not lams.IsEmpty then rootSrc, lams else seqExpr, []
        | MethodCall(mSel, src, [ CleanLambda selLam ]) when mSel.Name = "Select" ->
            let rootSrc, lams = unwrapWhere src
            if not lams.IsEmpty then rootSrc, lams else seqExpr, []
        | other -> other, []

    // Pattern 1: Any(innerSeq, [predicate]) or innerSeq.Where(predicate).Any()
    let internal tryMatchAny (e: Expression) : (Expression * LambdaExpression option) option =
        match e with
        | MethodCall(m, null, [ innerSeq; CleanLambda lam ]) when m.Name = "Any" -> Some(innerSeq, Some lam)
        | MethodCall(m, innerSeq, [ CleanLambda lam ]) when m.Name = "Any" -> Some(innerSeq, Some lam)
        | MethodCall(m, null, [ innerSeq ]) when m.Name = "Any" ->
            match ExpressionPatterns.unwrap innerSeq with
            | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | _ -> Some(innerSeq, None)
        | MethodCall(m, innerSeq, []) when m.Name = "Any" ->
            match ExpressionPatterns.unwrap innerSeq with
            | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | _ -> Some(innerSeq, None)
        | _ -> None

    // Pattern 2: innerSeq.Contains(keyExpr) or Contains(innerSeq, keyExpr).
    // A trailing non-scalar Select is split off as the key selector; a scalar-element sequence is kept whole
    // (it resolves to a single-column plan, and splitting could leave a source that does not resolve on its
    // own, e.g. GroupBy(..).Where(g => ..) without its key Select).
    let internal tryMatchContains (e: Expression) : (Expression * LambdaExpression option * Expression) option =
        let matchSeqAndKey (innerSeq: Expression) (keyExpr: Expression) =
            match ExpressionPatterns.unwrap innerSeq with
            | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when
                mSel.Name = "Select" && not (PolarsTypeHelper.IsScalarType selLam.ReturnType) ->
                Some(src, Some selLam, keyExpr)
            | MethodCall(mSel, src, [ CleanLambda selLam ]) when
                mSel.Name = "Select" && not (PolarsTypeHelper.IsScalarType selLam.ReturnType) ->
                Some(src, Some selLam, keyExpr)
            | otherSeq -> Some(otherSeq, None, keyExpr)

        match e with
        | MethodCall(m, null, [ innerSeq; keyExpr ]) when m.Name = "Contains" -> matchSeqAndKey innerSeq keyExpr
        | MethodCall(m, innerSeq, [ keyExpr ]) when m.Name = "Contains" -> matchSeqAndKey innerSeq keyExpr
        | _ -> None

    // Pattern 3: AggReductionCall (Count, Sum, Avg, Min, Max, etc.)
    let internal tryMatchAggCall (expr: Expression) : AggReductionCall option =
        let unwrapClean (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? MethodCallExpression as mc -> mc
            | _ -> null

        let mc = unwrapClean expr

        if isNull mc || not (isAggReductionMethod mc.Method.Name) then
            None
        else
            let methodName = mc.Method.Name
            let isCount = methodName = "Count" || methodName = "LongCount"

            let extractSeqAndSelector seqExpr =
                match ExpressionPatterns.unwrap seqExpr with
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
                        TargetType = mc.Type
                        SelectorLambdaOpt = selLamOpt
                        ExtraWhereLambdaOpt = None
                    }
            | null, [ innerSeq; CleanLambda lam ] ->
                Some
                    {
                        InnerSeq = innerSeq
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = if isCount then None else Some lam
                        ExtraWhereLambdaOpt = if isCount then Some lam else None
                    }
            | target, [] when not (isNull target) ->
                let src, selLamOpt = extractSeqAndSelector target

                Some
                    {
                        InnerSeq = src
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = selLamOpt
                        ExtraWhereLambdaOpt = None
                    }
            | target, [ CleanLambda lam ] when not (isNull target) ->
                Some
                    {
                        InnerSeq = target
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = if isCount then None else Some lam
                        ExtraWhereLambdaOpt = if isCount then Some lam else None
                    }
            | _ -> None

    let internal tryExtractScaledAgg (expr: Expression) : (Expression * AggScale) option =
        let unwrapClean = ExpressionPatterns.unwrap expr

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
            /// Conditions on the outer row alone (`x.c != null` in `Any(y => y.k == x.k && x.c != null)`).
            OuterFilters: Expression list
        }

    /// Classifies a list of conjunct conditions into equi-join pairs (outer, inner),
    /// cross-table theta conditions (inequalities), pure inner filters, and pure outer filters.
    let internal classifyConditions
        (outerNames: Set<string>)
        (innerName: string)
        (conditions: Expression list)
        : ClassifiedConditions option =
        let containsOuter (e: Expression) =
            outerNames |> Set.exists (fun name -> containsParameter name e)

        let classify (cond: Expression) =
            match ExpressionPatterns.unwrap cond with
            | Binary(ExpressionType.Equal, left, right) ->
                let cleanL = ExpressionPatterns.unwrap left
                let cleanR = ExpressionPatterns.unwrap right
                let lOuter = containsOuter cleanL
                let lInner = containsParameter innerName cleanL
                let rOuter = containsOuter cleanR
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
                        Choice4Of4 cond
            | other ->
                let hasOuter = containsOuter other
                let hasInner = containsParameter innerName other

                if not hasOuter && hasInner then Choice2Of4 other
                elif not hasOuter && not hasInner then Choice2Of4 other
                elif hasOuter && hasInner then Choice3Of4 other
                else Choice4Of4 other

        let classified = conditions |> List.map classify

        // A subquery inside an outer-only condition is not decorrelated here.
        let hasUnsupported =
            classified
            |> List.exists (function
                | Choice4Of4 c -> containsSubquery c
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

            let outerFilters =
                classified
                |> List.choose (function
                    | Choice4Of4 c -> Some c
                    | _ -> None)

            if equiPairs.IsEmpty && thetaConditions.IsEmpty then
                None
            else
                Some
                    {
                        EquiPairs = equiPairs
                        ThetaConditions = thetaConditions
                        InnerFilters = innerFilters
                        OuterFilters = outerFilters
                    }

    type internal CorrelatedParts =
        {
            RootSource: Expression
            InnerParam: ParameterExpression
            EquiPairs: (Expression * Expression) list
            /// Conditions on both rows, evaluated on the joined rows. They include the outer-only conditions,
            /// which belong to the subquery's predicate: `Count(y => p(y) && q(x))` is 0 where q(x) is false.
            ThetaConditions: Expression list
            InnerFilters: Expression list
            /// The outer-only conditions (also in ThetaConditions). A positive Any may hoist them to a filter
            /// on the outer rows instead: `Any(y => p && q(x))` is `q(x) && Any(y => p)`.
            OuterFilters: Expression list
        }

    /// Recursively unwraps Where cascades, validates decorrelation conditions against outerNames,
    /// and extracts root source, inner parameter, equi-pairs, theta conditions, and inner filters.
    let internal tryExtractCorrelatedParts
        (outerNames: Set<string>)
        (innerSeq: Expression)
        (extraWhereLambdaOpt: LambdaExpression option)
        : CorrelatedParts option =
        let rootSource, whereLams = unwrapWhere innerSeq

        let allWhereLams =
            match extraWhereLambdaOpt with
            | Some w -> whereLams @ [ w ]
            | None -> whereLams

        let rootHasOuter = outerNames |> Set.exists (fun name -> containsParameter name rootSource)
        if allWhereLams.IsEmpty || rootHasOuter then
            None
        else
            let innerParam = allWhereLams.[0].Parameters.[0]
            // Each predicate names the inner row with its own parameter (`s.Where(y => ..).Count(z => ..)`);
            // all are read as the first one's.
            let rebind (lam: LambdaExpression) : Expression =
                let own = lam.Parameters.[0]
                if obj.ReferenceEquals(own, innerParam) then
                    lam.Body
                else
                    { new ExpressionVisitor() with
                        override _.VisitParameter(node: ParameterExpression) =
                            if obj.ReferenceEquals(node, own) then innerParam :> Expression else node :> Expression
                    }.Visit lam.Body
            let conditions = allWhereLams |> List.collect (fun lam -> flattenAnd (rebind lam))

            classifyConditions outerNames innerParam.Name conditions
            |> Option.map (fun c ->
                {
                    RootSource = rootSource
                    InnerParam = innerParam
                    EquiPairs = c.EquiPairs
                    ThetaConditions = c.ThetaConditions @ c.OuterFilters
                    InnerFilters = c.InnerFilters
                    OuterFilters = c.OuterFilters
                })

    /// Combines and applies pushdown inner filters onto the right-side LogicalPlan.
    let internal tryApplyInnerFilters
        (rawRightPlan: LogicalPlan)
        (innerParam: ParameterExpression)
        (innerFilters: Expression list)
        : LogicalPlan option =
        if innerFilters.IsEmpty then
            Some rawRightPlan
        else
            let combinedInner =
                innerFilters |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

            let innerCols = (SchemaInference.inferPlanSchema rawRightPlan).ColumnNames
            let innerCtx = createContextForTypeWithCols innerParam.Type innerCols innerParam.Name

            ExprTranslator.tryTranslateToScalar innerCtx combinedInner
            |> Option.map (fun filterScalar ->
                LogicalPlan.Filter(rawRightPlan, filterScalar))

    type internal SymbolicJoinKeys =
        {
            OuterKeys: ScalarExpr list
            InnerKeys: ScalarExpr list
        }

    /// Safely translates join keys across outer and inner contexts into symbolic ScalarExpr.
    let internal tryTranslateJoinKeys
        (outerCtx: TranslationContext)
        (innerCtx: TranslationContext)
        (equiPairs: (Expression * Expression) list)
        : SymbolicJoinKeys option =
        let translated =
            equiPairs
            |> List.choose (fun (outerE, innerE) ->
                match
                    ExprTranslator.tryTranslateToScalar outerCtx outerE,
                    ExprTranslator.tryTranslateToScalar innerCtx innerE
                with
                | Some lk, Some rk -> Some(lk, rk)
                | _ -> None)

        if translated.Length <> equiPairs.Length then
            None
        else
            Some
                {
                    OuterKeys = translated |> List.map fst
                    InnerKeys = translated |> List.map snd
                }

    /// The inner side of a theta join with its equi keys computed into fresh columns, and those columns. Joining
    /// on them leaves the side's own columns in place: a coalescing join drops only the fresh key columns, while
    /// the theta filter and the aggregates read the side's columns (keys included) through the suffix.
    let internal withFreshKeyColumns (rightPlan: LogicalPlan) (innerKeys: ScalarExpr list) : LogicalPlan * ScalarExpr list =
        let uid = Guid.NewGuid().ToString "N"
        let names = innerKeys |> List.mapi (fun i _ -> $"__corr_key_{uid}_{i}")
        LogicalPlan.WithColumns(rightPlan, List.zip names innerKeys), names |> List.map ScalarExpr.Column

    /// Builds a symbolic reduction aggregation expression (Len() for Count/Any, or translated reduction op).
    let internal tryBuildAggScalarWithContext
        (rightCols: string list)
        (methodName: string)
        (isAnyOrCount: bool)
        (selectorLambdaOpt: LambdaExpression option)
        (explicitCtxOpt: TranslationContext option)
        (colNameResolver: string -> string)
        : ScalarExpr option =
        if isAnyOrCount || methodName = "Count" || methodName = "LongCount" then
            Some(ScalarExpr.Aggregation(AggKind.Len, None, false))
        elif methodName = "ToList" || methodName = "ToArray" then
            match selectorLambdaOpt with
            | Some selLam ->
                let ctx =
                    match explicitCtxOpt with
                    | Some c -> c
                    | None ->
                        let selParam = selLam.Parameters.[0]
                        createContextForTypeWithCols selParam.Type rightCols selParam.Name

                ExprTranslator.tryTranslateToScalar ctx selLam.Body
                |> Option.map (fun colScalar -> ScalarExpr.Aggregation(AggKind.List, Some colScalar, false))
            | None ->
                if not rightCols.IsEmpty then
                    let cName = colNameResolver rightCols.[0]
                    Some(ScalarExpr.Aggregation(AggKind.List, Some(ScalarExpr.Column cName), false))
                else
                    None
        else
            let aggKindOpt =
                match methodName with
                | "Sum" -> Some AggKind.Sum
                | "Average" | "Mean" -> Some AggKind.Mean
                | "Min" -> Some AggKind.Min
                | "Max" -> Some AggKind.Max
                | "Median" -> Some AggKind.Median
                | _ -> None

            match aggKindOpt with
            | None -> None
            | Some kind ->
                match selectorLambdaOpt with
                | Some selLam ->
                    let ctx =
                        match explicitCtxOpt with
                        | Some c -> c
                        | None ->
                            let selParam = selLam.Parameters.[0]
                            createContextForTypeWithCols selParam.Type rightCols selParam.Name

                    ExprTranslator.tryTranslateToScalar ctx selLam.Body
                    |> Option.map (fun colScalar -> ScalarExpr.Aggregation(kind, Some colScalar, false))
                | None ->
                    if not rightCols.IsEmpty then
                        let cName = colNameResolver rightCols.[0]
                        Some(ScalarExpr.Aggregation(kind, Some(ScalarExpr.Column cName), false))
                    else
                        None

    let internal tryBuildAggScalar
        (rightCols: string list)
        (methodName: string)
        (isAnyOrCount: bool)
        (selectorLambdaOpt: LambdaExpression option)
        : ScalarExpr option =
        tryBuildAggScalarWithContext rightCols methodName isAnyOrCount selectorLambdaOpt None id

    // =========================================================================
    // 3b. Uncorrelated Scalar Subqueries as One-Row Plans
    // =========================================================================

    /// An uncorrelated scalar subquery lowered to a one-row plan whose single column holds its value.
    type internal UncorrelatedScalar = { ColumnName: string; Plan: LogicalPlan }

    /// Applies Where lambdas, each translated over its own parameter, to a sequence plan.
    let private tryApplyWhereLambdas (plan: LogicalPlan) (lams: LambdaExpression list) : LogicalPlan option =
        lams
        |> List.fold (fun accOpt lam ->
            accOpt
            |> Option.bind (fun acc ->
                let cols = (SchemaInference.inferPlanSchema acc).ColumnNames
                let p = lam.Parameters.[0]
                let ctx = createContextForTypeWithCols p.Type cols p.Name
                ExprTranslator.tryTranslateToScalar ctx lam.Body |> Option.map (fun s -> LogicalPlan.Filter(acc, s)))) (Some plan)

    /// Lowers one uncorrelated scalar subquery (an aggregate, Any, or First/FirstOrDefault over a sequence) to a
    /// one-row plan, returning the expression that replaces it (a reference to the plan's column). First, Last,
    /// Single and ElementAt fail on a wrong row count through ScalarFunction.AssertCount; a backend that cannot assert
    /// leaves them to eager evaluation.
    let private tryLowerUncorrelatedScalar
        (resolveSequencePlan: Expression -> LogicalPlan option)
        (node: Expression)
        : (Expression * UncorrelatedScalar) option =
        let colName = $"__subquery_agg_u{Guid.NewGuid():N}"
        let resolveFiltered (seqExpr: Expression) (extra: LambdaExpression list) =
            let rootSource, whereLams = unwrapWhere seqExpr
            resolveSequencePlan rootSource |> Option.bind (fun plan -> tryApplyWhereLambdas plan (whereLams @ extra))

        // First / Single / ElementAt (and their OrDefault forms): the element (or its projection) of a one-row
        // aggregate over the first rows, guarded by a count check where LINQ throws on a wrong count.
        // `path` holds the members read from the element (`seq.Single(p).V`), applied after the projection.
        let elementOf (mc: MethodCallExpression) (path: System.Reflection.MemberInfo list) =
            let name = mc.Method.Name
            let isOrDefault = name.EndsWith "OrDefault"
            let baseName = if isOrDefault then name.Substring(0, name.Length - "OrDefault".Length) else name
            let args = mc.Arguments |> Seq.toList
            let seqExpr, predOpt, indexOpt =
                match baseName, mc.Object, args with
                | ("First" | "Last" | "Single"), null, [ s ] -> Some s, None, None
                | ("First" | "Last" | "Single"), null, [ s; CleanLambda p ] -> Some s, Some p, None
                | ("First" | "Last" | "Single"), t, [] when not (isNull t) -> Some t, None, None
                | ("First" | "Last" | "Single"), t, [ CleanLambda p ] when not (isNull t) -> Some t, Some p, None
                | "ElementAt", null, [ s; i ] when i.Type = typeof<int> ->
                    match PartialEvaluator.tryEvaluate i with
                    | Some(:? int as n) -> Some s, None, Some n
                    | _ -> None, None, None
                | _ -> None, None, None

            // The rows the element is picked from (a slice, or all for Last) and how it is picked.
            let window, check =
                let noElements = if predOpt.IsSome then "Sequence contains no matching element" else "Sequence contains no elements"
                let manyElements = if predOpt.IsSome then "Sequence contains more than one matching element" else "Sequence contains more than one element"
                let invalid msg = { Kind = QueryErrorKind.InvalidOperation; Message = msg }
                let atLeastOne error = { Min = 1L; Max = None; TooFew = error; TooMany = error }
                match baseName, indexOpt with
                | "First", _ -> Some(Some(0L, 1u), AggKind.First), (if isOrDefault then None else Some(atLeastOne (invalid noElements)))
                | "Last", _ -> Some(None, AggKind.Last), (if isOrDefault then None else Some(atLeastOne (invalid noElements)))
                | "Single", _ ->
                    Some(Some(0L, 2u), AggKind.First),
                    Some { Min = (if isOrDefault then 0L else 1L); Max = Some 1L; TooFew = invalid noElements; TooMany = invalid manyElements }
                | "ElementAt", Some n when n >= 0 ->
                    // Enumerable.ElementAt raises the exception's default message.
                    let outOfRange = { Kind = QueryErrorKind.ArgumentOutOfRange; Message = "" }
                    Some(Some(int64 n, 1u), AggKind.First), (if isOrDefault then None else Some(atLeastOne outOfRange))
                | _ -> None, None

            match seqExpr, window with
            | Some s, Some(slice, pick) when check.IsNone || CompilationScope.Current.Backend.SupportsAssertions ->
                let src, selOpt =
                    match ExpressionPatterns.unwrap s with
                    | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when mSel.Name = "Select" -> src, Some selLam
                    | MethodCall(mSel, src, [ CleanLambda selLam ]) when mSel.Name = "Select" -> src, Some selLam
                    | other -> other, None
                let selOpt =
                    if path.IsEmpty then
                        selOpt
                    else
                        let lam =
                            selOpt
                            |> Option.defaultWith (fun () ->
                                let p = Expression.Parameter(mc.Type, "e")
                                Expression.Lambda(p, p))
                        let body = path |> List.fold (fun acc m -> Expression.MakeMemberAccess(acc, m) :> Expression) lam.Body
                        Some(Expression.Lambda(body, lam.Parameters))
                resolveFiltered src (Option.toList predOpt)
                |> Option.bind (fun plan ->
                    let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
                    let valueOpt =
                        match selOpt with
                        | Some selLam ->
                            let p = selLam.Parameters.[0]
                            ExprTranslator.tryTranslateToScalar (createContextForTypeWithCols p.Type cols p.Name) selLam.Body
                        | None when cols.Length = 1 && PolarsTypeHelper.IsScalarType mc.Type -> Some(ScalarExpr.Column cols.Head)
                        | None -> None
                    valueOpt
                    |> Option.map (fun v ->
                        let projected = LogicalPlan.Project(plan, [ (colName, v) ])
                        let rows =
                            match slice with
                            | Some(offset, count) -> LogicalPlan.Limit(projected, offset, count)
                            | None -> projected
                        let picked = ScalarExpr.Aggregation(pick, Some(ScalarExpr.Column colName), false)
                        // An OrDefault over no rows is default(T): null, or the zero of a non-nullable value type.
                        // (A member read from a missing element throws in LINQ; here it propagates null.)
                        let element =
                            if isOrDefault && path.IsEmpty && mc.Type.IsValueType && isNull (Nullable.GetUnderlyingType mc.Type) then
                                ScalarExpr.Coalesce [ picked; ScalarExprOps.toLiteral (Activator.CreateInstance mc.Type) mc.Type ]
                            else
                                picked
                        let guarded =
                            match check with
                            | Some c ->
                                ScalarExpr.Function(
                                    ScalarFunction.AssertCount c,
                                    [ ScalarExpr.Aggregation(AggKind.Len, None, false); element ],
                                    LogicalType.Unknown
                                )
                            | None -> element
                        LogicalPlan.Project(rows, [ (colName, guarded) ])))
            | _ -> None

        let isElementOp (name: string) =
            match name with
            | "First" | "FirstOrDefault" | "Last" | "LastOrDefault" | "Single" | "SingleOrDefault" | "ElementAt"
            | "ElementAtOrDefault" -> true
            | _ -> false

        let rec peelMembers (e: Expression) (path: System.Reflection.MemberInfo list) =
            match ExpressionPatterns.unwrap e with
            | :? MemberExpression as me when not (isNull me.Expression) -> peelMembers me.Expression (me.Member :: path)
            | :? MethodCallExpression as mc when isElementOp mc.Method.Name -> Some(mc, path)
            | _ -> None

        match peelMembers node [] with
        | Some(mc, path) ->
            elementOf mc path
            |> Option.map (fun plan -> Expression.Parameter(node.Type, colName) :> Expression, { ColumnName = colName; Plan = plan })
        | None ->
        match ExpressionPatterns.unwrap node with
        | unwrapped ->
            match tryMatchAny unwrapped with
            | Some(innerSeq, predOpt) ->
                resolveFiltered innerSeq (Option.toList predOpt)
                |> Option.map (fun plan ->
                    let countPlan = LogicalPlan.Project(plan, [ (colName, ScalarExpr.Aggregation(AggKind.Len, None, false)) ])
                    let repl = Expression.GreaterThan(Expression.Parameter(typeof<uint32>, colName), Expression.Constant 0u) :> Expression
                    repl, { ColumnName = colName; Plan = countPlan })
            | None ->
                match tryMatchAggCall unwrapped with
                | Some agg when PolarsTypeHelper.IsScalarType agg.TargetType ->
                    resolveFiltered agg.InnerSeq (Option.toList agg.ExtraWhereLambdaOpt)
                    |> Option.bind (fun plan ->
                        let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
                        let isCount = agg.MethodName = "Count" || agg.MethodName = "LongCount"
                        tryBuildAggScalar cols agg.MethodName isCount agg.SelectorLambdaOpt
                        |> Option.map (fun aggScalar ->
                            Expression.Parameter(node.Type, colName) :> Expression,
                            { ColumnName = colName; Plan = LogicalPlan.Project(plan, [ (colName, aggScalar) ]) }))
                | _ -> None

    /// Replaces the uncorrelated scalar subqueries of an expression by references to one-row plans, which the
    /// caller attaches with `attachUncorrelatedScalars`. A subquery that cannot be lowered (Single on a backend
    /// without assertions, for instance) is still evaluated eagerly into a constant, which marks the compilation
    /// as evaluated at compile time. Returns None when the expression has no such subquery.
    let internal tryRewriteUncorrelatedScalars
        (resolveSequencePlan: Expression -> LogicalPlan option)
        (outerNames: Set<string>)
        (expr: Expression)
        : (Expression * UncorrelatedScalar list) option =
        let scalars = ResizeArray<UncorrelatedScalar>()
        let mutable folded = false
        let isCandidate (e: Expression) =
            containsSubquery e
            && not (outerNames |> Set.exists (fun name -> containsParameter name e))
            && PolarsTypeHelper.IsScalarType e.Type

        let rewritten =
            { new ExpressionVisitor() with
                override self.Visit(node: Expression) =
                    if isNull node || not (isCandidate node) then
                        base.Visit node
                    else
                        match tryLowerUncorrelatedScalar resolveSequencePlan node with
                        | Some(repl, scalar) ->
                            scalars.Add scalar
                            repl
                        | None ->
                            let visited = base.Visit node
                            if not (obj.ReferenceEquals(visited, node)) then
                                visited
                            else
                                match tryEvaluateUncorrelatedScalar node with
                                | Some constant ->
                                    folded <- true
                                    constant
                                | None -> node
            }.Visit(expr)

        if scalars.Count = 0 && not folded then None else Some(rewritten, List.ofSeq scalars)

    /// Attaches one-row scalar plans to a plan as extra columns, keeping its row order (a left join on a
    /// constant key, rather than a cross join, whose order is not guaranteed).
    let internal attachUncorrelatedScalars (plan: LogicalPlan) (scalars: UncorrelatedScalar list) : LogicalPlan =
        scalars
        |> List.fold (fun acc scalar ->
            let keyCol = $"__subquery_key_{Guid.NewGuid():N}"
            let key = ScalarExpr.Literal(LiteralValue.Int32 1)
            let left = LogicalPlan.WithColumns(acc, [ (keyCol, key) ])
            let right = LogicalPlan.WithColumns(scalar.Plan, [ (keyCol, key) ])
            let joined = LogicalPlan.Join(left, right, [ ScalarExpr.Column keyCol ], [ ScalarExpr.Column keyCol ], JoinKind.Left, None, JoinKeyCoalescing.Default, false)
            let keep = (SchemaInference.inferPlanSchema acc).ColumnNames @ [ scalar.ColumnName ]
            LogicalPlan.Project(joined, keep |> List.map (fun c -> c, ScalarExpr.Column c))) plan

    // =========================================================================
    // 4. Semi/Anti Join Filter Decorrelation (Step 1)
    // =========================================================================

    /// Tries to decorrelate a single conjunct expression relative to outerParam into Semi/Anti join operations.
    let internal tryDecorrelateConjunct
        (resolveSequencePlan: Expression -> LogicalPlan option)
        (outerPlan: LogicalPlan)
        (outerParam: ParameterExpression)
        (conjunctExpr: Expression)
        (rowShape: RowShape)
        : LogicalPlan option =
        let outerName = outerParam.Name
        let outerNames =
            Set.ofSeq (outerName :: (Set.toList rowShape.ScopeNames))

        let outerCols = (SchemaInference.inferPlanSchema outerPlan).ColumnNames
        let outerCtx =
            if not rowShape.IsEmpty then
                TranslationContext.ScopedEntity([ outerName ], rowShape, outerCols)
            else
                createContextForType outerParam.Type outerPlan outerName

        let isNegated, coreExpr =
            match ExpressionPatterns.unwrap conjunctExpr with
            | Unary(ExpressionType.Not, inner) -> true, ExpressionPatterns.unwrap inner
            | other -> false, other

        let tryAny () =
            option {
                let! innerSource, innerLambdaOpt = tryMatchAny coreExpr
                let! extracted = tryExtractCorrelatedParts outerNames innerSource innerLambdaOpt

                // `Any(y => p && q(x))` is `q(x) && Any(y => p)`: filter the outer rows before the semi join. A
                // negated Any keeps the rows where q(x) is false, so it evaluates q(x) with the other conditions.
                let! outerPlan, parts =
                    if isNegated || extracted.OuterFilters.IsEmpty then
                        Some(outerPlan, extracted)
                    else
                        extracted.OuterFilters
                        |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)
                        |> ExprTranslator.tryTranslateToScalar outerCtx
                        |> Option.map (fun pred ->
                            LogicalPlan.Filter(outerPlan, pred),
                            { extracted with
                                ThetaConditions =
                                    extracted.ThetaConditions
                                    |> List.filter (fun c -> not (extracted.OuterFilters |> List.exists (fun o -> obj.ReferenceEquals(o, c))))
                                OuterFilters = [] })

                let! rawRightPlan = resolveSequencePlan parts.RootSource
                let! rightPlan = tryApplyInnerFilters rawRightPlan parts.InnerParam parts.InnerFilters

                let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                let innerCtx =
                    createContextForTypeWithCols parts.InnerParam.Type rightCols parts.InnerParam.Name

                let joinHow = if isNegated then JoinKind.Anti else JoinKind.Semi

                if parts.ThetaConditions.IsEmpty then
                    let! keys = tryTranslateJoinKeys outerCtx innerCtx parts.EquiPairs
                    return LogicalPlan.Join(outerPlan, rightPlan, keys.OuterKeys, keys.InnerKeys, joinHow, None, JoinKeyCoalescing.Default, NullSemantics.nullKeysMatch ())
                else
                    let leftCols = outerCols |> Set.ofList
                    let originalCols = outerCols

                    let correlatedCtx =
                        TranslationContext.CorrelatedJoin([ outerName ], parts.InnerParam.Name, leftCols, "_right", rowShape)

                    let combinedThetaExpr =
                        parts.ThetaConditions
                        |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

                    let! thetaFilterScalar =
                        ExprTranslator.tryTranslateToScalar correlatedCtx combinedThetaExpr

                    let outerKeys = parts.EquiPairs |> List.choose (fun (o, _) -> ExprTranslator.tryTranslateToScalar outerCtx o)
                    let innerKeys = parts.EquiPairs |> List.choose (fun (_, i) -> ExprTranslator.tryTranslateToScalar innerCtx i)

                    let uid = Guid.NewGuid().ToString "N"
                    let ridCol = $"__subquery_rid_{uid}"

                    let withRidPlan = LogicalPlan.WithRowIndex(outerPlan, ridCol, Some 0u)
                    let joinedPlan =
                        if outerKeys.IsEmpty then
                            LogicalPlan.Join(withRidPlan, rightPlan, [], [], JoinKind.Cross, Some LinqConstants.DefaultRightSuffix, JoinKeyCoalescing.KeepBoth, false)
                        else
                            (let keyedRight, keyCols = withFreshKeyColumns rightPlan innerKeys in LogicalPlan.Join(withRidPlan, keyedRight, outerKeys, keyCols, JoinKind.Inner, Some LinqConstants.DefaultRightSuffix, JoinKeyCoalescing.Default, NullSemantics.nullKeysMatch ()))

                    let filteredPlan = LogicalPlan.Filter(joinedPlan, thetaFilterScalar)
                    let selectedPlan = LogicalPlan.Project(filteredPlan, [ (ridCol, ScalarExpr.Column ridCol) ])
                    let matchedRidsPlan = LogicalPlan.Distinct(selectedPlan, None, DistinctKeep.Any, false)

                    let semiJoinPlan =
                        LogicalPlan.Join(withRidPlan, matchedRidsPlan, [ ScalarExpr.Column ridCol ], [ ScalarExpr.Column ridCol ], joinHow, None, JoinKeyCoalescing.Default, false)

                    let restoreSchemaPlan =
                        LogicalPlan.Project(semiJoinPlan, originalCols |> List.map (fun c -> c, ScalarExpr.Column c))

                    return restoreSchemaPlan
            }

        let tryContains () =
            option {
                let! innerSource, selLambdaOpt, outerKeyExpr = tryMatchContains coreExpr
                do! guard (not (outerNames |> Set.exists (fun name -> containsParameter name innerSource)))
                let! rightPlan = resolveSequencePlan innerSource
                let! leftKey = ExprTranslator.tryTranslateToScalar outerCtx outerKeyExpr
                let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames

                let! rightKey =
                    match selLambdaOpt with
                    | Some selLambda ->
                        let p = selLambda.Parameters.[0]
                        let innerCtx = createContextForTypeWithCols p.Type rightCols p.Name
                        ExprTranslator.tryTranslateToScalar innerCtx selLambda.Body
                    | None ->
                        if not rightCols.IsEmpty then
                            Some(ScalarExpr.Column rightCols.[0])
                        else
                            None

                let joinHow = if isNegated then JoinKind.Anti else JoinKind.Semi
                return LogicalPlan.Join(outerPlan, rightPlan, [ leftKey ], [ rightKey ], joinHow, None, JoinKeyCoalescing.Default, NullSemantics.nullKeysMatch ())
            }

        let rec tryDisjunctiveAny (disjExpr: Expression) : LogicalPlan option =
            match ExpressionPatterns.unwrap disjExpr with
            | Binary((ExpressionType.OrElse | ExpressionType.Or), left, right) ->
                let rec flattenOr (e: Expression) =
                    match ExpressionPatterns.unwrap e with
                    | Binary((ExpressionType.OrElse | ExpressionType.Or), l, r) -> flattenOr l @ flattenOr r
                    | other -> [ other ]
                let branches = flattenOr left @ flattenOr right
                let extractBranchPlan (branch: Expression) : LogicalPlan option =
                    option {
                        let! innerSource, innerLambdaOpt = tryMatchAny (ExpressionPatterns.unwrap branch)
                        let! parts = tryExtractCorrelatedParts outerNames innerSource innerLambdaOpt
                        do! guard (parts.ThetaConditions.IsEmpty && parts.EquiPairs.Length = 1)
                        let! rawRightPlan = resolveSequencePlan parts.RootSource
                        let! rightPlan = tryApplyInnerFilters rawRightPlan parts.InnerParam parts.InnerFilters
                        let innerCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                        let innerCtx = createContextForTypeWithCols parts.InnerParam.Type innerCols parts.InnerParam.Name
                        let! keys = tryTranslateJoinKeys outerCtx innerCtx parts.EquiPairs
                        let keyAlias = "__disj_any_key__"
                        let projPlan = LogicalPlan.Project(rightPlan, [ (keyAlias, keys.InnerKeys.Head) ])
                        return projPlan
                    }
                let extractBranchOuterKey (branch: Expression) : ScalarExpr option =
                    option {
                        let! innerSource, innerLambdaOpt = tryMatchAny (ExpressionPatterns.unwrap branch)
                        let! parts = tryExtractCorrelatedParts outerNames innerSource innerLambdaOpt
                        do! guard (parts.ThetaConditions.IsEmpty && parts.EquiPairs.Length = 1)
                        let outerKeyExpr, _ = parts.EquiPairs.[0]
                        return! ExprTranslator.tryTranslateToScalar outerCtx outerKeyExpr
                    }
                let branchPlans = branches |> List.choose extractBranchPlan
                let branchOuterKeys = branches |> List.choose extractBranchOuterKey
                if branchPlans.Length = branches.Length && branchOuterKeys.Length = branches.Length then
                    let firstOuterKeyExpr =
                        let s, l = (tryMatchAny (ExpressionPatterns.unwrap branches.[0])).Value
                        let p = (tryExtractCorrelatedParts outerNames s l).Value
                        fst p.EquiPairs.[0]
                    let allSameOuterKey =
                        branches
                        |> List.forall (fun b ->
                            let s, l = (tryMatchAny (ExpressionPatterns.unwrap b)).Value
                            let p = (tryExtractCorrelatedParts outerNames s l).Value
                            ExpressionEqualityComparer.Instance.Equals(fst p.EquiPairs.[0], firstOuterKeyExpr))
                    if allSameOuterKey then
                        let unionPlan = branchPlans |> List.reduce (fun acc p -> LogicalPlan.Union(acc, p, true))
                        let joinHow = if isNegated then JoinKind.Anti else JoinKind.Semi
                        Some (LogicalPlan.Join(outerPlan, unionPlan, [ branchOuterKeys.[0] ], [ ScalarExpr.Column "__disj_any_key__" ], joinHow, None, JoinKeyCoalescing.Default, NullSemantics.nullKeysMatch ()))
                    else
                        None
                else
                    None
            | _ -> None

        tryAny () |> Option.orElseWith tryContains |> Option.orElseWith (fun () -> tryDisjunctiveAny coreExpr)

    // =========================================================================
    // 5. Correlated Scalar Aggregate Filter Decorrelation (Step 3)
    // =========================================================================

    /// Tries to decorrelate a correlated scalar aggregate expression (e.g. Min, Max, Average, Sum, Count)
    /// within a filter conjunct into a pre-aggregated GroupBy LogicalPlan, Left-Join, and pushdown comparison filter.
    let internal tryDecorrelateCorrelatedScalarAgg
        (resolveSequencePlan: Expression -> LogicalPlan option)
        (outerPlan: LogicalPlan)
        (outerParam: ParameterExpression)
        (conjunctExpr: Expression)
        (rowShape: RowShape)
        : LogicalPlan option =
        let outerName = outerParam.Name
        let outerNames =
            Set.ofSeq (outerName :: (Set.toList rowShape.ScopeNames))

        let outerCols = (SchemaInference.inferPlanSchema outerPlan).ColumnNames
        let outerCtx =
            if not rowShape.IsEmpty then
                TranslationContext.ScopedEntity([ outerName ], rowShape, outerCols)
            else
                createContextForType outerParam.Type outerPlan outerName

        let isNegated, coreConjunct =
            match ExpressionPatterns.unwrap conjunctExpr with
            | Unary(ExpressionType.Not, inner) -> true, ExpressionPatterns.unwrap inner
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
            option {
                let! baseOp = if isNegated then negateOp rawOp else Some rawOp

                let! outerSideExpr, aggCallExpr, scale, effectiveOp =
                    let containsOuter (m: Expression) = outerNames |> Set.exists (fun n -> containsParameter n m)
                    if not (containsSubquery left) && containsSubquery right then
                        match tryExtractScaledAgg right with
                        | Some(aggExpr, scale) ->
                            let scaleOk =
                                match scale with
                                | AggScale.Identity -> true
                                | AggScale.Multiply m
                                | AggScale.Divide m -> not (containsOuter m)

                            if scaleOk then Some(left, aggExpr, scale, baseOp) else None
                        | None -> None
                    elif not (containsSubquery right) && containsSubquery left then
                        match tryExtractScaledAgg left with
                        | Some(aggExpr, scale) ->
                            let scaleOk =
                                match scale with
                                | AggScale.Identity -> true
                                | AggScale.Multiply m
                                | AggScale.Divide m -> not (containsOuter m)

                            if scaleOk then
                                invertOp baseOp |> Option.map (fun invOp -> right, aggExpr, scale, invOp)
                            else
                                None
                        | None -> None
                    else
                        None

                let! aggCall = tryMatchAggCall aggCallExpr

                let! parts =
                    tryExtractCorrelatedParts outerNames aggCall.InnerSeq aggCall.ExtraWhereLambdaOpt

                let! rawRightPlan = resolveSequencePlan parts.RootSource
                let! rightPlan = tryApplyInnerFilters rawRightPlan parts.InnerParam parts.InnerFilters

                let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                let innerCtx =
                    createContextForTypeWithCols parts.InnerParam.Type rightCols parts.InnerParam.Name

                let isCount = aggCall.MethodName = "Count" || aggCall.MethodName = "LongCount"
                let uid = Guid.NewGuid().ToString "N"
                let aggColName = $"__subquery_agg_{uid}"

                let buildAggColBase (colName: string) : ScalarExpr =
                    if isCount then
                        ScalarExpr.Function(ScalarFunction.FillNull, [ ScalarExpr.Column colName; ScalarExpr.Literal(LiteralValue.UInt32 0u) ], LogicalTypes.UInt32)
                    elif aggCall.MethodName = "Sum" then
                        match PolarsTypeHelper.GetNumericZeroValue aggCall.TargetType with
                        | null -> ScalarExpr.Column colName
                        | zeroVal ->
                            let core = PolarsTypeHelper.UnwrapCoreType aggCall.TargetType
                            let zeroLit = toLiteral zeroVal core
                            ScalarExpr.Function(ScalarFunction.FillNull, [ ScalarExpr.Column colName; zeroLit ], LogicalType.Unknown)
                    else
                        ScalarExpr.Column colName

                if parts.ThetaConditions.IsEmpty then
                    let! keys = tryTranslateJoinKeys outerCtx innerCtx parts.EquiPairs

                    let! rawAggScalar =
                        tryBuildAggScalar rightCols aggCall.MethodName isCount aggCall.SelectorLambdaOpt

                    // Grouped by fresh key columns: a computed key (`b.I % 4`, named I) must not be applied again
                    // to the grouped column, nor collide with an outer column.
                    let keyedRight, keyCols = withFreshKeyColumns rightPlan keys.InnerKeys
                    let groupedPlan = LogicalPlan.GroupBy(keyedRight, keyCols, [ (aggColName, rawAggScalar) ], None, false)

                    let! outerSideScalar = ExprTranslator.tryTranslateToScalar outerCtx outerSideExpr

                    let aggColBaseScalar = buildAggColBase aggColName

                    let! scaledAggScalar =
                        match scale with
                        | AggScale.Identity -> Some aggColBaseScalar
                        | AggScale.Multiply multExpr ->
                            match PartialEvaluator.tryEvaluate multExpr with
                            | Some v when not (isNull v) ->
                                let lit = toLiteral v multExpr.Type
                                Some(ScalarExpr.Binary(BinaryOp.Multiply, lit, aggColBaseScalar))
                            | _ -> None
                        | AggScale.Divide divExpr ->
                            match PartialEvaluator.tryEvaluate divExpr with
                            | Some v when not (isNull v) ->
                                let lit = toLiteral v divExpr.Type
                                OperatorAndUdfTranslator.combineBinary
                                    ExpressionType.Divide
                                    (aggCall.TargetType, Some aggColBaseScalar)
                                    (divExpr.Type, Some lit)
                                    aggCall.TargetType
                            | _ -> None

                    // C# semantics as in row comparisons (null, NaN).
                    let! cmpScalar =
                        OperatorAndUdfTranslator.combineBinary
                            effectiveOp
                            (outerSideExpr.Type, Some outerSideScalar)
                            (aggCall.TargetType, Some scaledAggScalar)
                            typeof<bool>

                    let joinedPlan = LogicalPlan.Join(outerPlan, groupedPlan, keys.OuterKeys, keyCols, JoinKind.Left, None, JoinKeyCoalescing.Default, NullSemantics.nullKeysMatch ())
                    let filterPlan = LogicalPlan.Filter(joinedPlan, cmpScalar)
                    let originalCols = (SchemaInference.inferPlanSchema outerPlan).ColumnNames
                    let restorePlan =
                        if originalCols.Length > 0 then
                            LogicalPlan.Project(filterPlan, originalCols |> List.map (fun c -> c, ScalarExpr.Column c))
                        else
                            filterPlan

                    return restorePlan
                else
                    // Theta conditions present (composite conditions: equi-join + inequalities/cross conditions)
                    let leftCols = outerCols |> Set.ofList
                    let originalCols = outerCols
                    let rightSuffix = LinqConstants.DefaultRightSuffix

                    let correlatedCtx =
                        TranslationContext.CorrelatedJoin(
                            [ outerName ],
                            parts.InnerParam.Name,
                            leftCols,
                            rightSuffix,
                            rowShape
                        )

                    let combinedThetaExpr =
                        parts.ThetaConditions
                        |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

                    let! thetaFilterScalar =
                        ExprTranslator.tryTranslateToScalar correlatedCtx combinedThetaExpr

                    let outerKeys = parts.EquiPairs |> List.choose (fun (o, _) -> ExprTranslator.tryTranslateToScalar outerCtx o)
                    let innerKeys = parts.EquiPairs |> List.choose (fun (_, i) -> ExprTranslator.tryTranslateToScalar innerCtx i)

                    let! rawAggScalar =
                        if isCount then
                            Some(ScalarExpr.Aggregation(AggKind.Len, None, false))
                        else
                            let colResolver c =
                                if leftCols.Contains c then c + rightSuffix else c

                            tryBuildAggScalarWithContext
                                rightCols
                                aggCall.MethodName
                                isCount
                                aggCall.SelectorLambdaOpt
                                (Some correlatedCtx)
                                colResolver

                    let ridCol = $"__subquery_rid_{uid}"

                    let withRidPlan = LogicalPlan.WithRowIndex(outerPlan, ridCol, Some 0u)
                    let joinedPlan =
                        if outerKeys.IsEmpty then
                            LogicalPlan.Join(withRidPlan, rightPlan, [], [], JoinKind.Cross, Some rightSuffix, JoinKeyCoalescing.KeepBoth, false)
                        else
                            (let keyedRight, keyCols = withFreshKeyColumns rightPlan innerKeys in LogicalPlan.Join(withRidPlan, keyedRight, outerKeys, keyCols, JoinKind.Inner, Some rightSuffix, JoinKeyCoalescing.Default, NullSemantics.nullKeysMatch ()))

                    let filteredPlan = LogicalPlan.Filter(joinedPlan, thetaFilterScalar)
                    let groupedPlan =
                        LogicalPlan.GroupBy(
                            filteredPlan,
                            [ ScalarExpr.Column ridCol ],
                            [ (aggColName, rawAggScalar) ],
                            None,
                            false
                        )

                    let leftJoinPlan =
                        LogicalPlan.Join(
                            withRidPlan,
                            groupedPlan,
                            [ ScalarExpr.Column ridCol ],
                            [ ScalarExpr.Column ridCol ],
                            JoinKind.Left,
                            None,
                            JoinKeyCoalescing.Default,
                            false
                        )

                    let! outerSideScalar = ExprTranslator.tryTranslateToScalar outerCtx outerSideExpr

                    let aggColBaseScalar = buildAggColBase aggColName

                    let! scaledAggScalar =
                        match scale with
                        | AggScale.Identity -> Some aggColBaseScalar
                        | AggScale.Multiply multExpr ->
                            match PartialEvaluator.tryEvaluate multExpr with
                            | Some v when not (isNull v) ->
                                let lit = toLiteral v multExpr.Type
                                Some(ScalarExpr.Binary(BinaryOp.Multiply, lit, aggColBaseScalar))
                            | _ -> None
                        | AggScale.Divide divExpr ->
                            match PartialEvaluator.tryEvaluate divExpr with
                            | Some v when not (isNull v) ->
                                let lit = toLiteral v divExpr.Type
                                OperatorAndUdfTranslator.combineBinary
                                    ExpressionType.Divide
                                    (aggCall.TargetType, Some aggColBaseScalar)
                                    (divExpr.Type, Some lit)
                                    aggCall.TargetType
                            | _ -> None

                    // C# semantics as in row comparisons (null, NaN).
                    let! cmpScalar =
                        OperatorAndUdfTranslator.combineBinary
                            effectiveOp
                            (outerSideExpr.Type, Some outerSideScalar)
                            (aggCall.TargetType, Some scaledAggScalar)
                            typeof<bool>
                    let filteredPlan = LogicalPlan.Filter(leftJoinPlan, cmpScalar)
                    let restorePlan = LogicalPlan.Project(filteredPlan, originalCols |> List.map (fun c -> c, ScalarExpr.Column c))

                    return restorePlan
            }
        | _ -> None

    /// Attempts to decorrelate one or more subquery expressions within a filter lambda,
    /// returning the rewritten LogicalPlan and any residual outer filter lambda.
    /// `seq.All(p)` as `!seq.Any(x => !p(x))`, which decorrelates as an anti join or a negated match count.
    let private rewriteAllAsNotAny (body: Expression) : Expression =
        // `!(x != y)` is `x == y` and `!(x == y)` is `x != y` under C# equality (two-valued, NaN unequal);
        // `!!x` is `x`. An ordering comparison is not inverted (NaN makes both sides false).
        let negate (e: Expression) : Expression =
            match e with
            | :? UnaryExpression as u when u.NodeType = ExpressionType.Not && u.Type = typeof<bool> -> u.Operand
            | :? BinaryExpression as b when b.NodeType = ExpressionType.NotEqual && b.Type = typeof<bool> ->
                Expression.Equal(b.Left, b.Right, b.IsLiftedToNull, b.Method) :> Expression
            | :? BinaryExpression as b when b.NodeType = ExpressionType.Equal && b.Type = typeof<bool> ->
                Expression.NotEqual(b.Left, b.Right, b.IsLiftedToNull, b.Method) :> Expression
            | _ -> Expression.Not e :> Expression
        { new ExpressionVisitor() with
            override this.VisitUnary(node: UnaryExpression) =
                // Collapses the `!!` an `!All(..)` leaves.
                match base.VisitUnary node with
                | :? UnaryExpression as u when
                    u.NodeType = ExpressionType.Not
                    && u.Type = typeof<bool>
                    && u.Operand.NodeType = ExpressionType.Not
                    && u.Operand.Type = typeof<bool>
                    ->
                    (u.Operand :?> UnaryExpression).Operand
                | other -> other
            override this.VisitMethodCall(node: MethodCallExpression) =
                let visited = base.VisitMethodCall node
                match visited with
                | :? MethodCallExpression as mc when
                    mc.Method.Name = "All"
                    && isNull mc.Object
                    && mc.Arguments.Count = 2
                    && (mc.Method.DeclaringType = typeof<Enumerable> || mc.Method.DeclaringType = typeof<Queryable>)
                    ->
                    let source = mc.Arguments.[0]
                    let isQueryable = mc.Method.DeclaringType = typeof<Queryable>
                    match ExpressionPatterns.unwrap mc.Arguments.[1] with
                    | :? LambdaExpression as pred ->
                        let negated = Expression.Lambda(pred.Type, negate pred.Body, pred.Parameters)
                        let elementType = pred.Parameters.[0].Type
                        let predicateArg = if isQueryable then Expression.Quote negated :> Expression else negated :> Expression
                        let any =
                            Expression.Call((if isQueryable then typeof<Queryable> else typeof<Enumerable>), "Any", [| elementType |], source, predicateArg)
                        Expression.Not any :> Expression
                    | _ -> visited
                | other -> other
        }.Visit body

    let tryDecorrelateFilter
        (resolveSequencePlan: Expression -> LogicalPlan option)
        (outerPlan: LogicalPlan)
        (filterLambda: LambdaExpression)
        (rowShape: RowShape)
        : (LogicalPlan * LambdaExpression option) option =
        if filterLambda.Parameters.Count <> 1 then
            None
        else
            let filterLambda = Expression.Lambda(rewriteAllAsNotAny filterLambda.Body, filterLambda.Parameters)
            let outerParam = filterLambda.Parameters.[0]
            let outerNames =
                Set.ofSeq (outerParam.Name :: (Set.toList rowShape.ScopeNames))
            let conjuncts = flattenAnd filterLambda.Body

            let (finalPlan, residualConjunctsRev, wasDecorrelated) =
                conjuncts
                |> List.fold
                    (fun (currPlan, resAcc, decorrelatedAny) cond ->
                        match tryDecorrelateConjunct resolveSequencePlan currPlan outerParam cond rowShape with
                        | Some nextPlan -> nextPlan, resAcc, true
                        | None ->
                            match tryDecorrelateCorrelatedScalarAgg resolveSequencePlan currPlan outerParam cond rowShape with
                            | Some nextPlan -> nextPlan, resAcc, true
                            | None ->
                                // Uncorrelated scalar subqueries join the plan as one-row columns, and the
                                // conjunct filters right there (the columns do not leak to later stages).
                                let filteredWithScalars () =
                                    match tryRewriteUncorrelatedScalars resolveSequencePlan outerNames cond with
                                    | Some(rewritten, (_ :: _ as scalars)) ->
                                        let attached = attachUncorrelatedScalars currPlan scalars
                                        let attachedCols = (SchemaInference.inferPlanSchema attached).ColumnNames
                                        let ctx =
                                            if not rowShape.IsEmpty then
                                                TranslationContext.ScopedEntity([ outerParam.Name ], rowShape, attachedCols)
                                            else
                                                createContextForTypeWithCols outerParam.Type attachedCols outerParam.Name
                                        ExprTranslator.tryTranslateToScalar ctx rewritten
                                        |> Option.map (fun pred ->
                                            let originalCols = (SchemaInference.inferPlanSchema currPlan).ColumnNames
                                            LogicalPlan.Project(LogicalPlan.Filter(attached, pred), originalCols |> List.map (fun c -> c, ScalarExpr.Column c)))
                                    | _ -> None

                                match filteredWithScalars () with
                                | Some nextPlan -> nextPlan, resAcc, true
                                | None ->
                                    let foldedCond, changed = foldUncorrelatedSubqueries outerNames cond

                                    if changed then
                                        currPlan, foldedCond :: resAcc, true
                                    else
                                        currPlan, cond :: resAcc, decorrelatedAny)
                    (outerPlan, [], false)

            let residualConjuncts = List.rev residualConjunctsRev

            if not wasDecorrelated then
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

                Some(finalPlan, residualLambda)

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
        (outerNames: Set<string>)
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
            tryExtractCorrelatedParts outerNames innerSeq extraWhereLambdaOpt
            |> Option.map (fun parts ->
                // The selector reads the inner row by the predicates' parameter, as a correlated join translates it.
                let selectorLambdaOpt =
                    selectorLambdaOpt
                    |> Option.map (fun sel ->
                        let own = sel.Parameters.[0]
                        if obj.ReferenceEquals(own, parts.InnerParam) || own.Type <> parts.InnerParam.Type then
                            sel
                        else
                            let body =
                                { new ExpressionVisitor() with
                                    override _.VisitParameter(node: ParameterExpression) =
                                        if obj.ReferenceEquals(node, own) then parts.InnerParam :> Expression else node :> Expression
                                }.Visit sel.Body
                            Expression.Lambda(body, parts.InnerParam))
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
        (outerNames: Set<string>)
        (expr: Expression)
        : ProjectionSubqueryCandidate list =
        if
            isNull expr
            || not (containsSubquery expr)
            || not (outerNames |> Set.exists (fun name -> containsParameter name expr))
        then
            []
        else
            match tryExtractProjectionCandidate outerNames expr with
            | Some cand -> [ cand ]
            | None ->
                match expr with
                | :? NewExpression as ne ->
                    ne.Arguments
                    |> Seq.toList
                    |> List.collect (collectProjectionSubqueries outerNames)
                | :? MemberInitExpression as mie ->
                    let initArgs =
                        mie.NewExpression.Arguments
                        |> Seq.toList
                        |> List.collect (collectProjectionSubqueries outerNames)

                    let bindingArgs =
                        mie.Bindings
                        |> Seq.choose (function
                            | :? MemberAssignment as ma -> Some ma.Expression
                            | _ -> None)
                        |> Seq.toList
                        |> List.collect (collectProjectionSubqueries outerNames)

                    initArgs @ bindingArgs
                | MethodCall(_, target, args) ->
                    let targetSubs =
                        if isNull target then
                            []
                        else
                            collectProjectionSubqueries outerNames target

                    let argSubs = args |> List.collect (collectProjectionSubqueries outerNames)
                    targetSubs @ argSubs
                | Binary(_, left, right) ->
                    collectProjectionSubqueries outerNames left
                    @ collectProjectionSubqueries outerNames right
                | Unary(_, inner) -> collectProjectionSubqueries outerNames inner
                | :? ConditionalExpression as cond ->
                    collectProjectionSubqueries outerNames cond.Test
                    @ collectProjectionSubqueries outerNames cond.IfTrue
                    @ collectProjectionSubqueries outerNames cond.IfFalse
                | MemberAccess(target, _) ->
                    if isNull target then
                        []
                    else
                        collectProjectionSubqueries outerNames target
                | :? InvocationExpression as inv ->
                    let exprSubs = collectProjectionSubqueries outerNames inv.Expression

                    let argSubs =
                        inv.Arguments
                        |> Seq.toList
                        |> List.collect (collectProjectionSubqueries outerNames)

                    exprSubs @ argSubs
                | _ -> []

    let private canCoalesce (c1: ProjectionSubqueryCandidate) (c2: ProjectionSubqueryCandidate) : bool =
        let sameSource =
            obj.ReferenceEquals(c1.RootSource, c2.RootSource)
            || ExpressionEqualityComparer.Instance.Equals(c1.RootSource, c2.RootSource)

        let sameKeys =
            c1.EquiPairs.Length = c2.EquiPairs.Length
            && List.forall2
                (fun (o1: Expression, i1: Expression) (o2: Expression, i2: Expression) ->
                    ExpressionEqualityComparer.Instance.Equals(o1, o2)
                    && ExpressionEqualityComparer.Instance.Equals(i1, i2))
                c1.EquiPairs
                c2.EquiPairs

        let sameThetas =
            c1.ThetaConditions.Length = c2.ThetaConditions.Length
            && List.forall2
                (fun (t1: Expression) (t2: Expression) -> ExpressionEqualityComparer.Instance.Equals(t1, t2))
                c1.ThetaConditions
                c2.ThetaConditions

        let sameFilters =
            c1.InnerFilters.Length = c2.InnerFilters.Length
            && List.forall2
                (fun (f1: Expression) (f2: Expression) -> ExpressionEqualityComparer.Instance.Equals(f1, f2))
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
    /// and returns the rewritten LogicalPlan with the rewritten projection lambda.
    let tryDecorrelateProjection
        (resolveSequencePlan: Expression -> LogicalPlan option)
        (outerPlan: LogicalPlan)
        (projLambda: LambdaExpression)
        (rowShape: RowShape)
        : (LogicalPlan * LambdaExpression) option =
        if projLambda.Parameters.Count < 1 then
            None
        elif not (containsSubquery projLambda.Body) then
            None
        else
            let projLambda = Expression.Lambda(rewriteAllAsNotAny projLambda.Body, projLambda.Parameters)
            let outerParams = projLambda.Parameters |> Seq.toList
            let outerParamNames = outerParams |> List.map (fun p -> p.Name) |> Set.ofList
            let allOuterNames =
                Set.union outerParamNames rowShape.ScopeNames

            // Uncorrelated scalar subqueries become one-row plans joined to the outer plan; the projection then
            // reads their columns.
            let foldedBody, hasFolded, outerPlan =
                match tryRewriteUncorrelatedScalars resolveSequencePlan allOuterNames projLambda.Body with
                | Some(rewritten, scalars) -> rewritten, true, attachUncorrelatedScalars outerPlan scalars
                | None -> projLambda.Body, false, outerPlan

            let candidates = collectProjectionSubqueries allOuterNames foldedBody

            if candidates.IsEmpty then
                if hasFolded then
                    Some(outerPlan, Expression.Lambda(foldedBody, projLambda.Parameters))
                else
                    None
            else
                let groups = groupCandidates candidates
                let colNames = (SchemaInference.inferPlanSchema outerPlan).ColumnNames
                let outerCtx =
                    if not rowShape.IsEmpty then
                        TranslationContext.ScopedEntity(outerParamNames |> Set.toList, rowShape, colNames)
                    elif projLambda.Parameters.Count = 1 then
                        createContextForTypeWithCols projLambda.Parameters.[0].Type colNames projLambda.Parameters.[0].Name
                    else
                        TranslationContext.ScopedEntity(outerParamNames |> Set.toList, RowShape.Empty, colNames)

                let compileGroup (grpIdx: int) (grp: ProjectionSubqueryCandidate list) (currPlan: LogicalPlan) =
                    let head = grp.Head

                    let sameAggOp (c1: ProjectionSubqueryCandidate) (c2: ProjectionSubqueryCandidate) =
                        c1.MethodName = c2.MethodName
                        && c1.IsAny = c2.IsAny
                        && match c1.SelectorLambdaOpt, c2.SelectorLambdaOpt with
                           | None, None -> true
                           | Some s1, Some s2 -> ExpressionEqualityComparer.Instance.Equals(s1, s2)
                           | _ -> false

                    let rec assignAggCols buildAgg remaining assigned =
                        match remaining with
                        | [] -> Some(List.rev assigned)
                        | (candIdx, cand) :: tail ->
                            match assigned |> List.tryFind (fun (prevCand, _, _) -> sameAggOp cand prevCand) with
                            | Some(_, prevColName, _) ->
                                assignAggCols buildAgg tail ((cand, prevColName, None) :: assigned)
                            | None ->
                                let colName = $"__subquery_agg_{grpIdx}_{candIdx}"

                                match buildAgg cand with
                                | None -> None
                                | Some rawAggScalar ->
                                    assignAggCols buildAgg tail ((cand, colName, Some(colName, rawAggScalar)) :: assigned)

                    let buildFillNullCols
                        (assignments: (ProjectionSubqueryCandidate * string * (string * ScalarExpr) option) list)
                        : (string * ScalarExpr) list =
                        let handledCols = HashSet<string>()

                        assignments
                        |> List.choose (fun (cand, colName, _) ->
                            let defaultValOpt =
                                if cand.IsAny || cand.MethodName = "Count" || cand.MethodName = "LongCount" then
                                    Some(ScalarExpr.Literal(LiteralValue.UInt32 0u))
                                elif cand.MethodName = "Sum" then
                                    match PolarsTypeHelper.GetNumericZeroValue cand.TargetType with
                                    | null -> None
                                    | zeroVal ->
                                        let core = PolarsTypeHelper.UnwrapCoreType cand.TargetType
                                        Some(toLiteral zeroVal core)
                                else
                                    None

                            match defaultValOpt with
                            | Some defVal when handledCols.Add colName ->
                                let filled =
                                    ScalarExpr.Function(ScalarFunction.FillNull, [ ScalarExpr.Column colName; defVal ], LogicalType.Unknown)
                                Some(colName, filled)
                            | _ -> None)

                    let buildReplacements
                        (assignments: (ProjectionSubqueryCandidate * string * (string * ScalarExpr) option) list)
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

                    option {
                        let! rawRightPlan = resolveSequencePlan head.RootSource
                        let! rightPlan = tryApplyInnerFilters rawRightPlan head.InnerParam head.InnerFilters
                        let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames

                        let innerCtx =
                            createContextForTypeWithCols head.InnerParam.Type rightCols head.InnerParam.Name

                        if head.ThetaConditions.IsEmpty then
                            let! keys = tryTranslateJoinKeys outerCtx innerCtx head.EquiPairs

                            let buildAgg (cand: ProjectionSubqueryCandidate) =
                                tryBuildAggScalar rightCols cand.MethodName cand.IsAny cand.SelectorLambdaOpt

                            let! assignments = assignAggCols buildAgg indexedCandidates []

                            let aggExprsToEmit =
                                assignments |> List.choose (fun (_, _, exprOpt) -> exprOpt)

                            // Grouped by fresh key columns (see the filter case).
                            let keyedRight, keyCols = withFreshKeyColumns rightPlan keys.InnerKeys
                            let groupedPlan =
                                LogicalPlan.GroupBy(keyedRight, keyCols, aggExprsToEmit, None, false)

                            let joinPlan =
                                LogicalPlan.Join(
                                    currPlan,
                                    groupedPlan,
                                    keys.OuterKeys,
                                    keyCols,
                                    JoinKind.Left,
                                    None,
                                    JoinKeyCoalescing.Default,
                                    NullSemantics.nullKeysMatch ()
                                )

                            let fillNullCols = buildFillNullCols assignments
                            let withColsPlan =
                                if fillNullCols.IsEmpty then joinPlan
                                else LogicalPlan.WithColumns(joinPlan, fillNullCols)

                            let replacements = buildReplacements assignments
                            return withColsPlan, replacements
                        else
                            let leftCols = (SchemaInference.inferPlanSchema currPlan).ColumnNames |> Set.ofList
                            let rightSuffix = LinqConstants.DefaultRightSuffix

                            let correlatedCtx =
                                TranslationContext.CorrelatedJoin(
                                    outerParamNames |> Set.toList,
                                    head.InnerParam.Name,
                                    leftCols,
                                    rightSuffix,
                                    rowShape
                                )

                            let combinedThetaExpr =
                                head.ThetaConditions
                                |> List.reduce (fun a b -> Expression.AndAlso(a, b) :> Expression)

                            let! thetaFilterScalar =
                                ExprTranslator.tryTranslateToScalar correlatedCtx combinedThetaExpr

                            let! outerKeys, innerKeys =
                                if head.EquiPairs.IsEmpty then
                                    Some([], [])
                                else
                                    tryTranslateJoinKeys outerCtx innerCtx head.EquiPairs
                                    |> Option.map (fun k -> k.OuterKeys, k.InnerKeys)

                            let uid = Guid.NewGuid().ToString "N"
                            let ridCol = $"__subquery_rid_{grpIdx}_{uid}"

                            let withRidPlan = LogicalPlan.WithRowIndex(currPlan, ridCol, Some 0u)

                            let joinedPlan =
                                if outerKeys.IsEmpty then
                                    LogicalPlan.Join(
                                        withRidPlan,
                                        rightPlan,
                                        [],
                                        [],
                                        JoinKind.Cross,
                                        Some rightSuffix,
                                        JoinKeyCoalescing.KeepBoth,
                                        false
                                    )
                                else
                                    let keyedRight, keyCols = withFreshKeyColumns rightPlan innerKeys
                                    LogicalPlan.Join(
                                        withRidPlan,
                                        keyedRight,
                                        outerKeys,
                                        keyCols,
                                        JoinKind.Inner,
                                        Some rightSuffix,
                                        JoinKeyCoalescing.Default,
                                        NullSemantics.nullKeysMatch ()
                                    )

                            let filteredPlan = LogicalPlan.Filter(joinedPlan, thetaFilterScalar)

                            let buildAgg (cand: ProjectionSubqueryCandidate) =
                                let colResolver c =
                                    if leftCols.Contains c then c + rightSuffix else c

                                tryBuildAggScalarWithContext
                                    rightCols
                                    cand.MethodName
                                    cand.IsAny
                                    cand.SelectorLambdaOpt
                                    (Some correlatedCtx)
                                    colResolver

                            let! assignments = assignAggCols buildAgg indexedCandidates []

                            let aggExprsToEmit =
                                assignments |> List.choose (fun (_, _, exprOpt) -> exprOpt)

                            let groupedMatchesPlan =
                                LogicalPlan.GroupBy(
                                    filteredPlan,
                                    [ ScalarExpr.Column ridCol ],
                                    aggExprsToEmit,
                                    None,
                                    false
                                )

                            let joinPlan =
                                LogicalPlan.Join(
                                    withRidPlan,
                                    groupedMatchesPlan,
                                    [ ScalarExpr.Column ridCol ],
                                    [ ScalarExpr.Column ridCol ],
                                    JoinKind.Left,
                                    None,
                                    JoinKeyCoalescing.Default,
                                    false
                                )

                            let fillNullCols = buildFillNullCols assignments
                            let withColsPlan =
                                if fillNullCols.IsEmpty then joinPlan
                                else LogicalPlan.WithColumns(joinPlan, fillNullCols)

                            let replacements = buildReplacements assignments
                            return withColsPlan, replacements
                    }

                let rec chainGroups (plan: LogicalPlan) (replsAcc: (Expression * Expression) list) (remaining: (int * ProjectionSubqueryCandidate list) list) =
                    match remaining with
                    | [] -> Some(plan, replsAcc)
                    | (idx, grp) :: tail ->
                        match compileGroup idx grp plan with
                        | Some(nextPlan, repls) ->
                            chainGroups nextPlan (replsAcc @ repls) tail
                        | None -> None

                match chainGroups outerPlan [] (groups |> List.mapi (fun i g -> i, g)) with
                | None -> None
                | Some(finalPlan, allReplacements) ->
                    let replacer = SubqueryExpressionReplacer allReplacements
                    let rewrittenBody = replacer.Visit foldedBody

                    let rewrittenLambda = Expression.Lambda(rewrittenBody, projLambda.Parameters)

                    Some(finalPlan, rewrittenLambda)
