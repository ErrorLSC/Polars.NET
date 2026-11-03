namespace Polars.NET.Linq

open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

module AggTranslator =
    open ExprTranslator

    // =========================================================================
    // 1. Data Type & Casting Helpers
    // =========================================================================

    let private castCountExpr (isLong: bool) (expr: ExprHandle) =
        let t = if isLong then typeof<int64> else typeof<int>
        tryTranslateCast t expr |> Option.defaultValue expr

    // =========================================================================
    // 2. Group Context Resolution
    // =========================================================================

    /// Resolves the appropriate TranslationContext for inner group lambdas based on parameter type
    let private getParamContext
        (scalarColOpt: string option)
        (scopeMap: Map<string * string, string>)
        (p: ParameterExpression)
        : TranslationContext =
        if PolarsTypeHelper.IsScalarType p.Type then
            match scalarColOpt with
            | Some col when not (String.IsNullOrEmpty col) -> TranslationContext.Scalar(p.Name, col)
            | _ -> TranslationContext.Entity p.Name
        elif not scopeMap.IsEmpty then
            TranslationContext.ScopedEntity([ p.Name ], scopeMap, [])
        else
            TranslationContext.Entity p.Name

    /// Translates a single-parameter lambda expression over the group element.
    let private resolveLambda
        (scalarColOpt: string option)
        (scopeMap: Map<string * string, string>)
        (lambda: LambdaExpression)
        : ExprHandle option =
        if lambda.Parameters.Count <> 1 then
            None
        else
            let p = lambda.Parameters.[0]
            let ctx = getParamContext scalarColOpt scopeMap p

            match tryResolveColumnNameWithContext ctx lambda.Body with
            | Some cName -> Some(PolarsWrapper.Col cName)
            | None -> tryTranslateWithContext ctx lambda.Body

    // =========================================================================
    // 3. Composable Sequence Pipeline over Group Source
    // =========================================================================

    type private GroupSeq =
        | Root
        | Select of source: GroupSeq * selector: LambdaExpression
        | Where of source: GroupSeq * predicate: LambdaExpression
        | Distinct of source: GroupSeq
        | DistinctBy of source: GroupSeq * keySelector: LambdaExpression
        | OrderBy of source: GroupSeq * keySelector: LambdaExpression * isDescending: bool
        | ThenBy of source: GroupSeq * keySelector: LambdaExpression * isDescending: bool
        | OrderBySelf of source: GroupSeq * isDescending: bool
        | Take of source: GroupSeq * countExpr: Expression
        | Skip of source: GroupSeq * countExpr: Expression
        | ToList of source: GroupSeq
        | ToArray of source: GroupSeq

    /// Active pattern extracting intermediate transformation chains on the group source `g`.
    let rec private (|GroupSeqChain|_|) (groupParamName: string) (expr: Expression) : GroupSeq option =
        match unwrap expr with
        | null -> None
        | :? ParameterExpression as p when p.Name = groupParamName -> Some Root
        | MethodCall(m, null, [ GroupSeqChain groupParamName s; CleanLambda selector ]) when m.Name = "Select" ->
            Some(Select(s, selector))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s; CleanLambda predicate ]) when m.Name = "Where" ->
            Some(Where(s, predicate))
        | MethodCall(m, null, GroupSeqChain groupParamName s :: CleanLambda keySelector :: _) when m.Name = "OrderBy" ->
            Some(OrderBy(s, keySelector, false))
        | MethodCall(m, null, GroupSeqChain groupParamName s :: CleanLambda keySelector :: _) when m.Name = "OrderByDescending" ->
            Some(OrderBy(s, keySelector, true))
        | MethodCall(m, null, GroupSeqChain groupParamName s :: CleanLambda keySelector :: _) when m.Name = "ThenBy" ->
            Some(ThenBy(s, keySelector, false))
        | MethodCall(m, null, GroupSeqChain groupParamName s :: CleanLambda keySelector :: _) when m.Name = "ThenByDescending" ->
            Some(ThenBy(s, keySelector, true))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s ]) when m.Name = "Order" ->
            Some(OrderBySelf(s, false))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s ]) when m.Name = "OrderDescending" ->
            Some(OrderBySelf(s, true))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s; countExpr ]) when m.Name = "Take" ->
            Some(Take(s, countExpr))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s; countExpr ]) when m.Name = "Skip" ->
            Some(Skip(s, countExpr))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s ]) when m.Name = "Distinct" ->
            Some(Distinct s)
        | MethodCall(m, null, [ GroupSeqChain groupParamName s; CleanLambda keySelector ]) when m.Name = "DistinctBy" ->
            Some(DistinctBy(s, keySelector))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s ]) when m.Name = "ToList" ->
            Some(ToList s)
        | MethodCall(m, null, [ GroupSeqChain groupParamName s ]) when m.Name = "ToArray" ->
            Some(ToArray s)
        | _ -> None

    type private SortKey =
        {
            Expr: ExprHandle
            IsDescending: bool
        }

    type private ResolvedGroupSeq =
        {
            Target: ExprHandle option
            Filter: ExprHandle option
            IsDistinct: bool
            SortKeys: SortKey list
            Slice: (int64 option * int64 option) option
        }

    let private getCurrentScalar (scalarColOpt: string option) (targetOpt: ExprHandle option) =
        match targetOpt with
        | Some h ->
            let name = PolarsWrapper.ExprGetOutputName h
            if String.IsNullOrEmpty name then scalarColOpt else Some name
        | None -> scalarColOpt

    let private resolveKeyExpr
        (scalarColOpt: string option)
        (scopeMap: Map<string * string, string>)
        (prev: ResolvedGroupSeq)
        (keySelector: LambdaExpression)
        : ExprHandle option =
        let cleanBody = unwrap keySelector.Body
        if cleanBody = (keySelector.Parameters.[0] :> Expression) then
            prev.Target |> Option.map PolarsWrapper.CloneExpr
        else
            let currentScalar = getCurrentScalar scalarColOpt prev.Target
            resolveLambda currentScalar scopeMap keySelector

    let rec private evalGroupSeq
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (scopeMap: Map<string * string, string>)
        (seq: GroupSeq)
        : ResolvedGroupSeq option =
        match seq with
        | Root ->
            let targetOpt =
                match scalarColOpt with
                | Some col when not (String.IsNullOrEmpty col) -> Some(PolarsWrapper.Col col)
                | _ -> wholeRowStructOpt |> Option.map PolarsWrapper.CloneExpr

            Some
                {
                    Target = targetOpt
                    Filter = None
                    IsDistinct = false
                    SortKeys = []
                    Slice = None
                }

        | Select(source, selector) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                let currentScalar = getCurrentScalar scalarColOpt prev.Target
                resolveLambda currentScalar scopeMap selector
                |> Option.map (fun selExpr -> { prev with Target = Some selExpr }))

        | Where(source, predicate) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                let currentScalar = getCurrentScalar scalarColOpt prev.Target
                let p = predicate.Parameters.[0]
                let ctx = getParamContext currentScalar scopeMap p

                tryTranslateWithContext ctx predicate.Body
                |> Option.map (fun predExpr ->
                    let combinedFilter =
                        match prev.Filter with
                        | None -> predExpr
                        | Some f -> ExprTranslator.translateBinary ExpressionType.AndAlso f predExpr

                    { prev with Filter = Some combinedFilter }))

        | Distinct source ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.map (fun prev -> { prev with IsDistinct = true })

        | DistinctBy(source, keySelector) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                let currentScalar = getCurrentScalar scalarColOpt prev.Target
                resolveLambda currentScalar scopeMap keySelector
                |> Option.map (fun keyExpr ->
                    { prev with
                        Target = Some keyExpr
                        IsDistinct = true
                    }))

        | OrderBy(source, keySelector, isDesc) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                resolveKeyExpr scalarColOpt scopeMap prev keySelector
                |> Option.map (fun keyExpr ->
                    { prev with SortKeys = [ { Expr = keyExpr; IsDescending = isDesc } ] }))

        | ThenBy(source, keySelector, isDesc) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                resolveKeyExpr scalarColOpt scopeMap prev keySelector
                |> Option.map (fun keyExpr ->
                    { prev with SortKeys = prev.SortKeys @ [ { Expr = keyExpr; IsDescending = isDesc } ] }))

        | OrderBySelf(source, isDesc) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                prev.Target
                |> Option.map (fun t ->
                    { prev with SortKeys = [ { Expr = PolarsWrapper.CloneExpr t; IsDescending = isDesc } ] }))

        | Take(source, countExpr) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                tryEvaluate countExpr
                |> Option.map (fun c ->
                    let count = Convert.ToInt64 c
                    let newSlice =
                        match prev.Slice with
                        | Some(skipOpt, _) -> Some(skipOpt, Some count)
                        | None -> Some(None, Some count)
                    { prev with Slice = newSlice }))

        | Skip(source, countExpr) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source
            |> Option.bind (fun prev ->
                tryEvaluate countExpr
                |> Option.map (fun c ->
                    let count = Convert.ToInt64 c
                    let newSlice =
                        match prev.Slice with
                        | Some(_, takeOpt) -> Some(Some count, takeOpt)
                        | None -> Some(Some count, None)
                    { prev with Slice = newSlice }))

        | ToList source
        | ToArray source ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap source

    /// Combines the resolved target column expression with any sequence filter predicates, sorting, and slicing.
    let private getFilteredTarget (resolved: ResolvedGroupSeq) : ExprHandle option =
        match resolved.Target with
        | None -> None
        | Some initialTarget ->
            // 1. Filter
            let targetAfterFilter, sortKeysAfterFilter =
                match resolved.Filter with
                | Some f ->
                    let filteredTarget = PolarsWrapper.Filter(initialTarget, PolarsWrapper.CloneExpr f)
                    let filteredSortKeys =
                        resolved.SortKeys
                        |> List.map (fun sk ->
                            { sk with Expr = PolarsWrapper.Filter(sk.Expr, PolarsWrapper.CloneExpr f) })
                    filteredTarget, filteredSortKeys
                | None ->
                    initialTarget, resolved.SortKeys

            // 2. SortBy
            let targetAfterSort =
                if sortKeysAfterFilter.IsEmpty then
                    targetAfterFilter
                else
                    let byExprs = sortKeysAfterFilter |> List.map (fun sk -> sk.Expr) |> List.toArray
                    let desc = sortKeysAfterFilter |> List.map (fun sk -> sk.IsDescending) |> List.toArray
                    let nullsLast = Array.create byExprs.Length false
                    PolarsWrapper.SortBy(targetAfterFilter, byExprs, desc, nullsLast, multithreaded = true, maintainOrder = true)

            // 3. Slice (Skip / Take)
            let targetAfterSlice =
                match resolved.Slice with
                | Some(Some skip, Some take) ->
                    PolarsWrapper.ExprSlice(targetAfterSort, PolarsWrapper.Lit skip, PolarsWrapper.Lit take)
                | Some(None, Some take) ->
                    PolarsWrapper.Head(targetAfterSort, int take)
                | Some(Some skip, None) ->
                    PolarsWrapper.ExprSlice(targetAfterSort, PolarsWrapper.Lit skip, PolarsWrapper.Lit Int64.MaxValue)
                | _ ->
                    targetAfterSort

            // 4. Distinct
            let finalTarget =
                if resolved.IsDistinct then
                    PolarsWrapper.ExprUnique targetAfterSlice
                else
                    targetAfterSlice

            Some finalTarget

    let private tryExtractSep (sepExpr: Expression) : string option =
        match tryEvaluate sepExpr with
        | Some(:? string as sep) -> Some sep
        | Some(:? char as sep) -> Some(string sep)
        | _ -> None

    let private translateQuantileCall (targetH: ExprHandle) (qExpr: Expression) (methodExprOpt: Expression option) =
        match tryEvaluate qExpr with
        | Some qVal ->
            try
                let q = Convert.ToDouble qVal
                let method =
                    methodExprOpt
                    |> Option.bind tryEvaluate
                    |> Option.map tryExtractQuantileMethod
                    |> Option.defaultValue PlQuantileMethod.Nearest

                Some(PolarsWrapper.Quantile(targetH, q, method))
            with _ ->
                None
        | _ -> None

    // =========================================================================
    // 4. Core Recursive Aggregation Translator
    // =========================================================================

    let rec private unwrapQuoteOnly (e: Expression) =
        match e with
        | null -> null
        | Unary(ExpressionType.Quote, inner) -> unwrapQuoteOnly inner
        | other -> other

    let private isIntermediateSequenceMethod (m: MethodInfo) =
        if isNull m then false
        elif OperatorAndUdfTranslator.isInternalOrLinqMethod m then true
        elif m.DeclaringType = typeof<System.Linq.Enumerable> then
            match m.Name with
            | "Select" | "Where" | "OrderBy" | "OrderByDescending"
            | "ThenBy" | "ThenByDescending" | "Order" | "OrderDescending"
            | "Take" | "Skip" | "Distinct" | "DistinctBy"
            | "GroupBy" | "GroupJoin" | "Join" -> true
            | _ -> false
        else false

    let private isNativeReductionMethod (name: string) =
        match name with
        | "Sum" | "Mean" | "Average" | "Min" | "Max" | "Median"
        | "First" | "FirstOrDefault" | "Last" | "LastOrDefault"
        | "Std" | "StdDev" | "Var" | "Variance" -> true
        | _ -> false

    let private isScalarLikeResultType (t: Type) =
        PolarsTypeHelper.IsScalarType t
        || (t.IsValueType && not (typeof<System.Collections.IEnumerable>.IsAssignableFrom t))

    let rec private tryTranslateAggCore
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (scopeMap: Map<string * string, string>)
        (expr: Expression)
        : ExprHandle option =

        let cleanExpr = unwrapQuoteOnly expr

        match cleanExpr with
        // ---------------------------------------------------------------------
        // string.Join(sep, seq)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ sepExpr; GroupSeqChain groupParamName seq ]) when
            m.DeclaringType = typeof<string> && m.Name = "Join"
            ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq
            |> Option.bind getFilteredTarget
            |> Option.bind (fun colH ->
                tryExtractSep sepExpr
                |> Option.map (fun sep ->
                    let listExpr = PolarsWrapper.Implode(colH, maintainOrder = true)
                    PolarsWrapper.ListJoin(listExpr, sep, ignoreNulls = true)))

        // ---------------------------------------------------------------------
        // Count / LongCount over Sequence Chain (e.g. g.Count(), g.Distinct().Count(), g.Count(predicate))
        // ---------------------------------------------------------------------
        | MethodCall(m, null, args) when m.Name = "Count" || m.Name = "LongCount" ->
            let seqOpt =
                match args with
                | [ GroupSeqChain groupParamName seq ] -> Some seq
                | [ GroupSeqChain groupParamName seq; CleanLambda predicate ] -> Some(Where(seq, predicate))
                | _ -> None

            match seqOpt with
            | Some seq ->
                let isLong = m.Name = "LongCount"
                match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq with
                | Some resolved when resolved.IsDistinct ->
                    getFilteredTarget resolved
                    |> Option.map (fun t -> PolarsWrapper.NUnique t |> castCountExpr isLong)
                | Some resolved when resolved.Slice.IsSome ->
                    getFilteredTarget resolved
                    |> Option.map (fun t -> PolarsWrapper.Count t |> castCountExpr isLong)
                | Some resolved when resolved.Filter.IsSome ->
                    let castH = castCountExpr true resolved.Filter.Value
                    Some(PolarsWrapper.Sum castH)
                | Some _ ->
                    Some(PolarsWrapper.Len())
                | None -> None
            | None -> None

        // ---------------------------------------------------------------------
        // MinBy / MaxBy: seq.MinBy(x => x.ByCol).TargetCol
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda(Lambda([ p ], byBody)) ]),
                       memberInfo) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let ctx = getParamContext scalarColOpt scopeMap p

            tryTranslateWithContext ctx byBody
            |> Option.bind (fun byExpr ->
                let targetExpr = PolarsWrapper.Col memberInfo.Name
                match m.Name with
                | "MinBy" -> Some(PolarsWrapper.MinBy(targetExpr, byExpr))
                | "MaxBy" -> Some(PolarsWrapper.MaxBy(targetExpr, byExpr))
                | _ -> None)

        // ---------------------------------------------------------------------
        // Quantile
        // ---------------------------------------------------------------------
        | MethodCall(m, null, args) when m.Name = "Quantile" ->
            let quantileParsed =
                match args with
                | [ GroupSeqChain groupParamName seq; CleanLambda selector; qExpr ] ->
                    Some(Select(seq, selector), qExpr, None)
                | [ GroupSeqChain groupParamName seq; CleanLambda selector; qExpr; methodExpr ] ->
                    Some(Select(seq, selector), qExpr, Some methodExpr)
                | [ GroupSeqChain groupParamName seq; qExpr; methodExpr ] ->
                    Some(seq, qExpr, Some methodExpr)
                | [ GroupSeqChain groupParamName seq; qExpr ] when (match qExpr with CleanLambda _ -> false | _ -> true) ->
                    Some(seq, qExpr, None)
                | _ -> None

            match quantileParsed with
            | Some(seq, qExpr, methodExprOpt) ->
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq
                |> Option.bind getFilteredTarget
                |> Option.bind (fun colH -> translateQuantileCall colH qExpr methodExprOpt)
            | None -> None

        // ---------------------------------------------------------------------
        // Statistical Dispersion with explicit ddof: seq.Std(selector, ddof) / seq.Var(ddof)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda selector; ddof ]) when
            m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance" ->
            match tryEvaluate ddof with
            | Some ddofVal ->
                let ddofByte = Convert.ToByte ddofVal
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap (Select(seq, selector))
                |> Option.bind getFilteredTarget
                |> Option.bind (translateDispersionOp m.Name ddofByte)
            | None -> None

        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; ddof ]) when
            (m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance")
            && not (match ddof with CleanLambda _ -> true | _ -> false) ->
            match tryEvaluate ddof with
            | Some ddofVal ->
                let ddofByte = Convert.ToByte ddofVal
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq
                |> Option.bind getFilteredTarget
                |> Option.bind (translateDispersionOp m.Name ddofByte)
            | None -> None

        // ---------------------------------------------------------------------
        // Nested List Aggregation: ToList() / ToArray()
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq ]) when m.Name = "ToList" || m.Name = "ToArray" ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq with
            | Some resolved ->
                match getFilteredTarget resolved with
                | Some colH -> Some(PolarsWrapper.Implode(colH, maintainOrder = true))
                | None ->
                    wholeRowStructOpt
                    |> Option.map (fun rowStruct ->
                        let cloned = PolarsWrapper.CloneExpr rowStruct
                        PolarsWrapper.Implode(cloned, maintainOrder = true))
            | None -> None

        // ---------------------------------------------------------------------
        // Standard Reductions (with or without selector): e.g. g.Sum(x => x.Field), g.Sum()
        // ---------------------------------------------------------------------
        | MethodCall(m, null, args) when isNativeReductionMethod m.Name ->
            let seqOpt =
                match args with
                | [ GroupSeqChain groupParamName seq; CleanLambda selector ] -> Some(Select(seq, selector))
                | [ GroupSeqChain groupParamName seq ] -> Some seq
                | _ -> None

            match seqOpt with
            | Some seq ->
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq
                |> Option.bind getFilteredTarget
                |> Option.bind (translateReductionOp m.Name)
            | None -> None

        // ---------------------------------------------------------------------
        // Member Access on First / Last: seq.First().Prop / seq.Last().Prop
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ GroupSeqChain groupParamName seq ]), memberInfo) when
            m.Name = "First" || m.Name = "FirstOrDefault" || m.Name = "Last" || m.Name = "LastOrDefault"
            ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq with
            | Some resolved ->
                let memberCol = PolarsWrapper.Col memberInfo.Name
                let resolvedWithTarget = { resolved with Target = Some memberCol }
                getFilteredTarget resolvedWithTarget
                |> Option.bind (translateReductionOp m.Name)
            | None -> None

        // ---------------------------------------------------------------------
        // ElementAt / ElementAtOrDefault: seq.ElementAt(index)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; indexExpr ]) when
            m.Name = "ElementAt" || m.Name = "ElementAtOrDefault"
            ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq, tryEvaluate indexExpr with
            | Some resolved, Some idxVal ->
                let idx = Convert.ToInt64 idxVal
                getFilteredTarget resolved
                |> Option.map (fun colH -> PolarsWrapper.Get(colH, PolarsWrapper.Lit idx, nullOnOob = true))
            | _ -> None

        // ---------------------------------------------------------------------
        // Composite Expressions: Binary, Unary, Constant, Conditional, MethodCall
        // ---------------------------------------------------------------------
        | MethodCall(m, target, args) when isNumericDeclaringType m.DeclaringType && isNull target ->
            let translateScalar expr =
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap expr
                |> Option.map (fun h -> ScalarExpr.Opaque(h, [], ""))
            translateNumericMethod
                translateScalar
                m
                args
            |> Option.map PolarsScalarLowerer.lower

        | :? ConditionalExpression as cond ->
            match
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap cond.Test,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap cond.IfTrue,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap cond.IfFalse
            with
            | Some testH, Some trueH, Some falseH ->
                Some(PolarsWrapper.IfElse(testH, trueH, falseH))
            | _ -> None

        | Binary(op, left, right) ->
            match
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap left,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap right
            with
            | Some leftH, Some rightH -> Some(ExprTranslator.translateBinary op leftH rightH)
            | _ -> None

        | Unary(op, operand) ->
            tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap operand
            |> Option.bind (fun innerH ->
                match op with
                | ExpressionType.Convert
                | ExpressionType.ConvertChecked ->
                    tryTranslateCast cleanExpr.Type innerH
                | _ ->
                    translateUnary op innerH)

        | Constant(v, t) ->
            Some(toLiteralHandle v t)

        | _ ->
            tryTranslateGroupUdf groupParamName scalarColOpt wholeRowStructOpt scopeMap cleanExpr

    and private tryTranslateGroupUdf
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (scopeMap: Map<string * string, string>)
        (expr: Expression)
        : ExprHandle option =
        try
            if CoreConfig.DisallowUdfPushdown || isNull expr || expr.Type = typeof<System.Void> then
                None
            else
                let methodInfoOpt, rawArgs =
                    match expr with
                    | :? MethodCallExpression as mc ->
                        if isIntermediateSequenceMethod mc.Method then
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

                if rawArgs.IsEmpty || not (rawArgs |> List.exists (OperatorAndUdfTranslator.exprDependsOnParam groupParamName)) then
                    None
                else
                    let methodName =
                        match methodInfoOpt with
                        | Some m -> m.Name
                        | None -> "Invoke"

                    // Collect all argument expressions that depend on the group parameter
                    let dependentArgs =
                        rawArgs
                        |> List.filter (OperatorAndUdfTranslator.exprDependsOnParam groupParamName)

                    // Every dependent argument must be a sequence or rows from the group (GroupSeqChain)
                    let resolveArgHandle (arg: Expression) : ExprHandle option =
                        match (|GroupSeqChain|_|) groupParamName arg with
                        | Some seq ->
                            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt scopeMap seq
                            |> Option.bind getFilteredTarget
                        | None -> None

                    let translatedDependentArgs =
                        dependentArgs
                        |> List.map (fun a ->
                            match resolveArgHandle a with
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
                            let returnsScalar = isScalarLikeResultType expr.Type

                            ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                            UdfTracker.Record(sprintf "%s: %s" methodName (expr.ToString()))

                            if dynCount = 1 then
                                let argExpr, handle = dynArgs.[0]
                                let userFunc = OperatorAndUdfTranslator.compileSingleArgUdfDelegate argExpr expr
                                let arrowCallback = ArrowUdfBatchAdapter.CreateGroupAggregationSingle(userFunc, argExpr.Type, expr.Type)
                                Some(PolarsWrapper.Apply(handle, arrowCallback, outDataType, returnsScalar = returnsScalar))

                            elif dynCount > 1 then
                                let userFunc = OperatorAndUdfTranslator.compileMultiArgUdfDelegate dynArgs expr
                                let inputTypes = dynArgs |> List.map (fun (a, _) -> a.Type) |> List.toArray
                                let arrowCallback = ArrowUdfBatchAdapter.CreateGroupAggregationMulti(userFunc, inputTypes, expr.Type)
                                let mainHandle = snd dynArgs.[0]
                                let otherHandles = dynArgs |> List.tail |> List.map snd |> List.toArray
                                Some(PolarsWrapper.ApplyMany(mainHandle, otherHandles, arrowCallback, outDataType, returnsScalar = returnsScalar))
                            else
                                None
        with _ ->
            None

    // =========================================================================
    // 5. Public API: Group Aggregation Translation with Alias
    // =========================================================================

    /// Attempts to translate group collection operations into native Polars ExprHandle with optional alias.
    let tryTranslateAggWithScalarColAndScope
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (scopeMap: Map<string * string, string>)
        (expr: Expression)
        (aliasName: string)
        : ExprHandle option =

        tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt scopeMap expr
        |> Option.map (fun h ->
            if String.IsNullOrEmpty aliasName then h
            else PolarsWrapper.Alias(h, aliasName))
