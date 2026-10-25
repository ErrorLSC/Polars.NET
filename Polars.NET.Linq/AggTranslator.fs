namespace Polars.NET.Linq

open System
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers

module AggTranslator =
    open ExprTranslator

    // =========================================================================
    // 1. Data Type & Casting Helpers
    // =========================================================================

    let private int32DtypeExpr () =
        PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))

    let private int64DtypeExpr () =
        PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))

    let private castCountExpr (isLong: bool) (expr: ExprHandle) =
        let dtype = if isLong then int64DtypeExpr () else int32DtypeExpr ()
        PolarsWrapper.ExprCast(expr, dtype, strict = false, wrapNumerical = false)

    // =========================================================================
    // 2. Group Context Resolution
    // =========================================================================

    /// Resolves the appropriate TranslationContext for inner group lambdas based on parameter type
    let private getParamContext (scalarColOpt: string option) (p: ParameterExpression) : TranslationContext =
        if PolarsTypeHelper.IsScalarType p.Type || p.Type = typeof<string> then
            match scalarColOpt with
            | Some col when not (String.IsNullOrEmpty col) -> TranslationContext.Scalar(p.Name, col)
            | _ -> TranslationContext.Entity p.Name
        else
            TranslationContext.Entity p.Name

    /// Translates a single-parameter lambda expression over the group element.
    let private resolveLambda (scalarColOpt: string option) (lambda: LambdaExpression) : ExprHandle option =
        if lambda.Parameters.Count <> 1 then
            None
        else
            let p = lambda.Parameters.[0]
            let ctx = getParamContext scalarColOpt p

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

    /// Active pattern extracting intermediate transformation chains on the group source `g`.
    let rec private (|GroupSeqChain|_|) (groupParamName: string) (expr: Expression) : GroupSeq option =
        match unwrap expr with
        | null -> None
        | :? ParameterExpression as p when p.Name = groupParamName -> Some Root
        | MethodCall(m, null, [ sourceExpr; CleanLambda selector ]) when m.Name = "Select" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(Select(s, selector))
            | None -> None
        | MethodCall(m, null, [ sourceExpr; CleanLambda predicate ]) when m.Name = "Where" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(Where(s, predicate))
            | None -> None
        | MethodCall(m, null, sourceExpr :: CleanLambda keySelector :: _) when m.Name = "OrderBy" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(OrderBy(s, keySelector, false))
            | None -> None
        | MethodCall(m, null, sourceExpr :: CleanLambda keySelector :: _) when m.Name = "OrderByDescending" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(OrderBy(s, keySelector, true))
            | None -> None
        | MethodCall(m, null, sourceExpr :: CleanLambda keySelector :: _) when m.Name = "ThenBy" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(ThenBy(s, keySelector, false))
            | None -> None
        | MethodCall(m, null, sourceExpr :: CleanLambda keySelector :: _) when m.Name = "ThenByDescending" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(ThenBy(s, keySelector, true))
            | None -> None
        | MethodCall(m, null, [ sourceExpr ]) when m.Name = "Order" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(OrderBySelf(s, false))
            | None -> None
        | MethodCall(m, null, [ sourceExpr ]) when m.Name = "OrderDescending" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(OrderBySelf(s, true))
            | None -> None
        | MethodCall(m, null, [ sourceExpr; countExpr ]) when m.Name = "Take" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(Take(s, countExpr))
            | None -> None
        | MethodCall(m, null, [ sourceExpr; countExpr ]) when m.Name = "Skip" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(Skip(s, countExpr))
            | None -> None
        | MethodCall(m, null, [ sourceExpr ]) when m.Name = "Distinct" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(Distinct s)
            | None -> None
        | MethodCall(m, null, [ sourceExpr; CleanLambda keySelector ]) when m.Name = "DistinctBy" ->
            match (|GroupSeqChain|_|) groupParamName sourceExpr with
            | Some s -> Some(DistinctBy(s, keySelector))
            | None -> None
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

    let rec private evalGroupSeq
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
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
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                let currentScalar =
                    match prev.Target with
                    | Some h ->
                        let name = PolarsWrapper.ExprGetOutputName h
                        if String.IsNullOrEmpty name then scalarColOpt else Some name
                    | None -> scalarColOpt

                match resolveLambda currentScalar selector with
                | None -> None
                | Some selExpr -> Some { prev with Target = Some selExpr }

        | Where(source, predicate) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                let currentScalar =
                    match prev.Target with
                    | Some h ->
                        let name = PolarsWrapper.ExprGetOutputName h
                        if String.IsNullOrEmpty name then scalarColOpt else Some name
                    | None -> scalarColOpt

                let p = predicate.Parameters.[0]
                let ctx = getParamContext currentScalar p

                match tryTranslateWithContext ctx predicate.Body with
                | None -> None
                | Some predExpr ->
                    let combinedFilter =
                        match prev.Filter with
                        | None -> predExpr
                        | Some f -> ExprTranslator.translateBinary ExpressionType.AndAlso f predExpr

                    Some { prev with Filter = Some combinedFilter }

        | Distinct source ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev -> Some { prev with IsDistinct = true }

        | DistinctBy(source, keySelector) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                let currentScalar =
                    match prev.Target with
                    | Some h ->
                        let name = PolarsWrapper.ExprGetOutputName h
                        if String.IsNullOrEmpty name then scalarColOpt else Some name
                    | None -> scalarColOpt

                match resolveLambda currentScalar keySelector with
                | None -> None
                | Some keyExpr ->
                    Some
                        { prev with
                            Target = Some keyExpr
                            IsDistinct = true
                        }

        | OrderBy(source, keySelector, isDesc) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                let keyExprOpt =
                    let cleanBody = unwrap keySelector.Body
                    if cleanBody = (keySelector.Parameters.[0] :> Expression) then
                        prev.Target |> Option.map PolarsWrapper.CloneExpr
                    else
                        let currentScalar =
                            match prev.Target with
                            | Some h ->
                                let name = PolarsWrapper.ExprGetOutputName h
                                if String.IsNullOrEmpty name then scalarColOpt else Some name
                            | None -> scalarColOpt
                        resolveLambda currentScalar keySelector

                match keyExprOpt with
                | Some keyExpr ->
                    Some { prev with SortKeys = [ { Expr = keyExpr; IsDescending = isDesc } ] }
                | None -> None

        | ThenBy(source, keySelector, isDesc) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                let keyExprOpt =
                    let cleanBody = unwrap keySelector.Body
                    if cleanBody = (keySelector.Parameters.[0] :> Expression) then
                        prev.Target |> Option.map PolarsWrapper.CloneExpr
                    else
                        let currentScalar =
                            match prev.Target with
                            | Some h ->
                                let name = PolarsWrapper.ExprGetOutputName h
                                if String.IsNullOrEmpty name then scalarColOpt else Some name
                            | None -> scalarColOpt
                        resolveLambda currentScalar keySelector

                match keyExprOpt with
                | Some keyExpr ->
                    Some { prev with SortKeys = prev.SortKeys @ [ { Expr = keyExpr; IsDescending = isDesc } ] }
                | None -> None

        | OrderBySelf(source, isDesc) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                match prev.Target with
                | Some t ->
                    Some { prev with SortKeys = [ { Expr = PolarsWrapper.CloneExpr t; IsDescending = isDesc } ] }
                | None -> None

        | Take(source, countExpr) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                match tryEvaluate countExpr with
                | Some c ->
                    let count = Convert.ToInt64 c
                    let newSlice =
                        match prev.Slice with
                        | Some(skipOpt, _) -> Some(skipOpt, Some count)
                        | None -> Some(None, Some count)
                    Some { prev with Slice = newSlice }
                | None -> None

        | Skip(source, countExpr) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt source with
            | None -> None
            | Some prev ->
                match tryEvaluate countExpr with
                | Some c ->
                    let count = Convert.ToInt64 c
                    let newSlice =
                        match prev.Slice with
                        | Some(_, takeOpt) -> Some(Some count, takeOpt)
                        | None -> Some(Some count, None)
                    Some { prev with Slice = newSlice }
                | None -> None

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
                    match methodExprOpt with
                    | Some mExpr ->
                        match tryEvaluate mExpr with
                        | Some mVal -> tryExtractQuantileMethod mVal
                        | None -> PlQuantileMethod.Nearest
                    | None -> PlQuantileMethod.Nearest

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

    let rec private tryTranslateAggCore
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
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
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
            | Some resolved ->
                match getFilteredTarget resolved, tryExtractSep sepExpr with
                | Some colH, Some sep ->
                    let listExpr = PolarsWrapper.Implode(colH, maintainOrder = true)
                    Some(PolarsWrapper.ListJoin(listExpr, sep, ignoreNulls = true))
                | _ -> None
            | None -> None

        // ---------------------------------------------------------------------
        // Count / LongCount over Sequence Chain (e.g. g.Count(), g.Distinct().Count(), g.Count(predicate))
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq ]) when m.Name = "Count" || m.Name = "LongCount" ->
            let isLong = m.Name = "LongCount"

            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
            | Some resolved when resolved.IsDistinct ->
                getFilteredTarget resolved
                |> Option.map (fun t -> PolarsWrapper.NUnique t |> castCountExpr isLong)
            | Some resolved when resolved.Slice.IsSome ->
                getFilteredTarget resolved
                |> Option.map (fun t -> PolarsWrapper.Count t |> castCountExpr isLong)
            | Some resolved when resolved.Filter.IsSome ->
                let int64Dtype = int64DtypeExpr ()
                let castH = PolarsWrapper.ExprCast(resolved.Filter.Value, int64Dtype, strict = false, wrapNumerical = false)
                let sumH = PolarsWrapper.Sum castH
                Some sumH
            | Some _ ->
                Some(PolarsWrapper.Len())
            | None -> None

        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda predicate ]) when
            m.Name = "Count" || m.Name = "LongCount"
            ->
            let isLong = m.Name = "LongCount"
            let filteredSeq = Where(seq, predicate)

            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt filteredSeq with
            | Some resolved when resolved.IsDistinct ->
                getFilteredTarget resolved
                |> Option.map (fun t -> PolarsWrapper.NUnique t |> castCountExpr isLong)
            | Some resolved when resolved.Filter.IsSome ->
                let int64Dtype = int64DtypeExpr ()
                let castH = PolarsWrapper.ExprCast(resolved.Filter.Value, int64Dtype, strict = false, wrapNumerical = false)
                let sumH = PolarsWrapper.Sum castH
                Some sumH
            | Some _ ->
                Some(PolarsWrapper.Len())
            | None -> None

        // ---------------------------------------------------------------------
        // MinBy / MaxBy: seq.MinBy(x => x.ByCol).TargetCol
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda(Lambda([ p ], byBody)) ]),
                       memberInfo) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let ctx = getParamContext scalarColOpt p

            match tryTranslateWithContext ctx byBody with
            | Some byExpr ->
                let targetExpr = PolarsWrapper.Col memberInfo.Name

                match m.Name with
                | "MinBy" -> Some(PolarsWrapper.MinBy(targetExpr, byExpr))
                | "MaxBy" -> Some(PolarsWrapper.MaxBy(targetExpr, byExpr))
                | _ -> None
            | None -> None

        // ---------------------------------------------------------------------
        // Quantile
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda selector; qExpr ]) when
            m.Name = "Quantile"
            ->
            let projectedSeq = Select(seq, selector)

            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt projectedSeq with
            | Some resolved ->
                getFilteredTarget resolved
                |> Option.bind (fun colH -> translateQuantileCall colH qExpr None)
            | None -> None

        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda selector; qExpr; methodExpr ]) when
            m.Name = "Quantile"
            ->
            let projectedSeq = Select(seq, selector)

            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt projectedSeq with
            | Some resolved ->
                getFilteredTarget resolved
                |> Option.bind (fun colH -> translateQuantileCall colH qExpr (Some methodExpr))
            | None -> None

        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; qExpr; methodExpr ]) when m.Name = "Quantile" ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
            | Some resolved ->
                getFilteredTarget resolved
                |> Option.bind (fun colH -> translateQuantileCall colH qExpr (Some methodExpr))
            | None -> None

        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; qExpr ]) when
            m.Name = "Quantile" && not (match unwrap qExpr with :? LambdaExpression -> true | _ -> false)
            ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
            | Some resolved ->
                getFilteredTarget resolved
                |> Option.bind (fun colH -> translateQuantileCall colH qExpr None)
            | None -> None

        // ---------------------------------------------------------------------
        // Statistical Dispersion: seq.Std(selector, ddof) / seq.Var(selector, ddof)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda selector; ddofExpr ]) when
            m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance"
            ->
            let projectedSeq = Select(seq, selector)

            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt projectedSeq, tryEvaluate ddofExpr with
            | Some resolved, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal

                getFilteredTarget resolved
                |> Option.bind (translateDispersionOp m.Name ddof)
            | _ -> None

        // ---------------------------------------------------------------------
        // Nested List Aggregation: ToList() / ToArray()
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq ]) when m.Name = "ToList" || m.Name = "ToArray" ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
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
        // Standard Reductions with Selector: e.g. g.Sum(x => x.Field), g.Min(x => x.Field), g.Std(x => x.Field)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda selector ]) ->
            let projectedSeq = Select(seq, selector)

            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt projectedSeq with
            | Some resolved ->
                getFilteredTarget resolved
                |> Option.bind (translateReductionOp m.Name)
            | None -> None

        // ---------------------------------------------------------------------
        // Statistical Dispersion on pre-projected sequence with explicit ddof: e.g. g.Select(x => x.Salary).Var(0)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; ddofExpr ]) when
            (m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance")
            && not (match unwrap ddofExpr with :? LambdaExpression -> true | _ -> false)
            ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq, tryEvaluate ddofExpr with
            | Some resolved, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal

                getFilteredTarget resolved
                |> Option.bind (translateDispersionOp m.Name ddof)
            | _ -> None

        // ---------------------------------------------------------------------
        // Member Access on First / Last: seq.First().Prop / seq.Last().Prop
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ GroupSeqChain groupParamName seq ]), memberInfo) when
            m.Name = "First" || m.Name = "FirstOrDefault" || m.Name = "Last" || m.Name = "LastOrDefault"
            ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
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
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq, tryEvaluate indexExpr with
            | Some resolved, Some idxVal ->
                let idx = Convert.ToInt64 idxVal
                match getFilteredTarget resolved with
                | Some colH ->
                    Some(PolarsWrapper.Get(colH, PolarsWrapper.Lit idx, nullOnOob = true))
                | None -> None
            | _ -> None

        // ---------------------------------------------------------------------
        // Standard Reductions without Selector: e.g. g.Select(x => x.Col).Sum(), g.Sum()
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq ]) ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt seq with
            | Some resolved ->
                getFilteredTarget resolved
                |> Option.bind (translateReductionOp m.Name)
            | None -> None

        // ---------------------------------------------------------------------
        // Composite Expressions: Binary, Unary, Constant, Conditional, MethodCall
        // ---------------------------------------------------------------------
        | MethodCall(m, target, args) when isNumericDeclaringType m.DeclaringType && isNull target ->
            translateNumericMethodWithTranslator
                (tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt)
                m
                args

        | :? ConditionalExpression as cond ->
            match
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt cond.Test,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt cond.IfTrue,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt cond.IfFalse
            with
            | Some testH, Some trueH, Some falseH ->
                Some(PolarsWrapper.IfElse(testH, trueH, falseH))
            | _ -> None

        | Binary(op, left, right) ->
            match
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt left,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt right
            with
            | Some leftH, Some rightH -> Some(ExprTranslator.translateBinary op leftH rightH)
            | _ -> None

        | Unary(op, operand) ->
            tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt operand
            |> Option.bind (fun innerH ->
                match op with
                | ExpressionType.Convert
                | ExpressionType.ConvertChecked ->
                    tryTranslateCast cleanExpr.Type innerH
                | _ ->
                    translateUnary op innerH)

        | Constant(v, t) ->
            Some(toLiteralHandle v t)

        | _ -> None

    // =========================================================================
    // 5. Public API: Group Aggregation Translation with Alias
    // =========================================================================

    /// Attempts to translate group collection operations into native Polars ExprHandle with optional alias.
    let tryTranslateAggWithScalarCol
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (expr: Expression)
        (aliasName: string)
        : ExprHandle option =

        tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt expr
        |> Option.map (fun h ->
            if String.IsNullOrEmpty aliasName then h
            else PolarsWrapper.Alias(h, aliasName))
