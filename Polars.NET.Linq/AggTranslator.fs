namespace Polars.NET.Linq

open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Core.Config
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR

module AggTranslator =
    open ExprTranslator

    // =========================================================================
    // 1. Data Type & Casting Helpers
    // =========================================================================

    let private castCountExpr (isLong: bool) (expr: ScalarExpr) : ScalarExpr =
        let t = if isLong then typeof<int64> else typeof<int>
        tryTranslateCast t expr |> Option.defaultValue expr

    // =========================================================================
    // 2. Group Context Resolution
    // =========================================================================

    /// Resolves the appropriate TranslationContext for inner group lambdas based on parameter type
    let private getParamContext
        (scalarColOpt: string option)
        (rowShape: RowShape)
        (p: ParameterExpression)
        : TranslationContext =
        if PolarsTypeHelper.IsScalarType p.Type then
            match scalarColOpt with
            | Some col when not (String.IsNullOrEmpty col) -> TranslationContext.Scalar(p.Name, col)
            | _ -> TranslationContext.Entity p.Name
        elif not rowShape.IsEmpty then
            TranslationContext.ScopedEntity([ p.Name ], rowShape, [])
        else
            TranslationContext.Entity p.Name

    /// Translates a single-parameter lambda expression over the group element.
    let private resolveLambda
        (scalarColOpt: string option)
        (rowShape: RowShape)
        (lambda: LambdaExpression)
        : ScalarExpr option =
        if lambda.Parameters.Count <> 1 then
            None
        else
            let p = lambda.Parameters.[0]
            let ctx = getParamContext scalarColOpt rowShape p

            match tryResolveColumnNameWithContext ctx lambda.Body with
            | Some cName -> Some(ScalarExpr.Column cName)
            | None -> tryTranslateToScalar ctx lambda.Body

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
        | OrderBySelf of source: GroupSeq * isDescending: bool * elementType: Type
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
            Some(OrderBySelf(s, false, m.GetGenericArguments().[0]))
        | MethodCall(m, null, [ GroupSeqChain groupParamName s ]) when m.Name = "OrderDescending" ->
            Some(OrderBySelf(s, true, m.GetGenericArguments().[0]))
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

    type private ResolvedGroupSeq =
        {
            Target: ScalarExpr option
            Filter: ScalarExpr option
            IsDistinct: bool
            /// The orderings of OrderBy / ThenBy, as LINQ orders: nulls first ascending and last descending,
            /// NaN before the numbers (NaNSemantics.sortOrders).
            SortKeys: SortOrder list
            Slice: (int64 option * int64 option) option
        }

    let rec private tryGetOutputName (expr: ScalarExpr) : string option =
        match expr with
        | ScalarExpr.Column name when not (String.IsNullOrEmpty name) -> Some name
        | ScalarExpr.Alias(_, alias) when not (String.IsNullOrEmpty alias) -> Some alias
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _)
        | ScalarExpr.Slice(inner, _, _)
        | ScalarExpr.SortBy(inner, _) -> tryGetOutputName inner
        | ScalarExpr.Binary(_, left, _) -> tryGetOutputName left
        | _ -> None

    let private getCurrentScalar (scalarColOpt: string option) (targetOpt: ScalarExpr option) =
        match targetOpt with
        | Some e ->
            match tryGetOutputName e with
            | Some name -> Some name
            | None -> scalarColOpt
        | None -> scalarColOpt

    let private resolveKeyExpr
        (scalarColOpt: string option)
        (rowShape: RowShape)
        (prev: ResolvedGroupSeq)
        (keySelector: LambdaExpression)
        : ScalarExpr option =
        let cleanBody = unwrap keySelector.Body
        if cleanBody = (keySelector.Parameters.[0] :> Expression) then
            prev.Target
        else
            let currentScalar = getCurrentScalar scalarColOpt prev.Target
            resolveLambda currentScalar rowShape keySelector

    /// The orderings of one LINQ sort key of CLR type `keyType`.
    let private linqSortOrders (keyType: Type) (keyExpr: ScalarExpr) (isDesc: bool) =
        NaNSemantics.sortOrders keyType { SortOrder.Expr = keyExpr; Descending = isDesc; NullsLast = isDesc }

    let rec private evalGroupSeq
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ScalarExpr option)
        (rowShape: RowShape)
        (seq: GroupSeq)
        : ResolvedGroupSeq option =
        match seq with
        | Root ->
            let targetOpt =
                match scalarColOpt with
                | Some col when not (String.IsNullOrEmpty col) -> Some(ScalarExpr.Column col)
                | _ -> wholeRowStructOpt

            Some
                {
                    Target = targetOpt
                    Filter = None
                    IsDistinct = false
                    SortKeys = []
                    Slice = None
                }

        | Select(source, selector) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                let currentScalar = getCurrentScalar scalarColOpt prev.Target
                resolveLambda currentScalar rowShape selector
                |> Option.map (fun selExpr -> { prev with Target = Some selExpr }))

        | Where(source, predicate) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                let currentScalar = getCurrentScalar scalarColOpt prev.Target
                let p = predicate.Parameters.[0]
                let ctx = getParamContext currentScalar rowShape p

                tryTranslateToScalar ctx predicate.Body
                |> Option.map (fun predExpr ->
                    let combinedFilter =
                        match prev.Filter with
                        | None -> predExpr
                        | Some f -> ScalarExpr.Binary(BinaryOp.And, f, predExpr)

                    { prev with Filter = Some combinedFilter }))

        | Distinct source ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.map (fun prev -> { prev with IsDistinct = true })

        | DistinctBy(source, keySelector) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                let currentScalar = getCurrentScalar scalarColOpt prev.Target
                resolveLambda currentScalar rowShape keySelector
                |> Option.map (fun keyExpr ->
                    { prev with
                        Target = Some keyExpr
                        IsDistinct = true
                    }))

        | OrderBy(source, keySelector, isDesc) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                resolveKeyExpr scalarColOpt rowShape prev keySelector
                |> Option.map (fun keyExpr ->
                    { prev with SortKeys = linqSortOrders keySelector.Body.Type keyExpr isDesc }))

        | ThenBy(source, keySelector, isDesc) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                resolveKeyExpr scalarColOpt rowShape prev keySelector
                |> Option.map (fun keyExpr ->
                    { prev with SortKeys = prev.SortKeys @ linqSortOrders keySelector.Body.Type keyExpr isDesc }))

        | OrderBySelf(source, isDesc, elementType) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                prev.Target
                |> Option.map (fun t ->
                    { prev with SortKeys = linqSortOrders elementType t isDesc }))

        | Take(source, countExpr) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                PartialEvaluator.tryEvaluate countExpr
                |> Option.map (fun c ->
                    let count = Convert.ToInt64 c
                    let newSlice =
                        match prev.Slice with
                        | Some(skipOpt, _) -> Some(skipOpt, Some count)
                        | None -> Some(None, Some count)
                    { prev with Slice = newSlice }))

        | Skip(source, countExpr) ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source
            |> Option.bind (fun prev ->
                PartialEvaluator.tryEvaluate countExpr
                |> Option.map (fun c ->
                    let count = Convert.ToInt64 c
                    let newSlice =
                        match prev.Slice with
                        | Some(_, takeOpt) -> Some(Some count, takeOpt)
                        | None -> Some(Some count, None)
                    { prev with Slice = newSlice }))

        | ToList source
        | ToArray source ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape source

    /// Combines the resolved target column expression with any sequence filter predicates, sorting, and slicing.
    let private getFilteredTarget (resolved: ResolvedGroupSeq) : ScalarExpr option =
        match resolved.Target with
        | None -> None
        | Some initialTarget ->
            // 1. Filter
            let targetAfterFilter, sortKeysAfterFilter =
                match resolved.Filter with
                | Some f ->
                    let filteredTarget = ScalarExpr.Function(ScalarFunction.Filter, [ initialTarget; f ], LogicalType.Unknown)
                    let filteredSortKeys =
                        resolved.SortKeys
                        |> List.map (fun sk ->
                            { sk with Expr = ScalarExpr.Function(ScalarFunction.Filter, [ sk.Expr; f ], LogicalType.Unknown) })
                    filteredTarget, filteredSortKeys
                | None ->
                    initialTarget, resolved.SortKeys

            // 2. SortBy
            let targetAfterSort =
                if sortKeysAfterFilter.IsEmpty then
                    targetAfterFilter
                else
                    ScalarExpr.SortBy(targetAfterFilter, sortKeysAfterFilter)

            // 3. Slice (Skip / Take)
            let targetAfterSlice =
                match resolved.Slice with
                | Some(Some skip, Some take) ->
                    ScalarExpr.Slice(targetAfterSort, skip, uint32 take)
                | Some(None, Some take) ->
                    ScalarExpr.Slice(targetAfterSort, 0L, uint32 take)
                | Some(Some skip, None) ->
                    ScalarExpr.Slice(targetAfterSort, skip, UInt32.MaxValue)
                | _ ->
                    targetAfterSort

            // 4. Distinct
            let finalTarget =
                if resolved.IsDistinct then
                    ScalarExpr.Function(ScalarFunction.Unique, [ targetAfterSlice ], LogicalType.Unknown)
                else
                    targetAfterSlice

            Some finalTarget

    let private tryExtractSep (sepExpr: Expression) : string option =
        match PartialEvaluator.tryEvaluate sepExpr with
        | Some(:? string as sep) -> Some sep
        | Some(:? char as sep) -> Some(string sep)
        | _ -> None

    let private translateQuantileCall (targetS: ScalarExpr) (qExpr: Expression) (methodExprOpt: Expression option) : ScalarExpr option =
        let qOpt =
            match PartialEvaluator.tryEvaluate qExpr with
            | Some(:? IConvertible as qVal) ->
                try
                    Some(Convert.ToDouble qVal)
                with
                | :? FormatException
                | :? InvalidCastException
                | :? OverflowException -> None
            | _ -> None

        // An explicit method that cannot be evaluated is untranslatable, never replaced by the default.
        let methodOpt =
            match methodExprOpt with
            | None -> Some QuantileMethod.Nearest
            | Some methodExpr -> PartialEvaluator.tryEvaluate methodExpr |> Option.map quantileMethodOf

        match qOpt, methodOpt with
        | Some q, Some method ->
            Some(
                ScalarExpr.Function(
                    ScalarFunction.Quantile method,
                    [ targetS; ScalarExpr.Literal(LiteralValue.Float64 q) ],
                    LogicalTypes.Float64
                )
            )
        | _ -> None

    // =========================================================================
    // 4. Core Recursive Aggregation Translator
    // =========================================================================

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
        (wholeRowStructOpt: ScalarExpr option)
        (rowShape: RowShape)
        (expr: Expression)
        : ScalarExpr option =

        let (StripQuotes cleanExpr) = expr

        match cleanExpr with
        // ---------------------------------------------------------------------
        // string.Join(sep, seq)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ sepExpr; GroupSeqChain groupParamName seq ]) when
            m.DeclaringType = typeof<string> && m.Name = "Join"
            ->
            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq
            |> Option.bind getFilteredTarget
            |> Option.bind (fun colS ->
                tryExtractSep sepExpr
                |> Option.map (fun sep ->
                    let listExpr = ScalarExpr.Function(ScalarFunction.Implode, [ colS; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.ListOfUnknown)
                    ScalarExpr.Function(ScalarFunction.ListJoin, [ listExpr; ScalarExpr.Literal(LiteralValue.String sep) ], LogicalTypes.String)))

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
                match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq with
                | Some resolved when resolved.IsDistinct ->
                    getFilteredTarget resolved
                    |> Option.map (fun t -> ScalarExpr.Function(ScalarFunction.NUnique, [ t ], LogicalTypes.UInt32) |> castCountExpr isLong)
                | Some resolved when resolved.Slice.IsSome ->
                    getFilteredTarget resolved
                    |> Option.map (fun t -> ScalarExpr.Aggregation(AggKind.Count, Some t, false) |> castCountExpr isLong)
                | Some resolved when resolved.Filter.IsSome ->
                    let castS = castCountExpr true resolved.Filter.Value
                    Some(ScalarExpr.Aggregation(AggKind.Sum, Some castS, false))
                | Some _ ->
                    Some(ScalarExpr.Aggregation(AggKind.Len, None, false))
                | None -> None
            | None -> None

        // ---------------------------------------------------------------------
        // MinBy / MaxBy: seq.MinBy(x => x.ByCol).TargetCol
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda(Lambda([ p ], byBody)) ]),
                       memberInfo) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let ctx = getParamContext scalarColOpt rowShape p

            tryTranslateToScalar ctx byBody
            |> Option.bind (fun byExpr ->
                let targetExpr = ScalarExpr.Column memberInfo.Name
                match m.Name with
                | "MinBy" -> Some(ScalarExpr.Function(ScalarFunction.MinBy, [ targetExpr; byExpr ], LogicalType.Unknown))
                | "MaxBy" -> Some(ScalarExpr.Function(ScalarFunction.MaxBy, [ targetExpr; byExpr ], LogicalType.Unknown))
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
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq
                |> Option.bind getFilteredTarget
                |> Option.bind (fun colS -> translateQuantileCall colS qExpr methodExprOpt)
            | None -> None

        // ---------------------------------------------------------------------
        // Statistical Dispersion with explicit ddof: seq.Std(selector, ddof) / seq.Var(ddof)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; CleanLambda selector; ddof ]) when
            m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance" ->
            match PartialEvaluator.tryEvaluate ddof with
            | Some ddofVal ->
                let ddofByte = Convert.ToByte ddofVal
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape (Select(seq, selector))
                |> Option.bind getFilteredTarget
                |> Option.bind (translateDispersionOp m.Name ddofByte)
            | None -> None

        | MethodCall(m, null, [ GroupSeqChain groupParamName seq; ddof ]) when
            (m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance")
            && not (match ddof with CleanLambda _ -> true | _ -> false) ->
            match PartialEvaluator.tryEvaluate ddof with
            | Some ddofVal ->
                let ddofByte = Convert.ToByte ddofVal
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq
                |> Option.bind getFilteredTarget
                |> Option.bind (translateDispersionOp m.Name ddofByte)
            | None -> None

        // ---------------------------------------------------------------------
        // Nested List Aggregation: ToList() / ToArray()
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ GroupSeqChain groupParamName seq ]) when m.Name = "ToList" || m.Name = "ToArray" ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq with
            | Some resolved ->
                match getFilteredTarget resolved with
                | Some colS ->
                    Some(ScalarExpr.Function(ScalarFunction.Implode, [ colS; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.ListOfUnknown))
                | None ->
                    wholeRowStructOpt
                    |> Option.map (fun rowStruct ->
                        ScalarExpr.Function(ScalarFunction.Implode, [ rowStruct; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.ListOfUnknown))
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
                evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq
                |> Option.bind getFilteredTarget
                |> Option.bind (translateReductionOp m.Name)
            | None -> None

        // ---------------------------------------------------------------------
        // Member Access on First / Last: seq.First().Prop / seq.Last().Prop
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ GroupSeqChain groupParamName seq ]), memberInfo) when
            m.Name = "First" || m.Name = "FirstOrDefault" || m.Name = "Last" || m.Name = "LastOrDefault"
            ->
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq with
            | Some resolved ->
                let memberCol = ScalarExpr.Column memberInfo.Name
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
            match evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq, PartialEvaluator.tryEvaluate indexExpr with
            | Some resolved, Some idxVal ->
                let idx = Convert.ToInt64 idxVal
                getFilteredTarget resolved
                |> Option.map (fun colS ->
                    ScalarExpr.Function(ScalarFunction.SeriesGet, [ colS; ScalarExpr.Literal(LiteralValue.Int64 idx) ], LogicalType.Unknown))
            | _ -> None

        // ---------------------------------------------------------------------
        // Composite Expressions: Binary, Unary, Constant, Conditional, MethodCall
        // ---------------------------------------------------------------------
        | MethodCall(m, target, args) when isNumericDeclaringType m.DeclaringType && isNull target ->
            translateNumericMethod
                (tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape)
                m
                args

        | :? ConditionalExpression as cond ->
            match
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape cond.Test,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape cond.IfTrue,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape cond.IfFalse
            with
            | Some testS, Some trueS, Some falseS ->
                Some(ScalarExpr.Conditional(testS, trueS, falseS))
            | _ -> None

        | Binary(op, left, right) ->
            match
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape left,
                tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape right
            with
            | Some leftS, Some rightS -> OperatorAndUdfTranslator.tryTranslateBinary op leftS rightS
            | _ -> None

        | Unary(op, operand) ->
            tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape operand
            |> Option.bind (fun innerS ->
                match op with
                | ExpressionType.Convert
                | ExpressionType.ConvertChecked ->
                    tryTranslateCast cleanExpr.Type innerS
                | _ ->
                    OperatorAndUdfTranslator.translateUnary op innerS)

        | Constant(v, t) ->
            Some(toLiteral v t)

        | _ ->
            tryTranslateGroupUdf groupParamName scalarColOpt wholeRowStructOpt rowShape cleanExpr

    and private tryTranslateGroupUdf
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ScalarExpr option)
        (rowShape: RowShape)
        (expr: Expression)
        : ScalarExpr option =
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
                    let resolveArgScalar (arg: Expression) : ScalarExpr option =
                        match (|GroupSeqChain|_|) groupParamName arg with
                        | Some seq ->
                            evalGroupSeq groupParamName scalarColOpt wholeRowStructOpt rowShape seq
                            |> Option.bind getFilteredTarget
                        | None -> None

                    let translatedDependentArgs =
                        dependentArgs
                        |> List.map (fun a ->
                            match resolveArgScalar a with
                            | Some s -> Some(a, s)
                            | None -> None)

                    if translatedDependentArgs |> List.exists Option.isNone then
                        None
                    else
                        let isSupportedReturn = ScalarExprOps.isUdfReturnTypeSupported expr.Type

                        if not isSupportedReturn then
                            None
                        else
                            let retPlType =
                                (LogicalTypes.tryOfClrType expr.Type)
                                |> Option.defaultValue LogicalType.Unknown
                            let dynArgs = translatedDependentArgs |> List.choose id
                            let dynCount = dynArgs.Length
                            let returnsScalar = isScalarLikeResultType expr.Type

                            ArrowUdfBatchAdapter.WarnUdfDetected(methodName, expr.ToString())
                            CompilationScope.Current.RecordUdf(sprintf "%s: %s" methodName (expr.ToString()))

                            if dynCount = 1 then
                                let argExpr, scalarArg = dynArgs.[0]
                                let userFunc = OperatorAndUdfTranslator.compileSingleArgUdfDelegate argExpr expr
                                let arrowCallback = ArrowUdfBatchAdapter.CreateGroupAggregationSingle(userFunc, argExpr.Type, expr.Type)
                                Some(ScalarExpr.Udf(methodName, [ scalarArg ], retPlType, UdfInvoker.GroupSingleBatch(box arrowCallback, expr.Type, returnsScalar)))

                            elif dynCount > 1 then
                                let userFunc = OperatorAndUdfTranslator.compileMultiArgUdfDelegate dynArgs expr
                                let inputTypes = dynArgs |> List.map (fun (a, _) -> a.Type) |> List.toArray
                                let arrowCallback = ArrowUdfBatchAdapter.CreateGroupAggregationMulti(userFunc, inputTypes, expr.Type)
                                let scalarArgs = dynArgs |> List.map snd
                                Some(ScalarExpr.Udf(methodName, scalarArgs, retPlType, UdfInvoker.GroupMultiBatch(box arrowCallback, expr.Type, returnsScalar)))
                            else
                                None
        with
        | :? InvalidQueryArgumentException -> reraise ()
        | _ -> None

    // =========================================================================
    // 5. Public API: Group Aggregation Translation with Alias
    // =========================================================================

    /// Attempts to translate group collection operations into pure symbolic ScalarExpr with optional alias.
    let tryTranslateAggWithScalarColAndScope
        (groupParamName: string)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ScalarExpr option)
        (rowShape: RowShape)
        (expr: Expression)
        (aliasName: string)
        : ScalarExpr option =

        tryTranslateAggCore groupParamName scalarColOpt wholeRowStructOpt rowShape expr
        |> Option.map (fun s ->
            if String.IsNullOrEmpty aliasName then s
            else ScalarExpr.Alias(s, aliasName))
