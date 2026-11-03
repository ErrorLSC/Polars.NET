namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Arrow
open Polars.NET.Linq.IR
open System
open System.Linq
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal ColumnResolver =

    /// Strips Unary wraps like Convert or Quote recursively
    let inline internal unwrap (e: Expression) = ExpressionPatterns.unwrap e

    // =========================================================================
    // 1. Type & Literal Mapping Helpers
    // =========================================================================
    /// Maps standard LINQ reduction method names to their corresponding native Polars aggregation expressions.
    let internal translateReductionOp (methodName: string) (targetColExpr: ExprHandle) : ExprHandle option =
        match methodName with
        | "Sum" -> Some(PolarsWrapper.Sum targetColExpr)
        | "Mean"
        | "Average" -> Some(PolarsWrapper.Mean targetColExpr)
        | "Min" -> Some(PolarsWrapper.Min targetColExpr)
        | "Max" -> Some(PolarsWrapper.Max targetColExpr)
        | "Median" -> Some(PolarsWrapper.Median targetColExpr)
        | "First"
        | "FirstOrDefault" -> Some(PolarsWrapper.First(targetColExpr, ignoreNulls = false))
        | "Last"
        | "LastOrDefault" -> Some(PolarsWrapper.Last(targetColExpr, ignoreNulls = false))
        | "Std"
        | "StdDev" -> Some(PolarsWrapper.Std(targetColExpr, 1uy))
        | "Var"
        | "Variance" -> Some(PolarsWrapper.Var(targetColExpr, 1uy))
        | _ -> None

    /// Pure symbolic translation of standard LINQ reduction method names to ScalarExpr.Aggregation nodes.
    let internal translateReductionOpScalar (methodName: string) (targetColExpr: ScalarExpr) : ScalarExpr option =
        match methodName with
        | "Sum" -> Some(ScalarExpr.Aggregation(AggKind.Sum, Some targetColExpr, false))
        | "Mean"
        | "Average" -> Some(ScalarExpr.Aggregation(AggKind.Mean, Some targetColExpr, false))
        | "Min" -> Some(ScalarExpr.Aggregation(AggKind.Min, Some targetColExpr, false))
        | "Max" -> Some(ScalarExpr.Aggregation(AggKind.Max, Some targetColExpr, false))
        | "Median" -> Some(ScalarExpr.Aggregation(AggKind.Median, Some targetColExpr, false))
        | "First"
        | "FirstOrDefault" -> Some(ScalarExpr.Aggregation(AggKind.First, Some targetColExpr, false))
        | "Last"
        | "LastOrDefault" -> Some(ScalarExpr.Aggregation(AggKind.Last, Some targetColExpr, false))
        | "Std"
        | "StdDev" -> Some(ScalarExpr.Aggregation(AggKind.Std, Some targetColExpr, false))
        | "Var"
        | "Variance" -> Some(ScalarExpr.Aggregation(AggKind.Var, Some targetColExpr, false))
        | _ -> None

    /// Maps statistical dispersion reduction operations with explicit degree of freedom (ddof).
    let internal translateDispersionOp
        (methodName: string)
        (ddof: byte)
        (targetColExpr: ExprHandle)
        : ExprHandle option =
        match methodName with
        | "Std"
        | "StdDev" -> Some(PolarsWrapper.Std(targetColExpr, ddof))
        | "Var"
        | "Variance" -> Some(PolarsWrapper.Var(targetColExpr, ddof))
        | _ -> None

    /// Pure symbolic translation of statistical dispersion reduction operations.
    let internal translateDispersionOpScalar
        (methodName: string)
        (_ddof: byte)
        (targetColExpr: ScalarExpr)
        : ScalarExpr option =
        match methodName with
        | "Std"
        | "StdDev" -> Some(ScalarExpr.Aggregation(AggKind.Std, Some targetColExpr, false))
        | "Var"
        | "Variance" -> Some(ScalarExpr.Aggregation(AggKind.Var, Some targetColExpr, false))
        | _ -> None

    let internal isTemporalType (t: Type) =
        t = typeof<DateTime>
        || t = typeof<DateTimeOffset>
        || t = typeof<DateOnly>
        || t = typeof<TimeOnly>

    [<TailCall>]
    let rec internal isNullConstantExpr (e: Expression) =
        match e with
        | null -> true
        | :? ConstantExpression as c -> isNull c.Value
        | :? UnaryExpression as u when
            u.NodeType = ExpressionType.Convert
            || u.NodeType = ExpressionType.ConvertChecked
            || u.NodeType = ExpressionType.Quote
            ->
            isNullConstantExpr u.Operand
        | _ -> false

    let internal tryGetPolarsDataType (t: Type) : DataTypeHandle option =
        try
            let arrowType = ArrowTypeResolver.GetArrowTypeFromNetType t
            Some(PolarsWrapper.MapArrowToDataTypeHandle arrowType)
        with _ ->
            None

    [<TailCall>]
    let rec internal toLiteralHandle (value: obj) (t: Type) : ExprHandle =
        match value with
        | null -> PolarsWrapper.LitNull()
        | _ ->
            let coreType = PolarsTypeHelper.UnwrapCoreType(t)

            if coreType.IsEnum then
                let underlyingType = Enum.GetUnderlyingType coreType
                let underlyingVal = Convert.ChangeType(value, underlyingType)
                toLiteralHandle underlyingVal underlyingType
            else
                match value with
                | :? bool as b -> PolarsWrapper.Lit b
                | :? sbyte as sb -> PolarsWrapper.Lit sb
                | :? byte as b -> PolarsWrapper.Lit b
                | :? int16 as s -> PolarsWrapper.Lit s
                | :? uint16 as us -> PolarsWrapper.Lit us
                | :? int as i -> PolarsWrapper.Lit i
                | :? uint32 as ui -> PolarsWrapper.Lit ui
                | :? int64 as l -> PolarsWrapper.Lit l
                | :? uint64 as ul -> PolarsWrapper.Lit ul
                | :? Int128 as i128 -> PolarsWrapper.Lit i128
                | :? decimal as d -> PolarsWrapper.Lit d
                | :? Half as h -> PolarsWrapper.Lit h
                | :? float32 as f -> PolarsWrapper.Lit f
                | :? double as d -> PolarsWrapper.Lit d
                | :? char as c -> PolarsWrapper.Lit(string c)
                | :? string as s -> PolarsWrapper.Lit s
                | :? DateTime as dt -> PolarsWrapper.Lit dt
                | :? DateTimeOffset as doff -> PolarsWrapper.Lit doff
                | :? TimeSpan as ts -> PolarsWrapper.Lit ts
                | :? DateOnly as dof -> PolarsWrapper.Lit dof
                | :? TimeOnly as tof -> PolarsWrapper.Lit tof
                | other ->
                    failwithf
                        "Type '%s' is not supported as a constant literal in Polars LINQ"
                        (other.GetType().FullName)

    let internal tryGetPlDataType (t: Type) : PlDataType option =
        let core = PolarsTypeHelper.UnwrapCoreType t
        if core = typeof<bool> then Some PlDataType.Boolean
        elif core = typeof<sbyte> then Some PlDataType.Int8
        elif core = typeof<int16> then Some PlDataType.Int16
        elif core = typeof<int> then Some PlDataType.Int32
        elif core = typeof<int64> then Some PlDataType.Int64
        elif core = typeof<byte> then Some PlDataType.UInt8
        elif core = typeof<uint16> then Some PlDataType.UInt16
        elif core = typeof<uint32> then Some PlDataType.UInt32
        elif core = typeof<uint64> then Some PlDataType.UInt64
        elif core = typeof<float32> then Some PlDataType.Float32
        elif core = typeof<double> then Some PlDataType.Float64
        elif core = typeof<decimal> then Some PlDataType.Decimal
        elif core = typeof<string> then Some PlDataType.String
        elif core = typeof<DateOnly> then Some PlDataType.Date
        elif core = typeof<DateTime> || core = typeof<DateTimeOffset> then Some PlDataType.Datetime
        elif core = typeof<TimeSpan> then Some PlDataType.Duration
        elif core = typeof<TimeOnly> then Some PlDataType.Time
        else None

    [<TailCall>]
    let rec internal toLiteralScalar (value: obj) (t: Type) : ScalarExpr =
        match value with
        | null ->
            let dt = tryGetPlDataType t |> Option.defaultValue PlDataType.Null
            ScalarExpr.Literal(LiteralValue.Null dt)
        | _ ->
            let coreType = PolarsTypeHelper.UnwrapCoreType t
            if coreType.IsEnum then
                let underlyingType = Enum.GetUnderlyingType coreType
                let underlyingVal = Convert.ChangeType(value, underlyingType)
                toLiteralScalar underlyingVal underlyingType
            else
                match value with
                | :? char as c -> ScalarExpr.Literal(LiteralValue.String(string c))
                | _ -> ScalarExpr.Literal(LiteralValue.OfObject value)

    let internal tryTranslateCastScalar (targetType: Type) (operand: ScalarExpr) : ScalarExpr option =
        if targetType = typeof<obj> then
            Some operand
        else
            tryGetPlDataType targetType
            |> Option.map (fun dt -> ScalarExpr.Cast(operand, dt, isStrict = false))

    let internal tryTranslateCast (targetType: Type) (operand: ExprHandle) : ExprHandle option =
        if targetType = typeof<obj> then
            Some operand
        else
            tryGetPolarsDataType targetType
            |> Option.map (fun dtypeHandle ->
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle
                PolarsWrapper.ExprCast(operand, dtypeExpr, strict = false, wrapNumerical = false))

    // =========================================================================
    // 2. Column & Parameter Identifiers
    // =========================================================================

    /// Transparently unwraps .Value on tuple components: pair.Outer.Value.DeptName
    /// Returns Some true for Item1/Outer, Some false for Item2/Inner, None otherwise.
    let rec internal isTupleAccess (pairParam: string) (e: Expression) : bool option =
        match unwrap e with
        | :? MemberExpression as me ->
            match unwrap me.Expression with
            | :? ParameterExpression as p when p.Name = pairParam ->
                let mName = me.Member.Name

                if mName = "Item1" || mName = "Outer" then Some true
                elif mName = "Item2" || mName = "Inner" then Some false
                else None
            | other -> isTupleAccess pairParam other
        | _ -> None

    let rec private tryExtractGroupingCarrierMember (e: Expression) : string option =
        match unwrap e with
        | :? ParameterExpression as p when
            p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>> ->
            Some p.Name
        | MemberAccess(_, m) when
            (match m with
             | :? PropertyInfo as p ->
                 p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
             | :? FieldInfo as f ->
                 f.FieldType.IsGenericType && f.FieldType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
             | _ -> false) ->
            Some m.Name
        | MemberAccess(inner, nextM) when isNavigationMember nextM ->
            tryExtractGroupingCarrierMember inner
        | _ -> None

    let rec internal tryExtractScopeName (expr: Expression) : string option =
        match unwrap expr with
        | :? ParameterExpression as p ->
            let name = p.Name
            if not (isNull name) && not (name.StartsWith "<>") && not (name.Contains "TransparentIdentifier") && not (name.StartsWith "_arg") && not (name.StartsWith "tupled") then
                Some name
            else
                None
        | MemberAccess(target, m) ->
            let name = m.Name
            if not (name.StartsWith "<>") && not (name.Contains "TransparentIdentifier") && not (name.StartsWith "Item") && name <> "Fst" && name <> "Snd" then
                Some name
            else
                tryExtractScopeName target
        | MethodCall(m, _, [ arg ]) when m.Name = "Fst" || m.Name = "Snd" || m.Name = "fst" || m.Name = "snd" ->
            tryExtractScopeName arg
        | _ -> None

    /// Resolves the underlying physical column name using a strongly-typed TranslationContext.
    let rec internal tryResolveColumnNameWithContext (ctx: TranslationContext) (expr: Expression) : string option =
        if not ctx.AllowsColumnMemberLookup then
            None
        else
            match expr with
            | :? ParameterExpression as p when
                not (isNull p.Name)
                && (p.Name.StartsWith "__subquery_agg_" || p.Name.StartsWith "__subquery_col_")
                ->
                Some p.Name

            | :? ParameterExpression as p when
                not (isNull p.Name) && p.Name.StartsWith "__key_col_"
                ->
                Some(p.Name.Substring 10)

            | MemberAccess(inner, m) when isNullableValue m ->
                tryResolveColumnNameWithContext ctx inner

            | MemberAccess(_, m) when isNavigationMember m || isNullableHasValue m -> None

            | MemberAccess(MemberAccess(inner, k), itemProp) when
                k.Name = "Key" && (tryExtractGroupingCarrierMember inner).IsSome ->
                let rec isRootedInParam (e: Expression) =
                    match unwrap e with
                    | :? ParameterExpression as p -> isSyntheticScopeParam ctx.ContainsParam p
                    | MemberAccess(nextInner, _) -> isRootedInParam nextInner
                    | _ -> false

                if isRootedInParam inner then
                    let itemIdx =
                        if itemProp.Name = "Item1" || itemProp.Name = "Fst" then 0
                        elif itemProp.Name = "Item2" || itemProp.Name = "Snd" then 1
                        elif itemProp.Name.StartsWith "Item" then
                            match Int32.TryParse(itemProp.Name.Substring 4) with
                            | true, i -> i - 1
                            | _ -> -1
                        else -1

                    match ctx with
                    | TranslationContext.EntityWithCols(_, cols)
                    | TranslationContext.ScopedEntity(_, _, cols) ->
                        if itemIdx >= 0 && itemIdx < cols.Length then
                            Some cols.[itemIdx]
                        elif cols |> List.contains itemProp.Name then
                            Some itemProp.Name
                        else
                            Some itemProp.Name
                    | TranslationContext.TupleJoin(_, leftCols, _)
                    | TranslationContext.BinaryJoin(_, _, leftCols, _)
                    | TranslationContext.CorrelatedJoin(_, _, leftCols, _, _) ->
                        if leftCols.Contains itemProp.Name then
                            Some itemProp.Name
                        elif itemIdx >= 0 && itemIdx < leftCols.Count then
                            let sorted = leftCols |> Set.toList
                            Some sorted.[itemIdx]
                        else
                            Some itemProp.Name
                    | _ ->
                        Some itemProp.Name
                else
                    None

            | MemberAccess(inner, m) when m.Name = "Key" && (tryExtractGroupingCarrierMember inner).IsSome ->
                let rec isRootedInParam (e: Expression) =
                    match unwrap e with
                    | :? ParameterExpression as p -> isSyntheticScopeParam ctx.ContainsParam p
                    | MemberAccess(nextInner, _) -> isRootedInParam nextInner
                    | _ -> false

                if isRootedInParam inner then
                    match ctx with
                    | TranslationContext.EntityWithCols(_, cols)
                    | TranslationContext.ScopedEntity(_, _, cols) when not cols.IsEmpty ->
                        Some cols.[0]
                    | _ ->
                        tryExtractGroupingCarrierMember inner
                else
                    None

            | MemberAccess(inner, m) ->
                if
                    PolarsTypeHelper.IsScalarType inner.Type
                    || inner.Type = typeof<string>
                    || isTemporalType inner.Type
                then
                    None
                else
                    let rec isRootedIn (targetName: string) (e: Expression) =
                        if String.IsNullOrEmpty targetName then
                            false
                        else
                            match unwrap e with
                            | :? ParameterExpression as p -> p.Name = targetName
                            | MemberAccess(nextInner, nextM) when isNavigationMember nextM ->
                                isRootedIn targetName nextInner
                            | _ -> false

                    match ctx with
                    | TranslationContext.Merge(target, source, suffix) ->
                        if isRootedIn target inner then Some m.Name
                        elif isRootedIn source inner then Some(m.Name + suffix)
                        else None

                    | TranslationContext.BinaryJoin(leftParam, rightParam, leftCols, rightSuffix) ->
                        if isRootedIn leftParam inner then
                            Some m.Name
                        elif isRootedIn rightParam inner then
                            if leftCols.Contains m.Name then
                                Some(m.Name + rightSuffix)
                            else
                                Some m.Name
                        else
                            None

                    | TranslationContext.CorrelatedJoin(outerParams, innerParam, leftCols, rightSuffix, scopeMap) ->
                        let scopeOpt = tryExtractScopeName inner
                        match scopeOpt with
                        | Some s when s = innerParam ->
                            if leftCols.Contains m.Name then
                                Some(m.Name + rightSuffix)
                            else
                                Some m.Name
                        | Some s when List.contains s outerParams || (scopeMap |> Map.exists (fun (scope, _) _ -> scope = s)) ->
                            match scopeMap.TryFind(s, m.Name) with
                            | Some physicalCol -> Some physicalCol
                            | None ->
                                if leftCols.Contains m.Name then Some m.Name
                                else Some m.Name
                        | _ ->
                            if isRootedIn innerParam inner then
                                if leftCols.Contains m.Name then Some(m.Name + rightSuffix)
                                else Some m.Name
                            elif outerParams |> List.exists (fun p -> isRootedIn p inner) then
                                match scopeOpt with
                                | Some s ->
                                    match scopeMap.TryFind(s, m.Name) with
                                    | Some physicalCol -> Some physicalCol
                                    | None -> Some m.Name
                                | None -> Some m.Name
                            else
                                None

                    | TranslationContext.TupleJoin(pairParam, leftCols, rightSuffix) ->
                        match isTupleAccess pairParam inner with
                        | Some true -> Some m.Name

                        | Some false ->
                            if leftCols.Contains m.Name then
                                Some(m.Name + rightSuffix)
                            else
                                Some m.Name

                        | None ->
                            if leftCols.Contains m.Name then
                                Some m.Name
                            else
                                Some(m.Name + rightSuffix)

                    | TranslationContext.ScopedEntity(_, scopeMap, fallbackCols) ->
                        let rec isRootedInParamOrClosure (e: Expression) =
                            match unwrap e with
                            | :? ParameterExpression as p ->
                                isSyntheticScopeParam ctx.ContainsParam p
                            // Allow transparent navigation through intermediate scope / Nullable<T>.Value
                            | MemberAccess(nextInner, nextM) when isNavigationMember nextM ->
                                isRootedInParamOrClosure nextInner
                            | _ -> false

                        if isRootedInParamOrClosure inner then
                            let scopeOpt = tryExtractScopeName inner
                            match scopeOpt with
                            | Some scope ->
                                match scopeMap.TryFind(scope, m.Name) with
                                | Some physicalCol -> Some physicalCol
                                | None ->
                                    match scopeMap.TryFind("", m.Name) with
                                    | Some physicalCol -> Some physicalCol
                                    | None ->
                                        if List.contains m.Name fallbackCols then
                                            Some m.Name
                                        else
                                            fallbackCols
                                            |> List.tryFind (fun c -> String.Equals(c, m.Name, StringComparison.OrdinalIgnoreCase))
                                            |> Option.orElse (Some m.Name)
                            | None ->
                                match scopeMap.TryFind("", m.Name) with
                                | Some physicalCol -> Some physicalCol
                                | None ->
                                    if List.contains m.Name fallbackCols then
                                        Some m.Name
                                    else
                                        fallbackCols
                                        |> List.tryFind (fun c -> String.Equals(c, m.Name, StringComparison.OrdinalIgnoreCase))
                                        |> Option.orElse (Some m.Name)
                        else
                            None

                    | TranslationContext.EntityWithCols(_, cols) ->
                        let rec isRootedInParamOrClosure (e: Expression) =
                            match unwrap e with
                            | :? ParameterExpression as p ->
                                isSyntheticScopeParam ctx.ContainsParam p
                            // Allow transparent navigation through intermediate scope / Nullable<T>.Value
                            | MemberAccess(nextInner, nextM) when isNavigationMember nextM ->
                                isRootedInParamOrClosure nextInner
                            | _ -> false

                        if isRootedInParamOrClosure inner then
                            if not cols.IsEmpty && List.contains m.Name cols then
                                Some m.Name
                            else
                                let itemIdx =
                                    if m.Name = "Item1" || m.Name = "Fst" then 0
                                    elif m.Name = "Item2" || m.Name = "Snd" then 1
                                    elif m.Name.StartsWith "Item" then
                                        match Int32.TryParse(m.Name.Substring 4) with
                                        | true, i -> i - 1
                                        | _ -> -1
                                    else -1

                                if itemIdx >= 0 && itemIdx < cols.Length then
                                    Some cols.[itemIdx]
                                else
                                    cols |> List.tryFind (fun c -> String.Equals(c, m.Name, StringComparison.OrdinalIgnoreCase))
                                    |> Option.orElse (Some m.Name)
                        else
                            None

                    | _ ->
                        let rec isRootedInParamOrClosure (e: Expression) =
                            match unwrap e with
                            | :? ParameterExpression as p ->
                                isSyntheticScopeParam ctx.ContainsParam p
                            // Allow transparent navigation through intermediate scope / Nullable<T>.Value
                            | MemberAccess(nextInner, nextM) when isNavigationMember nextM ->
                                isRootedInParamOrClosure nextInner
                            | _ -> false

                        if isRootedInParamOrClosure inner then Some m.Name else None

            | _ -> None
