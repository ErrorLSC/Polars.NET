namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System
open System.Text
open System.Linq
open System.Reflection
open System.Linq.Expressions
open System.Text.RegularExpressions
open Polars.NET.Core.Helpers

/// Represents the semantic parameter context for LINQ expression tree translation.
[<RequireQualifiedAccess>]
type internal TranslationContext =
    /// Empty context with no parameter bindings (e.g. constants, unbound reductions, or outer Having predicates).
    | Empty

    /// Standard single-entity row context (e.g. e => e.Age), where member accesses resolve to DataFrame columns.
    | Entity of paramName: string

    /// Entity row context carrying known column names for precise tuple key and carrier member resolution.
    | EntityWithCols of paramName: string * cols: string list

    /// Scalar sequence element context (e.g. x => x.Length or x => x > 10).
    /// The parameter node itself resolves to physicalCol, while member accesses on it represent scalar operations.
    | Scalar of paramName: string * physicalCol: string

    /// Binary parameter context for Joins and Merges (e.g. (outer, inner) => new { outer.Id, inner.Name }).
    | BinaryJoin of leftParam: string * rightParam: string * leftCols: Set<string> * rightSuffix: string

    /// Merge context: (target, source) => predicate
    /// Target columns resolve to "Col", source columns resolve to "Col" + sourceSuffix.
    | Merge of targetParam: string * sourceParam: string * sourceSuffix: string

    /// Binary Tuple Join context for .NET 11 tuple joins returning (TOuter Outer, TInner Inner).
    /// Resolves pair.Outer (Item1) and pair.Inner (Item2) against physical columns in the joined LazyFrame.
    | TupleJoin of pairParam: string * leftCols: Set<string> * rightSuffix: string

    /// Binary correlated subquery join context (outerParams, innerParam, leftCols, rightSuffix, scopeMap)
    | CorrelatedJoin of outerParams: string list * innerParam: string * leftCols: Set<string> * rightSuffix: string * scopeMap: Map<string * string, string>

    /// Scoped entity context carrying known range variable / scope mappings (e.g. ("sn", "n_name") -> "n_name_right")
    /// across cascaded multi-table joins.
    | ScopedEntity of paramNames: string list * scopeMap: Map<string * string, string> * fallbackCols: string list

    /// List element placeholder context for row-level ListEval (pl.element()).
    | ListElement of paramName: string

    /// Determines whether the given parameter name matches any parameter active in the current scope.
    member this.ContainsParam(name: string) : bool =
        match this with
        | TranslationContext.Empty -> false
        | TranslationContext.Entity p -> p = name
        | TranslationContext.EntityWithCols(p, _) -> p = name
        | TranslationContext.ScopedEntity(paramsList, scopeMap, _) ->
            List.contains name paramsList || (scopeMap |> Map.exists (fun (scope, _) _ -> scope = name))
        | TranslationContext.ListElement p -> p = name
        | TranslationContext.Scalar(p, _) -> p = name
        | TranslationContext.BinaryJoin(left, right, _, _) -> name = left || name = right
        | TranslationContext.Merge(target, source, _) -> name = target || name = source
        | TranslationContext.TupleJoin(pairParam, _, _) -> name = pairParam
        | TranslationContext.CorrelatedJoin(outerParams, inner, _, _, scopeMap) ->
            List.contains name outerParams || name = inner || (scopeMap |> Map.exists (fun (s, _) _ -> s = name))

    /// Attempts to resolve the parameter expression node itself to a native Polars ExprHandle.
    member this.TryResolveParameterHandle(name: string) : ExprHandle option =
        match this with
        | TranslationContext.Scalar(p, physicalCol) when p = name && not (String.IsNullOrEmpty physicalCol) ->
            Some(PolarsWrapper.Col physicalCol)
        | TranslationContext.ListElement p when p = name ->
            Some(PolarsWrapper.Col "")
        | _ -> None

    /// Indicates whether member/property accesses are eligible to be resolved as DataFrame columns.
    member this.AllowsColumnMemberLookup: bool =
        match this with
        | TranslationContext.Scalar _ -> false
        | _ -> true

/// Recursive module for translating LINQ Expression Trees to native Polars Expr handles.
module internal rec ExprTranslator =

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

    [<TailCall>]
    let rec private isNullConstantExpr (e: Expression) =
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

    let private tryGetPolarsDataType (t: Type) : DataTypeHandle option =
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
    let rec private isTupleAccess (pairParam: string) (e: Expression) : bool option =
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

    // =========================================================================
    // 3. Member Access Pushdown Dispatcher
    // =========================================================================

    let private isTemporalType (t: Type) =
        t = typeof<DateTime>
        || t = typeof<DateTimeOffset>
        || t = typeof<DateOnly>
        || t = typeof<TimeOnly>

    let private translateMemberAccess (ctx: TranslationContext) (memberExpr: Expression) : ExprHandle option =
        match tryResolveColumnNameWithContext ctx memberExpr with
        | Some actualCol -> Some(PolarsWrapper.Col actualCol)
        | None ->
            match memberExpr with
            | MemberAccess(inner, m) when isNullableValue m ->
                tryTranslateWithContext ctx inner

            | MemberAccess(null, m) when m.DeclaringType = typeof<Math> || m.DeclaringType = typeof<MathF> ->
                match m.Name with
                | "PI" -> Some(PolarsWrapper.Lit Math.PI)
                | "E" -> Some(PolarsWrapper.Lit Math.E)
                | "Tau" -> Some(PolarsWrapper.Lit(Math.PI * 2.0))
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<string> && m.Name = "Length" ->
                tryTranslateWithContext ctx target |> Option.map PolarsWrapper.StrLenChars

            | MemberAccess(target, m) when not (isNull target) && isTemporalType target.Type ->
                let t () = tryTranslateWithContext ctx target

                match m.Name with
                | "Year" -> t () |> Option.map PolarsWrapper.DtYear
                | "Month" -> t () |> Option.map PolarsWrapper.DtMonth
                | "Day" -> t () |> Option.map PolarsWrapper.DtDay
                | "DayOfYear" -> t () |> Option.map PolarsWrapper.DtOrdinalDay
                | "DayOfWeek" ->
                    // Polars dt.weekday() returns ISO weekday (1..7, Monday=1, Sunday=7) as Int8.
                    // .NET System.DayOfWeek defines Sunday=0, Monday=1..Saturday=6, with underlying type Int32.
                    // We cast to Int32 and compute (weekday % 7) to:
                    // 1. Map Sunday (7 % 7 = 0) to DayOfWeek.Sunday and preserve Monday..Saturday (1..6).
                    // 2. Ensure the Arrow column type is Int32 so CLR materializer can cleanly unbox it to DayOfWeek.
                    t ()
                    |> Option.bind (fun dtH ->
                        let weekdayH = PolarsWrapper.DtWeekday dtH

                        match tryTranslateCast typeof<int> weekdayH with
                        | Some weekdayInt -> Some(PolarsWrapper.Rem(weekdayInt, PolarsWrapper.Lit 7))
                        | None ->
                            let rem = PolarsWrapper.Rem(weekdayH, PolarsWrapper.Lit 7y)
                            tryTranslateCast typeof<int> rem)
                | "Hour" -> t () |> Option.map PolarsWrapper.DtHour
                | "Minute" -> t () |> Option.map PolarsWrapper.DtMinute
                | "Second" -> t () |> Option.map PolarsWrapper.DtSecond
                | "Millisecond" -> t () |> Option.map PolarsWrapper.DtMillisecond
                | "Microsecond" -> t () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanosecond" -> t () |> Option.map PolarsWrapper.DtNanosecond
                | "Date" -> t () |> Option.map PolarsWrapper.DtDate
                | "TimeOfDay" -> t () |> Option.map PolarsWrapper.DtTime
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<TimeSpan> ->
                let t () = tryTranslateWithContext ctx target

                match m.Name with
                | "TotalDays" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalDays(x, fractional = true))
                | "TotalHours" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalHours(x, fractional = true))
                | "TotalMinutes" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalMinutes(x, fractional = true))
                | "TotalSeconds" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalSeconds(x, fractional = true))
                | "TotalMilliseconds" ->
                    t ()
                    |> Option.map (fun x -> PolarsWrapper.DtTotalMilliseconds(x, fractional = true))
                | "TotalMicroseconds" ->
                    t ()
                    |> Option.map (fun x -> PolarsWrapper.DtTotalMicroseconds(x, fractional = true))
                | "TotalNanoseconds" ->
                    t ()
                    |> Option.map (fun x -> PolarsWrapper.DtTotalNanoseconds(x, fractional = true))
                | "Days" -> t () |> Option.map PolarsWrapper.DtDay
                | "Hours" -> t () |> Option.map PolarsWrapper.DtHour
                | "Minutes" -> t () |> Option.map PolarsWrapper.DtMinute
                | "Seconds" -> t () |> Option.map PolarsWrapper.DtSecond
                | "Milliseconds" -> t () |> Option.map PolarsWrapper.DtMillisecond
                | "Microseconds" -> t () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanoseconds" -> t () |> Option.map PolarsWrapper.DtNanosecond
                | _ -> None

            // Translate Nullable<T>.HasValue directly to Polars is_not_null()
            | MemberAccess(target, m) when isNullableHasValue m ->
                match tryTranslateWithContext ctx target with
                | Some colH -> Some(PolarsWrapper.IsNotNull colH)
                | None ->
                    match ctx with
                    | TranslationContext.TupleJoin(pairParam, leftCols, _) ->
                        match isTupleAccess pairParam target with
                        | Some true ->
                            // Left side of join: picking any valid left column detects whether Outer row exists
                            leftCols
                            |> Seq.tryHead
                            |> Option.map (fun c -> PolarsWrapper.IsNotNull(PolarsWrapper.Col c))
                        | _ -> None
                    | _ -> None

            | _ ->
                tryEvaluate memberExpr
                |> Option.map (fun value -> toLiteralHandle value memberExpr.Type)

    // =========================================================================
    // 4. Method Call Dispatchers (String / DateTime / Math)
    // =========================================================================

    /// Evaluates StringComparison expression to determine case insensitivity.
    let private tryEvaluateCaseInsensitive (compExpr: Expression) : bool option =
        match tryEvaluate compExpr with
        | Some(:? StringComparison as sc) ->
            match sc with
            | StringComparison.OrdinalIgnoreCase
            | StringComparison.InvariantCultureIgnoreCase
            | StringComparison.CurrentCultureIgnoreCase -> Some true
            | _ -> Some false
        | Some(:? bool as ignoreCase) -> Some ignoreCase
        | Some other ->
            try
                let intVal = Convert.ToInt32 other

                match LanguagePrimitives.EnumOfValue<int, StringComparison> intVal with
                | StringComparison.OrdinalIgnoreCase
                | StringComparison.InvariantCultureIgnoreCase
                | StringComparison.CurrentCultureIgnoreCase -> Some true
                | _ -> Some false
            with _ ->
                None
        | None -> None

    let private tryExtractCharSet (expr: Expression) : char array option =
        match unwrap expr with
        | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
            let chars =
                nae.Expressions
                |> Seq.choose (fun e ->
                    match tryEvaluate e with
                    | Some(:? char as c) -> Some c
                    | Some(:? string as s) when s.Length = 1 -> Some s.[0]
                    | _ -> None)
                |> Seq.toArray
            if chars.Length = nae.Expressions.Count then Some chars else None
        | other ->
            match tryEvaluate other with
            | Some(:? (char array) as ca) -> Some ca
            | Some(:? string as s) -> Some(s.ToCharArray())
            | Some(:? (string array) as sa) ->
                let chars = sa |> Array.choose (fun s -> if s.Length = 1 then Some s.[0] else None)
                if chars.Length = sa.Length then Some chars else None
            | _ -> None

    let private buildCharClassRegex (chars: char array) : string =
        let escaped = chars |> Array.map (fun c -> Regex.Escape(string c))
        sprintf "(%s)" (String.concat "|" escaped)

    let private translateStringMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target

        match m.Name, args with
        // --- 1. String.Concat (Static string.Concat / flattened arguments) ---
        | "Concat", _ when isNull target ->
            let flatArgs =
                match args with
                | [ single ] ->
                    match unwrap single with
                    | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
                        nae.Expressions |> Seq.toList
                    | _ -> args
                | other -> other

            if flatArgs.IsEmpty then
                Some(PolarsWrapper.Lit "")
            elif flatArgs.Length = 1 then
                tryTranslateWithContext ctx flatArgs.Head
            else
                let argHandles = flatArgs |> List.choose (tryTranslateWithContext ctx)

                if argHandles.Length = flatArgs.Length then
                    let rustFmt = String.replicate argHandles.Length "{}"
                    Some(PolarsWrapper.FormatString(rustFmt, List.toArray argHandles))
                else
                    None

        // --- 1b. String.Join (Static string.Join with multiple arguments) ---
        | "Join", sepExpr :: rawItems when isNull target && not rawItems.IsEmpty ->
            let flatItems =
                match rawItems with
                | [ single ] ->
                    match unwrap single with
                    | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
                        nae.Expressions |> Seq.toList
                    | _ -> [ single ]
                | other -> other

            let sepStrOpt =
                match tryEvaluate sepExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some(string sep)
                | _ -> None

            match sepStrOpt with
            | Some sep ->
                let itemHandles = flatItems |> List.choose (tryTranslateWithContext ctx)
                if itemHandles.Length = flatItems.Length && not itemHandles.IsEmpty then
                    Some(PolarsWrapper.ConcatString(List.toArray itemHandles, sep, ignoreNulls = false))
                else
                    None
            | None -> None

        // --- 2. String.Format & Interpolated Strings ($"{a} - {b}") ---
        | "Format", _ when isNull target ->
            let formatExprOpt, rawItems =
                match args with
                | providerExpr :: formatExpr :: rest when typeof<IFormatProvider>.IsAssignableFrom(providerExpr.Type) ->
                    let items =
                        match rest with
                        | [ :? NewArrayExpression as nae ] when nae.NodeType = ExpressionType.NewArrayInit ->
                            nae.Expressions |> Seq.toList
                        | other -> other

                    Some formatExpr, items
                | formatExpr :: rest ->
                    let items =
                        match rest with
                        | [ :? NewArrayExpression as nae ] when nae.NodeType = ExpressionType.NewArrayInit ->
                            nae.Expressions |> Seq.toList
                        | other -> other

                    Some formatExpr, items
                | [] -> None, []

            match formatExprOpt with
            | Some formatExpr ->
                match tryEvaluate formatExpr with
                | Some(:? string as netFormat) ->
                    // Matches standard .NET placeholders like {0}, {1}, {0:F2}, etc.
                    let matches = Regex.Matches(netFormat, @"\{(\d+)(?::[^}]+)?\}")

                    let referencedIndices =
                        matches |> Seq.map (fun m -> Int32.Parse(m.Groups.[1].Value)) |> Seq.toList

                    if referencedIndices.IsEmpty then
                        Some(PolarsWrapper.Lit netFormat)
                    elif referencedIndices |> List.forall (fun idx -> idx >= 0 && idx < rawItems.Length) then
                        let orderedExprs = referencedIndices |> List.map (fun idx -> rawItems.[idx])
                        let argHandles = orderedExprs |> List.choose (tryTranslateWithContext ctx)

                        if argHandles.Length = orderedExprs.Length then
                            // Convert {0}, {1:F2} to Polars/Rust {} positional placeholders
                            let rustFmt = Regex.Replace(netFormat, @"\{(\d+)(?::[^}]+)?\}", "{}")
                            Some(PolarsWrapper.FormatString(rustFmt, List.toArray argHandles))
                        else
                            None
                    else
                        None
                | _ -> None
            | None -> None

        | "IsNullOrEmpty", [ arg ] when isNull target ->
            tryTranslateWithContext ctx arg
            |> Option.map (fun exprH ->
                let cloned = PolarsWrapper.CloneExpr exprH
                let isNull = PolarsWrapper.IsNull exprH
                let isEmpty = PolarsWrapper.Eq(cloned, PolarsWrapper.Lit "")
                PolarsWrapper.Or(isNull, isEmpty))

        | "IsNullOrWhiteSpace", [ arg ] when isNull target ->
            tryTranslateWithContext ctx arg
            |> Option.map (fun exprH ->
                let cloned = PolarsWrapper.CloneExpr exprH
                let isNull = PolarsWrapper.IsNull exprH
                let stripped = PolarsWrapper.StrStripChars(cloned, PolarsWrapper.LitNull())
                let isEmpty = PolarsWrapper.Eq(stripped, PolarsWrapper.Lit "")
                PolarsWrapper.Or(isNull, isEmpty))

        // --- Contains / StartsWith / EndsWith / IndexOf ---
        | ("Contains" | "StartsWith" | "EndsWith" | "IndexOf") as opName, arg :: rest when
            rest.IsEmpty || opName <> "Contains" || rest.Length = 1
            ->
            let applyOp (t: ExprHandle) (p: ExprHandle) =
                match opName with
                | "Contains" -> PolarsWrapper.StrContains(t, p, literal = true, strict = false)
                | "StartsWith" -> PolarsWrapper.StrStartsWith(t, p)
                | "EndsWith" -> PolarsWrapper.StrEndsWith(t, p)
                | _ ->
                    let findH = PolarsWrapper.StrFind(t, p, literal = true, strict = false)
                    let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                    PolarsWrapper.FillNull(castedH, PolarsWrapper.Lit -1)

            match rest with
            | [] ->
                match targetH (), tryTranslateWithContext ctx arg with
                | Some t, Some p -> Some(applyOp t p)
                | _ -> None
            | compExpr :: _ ->
                match targetH (), tryTranslateWithContext ctx arg, tryEvaluateCaseInsensitive compExpr with
                | Some t, Some p, Some true ->
                    Some(applyOp (PolarsWrapper.StrToLower t) (PolarsWrapper.StrToLower p))
                | Some t, Some p, Some false -> Some(applyOp t p)
                | _ -> None

        // --- IndexOfAny ---
        | "IndexOfAny", anyOfExpr :: rest ->
            match tryExtractCharSet anyOfExpr with
            | Some chars when chars.Length = 0 ->
                Some(PolarsWrapper.Lit -1)
            | Some chars ->
                let regexPat = buildCharClassRegex chars
                let patH () = PolarsWrapper.Lit regexPat

                match rest with
                | [] ->
                    targetH ()
                    |> Option.map (fun t ->
                        let findH = PolarsWrapper.StrFind(t, patH (), literal = false, strict = false)
                        let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                        PolarsWrapper.FillNull(castedH, PolarsWrapper.Lit -1))

                | [ startExpr ] ->
                    match targetH (), tryTranslateWithContext ctx startExpr with
                    | Some t, Some startH ->
                        let startHInt = tryTranslateCast typeof<int> startH |> Option.defaultValue startH
                        let startClone = PolarsWrapper.CloneExpr startHInt
                        let sliced = PolarsWrapper.StrSlice(t, startHInt, PolarsWrapper.LitNull())
                        let findH = PolarsWrapper.StrFind(sliced, patH (), literal = false, strict = false)
                        let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                        let fullIdx = PolarsWrapper.Add(startClone, castedH)
                        Some(PolarsWrapper.FillNull(fullIdx, PolarsWrapper.Lit -1))
                    | _ -> None

                | [ startExpr; countExpr ] ->
                    match targetH (), tryTranslateWithContext ctx startExpr, tryTranslateWithContext ctx countExpr with
                    | Some t, Some startH, Some countH ->
                        let startHInt = tryTranslateCast typeof<int> startH |> Option.defaultValue startH
                        let startClone = PolarsWrapper.CloneExpr startHInt
                        let countHInt = tryTranslateCast typeof<int> countH |> Option.defaultValue countH
                        let sliced = PolarsWrapper.StrSlice(t, startHInt, countHInt)
                        let findH = PolarsWrapper.StrFind(sliced, patH (), literal = false, strict = false)
                        let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                        let fullIdx = PolarsWrapper.Add(startClone, castedH)
                        Some(PolarsWrapper.FillNull(fullIdx, PolarsWrapper.Lit -1))
                    | _ -> None

                | _ -> None
            | None -> None

        | "ToUpper", []
        | "ToUpperInvariant", [] -> targetH () |> Option.map PolarsWrapper.StrToUpper
        | "ToLower", []
        | "ToLowerInvariant", [] -> targetH () |> Option.map PolarsWrapper.StrToLower
        | "Substring", [ startExpr ] ->
            match targetH (), tryTranslateWithContext ctx startExpr with
            | Some t, Some startH -> Some(PolarsWrapper.StrSlice(t, startH, PolarsWrapper.LitNull()))
            | _ -> None
        | "Substring", [ startExpr; lenExpr ] ->
            match targetH (), tryTranslateWithContext ctx startExpr, tryTranslateWithContext ctx lenExpr with
            | Some t, Some startH, Some lenH -> Some(PolarsWrapper.StrSlice(t, startH, lenH))
            | _ -> None

        // --- Replace ---
        | "Replace", [ oldExpr; newExpr ] ->
            match targetH (), tryTranslateWithContext ctx oldExpr, tryTranslateWithContext ctx newExpr with
            | Some t, Some o, Some n -> Some(PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
            | _ -> None

        | "Replace", [ oldExpr; newExpr; compExpr ] ->
            match targetH (), tryTranslateWithContext ctx newExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some n, Some false ->
                match tryTranslateWithContext ctx oldExpr with
                | Some o -> Some(PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
                | _ -> None
            | Some t, Some n, Some true ->
                match tryEvaluate oldExpr with
                | Some(:? string as oldStr) ->
                    let escapedPattern = sprintf "(?i)%s" (Regex.Escape oldStr)
                    Some(PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit escapedPattern, n, literal = false))
                | _ -> None
            | _ -> None

        // --- PadLeft / PadRight ---
        | "PadLeft", [ widthExpr ] ->
            match targetH (), tryTranslateWithContext ctx widthExpr with
            | Some t, Some w -> Some(PolarsWrapper.StrPadStart(t, w, " "))
            | _ -> None

        | "PadLeft", [ widthExpr; padCharExpr ] ->
            match targetH (), tryTranslateWithContext ctx widthExpr with
            | Some t, Some w ->
                match tryEvaluate padCharExpr with
                | Some(:? char as c) -> Some(PolarsWrapper.StrPadStart(t, w, string c))
                | Some(:? string as s) -> Some(PolarsWrapper.StrPadStart(t, w, s))
                | _ -> None
            | _ -> None

        | "PadRight", [ widthExpr ] ->
            match targetH (), tryTranslateWithContext ctx widthExpr with
            | Some t, Some w -> Some(PolarsWrapper.StrPadEnd(t, w, " "))
            | _ -> None

        | "PadRight", [ widthExpr; padCharExpr ] ->
            match targetH (), tryTranslateWithContext ctx widthExpr with
            | Some t, Some w ->
                match tryEvaluate padCharExpr with
                | Some(:? char as c) -> Some(PolarsWrapper.StrPadEnd(t, w, string c))
                | Some(:? string as s) -> Some(PolarsWrapper.StrPadEnd(t, w, s))
                | _ -> None
            | _ -> None

        // --- Normalize ---
        | "Normalize", [] ->
            targetH () |> Option.map (fun t -> PolarsWrapper.StrNormalize(t, NormalizationForm.FormC))

        | "Normalize", [ formExpr ] ->
            match targetH (), tryEvaluate formExpr with
            | Some t, Some(:? NormalizationForm as form) ->
                Some(PolarsWrapper.StrNormalize(t, form))
            | _ -> None

        // --- Indexer: s[i] (get_Chars) ---
        | "get_Chars", [ idxExpr ] ->
            match targetH (), tryTranslateWithContext ctx idxExpr with
            | Some t, Some idxH ->
                Some(PolarsWrapper.StrSlice(t, idxH, PolarsWrapper.Lit 1))
            | _ -> None

        // --- Split (Literal string.Split) ---
        | "Split", [ sepExpr ] ->
            let sepOpt =
                match tryEvaluate sepExpr with
                | Some(:? char as c) -> Some(string c)
                | Some(:? string as s) -> Some s
                | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                | _ -> None

            match targetH (), sepOpt with
            | Some t, Some s ->
                Some(PolarsWrapper.StrSplit(t, PolarsWrapper.Lit s, inclusive = false, literal = true, strict = false))
            | _ -> None

        | "Split", [ sepExpr; countExpr ]
        | "Split", sepExpr :: countExpr :: _ when args.Length <= 3 ->
            let sepOpt =
                match tryEvaluate sepExpr with
                | Some(:? char as c) -> Some(string c)
                | Some(:? string as s) -> Some s
                | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                | _ -> None

            let countOpt =
                match tryEvaluate countExpr with
                | Some(:? int as n) -> Some n
                | _ -> None

            match targetH (), sepOpt, countOpt with
            | Some t, Some s, Some n ->
                Some(PolarsWrapper.StrSplitN(t, PolarsWrapper.Lit s, n))
            | _ -> None

        // --- Trim / TrimStart / TrimEnd ---
        | ("Trim" | "TrimStart" | "TrimEnd") as trimOp, ([] | [ _ ]) ->
            let stripFn =
                match trimOp with
                | "TrimStart" -> PolarsWrapper.StrStripCharsStart
                | "TrimEnd" -> PolarsWrapper.StrStripCharsEnd
                | _ -> PolarsWrapper.StrStripChars

            match args with
            | [] -> targetH () |> Option.map (fun t -> stripFn (t, PolarsWrapper.LitNull()))
            | [ arg ] ->
                match unwrap arg with
                | :? ConstantExpression as ce when isNull ce.Value ->
                    targetH () |> Option.map (fun t -> stripFn (t, PolarsWrapper.LitNull()))
                | :? NewArrayExpression as nae when nae.Expressions.Count = 0 ->
                    targetH () |> Option.map (fun t -> stripFn (t, PolarsWrapper.LitNull()))
                | other ->
                    let charsOpt =
                        match tryEvaluate other with
                        | Some(:? (char array) as chars) -> Some(PolarsWrapper.Lit(String(chars)))
                        | Some(:? char as c) -> Some(PolarsWrapper.Lit(string c))
                        | Some(:? string as s) -> Some(PolarsWrapper.Lit s)
                        | _ -> tryTranslateWithContext ctx other

                    match targetH (), charsOpt with
                    | Some t, Some p -> Some(stripFn (t, p))
                    | _ -> None
            | _ -> None

        // --- Equals (Static & Instance) ---
        | "Equals", [ aExpr; bExpr ] when isNull target ->
            match tryTranslateWithContext ctx aExpr, tryTranslateWithContext ctx bExpr with
            | Some a, Some b -> Some(PolarsWrapper.Eq(a, b))
            | _ -> None

        | "Equals", [ aExpr; bExpr; compExpr ] when isNull target ->
            match tryTranslateWithContext ctx aExpr, tryTranslateWithContext ctx bExpr, tryEvaluateCaseInsensitive compExpr with
            | Some a, Some b, Some true ->
                Some(PolarsWrapper.Eq(PolarsWrapper.StrToLower a, PolarsWrapper.StrToLower b))
            | Some a, Some b, Some false ->
                Some(PolarsWrapper.Eq(a, b))
            | _ -> None

        | "Equals", [ otherExpr ] ->
            match targetH (), tryTranslateWithContext ctx otherExpr with
            | Some t, Some o -> Some(PolarsWrapper.Eq(t, o))
            | _ -> None

        | "Equals", [ otherExpr; compExpr ] ->
            match targetH (), tryTranslateWithContext ctx otherExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some o, Some true ->
                Some(PolarsWrapper.Eq(PolarsWrapper.StrToLower t, PolarsWrapper.StrToLower o))
            | Some t, Some o, Some false ->
                Some(PolarsWrapper.Eq(t, o))
            | _ -> None

        // --- CompareTo ---
        | "CompareTo", [ otherExpr ] ->
            match targetH (), tryTranslateWithContext ctx otherExpr with
            | Some t, Some o ->
                let tClone1 = PolarsWrapper.CloneExpr t
                let oClone1 = PolarsWrapper.CloneExpr o
                let gtCond = PolarsWrapper.Gt(t, o)
                let ltCond = PolarsWrapper.Lt(tClone1, oClone1)
                Some(PolarsWrapper.IfElse(gtCond, PolarsWrapper.Lit 1, PolarsWrapper.IfElse(ltCond, PolarsWrapper.Lit -1, PolarsWrapper.Lit 0)))
            | _ -> None

        // --- Remove ---
        | "Remove", [ startExpr ] ->
            match targetH (), tryTranslateWithContext ctx startExpr with
            | Some t, Some startH ->
                Some(PolarsWrapper.StrSlice(t, PolarsWrapper.Lit 0, startH))
            | _ -> None

        | "Remove", [ startExpr; countExpr ] ->
            match targetH (), tryTranslateWithContext ctx startExpr, tryTranslateWithContext ctx countExpr with
            | Some t, Some startH, Some countH ->
                let tClone = PolarsWrapper.CloneExpr t
                let startClone = PolarsWrapper.CloneExpr startH
                let prefix = PolarsWrapper.StrSlice(t, PolarsWrapper.Lit 0, startH)
                let suffixOffset = PolarsWrapper.Add(startClone, countH)
                let suffix = PolarsWrapper.StrSlice(tClone, suffixOffset, PolarsWrapper.LitNull())
                Some(PolarsWrapper.FormatString("{}{}", [| prefix; suffix |]))
            | _ -> None

        | _ -> None

    let private translateTemporalMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target

        match m.Name, args with
        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            match tryTranslateWithContext ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                let ambiguousExpr = PolarsWrapper.Lit "raise"

                Some(
                    PolarsWrapper.StrToDatetime(
                        e = strH,
                        unit = PlTimeUnit.Microseconds,
                        timeZone = null,
                        format = chronoFormat,
                        strict = false,
                        exact = true,
                        cache = true,
                        ambiguous = ambiguousExpr
                    )
                )
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            tryTranslateWithContext ctx strExpr
            |> Option.map (fun strH ->
                let ambiguousExpr = PolarsWrapper.Lit "raise"

                PolarsWrapper.StrToDatetime(
                    e = strH,
                    unit = PlTimeUnit.Microseconds,
                    timeZone = null,
                    format = null,
                    strict = false,
                    exact = false,
                    cache = true,
                    ambiguous = ambiguousExpr
                ))

        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateOnly> ->
            match tryTranslateWithContext ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                Some(PolarsWrapper.StrToDate(strH, chronoFormat, strict = false, exact = true, cache = true))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<DateOnly> ->
            tryTranslateWithContext ctx strExpr
            |> Option.map (fun strH ->
                PolarsWrapper.StrToDate(strH, format = null, strict = false, exact = false, cache = true))

        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<TimeOnly> ->
            match tryTranslateWithContext ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                Some(PolarsWrapper.StrToTime(strH, chronoFormat, strict = false, exact = true, cache = true))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<TimeOnly> ->
            tryTranslateWithContext ctx strExpr
            |> Option.map (fun strH ->
                PolarsWrapper.StrToTime(strH, format = null, strict = false, exact = true, cache = true))

        | "IsLeapYear", [ yearExpr ] when isNull target && m.DeclaringType = typeof<DateTime> ->
            tryTranslateWithContext ctx yearExpr
            |> Option.map (fun yearH ->
                let dateStrExpr = PolarsWrapper.FormatString("{}-01-01", [| yearH |])

                let dtExpr =
                    PolarsWrapper.StrToDate(dateStrExpr, "%Y-%m-%d", strict = false, exact = true, cache = true)

                PolarsWrapper.DtIsLeapYear dtExpr)

        | "ToString", [ formatExpr ] ->
            match targetH (), tryEvaluate formatExpr with
            | Some t, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat

                if not (String.IsNullOrEmpty chronoFormat) then
                    Some(PolarsWrapper.DtToString(t, chronoFormat))
                else
                    None
            | _ -> None

        | ("AddDays" | "AddHours" | "AddMinutes" | "AddSeconds") as addOp, [ offsetExpr ] ->
            let unitSuffix =
                match addOp with
                | "AddDays" -> "d"
                | "AddHours" -> "h"
                | "AddMinutes" -> "m"
                | _ -> "s"

            match targetH (), tryEvaluate offsetExpr with
            | Some t, Some v ->
                Some(PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit(sprintf "%g%s" (Convert.ToDouble v) unitSuffix)))
            | _ -> None
        | _ -> None

    // =========================================================================
    // 5. Numeric & Math Methods (Math, MathF, System.Decimal)
    // =========================================================================

    let private tryMapRoundMode (midpoint: MidpointRounding) : PlRoundMode option =
        match midpoint with
        | MidpointRounding.ToEven -> Some PlRoundMode.HalfToEven
        | MidpointRounding.AwayFromZero -> Some PlRoundMode.HalfAwayFromZero
        | MidpointRounding.ToZero -> Some PlRoundMode.ToZero
        | _ -> None

    let internal isNumericDeclaringType (t: Type) =
        not (isNull t)
        && (t = typeof<Math>
            || t = typeof<MathF>
            || t = typeof<decimal>
            || t = typeof<System.Numerics.BitOperations>)

    let internal translateNumericMethodWithTranslator
        (translateExpr: Expression -> ExprHandle option)
        (m: MethodInfo)
        (args: Expression list)
        : ExprHandle option =
        let t1 expr = translateExpr expr

        let t2 e1 e2 =
            match translateExpr e1, translateExpr e2 with
            | Some l, Some r -> Some(l, r)
            | _ -> None

        match m.Name, args with
        | "Abs", [ x ] -> t1 x |> Option.map PolarsWrapper.Abs
        | "Ceiling", [ x ] -> t1 x |> Option.map PolarsWrapper.Ceil
        | "Floor", [ x ] -> t1 x |> Option.map PolarsWrapper.Floor
        | "Truncate", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Truncate(h, 0u))
        | "Sign", [ x ] -> t1 x |> Option.map PolarsWrapper.Sign

        | "Round", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Round(h, 0u, PlRoundMode.HalfToEven))

        | "Round", [ x; digitsExpr ] when digitsExpr.Type = typeof<int> || digitsExpr.Type = typeof<uint32> ->
            match t1 x, tryEvaluate digitsExpr with
            | Some h, Some d ->
                let decimals = uint32 (Convert.ToInt32 d)
                Some(PolarsWrapper.Round(h, decimals, PlRoundMode.HalfToEven))
            | _ -> None

        | "Round", [ x; modeExpr ] when modeExpr.Type = typeof<MidpointRounding> ->
            match t1 x, tryEvaluate modeExpr with
            | Some h, Some(:? MidpointRounding as mode) ->
                tryMapRoundMode mode
                |> Option.map (fun plMode -> PolarsWrapper.Round(h, 0u, plMode))
            | _ -> None

        | "Round", [ x; digitsExpr; modeExpr ] ->
            match t1 x, tryEvaluate digitsExpr, tryEvaluate modeExpr with
            | Some h, Some d, Some(:? MidpointRounding as mode) ->
                let decimals = uint32 (Convert.ToInt32 d)

                tryMapRoundMode mode
                |> Option.map (fun plMode -> PolarsWrapper.Round(h, decimals, plMode))
            | _ -> None

        | "Min", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = PolarsWrapper.LtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                PolarsWrapper.IfElse(cond, l, r))

        | "Max", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = PolarsWrapper.GtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                PolarsWrapper.IfElse(cond, l, r))

        | "Clamp", [ valExpr; minExpr; maxExpr ] ->
            match t1 valExpr, t1 minExpr, t1 maxExpr with
            | Some v, Some minH, Some maxH -> Some(PolarsWrapper.Clip(v, minH, maxH))
            | _ -> None

        | "Sqrt", [ x ] -> t1 x |> Option.map PolarsWrapper.Sqrt
        | "Cbrt", [ x ] -> t1 x |> Option.map PolarsWrapper.Cbrt
        | "Exp", [ x ] -> t1 x |> Option.map PolarsWrapper.Exp
        | "Log", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit Math.E))
        | "Log10", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 10.0))
        | "Log2", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 2.0))
        | "Log", [ x; newBase ] -> t2 x newBase |> Option.map PolarsWrapper.Log

        | "Sin", [ x ] -> t1 x |> Option.map PolarsWrapper.Sin
        | "Cos", [ x ] -> t1 x |> Option.map PolarsWrapper.Cos
        | "Tan", [ x ] -> t1 x |> Option.map PolarsWrapper.Tan
        | "Asin", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcSin
        | "Acos", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcCos
        | "Atan", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcTan
        | "Sinh", [ x ] -> t1 x |> Option.map PolarsWrapper.Sinh
        | "Cosh", [ x ] -> t1 x |> Option.map PolarsWrapper.Cosh
        | "Tanh", [ x ] -> t1 x |> Option.map PolarsWrapper.Tanh
        | "Asinh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcSinh
        | "Acosh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcCosh
        | "Atanh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcTanh

        | "Pow", [ x; y ] -> t2 x y |> Option.map PolarsWrapper.Pow
        | "Atan2", [ y; x ] -> t2 y x |> Option.map PolarsWrapper.ArcTan2

        | "ToInt32", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<int>)
        | "ToUInt32", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<uint32>)
        | "ToInt64", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<int64>)
        | "ToUInt64", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<uint64>)
        | "ToSingle", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<float32>)
        | "ToDouble", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<double>)
        | "ToByte", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<byte>)
        | "ToSByte", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<sbyte>)
        | "ToInt16", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<int16>)
        | "ToUInt16", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<uint16>)

        | "PopCount", [ x ] -> t1 x |> Option.map PolarsWrapper.BitwiseCountOnes

        | "LeadingZeroCount", [ x ] -> t1 x |> Option.map PolarsWrapper.BitwiseLeadingZeros

        | "TrailingZeroCount", [ x ] -> t1 x |> Option.map PolarsWrapper.BitwiseTrailingZeros

        | _ -> None

    let private translateNumericMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (args: Expression list)
        : ExprHandle option =
        translateNumericMethodWithTranslator (tryTranslateWithContext ctx) m args

    // =========================================================================
    // 6. Regex Dispatcher (Native Inline Flag Pushdown & UDF Fallback)
    // =========================================================================

    /// Synthesizes Rust-compatible inline flag expressions from .NET RegexOptions.
    /// Returns Some(validPattern) if compatible with Rust regex engine, otherwise None.
    let private tryBuildRustRegexPattern (pat: string) (options: RegexOptions) : string option =
        // Options that alter regex execution semantics unsupported natively by Rust regex
        let unsupported =
            RegexOptions.RightToLeft
            ||| RegexOptions.ECMAScript
            ||| RegexOptions.ExplicitCapture

        if options.HasFlag unsupported then
            None
        else
            let mutable flags = ""

            if options.HasFlag RegexOptions.IgnoreCase then
                flags <- flags + "i"

            if options.HasFlag RegexOptions.Multiline then
                flags <- flags + "m"

            if options.HasFlag RegexOptions.Singleline then
                flags <- flags + "s"

            if options.HasFlag RegexOptions.IgnorePatternWhitespace then
                flags <- flags + "x"

            let candidate =
                if String.IsNullOrEmpty flags then
                    pat
                else
                    sprintf "(?%s:%s)" flags pat

            // Validate that Rust's regex compiler can parse the candidate (e.g. rejects lookaround)
            if PolarsWrapper.RegexIsValid candidate then
                Some candidate
            else
                None

    let private translateRegexInvocation (ctx: TranslationContext) (invocation: RegexInvocation) : ExprHandle option =
        let getRegex rxOpt pat =
            match rxOpt with
            | Some(rx: Regex) -> rx
            | None -> Regex pat

        let getOptions rxOpt =
            match rxOpt with
            | Some(rx: Regex) -> rx.Options
            | None -> RegexOptions.None

        match invocation with
        // --- 1. IsMatch ---
        | IsMatch(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrContains(t, PolarsWrapper.Lit validPat, literal = false, strict = false)
                | None ->
                    let rx = getRegex rxOpt pat
                    PolarsWrapper.MapStringPredicate(t, Func<string, bool>(rx.IsMatch)))

        // --- 2. ReplaceLiteral ---
        | ReplaceLiteral(target, pat, rep, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit validPat, PolarsWrapper.Lit rep, literal = false)
                | None ->
                    let rx = getRegex rxOpt pat

                    PolarsWrapper.MapStringTransform(
                        t,
                        Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep))
                    ))

        // --- 2b. ReplaceLiteralN (With count limit) ---
        | ReplaceLiteralN(target, pat, rep, count, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrReplace(
                        t,
                        PolarsWrapper.Lit validPat,
                        PolarsWrapper.Lit rep,
                        literal = false,
                        n = int64 count
                    )
                | None ->
                    let rx = getRegex rxOpt pat

                    PolarsWrapper.MapStringTransform(
                        t,
                        Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep, count))
                    ))

        // --- 3. ReplaceEvaluator (Managed delegate: always UDF) ---
        | ReplaceEvaluator(target, pat, eval, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let rx = getRegex rxOpt pat

                PolarsWrapper.MapStringTransform(
                    t,
                    Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, eval))
                ))

        // --- 4. MatchValue & Groups[i].Value ---
        | MatchValue(target, pat, groupIndex, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt
                let rx = getRegex rxOpt pat

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    // Rust Native StrExtract returns Null when not matched.
                    // To conform to BCL Match.Value / Group.Value semantics, coalesce Null to empty string (""),
                    // while preserving genuine Null input if target was originally Null.
                    let tCloned = PolarsWrapper.CloneExpr t
                    let extracted = PolarsWrapper.StrExtract(t, PolarsWrapper.Lit validPat, groupIndex)
                    let nonNullExtracted = PolarsWrapper.Coalesce [| extracted; PolarsWrapper.Lit "" |]
                    let isTargetNull = PolarsWrapper.IsNull tCloned
                    PolarsWrapper.IfElse(isTargetNull, PolarsWrapper.LitNull(), nonNullExtracted)
                | None ->
                    // Managed Arrow UDF fallback
                    PolarsWrapper.MapStringTransform(
                        t,
                        Func<string, string>(fun s ->
                            if isNull s then
                                null
                            else
                                let m = rx.Match(s)

                                if m.Success && groupIndex < m.Groups.Count then
                                    m.Groups.[groupIndex].Value
                                else
                                    "")
                    ))

        // --- 4b. MatchGroupNamed (Regex.Match().Groups["name"].Value -> StrExtractGroups) ---
        | MatchGroupNamed(target, pat, groupName, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt
                let rx = getRegex rxOpt pat

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    let tCloned = PolarsWrapper.CloneExpr t
                    let structH = PolarsWrapper.StrExtractGroups(t, validPat)
                    let fieldH = PolarsWrapper.StructFieldByName(structH, [| groupName |])
                    let nonNullExtracted = PolarsWrapper.Coalesce [| fieldH; PolarsWrapper.Lit "" |]
                    let isTargetNull = PolarsWrapper.IsNull tCloned
                    PolarsWrapper.IfElse(isTargetNull, PolarsWrapper.LitNull(), nonNullExtracted)
                | None ->
                    PolarsWrapper.MapStringTransform(
                        t,
                        Func<string, string>(fun s ->
                            if isNull s then null
                            else
                                let m = rx.Match s
                                if m.Success then m.Groups.[groupName].Value else "")
                    ))

        // --- 5. MatchesAll ---
        | MatchesAll(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat -> PolarsWrapper.StrExtractAll(t, PolarsWrapper.Lit validPat)
                | None ->
                    let rx = getRegex rxOpt pat

                    PolarsWrapper.MapStringToList(
                        t,
                        Func<string, Collections.Generic.IEnumerable<string>>(fun s ->
                            if isNull s then
                                null
                            else
                                let matches = rx.Matches(s)
                                let list = Collections.Generic.List<string>(matches.Count)

                                for m in matches do
                                    list.Add(m.Value)

                                list :> Collections.Generic.IEnumerable<string>)
                    ))

        // --- 6. MatchesCount / Regex.Count (.NET 7+) ---
        | MatchesCount(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrCountMatches(t, PolarsWrapper.Lit validPat, literal = false)
                | None ->
                    let rx = getRegex rxOpt pat
                    let intDtype = PolarsWrapper.NewPrimitiveType(int PlDataType.Int32)

                    PolarsWrapper.Map(
                        t,
                        Func<Apache.Arrow.IArrowArray, Apache.Arrow.IArrowArray>(fun arr ->
                            let strViewArr = arr :?> Apache.Arrow.StringViewArray
                            let builder = Apache.Arrow.Int32Array.Builder()

                            for i in 0 .. strViewArr.Length - 1 do
                                if strViewArr.IsNull(i) then
                                    builder.AppendNull() |> ignore
                                else
                                    builder.Append(rx.Matches(strViewArr.GetString(i)).Count) |> ignore

                            builder.Build() :> Apache.Arrow.IArrowArray),
                        intDtype
                    ))

        // --- 7. Split ---
        | Split(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrSplit(
                        t,
                        PolarsWrapper.Lit validPat,
                        inclusive = false,
                        literal = false,
                        strict = false
                    )
                | None ->
                    let rx = getRegex rxOpt pat

                    PolarsWrapper.MapStringToList(
                        t,
                        Func<string, seq<string>>(fun s -> if isNull s then null else rx.Split(s) :> seq<string>)
                    ))

    // =========================================================================
    // 7. Window, Shift, Diff & Rolling Operations
    // =========================================================================

    let private translateShiftMethod
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target

        let resolveOffset (expr: Expression) : ExprHandle option =
            match tryEvaluate expr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(PolarsWrapper.Lit intVal)
                with _ ->
                    tryTranslateWithContext ctx expr
            | None ->
                tryTranslateWithContext ctx expr

        let offsetHandleOpt =
            match args with
            | [] -> Some(PolarsWrapper.Lit 1L)
            | [ nExpr ] -> resolveOffset nExpr
            | _ -> None

        match targetH (), offsetHandleOpt with
        | Some eH, Some nH -> Some(PolarsWrapper.Shift(eH, nH))
        | _ -> None

    let private translateDiffMethod
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target

        let resolveN nExpr =
            match tryEvaluate nExpr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(PolarsWrapper.Lit intVal)
                with _ ->
                    tryTranslateWithContext ctx nExpr
            | None -> tryTranslateWithContext ctx nExpr

        match args with
        | [] ->
            targetH ()
            |> Option.map (fun eH -> PolarsWrapper.Diff(eH, PolarsWrapper.Lit 1L, PlNullBehavior.Ignore))
        | [ nExpr ] ->
            match targetH (), resolveN nExpr with
            | Some eH, Some nH -> Some(PolarsWrapper.Diff(eH, nH, PlNullBehavior.Ignore))
            | _ -> None
        | [ nExpr; nbExpr ] ->
            match targetH (), resolveN nExpr, tryEvaluate nbExpr with
            | Some eH, Some nH, Some(:? PlNullBehavior as nb) -> Some(PolarsWrapper.Diff(eH, nH, nb))
            | _ -> None
        | _ -> None

    let private tryExtractRankMethod (value: obj) : PlRankMethod =
        match value with
        | :? PlRankMethod as pm -> pm
        | null -> PlRankMethod.Average
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlRankMethod> byteVal
            with _ ->
                PlRankMethod.Average

    let private translateRankMethod
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target

        match args with
        | [] ->
            targetH ()
            |> Option.map (fun eH -> PolarsWrapper.Rank(eH, PlRankMethod.Average, false, Nullable()))

        | [ descExpr ] when descExpr.Type = typeof<bool> ->
            match targetH (), tryEvaluate descExpr with
            | Some eH, Some(:? bool as desc) -> Some(PolarsWrapper.Rank(eH, PlRankMethod.Average, desc, Nullable()))
            | _ -> None

        | [ methodExpr ] ->
            match targetH (), tryEvaluate methodExpr with
            | Some eH, Some mObj ->
                let m = tryExtractRankMethod mObj
                Some(PolarsWrapper.Rank(eH, m, false, Nullable()))
            | _ -> None

        | [ methodExpr; descExpr ] ->
            match targetH (), tryEvaluate methodExpr, tryEvaluate descExpr with
            | Some eH, Some mObj, Some(:? bool as desc) ->
                let m = tryExtractRankMethod mObj
                Some(PolarsWrapper.Rank(eH, m, desc, Nullable()))
            | _ -> None

        | [ methodExpr; descExpr; seedExpr ] ->
            match targetH (), tryEvaluate methodExpr, tryEvaluate descExpr, tryEvaluate seedExpr with
            | Some eH, Some mObj, Some(:? bool as desc), Some seedObj ->
                let m = tryExtractRankMethod mObj

                let seedNullable =
                    match seedObj with
                    | :? uint64 as s -> Nullable<uint64>(s)
                    | _ -> Nullable<uint64>()

                Some(PolarsWrapper.Rank(eH, m, desc, seedNullable))
            | _ -> None

        | _ -> None

    let private translatePctChangeMethod
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target

        match args with
        | [] -> targetH () |> Option.map (fun eH -> PolarsWrapper.PctChange(eH, 1L))
        | [ nExpr ] ->
            match targetH (), tryEvaluate nExpr with
            | Some eH, Some nVal -> Some(PolarsWrapper.PctChange(eH, Convert.ToInt64 nVal))
            | _ -> None
        | _ -> None

    let private translateOverMethod
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        match tryTranslateWithContext ctx target with
        | None -> None
        | Some targetH ->
            let unwrapArg (arg: Expression) : Expression =
                match unwrap arg with
                | null -> null
                | other -> other

            let extractList (arg: Expression) : Expression list =
                match unwrapArg arg with
                | null -> []
                | :? NewExpression as ne -> ne.Arguments |> Seq.toList
                | :? NewArrayExpression as nae -> nae.Expressions |> Seq.toList
                | :? MemberInitExpression as mie ->
                    mie.Bindings
                    |> Seq.choose (function :? MemberAssignment as ma -> Some ma.Expression | _ -> None)
                    |> Seq.toList
                | other -> [ other ]

            let translateList (exprs: Expression list) : ExprHandle array option =
                if exprs.IsEmpty then
                    Some [||]
                else
                    let handles =
                        exprs
                        |> List.choose (fun arg ->
                            match arg with
                            | :? LambdaExpression as lam ->
                                tryTranslateWithContext (TranslationContext.Entity lam.Parameters.[0].Name) lam.Body
                            | other -> tryTranslateWithContext ctx other)
                    if handles.Length = exprs.Length then
                        Some(Array.ofList handles)
                    else
                        None

            let rec isOrderSensitive (e: Expression) : bool =
                match unwrap e with
                | null -> false
                | :? MethodCallExpression as mc ->
                    match mc.Method.Name with
                    | "Shift" | "Diff" | "PctChange"
                    | "CumSum" | "CumMax" | "CumMin" | "CumProd" | "CumCount"
                    | "Interpolate" | "InterpolateBy" -> true
                    | _ -> false
                | :? UnaryExpression as ue -> isOrderSensitive ue.Operand
                | _ -> false

            // Identify partitions, order by, and descending
            let partitionExprs, orderExprs, isDescending =
                match args with
                | [ pArg ] ->
                    extractList pArg, [], false
                | [ pArg; oArg ] when oArg.Type = typeof<bool> ->
                    let desc = match tryEvaluate oArg with Some (:? bool as b) -> b | _ -> false
                    extractList pArg, [], desc
                | [ pArg; oArg ] ->
                    if isOrderSensitive target then
                        extractList pArg, extractList oArg, false
                    else
                        extractList pArg @ extractList oArg, [], false
                | pArg :: oArg :: descArg :: _ when descArg.Type = typeof<bool> ->
                    let desc = match tryEvaluate descArg with Some (:? bool as b) -> b | _ -> false
                    if desc || isOrderSensitive target then
                        extractList pArg, extractList oArg, desc
                    else
                        extractList pArg @ extractList oArg, [], false
                | other ->
                    other |> List.collect extractList, [], false

            match translateList partitionExprs, translateList orderExprs with
            | Some partitionHandles, Some orderHandles when partitionHandles.Length > 0 || orderHandles.Length > 0 ->
                Some(
                    PolarsWrapper.Over(
                        targetH,
                        partitionHandles,
                        orderHandles,
                        descending = isDescending,
                        nullsLast = false,
                        multithreaded = true,
                        maintainOrder = true,
                        mappingCode = PlWindowMapping.GroupsToRows
                    )
                )
            | _ -> None

    let private translateScalarReductionMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | ("Sum" | "Mean" | "Average" | "Min" | "Max" | "Median" | "First" | "Last"), [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.bind (translateReductionOp m.Name)

        | "Count", [] -> tryTranslateWithContext ctx effectiveTarget |> Option.map PolarsWrapper.Count

        | ("Std" | "StdDev"), [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.bind (translateDispersionOp m.Name 1uy)

        | ("Var" | "Variance"), [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.bind (translateDispersionOp m.Name 1uy)

        | ("Std" | "StdDev" | "Var" | "Variance"), [ ddofExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate ddofExpr with
            | Some colH, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal
                translateDispersionOp m.Name ddof colH
            | _ -> None

        | _ -> None

    let private translateCumulativeMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        let reverse =
            match effectiveArgs with
            | [ revExpr ] ->
                match tryEvaluate revExpr with
                | Some(:? bool as r) -> r
                | _ -> false
            | _ -> false

        tryTranslateWithContext ctx effectiveTarget
        |> Option.bind (fun colH ->
            match m.Name with
            | "CumSum" -> Some(PolarsWrapper.CumSum(colH, reverse))
            | "CumMax" -> Some(PolarsWrapper.CumMax(colH, reverse))
            | "CumMin" -> Some(PolarsWrapper.CumMin(colH, reverse))
            | "CumProd" -> Some(PolarsWrapper.CumProd(colH, reverse))
            | "CumCount" -> Some(PolarsWrapper.CumCount(colH, reverse))
            | _ -> None)

    let private tryExtractInterpolationMethod (value: obj) : PlInterpolationMethod =
        match value with
        | :? PlInterpolationMethod as pm -> pm
        | null -> PlInterpolationMethod.Linear
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlInterpolationMethod> byteVal
            with _ ->
                PlInterpolationMethod.Linear

    let private translateInterpolateMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | "Interpolate", [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.map (fun h -> PolarsWrapper.Interpolate(h, PlInterpolationMethod.Linear))

        | "Interpolate", [ methodExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate methodExpr with
            | Some targetH, Some methodVal ->
                let method = tryExtractInterpolationMethod methodVal
                Some(PolarsWrapper.Interpolate(targetH, method))
            | Some targetH, None -> Some(PolarsWrapper.Interpolate(targetH, PlInterpolationMethod.Linear))
            | _ -> None

        | "InterpolateBy", [ byExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryTranslateWithContext ctx byExpr with
            | Some targetH, Some byH -> Some(PolarsWrapper.InterpolateBy(targetH, byH))
            | _ -> None

        | _ -> None

    let internal tryExtractQuantileMethod (value: obj) : PlQuantileMethod =
        match value with
        | :? PlQuantileMethod as pm -> pm
        | null -> PlQuantileMethod.Nearest
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlQuantileMethod> byteVal
            with _ ->
                PlQuantileMethod.Nearest

    let internal translateQuantileMethod
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match effectiveArgs with
        | [ qExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate qExpr with
            | Some targetH, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some(PolarsWrapper.Quantile(targetH, q, PlQuantileMethod.Nearest))
                with _ ->
                    None
            | _ -> None

        | [ qExpr; methodExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate qExpr, tryEvaluate methodExpr with
            | Some targetH, Some qVal, Some mVal ->
                try
                    let q = Convert.ToDouble qVal
                    let method = tryExtractQuantileMethod mVal
                    Some(PolarsWrapper.Quantile(targetH, q, method))
                with _ ->
                    None
            | _ -> None

        | _ -> None

    let private isIntegerType (t: Type) =
        t = typeof<int>
        || t = typeof<int64>
        || t = typeof<int16>
        || t = typeof<sbyte>
        || t = typeof<byte>
        || t = typeof<uint32>
        || t = typeof<uint64>
        || t = typeof<uint16>

    let private translateConversionOrParseMethod
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetType = m.DeclaringType

        // 1. Static Parse on numeric types (int.Parse(s), long.Parse(s), decimal.Parse(s), double.Parse(s), etc.)
        if isNull target && (m.Name = "Parse" || m.Name = "TryParse") then
            match args with
            | strExpr :: _ when strExpr.Type = typeof<string> ->
                match tryTranslateWithContext ctx strExpr with
                | Some strH ->
                    if isIntegerType targetType then
                        tryGetPolarsDataType targetType
                        |> Option.map (fun dtype ->
                            PolarsWrapper.StrToInteger(strH, PolarsWrapper.Lit 10, dtype, strict = false))
                    elif targetType = typeof<decimal> then
                        Some(PolarsWrapper.StrToDecimal(strH, 4))
                    elif targetType = typeof<double> || targetType = typeof<float> || targetType = typeof<bool> then
                        tryTranslateCast targetType strH
                    else
                        None
                | None -> None
            | _ -> None

        // 2. Convert.* (Convert.ToInt32, Convert.ToHexString, Convert.FromHexString, etc.)
        elif isNull target && targetType = typeof<Convert> then
            match m.Name, args with
            | "FromHexString", [ strExpr ] ->
                tryTranslateWithContext ctx strExpr
                |> Option.map (fun strH -> PolarsWrapper.StrHexDecode(strH, strict = false))

            | "FromBase64String", [ strExpr ] ->
                tryTranslateWithContext ctx strExpr
                |> Option.map (fun strH -> PolarsWrapper.StrBase64Decode(strH, strict = false))

            | "ToHexString", [ strExpr ] ->
                tryTranslateWithContext ctx strExpr
                |> Option.map PolarsWrapper.StrHexEncode

            | "ToBase64String", [ strExpr ] ->
                tryTranslateWithContext ctx strExpr
                |> Option.map PolarsWrapper.StrBase64Encode

            // Convert.ToInt32(s), Convert.ToInt64(s), etc.
            | ("ToInt32" | "ToInt64" | "ToInt16" | "ToByte" | "ToSByte" | "ToUInt32" | "ToUInt64" | "ToUInt16") as convName, [ strExpr ] when strExpr.Type = typeof<string> ->
                let intType =
                    match convName with
                    | "ToInt32" -> typeof<int>
                    | "ToInt64" -> typeof<int64>
                    | "ToInt16" -> typeof<int16>
                    | "ToByte" -> typeof<byte>
                    | "ToSByte" -> typeof<sbyte>
                    | "ToUInt32" -> typeof<uint32>
                    | "ToUInt64" -> typeof<uint64>
                    | "ToUInt16" -> typeof<uint16>
                    | _ -> typeof<int>

                match tryTranslateWithContext ctx strExpr, tryGetPolarsDataType intType with
                | Some strH, Some dtype ->
                    Some(PolarsWrapper.StrToInteger(strH, PolarsWrapper.Lit 10, dtype, strict = false))
                | _ -> None

            // Convert.ToInt32(s, fromBase), Convert.ToInt64(s, fromBase)
            | ("ToInt32" | "ToInt64") as convName, [ strExpr; baseExpr ] when strExpr.Type = typeof<string> ->
                let intType = if convName = "ToInt32" then typeof<int> else typeof<int64>
                match tryTranslateWithContext ctx strExpr, tryTranslateWithContext ctx baseExpr, tryGetPolarsDataType intType with
                | Some strH, Some baseH, Some dtype ->
                    Some(PolarsWrapper.StrToInteger(strH, baseH, dtype, strict = false))
                | _ -> None

            | "ToDecimal", [ strExpr ] when strExpr.Type = typeof<string> ->
                tryTranslateWithContext ctx strExpr
                |> Option.map (fun strH -> PolarsWrapper.StrToDecimal(strH, 4))

            | "ToDouble", [ strExpr ] when strExpr.Type = typeof<string> ->
                tryTranslateWithContext ctx strExpr
                |> Option.bind (tryTranslateCast typeof<double>)

            | "ToSingle", [ strExpr ] when strExpr.Type = typeof<string> ->
                tryTranslateWithContext ctx strExpr
                |> Option.bind (tryTranslateCast typeof<float>)

            | "ToBoolean", [ strExpr ] when strExpr.Type = typeof<string> ->
                tryTranslateWithContext ctx strExpr
                |> Option.bind (tryTranslateCast typeof<bool>)

            | _ -> None

        else
            None

    let private translateMethodCall
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && args.Length > 0 then
                args.Head, args.Tail
            else
                target, args

        if (m.Name = "IsNull" || m.Name = "isNull") && args.Length = 1 then
            tryTranslateWithContext ctx args.Head
            |> Option.map PolarsWrapper.IsNull
        elif not (isNull effectiveTarget) && m.Name = "Shift" then
            translateShiftMethod ctx effectiveTarget effectiveArgs
        // Transparently unwrap client-side sequence materialization methods (ToArray, ToList, AsEnumerable)
        elif
            (m.Name = "ToArray" || m.Name = "ToList" || m.Name = "AsEnumerable")
            && effectiveArgs.IsEmpty
            && not (isNull effectiveTarget)
        then
            tryTranslateWithContext ctx effectiveTarget

        elif not (isNull effectiveTarget) && m.Name = "Diff" then
            translateDiffMethod ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "Over" then
            translateOverMethod ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "Rank" then
            translateRankMethod ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "PctChange" then
            translatePctChangeMethod ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "Quantile" then
            translateQuantileMethod ctx effectiveTarget effectiveArgs

        elif
            not (isNull effectiveTarget)
            && (m.Name = "Interpolate" || m.Name = "InterpolateBy")
        then
            translateInterpolateMethod ctx m effectiveTarget effectiveArgs

        elif
            not (isNull effectiveTarget)
            && (m.Name = "CumSum"
                || m.Name = "CumMax"
                || m.Name = "CumMin"
                || m.Name = "CumProd"
                || m.Name = "CumCount")
        then
            translateCumulativeMethod ctx m effectiveTarget effectiveArgs
        // Regex.Escape(str)
        elif m.DeclaringType = typeof<Regex> && isNull target && m.Name = "Escape" then
            match args with
            | [ strExpr ] ->
                tryTranslateWithContext ctx strExpr |> Option.map PolarsWrapper.StrEscapeRegex
            | _ -> None

        // 1. Conversion or Parse methods (int.Parse, Convert.ToInt32, Convert.FromHexString, etc.)
        elif
            m.DeclaringType = typeof<Convert>
            || (isNull target && (m.Name = "Parse" || m.Name = "TryParse"))
        then
            match translateConversionOrParseMethod ctx m target args with
            | Some h -> Some h
            | None ->
                if m.DeclaringType = typeof<DateTime> || m.DeclaringType = typeof<DateOnly> || m.DeclaringType = typeof<TimeOnly> then
                    translateTemporalMethod ctx m target args
                else
                    None

        // TextInfo.ToTitleCase(s)
        elif m.Name = "ToTitleCase" && args.Length >= 1 then
            match args |> List.tryFind (fun a -> a.Type = typeof<string>) with
            | Some strExpr ->
                tryTranslateWithContext ctx strExpr |> Option.map PolarsWrapper.StrToTitlecase
            | None -> None

        // Encoding.UTF8.GetByteCount(s)
        elif not (isNull m.DeclaringType) && typeof<Encoding>.IsAssignableFrom m.DeclaringType && m.Name = "GetByteCount" then
            match args with
            | [ strExpr ] when strExpr.Type = typeof<string> ->
                tryTranslateWithContext ctx strExpr |> Option.map PolarsWrapper.StrLenBytes
            | _ -> None

        // Enumerable string extensions: s.Take(n), s.TakeLast(n), s.Reverse()
        elif m.DeclaringType = typeof<Enumerable> && isNull target then
            match m.Name, args with
            | "Take", [ strExpr; countExpr ] when strExpr.Type = typeof<string> ->
                match tryTranslateWithContext ctx strExpr, tryTranslateWithContext ctx countExpr with
                | Some strH, Some countH -> Some(PolarsWrapper.StrHead(strH, countH))
                | _ -> None

            | "TakeLast", [ strExpr; countExpr ] when strExpr.Type = typeof<string> ->
                match tryTranslateWithContext ctx strExpr, tryTranslateWithContext ctx countExpr with
                | Some strH, Some countH -> Some(PolarsWrapper.StrTail(strH, countH))
                | _ -> None

            | "Reverse", [ strExpr ] when strExpr.Type = typeof<string> ->
                tryTranslateWithContext ctx strExpr |> Option.map PolarsWrapper.StrReverse

            | _ -> None

        // Microsoft.VisualBasic.Strings.StrReverse(s) or similar
        elif m.Name = "StrReverse" && isNull target && args.Length = 1 && args.[0].Type = typeof<string> then
            tryTranslateWithContext ctx args.[0] |> Option.map PolarsWrapper.StrReverse

        // char.ToUpper(c) / char.ToLower(c)
        elif m.DeclaringType = typeof<char> && isNull target then
            match m.Name, args with
            | ("ToUpper" | "ToUpperInvariant"), [ charExpr ] ->
                tryTranslateWithContext ctx charExpr |> Option.map PolarsWrapper.StrToUpper
            | ("ToLower" | "ToLowerInvariant"), [ charExpr ] ->
                tryTranslateWithContext ctx charExpr |> Option.map PolarsWrapper.StrToLower
            | _ -> None

        // 2. String methods (Static & Instance)
        elif
            m.DeclaringType = typeof<string>
            || not (isNull target) && target.Type = typeof<string>
        then
            translateStringMethod ctx m target args

        // 2. Temporal methods (DateTime / DateOnly / TimeOnly)
        elif
            m.DeclaringType = typeof<DateTime>
            || m.DeclaringType = typeof<DateOnly>
            || m.DeclaringType = typeof<TimeOnly>
            || not (isNull target) && isTemporalType target.Type
        then
            translateTemporalMethod ctx m target args

        // 3. Math / MathF / Decimal methods (All static)
        elif isNumericDeclaringType m.DeclaringType && isNull target then
            translateNumericMethod ctx m args

        elif
            m.Name = "Sum"
            || m.Name = "Mean"
            || m.Name = "Average"
            || m.Name = "Min"
            || m.Name = "Max"
            || m.Name = "Count"
            || m.Name = "Median"
            || m.Name = "First"
            || m.Name = "Last"
            || m.Name = "Std"
            || m.Name = "StdDev"
            || m.Name = "Var"
            || m.Name = "Variance"
        then
            translateScalarReductionMethod ctx m target args

        // 4. Decimal / Numeric Instance Methods (e.g. d.ToString())
        elif not (isNull target) && target.Type = typeof<decimal> then
            match m.Name, args with
            | "ToString", [] ->
                tryTranslateWithContext ctx target
                |> Option.bind (tryTranslateCast typeof<string>)

            | "ToString", [ formatExpr ] ->
                match tryTranslateWithContext ctx target, tryEvaluate formatExpr with
                | Some t, Some(:? string as fmt) ->
                    if fmt.StartsWith("F", StringComparison.OrdinalIgnoreCase) then
                        // Safely parse precision, defaulting to 2 decimal places if "F" / "f" has no explicit digits
                        let scale =
                            let s = fmt.Substring 1

                            match Int32.TryParse(s) with
                            | true, v -> v
                            | false, _ -> 2

                        // Two-phase cast: First cast to target scale Decimal (ensures rounding & zero-padding), then cast to String
                        let decType = PolarsWrapper.NewDecimalType(38, scale)
                        let decTypeExpr = PolarsWrapper.DataTypeExprFromDataType(decType)

                        let scaled =
                            PolarsWrapper.ExprCast(t, decTypeExpr, strict = false, wrapNumerical = false)

                        tryTranslateCast typeof<string> scaled
                    else
                        None
                | _ -> None
            | _ -> None

        else
            None

    // =========================================================================
    // 8. List / Array Row-Level Expression Dispatcher
    // =========================================================================

    let internal tryCreateSeriesHandleFromObject (objVal: obj) : SeriesHandle option =
        match objVal with
        | null -> None
        | :? Array as arr ->
            try
                let h = SeriesFactory.Create("", arr)
                if not (isNull h || h.IsInvalid) then Some h else None
            with _ ->
                None
        | :? System.Collections.IEnumerable as seq when seq.GetType() <> typeof<string> ->
            try
                let elemType =
                    match PolarsTypeHelper.TryGetEnumerableElementType (seq.GetType()) with
                    | null -> typeof<obj>
                    | t -> t

                let createGeneric =
                    typeof<SeriesFactory>
                        .GetMethod("CreateGenericType", BindingFlags.Public ||| BindingFlags.Static)
                        .MakeGenericMethod(elemType)

                let h = createGeneric.Invoke(null, [| box ""; seq |]) :?> SeriesHandle
                if not (isNull h || h.IsInvalid) then Some h else None
            with _ ->
                None
        | _ -> None

    let private translateListInvocation (ctx: TranslationContext) (invocation: ListInvocation) : ExprHandle option =
        let t1 expr = tryTranslateWithContext ctx expr

        let t2 e1 e2 =
            match tryTranslateWithContext ctx e1, tryTranslateWithContext ctx e2 with
            | Some l, Some r -> Some(l, r)
            | _ -> None

        match invocation with
        | ListLen target -> t1 target |> Option.map PolarsWrapper.ListLen
        | ListContains(target, itemExpr) ->
            match t2 target itemExpr with
            | Some(t, item) -> Some(PolarsWrapper.ListContains(t, item, nullsEqual = false))
            | None ->
                // Check if target is an in-memory collection (IN predicate: item IN target)
                option {
                    let! itemH = t1 itemExpr
                    let unwrapped = unwrapSpanOrArray target
                    let! evaluated = tryEvaluate unwrapped

                    match evaluated with
                    | :? System.Collections.IEnumerable as coll when coll.GetType() <> typeof<string> ->
                        let! seriesHandle = tryCreateSeriesHandleFromObject coll
                        let litH = PolarsWrapper.Lit seriesHandle
                        let implodedH = PolarsWrapper.Implode(litH, maintainOrder = false)
                        return PolarsWrapper.IsIn(itemH, implodedH, nullsEqual = false)
                    | _ -> return! None
                }
        | ListGet(target, indexExpr) ->
            t2 target indexExpr
            |> Option.map (fun (t, idx) -> PolarsWrapper.ListGet(t, idx, nullOnOob = true))
        | ListFirst target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListGet(t, PolarsWrapper.Lit 0, nullOnOob = true))
        | ListLast target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListGet(t, PolarsWrapper.Lit -1, nullOnOob = true))
        | ListSum target -> t1 target |> Option.map PolarsWrapper.ListSum
        | ListMean target -> t1 target |> Option.map PolarsWrapper.ListMean
        | ListMin target -> t1 target |> Option.map PolarsWrapper.ListMin
        | ListMax target -> t1 target |> Option.map PolarsWrapper.ListMax

        | ListJoin(target, separatorExpr) ->
            let sepOpt =
                match tryEvaluate separatorExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some(string sep)
                | _ -> None

            match unwrap target with
            | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
                match sepOpt with
                | Some sep ->
                    let items = nae.Expressions |> Seq.toList
                    let itemHandles = items |> List.choose (tryTranslateWithContext ctx)
                    if itemHandles.Length = items.Length && not itemHandles.IsEmpty then
                        Some(PolarsWrapper.ConcatString(List.toArray itemHandles, sep, ignoreNulls = false))
                    else
                        None
                | None -> None
            | _ ->
                match t1 target, sepOpt with
                | Some t, Some sep -> Some(PolarsWrapper.ListJoin(t, sep, ignoreNulls = true))
                | _ -> None

        | ListHead(target, countExpr) -> t2 target countExpr |> Option.map PolarsWrapper.ListHead
        | ListTail(target, countExpr) -> t2 target countExpr |> Option.map PolarsWrapper.ListTail

        | ListSlice(target, offsetExpr, lengthOpt) ->
            match t2 target offsetExpr with
            | Some(t, offset) ->
                match lengthOpt with
                | Some lenExpr -> t1 lenExpr |> Option.map (fun len -> PolarsWrapper.ListSlice(t, offset, len))
                | None -> Some(PolarsWrapper.ListSlice(t, offset, PolarsWrapper.Lit Int64.MaxValue))
            | None -> None

        | ListUnique target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListEval(t, PolarsWrapper.ExprUnique(PolarsWrapper.Col(""))))
        | ListReverse target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListEval(t, PolarsWrapper.Reverse(PolarsWrapper.Col(""))))
        | ListSort(target, descending) ->
            t1 target
            |> Option.map (fun t ->
                PolarsWrapper.ListSort(t, descending = descending, nullsLast = false, maintainOrder = false))

        | ListSetIntersection(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetIntersection
        | ListSetUnion(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetUnion
        | ListSetDifference(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetDifference

        | ListAny(target, None) ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.Gt(PolarsWrapper.ListLen t, PolarsWrapper.Lit 0))

        | ListAny(target, Some lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.ListElement lam.Parameters.[0].Name

                tryTranslateWithContext innerCtx lam.Body
                |> Option.map (fun innerCond ->
                    let anyExpr = PolarsWrapper.Any(innerCond, ignoreNulls = false)
                    let boolList = PolarsWrapper.ListEval(t, anyExpr)
                    PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true))
            | None -> None

        | ListAny _ -> None

        | ListAll(target, lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.ListElement lam.Parameters.[0].Name

                tryTranslateWithContext innerCtx lam.Body
                |> Option.map (fun innerCond ->
                    let allExpr = PolarsWrapper.All(innerCond, ignoreNulls = false)
                    let boolList = PolarsWrapper.ListEval(t, allExpr)
                    PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true))
            | None -> None
        | ListAll _ -> None

        | ListSelect(target, lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.ListElement lam.Parameters.[0].Name

                tryTranslateWithContext innerCtx lam.Body
                |> Option.map (fun mapperExpr -> PolarsWrapper.ListEval(t, mapperExpr))
            | None -> None
        | ListSelect _ -> None

    // =========================================================================
    // 8b. JSON Operations (System.Text.Json)
    // =========================================================================

    let private translateJsonInvocation (ctx: TranslationContext) (invocation: JsonInvocation) : ExprHandle option =
        match invocation with
        | JsonPathMatch(target, path, targetType) ->
            tryTranslateWithContext ctx target
            |> Option.bind (fun t ->
                let pathH = PolarsWrapper.Lit path
                let matchH = PolarsWrapper.StrJsonPathMatch(t, pathH)
                if targetType = typeof<string> || targetType = typeof<obj> then
                    Some matchH
                elif targetType = typeof<bool> || targetType = typeof<Nullable<bool>> then
                    let lowerH = PolarsWrapper.StrToLower matchH
                    Some(PolarsWrapper.Eq(lowerH, PolarsWrapper.Lit "true"))
                else
                    tryTranslateCast targetType matchH)

        | JsonSerialize target ->
            let unwrappedTarget = unwrap target
            match unwrappedTarget with
            | :? NewExpression as ne ->
                let memberNames =
                    if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                        ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                    elif not (isNull ne.Constructor) then
                        let ctorParams = ne.Constructor.GetParameters()
                        ctorParams |> Array.map (fun p -> p.Name)
                    else
                        [||]

                let argHandles =
                    ne.Arguments
                    |> Seq.mapi (fun i argExpr ->
                        match tryTranslateWithContext ctx argExpr with
                        | Some h ->
                            let fieldName = if i < memberNames.Length then memberNames.[i] else sprintf "field_%d" i
                            Some(PolarsWrapper.Alias(h, fieldName))
                        | None -> None)
                    |> Seq.toList

                if argHandles |> List.forall Option.isSome then
                    let handlesArray = argHandles |> List.map Option.get |> List.toArray
                    let structH = PolarsWrapper.AsStruct handlesArray
                    Some(PolarsWrapper.StructJsonEncode structH)
                else
                    None

            | :? MemberInitExpression as mie ->
                let bindings =
                    mie.Bindings
                    |> Seq.choose (fun b ->
                        match b with
                        | :? MemberAssignment as ma ->
                            match tryTranslateWithContext ctx ma.Expression with
                            | Some h -> Some(PolarsWrapper.Alias(h, ma.Member.Name))
                            | None -> None
                        | _ -> None)
                    |> Seq.toArray
                if bindings.Length = mie.Bindings.Count then
                    let structH = PolarsWrapper.AsStruct bindings
                    Some(PolarsWrapper.StructJsonEncode structH)
                else
                    None

            | other ->
                tryTranslateWithContext ctx other
                |> Option.map (fun h -> PolarsWrapper.StructJsonEncode h)

        | JsonDeserialize(target, dtoType, steps) ->
            match JsonTypeInference.inferPolarsJsonDataType dtoType with
            | None -> None
            | Some dtypeHandle ->
                use _dtypeScope = dtypeHandle
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle

                match tryTranslateWithContext ctx target with
                | None ->
                    dtypeExpr.Dispose()
                    None
                | Some targetH ->
                    let decodedH = PolarsWrapper.StrJsonDecode(targetH, dtypeExpr)

                    let rec traverseSteps (currentH: ExprHandle) (currType: Type) (remainingSteps: JsonAccessStep list) : ExprHandle option =
                        match remainingSteps with
                        | [] -> Some currentH

                        | PropertyStep m :: rest when
                            (m.Name = "Length" || m.Name = "Count")
                            && not (isNull (PolarsTypeHelper.TryGetEnumerableElementType currType)) ->
                            let lenH = PolarsWrapper.ListLen currentH
                            traverseSteps lenH typeof<int> rest

                        | PropertyStep m :: rest ->
                            let memberType =
                                match m with
                                | :? PropertyInfo as p -> p.PropertyType
                                | :? FieldInfo as f -> f.FieldType
                                | _ -> typeof<obj>

                            match JsonTypeInference.tryResolveJsonFieldName currType m.Name with
                            | Some jsonFieldName ->
                                let fieldH = PolarsWrapper.StructFieldByName(currentH, [| jsonFieldName |])
                                traverseSteps fieldH memberType rest
                            | None ->
                                let fieldH = PolarsWrapper.StructFieldByName(currentH, [| m.Name |])
                                traverseSteps fieldH memberType rest

                        | IndexStep indexExpr :: rest ->
                            let elemType =
                                let elemOpt = PolarsTypeHelper.TryGetEnumerableElementType currType
                                if not (isNull elemOpt) then elemOpt
                                elif currType.IsArray then currType.GetElementType()
                                elif currType.IsGenericType then currType.GetGenericArguments().[0]
                                else typeof<obj>

                            match tryTranslateWithContext ctx indexExpr with
                            | Some idxH ->
                                let itemH = PolarsWrapper.ListGet(currentH, idxH, nullOnOob = true)
                                traverseSteps itemH elemType rest
                            | None -> None

                    traverseSteps decodedH dtoType steps

    // =========================================================================
    // 9. Operators & Casts
    // =========================================================================

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
                    if not (isNull cleanE.Type) && not (PolarsTypeHelper.IsScalarType cleanE.Type || cleanE.Type = typeof<string>) then
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

    let private translateBinaryExpr
        (ctx: TranslationContext)
        (op: ExpressionType)
        (left: Expression)
        (right: Expression)
        : ExprHandle option =
        let translateNullCheckTarget targetExpr =
            tryTranslateWithContext ctx targetExpr
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
            match tryTranslateWithContext ctx left, tryEvaluate right with
            | Some leftH, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some(PolarsWrapper.BitLeftShift(leftH, n))
                with _ ->
                    None
            | _ -> None

        elif op = ExpressionType.RightShift then
            match tryTranslateWithContext ctx left, tryEvaluate right with
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
            match tryTranslateWithContext ctx left, tryTranslateWithContext ctx right with
            | Some l, Some r -> Some(PolarsWrapper.FormatString("{}{}", [| l; r |]))
            | _ -> None
        else
            match tryTranslateWithContext ctx left, tryTranslateWithContext ctx right with
            | Some l, Some r -> Some(translateBinary op l r)
            | _ -> None

    let private translateUnaryExpr
        (ctx: TranslationContext)
        (op: ExpressionType)
        (operand: Expression)
        (targetType: Type)
        : ExprHandle option =
        match op with
        | ExpressionType.Convert
        | ExpressionType.ConvertChecked ->
            tryTranslateWithContext ctx operand |> Option.bind (tryTranslateCast targetType)
        | _ -> tryTranslateWithContext ctx operand |> Option.bind (translateUnary op)

    // =========================================================================
    // 10. Main Entrypoint Dispatcher
    // =========================================================================

    /// Attempts to translate an Expression AST node to a native Polars ExprHandle using a strongly-typed TranslationContext.
    let tryTranslateWithContext (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
        try
            match expr with
            // 0. Row-level String Split exact index access: s.Split(sep)[idx] -> StrSplitExact + StructFieldByIndex
            | SplitIndexAccess(target, sep, idx) ->
                let n =
                    match SplitOptContext.Current with
                    | Some map ->
                        match map.TryFind(target.ToString(), sep) with
                        | Some maxIdx -> maxIdx
                        | None -> idx
                    | None -> idx

                match tryTranslateWithContext ctx target with
                | Some t ->
                    let splitH = PolarsWrapper.StrSplitExact(t, PolarsWrapper.Lit sep, n, inclusive = false)
                    Some(PolarsWrapper.StructFieldByIndex(splitH, int64 idx))
                | _ -> None

            // 1. Row-level List / Array operations
            | ArrayIndexOp(target, indexExpr) ->
                match tryTranslateWithContext ctx target, tryTranslateWithContext ctx indexExpr with
                | Some t, Some idx -> Some(PolarsWrapper.ListGet(t, idx, nullOnOob = true))
                | _ -> None

            // 1. Regex operations
            | RegexInvocation invocation -> translateRegexInvocation ctx invocation

            // 2. List / Array operations
            | ListInvocation listInv -> translateListInvocation ctx listInv

            // 2b. JSON operations (System.Text.Json)
            | JsonInvocation jsonInv -> translateJsonInvocation ctx jsonInv

            // 3. Struct field pushdown (only when expression does NOT resolve to a top-level column or Nullable<T>.HasValue)
            | StructFieldAccess(parentExpr, fieldName) when
                (tryResolveColumnNameWithContext ctx expr).IsNone
                && match expr with
                   | MemberAccess(_, m) -> not (isNullableHasValue m)
                   | _ -> true
                ->
                tryTranslateWithContext ctx parentExpr
                |> Option.map (fun parentHandle -> PolarsWrapper.StructFieldByName(parentHandle, [| fieldName |]))

            // 4. Member Access (Columns, temporal/time properties, Math constants, captured closures)
            | MemberAccess _ as memberExpr -> translateMemberAccess ctx memberExpr

            // 5. Method Calls (Strings, DateTime, Math, etc.)
            | MethodCall(m, target, args) -> translateMethodCall ctx m target args

            // 6. Binary operations
            | Binary(op, left, right) -> translateBinaryExpr ctx op left right

            // 7. Unary operations & conversions
            | Unary(op, operand) -> translateUnaryExpr ctx op operand expr.Type

            // 8. Conditional ternary (test ? true : false)
            | :? ConditionalExpression as cond ->
                match
                    tryTranslateWithContext ctx cond.Test,
                    tryTranslateWithContext ctx cond.IfTrue,
                    tryTranslateWithContext ctx cond.IfFalse
                with
                | Some testH, Some trueH, Some falseH -> Some(PolarsWrapper.IfElse(testH, trueH, falseH))
                | _ -> None

            // 9. Constants
            | Constant(value, t) -> Some(toLiteralHandle value t)

            // 10. Parameter mapping: delegate physical column resolution directly to TranslationContext
            | :? ParameterExpression as p ->
                if
                    not (isNull p.Name)
                    && (p.Name.StartsWith "__subquery_agg_" || p.Name.StartsWith "__subquery_col_")
                then
                    Some(PolarsWrapper.Col p.Name)
                elif not (isNull p.Name) && p.Name.StartsWith "__key_col_" then
                    Some(PolarsWrapper.Col(p.Name.Substring 10))
                else
                    ctx.TryResolveParameterHandle p.Name

            | _ -> None
        with _ ->
            None
