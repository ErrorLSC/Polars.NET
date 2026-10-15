namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System
open System.Reflection
open System.Linq.Expressions
open System.Text.RegularExpressions
open Polars.NET.Core.Helpers
open Apache.Arrow.Types

/// Represents the semantic parameter context for LINQ expression tree translation.
[<RequireQualifiedAccess>]
type internal TranslationContext =
    /// Empty context with no parameter bindings (e.g. constants, unbound reductions, or outer Having predicates).
    | Empty

    /// Standard single-entity row context (e.g. e => e.Age), where member accesses resolve to DataFrame columns.
    | Entity of paramName: string

    /// Scalar sequence element context (e.g. x => x.Length or x => x > 10).
    /// The parameter node itself resolves to physicalCol, while member accesses on it represent scalar operations.
    | Scalar of paramName: string * physicalCol: string

    /// Binary parameter context for Joins and Merges (e.g. (outer, inner) => new { outer.Id, inner.Name }).
    | BinaryJoin of leftParam: string * rightParam: string

    /// Merge context: (target, source) => predicate
    /// Target columns resolve to "Col", source columns resolve to "Col" + sourceSuffix.
    | Merge of targetParam: string * sourceParam: string * sourceSuffix: string

    /// Determines whether the given parameter name matches any parameter active in the current scope.
    member this.ContainsParam(name: string) : bool =
        match this with
        | TranslationContext.Empty -> false
        | TranslationContext.Entity p -> p = name
        | TranslationContext.Scalar (p, _) -> p = name
        | TranslationContext.BinaryJoin (left, right) -> name = left || name = right
        | TranslationContext.Merge (target, source, _) -> name = target || name = source

    /// Attempts to resolve the parameter expression node itself to a native Polars ExprHandle.
    member this.TryResolveParameterHandle(name: string) : ExprHandle option =
        match this with
        | TranslationContext.Scalar (p, physicalCol) when p = name && not (String.IsNullOrEmpty physicalCol) ->
            Some (PolarsWrapper.Col physicalCol)
        | TranslationContext.Entity p when p = name ->
            // Entity placeholder within expressions (e.g. list_eval inner element placeholder)
            Some (PolarsWrapper.Col "")
        | _ -> None

    /// Indicates whether member/property accesses are eligible to be resolved as DataFrame columns.
    member this.AllowsColumnMemberLookup : bool =
        match this with
        | TranslationContext.Scalar _ -> false
        | _ -> true

/// Recursive module for translating LINQ Expression Trees to native Polars Expr handles.
module internal rec ExprTranslator =

    /// Strips Unary wraps like Convert or Quote recursively
    let rec internal unwrap (e: Expression) =
        match e with
        | null -> null
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) -> unwrap inner
        | other -> other

    // =========================================================================
    // 1. Type & Literal Mapping Helpers
    // =========================================================================
    /// Maps standard LINQ reduction method names to their corresponding native Polars aggregation expressions.
    let internal translateReductionOp (methodName: string)(targetColExpr: ExprHandle) : ExprHandle option =
        match methodName with
        | "Sum"     -> Some (PolarsWrapper.Sum targetColExpr)
        | "Mean"
        | "Average" -> Some (PolarsWrapper.Mean targetColExpr)
        | "Min"     -> Some (PolarsWrapper.Min targetColExpr)
        | "Max"     -> Some (PolarsWrapper.Max targetColExpr)
        | "Median"  -> Some (PolarsWrapper.Median targetColExpr)
        | "First"   -> Some (PolarsWrapper.First(targetColExpr, ignoreNulls = false))
        | "Last"    -> Some (PolarsWrapper.Last(targetColExpr, ignoreNulls = false))
        | "Std"
        | "StdDev"   -> Some (PolarsWrapper.Std(targetColExpr, 1uy))
        | "Var"
        | "Variance" -> Some (PolarsWrapper.Var(targetColExpr, 1uy))
        | _          -> None

    /// Maps statistical dispersion reduction operations with explicit degree of freedom (ddof).
    let internal translateDispersionOp (methodName: string) (ddof: byte) (targetColExpr: ExprHandle) : ExprHandle option =
        match methodName with
        | "Std"
        | "StdDev"  -> Some (PolarsWrapper.Std(targetColExpr, ddof))
        | "Var"
        | "Variance"-> Some (PolarsWrapper.Var(targetColExpr, ddof))
        | _         -> None

    let rec private isNullConstantExpr (e: Expression) =
        match e with
        | null -> true
        | :? ConstantExpression as c -> isNull c.Value
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert ||
                                       u.NodeType = ExpressionType.ConvertChecked ||
                                       u.NodeType = ExpressionType.Quote ->
            isNullConstantExpr u.Operand
        | _ -> false

    let private timeUnitMap (unit: TimeUnit) : byte =
        match unit with
        | TimeUnit.Second -> PlTimeUnit.Second |> byte
        | TimeUnit.Millisecond -> PlTimeUnit.Milliseconds |> byte
        | TimeUnit.Microsecond -> PlTimeUnit.Microseconds |> byte
        | TimeUnit.Nanosecond -> PlTimeUnit.Nanoseconds |> byte
        | _ -> invalidArg (nameof unit) "Unknown TimeUnit"

    let private arrowTypeToPolarsDataType (arrowType: IArrowType) : DataTypeHandle option =
        match arrowType with
        | :? BooleanType   -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Boolean))
        | :? Int8Type      -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int8))
        | :? UInt8Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt8))
        | :? Int16Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int16))
        | :? UInt16Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt16))
        | :? Int32Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))
        | :? UInt32Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt32))
        | :? Int64Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))
        | :? UInt64Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt64))
        | :? HalfFloatType -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float16))
        | :? FloatType     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float32))
        | :? DoubleType    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float64))
        | :? StringType
        | :? StringViewType
        | :? LargeStringType -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.String))
        | :? Date32Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Date))
        | :? Time64Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Time))
        | :? DurationType as du  -> Some (PolarsWrapper.NewDurationType(timeUnitMap du.Unit))
        | :? TimestampType as ts ->
            let tz = if String.IsNullOrEmpty ts.Timezone then null else ts.Timezone
            Some (PolarsWrapper.NewDateTimeType(timeUnitMap ts.Unit, tz))
        | :? Decimal128Type as dec ->
            Some (PolarsWrapper.NewDecimalType(int dec.Precision, int dec.Scale))
        | _ -> None

    let private tryGetPolarsDataType (t: Type) : DataTypeHandle option =
        try
            (ArrowTypeResolver.GetArrowTypeFromNetType >> arrowTypeToPolarsDataType) t
        with _ -> None

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
                | :? char as c -> PolarsWrapper.Lit (string c)
                | :? string as s -> PolarsWrapper.Lit s
                | :? DateTime as dt -> PolarsWrapper.Lit dt
                | :? DateTimeOffset as doff -> PolarsWrapper.Lit doff
                | :? TimeSpan as ts -> PolarsWrapper.Lit ts
                | :? DateOnly as dof -> PolarsWrapper.Lit dof
                | :? TimeOnly as tof -> PolarsWrapper.Lit tof
                | other ->
                    failwithf "Type '%s' is not supported as a constant literal in Polars LINQ" (other.GetType().FullName)

    let private tryTranslateCast (targetType: Type) (operand: ExprHandle) : ExprHandle option =
        if targetType = typeof<obj> then
            Some operand
        else
            tryGetPolarsDataType targetType
            |> Option.map (fun dtypeHandle ->
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType(dtypeHandle)
                PolarsWrapper.ExprCast(operand, dtypeExpr, strict = false, wrapNumerical = false))

    // =========================================================================
    // 2. Column & Parameter Identifiers
    // =========================================================================

    let internal isNavigationMember (m: MemberInfo) =
        if isNull m || isNull m.DeclaringType then false
        else
            let typeName = m.DeclaringType.Name
            let memberName = m.Name

            typeName.StartsWith "Tuple" ||
            typeName.StartsWith "ValueTuple" ||
            typeName.StartsWith "AnonymousObject" ||
            typeName.Contains "TransparentIdentifier" ||
            memberName.Contains "TransparentIdentifier" ||
            memberName.StartsWith "<>h__" ||
            (typeName.Contains "AnonymousType" &&
            match m with
            | :? PropertyInfo as p -> not (PolarsTypeHelper.IsScalarType p.PropertyType)
            | :? FieldInfo as f    -> not (PolarsTypeHelper.IsScalarType f.FieldType)
            | _                    -> false)

    /// Resolves the underlying physical column name using a strongly-typed TranslationContext.
    let rec internal tryResolveColumnNameWithContext (ctx: TranslationContext) (expr: Expression) : string option =
        if not ctx.AllowsColumnMemberLookup then
            None
        else
            match expr with
            | MemberAccess(_, m) when isNavigationMember m ->
                None

            | MemberAccess(inner, m) ->
                if PolarsTypeHelper.IsScalarType inner.Type || inner.Type = typeof<string> || isTemporalType inner.Type then
                    None
                else
                    let rec isRootedIn (targetName: string) (e: Expression) =
                        if String.IsNullOrEmpty targetName then false
                        else
                            match unwrap e with
                            | :? ParameterExpression as p -> p.Name = targetName
                            | MemberAccess(nextInner, nextM) when isNavigationMember nextM -> isRootedIn targetName nextInner
                            | _ -> false

                    match ctx with
                    | TranslationContext.Merge (target, source, suffix) ->
                        if isRootedIn target inner then Some m.Name
                        elif isRootedIn source inner then Some (m.Name + suffix)
                        else None

                    | _ ->
                        let rec isRootedInParamOrClosure (e: Expression) =
                            match unwrap e with
                            | :? ParameterExpression as p ->
                                ctx.ContainsParam p.Name ||
                                p.Name.StartsWith "_arg" ||
                                p.Name.StartsWith "tupled" ||
                                p.Name.Contains "TransparentIdentifier" ||
                                p.Name.StartsWith "<>h__" ||
                                p.Type.Name.StartsWith "AnonymousObject" ||
                                p.Type.Name.Contains "AnonymousType" ||
                                p.Type.Name.StartsWith "Tuple" ||
                                p.Type.Name.StartsWith "ValueTuple"
                            | MemberAccess(nextInner, nextM) when isNavigationMember nextM ->
                                isRootedInParamOrClosure nextInner
                            | _ -> false

                        if isRootedInParamOrClosure inner then Some m.Name
                        else None

            | _ -> None

    // =========================================================================
    // 3. Member Access Pushdown Dispatcher
    // =========================================================================

    let private isTemporalType (t: Type) =
        t = typeof<DateTime> || t = typeof<DateTimeOffset> || t = typeof<DateOnly> || t = typeof<TimeOnly>

    let private translateMemberAccess (ctx: TranslationContext) (memberExpr: Expression) : ExprHandle option =
        match tryResolveColumnNameWithContext ctx memberExpr with
        | Some actualCol -> Some (PolarsWrapper.Col actualCol)
        | None ->
            match memberExpr with
            | MemberAccess(null, m) when m.DeclaringType = typeof<Math> || m.DeclaringType = typeof<MathF> ->
                match m.Name with
                | "PI"  -> Some (PolarsWrapper.Lit Math.PI)
                | "E"   -> Some (PolarsWrapper.Lit Math.E)
                | "Tau" -> Some (PolarsWrapper.Lit (Math.PI * 2.0))
                | _     -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<string> && m.Name = "Length" ->
                tryTranslateWithContext ctx target |> Option.map PolarsWrapper.StrLenChars

            | MemberAccess(target, m) when not (isNull target) && isTemporalType target.Type ->
                let t () = tryTranslateWithContext ctx target
                match m.Name with
                | "Year"        -> t () |> Option.map PolarsWrapper.DtYear
                | "Month"       -> t () |> Option.map PolarsWrapper.DtMonth
                | "Day"         -> t () |> Option.map PolarsWrapper.DtDay
                | "DayOfYear"   -> t () |> Option.map PolarsWrapper.DtOrdinalDay
                | "DayOfWeek"   ->
                    // Polars dt.weekday() returns ISO weekday (1..7, Monday=1, Sunday=7) as Int8.
                    // .NET System.DayOfWeek defines Sunday=0, Monday=1..Saturday=6, with underlying type Int32.
                    // We cast to Int32 and compute (weekday % 7) to:
                    // 1. Map Sunday (7 % 7 = 0) to DayOfWeek.Sunday and preserve Monday..Saturday (1..6).
                    // 2. Ensure the Arrow column type is Int32 so CLR materializer can cleanly unbox it to DayOfWeek.
                    t () |> Option.bind (fun dtH ->
                        let weekdayH = PolarsWrapper.DtWeekday dtH
                        match tryTranslateCast typeof<int> weekdayH with
                        | Some weekdayInt ->
                            Some (PolarsWrapper.Rem(weekdayInt, PolarsWrapper.Lit 7))
                        | None ->
                            let rem = PolarsWrapper.Rem(weekdayH, PolarsWrapper.Lit 7y)
                            tryTranslateCast typeof<int> rem)
                | "Hour"        -> t () |> Option.map PolarsWrapper.DtHour
                | "Minute"      -> t () |> Option.map PolarsWrapper.DtMinute
                | "Second"      -> t () |> Option.map PolarsWrapper.DtSecond
                | "Millisecond" -> t () |> Option.map PolarsWrapper.DtMillisecond
                | "Microsecond" -> t () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanosecond"  -> t () |> Option.map PolarsWrapper.DtNanosecond
                | "Date"        -> t () |> Option.map PolarsWrapper.DtDate
                | "TimeOfDay"   -> t () |> Option.map PolarsWrapper.DtTime
                | _             -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<TimeSpan> ->
                let t () = tryTranslateWithContext ctx target
                match m.Name with
                | "TotalDays"         -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalDays(x, fractional = true))
                | "TotalHours"        -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalHours(x, fractional = true))
                | "TotalMinutes"      -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalMinutes(x, fractional = true))
                | "TotalSeconds"      -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalSeconds(x, fractional = true))
                | "TotalMilliseconds" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalMilliseconds(x, fractional = true))
                | "TotalMicroseconds" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalMicroseconds(x, fractional = true))
                | "TotalNanoseconds"  -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalNanoseconds(x, fractional = true))
                | "Days"              -> t () |> Option.map PolarsWrapper.DtDay
                | "Hours"             -> t () |> Option.map PolarsWrapper.DtHour
                | "Minutes"           -> t () |> Option.map PolarsWrapper.DtMinute
                | "Seconds"           -> t () |> Option.map PolarsWrapper.DtSecond
                | "Milliseconds"      -> t () |> Option.map PolarsWrapper.DtMillisecond
                | "Microseconds"      -> t () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanoseconds"       -> t () |> Option.map PolarsWrapper.DtNanosecond
                | _                   -> None

            | _ ->
                tryEvaluate memberExpr
                |> Option.map (fun value -> toLiteralHandle value memberExpr.Type)

    // =========================================================================
    // 4. Method Call Dispatchers (String / DateTime / Math)
    // =========================================================================

    /// Evaluates StringComparison expression to determine case insensitivity.
    let private tryEvaluateCaseInsensitive (compExpr: Expression) : bool option =
        match tryEvaluate compExpr with
        | Some (:? StringComparison as sc) ->
            match sc with
            | StringComparison.OrdinalIgnoreCase
            | StringComparison.InvariantCultureIgnoreCase
            | StringComparison.CurrentCultureIgnoreCase -> Some true
            | _ -> Some false
        | Some (:? bool as ignoreCase) ->
            Some ignoreCase
        | Some other ->
            try
                let intVal = Convert.ToInt32 other
                match LanguagePrimitives.EnumOfValue<int, StringComparison> intVal with
                | StringComparison.OrdinalIgnoreCase
                | StringComparison.InvariantCultureIgnoreCase
                | StringComparison.CurrentCultureIgnoreCase -> Some true
                | _ -> Some false
            with _ -> None
        | None -> None

    let private translateStringMethod (ctx: TranslationContext) (m: MethodInfo) (target: Expression) (args: Expression list) : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target
        match m.Name, args with
        // --- 1. String.Concat (Static string.Concat / flattened arguments) ---
        | "Concat", _ when isNull target ->
            let flatArgs =
                match args with
                | [ :? NewArrayExpression as nae ] when nae.NodeType = ExpressionType.NewArrayInit ->
                    nae.Expressions |> Seq.toList
                | other -> other

            if flatArgs.IsEmpty then
                Some (PolarsWrapper.Lit "")
            else
                let argHandles = flatArgs |> List.choose (tryTranslateWithContext ctx)
                if argHandles.Length = flatArgs.Length then
                    let rustFmt = String.replicate argHandles.Length "{}"
                    Some (PolarsWrapper.FormatString(rustFmt, List.toArray argHandles))
                else
                    None

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
                | Some (:? string as netFormat) ->
                    // Matches standard .NET placeholders like {0}, {1}, {0:F2}, etc.
                    let matches = Regex.Matches(netFormat, @"\{(\d+)(?::[^}]+)?\}")
                    let referencedIndices =
                        matches
                        |> Seq.map (fun m -> Int32.Parse(m.Groups.[1].Value))
                        |> Seq.toList

                    if referencedIndices.IsEmpty then
                        Some (PolarsWrapper.Lit netFormat)
                    elif referencedIndices |> List.forall (fun idx -> idx >= 0 && idx < rawItems.Length) then
                        let orderedExprs = referencedIndices |> List.map (fun idx -> rawItems.[idx])
                        let argHandles = orderedExprs |> List.choose (tryTranslateWithContext ctx)
                        if argHandles.Length = orderedExprs.Length then
                            // Convert {0}, {1:F2} to Polars/Rust {} positional placeholders
                            let rustFmt = Regex.Replace(netFormat, @"\{(\d+)(?::[^}]+)?\}", "{}")
                            Some (PolarsWrapper.FormatString(rustFmt, List.toArray argHandles))
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

        // --- Contains ---
        | "Contains", [ arg ] ->
            match targetH (), tryTranslateWithContext ctx arg with
            | Some t, Some p -> Some (PolarsWrapper.StrContains(t, p, literal = true, strict = false))
            | _ -> None

        | "Contains", [ arg; compExpr ] ->
            match targetH (), tryTranslateWithContext ctx arg, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some p, Some true ->
                let tLower = PolarsWrapper.StrToLower t
                let pLower = PolarsWrapper.StrToLower p
                Some (PolarsWrapper.StrContains(tLower, pLower, literal = true, strict = false))
            | Some t, Some p, Some false ->
                Some (PolarsWrapper.StrContains(t, p, literal = true, strict = false))
            | _ -> None

        // --- StartsWith ---
        | "StartsWith", [ arg ] ->
            match targetH (), tryTranslateWithContext ctx arg with
            | Some t, Some p -> Some (PolarsWrapper.StrStartsWith(t, p))
            | _ -> None

        | "StartsWith", [ arg; compExpr ]
        | "StartsWith", arg :: compExpr :: _ ->
            match targetH (), tryTranslateWithContext ctx arg, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some p, Some true ->
                let tLower = PolarsWrapper.StrToLower t
                let pLower = PolarsWrapper.StrToLower p
                Some (PolarsWrapper.StrStartsWith(tLower, pLower))
            | Some t, Some p, Some false ->
                Some (PolarsWrapper.StrStartsWith(t, p))
            | _ -> None

        // --- EndsWith ---
        | "EndsWith", [ arg ] ->
            match targetH (), tryTranslateWithContext ctx arg with
            | Some t, Some p -> Some (PolarsWrapper.StrEndsWith(t, p))
            | _ -> None

        | "EndsWith", [ arg; compExpr ]
        | "EndsWith", arg :: compExpr :: _ ->
            match targetH (), tryTranslateWithContext ctx arg, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some p, Some true ->
                let tLower = PolarsWrapper.StrToLower t
                let pLower = PolarsWrapper.StrToLower p
                Some (PolarsWrapper.StrEndsWith(tLower, pLower))
            | Some t, Some p, Some false ->
                Some (PolarsWrapper.StrEndsWith(t, p))
            | _ -> None

        | "ToUpper", [] | "ToUpperInvariant", [] -> targetH () |> Option.map PolarsWrapper.StrToUpper
        | "ToLower", [] | "ToLowerInvariant", [] -> targetH () |> Option.map PolarsWrapper.StrToLower
        | "Substring", [ startExpr ] ->
            match targetH (), tryTranslateWithContext ctx startExpr with
            | Some t, Some startH -> Some (PolarsWrapper.StrSlice(t, startH, PolarsWrapper.LitNull()))
            | _ -> None
        | "Substring", [ startExpr; lenExpr ] ->
            match targetH (), tryTranslateWithContext ctx startExpr, tryTranslateWithContext ctx lenExpr with
            | Some t, Some startH, Some lenH -> Some (PolarsWrapper.StrSlice(t, startH, lenH))
            | _ -> None

        // --- Replace ---
        | "Replace", [ oldExpr; newExpr ] ->
            match targetH (), tryTranslateWithContext ctx oldExpr, tryTranslateWithContext ctx newExpr with
            | Some t, Some o, Some n -> Some (PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
            | _ -> None

        | "Replace", [ oldExpr; newExpr; compExpr ] ->
            match targetH (), tryTranslateWithContext ctx newExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some n, Some false ->
                match tryTranslateWithContext ctx oldExpr with
                | Some o -> Some (PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
                | _ -> None
            | Some t, Some n, Some true ->
                match tryEvaluate oldExpr with
                | Some (:? string as oldStr) ->
                    let escapedPattern = sprintf "(?i)%s" (Regex.Escape oldStr)
                    Some (PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit escapedPattern, n, literal = false))
                | _ -> None
            | _ -> None

        // --- Split (Literal string.Split) ---
        | "Split", [ sepExpr ] ->
            match targetH (), tryEvaluate sepExpr with
            | Some t, Some (:? char as c) ->
                Some (PolarsWrapper.StrSplit(t, PolarsWrapper.Lit (string c), inclusive = false, literal = true, strict = false))
            | Some t, Some (:? string as s) ->
                Some (PolarsWrapper.StrSplit(t, PolarsWrapper.Lit s, inclusive = false, literal = true, strict = false))
            | _ -> None

        | "Trim", []      -> targetH () |> Option.map (fun t -> PolarsWrapper.StrStripChars(t, PolarsWrapper.LitNull()))
        | "TrimStart", [] -> targetH () |> Option.map (fun t -> PolarsWrapper.StrStripCharsStart(t, PolarsWrapper.LitNull()))
        | "TrimEnd", []   -> targetH () |> Option.map (fun t -> PolarsWrapper.StrStripCharsEnd(t, PolarsWrapper.LitNull()))
        | _ -> None

    let private translateTemporalMethod (ctx: TranslationContext) (m: MethodInfo) (target: Expression) (args: Expression list) : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target
        match m.Name, args with
        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            match tryTranslateWithContext ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some (:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                let ambiguousExpr = PolarsWrapper.Lit "raise"
                Some (PolarsWrapper.StrToDatetime(
                    e = strH,
                    unit = PlTimeUnit.Microseconds,
                    timeZone = null,
                    format = chronoFormat,
                    strict = false,
                    exact = true,
                    cache = true,
                    ambiguous = ambiguousExpr
                ))
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
            | Some strH, Some (:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                Some (PolarsWrapper.StrToDate(strH, chronoFormat, strict = false, exact = true, cache = true))
            | _ -> None

        | "IsLeapYear", [ yearExpr ] when isNull target && m.DeclaringType = typeof<DateTime> ->
            tryTranslateWithContext ctx yearExpr
            |> Option.map (fun yearH ->
                let dateStrExpr = PolarsWrapper.FormatString("{}-01-01", [| yearH |])
                let dtExpr = PolarsWrapper.StrToDate(dateStrExpr, "%Y-%m-%d", strict = false, exact = true, cache = true)
                PolarsWrapper.DtIsLeapYear dtExpr)

        | "ToString", [ formatExpr ] ->
            match targetH (), tryEvaluate formatExpr with
            | Some t, Some (:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                if not (String.IsNullOrEmpty chronoFormat) then
                    Some (PolarsWrapper.DtToString(t, chronoFormat))
                else None
            | _ -> None

        | "AddDays", [ daysExpr ] ->
            match targetH (), tryEvaluate daysExpr with
            | Some t, Some d -> Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit (sprintf "%gd" (Convert.ToDouble d))))
            | _ -> None
        | "AddHours", [ hoursExpr ] ->
            match targetH (), tryEvaluate hoursExpr with
            | Some t, Some h -> Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit (sprintf "%gh" (Convert.ToDouble h))))
            | _ -> None
        | "AddMinutes", [ minsExpr ] ->
            match targetH (), tryEvaluate minsExpr with
            | Some t, Some mVal -> Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit (sprintf "%gm" (Convert.ToDouble mVal))))
            | _ -> None
        | "AddSeconds", [ secsExpr ] ->
            match targetH (), tryEvaluate secsExpr with
            | Some t, Some sVal -> Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit (sprintf "%gs" (Convert.ToDouble sVal))))
            | _ -> None
        | _ -> None

    // =========================================================================
    // 5. Numeric & Math Methods (Math, MathF, System.Decimal)
    // =========================================================================

    let private tryMapRoundMode (midpoint: MidpointRounding) : PlRoundMode option =
        match midpoint with
        | MidpointRounding.ToEven       -> Some PlRoundMode.HalfToEven
        | MidpointRounding.AwayFromZero -> Some PlRoundMode.HalfAwayFromZero
        | MidpointRounding.ToZero       -> Some PlRoundMode.ToZero
        | _                             -> None

    let private isNumericDeclaringType (t: Type) =
        t = typeof<Math> || t = typeof<MathF> || t = typeof<decimal> || t = typeof<System.Numerics.BitOperations>

    let private translateNumericMethod (ctx: TranslationContext) (m: MethodInfo) (args: Expression list) : ExprHandle option =
        let t1 expr = tryTranslateWithContext ctx expr
        let t2 e1 e2 =
            match tryTranslateWithContext ctx e1, tryTranslateWithContext ctx e2 with
            | Some l, Some r -> Some (l, r)
            | _ -> None

        match m.Name, args with
        | "Abs", [ x ]      -> t1 x |> Option.map PolarsWrapper.Abs
        | "Ceiling", [ x ]  -> t1 x |> Option.map PolarsWrapper.Ceil
        | "Floor", [ x ]    -> t1 x |> Option.map PolarsWrapper.Floor
        | "Truncate", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Truncate(h, 0u))
        | "Sign", [ x ]     -> t1 x |> Option.map PolarsWrapper.Sign

        | "Round", [ x ] ->
            t1 x |> Option.map (fun h -> PolarsWrapper.Round(h, 0u, PlRoundMode.HalfToEven))

        | "Round", [ x; digitsExpr ] when digitsExpr.Type = typeof<int> ->
            match t1 x, tryEvaluate digitsExpr with
            | Some h, Some d ->
                let decimals = uint32 (Convert.ToInt32 d)
                Some (PolarsWrapper.Round(h, decimals, PlRoundMode.HalfToEven))
            | _ -> None

        | "Round", [ x; modeExpr ] when modeExpr.Type = typeof<MidpointRounding> ->
            match t1 x, tryEvaluate modeExpr with
            | Some h, Some (:? MidpointRounding as mode) ->
                tryMapRoundMode mode
                |> Option.map (fun plMode -> PolarsWrapper.Round(h, 0u, plMode))
            | _ -> None

        | "Round", [ x; digitsExpr; modeExpr ] ->
            match t1 x, tryEvaluate digitsExpr, tryEvaluate modeExpr with
            | Some h, Some d, Some (:? MidpointRounding as mode) ->
                let decimals = uint32 (Convert.ToInt32 d)
                tryMapRoundMode mode
                |> Option.map (fun plMode -> PolarsWrapper.Round(h, decimals, plMode))
            | _ -> None

        | "Min", [ x; y ] ->
            t2 x y |> Option.map (fun (l, r) ->
                let cond = PolarsWrapper.LtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                PolarsWrapper.IfElse(cond, l, r))

        | "Max", [ x; y ] ->
            t2 x y |> Option.map (fun (l, r) ->
                let cond = PolarsWrapper.GtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                PolarsWrapper.IfElse(cond, l, r))

        | "Clamp", [ valExpr; minExpr; maxExpr ] ->
            match t1 valExpr, t1 minExpr, t1 maxExpr with
            | Some v, Some minH, Some maxH -> Some (PolarsWrapper.Clip(v, minH, maxH))
            | _ -> None

        | "Sqrt", [ x ]  -> t1 x |> Option.map PolarsWrapper.Sqrt
        | "Cbrt", [ x ]  -> t1 x |> Option.map PolarsWrapper.Cbrt
        | "Exp", [ x ]   -> t1 x |> Option.map PolarsWrapper.Exp
        | "Log", [ x ]   -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit Math.E))
        | "Log10", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 10.0))
        | "Log2", [ x ]  -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 2.0))
        | "Log", [ x; newBase ] -> t2 x newBase |> Option.map PolarsWrapper.Log

        | "Sin", [ x ]   -> t1 x |> Option.map PolarsWrapper.Sin
        | "Cos", [ x ]   -> t1 x |> Option.map PolarsWrapper.Cos
        | "Tan", [ x ]   -> t1 x |> Option.map PolarsWrapper.Tan
        | "Asin", [ x ]  -> t1 x |> Option.map PolarsWrapper.ArcSin
        | "Acos", [ x ]  -> t1 x |> Option.map PolarsWrapper.ArcCos
        | "Atan", [ x ]  -> t1 x |> Option.map PolarsWrapper.ArcTan
        | "Sinh", [ x ]  -> t1 x |> Option.map PolarsWrapper.Sinh
        | "Cosh", [ x ]  -> t1 x |> Option.map PolarsWrapper.Cosh
        | "Tanh", [ x ]  -> t1 x |> Option.map PolarsWrapper.Tanh
        | "Asinh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcSinh
        | "Acosh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcCosh
        | "Atanh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcTanh

        | "Pow", [ x; y ]   -> t2 x y |> Option.map PolarsWrapper.Pow
        | "Atan2", [ y; x ] -> t2 y x |> Option.map PolarsWrapper.ArcTan2

        | "ToInt32", [ x ]   -> t1 x |> Option.bind (tryTranslateCast typeof<int>)
        | "ToUInt32", [ x ]  -> t1 x |> Option.bind (tryTranslateCast typeof<uint32>)
        | "ToInt64", [ x ]   -> t1 x |> Option.bind (tryTranslateCast typeof<int64>)
        | "ToUInt64", [ x ]  -> t1 x |> Option.bind (tryTranslateCast typeof<uint64>)
        | "ToSingle", [ x ]  -> t1 x |> Option.bind (tryTranslateCast typeof<float32>)
        | "ToDouble", [ x ]  -> t1 x |> Option.bind (tryTranslateCast typeof<double>)
        | "ToByte", [ x ]    -> t1 x |> Option.bind (tryTranslateCast typeof<byte>)
        | "ToSByte", [ x ]   -> t1 x |> Option.bind (tryTranslateCast typeof<sbyte>)
        | "ToInt16", [ x ]   -> t1 x |> Option.bind (tryTranslateCast typeof<int16>)
        | "ToUInt16", [ x ]  -> t1 x |> Option.bind (tryTranslateCast typeof<uint16>)

        | "PopCount", [ x ] ->
            t1 x |> Option.map PolarsWrapper.BitwiseCountOnes

        | "LeadingZeroCount", [ x ] ->
            t1 x |> Option.map PolarsWrapper.BitwiseLeadingZeros

        | "TrailingZeroCount", [ x ] ->
            t1 x |> Option.map PolarsWrapper.BitwiseTrailingZeros

        | _ -> None

    // =========================================================================
    // 6. Regex Dispatcher (Native Inline Flag Pushdown & UDF Fallback)
    // =========================================================================

    /// Synthesizes Rust-compatible inline flag expressions from .NET RegexOptions.
    /// Returns Some(validPattern) if compatible with Rust regex engine, otherwise None.
    let private tryBuildRustRegexPattern (pat: string) (options: RegexOptions) : string option =
        // Options that alter regex execution semantics unsupported natively by Rust regex
        let unsupported =
            RegexOptions.RightToLeft |||
            RegexOptions.ECMAScript |||
            RegexOptions.ExplicitCapture

        if options.HasFlag unsupported then None
        else
            let mutable flags = ""
            if options.HasFlag RegexOptions.IgnoreCase then flags <- flags + "i"
            if options.HasFlag RegexOptions.Multiline then flags <- flags + "m"
            if options.HasFlag RegexOptions.Singleline then flags <- flags + "s"
            if options.HasFlag RegexOptions.IgnorePatternWhitespace then flags <- flags + "x"

            let candidate =
                if String.IsNullOrEmpty flags then pat
                else sprintf "(?%s:%s)" flags pat

            // Validate that Rust's regex compiler can parse the candidate (e.g. rejects lookaround)
            if PolarsWrapper.RegexIsValid candidate then
                Some candidate
            else
                None

    let private translateRegexInvocation (ctx: TranslationContext) (invocation: RegexInvocation) : ExprHandle option =
        let getRegex rxOpt pat =
            match rxOpt with
            | Some (rx: Regex) -> rx
            | None -> Regex pat

        let getOptions rxOpt =
            match rxOpt with
            | Some (rx: Regex) -> rx.Options
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
                    PolarsWrapper.MapStringTransform(t, Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep))))

        // --- 3. ReplaceEvaluator (Managed delegate: always UDF) ---
        | ReplaceEvaluator(target, pat, eval, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let rx = getRegex rxOpt pat
                PolarsWrapper.MapStringTransform(t, Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, eval))))

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
                    PolarsWrapper.MapStringTransform(t, Func<string, string>(fun s ->
                        if isNull s then null
                        else
                            let m = rx.Match(s)
                            if m.Success && groupIndex < m.Groups.Count then m.Groups.[groupIndex].Value else "")))

        // --- 5. MatchesAll ---
        | MatchesAll(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrExtractAll(t, PolarsWrapper.Lit validPat)
                | None ->
                    let rx = getRegex rxOpt pat
                    PolarsWrapper.MapStringToList(t, Func<string, Collections.Generic.IEnumerable<string>>(fun s ->
                        if isNull s then null
                        else
                            let matches = rx.Matches(s)
                            let list = Collections.Generic.List<string>(matches.Count)
                            for m in matches do list.Add(m.Value)
                            list :> Collections.Generic.IEnumerable<string>)))

        // --- 6. MatchesCount / Regex.Count (.NET 7+) ---
        | MatchesCount(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    let listExpr = PolarsWrapper.StrExtractAll(t, PolarsWrapper.Lit validPat)
                    PolarsWrapper.ListLen listExpr
                | None ->
                    let rx = getRegex rxOpt pat
                    let intDtype = PolarsWrapper.NewPrimitiveType(int PlDataType.Int32)
                    PolarsWrapper.Map(t, Func<Apache.Arrow.IArrowArray, Apache.Arrow.IArrowArray>(fun arr ->
                        let strViewArr = arr :?> Apache.Arrow.StringViewArray
                        let builder = Apache.Arrow.Int32Array.Builder()
                        for i in 0 .. strViewArr.Length - 1 do
                            if strViewArr.IsNull(i) then builder.AppendNull() |> ignore
                            else builder.Append(rx.Matches(strViewArr.GetString(i)).Count) |> ignore
                        builder.Build() :> Apache.Arrow.IArrowArray
                    ), intDtype))

        // --- 7. Split ---
        | Split(target, pat, rxOpt) ->
            tryTranslateWithContext ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrSplit(t, PolarsWrapper.Lit validPat, inclusive = false, literal = false, strict = false)
                | None ->
                    let rx = getRegex rxOpt pat
                    PolarsWrapper.MapStringToList(t, Func<string, seq<string>>(fun s ->
                        if isNull s then null else rx.Split(s) :> seq<string>)))

    // =========================================================================
    // 7. Window, Shift, Diff & Rolling Operations
    // =========================================================================

    let private translateShiftMethod (ctx: TranslationContext) (target: Expression) (args: Expression list) : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target
        match args with
        | [ nExpr ] ->
            let offsetHandleOpt =
                match tryEvaluate nExpr with
                | Some nVal ->
                    try
                        let intVal = Convert.ToInt64 nVal
                        Some (PolarsWrapper.Lit intVal)
                    with _ ->
                        tryTranslateWithContext ctx nExpr
                | None ->
                    tryTranslateWithContext ctx nExpr

            match targetH (), offsetHandleOpt with
            | Some eH, Some nH -> Some (PolarsWrapper.Shift(eH, nH))
            | _ -> None
        | _ -> None

    let private translateDiffMethod (ctx: TranslationContext) (target: Expression) (args: Expression list) : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target
        let resolveN nExpr =
            match tryEvaluate nExpr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some (PolarsWrapper.Lit intVal)
                with _ ->
                    tryTranslateWithContext ctx nExpr
            | None ->
                tryTranslateWithContext ctx nExpr

        match args with
        | [] ->
            targetH () |> Option.map (fun eH ->
                PolarsWrapper.Diff(eH, PolarsWrapper.Lit 1L, PlNullBehavior.Ignore))
        | [ nExpr ] ->
            match targetH (), resolveN nExpr with
            | Some eH, Some nH -> Some (PolarsWrapper.Diff(eH, nH, PlNullBehavior.Ignore))
            | _ -> None
        | [ nExpr; nbExpr ] ->
            match targetH (), resolveN nExpr, tryEvaluate nbExpr with
            | Some eH, Some nH, Some (:? PlNullBehavior as nb) ->
                Some (PolarsWrapper.Diff(eH, nH, nb))
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

    let private translateRankMethod (ctx: TranslationContext) (target: Expression) (args: Expression list) : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target
        match args with
        | [] ->
            targetH () |> Option.map (fun eH ->
                PolarsWrapper.Rank(eH, PlRankMethod.Average, false, Nullable()))

        | [ descExpr ] when descExpr.Type = typeof<bool> ->
            match targetH (), tryEvaluate descExpr with
            | Some eH, Some (:? bool as desc) ->
                Some (PolarsWrapper.Rank(eH, PlRankMethod.Average, desc, Nullable()))
            | _ -> None

        | [ methodExpr ] ->
            match targetH (), tryEvaluate methodExpr with
            | Some eH, Some mObj ->
                let m = tryExtractRankMethod mObj
                Some (PolarsWrapper.Rank(eH, m, false, Nullable()))
            | _ -> None

        | [ methodExpr; descExpr ] ->
            match targetH (), tryEvaluate methodExpr, tryEvaluate descExpr with
            | Some eH, Some mObj, Some (:? bool as desc) ->
                let m = tryExtractRankMethod mObj
                Some (PolarsWrapper.Rank(eH, m, desc, Nullable()))
            | _ -> None

        | [ methodExpr; descExpr; seedExpr ] ->
            match targetH (), tryEvaluate methodExpr, tryEvaluate descExpr, tryEvaluate seedExpr with
            | Some eH, Some mObj, Some (:? bool as desc), Some seedObj ->
                let m = tryExtractRankMethod mObj
                let seedNullable =
                    match seedObj with
                    | :? uint64 as s -> Nullable<uint64>(s)
                    | _ -> Nullable<uint64>()
                Some (PolarsWrapper.Rank(eH, m, desc, seedNullable))
            | _ -> None

        | _ -> None

    let private translatePctChangeMethod (ctx: TranslationContext) (target: Expression) (args: Expression list) : ExprHandle option =
        let targetH () = tryTranslateWithContext ctx target
        match args with
        | [] -> targetH () |> Option.map (fun eH -> PolarsWrapper.PctChange(eH, 1L))
        | [ nExpr ] ->
            match targetH (), tryEvaluate nExpr with
            | Some eH, Some nVal -> Some (PolarsWrapper.PctChange(eH, Convert.ToInt64 nVal))
            | _ -> None
        | _ -> None

    let private translateOverMethod (ctx: TranslationContext) (target: Expression) (args: Expression list) : ExprHandle option =
        match tryTranslateWithContext ctx target with
        | Some targetH ->
            let extractPartitions (exprs: Expression list) : Expression list =
                exprs
                |> List.collect (fun arg ->
                    match arg with
                    | :? NewExpression as ne ->
                        ne.Arguments |> Seq.toList
                    | :? NewArrayExpression as nae ->
                        nae.Expressions |> Seq.toList
                    | other -> [ other ]
                )

            let flattenedArgs = extractPartitions args
            let partitionHandles =
                flattenedArgs
                |> List.choose (fun arg ->
                    match arg with
                    | :? LambdaExpression as lam -> tryTranslateWithContext (TranslationContext.Entity lam.Parameters.[0].Name) lam.Body
                    | other -> tryTranslateWithContext ctx other
                )

            if partitionHandles.Length > 0 && partitionHandles.Length = flattenedArgs.Length then
                Some (PolarsWrapper.Over(
                    targetH,
                    Array.ofList partitionHandles,
                    [||],
                    descending = false,
                    nullsLast = false,
                    multithreaded = true,
                    maintainOrder = true,
                    mappingCode = PlWindowMapping.GroupsToRows
                ))
            else
                None
        | None -> None

    let private translateScalarReductionMethod (ctx: TranslationContext) (m: MethodInfo) (target: Expression) (args: Expression list) : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | ("Sum" | "Mean" | "Average" | "Min" | "Max" | "Median" | "First" | "Last"), [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.bind (translateReductionOp m.Name)

        | "Count", [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.map PolarsWrapper.Count

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

    let private translateCumulativeMethod (ctx: TranslationContext) (m: MethodInfo) (target: Expression) (args: Expression list) : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then args.Head, args.Tail
            else target, args

        let reverse =
            match effectiveArgs with
            | [ revExpr ] ->
                match tryEvaluate revExpr with
                | Some (:? bool as r) -> r
                | _ -> false
            | _ -> false

        tryTranslateWithContext ctx effectiveTarget
        |> Option.bind (fun colH ->
            match m.Name with
            | "CumSum"   -> Some (PolarsWrapper.CumSum(colH, reverse))
            | "CumMax"   -> Some (PolarsWrapper.CumMax(colH, reverse))
            | "CumMin"   -> Some (PolarsWrapper.CumMin(colH, reverse))
            | "CumProd"  -> Some (PolarsWrapper.CumProd(colH, reverse))
            | "CumCount" -> Some (PolarsWrapper.CumCount(colH, reverse))
            | _          -> None)

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

    let private translateInterpolateMethod (ctx: TranslationContext) (m: MethodInfo) (target: Expression) (args: Expression list) : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then args.Head, args.Tail
            else target, args

        match m.Name, effectiveArgs with
        | "Interpolate", [] ->
            tryTranslateWithContext ctx effectiveTarget
            |> Option.map (fun h -> PolarsWrapper.Interpolate(h, PlInterpolationMethod.Linear))

        | "Interpolate", [ methodExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate methodExpr with
            | Some targetH, Some methodVal ->
                let method = tryExtractInterpolationMethod methodVal
                Some (PolarsWrapper.Interpolate(targetH, method))
            | Some targetH, None ->
                Some (PolarsWrapper.Interpolate(targetH, PlInterpolationMethod.Linear))
            | _ -> None

        | "InterpolateBy", [ byExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryTranslateWithContext ctx byExpr with
            | Some targetH, Some byH ->
                Some (PolarsWrapper.InterpolateBy(targetH, byH))
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

    let internal translateQuantileMethod (ctx: TranslationContext) (target: Expression) (args: Expression list) : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then args.Head, args.Tail
            else target, args

        match effectiveArgs with
        | [ qExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate qExpr with
            | Some targetH, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some (PolarsWrapper.Quantile(targetH, q, PlQuantileMethod.Nearest))
                with _ -> None
            | _ -> None

        | [ qExpr; methodExpr ] ->
            match tryTranslateWithContext ctx effectiveTarget, tryEvaluate qExpr, tryEvaluate methodExpr with
            | Some targetH, Some qVal, Some mVal ->
                try
                    let q = Convert.ToDouble qVal
                    let method = tryExtractQuantileMethod mVal
                    Some (PolarsWrapper.Quantile(targetH, q, method))
                with _ -> None
            | _ -> None

        | _ -> None

    let private translateMethodCall (ctx: TranslationContext) (m: MethodInfo) (target: Expression) (args: Expression list) : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && args.Length > 0 then
                args.Head, args.Tail
            else
                target, args

        if not (isNull effectiveTarget) && m.Name = "Shift" then
            translateShiftMethod ctx effectiveTarget effectiveArgs

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

        elif not (isNull effectiveTarget) && (m.Name = "Interpolate" || m.Name = "InterpolateBy") then
            translateInterpolateMethod ctx m effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) &&
            (m.Name = "CumSum" || m.Name = "CumMax" || m.Name = "CumMin" || m.Name = "CumProd" || m.Name = "CumCount") then
            translateCumulativeMethod ctx m effectiveTarget effectiveArgs

        elif m.Name = "Sum" || m.Name = "Mean" || m.Name = "Average" ||
             m.Name = "Min" || m.Name = "Max"  || m.Name = "Count"   ||
             m.Name = "Median" || m.Name = "First" || m.Name = "Last" ||
             m.Name = "Std" || m.Name = "StdDev" || m.Name = "Var" || m.Name = "Variance" then
            translateScalarReductionMethod ctx m target args

        // 1. String methods (Static & Instance)
        elif m.DeclaringType = typeof<string> || not (isNull target) && target.Type = typeof<string> then
            translateStringMethod ctx m target args

        // 2. Temporal methods (DateTime / DateOnly / TimeOnly)
        elif m.DeclaringType = typeof<DateTime> || not (isNull target) && isTemporalType target.Type then
            translateTemporalMethod ctx m target args

        // 3. Math / MathF / Decimal methods (All static)
        elif isNumericDeclaringType m.DeclaringType && isNull target then
            translateNumericMethod ctx m args

        // 4. Decimal / Numeric Instance Methods (e.g. d.ToString())
        elif not (isNull target) && target.Type = typeof<decimal> then
            match m.Name, args with
            | "ToString", [] ->
                tryTranslateWithContext ctx target
                |> Option.bind (tryTranslateCast typeof<string>)

            | "ToString", [ formatExpr ] ->
                match tryTranslateWithContext ctx target, tryEvaluate formatExpr with
                | Some t, Some (:? string as fmt) ->
                    if fmt.StartsWith("F", StringComparison.OrdinalIgnoreCase) then
                        let precision = fmt.Substring(1)
                        let rustFmt = sprintf "{:.%sf}" precision
                        Some (PolarsWrapper.FormatString(rustFmt, [| t |]))
                    else
                        None
                | _ -> None
            | _ -> None

        else
            None

    // =========================================================================
    // 8. List / Array Row-Level Expression Dispatcher
    // =========================================================================

    let private translateListInvocation (ctx: TranslationContext) (invocation: ListInvocation) : ExprHandle option =
        let t1 expr = tryTranslateWithContext ctx expr
        let t2 e1 e2 =
            match tryTranslateWithContext ctx e1, tryTranslateWithContext ctx e2 with
            | Some l, Some r -> Some (l, r)
            | _ -> None

        match invocation with
        | ListLen target -> t1 target |> Option.map PolarsWrapper.ListLen
        | ListContains(target, itemExpr) ->
            t2 target itemExpr |> Option.map (fun (t, item) -> PolarsWrapper.ListContains(t, item, nullsEqual = false))
        | ListGet(target, indexExpr) ->
            t2 target indexExpr |> Option.map (fun (t, idx) -> PolarsWrapper.ListGet(t, idx, nullOnOob = true))
        | ListFirst target ->
            t1 target |> Option.map (fun t -> PolarsWrapper.ListGet(t, PolarsWrapper.Lit 0, nullOnOob = true))
        | ListLast target ->
            t1 target |> Option.map (fun t -> PolarsWrapper.ListGet(t, PolarsWrapper.Lit -1, nullOnOob = true))
        | ListSum target  -> t1 target |> Option.map PolarsWrapper.ListSum
        | ListMean target -> t1 target |> Option.map PolarsWrapper.ListMean
        | ListMin target  -> t1 target |> Option.map PolarsWrapper.ListMin
        | ListMax target  -> t1 target |> Option.map PolarsWrapper.ListMax

        | ListJoin(target, separatorExpr) ->
            match t1 target, tryEvaluate separatorExpr with
            | Some t, Some (:? string as sep) -> Some (PolarsWrapper.ListJoin(t, sep, ignoreNulls = true))
            | _ -> None

        | ListHead(target, countExpr) -> t2 target countExpr |> Option.map PolarsWrapper.ListHead
        | ListTail(target, countExpr) -> t2 target countExpr |> Option.map PolarsWrapper.ListTail

        | ListSlice(target, offsetExpr, lengthOpt) ->
            match t2 target offsetExpr with
            | Some (t, offset) ->
                match lengthOpt with
                | Some lenExpr -> t1 lenExpr |> Option.map (fun len -> PolarsWrapper.ListSlice(t, offset, len))
                | None -> Some (PolarsWrapper.ListSlice(t, offset, PolarsWrapper.Lit Int64.MaxValue))
            | None -> None

        | ListUnique target  -> t1 target |> Option.map (fun t -> PolarsWrapper.ListEval(t, PolarsWrapper.ExprUnique(PolarsWrapper.Col(""))))
        | ListReverse target -> t1 target |> Option.map (fun t -> PolarsWrapper.ListEval(t, PolarsWrapper.Reverse(PolarsWrapper.Col(""))))
        | ListSort(target, descending) ->
            t1 target |> Option.map (fun t -> PolarsWrapper.ListSort(t, descending = descending, nullsLast = false, maintainOrder = false))

        | ListSetIntersection(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetIntersection
        | ListSetUnion(target, otherExpr)        -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetUnion
        | ListSetDifference(target, otherExpr)   -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetDifference

        | ListAny(target, None) ->
            t1 target |> Option.map (fun t -> PolarsWrapper.Gt(PolarsWrapper.ListLen t, PolarsWrapper.Lit 0))

        | ListAny(target, Some lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.Entity lam.Parameters.[0].Name
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
                let innerCtx = TranslationContext.Entity lam.Parameters.[0].Name
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
                let innerCtx = TranslationContext.Entity lam.Parameters.[0].Name
                tryTranslateWithContext innerCtx lam.Body
                |> Option.map (fun mapperExpr -> PolarsWrapper.ListEval(t, mapperExpr))
            | None -> None
        | ListSelect _ -> None

    // =========================================================================
    // 9. Operators & Casts
    // =========================================================================

    let internal translateBinary (nodeType: ExpressionType) (left: ExprHandle) (right: ExprHandle) : ExprHandle =
        match nodeType with
        | ExpressionType.Equal              -> PolarsWrapper.Eq(left, right)
        | ExpressionType.NotEqual           -> PolarsWrapper.Neq(left, right)
        | ExpressionType.GreaterThan        -> PolarsWrapper.Gt(left, right)
        | ExpressionType.GreaterThanOrEqual -> PolarsWrapper.GtEq(left, right)
        | ExpressionType.LessThan           -> PolarsWrapper.Lt(left, right)
        | ExpressionType.LessThanOrEqual    -> PolarsWrapper.LtEq(left, right)
        | ExpressionType.Add                -> PolarsWrapper.Add(left, right)
        | ExpressionType.Subtract           -> PolarsWrapper.Sub(left, right)
        | ExpressionType.Multiply           -> PolarsWrapper.Mul(left, right)
        | ExpressionType.Divide             -> PolarsWrapper.Div(left, right)
        | ExpressionType.Modulo             -> PolarsWrapper.Rem(left, right)
        | ExpressionType.Power              -> PolarsWrapper.Pow(left, right)
        | ExpressionType.And
        | ExpressionType.AndAlso            -> PolarsWrapper.And(left, right)
        | ExpressionType.Or
        | ExpressionType.OrElse             -> PolarsWrapper.Or(left, right)
        | ExpressionType.ExclusiveOr        -> PolarsWrapper.Xor(left, right)
        | ExpressionType.Coalesce           -> PolarsWrapper.Coalesce [| left; right |]
        | other ->
            failwithf "Binary operator '%A' is not currently supported in Polars LINQ" other

    let private translateUnary (nodeType: ExpressionType) (operand: ExprHandle) : ExprHandle option =
        match nodeType with
        | ExpressionType.Not          -> Some (PolarsWrapper.Not operand)
        | ExpressionType.Negate
        | ExpressionType.NegateChecked -> Some (PolarsWrapper.Neg operand)
        | ExpressionType.UnaryPlus    -> Some operand
        | _ -> None

    let private translateBinaryExpr (ctx: TranslationContext) (op: ExpressionType) (left: Expression) (right: Expression) : ExprHandle option =
        if op = ExpressionType.NotEqual && isNullConstantExpr right then
            tryTranslateWithContext ctx left |> Option.map PolarsWrapper.IsNotNull
        elif op = ExpressionType.NotEqual && isNullConstantExpr left then
            tryTranslateWithContext ctx right |> Option.map PolarsWrapper.IsNotNull
        elif op = ExpressionType.Equal && isNullConstantExpr right then
            tryTranslateWithContext ctx left |> Option.map PolarsWrapper.IsNull
        elif op = ExpressionType.Equal && isNullConstantExpr left then
            tryTranslateWithContext ctx right |> Option.map PolarsWrapper.IsNull
        elif op = ExpressionType.LeftShift then
            match tryTranslateWithContext ctx left, tryEvaluate right with
            | Some leftH, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some (PolarsWrapper.BitLeftShift(leftH, n))
                with _ -> None
            | _ -> None

        elif op = ExpressionType.RightShift then
            match tryTranslateWithContext ctx left, tryEvaluate right with
            | Some leftH, Some nVal ->
                try
                    let n = Convert.ToInt32 nVal
                    Some (PolarsWrapper.BitRightShift(leftH, n))
                with _ -> None
            | _ -> None
        // Intercept String concatenation (+) to synthesize native FormatString
        elif op = ExpressionType.Add && (left.Type = typeof<string> || right.Type = typeof<string>) then
            match tryTranslateWithContext ctx left, tryTranslateWithContext ctx right with
            | Some l, Some r ->
                Some (PolarsWrapper.FormatString("{}{}", [| l; r |]))
            | _ -> None
        else
            match tryTranslateWithContext ctx left, tryTranslateWithContext ctx right with
            | Some l, Some r -> Some (translateBinary op l r)
            | _ -> None

    let private translateUnaryExpr (ctx: TranslationContext) (op: ExpressionType) (operand: Expression) (targetType: Type) : ExprHandle option =
        match op with
        | ExpressionType.Convert
        | ExpressionType.ConvertChecked ->
            tryTranslateWithContext ctx operand
            |> Option.bind (tryTranslateCast targetType)
        | _ ->
            tryTranslateWithContext ctx operand
            |> Option.bind (translateUnary op)

    // =========================================================================
    // 10. Main Entrypoint Dispatcher
    // =========================================================================

    /// Attempts to translate an Expression AST node to a native Polars ExprHandle using a strongly-typed TranslationContext.
    let tryTranslateWithContext (ctx: TranslationContext) (expr: Expression) : ExprHandle option =
        try
            match expr with
            // 1. Row-level List / Array operations
            | ArrayIndexOp(target, indexExpr) ->
                match tryTranslateWithContext ctx target, tryTranslateWithContext ctx indexExpr with
                | Some t, Some idx -> Some (PolarsWrapper.ListGet(t, idx, nullOnOob = true))
                | _ -> None

            | ListInvocation listInv ->
                translateListInvocation ctx listInv

            // 2. Struct field pushdown
            | StructFieldAccess(parentExpr, fieldName) ->
                tryTranslateWithContext ctx parentExpr
                |> Option.map (fun parentHandle -> PolarsWrapper.StructFieldByName(parentHandle, [| fieldName |]))

            // 3. Regex operations
            | RegexInvocation invocation ->
                translateRegexInvocation ctx invocation

            // 4. Member Access (Columns, temporal/time properties, Math constants, captured closures)
            | MemberAccess _ as memberExpr ->
                translateMemberAccess ctx memberExpr

            // 5. Method Calls (Strings, DateTime, Math, etc.)
            | MethodCall(m, target, args) ->
                translateMethodCall ctx m target args

            // 6. Binary operations
            | Binary(op, left, right) ->
                translateBinaryExpr ctx op left right

            // 7. Unary operations & conversions
            | Unary(op, operand) ->
                translateUnaryExpr ctx op operand expr.Type

            // 8. Conditional ternary (test ? true : false)
            | :? ConditionalExpression as cond ->
                match tryTranslateWithContext ctx cond.Test,
                      tryTranslateWithContext ctx cond.IfTrue,
                      tryTranslateWithContext ctx cond.IfFalse with
                | Some testH, Some trueH, Some falseH ->
                    Some (PolarsWrapper.IfElse(testH, trueH, falseH))
                | _ -> None

            // 9. Constants
            | Constant(value, t) ->
                Some (toLiteralHandle value t)

            // 10. Parameter mapping: delegate physical column resolution directly to TranslationContext
            | :? ParameterExpression as p ->
                ctx.TryResolveParameterHandle p.Name

            | _ -> None
        with _ ->
            None
