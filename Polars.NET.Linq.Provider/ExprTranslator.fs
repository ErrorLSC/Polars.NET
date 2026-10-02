namespace Polars.NET.Linq.Provider

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Core.Helpers
open Apache.Arrow.Types

module ExprTranslator =

    let rec private isNullConstantExpr (e: Expression) =
        match e with
        | null -> true
        | :? ConstantExpression as c -> isNull c.Value
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert || 
                                       u.NodeType = ExpressionType.ConvertChecked || 
                                       u.NodeType = ExpressionType.Quote ->
            isNullConstantExpr u.Operand
        | _ -> false

    let timeUnitMap (unit: Apache.Arrow.Types.TimeUnit) : byte =
        match unit with
        | TimeUnit.Second -> PlTimeUnit.Second |> byte
        | TimeUnit.Millisecond -> PlTimeUnit.Milliseconds |> byte
        | TimeUnit.Microsecond -> PlTimeUnit.Microseconds |> byte
        | TimeUnit.Nanosecond -> PlTimeUnit.Nanoseconds |> byte
        | _ -> invalidArg (nameof unit) "Unknown TimeUnit"

    // Maps an Arrow IArrowType to a Polars Native DataTypeHandle
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
        | _ ->
            None

    /// Maps a CLR Type directly to native DataTypeHandle via existing ArrowTypeResolver
    let private tryGetPolarsDataType (t: Type) : DataTypeHandle option =
        try
            (ArrowTypeResolver.GetArrowTypeFromNetType >> arrowTypeToPolarsDataType) t
        with _ ->
            None

    /// Translates constant values to native literal ExprHandle
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
                | :? string as s -> PolarsWrapper.Lit s
                | :? DateTime as dt -> PolarsWrapper.Lit dt
                | :? DateTimeOffset as doff -> PolarsWrapper.Lit doff
                | :? TimeSpan as ts -> PolarsWrapper.Lit ts
                | :? DateOnly as dof -> PolarsWrapper.Lit dof
                | :? TimeOnly as tof -> PolarsWrapper.Lit tof
                | other ->
                    failwithf "Type '%s' is not supported as a constant literal in Polars LINQ" (other.GetType().FullName)

    /// Translates binary operators to native Expr binary expressions
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
        | other ->
            failwithf "Binary operator '%A' is not currently supported in Polars LINQ" other

    /// Translates unary operators to native Expr unary expressions
    let private translateUnary (nodeType: ExpressionType) (operand: ExprHandle) : ExprHandle option =
        match nodeType with
        | ExpressionType.Not -> Some (PolarsWrapper.Not operand)
        | ExpressionType.Negate
        | ExpressionType.NegateChecked -> Some (PolarsWrapper.Neg operand)
        | ExpressionType.UnaryPlus -> Some operand
        | _ -> None

    /// Translates explicit and implicit casting nodes into native Polars ExprCast
    let private tryTranslateCast (targetType: Type) (operand: ExprHandle) : ExprHandle option =
        if targetType = typeof<obj> then
            Some operand
        else
            match tryGetPolarsDataType targetType with
            | Some dtypeHandle ->
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType(dtypeHandle)
                Some (PolarsWrapper.ExprCast(operand, dtypeExpr, strict = false, wrapNumerical = false))
            | None -> None

    let private isFSharpAnonymousMember (m: MemberInfo) =
        m.DeclaringType.Name.StartsWith "AnonymousObject" || 
        m.DeclaringType.Name.StartsWith "Tuple" ||
        m.DeclaringType.Name.Contains "TransparentIdentifier"

    let rec internal tryResolveColumnName (paramName: string) (expr: Expression) : string option =
        match expr with
        // Case 1: Accessing the tuple wrapper itself (e.g., _arg1.Item1) -> not a column
        | MemberAccess(_, m) when isFSharpAnonymousMember m ->
            None

        // Case 2: Accessing real entity properties (e.g., e.Name, tupledArg.Item1.DeptName)
        | MemberAccess(inner, m) ->
            let rec isRootedInParamOrClosure (e: Expression) =
                match e with
                | :? ParameterExpression as p ->
                    p.Name = paramName || 
                    p.Name.StartsWith "_arg" || 
                    p.Name.StartsWith "tupled" ||
                    p.Type.Name.StartsWith "AnonymousObject" ||
                    p.Type.Name.StartsWith "Tuple"
                | MemberAccess(nextInner, nextM) when isFSharpAnonymousMember nextM ->
                    isRootedInParamOrClosure nextInner
                | _ -> false

            if isRootedInParamOrClosure inner then
                Some m.Name
            else
                None

        | _ -> None

    /// Attempts to translate an Expression AST node to a native Polars ExprHandle.
    let rec tryTranslate (paramName: string) (expr: Expression) : ExprHandle option =
        try
            match expr with
            // 1. Column Reference: (tupledArg.Item1.DeptName / _arg1.Item1.DeptId)
            | MemberAccess _ as memberExpr when (tryResolveColumnName paramName memberExpr).IsSome ->
                let actualCol = (tryResolveColumnName paramName memberExpr).Value
                Some (PolarsWrapper.Col(actualCol))

            // 2. Math Constants (Math.PI, Math.E)
            | MemberAccess(null, m) when m.DeclaringType = typeof<Math> || m.DeclaringType = typeof<MathF> ->
                match m.Name with
                | "PI"  -> Some (PolarsWrapper.Lit Math.PI)
                | "E"   -> Some (PolarsWrapper.Lit Math.E)
                | "Tau" -> Some (PolarsWrapper.Lit (Math.PI * 2.0))
                | _     -> None

            // 3. String Member Access: s.Length -> StrLenChars
            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<string> && m.Name = "Length" ->
                tryTranslate paramName target |> Option.map PolarsWrapper.StrLenChars

            // 4. DateTime / DateOnly / TimeOnly Member Access Pushdown
            | MemberAccess(target, m) when not (isNull target) && 
                (target.Type = typeof<DateTime> || 
                 target.Type = typeof<DateTimeOffset> || 
                 target.Type = typeof<DateOnly> || 
                 target.Type = typeof<TimeOnly>) ->

                let translateTarget () = tryTranslate paramName target
                match m.Name with
                | "Year"        -> translateTarget () |> Option.map PolarsWrapper.DtYear
                | "Month"       -> translateTarget () |> Option.map PolarsWrapper.DtMonth
                | "Day"         -> translateTarget () |> Option.map PolarsWrapper.DtDay
                | "DayOfYear"   -> translateTarget () |> Option.map PolarsWrapper.DtOrdinalDay
                | "DayOfWeek"   -> translateTarget () |> Option.map PolarsWrapper.DtWeekday
                | "Hour"        -> translateTarget () |> Option.map PolarsWrapper.DtHour
                | "Minute"      -> translateTarget () |> Option.map PolarsWrapper.DtMinute
                | "Second"      -> translateTarget () |> Option.map PolarsWrapper.DtSecond
                | "Millisecond" -> translateTarget () |> Option.map PolarsWrapper.DtMillisecond
                | "Microsecond" -> translateTarget () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanosecond"  -> translateTarget () |> Option.map PolarsWrapper.DtNanosecond
                | "Date"        -> translateTarget () |> Option.map PolarsWrapper.DtDate
                | "TimeOfDay"   -> translateTarget () |> Option.map PolarsWrapper.DtTime
                | _             -> None

            // 5. TimeSpan (Duration) Member Access Pushdown
            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<TimeSpan> ->
                let translateTarget () = tryTranslate paramName target
                match m.Name with
                | "TotalDays"         -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalDays(t, fractional = true))
                | "TotalHours"        -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalHours(t, fractional = true))
                | "TotalMinutes"      -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalMinutes(t, fractional = true))
                | "TotalSeconds"      -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalSeconds(t, fractional = true))
                | "TotalMilliseconds" -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalMilliseconds(t, fractional = true))
                | "TotalMicroseconds" -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalMicroseconds(t, fractional = true))
                | "TotalNanoseconds"  -> translateTarget () |> Option.map (fun t -> PolarsWrapper.DtTotalNanoseconds(t, fractional = true))
                | "Days"              -> translateTarget () |> Option.map PolarsWrapper.DtDay
                | "Hours"             -> translateTarget () |> Option.map PolarsWrapper.DtHour
                | "Minutes"           -> translateTarget () |> Option.map PolarsWrapper.DtMinute
                | "Seconds"           -> translateTarget () |> Option.map PolarsWrapper.DtSecond
                | "Milliseconds"      -> translateTarget () |> Option.map PolarsWrapper.DtMillisecond
                | "Microseconds"      -> translateTarget () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanoseconds"       -> translateTarget () |> Option.map PolarsWrapper.DtNanosecond
                | _                   -> None

            // 6. Closures / Captured Variables (Evaluated safely via tryEvaluate)
            | MemberAccess _ as memberExpr ->
                match tryEvaluate memberExpr with
                | Some value -> Some (toLiteralHandle value memberExpr.Type)
                | None -> None

            // 7. Constant Literals
            | Constant(value, t) ->
                Some (toLiteralHandle value t)

            // 8. Static String Methods (string.IsNullOrEmpty, string.IsNullOrWhiteSpace)
            | MethodCall(m, null, [ arg ]) when m.DeclaringType = typeof<string> ->
                match m.Name with
                | "IsNullOrEmpty" ->
                    match tryTranslate paramName arg with
                    | Some exprH ->
                        // CRITICAL: Clone BEFORE passing exprH to IsNull, because IsNull transfers ownership!
                        let cloned = PolarsWrapper.CloneExpr exprH
                        let isNull = PolarsWrapper.IsNull exprH
                        let isEmpty = PolarsWrapper.Eq(cloned, PolarsWrapper.Lit "")
                        Some (PolarsWrapper.Or(isNull, isEmpty))
                    | None -> None

                | "IsNullOrWhiteSpace" ->
                    match tryTranslate paramName arg with
                    | Some exprH ->
                        // CRITICAL: Clone BEFORE passing exprH to IsNull
                        let cloned = PolarsWrapper.CloneExpr exprH
                        let isNull = PolarsWrapper.IsNull exprH
                        let stripped = PolarsWrapper.StrStripChars(cloned, PolarsWrapper.LitNull())
                        let isEmpty = PolarsWrapper.Eq(stripped, PolarsWrapper.Lit "")
                        Some (PolarsWrapper.Or(isNull, isEmpty))
                    | None -> None

                | _ -> None

            // =========================================================================
            // 8.1 Static Regex Method Calls (Regex.IsMatch, Regex.Replace)
            // =========================================================================
            | MethodCall(m, null, [ target; patternExpr ]) 
                when m.DeclaringType = typeof<System.Text.RegularExpressions.Regex> && m.Name = "IsMatch" ->

                let translateTarget () = tryTranslate paramName target

                match translateTarget(), tryEvaluate patternExpr with
                | Some t, Some (:? string as pat) ->
                    if PolarsWrapper.RegexIsValid(pat) then
                        // 1. Rust Native Pushdown (Strictly linear-time, SIMD vectorized)
                        Some (PolarsWrapper.StrContains(t, PolarsWrapper.Lit pat, literal = false, strict = false))
                    else
                        // 2. In-Graph Arrow Chunk UDF Pushdown (Handles Lookarounds / SG Regex via .NET)
                        let rx = System.Text.RegularExpressions.Regex(pat)
                        let predicate = Func<string, bool>(fun s -> rx.IsMatch(s))
                        Some (PolarsWrapper.MapStringPredicate(t, predicate))
                | _ -> None

            | MethodCall(m, null, [ target; patternExpr; replaceExpr ]) 
                when m.DeclaringType = typeof<System.Text.RegularExpressions.Regex> && m.Name = "Replace" ->

                let translateTarget () = tryTranslate paramName target

                match translateTarget(), tryEvaluate patternExpr, tryEvaluate replaceExpr with
                | Some t, Some (:? string as pat), Some (:? string as rep) ->
                    if PolarsWrapper.RegexIsValid(pat) then
                        // 1. Rust Native Pushdown
                        Some (PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit pat, PolarsWrapper.Lit rep, literal = false))
                    else
                        // 2. In-Graph Arrow Chunk UDF Pushdown
                        let rx = System.Text.RegularExpressions.Regex(pat)
                        let transform = Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep))
                        Some (PolarsWrapper.MapStringTransform(t, transform))
                | _ -> None

            // =========================================================================
            // 8.2 Instance Regex & Source Generated Regex (rx.IsMatch(s), rx.Replace(s, "..."))
            // =========================================================================
            | MethodCall(m, regexInstanceExpr, [ target ]) 
                when not (isNull regexInstanceExpr) && 
                     typeof<System.Text.RegularExpressions.Regex>.IsAssignableFrom(regexInstanceExpr.Type) && 
                     m.Name = "IsMatch" ->

                let translateTarget () = tryTranslate paramName target

                match translateTarget(), tryEvaluate regexInstanceExpr with
                | Some t, Some (:? System.Text.RegularExpressions.Regex as rx) ->
                    let pat = rx.ToString()
                    if PolarsWrapper.RegexIsValid(pat) then
                        // 1. Rust Native Pushdown
                        Some (PolarsWrapper.StrContains(t, PolarsWrapper.Lit pat, literal = false, strict = false))
                    else
                        // 2. In-Graph Arrow Chunk UDF Pushdown (Preserves SG Regex performance)
                        let predicate = Func<string, bool>(fun s -> rx.IsMatch(s))
                        Some (PolarsWrapper.MapStringPredicate(t, predicate))
                | _ -> None

            | MethodCall(m, regexInstanceExpr, [ target; replaceExpr ]) 
                when not (isNull regexInstanceExpr) && 
                     typeof<System.Text.RegularExpressions.Regex>.IsAssignableFrom(regexInstanceExpr.Type) && 
                     m.Name = "Replace" ->

                let translateTarget () = tryTranslate paramName target

                match translateTarget(), tryEvaluate regexInstanceExpr, tryEvaluate replaceExpr with
                | Some t, Some (:? System.Text.RegularExpressions.Regex as rx), Some (:? string as rep) ->
                    let pat = rx.ToString()
                    if PolarsWrapper.RegexIsValid(pat) then
                        // 1. Rust Native Pushdown
                        Some (PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit pat, PolarsWrapper.Lit rep, literal = false))
                    else
                        // 2. In-Graph Arrow Chunk UDF Pushdown
                        let transform = Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep))
                        Some (PolarsWrapper.MapStringTransform(t, transform))
                | _ -> None

            // 9. Instance String Methods Pushdown
            | MethodCall(m, target, args) when not (isNull target) && target.Type = typeof<string> ->
                let translateTarget () = tryTranslate paramName target
                match m.Name, args with
                | "Contains", [ arg ] ->
                    match translateTarget(), tryTranslate paramName arg with
                    | Some t, Some p -> Some (PolarsWrapper.StrContains(t, p, literal = true, strict = false))
                    | _ -> None
                | "StartsWith", [ arg ] ->
                    match translateTarget(), tryTranslate paramName arg with
                    | Some t, Some p -> Some (PolarsWrapper.StrStartsWith(t, p))
                    | _ -> None
                | "EndsWith", [ arg ] ->
                    match translateTarget(), tryTranslate paramName arg with
                    | Some t, Some p -> Some (PolarsWrapper.StrEndsWith(t, p))
                    | _ -> None
                | "ToUpper", [] | "ToUpperInvariant", [] ->
                    translateTarget() |> Option.map PolarsWrapper.StrToUpper
                | "ToLower", [] | "ToLowerInvariant", [] ->
                    translateTarget() |> Option.map PolarsWrapper.StrToLower
                | "Substring", [ startExpr ] ->
                    match translateTarget(), tryTranslate paramName startExpr with
                    | Some t, Some startH -> Some (PolarsWrapper.StrSlice(t, startH, PolarsWrapper.LitNull()))
                    | _ -> None
                | "Substring", [ startExpr; lenExpr ] ->
                    match translateTarget(), tryTranslate paramName startExpr, tryTranslate paramName lenExpr with
                    | Some t, Some startH, Some lenH -> Some (PolarsWrapper.StrSlice(t, startH, lenH))
                    | _ -> None
                | "Replace", [ oldExpr; newExpr ] ->
                    match translateTarget(), tryTranslate paramName oldExpr, tryTranslate paramName newExpr with
                    | Some t, Some o, Some n -> Some (PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
                    | _ -> None
                | "Trim", [] ->
                    translateTarget() |> Option.map (fun t -> PolarsWrapper.StrStripChars(t, PolarsWrapper.LitNull()))
                | "TrimStart", [] ->
                    translateTarget() |> Option.map (fun t -> PolarsWrapper.StrStripCharsStart(t, PolarsWrapper.LitNull()))
                | "TrimEnd", [] ->
                    translateTarget() |> Option.map (fun t -> PolarsWrapper.StrStripCharsEnd(t, PolarsWrapper.LitNull()))
                | _ -> None

            // 10. DateTime Instance Methods Pushdown
            | MethodCall(m, target, args) when not (isNull target) && 
                (target.Type = typeof<DateTime> || 
                 target.Type = typeof<DateTimeOffset> || 
                 target.Type = typeof<DateOnly> || 
                 target.Type = typeof<TimeOnly>) ->

                let translateTarget () = tryTranslate paramName target
                match m.Name, args with
                | "ToString", [ formatExpr ] ->
                    match translateTarget(), tryEvaluate formatExpr with
                    | Some t, Some (:? string as netFormat) ->
                        let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                        if not (String.IsNullOrEmpty chronoFormat) then
                            Some (PolarsWrapper.DtToString(t, chronoFormat))
                        else None
                    | _ -> None
                | "AddDays", [ daysExpr ] ->
                    match translateTarget(), tryEvaluate daysExpr with
                    | Some t, Some d ->
                        let offsetStr = sprintf "%gd" (Convert.ToDouble d)
                        Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit offsetStr))
                    | _ -> None
                | "AddHours", [ hoursExpr ] ->
                    match translateTarget(), tryEvaluate hoursExpr with
                    | Some t, Some h ->
                        let offsetStr = sprintf "%gh" (Convert.ToDouble h)
                        Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit offsetStr))
                    | _ -> None
                | "AddMinutes", [ minsExpr ] ->
                    match translateTarget(), tryEvaluate minsExpr with
                    | Some t, Some mVal ->
                        let offsetStr = sprintf "%gm" (Convert.ToDouble mVal)
                        Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit offsetStr))
                    | _ -> None
                | "AddSeconds", [ secsExpr ] ->
                    match translateTarget(), tryEvaluate secsExpr with
                    | Some t, Some sVal ->
                        let offsetStr = sprintf "%gs" (Convert.ToDouble sVal)
                        Some (PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit offsetStr))
                    | _ -> None
                | _ -> None

            // 11. Static DateTime Methods (e.g. DateTime.IsLeapYear)
            | MethodCall(m, null, [ yearExpr ]) when m.DeclaringType = typeof<DateTime> && m.Name = "IsLeapYear" ->
                match tryTranslate paramName yearExpr with
                | Some yearH ->
                    let dateStrExpr = PolarsWrapper.FormatString("{}-01-01", [| yearH |])
                    let dtExpr = PolarsWrapper.StrToDate(dateStrExpr, "%Y-%m-%d", strict = false, exact = true, cache = true)
                    Some (PolarsWrapper.DtIsLeapYear dtExpr)
                | None -> None

            // 12. Math / MathF Static Methods Pushdown
            | MethodCall(m, null, args) when m.DeclaringType = typeof<Math> || m.DeclaringType = typeof<MathF> ->
                match m.Name, args with
                | "Abs", [ x ]   -> tryTranslate paramName x |> Option.map PolarsWrapper.Abs
                | "Sqrt", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.Sqrt
                | "Cbrt", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.Cbrt
                | "Exp", [ x ]   -> tryTranslate paramName x |> Option.map PolarsWrapper.Exp
                | "Log", [ x ]   -> 
                    tryTranslate paramName x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit Math.E))
                | "Log10", [ x ] -> 
                    tryTranslate paramName x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 10.0))
                | "Log2", [ x ]  -> 
                    tryTranslate paramName x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 2.0))
                | "Ceiling", [ x ] -> tryTranslate paramName x |> Option.map PolarsWrapper.Ceil
                | "Floor", [ x ]   -> tryTranslate paramName x |> Option.map PolarsWrapper.Floor
                | "Sign", [ x ]    -> tryTranslate paramName x |> Option.map PolarsWrapper.Sign

                | "Sin", [ x ]   -> tryTranslate paramName x |> Option.map PolarsWrapper.Sin
                | "Cos", [ x ]   -> tryTranslate paramName x |> Option.map PolarsWrapper.Cos
                | "Tan", [ x ]   -> tryTranslate paramName x |> Option.map PolarsWrapper.Tan
                | "Asin", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.ArcSin
                | "Acos", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.ArcCos
                | "Atan", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.ArcTan
                | "Sinh", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.Sinh
                | "Cosh", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.Cosh
                | "Tanh", [ x ]  -> tryTranslate paramName x |> Option.map PolarsWrapper.Tanh
                | "Asinh", [ x ] -> tryTranslate paramName x |> Option.map PolarsWrapper.ArcSinh
                | "Acosh", [ x ] -> tryTranslate paramName x |> Option.map PolarsWrapper.ArcCosh
                | "Atanh", [ x ] -> tryTranslate paramName x |> Option.map PolarsWrapper.ArcTanh

                | "Pow", [ x; y ] ->
                    match tryTranslate paramName x, tryTranslate paramName y with
                    | Some l, Some r -> Some (PolarsWrapper.Pow(l, r))
                    | _ -> None
                | "Atan2", [ y; x ] ->
                    match tryTranslate paramName y, tryTranslate paramName x with
                    | Some yH, Some xH -> Some (PolarsWrapper.ArcTan2(yH, xH))
                    | _ -> None
                | "Log", [ x; newBase ] ->
                    match tryTranslate paramName x, tryTranslate paramName newBase with
                    | Some xH, Some bH -> Some (PolarsWrapper.Log(xH, bH))
                    | _ -> None
                | "Min", [ x; y ] ->
                    match tryTranslate paramName x, tryTranslate paramName y with
                    | Some l, Some r ->
                        let cond = PolarsWrapper.LtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                        Some (PolarsWrapper.IfElse(cond, l, r))
                    | _ -> None
                | "Max", [ x; y ] ->
                    match tryTranslate paramName x, tryTranslate paramName y with
                    | Some l, Some r ->
                        let cond = PolarsWrapper.GtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                        Some (PolarsWrapper.IfElse(cond, l, r))
                    | _ -> None
                | "Clamp", [ valExpr; minExpr; maxExpr ] ->
                    match tryTranslate paramName valExpr, tryTranslate paramName minExpr, tryTranslate paramName maxExpr with
                    | Some v, Some minH, Some maxH -> Some (PolarsWrapper.Clip(v, minH, maxH))
                    | _ -> None
                | "Round", [ x ] ->
                    tryTranslate paramName x |> Option.map (fun h -> PolarsWrapper.Round(h, 0u, PlRoundMode.HalfToEven))
                | "Round", [ x; digitsExpr ] ->
                    match tryTranslate paramName x, tryEvaluate digitsExpr with
                    | Some h, Some d ->
                        let decimals = uint32 (Convert.ToInt32 d)
                        Some (PolarsWrapper.Round(h, decimals, PlRoundMode.HalfToEven))
                    | _ -> None
                | "Truncate", [ x ] ->
                    tryTranslate paramName x |> Option.map (fun h -> PolarsWrapper.Truncate(h, 0u))
                    | _ -> None

            // 13. Binary Expressions (Arithmetic, Logical, Equality, Null checks)
            | Binary(op, left, right) ->
                if op = ExpressionType.NotEqual && isNullConstantExpr right then
                    tryTranslate paramName left |> Option.map PolarsWrapper.IsNotNull
                elif op = ExpressionType.NotEqual && isNullConstantExpr left then
                    tryTranslate paramName right |> Option.map PolarsWrapper.IsNotNull
                elif op = ExpressionType.Equal && isNullConstantExpr right then
                    tryTranslate paramName left |> Option.map PolarsWrapper.IsNull
                elif op = ExpressionType.Equal && isNullConstantExpr left then
                    tryTranslate paramName right |> Option.map PolarsWrapper.IsNull
                else
                    match tryTranslate paramName left, tryTranslate paramName right with
                    | Some l, Some r -> Some (translateBinary op l r)
                    | _ -> None

            // 14. Unary Casting & Operations
            | Unary(ExpressionType.Convert, operandExpr)
            | Unary(ExpressionType.ConvertChecked, operandExpr) ->
                match tryTranslate paramName operandExpr with
                | Some inner -> tryTranslateCast expr.Type inner
                | None -> None

            | Unary(op, operandExpr) ->
                match tryTranslate paramName operandExpr with
                | Some inner -> translateUnary op inner
                | None -> None

            // 15. Ternary Conditional Operator (test ? ifTrue : ifFalse)
            | :? ConditionalExpression as condExpr ->
                match tryTranslate paramName condExpr.Test,
                      tryTranslate paramName condExpr.IfTrue,
                      tryTranslate paramName condExpr.IfFalse with
                | Some testHandle, Some trueHandle, Some falseHandle ->
                    Some (PolarsWrapper.IfElse(testHandle, trueHandle, falseHandle))
                | _ -> None

            | _ -> None
        with _ ->
            None