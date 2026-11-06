namespace Polars.NET.Linq.Backend.Polars

open System
open System.Collections
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Arrow
open Polars.NET.Core.Enums
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR

/// Lowers pure symbolic ScalarExpr AST nodes to unmanaged Polars ExprHandle pointers.
module internal PolarsScalarLowerer =

    /// Converts an in-memory collection (array or IEnumerable) into a native SeriesHandle.
    /// Conversion errors propagate with their original message instead of being reduced to a generic failure.
    let internal createSeriesHandle (name: string) (objVal: obj) : SeriesHandle =
        let checkedHandle (h: SeriesHandle) =
            if isNull h || h.IsInvalid then
                invalidOp (sprintf "Converting a %s to a Polars Series produced no series." (objVal.GetType().Name))
            h

        match objVal with
        | :? Array as arr -> SeriesFactory.Create(name, arr) |> checkedHandle
        | :? IEnumerable as seq when not (seq :? string) ->
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType(seq.GetType()) with
                | null -> typeof<obj>
                | t -> t

            let createGeneric =
                typeof<SeriesFactory>
                    .GetMethod("CreateGenericType", BindingFlags.Public ||| BindingFlags.Static)
                    .MakeGenericMethod(elemType)

            try
                createGeneric.Invoke(null, [| box name; seq |]) :?> SeriesHandle |> checkedHandle
            with :? TargetInvocationException as e when not (isNull e.InnerException) ->
                Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw()
                Unchecked.defaultof<_>
        | null -> invalidArg (nameof objVal) "Cannot convert null to a Polars Series."
        | other -> invalidArg (nameof objVal) (sprintf "Cannot convert a %s to a Polars Series." (other.GetType().Name))

    /// Materializes a symbolic JsonDataType into a native Polars DataTypeHandle.
    let rec internal lowerJsonDataType (dt: JsonDataType) : DataTypeHandle =
        match dt with
        | JsonDataType.Primitive primitiveType -> PolarsVocabulary.toDataTypeHandle primitiveType
        | JsonDataType.List elem -> PolarsWrapper.NewListType(lowerJsonDataType elem)
        | JsonDataType.Struct fields ->
            let names = fields |> List.map fst |> List.toArray
            let handles = fields |> List.map (snd >> lowerJsonDataType) |> List.toArray
            PolarsWrapper.NewStructType(names, handles)

    /// Infers the JSON decode schema for a .NET type and materializes it into a native DataTypeHandle.
    let internal inferPolarsJsonDataType (t: Type) : DataTypeHandle option =
        JsonDataTypeInference.tryInfer t |> Option.map lowerJsonDataType

    /// Strongly-typed zero-boxing conversion from LiteralValue to Polars ExprHandle
    let lowerLiteral (value: LiteralValue) : ExprHandle =
        match value with
        | LiteralValue.Null _ -> PolarsWrapper.LitNull()
        | LiteralValue.Bool b -> PolarsWrapper.Lit b
        | LiteralValue.Int8 sb -> PolarsWrapper.Lit sb
        | LiteralValue.Int16 s -> PolarsWrapper.Lit s
        | LiteralValue.Int32 i -> PolarsWrapper.Lit i
        | LiteralValue.Int64 l -> PolarsWrapper.Lit l
        | LiteralValue.UInt8 b -> PolarsWrapper.Lit b
        | LiteralValue.UInt16 us -> PolarsWrapper.Lit us
        | LiteralValue.UInt32 ui -> PolarsWrapper.Lit ui
        | LiteralValue.UInt64 ul -> PolarsWrapper.Lit ul
        | LiteralValue.Float32 f -> PolarsWrapper.Lit f
        | LiteralValue.Float64 d -> PolarsWrapper.Lit d
        | LiteralValue.Decimal m -> PolarsWrapper.Lit m
        | LiteralValue.String s -> PolarsWrapper.Lit s
        | LiteralValue.Date d -> PolarsWrapper.Lit d
        | LiteralValue.DateTime dt -> PolarsWrapper.Lit dt
        | LiteralValue.DateTimeOffset dto -> PolarsWrapper.Lit dto
        | LiteralValue.TimeSpan ts -> PolarsWrapper.Lit ts
        | LiteralValue.TimeOnly to' -> PolarsWrapper.Lit to'

    /// A typed array holding the values of homogeneous literals (strings or integers), for a Series literal.
    let private literalArray (values: LiteralValue list) : Array =
        let boxed =
            values
            |> List.map (function
                | LiteralValue.String s -> box s
                | LiteralValue.Int8 v -> box v
                | LiteralValue.Int16 v -> box v
                | LiteralValue.Int32 v -> box v
                | LiteralValue.Int64 v -> box v
                | LiteralValue.UInt8 v -> box v
                | LiteralValue.UInt16 v -> box v
                | LiteralValue.UInt32 v -> box v
                | LiteralValue.UInt64 v -> box v
                | other -> failwithf "[Polars.NET.Linq Bug] Unsupported literal in a value list: %A" other)
        let elementType =
            match boxed with
            | first :: _ -> first.GetType()
            | [] -> typeof<string>
        let array = Array.CreateInstance(elementType, boxed.Length)
        boxed |> List.iteri (fun i v -> array.SetValue(v, i))
        array

    /// Lowers a SortOrder into a tuple of (ExprHandle, isDescending, nullsLast)
    let rec lowerSortOrder (order: SortOrder) : ExprHandle * bool * bool =
        lower order.Expr, order.Descending, order.NullsLast

    /// Lowers a ScalarExpr recursively to an ExprHandle
    and lower (expr: ScalarExpr) : ExprHandle =
        match expr with
        | ScalarExpr.Column name -> PolarsWrapper.Col name

        | ScalarExpr.Literal lit -> lowerLiteral lit

        | ScalarExpr.Binary(op, left, right) ->
            let leftH = lower left
            let rightH = lower right
            match op with
            | BinaryOp.Eq -> PolarsWrapper.Eq(leftH, rightH)
            | BinaryOp.NotEq -> PolarsWrapper.Neq(leftH, rightH)
            | BinaryOp.Lt -> PolarsWrapper.Lt(leftH, rightH)
            | BinaryOp.LtEq -> PolarsWrapper.LtEq(leftH, rightH)
            | BinaryOp.Gt -> PolarsWrapper.Gt(leftH, rightH)
            | BinaryOp.GtEq -> PolarsWrapper.GtEq(leftH, rightH)
            | BinaryOp.And -> PolarsWrapper.And(leftH, rightH)
            | BinaryOp.Or -> PolarsWrapper.Or(leftH, rightH)
            | BinaryOp.Xor -> PolarsWrapper.Xor(leftH, rightH)
            | BinaryOp.Add -> PolarsWrapper.Add(leftH, rightH)
            | BinaryOp.Subtract -> PolarsWrapper.Sub(leftH, rightH)
            | BinaryOp.Multiply -> PolarsWrapper.Mul(leftH, rightH)
            | BinaryOp.Divide -> PolarsWrapper.Div(leftH, rightH)
            | BinaryOp.Modulo -> PolarsWrapper.Rem(leftH, rightH)
            | BinaryOp.FloorDivide -> PolarsWrapper.FloorDiv(leftH, rightH)
            | BinaryOp.Power -> PolarsWrapper.Pow(leftH, rightH)
            | BinaryOp.StringConcat -> PolarsWrapper.FormatString("{}{}", [| leftH; rightH |])
            | BinaryOp.Contains -> PolarsWrapper.StrContains(leftH, rightH, literal = false, strict = false)
            | BinaryOp.StartsWith -> PolarsWrapper.StrStartsWith(leftH, rightH)
            | BinaryOp.EndsWith -> PolarsWrapper.StrEndsWith(leftH, rightH)
            | BinaryOp.Like -> PolarsWrapper.StrContains(leftH, rightH, literal = true, strict = false)
            | BinaryOp.RegexMatch -> PolarsWrapper.StrContains(leftH, rightH, literal = false, strict = false)

        | ScalarExpr.Unary(op, inner) ->
            let innerH = lower inner
            match op with
            | UnaryOp.Not -> PolarsWrapper.Not innerH
            | UnaryOp.Negate -> PolarsWrapper.Neg innerH
            | UnaryOp.IsNull -> PolarsWrapper.IsNull innerH
            | UnaryOp.IsNotNull -> PolarsWrapper.IsNotNull innerH
            | UnaryOp.Abs -> PolarsWrapper.Abs innerH

        | ScalarExpr.Cast(inner, targetType, isStrict) ->
            let innerH = lower inner
            let dtHandle = PolarsVocabulary.toDataTypeHandle targetType
            use _h = dtHandle
            let dtExpr = PolarsWrapper.DataTypeExprFromDataType dtHandle
            PolarsWrapper.ExprCast(innerH, dtExpr, strict = isStrict, wrapNumerical = false)

        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            let testH = lower test
            let trueH = lower ifTrue
            let falseH = lower ifFalse
            PolarsWrapper.IfElse(testH, trueH, falseH)

        | ScalarExpr.Coalesce exprs ->
            let handles = exprs |> List.map lower |> List.toArray
            PolarsWrapper.Coalesce handles

        | ScalarExpr.ItemAccess(target, index) ->
            let targetH = lower target
            let indexH = lower index
            PolarsWrapper.ListGet(targetH, indexH, nullOnOob = true)

        | ScalarExpr.Slice(target, offset, length) ->
            let targetH = lower target
            if offset = 0L && length < UInt32.MaxValue then
                PolarsWrapper.Head(targetH, int length)
            elif length = UInt32.MaxValue then
                PolarsWrapper.ExprSlice(targetH, PolarsWrapper.Lit offset, PolarsWrapper.Lit Int64.MaxValue)
            else
                PolarsWrapper.ExprSlice(targetH, PolarsWrapper.Lit offset, PolarsWrapper.Lit(int64 length))

        | ScalarExpr.SortBy(inner, orderBy) ->
            let innerH = lower inner
            let byExprs = orderBy |> List.map (fun o -> lower o.Expr) |> List.toArray
            let desc = orderBy |> List.map (fun o -> o.Descending) |> List.toArray
            let nullsLast = orderBy |> List.map (fun o -> o.NullsLast) |> List.toArray
            PolarsWrapper.SortBy(innerH, byExprs, desc, nullsLast, multithreaded = true, maintainOrder = true)

        | ScalarExpr.Alias(inner, alias) ->
            let innerH = lower inner
            PolarsWrapper.Alias(innerH, alias)

        | ScalarExpr.Aggregation(kind, exprOpt, isDistinct) ->
            let baseH =
                match exprOpt with
                | Some e ->
                    let h = lower e
                    if isDistinct && kind <> AggKind.Count then PolarsWrapper.ExprUnique h else h
                | None -> PolarsWrapper.Col "*"

            match kind with
            | AggKind.Sum -> PolarsWrapper.Sum baseH
            | AggKind.Mean -> PolarsWrapper.Mean baseH
            | AggKind.Min -> PolarsWrapper.Min baseH
            | AggKind.Max -> PolarsWrapper.Max baseH
            | AggKind.Count -> if isDistinct then PolarsWrapper.NUnique baseH else PolarsWrapper.Count baseH
            | AggKind.First -> PolarsWrapper.First(baseH, false)
            | AggKind.Last -> PolarsWrapper.Last(baseH, false)
            | AggKind.Median -> PolarsWrapper.Median baseH
            | AggKind.Std ddof -> PolarsWrapper.Std(baseH, ddof)
            | AggKind.Var ddof -> PolarsWrapper.Var(baseH, ddof)
            | AggKind.Quantile prob -> PolarsWrapper.Quantile(baseH, prob, PlQuantileMethod.Nearest)
            | AggKind.List -> PolarsWrapper.Implode(baseH, false)
            | AggKind.Len -> PolarsWrapper.Len()

        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            let innerH = lower inner
            let partHs = partitionBy |> List.map lower |> List.toArray
            let orderHs = orderBy |> List.map (fun o -> lower o.Expr) |> List.toArray
            let isDesc = orderBy |> List.tryHead |> Option.map (fun o -> o.Descending) |> Option.defaultValue false
            let nullsLast = orderBy |> List.tryHead |> Option.map (fun o -> o.NullsLast) |> Option.defaultValue false
            PolarsWrapper.Over(
                innerH,
                partHs,
                orderHs,
                descending = isDesc,
                nullsLast = nullsLast,
                multithreaded = true,
                maintainOrder = true,
                mappingCode = PlWindowMapping.GroupsToRows
            )

        | ScalarExpr.Function(ScalarFunction.ReplaceStrict(oldValues, newValues), input :: fallback, returnType) ->
            let inputH = lower input
            let oldH = PolarsWrapper.Lit(createSeriesHandle "old" (literalArray oldValues))
            let newH = PolarsWrapper.Lit(createSeriesHandle "new" (literalArray newValues))
            let defaultH =
                match fallback with
                | [] -> null
                | [ d ] -> lower d
                | _ -> failwith "[Polars.NET.Linq Bug] replace_strict takes at most one default."
            use dtHandle = PolarsVocabulary.toDataTypeHandle returnType
            let dtExpr = PolarsWrapper.DataTypeExprFromDataType dtHandle
            PolarsWrapper.ExprReplaceStrict(inputH, oldH, newH, defaultH, dtExpr)

        | ScalarExpr.EnumConvert(conversion, inner, _) ->
            failwithf
                "[Polars.NET.Linq Bug] Enum conversion %A of %s reached lowering unresolved; EnumResolution must run first."
                conversion
                (LogicalPlanOps.formatScalar inner)

        | ScalarExpr.Function(fn, args, _) ->
            match fn, args with
            // Math
            | ScalarFunction.Abs, [ a ] -> PolarsWrapper.Abs(lower a)
            | ScalarFunction.Round mode, [ a ] -> PolarsWrapper.Round(lower a, 0u, PolarsVocabulary.roundMode mode)
            | ScalarFunction.Round mode, [ a; ScalarExpr.Literal decLit ] ->
                let decimals =
                    match decLit with
                    | LiteralValue.Int32 d -> uint32 (max 0 d)
                    | LiteralValue.Int64 d -> uint32 (max 0L d)
                    | LiteralValue.UInt32 d -> d
                    | LiteralValue.UInt8 d -> uint32 d
                    | LiteralValue.Int16 d -> uint32 (max 0s d)
                    | _ -> 0u
                PolarsWrapper.Round(lower a, decimals, PolarsVocabulary.roundMode mode)
            | ScalarFunction.Floor, [ a ] -> PolarsWrapper.Floor(lower a)
            | ScalarFunction.Ceil, [ a ] -> PolarsWrapper.Ceil(lower a)
            | ScalarFunction.Truncate, [ a ] -> PolarsWrapper.Truncate(lower a, 0u)
            | ScalarFunction.Sign, [ a ] -> PolarsWrapper.Sign(lower a)
            | ScalarFunction.Sqrt, [ a ] -> PolarsWrapper.Sqrt(lower a)
            | ScalarFunction.Cbrt, [ a ] -> PolarsWrapper.Cbrt(lower a)
            | ScalarFunction.Exp, [ a ] -> PolarsWrapper.Exp(lower a)
            | ScalarFunction.Log, [ a ] -> PolarsWrapper.Log(lower a, PolarsWrapper.Lit Math.E)
            | ScalarFunction.Log, [ a; b ] -> PolarsWrapper.Log(lower a, lower b)
            | ScalarFunction.Pow, [ a; b ] -> PolarsWrapper.Pow(lower a, lower b)
            | ScalarFunction.Sin, [ a ] -> PolarsWrapper.Sin(lower a)
            | ScalarFunction.Cos, [ a ] -> PolarsWrapper.Cos(lower a)
            | ScalarFunction.Tan, [ a ] -> PolarsWrapper.Tan(lower a)
            | ScalarFunction.Asin, [ a ] -> PolarsWrapper.ArcSin(lower a)
            | ScalarFunction.Acos, [ a ] -> PolarsWrapper.ArcCos(lower a)
            | ScalarFunction.Atan, [ a ] -> PolarsWrapper.ArcTan(lower a)
            | ScalarFunction.Atan2, [ y; x ] -> PolarsWrapper.ArcTan2(lower y, lower x)
            | ScalarFunction.Sinh, [ a ] -> PolarsWrapper.Sinh(lower a)
            | ScalarFunction.Cosh, [ a ] -> PolarsWrapper.Cosh(lower a)
            | ScalarFunction.Tanh, [ a ] -> PolarsWrapper.Tanh(lower a)
            | ScalarFunction.Clamp, [ v; mn; mx ] -> PolarsWrapper.Clip(lower v, lower mn, lower mx)

            // String
            | ScalarFunction.ToUpper, [ a ] -> PolarsWrapper.StrToUpper(lower a)
            | ScalarFunction.ToLower, [ a ] -> PolarsWrapper.StrToLower(lower a)
            | ScalarFunction.ToTitlecase, [ a ] -> PolarsWrapper.StrToTitlecase(lower a)
            | ScalarFunction.LenChars, [ a ] -> PolarsWrapper.StrLenChars(lower a)
            | ScalarFunction.LenBytes, [ a ] -> PolarsWrapper.StrLenBytes(lower a)
            | ScalarFunction.Reverse, [ a ] -> PolarsWrapper.StrReverse(lower a)
            | ScalarFunction.Slice, [ a; off; len ] -> PolarsWrapper.StrSlice(lower a, lower off, lower len)
            | ScalarFunction.SplitExact, [ a; by; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.StrSplitExact(lower a, lower by, n, inclusive = false)
            | ScalarFunction.Replace, [ a; pat; v; ScalarExpr.Literal(LiteralValue.Int64 n) ] ->
                PolarsWrapper.StrReplace(lower a, lower pat, lower v, literal = false, n = n)
            | ScalarFunction.ReplaceAll, [ a; pat; v ] ->
                PolarsWrapper.StrReplaceAll(lower a, lower pat, lower v, literal = false)
            | ScalarFunction.ReplaceAll, [ a; pat; v; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrReplaceAll(lower a, lower pat, lower v, literal = literal)
            | ScalarFunction.Strip, [ a; m ] -> PolarsWrapper.StrStripChars(lower a, lower m)
            | ScalarFunction.StripStart, [ a; m ] -> PolarsWrapper.StrStripCharsStart(lower a, lower m)
            | ScalarFunction.StripEnd, [ a; m ] -> PolarsWrapper.StrStripCharsEnd(lower a, lower m)
            | ScalarFunction.FormatString, formatStrLit :: argExprs ->
                let fmt = match formatStrLit with ScalarExpr.Literal(LiteralValue.String s) -> s | _ -> failwith "format must be string literal"
                let argHs = argExprs |> List.map lower |> List.toArray
                PolarsWrapper.FormatString(fmt, argHs)
            | ScalarFunction.ConcatString, sepLit :: itemExprs ->
                let sep = match sepLit with ScalarExpr.Literal(LiteralValue.String s) -> s | _ -> ""
                let itemHs = itemExprs |> List.map lower |> List.toArray
                PolarsWrapper.ConcatString(itemHs, sep, ignoreNulls = false)
            | ScalarFunction.FillNull, [ e; v ] ->
                PolarsWrapper.FillNull(lower e, lower v)
            | ScalarFunction.PadStart, [ t; w; ScalarExpr.Literal(LiteralValue.String pad) ] ->
                PolarsWrapper.StrPadStart(lower t, lower w, pad)
            | ScalarFunction.PadEnd, [ t; w; ScalarExpr.Literal(LiteralValue.String pad) ] ->
                PolarsWrapper.StrPadEnd(lower t, lower w, pad)
            | ScalarFunction.Normalize form, [ t ] ->
                PolarsWrapper.StrNormalize(lower t, form)
            | ScalarFunction.StrContains, [ t; p; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrContains(lower t, lower p, literal = literal, strict = false)
            | ScalarFunction.StartsWith, [ t; p ] ->
                PolarsWrapper.StrStartsWith(lower t, lower p)
            | ScalarFunction.EndsWith, [ t; p ] ->
                PolarsWrapper.StrEndsWith(lower t, lower p)
            | ScalarFunction.StrFind, [ t; p; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrFind(lower t, lower p, literal = literal, strict = false)
            | ScalarFunction.StrSplit, [ t; by; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrSplit(lower t, lower by, inclusive = false, literal = literal, strict = false)
            | ScalarFunction.StrSplit, [ t; by ] ->
                PolarsWrapper.StrSplit(lower t, lower by, inclusive = false, literal = true, strict = false)
            | ScalarFunction.StrSplitN, [ t; by; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.StrSplitN(lower t, lower by, n)
            | ScalarFunction.StrExtract, [ t; pat; ScalarExpr.Literal(LiteralValue.Int32 idx) ] ->
                PolarsWrapper.StrExtract(lower t, lower pat, idx)
            | ScalarFunction.StrExtractGroups, [ t; ScalarExpr.Literal(LiteralValue.String pat) ] ->
                PolarsWrapper.StrExtractGroups(lower t, pat)
            | ScalarFunction.StrExtractAll, [ t; pat ] ->
                PolarsWrapper.StrExtractAll(lower t, lower pat)
            | ScalarFunction.StrCountMatches, [ t; pat; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrCountMatches(lower t, lower pat, literal = literal)

            // Temporal
            | ScalarFunction.DtYear, [ a ] -> PolarsWrapper.DtYear(lower a)
            | ScalarFunction.DtMonth, [ a ] -> PolarsWrapper.DtMonth(lower a)
            | ScalarFunction.DtDay, [ a ] -> PolarsWrapper.DtDay(lower a)
            | ScalarFunction.DtOrdinalDay, [ a ] -> PolarsWrapper.DtOrdinalDay(lower a)
            | ScalarFunction.DtWeekday, [ a ] -> PolarsWrapper.DtWeekday(lower a)
            | ScalarFunction.DtHour, [ a ] -> PolarsWrapper.DtHour(lower a)
            | ScalarFunction.DtMinute, [ a ] -> PolarsWrapper.DtMinute(lower a)
            | ScalarFunction.DtSecond, [ a ] -> PolarsWrapper.DtSecond(lower a)
            | ScalarFunction.DtMillisecond, [ a ] -> PolarsWrapper.DtMillisecond(lower a)
            | ScalarFunction.DtMicrosecond, [ a ] -> PolarsWrapper.DtMicrosecond(lower a)
            | ScalarFunction.DtNanosecond, [ a ] -> PolarsWrapper.DtNanosecond(lower a)
            | ScalarFunction.DtDate, [ a ] -> PolarsWrapper.DtDate(lower a)
            | ScalarFunction.DtTime, [ a ] -> PolarsWrapper.DtTime(lower a)
            | ScalarFunction.DtTotalDays, [ a ] -> PolarsWrapper.DtTotalDays(lower a, fractional = true)
            | ScalarFunction.DtTotalHours, [ a ] -> PolarsWrapper.DtTotalHours(lower a, fractional = true)
            | ScalarFunction.DtTotalMinutes, [ a ] -> PolarsWrapper.DtTotalMinutes(lower a, fractional = true)
            | ScalarFunction.DtTotalSeconds, [ a ] -> PolarsWrapper.DtTotalSeconds(lower a, fractional = true)
            | ScalarFunction.DtTotalMilliseconds, [ a ] -> PolarsWrapper.DtTotalMilliseconds(lower a, fractional = true)
            | ScalarFunction.DtTotalMicroseconds, [ a ] -> PolarsWrapper.DtTotalMicroseconds(lower a, fractional = true)
            | ScalarFunction.DtTotalNanoseconds, [ a ] -> PolarsWrapper.DtTotalNanoseconds(lower a, fractional = true)
            | ScalarFunction.StrToDatetime, [ e; ScalarExpr.Literal(LiteralValue.String fmt); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                let formatStr = if String.IsNullOrEmpty fmt then null else fmt
                PolarsWrapper.StrToDatetime(e = lower e, unit = PlTimeUnit.Microseconds, timeZone = null, format = formatStr, strict = false, exact = exact, cache = true, ambiguous = PolarsWrapper.Lit "raise")
            | ScalarFunction.StrToDatetime, [ e; ScalarExpr.Literal(LiteralValue.Null _); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToDatetime(e = lower e, unit = PlTimeUnit.Microseconds, timeZone = null, format = null, strict = false, exact = exact, cache = true, ambiguous = PolarsWrapper.Lit "raise")
            | ScalarFunction.StrToDate, [ e; ScalarExpr.Literal(LiteralValue.String fmt); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToDate(lower e, fmt, strict = false, exact = exact, cache = true)
            | ScalarFunction.StrToDate, [ e; ScalarExpr.Literal(LiteralValue.Null _); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToDate(lower e, format = null, strict = false, exact = exact, cache = true)
            | ScalarFunction.StrToTime, [ e; ScalarExpr.Literal(LiteralValue.String fmt); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToTime(lower e, fmt, strict = false, exact = exact, cache = true)
            | ScalarFunction.StrToTime, [ e; ScalarExpr.Literal(LiteralValue.Null _); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToTime(lower e, format = null, strict = false, exact = exact, cache = true)
            | ScalarFunction.DtIsLeapYear, [ e ] ->
                PolarsWrapper.DtIsLeapYear(lower e)
            | ScalarFunction.DtToString, [ e; ScalarExpr.Literal(LiteralValue.String fmt) ] ->
                PolarsWrapper.DtToString(lower e, fmt)
            | ScalarFunction.DtOffsetBy, [ e; ScalarExpr.Literal(LiteralValue.String offset) ] ->
                PolarsWrapper.DtOffsetBy(lower e, PolarsWrapper.Lit offset)

            // Struct & List
            | ScalarFunction.StructFieldByIndex, [ a; ScalarExpr.Literal(LiteralValue.Int64 idx) ] ->
                PolarsWrapper.StructFieldByIndex(lower a, idx)
            | ScalarFunction.StructFieldByIndex, [ a; ScalarExpr.Literal(LiteralValue.Int32 idx) ] ->
                PolarsWrapper.StructFieldByIndex(lower a, int64 idx)
            | ScalarFunction.StructFieldByName, [ a; ScalarExpr.Literal(LiteralValue.String name) ] ->
                PolarsWrapper.StructFieldByName(lower a, [| name |])
            | ScalarFunction.ListGet, [ a; idx ] -> PolarsWrapper.ListGet(lower a, lower idx, nullOnOob = true)
            | ScalarFunction.ListContains, [ a; item ] -> PolarsWrapper.ListContains(lower a, lower item, nullsEqual = true)
            | ScalarFunction.ListLen, [ a ] -> PolarsWrapper.ListLen(lower a)
            | ScalarFunction.IsIn, [ a; b ] -> PolarsWrapper.IsIn(lower a, lower b, nullsEqual = false)
            | ScalarFunction.ListSum, [ a ] -> PolarsWrapper.ListSum(lower a)
            | ScalarFunction.ListMean, [ a ] -> PolarsWrapper.ListMean(lower a)
            | ScalarFunction.ListMin, [ a ] -> PolarsWrapper.ListMin(lower a)
            | ScalarFunction.ListMax, [ a ] -> PolarsWrapper.ListMax(lower a)
            | ScalarFunction.ListJoin, [ a; ScalarExpr.Literal(LiteralValue.String sep) ] -> PolarsWrapper.ListJoin(lower a, sep, ignoreNulls = true)
            | ScalarFunction.ListHead, [ a; c ] -> PolarsWrapper.ListHead(lower a, lower c)
            | ScalarFunction.ListTail, [ a; c ] -> PolarsWrapper.ListTail(lower a, lower c)
            | ScalarFunction.ListSlice, [ a; off; len ] -> PolarsWrapper.ListSlice(lower a, lower off, lower len)
            | ScalarFunction.ListUnique, [ a ] -> PolarsWrapper.ListEval(lower a, PolarsWrapper.ExprUnique(PolarsWrapper.Col("")))
            | ScalarFunction.ListReverse, [ a ] -> PolarsWrapper.ListEval(lower a, PolarsWrapper.Reverse(PolarsWrapper.Col("")))
            | ScalarFunction.ListSort, [ a; ScalarExpr.Literal(LiteralValue.Bool desc) ] -> PolarsWrapper.ListSort(lower a, descending = desc, nullsLast = false, maintainOrder = false)
            | ScalarFunction.ListIntersection, [ a; b ] -> PolarsWrapper.ListSetIntersection(lower a, lower b)
            | ScalarFunction.ListUnion, [ a; b ] -> PolarsWrapper.ListSetUnion(lower a, lower b)
            | ScalarFunction.ListDifference, [ a; b ] -> PolarsWrapper.ListSetDifference(lower a, lower b)
            | ScalarFunction.ListAny, [ t; cond ] ->
                let anyExpr = PolarsWrapper.Any(lower cond, ignoreNulls = false)
                let boolList = PolarsWrapper.ListEval(lower t, anyExpr)
                PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true)
            | ScalarFunction.ListAll, [ t; cond ] ->
                let allExpr = PolarsWrapper.All(lower cond, ignoreNulls = false)
                let boolList = PolarsWrapper.ListEval(lower t, allExpr)
                PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true)
            | ScalarFunction.ListEval, [ t; mapper ] -> PolarsWrapper.ListEval(lower t, lower mapper)

            // JSON
            | ScalarFunction.StrJsonPathMatch, [ a; b ] -> PolarsWrapper.StrJsonPathMatch(lower a, lower b)
            | ScalarFunction.StructJsonEncode, [ a ] -> PolarsWrapper.StructJsonEncode(lower a)
            | ScalarFunction.AsStruct, items -> PolarsWrapper.AsStruct(items |> List.map lower |> List.toArray)
            | ScalarFunction.StrJsonDecode jsonType, [ a ] ->
                let dtypeHandle = lowerJsonDataType jsonType
                use _h = dtypeHandle
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle
                PolarsWrapper.StrJsonDecode(lower a, dtypeExpr)

            // Window & Extensions
            | ScalarFunction.Shift, [ a; b ] -> PolarsWrapper.Shift(lower a, lower b)
            | ScalarFunction.Diff nb, [ a; b ] ->
                PolarsWrapper.Diff(lower a, lower b, PolarsVocabulary.nullBehavior nb)
            | ScalarFunction.Rank(m, seed), [ a; ScalarExpr.Literal(LiteralValue.Bool desc) ] ->
                PolarsWrapper.Rank(lower a, PolarsVocabulary.rankMethod m, desc, Option.toNullable seed)
            | ScalarFunction.PctChange, [ a; ScalarExpr.Literal(LiteralValue.Int64 n) ] -> PolarsWrapper.PctChange(lower a, n)
            | ScalarFunction.Interpolate m, [ a ] ->
                PolarsWrapper.Interpolate(lower a, PolarsVocabulary.interpolationMethod m)
            | ScalarFunction.InterpolateBy, [ a; b ] -> PolarsWrapper.InterpolateBy(lower a, lower b)
            | ScalarFunction.CumSum, [ a ] -> PolarsWrapper.CumSum(lower a, false)
            | ScalarFunction.CumSum, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumSum(lower a, rev)
            | ScalarFunction.CumMin, [ a ] -> PolarsWrapper.CumMin(lower a, false)
            | ScalarFunction.CumMin, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumMin(lower a, rev)
            | ScalarFunction.CumMax, [ a ] -> PolarsWrapper.CumMax(lower a, false)
            | ScalarFunction.CumMax, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumMax(lower a, rev)
            | ScalarFunction.CumProd, [ a ] -> PolarsWrapper.CumProd(lower a, false)
            | ScalarFunction.CumProd, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumProd(lower a, rev)
            | ScalarFunction.CumCount, [ a ] -> PolarsWrapper.CumCount(lower a, false)
            | ScalarFunction.CumCount, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumCount(lower a, rev)
            | ScalarFunction.RegexEscape, [ a ] -> PolarsWrapper.StrEscapeRegex(lower a)
            | ScalarFunction.Head, [ a; b ] -> PolarsWrapper.StrHead(lower a, lower b)
            | ScalarFunction.Tail, [ a; b ] -> PolarsWrapper.StrTail(lower a, lower b)
            | ScalarFunction.StrToInteger targetType, [ a; b ] ->
                let dtHandle = PolarsVocabulary.toDataTypeHandle targetType
                PolarsWrapper.StrToInteger(lower a, lower b, dtHandle, strict = false)
            | ScalarFunction.StrToDecimal, [ a; ScalarExpr.Literal(LiteralValue.Int32 scale) ] ->
                PolarsWrapper.StrToDecimal(lower a, scale)
            | ScalarFunction.HexEncode, [ a ] -> PolarsWrapper.StrHexEncode(lower a)
            | ScalarFunction.HexDecode, [ a ] -> PolarsWrapper.StrHexDecode(lower a, false)
            | ScalarFunction.Base64Encode, [ a ] -> PolarsWrapper.StrBase64Encode(lower a)
            | ScalarFunction.Base64Decode, [ a ] -> PolarsWrapper.StrBase64Decode(lower a, false)

            // Bitwise operations
            | ScalarFunction.BitLeftShift, [ a; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.BitLeftShift(lower a, n)
            | ScalarFunction.BitRightShift, [ a; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.BitRightShift(lower a, n)
            | ScalarFunction.BitwiseCountOnes, [ a ] ->
                PolarsWrapper.BitwiseCountOnes(lower a)
            | ScalarFunction.BitwiseLeadingZeros, [ a ] ->
                PolarsWrapper.BitwiseLeadingZeros(lower a)
            | ScalarFunction.BitwiseTrailingZeros, [ a ] ->
                PolarsWrapper.BitwiseTrailingZeros(lower a)

            // Series & Group-Level Operations
            | ScalarFunction.Implode, [ a ] ->
                PolarsWrapper.Implode(lower a, maintainOrder = true)
            | ScalarFunction.Implode, [ a; ScalarExpr.Literal(LiteralValue.Bool maintainOrder) ] ->
                PolarsWrapper.Implode(lower a, maintainOrder = maintainOrder)
            | ScalarFunction.NUnique, [ a ] ->
                PolarsWrapper.NUnique(lower a)
            | ScalarFunction.Unique, [ a ] ->
                PolarsWrapper.ExprUnique(lower a)
            | ScalarFunction.MinBy, [ a; by ] ->
                PolarsWrapper.MinBy(lower a, lower by)
            | ScalarFunction.MaxBy, [ a; by ] ->
                PolarsWrapper.MaxBy(lower a, lower by)
            | ScalarFunction.Filter, [ a; pred ] ->
                PolarsWrapper.Filter(lower a, lower pred)
            | ScalarFunction.Quantile m, [ a; ScalarExpr.Literal(LiteralValue.Float64 q) ] ->
                PolarsWrapper.Quantile(lower a, q, PolarsVocabulary.quantileMethod m)
            | ScalarFunction.SeriesGet, [ a; idx ] ->
                PolarsWrapper.Get(lower a, lower idx, nullOnOob = true)

            | other, _ -> failwithf "Unsupported function in PolarsScalarLowerer: %s" other.Name

        | ScalarExpr.Udf(_, args, _, invoker) ->
            let argHs = args |> List.map lower |> List.toArray
            match invoker with
            | UdfInvoker.SingleBatch(cbObj, clrReturnType) ->
                let outDataType = ArrowUdfBatchAdapter.GetDataTypeHandle clrReturnType
                let func = cbObj :?> Func<Apache.Arrow.IArrowArray, Apache.Arrow.IArrowArray>
                PolarsWrapper.Map(argHs.[0], func, outDataType)
            | UdfInvoker.MultiBatch(cbObj, clrReturnType) ->
                let outDataType = ArrowUdfBatchAdapter.GetDataTypeHandle clrReturnType
                let func = cbObj :?> Func<System.Collections.Generic.IReadOnlyList<Apache.Arrow.IArrowArray>, Apache.Arrow.IArrowArray>
                let mainH = argHs.[0]
                let otherHs = argHs.[1..]
                PolarsWrapper.MapMany(mainH, otherHs, func, outDataType)
            | UdfInvoker.GroupSingleBatch(cbObj, clrReturnType, returnsScalar) ->
                let outDataType = ArrowUdfBatchAdapter.GetDataTypeHandle clrReturnType
                let func = cbObj :?> Func<Apache.Arrow.IArrowArray, Apache.Arrow.IArrowArray>
                PolarsWrapper.Apply(argHs.[0], func, outDataType, returnsScalar = returnsScalar)
            | UdfInvoker.GroupMultiBatch(cbObj, clrReturnType, returnsScalar) ->
                let outDataType = ArrowUdfBatchAdapter.GetDataTypeHandle clrReturnType
                let func = cbObj :?> Func<System.Collections.Generic.IReadOnlyList<Apache.Arrow.IArrowArray>, Apache.Arrow.IArrowArray>
                let mainH = argHs.[0]
                let otherHs = argHs.[1..]
                PolarsWrapper.ApplyMany(mainH, otherHs, func, outDataType, returnsScalar = returnsScalar)
            | UdfInvoker.StringPredicate pred ->
                PolarsWrapper.MapStringPredicate(argHs.[0], pred)
            | UdfInvoker.StringTransform trans ->
                PolarsWrapper.MapStringTransform(argHs.[0], trans)
            | UdfInvoker.StringToList toList ->
                PolarsWrapper.MapStringToList(argHs.[0], toList)

        | ScalarExpr.InList(inner, source, negated) ->
            let innerH = lower inner
            let listH =
                match source with
                | InListSource.Collection(coll, _) ->
                    let seriesLit = PolarsWrapper.Lit(createSeriesHandle "in_vals" coll)
                    PolarsWrapper.Implode(seriesLit, maintainOrder = false)
                | InListSource.Literals _ ->
                    failwith "InListSource.Literals must be lowered via Disjunction or Collection"
            let isInH = PolarsWrapper.IsIn(innerH, listH, nullsEqual = true)
            if negated then PolarsWrapper.Not isInH else isInH

        | ScalarExpr.Subquery(_, kind) ->
            failwithf "Subquery scalar expression must be decorrelated into relational join before physical lowering: %A" kind


    /// Lowers a CLR constant of the given type directly to a native literal ExprHandle.
    let lowerConstant (value: obj) (t: Type) : ExprHandle =
        ScalarExprOps.toLiteral value t |> EnumResolution.resolveScalar LogicalSchema.Empty |> lower
