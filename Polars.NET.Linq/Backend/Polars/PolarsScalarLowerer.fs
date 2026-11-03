namespace Polars.NET.Linq.Backend.Polars

open System
open Polars.NET.Core
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR

/// Lowers pure symbolic ScalarExpr AST nodes to unmanaged Polars ExprHandle pointers.
module internal PolarsScalarLowerer =

    let rec internal inferPolarsJsonDataType (t: Type) : DataTypeHandle option =
        let coreType = PolarsTypeHelper.UnwrapCoreType t
        if PolarsTypeHelper.IsScalarType coreType then
            if coreType = typeof<Guid> then
                Some(PolarsWrapper.NewPrimitiveType(int PlDataType.String))
            else
                try
                    let arrowType = ArrowTypeResolver.GetArrowTypeFromNetType coreType
                    Some(PolarsWrapper.MapArrowToDataTypeHandle arrowType)
                with _ ->
                    None
        elif not (isNull (PolarsTypeHelper.TryGetEnumerableElementType coreType)) then
            let elemType = PolarsTypeHelper.TryGetEnumerableElementType coreType
            inferPolarsJsonDataType elemType |> Option.map PolarsWrapper.NewListType
        elif PolarsTypeHelper.IsStructType coreType then
            let members = ArrowTypeResolver.GetReadableMembers coreType
            let fields =
                members
                |> Array.map (fun m ->
                    let memberType = ArrowTypeResolver.GetMemberType m
                    let jsonName = PolarsTypeHelper.GetSerializedName m
                    jsonName, inferPolarsJsonDataType memberType)
            if fields |> Array.forall (fun (_, opt) -> opt.IsSome) then
                let names = fields |> Array.map fst
                let handles = fields |> Array.map (snd >> Option.get)
                Some(PolarsWrapper.NewStructType(names, handles))
            else
                None
        else
            None

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
            let dtHandle =
                match targetType with
                | PlDataType.Decimal -> PolarsWrapper.NewDecimalType(38, 18)
                | other -> PolarsWrapper.NewPrimitiveType(int other)
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
            PolarsWrapper.ExprSlice(targetH, PolarsWrapper.Lit offset, PolarsWrapper.Lit length)

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

        | ScalarExpr.Function(fn, args, _) ->
            match fn, args with
            // Math
            | ScalarFunction.Abs, [ a ] -> PolarsWrapper.Abs(lower a)
            | ScalarFunction.Round, [ a ] -> PolarsWrapper.Round(lower a, 0u, PlRoundMode.HalfToEven)
            | ScalarFunction.Round, [ a; ScalarExpr.Literal decLit ] ->
                let decimals =
                    match decLit with
                    | LiteralValue.Int32 d -> uint32 (max 0 d)
                    | LiteralValue.Int64 d -> uint32 (max 0L d)
                    | LiteralValue.UInt32 d -> d
                    | LiteralValue.UInt8 d -> uint32 d
                    | LiteralValue.Int16 d -> uint32 (max 0s d)
                    | _ -> 0u
                PolarsWrapper.Round(lower a, decimals, PlRoundMode.HalfToEven)
            | ScalarFunction.Round, [ a; ScalarExpr.Literal decLit; ScalarExpr.Literal modeLit ] ->
                let decimals =
                    match decLit with
                    | LiteralValue.Int32 d -> uint32 (max 0 d)
                    | LiteralValue.Int64 d -> uint32 (max 0L d)
                    | LiteralValue.UInt32 d -> d
                    | LiteralValue.UInt8 d -> uint32 d
                    | LiteralValue.Int16 d -> uint32 (max 0s d)
                    | _ -> 0u
                let plMode =
                    match modeLit with
                    | LiteralValue.Int32 m -> LanguagePrimitives.EnumOfValue<byte, PlRoundMode> (byte m)
                    | LiteralValue.UInt8 m -> LanguagePrimitives.EnumOfValue<byte, PlRoundMode> m
                    | LiteralValue.Int64 m -> LanguagePrimitives.EnumOfValue<byte, PlRoundMode> (byte m)
                    | _ -> PlRoundMode.HalfToEven
                PolarsWrapper.Round(lower a, decimals, plMode)
            | ScalarFunction.CastDecimal, [ t; ScalarExpr.Literal(LiteralValue.Int32 scale) ] ->
                let decType = PolarsWrapper.NewDecimalType(38, scale)
                use _h = decType
                let decTypeExpr = PolarsWrapper.DataTypeExprFromDataType decType
                PolarsWrapper.ExprCast(lower t, decTypeExpr, strict = false, wrapNumerical = false)
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
            | ScalarFunction.Normalize, [ t; ScalarExpr.Literal(LiteralValue.Int32 form) ] ->
                PolarsWrapper.StrNormalize(lower t, LanguagePrimitives.EnumOfValue<int, System.Text.NormalizationForm> form)
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
            | ScalarFunction.StrJsonDecode, [ a; ScalarExpr.Literal(LiteralValue.String typeName) ] ->
                let dtoType = Type.GetType typeName
                let dtypeHandle = inferPolarsJsonDataType dtoType |> Option.get
                use _h = dtypeHandle
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle
                PolarsWrapper.StrJsonDecode(lower a, dtypeExpr)

            // Window & Extensions
            | ScalarFunction.Shift, [ a; b ] -> PolarsWrapper.Shift(lower a, lower b)
            | ScalarFunction.Diff, [ a; b; ScalarExpr.Literal(LiteralValue.Int32 nb) ] ->
                PolarsWrapper.Diff(lower a, lower b, LanguagePrimitives.EnumOfValue<byte, PlNullBehavior> (byte nb))
            | ScalarFunction.Rank, [ a; ScalarExpr.Literal(LiteralValue.Int32 m); ScalarExpr.Literal(LiteralValue.Bool desc) ] ->
                PolarsWrapper.Rank(lower a, LanguagePrimitives.EnumOfValue<byte, PlRankMethod> (byte m), desc, Nullable())
            | ScalarFunction.PctChange, [ a; ScalarExpr.Literal(LiteralValue.Int64 n) ] -> PolarsWrapper.PctChange(lower a, n)
            | ScalarFunction.Interpolate, [ a; ScalarExpr.Literal(LiteralValue.Int32 m) ] ->
                PolarsWrapper.Interpolate(lower a, LanguagePrimitives.EnumOfValue<byte, PlInterpolationMethod> (byte m))
            | ScalarFunction.InterpolateBy, [ a; b ] -> PolarsWrapper.InterpolateBy(lower a, lower b)
            | ScalarFunction.CumSum, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumSum(lower a, rev)
            | ScalarFunction.CumMin, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumMin(lower a, rev)
            | ScalarFunction.CumMax, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumMax(lower a, rev)
            | ScalarFunction.CumProd, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumProd(lower a, rev)
            | ScalarFunction.CumCount, [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumCount(lower a, rev)
            | ScalarFunction.RegexEscape, [ a ] -> PolarsWrapper.StrEscapeRegex(lower a)
            | ScalarFunction.Head, [ a; b ] -> PolarsWrapper.StrHead(lower a, lower b)
            | ScalarFunction.Tail, [ a; b ] -> PolarsWrapper.StrTail(lower a, lower b)
            | ScalarFunction.StrToInteger, [ a; b; ScalarExpr.Literal(LiteralValue.Int32 dtypeVal) ] ->
                let dtHandle = PolarsWrapper.NewPrimitiveType dtypeVal
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

            | other, _ -> failwithf "Unsupported function in PolarsScalarLowerer: %s" other.Name

        | ScalarExpr.Subquery(_, kind) ->
            failwithf "Subquery scalar expression must be decorrelated into relational join before physical lowering: %A" kind

        | ScalarExpr.Opaque(handle, _, _) ->
            PolarsWrapper.CloneExpr handle

