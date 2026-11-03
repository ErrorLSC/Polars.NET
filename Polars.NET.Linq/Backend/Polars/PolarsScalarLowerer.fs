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

        | ScalarExpr.Function(name, args, _) ->
            match name.ToLowerInvariant(), args with
            // Math
            | "abs", [ a ] -> PolarsWrapper.Abs(lower a)
            | "round", [ a ] -> PolarsWrapper.Round(lower a, 0u, PlRoundMode.HalfToEven)
            | "round", [ a; ScalarExpr.Literal decLit ] ->
                let decimals =
                    match decLit with
                    | LiteralValue.Int32 d -> uint32 (max 0 d)
                    | LiteralValue.Int64 d -> uint32 (max 0L d)
                    | LiteralValue.UInt32 d -> d
                    | LiteralValue.UInt8 d -> uint32 d
                    | LiteralValue.Int16 d -> uint32 (max 0s d)
                    | _ -> 0u
                PolarsWrapper.Round(lower a, decimals, PlRoundMode.HalfToEven)
            | "round", [ a; ScalarExpr.Literal decLit; ScalarExpr.Literal modeLit ] ->
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
            | "cast_decimal", [ t; ScalarExpr.Literal(LiteralValue.Int32 scale) ] ->
                let decType = PolarsWrapper.NewDecimalType(38, scale)
                use _h = decType
                let decTypeExpr = PolarsWrapper.DataTypeExprFromDataType decType
                PolarsWrapper.ExprCast(lower t, decTypeExpr, strict = false, wrapNumerical = false)
            | "floor", [ a ] -> PolarsWrapper.Floor(lower a)
            | "ceil", [ a ]
            | "ceiling", [ a ] -> PolarsWrapper.Ceil(lower a)
            | "truncate", [ a ] -> PolarsWrapper.Truncate(lower a, 0u)
            | "sign", [ a ] -> PolarsWrapper.Sign(lower a)
            | "sqrt", [ a ] -> PolarsWrapper.Sqrt(lower a)
            | "cbrt", [ a ] -> PolarsWrapper.Cbrt(lower a)
            | "exp", [ a ] -> PolarsWrapper.Exp(lower a)
            | "log", [ a ] -> PolarsWrapper.Log(lower a, PolarsWrapper.Lit Math.E)
            | "log", [ a; b ] -> PolarsWrapper.Log(lower a, lower b)
            | "pow", [ a; b ] -> PolarsWrapper.Pow(lower a, lower b)
            | "sin", [ a ] -> PolarsWrapper.Sin(lower a)
            | "cos", [ a ] -> PolarsWrapper.Cos(lower a)
            | "tan", [ a ] -> PolarsWrapper.Tan(lower a)
            | "asin", [ a ] -> PolarsWrapper.ArcSin(lower a)
            | "acos", [ a ] -> PolarsWrapper.ArcCos(lower a)
            | "atan", [ a ] -> PolarsWrapper.ArcTan(lower a)
            | "sinh", [ a ] -> PolarsWrapper.Sinh(lower a)
            | "cosh", [ a ] -> PolarsWrapper.Cosh(lower a)
            | "tanh", [ a ] -> PolarsWrapper.Tanh(lower a)
            | "clamp", [ v; mn; mx ]
            | "clip", [ v; mn; mx ] -> PolarsWrapper.Clip(lower v, lower mn, lower mx)

            // String
            | "to_upper", [ a ] -> PolarsWrapper.StrToUpper(lower a)
            | "to_lower", [ a ] -> PolarsWrapper.StrToLower(lower a)
            | "to_titlecase", [ a ] -> PolarsWrapper.StrToTitlecase(lower a)
            | "len_chars", [ a ] -> PolarsWrapper.StrLenChars(lower a)
            | "len_bytes", [ a ] -> PolarsWrapper.StrLenBytes(lower a)
            | "reverse", [ a ] -> PolarsWrapper.StrReverse(lower a)
            | "slice", [ a; off; len ]
            | "str_slice", [ a; off; len ] -> PolarsWrapper.StrSlice(lower a, lower off, lower len)
            | "split_exact", [ a; by; ScalarExpr.Literal(LiteralValue.Int32 n) ]
            | "str_split_exact", [ a; by; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.StrSplitExact(lower a, lower by, n, inclusive = false)
            | "replace", [ a; pat; v; ScalarExpr.Literal(LiteralValue.Int64 n) ]
            | "str_replace", [ a; pat; v; ScalarExpr.Literal(LiteralValue.Int64 n) ] ->
                PolarsWrapper.StrReplace(lower a, lower pat, lower v, literal = false, n = n)
            | "replace_all", [ a; pat; v ]
            | "str_replace_all", [ a; pat; v ] ->
                PolarsWrapper.StrReplaceAll(lower a, lower pat, lower v, literal = false)
            | "str_replace_all", [ a; pat; v; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrReplaceAll(lower a, lower pat, lower v, literal = literal)
            | "strip", [ a; m ]
            | "str_strip", [ a; m ] -> PolarsWrapper.StrStripChars(lower a, lower m)
            | "trim_start", [ a; m ]
            | "str_strip_start", [ a; m ] -> PolarsWrapper.StrStripCharsStart(lower a, lower m)
            | "trim_end", [ a; m ]
            | "str_strip_end", [ a; m ] -> PolarsWrapper.StrStripCharsEnd(lower a, lower m)
            | "format_string", formatStrLit :: argExprs ->
                let fmt = match formatStrLit with ScalarExpr.Literal(LiteralValue.String s) -> s | _ -> failwith "format must be string literal"
                let argHs = argExprs |> List.map lower |> List.toArray
                PolarsWrapper.FormatString(fmt, argHs)
            | "concat_str", sepLit :: itemExprs ->
                let sep = match sepLit with ScalarExpr.Literal(LiteralValue.String s) -> s | _ -> ""
                let itemHs = itemExprs |> List.map lower |> List.toArray
                PolarsWrapper.ConcatString(itemHs, sep, ignoreNulls = false)
            | "fill_null", [ e; v ] ->
                PolarsWrapper.FillNull(lower e, lower v)
            | "pad_start", [ t; w; ScalarExpr.Literal(LiteralValue.String pad) ] ->
                PolarsWrapper.StrPadStart(lower t, lower w, pad)
            | "pad_end", [ t; w; ScalarExpr.Literal(LiteralValue.String pad) ] ->
                PolarsWrapper.StrPadEnd(lower t, lower w, pad)
            | "normalize", [ t; ScalarExpr.Literal(LiteralValue.Int32 form) ] ->
                PolarsWrapper.StrNormalize(lower t, LanguagePrimitives.EnumOfValue<int, System.Text.NormalizationForm> form)
            | "str_contains", [ t; p; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrContains(lower t, lower p, literal = literal, strict = false)
            | "starts_with", [ t; p ] ->
                PolarsWrapper.StrStartsWith(lower t, lower p)
            | "ends_with", [ t; p ] ->
                PolarsWrapper.StrEndsWith(lower t, lower p)
            | "str_find", [ t; p; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrFind(lower t, lower p, literal = literal, strict = false)
            | "str_split", [ t; by; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrSplit(lower t, lower by, inclusive = false, literal = literal, strict = false)
            | "str_split", [ t; by ] ->
                PolarsWrapper.StrSplit(lower t, lower by, inclusive = false, literal = true, strict = false)
            | "str_split_n", [ t; by; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.StrSplitN(lower t, lower by, n)
            | "str_extract", [ t; pat; ScalarExpr.Literal(LiteralValue.Int32 idx) ] ->
                PolarsWrapper.StrExtract(lower t, lower pat, idx)
            | "str_extract_groups", [ t; ScalarExpr.Literal(LiteralValue.String pat) ] ->
                PolarsWrapper.StrExtractGroups(lower t, pat)
            | "str_extract_all", [ t; pat ] ->
                PolarsWrapper.StrExtractAll(lower t, lower pat)
            | "str_count_matches", [ t; pat; ScalarExpr.Literal(LiteralValue.Bool literal) ] ->
                PolarsWrapper.StrCountMatches(lower t, lower pat, literal = literal)

            // Temporal
            | "dt_year", [ a ] -> PolarsWrapper.DtYear(lower a)
            | "dt_month", [ a ] -> PolarsWrapper.DtMonth(lower a)
            | "dt_day", [ a ] -> PolarsWrapper.DtDay(lower a)
            | "dt_ordinal_day", [ a ] -> PolarsWrapper.DtOrdinalDay(lower a)
            | "dt_weekday", [ a ] -> PolarsWrapper.DtWeekday(lower a)
            | "dt_hour", [ a ] -> PolarsWrapper.DtHour(lower a)
            | "dt_minute", [ a ] -> PolarsWrapper.DtMinute(lower a)
            | "dt_second", [ a ] -> PolarsWrapper.DtSecond(lower a)
            | "dt_millisecond", [ a ] -> PolarsWrapper.DtMillisecond(lower a)
            | "dt_microsecond", [ a ] -> PolarsWrapper.DtMicrosecond(lower a)
            | "dt_nanosecond", [ a ] -> PolarsWrapper.DtNanosecond(lower a)
            | "dt_date", [ a ] -> PolarsWrapper.DtDate(lower a)
            | "dt_time", [ a ] -> PolarsWrapper.DtTime(lower a)
            | "dt_total_days", [ a ] -> PolarsWrapper.DtTotalDays(lower a, fractional = true)
            | "dt_total_hours", [ a ] -> PolarsWrapper.DtTotalHours(lower a, fractional = true)
            | "dt_total_minutes", [ a ] -> PolarsWrapper.DtTotalMinutes(lower a, fractional = true)
            | "dt_total_seconds", [ a ] -> PolarsWrapper.DtTotalSeconds(lower a, fractional = true)
            | "dt_total_milliseconds", [ a ] -> PolarsWrapper.DtTotalMilliseconds(lower a, fractional = true)
            | "dt_total_microseconds", [ a ] -> PolarsWrapper.DtTotalMicroseconds(lower a, fractional = true)
            | "dt_total_nanoseconds", [ a ] -> PolarsWrapper.DtTotalNanoseconds(lower a, fractional = true)
            | "str_to_datetime", [ e; ScalarExpr.Literal(LiteralValue.String fmt); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                let formatStr = if String.IsNullOrEmpty fmt then null else fmt
                PolarsWrapper.StrToDatetime(e = lower e, unit = PlTimeUnit.Microseconds, timeZone = null, format = formatStr, strict = false, exact = exact, cache = true, ambiguous = PolarsWrapper.Lit "raise")
            | "str_to_datetime", [ e; ScalarExpr.Literal(LiteralValue.Null _); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToDatetime(e = lower e, unit = PlTimeUnit.Microseconds, timeZone = null, format = null, strict = false, exact = exact, cache = true, ambiguous = PolarsWrapper.Lit "raise")
            | "str_to_date", [ e; ScalarExpr.Literal(LiteralValue.String fmt); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToDate(lower e, fmt, strict = false, exact = exact, cache = true)
            | "str_to_date", [ e; ScalarExpr.Literal(LiteralValue.Null _); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToDate(lower e, format = null, strict = false, exact = exact, cache = true)
            | "str_to_time", [ e; ScalarExpr.Literal(LiteralValue.String fmt); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToTime(lower e, fmt, strict = false, exact = exact, cache = true)
            | "str_to_time", [ e; ScalarExpr.Literal(LiteralValue.Null _); ScalarExpr.Literal(LiteralValue.Bool exact) ] ->
                PolarsWrapper.StrToTime(lower e, format = null, strict = false, exact = exact, cache = true)
            | "dt_is_leap_year", [ e ] ->
                PolarsWrapper.DtIsLeapYear(lower e)
            | "dt_to_string", [ e; ScalarExpr.Literal(LiteralValue.String fmt) ] ->
                PolarsWrapper.DtToString(lower e, fmt)
            | "dt_offset_by", [ e; ScalarExpr.Literal(LiteralValue.String offset) ] ->
                PolarsWrapper.DtOffsetBy(lower e, PolarsWrapper.Lit offset)

            // Struct & List
            | "struct_field_by_index", [ a; ScalarExpr.Literal(LiteralValue.Int64 idx) ] ->
                PolarsWrapper.StructFieldByIndex(lower a, idx)
            | "struct_field_by_index", [ a; ScalarExpr.Literal(LiteralValue.Int32 idx) ] ->
                PolarsWrapper.StructFieldByIndex(lower a, int64 idx)
            | "struct_field_by_name", [ a; ScalarExpr.Literal(LiteralValue.String name) ] ->
                PolarsWrapper.StructFieldByName(lower a, [| name |])
            | "list_get", [ a; idx ] -> PolarsWrapper.ListGet(lower a, lower idx, nullOnOob = true)
            | "list_contains", [ a; item ] -> PolarsWrapper.ListContains(lower a, lower item, nullsEqual = true)
            | "list_len", [ a ] -> PolarsWrapper.ListLen(lower a)
            | "is_in", [ a; b ] -> PolarsWrapper.IsIn(lower a, lower b, nullsEqual = false)
            | "list_sum", [ a ] -> PolarsWrapper.ListSum(lower a)
            | "list_mean", [ a ] -> PolarsWrapper.ListMean(lower a)
            | "list_min", [ a ] -> PolarsWrapper.ListMin(lower a)
            | "list_max", [ a ] -> PolarsWrapper.ListMax(lower a)
            | "list_join", [ a; ScalarExpr.Literal(LiteralValue.String sep) ] -> PolarsWrapper.ListJoin(lower a, sep, ignoreNulls = true)
            | "list_head", [ a; c ] -> PolarsWrapper.ListHead(lower a, lower c)
            | "list_tail", [ a; c ] -> PolarsWrapper.ListTail(lower a, lower c)
            | "list_slice", [ a; off; len ] -> PolarsWrapper.ListSlice(lower a, lower off, lower len)
            | "list_unique", [ a ] -> PolarsWrapper.ListEval(lower a, PolarsWrapper.ExprUnique(PolarsWrapper.Col("")))
            | "list_reverse", [ a ] -> PolarsWrapper.ListEval(lower a, PolarsWrapper.Reverse(PolarsWrapper.Col("")))
            | "list_sort", [ a; ScalarExpr.Literal(LiteralValue.Bool desc) ] -> PolarsWrapper.ListSort(lower a, descending = desc, nullsLast = false, maintainOrder = false)
            | "list_intersection", [ a; b ] -> PolarsWrapper.ListSetIntersection(lower a, lower b)
            | "list_union", [ a; b ] -> PolarsWrapper.ListSetUnion(lower a, lower b)
            | "list_difference", [ a; b ] -> PolarsWrapper.ListSetDifference(lower a, lower b)
            | "list_any", [ t; cond ] ->
                let anyExpr = PolarsWrapper.Any(lower cond, ignoreNulls = false)
                let boolList = PolarsWrapper.ListEval(lower t, anyExpr)
                PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true)
            | "list_all", [ t; cond ] ->
                let allExpr = PolarsWrapper.All(lower cond, ignoreNulls = false)
                let boolList = PolarsWrapper.ListEval(lower t, allExpr)
                PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true)
            | "list_eval", [ t; mapper ] -> PolarsWrapper.ListEval(lower t, lower mapper)

            // JSON
            | "str_json_path_match", [ a; b ] -> PolarsWrapper.StrJsonPathMatch(lower a, lower b)
            | "struct_json_encode", [ a ] -> PolarsWrapper.StructJsonEncode(lower a)
            | "as_struct", items -> PolarsWrapper.AsStruct(items |> List.map lower |> List.toArray)
            | "str_json_decode", [ a; ScalarExpr.Literal(LiteralValue.String typeName) ] ->
                let dtoType = Type.GetType typeName
                let dtypeHandle = inferPolarsJsonDataType dtoType |> Option.get
                use _h = dtypeHandle
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle
                PolarsWrapper.StrJsonDecode(lower a, dtypeExpr)

            // Window & Extensions
            | "shift", [ a; b ] -> PolarsWrapper.Shift(lower a, lower b)
            | "diff", [ a; b; ScalarExpr.Literal(LiteralValue.Int32 nb) ] ->
                PolarsWrapper.Diff(lower a, lower b, LanguagePrimitives.EnumOfValue<byte, PlNullBehavior> (byte nb))
            | "rank", [ a; ScalarExpr.Literal(LiteralValue.Int32 m); ScalarExpr.Literal(LiteralValue.Bool desc) ] ->
                PolarsWrapper.Rank(lower a, LanguagePrimitives.EnumOfValue<byte, PlRankMethod> (byte m), desc, Nullable())
            | "pct_change", [ a; ScalarExpr.Literal(LiteralValue.Int64 n) ] -> PolarsWrapper.PctChange(lower a, n)
            | "interpolate", [ a; ScalarExpr.Literal(LiteralValue.Int32 m) ] ->
                PolarsWrapper.Interpolate(lower a, LanguagePrimitives.EnumOfValue<byte, PlInterpolationMethod> (byte m))
            | "interpolate_by", [ a; b ] -> PolarsWrapper.InterpolateBy(lower a, lower b)
            | "cum_sum", [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumSum(lower a, rev)
            | "cum_min", [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumMin(lower a, rev)
            | "cum_max", [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumMax(lower a, rev)
            | "cum_prod", [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumProd(lower a, rev)
            | "cum_count", [ a; ScalarExpr.Literal(LiteralValue.Bool rev) ] -> PolarsWrapper.CumCount(lower a, rev)
            | "regex_escape", [ a ] -> PolarsWrapper.StrEscapeRegex(lower a)
            | "head", [ a; b ] -> PolarsWrapper.StrHead(lower a, lower b)
            | "tail", [ a; b ] -> PolarsWrapper.StrTail(lower a, lower b)
            | "str_to_integer", [ a; b; ScalarExpr.Literal(LiteralValue.Int32 dtypeVal) ] ->
                let dtHandle = PolarsWrapper.NewPrimitiveType dtypeVal
                PolarsWrapper.StrToInteger(lower a, lower b, dtHandle, strict = false)
            | "str_to_decimal", [ a; ScalarExpr.Literal(LiteralValue.Int32 scale) ] ->
                PolarsWrapper.StrToDecimal(lower a, scale)
            | "hex_encode", [ a ] -> PolarsWrapper.StrHexEncode(lower a)
            | "hex_decode", [ a ] -> PolarsWrapper.StrHexDecode(lower a, false)
            | "base64_encode", [ a ] -> PolarsWrapper.StrBase64Encode(lower a)
            | "base64_decode", [ a ] -> PolarsWrapper.StrBase64Decode(lower a, false)

            // Bitwise operations
            | "bit_left_shift", [ a; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.BitLeftShift(lower a, n)
            | "bit_right_shift", [ a; ScalarExpr.Literal(LiteralValue.Int32 n) ] ->
                PolarsWrapper.BitRightShift(lower a, n)

            | _ -> failwithf "Unsupported function in PolarsScalarLowerer: %s" name

        | ScalarExpr.Subquery(_, kind) ->
            failwithf "Subquery scalar expression must be decorrelated into relational join before physical lowering: %A" kind

        | ScalarExpr.Opaque(handle, _, _) ->
            PolarsWrapper.CloneExpr handle

