namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions

/// Strongly-typed literal representation eliminating heap boxing and type ambiguity
[<RequireQualifiedAccess>]
type LiteralValue =
    | Null of LogicalType
    | Bool of bool
    | Int8 of sbyte
    | Int16 of int16
    | Int32 of int
    | Int64 of int64
    | UInt8 of byte
    | UInt16 of uint16
    | UInt32 of uint32
    | UInt64 of uint64
    | Float16 of Half
    | Float32 of float32
    | Float64 of double
    | Decimal of decimal
    | String of string
    | Date of DateOnly
    | DateTime of DateTime
    | DateTimeOffset of DateTimeOffset
    | TimeSpan of TimeSpan
    | TimeOnly of TimeOnly
    with
        /// Returns the exact Polars data type for this literal value.
        member this.DataType: LogicalType =
            match this with
            | Null dt -> dt
            | Bool _ -> LogicalTypes.Boolean
            | Int8 _ -> LogicalTypes.Int8
            | Int16 _ -> LogicalTypes.Int16
            | Int32 _ -> LogicalTypes.Int32
            | Int64 _ -> LogicalTypes.Int64
            | UInt8 _ -> LogicalTypes.UInt8
            | UInt16 _ -> LogicalTypes.UInt16
            | UInt32 _ -> LogicalTypes.UInt32
            | Float16 _ -> LogicalTypes.Float16
            | UInt64 _ -> LogicalTypes.UInt64
            | Float32 _ -> LogicalTypes.Float32
            | Float64 _ -> LogicalTypes.Float64
            | Decimal _ -> LogicalTypes.Decimal
            | String _ -> LogicalTypes.String
            | Date _ -> LogicalTypes.Date
            | DateTime _ -> LogicalTypes.Datetime
            | DateTimeOffset _ -> LogicalTypes.DatetimeUtc
            | TimeSpan _ -> LogicalTypes.Duration
            | TimeOnly _ -> LogicalTypes.Time

        /// Extracts the boxed object representation on demand for FFI boundaries.
        member this.BoxedValue: obj =
            match this with
            | Null _ -> null
            | Bool b -> box b
            | Int8 i -> box i
            | Int16 i -> box i
            | Int32 i -> box i
            | Int64 i -> box i
            | UInt8 u -> box u
            | UInt16 u -> box u
            | UInt32 u -> box u
            | UInt64 u -> box u
            | Float16 h -> box h
            | Float32 f -> box f
            | Float64 f -> box f
            | Decimal m -> box m
            | String s -> box s
            | Date d -> box d
            | DateTime dt -> box dt
            | DateTimeOffset dto -> box dto
            | TimeSpan ts -> box ts
            | TimeOnly to' -> box to'

        /// Converts a dynamic runtime object into a strongly-typed LiteralValue.
        static member OfObject(raw: obj) : LiteralValue =
            match LiteralValue.TryOfObject raw with
            | Some literal -> literal
            | None -> failwithf "Unsupported literal value type: %s" (raw.GetType().FullName)

        /// Converts a dynamic runtime object into a strongly-typed LiteralValue, or None for a value of a type
        /// without a literal form.
        static member TryOfObject(raw: obj) : LiteralValue option =
            match raw with
            | null -> Some(LiteralValue.Null LogicalTypes.Null)
            | :? bool as b -> Some(LiteralValue.Bool b)
            | :? int as i -> Some(LiteralValue.Int32 i)
            | :? int64 as i -> Some(LiteralValue.Int64 i)
            | :? double as d -> Some(LiteralValue.Float64 d)
            | :? float32 as f -> Some(LiteralValue.Float32 f)
            | :? Half as h -> Some(LiteralValue.Float16 h)
            | :? string as s -> Some(LiteralValue.String s)
            | :? decimal as m -> Some(LiteralValue.Decimal m)
            | :? sbyte as b -> Some(LiteralValue.Int8 b)
            | :? int16 as s -> Some(LiteralValue.Int16 s)
            | :? byte as b -> Some(LiteralValue.UInt8 b)
            | :? uint16 as u -> Some(LiteralValue.UInt16 u)
            | :? uint32 as u -> Some(LiteralValue.UInt32 u)
            | :? uint64 as u -> Some(LiteralValue.UInt64 u)
            | :? DateOnly as d -> Some(LiteralValue.Date d)
            | :? DateTime as dt -> Some(LiteralValue.DateTime dt)
            | :? DateTimeOffset as dto -> Some(LiteralValue.DateTimeOffset dto)
            | :? TimeSpan as ts -> Some(LiteralValue.TimeSpan ts)
            | :? TimeOnly as to' -> Some(LiteralValue.TimeOnly to')
            | _ -> None

/// Binary operations supported in ScalarExpr
[<RequireQualifiedAccess>]
type BinaryOp =
    // Comparison
    | Eq
    | NotEq
    /// C# equality: null equals null (Polars eq_missing). Used where both operands may be null.
    | EqMissing
    /// C# inequality: null equals null (Polars ne_missing).
    | NotEqMissing
    | Lt
    | LtEq
    | Gt
    | GtEq
    // Logical
    | And
    | Or
    | Xor
    // Arithmetic
    | Add
    | Subtract
    | Multiply
    | Divide
    | Modulo
    | FloorDivide
    | Power
    // String & Pattern
    /// .NET string concatenation (`a + b` over strings): a null operand concatenates as the empty string.
    | StringConcat
    | Like
    | Contains
    | StartsWith
    | EndsWith
    | RegexMatch

/// Unary operations supported in ScalarExpr
[<RequireQualifiedAccess>]
type UnaryOp =
    | Not
    | Negate
    | IsNull
    | IsNotNull
    /// Whether a floating value is NaN (null for null).
    | IsNan
    | Abs

/// Aggregation kinds supported in ScalarExpr and GroupBy/Window nodes
[<RequireQualifiedAccess>]
type AggKind =
    | Sum
    | Mean
    /// .NET Min: over floating values, NaN as soon as one value is NaN (Max skips NaN unless every value is NaN).
    | Min
    | Max
    | Count
    | First
    | Last
    | Median
    | Std of ddof: byte
    | Var of ddof: byte
    | Quantile of prob: double
    | List
    | Len

/// Single field representation within a LogicalSchema
type LogicalField =
    {
        Name: string
        DataType: LogicalType
        Nullable: bool
    }

/// Relational schema description for logical plan nodes
type LogicalSchema =
    {
        Fields: LogicalField list
    }
    with
        static member Empty = { Fields = [] }

        static member OfNames(names: string seq) =
            {
                Fields =
                    names
                    |> Seq.map (fun n ->
                        {
                            Name = n
                            DataType = LogicalType.Unknown
                            Nullable = true
                        })
                    |> Seq.toList
            }

        static member OfFields(fields: (string * LogicalType) seq) =
            {
                Fields =
                    fields
                    |> Seq.map (fun (n, dt) ->
                        {
                            Name = n
                            DataType = dt
                            Nullable = true
                        })
                    |> Seq.toList
            }

        member this.ColumnNames: string list = this.Fields |> List.map (fun f -> f.Name)

        member this.FieldCount: int = this.Fields.Length

        member this.TryFindField(name: string) : LogicalField option =
            this.Fields |> List.tryFind (fun f -> f.Name = name)

        member this.Contains(name: string) : bool =
            this.Fields |> List.exists (fun f -> f.Name = name)

        member this.AddField(field: LogicalField) : LogicalSchema =
            {
                Fields = this.Fields @ [ field ]
            }

        member this.Merge(other: LogicalSchema) : LogicalSchema =
            let existing = this.ColumnNames |> Set.ofList

            let uniqueOther =
                other.Fields |> List.filter (fun f -> not (existing.Contains f.Name))

            {
                Fields = this.Fields @ uniqueOther
            }

/// The kind of error a failed check raises, in the vocabulary of the query language (LINQ's exceptions).
[<RequireQualifiedAccess>]
type QueryErrorKind =
    | InvalidOperation
    | ArgumentOutOfRange

/// An error a query raises when a check fails while it runs. An empty message stands for the error kind's
/// default message.
type QueryError = { Kind: QueryErrorKind; Message: string }

/// A required range for a row count (`Single` needs exactly one row, `First` at least one, `ElementAt(n)`
/// at least one after skipping n), and the error each side of the range raises.
type CountCheck =
    {
        Min: int64
        Max: int64 option
        TooFew: QueryError
        TooMany: QueryError
    }

/// Strongly-typed built-in scalar functions for intermediate representation
[<RequireQualifiedAccess>]
type ScalarFunction =
    // --- Math / Numeric ---
    | Abs
    | Round of mode: RoundMode
    | Floor
    | Ceil
    | Truncate
    | Sign
    | Sqrt
    | Cbrt
    | Exp
    | Log
    | Pow
    | Sin
    | Cos
    | Tan
    | Asin
    | Acos
    | Atan
    | Atan2
    | Sinh
    | Cosh
    | Tanh
    | Clamp

    // --- String ---
    | ToUpper
    | ToLower
    | ToTitlecase
    | LenChars
    | LenBytes
    | Reverse
    | Slice
    | SplitExact
    | Replace
    | ReplaceAll
    | Strip
    | StripStart
    | StripEnd
    /// Fills the `{}` holes of a format literal with its arguments, as String.Format / String.Concat: a null
    /// argument formats as the empty string.
    | FormatString
    /// Concatenates its items (after a separator literal), as String.Concat / String.Join over values: a null
    /// item contributes the empty string, never a null result.
    | ConcatString
    | PadStart
    | PadEnd
    | Normalize of form: System.Text.NormalizationForm
    | StrContains
    | StartsWith
    | EndsWith
    | StrFind
    | StrSplit
    | StrSplitN
    | StrExtract
    | StrExtractGroups
    | StrExtractAll
    | StrCountMatches
    | RegexEscape
    | Head
    | Tail
    | HexEncode
    | HexDecode
    | Base64Encode
    | Base64Decode
    | StrToInteger of targetType: LogicalType
    | StrToDecimal

    // --- Temporal ---
    | DtYear
    | DtMonth
    | DtDay
    | DtOrdinalDay
    | DtWeekday
    | DtHour
    | DtMinute
    | DtSecond
    | DtMillisecond
    | DtMicrosecond
    | DtNanosecond
    | DtDate
    | DtTime
    | DtTotalDays
    | DtTotalHours
    | DtTotalMinutes
    | DtTotalSeconds
    | DtTotalMilliseconds
    | DtTotalMicroseconds
    | DtTotalNanoseconds
    | StrToDatetime
    | StrToDate
    | StrToTime
    | DtIsLeapYear
    | DtToString
    | DtOffsetBy

    // --- Null & Conditional ---
    | FillNull
    | Coalesce

    // --- Struct & List ---
    | StructFieldByIndex
    | StructFieldByName
    | AsStruct
    | ListGet
    /// Whether a list holds an item; `nullsEqual` decides whether a null item matches a null element.
    | ListContains of nullsEqual: bool
    | ListLen
    /// Whether a value is in a list value; `nullsEqual` decides whether a null value matches a null element.
    | IsIn of nullsEqual: bool
    | ListSum
    | ListMean
    | ListMin
    | ListMax
    /// Joins a list of strings with a separator, as String.Join: a null element contributes the empty string.
    | ListJoin
    | ListHead
    | ListTail
    | ListSlice
    | ListUnique
    | ListReverse
    | ListSort
    | ListIntersection
    | ListUnion
    | ListDifference
    | ListAny
    | ListAll
    | ListEval

    // --- JSON ---
    | StrJsonPathMatch
    | StructJsonEncode
    | StrJsonDecode of targetType: JsonDataType

    // --- Window & Series ---
    | Shift
    | Diff of nullBehavior: NullBehavior
    | Rank of method: RankMethod * seed: uint64 option
    | PctChange
    | Interpolate of method: InterpolationMethod
    | InterpolateBy
    | CumSum
    | CumMin
    | CumMax
    | CumProd
    | CumCount

    // --- Bitwise ---
    | BitLeftShift
    | BitRightShift
    | BitwiseCountOnes
    | BitwiseLeadingZeros
    | BitwiseTrailingZeros

    // --- Group / Sequence Expression Modifiers ---
    | Implode
    | NUnique
    | Unique
    | MinBy
    | MaxBy
    | Filter
    | Quantile of method: QuantileMethod
    | SeriesGet
    /// Maps each value listed in `oldValues` to the value at the same position of `newValues`. A value not
    /// listed takes the optional second argument, or is an error without one.
    | ReplaceStrict of oldValues: LiteralValue list * newValues: LiteralValue list
    /// `[value]`: the value when it is null or one of `values`; otherwise the query fails with `message`, its
    /// first "{}" replaced by the value. It has an effect, as AssertCount.
    | RequireListed of values: LiteralValue list * message: string
    /// `[count; value]`: the value when the count lies in the check's range; otherwise the query fails with
    /// the check's error. It has an effect, so it must be evaluated wherever its value is used, never folded
    /// or reordered past the computation of its count.
    | AssertCount of check: CountCheck

    // --- Custom / Extension Function ---
    | Custom of name: string

    member this.Name: string =
        match this with
        | ScalarFunction.Abs -> "abs"
        | ScalarFunction.Round _ -> "round"
        | ScalarFunction.Floor -> "floor"
        | ScalarFunction.Ceil -> "ceil"
        | ScalarFunction.Truncate -> "truncate"
        | ScalarFunction.Sign -> "sign"
        | ScalarFunction.Sqrt -> "sqrt"
        | ScalarFunction.Cbrt -> "cbrt"
        | ScalarFunction.Exp -> "exp"
        | ScalarFunction.Log -> "log"
        | ScalarFunction.Pow -> "pow"
        | ScalarFunction.Sin -> "sin"
        | ScalarFunction.Cos -> "cos"
        | ScalarFunction.Tan -> "tan"
        | ScalarFunction.Asin -> "asin"
        | ScalarFunction.Acos -> "acos"
        | ScalarFunction.Atan -> "atan"
        | ScalarFunction.Atan2 -> "atan2"
        | ScalarFunction.Sinh -> "sinh"
        | ScalarFunction.Cosh -> "cosh"
        | ScalarFunction.Tanh -> "tanh"
        | ScalarFunction.Clamp -> "clamp"
        | ScalarFunction.ToUpper -> "to_upper"
        | ScalarFunction.ToLower -> "to_lower"
        | ScalarFunction.ToTitlecase -> "to_titlecase"
        | ScalarFunction.LenChars -> "len_chars"
        | ScalarFunction.LenBytes -> "len_bytes"
        | ScalarFunction.Reverse -> "reverse"
        | ScalarFunction.Slice -> "slice"
        | ScalarFunction.SplitExact -> "split_exact"
        | ScalarFunction.Replace -> "replace"
        | ScalarFunction.ReplaceAll -> "replace_all"
        | ScalarFunction.Strip -> "strip"
        | ScalarFunction.StripStart -> "strip_start"
        | ScalarFunction.StripEnd -> "strip_end"
        | ScalarFunction.FormatString -> "format_string"
        | ScalarFunction.ConcatString -> "concat_str"
        | ScalarFunction.PadStart -> "pad_start"
        | ScalarFunction.PadEnd -> "pad_end"
        | ScalarFunction.Normalize _ -> "normalize"
        | ScalarFunction.StrContains -> "str_contains"
        | ScalarFunction.StartsWith -> "starts_with"
        | ScalarFunction.EndsWith -> "ends_with"
        | ScalarFunction.StrFind -> "str_find"
        | ScalarFunction.StrSplit -> "str_split"
        | ScalarFunction.StrSplitN -> "str_split_n"
        | ScalarFunction.StrExtract -> "str_extract"
        | ScalarFunction.StrExtractGroups -> "str_extract_groups"
        | ScalarFunction.StrExtractAll -> "str_extract_all"
        | ScalarFunction.StrCountMatches -> "str_count_matches"
        | ScalarFunction.RegexEscape -> "regex_escape"
        | ScalarFunction.Head -> "head"
        | ScalarFunction.Tail -> "tail"
        | ScalarFunction.HexEncode -> "hex_encode"
        | ScalarFunction.HexDecode -> "hex_decode"
        | ScalarFunction.Base64Encode -> "base64_encode"
        | ScalarFunction.Base64Decode -> "base64_decode"
        | ScalarFunction.StrToInteger _ -> "str_to_integer"
        | ScalarFunction.StrToDecimal -> "str_to_decimal"
        | ScalarFunction.DtYear -> "dt_year"
        | ScalarFunction.DtMonth -> "dt_month"
        | ScalarFunction.DtDay -> "dt_day"
        | ScalarFunction.DtOrdinalDay -> "dt_ordinal_day"
        | ScalarFunction.DtWeekday -> "dt_weekday"
        | ScalarFunction.DtHour -> "dt_hour"
        | ScalarFunction.DtMinute -> "dt_minute"
        | ScalarFunction.DtSecond -> "dt_second"
        | ScalarFunction.DtMillisecond -> "dt_millisecond"
        | ScalarFunction.DtMicrosecond -> "dt_microsecond"
        | ScalarFunction.DtNanosecond -> "dt_nanosecond"
        | ScalarFunction.DtDate -> "dt_date"
        | ScalarFunction.DtTime -> "dt_time"
        | ScalarFunction.DtTotalDays -> "dt_total_days"
        | ScalarFunction.DtTotalHours -> "dt_total_hours"
        | ScalarFunction.DtTotalMinutes -> "dt_total_minutes"
        | ScalarFunction.DtTotalSeconds -> "dt_total_seconds"
        | ScalarFunction.DtTotalMilliseconds -> "dt_total_milliseconds"
        | ScalarFunction.DtTotalMicroseconds -> "dt_total_microseconds"
        | ScalarFunction.DtTotalNanoseconds -> "dt_total_nanoseconds"
        | ScalarFunction.StrToDatetime -> "str_to_datetime"
        | ScalarFunction.StrToDate -> "str_to_date"
        | ScalarFunction.StrToTime -> "str_to_time"
        | ScalarFunction.DtIsLeapYear -> "dt_is_leap_year"
        | ScalarFunction.DtToString -> "dt_to_string"
        | ScalarFunction.DtOffsetBy -> "dt_offset_by"
        | ScalarFunction.FillNull -> "fill_null"
        | ScalarFunction.Coalesce -> "coalesce"
        | ScalarFunction.StructFieldByIndex -> "struct_field_by_index"
        | ScalarFunction.StructFieldByName -> "struct_field_by_name"
        | ScalarFunction.AsStruct -> "as_struct"
        | ScalarFunction.ListGet -> "list_get"
        | ScalarFunction.ListContains _ -> "list_contains"
        | ScalarFunction.ListLen -> "list_len"
        | ScalarFunction.IsIn _ -> "is_in"
        | ScalarFunction.ListSum -> "list_sum"
        | ScalarFunction.ListMean -> "list_mean"
        | ScalarFunction.ListMin -> "list_min"
        | ScalarFunction.ListMax -> "list_max"
        | ScalarFunction.ListJoin -> "list_join"
        | ScalarFunction.ListHead -> "list_head"
        | ScalarFunction.ListTail -> "list_tail"
        | ScalarFunction.ListSlice -> "list_slice"
        | ScalarFunction.ListUnique -> "list_unique"
        | ScalarFunction.ListReverse -> "list_reverse"
        | ScalarFunction.ListSort -> "list_sort"
        | ScalarFunction.ListIntersection -> "list_intersection"
        | ScalarFunction.ListUnion -> "list_union"
        | ScalarFunction.ListDifference -> "list_difference"
        | ScalarFunction.ListAny -> "list_any"
        | ScalarFunction.ListAll -> "list_all"
        | ScalarFunction.ListEval -> "list_eval"
        | ScalarFunction.StrJsonPathMatch -> "str_json_path_match"
        | ScalarFunction.StructJsonEncode -> "struct_json_encode"
        | ScalarFunction.StrJsonDecode _ -> "str_json_decode"
        | ScalarFunction.Shift -> "shift"
        | ScalarFunction.Diff _ -> "diff"
        | ScalarFunction.Rank _ -> "rank"
        | ScalarFunction.PctChange -> "pct_change"
        | ScalarFunction.Interpolate _ -> "interpolate"
        | ScalarFunction.InterpolateBy -> "interpolate_by"
        | ScalarFunction.CumSum -> "cum_sum"
        | ScalarFunction.CumMin -> "cum_min"
        | ScalarFunction.CumMax -> "cum_max"
        | ScalarFunction.CumProd -> "cum_prod"
        | ScalarFunction.CumCount -> "cum_count"
        | ScalarFunction.BitLeftShift -> "bit_left_shift"
        | ScalarFunction.BitRightShift -> "bit_right_shift"
        | ScalarFunction.BitwiseCountOnes -> "bitwise_count_ones"
        | ScalarFunction.BitwiseLeadingZeros -> "bitwise_leading_zeros"
        | ScalarFunction.BitwiseTrailingZeros -> "bitwise_trailing_zeros"
        | ScalarFunction.Implode -> "implode"
        | ScalarFunction.NUnique -> "n_unique"
        | ScalarFunction.Unique -> "unique"
        | ScalarFunction.MinBy -> "min_by"
        | ScalarFunction.MaxBy -> "max_by"
        | ScalarFunction.Filter -> "filter"
        | ScalarFunction.Quantile _ -> "quantile"
        | ScalarFunction.SeriesGet -> "series_get"
        | ScalarFunction.ReplaceStrict _ -> "replace_strict"
        | ScalarFunction.AssertCount _ -> "assert_count"
        | ScalarFunction.RequireListed _ -> "require_listed"
        | ScalarFunction.Custom name -> name

    static member FromString(str: string) : ScalarFunction =
        match str.ToLowerInvariant() with
        | "abs" -> ScalarFunction.Abs
        | "round" -> ScalarFunction.Round RoundMode.HalfToEven
        | "floor" -> ScalarFunction.Floor
        | "ceil" | "ceiling" -> ScalarFunction.Ceil
        | "truncate" -> ScalarFunction.Truncate
        | "sign" -> ScalarFunction.Sign
        | "sqrt" -> ScalarFunction.Sqrt
        | "cbrt" -> ScalarFunction.Cbrt
        | "exp" -> ScalarFunction.Exp
        | "log" -> ScalarFunction.Log
        | "pow" -> ScalarFunction.Pow
        | "sin" -> ScalarFunction.Sin
        | "cos" -> ScalarFunction.Cos
        | "tan" -> ScalarFunction.Tan
        | "asin" -> ScalarFunction.Asin
        | "acos" -> ScalarFunction.Acos
        | "atan" -> ScalarFunction.Atan
        | "atan2" -> ScalarFunction.Atan2
        | "sinh" -> ScalarFunction.Sinh
        | "cosh" -> ScalarFunction.Cosh
        | "tanh" -> ScalarFunction.Tanh
        | "clamp" | "clip" -> ScalarFunction.Clamp
        | "to_upper" -> ScalarFunction.ToUpper
        | "to_lower" -> ScalarFunction.ToLower
        | "to_titlecase" -> ScalarFunction.ToTitlecase
        | "len_chars" -> ScalarFunction.LenChars
        | "len_bytes" -> ScalarFunction.LenBytes
        | "reverse" -> ScalarFunction.Reverse
        | "slice" | "str_slice" -> ScalarFunction.Slice
        | "split_exact" | "str_split_exact" -> ScalarFunction.SplitExact
        | "replace" | "str_replace" -> ScalarFunction.Replace
        | "replace_all" | "str_replace_all" -> ScalarFunction.ReplaceAll
        | "strip" | "str_strip" -> ScalarFunction.Strip
        | "trim_start" | "str_strip_start" -> ScalarFunction.StripStart
        | "trim_end" | "str_strip_end" -> ScalarFunction.StripEnd
        | "format_string" -> ScalarFunction.FormatString
        | "concat_str" -> ScalarFunction.ConcatString
        | "fill_null" -> ScalarFunction.FillNull
        | "coalesce" -> ScalarFunction.Coalesce
        | "pad_start" -> ScalarFunction.PadStart
        | "pad_end" -> ScalarFunction.PadEnd
        | "normalize" -> ScalarFunction.Normalize System.Text.NormalizationForm.FormC
        | "str_contains" -> ScalarFunction.StrContains
        | "starts_with" -> ScalarFunction.StartsWith
        | "ends_with" -> ScalarFunction.EndsWith
        | "str_find" -> ScalarFunction.StrFind
        | "str_split" -> ScalarFunction.StrSplit
        | "str_split_n" -> ScalarFunction.StrSplitN
        | "str_extract" -> ScalarFunction.StrExtract
        | "str_extract_groups" -> ScalarFunction.StrExtractGroups
        | "str_extract_all" -> ScalarFunction.StrExtractAll
        | "str_count_matches" -> ScalarFunction.StrCountMatches
        | "regex_escape" -> ScalarFunction.RegexEscape
        | "head" -> ScalarFunction.Head
        | "tail" -> ScalarFunction.Tail
        | "hex_encode" -> ScalarFunction.HexEncode
        | "hex_decode" -> ScalarFunction.HexDecode
        | "base64_encode" -> ScalarFunction.Base64Encode
        | "base64_decode" -> ScalarFunction.Base64Decode
        | "str_to_integer" -> ScalarFunction.StrToInteger LogicalTypes.Int64
        | "str_to_decimal" -> ScalarFunction.StrToDecimal
        | "dt_year" -> ScalarFunction.DtYear
        | "dt_month" -> ScalarFunction.DtMonth
        | "dt_day" -> ScalarFunction.DtDay
        | "dt_ordinal_day" -> ScalarFunction.DtOrdinalDay
        | "dt_weekday" -> ScalarFunction.DtWeekday
        | "dt_hour" -> ScalarFunction.DtHour
        | "dt_minute" -> ScalarFunction.DtMinute
        | "dt_second" -> ScalarFunction.DtSecond
        | "dt_millisecond" -> ScalarFunction.DtMillisecond
        | "dt_microsecond" -> ScalarFunction.DtMicrosecond
        | "dt_nanosecond" -> ScalarFunction.DtNanosecond
        | "dt_date" -> ScalarFunction.DtDate
        | "dt_time" -> ScalarFunction.DtTime
        | "dt_total_days" -> ScalarFunction.DtTotalDays
        | "dt_total_hours" -> ScalarFunction.DtTotalHours
        | "dt_total_minutes" -> ScalarFunction.DtTotalMinutes
        | "dt_total_seconds" -> ScalarFunction.DtTotalSeconds
        | "dt_total_milliseconds" -> ScalarFunction.DtTotalMilliseconds
        | "dt_total_microseconds" -> ScalarFunction.DtTotalMicroseconds
        | "dt_total_nanoseconds" -> ScalarFunction.DtTotalNanoseconds
        | "str_to_datetime" -> ScalarFunction.StrToDatetime
        | "str_to_date" -> ScalarFunction.StrToDate
        | "str_to_time" -> ScalarFunction.StrToTime
        | "dt_is_leap_year" -> ScalarFunction.DtIsLeapYear
        | "dt_to_string" -> ScalarFunction.DtToString
        | "dt_offset_by" -> ScalarFunction.DtOffsetBy
        | "struct_field_by_index" -> ScalarFunction.StructFieldByIndex
        | "struct_field_by_name" -> ScalarFunction.StructFieldByName
        | "as_struct" -> ScalarFunction.AsStruct
        | "list_get" -> ScalarFunction.ListGet
        | "list_contains" -> ScalarFunction.ListContains true
        | "list_len" -> ScalarFunction.ListLen
        | "is_in" -> ScalarFunction.IsIn false
        | "list_sum" -> ScalarFunction.ListSum
        | "list_mean" -> ScalarFunction.ListMean
        | "list_min" -> ScalarFunction.ListMin
        | "list_max" -> ScalarFunction.ListMax
        | "list_join" -> ScalarFunction.ListJoin
        | "list_head" -> ScalarFunction.ListHead
        | "list_tail" -> ScalarFunction.ListTail
        | "list_slice" -> ScalarFunction.ListSlice
        | "list_unique" -> ScalarFunction.ListUnique
        | "list_reverse" -> ScalarFunction.ListReverse
        | "list_sort" -> ScalarFunction.ListSort
        | "list_intersection" -> ScalarFunction.ListIntersection
        | "list_union" -> ScalarFunction.ListUnion
        | "list_difference" -> ScalarFunction.ListDifference
        | "list_any" -> ScalarFunction.ListAny
        | "list_all" -> ScalarFunction.ListAll
        | "list_eval" -> ScalarFunction.ListEval
        | "str_json_path_match" -> ScalarFunction.StrJsonPathMatch
        | "struct_json_encode" -> ScalarFunction.StructJsonEncode
        | "str_json_decode" -> raise (NotSupportedException "str_json_decode carries its target schema and cannot be created from a function name.")
        | "shift" -> ScalarFunction.Shift
        | "diff" -> ScalarFunction.Diff NullBehavior.Ignore
        | "rank" -> ScalarFunction.Rank(RankMethod.Average, None)
        | "pct_change" -> ScalarFunction.PctChange
        | "interpolate" -> ScalarFunction.Interpolate InterpolationMethod.Linear
        | "interpolate_by" -> ScalarFunction.InterpolateBy
        | "cum_sum" -> ScalarFunction.CumSum
        | "cum_min" -> ScalarFunction.CumMin
        | "cum_max" -> ScalarFunction.CumMax
        | "cum_prod" -> ScalarFunction.CumProd
        | "cum_count" -> ScalarFunction.CumCount
        | "bit_left_shift" -> ScalarFunction.BitLeftShift
        | "bit_right_shift" -> ScalarFunction.BitRightShift
        | "bitwise_count_ones" | "popcnt" | "popcount" -> ScalarFunction.BitwiseCountOnes
        | "bitwise_leading_zeros" | "leading_zeros" | "leadingzerocount" -> ScalarFunction.BitwiseLeadingZeros
        | "bitwise_trailing_zeros" | "trailing_zeros" | "trailingzerocount" -> ScalarFunction.BitwiseTrailingZeros
        | "implode" -> ScalarFunction.Implode
        | "n_unique" -> ScalarFunction.NUnique
        | "unique" -> ScalarFunction.Unique
        | "min_by" -> ScalarFunction.MinBy
        | "max_by" -> ScalarFunction.MaxBy
        | "filter" -> ScalarFunction.Filter
        | "quantile" -> ScalarFunction.Quantile QuantileMethod.Nearest
        | "series_get" -> ScalarFunction.SeriesGet
        | other -> ScalarFunction.Custom other

/// Strongly-typed descriptor for managed UDF execution strategies
[<RequireQualifiedAccess>]
type UdfInvoker =
    | SingleBatch of callback: obj * clrReturnType: Type
    | MultiBatch of callback: obj * clrReturnType: Type
    | GroupSingleBatch of callback: obj * clrReturnType: Type * returnsScalar: bool
    | GroupMultiBatch of callback: obj * clrReturnType: Type * returnsScalar: bool
    | StringPredicate of predicate: Func<string, bool>
    | StringTransform of transform: Func<string, string>
    | StringToList of mapper: Func<string, seq<string>>

/// In-memory collection source for IN predicates
[<RequireQualifiedAccess>]
type InListSource =
    | Literals of LiteralValue list
    | Collection of values: System.Collections.IEnumerable * elementType: Type

/// How a .NET enum maps to a Polars Enum: its categories (member names) and their underlying values, both in
/// category order, and the underlying integer type.
type EnumMapping =
    {
        /// The enum type's full name, for error messages.
        Name: string
        Categories: string list
        Values: LiteralValue list
        Underlying: LogicalType
    }

/// A conversion involving a .NET enum whose column representation is not known when it is translated: an Enum
/// column (categories) or an integer column (underlying values). EnumResolution picks the form from the schema.
[<RequireQualifiedAccess>]
type EnumConversion =
    /// The underlying value of an enum (`(int)e`, and enum comparisons, which C# performs on underlying values).
    | ToValue
    /// The enum holding an underlying value (`(E)i`, and enum constants, which arrive as their underlying value).
    | FromValue
    /// The member name of an enum (`e.ToString()`); an undefined value gives its number, as in .NET.
    | ToName

/// Pure symbolic scalar and column expression tree
type [<RequireQualifiedAccess>] ScalarExpr =
    | Column of name: string
    | Literal of value: LiteralValue
    | Binary of op: BinaryOp * left: ScalarExpr * right: ScalarExpr
    | Unary of op: UnaryOp * expr: ScalarExpr
    | Cast of expr: ScalarExpr * targetType: LogicalType * isStrict: bool
    | Function of func: ScalarFunction * args: ScalarExpr list * returnType: LogicalType
    | Aggregation of kind: AggKind * expr: ScalarExpr option * isDistinct: bool
    | Window of expr: ScalarExpr * partitionBy: ScalarExpr list * orderBy: SortOrder list
    | SortBy of expr: ScalarExpr * by: SortOrder list
    | Conditional of test: ScalarExpr * ifTrue: ScalarExpr * ifFalse: ScalarExpr
    | Coalesce of exprs: ScalarExpr list
    | ItemAccess of target: ScalarExpr * index: ScalarExpr
    | Slice of target: ScalarExpr * offset: int64 * length: uint32
    | Subquery of plan: LogicalPlan * kind: SubqueryKind
    | Alias of expr: ScalarExpr * alias: string
    | Udf of name: string * args: ScalarExpr list * returnType: LogicalType * invoker: UdfInvoker
    /// Whether a value is in an in-memory collection. With `nullsEqual` a null value is in a collection holding
    /// null (.NET Contains); without, a null value gives null, as SQL IN.
    | InList of expr: ScalarExpr * source: InListSource * negated: bool * nullsEqual: bool
    /// An enum conversion awaiting EnumResolution, which replaces it before the plan is lowered.
    | EnumConvert of conversion: EnumConversion * expr: ScalarExpr * mapping: EnumMapping

/// Subquery semantics when a scalar expression embeds a logical plan
and [<RequireQualifiedAccess>] SubqueryKind =
    | ScalarScalar
    | Exists of negated: bool
    | InSubquery of target: ScalarExpr * negated: bool

/// Ordering specification for sorting and window operations
and SortOrder =
    {
        Expr: ScalarExpr
        Descending: bool
        NullsLast: bool
    }

/// Abstract representation of external or physical table scan sources
and ILogicalTableSource =
    abstract member Name: string
    abstract member Schema: LogicalSchema

/// Input source for Leaf Scan nodes
and [<RequireQualifiedAccess>] LogicalPlanSource =
    /// Index into the source bindings of the compiled query that owns the plan.
    | Slot of index: int
    | SubqueryRef of subqueryId: string
    | Empty

/// Data a Scan slot reads at execution time. Bindings live beside the plan, never inside it,
/// so a LogicalPlan carries no source instances.
and [<RequireQualifiedAccess>] SourceBinding =
    | Table of source: ILogicalTableSource
    /// Snapshot of an in-memory .NET collection (typed array of elementType). Materialized by the backend at lowering time.
    | InMemory of rows: Array * elementType: Type

/// Specification for multi-dimensional Grouping Sets (ROLLUP, CUBE, GROUPING SETS). The result is one block of
/// rows per level, in Levels order: a level groups by its subset of KeyAliases (by index; groups in first-key
/// order, an empty subset giving one grand-total row), leaves the keys outside the subset null, and sets the
/// columns its literal map names. OutputColumns lists the result's columns.
and LogicalGroupingSetsSpec =
    {
        KeyAliases: (string * ScalarExpr) list
        Aggs: (string * ScalarExpr) list
        Levels: (int list * Map<string, ScalarExpr>) list
        OutputColumns: string list
    }

/// Specification for Logical AsOf join operations. A null `on` or `by` key never matches (on either side).
and LogicalAsOfJoinSpec =
    {
        LeftOn: ScalarExpr
        RightOn: ScalarExpr
        LeftBy: ScalarExpr list
        RightBy: ScalarExpr list
        Strategy: AsofStrategy
        ToleranceStr: string option
        ToleranceInt: int64 option
        ToleranceFloat: double option
        AllowEq: bool
        CheckSorted: bool
        Suffix: string option
        Coalesce: JoinKeyCoalescing
    }

/// Pure relational algebra logical plan tree. Every backend implements the semantics stated here and in
/// IR/SEMANTICS.md; Polars.NET.Linq.Conformance.Tests checks them on each backend.
/// Rows are ordered: a node keeps its input's order unless stated otherwise.
and [<RequireQualifiedAccess>] LogicalPlan =
    | Scan of source: LogicalPlanSource * schema: LogicalSchema
    /// Keeps the rows whose predicate is true; a null predicate drops the row, as false does.
    | Filter of input: LogicalPlan * predicate: ScalarExpr
    /// One row per input row, with exactly the listed columns; when every projection aggregates, one row.
    /// An expression that reads other rows (window, rank, cumulative) sees this node's whole input.
    | Project of input: LogicalPlan * projections: (string * ScalarExpr) list
    /// The input columns plus (or replaced by) the listed ones, evaluated as in Project.
    | WithColumns of input: LogicalPlan * columns: (string * ScalarExpr) list
    /// Inner / Left: left order, then right order among a row's matches. Semi / Anti: left order. Cross: no
    /// order. A null key matches a null key exactly when `nullsEqual`.
    | Join of
        left: LogicalPlan *
        right: LogicalPlan *
        leftOn: ScalarExpr list *
        rightOn: ScalarExpr list *
        how: JoinKind *
        suffix: string option *
        coalesce: JoinKeyCoalescing *
        nullsEqual: bool
    | JoinAsOf of left: LogicalPlan * right: LogicalPlan * spec: LogicalAsOfJoinSpec
    /// One row per distinct key; `having` (an aggregate predicate over a group's rows) keeps the groups for
    /// which it is true. With `maintainOrder` groups appear in the order their keys first appear, otherwise
    /// in no order.
    | GroupBy of
        input: LogicalPlan *
        keys: ScalarExpr list *
        aggs: (string * ScalarExpr) list *
        having: ScalarExpr option *
        maintainOrder: bool
    | GroupingSets of input: LogicalPlan * spec: LogicalGroupingSetsSpec
    /// A stable sort: rows with equal keys keep their input order. Nulls go first or last by NullsLast.
    /// (CoreConfig.AllowUnstableOrdering lets the Polars backend drop the stability, by user choice.)
    | Sort of input: LogicalPlan * orderings: SortOrder list
    /// `count` rows after skipping `offset`.
    | Limit of input: LogicalPlan * offset: int64 * count: uint32
    /// One row per distinct value of `subset` (all columns when None), the one `keep` names; in input order
    /// with `maintainOrder`, otherwise in no order.
    | Distinct of input: LogicalPlan * subset: string list option * keep: DistinctKeep * maintainOrder: bool
    | Explode of input: LogicalPlan * columns: string list * emptyAsNull: bool * keepNulls: bool
    | Unpivot of
        input: LogicalPlan *
        index: string list *
        on: string list option *
        variableName: string *
        valueName: string
    /// The left rows, then the right rows; without `isAll`, duplicates removed.
    | Union of left: LogicalPlan * right: LogicalPlan * isAll: bool
    | WithRowIndex of input: LogicalPlan * name: string * offset: uint32 option
    | Rename of input: LogicalPlan * existingNames: string list * newNames: string list
    | Reverse of input: LogicalPlan
    | SkipLast of input: LogicalPlan * count: uint32
    | HorizontalConcat of inputs: LogicalPlan list
    | Empty of schema: LogicalSchema

/// One managed row operation applied after the native plan, in pipeline order.
[<RequireQualifiedAccess>]
type ClientRowOp =
    /// Keeps the rows for which the compiled predicate holds.
    | Filter of Predicate: Func<obj, bool> * Source: LambdaExpression
    /// Replaces each row with the compiled selector's result.
    | Map of Selector: Func<obj, obj> * Source: LambdaExpression

    /// The LINQ lambda the operation was compiled from.
    member this.Lambda: LambdaExpression =
        match this with
        | Filter(_, lambda)
        | Map(_, lambda) -> lambda

/// Managed work that remains after the native LogicalPlan has been executed.
[<RequireQualifiedAccess>]
type ClientResidual =
    /// The whole pipeline runs natively.
    | None
    /// Native rows are materialized as the first operation's input type, then the operations
    /// are applied in pipeline order, row by row.
    | RowOps of Ops: ClientRowOp list
    /// The remaining pipeline needs LINQ-to-Objects (ordering, paging, chunking, ... after a client step).
    | LinqToObjects

/// Shared evaluation of ClientResidual row operations; every execution path applies them through here.
module ClientResidualOps =

    /// Element type the native rows are materialized as before the first operation.
    let rowType (ops: ClientRowOp list) : Type =
        match ops with
        | first :: _ -> first.Lambda.Parameters.[0].Type
        | [] -> invalidArg (nameof ops) "ClientResidual.RowOps requires at least one operation."

    /// Applies the operations to the materialized rows, in pipeline order.
    let apply (ops: ClientRowOp list) (rows: System.Collections.IEnumerable) : seq<obj> =
        ops
        |> List.fold
            (fun (acc: seq<obj>) op ->
                match op with
                | ClientRowOp.Filter(predicate, _) -> Seq.filter predicate.Invoke acc
                | ClientRowOp.Map(selector, _) -> Seq.map selector.Invoke acc)
            (Seq.cast<obj> rows)

    /// The client predicates (with their source lambdas) the residual evaluates, for policy checks and diagnostics.
    let filters (residual: ClientResidual) : (Func<obj, bool> * LambdaExpression) list =
        match residual with
        | ClientResidual.RowOps ops ->
            ops |> List.choose (function ClientRowOp.Filter(p, l) -> Some(p, l) | ClientRowOp.Map _ -> Option.None)
        | _ -> []

    /// When the residual only projects (no filtering, no fallback), the composed projection over the native rows.
    let tryProjectionOnly (residual: ClientResidual) : (Type * Func<obj, obj>) option =
        match residual with
        | ClientResidual.RowOps ops when ops |> List.forall (function ClientRowOp.Map _ -> true | _ -> false) ->
            let selectors = ops |> List.map (function ClientRowOp.Map(f, _) -> f | ClientRowOp.Filter _ -> null)
            Some(rowType ops, Func<obj, obj>(fun row -> selectors |> List.fold (fun acc f -> f.Invoke acc) row))
        | _ -> Option.None

    let isNative (residual: ClientResidual) : bool =
        match residual with
        | ClientResidual.None -> true
        | _ -> false

/// A compiled logical query: the native LogicalPlan, the sources its Scan slots refer to,
/// and the managed residual evaluated on its rows.
type CompiledLogicalQuery =
    {
        Plan: LogicalPlan
        Sources: SourceBinding array
        Residual: ClientResidual
        /// Join scopes still in effect over the plan's rows: (scope, member) -> physical column. Non-empty when
        /// the rows are a transparent identifier whose entities are kept as flat columns; a consumer that
        /// materializes the elements packs those entities first (EntityPacking.packRow), while one that grafts
        /// the plan reads the flat columns.
        RowShape: RowShape
    }

/// Internal interface implemented by PolarsQueryable to expose IR compilation for testing, inspection, and subtree grafting.
type internal IPolarsInternalExplainable =
    abstract member CompileLogicalQuery: unit -> CompiledLogicalQuery
    abstract member TryGetPureLogicalPlan: unit -> (LogicalPlan * SourceBinding array) option
    /// Whether the query's rows can be produced by a native plan, without full LINQ-to-Objects fallback.
    abstract member IsNativelyExecutable: unit -> bool
    /// The table this query's root reads (shared by every query derived from the same root).
    abstract member RootTable: ILogicalTableSource
