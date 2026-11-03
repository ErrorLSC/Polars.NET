namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Core

/// Strongly-typed literal representation eliminating heap boxing and type ambiguity
[<RequireQualifiedAccess>]
type LiteralValue =
    | Null of PlDataType
    | Bool of bool
    | Int8 of sbyte
    | Int16 of int16
    | Int32 of int
    | Int64 of int64
    | UInt8 of byte
    | UInt16 of uint16
    | UInt32 of uint32
    | UInt64 of uint64
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
        member this.DataType: PlDataType =
            match this with
            | Null dt -> dt
            | Bool _ -> PlDataType.Boolean
            | Int8 _ -> PlDataType.Int8
            | Int16 _ -> PlDataType.Int16
            | Int32 _ -> PlDataType.Int32
            | Int64 _ -> PlDataType.Int64
            | UInt8 _ -> PlDataType.UInt8
            | UInt16 _ -> PlDataType.UInt16
            | UInt32 _ -> PlDataType.UInt32
            | UInt64 _ -> PlDataType.UInt64
            | Float32 _ -> PlDataType.Float32
            | Float64 _ -> PlDataType.Float64
            | Decimal _ -> PlDataType.Decimal
            | String _ -> PlDataType.String
            | Date _ -> PlDataType.Date
            | DateTime _ -> PlDataType.Datetime
            | DateTimeOffset _ -> PlDataType.Datetime
            | TimeSpan _ -> PlDataType.Duration
            | TimeOnly _ -> PlDataType.Time

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
            match raw with
            | null -> LiteralValue.Null PlDataType.Null
            | :? bool as b -> LiteralValue.Bool b
            | :? int as i -> LiteralValue.Int32 i
            | :? int64 as i -> LiteralValue.Int64 i
            | :? double as d -> LiteralValue.Float64 d
            | :? float32 as f -> LiteralValue.Float32 f
            | :? string as s -> LiteralValue.String s
            | :? decimal as m -> LiteralValue.Decimal m
            | :? sbyte as b -> LiteralValue.Int8 b
            | :? int16 as s -> LiteralValue.Int16 s
            | :? byte as b -> LiteralValue.UInt8 b
            | :? uint16 as u -> LiteralValue.UInt16 u
            | :? uint32 as u -> LiteralValue.UInt32 u
            | :? uint64 as u -> LiteralValue.UInt64 u
            | :? DateOnly as d -> LiteralValue.Date d
            | :? DateTime as dt -> LiteralValue.DateTime dt
            | :? DateTimeOffset as dto -> LiteralValue.DateTimeOffset dto
            | :? TimeSpan as ts -> LiteralValue.TimeSpan ts
            | :? TimeOnly as to' -> LiteralValue.TimeOnly to'
            | other -> failwithf "Unsupported literal value type: %s" (other.GetType().FullName)

/// Binary operations supported in ScalarExpr
[<RequireQualifiedAccess>]
type BinaryOp =
    // Comparison
    | Eq
    | NotEq
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
    | Abs

/// Aggregation kinds supported in ScalarExpr and GroupBy/Window nodes
[<RequireQualifiedAccess>]
type AggKind =
    | Sum
    | Mean
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
        DataType: PlDataType
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
                            DataType = PlDataType.Unknown
                            Nullable = true
                        })
                    |> Seq.toList
            }

        static member OfFields(fields: (string * PlDataType) seq) =
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

/// Strongly-typed built-in scalar functions for intermediate representation
[<RequireQualifiedAccess>]
type ScalarFunction =
    // --- Math / Numeric ---
    | Abs
    | Round
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
    | CastDecimal

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
    | FormatString
    | ConcatString
    | PadStart
    | PadEnd
    | Normalize
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
    | StrToInteger
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
    | ListContains
    | ListLen
    | IsIn
    | ListSum
    | ListMean
    | ListMin
    | ListMax
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
    | StrJsonDecode

    // --- Window & Series ---
    | Shift
    | Diff
    | Rank
    | PctChange
    | Interpolate
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

    // --- Custom / Extension Function ---
    | Custom of name: string

    member this.Name: string =
        match this with
        | ScalarFunction.Abs -> "abs"
        | ScalarFunction.Round -> "round"
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
        | ScalarFunction.CastDecimal -> "cast_decimal"
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
        | ScalarFunction.Normalize -> "normalize"
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
        | ScalarFunction.StrToInteger -> "str_to_integer"
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
        | ScalarFunction.ListContains -> "list_contains"
        | ScalarFunction.ListLen -> "list_len"
        | ScalarFunction.IsIn -> "is_in"
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
        | ScalarFunction.StrJsonDecode -> "str_json_decode"
        | ScalarFunction.Shift -> "shift"
        | ScalarFunction.Diff -> "diff"
        | ScalarFunction.Rank -> "rank"
        | ScalarFunction.PctChange -> "pct_change"
        | ScalarFunction.Interpolate -> "interpolate"
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
        | ScalarFunction.Custom name -> name

    static member FromString(str: string) : ScalarFunction =
        match str.ToLowerInvariant() with
        | "abs" -> ScalarFunction.Abs
        | "round" -> ScalarFunction.Round
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
        | "cast_decimal" -> ScalarFunction.CastDecimal
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
        | "normalize" -> ScalarFunction.Normalize
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
        | "str_to_integer" -> ScalarFunction.StrToInteger
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
        | "list_contains" -> ScalarFunction.ListContains
        | "list_len" -> ScalarFunction.ListLen
        | "is_in" -> ScalarFunction.IsIn
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
        | "str_json_decode" -> ScalarFunction.StrJsonDecode
        | "shift" -> ScalarFunction.Shift
        | "diff" -> ScalarFunction.Diff
        | "rank" -> ScalarFunction.Rank
        | "pct_change" -> ScalarFunction.PctChange
        | "interpolate" -> ScalarFunction.Interpolate
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
        | other -> ScalarFunction.Custom other

/// Pure symbolic scalar and column expression tree
type [<RequireQualifiedAccess>] ScalarExpr =
    | Column of name: string
    | Literal of value: LiteralValue
    | Binary of op: BinaryOp * left: ScalarExpr * right: ScalarExpr
    | Unary of op: UnaryOp * expr: ScalarExpr
    | Cast of expr: ScalarExpr * targetType: PlDataType * isStrict: bool
    | Function of func: ScalarFunction * args: ScalarExpr list * returnType: PlDataType
    | Aggregation of kind: AggKind * expr: ScalarExpr option * isDistinct: bool
    | Window of expr: ScalarExpr * partitionBy: ScalarExpr list * orderBy: SortOrder list
    | Conditional of test: ScalarExpr * ifTrue: ScalarExpr * ifFalse: ScalarExpr
    | Coalesce of exprs: ScalarExpr list
    | ItemAccess of target: ScalarExpr * index: ScalarExpr
    | Slice of target: ScalarExpr * offset: int64 * length: uint32
    | Subquery of plan: LogicalPlan * kind: SubqueryKind
    | Alias of expr: ScalarExpr * alias: string
    | Opaque of handle: ExprHandle * referencedCols: string list * description: string

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

/// Input source for Leaf Scan nodes
and [<RequireQualifiedAccess>] LogicalPlanSource =
    | LazyFrame of LazyFrameHandle
    | SubqueryRef of subqueryId: string
    | InMemoryTable of name: string * rows: obj list
    | Empty

/// Specification for Logical AsOf join operations
and LogicalAsOfJoinSpec =
    {
        LeftOn: ScalarExpr
        RightOn: ScalarExpr
        LeftBy: ScalarExpr list
        RightBy: ScalarExpr list
        Strategy: PlAsofStrategy
        ToleranceStr: string option
        ToleranceInt: int64 option
        ToleranceFloat: double option
        AllowEq: bool
        CheckSorted: bool
        Suffix: string option
        Validation: PlJoinValidation
        Coalesce: PlJoinCoalesce
        MaintainOrder: PlJoinMaintainOrder
        JoinSide: PlJoinSide
        NullsEqual: bool
    }

/// Pure relational algebra logical plan tree
and [<RequireQualifiedAccess>] LogicalPlan =
    | Scan of source: LogicalPlanSource * schema: LogicalSchema
    | Filter of input: LogicalPlan * predicate: ScalarExpr
    | Project of input: LogicalPlan * projections: (string * ScalarExpr) list
    | WithColumns of input: LogicalPlan * columns: (string * ScalarExpr) list
    | Join of
        left: LogicalPlan *
        right: LogicalPlan *
        leftOn: ScalarExpr list *
        rightOn: ScalarExpr list *
        how: PlJoinType *
        suffix: string option *
        coalesce: PlJoinCoalesce *
        nullsEqual: bool
    | JoinAsOf of left: LogicalPlan * right: LogicalPlan * spec: LogicalAsOfJoinSpec
    | GroupBy of
        input: LogicalPlan *
        keys: ScalarExpr list *
        aggs: (string * ScalarExpr) list *
        having: ScalarExpr option *
        maintainOrder: bool
    | Sort of input: LogicalPlan * orderings: SortOrder list
    | Limit of input: LogicalPlan * offset: int64 * count: uint32
    | Distinct of input: LogicalPlan * subset: string list option * keep: PlUniqueKeepStrategy
    | Explode of input: LogicalPlan * columns: string list * emptyAsNull: bool * keepNulls: bool
    | Unpivot of
        input: LogicalPlan *
        index: string list *
        on: string list option *
        variableName: string *
        valueName: string
    | Union of left: LogicalPlan * right: LogicalPlan * isAll: bool
    | WithRowIndex of input: LogicalPlan * name: string * offset: uint32 option
    | Rename of input: LogicalPlan * existingNames: string list * newNames: string list
    | Reverse of input: LogicalPlan
    | SkipLast of input: LogicalPlan * count: uint32
    | HorizontalConcat of inputs: LogicalPlan list
    | Empty of schema: LogicalSchema

/// Represents a compiled logical query containing the pure symbolic LogicalPlan IR tree
/// alongside pure managed boundary classifications (client predicates, projection, fallback flags).
type CompiledLogicalQuery =
    {
        Plan: LogicalPlan
        ClientPredicates: (Func<obj, bool> * LambdaExpression) list
        ClientProjection: (Type * Func<obj, obj>) option
        RequiresClientFallback: bool
    }
