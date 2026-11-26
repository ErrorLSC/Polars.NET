namespace Kaminari.Samidare.Sql

open System
open System.Collections.Generic
open System.Globalization
open System.Text
open Apache.Arrow.Types
open Kaminari.Samidare.IR

/// Where a Delta table keeps an expression, which decides what it may read and call.
[<RequireQualifiedAccess>]
type SparkExpressionUse =
    /// A CHECK constraint or an invariant: a Boolean over the row.
    | Check
    /// A generated column's expression: deterministic, over the row.
    | Generated
    /// A column default: a constant (no column), where current_date() and current_timestamp() give the write's time.
    | Default

/// What a Spark expression is bound in.
type internal SparkContext =
    { Text: string
      /// The columns a name may refer to, as defined, with their types (a Delta TIMESTAMP is an instant: a timestamp
      /// with a time zone; a TIMESTAMP_NTZ has none).
      Columns: (string * LogicalType) list
      Use: SparkExpressionUse
      /// The session time zone: an instant's date, parts and text are its wall clock there (D2).
      Zone: TimeZoneInfo
      /// The write's time: current_date() and current_timestamp().
      Now: DateTimeOffset
      /// The columns read, as defined, in the order first read.
      Read: List<string> }

/// Binds Spark SQL expressions as Delta stores them (CHECK constraints, generated columns, column defaults) to the IR
/// with Spark 4's ANSI semantics, verified against Spark 4.2 (sql-s7-delta-expressions). Names ignore case, quoted or
/// not; `a.b` is a struct's field. Integer arithmetic, casts to narrower types and division fail the write where Spark
/// fails it (CheckedNarrow, CheckedRescale, CheckedDivide; the caller runs RaisingGuards). What Spark converts at run
/// time and the engine cannot reproduce is refused by name: text columns read as numbers or dates, floating values cast
/// to decimals or text, decimal arithmetic that may exceed 38 digits.
module internal SparkExpressions =

    let private fail (cx: SparkContext) (span: SqlSpan) (reason: string) (suggestion: string option) : 'T =
        raise (SqlException(reason, cx.Text, span, suggestion))

    let private value expr t : SqlValue = { Expr = expr; Type = t }
    let private lit (v: LiteralValue) = ScalarExpr.Literal v
    let private nullOf (t: LogicalType) = value (lit (LiteralValue.Null t)) t
    let private fn (f: ScalarFunction) (args: ScalarExpr list) (t: LogicalType) = ScalarExpr.Function(f, args, t)
    let private cast (e: ScalarExpr) (t: LogicalType) = ScalarExpr.Cast(e, t, false)
    let private bin (op: BinaryOp) (a: ScalarExpr) (b: ScalarExpr) = ScalarExpr.Binary(op, a, b)
    let private str (s: string) = lit (LiteralValue.String s)
    let private i32 (n: int) = lit (LiteralValue.Int32 n)
    let private i64 (n: int64) = lit (LiteralValue.Int64 n)
    let private isNull (e: ScalarExpr) = ScalarExpr.Unary(UnaryOp.IsNull, e)
    /// `e`, null where `x` is (a value computed from x by a function that reads null as something else).
    let private nullWhere (x: ScalarExpr) (t: LogicalType) (e: ScalarExpr) = ScalarExpr.Conditional(isNull x, lit (LiteralValue.Null t), e)

    let private where (cx: SparkContext) =
        match cx.Use with
        | SparkExpressionUse.Check -> "in a CHECK constraint"
        | SparkExpressionUse.Generated -> "in a generated column"
        | SparkExpressionUse.Default -> "in a column default"

    // ----------------------------------------------------------------------------------------------------------
    // Types
    // ----------------------------------------------------------------------------------------------------------

    let private classify = SqlTypes.classify

    /// Whether a type is an instant (Spark's TIMESTAMP): a timestamp with a time zone.
    let private isInstant (t: LogicalType) =
        match t with
        | LogicalType.Arrow(:? TimestampType as ts) -> not (String.IsNullOrEmpty ts.Timezone)
        | _ -> false

    /// A type's name as Spark writes it, for messages.
    let display (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Null -> "VOID"
        | SqlTypeClass.Integer(32, true) -> "INT"
        | SqlTypeClass.Floating 32 -> "FLOAT"
        | SqlTypeClass.Decimal(p, s) -> $"DECIMAL({p},{s})"
        | SqlTypeClass.String -> "STRING"
        | SqlTypeClass.Timestamp -> if isInstant t then "TIMESTAMP" else "TIMESTAMP_NTZ"
        | _ -> SqlTypes.display t

    let private integerOf (bits: int) =
        match bits with
        | 8 -> LogicalTypes.Int8
        | 16 -> LogicalTypes.Int16
        | 32 -> LogicalTypes.Int32
        | _ -> LogicalTypes.Int64

    /// The digits of an integer literal (Spark types an integer literal in decimal arithmetic by its digits).
    let private digits (n: int64) = if n = 0L then 1 else (string (if n = Int64.MinValue then Int64.MaxValue else abs n)).Length

    /// A numeric value as Spark sees it in decimal arithmetic: a decimal's precision and scale; an integer literal's
    /// digits; an integer column's type (TINYINT (3,0), SMALLINT (5,0), INT (10,0), BIGINT (20,0)).
    let private decimalOf (v: SqlValue) =
        match v.Expr, classify v.Type with
        | ScalarExpr.Literal(LiteralValue.Int8 n), _ -> Some(digits (int64 n), 0)
        | ScalarExpr.Literal(LiteralValue.Int16 n), _ -> Some(digits (int64 n), 0)
        | ScalarExpr.Literal(LiteralValue.Int32 n), _ -> Some(digits (int64 n), 0)
        | ScalarExpr.Literal(LiteralValue.Int64 n), _ -> Some(digits n, 0)
        | _, SqlTypeClass.Decimal(p, s) -> Some(p, s)
        | _, SqlTypeClass.Integer(8, _) -> Some(3, 0)
        | _, SqlTypeClass.Integer(16, _) -> Some(5, 0)
        | _, SqlTypeClass.Integer(32, _) -> Some(10, 0)
        | _, SqlTypeClass.Integer _ -> Some(20, 0)
        | _ -> None

    /// A numeric type's precision and scale as a decimal.
    let private decimalOfType (t: LogicalType) = decimalOf (nullOf t)

    /// The type two numeric values meet in, as Spark widens them (by their types; an integer literal is an INT or a
    /// BIGINT here): the wider integer; FLOAT with a FLOAT, a TINYINT or a SMALLINT; DOUBLE with a DOUBLE, a wider
    /// integer or a decimal; a decimal holding both.
    let private numericCommon (a: SqlValue) (b: SqlValue) : LogicalType option =
        match classify a.Type, classify b.Type with
        | SqlTypeClass.Integer(x, true), SqlTypeClass.Integer(y, true) -> Some(integerOf (max x y))
        | SqlTypeClass.Floating x, SqlTypeClass.Floating y -> Some(if max x y <= 32 then LogicalTypes.Float32 else LogicalTypes.Float64)
        | SqlTypeClass.Floating x, SqlTypeClass.Integer(bits, _)
        | SqlTypeClass.Integer(bits, _), SqlTypeClass.Floating x -> Some(if x <= 32 && bits <= 16 then LogicalTypes.Float32 else LogicalTypes.Float64)
        | SqlTypeClass.Floating _, SqlTypeClass.Decimal _
        | SqlTypeClass.Decimal _, SqlTypeClass.Floating _ -> Some LogicalTypes.Float64
        | (SqlTypeClass.Integer(_, true) | SqlTypeClass.Decimal _), (SqlTypeClass.Integer(_, true) | SqlTypeClass.Decimal _) ->
            let (p1, s1), (p2, s2) = (decimalOfType a.Type).Value, (decimalOfType b.Type).Value
            let s = max s1 s2
            Some(SqlTypes.decimal (SqlTypes.fitDecimal (max (p1 - s1) (p2 - s2) + s) s))
        | _ -> None

    let private isNumeric (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Integer(_, true)
        | SqlTypeClass.Decimal _
        | SqlTypeClass.Floating _ -> true
        | _ -> false

    let private isUtc (zone: TimeZoneInfo) = zone.BaseUtcOffset = TimeSpan.Zero && not zone.SupportsDaylightSavingTime

    /// An instant as the session zone's wall clock (a timestamp without a zone); other values as they are.
    let private local (cx: SparkContext) (v: SqlValue) : ScalarExpr =
        if isInstant v.Type then fn (ScalarFunction.DtInZone cx.Zone.Id) [ v.Expr ] LogicalTypes.Datetime else v.Expr

    // ----------------------------------------------------------------------------------------------------------
    // Literals
    // ----------------------------------------------------------------------------------------------------------

    let private decimalLiteral (m: decimal) (precision: int) (scale: int) =
        let t = SqlTypes.decimal (precision, scale)
        value (ScalarExpr.Cast(lit (LiteralValue.Decimal m), t, false)) t

    /// A decimal's precision and scale as its text gives them (`0.05` is DECIMAL(2,2), `1.50` DECIMAL(3,2)).
    let private precisionOf (m: decimal) =
        let scale = int m.Scale
        let unscaled = (abs m).ToString(CultureInfo.InvariantCulture).Replace(".", "").TrimStart('0')
        max 1 (max unscaled.Length scale), scale

    let private integerLiteral (cx: SparkContext) (span: SqlSpan) (text: string) =
        match Int32.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture) with
        | true, v -> value (i32 v) LogicalTypes.Int32
        | _ ->
            match Int64.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture) with
            | true, v -> value (i64 v) LogicalTypes.Int64
            | _ ->
                let trimmed = text.TrimStart('0')
                match Decimal.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture) with
                | true, v when trimmed.Length <= SqlTypes.maxPrecision -> decimalLiteral v trimmed.Length 0
                | _ -> fail cx span $"The number {text} has more than {SqlTypes.maxPrecision} digits" None

    let private parseDecimal (cx: SparkContext) (span: SqlSpan) (text: string) =
        match Decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture) with
        | true, m ->
            let p, s = precisionOf m
            if p > SqlTypes.maxPrecision then fail cx span $"The number {text} has more than {SqlTypes.maxPrecision} digits" None
            decimalLiteral m p s
        | _ -> fail cx span $"The number {text} has more than {SqlTypes.maxPrecision} digits" None

    let private suffixed (cx: SparkContext) (span: SqlSpan) (number: string) (suffix: string) =
        let integral (range: string) (make: int64 -> LiteralValue) (low: int64) (high: int64) (t: LogicalType) =
            match Int64.TryParse(number, NumberStyles.None, CultureInfo.InvariantCulture) with
            | true, v when v >= low && v <= high -> value (lit (make v)) t
            | _ -> fail cx span $"{number}{suffix} is beyond the range of {range}" None
        match suffix with
        | "Y" -> integral "TINYINT" (fun v -> LiteralValue.Int8(sbyte v)) (int64 SByte.MinValue) (int64 SByte.MaxValue) LogicalTypes.Int8
        | "S" -> integral "SMALLINT" (fun v -> LiteralValue.Int16(int16 v)) (int64 Int16.MinValue) (int64 Int16.MaxValue) LogicalTypes.Int16
        | "L" -> integral "BIGINT" LiteralValue.Int64 Int64.MinValue Int64.MaxValue LogicalTypes.Int64
        | "D" -> value (lit (LiteralValue.Float64(Double.Parse(number, CultureInfo.InvariantCulture)))) LogicalTypes.Float64
        | "F" -> value (lit (LiteralValue.Float32(Single.Parse(number, CultureInfo.InvariantCulture)))) LogicalTypes.Float32
        | _ -> parseDecimal cx span number

    let private dateFormats = [| "yyyy-M-d"; "yyyy-M-d'T'" |]
    let private timestampFormats =
        [| "yyyy-M-d H:m:s"; "yyyy-M-d H:m:s.FFFFFF"; "yyyy-M-d'T'H:m:s"; "yyyy-M-d'T'H:m:s.FFFFFF"; "yyyy-M-d H:m"; "yyyy-M-d'T'H:m"; "yyyy-M-d" |]

    let private parseDate (text: string) =
        match DateOnly.TryParseExact(text.Trim(), dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None) with
        | true, d -> Some d
        | _ -> None

    let private parseTimestamp (text: string) =
        match DateTime.TryParseExact(text.Trim(), timestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None) with
        | true, d -> Some d
        | _ -> None

    /// An instant literal (the engine reads a date-time literal without its zone: the cast gives it UTC's).
    let private instant (v: DateTimeOffset) =
        value (cast (lit (LiteralValue.DateTime v.UtcDateTime)) LogicalTypes.DatetimeUtc) LogicalTypes.DatetimeUtc

    /// A wall-clock time in the session zone as an instant.
    let private instantOf (cx: SparkContext) (span: SqlSpan) (wallClock: DateTime) =
        let unspecified = DateTime.SpecifyKind(wallClock, DateTimeKind.Unspecified)
        if cx.Zone.IsInvalidTime unspecified then
            fail cx span $"{wallClock:``yyyy-MM-dd HH:mm:ss``} does not exist in the time zone {cx.Zone.Id} (a daylight saving gap)" None
        let utc = TimeZoneInfo.ConvertTimeToUtc(unspecified, cx.Zone)
        instant (DateTimeOffset(utc, TimeSpan.Zero))

    let private typedLiteral (cx: SparkContext) (span: SqlSpan) (typeName: string) (text: string) =
        match typeName with
        | "DATE" ->
            match parseDate text with
            | Some d -> value (lit (LiteralValue.Date d)) LogicalTypes.Date
            | None -> fail cx span $"'{text}' is not a date" (Some "Write dates as 'YYYY-MM-DD'.")
        | "TIMESTAMP"
        | "TIMESTAMP_LTZ"
        | "TIMESTAMP_NTZ" ->
            match parseTimestamp text with
            | Some d when typeName = "TIMESTAMP_NTZ" -> value (lit (LiteralValue.DateTime d)) LogicalTypes.Datetime
            | Some d -> instantOf cx span d
            | None -> fail cx span $"'{text}' is not a timestamp" (Some "Write timestamps as 'YYYY-MM-DD HH:MM:SS[.ffffff]', without a zone.")
        | _ -> fail cx span $"{typeName} literals are not supported" None

    /// A string literal read as a value of type `target` (Spark casts the text; one that does not convert fails every
    /// row): an integer as a BIGINT, a decimal at its own precision, a floating value as DOUBLE.
    let private literalAs (cx: SparkContext) (span: SqlSpan) (text: string) (target: LogicalType) : SqlValue =
        let malformed (what: string) =
            fail cx span $"The string '{text}' is not a valid {what}: Spark fails every row on it (CAST_INVALID_INPUT)" None
        let trimmed = text.Trim()
        match classify target with
        | SqlTypeClass.Integer _ ->
            match Int64.TryParse(trimmed, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture) with
            | true, v -> value (i64 v) LogicalTypes.Int64
            | _ -> malformed "BIGINT"
        | SqlTypeClass.Decimal _ ->
            match Decimal.TryParse(trimmed, NumberStyles.AllowLeadingSign ||| NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture) with
            | true, m ->
                let p, s = precisionOf m
                if p > SqlTypes.maxPrecision then malformed (display target)
                decimalLiteral m p s
            | _ -> malformed (display target)
        | SqlTypeClass.Floating _ ->
            match Double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture) with
            | true, v -> value (lit (LiteralValue.Float64 v)) LogicalTypes.Float64
            | _ -> malformed "DOUBLE"
        | SqlTypeClass.Boolean ->
            match trimmed.ToLowerInvariant() with
            | "true" | "t" | "yes" | "y" | "1" -> value (lit (LiteralValue.Bool true)) LogicalTypes.Boolean
            | "false" | "f" | "no" | "n" | "0" -> value (lit (LiteralValue.Bool false)) LogicalTypes.Boolean
            | _ -> malformed "BOOLEAN"
        | SqlTypeClass.Date ->
            match parseDate trimmed with
            | Some d -> value (lit (LiteralValue.Date d)) LogicalTypes.Date
            | None -> malformed "DATE"
        | SqlTypeClass.Timestamp ->
            match parseTimestamp trimmed with
            | Some d when isInstant target -> instantOf cx span d
            | Some d -> value (lit (LiteralValue.DateTime d)) LogicalTypes.Datetime
            | None -> malformed (display target)
        | _ -> value (str text) LogicalTypes.String

    let private stringLiteral (v: SqlValue) =
        match v.Expr with
        | ScalarExpr.Literal(LiteralValue.String s) -> Some s
        | _ -> None

    // ----------------------------------------------------------------------------------------------------------
    // Implicit conversions
    // ----------------------------------------------------------------------------------------------------------

    /// A numeric value as a numeric type: a literal as a literal of it, otherwise a cast (a widening, which is exact).
    let private widen (v: SqlValue) (t: LogicalType) : SqlValue =
        if v.Type.Display = t.Display then v
        else
            let integer =
                match v.Expr with
                | ScalarExpr.Literal(LiteralValue.Int8 n) -> Some(int64 n)
                | ScalarExpr.Literal(LiteralValue.Int16 n) -> Some(int64 n)
                | ScalarExpr.Literal(LiteralValue.Int32 n) -> Some(int64 n)
                | ScalarExpr.Literal(LiteralValue.Int64 n) -> Some n
                | _ -> None
            match integer, classify t with
            | Some n, SqlTypeClass.Integer(64, true) -> value (i64 n) t
            | Some n, SqlTypeClass.Integer(32, true) when n >= int64 Int32.MinValue && n <= int64 Int32.MaxValue -> value (i32 (int n)) t
            | Some n, SqlTypeClass.Decimal _ -> value (ScalarExpr.Cast(lit (LiteralValue.Decimal(decimal n)), t, false)) t
            | _ ->
                match v.Expr, classify t with
                | ScalarExpr.Cast(ScalarExpr.Literal(LiteralValue.Decimal m), _, _), SqlTypeClass.Decimal _ -> value (ScalarExpr.Cast(lit (LiteralValue.Decimal m), t, false)) t
                | _ -> value (cast v.Expr t) t

    /// The type two values compare in (`comparing`) or combine in (CASE, COALESCE, IN): NULL meets anything; a string
    /// literal is read as the other side's type; numbers widen; a date meets a TIMESTAMP_NTZ as one. Compared, a date or
    /// a TIMESTAMP_NTZ meets an instant on the session zone's wall clock. A text column meets only text (Spark would
    /// cast it at run time).
    let rec private common (cx: SparkContext) (span: SqlSpan) (what: string) (comparing: bool) (a: SqlValue) (b: SqlValue) : LogicalType =
        let refuse () = fail cx span $"Cannot {what} {display a.Type} and {display b.Type}" None
        if a.Type.Display = b.Type.Display then a.Type
        else
            match classify a.Type, classify b.Type with
            | SqlTypeClass.Null, _ -> b.Type
            | _, SqlTypeClass.Null -> a.Type
            | SqlTypeClass.String, SqlTypeClass.String -> LogicalTypes.String
            | SqlTypeClass.String, _ when (stringLiteral a).IsSome -> common cx span what comparing (literalAs cx span (stringLiteral a).Value b.Type) b
            | _, SqlTypeClass.String when (stringLiteral b).IsSome -> common cx span what comparing a (literalAs cx span (stringLiteral b).Value a.Type)
            | SqlTypeClass.String, _
            | _, SqlTypeClass.String ->
                fail cx span $"Cannot {what} {display a.Type} and {display b.Type}: Spark casts the text at run time, which is not supported" None
            | _ when isNumeric a.Type && isNumeric b.Type ->
                match numericCommon a b with
                | Some t -> t
                | None -> refuse ()
            | SqlTypeClass.Date, SqlTypeClass.Timestamp
            | SqlTypeClass.Timestamp, SqlTypeClass.Date ->
                if comparing || not (isInstant a.Type || isInstant b.Type) then LogicalTypes.Datetime else refuse ()
            | SqlTypeClass.Timestamp, SqlTypeClass.Timestamp ->
                if isInstant a.Type && isInstant b.Type then a.Type
                elif comparing then LogicalTypes.Datetime
                else refuse ()
            | _ -> refuse ()

    /// A value as the type `common` chose.
    let private coerce (cx: SparkContext) (span: SqlSpan) (v: SqlValue) (t: LogicalType) : SqlValue =
        if v.Type.Display = t.Display then v
        else
            match classify v.Type, stringLiteral v with
            | SqlTypeClass.Null, _ -> nullOf t
            | SqlTypeClass.String, Some text ->
                let parsed = literalAs cx span text t
                if isNumeric t then widen parsed t
                elif parsed.Type.Display = t.Display || isInstant t then parsed
                else value (cast parsed.Expr t) t
            | _ when isNumeric v.Type -> widen v t
            | SqlTypeClass.Date, _ -> value (cast v.Expr t) t
            | SqlTypeClass.Timestamp, _ when isInstant t -> value (cast v.Expr t) t
            | SqlTypeClass.Timestamp, _ -> value (local cx v) t
            | _ -> value (cast v.Expr t) t

    let private meet (cx: SparkContext) (span: SqlSpan) (what: string) (comparing: bool) (a: SqlValue) (b: SqlValue) =
        let t = common cx span what comparing a b
        coerce cx span a t, coerce cx span b t, t

    let private meetAll (cx: SparkContext) (span: SqlSpan) (what: string) (comparing: bool) (values: SqlValue list) =
        let t = values |> List.reduce (fun a b -> nullOf (common cx span what comparing a b)) |> fun v -> v.Type
        values |> List.map (fun v -> coerce cx span v t), t

    // ----------------------------------------------------------------------------------------------------------
    // Checked arithmetic
    // ----------------------------------------------------------------------------------------------------------

    let private bitsOf (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Integer(bits, _) -> bits
        | _ -> 64

    /// An integer operation in a type wide enough for every result (BIGINT for narrower types, DECIMAL(38,0) for
    /// BIGINT), narrowed back to `t`: Spark's ARITHMETIC_OVERFLOW where the result leaves it.
    let private checkedIntegral (t: LogicalType) (compute: (ScalarExpr -> ScalarExpr) -> ScalarExpr) =
        let wide = if bitsOf t < 64 then LogicalTypes.Int64 else SqlTypes.decimal (SqlTypes.maxPrecision, 0)
        ScalarExprOps.checkedNarrow t ScalarExprOps.unguarded (compute (fun e -> cast e wide))

    let private negate (cx: SparkContext) (span: SqlSpan) (v: SqlValue) =
        match v.Expr, classify v.Type with
        | ScalarExpr.Literal(LiteralValue.Int32 n), _ when n <> Int32.MinValue -> value (i32 -n) v.Type
        | ScalarExpr.Literal(LiteralValue.Int64 n), _ when n <> Int64.MinValue -> value (i64 -n) v.Type
        | ScalarExpr.Literal(LiteralValue.Float64 x), _ -> value (lit (LiteralValue.Float64 -x)) v.Type
        | ScalarExpr.Literal(LiteralValue.Float32 x), _ -> value (lit (LiteralValue.Float32 -x)) v.Type
        | ScalarExpr.Cast(ScalarExpr.Literal(LiteralValue.Decimal m), t, strict), _ -> value (ScalarExpr.Cast(lit (LiteralValue.Decimal -m), t, strict)) v.Type
        | _, SqlTypeClass.Null -> v
        | _, SqlTypeClass.Integer(_, true) -> value (checkedIntegral v.Type (fun wide -> ScalarExpr.Unary(UnaryOp.Negate, wide v.Expr))) v.Type
        | _, (SqlTypeClass.Decimal _ | SqlTypeClass.Floating _) -> value (ScalarExpr.Unary(UnaryOp.Negate, v.Expr)) v.Type
        | _ -> fail cx span $"Cannot negate {display v.Type}" None

    let private opText (op: SqlBinaryOp) =
        match op with
        | SqlBinaryOp.Add -> "+"
        | SqlBinaryOp.Subtract -> "-"
        | SqlBinaryOp.Multiply -> "*"
        | SqlBinaryOp.Divide -> "/"
        | SqlBinaryOp.IntegerDivide -> "div"
        | _ -> "%"

    let private arithmetic (cx: SparkContext) (span: SqlSpan) (op: SqlBinaryOp) (l: SqlValue) (r: SqlValue) : SqlValue =
        let text = opText op
        let operand (v: SqlValue) =
            match classify v.Type with
            | SqlTypeClass.Null -> ()
            | _ when isNumeric v.Type -> ()
            | SqlTypeClass.String when (stringLiteral v).IsSome -> ()
            | _ -> fail cx span $"'{text}' takes numbers, not {display v.Type}" (if classify v.Type = SqlTypeClass.Date then Some "Date arithmetic is not supported." else None)
        operand l
        operand r
        // A string literal is read as a number (Spark casts it to DOUBLE in arithmetic).
        let asNumber (v: SqlValue) =
            match stringLiteral v with
            | Some s -> literalAs cx span s LogicalTypes.Float64
            | None -> v
        let l, r = asNumber l, asNumber r
        // NULL takes the other operand's type.
        let l, r =
            match classify l.Type, classify r.Type with
            | SqlTypeClass.Null, SqlTypeClass.Null -> nullOf LogicalTypes.Int32, nullOf LogicalTypes.Int32
            | SqlTypeClass.Null, _ -> nullOf r.Type, r
            | _, SqlTypeClass.Null -> l, nullOf l.Type
            | _ -> l, r
        let floating = (match classify l.Type with SqlTypeClass.Floating _ -> true | _ -> false) || (match classify r.Type with SqlTypeClass.Floating _ -> true | _ -> false)
        let integral = (match classify l.Type with SqlTypeClass.Integer _ -> true | _ -> false) && (match classify r.Type with SqlTypeClass.Integer _ -> true | _ -> false)
        let decimalOperands () =
            let d1, d2 = (decimalOf l).Value, (decimalOf r).Value
            let asDecimal (v: SqlValue) (p, s) = widen v (SqlTypes.decimal (p, s))
            d1, d2, asDecimal l d1, asDecimal r d2
        let divide kind t (a: SqlValue) (b: SqlValue) = value (ScalarExprOps.checkedDivide kind t ScalarExprOps.unguarded a.Expr b.Expr) t
        match op with
        | SqlBinaryOp.Add
        | SqlBinaryOp.Subtract
        | SqlBinaryOp.Multiply ->
            let irOp =
                match op with
                | SqlBinaryOp.Add -> BinaryOp.Add
                | SqlBinaryOp.Subtract -> BinaryOp.Subtract
                | _ -> BinaryOp.Multiply
            let t = (numericCommon l r).Value
            match classify t with
            | SqlTypeClass.Integer _ ->
                let a, b = widen l t, widen r t
                value (checkedIntegral t (fun wide -> bin irOp (wide a.Expr) (wide b.Expr))) t
            | SqlTypeClass.Floating _ -> value (bin irOp (widen l t).Expr (widen r t).Expr) t
            | _ ->
                let (p1, s1), (p2, s2), a, b = decimalOperands ()
                let exactPrecision, scale =
                    match op with
                    | SqlBinaryOp.Multiply -> p1 + p2 + 1, s1 + s2
                    | _ -> max (p1 - s1) (p2 - s2) + max s1 s2 + 1, max s1 s2
                if exactPrecision > SqlTypes.maxPrecision then
                    fail cx span $"DECIMAL({p1},{s1}) {text} DECIMAL({p2},{s2}) may need {exactPrecision} digits, more than a decimal holds"
                        (Some "Cast the operands to narrower decimals first.")
                let result = SqlTypes.decimal (exactPrecision, scale)
                // The engine's decimal result has the larger operand scale (rounded) and a bounded precision: both
                // operands are widened to 38 digits at the result's scale. The result type holds every result, so the
                // cast to it is exact.
                let wide (v: SqlValue) = cast v.Expr (SqlTypes.decimal (SqlTypes.maxPrecision, scale))
                value (cast (bin irOp (wide a) (wide b)) result) result
        | SqlBinaryOp.Divide ->
            if floating || integral then
                divide DivisionKind.Divide LogicalTypes.Float64 (widen l LogicalTypes.Float64) (widen r LogicalTypes.Float64)
            else
                let d1, d2, a, b = decimalOperands ()
                divide DivisionKind.Divide (SqlTypes.decimal (SqlTypes.decimalResult SqlBinaryOp.Divide d1 d2)) a b
        | SqlBinaryOp.Modulo ->
            if floating || integral then
                let t = (numericCommon l r).Value
                divide DivisionKind.Remainder t (widen l t) (widen r t)
            else
                let d1, d2, a, b = decimalOperands ()
                divide DivisionKind.Remainder (SqlTypes.decimal (SqlTypes.decimalResult SqlBinaryOp.Modulo d1 d2)) a b
        | _ ->
            // div: the truncated quotient as a BIGINT.
            if floating then fail cx span $"div takes integers or decimals, not {display l.Type} and {display r.Type}" None
            elif integral then divide DivisionKind.IntegerDivide LogicalTypes.Int64 (widen l LogicalTypes.Int64) (widen r LogicalTypes.Int64)
            else
                // Over decimals, the exact quotient of x minus its remainder (x - x % y is a multiple of y), as a BIGINT.
                let (p1, s1), (p2, s2), a, b = decimalOperands ()
                let s = max s1 s2
                let remainderType = SqlTypes.decimal (SqlTypes.decimalResult SqlBinaryOp.Modulo (p1, s1) (p2, s2))
                let remainder = ScalarExprOps.checkedDivide DivisionKind.Remainder remainderType ScalarExprOps.unguarded a.Expr b.Expr
                let multipleType = SqlTypes.decimal (SqlTypes.fitDecimal (p1 - s1 + s) s)
                let multiple = cast (bin BinaryOp.Subtract (cast a.Expr multipleType) (cast remainder multipleType)) multipleType
                let quotient = ScalarExprOps.checkedDivide DivisionKind.Divide (SqlTypes.decimal (SqlTypes.maxPrecision, 0)) ScalarExprOps.unguarded multiple b.Expr
                value (ScalarExprOps.checkedNarrow LogicalTypes.Int64 ScalarExprOps.unguarded quotient) LogicalTypes.Int64

    // ----------------------------------------------------------------------------------------------------------
    // Casts and text
    // ----------------------------------------------------------------------------------------------------------

    /// The type `CAST(x AS name)` names in Spark.
    let private typeOf (cx: SparkContext) (t: SqlTypeName) : LogicalType =
        match t.Name, t.Parameters with
        | "BOOLEAN", [] -> LogicalTypes.Boolean
        | ("TINYINT" | "BYTE"), [] -> LogicalTypes.Int8
        | ("SMALLINT" | "SHORT"), [] -> LogicalTypes.Int16
        | ("INT" | "INTEGER"), [] -> LogicalTypes.Int32
        | ("BIGINT" | "LONG"), [] -> LogicalTypes.Int64
        | ("FLOAT" | "REAL"), [] -> LogicalTypes.Float32
        | "DOUBLE", [] -> LogicalTypes.Float64
        | ("DECIMAL" | "DEC" | "NUMERIC"), [] -> SqlTypes.decimal (10, 0)
        | ("DECIMAL" | "DEC" | "NUMERIC"), [ p ] when p >= 1 && p <= SqlTypes.maxPrecision -> SqlTypes.decimal (p, 0)
        | ("DECIMAL" | "DEC" | "NUMERIC"), [ p; s ] when p >= 1 && p <= SqlTypes.maxPrecision && s >= 0 && s <= p -> SqlTypes.decimal (p, s)
        | "STRING", [] -> LogicalTypes.String
        | "DATE", [] -> LogicalTypes.Date
        | ("TIMESTAMP" | "TIMESTAMP_LTZ"), [] -> LogicalTypes.DatetimeUtc
        | "TIMESTAMP_NTZ", [] -> LogicalTypes.Datetime
        | ("CHAR" | "VARCHAR"), _ -> fail cx t.Span $"CAST to {t.Name} is not supported" (Some "Cast to STRING.")
        | _ -> fail cx t.Span $"Unknown or unsupported type {t.Name}" (Some "Types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL(p,s), STRING, DATE, TIMESTAMP, TIMESTAMP_NTZ.")

    /// An instant's or a TIMESTAMP_NTZ's text as Spark writes it: `yyyy-MM-dd HH:mm:ss`, then the fraction of a second
    /// without its trailing zeros, if any.
    let private timestampText (cx: SparkContext) (v: SqlValue) =
        let wallClock = local cx v
        let whole = fn ScalarFunction.DtToString [ wallClock; str "yyyy-MM-dd HH:mm:ss" ] LogicalTypes.String
        let fraction = fn ScalarFunction.StripEnd [ fn ScalarFunction.DtToString [ wallClock; str "ffffff" ] LogicalTypes.String; str "0" ] LogicalTypes.String
        let text = ScalarExpr.Conditional(bin BinaryOp.Eq fraction (str ""), whole, bin BinaryOp.StringConcat (bin BinaryOp.StringConcat whole (str ".")) fraction)
        nullWhere v.Expr LogicalTypes.String text

    /// A value as text, as Spark's CAST(x AS STRING) writes it.
    let rec private asText (cx: SparkContext) (span: SqlSpan) (v: SqlValue) : SqlValue =
        match classify v.Type with
        | SqlTypeClass.String -> v
        | SqlTypeClass.Null -> nullOf LogicalTypes.String
        | SqlTypeClass.Integer(_, true)
        | SqlTypeClass.Decimal _
        | SqlTypeClass.Boolean
        | SqlTypeClass.Date -> value (cast v.Expr LogicalTypes.String) LogicalTypes.String
        | SqlTypeClass.Timestamp -> value (timestampText cx v) LogicalTypes.String
        | SqlTypeClass.Floating _ ->
            fail cx span $"{display v.Type} as text is not supported: Spark writes floating values as Java does (1.0E10)" None
        | _ -> fail cx span $"{display v.Type} as text is not supported" None

    and private castTo (cx: SparkContext) (span: SqlSpan) (v: SqlValue) (target: LogicalType) : SqlValue =
        let refuse (why: string) = fail cx span $"CAST of {display v.Type} to {display target} is not supported{why}" None
        if v.Type.Display = target.Display then v
        else
            match classify v.Type, classify target with
            | SqlTypeClass.Null, _ -> nullOf target
            | SqlTypeClass.String, SqlTypeClass.String -> v
            | SqlTypeClass.String, _ ->
                match stringLiteral v with
                | Some text -> castTo cx span (literalAs cx span text target) target
                | None -> refuse ": Spark fails the write where the text does not convert, which is not supported"
            | _, SqlTypeClass.String -> asText cx span v
            | SqlTypeClass.Integer(from, true), SqlTypeClass.Integer(bits, true) ->
                if from <= bits then widen v target else value (ScalarExprOps.checkedNarrow target ScalarExprOps.unguarded v.Expr) target
            | (SqlTypeClass.Decimal _ | SqlTypeClass.Floating _), SqlTypeClass.Integer(_, true) ->
                // Truncated toward zero; NaN, infinities and values beyond the type fail (CAST_OVERFLOW).
                value (ScalarExprOps.checkedNarrow target ScalarExprOps.unguarded v.Expr) target
            | (SqlTypeClass.Integer(_, true) | SqlTypeClass.Decimal _), SqlTypeClass.Decimal(p, s) ->
                let p1, s1 = (decimalOfType v.Type).Value
                if p1 - s1 <= p - s && s1 <= s then widen v target
                else
                    let source = widen v (SqlTypes.decimal (p1, s1))
                    value (ScalarExprOps.checkedRescale target ScalarExprOps.unguarded source.Expr) target
            | SqlTypeClass.Floating _, SqlTypeClass.Decimal _ ->
                refuse ": Spark rounds the value's shortest decimal text (2.675D is 2.68), the engine its binary value"
            | (SqlTypeClass.Integer(_, true) | SqlTypeClass.Decimal _ | SqlTypeClass.Floating _ | SqlTypeClass.Boolean), SqlTypeClass.Floating _
            | SqlTypeClass.Boolean, (SqlTypeClass.Integer(_, true) | SqlTypeClass.Decimal _) -> value (cast v.Expr target) target
            | (SqlTypeClass.Integer(_, true) | SqlTypeClass.Floating _), SqlTypeClass.Boolean -> value (cast v.Expr target) target
            | SqlTypeClass.Decimal _, SqlTypeClass.Boolean ->
                value (bin BinaryOp.NotEq v.Expr (ScalarExpr.Cast(lit (LiteralValue.Decimal 0m), v.Type, false))) target
            | SqlTypeClass.Timestamp, SqlTypeClass.Date -> value (fn ScalarFunction.DtDate [ local cx v ] LogicalTypes.Date) target
            | SqlTypeClass.Date, SqlTypeClass.Timestamp when not (isInstant target) -> value (cast v.Expr target) target
            | SqlTypeClass.Timestamp, SqlTypeClass.Timestamp when isInstant v.Type && not (isInstant target) -> value (local cx v) target
            | SqlTypeClass.Timestamp, SqlTypeClass.Timestamp when isInstant v.Type -> value (cast v.Expr target) target
            | (SqlTypeClass.Date | SqlTypeClass.Timestamp), SqlTypeClass.Timestamp ->
                // A wall clock as an instant of the session zone: only UTC is supported.
                if isUtc cx.Zone then value (cast v.Expr target) target
                else refuse $" in the session time zone {cx.Zone.Id} (only in UTC)"
            | _ -> refuse ""

    // ----------------------------------------------------------------------------------------------------------
    // Patterns
    // ----------------------------------------------------------------------------------------------------------

    /// A LIKE pattern as Spark reads it (`\` escapes `_`, `%` and itself; any other escaped character is an error).
    let private likeTest (cx: SparkContext) (span: SqlSpan) (subject: ScalarExpr) (patternText: string) (escape: char) =
        let pieces = ResizeArray<Choice<string, unit, unit>>()
        let text = StringBuilder()
        let flush () =
            if text.Length > 0 then
                pieces.Add(Choice1Of3(text.ToString()))
                text.Clear() |> ignore
        let mutable i = 0
        while i < patternText.Length do
            let c = patternText.[i]
            if c = escape then
                if i + 1 >= patternText.Length then fail cx span "A LIKE pattern cannot end with its escape character" None
                let next = patternText.[i + 1]
                if next <> '_' && next <> '%' && next <> escape then
                    fail cx span $"The escape character is not allowed to precede '{next}' in a LIKE pattern" None
                text.Append next |> ignore
                i <- i + 2
            elif c = '%' then
                flush ()
                if pieces.Count = 0 || pieces.[pieces.Count - 1] <> Choice2Of3() then pieces.Add(Choice2Of3())
                i <- i + 1
            elif c = '_' then
                flush ()
                pieces.Add(Choice3Of3())
                i <- i + 1
            else
                text.Append c |> ignore
                i <- i + 1
        flush ()
        match List.ofSeq pieces with
        | [] -> bin BinaryOp.Eq subject (str "")
        | [ Choice1Of3 t ] -> bin BinaryOp.Eq subject (str t)
        | [ Choice1Of3 t; Choice2Of3() ] -> bin BinaryOp.StartsWith subject (str t)
        | [ Choice2Of3(); Choice1Of3 t ] -> bin BinaryOp.EndsWith subject (str t)
        | [ Choice2Of3(); Choice1Of3 t; Choice2Of3() ] -> bin BinaryOp.Like subject (str t)
        | [ Choice2Of3() ] -> ScalarExpr.Unary(UnaryOp.IsNotNull, subject)
        | pieces ->
            let regex = StringBuilder("^(?s:")
            for piece in pieces do
                match piece with
                | Choice1Of3 t -> regex.Append(Text.RegularExpressions.Regex.Escape t) |> ignore
                | Choice2Of3() -> regex.Append(".*") |> ignore
                | Choice3Of3() -> regex.Append('.') |> ignore
            bin BinaryOp.RegexMatch subject (str (regex.Append(")$").ToString()))

    /// Java's line terminators, which `.` does not match and `$` may stand before.
    let private lineEnds = @"\n\r\x{85}\x{2028}\x{2029}"

    /// A Java regular expression (Spark's RLIKE: found anywhere in the text) as one the engine's regex engine reads
    /// alike, or the construct that has no equivalent there. Java's `\d`, `\w`, `\s` are ASCII and its `.` stops at
    /// every line terminator, so they are spelt out; lookaround, back-references, possessive quantifiers, nested
    /// classes and the other Java-only forms are refused. Not covered: `(?i)` folds case for ASCII letters in Java,
    /// for every letter in the engine.
    let javaRegex (pattern: string) : Result<string, string> =
        let out = StringBuilder()
        let mutable error = None
        let refuse (what: string) = if error.IsNone then error <- Some what
        let classes =
            dict [ 'd', ("0-9", false); 'D', ("0-9", true); 'w', ("a-zA-Z0-9_", false); 'W', ("a-zA-Z0-9_", true)
                   's', (@" \t\n\x0B\f\r", false); 'S', (@" \t\n\x0B\f\r", true) ]
        let hexAt (from: int) (count: int) =
            from + count <= pattern.Length
            && pattern.AsSpan(from, count).IndexOfAnyExcept("0123456789abcdefABCDEF".AsSpan()) < 0
        let mutable i = 0
        let mutable inClass = false
        let mutable classStart = false
        /// After a quantifier: a lazy `?` is kept, a possessive `+` refused.
        let afterQuantifier () =
            if i < pattern.Length && pattern.[i] = '?' then
                out.Append '?' |> ignore
                i <- i + 1
            elif i < pattern.Length && pattern.[i] = '+' then refuse "a possessive quantifier"
        while i < pattern.Length && error.IsNone do
            let c = pattern.[i]
            if c = '\\' then
                if i + 1 >= pattern.Length then
                    refuse "a pattern ending in a backslash"
                    i <- i + 1
                else
                    let n = pattern.[i + 1]
                    i <- i + 2
                    match classes.TryGetValue n with
                    | true, (set, negated) ->
                        if not inClass then out.Append(if negated then "[^" else "[").Append(set).Append(']') |> ignore
                        elif negated then refuse $"\\{n} inside a class"
                        else out.Append set |> ignore
                    | _ ->
                        match n with
                        | 't' | 'n' | 'r' | 'f' -> out.Append('\\').Append(n) |> ignore
                        | 'x' when hexAt i 2 ->
                            out.Append(@"\x{").Append(pattern, i, 2).Append('}') |> ignore
                            i <- i + 2
                        | 'u' when hexAt i 4 ->
                            out.Append(@"\x{").Append(pattern, i, 4).Append('}') |> ignore
                            i <- i + 4
                        // An escaped symbol stands for itself.
                        | _ when n < '\u0080' && not (Char.IsLetterOrDigit n) -> out.Append(@"\x{").Append((int n).ToString("x2", CultureInfo.InvariantCulture)).Append('}') |> ignore
                        | _ -> refuse $"\\{n}"
                    classStart <- false
            elif inClass then
                match c with
                | ']' when not classStart ->
                    out.Append ']' |> ignore
                    inClass <- false
                    i <- i + 1
                | '[' -> refuse "a class inside a class"
                | ']' -> refuse "a class starting with ]"
                | '&' when i + 1 < pattern.Length && pattern.[i + 1] = '&' -> refuse "a class intersection (&&)"
                | ('-' | '~') when i + 1 < pattern.Length && pattern.[i + 1] = c -> refuse $"{c}{c} inside a class"
                | _ ->
                    out.Append c |> ignore
                    i <- i + 1
                classStart <- false
            else
                match c with
                | '[' ->
                    out.Append '[' |> ignore
                    i <- i + 1
                    if i < pattern.Length && pattern.[i] = '^' then
                        out.Append '^' |> ignore
                        i <- i + 1
                    inClass <- true
                    classStart <- true
                | '.' ->
                    out.Append("[^").Append(lineEnds).Append(']') |> ignore
                    i <- i + 1
                | '$' ->
                    // At the end, or before a final line terminator.
                    out.Append(@"(?:\r\n|[").Append(lineEnds).Append("])?$") |> ignore
                    i <- i + 1
                | '(' when i + 1 < pattern.Length && pattern.[i + 1] = '?' ->
                    if String.CompareOrdinal(pattern, i, "(?:", 0, 3) = 0 then
                        out.Append "(?:" |> ignore
                        i <- i + 3
                    elif String.CompareOrdinal(pattern, i, "(?i)", 0, 4) = 0 || String.CompareOrdinal(pattern, i, "(?i:", 0, 4) = 0 then
                        out.Append(pattern, i, 4) |> ignore
                        i <- i + 4
                    else refuse "a group construct other than (?:...) and (?i)"
                | '*' | '+' | '?' ->
                    out.Append c |> ignore
                    i <- i + 1
                    afterQuantifier ()
                | '{' ->
                    let close = pattern.IndexOf('}', i)
                    let body = if close < 0 then "" else pattern.Substring(i + 1, close - i - 1)
                    let bounds = body.Split(',')
                    let isCount (s: string) = s.Length > 0 && s |> Seq.forall Char.IsAsciiDigit
                    if close >= 0 && bounds.Length <= 2 && isCount bounds.[0] && (bounds.Length = 1 || bounds.[1] = "" || isCount bounds.[1]) then
                        out.Append(pattern, i, close - i + 1) |> ignore
                        i <- close + 1
                        afterQuantifier ()
                    else refuse "a { that does not start a repetition count"
                | ']' | '}' ->
                    out.Append('\\').Append(c) |> ignore
                    i <- i + 1
                | _ ->
                    out.Append c |> ignore
                    i <- i + 1
        if error.IsNone && inClass then refuse "an unclosed class"
        match error with
        | Some what -> Error what
        | None -> Ok(out.ToString())

    // ----------------------------------------------------------------------------------------------------------
    // Date and time
    // ----------------------------------------------------------------------------------------------------------

    /// A Java date pattern (date_format) as a .NET one (DtToString), for the letters both write alike.
    let private datePattern (pattern: string) : string option =
        let out = StringBuilder()
        let mutable ok = true
        let mutable i = 0
        while ok && i < pattern.Length do
            let c = pattern.[i]
            let mutable run = 1
            while i + run < pattern.Length && pattern.[i + run] = c do
                run <- run + 1
            match c, run with
            | 'y', 4 -> out.Append "yyyy" |> ignore
            | 'y', 2 -> out.Append "yy" |> ignore
            | ('M' | 'd' | 'H' | 'm' | 's'), 2 -> out.Append(c, 2) |> ignore
            | 'S', 3 -> out.Append "fff" |> ignore
            | 'S', 6 -> out.Append "ffffff" |> ignore
            | ('-' | '/' | ':' | ' ' | '.' | '_' | ','), _ -> out.Append(c, run) |> ignore
            | '\'', 1 when i + 2 < pattern.Length && pattern.[i + 1] = 'T' && pattern.[i + 2] = '\'' ->
                out.Append 'T' |> ignore
                run <- 3
            | _ -> ok <- false
            i <- i + run
        if ok && out.Length > 0 then Some(out.ToString()) else None

    let private daysOf (date: ScalarExpr) = cast date LogicalTypes.Int32
    let private dateOfDays (days: ScalarExpr) = cast days LogicalTypes.Date
    let private part (f: ScalarFunction) (e: ScalarExpr) = cast (fn f [ e ] LogicalTypes.Int32) LogicalTypes.Int32

    /// The first day of a date's year, quarter, month or week (Monday).
    let private truncateDate (cx: SparkContext) (span: SqlSpan) (unit: string) (date: ScalarExpr) =
        let back (f: ScalarFunction) = dateOfDays (bin BinaryOp.Subtract (daysOf date) (bin BinaryOp.Subtract (part f date) (i32 1)))
        match unit.ToUpperInvariant() with
        | "YEAR" | "YYYY" | "YY" -> back ScalarFunction.DtOrdinalDay
        | "MONTH" | "MM" | "MON" -> back ScalarFunction.DtDay
        | "WEEK" -> back ScalarFunction.DtWeekday
        | "QUARTER" ->
            let monthStart = back ScalarFunction.DtDay
            let intoQuarter = bin BinaryOp.Modulo (bin BinaryOp.Subtract (part ScalarFunction.DtMonth date) (i32 1)) (i32 3)
            let back' (months: int) = fn ScalarFunction.DtOffsetMonths [ monthStart; i32 -months ] LogicalTypes.Date
            ScalarExpr.Conditional(bin BinaryOp.Eq intoQuarter (i32 0), monthStart,
                ScalarExpr.Conditional(bin BinaryOp.Eq intoQuarter (i32 1), back' 1, back' 2))
        | _ -> fail cx span $"'{unit}' is not a unit of trunc()" (Some "Units: YEAR, QUARTER, MONTH, WEEK.")

    // ----------------------------------------------------------------------------------------------------------
    // Expressions
    // ----------------------------------------------------------------------------------------------------------

    let private aggregateNames =
        set [ "count"; "sum"; "avg"; "mean"; "min"; "max"; "first"; "last"; "any_value"; "collect_list"; "collect_set"; "stddev"
              "variance"; "count_if"; "bool_and"; "bool_or"; "every"; "some"; "any"; "percentile"; "median" ]
    let private nondeterministic =
        set [ "rand"; "randn"; "random"; "uuid"; "shuffle"; "monotonically_increasing_id"; "spark_partition_id"; "input_file_name"
              "current_user"; "user"; "session_user" ]
    let private currentNames = set [ "current_date"; "current_timestamp"; "now"; "localtimestamp" ]
    let private functionNames =
        set [ "isnull"; "isnotnull"; "coalesce"; "nvl"; "ifnull"; "nvl2"; "nullif"; "if"; "greatest"; "least"; "length"; "char_length"
              "character_length"; "upper"; "ucase"; "lower"; "lcase"; "substring"; "substr"; "left"; "right"; "concat"; "concat_ws"
              "trim"; "ltrim"; "rtrim"; "replace"; "lpad"; "rpad"; "startswith"; "endswith"; "contains"; "abs"; "round"; "bround"
              "floor"; "ceil"; "ceiling"; "pmod"; "year"; "month"; "day"; "dayofmonth"; "hour"; "minute"; "second"; "quarter"
              "dayofweek"; "weekday"; "date_format"; "trunc"; "date_trunc"; "current_date"; "current_timestamp" ]

    let private expectBoolean (cx: SparkContext) (span: SqlSpan) (what: string) (v: SqlValue) =
        match classify v.Type with
        | SqlTypeClass.Boolean -> v
        | SqlTypeClass.Null -> nullOf LogicalTypes.Boolean
        | _ -> fail cx span $"{what} must be a BOOLEAN, not {display v.Type}" None

    let private current (cx: SparkContext) (span: SqlSpan) (name: string) =
        if cx.Use <> SparkExpressionUse.Default then
            fail cx span $"{name}() is not allowed {where cx}: its value changes from one write to the next" (Some "Only a column default may read the current time.")
        let micros = DateTimeOffset(cx.Now.UtcTicks - cx.Now.UtcTicks % 10L, TimeSpan.Zero)
        match name with
        | "current_date" ->
            let wallClock = TimeZoneInfo.ConvertTime(micros, cx.Zone)
            value (lit (LiteralValue.Date(DateOnly.FromDateTime wallClock.DateTime))) LogicalTypes.Date
        | "localtimestamp" -> value (lit (LiteralValue.DateTime (TimeZoneInfo.ConvertTime(micros, cx.Zone)).DateTime)) LogicalTypes.Datetime
        | _ -> instant micros

    let rec private bind (cx: SparkContext) (e: SqlExpr) : SqlValue =
        let span = e.Span
        match e.Kind with
        | SqlExprKind.Literal literal ->
            match literal with
            | SqlLiteral.Integer text -> integerLiteral cx span text
            | SqlLiteral.Decimal text -> parseDecimal cx span text
            | SqlLiteral.Float text -> value (lit (LiteralValue.Float64(Double.Parse(text, CultureInfo.InvariantCulture)))) LogicalTypes.Float64
            | SqlLiteral.Suffixed(number, suffix) -> suffixed cx span number suffix
            | SqlLiteral.String s -> value (str s) LogicalTypes.String
            | SqlLiteral.Boolean b -> value (lit (LiteralValue.Bool b)) LogicalTypes.Boolean
            | SqlLiteral.Null -> nullOf LogicalTypes.Null
            | SqlLiteral.Typed(typeName, text) -> typedLiteral cx span typeName text
            | SqlLiteral.Interval _ -> fail cx span "Intervals are not supported" None

        | SqlExprKind.Column name -> column cx span name

        | SqlExprKind.Unary(SqlUnaryOp.Not, operand) ->
            let v = expectBoolean cx operand.Span "The operand of NOT" (bind cx operand)
            value (ScalarExpr.Unary(UnaryOp.Not, v.Expr)) LogicalTypes.Boolean
        | SqlExprKind.Unary(SqlUnaryOp.Negate, operand) -> negate cx span (bind cx operand)
        | SqlExprKind.Unary(SqlUnaryOp.Plus, operand) ->
            let v = bind cx operand
            if not (isNumeric v.Type || SqlTypes.isNull v.Type) then fail cx span $"Unary + takes a number, not {display v.Type}" None
            v

        | SqlExprKind.Binary((SqlBinaryOp.And | SqlBinaryOp.Or) as op, left, right) ->
            let what = if op = SqlBinaryOp.And then "An operand of AND" else "An operand of OR"
            let l = expectBoolean cx left.Span what (bind cx left)
            let r = expectBoolean cx right.Span what (bind cx right)
            value (bin (if op = SqlBinaryOp.And then BinaryOp.And else BinaryOp.Or) l.Expr r.Expr) LogicalTypes.Boolean

        | SqlExprKind.Binary((SqlBinaryOp.Eq | SqlBinaryOp.NotEq | SqlBinaryOp.Lt | SqlBinaryOp.LtEq | SqlBinaryOp.Gt | SqlBinaryOp.GtEq) as op, left, right) ->
            let l, r, _ = meet cx span "compare" true (bind cx left) (bind cx right)
            let irOp =
                match op with
                | SqlBinaryOp.Eq -> BinaryOp.Eq
                | SqlBinaryOp.NotEq -> BinaryOp.NotEq
                | SqlBinaryOp.Lt -> BinaryOp.Lt
                | SqlBinaryOp.LtEq -> BinaryOp.LtEq
                | SqlBinaryOp.Gt -> BinaryOp.Gt
                | _ -> BinaryOp.GtEq
            value (bin irOp l.Expr r.Expr) LogicalTypes.Boolean

        | SqlExprKind.Binary(SqlBinaryOp.Concat, left, right) -> concat cx span [ bind cx left, left.Span; bind cx right, right.Span ]

        | SqlExprKind.Binary(op, left, right) -> arithmetic cx span op (bind cx left) (bind cx right)

        | SqlExprKind.IsNull(operand, negated) ->
            let v = bind cx operand
            value (ScalarExpr.Unary((if negated then UnaryOp.IsNotNull else UnaryOp.IsNull), v.Expr)) LogicalTypes.Boolean

        | SqlExprKind.IsDistinctFrom(left, right, negated) ->
            let l, r, _ = meet cx span "compare" true (bind cx left) (bind cx right)
            value (bin (if negated then BinaryOp.EqMissing else BinaryOp.NotEqMissing) l.Expr r.Expr) LogicalTypes.Boolean

        | SqlExprKind.Between(operand, low, high, negated) ->
            let values, _ = meetAll cx span "compare" true [ bind cx operand; bind cx low; bind cx high ]
            let x, lo, hi = values.[0], values.[1], values.[2]
            let between = bin BinaryOp.And (bin BinaryOp.GtEq x.Expr lo.Expr) (bin BinaryOp.LtEq x.Expr hi.Expr)
            value (if negated then ScalarExpr.Unary(UnaryOp.Not, between) else between) LogicalTypes.Boolean

        | SqlExprKind.InList(operand, items, negated) ->
            let values, _ = meetAll cx span "compare" true (bind cx operand :: (items |> List.map (bind cx)))
            let x, items = values.Head, values.Tail
            let isNullLiteral (v: SqlValue) = match v.Expr with ScalarExpr.Literal(LiteralValue.Null _) -> true | _ -> false
            let present = items |> List.filter (isNullLiteral >> not)
            let literals = present |> List.choose (fun v -> match v.Expr with ScalarExpr.Literal l -> Some l | _ -> None)
            let found =
                if present.IsEmpty then lit (LiteralValue.Bool false)
                elif literals.Length = present.Length then ScalarExpr.InList(x.Expr, InListSource.Literals literals, false, false)
                else present |> List.map (fun v -> bin BinaryOp.Eq x.Expr v.Expr) |> List.reduce (bin BinaryOp.Or)
            // A null item makes a value that matches no item unknown; a null value is unknown.
            let unknown = lit (LiteralValue.Null LogicalTypes.Boolean)
            let test =
                if present.Length < items.Length then ScalarExpr.Conditional(found, lit (LiteralValue.Bool true), unknown)
                else ScalarExpr.Conditional(isNull x.Expr, unknown, found)
            value (if negated then ScalarExpr.Unary(UnaryOp.Not, test) else test) LogicalTypes.Boolean

        | SqlExprKind.InSubquery _
        | SqlExprKind.Quantified _
        | SqlExprKind.Exists _
        | SqlExprKind.ScalarSubquery _ -> fail cx span $"Subqueries are not allowed {where cx}" None

        | SqlExprKind.Like(operand, pattern, escape, negated) -> like cx span operand pattern escape negated false
        | SqlExprKind.ILike(operand, pattern, escape, negated) -> like cx span operand pattern escape negated true
        | SqlExprKind.RLike(operand, pattern, negated) ->
            let subject = text cx operand.Span (bind cx operand)
            let patternText =
                match (bind cx pattern).Expr with
                | ScalarExpr.Literal(LiteralValue.String p) -> p
                | _ -> fail cx pattern.Span "An RLIKE pattern must be a string literal" None
            match javaRegex patternText with
            | Ok regex ->
                let test = bin BinaryOp.RegexMatch subject.Expr (str regex)
                value (if negated then ScalarExpr.Unary(UnaryOp.Not, test) else test) LogicalTypes.Boolean
            | Error what ->
                fail cx pattern.Span $"The RLIKE pattern uses {what}, which the engine's regular expressions do not read as Java does" None

        | SqlExprKind.Case(operand, whens, otherwise) ->
            let tests =
                match operand with
                | Some o ->
                    let subject = bind cx o
                    whens
                    |> List.map (fun (w, _) ->
                        let l, r, _ = meet cx w.Span "compare" true subject (bind cx w)
                        value (bin BinaryOp.Eq l.Expr r.Expr) LogicalTypes.Boolean)
                | None -> whens |> List.map (fun (w, _) -> expectBoolean cx w.Span "A WHEN condition" (bind cx w))
            let results = (whens |> List.map (fun (_, v) -> bind cx v)) @ [ otherwise |> Option.map (bind cx) |> Option.defaultValue (nullOf LogicalTypes.Null) ]
            let results, t = meetAll cx span "combine CASE results of" false results
            let rec chain (tests: SqlValue list) (results: SqlValue list) =
                match tests, results with
                | [], [ last ] -> last.Expr
                | test :: tests, result :: results -> ScalarExpr.Conditional(test.Expr, result.Expr, chain tests results)
                | _ -> invalidOp "CASE arms"
            value (chain tests results) t

        | SqlExprKind.Cast(operand, typeName, isTry) ->
            if isTry then fail cx span "TRY_CAST is not supported" None
            castTo cx span (bind cx operand) (typeOf cx typeName)

        | SqlExprKind.Extract(field, operand) ->
            let v = bind cx operand
            match field with
            | "YEAR" -> datePart cx span "year" ScalarFunction.DtYear v
            | "MONTH" -> datePart cx span "month" ScalarFunction.DtMonth v
            | "DAY" -> datePart cx span "day" ScalarFunction.DtDay v
            | "HOUR" -> timePart cx span "hour" ScalarFunction.DtHour v
            | "MINUTE" -> timePart cx span "minute" ScalarFunction.DtMinute v
            | "SECOND" -> timePart cx span "second" ScalarFunction.DtSecond v
            | _ -> fail cx span $"EXTRACT({field} ...) is not supported" (Some "Fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.")

        | SqlExprKind.Function call -> callFunction cx span call

    and private column (cx: SparkContext) (span: SqlSpan) (name: SqlName) : SqlValue =
        let first = name.Head
        match cx.Columns |> List.filter (fun (n, _) -> String.Equals(n, first.Text, StringComparison.OrdinalIgnoreCase)) with
        | [ (defined, t) ] ->
            if cx.Use = SparkExpressionUse.Default then
                fail cx span $"A column default cannot read a column ({defined})" (Some "A default is a constant: a literal, or current_date() / current_timestamp().")
            if not (cx.Read.Contains defined) then cx.Read.Add defined
            name.Tail
            |> List.fold
                (fun (v: SqlValue) (field: SqlIdentifier) ->
                    let fields =
                        match v.Type with
                        | LogicalType.Arrow(:? StructType as st) -> st.Fields |> Seq.map (fun f -> f.Name, LogicalType.Arrow f.DataType) |> List.ofSeq
                        | LogicalType.StructOf fields -> fields
                        | _ -> fail cx field.Span $"{display v.Type} has no fields: '{field.Text}' cannot be read from it" None
                    match fields |> List.filter (fun (n, _) -> String.Equals(n, field.Text, StringComparison.OrdinalIgnoreCase)) with
                    | [ (n, ft) ] -> value (fn ScalarFunction.StructFieldByName [ v.Expr; str n ] ft) ft
                    | [] ->
                        let hint = SqlNames.suggest field.Text (fields |> List.map fst) |> Option.map (fun n -> $"Did you mean {n}?")
                        fail cx field.Span $"No field '{field.Text}' in the struct" hint
                    | many ->
                        let names = String.Join(", ", many |> List.map fst)
                        fail cx field.Span $"'{field.Text}' names several fields: {names}" None)
                (value (ScalarExpr.Column defined) t)
        | [] when name.Tail.IsEmpty && currentNames.Contains(first.Text.ToLowerInvariant()) -> current cx span (first.Text.ToLowerInvariant())
        | [] ->
            let hint = SqlNames.suggest first.Text (cx.Columns |> List.map fst) |> Option.map (fun n -> $"Did you mean {n}?")
            fail cx span $"Unknown column {first.Text}" hint
        | many ->
            let names = String.Join(", ", many |> List.map fst)
            fail cx span $"'{first.Text}' names several columns: {names}" None

    /// A string operand (NULL as a null string).
    and private text (cx: SparkContext) (span: SqlSpan) (v: SqlValue) : SqlValue =
        match classify v.Type with
        | SqlTypeClass.String -> v
        | SqlTypeClass.Null -> nullOf LogicalTypes.String
        | _ -> fail cx span $"Expected a STRING, not {display v.Type}" None

    and private concat (cx: SparkContext) (span: SqlSpan) (items: (SqlValue * SqlSpan) list) : SqlValue =
        // Spark's concat and || are null when an item is (the IR's StringConcat reads null as the empty string).
        let items = items |> List.map (fun (v, s) -> asText cx s v)
        let anyNull = items |> List.map (fun v -> isNull v.Expr) |> List.reduce (bin BinaryOp.Or)
        let joined = items |> List.map (fun v -> v.Expr) |> List.reduce (bin BinaryOp.StringConcat)
        value (ScalarExpr.Conditional(anyNull, lit (LiteralValue.Null LogicalTypes.String), joined)) LogicalTypes.String

    and private like (cx: SparkContext) (span: SqlSpan) (operand: SqlExpr) (pattern: SqlExpr) (escape: SqlExpr option) (negated: bool) (insensitive: bool) =
        let subject = text cx operand.Span (bind cx operand)
        let patternText =
            match (bind cx pattern).Expr with
            | ScalarExpr.Literal(LiteralValue.String p) -> p
            | _ -> fail cx pattern.Span "A LIKE pattern must be a string literal" None
        let escapeChar =
            match escape with
            | None -> '\\'
            | Some e ->
                match (bind cx e).Expr with
                | ScalarExpr.Literal(LiteralValue.String s) when s.Length = 1 -> s.[0]
                | _ -> fail cx e.Span "ESCAPE takes one character, written as a string literal" None
        // ILIKE is LIKE over both sides in lower case.
        let subject, patternText =
            if insensitive then fn ScalarFunction.ToLower [ subject.Expr ] LogicalTypes.String, patternText.ToLowerInvariant()
            else subject.Expr, patternText
        let test = likeTest cx pattern.Span subject patternText escapeChar
        value (if negated then ScalarExpr.Unary(UnaryOp.Not, test) else test) LogicalTypes.Boolean

    /// A date part of a date or a timestamp (an instant's in the session zone), as an INT.
    and private datePart (cx: SparkContext) (span: SqlSpan) (name: string) (f: ScalarFunction) (v: SqlValue) : SqlValue =
        match classify v.Type, stringLiteral v with
        | SqlTypeClass.Null, _ -> nullOf LogicalTypes.Int32
        | (SqlTypeClass.Date | SqlTypeClass.Timestamp), _ -> value (part f (local cx v)) LogicalTypes.Int32
        | SqlTypeClass.String, Some s -> datePart cx span name f (literalAs cx span s LogicalTypes.Date)
        | _ -> fail cx span $"{name}() takes a DATE or a TIMESTAMP, not {display v.Type}" None

    /// A time part of a timestamp (an instant's in the session zone; a date's is 0), as an INT.
    and private timePart (cx: SparkContext) (span: SqlSpan) (name: string) (f: ScalarFunction) (v: SqlValue) : SqlValue =
        match classify v.Type, stringLiteral v with
        | SqlTypeClass.Null, _ -> nullOf LogicalTypes.Int32
        | SqlTypeClass.Timestamp, _ -> value (part f (local cx v)) LogicalTypes.Int32
        | SqlTypeClass.Date, _ -> value (nullWhere v.Expr LogicalTypes.Int32 (i32 0)) LogicalTypes.Int32
        | SqlTypeClass.String, Some s -> timePart cx span name f (literalAs cx span s LogicalTypes.Datetime)
        | _ -> fail cx span $"{name}() takes a TIMESTAMP, not {display v.Type}" None

    and private callFunction (cx: SparkContext) (span: SqlSpan) (call: SqlFunctionCall) : SqlValue =
        let name =
            match call.Name with
            | [ n ] -> n.Text.ToLowerInvariant()
            | _ -> fail cx span "Qualified function names are not supported" None
        if call.Over.IsSome then fail cx span $"Window functions are not allowed {where cx}" None
        if aggregateNames.Contains name || call.Star || call.Distinct then fail cx span $"Aggregate functions are not allowed {where cx}" None
        if nondeterministic.Contains name then
            fail cx span $"{name}() is not allowed {where cx}: it is not deterministic" None
        let args = call.Arguments |> List.map (bind cx)
        let argSpan (i: int) = call.Arguments.[i].Span
        let arity (counts: int list) =
            if not (counts |> List.contains args.Length) then
                let expected = String.Join(" or ", counts)
                fail cx span $"{name}() takes {expected} argument(s), not {args.Length}" None
        /// An argument that must be a whole number written as a literal.
        let wholeLiteral (i: int) =
            match args.[i].Expr with
            | ScalarExpr.Literal(LiteralValue.Int32 n) -> n
            | ScalarExpr.Literal(LiteralValue.Int64 n) when n >= int64 Int32.MinValue && n <= int64 Int32.MaxValue -> int n
            | _ -> fail cx (argSpan i) $"Argument {i + 1} of {name}() must be a whole number written as a literal" None
        let stringArg (i: int) =
            match stringLiteral args.[i] with
            | Some s -> s
            | None -> fail cx (argSpan i) $"Argument {i + 1} of {name}() must be a string literal" None
        let textArg (i: int) = text cx (argSpan i) args.[i]
        let numberArg (i: int) =
            let v = args.[i]
            if not (isNumeric v.Type || SqlTypes.isNull v.Type) then fail cx (argSpan i) $"{name}() takes a number, not {display v.Type}" None
            v
        let combine (what: string) (values: SqlValue list) = meetAll cx span what false values
        let str' (e: ScalarExpr) = value e LogicalTypes.String
        match name with
        | "current_date"
        | "current_timestamp"
        | "now"
        | "localtimestamp" ->
            arity [ 0 ]
            current cx span name
        | "isnull"
        | "isnotnull" ->
            arity [ 1 ]
            value (ScalarExpr.Unary((if name = "isnull" then UnaryOp.IsNull else UnaryOp.IsNotNull), args.[0].Expr)) LogicalTypes.Boolean
        | "coalesce"
        | "nvl"
        | "ifnull" ->
            if name = "coalesce" then (if args.IsEmpty then fail cx span "coalesce() needs arguments" None) else arity [ 2 ]
            let values, t = combine $"combine {name}() arguments of" args
            value (ScalarExpr.Coalesce(values |> List.map (fun v -> v.Expr))) t
        | "nvl2" ->
            arity [ 3 ]
            let values, t = combine "combine nvl2() results of" [ args.[1]; args.[2] ]
            value (ScalarExpr.Conditional(ScalarExpr.Unary(UnaryOp.IsNotNull, args.[0].Expr), values.[0].Expr, values.[1].Expr)) t
        | "nullif" ->
            arity [ 2 ]
            let a, b, t = meet cx span "compare" true args.[0] args.[1]
            // The first argument's type and value (compared in the common type).
            ignore t
            value (ScalarExpr.Conditional(bin BinaryOp.Eq a.Expr b.Expr, lit (LiteralValue.Null args.[0].Type), args.[0].Expr)) args.[0].Type
        | "if" ->
            arity [ 3 ]
            let test = expectBoolean cx (argSpan 0) "The condition of if()" args.[0]
            let values, t = combine "combine if() results of" [ args.[1]; args.[2] ]
            value (ScalarExpr.Conditional(test.Expr, values.[0].Expr, values.[1].Expr)) t
        | "greatest"
        | "least" ->
            if args.Length < 2 then fail cx span $"{name}() takes at least 2 arguments" None
            let values, t = combine $"compare {name}() arguments of" args
            // Nulls are skipped; NaN is above every number, as the engine's comparisons order it.
            let pick (a: ScalarExpr) (b: ScalarExpr) =
                let ordered = ScalarExpr.Conditional(bin (if name = "greatest" then BinaryOp.GtEq else BinaryOp.LtEq) a b, a, b)
                ScalarExpr.Conditional(isNull a, b, ScalarExpr.Conditional(isNull b, a, ordered))
            value (values |> List.map (fun v -> v.Expr) |> List.reduce pick) t
        | "length"
        | "char_length"
        | "character_length" ->
            arity [ 1 ]
            let s = textArg 0
            value (cast (fn ScalarFunction.LenChars [ s.Expr ] LogicalTypes.UInt32) LogicalTypes.Int32) LogicalTypes.Int32
        | "upper"
        | "ucase"
        | "lower"
        | "lcase" ->
            arity [ 1 ]
            let f = if name = "upper" || name = "ucase" then ScalarFunction.ToUpper else ScalarFunction.ToLower
            str' (fn f [ (textArg 0).Expr ] LogicalTypes.String)
        | "substring"
        | "substr" ->
            arity [ 2; 3 ]
            let s = (textArg 0).Expr
            let pos = wholeLiteral 1
            let len = if args.Length = 3 then Some(max 0 (wholeLiteral 2)) else None
            let slice (offset: ScalarExpr) (length: ScalarExpr) = fn ScalarFunction.Slice [ s; offset; length ] LogicalTypes.String
            let lengthOf (n: int option) = match n with Some n -> i64 (int64 n) | None -> lit (LiteralValue.Null LogicalTypes.Int64)
            if pos >= 0 then str' (slice (i64 (int64 (max 0 (pos - 1)))) (lengthOf len))
            else
                // From the end: a start before the text's first character shortens what is taken.
                let start = bin BinaryOp.Add (cast (fn ScalarFunction.LenChars [ s ] LogicalTypes.UInt32) LogicalTypes.Int64) (i64 (int64 pos))
                let before = bin BinaryOp.Lt start (i64 0L)
                let offset = ScalarExpr.Conditional(before, i64 0L, start)
                let length =
                    match len with
                    | None -> lengthOf None
                    | Some n ->
                        let shortened = bin BinaryOp.Add start (i64 (int64 n))
                        ScalarExpr.Conditional(before, ScalarExpr.Conditional(bin BinaryOp.Lt shortened (i64 0L), i64 0L, shortened), i64 (int64 n))
                str' (slice offset length)
        | "left"
        | "right" ->
            arity [ 2 ]
            let s = (textArg 0).Expr
            let n = max 0 (wholeLiteral 1)
            if name = "left" || n = 0 then str' (fn ScalarFunction.Slice [ s; i64 0L; i64 (int64 n) ] LogicalTypes.String)
            else str' (fn ScalarFunction.Slice [ s; i64 (int64 -n); lit (LiteralValue.Null LogicalTypes.Int64) ] LogicalTypes.String)
        | "concat" ->
            if args.IsEmpty then fail cx span "concat() needs arguments" None
            concat cx span (List.mapi (fun i v -> v, argSpan i) args)
        | "concat_ws" ->
            if args.Length < 2 then fail cx span "concat_ws() takes a separator and at least one value" None
            let separator = stringArg 0
            // Null values are skipped (all null: the empty string).
            let items = args.Tail |> List.mapi (fun i v -> (asText cx (argSpan (i + 1)) v).Expr)
            let joined =
                items
                |> List.fold
                    (fun (acc: ScalarExpr) (x: ScalarExpr) ->
                        ScalarExpr.Conditional(isNull x, acc,
                            ScalarExpr.Conditional(isNull acc, x, bin BinaryOp.StringConcat (bin BinaryOp.StringConcat acc (str separator)) x)))
                    (lit (LiteralValue.Null LogicalTypes.String))
            str' (ScalarExpr.Coalesce [ joined; str "" ])
        | "trim"
        | "ltrim"
        | "rtrim" ->
            arity [ 1 ]
            let f =
                match name with
                | "trim" -> ScalarFunction.Strip
                | "ltrim" -> ScalarFunction.StripStart
                | _ -> ScalarFunction.StripEnd
            str' (fn f [ (textArg 0).Expr; str " " ] LogicalTypes.String)
        | "replace" ->
            arity [ 2; 3 ]
            let s = (textArg 0).Expr
            let search = stringArg 1
            let replacement = if args.Length = 3 then stringArg 2 else ""
            if search = "" then str' s
            else str' (fn ScalarFunction.ReplaceAll [ s; str search; str replacement; lit (LiteralValue.Bool true) ] LogicalTypes.String)
        | "lpad"
        | "rpad" ->
            arity [ 2; 3 ]
            let s = (textArg 0).Expr
            let n = max 0 (wholeLiteral 1)
            let pad = if args.Length = 3 then stringArg 2 else " "
            if pad.Length <> 1 then fail cx (argSpan 2) $"{name}() pads with one character in this version" None
            // Longer text is cut to n characters (from the start).
            let cut = fn ScalarFunction.Slice [ s; i64 0L; i64 (int64 n) ] LogicalTypes.String
            let padded = fn (if name = "lpad" then ScalarFunction.PadStart else ScalarFunction.PadEnd) [ s; i32 n; str pad ] LogicalTypes.String
            let long = bin BinaryOp.GtEq (cast (fn ScalarFunction.LenChars [ s ] LogicalTypes.UInt32) LogicalTypes.Int64) (i64 (int64 n))
            str' (ScalarExpr.Conditional(long, cut, padded))
        | "startswith"
        | "endswith"
        | "contains" ->
            arity [ 2 ]
            let op =
                match name with
                | "startswith" -> BinaryOp.StartsWith
                | "endswith" -> BinaryOp.EndsWith
                | _ -> BinaryOp.Like
            value (bin op (textArg 0).Expr (textArg 1).Expr) LogicalTypes.Boolean
        | "abs" ->
            arity [ 1 ]
            let v = numberArg 0
            match classify v.Type with
            | SqlTypeClass.Integer _ -> value (checkedIntegral v.Type (fun wide -> ScalarExpr.Unary(UnaryOp.Abs, wide v.Expr))) v.Type
            | _ -> value (ScalarExpr.Unary(UnaryOp.Abs, v.Expr)) v.Type
        | "round"
        | "bround" ->
            arity [ 1; 2 ]
            let v = numberArg 0
            let d = if args.Length = 2 then wholeLiteral 1 else 0
            match classify v.Type with
            | SqlTypeClass.Integer _ when d >= 0 -> v
            | SqlTypeClass.Decimal(p, s) when d >= 0 ->
                if d >= s then v
                else
                    let t = SqlTypes.decimal (min SqlTypes.maxPrecision (p - s + d + 1), d)
                    if name = "round" then value (ScalarExprOps.checkedRescale t ScalarExprOps.unguarded v.Expr) t
                    else
                        // One more integral digit for the rounding to carry into.
                        let wider = SqlTypes.decimal (min SqlTypes.maxPrecision (p + 1), s)
                        value (cast (fn (ScalarFunction.Round RoundMode.HalfToEven) [ cast v.Expr wider; i32 d ] wider) t) t
            | SqlTypeClass.Floating _ when d = 0 ->
                let mode = if name = "round" then RoundMode.HalfAwayFromZero else RoundMode.HalfToEven
                value (fn (ScalarFunction.Round mode) [ v.Expr; i32 0 ] v.Type) v.Type
            | SqlTypeClass.Floating _ ->
                fail cx span $"{name}() of a floating value to {d} digits is not supported: Spark rounds the value's shortest decimal text" (Some "Cast it to a DECIMAL first.")
            | SqlTypeClass.Null -> v
            | _ -> fail cx (argSpan 1) $"{name}() to a negative number of digits is not supported" None
        | "floor"
        | "ceil"
        | "ceiling" ->
            arity [ 1 ]
            let v = numberArg 0
            let floor = name = "floor"
            match classify v.Type with
            | SqlTypeClass.Integer _ -> widen v LogicalTypes.Int64
            | SqlTypeClass.Floating _ ->
                // Java's conversion: saturated, NaN 0 (the IR's floating-to-integer cast).
                value (cast (fn (if floor then ScalarFunction.Floor else ScalarFunction.Ceil) [ v.Expr ] v.Type) LogicalTypes.Int64) LogicalTypes.Int64
            | SqlTypeClass.Decimal(p, s) ->
                let t = SqlTypes.decimal (min SqlTypes.maxPrecision (p - s + 1), 0)
                // The nearest whole number (the engine's cast rounds), then one step down (floor) or up (ceil) where it
                // lies on the other side; compared at the value's scale, one digit wider.
                let whole = cast v.Expr t
                let wider = SqlTypes.decimal (min SqlTypes.maxPrecision (p + 1), s)
                let past = bin (if floor then BinaryOp.Gt else BinaryOp.Lt) (cast whole wider) (cast v.Expr wider)
                let step = bin (if floor then BinaryOp.Subtract else BinaryOp.Add) (cast whole wider) (ScalarExpr.Cast(lit (LiteralValue.Decimal 1m), wider, false))
                value (ScalarExpr.Conditional(past, cast step t, whole)) t
            | _ -> nullOf LogicalTypes.Int64
        | "pmod" ->
            arity [ 2 ]
            let a, b = numberArg 0, numberArg 1
            let r = arithmetic cx span SqlBinaryOp.Modulo a b
            let n = widen b r.Type
            let negative = bin BinaryOp.Lt r.Expr (ScalarExpr.Cast(lit (LiteralValue.Int32 0), r.Type, false))
            let shifted = arithmetic cx span SqlBinaryOp.Modulo (arithmetic cx span SqlBinaryOp.Add r n) n
            value (ScalarExpr.Conditional(negative, (castTo cx span shifted r.Type).Expr, r.Expr)) r.Type
        | "year" -> arity [ 1 ]; datePart cx span name ScalarFunction.DtYear args.[0]
        | "month" -> arity [ 1 ]; datePart cx span name ScalarFunction.DtMonth args.[0]
        | "day"
        | "dayofmonth" -> arity [ 1 ]; datePart cx span name ScalarFunction.DtDay args.[0]
        | "quarter" ->
            arity [ 1 ]
            let m = datePart cx span name ScalarFunction.DtMonth args.[0]
            value (bin BinaryOp.TruncatedDivide (bin BinaryOp.Add m.Expr (i32 2)) (i32 3)) LogicalTypes.Int32
        | "dayofweek"
        | "weekday" ->
            arity [ 1 ]
            // ISO weekday (Monday 1); Spark's dayofweek is Sunday 1, weekday Monday 0.
            let w = datePart cx span name ScalarFunction.DtWeekday args.[0]
            if name = "weekday" then value (bin BinaryOp.Subtract w.Expr (i32 1)) LogicalTypes.Int32
            else value (bin BinaryOp.Add (bin BinaryOp.Modulo w.Expr (i32 7)) (i32 1)) LogicalTypes.Int32
        | "hour" -> arity [ 1 ]; timePart cx span name ScalarFunction.DtHour args.[0]
        | "minute" -> arity [ 1 ]; timePart cx span name ScalarFunction.DtMinute args.[0]
        | "second" -> arity [ 1 ]; timePart cx span name ScalarFunction.DtSecond args.[0]
        | "date_format" ->
            arity [ 2 ]
            let v = args.[0]
            let pattern = stringArg 1
            let format =
                match datePattern pattern with
                | Some f -> f
                | None -> fail cx (argSpan 1) $"The date pattern '{pattern}' is not supported" (Some "Patterns of yyyy, yy, MM, dd, HH, mm, ss, SSS, SSSSSS and - / : . _ , 'T' and spaces are.")
            let wallClock =
                match classify v.Type with
                | SqlTypeClass.Timestamp -> local cx v
                | SqlTypeClass.Date -> cast v.Expr LogicalTypes.Datetime
                | SqlTypeClass.Null -> v.Expr
                | _ -> fail cx (argSpan 0) $"date_format() takes a DATE or a TIMESTAMP, not {display v.Type}" None
            str' (fn ScalarFunction.DtToString [ wallClock; str format ] LogicalTypes.String)
        | "trunc" ->
            arity [ 2 ]
            let v = args.[0]
            let date =
                match classify v.Type with
                | SqlTypeClass.Date -> v.Expr
                | SqlTypeClass.Timestamp -> fn ScalarFunction.DtDate [ local cx v ] LogicalTypes.Date
                | SqlTypeClass.Null -> v.Expr
                | _ -> fail cx (argSpan 0) $"trunc() takes a DATE, not {display v.Type}" None
            value (truncateDate cx (argSpan 1) (stringArg 1) date) LogicalTypes.Date
        | "date_trunc" ->
            arity [ 2 ]
            let unit = (stringArg 0).ToUpperInvariant()
            let v = args.[1]
            let wallClock =
                match classify v.Type with
                | SqlTypeClass.Timestamp -> local cx v
                | SqlTypeClass.Date -> cast v.Expr LogicalTypes.Datetime
                | _ -> fail cx (argSpan 1) $"date_trunc() takes a TIMESTAMP, not {display v.Type}" None
            let micros (unitMicros: int64) =
                let us = cast wallClock LogicalTypes.Int64
                cast (bin BinaryOp.Multiply (bin BinaryOp.FloorDivide us (i64 unitMicros)) (i64 unitMicros)) LogicalTypes.Datetime
            let truncated =
                match unit with
                | "YEAR" | "YYYY" | "YY" | "QUARTER" | "MONTH" | "MM" | "MON" | "WEEK" ->
                    cast (truncateDate cx (argSpan 0) unit (fn ScalarFunction.DtDate [ wallClock ] LogicalTypes.Date)) LogicalTypes.Datetime
                | "DAY" | "DD" -> micros 86_400_000_000L
                | "HOUR" -> micros 3_600_000_000L
                | "MINUTE" -> micros 60_000_000L
                | "SECOND" -> micros 1_000_000L
                | "MILLISECOND" -> micros 1_000L
                | "MICROSECOND" -> wallClock
                | _ -> fail cx (argSpan 0) $"'{unit}' is not a unit of date_trunc()" (Some "Units: YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND.")
            // Spark gives a TIMESTAMP: the truncated wall clock as an instant of the session zone.
            if not (isUtc cx.Zone) then
                fail cx span $"date_trunc() is not supported in the session time zone {cx.Zone.Id} (only in UTC)" None
            value (cast truncated LogicalTypes.DatetimeUtc) LogicalTypes.DatetimeUtc
        | _ ->
            let hint = SqlNames.suggest name (Set.toList functionNames) |> Option.map (fun n -> $"Did you mean {n}?")
            fail cx span $"Unknown or unsupported function {name}()" hint

    /// Binds a Spark expression over the columns of a table. The text is read as Spark writes it; the value is guarded
    /// (RaisingGuards). Returns the value and the columns it reads, as defined. Fails with SqlException where the text
    /// is not a Spark expression this profile supports, or not one `use'` allows.
    let bind' (text: string) (columns: (string * LogicalType) list) (use': SparkExpressionUse) (zone: TimeZoneInfo) (now: DateTimeOffset) =
        let parsed = SqlParser.parseExpression SqlDialect.SparkExpression text
        let cx = { Text = text; Columns = columns; Use = use'; Zone = zone; Now = now; Read = List() }
        let v = bind cx parsed
        let v =
            match use' with
            | SparkExpressionUse.Check -> expectBoolean cx parsed.Span "A CHECK constraint" v
            | _ -> v
        { v with Expr = RaisingGuards.ofExpr v.Expr }, List.ofSeq cx.Read
