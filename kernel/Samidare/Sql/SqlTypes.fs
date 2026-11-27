namespace Kaminari.Samidare.Sql

open Apache.Arrow.Types
open Kaminari.Samidare.IR

/// What SQL sees of an IR type: its family, and a decimal's precision and scale.
[<RequireQualifiedAccess>]
type internal SqlTypeClass =
    | Null
    | Boolean
    /// A signed or unsigned integer of this many bits.
    | Integer of bits: int * signed: bool
    | Decimal of precision: int * scale: int
    | Floating of bits: int
    | String
    | Date
    | Timestamp
    /// A map of these key and value types.
    | Map of key: LogicalType * value: LogicalType
    | Other

/// SQL's type rules over IR types (sql-frontend design, Q4): numeric promotion, decimal results as Spark computes them,
/// and the types CAST names.
module internal SqlTypes =

    /// The widest decimal.
    let maxPrecision = 38

    let classify (t: LogicalType) : SqlTypeClass =
        match t with
        | LogicalType.Arrow arrow ->
            match arrow with
            | :? NullType -> SqlTypeClass.Null
            | :? BooleanType -> SqlTypeClass.Boolean
            | :? Int8Type -> SqlTypeClass.Integer(8, true)
            | :? Int16Type -> SqlTypeClass.Integer(16, true)
            | :? Int32Type -> SqlTypeClass.Integer(32, true)
            | :? Int64Type -> SqlTypeClass.Integer(64, true)
            | :? UInt8Type -> SqlTypeClass.Integer(8, false)
            | :? UInt16Type -> SqlTypeClass.Integer(16, false)
            | :? UInt32Type -> SqlTypeClass.Integer(32, false)
            | :? UInt64Type -> SqlTypeClass.Integer(64, false)
            | :? Decimal128Type as d -> SqlTypeClass.Decimal(d.Precision, d.Scale)
            | :? Decimal256Type as d -> SqlTypeClass.Decimal(d.Precision, d.Scale)
            | :? HalfFloatType -> SqlTypeClass.Floating 16
            | :? FloatType -> SqlTypeClass.Floating 32
            | :? DoubleType -> SqlTypeClass.Floating 64
            | :? StringType
            | :? StringViewType
            | :? LargeStringType -> SqlTypeClass.String
            | :? Date32Type
            | :? Date64Type -> SqlTypeClass.Date
            | :? TimestampType -> SqlTypeClass.Timestamp
            | :? MapType as m -> SqlTypeClass.Map(LogicalType.Arrow m.KeyField.DataType, LogicalType.Arrow m.ValueField.DataType)
            | _ -> SqlTypeClass.Other
        | _ -> SqlTypeClass.Other

    let isNumeric (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Integer _
        | SqlTypeClass.Decimal _
        | SqlTypeClass.Floating _ -> true
        | _ -> false

    let isInteger (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Integer _ -> true
        | _ -> false

    let isNull (t: LogicalType) = classify t = SqlTypeClass.Null

    /// The SQL name of a type, for messages.
    let rec display (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Null -> "NULL"
        | SqlTypeClass.Boolean -> "BOOLEAN"
        | SqlTypeClass.Integer(8, true) -> "TINYINT"
        | SqlTypeClass.Integer(16, true) -> "SMALLINT"
        | SqlTypeClass.Integer(32, true) -> "INTEGER"
        | SqlTypeClass.Integer(64, true) -> "BIGINT"
        | SqlTypeClass.Integer(bits, _) -> $"UINT{bits}"
        | SqlTypeClass.Decimal(p, s) -> $"DECIMAL({p}, {s})"
        | SqlTypeClass.Floating 32 -> "REAL"
        | SqlTypeClass.Floating 16 -> "HALF"
        | SqlTypeClass.Floating _ -> "DOUBLE"
        | SqlTypeClass.String -> "VARCHAR"
        | SqlTypeClass.Date -> "DATE"
        | SqlTypeClass.Timestamp -> "TIMESTAMP"
        | SqlTypeClass.Map(key, value) -> $"MAP({display key}, {display value})"
        | SqlTypeClass.Other -> t.Display

    /// The map type of these key and value types (keys never null).
    let mapOf (key: LogicalType) (value: LogicalType) : LogicalType option =
        match key, value with
        | LogicalType.Arrow k, LogicalType.Arrow v -> Some(LogicalType.Arrow(MapType(Apache.Arrow.Field("key", k, false), Apache.Arrow.Field("value", v, true))))
        | _ -> None

    /// The decimal holding every value of an integer type.
    let integerDecimal (bits: int) (signed: bool) =
        match bits, signed with
        | 8, _ -> 3, 0
        | 16, _ -> 5, 0
        | 32, _ -> 10, 0
        | 64, true -> 19, 0
        | _ -> 20, 0

    let private integerOf (bits: int) =
        match bits with
        | 8 -> LogicalTypes.Int8
        | 16 -> LogicalTypes.Int16
        | 32 -> LogicalTypes.Int32
        | _ -> LogicalTypes.Int64

    /// A decimal of more than 38 digits made to fit, as Spark does: the integral digits kept, the scale cut to what
    /// is left but never below min(scale, 6).
    let fitDecimal (precision: int) (scale: int) =
        if precision <= maxPrecision then precision, scale
        else
            let integral = precision - scale
            let minimumScale = min scale 6
            maxPrecision, max (maxPrecision - integral) minimumScale

    let decimal (precision: int, scale: int) = LogicalTypes.decimalOf precision scale

    /// A numeric type as a decimal (integers exactly; a decimal as itself).
    let asDecimal (t: LogicalType) =
        match classify t with
        | SqlTypeClass.Decimal(p, s) -> Some(p, s)
        | SqlTypeClass.Integer(bits, signed) -> Some(integerDecimal bits signed)
        | _ -> None

    /// The type two numeric operands meet in: the wider integer; a decimal holding both; a floating value as soon as one
    /// is floating. None when one is not numeric.
    let commonNumeric (a: LogicalType) (b: LogicalType) : LogicalType option =
        match classify a, classify b with
        | SqlTypeClass.Floating x, SqlTypeClass.Floating y -> Some(if max x y <= 32 then LogicalTypes.Float32 else LogicalTypes.Float64)
        | SqlTypeClass.Floating _, (SqlTypeClass.Integer _ | SqlTypeClass.Decimal _)
        | (SqlTypeClass.Integer _ | SqlTypeClass.Decimal _), SqlTypeClass.Floating _ -> Some LogicalTypes.Float64
        | SqlTypeClass.Integer(x, true), SqlTypeClass.Integer(y, true) -> Some(integerOf (max x y))
        // An unsigned integer meets in the signed type twice as wide (64 bits at most).
        | SqlTypeClass.Integer(x, _), SqlTypeClass.Integer(y, _) -> Some(integerOf (min 64 (2 * max x y)))
        | (SqlTypeClass.Integer _ | SqlTypeClass.Decimal _), (SqlTypeClass.Integer _ | SqlTypeClass.Decimal _) ->
            let p1, s1 = (asDecimal a).Value
            let p2, s2 = (asDecimal b).Value
            let scale = max s1 s2
            Some(decimal (fitDecimal (max (p1 - s1) (p2 - s2) + scale) scale))
        | _ -> None

    /// The type two values compare, combine (CASE, COALESCE, UNION) or are tested for equality in: equal families,
    /// numeric promotion, a string meeting a date or timestamp as that (its text read as one), NULL meeting anything.
    let common (a: LogicalType) (b: LogicalType) : LogicalType option =
        match classify a, classify b with
        | SqlTypeClass.Null, _ -> Some b
        | _, SqlTypeClass.Null -> Some a
        | SqlTypeClass.Boolean, SqlTypeClass.Boolean -> Some a
        | SqlTypeClass.String, SqlTypeClass.String -> Some LogicalTypes.String
        | SqlTypeClass.Date, SqlTypeClass.Date -> Some a
        | SqlTypeClass.Timestamp, SqlTypeClass.Timestamp -> Some a
        | SqlTypeClass.Date, SqlTypeClass.Timestamp
        | SqlTypeClass.Timestamp, SqlTypeClass.Date -> Some LogicalTypes.Datetime
        | SqlTypeClass.String, (SqlTypeClass.Date | SqlTypeClass.Timestamp) -> Some b
        | (SqlTypeClass.Date | SqlTypeClass.Timestamp), SqlTypeClass.String -> Some a
        // Maps meet only maps of the same types (compared entry by entry, in entry order).
        | SqlTypeClass.Map _, SqlTypeClass.Map _ -> if display a = display b then Some a else None
        | _ -> commonNumeric a b

    /// The result of `a op b` over two decimals (integers taken as decimals), as Spark types it.
    let decimalResult (op: SqlBinaryOp) (p1: int, s1: int) (p2: int, s2: int) : int * int =
        match op with
        | SqlBinaryOp.Add
        | SqlBinaryOp.Subtract ->
            let scale = max s1 s2
            fitDecimal (max (p1 - s1) (p2 - s2) + scale + 1) scale
        | SqlBinaryOp.Multiply -> fitDecimal (p1 + p2 + 1) (s1 + s2)
        | SqlBinaryOp.Divide ->
            let scale = max 6 (s1 + p2 + 1)
            fitDecimal (p1 - s1 + s2 + scale) scale
        | SqlBinaryOp.Modulo ->
            let scale = max s1 s2
            fitDecimal (min (p1 - s1) (p2 - s2) + scale) scale
        | _ -> invalidOp $"No decimal rule for {op}"

    /// The type `CAST(x AS name(parameters))` names, or None for a name SQL does not know.
    let rec ofTypeName (t: SqlTypeName) : LogicalType option =
        match t.Name, t.Parameters with
        | "MAP", [] ->
            match t.Arguments |> List.map ofTypeName with
            | [ Some key; Some value ] -> mapOf key value
            | _ -> None
        | ("BOOLEAN" | "BOOL"), [] -> Some LogicalTypes.Boolean
        | "TINYINT", [] -> Some LogicalTypes.Int8
        | ("SMALLINT" | "INT2"), [] -> Some LogicalTypes.Int16
        | ("INTEGER" | "INT" | "INT4"), [] -> Some LogicalTypes.Int32
        | ("BIGINT" | "INT8"), [] -> Some LogicalTypes.Int64
        | ("DECIMAL" | "NUMERIC" | "DEC"), [] -> Some(decimal (18, 3))
        | ("DECIMAL" | "NUMERIC" | "DEC"), [ p ] when p >= 1 && p <= maxPrecision -> Some(decimal (p, 0))
        | ("DECIMAL" | "NUMERIC" | "DEC"), [ p; s ] when p >= 1 && p <= maxPrecision && s >= 0 && s <= p -> Some(decimal (p, s))
        | ("REAL" | "FLOAT4"), [] -> Some LogicalTypes.Float32
        | ("DOUBLE" | "DOUBLE PRECISION" | "FLOAT" | "FLOAT8"), [] -> Some LogicalTypes.Float64
        | ("VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" | "TEXT" | "STRING"), ([] | [ _ ]) -> Some LogicalTypes.String
        | "DATE", [] -> Some LogicalTypes.Date
        | ("TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE"), [] -> Some LogicalTypes.Datetime
        | _ -> None
