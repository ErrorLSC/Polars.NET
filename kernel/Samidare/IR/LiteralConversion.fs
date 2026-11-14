namespace Kaminari.Samidare.IR

open System
open System.Text
open Apache.Arrow.Types

/// How the values of a column are numbers, times or bytes: what IR literals it is compared with and how they map
/// onto its stored values.
[<RequireQualifiedAccess>]
type ValueDomain =
    /// Integers whose every value a double holds exactly (up to 32 bits): a floating literal compares as a number.
    | Integer
    /// 64-bit integers: the engine compares them with a floating literal as doubles, which rounds values beyond 2^53,
    /// so floating literals are not converted (only exact ones).
    | WideInteger
    /// Decimals: integers scaled by 10^scale.
    | Decimal of scale: int
    | Boolean
    /// Days since 1970-01-01.
    | Date
    /// Milliseconds since 1970-01-01, whole days.
    | Date64
    /// Time of day in the unit.
    | Time of unit: TimeUnit
    /// Wall-clock timestamps (no zone) in the unit since 1970-01-01.
    | Timestamp of unit: TimeUnit
    /// Instants (UTC-adjusted) in the unit since 1970-01-01T00:00Z.
    | TimestampUtc of unit: TimeUnit
    /// Durations in the unit.
    | Duration of unit: TimeUnit
    /// Floating values (Float16, Float32, Float64).
    | Floating
    /// UTF-8 strings.
    | String

/// A literal in a domain's stored representation.
[<RequireQualifiedAccess>]
type StoredLiteral =
    /// The integer `value`, or, when `half`, a number strictly between `value` and `value + 1` (a literal finer than
    /// the stored unit, as 2.5 against integers or a tick against milliseconds).
    | Integer of value: Int128 * half: bool
    | Floating of value: float
    | Bytes of value: byte[]

/// Literals of the IR into the stored representation of a column's values, exactly or with the remainder marked
/// (see StoredLiteral.Integer). Shared by every reader that compares stored values with IR literals: statistics
/// pruning, row filters.
[<RequireQualifiedAccess>]
module LiteralConversion =

    let private epochTicks = DateTime.UnixEpoch.Ticks
    let private epochDay = DateOnly(1970, 1, 1).DayNumber
    let private i128 (v: int64) : Int128 = Int128.op_Implicit v

    let private pow10 (exponent: int) : Int128 =
        if exponent < 0 || exponent > 38 then raise (OverflowException())
        let mutable p = Int128.One
        for _ in 1..exponent do
            p <- Int128.op_CheckedMultiply (p, i128 10L)
        p

    /// value / divisor, floored, and whether there was a remainder.
    let private floorDivide (value: Int128) (divisor: Int128) : StoredLiteral =
        let struct (q, r) = Int128.DivRem(value, divisor)
        let q = if r <> Int128.Zero && value < Int128.Zero then q - Int128.One else q
        StoredLiteral.Integer(q, r <> Int128.Zero)

    /// A count of 100 ns ticks in a unit.
    let private fromTicks (unit: TimeUnit) (ticks: int64) : StoredLiteral =
        match unit with
        | TimeUnit.Second -> floorDivide (i128 ticks) (i128 TimeSpan.TicksPerSecond)
        | TimeUnit.Millisecond -> floorDivide (i128 ticks) (i128 TimeSpan.TicksPerMillisecond)
        | TimeUnit.Microsecond -> floorDivide (i128 ticks) (i128 10L)
        | _ -> StoredLiteral.Integer(Int128.op_CheckedMultiply (i128 ticks, i128 100L), false)

    let private tryExactInteger (literal: LiteralValue) : Int128 voption =
        match literal with
        | LiteralValue.Int8 v -> ValueSome(i128 (int64 v))
        | LiteralValue.Int16 v -> ValueSome(i128 (int64 v))
        | LiteralValue.Int32 v -> ValueSome(i128 (int64 v))
        | LiteralValue.Int64 v -> ValueSome(i128 v)
        | LiteralValue.UInt8 v -> ValueSome(i128 (int64 v))
        | LiteralValue.UInt16 v -> ValueSome(i128 (int64 v))
        | LiteralValue.UInt32 v -> ValueSome(i128 (int64 v))
        | LiteralValue.UInt64 v -> ValueSome(Int128.op_Implicit v)
        | _ -> ValueNone

    /// An exact number scaled by 10^scale, floored, with any fraction marked; floating literals only when allowed.
    let private tryInteger (literal: LiteralValue) (scale: int) (allowFloat: bool) : StoredLiteral option =
        match literal with
        | LiteralValue.Decimal d ->
            // d = mantissa / 10^e; d * 10^scale = mantissa * 10^(scale - e).
            let bits = Decimal.GetBits d
            let magnitude =
                (Int128.op_Implicit (uint64 (uint32 bits[2])) <<< 64)
                ||| (Int128.op_Implicit (uint64 (uint32 bits[1])) <<< 32)
                ||| Int128.op_Implicit (uint64 (uint32 bits[0]))
            let mantissa = if bits[3] < 0 then -magnitude else magnitude
            let e = (bits[3] >>> 16) &&& 0xFF
            if scale >= e then
                Some(StoredLiteral.Integer(Int128.op_CheckedMultiply (mantissa, pow10 (scale - e)), false))
            else
                Some(floorDivide mantissa (pow10 (e - scale)))
        | LiteralValue.Float16 _
        | LiteralValue.Float32 _
        | LiteralValue.Float64 _ when allowFloat ->
            let v =
                match literal with
                | LiteralValue.Float16 h -> float h
                | LiteralValue.Float32 f -> float f
                | LiteralValue.Float64 x -> x
                | _ -> nan
            // Within ±1e30 a double's floor is exact in Int128; beyond, and NaN or infinities, nothing is assumed.
            if not (Double.IsFinite v) || abs v > 1e30 then
                None
            else
                let floor = Math.Floor v
                Some(StoredLiteral.Integer(Int128.op_Explicit floor, floor <> v))
        | _ ->
            match tryExactInteger literal with
            | ValueSome n -> Some(StoredLiteral.Integer(Int128.op_CheckedMultiply (n, pow10 scale), false))
            | ValueNone -> None

    let private tryFloating (literal: LiteralValue) : StoredLiteral option =
        match literal with
        | LiteralValue.Float16 h -> Some(StoredLiteral.Floating(float h))
        | LiteralValue.Float32 f -> Some(StoredLiteral.Floating(float f))
        | LiteralValue.Float64 x -> Some(StoredLiteral.Floating x)
        | LiteralValue.Decimal d ->
            // Only where the double is the same number.
            let x = float d
            if decimal x = d then Some(StoredLiteral.Floating x) else None
        | _ ->
            // Integers only where the double is the same number.
            match tryExactInteger literal with
            | ValueSome n when Int128.Abs n <= (Int128.One <<< 53) -> Some(StoredLiteral.Floating(float n))
            | _ -> None

    /// The literal in the domain's stored representation, or None when it does not convert (a literal of another
    /// kind, NaN or an infinity against integers, a value beyond Int128).
    let tryConvert (domain: ValueDomain) (literal: LiteralValue) : StoredLiteral option =
        try
            match domain, literal with
            | ValueDomain.Integer, _ -> tryInteger literal 0 true
            | ValueDomain.WideInteger, _ -> tryInteger literal 0 false
            | ValueDomain.Decimal scale, _ -> tryInteger literal scale false
            | ValueDomain.Boolean, LiteralValue.Bool b -> Some(StoredLiteral.Integer((if b then Int128.One else Int128.Zero), false))
            | ValueDomain.Date, LiteralValue.Date d -> Some(StoredLiteral.Integer(i128 (int64 (d.DayNumber - epochDay)), false))
            | ValueDomain.Date64, LiteralValue.Date d ->
                Some(StoredLiteral.Integer(Int128.op_CheckedMultiply (i128 (int64 (d.DayNumber - epochDay)), i128 86_400_000L), false))
            | ValueDomain.Time unit, LiteralValue.TimeOnly t -> Some(fromTicks unit t.Ticks)
            | ValueDomain.Timestamp unit, LiteralValue.DateTime t -> Some(fromTicks unit (t.Ticks - epochTicks))
            | ValueDomain.TimestampUtc unit, LiteralValue.DateTimeOffset t -> Some(fromTicks unit (t.UtcTicks - epochTicks))
            | ValueDomain.Duration unit, LiteralValue.TimeSpan t -> Some(fromTicks unit t.Ticks)
            | ValueDomain.Floating, _ -> tryFloating literal
            | ValueDomain.String, LiteralValue.String s -> Some(StoredLiteral.Bytes(Encoding.UTF8.GetBytes s))
            | _ -> None
        with :? OverflowException ->
            None

    /// The domain of an Arrow column type (a dictionary's values), or None for types no IR literal compares with.
    let rec ofArrowType (arrowType: IArrowType) : ValueDomain option =
        match arrowType with
        | :? DictionaryType as d -> ofArrowType d.ValueType
        | :? Int8Type
        | :? Int16Type
        | :? Int32Type
        | :? UInt8Type
        | :? UInt16Type
        | :? UInt32Type -> Some ValueDomain.Integer
        | :? Int64Type
        | :? UInt64Type -> Some ValueDomain.WideInteger
        | :? Decimal128Type as d -> Some(ValueDomain.Decimal d.Scale)
        | :? BooleanType -> Some ValueDomain.Boolean
        | :? Date32Type -> Some ValueDomain.Date
        | :? Date64Type -> Some ValueDomain.Date64
        | :? Time32Type as t -> Some(ValueDomain.Time t.Unit)
        | :? Time64Type as t -> Some(ValueDomain.Time t.Unit)
        | :? TimestampType as t ->
            Some(if String.IsNullOrEmpty t.Timezone then ValueDomain.Timestamp t.Unit else ValueDomain.TimestampUtc t.Unit)
        | :? DurationType as t -> Some(ValueDomain.Duration t.Unit)
        | :? HalfFloatType
        | :? FloatType
        | :? DoubleType -> Some ValueDomain.Floating
        | :? StringType
        | :? StringViewType -> Some ValueDomain.String
        | _ -> None
