namespace Polars.NET.Linq.IR

open System
open Apache.Arrow.Types
open Polars.NET.Core.Arrow

/// Structural identity of Apache Arrow types. Arrow type objects use reference equality, so two
/// separately constructed Decimal128Type(38, 18) instances are not Equal; the IR compares by this key instead.
module internal ArrowTypeKey =

    let private unitKey (unit: TimeUnit) : string =
        match unit with
        | TimeUnit.Second -> "s"
        | TimeUnit.Millisecond -> "ms"
        | TimeUnit.Microsecond -> "us"
        | TimeUnit.Nanosecond -> "ns"
        | other -> string other

    let rec ofArrow (t: IArrowType) : string =
        match t with
        | :? TimestampType as ts ->
            if String.IsNullOrEmpty ts.Timezone then sprintf "Timestamp(%s)" (unitKey ts.Unit)
            else sprintf "Timestamp(%s, %s)" (unitKey ts.Unit) ts.Timezone
        | :? Time32Type as x -> sprintf "Time32(%s)" (unitKey x.Unit)
        | :? Time64Type as x -> sprintf "Time64(%s)" (unitKey x.Unit)
        | :? DurationType as d -> sprintf "Duration(%s)" (unitKey d.Unit)
        | :? Decimal128Type as d -> sprintf "Decimal128(%d, %d)" d.Precision d.Scale
        | :? Decimal256Type as d -> sprintf "Decimal256(%d, %d)" d.Precision d.Scale
        | :? FixedSizeBinaryType as f -> sprintf "FixedSizeBinary(%d)" f.ByteWidth
        | :? MapType as m -> sprintf "Map<%s, %s>" (ofArrow m.KeyField.DataType) (ofArrow m.ValueField.DataType)
        | :? ListType as l -> sprintf "List<%s>" (ofArrow l.ValueDataType)
        | :? LargeListType as l -> sprintf "LargeList<%s>" (ofArrow l.ValueDataType)
        | :? FixedSizeListType as l -> sprintf "FixedSizeList<%s, %d>" (ofArrow l.ValueDataType) l.ListSize
        | :? StructType as s ->
            s.Fields
            |> Seq.map (fun f -> sprintf "%s: %s" f.Name (ofArrow f.DataType))
            |> String.concat ", "
            |> sprintf "Struct<%s>"
        | :? DictionaryType as d -> sprintf "Dictionary<%s, %s>" (ofArrow d.IndexType) (ofArrow d.ValueType)
        | other -> string other.TypeId

/// Logical data type carried by the IR: an Apache Arrow type, a list whose element type is not
/// statically known, or an unknown type left to the backend to infer.
/// Equality and hashing are structural (see ArrowTypeKey).
[<CustomEquality; NoComparison; RequireQualifiedAccess; StructuredFormatDisplay("{Display}")>]
type LogicalType =
    | Unknown
    | Arrow of arrowType: IArrowType
    | ListOf of element: LogicalType

    /// Structural key used for equality, hashing and display.
    member this.Display: string =
        match this with
        | Unknown -> "Unknown"
        | Arrow t -> ArrowTypeKey.ofArrow t
        | ListOf element -> sprintf "List<%s>" element.Display

    override this.ToString() = this.Display

    override this.Equals(other: obj) =
        match other with
        | :? LogicalType as o -> this.Display = o.Display
        | _ -> false

    override this.GetHashCode() = this.Display.GetHashCode()

/// Canonical LogicalType values and CLR type mapping for the IR.
[<RequireQualifiedAccess>]
module LogicalTypes =

    let Null = LogicalType.Arrow NullType.Default
    let Boolean = LogicalType.Arrow BooleanType.Default
    let Int8 = LogicalType.Arrow Int8Type.Default
    let Int16 = LogicalType.Arrow Int16Type.Default
    let Int32 = LogicalType.Arrow Int32Type.Default
    let Int64 = LogicalType.Arrow Int64Type.Default
    let UInt8 = LogicalType.Arrow UInt8Type.Default
    let UInt16 = LogicalType.Arrow UInt16Type.Default
    let UInt32 = LogicalType.Arrow UInt32Type.Default
    let UInt64 = LogicalType.Arrow UInt64Type.Default
    let Float32 = LogicalType.Arrow FloatType.Default
    let Float64 = LogicalType.Arrow DoubleType.Default
    /// Fixed-point decimal with the given precision and scale.
    let decimalOf (precision: int) (scale: int) = LogicalType.Arrow(Decimal128Type(precision, scale))
    /// Matches DataFrameBuilder / ArrowTypeResolver for System.Decimal.
    let Decimal = decimalOf 38 18
    let String = LogicalType.Arrow StringViewType.Default
    let Binary = LogicalType.Arrow BinaryViewType.Default
    let Date = LogicalType.Arrow Date32Type.Default
    /// Naive (time-zone-less) microsecond timestamp: System.DateTime.
    let Datetime = LogicalType.Arrow(TimestampType(TimeUnit.Microsecond, (null: string)))
    /// UTC microsecond timestamp: System.DateTimeOffset values are normalized to UTC.
    let DatetimeUtc = LogicalType.Arrow(TimestampType(TimeUnit.Microsecond, "UTC"))
    let Time = LogicalType.Arrow(Time64Type(TimeUnit.Nanosecond))
    let Duration = LogicalType.Arrow DurationType.Microsecond
    /// List whose element type is not statically known.
    let ListOfUnknown = LogicalType.ListOf LogicalType.Unknown

    /// Maps a CLR type to its IR logical type, using the same Arrow mapping as DataFrame ingestion.
    /// Returns None for types without a Polars logical counterpart: dictionaries / maps, and types (or nested
    /// element / member types) without an Arrow representation, such as object, enums or member-less classes.
    let tryOfClrType (t: Type) : LogicalType option =
        if isNull t then
            None
        else
            match ArrowTypeResolver.TryGetArrowTypeFromNetType t with
            | null
            | :? MapType -> None
            | arrowType -> Some(LogicalType.Arrow arrowType)

