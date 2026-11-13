using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Schema;

/// <summary>How the values of a leaf are turned into its Arrow column.</summary>
internal enum ValueKind
{
    /// <summary>Only nulls (UNKNOWN annotation).</summary>
    Null,
    Boolean,
    /// <summary>INT32 copied as it is: Int32, UInt32, Date32, Time32(ms).</summary>
    Int32,
    /// <summary>INT32 narrowed, each value checked against the range.</summary>
    Int8,
    Int16,
    UInt8,
    UInt16,
    /// <summary>INT64 copied as it is: Int64, UInt64, Time64, Timestamp, Duration.</summary>
    Int64,
    /// <summary>INT64 timestamps rescaled to the unit the file's Arrow schema gives (exactly, or an error).</summary>
    RescaledInt64,
    /// <summary>Unannotated INT64 narrowed to the integer type the file's Arrow schema gives, checked.</summary>
    NarrowInt64,
    Float,
    Double,
    /// <summary>A decimal stored in INT32, INT64, or big-endian bytes (FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY).</summary>
    DecimalFromInt32,
    DecimalFromInt64,
    DecimalFromBytes,
    /// <summary>INT96 (Julian day and nanoseconds of the day) as a timestamp.</summary>
    Int96Timestamp,
    /// <summary>A TIMESTAMP in a 12-byte little-endian signed count of units, narrowed to 64 bits.</summary>
    FlbaTimestamp,
    /// <summary>FIXED_LEN_BYTE_ARRAY(2) half floats.</summary>
    Float16,
    /// <summary>INTERVAL: FIXED_LEN_BYTE_ARRAY(12) of months, days, milliseconds.</summary>
    Interval,
    FixedBinary,
    /// <summary>BYTE_ARRAY as UTF-8 strings (validated).</summary>
    Utf8,
    Binary,
}

/// <summary>A leaf's Arrow type and how its values get there.</summary>
internal sealed record LeafType(IArrowType ArrowType, ValueKind Kind)
{
    public int Scale { get; init; }
    public int Precision { get; init; }

    /// <summary>The unit of a timestamp from INT96 or a 12-byte carrier, or of a TIMESTAMP (for narrowing).</summary>
    public TimeUnit Unit { get; init; }

    /// <summary>The bytes per value of a FIXED_LEN_BYTE_ARRAY.</summary>
    public int TypeLength { get; init; }

    /// <summary>Whether the column is output dictionary-encoded (the file's Arrow schema asks for it).</summary>
    public bool Dictionary { get; init; }

    /// <summary>For <see cref="ValueKind.RescaledInt64"/>: the factor values are multiplied or divided by.</summary>
    public long Multiplier { get; init; } = 1;
    public long Divisor { get; init; } = 1;
}

/// <summary>Maps a Parquet leaf (physical type and annotations) to Arrow, as LogicalTypes.md describes.</summary>
internal static class LeafTypes
{
    /// <summary>
    /// The type of a leaf: the logical type when it is known and fits the physical type, else the converted type,
    /// else the physical type alone (an annotation that does not fit is ignored, as the specification asks).
    /// </summary>
    public static LeafType Resolve(SchemaElement e, ParquetReadOptions options)
    {
        ParquetType physical = e.Type!.Value;
        int length = e.TypeLength ?? 0;
        LeafType? annotated = e.LogicalType is { } logical ? FromLogical(logical, physical, length, e) : FromConverted(e, physical, length);
        // A logical type that does not apply leaves the converted type to try (writers set both).
        annotated ??= e.LogicalType is not null ? FromConverted(e, physical, length) : null;
        return annotated ?? Physical(physical, length, options);
    }

    private static LeafType Physical(ParquetType physical, int length, ParquetReadOptions options) => physical switch
    {
        ParquetType.Boolean => new(BooleanType.Default, ValueKind.Boolean),
        ParquetType.Int32 => new(Int32Type.Default, ValueKind.Int32),
        ParquetType.Int64 => new(Int64Type.Default, ValueKind.Int64),
        ParquetType.Int96 => Int96(options.Int96Unit),
        ParquetType.Float => new(FloatType.Default, ValueKind.Float),
        ParquetType.Double => new(DoubleType.Default, ValueKind.Double),
        ParquetType.ByteArray => options.BinaryAsString ? new(StringType.Default, ValueKind.Utf8) : new(BinaryType.Default, ValueKind.Binary),
        ParquetType.FixedLenByteArray => new(new FixedSizeBinaryType(length), ValueKind.FixedBinary) { TypeLength = length },
        _ => throw new ParquetFormatException($"Unknown physical type {(int)physical}."),
    };

    /// <summary>INT96 timestamps carry no zone: Spark's are UTC instants, but Arrow readers return them as local values.</summary>
    public static LeafType Int96(TimeUnit unit) => new(new TimestampType(unit, (string?)null), ValueKind.Int96Timestamp) { Unit = unit, TypeLength = 12 };

    private static LeafType? FromLogical(LogicalType logical, ParquetType physical, int length, SchemaElement e)
    {
        switch (logical.Kind)
        {
            case LogicalTypeKind.String or LogicalTypeKind.Enum or LogicalTypeKind.Json when physical == ParquetType.ByteArray:
                return new(StringType.Default, ValueKind.Utf8);
            case LogicalTypeKind.Bson or LogicalTypeKind.Geometry or LogicalTypeKind.Geography when physical == ParquetType.ByteArray:
                return new(BinaryType.Default, ValueKind.Binary);
            case LogicalTypeKind.Uuid when physical == ParquetType.FixedLenByteArray && length == 16:
                return new(new FixedSizeBinaryType(16), ValueKind.FixedBinary) { TypeLength = 16 };
            case LogicalTypeKind.Float16 when physical == ParquetType.FixedLenByteArray && length == 2:
                return new(HalfFloatType.Default, ValueKind.Float16) { TypeLength = 2 };
            case LogicalTypeKind.Decimal:
                return Decimal(logical.Precision, logical.Scale, physical, length);
            case LogicalTypeKind.Date when physical == ParquetType.Int32:
                return new(Date32Type.Default, ValueKind.Int32);
            case LogicalTypeKind.Time:
                return logical.Unit switch
                {
                    ParquetTimeUnit.Millis when physical == ParquetType.Int32 => new(new Time32Type(TimeUnit.Millisecond), ValueKind.Int32),
                    ParquetTimeUnit.Micros when physical == ParquetType.Int64 => new(new Time64Type(TimeUnit.Microsecond), ValueKind.Int64),
                    ParquetTimeUnit.Nanos when physical == ParquetType.Int64 => new(new Time64Type(TimeUnit.Nanosecond), ValueKind.Int64),
                    _ => null,
                };
            case LogicalTypeKind.Timestamp when logical.Unit is { } unit:
            {
                TimeUnit arrowUnit = Arrow(unit);
                var type = new TimestampType(arrowUnit, logical.IsAdjustedToUtc ? "UTC" : null);
                return physical switch
                {
                    ParquetType.Int64 => new(type, ValueKind.Int64) { Unit = arrowUnit },
                    ParquetType.FixedLenByteArray when length == 12 => new(type, ValueKind.FlbaTimestamp) { Unit = arrowUnit, TypeLength = 12 },
                    _ => null,
                };
            }
            case LogicalTypeKind.Integer:
                return Integer(logical.BitWidth, logical.IsSigned, physical);
            case LogicalTypeKind.Unknown:
                return new(NullType.Default, ValueKind.Null);
            default:
                return null; // unknown members, and annotations that do not fit the physical type
        }
    }

    private static LeafType? FromConverted(SchemaElement e, ParquetType physical, int length) => e.ConvertedType switch
    {
        ConvertedType.Utf8 or ConvertedType.Enum or ConvertedType.Json when physical == ParquetType.ByteArray => new(StringType.Default, ValueKind.Utf8),
        ConvertedType.Bson when physical == ParquetType.ByteArray => new(BinaryType.Default, ValueKind.Binary),
        ConvertedType.Decimal when e.Precision is int precision => Decimal(precision, e.Scale ?? 0, physical, length),
        ConvertedType.Date when physical == ParquetType.Int32 => new(Date32Type.Default, ValueKind.Int32),
        ConvertedType.TimeMillis when physical == ParquetType.Int32 => new(new Time32Type(TimeUnit.Millisecond), ValueKind.Int32),
        ConvertedType.TimeMicros when physical == ParquetType.Int64 => new(new Time64Type(TimeUnit.Microsecond), ValueKind.Int64),
        // The legacy timestamps are UTC-normalized (LogicalTypes.md, backward compatibility).
        ConvertedType.TimestampMillis when physical == ParquetType.Int64 => new(new TimestampType(TimeUnit.Millisecond, "UTC"), ValueKind.Int64) { Unit = TimeUnit.Millisecond },
        ConvertedType.TimestampMicros when physical == ParquetType.Int64 => new(new TimestampType(TimeUnit.Microsecond, "UTC"), ValueKind.Int64) { Unit = TimeUnit.Microsecond },
        ConvertedType.Int8 => Integer(8, true, physical),
        ConvertedType.Int16 => Integer(16, true, physical),
        ConvertedType.Int32 => Integer(32, true, physical),
        ConvertedType.Int64 => Integer(64, true, physical),
        ConvertedType.UInt8 => Integer(8, false, physical),
        ConvertedType.UInt16 => Integer(16, false, physical),
        ConvertedType.UInt32 => Integer(32, false, physical),
        ConvertedType.UInt64 => Integer(64, false, physical),
        ConvertedType.Interval when physical == ParquetType.FixedLenByteArray && length == 12 =>
            new(new IntervalType(IntervalUnit.MonthDayNanosecond), ValueKind.Interval) { TypeLength = 12 },
        _ => null,
    };

    private static LeafType? Integer(int bits, bool signed, ParquetType physical) => (bits, signed, physical) switch
    {
        (8, true, ParquetType.Int32) => new(Int8Type.Default, ValueKind.Int8),
        (16, true, ParquetType.Int32) => new(Int16Type.Default, ValueKind.Int16),
        (32, true, ParquetType.Int32) => new(Int32Type.Default, ValueKind.Int32),
        (8, false, ParquetType.Int32) => new(UInt8Type.Default, ValueKind.UInt8),
        (16, false, ParquetType.Int32) => new(UInt16Type.Default, ValueKind.UInt16),
        (32, false, ParquetType.Int32) => new(UInt32Type.Default, ValueKind.Int32),
        (64, true, ParquetType.Int64) => new(Int64Type.Default, ValueKind.Int64),
        (64, false, ParquetType.Int64) => new(UInt64Type.Default, ValueKind.Int64),
        _ => null,
    };

    /// <summary>A decimal, when its precision and scale are valid and fit the physical type (Arrow holds up to 76 digits).</summary>
    private static LeafType? Decimal(int precision, int scale, ParquetType physical, int length)
    {
        if (precision < 1 || scale < 0 || scale > precision || precision > 76) return null;
        ValueKind? kind = physical switch
        {
            ParquetType.Int32 when precision <= 9 => ValueKind.DecimalFromInt32,
            ParquetType.Int64 when precision <= 18 => ValueKind.DecimalFromInt64,
            ParquetType.FixedLenByteArray when length <= 32 && precision <= MaxDigits(length) => ValueKind.DecimalFromBytes,
            ParquetType.ByteArray => ValueKind.DecimalFromBytes,
            _ => null,
        };
        if (kind is null) return null;
        IArrowType type = precision <= 38 ? new Decimal128Type(precision, scale) : new Decimal256Type(precision, scale);
        return new LeafType(type, kind.Value) { Precision = precision, Scale = scale, TypeLength = length };
    }

    /// <summary>The digits a two's complement of <paramref name="bytes"/> bytes always holds: floor(log10(2^(8n-1) - 1)).</summary>
    public static int MaxDigits(int bytes) => (int)Math.Floor((8 * bytes - 1) * Math.Log10(2));

    /// <summary>Nanoseconds per unit.</summary>
    public static long Nanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };

    public static TimeUnit Arrow(ParquetTimeUnit unit) => unit switch
    {
        ParquetTimeUnit.Millis => TimeUnit.Millisecond,
        ParquetTimeUnit.Micros => TimeUnit.Microsecond,
        _ => TimeUnit.Nanosecond,
    };
}
