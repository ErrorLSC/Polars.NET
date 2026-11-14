using System.Buffers.Binary;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Kaminari.Seseragi.Files.Parquet.Pruning;

/// <summary>How the stored values of a leaf compare.</summary>
internal enum DomainKind
{
    /// <summary>Exact integers (integers, decimals, dates, times, timestamps, booleans), compared as <see cref="Int128"/>.</summary>
    Integer,
    /// <summary>FLOAT, DOUBLE, FLOAT16: compared as doubles, NaN greater than every number and equal to itself.</summary>
    Floating,
    /// <summary>Strings: unsigned byte-wise (code point order, as the IR compares strings).</summary>
    Bytes,
}

/// <summary>Which literals a leaf takes, and how they map onto its stored values.</summary>
internal enum LiteralShape
{
    Integer,
    Decimal,
    Date,
    Time,
    Timestamp,
    TimestampUtc,
    Boolean,
    Floating,
    String,
}

/// <summary>
/// A literal in a leaf's stored representation. For <see cref="DomainKind.Integer"/>, the literal is
/// <see cref="Integer"/>, or lies strictly between it and the next integer when <see cref="Half"/> (a literal finer
/// than the stored unit, as 2.5 against integers).
/// </summary>
internal readonly record struct Bound(Int128 Integer = default, bool Half = false, double Float = 0, byte[]? Bytes = null);

/// <summary>
/// The statistics rules of one leaf: which statistics fields may be used (the column order and the physical type
/// decide), how its stored bounds decode, and how IR literals convert into the stored representation. A literal that
/// does not convert exactly to a modelled kind is refused, which keeps every unit.
/// </summary>
internal sealed class LeafDomain
{
    private LeafDomain(int leaf, DomainKind kind, LiteralShape shape, ParquetType physical)
    {
        Leaf = leaf;
        Kind = kind;
        Shape = shape;
        Physical = physical;
    }

    public int Leaf { get; }
    public DomainKind Kind { get; }
    public LiteralShape Shape { get; }
    public ParquetType Physical { get; }

    /// <summary>INT32 / INT64 read as unsigned (UINT annotations).</summary>
    public bool Unsigned { get; private init; }

    /// <summary>The scale of a decimal.</summary>
    public int Scale { get; private init; }

    /// <summary>The stored unit of a time or timestamp.</summary>
    public TimeUnit Unit { get; private init; }

    /// <summary>A FLOAT16 (FIXED_LEN_BYTE_ARRAY(2)).</summary>
    public bool Half16 { get; private init; }

    /// <summary>The bytes of a FIXED_LEN_BYTE_ARRAY value.</summary>
    public int TypeLength { get; private init; }

    /// <summary>A decimal in big-endian two's complement bytes.</summary>
    public bool BigEndianDecimal { get; private init; }

    /// <summary>Whether <c>min_value</c> / <c>max_value</c> may be used (the file gives this leaf an order that fits it).</summary>
    public bool TypedStatistics { get; private set; }

    /// <summary>Whether the deprecated signed <c>min</c> / <c>max</c> may be used (no column orders, a signed fixed-width type).</summary>
    public bool LegacyStatistics { get; private set; }

    /// <summary>
    /// The rules of <paramref name="leaf"/>; null when its values cannot be compared by statistics (repeated, INT96,
    /// INTERVAL, binary, unsigned decimals, …).
    /// </summary>
    /// <param name="leaf">The leaf.</param>
    /// <param name="type">The leaf's type from its Parquet annotations (not the file's Arrow schema).</param>
    /// <param name="orders">The file's column orders, or null when it has none.</param>
    /// <param name="leafCount">The number of leaves of the file (column orders must give one each).</param>
    public static LeafDomain? Create(SchemaNode leaf, LeafType type, ColumnOrder[]? orders, int leafCount)
    {
        if (leaf.MaxRepetition != 0) return null; // a row holds many values: no row-level meaning
        ParquetType physical = leaf.Element.Type!.Value;
        LeafDomain? domain = (type.Kind, type.ArrowType) switch
        {
            (ValueKind.Boolean, _) => new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Boolean, physical),
            (ValueKind.Int32 or ValueKind.Int8 or ValueKind.Int16 or ValueKind.UInt8 or ValueKind.UInt16 or ValueKind.Int64, var arrow) =>
                arrow switch
                {
                    Int8Type or Int16Type or Int32Type or Int64Type => new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Integer, physical),
                    UInt8Type or UInt16Type or UInt32Type or UInt64Type => new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Integer, physical) { Unsigned = true },
                    Date32Type => new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Date, physical),
                    Time32Type t => new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Time, physical) { Unit = t.Unit },
                    Time64Type t => new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Time, physical) { Unit = t.Unit },
                    TimestampType t => new(leaf.LeafIndex, DomainKind.Integer,
                        string.IsNullOrEmpty(t.Timezone) ? LiteralShape.Timestamp : LiteralShape.TimestampUtc, physical) { Unit = t.Unit },
                    _ => null,
                },
            (ValueKind.DecimalFromInt32 or ValueKind.DecimalFromInt64, _) =>
                new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Decimal, physical) { Scale = type.Scale },
            (ValueKind.DecimalFromBytes, _) =>
                new(leaf.LeafIndex, DomainKind.Integer, LiteralShape.Decimal, physical) { Scale = type.Scale, BigEndianDecimal = true },
            (ValueKind.Float or ValueKind.Double, _) => new(leaf.LeafIndex, DomainKind.Floating, LiteralShape.Floating, physical),
            (ValueKind.Float16, _) => new(leaf.LeafIndex, DomainKind.Floating, LiteralShape.Floating, physical) { Half16 = true },
            (ValueKind.Utf8, _) => new(leaf.LeafIndex, DomainKind.Bytes, LiteralShape.String, physical),
            _ => null,
        };
        if (domain is null) return null;
        domain = new LeafDomain(domain.Leaf, domain.Kind, domain.Shape, physical)
        {
            Unsigned = domain.Unsigned, Scale = domain.Scale, Unit = domain.Unit, Half16 = domain.Half16,
            BigEndianDecimal = domain.BigEndianDecimal, TypeLength = leaf.Element.TypeLength ?? 0,
        };

        // Statistics rules (probed against pyarrow 26 / Arrow C++, see the pushdown notes): with column orders, only
        // min_value / max_value and only under an order that fits the type; without, only the deprecated min / max,
        // and only for signed fixed-width physical types (their old byte order was signed comparison).
        if (orders is not null)
        {
            if (orders.Length == leafCount)
            {
                ColumnOrder order = orders[leaf.LeafIndex];
                domain.TypedStatistics = order == ColumnOrder.TypeDefinedOrder
                                         || order == ColumnOrder.Ieee754TotalOrder && domain.Kind == DomainKind.Floating;
            }
        }
        else
        {
            domain.LegacyStatistics = physical is ParquetType.Int32 or ParquetType.Int64 or ParquetType.Float or ParquetType.Double
                                      && !domain.Unsigned;
        }
        return domain;
    }

    // ------------------------------------------------------------------
    // Comparing a unit's values with a literal
    // ------------------------------------------------------------------

    /// <summary>Whether a value of the unit may be NaN.</summary>
    public bool NaNPossible(in LeafStatistics s) => Kind == DomainKind.Floating && (s.NanCount is not long n || n > 0);

    /// <summary>The signs <c>value - literal</c> may take over the (non-null) values of the unit.</summary>
    /// <param name="s">The unit's statistics of the leaf.</param>
    /// <param name="literal">The literal, in the stored representation.</param>
    /// <param name="excludeNaN">NaN rows need not be considered (a guard in the enclosing conjunction excludes them).</param>
    public Signs PossibleSigns(in LeafStatistics s, in Bound literal, bool excludeNaN)
    {
        int lo = -1, hi = 1; // without bounds: anything
        if (s.Min is { } min && TryCompare(min, literal, out int c)) lo = c;
        if (s.Max is { } max && TryCompare(max, literal, out int d)) hi = d;
        Signs signs = lo > hi
            ? Signs.Any // min above max: damaged statistics, so assume nothing
            : (lo <= -1 && hi >= -1 ? Signs.Less : 0) | (lo <= 0 && hi >= 0 ? Signs.Equal : 0) | (lo <= 1 && hi >= 1 ? Signs.Greater : 0);
        if (Kind == DomainKind.Integer && literal.Half) signs &= ~Signs.Equal;
        // NaN is not in the bounds; it is greater than every number and equal to NaN.
        if (!excludeNaN && NaNPossible(s)) signs |= double.IsNaN(literal.Float) ? Signs.Equal : Signs.Greater;
        return signs;
    }

    /// <summary>
    /// Whether a (non-null) value of the unit may start with <paramref name="prefix"/>, and whether one may not.
    /// Values starting with it lie in [prefix, successor) where the successor increments the last byte below 0xFF;
    /// when both bounds start with it, every value between them does.
    /// </summary>
    public (bool Some, bool Other) PrefixPossible(in LeafStatistics s, byte[] prefix)
    {
        byte[]? min = s.Min, max = s.Max;
        if (min is not null && max is not null && min.AsSpan().SequenceCompareTo(max) > 0) return (true, true); // damaged
        bool some = !(max is not null && max.AsSpan().SequenceCompareTo(prefix) < 0)
                    && !(min is not null && Successor(prefix) is { } upper && min.AsSpan().SequenceCompareTo(upper) >= 0);
        bool other = !(min is not null && max is not null && min.AsSpan().StartsWith(prefix) && max.AsSpan().StartsWith(prefix));
        return (some, other);

        static byte[]? Successor(byte[] prefix)
        {
            int end = prefix.Length;
            while (end > 0 && prefix[end - 1] == 0xFF) end--;
            if (end == 0) return null; // no string above all those with the prefix
            byte[] next = prefix[..end];
            next[end - 1]++;
            return next;
        }
    }

    /// <summary>
    /// The PLAIN bytes a value equal to <paramref name="literal"/> is stored as, the keys of a Bloom filter (both zeros
    /// for a floating zero: the IR's equality does not tell them apart, their bytes differ). Empty when this leaf's
    /// values are not looked up that way (booleans, half floats, decimals in variable-length bytes, NaN, a literal no
    /// stored value can equal exactly).
    /// </summary>
    public byte[][] BloomKeys(in Bound literal)
    {
        switch (Kind)
        {
            case DomainKind.Bytes:
                return [literal.Bytes!];
            case DomainKind.Floating when !Half16 && !double.IsNaN(literal.Float):
            {
                double v = literal.Float;
                if (Physical == ParquetType.Double) return v == 0 ? [Bytes(0.0), Bytes(-0.0)] : [Bytes(v)];
                float f = (float)v;
                if ((double)f != v) return [];
                return f == 0 ? [Bytes(0f), Bytes(-0f)] : [Bytes(f)];
            }
            case DomainKind.Integer when !literal.Half:
            {
                Int128 v = literal.Integer;
                if (BigEndianDecimal)
                {
                    if (Physical != ParquetType.FixedLenByteArray || TypeLength is <= 0 or > 16) return [];
                    Span<byte> full = stackalloc byte[16];
                    BinaryPrimitives.WriteInt128BigEndian(full, v);
                    // The value must fit the width: the bytes dropped are sign extension.
                    byte sign = (byte)(v < 0 ? 0xFF : 0);
                    foreach (byte b in full[..(16 - TypeLength)]) if (b != sign) return [];
                    if (TypeLength < 16 && ((full[16 - TypeLength] & 0x80) != 0) != (v < 0)) return [];
                    return [full[(16 - TypeLength)..].ToArray()];
                }
                bool fits = (Physical, Unsigned) switch
                {
                    (ParquetType.Int32, false) => v >= int.MinValue && v <= int.MaxValue,
                    (ParquetType.Int32, true) => v >= 0 && v <= uint.MaxValue,
                    (ParquetType.Int64, false) => v >= long.MinValue && v <= long.MaxValue,
                    (ParquetType.Int64, true) => v >= 0 && v <= ulong.MaxValue,
                    _ => false,
                };
                if (!fits) return [];
                // Unsigned values are stored as the same bits.
                return Physical == ParquetType.Int32
                    ? [Little(Unsigned ? unchecked((int)(uint)v) : (int)v)]
                    : [Little(Unsigned ? unchecked((long)(ulong)v) : (long)v)];
            }
            default:
                return [];
        }

        static byte[] Bytes<T>(T value) where T : unmanaged
        {
            byte[] b = new byte[System.Runtime.CompilerServices.Unsafe.SizeOf<T>()];
            System.Runtime.InteropServices.MemoryMarshal.Write(b, in value);
            return b;
        }

        static byte[] Little<T>(T value) where T : unmanaged => BitConverter.IsLittleEndian ? Bytes(value) : throw new PlatformNotSupportedException();
    }

    /// <summary>The sign of <c>stored - literal</c>; false when the stored bound does not decode (then it is not used).</summary>
    private bool TryCompare(byte[] stored, in Bound literal, out int sign)
    {
        sign = 0;
        switch (Kind)
        {
            case DomainKind.Integer:
            {
                if (!TryDecodeInteger(stored, out Int128 v)) return false;
                sign = v < literal.Integer ? -1 : v > literal.Integer ? 1 : literal.Half ? -1 : 0;
                return true;
            }
            case DomainKind.Floating:
            {
                if (!TryDecodeFloat(stored, out double v) || double.IsNaN(v)) return false;
                sign = double.IsNaN(literal.Float) ? -1 : v < literal.Float ? -1 : v > literal.Float ? 1 : 0;
                return true;
            }
            default:
                sign = Math.Sign(stored.AsSpan().SequenceCompareTo(literal.Bytes));
                return true;
        }
    }

    private bool TryDecodeInteger(ReadOnlySpan<byte> b, out Int128 value)
    {
        value = default;
        if (BigEndianDecimal) return TryBigEndian(b, out value);
        switch (Physical)
        {
            case ParquetType.Boolean when b.Length == 1:
                if (b[0] > 1) return false;
                value = b[0];
                return true;
            case ParquetType.Int32 when b.Length == 4:
                value = Unsigned ? BinaryPrimitives.ReadUInt32LittleEndian(b) : BinaryPrimitives.ReadInt32LittleEndian(b);
                return true;
            case ParquetType.Int64 when b.Length == 8:
                value = Unsigned ? BinaryPrimitives.ReadUInt64LittleEndian(b) : BinaryPrimitives.ReadInt64LittleEndian(b);
                return true;
            default:
                return false;
        }

        static bool TryBigEndian(ReadOnlySpan<byte> b, out Int128 value)
        {
            value = default;
            if (b.IsEmpty) return false;
            int sign = (sbyte)b[0] < 0 ? 0xFF : 0;
            // Leading bytes beyond 16 must be sign extension.
            while (b.Length > 16)
            {
                if (b[0] != sign) return false;
                b = b[1..];
            }
            if (b.Length == 16 && ((sbyte)b[0] < 0 ? 0xFF : 0) != sign) return false;
            Span<byte> full = stackalloc byte[16];
            full.Fill((byte)sign);
            b.CopyTo(full[(16 - b.Length)..]);
            value = BinaryPrimitives.ReadInt128BigEndian(full);
            return true;
        }
    }

    private bool TryDecodeFloat(ReadOnlySpan<byte> b, out double value)
    {
        value = 0;
        if (Half16)
        {
            if (b.Length != 2) return false;
            value = (double)BinaryPrimitives.ReadHalfLittleEndian(b);
            return true;
        }
        switch (Physical)
        {
            case ParquetType.Float when b.Length == 4:
                value = BinaryPrimitives.ReadSingleLittleEndian(b);
                return true;
            case ParquetType.Double when b.Length == 8:
                value = BinaryPrimitives.ReadDoubleLittleEndian(b);
                return true;
            default:
                return false;
        }
    }

    // ------------------------------------------------------------------
    // Literals into the stored representation
    // ------------------------------------------------------------------

    /// <summary>
    /// The literal in this leaf's stored representation (Samidare's <see cref="LiteralConversion"/>, the rules every
    /// reader shares); false when it does not convert (the unit is then kept).
    /// </summary>
    public bool TryConvert(LiteralValue literal, out Bound bound)
    {
        bound = default;
        ValueDomain domain = Shape switch
        {
            // A double holds every 32-bit value; 64-bit ones compare with floating literals as doubles (rounded).
            LiteralShape.Integer => Physical == ParquetType.Int64 ? ValueDomain.WideInteger : ValueDomain.Integer,
            LiteralShape.Decimal => ValueDomain.NewDecimal(Scale),
            LiteralShape.Boolean => ValueDomain.Boolean,
            LiteralShape.Date => ValueDomain.Date,
            LiteralShape.Time => ValueDomain.NewTime(Unit),
            LiteralShape.Timestamp => ValueDomain.NewTimestamp(Unit),
            LiteralShape.TimestampUtc => ValueDomain.NewTimestampUtc(Unit),
            LiteralShape.Floating => ValueDomain.Floating,
            _ => ValueDomain.String,
        };
        if (LiteralConversion.tryConvert(domain, literal) is not { } stored) return false;
        bound = stored.Value switch
        {
            StoredLiteral.Integer i => new Bound(Integer: i.value, Half: i.half),
            StoredLiteral.Floating f => new Bound(Float: f.value),
            StoredLiteral.Bytes b => new Bound(Bytes: b.value),
            _ => throw new InvalidOperationException("Unknown stored literal."),
        };
        return true;
    }
}
