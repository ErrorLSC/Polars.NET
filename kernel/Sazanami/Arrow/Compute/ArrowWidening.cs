using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.TypeSystem;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>What conversions <see cref="ArrowWidening"/> may compile besides the lossless ones.</summary>
/// <param name="Lossy">
/// Also conversions that approximate values without failing: integers and decimals into floats that cannot hold them
/// exactly (rounded to the nearest), floats into narrower floats, times, timestamps and durations into a coarser unit
/// (rounded down). Never narrower integers, other meanings (wall-clock times and instants, strings and numbers,
/// timestamps and dates) nor decimals that lose digits.
/// </param>
/// <param name="InsertMissingFields">Whether struct fields of the target that the source lacks become nulls (else a refusal).</param>
internal readonly record struct ArrowConversionRules(bool Lossy = false, bool InsertMissingFields = false);

/// <summary>Why a conversion was refused.</summary>
internal enum ArrowConversionProblem
{
    /// <summary>The types hold different things, or the target cannot hold every value even approximately.</summary>
    Incompatible,

    /// <summary>The conversion would approximate values: <see cref="ArrowConversionRules.Lossy"/> allows it.</summary>
    Lossy,

    /// <summary>A struct field of the target is missing: <see cref="ArrowConversionRules.InsertMissingFields"/> allows it.</summary>
    MissingField,
}

/// <summary>A refused conversion: the field it is about (from the root, <c>s.b</c>, <c>l[]</c>, <c>m{key}</c>), and why.</summary>
internal sealed record ArrowConversionError(string Path, string Message, ArrowConversionProblem Problem);

/// <summary>
/// Conversions between Arrow types as datasets need them (files written over time with different types for one
/// column): lossless widenings — every value of the source is a value of the target — and, when asked, lossy ones
/// (<see cref="ArrowConversionRules.Lossy"/>). Struct fields match by name (order free, extra source fields left out,
/// missing ones null when asked); lists, fixed-size lists and maps convert their elements; strings and binaries
/// convert between the plain and the view layout, dictionaries of them to and from their values.
/// </summary>
/// <remarks>
/// Lossless: integers into wider integers (unsigned into wider signed too); integers into floats whose mantissa holds
/// them (8 bits into half, 16 into float, 32 into double); floats into wider floats; decimals into decimals of no
/// fewer integer digits and no smaller scale (of any width, 32 to 256 bits); integers into decimals of enough integer digits (3, 5, 10, 19; 20 for
/// unsigned 64-bit); decimals of scale 0 into floats holding their digits; dates into wall-clock timestamps; times,
/// timestamps and durations into finer units (values beyond the target's range fail when converted); timestamps of
/// one zone into another (the same instants); nulls (the null type) into anything. Wall-clock timestamps and instants
/// never convert into each other.
/// </remarks>
internal static class ArrowWidening
{
    /// <summary>The conversion from <paramref name="from"/> to <paramref name="to"/>, or the reason there is none.</summary>
    /// <param name="root">The path of the converted value in messages (a column's name).</param>
    public static bool TryCompile(IArrowType from, IArrowType to, ArrowConversionRules rules, string root,
        [NotNullWhen(true)] out ArrowConversion? conversion, [NotNullWhen(false)] out ArrowConversionError? error)
    {
        ArgumentNullException.ThrowIfNull(from);
        ArgumentNullException.ThrowIfNull(to);
        error = null;
        conversion = Compile(from, to, rules, root, ref error);
        return conversion is not null;
    }

    /// <summary>Whether <paramref name="from"/> converts to <paramref name="to"/> under the rules.</summary>
    public static bool CanConvert(IArrowType from, IArrowType to, ArrowConversionRules rules) => TryCompile(from, to, rules, "", out _, out _);

    /// <summary>
    /// The narrowest type both types convert to (struct fields united: <paramref name="a"/>'s first, then those only
    /// <paramref name="b"/> has; a list's element and a map's entries as <paramref name="a"/> names them); null when
    /// there is none.
    /// </summary>
    public static IArrowType? Common(IArrowType a, IArrowType b, bool lossy)
    {
        ArgumentNullException.ThrowIfNull(a);
        ArgumentNullException.ThrowIfNull(b);
        if (ArrowTypes.SameType(a, b)) return a;
        return (a, b) switch
        {
            (NullType, _) => b,
            (_, NullType) => a,
            (DictionaryType x, DictionaryType y) => Common(x.ValueType, y.ValueType, lossy),
            (DictionaryType x, _) => Common(x.ValueType, b, lossy),
            (_, DictionaryType y) => Common(a, y.ValueType, lossy),
            (IntegerType x, IntegerType y) => CommonInteger(x, y, lossy),
            (IntegerType x, FloatingPointType y) => CommonFloat(a, y, lossy),
            (FloatingPointType x, IntegerType y) => CommonFloat(b, x, lossy),
            (FloatingPointType x, FloatingPointType y) => x.Precision >= y.Precision ? a : b,
            (IntegerType or Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type,
                IntegerType or Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type) => CommonDecimal(a, b),
            (Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type, FloatingPointType y) => CommonFloat(a, y, lossy),
            (FloatingPointType x, Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type) => CommonFloat(b, x, lossy),
            (Date32Type, TimestampType { Timezone: null or "" }) => b,
            (TimestampType { Timezone: null or "" }, Date32Type) => a,
            (TimestampType x, TimestampType y) when Zoned(x) == Zoned(y) => Rank(y.Unit) > Rank(x.Unit) ? new TimestampType(y.Unit, x.Timezone) : a,
            (Time32Type or Time64Type, Time32Type or Time64Type) => Rank(Unit(b)) > Rank(Unit(a)) ? b : a,
            (DurationType x, DurationType y) => Rank(y.Unit) > Rank(x.Unit) ? b : a,
            // Strings (binaries) in two layouts: the view layout when either is (a reader's dictionaries hold plain values),
            // else the large one when either is (it holds more bytes per batch).
            (StringType or LargeStringType or StringViewType, StringType or LargeStringType or StringViewType) =>
                a is StringViewType || b is not StringViewType && a is LargeStringType ? a : b,
            (BinaryType or LargeBinaryType or BinaryViewType, BinaryType or LargeBinaryType or BinaryViewType) =>
                a is BinaryViewType || b is not BinaryViewType && a is LargeBinaryType ? a : b,
            (StructType x, StructType y) => CommonStruct(x, y, lossy),
            // Lists in several layouts: the view layout when either is, else the large one when either is.
            (ListType or LargeListType or ListViewType or LargeListViewType, ListType or LargeListType or ListViewType or LargeListViewType) =>
                Common(ListElement(a), ListElement(b), lossy) is { } element
                    ? WithElement(a is ListViewType or LargeListViewType || b is not (ListViewType or LargeListViewType) && a is LargeListType ? a : b, a, element)
                    : null,
            (FixedSizeListType x, FixedSizeListType y) when x.ListSize == y.ListSize => Common(x.ValueDataType, y.ValueDataType, lossy) is { } element
                ? new FixedSizeListType(new Field(x.ValueField.Name, element, nullable: true), x.ListSize) : null,
            (MapType x, MapType y) => Common(x.KeyField.DataType, y.KeyField.DataType, lossy) is { } key && Common(x.ValueField.DataType, y.ValueField.DataType, lossy) is { } value
                ? new MapType(new Field(x.KeyField.Name, key, nullable: false), new Field(x.ValueField.Name, value, nullable: true), x.KeySorted && y.KeySorted)
                : null,
            _ => null,
        };
    }

    // ---- compiling ------------------------------------------------------------------------------------------------

    private static ArrowConversion? Compile(IArrowType from, IArrowType to, ArrowConversionRules rules, string path, ref ArrowConversionError? error)
    {
        if (ArrowTypes.SameType(from, to)) return new IdentityConversion(to);
        switch (from, to)
        {
            case (NullType, _):
                return new NullsConversion(to);
            case (DictionaryType f, DictionaryType t):
            {
                // Indices into wider ones, the dictionary's values converted (shared when they stay).
                // Into wider indices, or into any of 32 bits or more: a valid index is below an array's length (2^31).
                if (!ArrowTypes.SameType(f.IndexType, t.IndexType) && !IntegerWidens((IntegerType)f.IndexType, (IntegerType)t.IndexType)
                    && ((IntegerType)t.IndexType).BitWidth < 32)
                    return Refuse(ArrowConversionProblem.Incompatible, ref error);
                return Compile(f.ValueType, t.ValueType, rules, path, ref error) is { } values ? new DictionaryIndexConversion(t, values) : null;
            }
            case (DictionaryType d, _):
                return Compile(d.ValueType, to, rules, path, ref error) is { } decoded ? new DictionaryDecodeConversion(to, decoded) : null;
            case (_, DictionaryType d):
                return Compile(from, d.ValueType, rules, path, ref error) is { } encoded ? new DictionaryEncodeConversion(d, encoded) : null;
            case (_, _) when SameFamily(from, to):
                return new RelayoutConversion(to);
            case (IntegerType f, IntegerType t):
                if (IntegerWidens(f, t)) return NumericConversion.Create(from, to);
                return Refuse(ArrowConversionProblem.Incompatible, ref error);
            case (IntegerType f, FloatingPointType t):
                if (MagnitudeBits(f) <= MantissaBits(t)) return NumericConversion.Create(from, to);
                return rules.Lossy ? NumericConversion.Create(from, to) : Refuse(ArrowConversionProblem.Lossy, ref error);
            case (FloatingPointType f, FloatingPointType t):
                if (t.Precision > f.Precision) return NumericConversion.Create(from, to);
                return rules.Lossy ? NumericConversion.Create(from, to) : Refuse(ArrowConversionProblem.Lossy, ref error);
            case (IntegerType f, Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type):
            {
                var (p, s) = ArrowTypes.DecimalParameters(to);
                if (Digits(f) <= p - s) return new DecimalConversion(from, to);
                return Refuse(ArrowConversionProblem.Incompatible, ref error);
            }
            case (Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type, Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type):
            {
                var (p1, s1) = ArrowTypes.DecimalParameters(from);
                var (p2, s2) = ArrowTypes.DecimalParameters(to);
                if (s2 >= s1 && p2 - s2 >= p1 - s1) return new DecimalConversion(from, to);
                return Refuse(ArrowConversionProblem.Incompatible, ref error);
            }
            case (Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type, FloatingPointType t):
            {
                var (p, s) = ArrowTypes.DecimalParameters(from);
                if (s == 0 && p <= FloatDigits(t)) return new DecimalToFloatConversion(from, to);
                return rules.Lossy ? new DecimalToFloatConversion(from, to) : Refuse(ArrowConversionProblem.Lossy, ref error);
            }
            case (Date32Type, TimestampType t) when !Zoned(t):
                return new ScaleConversion(to, sourceWidth: 4, targetWidth: 8, multiply: 86_400 * Pow10(3 * Rank(t.Unit)), divide: 1);
            case (TimestampType f, TimestampType t) when Zoned(f) == Zoned(t):
                return Units(Rank(f.Unit), Rank(t.Unit), 8, 8, ref error);
            case (Time32Type or Time64Type, Time32Type or Time64Type):
                return Units(Rank(Unit(from)), Rank(Unit(to)), from is Time32Type ? 4 : 8, to is Time32Type ? 4 : 8, ref error);
            case (DurationType f, DurationType t):
                return Units(Rank(f.Unit), Rank(t.Unit), 8, 8, ref error);
            case (StructType f, StructType t):
                return Struct(f, t, rules, path, ref error);
            case (ListType or LargeListType or ListViewType or LargeListViewType, ListType or LargeListType or ListViewType or LargeListViewType):
                return Compile(ListElement(from), ListElement(to), rules, path + "[]", ref error) is { } element ? new ListConversion(to, element) : null;
            case (FixedSizeListType f, FixedSizeListType t) when f.ListSize == t.ListSize:
                return Compile(f.ValueDataType, t.ValueDataType, rules, path + "[]", ref error) is { } items ? new FixedSizeListConversion(t, items) : null;
            case (MapType f, MapType t):
            {
                if (Compile(f.KeyField.DataType, t.KeyField.DataType, rules, path + "{key}", ref error) is not { } key) return null;
                if (Compile(f.ValueField.DataType, t.ValueField.DataType, rules, path + "{value}", ref error) is not { } value) return null;
                return new ListConversion(to, new StructConversion(t.KeyValueType, [(0, key), (1, value)]));
            }
            default:
                return Refuse(ArrowConversionProblem.Incompatible, ref error);
        }

        ArrowConversion? Units(int fromRank, int toRank, int sourceWidth, int targetWidth, ref ArrowConversionError? error)
        {
            if (toRank >= fromRank) return new ScaleConversion(to, sourceWidth, targetWidth, Pow10(3 * (toRank - fromRank)), 1);
            return rules.Lossy ? new ScaleConversion(to, sourceWidth, targetWidth, 1, Pow10(3 * (fromRank - toRank))) : Refuse(ArrowConversionProblem.Lossy, ref error);
        }

        ArrowConversion? Refuse(ArrowConversionProblem problem, ref ArrowConversionError? error)
        {
            string message = problem == ArrowConversionProblem.Lossy
                ? $"{ArrowTypes.Describe(from)} is read as {ArrowTypes.Describe(to)} only by a lossy conversion"
                : $"{ArrowTypes.Describe(from)} cannot be read as {ArrowTypes.Describe(to)}";
            error = new ArrowConversionError(path, message, problem);
            return null;
        }
    }

    private static StructConversion? Struct(StructType from, StructType to, ArrowConversionRules rules, string path, ref ArrowConversionError? error)
    {
        var fields = new (int Source, ArrowConversion Conversion)[to.Fields.Count];
        for (int i = 0; i < fields.Length; i++)
        {
            Field target = to.Fields[i];
            string at = path.Length == 0 ? target.Name : $"{path}.{target.Name}";
            int source = from.GetFieldIndex(target.Name, StringComparer.Ordinal);
            if (source < 0)
            {
                if (!rules.InsertMissingFields)
                {
                    error = new ArrowConversionError(at, "the field is missing", ArrowConversionProblem.MissingField);
                    return null;
                }
                fields[i] = (-1, new NullsConversion(target.DataType));
                continue;
            }
            if (Compile(from.Fields[source].DataType, target.DataType, rules, at, ref error) is not { } conversion) return null;
            fields[i] = (source, conversion);
        }
        return new StructConversion(to, fields);
    }

    // ---- common types ---------------------------------------------------------------------------------------------

    internal static IArrowType? CommonInteger(IntegerType a, IntegerType b, bool lossy)
    {
        if (a.IsSigned == b.IsSigned) return a.BitWidth >= b.BitWidth ? a : b;
        int signed = a.IsSigned ? a.BitWidth : b.BitWidth, unsigned = a.IsSigned ? b.BitWidth : a.BitWidth;
        int bits = Math.Max(signed, 2 * unsigned);
        return bits switch
        {
            8 => Int8Type.Default,
            16 => Int16Type.Default,
            32 => Int32Type.Default,
            64 => Int64Type.Default,
            _ => lossy ? DoubleType.Default : null,
        };
    }

    /// <summary>The narrowest float, from <paramref name="floating"/>'s precision up, holding every value of <paramref name="other"/>.</summary>
    private static IArrowType? CommonFloat(IArrowType other, FloatingPointType floating, bool lossy)
    {
        foreach (FloatingPointType candidate in (FloatingPointType[])[HalfFloatType.Default, FloatType.Default, DoubleType.Default])
        {
            if (candidate.Precision >= floating.Precision && CanConvert(other, candidate, default)) return candidate;
        }
        return lossy ? DoubleType.Default : null;
    }

    /// <summary>
    /// The decimal of both scales and both integer digits, as wide as the wider decimal (integers count as 32 bits) or
    /// wider when its precision needs it (Arrow C++'s rule): decimal32(9, 2) and decimal32(9, 3) unite into
    /// decimal64(10, 3).
    /// </summary>
    private static IArrowType? CommonDecimal(IArrowType a, IArrowType b)
    {
        var (pa, sa, ba) = DecimalOrInteger(a);
        var (pb, sb, bb) = DecimalOrInteger(b);
        int scale = Math.Max(sa, sb), precision = Math.Max(pa - sa, pb - sb) + scale;
        return (Math.Max(ba, bb), precision) switch
        {
            (32, <= 9) => new Decimal32Type(precision, scale),
            (<= 64, <= 18) => new Decimal64Type(precision, scale),
            (<= 128, <= 38) => new Decimal128Type(precision, scale),
            (_, <= 76) => new Decimal256Type(precision, scale),
            _ => null,
        };

        static (int Precision, int Scale, int Bits) DecimalOrInteger(IArrowType type) => type switch
        {
            IntegerType i => (Digits(i), 0, 32),
            Decimal32Type d => (d.Precision, d.Scale, 32),
            Decimal64Type d => (d.Precision, d.Scale, 64),
            Decimal128Type d => (d.Precision, d.Scale, 128),
            Decimal256Type d => (d.Precision, d.Scale, 256),
            _ => throw new ArgumentException($"{type.Name} is no integer or decimal type.", nameof(type)),
        };
    }

    private static StructType? CommonStruct(StructType a, StructType b, bool lossy)
    {
        List<Field> fields = [];
        foreach (Field field in a.Fields)
        {
            int other = b.GetFieldIndex(field.Name, StringComparer.Ordinal);
            IArrowType? type = other < 0 ? field.DataType : Common(field.DataType, b.Fields[other].DataType, lossy);
            if (type is null) return null;
            fields.Add(new Field(field.Name, type, nullable: true, field.Metadata));
        }
        foreach (Field field in b.Fields)
        {
            if (a.GetFieldIndex(field.Name, StringComparer.Ordinal) < 0) fields.Add(new Field(field.Name, field.DataType, nullable: true, field.Metadata));
        }
        return new StructType(fields);
    }

    // ---- type facts -----------------------------------------------------------------------------------------------

    /// <summary>The element type of a list of any layout.</summary>
    internal static IArrowType ListElement(IArrowType list) => list switch
    {
        ListType l => l.ValueDataType,
        LargeListType l => l.ValueDataType,
        ListViewType l => l.ValueDataType,
        LargeListViewType l => l.ValueDataType,
        _ => throw new ArgumentException($"{list.Name} is no list."),
    };

    /// <summary>A list in the layout of <paramref name="layout"/> of <paramref name="element"/>, its item named as <paramref name="named"/>'s.</summary>
    internal static IArrowType WithElement(IArrowType layout, IArrowType named, IArrowType element)
    {
        Field? field = named switch
        {
            ListType l => l.ValueField,
            LargeListType l => l.ValueField,
            ListViewType l => l.ValueField,
            LargeListViewType l => l.ValueField,
            _ => null,
        };
        var item = new Field(field?.Name ?? "item", element, nullable: true, field?.Metadata); // its metadata kept (an engine's Enum declaration)
        return layout switch
        {
            LargeListType => new LargeListType(item),
            ListViewType => new ListViewType(item),
            LargeListViewType => new LargeListViewType(item),
            _ => new ListType(item),
        };
    }

    /// <summary>Whether both are strings, or both binaries, in the plain or view layout.</summary>
    private static bool SameFamily(IArrowType a, IArrowType b) =>
        a is StringType or LargeStringType or StringViewType && b is StringType or LargeStringType or StringViewType
        || a is BinaryType or LargeBinaryType or BinaryViewType && b is BinaryType or LargeBinaryType or BinaryViewType;

    private static bool IntegerWidens(IntegerType from, IntegerType to) =>
        to.BitWidth > from.BitWidth && (to.IsSigned || !from.IsSigned);

    /// <summary>The bits of an integer's magnitude: every value is at most 2^bits in absolute value.</summary>
    private static int MagnitudeBits(IntegerType type) => type.IsSigned ? type.BitWidth - 1 : type.BitWidth;

    /// <summary>The bits of a float's significand (with the hidden bit): integers up to 2^bits are exact.</summary>
    private static int MantissaBits(FloatingPointType type) => type.Precision switch
    {
        FloatingPointType.PrecisionKind.Half => 11,
        FloatingPointType.PrecisionKind.Single => 24,
        _ => 53,
    };

    /// <summary>The decimal digits every integer up to the float's exact limit has (3 for half, 7 for float, 15 for double).</summary>
    private static int FloatDigits(FloatingPointType type) => type.Precision switch
    {
        FloatingPointType.PrecisionKind.Half => 3,
        FloatingPointType.PrecisionKind.Single => 7,
        _ => 15,
    };

    /// <summary>The decimal digits of an integer type's largest magnitude.</summary>
    private static int Digits(IntegerType type) => (type.BitWidth, type.IsSigned) switch
    {
        (8, _) => 3,
        (16, _) => 5,
        (32, _) => 10,
        (64, true) => 19,
        _ => 20,
    };

    private static bool Zoned(TimestampType type) => !string.IsNullOrEmpty(type.Timezone);

    private static TimeUnit Unit(IArrowType type) => type switch
    {
        Time32Type t => t.Unit,
        Time64Type t => t.Unit,
        _ => throw new ArgumentException(type.Name),
    };

    private static int Rank(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 0,
        TimeUnit.Millisecond => 1,
        TimeUnit.Microsecond => 2,
        _ => 3,
    };

    private static long Pow10(int exponent)
    {
        long p = 1;
        for (int i = 0; i < exponent; i++) p *= 10;
        return p;
    }
}
