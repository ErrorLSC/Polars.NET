using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>
/// Compiles encoders that write Arrow batches as a given Avro schema (the mirror of reading with a reader schema):
/// Avro fields find their Arrow columns by name or field id; an Avro field the batch lacks writes its declared
/// default (an error without one); an Arrow column the schema lacks is an error. Types follow the reader's rules
/// in reverse: lossless widening is free, lossy conversions are checked per value, different meanings (an instant
/// written as a wall-clock time, a date as a timestamp) are refused before anything is written. General unions are
/// not supported.
/// </summary>
internal static class AvroGivenSchema
{
    public static RowEncoder Build(Schema arrow, AvroSchema avro, AvroFieldMatching matching)
    {
        var compiler = new Compiler(matching);
        if (avro is AvroRecordSchema record)
        {
            var (encoders, sources) = compiler.Fields(arrow.FieldsList, record, "");
            return new RowEncoder(encoders, sources);
        }
        if (arrow.FieldsList.Count != 1)
            throw new ArgumentException($"A non-record writer schema writes one column, but the batches have {arrow.FieldsList.Count}.");
        return new RowEncoder([compiler.Field(arrow.FieldsList[0], avro, arrow.FieldsList[0].Name)]);
    }

    private sealed class Compiler(AvroFieldMatching matching)
    {
        // ---- records --------------------------------------------------------------------------------------------

        public (AvroEncoder[] Encoders, int[] Sources) Fields(IReadOnlyList<Field> arrowFields, AvroRecordSchema record, string path)
        {
            Dictionary<string, int> byKey = new(StringComparer.Ordinal);
            for (int i = 0; i < arrowFields.Count; i++)
            {
                string key = ArrowKey(arrowFields[i], path);
                if (!byKey.TryAdd(key, i)) throw new ArgumentException($"Two columns of '{Display(path)}' match the writer schema as {Describe(key)}.");
            }
            var encoders = new AvroEncoder[record.Fields.Count];
            var sources = new int[record.Fields.Count];
            var used = new bool[arrowFields.Count];
            for (int j = 0; j < record.Fields.Count; j++)
            {
                AvroField field = record.Fields[j];
                string fieldPath = Join(path, field.Name);
                string key = AvroKey(field, fieldPath);
                if (byKey.TryGetValue(key, out int i))
                {
                    encoders[j] = Field(arrowFields[i], field.Schema, fieldPath);
                    sources[j] = i;
                    used[i] = true;
                }
                else if (field.Default is { } value)
                {
                    encoders[j] = new ConstantEncoder(AvroDefaults.Encode(field.Schema, value, fieldPath)); // validated
                    sources[j] = -1;
                }
                else
                {
                    throw new ArgumentException($"The writer schema field '{fieldPath}' has no column and no default.");
                }
            }
            for (int i = 0; i < arrowFields.Count; i++)
            {
                if (!used[i])
                    throw new ArgumentException($"The column '{Join(path, arrowFields[i].Name)}' is not in the writer schema; it would be dropped.");
            }
            return (encoders, sources);
        }

        private string ArrowKey(Field f, string path) => matching == AvroFieldMatching.ByName
            ? f.Name
            : ArrowFieldIds.Of(f)?.ToString(System.Globalization.CultureInfo.InvariantCulture)
                ?? throw new ArgumentException($"The column '{Join(path, f.Name)}' has no field id to match by.");

        private string AvroKey(AvroField f, string path) => matching == AvroFieldMatching.ByName
            ? f.Name
            : f.FieldId?.ToString(System.Globalization.CultureInfo.InvariantCulture)
                ?? throw new ArgumentException($"The writer schema field '{path}' has no field-id to match by.");

        private string Describe(string key) => matching == AvroFieldMatching.ByName ? $"'{key}'" : $"id {key}";

        // ---- nullability ------------------------------------------------------------------------------------------

        /// <summary>A column (with its nullability) written as an Avro type.</summary>
        public AvroEncoder Field(Field f, AvroSchema avro, string path)
        {
            if (avro is AvroUnionSchema union)
            {
                List<int> values = [.. Enumerable.Range(0, union.Branches.Count).Where(i => i != union.NullIndex)];
                if (values.Count > 1)
                    throw new NotSupportedException($"'{path}': writing general unions ({union.Branches.Count} branches) is not supported; only [\"null\", T] and [T, \"null\"].");
                if (values.Count == 0)
                {
                    return f.DataType is NullType
                        ? new BranchEncoder(union.NullIndex, new NullEncoder())
                        : throw Mismatch(path, f.DataType, union);
                }
                AvroEncoder inner = Type(f.DataType, union.Branches[values[0]], path);
                return union.NullIndex >= 0
                    ? new NullableEncoder(inner, union.NullIndex, values[0])
                    : new RequiredEncoder(new BranchEncoder(values[0], inner), $"column '{path}'");
            }
            AvroEncoder encoder = Type(f.DataType, avro, path);
            return avro.Type == AvroType.Null || !f.IsNullable ? encoder : new RequiredEncoder(encoder, $"column '{path}'");
        }

        // ---- types ----------------------------------------------------------------------------------------------

        private AvroEncoder Type(IArrowType t, AvroSchema a, string path)
        {
            // Dictionaries write their values (an enum looks the value up among its symbols).
            if (t is DictionaryType d) return Dictionary(d, Type(d.ValueType, a, path), path);
            return a switch
            {
                AvroRecordSchema r when t is StructType s => Record(s, r, path),
                AvroEnumSchema e when t is StringType or LargeStringType or StringViewType => new EnumEncoder(e.Symbols, path),
                AvroFixedSchema f => Fixed(t, f, path),
                AvroArraySchema arr => Array(t, arr, path),
                AvroMapSchema m when t is MapType mt && IsString(mt.KeyField.DataType) =>
                    new MapEncoder(Type(mt.KeyField.DataType, AvroSchema.String, path + ".key"), Field(mt.ValueField, m.Values, path + ".value")),
                AvroPrimitiveSchema p => Primitive(t, p, path),
                _ => throw Mismatch(path, t, a),
            };
        }

        private AvroEncoder Record(StructType s, AvroRecordSchema r, string path)
        {
            var (encoders, sources) = Fields(s.Fields, r, path);
            return new MappedStructEncoder(encoders, sources);
        }

        private AvroEncoder Array(IArrowType t, AvroArraySchema a, string path)
        {
            // An Arrow map written as an array of two-field records (Iceberg's and DuckDB's maps): key and value by
            // position, the key required.
            if (t is MapType m)
            {
                if (a.Items is not AvroRecordSchema { Fields.Count: 2 } entries) throw Mismatch(path, t, a);
                var key = Field(m.KeyField, entries.Fields[0].Schema, path + ".key");
                var value = Field(m.ValueField, entries.Fields[1].Schema, path + ".value");
                return new ListEncoder(ListLayout.Int32Offsets, 0, new StructEncoder([key, value]));
            }
            return t switch
            {
                ListType l => new ListEncoder(ListLayout.Int32Offsets, 0, Field(l.ValueField, a.Items, path + ".item")),
                LargeListType l => new ListEncoder(ListLayout.Int64Offsets, 0, Field(l.ValueField, a.Items, path + ".item")),
                FixedSizeListType l => new ListEncoder(ListLayout.FixedSize, l.ListSize, Field(l.ValueField, a.Items, path + ".item")),
                _ => throw Mismatch(path, t, a),
            };
        }

        private static AvroEncoder Fixed(IArrowType t, AvroFixedSchema f, string path) => (f.Logical, t) switch
        {
            (AvroLogicalKind.Decimal, _) when IsDecimal(t) => Decimal(t, f, path, f.Size),
            (AvroLogicalKind.Duration, IntervalType { Unit: IntervalUnit.DayTime }) => new DayTimeDurationEncoder(),
            (AvroLogicalKind.Duration, IntervalType { Unit: IntervalUnit.MonthDayNanosecond }) => new MonthDayNanoDurationEncoder(),
            (_, FixedSizeBinaryType b) when b.ByteWidth == f.Size && !IsDecimal(t) => new FixedEncoder(f.Size),
            (_, BinaryType or LargeBinaryType or BinaryViewType) => new FixedFromBinaryEncoder(f.Size, path),
            _ => throw Mismatch(path, t, f),
        };

        private static AvroEncoder Primitive(IArrowType t, AvroPrimitiveSchema p, string path) => p.Type switch
        {
            AvroType.Null => t is NullType ? new NullEncoder() : throw Mismatch(path, t, p),
            AvroType.Boolean => t is BooleanType ? new BooleanEncoder() : throw Mismatch(path, t, p),
            AvroType.Int or AvroType.Long => Integer(t, p, path),
            AvroType.Float or AvroType.Double => Floating(t, p, path),
            AvroType.Bytes when p.Logical == AvroLogicalKind.Decimal =>
                IsDecimal(t) ? Decimal(t, p, path, -1) : IsBinary(t) ? Binary(t) : throw Mismatch(path, t, p),
            AvroType.Bytes => IsBinary(t) || IsString(t) ? Binary(t)
                : t is FixedSizeBinaryType fb ? new FixedAsBytesEncoder(fb.ByteWidth)
                : IsDecimal(t) ? Decimal(t, AvroSchema.DecimalBytes(DecimalOf(t).Precision, DecimalOf(t).Scale), path, -1)
                : throw Mismatch(path, t, p),
            AvroType.String => IsString(t) ? Binary(t) : IsBinary(t) ? new Utf8CheckedEncoder(Binary(t), path) : throw Mismatch(path, t, p),
            _ => throw Mismatch(path, t, p),
        };

        // ---- integers and the logical types over them -----------------------------------------------------------

        /// <summary>
        /// Integers and integer-backed logical types. A plain Arrow integer may go to any int/long (the reader then
        /// gives it the logical meaning); an Arrow temporal type needs the same meaning (an instant stays an instant,
        /// a wall-clock time stays one; dates, times and timestamps never mix) and is rescaled between units, a
        /// rescaling that would drop digits being refused per value. A temporal Arrow value may also go to a plain
        /// int/long, as its stored number.
        /// </summary>
        private static AvroEncoder Integer(IArrowType t, AvroPrimitiveSchema p, string path)
        {
            bool toInt = p.Type == AvroType.Int;
            string what = $"column '{path}' ({t.Name})";
            long factorNs = (p.Logical, t) switch
            {
                (AvroLogicalKind.None, _) => 0,                                 // the stored number
                _ when IsPlainInteger(t) => 0,                                  // a plain number takes the reader's meaning
                (AvroLogicalKind.Date, Date32Type) => 0,
                (AvroLogicalKind.Date, Date64Type) => -86_400_000,              // ms → days
                (AvroLogicalKind.TimeMillis or AvroLogicalKind.TimeMicros, TimeType tt) => Ratio(tt.Unit, Unit(p.Logical)),
                (AvroLogicalKind.TimestampMillis or AvroLogicalKind.TimestampMicros or AvroLogicalKind.TimestampNanos
                    or AvroLogicalKind.LocalTimestampMillis or AvroLogicalKind.LocalTimestampMicros
                    or AvroLogicalKind.LocalTimestampNanos, TimestampType ts) when SameMeaning(ts, p) => Ratio(ts.Unit, Unit(p.Logical)),
                (AvroLogicalKind.SparkDayTimeInterval, DurationType du) => Ratio(du.Unit, TimeUnit.Microsecond),
                (AvroLogicalKind.SparkYearMonthInterval, IntervalType { Unit: IntervalUnit.YearMonth }) => 0,
                _ => throw Mismatch(path, t, p),
            };
            long multiply = factorNs > 1 ? factorNs : 1;
            long divide = factorNs < 0 ? -factorNs : 1;
            return t switch
            {
                Int8Type => new IntegerEncoder<sbyte>(multiply, divide, toInt, what),
                Int16Type => new IntegerEncoder<short>(multiply, divide, toInt, what),
                Int32Type or Date32Type or Time32Type or IntervalType { Unit: IntervalUnit.YearMonth } => new IntegerEncoder<int>(multiply, divide, toInt, what),
                Int64Type or Date64Type or Time64Type or TimestampType or DurationType => new IntegerEncoder<long>(multiply, divide, toInt, what),
                UInt8Type => new IntegerEncoder<byte>(multiply, divide, toInt, what),
                UInt16Type => new IntegerEncoder<ushort>(multiply, divide, toInt, what),
                UInt32Type => new IntegerEncoder<uint>(multiply, divide, toInt, what),
                UInt64Type => new IntegerEncoder<ulong>(multiply, divide, toInt, what),
                _ => throw Mismatch(path, t, p),
            };
        }

        // An instant (zoned Arrow timestamp) to timestamp-* without "adjust-to-utc": false; a wall-clock time
        // (zone-less) to local-timestamp-* or timestamp-* with "adjust-to-utc": false.
        private static bool SameMeaning(TimestampType ts, AvroSchema p)
        {
            bool wall = p.Logical is AvroLogicalKind.LocalTimestampMillis or AvroLogicalKind.LocalTimestampMicros
                or AvroLogicalKind.LocalTimestampNanos || p.AdjustToUtc == false;
            return string.IsNullOrEmpty(ts.Timezone) == wall;
        }

        private static TimeUnit Unit(AvroLogicalKind k) => k switch
        {
            AvroLogicalKind.TimeMillis or AvroLogicalKind.TimestampMillis or AvroLogicalKind.LocalTimestampMillis => TimeUnit.Millisecond,
            AvroLogicalKind.TimeMicros or AvroLogicalKind.TimestampMicros or AvroLogicalKind.LocalTimestampMicros => TimeUnit.Microsecond,
            _ => TimeUnit.Nanosecond,
        };

        /// <summary>The factor from one unit to another: positive multiplies, negative divides (1 or 0: none).</summary>
        private static long Ratio(TimeUnit from, TimeUnit to)
        {
            static long Nanos(TimeUnit u) => u switch
            {
                TimeUnit.Second => 1_000_000_000,
                TimeUnit.Millisecond => 1_000_000,
                TimeUnit.Microsecond => 1_000,
                _ => 1,
            };
            long f = Nanos(from), t = Nanos(to);
            return f >= t ? f / t : -(t / f);
        }

        // ---- floating point ---------------------------------------------------------------------------------------

        private static AvroEncoder Floating(IArrowType t, AvroPrimitiveSchema p, string path)
        {
            bool toDouble = p.Type == AvroType.Double;
            return t switch
            {
                FloatType => toDouble ? new WideningFloatEncoder() : new FloatEncoder(),
                DoubleType => toDouble ? new DoubleEncoder() : new NarrowingDoubleEncoder(path),
                HalfFloatType => toDouble ? new WideningHalfEncoder() : new HalfEncoder(),
                _ when IsPlainInteger(t) => new IntegerToFloatingEncoder(toDouble, path),
                _ => throw Mismatch(path, t, p),
            };
        }

        // ---- decimals -------------------------------------------------------------------------------------------

        /// <summary>An Arrow decimal to an Avro decimal of the same scale: bytes (shortest two's complement) or a
        /// fixed of the given size (sign-extended). A narrower precision, or a fixed too small for a value, is
        /// checked per value.</summary>
        private static AvroEncoder Decimal(IArrowType t, AvroSchema a, string path, int fixedSize)
        {
            var (precision, scale, width) = DecimalOf(t);
            if (a.Scale != scale)
                throw new ArgumentException($"'{path}': decimal({precision}, {scale}) cannot be written as decimal({a.Precision}, {a.Scale}): the scales differ.");
            int? maxDigits = a.Precision < precision ? a.Precision : null;
            return fixedSize < 0 && maxDigits is null
                ? new DecimalEncoder(width)
                : new CheckedDecimalEncoder(width, fixedSize, maxDigits, path);
        }

        private static (int Precision, int Scale, int Width) DecimalOf(IArrowType t) => t switch
        {
            Decimal32Type d => (d.Precision, d.Scale, 4),
            Decimal64Type d => (d.Precision, d.Scale, 8),
            Decimal128Type d => (d.Precision, d.Scale, 16),
            Decimal256Type d => (d.Precision, d.Scale, 32),
            _ => throw new InvalidOperationException($"{t.Name} is not a decimal."),
        };

        // ---- helpers ----------------------------------------------------------------------------------------------

        private static AvroEncoder Dictionary(DictionaryType d, AvroEncoder values, string path) => d.IndexType switch
        {
            Int8Type => new DictionaryEncoder<sbyte>(values),
            Int16Type => new DictionaryEncoder<short>(values),
            Int32Type => new DictionaryEncoder<int>(values),
            Int64Type => new DictionaryEncoder<long>(values),
            UInt8Type => new DictionaryEncoder<byte>(values),
            UInt16Type => new DictionaryEncoder<ushort>(values),
            UInt32Type => new DictionaryEncoder<uint>(values),
            UInt64Type => new DictionaryEncoder<ulong>(values),
            _ => throw new NotSupportedException($"'{path}': dictionary indices of {d.IndexType.Name} are not supported."),
        };

        private static AvroEncoder Binary(IArrowType t) => t switch
        {
            BinaryType or StringType => new BinaryEncoder(),
            LargeBinaryType or LargeStringType => new LargeBinaryEncoder(),
            _ => new BinaryViewEncoder(),
        };

        private static bool IsString(IArrowType t) => t is StringType or LargeStringType or StringViewType;
        private static bool IsBinary(IArrowType t) => t is BinaryType or LargeBinaryType or BinaryViewType;
        private static bool IsDecimal(IArrowType t) => t is Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type;
        private static bool IsPlainInteger(IArrowType t) => t is Int8Type or Int16Type or Int32Type or Int64Type
            or UInt8Type or UInt16Type or UInt32Type or UInt64Type;

        private static string Join(string path, string name) => path.Length == 0 ? name : $"{path}.{name}";
        private static string Display(string path) => path.Length == 0 ? "(top level)" : path;

        private static ArgumentException Mismatch(string path, IArrowType t, AvroSchema a) =>
            new($"The column '{path}' ({t.Name}) cannot be written as {Show(a)}.");

        private static string Show(AvroSchema a) => a switch
        {
            AvroNamedSchema n => $"{a.Type.ToString().ToLowerInvariant()} '{n.FullName}'" + (a.Logical == AvroLogicalKind.None ? "" : $" ({a.Logical})"),
            { Logical: not AvroLogicalKind.None } => $"{a.Type.ToString().ToLowerInvariant()} ({a.Logical}{(a.AdjustToUtc is bool adj ? $", adjust-to-utc {adj.ToString().ToLowerInvariant()}" : "")})",
            _ => a.Type.ToString().ToLowerInvariant(),
        };
    }
}

// ---- encoders the given schema adds -----------------------------------------------------------------------------

/// <summary>A field the batch lacks: the encoded default of the schema, the same for every row.</summary>
internal sealed class ConstantEncoder(byte[] bytes) : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) => w.WriteFixed(bytes);
}

/// <summary>A single-branch union value: the branch number, then the value.</summary>
internal sealed class BranchEncoder(int branch, AvroEncoder inner) : AvroEncoder
{
    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        inner.Bind(array);
    }

    public override void Release()
    {
        base.Release();
        inner.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        w.WriteLong(branch);
        inner.Encode(index, w);
    }
}

/// <summary>A struct whose Avro fields come from its children by position (-1: a constant).</summary>
internal sealed class MappedStructEncoder(AvroEncoder[] fields, int[] sources) : AvroEncoder
{
    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        var s = (StructArray)array;
        for (int i = 0; i < fields.Length; i++)
            if (sources[i] >= 0) fields[i].Bind(s.Fields[sources[i]]);
    }

    public override void Release()
    {
        base.Release();
        foreach (var f in fields) f.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        // StructArray.Fields are already sliced to the parent's offset and length.
        foreach (var f in fields) f.Encode(index, w);
    }
}

internal sealed class WideningFloatEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) => w.WriteDouble(Values<float>(Array)[index]);
}

internal sealed class WideningHalfEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) => w.WriteDouble((double)Values<Half>(Array)[index]);
}

/// <summary>A double written as float only when the float holds it exactly (NaN and infinities included).</summary>
internal sealed class NarrowingDoubleEncoder(string path) : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w)
    {
        double d = Values<double>(Array)[index];
        float f = (float)d;
        if ((double)f != d && !double.IsNaN(d)) throw Lossy($"column '{path}' value {d} as float");
        w.WriteFloat(f);
    }
}

/// <summary>A plain integer column written as float or double, refused when the value does not convert exactly.</summary>
internal sealed class IntegerToFloatingEncoder(bool toDouble, string path) : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w)
    {
        long value = Array switch
        {
            Int8Array a => a.Values[index],
            Int16Array a => a.Values[index],
            Int32Array a => a.Values[index],
            Int64Array a => a.Values[index],
            UInt8Array a => a.Values[index],
            UInt16Array a => a.Values[index],
            UInt32Array a => a.Values[index],
            UInt64Array a => a.Values[index] <= long.MaxValue ? (long)a.Values[index] : throw Lossy($"column '{path}' value {a.Values[index]}"),
            _ => throw new InvalidOperationException($"Unexpected {Array.GetType().Name}."),
        };
        if (toDouble)
        {
            double d = value;
            if (!LongAsDoubleDecoder.Exact(d, value)) throw Lossy($"column '{path}' value {value} as double");
            w.WriteDouble(d);
        }
        else
        {
            float f = value;
            if (!LongAsDoubleDecoder.Exact(f, value)) throw Lossy($"column '{path}' value {value} as float");
            w.WriteFloat(f);
        }
    }
}

/// <summary>Binary written as a string only when it is valid UTF-8.</summary>
internal sealed class Utf8CheckedEncoder(AvroEncoder inner, string path) : AvroEncoder
{
    private static readonly UTF8Encoding Strict = new(false, true);

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        inner.Bind(array);
    }

    public override void Release()
    {
        base.Release();
        inner.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        ReadOnlySpan<byte> bytes = Array switch
        {
            BinaryArray a => a.GetBytes(index),
            LargeBinaryArray a => a.GetBytes(index),
            BinaryViewArray a => a.GetBytes(index),
            _ => throw new InvalidOperationException($"Unexpected {Array.GetType().Name}."),
        };
        try
        {
            Strict.GetCharCount(bytes);
        }
        catch (DecoderFallbackException)
        {
            throw new NotSupportedException($"Cannot write the column '{path}' as an Avro string: a value is not valid UTF-8.");
        }
        inner.Encode(index, w);
    }
}

internal sealed class FixedAsBytesEncoder(int width) : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) =>
        w.WriteBytes(Array.Data.Buffers[1].Span.Slice((Array.Offset + index) * width, width));
}

/// <summary>Binary written as a fixed only when the value has exactly the fixed size.</summary>
internal sealed class FixedFromBinaryEncoder(int size, string path) : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w)
    {
        ReadOnlySpan<byte> bytes = Array switch
        {
            BinaryArray a => a.GetBytes(index),
            LargeBinaryArray a => a.GetBytes(index),
            BinaryViewArray a => a.GetBytes(index),
            _ => throw new InvalidOperationException($"Unexpected {Array.GetType().Name}."),
        };
        if (bytes.Length != size)
            throw new NotSupportedException($"Cannot write the column '{path}' as fixed({size}): a value has {bytes.Length} bytes.");
        w.WriteFixed(bytes);
    }
}

/// <summary>An enum from strings: each value must be one of the symbols.</summary>
internal sealed class EnumEncoder(IReadOnlyList<string> symbols, string path) : AvroEncoder
{
    private readonly Dictionary<string, int> _index = symbols.Select((s, i) => (s, i)).ToDictionary(x => x.s, x => x.i, StringComparer.Ordinal);

    public override void Encode(int index, AvroBufferWriter w)
    {
        string value = Array switch
        {
            StringArray a => a.GetString(index),
            LargeStringArray a => a.GetString(index),
            StringViewArray a => a.GetString(index),
            _ => throw new InvalidOperationException($"Unexpected {Array.GetType().Name}."),
        } ?? "";
        if (!_index.TryGetValue(value, out int symbol))
            throw new NotSupportedException($"Cannot write the column '{path}' as an Avro enum: '{value}' is not one of its symbols.");
        w.WriteLong(symbol);
    }
}

/// <summary>A decimal written as bytes or a fixed of <paramref name="fixedSize"/> bytes (sign-extended), refusing a
/// value with more than <paramref name="maxDigits"/> digits or too wide for the fixed.</summary>
internal sealed class CheckedDecimalEncoder(int width, int fixedSize, int? maxDigits, string path) : AvroEncoder
{
    private readonly BigInteger? _limit = maxDigits is int digits ? BigInteger.Pow(10, digits) : null;

    public override void Encode(int index, AvroBufferWriter w)
    {
        ReadOnlySpan<byte> le = Array.Data.Buffers[1].Span.Slice((Array.Offset + index) * width, width);
        if (_limit is { } limit && BigInteger.Abs(new BigInteger(le, isUnsigned: false, isBigEndian: false)) >= limit)
            throw new NotSupportedException($"Cannot write the column '{path}': a value has more than {maxDigits} digits.");
        byte sign = (le[width - 1] & 0x80) != 0 ? (byte)0xFF : (byte)0;
        if (fixedSize < 0)
        {
            int length = width;
            while (length > 1 && le[length - 1] == sign && ((le[length - 2] ^ sign) & 0x80) == 0) length--;
            Span<byte> be = stackalloc byte[length];
            for (int i = 0; i < length; i++) be[i] = le[length - 1 - i];
            w.WriteBytes(be);
            return;
        }
        // Bytes beyond the fixed must be sign bytes, and the fixed's top bit must carry the sign.
        for (int i = fixedSize; i < width; i++)
        {
            if (le[i] != sign) throw TooWide();
        }
        if (fixedSize < width && ((le[fixedSize - 1] ^ sign) & 0x80) != 0) throw TooWide();
        Span<byte> fixedBe = stackalloc byte[fixedSize];
        for (int i = 0; i < fixedSize; i++) fixedBe[i] = i < fixedSize - width ? sign : le[fixedSize - 1 - i];
        w.WriteFixed(fixedBe);
    }

    private NotSupportedException TooWide() =>
        new($"Cannot write the column '{path}': a value does not fit the {fixedSize}-byte fixed decimal.");
}
