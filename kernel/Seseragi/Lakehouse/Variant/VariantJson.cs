using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Variant;

/// <summary>
/// Renders values in the Parquet Variant binary encoding (Delta's <c>variantType</c>, Iceberg v3's variant) as JSON,
/// byte for byte as Spark's <c>to_json</c> renders them (<c>org.apache.spark.types.variant.Variant.toJson</c>):
/// compact, object fields in their stored order, integers and decimals as plain numbers, doubles and floats as
/// Java prints them (non-finite ones quoted), dates, timestamps, binaries (base64) and UUIDs as strings.
/// Timestamps with a time zone are rendered in UTC.
/// </summary>
public static class VariantJson
{
    /// <summary>The value as JSON text.</summary>
    /// <param name="metadata">The variant's metadata (its dictionary of field names).</param>
    /// <param name="value">The variant's value.</param>
    /// <exception cref="InvalidDataException">The bytes do not follow the encoding.</exception>
    public static string ToJson(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value)
    {
        var buffer = new ArrayBufferWriter<byte>(value.Length * 2 + 16);
        Write(metadata, value, buffer);
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Writes the value as UTF-8 JSON text.</summary>
    /// <param name="metadata">The variant's metadata (its dictionary of field names).</param>
    /// <param name="value">The variant's value.</param>
    /// <param name="output">Where the text goes.</param>
    /// <exception cref="InvalidDataException">The bytes do not follow the encoding.</exception>
    public static void Write(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        try
        {
            var dictionary = new Dictionary(metadata);
            Value(dictionary, value, output, 0);
        }
        catch (Exception e) when (e is IndexOutOfRangeException or ArgumentOutOfRangeException or OverflowException)
        {
            throw new InvalidDataException("The variant value is damaged.", e);
        }
    }

    /// <summary>Nesting deeper than this is refused (Spark's own limit is far lower than any real document needs).</summary>
    private const int MaxDepth = 1000;

    // ---- basic types (low two bits of the value header) and primitive type ids (the six bits above them)

    private const int Primitive = 0, ShortString = 1, ObjectType = 2, ArrayType = 3;

    private const int Null = 0, True = 1, False = 2, Int8 = 3, Int16 = 4, Int32 = 5, Int64 = 6, Double = 7, Decimal4 = 8,
        Decimal8 = 9, Decimal16 = 10, Date = 11, Timestamp = 12, TimestampNtz = 13, Float = 14, Binary = 15, String = 16,
        Uuid = 20;

    /// <summary>The metadata's dictionary of field names.</summary>
    private readonly ref struct Dictionary
    {
        private readonly ReadOnlySpan<byte> _metadata;
        private readonly int _offsetSize, _count, _offsets, _strings;

        public Dictionary(ReadOnlySpan<byte> metadata)
        {
            if (metadata.IsEmpty) throw new InvalidDataException("The variant metadata is empty.");
            byte header = metadata[0];
            if ((header & 0x0F) != 1) throw new InvalidDataException($"Variant metadata version {header & 0x0F} is not supported.");
            _metadata = metadata;
            _offsetSize = ((header >> 6) & 0x3) + 1;
            _count = checked((int)Unsigned(metadata[1..], _offsetSize));
            _offsets = 1 + _offsetSize;
            _strings = _offsets + (_count + 1) * _offsetSize;
        }

        public ReadOnlySpan<byte> Name(int id)
        {
            if ((uint)id >= (uint)_count) throw new InvalidDataException($"The variant names field {id} of {_count}.");
            int start = checked((int)Unsigned(_metadata[(_offsets + id * _offsetSize)..], _offsetSize));
            int end = checked((int)Unsigned(_metadata[(_offsets + (id + 1) * _offsetSize)..], _offsetSize));
            return _metadata.Slice(_strings + start, end - start);
        }
    }

    private static void Value(in Dictionary dictionary, ReadOnlySpan<byte> value, IBufferWriter<byte> output, int depth)
    {
        if (depth > MaxDepth) throw new InvalidDataException("The variant nests too deeply.");
        byte header = value[0];
        int argument = header >> 2;
        switch (header & 0x3)
        {
            case ShortString:
                JsonString(value.Slice(1, argument), output);
                break;
            case ObjectType:
                Object(dictionary, value, argument, output, depth);
                break;
            case ArrayType:
                Array(dictionary, value, argument, output, depth);
                break;
            default:
                Scalar(value, argument, output);
                break;
        }

        static void Object(in Dictionary dictionary, ReadOnlySpan<byte> value, int argument, IBufferWriter<byte> output, int depth)
        {
            int offsetSize = (argument & 0x3) + 1;
            int idSize = ((argument >> 2) & 0x3) + 1;
            bool large = (argument & 0x10) != 0;
            int countSize = large ? 4 : 1;
            int count = checked((int)Unsigned(value[1..], countSize));
            int ids = 1 + countSize;
            int offsets = ids + count * idSize;
            int values = offsets + (count + 1) * offsetSize;

            Raw(output, "{"u8);
            for (int i = 0; i < count; i++)
            {
                if (i > 0) Raw(output, ","u8);
                int id = checked((int)Unsigned(value[(ids + i * idSize)..], idSize));
                JsonString(dictionary.Name(id), output);
                Raw(output, ":"u8);
                int offset = checked((int)Unsigned(value[(offsets + i * offsetSize)..], offsetSize));
                Value(dictionary, value[(values + offset)..], output, depth + 1);
            }
            Raw(output, "}"u8);
        }

        static void Array(in Dictionary dictionary, ReadOnlySpan<byte> value, int argument, IBufferWriter<byte> output, int depth)
        {
            int offsetSize = (argument & 0x3) + 1;
            bool large = (argument & 0x4) != 0;
            int countSize = large ? 4 : 1;
            int count = checked((int)Unsigned(value[1..], countSize));
            int offsets = 1 + countSize;
            int values = offsets + (count + 1) * offsetSize;

            Raw(output, "["u8);
            for (int i = 0; i < count; i++)
            {
                if (i > 0) Raw(output, ","u8);
                int offset = checked((int)Unsigned(value[(offsets + i * offsetSize)..], offsetSize));
                Value(dictionary, value[(values + offset)..], output, depth + 1);
            }
            Raw(output, "]"u8);
        }

        static void Scalar(ReadOnlySpan<byte> value, int type, IBufferWriter<byte> output)
        {
            ReadOnlySpan<byte> data = value[1..];
            switch (type)
            {
                case Null:
                    Raw(output, "null"u8);
                    break;
                case True:
                    Raw(output, "true"u8);
                    break;
                case False:
                    Raw(output, "false"u8);
                    break;
                case Int8:
                    Text(output, ((sbyte)data[0]).ToString(CultureInfo.InvariantCulture));
                    break;
                case Int16:
                    Text(output, BinaryPrimitives.ReadInt16LittleEndian(data).ToString(CultureInfo.InvariantCulture));
                    break;
                case Int32:
                    Text(output, BinaryPrimitives.ReadInt32LittleEndian(data).ToString(CultureInfo.InvariantCulture));
                    break;
                case Int64:
                    Text(output, BinaryPrimitives.ReadInt64LittleEndian(data).ToString(CultureInfo.InvariantCulture));
                    break;
                case Double:
                {
                    double d = BinaryPrimitives.ReadDoubleLittleEndian(data);
                    Number(output, JavaNumbers.Double(d), double.IsFinite(d));
                    break;
                }
                case Float:
                {
                    float f = BinaryPrimitives.ReadSingleLittleEndian(data);
                    Number(output, JavaNumbers.Float(f), float.IsFinite(f));
                    break;
                }
                case Decimal4:
                    Text(output, PlainDecimal(BinaryPrimitives.ReadInt32LittleEndian(data[1..]), data[0]));
                    break;
                case Decimal8:
                    Text(output, PlainDecimal(BinaryPrimitives.ReadInt64LittleEndian(data[1..]), data[0]));
                    break;
                case Decimal16:
                    Text(output, PlainDecimal(new BigInteger(data.Slice(1, 16), isUnsigned: false, isBigEndian: false), data[0]));
                    break;
                case Date:
                    Quoted(output, JavaTime.Date(BinaryPrimitives.ReadInt32LittleEndian(data)));
                    break;
                case Timestamp:
                    Quoted(output, JavaTime.DateTime(BinaryPrimitives.ReadInt64LittleEndian(data)) + "+00:00");
                    break;
                case TimestampNtz:
                    Quoted(output, JavaTime.DateTime(BinaryPrimitives.ReadInt64LittleEndian(data)));
                    break;
                case Binary:
                {
                    int length = BinaryPrimitives.ReadInt32LittleEndian(data);
                    Quoted(output, Convert.ToBase64String(data.Slice(4, length)));
                    break;
                }
                case String:
                {
                    int length = BinaryPrimitives.ReadInt32LittleEndian(data);
                    JsonString(data.Slice(4, length), output);
                    break;
                }
                case Uuid:
                    Quoted(output, new Guid(data[..16], bigEndian: true).ToString("D"));
                    break;
                default:
                    throw new InvalidDataException($"The variant primitive type {type} is not supported.");
            }
        }

        // A number, or its text quoted when it is not finite (Spark's rendering of NaN and the infinities).
        static void Number(IBufferWriter<byte> output, string text, bool finite)
        {
            if (finite) Text(output, text);
            else Quoted(output, text);
        }

        // A decimal as Spark renders it: Java's BigDecimal.stripTrailingZeros().toPlainString(), every digit, no
        // exponent, no zeros at the end of the fraction (no point when none is left).
        static string PlainDecimal(BigInteger unscaled, int scale)
        {
            string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
            if (scale > 0)
            {
                digits = digits.PadLeft(scale + 1, '0');
                string fraction = digits[^scale..].TrimEnd('0');
                digits = fraction.Length == 0 ? digits[..^scale] : digits[..^scale] + "." + fraction;
            }
            return unscaled.Sign < 0 && digits.Any(c => c is not ('0' or '.')) ? "-" + digits : digits;
        }
    }

    /// <summary>Text as a JSON string, escaped as Jackson's <c>JsonGenerator.writeString</c> escapes it.</summary>
    private static void JsonString(ReadOnlySpan<byte> utf8, IBufferWriter<byte> output)
    {
        Raw(output, "\""u8);
        int start = 0;
        for (int i = 0; i < utf8.Length; i++)
        {
            byte b = utf8[i];
            if (b >= 0x20 && b != (byte)'"' && b != (byte)'\\') continue;
            Raw(output, utf8[start..i]);
            ReadOnlySpan<byte> escape = b switch
            {
                (byte)'"' => "\\\""u8,
                (byte)'\\' => "\\\\"u8,
                (byte)'\n' => "\\n"u8,
                (byte)'\r' => "\\r"u8,
                (byte)'\t' => "\\t"u8,
                (byte)'\b' => "\\b"u8,
                (byte)'\f' => "\\f"u8,
                _ => default,
            };
            if (escape.IsEmpty)
            {
                // Written straight into the output: \u00XX.
                Span<byte> unicode = output.GetSpan(6);
                "\\u00"u8.CopyTo(unicode);
                unicode[4] = HexDigits[b >> 4];
                unicode[5] = HexDigits[b & 0xF];
                output.Advance(6);
            }
            else
            {
                Raw(output, escape);
            }
            start = i + 1;
        }
        Raw(output, utf8[start..]);
        Raw(output, "\""u8);
    }

    private static ReadOnlySpan<byte> HexDigits => "0123456789ABCDEF"u8;

    private static void Quoted(IBufferWriter<byte> output, string text)
    {
        Raw(output, "\""u8);
        Text(output, text);
        Raw(output, "\""u8);
    }

    private static void Text(IBufferWriter<byte> output, string text)
    {
        Span<byte> span = output.GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length));
        output.Advance(Encoding.UTF8.GetBytes(text, span));
    }

    private static void Raw(IBufferWriter<byte> output, ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(output.GetSpan(bytes.Length));
        output.Advance(bytes.Length);
    }

    /// <summary>A little-endian unsigned integer of 1 to 4 bytes.</summary>
    private static uint Unsigned(ReadOnlySpan<byte> bytes, int size) => size switch
    {
        1 => bytes[0],
        2 => BinaryPrimitives.ReadUInt16LittleEndian(bytes),
        3 => (uint)(bytes[0] | bytes[1] << 8 | bytes[2] << 16),
        _ => BinaryPrimitives.ReadUInt32LittleEndian(bytes),
    };
}
