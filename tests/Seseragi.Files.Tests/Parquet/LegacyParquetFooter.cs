using System.Buffers.Binary;
using System.Text;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The footer parser as it was before the full metadata model (P1), kept to check the new one against.</summary>
internal static class LegacyParquetFooter
{
    /// <summary>Parses the Thrift-encoded <c>FileMetaData</c>.</summary>
    /// <exception cref="InvalidDataException">The bytes are not a valid footer.</exception>
    public static ParquetMetadata Parse(ReadOnlySpan<byte> footer)
    {
        try
        {
            var reader = new CompactReader(footer);
            ThriftStruct meta = reader.ReadStruct();
            return new ParquetMetadata
            {
                Schema = [.. meta.List(2).Select(e => SchemaElement((ThriftStruct)e))],
                RowCount = meta.Long(3) ?? 0,
                RowGroups = [.. meta.List(4).Select(g => RowGroup((ThriftStruct)g))],
            };
        }
        catch (Exception e) when (e is IndexOutOfRangeException or ArgumentOutOfRangeException or InvalidCastException or OverflowException)
        {
            throw new InvalidDataException("The Parquet footer is damaged.", e);
        }
    }

    private static ParquetSchemaElement SchemaElement(ThriftStruct e)
    {
        int? scale = null, precision = null, bitWidth = null;
        ParquetTimeUnit? unit = null;
        bool date = false, variant = false;
        if (e.Struct(10) is ThriftStruct logical)
        {
            variant = logical.Has(16);
            if (logical.Struct(5) is ThriftStruct dec)
            {
                scale = (int?)dec.Long(1);
                precision = (int?)dec.Long(2);
            }
            date = logical.Has(6);
            if (logical.Struct(8) is ThriftStruct ts && ts.Struct(2) is ThriftStruct u)
                unit = u.Has(1) ? ParquetTimeUnit.Millis : u.Has(2) ? ParquetTimeUnit.Micros : u.Has(3) ? ParquetTimeUnit.Nanos : null;
            if (logical.Struct(10) is ThriftStruct integer) bitWidth = (int?)integer.Long(1);
        }
        long? converted = e.Long(6);
        if (scale is null && converted == 5)                                  // DECIMAL
        {
            scale = (int?)e.Long(7) ?? 0;
            precision = (int?)e.Long(8);
        }
        if (unit is null && converted == 9) unit = ParquetTimeUnit.Millis;    // TIMESTAMP_MILLIS
        if (unit is null && converted == 10) unit = ParquetTimeUnit.Micros;   // TIMESTAMP_MICROS
        date |= converted == 6;                                               // DATE
        bitWidth ??= converted switch                                         // INT_8 … INT_64, UINT_8 … UINT_64
        {
            11 or 15 => 8,
            12 or 16 => 16,
            13 or 17 => 32,
            14 or 18 => 64,
            _ => null,
        };
        return new ParquetSchemaElement(
            e.String(4) ?? "", (int?)e.Long(1), (int?)e.Long(3), (int)(e.Long(5) ?? 0), (int?)e.Long(2), scale, unit, (int?)e.Long(9),
            bitWidth, precision, date, variant);
    }

    private static ParquetRowGroup RowGroup(ThriftStruct g) => new(
        g.Long(3) ?? 0,
        [.. g.List(1).Select(c =>
        {
            ThriftStruct meta = ((ThriftStruct)c).Struct(3) ?? throw new InvalidDataException("A column chunk has no metadata.");
            ThriftStruct? s = meta.Struct(12);
            // Only min_value / max_value (fields 5, 6): the legacy min / max used a signed byte order for binary.
            ParquetColumnStatistics? statistics = s is null ? null : new(s.Bytes(6), s.Bytes(5), s.Long(3));
            return new ParquetColumnChunk([.. meta.List(3).Select(p => Encoding.UTF8.GetString((byte[])p))], meta.Long(5) ?? 0, statistics);
        })]);

    // ------------------------------------------------------------------
    // Thrift compact protocol
    // ------------------------------------------------------------------

    /// <summary>A decoded struct: field id to value (long, double, bool, byte[], list or struct).</summary>
    private sealed class ThriftStruct
    {
        private readonly Dictionary<short, object> _fields = [];

        public void Set(short id, object value) => _fields[id] = value;
        public bool Has(short id) => _fields.ContainsKey(id);
        public long? Long(short id) => _fields.TryGetValue(id, out object? v) ? Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture) : null;
        public byte[]? Bytes(short id) => _fields.TryGetValue(id, out object? v) ? (byte[])v : null;
        public string? String(short id) => Bytes(id) is byte[] b ? Encoding.UTF8.GetString(b) : null;
        public ThriftStruct? Struct(short id) => _fields.TryGetValue(id, out object? v) ? (ThriftStruct)v : null;
        public List<object> List(short id) => _fields.TryGetValue(id, out object? v) ? (List<object>)v : [];
    }

    private ref struct CompactReader(ReadOnlySpan<byte> data)
    {
        private const int MaxDepth = 64;
        private readonly ReadOnlySpan<byte> _data = data;
        private int _at;
        private int _depth;

        public ThriftStruct ReadStruct()
        {
            if (++_depth > MaxDepth) throw new InvalidDataException("The Parquet footer nests too deeply.");
            var result = new ThriftStruct();
            short last = 0;
            while (true)
            {
                byte header = _data[_at++];
                if (header == 0) break; // stop
                int type = header & 0x0F;
                int delta = header >> 4;
                short id = delta != 0 ? (short)(last + delta) : (short)ZigZag(Varint());
                last = id;
                object? value = type switch
                {
                    1 => true,
                    2 => false,
                    _ => ReadValue(type),
                };
                if (value is not null) result.Set(id, value);
            }
            _depth--;
            return result;
        }

        private object? ReadValue(int type)
        {
            switch (type)
            {
                case 1:
                case 2:
                    return _data[_at++] == 1; // a bool inside a list or map
                case 3:
                    return (long)(sbyte)_data[_at++];
                case 4:
                case 5:
                case 6:
                    return ZigZag(Varint());
                case 7:
                {
                    double d = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_at, 8));
                    _at += 8;
                    return d;
                }
                case 8:
                {
                    int length = checked((int)Varint());
                    byte[] bytes = _data.Slice(_at, length).ToArray();
                    _at += length;
                    return bytes;
                }
                case 9:
                case 10:
                {
                    byte header = _data[_at++];
                    int count = header >> 4;
                    if (count == 15) count = checked((int)Varint());
                    int elementType = header & 0x0F;
                    var list = new List<object>(Math.Min(count, 1024));
                    for (int i = 0; i < count; i++) list.Add(ReadValue(elementType)!);
                    return list;
                }
                case 11:
                {
                    int count = checked((int)Varint());
                    if (count == 0) return null;
                    byte types = _data[_at++];
                    for (int i = 0; i < count; i++)
                    {
                        ReadValue(types >> 4);
                        ReadValue(types & 0x0F);
                    }
                    return null; // maps are not needed
                }
                case 12:
                    return ReadStruct();
                default:
                    throw new InvalidDataException($"Unknown Thrift compact type {type} in the Parquet footer.");
            }
        }

        private ulong Varint()
        {
            ulong result = 0;
            for (int shift = 0; shift < 64; shift += 7)
            {
                byte b = _data[_at++];
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) return result;
            }
            throw new InvalidDataException("A varint in the Parquet footer is too long.");
        }

        private static long ZigZag(ulong n) => (long)(n >> 1) ^ -(long)(n & 1);
    }
}
