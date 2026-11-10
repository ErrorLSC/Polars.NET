using System.Buffers.Binary;
using System.Text;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Parquet;

/// <summary>The time unit of a Parquet timestamp.</summary>
public enum ParquetTimeUnit
{
    /// <summary>Milliseconds.</summary>
    Millis,
    /// <summary>Microseconds.</summary>
    Micros,
    /// <summary>Nanoseconds.</summary>
    Nanos,
}

/// <summary>One node of the flattened Parquet schema (the footer's <c>SchemaElement</c>), as far as statistics need it.</summary>
/// <param name="Name">The node name.</param>
/// <param name="PhysicalType">The physical type of a leaf (Thrift <c>Type</c>: 0 BOOLEAN … 7 FIXED_LEN_BYTE_ARRAY); null for a group.</param>
/// <param name="Repetition">0 required, 1 optional, 2 repeated; null for the root.</param>
/// <param name="ChildCount">The number of children of a group; 0 for a leaf.</param>
/// <param name="TypeLength">The byte length of a FIXED_LEN_BYTE_ARRAY.</param>
/// <param name="DecimalScale">The scale of a decimal, from the logical or the converted type.</param>
/// <param name="TimeUnit">The unit of a timestamp, from the logical or the converted type.</param>
public sealed record ParquetSchemaElement(
    string Name, int? PhysicalType, int? Repetition, int ChildCount, int? TypeLength, int? DecimalScale, ParquetTimeUnit? TimeUnit);

/// <summary>The statistics of one column chunk: the raw PLAIN-encoded bounds and the null count, each absent when not written.</summary>
public sealed record ParquetColumnStatistics(byte[]? Min, byte[]? Max, long? NullCount);

/// <summary>One column chunk of a row group.</summary>
/// <param name="Path">The path of the leaf in the schema (top-level name first).</param>
/// <param name="ValueCount">The number of values, nulls included (for repeated leaves: elements).</param>
/// <param name="Statistics">The chunk statistics, or null when none were written.</param>
public sealed record ParquetColumnChunk(IReadOnlyList<string> Path, long ValueCount, ParquetColumnStatistics? Statistics);

/// <summary>One row group.</summary>
public sealed record ParquetRowGroup(long RowCount, IReadOnlyList<ParquetColumnChunk> Columns);

/// <summary>
/// The footer of a Parquet file (its <c>FileMetaData</c>): the schema, the row count and the statistics of every
/// column chunk. Read with the Thrift compact protocol, no dependencies; fields not needed are skipped.
/// </summary>
public sealed class ParquetMetadata
{
    private const int TailLength = 8; // footer length (4 bytes, little-endian) + "PAR1"

    /// <summary>The flattened schema, the root first.</summary>
    public required IReadOnlyList<ParquetSchemaElement> Schema { get; init; }

    /// <summary>The number of rows in the file.</summary>
    public required long RowCount { get; init; }

    /// <summary>The row groups.</summary>
    public required IReadOnlyList<ParquetRowGroup> RowGroups { get; init; }

    /// <summary>Reads the footer of a file of known size with at most two ranged reads.</summary>
    /// <exception cref="InvalidDataException">The file is not a Parquet file, or its footer is damaged.</exception>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    public static ParquetMetadata Read(IFileSystem fileSystem, string path, long size)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        if (size < 12) throw new InvalidDataException($"{path} is too short to be a Parquet file.");
        // Most footers are small: one read of the last 64 KiB usually holds the whole footer.
        int guess = (int)Math.Min(size, 64 * 1024);
        byte[] tail = fileSystem.ReadRange(path, size - guess, guess) ?? throw new FileNotFoundException($"{path} does not exist.", path);
        if (tail.Length != guess) throw new InvalidDataException($"{path} is shorter than its listed size.");
        int footer = FooterLength(tail.AsSpan(tail.Length - TailLength), path);
        if (footer + TailLength > size - 4) throw new InvalidDataException($"{path} has a footer longer than the file.");
        if (footer + TailLength <= tail.Length) return Parse(tail.AsSpan(tail.Length - TailLength - footer, footer));
        byte[] whole = fileSystem.ReadRange(path, size - TailLength - footer, footer) ?? throw new FileNotFoundException($"{path} does not exist.", path);
        return Parse(whole);
    }

    /// <summary>Reads the footer of a whole file in memory.</summary>
    /// <exception cref="InvalidDataException">The bytes are not a Parquet file, or its footer is damaged.</exception>
    public static ParquetMetadata FromFile(ReadOnlySpan<byte> file)
    {
        if (file.Length < 12 || !file[..4].SequenceEqual("PAR1"u8)) throw new InvalidDataException("Not a Parquet file.");
        int footer = FooterLength(file[^TailLength..], "the file");
        if (footer + TailLength > file.Length - 4) throw new InvalidDataException("The Parquet footer is longer than the file.");
        return Parse(file.Slice(file.Length - TailLength - footer, footer));
    }

    private static int FooterLength(ReadOnlySpan<byte> tail, string path)
    {
        if (!tail[4..].SequenceEqual("PAR1"u8)) throw new InvalidDataException($"{path} is not a Parquet file (no PAR1 at the end).");
        int length = BinaryPrimitives.ReadInt32LittleEndian(tail);
        if (length <= 0) throw new InvalidDataException($"{path} has an invalid footer length {length}.");
        return length;
    }

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
        int? scale = null;
        ParquetTimeUnit? unit = null;
        if (e.Struct(10) is ThriftStruct logical)
        {
            if (logical.Struct(5) is ThriftStruct dec) scale = (int?)dec.Long(1);
            if (logical.Struct(8) is ThriftStruct ts && ts.Struct(2) is ThriftStruct u)
                unit = u.Has(1) ? ParquetTimeUnit.Millis : u.Has(2) ? ParquetTimeUnit.Micros : u.Has(3) ? ParquetTimeUnit.Nanos : null;
        }
        long? converted = e.Long(6);
        if (scale is null && converted == 5) scale = (int?)e.Long(7) ?? 0;   // DECIMAL
        if (unit is null && converted == 9) unit = ParquetTimeUnit.Millis;    // TIMESTAMP_MILLIS
        if (unit is null && converted == 10) unit = ParquetTimeUnit.Micros;   // TIMESTAMP_MICROS
        return new ParquetSchemaElement(
            e.String(4) ?? "", (int?)e.Long(1), (int?)e.Long(3), (int)(e.Long(5) ?? 0), (int?)e.Long(2), scale, unit);
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
