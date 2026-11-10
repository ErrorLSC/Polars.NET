using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The <c>stats</c> of a data file, from its Parquet footer: the row count, and for top-level columns that are not
/// structs, arrays or maps, the null count and (not for booleans and binary) the minimum and maximum.
/// </summary>
/// <remarks>
/// Only what the footer proves is written: a bound is left out when a row group with values has no bound, a null
/// count when a row group has none. Bounds follow Spark: timestamps cut to milliseconds, a string minimum cut to
/// <see cref="StringPrefixLength"/> characters, a longer string maximum left out (a cut one would not be an upper
/// bound), NaN and infinite bounds left out.
/// </remarks>
public static class FileStatistics
{
    /// <summary>The longest string kept whole in statistics.</summary>
    public const int StringPrefixLength = 32;

    /// <summary>The stats JSON of a data file.</summary>
    /// <param name="metadata">The file footer.</param>
    /// <param name="dataFields">The table columns the file holds (not the partition columns), found in the file by their physical names.</param>
    public static string FromParquet(ParquetMetadata metadata, IReadOnlyList<StructField> dataFields)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(dataFields);
        var leaves = dataFields.Where(f => f.Type is not (StructType or ArrayType or MapType)).ToList();
        var schemaByName = TopLevelLeaves(metadata.Schema);

        var mins = new List<(string Name, string Json)>();
        var maxs = new List<(string Name, string Json)>();
        var nulls = new List<(string Name, long Count)>();
        foreach (StructField field in leaves)
        {
            string name = field.PhysicalName;
            schemaByName.TryGetValue(name, out ParquetSchemaElement? element);
            var chunks = metadata.RowGroups
                .Select(g => (Rows: g.RowCount, Chunk: g.Columns.FirstOrDefault(c => c.Path.Count == 1 && c.Path[0] == name)))
                .ToList();
            if (element is null || chunks.Any(c => c.Chunk is null))
            {
                // Not in the file: every row is null (an old file read with a newer schema has the same shape).
                if (element is null) nulls.Add((name, metadata.RowCount));
                continue;
            }

            long? nullCount = 0;
            foreach (var (_, chunk) in chunks) nullCount = chunk!.Statistics?.NullCount is long n && nullCount is long sum ? sum + n : null;
            if (nullCount is long total) nulls.Add((name, total));

            if (!HasBounds(field.Type)) continue;
            Value? min = null, max = null;
            bool minKnown = true, maxKnown = true;
            foreach (var (rows, chunk) in chunks)
            {
                ParquetColumnStatistics? s = chunk!.Statistics;
                bool allNull = s?.NullCount is long n && n >= rows;
                if (allNull || rows == 0) continue;
                Value? lo = s?.Min is byte[] a ? Decode(a, field.Type, element) : null;
                Value? hi = s?.Max is byte[] b ? Decode(b, field.Type, element) : null;
                if (lo is null) minKnown = false;
                else if (min is null || lo.CompareTo(min) < 0) min = lo;
                if (hi is null) maxKnown = false;
                else if (max is null || hi.CompareTo(max) > 0) max = hi;
            }
            if (minKnown && min?.Format(field.Type, isMax: false) is string minJson) mins.Add((name, minJson));
            if (maxKnown && max?.Format(field.Type, isMax: true) is string maxJson) maxs.Add((name, maxJson));
        }

        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("numRecords", metadata.RowCount);
            if (leaves.Count > 0)
            {
                Section(json, "minValues", mins);
                Section(json, "maxValues", maxs);
                json.WriteStartObject("nullCount");
                foreach (var (name, count) in nulls) json.WriteNumber(name, count);
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);

        static void Section(Utf8JsonWriter json, string section, List<(string Name, string Json)> values)
        {
            json.WriteStartObject(section);
            foreach (var (name, value) in values)
            {
                json.WritePropertyName(name);
                json.WriteRawValue(value);
            }
            json.WriteEndObject();
        }
    }

    /// <summary>The table property naming the columns that get statistics; it takes precedence over the count.</summary>
    public const string StatsColumnsKey = "delta.dataSkippingStatsColumns";

    /// <summary>The table property counting the leading leaf columns that get statistics (32 by default, -1 for all).</summary>
    public const string NumIndexedColsKey = "delta.dataSkippingNumIndexedCols";

    /// <summary>
    /// The data columns whose statistics are written, as Spark selects them: the columns <see cref="StatsColumnsKey"/>
    /// names (a struct names all its fields), else the first <see cref="NumIndexedColsKey"/> leaves of the data
    /// schema in order (32 by default, -1 for all), where every struct field is a leaf and an array or map is one.
    /// Of these, the ones this kernel writes statistics for are returned: the top-level non-nested columns.
    /// </summary>
    /// <param name="dataFields">The table columns that are not partition columns, in schema order.</param>
    /// <param name="configuration">The table properties.</param>
    public static IReadOnlyList<StructField> IndexedFields(IReadOnlyList<StructField> dataFields, IReadOnlyDictionary<string, string>? configuration)
    {
        ArgumentNullException.ThrowIfNull(dataFields);
        if (configuration is not null && configuration.TryGetValue(StatsColumnsKey, out string? named) && !string.IsNullOrWhiteSpace(named))
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (string entry in named.Split(','))
            {
                string name = entry.Trim();
                // A top-level column, possibly in backticks; nested paths name struct fields, which get no statistics here.
                if (name.Length >= 2 && name[0] == '`' && name[^1] == '`' && name.IndexOf('`', 1) == name.Length - 1) name = name[1..^1].Replace("``", "`");
                else if (name.Contains('.', StringComparison.Ordinal)) continue;
                names.Add(name);
            }
            return [.. dataFields.Where(f => names.Contains(f.Name) && IsLeaf(f.Type))];
        }

        int count = 32;
        if (configuration is not null && configuration.TryGetValue(NumIndexedColsKey, out string? text)
            && int.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int parsed))
            count = parsed;
        if (count < 0) return [.. dataFields.Where(f => IsLeaf(f.Type))];

        var result = new List<StructField>();
        int seen = 0;
        foreach (StructField field in dataFields)
        {
            if (seen >= count) break;
            if (IsLeaf(field.Type)) result.Add(field);
            seen += Leaves(field.Type);
        }
        return result;

        static bool IsLeaf(DeltaType type) => type is not (StructType or ArrayType or MapType);
        static int Leaves(DeltaType type) => type is StructType s ? s.Fields.Sum(f => Leaves(f.Type)) : 1;
    }

    /// <summary>The null count of every top-level column the footer has statistics for, summed over row groups.</summary>
    public static IReadOnlyDictionary<string, long> NullCounts(ParquetMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        var result = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (string name in TopLevelLeaves(metadata.Schema).Keys)
        {
            long? sum = 0;
            foreach (ParquetRowGroup group in metadata.RowGroups)
            {
                ParquetColumnChunk? chunk = group.Columns.FirstOrDefault(c => c.Path.Count == 1 && c.Path[0] == name);
                sum = chunk?.Statistics?.NullCount is long n && sum is long s ? s + n : null;
            }
            if (sum is long total) result[name] = total;
        }
        return result;
    }

    private static bool HasBounds(DeltaType type) =>
        type is DecimalType || type is PrimitiveType { Name: not (PrimitiveType.Boolean or PrimitiveType.Binary) };

    /// <summary>The leaves directly under the root of the flattened schema, by name.</summary>
    private static Dictionary<string, ParquetSchemaElement> TopLevelLeaves(IReadOnlyList<ParquetSchemaElement> schema)
    {
        var result = new Dictionary<string, ParquetSchemaElement>(StringComparer.Ordinal);
        if (schema.Count == 0) return result;
        int at = 1;
        for (int child = 0; child < schema[0].ChildCount && at < schema.Count; child++)
        {
            ParquetSchemaElement element = schema[at];
            if (element.ChildCount == 0 && element.Repetition != 2) result[element.Name] = element;
            at = Skip(schema, at);
        }
        return result;

        static int Skip(IReadOnlyList<ParquetSchemaElement> schema, int at)
        {
            int children = schema[at].ChildCount;
            at++;
            for (int i = 0; i < children; i++) at = Skip(schema, at);
            return at;
        }
    }

    // ------------------------------------------------------------------
    // Values
    // ------------------------------------------------------------------

    /// <summary>A decoded bound: an integer (also dates, timestamps in microseconds, decimals unscaled), a float, or UTF-8 bytes.</summary>
    private sealed class Value : IComparable<Value>
    {
        public BigInteger? Integer;
        public double? Float;
        public byte[]? Utf8;

        public int CompareTo(Value? other)
        {
            if (other is null) return 1;
            if (Integer is BigInteger a && other.Integer is BigInteger b) return a.CompareTo(b);
            if (Float is double x && other.Float is double y) return x.CompareTo(y);
            return Utf8!.AsSpan().SequenceCompareTo(other.Utf8); // unsigned bytes: the Parquet order of UTF-8
        }

        /// <summary>The JSON of the bound, or null when it is left out.</summary>
        public string? Format(DeltaType type, bool isMax)
        {
            switch (type)
            {
                case DecimalType d:
                {
                    BigInteger unscaled = Integer!.Value;
                    string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(d.Scale + 1, '0');
                    string text = d.Scale == 0 ? digits : digits[..^d.Scale] + "." + digits[^d.Scale..];
                    return unscaled.Sign < 0 ? "-" + text : text;
                }
                case PrimitiveType { Name: PrimitiveType.Float }:
                    return double.IsFinite(Float!.Value) ? ((float)Float.Value).ToString("R", CultureInfo.InvariantCulture) : null;
                case PrimitiveType { Name: PrimitiveType.Double }:
                    return double.IsFinite(Float!.Value) ? Float.Value.ToString("R", CultureInfo.InvariantCulture) : null;
                case PrimitiveType { Name: PrimitiveType.Date }:
                    return Quote(DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + (int)Integer!.Value)
                        .ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                case PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } p:
                {
                    // Microseconds, cut down to milliseconds (readers allow for the cut).
                    long micros = (long)Integer!.Value;
                    long millis = micros / 1000 - (micros % 1000 < 0 ? 1 : 0); // floor
                    DateTime t = DateTime.UnixEpoch.AddMilliseconds(millis);
                    return Quote(t.ToString(p.Name == PrimitiveType.Timestamp ? "yyyy-MM-dd'T'HH:mm:ss.fff'Z'" : "yyyy-MM-dd'T'HH:mm:ss.fff", CultureInfo.InvariantCulture));
                }
                case PrimitiveType { Name: PrimitiveType.String }:
                {
                    string s = Encoding.UTF8.GetString(Utf8!);
                    if (s.Length > StringPrefixLength)
                    {
                        // A cut minimum is still a lower bound; a cut maximum would not be an upper bound.
                        if (isMax) return null;
                        s = s[..StringPrefixLength];
                        if (char.IsHighSurrogate(s[^1])) s = s[..^1];
                    }
                    return JsonSerializerString(s);
                }
                default:
                    return Integer!.Value.ToString(CultureInfo.InvariantCulture);
            }
        }

        private static string Quote(string s) => "\"" + s + "\"";

        private static string JsonSerializerString(string s)
        {
            var buffer = new System.Buffers.ArrayBufferWriter<byte>();
            using (var json = new Utf8JsonWriter(buffer, ActionWriter.JsonOptions)) json.WriteStringValue(s);
            return Encoding.UTF8.GetString(buffer.WrittenSpan);
        }
    }

    /// <summary>Decodes a PLAIN-encoded bound; null when the physical type does not fit the column type.</summary>
    private static Value? Decode(byte[] bytes, DeltaType type, ParquetSchemaElement element)
    {
        int? physical = element.PhysicalType;
        switch (type)
        {
            case DecimalType:
            {
                if (element.DecimalScale is null) return null;
                BigInteger unscaled = physical switch
                {
                    1 when bytes.Length == 4 => BinaryPrimitives.ReadInt32LittleEndian(bytes),
                    2 when bytes.Length == 8 => BinaryPrimitives.ReadInt64LittleEndian(bytes),
                    6 or 7 when bytes.Length > 0 => new BigInteger(bytes, isUnsigned: false, isBigEndian: true),
                    _ => default,
                };
                if (physical is not (1 or 2 or 6 or 7) || (physical is 1 && bytes.Length != 4) || (physical is 2 && bytes.Length != 8) || bytes.Length == 0)
                    return null;
                if (element.DecimalScale != ((DecimalType)type).Scale) return null;
                return new Value { Integer = unscaled };
            }
            case PrimitiveType p:
                switch (p.Name)
                {
                    case PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Date:
                        return physical == 1 && bytes.Length == 4 ? new Value { Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) } : null;
                    case PrimitiveType.Long:
                        return physical == 2 && bytes.Length == 8 ? new Value { Integer = BinaryPrimitives.ReadInt64LittleEndian(bytes) }
                             : physical == 1 && bytes.Length == 4 ? new Value { Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) }
                             : null;
                    case PrimitiveType.Float or PrimitiveType.Double:
                        return physical == 4 && bytes.Length == 4 ? Finite(BinaryPrimitives.ReadSingleLittleEndian(bytes))
                             : physical == 5 && bytes.Length == 8 ? Finite(BinaryPrimitives.ReadDoubleLittleEndian(bytes))
                             : null;
                    case PrimitiveType.Timestamp or PrimitiveType.TimestampNtz:
                    {
                        if (physical != 2 || bytes.Length != 8 || element.TimeUnit is not ParquetTimeUnit unit) return null;
                        long raw = BinaryPrimitives.ReadInt64LittleEndian(bytes);
                        BigInteger micros = unit switch
                        {
                            ParquetTimeUnit.Millis => (BigInteger)raw * 1000,
                            ParquetTimeUnit.Micros => raw,
                            _ => BigInteger.Divide(raw - (raw < 0 && raw % 1000 != 0 ? 1000 : 0), 1000), // floor
                        };
                        return new Value { Integer = micros };
                    }
                    case PrimitiveType.String:
                        // A bound that is not valid UTF-8 (a cut inside a character) is not trusted.
                        return physical == 6 && Utf8.IsValid(bytes) ? new Value { Utf8 = bytes } : null;
                }
                break;
        }
        return null;

        static Value? Finite(double d) => double.IsNaN(d) ? null : new Value { Float = d };
    }
}
