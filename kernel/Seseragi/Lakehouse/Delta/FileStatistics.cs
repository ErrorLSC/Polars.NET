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
/// The <c>stats</c> of a data file, from its Parquet footer: the row count, and for the leaves of the columns (struct
/// fields at any depth, nested as Spark nests them; not arrays or maps) the null count and (not for booleans and
/// binary) the minimum and maximum.
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

    /// <summary>The stats JSON of a data file, for every leaf of the columns (struct fields at any depth; not arrays or maps).</summary>
    /// <param name="metadata">The file footer.</param>
    /// <param name="dataFields">The table columns the file holds (not the partition columns), found in the file by their physical names.</param>
    public static string FromParquet(ParquetMetadata metadata, IReadOnlyList<StructField> dataFields)
    {
        ArgumentNullException.ThrowIfNull(dataFields);
        return FromParquet(metadata, Leaves(dataFields, _ => true));
    }

    /// <summary>
    /// The stats JSON of a data file for the given leaves: paths of fields from a table column down to a field that
    /// is not a struct (see <see cref="IndexedLeaves"/>). Nested fields' statistics nest under their structs, keyed
    /// by physical names, as Spark writes them. A leaf inside a struct counts as null where the struct is null.
    /// </summary>
    /// <param name="metadata">The file footer.</param>
    /// <param name="leaves">The leaves to give statistics for; arrays and maps get none.</param>
    public static string FromParquet(ParquetMetadata metadata, IReadOnlyList<StructField[]> leaves)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(leaves);
        leaves = [.. leaves.Where(p => p[^1].Type is not (StructType or ArrayType or MapType))];
        Dictionary<string, ParquetSchemaElement> elements = LeafElements(metadata.Schema);

        var mins = new List<(string[] Path, string Json)>();
        var maxs = new List<(string[] Path, string Json)>();
        var nulls = new List<(string[] Path, string Json)>();
        foreach (StructField[] leaf in leaves)
        {
            StructField field = leaf[^1];
            string[] path = [.. leaf.Select(f => f.PhysicalName)];
            elements.TryGetValue(string.Join(PathSeparator, path), out ParquetSchemaElement? element);
            var chunks = metadata.RowGroups
                .Select(g => (Rows: g.RowCount, Chunk: g.Columns.FirstOrDefault(c => c.Path.SequenceEqual(path))))
                .ToList();
            if (element is null || chunks.Any(c => c.Chunk is null))
            {
                // Not in the file: every row is null (an old file read with a newer schema has the same shape).
                if (element is null) nulls.Add((path, metadata.RowCount.ToString(CultureInfo.InvariantCulture)));
                continue;
            }

            long? nullCount = 0;
            foreach (var (_, chunk) in chunks) nullCount = chunk!.Statistics?.NullCount is long n && nullCount is long sum ? sum + n : null;
            if (nullCount is long total) nulls.Add((path, total.ToString(CultureInfo.InvariantCulture)));

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
            if (minKnown && min?.Format(field.Type, isMax: false) is string minJson) mins.Add((path, minJson));
            if (maxKnown && max?.Format(field.Type, isMax: true) is string maxJson) maxs.Add((path, maxJson));
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
                Section(json, "nullCount", nulls);
            }
            json.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);

        static void Section(Utf8JsonWriter json, string section, List<(string[] Path, string Json)> values)
        {
            json.WriteStartObject(section);
            Nest(json, values, 0);
            json.WriteEndObject();
        }

        // The values under one struct: leaves written, nested ones grouped under their next field (in order).
        static void Nest(Utf8JsonWriter json, List<(string[] Path, string Json)> values, int depth)
        {
            foreach (var group in values.GroupBy(v => v.Path[depth]))
            {
                json.WritePropertyName(group.Key);
                var items = group.ToList();
                if (items.Count == 1 && items[0].Path.Length == depth + 1)
                {
                    json.WriteRawValue(items[0].Json);
                    continue;
                }
                json.WriteStartObject();
                Nest(json, items, depth + 1);
                json.WriteEndObject();
            }
        }
    }

    private const char PathSeparator = '\u001f';

    /// <summary>The leaves of the flattened Parquet schema (not inside repeated groups), by their path of names.</summary>
    private static Dictionary<string, ParquetSchemaElement> LeafElements(IReadOnlyList<ParquetSchemaElement> schema)
    {
        var result = new Dictionary<string, ParquetSchemaElement>(StringComparer.Ordinal);
        if (schema.Count == 0) return result;
        int at = 1;
        for (int child = 0; child < schema[0].ChildCount && at < schema.Count; child++) at = Walk(schema, at, null, result);
        return result;

        static int Walk(IReadOnlyList<ParquetSchemaElement> schema, int at, string? prefix, Dictionary<string, ParquetSchemaElement> result)
        {
            ParquetSchemaElement element = schema[at];
            string path = prefix is null ? element.Name : prefix + PathSeparator + element.Name;
            bool repeated = element.Repetition == 2;
            at++;
            if (element.ChildCount == 0)
            {
                if (!repeated) result[path] = element;
                return at;
            }
            for (int i = 0; i < element.ChildCount; i++)
            {
                // Below a repeated group (a list or map) nothing is a leaf of the table's structs.
                at = repeated ? Skip(schema, at) : Walk(schema, at, path, result);
            }
            return at;
        }

        static int Skip(IReadOnlyList<ParquetSchemaElement> schema, int at)
        {
            int children = schema[at].ChildCount;
            at++;
            for (int i = 0; i < children; i++) at = Skip(schema, at);
            return at;
        }
    }

    /// <summary>The leaves under the columns (struct fields at any depth, arrays and maps as leaves), in schema order, where <paramref name="keep"/> holds.</summary>
    private static List<StructField[]> Leaves(IReadOnlyList<StructField> fields, Func<string, bool> keep)
    {
        var result = new List<StructField[]>();
        void Walk(StructField field, List<StructField> path)
        {
            path.Add(field);
            if (field.Type is StructType s)
            {
                foreach (StructField child in s.Fields) Walk(child, path);
            }
            else if (keep(string.Join('.', path.Select(f => f.Name))))
            {
                result.Add([.. path]);
            }
            path.RemoveAt(path.Count - 1);
        }
        foreach (StructField field in fields) Walk(field, []);
        return result;
    }

    /// <summary>The table property naming the columns that get statistics; it takes precedence over the count.</summary>
    public const string StatsColumnsKey = "delta.dataSkippingStatsColumns";

    /// <summary>The table property counting the leading leaf columns that get statistics (32 by default, -1 for all).</summary>
    public const string NumIndexedColsKey = "delta.dataSkippingNumIndexedCols";

    /// <summary>
    /// The leaves whose statistics are written, as Spark selects them: the columns <see cref="StatsColumnsKey"/>
    /// names (dotted for nested fields; a struct names all its fields), else the first <see cref="NumIndexedColsKey"/>
    /// leaves of the data schema in order (32 by default, -1 for all), where every struct field is a leaf and an
    /// array or map is one. Arrays and maps are counted but left out (their statistics cannot be read from a footer).
    /// </summary>
    /// <param name="dataFields">The table columns that are not partition columns, in schema order.</param>
    /// <param name="configuration">The table properties.</param>
    /// <returns>Paths of fields from a column down to the leaf, in schema order.</returns>
    public static IReadOnlyList<StructField[]> IndexedLeaves(IReadOnlyList<StructField> dataFields, IReadOnlyDictionary<string, string>? configuration)
    {
        ArgumentNullException.ThrowIfNull(dataFields);
        List<StructField[]> leaves;
        if (configuration is not null && configuration.TryGetValue(StatsColumnsKey, out string? named))
        {
            // Set, even empty (Spark: an empty list collects no column statistics, only the row count).
            var names = new List<string>();
            foreach (IReadOnlyList<string> parts in StatsColumns(named)) names.Add(string.Join('.', parts));
            leaves = Leaves(dataFields, path => names.Any(n =>
                path.Equals(n, StringComparison.OrdinalIgnoreCase) || path.StartsWith(n + ".", StringComparison.OrdinalIgnoreCase)));
        }
        else
        {
            int count = 32;
            if (configuration is not null && configuration.TryGetValue(NumIndexedColsKey, out string? text)
                && int.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int parsed))
                count = parsed;
            leaves = Leaves(dataFields, _ => true);
            if (count >= 0) leaves = [.. leaves.Take(count)];
        }
        return [.. leaves.Where(p => p[^1].Type is not (ArrayType or MapType))];
    }

    /// <summary>
    /// The entries of <see cref="StatsColumnsKey"/>: comma-separated dotted names, each part possibly in backticks
    /// (a backticked part may hold dots); blank entries skipped.
    /// </summary>
    public static List<IReadOnlyList<string>> StatsColumns(string property)
    {
        ArgumentNullException.ThrowIfNull(property);
        var entries = new List<IReadOnlyList<string>>();
        foreach (string entry in property.Split(','))
        {
            if (entry.Trim().Length > 0) entries.Add(ColumnNames.Split(entry.Trim()));
        }
        return entries;
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
