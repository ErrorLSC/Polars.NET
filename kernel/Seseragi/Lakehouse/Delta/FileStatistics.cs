using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Files.Parquet;

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
        return FromParquet(metadata, new StatisticsLeaves(Leaves(dataFields, _ => true)));
    }

    /// <summary>
    /// The stats JSON of a data file for the given leaves: paths of fields from a table column down to a field that
    /// is not a struct (see <see cref="IndexedLeaves(IReadOnlyList{StructField}, IReadOnlyDictionary{string, string}?)"/>). Nested fields' statistics nest under their structs, keyed
    /// by physical names, as Spark writes them. A leaf inside a struct counts as null where the struct is null.
    /// </summary>
    /// <param name="metadata">The file footer.</param>
    /// <param name="leaves">The leaves to give statistics for; arrays and maps get none.</param>
    public static string FromParquet(ParquetMetadata metadata, IReadOnlyList<StructField[]> leaves)
    {
        ArgumentNullException.ThrowIfNull(leaves);
        return FromParquet(metadata, new StatisticsLeaves(leaves));
    }

    /// <summary>
    /// <see cref="FromParquet(ParquetMetadata, IReadOnlyList{StructField[]})"/> with the leaves prepared once for the
    /// files of a write. Only the stats text is allocated: the bounds are read from the footer's own bytes and written
    /// straight into the JSON.
    /// </summary>
    internal static string FromParquet(ParquetMetadata metadata, StatisticsLeaves leaves)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(leaves);
        int count = leaves.Count;
        LeafStatistics[] results = t_results is { } kept && kept.Length >= count ? kept : t_results = new LeafStatistics[Math.Max(count, 16)];
        try
        {
            Gather(metadata, leaves, results.AsSpan(0, count));
            using var buffer = new NativeBufferWriter(1024);
            using (var json = new Utf8JsonWriter(buffer, ActionWriter.JsonOptions))
            {
                json.WriteStartObject();
                json.WriteNumber("numRecords"u8, metadata.RowCount);
                if (count > 0)
                {
                    Section(json, "minValues"u8, leaves, results, Part.Min);
                    Section(json, "maxValues"u8, leaves, results, Part.Max);
                    Section(json, "nullCount"u8, leaves, results, Part.Nulls);
                }
                json.WriteEndObject();
            }
            return Encoding.UTF8.GetString(buffer.WrittenSpan);
        }
        finally
        {
            results.AsSpan(0, count).Clear(); // let the footer's bound bytes go
        }
    }

    /// <summary>Per thread: the statistics of the leaves of the file being described.</summary>
    [ThreadStatic] private static LeafStatistics[]? t_results;

    /// <summary>What a leaf's statistics are in one file.</summary>
    private struct LeafStatistics
    {
        public Bound Min, Max;
        public long NullCount;
        public bool HasMin, HasMax, HasNullCount;
    }

    private enum Part { Min, Max, Nulls }

    /// <summary>The statistics of every leaf in the footer: its null count, and the bounds that are written.</summary>
    private static void Gather(ParquetMetadata metadata, StatisticsLeaves leaves, Span<LeafStatistics> results)
    {
        IReadOnlyList<ParquetSchemaElement> schema = metadata.Schema;
        int elements = schema.Count;
        Span<int> ends = elements <= 256 ? stackalloc int[elements] : new int[elements];
        Span<int> ordinals = elements <= 256 ? stackalloc int[elements] : new int[elements];
        int rootEnd = Index(schema, ends, ordinals);
        var chunks = new Chunks(metadata);
        int groups = chunks.GroupCount;
        for (int i = 0; i < results.Length; i++)
        {
            ref LeafStatistics result = ref results[i];
            string[] path = leaves.Paths[i];
            int element = Find(schema, ends, rootEnd, path);
            if (element < 0)
            {
                // Not in the file: every row is null (an old file read with a newer schema has the same shape).
                result.NullCount = metadata.RowCount;
                result.HasNullCount = true;
                continue;
            }
            int ordinal = ordinals[element];
            bool everyChunk = true;
            long? nullCount = 0;
            for (int g = 0; g < groups && everyChunk; g++)
            {
                if (!chunks.TryGet(g, ordinal, path, out Statistic chunk)) everyChunk = false;
                else nullCount = chunk.NullCount is long n && nullCount is long sum ? sum + n : null;
            }
            if (!everyChunk) continue;
            result.HasNullCount = nullCount.HasValue;
            result.NullCount = nullCount.GetValueOrDefault();

            DeltaType type = leaves.Types[i];
            if (!HasBounds(type)) continue;
            ParquetSchemaElement leaf = schema[element];
            Bound min = default, max = default;
            bool minKnown = true, maxKnown = true;
            for (int g = 0; g < groups; g++)
            {
                chunks.TryGet(g, ordinal, path, out Statistic chunk);
                long rows = chunks.Rows(g);
                bool allNull = chunk.NullCount is long n && n >= rows;
                if (allNull || rows == 0) continue;
                if (!chunk.Exists || chunk.Min is not { } a || !Decode(a, type, leaf, out Bound lo)) minKnown = false;
                else if (min.Kind == BoundKind.None || Compare(lo, min) < 0) min = lo;
                if (!chunk.Exists || chunk.Max is not { } b || !Decode(b, type, leaf, out Bound hi)) maxKnown = false;
                else if (max.Kind == BoundKind.None || Compare(hi, max) > 0) max = hi;
            }
            result.HasMin = minKnown && min.Kind != BoundKind.None && Written(min, type, isMax: false);
            result.HasMax = maxKnown && max.Kind != BoundKind.None && Written(max, type, isMax: true);
            result.Min = min;
            result.Max = max;
        }

        // Indexes the flattened schema: for each element the index after its subtree and the ordinal of its first leaf (a
        // leaf's column chunk in every row group). Returns the index after the root's children.
        static int Index(IReadOnlyList<ParquetSchemaElement> schema, Span<int> ends, Span<int> ordinals)
        {
            if (schema.Count == 0) return 0;
            int at = 1, leaf = 0;
            for (int child = 0; child < schema[0].ChildCount && at < schema.Count; child++) at = Walk(schema, at, ends, ordinals, ref leaf);
            return at;

            static int Walk(IReadOnlyList<ParquetSchemaElement> schema, int at, Span<int> ends, Span<int> ordinals, ref int leaf)
            {
                ParquetSchemaElement element = schema[at];
                ordinals[at] = leaf;
                int next = at + 1;
                if (element.ChildCount == 0) leaf++;
                for (int i = 0; i < element.ChildCount; i++) next = Walk(schema, next, ends, ordinals, ref leaf);
                ends[at] = next;
                return next;
            }
        }

        // The schema element of the leaf at path (names from a top-level column down), or -1 when the
        // file has none there or the leaf is repeated or under a repeated group (a list or map). Of equal names, the last.
        static int Find(IReadOnlyList<ParquetSchemaElement> schema, ReadOnlySpan<int> ends, int rootEnd, string[] path)
        {
            int from = 1, to = rootEnd;
            for (int depth = 0; depth < path.Length; depth++)
            {
                int found = -1;
                for (int at = from; at < to; at = ends[at])
                {
                    if (schema[at].Name == path[depth]) found = at;
                }
                if (found < 0 || schema[found].Repetition == 2) return -1;
                bool last = depth == path.Length - 1;
                if (last != (schema[found].ChildCount == 0)) return -1;
                (from, to) = (found + 1, ends[found]);
                if (last) return found;
            }
            return -1;
        }

        static bool HasBounds(DeltaType type) =>
            type is DecimalType || type is PrimitiveType { Name: not (PrimitiveType.Boolean or PrimitiveType.Binary) };

        // Decodes a PLAIN-encoded bound; false when the physical type does not fit the column type.
        static bool Decode(byte[] bytes, DeltaType type, ParquetSchemaElement element, out Bound bound)
        {
            bound = default;
            int? physical = element.PhysicalType;
            switch (type)
            {
                case DecimalType decimalType:
                {
                    if (element.DecimalScale is null) return false;
                    if (physical is not (1 or 2 or 6 or 7) || (physical is 1 && bytes.Length != 4) || (physical is 2 && bytes.Length != 8) || bytes.Length == 0)
                        return false;
                    if (element.DecimalScale != decimalType.Scale) return false;
                    bound = physical switch
                    {
                        1 => new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) },
                        2 => new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt64LittleEndian(bytes) },
                        _ when bytes.Length <= 16 => new Bound { Kind = BoundKind.Integer, Integer = BigEndian(bytes) },
                        _ => new Bound { Kind = BoundKind.LongDecimal, Bytes = bytes },
                    };
                    return true;
                }
                case PrimitiveType p:
                    switch (p.Name)
                    {
                        case PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer or PrimitiveType.Date:
                            if (physical != 1 || bytes.Length != 4) return false;
                            bound = new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) };
                            return true;
                        case PrimitiveType.Long:
                            if (physical == 2 && bytes.Length == 8) bound = new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt64LittleEndian(bytes) };
                            else if (physical == 1 && bytes.Length == 4) bound = new Bound { Kind = BoundKind.Integer, Integer = BinaryPrimitives.ReadInt32LittleEndian(bytes) };
                            else return false;
                            return true;
                        case PrimitiveType.Float or PrimitiveType.Double:
                        {
                            double value;
                            if (physical == 4 && bytes.Length == 4) value = BinaryPrimitives.ReadSingleLittleEndian(bytes);
                            else if (physical == 5 && bytes.Length == 8) value = BinaryPrimitives.ReadDoubleLittleEndian(bytes);
                            else return false;
                            if (double.IsNaN(value)) return false;
                            bound = new Bound { Kind = BoundKind.Float, Float = value };
                            return true;
                        }
                        case PrimitiveType.Timestamp or PrimitiveType.TimestampNtz:
                        {
                            if (physical != 2 || bytes.Length != 8 || element.TimeUnit is not ParquetTimeUnit unit) return false;
                            long raw = BinaryPrimitives.ReadInt64LittleEndian(bytes);
                            Int128 micros = unit switch
                            {
                                ParquetTimeUnit.Millis => (Int128)raw * 1000,
                                ParquetTimeUnit.Micros => raw,
                                _ => unchecked(raw - (raw < 0 && raw % 1000 != 0 ? 1000 : 0)) / 1000, // floor
                            };
                            bound = new Bound { Kind = BoundKind.Integer, Integer = micros };
                            return true;
                        }
                        case PrimitiveType.String:
                            // A bound that is not valid UTF-8 (a cut inside a character) is not trusted.
                            if (physical != 6 || !Utf8.IsValid(bytes)) return false;
                            bound = new Bound { Kind = BoundKind.Utf8, Bytes = bytes };
                            return true;
                    }
                    break;
            }
            return false;

            // A two's complement big-endian integer of at most 16 bytes.
            static Int128 BigEndian(ReadOnlySpan<byte> bytes)
            {
                Span<byte> wide = stackalloc byte[16];
                wide[..^bytes.Length].Fill((bytes[0] & 0x80) != 0 ? (byte)0xFF : (byte)0);
                bytes.CopyTo(wide[^bytes.Length..]);
                return BinaryPrimitives.ReadInt128BigEndian(wide);
            }
        }

        static int Compare(in Bound a, in Bound b) => (a.Kind, b.Kind) switch
        {
            (BoundKind.Integer, BoundKind.Integer) => a.Integer.CompareTo(b.Integer),
            (BoundKind.Integer or BoundKind.LongDecimal, BoundKind.Integer or BoundKind.LongDecimal) => a.Big.CompareTo(b.Big),
            (BoundKind.Float, BoundKind.Float) => a.Float.CompareTo(b.Float),
            _ => a.Bytes.AsSpan().SequenceCompareTo(b.Bytes), // unsigned bytes: the Parquet order of UTF-8
        };

        // Whether a bound is written: not a non-finite float, not a string maximum longer than the prefix kept. A date
        // or time the log cannot hold fails here, leaf by leaf, as when the bounds were formatted while gathered.
        static bool Written(in Bound bound, DeltaType type, bool isMax) => type switch
        {
            PrimitiveType { Name: PrimitiveType.Float or PrimitiveType.Double } => double.IsFinite(bound.Float),
            PrimitiveType { Name: PrimitiveType.String } => !isMax || Utf16Cut(bound.Bytes) == bound.Bytes!.Length,
            PrimitiveType { Name: PrimitiveType.Date } => DayOf(bound) >= DateOnly.MinValue,
            PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } => InstantOf(bound) >= DateTime.MinValue,
            _ => true,
        };
    }

    /// <summary>The statistics of one column chunk, as the footer has them.</summary>
    private readonly record struct Statistic(bool Exists, byte[]? Min, byte[]? Max, long? NullCount);

    /// <summary>
    /// The column chunks of a footer, found by path: in its Thrift objects (no views made) first at the leaf's ordinal,
    /// where a valid file has it, else (a footer built from its views alone, or one whose chunks name other paths)
    /// by search.
    /// </summary>
    private readonly struct Chunks(ParquetMetadata metadata)
    {
        private readonly FileMetaData? _thrift = metadata.Thrift;

        public int GroupCount => _thrift?.RowGroups.Length ?? metadata.RowGroups.Count;

        public long Rows(int group) => _thrift is { } thrift ? thrift.RowGroups[group].NumRows : metadata.RowGroups[group].RowCount;

        /// <summary>The chunk of the leaf in the group; false when the group has none.</summary>
        public bool TryGet(int group, int ordinal, ReadOnlySpan<string> path, out Statistic statistic)
        {
            if (_thrift is { } thrift)
            {
                ColumnChunk[] columns = thrift.RowGroups[group].Columns;
                if ((uint)ordinal < (uint)columns.Length && columns[ordinal].MetaData is { } meta && Same(meta.PathInSchema, path))
                    return Found(meta, out statistic);
                foreach (ColumnChunk chunk in columns)
                {
                    if (chunk.MetaData is { } other && Same(other.PathInSchema, path)) return Found(other, out statistic);
                }
            }
            else
            {
                foreach (ParquetColumnChunk chunk in metadata.RowGroups[group].Columns)
                {
                    if (!Same(chunk.Path, path)) continue;
                    ParquetColumnStatistics? s = chunk.Statistics;
                    statistic = new Statistic(true, s?.Min, s?.Max, s?.NullCount);
                    return true;
                }
            }
            statistic = default;
            return false;

            static bool Found(ColumnMetaData meta, out Statistic statistic)
            {
                Statistics? s = meta.Statistics;
                statistic = new Statistic(true, s?.MinValue, s?.MaxValue, s?.NullCount);
                return true;
            }

            static bool Same(IReadOnlyList<string> a, ReadOnlySpan<string> b)
            {
                if (a.Count != b.Length) return false;
                for (int i = 0; i < b.Length; i++)
                {
                    if (a[i] != b[i]) return false;
                }
                return true;
            }
        }
    }

    /// <summary>
    /// One section of the stats (minimum, maximum or null count of the leaves that have one), nested under the structs
    /// of the leaves: a struct's object only holds what is written for its fields, and is left out when that is
    /// nothing.
    /// </summary>
    private static void Section(Utf8JsonWriter json, ReadOnlySpan<byte> name, StatisticsLeaves leaves, LeafStatistics[] results, Part part)
    {
        json.WriteStartObject(name);
        if (leaves.Contiguous)
        {
            // The leaves of a struct follow one another: open its object at its first written leaf, close it after
            // its last.
            int open = 0, previous = -1;
            for (int i = 0; i < leaves.Count; i++)
            {
                if (!Has(results[i], part)) continue;
                int[] prefixes = leaves.Prefixes[i];
                int depth = prefixes.Length - 1, common = 0;
                if (previous >= 0)
                {
                    int[] before = leaves.Prefixes[previous];
                    while (common < open && common < depth && before[common] == prefixes[common]) common++;
                }
                for (; open > common; open--) json.WriteEndObject();
                for (; open < depth; open++) json.WriteStartObject(leaves.Names[i][open]);
                json.WritePropertyName(leaves.Names[i][depth]);
                Value(json, leaves.Types[i], results[i], part);
                previous = i;
            }
            for (; open > 0; open--) json.WriteEndObject();
        }
        else
        {
            List<int> written = [];
            for (int i = 0; i < leaves.Count; i++)
            {
                if (Has(results[i], part)) written.Add(i);
            }
            Nest(json, leaves, results, part, written, 0);
        }
        json.WriteEndObject();

        // Leaves in any order: the values under one struct grouped under its first leaf's place.
        static void Nest(Utf8JsonWriter json, StatisticsLeaves leaves, LeafStatistics[] results, Part part, List<int> items, int depth)
        {
            foreach (IGrouping<int, int> group in items.GroupBy(i => leaves.Prefixes[i][depth]))
            {
                int first = group.First();
                json.WritePropertyName(leaves.Names[first][depth]);
                List<int> members = [.. group];
                if (members.Count == 1 && leaves.Prefixes[first].Length == depth + 1)
                {
                    Value(json, leaves.Types[first], results[first], part);
                    continue;
                }
                json.WriteStartObject();
                Nest(json, leaves, results, part, members, depth + 1);
                json.WriteEndObject();
            }
        }

        static bool Has(in LeafStatistics result, Part part) => part switch
        {
            Part.Min => result.HasMin,
            Part.Max => result.HasMax,
            _ => result.HasNullCount,
        };

        static void Value(Utf8JsonWriter json, DeltaType type, in LeafStatistics result, Part part)
        {
            switch (part)
            {
                case Part.Min: Write(json, result.Min, type, isMax: false); break;
                case Part.Max: Write(json, result.Max, type, isMax: true); break;
                default: json.WriteNumberValue(result.NullCount); break;
            }
        }

        // Writes a bound as the log holds it.
        static void Write(Utf8JsonWriter json, in Bound bound, DeltaType type, bool isMax)
        {
            Span<byte> text = stackalloc byte[64];
            int length;
            switch (type)
            {
                case DecimalType d:
                    if (bound.Kind == BoundKind.LongDecimal)
                    {
                        json.WriteRawValue(WideDecimalText(bound.Big, d.Scale));
                        return;
                    }
                    length = DecimalText(bound.Integer, d.Scale, text);
                    break;
                case PrimitiveType { Name: PrimitiveType.Float }:
                    ((float)bound.Float).TryFormat(text, out length, "R", CultureInfo.InvariantCulture);
                    break;
                case PrimitiveType { Name: PrimitiveType.Double }:
                    bound.Float.TryFormat(text, out length, "R", CultureInfo.InvariantCulture);
                    break;
                case PrimitiveType { Name: PrimitiveType.Date }:
                    text[0] = (byte)'"';
                    DayOf(bound).TryFormat(text[1..], out length, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                    text[++length] = (byte)'"';
                    length++;
                    break;
                case PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } p:
                {
                    text[0] = (byte)'"';
                    InstantOf(bound).TryFormat(text[1..], out length, p.Name == PrimitiveType.Timestamp ? "yyyy-MM-dd'T'HH:mm:ss.fff'Z'" : "yyyy-MM-dd'T'HH:mm:ss.fff", CultureInfo.InvariantCulture);
                    text[++length] = (byte)'"';
                    length++;
                    break;
                }
                case PrimitiveType { Name: PrimitiveType.String }:
                    json.WriteStringValue(bound.Bytes.AsSpan(0, Utf16Cut(bound.Bytes)));
                    return;
                default:
                    json.WriteNumberValue(checked((long)bound.Integer));
                    return;
            }
            json.WriteRawValue(text[..length]); // validated: a float column with a double bound beyond its range gives Infinity, refused as before

            // An unscaled decimal of at most 128 bits as text.
            static int DecimalText(Int128 unscaled, int scale, Span<byte> text)
            {
                Span<byte> digits = stackalloc byte[40];
                UInt128 magnitude = unscaled < 0 ? (UInt128)(-(unscaled + 1)) + 1 : (UInt128)unscaled;
                magnitude.TryFormat(digits, out int count, default, CultureInfo.InvariantCulture);
                int padded = Math.Max(count, scale + 1), at = 0;
                if (unscaled < 0) text[at++] = (byte)'-';
                Span<byte> whole = text.Slice(at, padded);
                whole[..(padded - count)].Fill((byte)'0');
                digits[..count].CopyTo(whole[(padded - count)..]);
                at += padded;
                if (scale == 0) return at;
                whole[^scale..].CopyTo(text[(at - scale + 1)..]);
                text[at - scale] = (byte)'.';
                return at + 1;
            }

            // A wider decimal (beyond the 38 digits of the table's type: kept as written).
            static string WideDecimalText(BigInteger unscaled, int scale)
            {
                string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(scale + 1, '0');
                string text = scale == 0 ? digits : digits[..^scale] + "." + digits[^scale..];
                return unscaled.Sign < 0 ? "-" + text : text;
            }
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
    public static IReadOnlyList<StructField[]> IndexedLeaves(IReadOnlyList<StructField> dataFields, IReadOnlyDictionary<string, string>? configuration) =>
        IndexedLeaves(dataFields, configuration, includeContainers: false);

    /// <summary>
    /// <see cref="IndexedLeaves(IReadOnlyList{StructField}, IReadOnlyDictionary{string, string}?)"/>, with arrays and
    /// maps kept when <paramref name="includeContainers"/>: Spark counts their nulls (a checkpoint's
    /// <c>nullCount</c> lists them) though it keeps no bounds for them.
    /// </summary>
    internal static IReadOnlyList<StructField[]> IndexedLeaves(
        IReadOnlyList<StructField> dataFields, IReadOnlyDictionary<string, string>? configuration, bool includeContainers)
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
        return includeContainers ? leaves : [.. leaves.Where(p => p[^1].Type is not (ArrayType or MapType))];
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
        IReadOnlyList<ParquetSchemaElement> schema = metadata.Schema;
        if (schema.Count == 0) return result;
        var chunks = new Chunks(metadata);
        int groups = chunks.GroupCount;
        int at = 1, ordinal = 0;
        for (int child = 0; child < schema[0].ChildCount && at < schema.Count; child++)
        {
            ParquetSchemaElement element = schema[at];
            int next = Skip(schema, at, ref ordinal);
            if (element.ChildCount == 0 && element.Repetition != 2 && !result.ContainsKey(element.Name))
            {
                long? sum = 0;
                for (int g = 0; g < groups && sum is not null; g++)
                {
                    sum = chunks.TryGet(g, ordinal - 1, [element.Name], out Statistic chunk) && chunk.NullCount is long n ? sum + n : null;
                }
                if (sum is long total) result[element.Name] = total;
            }
            at = next;
        }
        return result;

        // The index after the subtree at `at`, counting its leaves.
        static int Skip(IReadOnlyList<ParquetSchemaElement> schema, int at, ref int leaves)
        {
            int children = schema[at].ChildCount;
            if (children == 0) leaves++;
            at++;
            for (int i = 0; i < children; i++) at = Skip(schema, at, ref leaves);
            return at;
        }
    }

    // ------------------------------------------------------------------
    // Values
    // ------------------------------------------------------------------

    private enum BoundKind : byte { None, Integer, Float, Utf8, LongDecimal }

    /// <summary>
    /// A decoded bound: an integer (also dates, timestamps in microseconds, decimals unscaled), a float, UTF-8 bytes,
    /// or the big-endian bytes of a decimal wider than 128 bits; the bytes are the footer's own.
    /// </summary>
    private readonly struct Bound
    {
        public BoundKind Kind { get; init; }
        public Int128 Integer { get; init; }
        public double Float { get; init; }
        public byte[]? Bytes { get; init; }

        public BigInteger Big => Kind == BoundKind.LongDecimal ? new BigInteger(Bytes, isUnsigned: false, isBigEndian: true) : (BigInteger)Integer;
    }

    /// <summary>The day of a date bound (days since the epoch).</summary>
    /// <exception cref="ArgumentOutOfRangeException">The day is beyond the dates .NET holds.</exception>
    private static DateOnly DayOf(in Bound bound) =>
        DateOnly.FromDayNumber(DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + checked((int)bound.Integer));

    /// <summary>The instant of a timestamp bound (microseconds since the epoch), cut down to milliseconds (readers allow for the cut).</summary>
    /// <exception cref="OverflowException">The microseconds are beyond a long.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The instant is beyond the times .NET holds.</exception>
    private static DateTime InstantOf(in Bound bound)
    {
        long micros = checked((long)bound.Integer);
        long millis = micros / 1000 - (micros % 1000 < 0 ? 1 : 0); // floor
        return DateTime.UnixEpoch.AddMilliseconds(millis);
    }

    /// <summary>
    /// The UTF-8 bytes of the first <see cref="StringPrefixLength"/> UTF-16 code units of valid UTF-8 text (all of
    /// it when shorter), without splitting a surrogate pair: a cut minimum is still a lower bound.
    /// </summary>
    private static int Utf16Cut(ReadOnlySpan<byte> utf8)
    {
        int units = 0, at = 0;
        while (at < utf8.Length)
        {
            byte lead = utf8[at];
            (int length, int width) = lead switch
            {
                < 0x80 => (1, 1),
                < 0xE0 => (2, 1),
                < 0xF0 => (3, 1),
                _ => (4, 2),
            };
            if (units + width > StringPrefixLength) break;
            units += width;
            at += length;
        }
        return at;
    }

}

/// <summary>
/// The leaves a write gives statistics for, prepared once for all its files: their physical paths, types, JSON
/// names, and which struct each level belongs to (whether the leaves of each struct follow one another).
/// </summary>
internal sealed class StatisticsLeaves
{
    /// <summary>Prepares <paramref name="leaves"/> (paths of fields from a table column down; struct, array and map leaves are left out).</summary>
    public StatisticsLeaves(IReadOnlyList<StructField[]> leaves)
    {
        StructField[][] kept = [.. leaves.Where(p => p[^1].Type is not (StructType or ArrayType or MapType))];
        Count = kept.Length;
        Paths = new string[Count][];
        Types = new DeltaType[Count];
        Names = new JsonEncodedText[Count][];
        Prefixes = new int[Count][];
        var ids = new Dictionary<(int Parent, string Name), int>();
        HashSet<int> started = [];
        Contiguous = true;
        for (int i = 0; i < Count; i++)
        {
            StructField[] leaf = kept[i];
            Paths[i] = [.. leaf.Select(f => f.PhysicalName)];
            Types[i] = leaf[^1].Type;
            Names[i] = [.. Paths[i].Select(n => JsonEncodedText.Encode(n, ActionWriter.JsonOptions.Encoder))];
            int[] prefixes = new int[leaf.Length];
            int parent = -1;
            for (int d = 0; d < leaf.Length; d++)
            {
                if (!ids.TryGetValue((parent, Paths[i][d]), out int id)) ids[(parent, Paths[i][d])] = id = ids.Count;
                prefixes[d] = parent = id;
            }
            // Each struct's leaves in one run: a struct (or leaf) that starts here was not seen before.
            int[]? before = i > 0 ? Prefixes[i - 1] : null;
            for (int d = 0; d < prefixes.Length; d++)
            {
                if (before is not null && d < before.Length && before[d] == prefixes[d]) continue;
                if (!started.Add(prefixes[d])) Contiguous = false;
            }
            Prefixes[i] = prefixes;
        }
    }

    /// <summary>The number of leaves.</summary>
    public int Count { get; }

    /// <summary>Each leaf's path of physical names.</summary>
    public string[][] Paths { get; }

    /// <summary>Each leaf's type.</summary>
    public DeltaType[] Types { get; }

    /// <summary>Each leaf's path of names as JSON property names.</summary>
    public JsonEncodedText[][] Names { get; }

    /// <summary>Each leaf's path as ids of its structs (and of itself, last): equal ids, the same struct.</summary>
    public int[][] Prefixes { get; }

    /// <summary>Whether the leaves of every struct follow one another (as in schema order).</summary>
    public bool Contiguous { get; }
}
