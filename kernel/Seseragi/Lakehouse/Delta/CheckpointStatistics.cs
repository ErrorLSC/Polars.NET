using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The typed columns a classic checkpoint carries beside the text ones, as delta-spark writes them
/// (<c>delta.checkpoint.writeStatsAsStruct</c>, on by default): <c>add.stats_parsed</c>, the file statistics as a
/// struct in the table's own types, and <c>add.partitionValues_parsed</c>, the partition values typed. Readers then
/// skip files without parsing JSON. Fields are named by physical name.
/// </summary>
/// <remarks>
/// The struct follows the table's current schema and statistics columns: <c>minValues</c> / <c>maxValues</c> hold
/// the indexed leaves of a type with bounds, <c>nullCount</c> every indexed leaf (booleans, binaries, arrays and maps
/// too), <c>tightBounds</c> comes with deletion vectors; an empty struct is left out. Each file's JSON statistics are
/// read into it; a value missing there, or not readable as the column's type, is null.
/// </remarks>
internal sealed class CheckpointStatistics
{
    /// <summary>The table property that writes the text <c>stats</c> column (on by default).</summary>
    public const string WriteJsonKey = "delta.checkpoint.writeStatsAsJson";

    /// <summary>The table property that writes <c>stats_parsed</c> and <c>partitionValues_parsed</c> (on by default).</summary>
    public const string WriteStructKey = "delta.checkpoint.writeStatsAsStruct";

    private readonly Leaf[] _bounds;
    private readonly string[][] _nullCounts;
    private readonly bool _tightBounds;
    private readonly Leaf[] _partitions;

    /// <summary>A typed column: its path of physical names, its Delta type and its Parquet leaf.</summary>
    private sealed record Leaf(string[] Path, DeltaType Type, ParquetLeaf Node);

    private CheckpointStatistics(Leaf[] bounds, string[][] nullCounts, bool tightBounds, Leaf[] partitions)
    {
        _bounds = bounds;
        _nullCounts = nullCounts;
        _tightBounds = tightBounds;
        _partitions = partitions;
    }

    /// <summary>Whether the checkpoint has the text <c>stats</c> column.</summary>
    public static bool WritesJson(MetadataAction metadata) => IsOn(metadata, WriteJsonKey);

    /// <summary>The typed columns of a table, or null when the table writes none (<see cref="WriteStructKey"/> false).</summary>
    public static CheckpointStatistics? For(MetadataAction metadata, ProtocolAction protocol)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(protocol);
        if (!IsOn(metadata, WriteStructKey)) return null;

        StructType schema = StructType.Parse(metadata.SchemaString);
        var partitionNames = new HashSet<string>(metadata.PartitionColumns, StringComparer.Ordinal);
        StructField[] dataFields = [.. schema.Fields.Where(f => !partitionNames.Contains(f.Name))];
        IReadOnlyList<StructField[]> indexed = FileStatistics.IndexedLeaves(dataFields, metadata.Configuration, includeContainers: true);

        Leaf[] bounds = [.. indexed.Where(p => Clustering.HasStatistics(p[^1].Type)).Select(p => LeafOf(p, p[^1].Type))];
        string[][] nullCounts = [.. indexed.Select(Physical)];
        bool tightBounds = (protocol.ReaderFeatures ?? []).Contains(TableFeatures.DeletionVectors);
        Leaf[] partitions = [.. metadata.PartitionColumns
            .Select(name => schema.Field(name) ?? throw new DeltaProtocolException($"The partition column '{name}' is not in the schema."))
            .Select(f => LeafOf([f], f.Type))];
        return new CheckpointStatistics(bounds, nullCounts, tightBounds, partitions);
    }

    /// <summary>The <c>partitionValues_parsed</c> node; null for a table without partition columns.</summary>
    public ParquetStruct? PartitionNode() =>
        _partitions.Length == 0 ? null : new ParquetStruct("partitionValues_parsed", Repetition.Optional, [.. _partitions.Select(l => l.Node)]);

    /// <summary>The <c>stats_parsed</c> node.</summary>
    public ParquetStruct StatsNode()
    {
        var children = new List<ParquetNode> { Int64Leaf("numRecords") };
        if (_bounds.Length > 0)
        {
            children.Add(Nest("minValues", [.. _bounds.Select(l => (l.Path, (ParquetNode)l.Node))]));
            children.Add(Nest("maxValues", [.. _bounds.Select(l => (l.Path, (ParquetNode)l.Node))]));
        }
        if (_nullCounts.Length > 0) children.Add(Nest("nullCount", [.. _nullCounts.Select(p => (p, (ParquetNode)Int64Leaf(p[^1])))]));
        if (_tightBounds) children.Add(new ParquetLeaf("tightBounds", Repetition.Optional, PhysicalType.Boolean));
        return new ParquetStruct("stats_parsed", Repetition.Optional, children);
    }

    /// <summary>The <c>partitionValues_parsed</c> value of a file: its partition values, typed.</summary>
    public Dictionary<string, object?>? PartitionValue(AddAction add)
    {
        ArgumentNullException.ThrowIfNull(add);
        if (_partitions.Length == 0) return null;
        var value = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (Leaf leaf in _partitions)
        {
            string? text = add.PartitionValues is { } values && values.TryGetValue(leaf.Path[0], out string? v) ? v : null;
            value[leaf.Path[0]] = string.IsNullOrEmpty(text) ? null : FromPartitionText(text, leaf);
        }
        return value;
    }

    /// <summary>The <c>stats_parsed</c> value of a file, read from its JSON statistics; null when it has none.</summary>
    public Dictionary<string, object?>? StatsValue(string? stats)
    {
        if (string.IsNullOrEmpty(stats)) return null;
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(stats);
        }
        catch (JsonException)
        {
            return null;
        }
        using (doc)
        {
            JsonElement root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return null;
            var value = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["numRecords"] = root.TryGetProperty("numRecords", out JsonElement n) && n.TryGetInt64(out long rows) ? rows : null,
            };
            if (_bounds.Length > 0)
            {
                value["minValues"] = NestValues(root, "minValues", _bounds.Select(l => (l.Path, (Func<JsonElement, object?>)(e => FromStat(e, l)))));
                value["maxValues"] = NestValues(root, "maxValues", _bounds.Select(l => (l.Path, (Func<JsonElement, object?>)(e => FromStat(e, l)))));
            }
            if (_nullCounts.Length > 0)
                value["nullCount"] = NestValues(root, "nullCount",
                    _nullCounts.Select(p => (p, (Func<JsonElement, object?>)(e => e.ValueKind == JsonValueKind.Number && e.TryGetInt64(out long c) ? c : null))));
            if (_tightBounds)
                value["tightBounds"] = root.TryGetProperty("tightBounds", out JsonElement t) && t.ValueKind is JsonValueKind.True or JsonValueKind.False
                    ? t.GetBoolean()
                    : null;
            return value;
        }
    }

    // ------------------------------------------------------------------ schema

    private static string[] Physical(StructField[] path) => [.. path.Select(f => f.PhysicalName)];

    private static Leaf LeafOf(StructField[] path, DeltaType type)
    {
        string name = path[^1].PhysicalName;
        ParquetLeaf node = type switch
        {
            DecimalType d when d.Precision <= 9 => Decimal(name, PhysicalType.Int32, d),
            DecimalType d when d.Precision <= 18 => Decimal(name, PhysicalType.Int64, d),
            DecimalType d => Decimal(name, PhysicalType.FixedLenByteArray, d) with { TypeLength = DecimalBytes(d.Precision) },
            PrimitiveType p => p.Name switch
            {
                PrimitiveType.Byte => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int8),
                PrimitiveType.Short => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int16),
                PrimitiveType.Integer => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Int32),
                PrimitiveType.Long => Int64Leaf(name),
                PrimitiveType.Float => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Float),
                PrimitiveType.Double => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Double),
                PrimitiveType.Date => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Date),
                // Spark writes INT96 here; INT64 microseconds is the modern encoding every reader takes for a timestamp.
                PrimitiveType.Timestamp => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.TimestampMicros),
                PrimitiveType.TimestampNtz => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.LocalTimestampMicros),
                PrimitiveType.Boolean => new ParquetLeaf(name, Repetition.Optional, PhysicalType.Boolean),
                PrimitiveType.Binary => new ParquetLeaf(name, Repetition.Optional, PhysicalType.ByteArray),
                _ => new ParquetLeaf(name, Repetition.Optional, PhysicalType.ByteArray, Utf8: true),
            },
            _ => throw new DeltaProtocolException($"The column {name} of type {type} has no typed statistics."),
        };
        return new Leaf(Physical(path), type, node);
    }

    private static ParquetLeaf Int64Leaf(string name) => new(name, Repetition.Optional, PhysicalType.Int64);

    private static ParquetLeaf Decimal(string name, PhysicalType physical, DecimalType d) =>
        new(name, Repetition.Optional, physical, Annotation: Annotation.Decimal, Precision: d.Precision, Scale: d.Scale);

    /// <summary>The fewest bytes whose two's complement holds every unscaled value of the precision (Spark's minBytesForPrecision).</summary>
    private static int DecimalBytes(int precision)
    {
        BigInteger largest = BigInteger.Pow(10, precision);
        int bytes = 1;
        while (BigInteger.Pow(2, 8 * bytes - 1) < largest) bytes++;
        return bytes;
    }

    /// <summary>A struct named <paramref name="name"/> holding the leaves at their paths, nested structs in first-seen order.</summary>
    private static ParquetStruct Nest(string name, (string[] Path, ParquetNode Leaf)[] leaves)
    {
        var children = new List<ParquetNode>();
        foreach (var group in leaves.GroupBy(l => l.Path[0]))
        {
            (string[] Path, ParquetNode Leaf)[] items = [.. group];
            children.Add(items is [{ Path.Length: 1 } single]
                ? single.Leaf
                : Nest(group.Key, [.. items.Select(i => (i.Path[1..], i.Leaf))]));
        }
        return new ParquetStruct(name, Repetition.Optional, children);
    }

    // ------------------------------------------------------------------ values

    /// <summary>
    /// The struct of values under <paramref name="name"/>, each leaf read from the JSON at its path, as Spark's
    /// <c>from_json</c> reads it: a struct the JSON lacks (or holds as something else) is null as a whole.
    /// </summary>
    private static Dictionary<string, object?>? NestValues(JsonElement root, string name, IEnumerable<(string[] Path, Func<JsonElement, object?> Read)> leaves) =>
        root.TryGetProperty(name, out JsonElement section) ? Fill(section, [.. leaves]) : null;

    private static Dictionary<string, object?>? Fill(JsonElement element, (string[] Path, Func<JsonElement, object?> Read)[] leaves)
    {
        if (element.ValueKind != JsonValueKind.Object) return null;
        var value = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var group in leaves.GroupBy(l => l.Path[0]))
        {
            bool present = element.TryGetProperty(group.Key, out JsonElement child) && child.ValueKind != JsonValueKind.Null;
            value[group.Key] = group.ToArray() is [{ Path.Length: 1 } leaf]
                ? present ? leaf.Read(child) : null
                : present ? Fill(child, [.. group.Select(l => (l.Path[1..], l.Read))]) : null;
        }
        return value;
    }

    /// <summary>A statistic as the column's value; null when the JSON does not hold one of that type.</summary>
    private static object? FromStat(JsonElement e, Leaf leaf) =>
        e.ValueKind switch
        {
            JsonValueKind.Number => FromNumber(e, leaf),
            JsonValueKind.String => FromText(e.GetString()!, leaf, partition: false),
            _ => null,
        };

    private static object? FromNumber(JsonElement e, Leaf leaf) => leaf.Type switch
    {
        DecimalType d => Unscaled(e.GetRawText(), d, leaf),
        PrimitiveType { Name: PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer } => e.TryGetInt32(out int i) ? i : null,
        PrimitiveType { Name: PrimitiveType.Long } => e.TryGetInt64(out long l) ? l : null,
        // Negative zero becomes zero, as Spark's typed statistics hold it (the two compare equal).
        PrimitiveType { Name: PrimitiveType.Float } => e.TryGetSingle(out float f) ? f + 0f : null,
        PrimitiveType { Name: PrimitiveType.Double } => e.TryGetDouble(out double d) ? d + 0d : null,
        _ => null,
    };

    private static object? FromPartitionText(string text, Leaf leaf) => leaf.Type switch
    {
        PrimitiveType { Name: PrimitiveType.Boolean } => bool.TryParse(text, out bool b) ? b : null,
        PrimitiveType { Name: PrimitiveType.Byte or PrimitiveType.Short or PrimitiveType.Integer } =>
            int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int i) ? i : null,
        PrimitiveType { Name: PrimitiveType.Long } => long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l) ? l : null,
        PrimitiveType { Name: PrimitiveType.Float } => float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float f) ? f : null,
        PrimitiveType { Name: PrimitiveType.Double } => double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) ? d : null,
        PrimitiveType { Name: PrimitiveType.Binary } => null, // the protocol's escaped byte text; readers take the partitionValues map
        _ => FromText(text, leaf, partition: true),
    };

    /// <summary>Text as the column's value: strings, decimals, dates and timestamps (as the log and partition paths write them).</summary>
    private static object? FromText(string text, Leaf leaf, bool partition) => leaf.Type switch
    {
        DecimalType d => Unscaled(text, d, leaf),
        PrimitiveType { Name: PrimitiveType.String } => text,
        PrimitiveType { Name: PrimitiveType.Date } =>
            DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly date) ? date.DayNumber - EpochDay : null,
        PrimitiveType { Name: PrimitiveType.Timestamp or PrimitiveType.TimestampNtz } => Micros(text),
        // A float column widened to double keeps its old statistics as text in some writers.
        PrimitiveType { Name: PrimitiveType.Double } when !partition =>
            double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) ? d : null,
        _ => null,
    };

    private static readonly int EpochDay = new DateOnly(1970, 1, 1).DayNumber;

    /// <summary>
    /// Microseconds since the epoch of a timestamp (with an offset: the instant; without: the local date-time read as
    /// UTC, as timestamp_ntz is stored); a date is its midnight (statistics written before a date → timestamp_ntz widening).
    /// </summary>
    private static long? Micros(string text)
    {
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset ts))
            return (ts.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerMicrosecond;
        return null;
    }

    /// <summary>
    /// The unscaled value of decimal text (<c>-12.5</c> at scale 2 is -1250): a number for INT32 / INT64, big-endian
    /// two's complement bytes for a fixed-length array; null when the text has more fraction digits than the scale or
    /// more digits than the precision.
    /// </summary>
    private static object? Unscaled(string text, DecimalType type, Leaf leaf)
    {
        string digits = text.Trim();
        bool negative = digits.StartsWith('-');
        digits = digits.TrimStart('-', '+');
        if (digits.Contains('e', StringComparison.OrdinalIgnoreCase))
        {
            if (!decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal plain)) return null;
            digits = Math.Abs(plain).ToString(CultureInfo.InvariantCulture);
        }
        int dot = digits.IndexOf('.');
        string whole = dot < 0 ? digits : digits[..dot];
        string fraction = dot < 0 ? "" : digits[(dot + 1)..].TrimEnd('0');
        if (fraction.Length > type.Scale || !(whole + fraction).All(char.IsAsciiDigit)) return null;
        var unscaled = BigInteger.Parse(whole.Length == 0 ? "0" : whole, CultureInfo.InvariantCulture) * BigInteger.Pow(10, type.Scale)
            + (fraction.Length == 0 ? BigInteger.Zero : BigInteger.Parse(fraction, CultureInfo.InvariantCulture) * BigInteger.Pow(10, type.Scale - fraction.Length));
        if (BigInteger.Abs(unscaled) >= BigInteger.Pow(10, type.Precision)) return null;
        if (negative) unscaled = -unscaled;
        return leaf.Node.Type switch
        {
            PhysicalType.Int32 => (int)unscaled,
            PhysicalType.Int64 => (long)unscaled,
            _ => BigEndian(unscaled, leaf.Node.TypeLength),
        };
    }

    private static byte[] BigEndian(BigInteger value, int length)
    {
        byte[] bytes = value.ToByteArray(isUnsigned: false, isBigEndian: true);
        if (bytes.Length == length) return bytes;
        var padded = new byte[length];
        if (value.Sign < 0) padded.AsSpan().Fill(0xFF);
        bytes.CopyTo(padded, length - bytes.Length);
        return padded;
    }

    private static bool IsOn(MetadataAction metadata, string key) =>
        !metadata.Configuration.TryGetValue(key, out string? value) || !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);
}
