using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Files.Parquet;

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
internal sealed partial class CheckpointStatistics
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
        List<ParquetNode> children = [Int64Leaf("numRecords")];
        if (_bounds.Length > 0)
        {
            children.Add(Nest("minValues", [.. _bounds.Select(l => (l.Path, (ParquetNode)l.Node))]));
            children.Add(Nest("maxValues", [.. _bounds.Select(l => (l.Path, (ParquetNode)l.Node))]));
        }
        if (_nullCounts.Length > 0) children.Add(Nest("nullCount", [.. _nullCounts.Select(p => (p, (ParquetNode)Int64Leaf(p[^1])))]));
        if (_tightBounds) children.Add(new ParquetLeaf("tightBounds", Repetition.Optional, PhysicalType.Boolean));
        return new ParquetStruct("stats_parsed", Repetition.Optional, children);
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
        List<ParquetNode> children = [];
        foreach (var group in leaves.GroupBy(l => l.Path[0]))
        {
            (string[] Path, ParquetNode Leaf)[] items = [.. group];
            children.Add(items is [{ Path.Length: 1 } single]
                ? single.Leaf
                : Nest(group.Key, [.. items.Select(i => (i.Path[1..], i.Leaf))]));
        }
        return new ParquetStruct(name, Repetition.Optional, children);
    }

    private static readonly int EpochDay = new DateOnly(1970, 1, 1).DayNumber;

    private static bool IsOn(MetadataAction metadata, string key) =>
        !metadata.Configuration.TryGetValue(key, out string? value) || !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);
}
