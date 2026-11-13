using System.Text.Json;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// Writes checkpoint rows, given in their JSON shape, as real Parquet checkpoint files: every action column of the
/// schema is there, the ones a row does not name are null. Maps may be JSON objects or arrays of key/value entries.
/// </summary>
internal static class CheckpointParquet
{
    public static ParquetLeaf Str(string name) => new(name, Repetition.Optional, PhysicalType.ByteArray, Utf8: true);
    public static ParquetLeaf I64(string name) => new(name, Repetition.Optional, PhysicalType.Int64);
    public static ParquetLeaf I32(string name) => new(name, Repetition.Optional, PhysicalType.Int32);
    public static ParquetLeaf Bool(string name) => new(name, Repetition.Optional, PhysicalType.Boolean);
    public static ParquetStringMap Map(string name) => new(name, Repetition.Optional);
    public static ParquetList Strings(string name) => new(name, Repetition.Optional, Str("element"));
    public static ParquetStruct Struct(string name, params ParquetNode[] children) => new(name, Repetition.Optional, children);

    private static ParquetStruct DeletionVector() =>
        Struct("deletionVector", Str("storageType"), Str("pathOrInlineDv"), I32("offset"), I32("sizeInBytes"), I64("cardinality"));

    /// <summary>A classic checkpoint's columns, as Spark writes them.</summary>
    /// <param name="statsText">Whether <c>add</c> has the text <c>stats</c> column.</param>
    /// <param name="addExtra">More <c>add</c> fields (<c>stats_parsed</c>, <c>partitionValues_parsed</c>).</param>
    public static List<ParquetNode> Classic(bool statsText = true, params ParquetNode[] addExtra) =>
    [
        Struct("txn", Str("appId"), I64("version"), I64("lastUpdated")),
        Struct("add",
        [
            Str("path"), Map("partitionValues"), I64("size"), I64("modificationTime"), Bool("dataChange"), Map("tags"),
            DeletionVector(), I64("baseRowId"), I64("defaultRowCommitVersion"), Str("clusteringProvider"),
            .. statsText ? [Str("stats")] : (ParquetNode[])[],
            .. addExtra,
        ]),
        Struct("remove", Str("path"), I64("deletionTimestamp"), Bool("dataChange"), Bool("extendedFileMetadata"), Map("partitionValues"),
            I64("size"), DeletionVector(), I64("baseRowId"), I64("defaultRowCommitVersion")),
        Struct("metaData", Str("id"), Str("name"), Str("description"), Struct("format", Str("provider"), Map("options")),
            Str("schemaString"), Strings("partitionColumns"), Map("configuration"), I64("createdTime")),
        Struct("protocol", I32("minReaderVersion"), I32("minWriterVersion"), Strings("readerFeatures"), Strings("writerFeatures")),
        Struct("domainMetadata", Str("domain"), Str("configuration"), Bool("removed")),
    ];

    /// <summary>A V2 checkpoint's top-level columns (Parquet form): the classic ones, its metadata and sidecars.</summary>
    public static List<ParquetNode> V2() =>
    [
        Struct("checkpointMetadata", I64("version"), Map("tags")),
        Struct("sidecar", Str("path"), I64("sizeInBytes"), I64("modificationTime"), Map("tags")),
        .. Classic(),
    ];

    /// <summary>The Parquet file of the rows under <paramref name="schema"/>.</summary>
    public static byte[] Write(IReadOnlyList<ParquetNode> schema, params string[] rows) =>
        MinimalParquetWriter.Write(schema, [.. rows.Select(row => Row(schema, row))]);

    /// <summary>Writes a classic checkpoint file.</summary>
    public static MemoryFileSystem PutCheckpoint(this MemoryFileSystem fs, string path, params string[] rows) =>
        fs.PutBytes(path, Write(Classic(), rows));

    private static IReadOnlyDictionary<string, object?> Row(IReadOnlyList<ParquetNode> schema, string json)
    {
        using JsonDocument doc = JsonDocument.Parse(json);
        return schema.ToDictionary(c => c.Name, c => doc.RootElement.TryGetProperty(c.Name, out JsonElement v) ? Value(c, v) : null);
    }

    private static object? Value(ParquetNode node, JsonElement e) => e.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined ? null : node switch
    {
        ParquetStruct s => s.Children.ToDictionary(c => c.Name, c => e.TryGetProperty(c.Name, out JsonElement v) ? Value(c, v) : null),
        ParquetStringMap when e.ValueKind == JsonValueKind.Object =>
            e.EnumerateObject().Select(p => new KeyValuePair<string, string?>(p.Name, p.Value.GetString())).ToList(),
        ParquetStringMap => e.EnumerateArray()
            .Select(x => new KeyValuePair<string, string?>(x.GetProperty("key").GetString()!, x.TryGetProperty("value", out JsonElement v) ? v.GetString() : null))
            .ToList(),
        ParquetList l => e.EnumerateArray().Select(x => Value(l.Element, x)).ToList(),
        ParquetLeaf { Type: PhysicalType.Int64 } => e.GetInt64(),
        ParquetLeaf { Type: PhysicalType.Int32 } => e.GetInt32(),
        ParquetLeaf { Type: PhysicalType.Boolean } => e.GetBoolean(),
        ParquetLeaf { Type: PhysicalType.Double } => e.ValueKind == JsonValueKind.String
            ? double.Parse(e.GetString()!, System.Globalization.CultureInfo.InvariantCulture)
            : e.GetDouble(),
        ParquetLeaf { Type: PhysicalType.Float } => e.GetSingle(),
        ParquetLeaf => e.GetString(),
        _ => throw new NotSupportedException(node.GetType().Name),
    };
}
