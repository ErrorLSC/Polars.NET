using System.Globalization;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Writes classic single-file checkpoints: the whole state of a table at one version in one Parquet file, so a
/// reader does not replay every commit, plus the <c>_last_checkpoint</c> hint.
/// </summary>
public static class Checkpoints
{
    /// <summary>The table property giving how many commits lie between checkpoints.</summary>
    public const string IntervalKey = "delta.checkpointInterval";

    /// <summary>The table property giving how long removed files stay referenced (for time travel and vacuum).</summary>
    public const string TombstoneRetentionKey = "delta.deletedFileRetentionDuration";

    private const int DefaultInterval = 10;
    private static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(7);

    /// <summary>
    /// Whether a checkpoint is due at this snapshot's version: every <c>delta.checkpointInterval</c> commits (10 by
    /// default). Tables with V2 checkpoints are not checkpointed by this kernel yet.
    /// </summary>
    public static bool IsDue(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.Version <= 0) return false;
        if ((snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.V2Checkpoint)) return false;
        int interval = snapshot.Metadata.Configuration.TryGetValue(IntervalKey, out string? text)
            && int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n) && n > 0 ? n : DefaultInterval;
        return snapshot.Version % interval == 0;
    }

    /// <summary>Writes the checkpoint of the snapshot's version and points <c>_last_checkpoint</c> at it.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="snapshot">The state to write.</param>
    /// <param name="now">The clock that decides which tombstones have expired; the current time when null.</param>
    /// <exception cref="DeltaProtocolException">The table uses V2 checkpoints.</exception>
    public static void Write(IWritableFileSystem fileSystem, Snapshot snapshot, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(snapshot);
        if ((snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.V2Checkpoint))
            throw new DeltaProtocolException("The table uses V2 checkpoints, which this kernel does not write yet.");

        long expiredBefore = ((now ?? DateTimeOffset.UtcNow) - Retention(snapshot.Metadata)).ToUnixTimeMilliseconds();
        var rows = new List<IReadOnlyDictionary<string, object?>>
        {
            Row("protocol", Protocol(snapshot.Protocol)),
            Row("metaData", Metadata(snapshot.Metadata)),
        };
        rows.AddRange(snapshot.AppTransactions.Values.Select(t => Row("txn", Txn(t))));
        rows.AddRange(snapshot.DomainMetadata.Values.Select(d => Row("domainMetadata", Domain(d))));
        rows.AddRange(snapshot.Files.Select(a => Row("add", Add(a))));
        rows.AddRange(snapshot.Tombstones
            .Where(r => r.DeletionTimestamp is null || r.DeletionTimestamp >= expiredBefore)
            .Select(r => Row("remove", Remove(r))));

        byte[] parquet = MinimalParquetWriter.Write(Schema, rows);
        string path = $"{LogFile.LogDirectory}/{snapshot.Version:D20}.checkpoint.parquet";
        // The same version always gives the same checkpoint, so replacing one written concurrently is harmless.
        fileSystem.Overwrite(path, parquet);

        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("version", snapshot.Version);
            json.WriteNumber("size", rows.Count);
            json.WriteNumber("sizeInBytes", parquet.Length);
            json.WriteNumber("numOfAddFiles", snapshot.Files.Count);
            json.WriteEndObject();
        }
        // The hint only ever moves forward; a stale or missing one costs readers a longer listing, nothing more.
        if (LastCheckpointVersion(fileSystem) < snapshot.Version)
            fileSystem.Overwrite($"{LogFile.LogDirectory}/_last_checkpoint", stream.ToArray());
    }

    private static long LastCheckpointVersion(IFileSystem fileSystem)
    {
        try
        {
            byte[]? bytes = fileSystem.ReadAll($"{LogFile.LogDirectory}/_last_checkpoint");
            if (bytes is null) return -1;
            using JsonDocument doc = JsonDocument.Parse(bytes);
            return doc.RootElement.TryGetProperty("version", out JsonElement v) && v.TryGetInt64(out long version) ? version : -1;
        }
        catch (JsonException)
        {
            return -1;
        }
    }

    /// <summary>The tombstone retention: <c>interval N unit</c> as Delta writes it, 7 days when unset or unreadable.</summary>
    internal static TimeSpan Retention(MetadataAction metadata)
    {
        if (!metadata.Configuration.TryGetValue(TombstoneRetentionKey, out string? text)) return DefaultRetention;
        string[] parts = text.Trim().ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 3 && parts[0] == "interval" && double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double n))
        {
            TimeSpan? unit = parts[2].TrimEnd('s') switch
            {
                "millisecond" => TimeSpan.FromMilliseconds(1),
                "second" => TimeSpan.FromSeconds(1),
                "minute" => TimeSpan.FromMinutes(1),
                "hour" => TimeSpan.FromHours(1),
                "day" => TimeSpan.FromDays(1),
                "week" => TimeSpan.FromDays(7),
                _ => null,
            };
            if (unit is TimeSpan u) return u * n;
        }
        return DefaultRetention;
    }

    // ------------------------------------------------------------------
    // The checkpoint schema (the classic one; every action column is a nullable struct)
    // ------------------------------------------------------------------

    private static ParquetLeaf Str(string name) => new(name, Repetition.Optional, PhysicalType.ByteArray, Utf8: true);
    private static ParquetLeaf I64(string name) => new(name, Repetition.Optional, PhysicalType.Int64);
    private static ParquetLeaf I32(string name) => new(name, Repetition.Optional, PhysicalType.Int32);
    private static ParquetLeaf Bool(string name) => new(name, Repetition.Optional, PhysicalType.Boolean);
    private static ParquetStringMap Map(string name) => new(name, Repetition.Optional);
    private static ParquetList Strings(string name) => new(name, Repetition.Optional, Str("element"));
    private static ParquetStruct Struct(string name, params ParquetNode[] children) => new(name, Repetition.Optional, children);

    private static ParquetStruct DeletionVectorNode() => Struct("deletionVector",
        Str("storageType"), Str("pathOrInlineDv"), I32("offset"), I32("sizeInBytes"), I64("cardinality"));

    private static readonly IReadOnlyList<ParquetNode> Schema =
    [
        Struct("txn", Str("appId"), I64("version"), I64("lastUpdated")),
        Struct("add", Str("path"), Map("partitionValues"), I64("size"), I64("modificationTime"), Bool("dataChange"), Str("stats"),
            Map("tags"), DeletionVectorNode(), I64("baseRowId"), I64("defaultRowCommitVersion"), Str("clusteringProvider")),
        Struct("remove", Str("path"), I64("deletionTimestamp"), Bool("dataChange"), Bool("extendedFileMetadata"), Map("partitionValues"),
            I64("size"), DeletionVectorNode(), I64("baseRowId"), I64("defaultRowCommitVersion")),
        Struct("metaData", Str("id"), Str("name"), Str("description"), Struct("format", Str("provider"), Map("options")),
            Str("schemaString"), Strings("partitionColumns"), Map("configuration"), I64("createdTime")),
        Struct("protocol", I32("minReaderVersion"), I32("minWriterVersion"), Strings("readerFeatures"), Strings("writerFeatures")),
        Struct("domainMetadata", Str("domain"), Str("configuration"), Bool("removed")),
    ];

    private static Dictionary<string, object?> Row(string column, Dictionary<string, object?> action) => new() { [column] = action };

    private static IEnumerable<KeyValuePair<string, string?>>? NullablePairs(IReadOnlyDictionary<string, string?>? map) => map;

    private static IEnumerable<KeyValuePair<string, string?>> Pairs(IReadOnlyDictionary<string, string> map) =>
        map.Select(kv => new KeyValuePair<string, string?>(kv.Key, kv.Value));

    private static Dictionary<string, object?>? DeletionVector(DeletionVectorDescriptor? dv) => dv is null ? null : new()
    {
        ["storageType"] = dv.StorageType,
        ["pathOrInlineDv"] = dv.PathOrInlineDv,
        ["offset"] = dv.Offset,
        ["sizeInBytes"] = dv.SizeInBytes,
        ["cardinality"] = dv.Cardinality,
    };

    private static Dictionary<string, object?> Protocol(ProtocolAction p) => new()
    {
        ["minReaderVersion"] = p.MinReaderVersion,
        ["minWriterVersion"] = p.MinWriterVersion,
        ["readerFeatures"] = p.ReaderFeatures,
        ["writerFeatures"] = p.WriterFeatures,
    };

    private static Dictionary<string, object?> Metadata(MetadataAction m) => new()
    {
        ["id"] = m.Id,
        ["name"] = m.Name,
        ["description"] = m.Description,
        ["format"] = new Dictionary<string, object?> { ["provider"] = m.FormatProvider, ["options"] = Pairs(m.FormatOptions) },
        ["schemaString"] = m.SchemaString,
        ["partitionColumns"] = m.PartitionColumns,
        ["configuration"] = Pairs(m.Configuration),
        ["createdTime"] = m.CreatedTime,
    };

    private static Dictionary<string, object?> Txn(TxnAction t) => new()
    {
        ["appId"] = t.AppId,
        ["version"] = t.Version,
        ["lastUpdated"] = t.LastUpdated,
    };

    private static Dictionary<string, object?> Domain(DomainMetadataAction d) => new()
    {
        ["domain"] = d.Domain,
        ["configuration"] = d.Configuration,
        ["removed"] = d.Removed,
    };

    private static Dictionary<string, object?> Add(AddAction a) => new()
    {
        ["path"] = a.Path,
        ["partitionValues"] = NullablePairs(a.PartitionValues),
        ["size"] = a.Size,
        ["modificationTime"] = a.ModificationTime,
        // Files in a checkpoint do not change data; they are the table as it is.
        ["dataChange"] = false,
        ["stats"] = a.Stats,
        ["tags"] = NullablePairs(a.Tags),
        ["deletionVector"] = DeletionVector(a.DeletionVector),
        ["baseRowId"] = a.BaseRowId,
        ["defaultRowCommitVersion"] = a.DefaultRowCommitVersion,
        ["clusteringProvider"] = a.ClusteringProvider,
    };

    private static Dictionary<string, object?> Remove(RemoveAction r) => new()
    {
        ["path"] = r.Path,
        ["deletionTimestamp"] = r.DeletionTimestamp,
        ["dataChange"] = false,
        ["extendedFileMetadata"] = r.ExtendedFileMetadata,
        ["partitionValues"] = NullablePairs(r.PartitionValues),
        ["size"] = r.Size,
        ["deletionVector"] = DeletionVector(r.DeletionVector),
        ["baseRowId"] = r.BaseRowId,
        ["defaultRowCommitVersion"] = r.DefaultRowCommitVersion,
    };
}
