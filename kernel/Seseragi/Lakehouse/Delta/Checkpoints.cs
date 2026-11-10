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
    /// default).
    /// </summary>
    public static bool IsDue(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.Version <= 0) return false;
        int interval = snapshot.Metadata.Configuration.TryGetValue(IntervalKey, out string? text)
            && int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n) && n > 0 ? n : DefaultInterval;
        return snapshot.Version % interval == 0;
    }

    /// <summary>
    /// Writes the checkpoint of the snapshot's version and points <c>_last_checkpoint</c> at it. A table with the
    /// <c>v2Checkpoint</c> feature gets a V2 checkpoint (<c>n.checkpoint.UUID.json</c> holding its file actions
    /// inline); any other a classic one (<c>n.checkpoint.parquet</c>).
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="snapshot">The state to write.</param>
    /// <param name="now">The clock that decides which tombstones have expired; the current time when null.</param>
    /// <returns>The table-relative path of the checkpoint.</returns>
    public static string Write(IWritableFileSystem fileSystem, Snapshot snapshot, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(snapshot);

        long expiredBefore = ((now ?? DateTimeOffset.UtcNow) - Retention(snapshot.Metadata)).ToUnixTimeMilliseconds();
        RemoveAction[] tombstones = [.. snapshot.Tombstones.Where(r => r.DeletionTimestamp is null || r.DeletionTimestamp >= expiredBefore)];
        bool v2 = (snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.V2Checkpoint);

        string path;
        byte[] content;
        int actionCount;
        if (v2)
        {
            var actions = new List<DeltaAction>
            {
                new CheckpointMetadataAction { Version = snapshot.Version },
                snapshot.Protocol,
                snapshot.Metadata,
            };
            actions.AddRange(snapshot.AppTransactions.Values);
            actions.AddRange(snapshot.DomainMetadata.Values);
            actions.AddRange(snapshot.Files.Select(WithoutDataChange));
            actions.AddRange(tombstones.Select(WithoutDataChange));
            content = ActionWriter.WriteCommit(actions);
            actionCount = actions.Count;
            // A V2 checkpoint has a unique name, so writers checkpointing the same version never collide.
            path = $"{LogFile.LogDirectory}/{snapshot.Version:D20}.checkpoint.{Guid.NewGuid()}.json";
            if (!fileSystem.TryCreate(path, content)) throw new DeltaException($"The checkpoint {path} already exists.");
        }
        else
        {
            var rows = new List<IReadOnlyDictionary<string, object?>>
            {
                Row("protocol", Protocol(snapshot.Protocol)),
                Row("metaData", Metadata(snapshot.Metadata)),
            };
            rows.AddRange(snapshot.AppTransactions.Values.Select(t => Row("txn", Txn(t))));
            rows.AddRange(snapshot.DomainMetadata.Values.Select(d => Row("domainMetadata", Domain(d))));
            rows.AddRange(snapshot.Files.Select(a => Row("add", Add(a))));
            rows.AddRange(tombstones.Select(r => Row("remove", Remove(r))));
            content = MinimalParquetWriter.Write(Schema, rows);
            actionCount = rows.Count;
            path = $"{LogFile.LogDirectory}/{snapshot.Version:D20}.checkpoint.parquet";
            // The same version always gives the same checkpoint, so replacing one written concurrently is harmless.
            fileSystem.Overwrite(path, content);
        }

        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("version", snapshot.Version);
            json.WriteNumber("size", actionCount);
            json.WriteNumber("sizeInBytes", content.Length);
            json.WriteNumber("numOfAddFiles", snapshot.Files.Count);
            if (v2)
            {
                json.WriteStartObject("v2Checkpoint");
                json.WriteString("path", path[(LogFile.LogDirectory.Length + 1)..]);
                json.WriteNumber("sizeInBytes", content.Length);
                json.WriteNumber("modificationTime", (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds());
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        // The hint only ever moves forward; a stale or missing one costs readers a longer listing, nothing more.
        if (LastCheckpointVersion(fileSystem) < snapshot.Version)
            fileSystem.Overwrite($"{LogFile.LogDirectory}/_last_checkpoint", stream.ToArray());
        return path;
    }

    /// <summary>Files in a checkpoint do not change data; they are the table as it is.</summary>
    private static AddAction WithoutDataChange(AddAction a) => new()
    {
        Path = a.Path, PartitionValues = a.PartitionValues, Size = a.Size, ModificationTime = a.ModificationTime, DataChange = false,
        Stats = a.Stats, Tags = a.Tags, DeletionVector = a.DeletionVector, BaseRowId = a.BaseRowId,
        DefaultRowCommitVersion = a.DefaultRowCommitVersion, ClusteringProvider = a.ClusteringProvider,
    };

    private static RemoveAction WithoutDataChange(RemoveAction r) => new()
    {
        Path = r.Path, DeletionTimestamp = r.DeletionTimestamp, DataChange = false, ExtendedFileMetadata = r.ExtendedFileMetadata,
        PartitionValues = r.PartitionValues, Size = r.Size, DeletionVector = r.DeletionVector, BaseRowId = r.BaseRowId,
        DefaultRowCommitVersion = r.DefaultRowCommitVersion,
    };

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
    internal static TimeSpan Retention(MetadataAction metadata) =>
        metadata.Configuration.TryGetValue(TombstoneRetentionKey, out string? text) && TryParseInterval(text, out TimeSpan value)
            ? value
            : DefaultRetention;

    /// <summary>Reads a duration property such as <c>interval 7 days</c>.</summary>
    internal static bool TryParseInterval(string text, out TimeSpan value)
    {
        value = default;
        string[] parts = text.Trim().ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3 || parts[0] != "interval"
            || !double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double n))
            return false;
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
        if (unit is not TimeSpan u) return false;
        value = u * n;
        return true;
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
