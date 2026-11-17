using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Files.Parquet;

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

    /// <summary>The table property choosing classic or V2 checkpoints.</summary>
    public const string PolicyKey = "delta.checkpointPolicy";

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
    /// inline) when its <c>delta.checkpointPolicy</c> is <c>v2</c>; any other a classic one (<c>n.checkpoint.parquet</c>).
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="snapshot">The state to write.</param>
    /// <param name="now">The clock that decides which tombstones have expired; the current time when null.</param>
    /// <returns>The table-relative path of the checkpoint.</returns>
    public static string Write(IWritableFileSystem fileSystem, Snapshot snapshot, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(snapshot);
        // A catalog-managed table gets checkpoints only for published versions (the protocol's rule).
        if (CatalogManaged.IsCatalogManaged(snapshot.Protocol)
            && !fileSystem.List(LogFile.LogDirectory, $"{snapshot.Version:D20}").Any(f => f.Path == LogFile.CommitPath(snapshot.Version)))
            throw new DeltaException($"Version {snapshot.Version} is not published yet: a checkpoint waits until it is.");

        RemoveAction[] tombstones = LiveTombstones(snapshot, now);
        // V2 checkpoints only under the v2 policy (Spark): a table keeping the feature with the classic policy (as while
        // the feature is being dropped) gets classic ones.
        bool v2 = (snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.V2Checkpoint)
            && snapshot.Metadata.Configuration.TryGetValue(PolicyKey, out string? policy) && policy == "v2";

        // The file goes to the store as it is written (in parts on object stores); it appears whole at commit, and an
        // exception on the way leaves nothing.
        string path;
        long sizeInBytes;
        int actionCount;
        if (v2)
        {
            List<DeltaAction> actions =
            [
                new CheckpointMetadataAction { Version = snapshot.Version },
                snapshot.Protocol,
                snapshot.Metadata,
                .. snapshot.AppTransactions.Values,
                .. snapshot.DomainMetadata.Values,
                .. snapshot.Files.Select(WithoutDataChange),
                .. tombstones.Select(WithoutDataChange),
            ];
            // A V2 checkpoint has a unique name, so writers checkpointing the same version never collide.
            path = $"{LogFile.LogDirectory}/{snapshot.Version:D20}.checkpoint.{Guid.NewGuid()}.json";
            using FileWriteStream file = fileSystem.OpenWrite(path, createOnly: true);
            actionCount = ActionWriter.WriteCommit(actions, file);
            sizeInBytes = file.Position;
            if (!file.Commit()) throw new DeltaException($"The checkpoint {path} already exists.");
        }
        else
        {
            path = $"{LogFile.LogDirectory}/{snapshot.Version:D20}.checkpoint.parquet";
            // The same version always gives the same checkpoint, so replacing one written concurrently is harmless.
            using FileWriteStream file = fileSystem.OpenWrite(path, createOnly: false);
            actionCount = WriteParquet(file, snapshot, tombstones);
            sizeInBytes = file.Position;
            file.Commit();
        }

        var hint = new System.Buffers.ArrayBufferWriter<byte>(256); // a few hundred bytes
        using (var json = new Utf8JsonWriter(hint, ActionWriter.JsonOptions))
        {
            json.WriteStartObject();
            json.WriteNumber("version", snapshot.Version);
            json.WriteNumber("size", actionCount);
            json.WriteNumber("sizeInBytes", sizeInBytes);
            json.WriteNumber("numOfAddFiles", snapshot.Files.Count);
            if (v2)
            {
                json.WriteStartObject("v2Checkpoint");
                json.WriteString("path", path[(LogFile.LogDirectory.Length + 1)..]);
                json.WriteNumber("sizeInBytes", sizeInBytes);
                json.WriteNumber("modificationTime", (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds());
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        // The hint only ever moves forward; a stale or missing one costs readers a longer listing, nothing more.
        if (LastCheckpointVersion(fileSystem) < snapshot.Version)
            fileSystem.Overwrite($"{LogFile.LogDirectory}/_last_checkpoint", hint.WrittenMemory);
        return path;
    }

    /// <summary>The tombstones a checkpoint keeps: those not yet expired at <paramref name="now"/>.</summary>
    private static RemoveAction[] LiveTombstones(Snapshot snapshot, DateTimeOffset? now)
    {
        long expiredBefore = ((now ?? DateTimeOffset.UtcNow) - Retention(snapshot.Metadata)).ToUnixTimeMilliseconds();
        return [.. snapshot.Tombstones.Where(r => r.DeletionTimestamp is null || r.DeletionTimestamp >= expiredBefore)];
    }

    /// <summary>
    /// Writes the classic checkpoint of the snapshot to a stream, its rows built straight into native Arrow batches of
    /// <see cref="CheckpointBatches.BatchRows"/> rows, each encoded before the next is built; returns the number of
    /// actions. Statistics as JSON text and, by default, typed (stats_parsed, partitionValues_parsed), as Spark writes them.
    /// </summary>
    internal static int WriteParquet(Stream stream, Snapshot snapshot, IReadOnlyList<RemoveAction> tombstones)
    {
        bool json = CheckpointStatistics.WritesJson(snapshot.Metadata);
        CheckpointStatistics? typed = CheckpointStatistics.For(snapshot.Metadata, snapshot.Protocol);
        Apache.Arrow.Schema schema = ParquetNodeArrow.ArrowSchema(Schema(json, typed));
        using var batches = new CheckpointBatches(schema, json, typed);
        using ParquetWriter writer = ParquetWriter.Create(stream, schema, FileOptions);
        int actions = 0;

        // A failure on the way (a row the batches refuse, the stream, a cancellation) gives the file up: disposing the
        // writer must not encode and send the buffered row group (a checkpoint is one) for nothing.
        try
        {
            batches.AppendProtocol(snapshot.Protocol);
            batches.AppendMetadata(snapshot.Metadata);
            actions += 2;
            foreach (TxnAction t in snapshot.AppTransactions.Values)
            {
                batches.AppendTxn(t);
                Flush(full: true);
                actions++;
            }
            foreach (DomainMetadataAction d in snapshot.DomainMetadata.Values)
            {
                batches.AppendDomain(d);
                Flush(full: true);
                actions++;
            }
            foreach (AddAction a in snapshot.Files)
            {
                batches.AppendAdd(a);
                Flush(full: true);
                actions++;
            }
            foreach (RemoveAction r in tombstones)
            {
                batches.AppendRemove(r);
                Flush(full: true);
                actions++;
            }
            Flush(full: false);
            writer.Complete();
            return actions;
        }
        catch
        {
            writer.Abort();
            throw;
        }

        // Encodes the rows built so far: once a batch is full, or at the end.
        void Flush(bool full)
        {
            if (batches.Rows == 0 || (full && batches.Rows < CheckpointBatches.BatchRows)) return;
            using RecordBatch batch = batches.Export();
            writer.Write(batch);
        }
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
            using FileBytes? bytes = fileSystem.ReadFile($"{LogFile.LogDirectory}/_last_checkpoint");
            if (bytes is null) return -1;
            using JsonDocument doc = JsonDocument.Parse(bytes.Memory);
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
    /// <summary>Reads an interval property as Spark reads it (see <see cref="Intervals"/>); false when it cannot be used.</summary>
    internal static bool TryParseInterval(string text, out TimeSpan value) => Intervals.TryRead(text, out value);

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
        Str("storageType"), Str("pathOrInlineDv"), I32("offset"), I32("sizeInBytes"), I64("cardinality"), I64("maxRowIndex"));

    /// <summary>The checkpoint schema; the <c>add</c> columns in Spark's order, statistics as the table asks.</summary>
    /// <summary>
    /// How checkpoint files are written, as Spark writes them: Snappy, statistics and page indexes, row groups of up to
    /// 128 MiB of pages whatever the rows, no stored Arrow schema; the same actions always give the same bytes (a
    /// checkpoint written concurrently for the same version may be replaced).
    /// </summary>
    private static readonly ParquetWriteOptions FileOptions = new() { RowGroupRows = int.MaxValue, StoreArrowSchema = false, LeaveOpen = true };

    private static IReadOnlyList<ParquetNode> Schema(bool json, CheckpointStatistics? typed) =>
    [
        Struct("txn", Str("appId"), I64("version"), I64("lastUpdated")),
        Struct("add",
        [
            Str("path"), Map("partitionValues"), I64("size"), I64("modificationTime"), Bool("dataChange"), Map("tags"),
            DeletionVectorNode(), I64("baseRowId"), I64("defaultRowCommitVersion"), Str("clusteringProvider"),
            .. json ? [Str("stats")] : (ParquetNode[])[],
            .. typed?.PartitionNode() is { } partitions ? [partitions] : (ParquetNode[])[],
            .. typed is null ? [] : (ParquetNode[])[typed.StatsNode()],
        ]),
        Struct("remove", Str("path"), I64("deletionTimestamp"), Bool("dataChange"), Bool("extendedFileMetadata"), Map("partitionValues"),
            I64("size"), DeletionVectorNode(), I64("baseRowId"), I64("defaultRowCommitVersion")),
        Struct("metaData", Str("id"), Str("name"), Str("description"), Struct("format", Str("provider"), Map("options")),
            Str("schemaString"), Strings("partitionColumns"), Map("configuration"), I64("createdTime")),
        Struct("protocol", I32("minReaderVersion"), I32("minWriterVersion"), Strings("readerFeatures"), Strings("writerFeatures")),
        Struct("domainMetadata", Str("domain"), Str("configuration"), Bool("removed")),
    ];
}
