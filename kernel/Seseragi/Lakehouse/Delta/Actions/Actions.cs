using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>One action of the transaction log.</summary>
public abstract class DeltaAction
{
    private protected DeltaAction() { }
}

/// <summary>The protocol versions and table features a reader and a writer must implement.</summary>
public sealed class ProtocolAction : DeltaAction
{
    /// <summary>The lowest reader version that may read the table.</summary>
    public required int MinReaderVersion { get; init; }
    /// <summary>The lowest writer version that may write the table.</summary>
    public required int MinWriterVersion { get; init; }
    /// <summary>Reader table features; null when the reader version is below 3.</summary>
    public IReadOnlyList<string>? ReaderFeatures { get; init; }
    /// <summary>Writer table features; null when the writer version is below 7.</summary>
    public IReadOnlyList<string>? WriterFeatures { get; init; }
}

/// <summary>The table metadata: schema, partitioning and configuration.</summary>
public sealed class MetadataAction : DeltaAction
{
    /// <summary>The table id.</summary>
    public required string Id { get; init; }
    /// <summary>The table name, if set.</summary>
    public string? Name { get; init; }
    /// <summary>The table description, if set.</summary>
    public string? Description { get; init; }
    /// <summary>The data file format, in practice <c>parquet</c>.</summary>
    public string FormatProvider { get; init; } = "parquet";
    /// <summary>Format options.</summary>
    public IReadOnlyDictionary<string, string> FormatOptions { get; init; } = EmptyMap.Instance;
    /// <summary>The schema as JSON text.</summary>
    public required string SchemaString { get; init; }
    /// <summary>The partition columns in order.</summary>
    public IReadOnlyList<string> PartitionColumns { get; init; } = [];
    /// <summary>Table properties.</summary>
    public IReadOnlyDictionary<string, string> Configuration { get; init; } = EmptyMap.Instance;
    /// <summary>Creation time in milliseconds since the epoch, if recorded.</summary>
    public long? CreatedTime { get; init; }
}

/// <summary>Adds a data file to the table.</summary>
public sealed class AddAction : DeltaAction
{
    /// <summary>The path as written in the log: relative to the table root and URL-encoded, or an absolute URI.</summary>
    public required string Path { get; init; }
    /// <summary>Partition column to value; a null value is a null partition.</summary>
    public IReadOnlyDictionary<string, string?> PartitionValues { get; init; } = EmptyMap.Nullable;
    /// <summary>File size in bytes.</summary>
    public long Size { get; init; }
    /// <summary>File modification time in milliseconds since the epoch.</summary>
    public long ModificationTime { get; init; }
    /// <summary>False when the action only rearranges existing data.</summary>
    public bool DataChange { get; init; } = true;
    /// <summary>File statistics as JSON text, if present.</summary>
    public string? Stats { get; init; }
    /// <summary>File tags, if present.</summary>
    public IReadOnlyDictionary<string, string?>? Tags { get; init; }
    /// <summary>The deletion vector of the file, if it has one.</summary>
    public DeletionVectorDescriptor? DeletionVector { get; init; }
    /// <summary>The first row id of the file under row tracking.</summary>
    public long? BaseRowId { get; init; }
    /// <summary>The version that first added the file under row tracking.</summary>
    public long? DefaultRowCommitVersion { get; init; }
    /// <summary>The clustering implementation that wrote the file, if any.</summary>
    public string? ClusteringProvider { get; init; }

    /// <summary>The path with URL encoding removed.</summary>
    public string DecodedPath => Uri.UnescapeDataString(Path);

    private FileStats? _parsedStats;
    private bool _statsParsed;

    /// <summary>The statistics parsed from <see cref="Stats"/>, or null when there are none.</summary>
    public FileStats? ParsedStats
    {
        get
        {
            if (!_statsParsed)
            {
                _parsedStats = FileStats.Parse(Stats);
                _statsParsed = true;
            }
            return _parsedStats;
        }
    }
}

/// <summary>Removes a data file from the table.</summary>
public sealed class RemoveAction : DeltaAction
{
    /// <summary>The path as written in the log.</summary>
    public required string Path { get; init; }
    /// <summary>When the file was removed, in milliseconds since the epoch.</summary>
    public long? DeletionTimestamp { get; init; }
    /// <summary>False when the action only rearranges existing data.</summary>
    public bool DataChange { get; init; } = true;
    /// <summary>Whether the fields below were written.</summary>
    public bool ExtendedFileMetadata { get; init; }
    /// <summary>Partition values of the removed file, if written.</summary>
    public IReadOnlyDictionary<string, string?>? PartitionValues { get; init; }
    /// <summary>Size of the removed file, if written.</summary>
    public long? Size { get; init; }
    /// <summary>The deletion vector the removed file had, if any.</summary>
    public DeletionVectorDescriptor? DeletionVector { get; init; }
    /// <summary>The first row id of the file under row tracking.</summary>
    public long? BaseRowId { get; init; }
    /// <summary>The version that first added the file under row tracking.</summary>
    public long? DefaultRowCommitVersion { get; init; }
}

/// <summary>Adds a change data file, read only by change data feed queries.</summary>
public sealed class CdcAction : DeltaAction
{
    /// <summary>The path as written in the log.</summary>
    public required string Path { get; init; }
    /// <summary>Partition column to value.</summary>
    public IReadOnlyDictionary<string, string?> PartitionValues { get; init; } = EmptyMap.Nullable;
    /// <summary>File size in bytes.</summary>
    public long Size { get; init; }
    /// <summary>File tags, if present.</summary>
    public IReadOnlyDictionary<string, string?>? Tags { get; init; }
}

/// <summary>The progress of an application writing to the table.</summary>
public sealed class TxnAction : DeltaAction
{
    /// <summary>The application id.</summary>
    public required string AppId { get; init; }
    /// <summary>The application's own version.</summary>
    public required long Version { get; init; }
    /// <summary>When it was last updated, in milliseconds since the epoch.</summary>
    public long? LastUpdated { get; init; }
}

/// <summary>Configuration of a named domain.</summary>
public sealed class DomainMetadataAction : DeltaAction
{
    /// <summary>The domain name.</summary>
    public required string Domain { get; init; }
    /// <summary>The configuration text.</summary>
    public required string Configuration { get; init; }
    /// <summary>True when this action removes the domain.</summary>
    public bool Removed { get; init; }
}

/// <summary>Provenance of a commit. Its content is free-form; the fields here are the ones the protocol gives meaning.</summary>
public sealed class CommitInfoAction : DeltaAction
{
    /// <summary>The whole action as JSON text.</summary>
    public required string Json { get; init; }
    /// <summary>The writer's clock at commit, in milliseconds since the epoch, if written.</summary>
    public long? Timestamp { get; init; }
    /// <summary>The in-commit timestamp, if the feature is on.</summary>
    public long? InCommitTimestamp { get; init; }
    /// <summary>The operation name, if written.</summary>
    public string? Operation { get; init; }
}

/// <summary>Points from a V2 checkpoint at a sidecar file holding file actions.</summary>
public sealed class SidecarAction : DeltaAction
{
    /// <summary>The file name inside <c>_delta_log/_sidecars</c>, or an absolute URI.</summary>
    public required string Path { get; init; }
    /// <summary>The file size in bytes.</summary>
    public long SizeInBytes { get; init; }
    /// <summary>The file modification time in milliseconds since the epoch.</summary>
    public long ModificationTime { get; init; }
}

/// <summary>Describes a V2 checkpoint.</summary>
public sealed class CheckpointMetadataAction : DeltaAction
{
    /// <summary>The version the checkpoint was taken at.</summary>
    public required long Version { get; init; }
}
