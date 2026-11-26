using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>What Spark's <c>DESCRIBE DETAIL</c> tells about a table, at its latest version.</summary>
/// <param name="Id">The table's id (its metadata's).</param>
/// <param name="Name">The name in the metadata; null when none.</param>
/// <param name="Description">The description in the metadata; null when none.</param>
/// <param name="Location">Where the table is.</param>
/// <param name="CreatedAt">When the table was created (its metadata's creation time); null when not recorded.</param>
/// <param name="LastModified">When the latest version was committed (its in-commit timestamp when the table records
/// them, else the time of its commit file).</param>
/// <param name="Version">The latest version.</param>
/// <param name="PartitionColumns">The partition columns.</param>
/// <param name="ClusteringColumns">The liquid clustering columns, dotted for nested fields.</param>
/// <param name="NumFiles">The data files of the latest version.</param>
/// <param name="SizeInBytes">Their total size.</param>
/// <param name="Properties">The table properties.</param>
/// <param name="MinReaderVersion">The protocol's reader version.</param>
/// <param name="MinWriterVersion">The protocol's writer version.</param>
/// <param name="TableFeatures">The table features the protocol supports (those a legacy version implies too), sorted.</param>
public sealed record TableDetail(
    string Id, string? Name, string? Description, string Location, DateTimeOffset? CreatedAt, DateTimeOffset LastModified, long Version,
    IReadOnlyList<string> PartitionColumns, IReadOnlyList<string> ClusteringColumns, long NumFiles, long SizeInBytes,
    IReadOnlyDictionary<string, string> Properties, int MinReaderVersion, int MinWriterVersion, IReadOnlyList<string> TableFeatures)
{
    /// <summary>The detail of the table at its latest version.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="location">Where the table is, as reported.</param>
    public static TableDetail Of(IFileSystem fileSystem, Snapshot table, string location)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(location);
        IReadOnlyList<string[]> clustered = Clustering.PhysicalColumns(table.DomainMetadata) ?? [];
        return new TableDetail(
            table.Metadata.Id,
            table.Metadata.Name,
            table.Metadata.Description,
            location,
            table.Metadata.CreatedTime is long created ? DateTimeOffset.FromUnixTimeMilliseconds(created) : null,
            CommitTimeline.Load(fileSystem, table.Metadata).TimestampOf(table.Version),
            table.Version,
            [.. table.PartitionColumns],
            [.. Clustering.Fields(table.Schema, clustered).Select(Clustering.LogicalName)],
            table.Files.Count,
            table.Files.Sum(f => f.Size),
            table.Metadata.Configuration,
            table.Protocol.MinReaderVersion,
            table.Protocol.MinWriterVersion,
            [.. Delta.TableFeatures.WriterFeaturesOf(table.Protocol).Union(Delta.TableFeatures.ReaderFeaturesOf(table.Protocol)).Order(StringComparer.Ordinal)]);
    }
}
