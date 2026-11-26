using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Shallow clones, as Spark's <c>CREATE TABLE … SHALLOW CLONE</c>: a new table whose version 0 refers to the source's
/// data files (and deletion vector files) by their absolute paths, copying none. The public Delta API calls it.
/// </summary>
internal static class DeltaKernelClone
{
    /// <summary>The operation name, as Spark records it.</summary>
    public const string Operation = "CLONE";

    /// <summary>Clones the source (at a version or a time, else its latest) into an empty location; returns 0.</summary>
    /// <param name="sourcePath">The source table: a local directory or a cloud URL.</param>
    /// <param name="targetPath">Where the clone goes: nothing there yet; on the source's store (both local, or the same bucket).</param>
    /// <param name="version">The source version; null for the latest (or the one <paramref name="timestamp"/> picks).</param>
    /// <param name="timestamp">The source version current at this time; null for the latest.</param>
    /// <param name="properties">Table properties set on the clone, over the source's.</param>
    /// <param name="cloud">Cloud settings, for both tables.</param>
    /// <exception cref="PolarsException">The source is missing or cannot be read, the target is not empty, or the two
    /// are on different stores.</exception>
    public static long Clone(
        string sourcePath, string targetPath, long? version, DateTimeOffset? timestamp, IReadOnlyDictionary<string, string>? properties,
        CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourcePath);
        ArgumentException.ThrowIfNullOrEmpty(targetPath);
        if (version is not null && timestamp is not null) throw new ArgumentException("Give a version or a timestamp, not both.");
        string sourceRoot = Root(sourcePath), targetRoot = Root(targetPath);
        if (Store(sourceRoot) != Store(targetRoot))
            throw new PolarsException($"Cannot clone {sourcePath} into {targetPath}: a clone refers to the source's files, so both must be on one store (both local, or in the same bucket).");
        if (sourceRoot == targetRoot || targetRoot.StartsWith(sourceRoot + "/", StringComparison.Ordinal))
            throw new PolarsException($"Cannot clone {sourcePath} into {targetPath}: the clone cannot be inside the source table.");

        var (source, _) = TableStorage.Open(sourcePath, cloud);
        IWritableFileSystem target = TableStorage.Open(targetPath, cloud).Store;
        try
        {
            Snapshot latest = DeltaKernelWrite.TryLoad(source, sourcePath, cloud) ?? throw new PolarsException($"No Delta table at {sourcePath}.");
            if (CatalogManaged.IsCatalogManaged(latest.Protocol))
                throw new PolarsException($"Cannot clone {sourcePath}: it is a catalog-managed table, read through its catalog only.");
            Snapshot snapshot = version is long v ? Snapshot.Load(source, v)
                : timestamp is DateTimeOffset t ? Snapshot.LoadAsOf(source, t)
                : latest;
            TableFeatures.EnsureReadable(snapshot.Protocol, snapshot.Metadata);
            if (target.ListAll("").Count > 0)
                throw new PolarsException($"Cannot clone into {targetPath}: the location is not empty.");

            // In-commit timestamps of the source start at a version of its own: the clone has them from version 0.
            var configuration = snapshot.Metadata.Configuration
                .Where(p => p.Key is not (Kaminari.Seseragi.Lakehouse.Delta.Log.CommitTimeline.EnablementVersionKey or Kaminari.Seseragi.Lakehouse.Delta.Log.CommitTimeline.EnablementTimestampKey))
                .ToDictionary(StringComparer.Ordinal);
            foreach (var (key, value) in properties ?? new Dictionary<string, string>())
            {
                if (key.StartsWith(Constraints.ConfigurationPrefix, StringComparison.OrdinalIgnoreCase))
                    throw new PolarsException(AlterTable.ConstraintPropertyRefused(key));
                configuration[key] = value;
            }
            MetadataAction metadata = new()
            {
                Id = Guid.NewGuid().ToString(), Name = snapshot.Metadata.Name, Description = snapshot.Metadata.Description,
                FormatProvider = snapshot.Metadata.FormatProvider, FormatOptions = snapshot.Metadata.FormatOptions,
                SchemaString = snapshot.Metadata.SchemaString, PartitionColumns = snapshot.Metadata.PartitionColumns,
                Configuration = configuration, CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            };
            // The source's protocol, raised for what the properties turn on.
            ProtocolAction needed = DeltaKernelWrite.ProtocolFor(snapshot.Schema, configuration);
            ProtocolAction protocol = DeltaKernelCreate.AtLeast(snapshot.Protocol, needed);

            Transaction transaction = Transaction.Create(target, protocol, metadata, Operation)
                .WithParameter("source", $"delta.`{sourceRoot}`")
                .WithParameter("sourceVersion", snapshot.Version.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .WithMetric("removedFilesSize", 0)
                .WithMetric("numRemovedFiles", 0)
                .WithMetric("sourceTableSize", snapshot.Files.Sum(f => f.Size))
                .WithMetric("numCopiedFiles", 0)
                .WithMetric("copiedFilesSize", 0)
                .WithMetric("sourceNumOfFiles", snapshot.Files.Count);
            foreach (DomainMetadataAction domain in snapshot.DomainMetadata.Values) transaction.Add(domain);
            foreach (AddAction file in snapshot.Files) transaction.Add(Referenced(file, sourceRoot));
            return transaction.Commit();
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            (source as IDisposable)?.Dispose();
            (target as IDisposable)?.Dispose();
        }
    }

    /// <summary>The source's file as the clone refers to it: by its absolute path, its deletion vector too.</summary>
    private static AddAction Referenced(AddAction file, string sourceRoot) => new()
    {
        Path = Absolute(file.Path, sourceRoot), PartitionValues = file.PartitionValues, Size = file.Size,
        ModificationTime = file.ModificationTime, DataChange = true, Stats = file.Stats, Tags = file.Tags,
        DeletionVector = file.DeletionVector is { StorageType: "u" } dv
            ? new DeletionVectorDescriptor
            {
                StorageType = "p", PathOrInlineDv = Absolute(dv.FilePath(), sourceRoot), // a prefix and a UUID name: nothing to escape
                Offset = dv.Offset, SizeInBytes = dv.SizeInBytes, Cardinality = dv.Cardinality,
            }
            : file.DeletionVector,
        BaseRowId = file.BaseRowId, DefaultRowCommitVersion = file.DefaultRowCommitVersion, ClusteringProvider = file.ClusteringProvider,
    };

    /// <summary>An escaped path of the source made absolute (one that is already, a clone's, stays).</summary>
    private static string Absolute(string escaped, string sourceRoot) =>
        escaped.Contains("://", StringComparison.Ordinal) || escaped.StartsWith("file:", StringComparison.Ordinal)
            ? escaped
            : ManagedStores.IsUrl(sourceRoot)
                ? $"{sourceRoot}/{escaped}"
                // As Spark (Hadoop) writes a local path: file:/dir/file, which Spark needs to match a file's deletion vector.
                : "file:" + new Uri(sourceRoot + "/").AbsolutePath + escaped;

    /// <summary>A table location normalized: a URL without its trailing slash, a local directory made absolute.</summary>
    private static string Root(string path) =>
        ManagedStores.IsUrl(path) ? path.TrimEnd('/') : Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar);

    /// <summary>The store a location is on: the scheme and authority of a URL; local paths all share one.</summary>
    private static string Store(string root)
    {
        if (!ManagedStores.IsUrl(root)) return "";
        int start = root.IndexOf("://", StringComparison.Ordinal) + 3;
        int slash = root.IndexOf('/', start);
        return slash < 0 ? root : root[..slash];
    }
}

/// <summary>Converts a directory of Parquet files into a Delta table in place (see <see cref="ConvertToDelta"/>).</summary>
internal static class DeltaKernelConvert
{
    /// <summary>Converts; returns 0, or null when the directory holds a Delta table already.</summary>
    /// <param name="path">The directory: a local directory or a cloud URL.</param>
    /// <param name="partitionSchema">The partition columns and their types; null for none.</param>
    /// <param name="collectStatistics">Give each file the statistics of its footer.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? Convert(string path, Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType? partitionSchema, bool collectStatistics, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        IWritableFileSystem store = TableStorage.Open(path, cloud).Store;
        try
        {
            long? version = ConvertToDelta.Convert(store, partitionSchema, collectStatistics);
            if (version is long committed) DeltaKernelWrite.TryCheckpoint(store, path, committed, cloud);
            return version;
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            (store as IDisposable)?.Dispose();
        }
    }
}
