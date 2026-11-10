using Apache.Arrow;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>
/// What the row-level operations (delete, update, later merge) share: the table they work on, finding the rows a
/// predicate matches file by file, and collecting the actions they commit.
/// </summary>
internal sealed class DmlTable : IDisposable
{
    private const string FileColumn = "__seseragi_file";

    private DmlTable(string tablePath, IWritableFileSystem fileSystem, string root, CloudArgs? cloud, Snapshot snapshot)
    {
        TablePath = tablePath;
        FileSystem = fileSystem;
        Root = root;
        Cloud = cloud;
        Snapshot = snapshot;
    }

    public string TablePath { get; }
    public IWritableFileSystem FileSystem { get; }
    /// <summary>The location data file paths are relative to.</summary>
    public string Root { get; }
    /// <summary>Cloud settings as given; null for a local table.</summary>
    public CloudArgs? Cloud { get; }
    /// <summary>Cloud settings for the engine's scans.</summary>
    public CloudArgs EngineCloud => Cloud ?? CloudArgs.Local;
    public Snapshot Snapshot { get; }
    public string[] PartitionColumns => [.. Snapshot.PartitionColumns];

    /// <summary>Opens the table at its latest version.</summary>
    /// <exception cref="PolarsException">There is no table at the path.</exception>
    public static DmlTable Open(string tablePath, CloudArgs? cloud)
    {
        bool isUrl = tablePath.Contains("://", StringComparison.Ordinal);
        IWritableFileSystem fileSystem = isUrl ? new ObjectStoreFileSystem(tablePath.TrimEnd('/'), cloud) : new LocalFileSystem(tablePath);
        try
        {
            string root = fileSystem is LocalFileSystem local ? local.Root : tablePath.TrimEnd('/');
            Snapshot snapshot = DeltaKernelWrite.TryLoad(fileSystem, tablePath, cloud)
                ?? throw new PolarsException($"No Delta table at {tablePath}.");
            return new DmlTable(tablePath, fileSystem, root, cloud, snapshot);
        }
        catch
        {
            (fileSystem as IDisposable)?.Dispose();
            throw;
        }
    }

    public void Dispose() => (FileSystem as IDisposable)?.Dispose();

    /// <summary>Whether new deleted rows are marked in deletion vectors: the feature is supported and turned on.</summary>
    public bool DeletionVectorsEnabled =>
        Snapshot.Protocol.MinWriterVersion >= 7
        && (Snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.DeletionVectors)
        && IsTrue(TableFeatures.EnableDeletionVectorsKey);

    /// <summary>Whether changed rows are also written as change data.</summary>
    public bool ChangeFeedEnabled => IsTrue(ChangeFeed.EnableKey);

    private bool IsTrue(string key) =>
        Snapshot.Metadata.Configuration.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>Refuses writing data files where the kernel cannot keep the table's column mapping yet.</summary>
    public void EnsureFilesWritable()
    {
        if (Snapshot.ColumnMappingMode != ColumnMappingMode.None && Snapshot.Schema.Fields.Any(f => DeltaKernelWrite.NeedsMapping(f.Type)))
            throw new PolarsException("Writing files of a table with column mapping inside nested columns is not supported yet.");
    }

    /// <summary>Scans one data file as table rows; see <see cref="DeltaKernelScan.ScanFile"/>.</summary>
    public LazyFrameHandle ScanFile(AddAction file, long[]? dropRows = null, long[]? onlyRows = null, bool withRowIndex = false) =>
        DeltaKernelScan.ScanFile(FileSystem, Root, Snapshot, file, EngineCloud, dropRows, onlyRows, withRowIndex);

    /// <summary>
    /// The live rows of each candidate file that match the predicate, as ascending row positions in the file, keyed
    /// by the candidate's index; files without a match are absent. One engine query over all candidates. A row for
    /// which the predicate is null does not match. Consumes <paramref name="predicate"/>.
    /// </summary>
    public Dictionary<int, long[]> MatchingRows(IReadOnlyList<AddAction> candidates, ExprHandle predicate)
    {
        var result = new Dictionary<int, long[]>();
        if (candidates.Count == 0)
        {
            predicate.Dispose();
            return result;
        }

        var frames = new List<LazyFrameHandle>(candidates.Count);
        try
        {
            for (int i = 0; i < candidates.Count; i++)
            {
                LazyFrameHandle lf = ScanFile(candidates[i], withRowIndex: true);
                frames.Add(PolarsWrapper.LazyWithColumns(lf, [PolarsWrapper.Alias(PolarsWrapper.Lit(i), FileColumn)]));
            }
            LazyFrameHandle all = frames.Count == 1
                ? frames[0]
                : PolarsWrapper.LazyConcat([.. frames], PlConcatType.Vertical, rechunk: false, parallel: true, toSupertypes: false, maintainOrder: false);
            frames.Clear(); // owned by the query now
            LazyFrameHandle matched = PolarsWrapper.LazySelect(
                PolarsWrapper.LazyFilter(all, predicate),
                [PolarsWrapper.Col(FileColumn), PolarsWrapper.Col(DeltaKernelScan.RowIndexColumn)]);

            using DataFrameHandle df = PolarsWrapper.LazyCollect(matched, PlEngine.Auto);
            if (PolarsWrapper.DataFrameHeight(df) == 0) return result;
            int[] files = Int32Column(df, FileColumn);
            long[] rows = Int64Column(df, DeltaKernelScan.RowIndexColumn);

            var byFile = new Dictionary<int, List<long>>();
            for (int k = 0; k < files.Length; k++)
            {
                if (!byFile.TryGetValue(files[k], out List<long>? list)) byFile[files[k]] = list = [];
                list.Add(rows[k]);
            }
            foreach (var (file, list) in byFile) result[file] = DeletionVector.FromRows(list).DeletedRows;
            return result;
        }
        finally
        {
            foreach (LazyFrameHandle frame in frames) frame.Dispose();
        }
    }

    /// <summary>The rows of a file that are not deleted: from its statistics when recorded, else counted.</summary>
    public long LiveRows(AddAction file)
    {
        if (file.ParsedStats?.NumRecords is long records)
            return records - (file.DeletionVector?.Cardinality ?? 0);
        LazyFrameHandle count = PolarsWrapper.LazySelect(ScanFile(file), [PolarsWrapper.Alias(PolarsWrapper.Len(), "n")]);
        using DataFrameHandle df = PolarsWrapper.LazyCollect(count, PlEngine.Auto);
        using SeriesHandle n = PolarsWrapper.DataFrameGetColumn(df, "n");
        using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
        using SeriesHandle asLong = PolarsWrapper.SeriesCast(n, int64, strict: true, wrapNumerical: false);
        return PolarsWrapper.SeriesGetInt64Fast(asLong, 0);
    }

    /// <summary>A boolean expression: whether the row (by <see cref="DeltaKernelScan.RowIndexColumn"/>) is one of <paramref name="rows"/>.</summary>
    public static ExprHandle RowIsListed(long[] rows) =>
        PolarsWrapper.IsIn(
            PolarsWrapper.Col(DeltaKernelScan.RowIndexColumn),
            PolarsWrapper.Implode(PolarsWrapper.Lit(PolarsWrapper.SeriesNew("rows", rows)), maintainOrder: false),
            nullsEqual: false);

    /// <summary>Concatenates frames in order (taking them), or null for none.</summary>
    public static LazyFrameHandle? Concat(List<LazyFrameHandle> frames)
    {
        if (frames.Count == 0) return null;
        LazyFrameHandle all = frames.Count == 1
            ? frames[0]
            : PolarsWrapper.LazyConcat([.. frames], PlConcatType.Vertical, rechunk: false, parallel: true, toSupertypes: false, maintainOrder: true);
        frames.Clear();
        return all;
    }

    /// <summary>Adds the change type column.</summary>
    public static LazyFrameHandle WithChangeType(LazyFrameHandle lf, string changeType) =>
        PolarsWrapper.LazyWithColumns(lf, [PolarsWrapper.Alias(PolarsWrapper.Lit(changeType), DeltaKernelScan.ChangeTypeColumn)]);

    internal static int[] Int32Column(DataFrameHandle df, string name)
    {
        using SeriesHandle column = PolarsWrapper.DataFrameGetColumn(df, name);
        using SeriesHandle chunked = PolarsWrapper.SeriesRechunk(column);
        var array = (Int32Array)PolarsWrapper.SeriesToArrow(chunked);
        using (array) return array.Values.ToArray();
    }

    internal static long[] Int64Column(DataFrameHandle df, string name)
    {
        using SeriesHandle column = PolarsWrapper.DataFrameGetColumn(df, name);
        using SeriesHandle chunked = PolarsWrapper.SeriesRechunk(column);
        var array = (Int64Array)PolarsWrapper.SeriesToArrow(chunked);
        using (array) return array.Values.ToArray();
    }
}

/// <summary>The actions a row-level operation commits, collected file by file.</summary>
internal sealed class DmlPlan(long now)
{
    public long Now => now;
    public List<RemoveAction> Removes { get; } = [];
    public List<AddAction> Adds { get; } = [];
    public List<CdcAction> ChangeFiles { get; } = [];
    public long CopiedRows { get; set; }
    public int VectorsAdded { get; set; }
    public int Rewritten { get; set; }

    public bool IsEmpty => Removes.Count == 0 && Adds.Count == 0;

    /// <summary>Removes a data file (as it is now, deletion vector included).</summary>
    public void Remove(AddAction file) => Removes.Add(new RemoveAction
    {
        Path = file.Path,
        DeletionTimestamp = now,
        DataChange = true,
        ExtendedFileMetadata = true,
        PartitionValues = file.PartitionValues,
        Size = file.Size,
        DeletionVector = file.DeletionVector,
        BaseRowId = file.BaseRowId,
        DefaultRowCommitVersion = file.DefaultRowCommitVersion,
    });

    /// <summary>
    /// Marks rows of files deleted with deletion vectors, merged with the vectors they have: writes one vector file,
    /// then removes each file and adds it back with its new vector.
    /// </summary>
    public void MarkDeleted(DmlTable table, List<(AddAction File, long[] Rows)> files)
    {
        if (files.Count == 0) return;
        var vectors = files.Select(f =>
        {
            DeletionVector fresh = DeletionVector.FromRows(f.Rows);
            return f.File.DeletionVector is null ? fresh : DeletionVector.Read(table.FileSystem, f.File.DeletionVector).Union(fresh);
        }).ToList();
        IReadOnlyList<DeletionVectorDescriptor> descriptors = DeletionVector.WriteFile(table.FileSystem, vectors);
        for (int i = 0; i < files.Count; i++)
        {
            AddAction file = files[i].File;
            Remove(file);
            Adds.Add(new AddAction
            {
                Path = file.Path,
                PartitionValues = file.PartitionValues,
                Size = file.Size,
                ModificationTime = file.ModificationTime,
                DataChange = true,
                Stats = FileStats.WithLooseBounds(file.Stats),
                Tags = file.Tags,
                DeletionVector = descriptors[i],
                BaseRowId = file.BaseRowId,
                DefaultRowCommitVersion = file.DefaultRowCommitVersion,
                ClusteringProvider = file.ClusteringProvider,
            });
            VectorsAdded++;
        }
    }

    /// <summary>Adds the plan's actions to the transaction, with the metrics every row-level operation records.</summary>
    public void AddTo(Transaction transaction)
    {
        foreach (RemoveAction remove in Removes) transaction.Add(remove);
        foreach (AddAction add in Adds) transaction.Add(add);
        foreach (CdcAction cdc in ChangeFiles) transaction.Add(cdc);
        transaction.WithMetric("numRemovedFiles", Removes.Count)
                   .WithMetric("numAddedFiles", Adds.Count)
                   .WithMetric("numDeletionVectorsAdded", VectorsAdded)
                   .WithMetric("numDeletionVectorsRemoved", Removes.Count(r => r.DeletionVector is not null))
                   .WithMetric("numAddedChangeFiles", ChangeFiles.Count)
                   .WithMetric("numCopiedRows", CopiedRows);
    }
}
