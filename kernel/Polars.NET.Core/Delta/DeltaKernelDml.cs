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
/// Runs a whole operation again when its commit conflicts with another writer's: each attempt reads the table anew
/// and plans from it (its files of a failed attempt were already deleted). Up to <c>POLARS_DELTA_MAX_RETRIES</c>
/// attempts (5 by default), with exponential backoff and jitter, as the native implementation did.
/// </summary>
internal static class ConflictRetry
{
    internal static int MaxAttempts =>
        int.TryParse(Environment.GetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES"), out int n) && n > 0 ? n : 5;

    internal static T Run<T>(Func<T> attempt)
    {
        int max = MaxAttempts;
        for (int i = 1; ; i++)
        {
            try
            {
                return attempt();
            }
            catch (Exception e) when (i < max && IsConflict(e))
            {
                int wait = 50 << Math.Min(i - 1, 6);
                Thread.Sleep(wait + Random.Shared.Next(wait));
            }
        }
    }

    private static bool IsConflict(Exception e) =>
        e is DeltaCommitConflictException || e.InnerException is DeltaCommitConflictException;
}

/// <summary>
/// What the row-level operations (delete, update, later merge) share: the table they work on, finding the rows a
/// predicate matches file by file, and collecting the actions they commit.
/// </summary>
internal sealed class DmlTable : IDisposable
{
    private const string FileColumn = "__seseragi_file";

    private DmlTable(string tablePath, TrackingFileSystem fileSystem, string root, CloudArgs? cloud, Snapshot snapshot)
    {
        TablePath = tablePath;
        FileSystem = fileSystem;
        Root = root;
        Cloud = cloud;
        Snapshot = snapshot;
    }

    public string TablePath { get; }
    /// <summary>The table's storage; files the operation writes are tracked for rollback.</summary>
    public TrackingFileSystem FileSystem { get; }
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
        var (store, root) = TableStorage.Open(tablePath, cloud);
        // Files the operation writes are deleted again if it fails before its commit lands (see TrackingFileSystem).
        var fileSystem = new TrackingFileSystem(store);
        try
        {
            Snapshot snapshot = DeltaKernelWrite.TryLoad(fileSystem, tablePath, cloud)
                ?? throw new PolarsException($"No Delta table at {tablePath}.");
            // Every operation through here writes: refused before it reads or writes any data file, as Spark checks a
            // write when it starts (one with nothing to do would otherwise never reach the commit's check).
            try
            {
                TableFeatures.EnsureWritable(snapshot.Protocol, snapshot.Metadata);
            }
            catch (DeltaException e)
            {
                throw new PolarsException(e.Message, e);
            }
            return new DmlTable(tablePath, fileSystem, root, cloud, snapshot);
        }
        catch
        {
            fileSystem.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Ends the operation: files it wrote are deleted when its commit certainly did not land (it failed, or never
    /// committed); after a commit, or when its fate is unknown, they stay. Then releases the storage.
    /// </summary>
    public void Dispose()
    {
        try
        {
            FileSystem.Rollback();
        }
        finally
        {
            FileSystem.Dispose();
        }
    }

    /// <summary>Whether new deleted rows are marked in deletion vectors: the feature is supported and turned on.</summary>
    public bool DeletionVectorsEnabled =>
        Snapshot.Protocol.MinWriterVersion >= 7
        && (Snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.DeletionVectors)
        && IsTrue(TableFeatures.EnableDeletionVectorsKey);

    /// <summary>Whether changed rows are also written as change data.</summary>
    public bool ChangeFeedEnabled => IsTrue(ChangeFeed.EnableKey);

    private bool IsTrue(string key) =>
        Snapshot.Metadata.Configuration.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The table's materialized row id and row commit version column names when row tracking is on (rewritten
    /// rows must then keep their ids); null otherwise.
    /// </summary>
    public (string RowId, string CommitVersion)? MaterializedRowColumns =>
        RowTracking.IsEnabled(Snapshot.Protocol, Snapshot.Metadata) ? RowTracking.MaterializedColumns(Snapshot.Metadata) : null;

    /// <summary>
    /// The columns rows of this table carry through an operation besides the table's own: the row id and commit
    /// version under row tracking, none otherwise.
    /// </summary>
    public string[] RowIdColumns => MaterializedRowColumns is null ? [] : [DeltaKernelScan.RowIdColumn, DeltaKernelScan.RowCommitVersionColumn];

    /// <summary>
    /// Scans one data file as table rows; see <see cref="DeltaKernelScan.ScanFile"/>. Under row tracking the rows
    /// carry their ids and commit versions (<see cref="RowIdColumns"/>).
    /// </summary>
    public LazyFrameHandle ScanFile(AddAction file, long[]? dropRows = null, long[]? onlyRows = null, bool withRowIndex = false) =>
        DeltaKernelScan.ScanFile(FileSystem, Root, Snapshot, file, EngineCloud, dropRows, onlyRows, withRowIndex, rowTracking: MaterializedRowColumns);

    /// <summary>Rows that are changed: under row tracking their commit version is cleared (the commit's version applies).</summary>
    public LazyFrameHandle Changed(LazyFrameHandle rows) => MaterializedRowColumns is null
        ? rows
        : PolarsWrapper.LazyWithColumns(rows, [PolarsWrapper.Alias(NullInt64(), DeltaKernelScan.RowCommitVersionColumn)]);

    /// <summary>A null of type Int64.</summary>
    public static ExprHandle NullInt64()
    {
        using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
        return PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), PolarsWrapper.DataTypeExprFromDataType(int64), strict: true, wrapNumerical: false);
    }

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

    internal static ulong[] UInt64Column(DataFrameHandle df, string name)
    {
        using SeriesHandle column = PolarsWrapper.DataFrameGetColumn(df, name);
        using SeriesHandle chunked = PolarsWrapper.SeriesRechunk(column);
        var array = (UInt64Array)PolarsWrapper.SeriesToArrow(chunked);
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
    /// <param name="file">The file.</param>
    /// <param name="dataChange">False when the rows stay in the table, rewritten elsewhere (optimize).</param>
    public void Remove(AddAction file, bool dataChange = true) => Removes.Add(new RemoveAction
    {
        Path = file.Path,
        DeletionTimestamp = now,
        DataChange = dataChange,
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
