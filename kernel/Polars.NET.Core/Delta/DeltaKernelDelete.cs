using System.Text.Json;
using Apache.Arrow;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>What a delete did.</summary>
/// <param name="Version">The version committed; null when no row matched and nothing was committed.</param>
/// <param name="DeletedRows">Rows deleted.</param>
/// <param name="RemovedFiles">Data files removed, including those replaced by a rewrite or given a new deletion vector.</param>
/// <param name="AddedFiles">Data files added: rewrites and files re-added with a deletion vector.</param>
/// <param name="DeletionVectorsAdded">Files whose deleted rows were recorded in a deletion vector.</param>
/// <param name="RewrittenFiles">Files rewritten without the deleted rows.</param>
/// <param name="ChangeFiles">Change data files written for the change data feed.</param>
internal sealed record DeltaDeleteResult(
    long? Version, long DeletedRows, int RemovedFiles, int AddedFiles, int DeletionVectorsAdded, int RewrittenFiles, int ChangeFiles);

/// <summary>
/// Deletes rows from a Delta table through the managed kernel. The engine finds the matching rows file by file; per
/// file the kernel removes it (every live row matched), marks the rows in a deletion vector (the table has them
/// enabled), or has the engine rewrite it without them. On a table with the change data feed, the deleted rows are
/// also written as change data. Not reachable from the public API yet.
/// </summary>
internal static class DeltaKernelDelete
{
    private const string FileColumn = "__seseragi_file";

    /// <summary>Deletes the rows matching <paramref name="predicate"/>; with no predicate, every row.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="predicate">The rows to delete, over the table's logical columns; consumed. A row for which it is
    /// null is kept. Null deletes every row.</param>
    /// <param name="fileFilter">Optional: a predicate that every deleted row satisfies, used to leave out files that
    /// hold none (and to narrow what counts as a conflicting concurrent write). It does not choose rows.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static DeltaDeleteResult Delete(string tablePath, ExprHandle? predicate, Predicate? fileFilter = null, CloudArgs? cloud = null)
    {
        if (predicate is null && fileFilter is not null)
            throw new ArgumentException("A file filter narrows a predicate; without a predicate every row is deleted.", nameof(fileFilter));

        bool isUrl = tablePath.Contains("://", StringComparison.Ordinal);
        IWritableFileSystem fileSystem = isUrl ? new ObjectStoreFileSystem(tablePath.TrimEnd('/'), cloud) : new LocalFileSystem(tablePath);
        string root = fileSystem is LocalFileSystem local ? local.Root : tablePath.TrimEnd('/');
        CloudArgs engineCloud = cloud ?? CloudArgs.Local;
        try
        {
            Snapshot snapshot = DeltaKernelWrite.TryLoad(fileSystem, tablePath, cloud)
                ?? throw new PolarsException($"No Delta table at {tablePath}.");
            IReadOnlyList<AddAction> candidates = fileFilter is null ? snapshot.Files : snapshot.FilesMatching(fileFilter);

            var transaction = Transaction.Start(fileSystem, snapshot, "DELETE");
            transaction.ReadPredicate(fileFilter).ReadFiles(candidates);
            string predicateText = predicate is null ? "true" : PolarsWrapper.ExprToString(predicate);
            transaction.WithParameter("predicate", JsonSerializer.Serialize(new[] { predicateText }));

            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var plan = new Plan(snapshot, now);

            if (predicate is null)
            {
                foreach (AddAction file in candidates) plan.RemoveWhole(file, LiveRows(fileSystem, root, snapshot, file, engineCloud));
            }
            else
            {
                Dictionary<int, long[]> hits = MatchingRows(fileSystem, root, snapshot, candidates, predicate, engineCloud);
                predicate = null; // consumed
                PlanFiles(fileSystem, root, snapshot, candidates, hits, plan, engineCloud);
            }

            if (plan.DeletedRows == 0 && plan.Removes.Count == 0)
                return new DeltaDeleteResult(null, 0, 0, 0, 0, 0, 0);

            foreach (RemoveAction remove in plan.Removes) transaction.Add(remove);
            foreach (AddAction add in plan.Adds) transaction.Add(add);
            foreach (CdcAction cdc in plan.ChangeFiles) transaction.Add(cdc);

            transaction.WithMetric("numDeletedRows", plan.DeletedRows)
                       .WithMetric("numRemovedFiles", plan.Removes.Count)
                       .WithMetric("numAddedFiles", plan.Adds.Count)
                       .WithMetric("numDeletionVectorsAdded", plan.VectorsAdded)
                       .WithMetric("numDeletionVectorsRemoved", plan.Removes.Count(r => r.DeletionVector is not null))
                       .WithMetric("numAddedChangeFiles", plan.ChangeFiles.Count)
                       .WithMetric("numCopiedRows", plan.CopiedRows);
            long version = transaction.Commit();
            DeltaKernelWrite.TryCheckpoint(fileSystem, tablePath, version, cloud);
            return new DeltaDeleteResult(
                version, plan.DeletedRows, plan.Removes.Count, plan.Adds.Count, plan.VectorsAdded, plan.Rewritten, plan.ChangeFiles.Count);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
        finally
        {
            predicate?.Dispose();
            (fileSystem as IDisposable)?.Dispose();
        }
    }

    /// <summary>The actions a delete commits, collected file by file.</summary>
    private sealed class Plan(Snapshot snapshot, long now)
    {
        public List<RemoveAction> Removes { get; } = [];
        public List<AddAction> Adds { get; } = [];
        public List<CdcAction> ChangeFiles { get; } = [];
        public long DeletedRows { get; set; }
        public long CopiedRows { get; set; }
        public int VectorsAdded { get; set; }
        public int Rewritten { get; set; }

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

        public void RemoveWhole(AddAction file, long liveRows)
        {
            Remove(file);
            DeletedRows += liveRows;
        }

        public Snapshot Snapshot => snapshot;
    }

    /// <summary>
    /// The live rows of each candidate file that match the predicate, as row positions in the file, keyed by the
    /// candidate's index. Files without a match are absent. One engine query over all candidates.
    /// </summary>
    private static Dictionary<int, long[]> MatchingRows(
        IFileSystem fileSystem, string root, Snapshot snapshot, IReadOnlyList<AddAction> candidates, ExprHandle predicate, CloudArgs cloud)
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
                LazyFrameHandle lf = DeltaKernelScan.ScanFile(fileSystem, root, snapshot, candidates[i], cloud, withRowIndex: true);
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

    private static void PlanFiles(
        IWritableFileSystem fileSystem, string root, Snapshot snapshot, IReadOnlyList<AddAction> candidates,
        Dictionary<int, long[]> hits, Plan plan, CloudArgs cloud)
    {
        bool useVectors = DeletionVectorsEnabled(snapshot);
        string[] partitions = [.. snapshot.PartitionColumns];
        var vectorFiles = new List<(AddAction File, DeletionVector Vector)>();
        var deletedFrames = new List<LazyFrameHandle>();
        bool changeFeed = ChangeFeedEnabled(snapshot);
        try
        {
            foreach (var (index, rows) in hits.OrderBy(h => h.Key))
            {
                AddAction file = candidates[index];
                plan.DeletedRows += rows.Length;
                if (changeFeed)
                    deletedFrames.Add(DeltaKernelScan.ScanFile(fileSystem, root, snapshot, file, cloud, onlyRows: rows));

                long live = LiveRows(fileSystem, root, snapshot, file, cloud);
                if (rows.Length >= live)
                {
                    plan.Remove(file);
                }
                else if (useVectors)
                {
                    DeletionVector old = file.DeletionVector is null
                        ? DeletionVector.FromRows([])
                        : DeletionVector.Read(fileSystem, file.DeletionVector);
                    vectorFiles.Add((file, old.Union(DeletionVector.FromRows(rows))));
                }
                else
                {
                    if (snapshot.ColumnMappingMode != ColumnMappingMode.None && snapshot.Schema.Fields.Any(f => DeltaKernelWrite.NeedsMapping(f.Type)))
                        throw new PolarsException("Rewriting files of a table with column mapping inside nested columns is not supported yet.");
                    using DataFrameHandle kept = PolarsWrapper.LazyCollect(
                        DeltaKernelScan.ScanFile(fileSystem, root, snapshot, file, cloud, dropRows: rows), PlEngine.Auto);
                    plan.CopiedRows += PolarsWrapper.DataFrameHeight(kept);
                    plan.Remove(file);
                    plan.Adds.AddRange(DeltaKernelWrite.WriteFiles(fileSystem, kept, snapshot.Schema, partitions, plan.Removes[^1].DeletionTimestamp!.Value));
                    plan.Rewritten++;
                }
            }

            if (vectorFiles.Count > 0)
            {
                IReadOnlyList<DeletionVectorDescriptor> descriptors =
                    DeletionVector.WriteFile(fileSystem, [.. vectorFiles.Select(v => v.Vector)]);
                for (int i = 0; i < vectorFiles.Count; i++)
                {
                    AddAction file = vectorFiles[i].File;
                    plan.Remove(file);
                    plan.Adds.Add(new AddAction
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
                    plan.VectorsAdded++;
                }
            }

            if (deletedFrames.Count > 0)
            {
                LazyFrameHandle all = deletedFrames.Count == 1
                    ? deletedFrames[0]
                    : PolarsWrapper.LazyConcat([.. deletedFrames], PlConcatType.Vertical, rechunk: false, parallel: true, toSupertypes: false, maintainOrder: true);
                deletedFrames.Clear();
                LazyFrameHandle withType = PolarsWrapper.LazyWithColumns(all,
                    [PolarsWrapper.Alias(PolarsWrapper.Lit("delete"), DeltaKernelScan.ChangeTypeColumn)]);
                using DataFrameHandle deleted = PolarsWrapper.LazyCollect(withType, PlEngine.Auto);
                plan.ChangeFiles.AddRange(DeltaKernelWrite.WriteChangeFiles(fileSystem, deleted, snapshot.Schema, partitions));
            }
        }
        finally
        {
            foreach (LazyFrameHandle frame in deletedFrames) frame.Dispose();
        }
    }

    /// <summary>The rows of a file that are not deleted: from its statistics when recorded, else counted.</summary>
    private static long LiveRows(IFileSystem fileSystem, string root, Snapshot snapshot, AddAction file, CloudArgs cloud)
    {
        if (file.ParsedStats?.NumRecords is long records)
            return records - (file.DeletionVector?.Cardinality ?? 0);
        LazyFrameHandle count = PolarsWrapper.LazySelect(
            DeltaKernelScan.ScanFile(fileSystem, root, snapshot, file, cloud), [PolarsWrapper.Alias(PolarsWrapper.Len(), "n")]);
        using DataFrameHandle df = PolarsWrapper.LazyCollect(count, PlEngine.Auto);
        using SeriesHandle n = PolarsWrapper.DataFrameGetColumn(df, "n");
        using DataTypeHandle int64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64);
        using SeriesHandle asLong = PolarsWrapper.SeriesCast(n, int64, strict: true, wrapNumerical: false);
        return PolarsWrapper.SeriesGetInt64Fast(asLong, 0);
    }

    private static bool DeletionVectorsEnabled(Snapshot snapshot) =>
        snapshot.Protocol.MinWriterVersion >= 7
        && (snapshot.Protocol.WriterFeatures ?? []).Contains(TableFeatures.DeletionVectors)
        && IsTrue(snapshot, TableFeatures.EnableDeletionVectorsKey);

    private static bool ChangeFeedEnabled(Snapshot snapshot) => IsTrue(snapshot, ChangeFeed.EnableKey);

    private static bool IsTrue(Snapshot snapshot, string key) =>
        snapshot.Metadata.Configuration.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    private static int[] Int32Column(DataFrameHandle df, string name)
    {
        using SeriesHandle column = PolarsWrapper.DataFrameGetColumn(df, name);
        using SeriesHandle chunked = PolarsWrapper.SeriesRechunk(column);
        var array = (Int32Array)PolarsWrapper.SeriesToArrow(chunked);
        using (array) return array.Values.ToArray();
    }

    private static long[] Int64Column(DataFrameHandle df, string name)
    {
        using SeriesHandle column = PolarsWrapper.DataFrameGetColumn(df, name);
        using SeriesHandle chunked = PolarsWrapper.SeriesRechunk(column);
        var array = (Int64Array)PolarsWrapper.SeriesToArrow(chunked);
        using (array) return array.Values.ToArray();
    }
}
