using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Sazanami.Curves;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Delta;

/// <summary>What an optimize did.</summary>
/// <param name="Version">The version committed; null when nothing needed rewriting.</param>
/// <param name="RemovedFiles">Data files replaced.</param>
/// <param name="AddedFiles">Data files written.</param>
/// <param name="RemovedBytes">Size of the files replaced.</param>
/// <param name="AddedBytes">Size of the files written.</param>
/// <param name="DeletionVectorsRemoved">Replaced files that had a deletion vector (their deleted rows are gone for good).</param>
internal sealed record DeltaOptimizeResult(
    long? Version, int RemovedFiles, int AddedFiles, long RemovedBytes, long AddedBytes, int DeletionVectorsRemoved);

/// <summary>
/// Rewrites the data files of a Delta table through the managed kernel without changing its rows: compaction of
/// small files and of files with deletion vectors (whose deleted rows are dropped), or Z-ordering. Every change is
/// committed at once, as data that did not change (<c>dataChange</c> false), so readers of the change data feed and
/// streams see nothing. The public Delta API calls it.
/// </summary>
internal static class DeltaKernelOptimize
{
    private const string ZPrefix = "__seseragi_z";

    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="targetFileSize">The size, in bytes, files are compacted toward.</param>
    /// <param name="partitionFilters">Only partitions whose partition columns have these values (text, as in the log).</param>
    /// <param name="zOrderColumns">Z-order the rows of each partition by these columns: every file of the partition is rewritten.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static DeltaOptimizeResult Optimize(
        string tablePath, long targetFileSize = 128L * 1024 * 1024, IReadOnlyDictionary<string, string?>? partitionFilters = null,
        string[]? zOrderColumns = null, CloudArgs? cloud = null) =>
        // Retried as a whole on a commit conflict (the files to compact are chosen anew).
        ConflictRetry.Run(() => OptimizeOnce(tablePath, targetFileSize, partitionFilters, zOrderColumns, cloud));

    private static DeltaOptimizeResult OptimizeOnce(
        string tablePath, long targetFileSize, IReadOnlyDictionary<string, string?>? partitionFilters,
        string[]? zOrderColumns, CloudArgs? cloud)
    {
        if (targetFileSize <= 0) throw new ArgumentOutOfRangeException(nameof(targetFileSize), "The target file size must be positive.");
        if (zOrderColumns is { Length: 0 }) zOrderColumns = null;
        try
        {
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            Snapshot snapshot = table.Snapshot;
            Check(snapshot, partitionFilters, zOrderColumns);

            // Files of the chosen partitions, by partition.
            var partitions = new Dictionary<string, List<AddAction>>(StringComparer.Ordinal);
            foreach (AddAction file in snapshot.Files)
            {
                Dictionary<string, string?> values = snapshot.PartitionValuesOf(file);
                if (partitionFilters is not null && partitionFilters.Any(f => values.GetValueOrDefault(f.Key) != f.Value)) continue;
                string key = string.Join("\u0001", snapshot.PartitionColumns.Select(c => values.GetValueOrDefault(c) is string v ? "V" + v : "N"));
                if (!partitions.TryGetValue(key, out List<AddAction>? list)) partitions[key] = list = [];
                list.Add(file);
            }

            List<List<AddAction>> bins = zOrderColumns is null
                ? [.. partitions.Values.SelectMany(files => CompactionBins(files, targetFileSize))]
                : [.. partitions.Values]; // Z-ordering rewrites every file of a chosen partition
            if (bins.Count == 0) return new DeltaOptimizeResult(null, 0, 0, 0, 0, 0);
            table.EnsureFilesWritable();

            var plan = new DmlPlan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            foreach (List<AddAction> bin in bins)
                Rewrite(table, bin, zOrderColumns, targetFileSize, plan);

            var transaction = Transaction.Start(table.FileSystem, snapshot, "OPTIMIZE");
            // Rewriting reads the files: a concurrent change to one of them is a conflict; new files are not.
            transaction.ReadFiles(bins.SelectMany(b => b));
            transaction.WithParameter("predicate", JsonText.StringArray(
                    (partitionFilters ?? new Dictionary<string, string?>()).Select(f => f.Value is null ? $"{f.Key} IS NULL" : $"{f.Key} = '{f.Value}'").ToArray()))
                .WithParameter("zOrderBy", JsonText.StringArray(zOrderColumns ?? []))
                .WithParameter("clusterBy", "[]")
                .WithParameter("auto", "false");
            foreach (RemoveAction remove in plan.Removes) transaction.Add(remove);
            foreach (AddAction add in plan.Adds) transaction.Add(add);
            long removedBytes = plan.Removes.Sum(r => r.Size ?? 0), addedBytes = plan.Adds.Sum(a => a.Size);
            int vectorsRemoved = plan.Removes.Count(r => r.DeletionVector is not null);
            transaction.WithMetric("numRemovedFiles", plan.Removes.Count)
                       .WithMetric("numAddedFiles", plan.Adds.Count)
                       .WithMetric("numRemovedBytes", removedBytes)
                       .WithMetric("numAddedBytes", addedBytes)
                       .WithMetric("numDeletionVectorsRemoved", vectorsRemoved)
                       .WithMetric("totalConsideredFiles", partitions.Values.Sum(p => p.Count));
            // The size distribution of the files written, as Spark reports it.
            long[] sizes = [.. plan.Adds.Select(f => f.Size).Order()];
            if (sizes.Length > 0)
            {
                long Percentile(double q) => sizes[Math.Min(sizes.Length - 1, (int)(q * sizes.Length))];
                transaction.WithMetric("minFileSize", sizes[0]).WithMetric("maxFileSize", sizes[^1])
                           .WithMetric("p25FileSize", Percentile(0.25)).WithMetric("p50FileSize", Percentile(0.5))
                           .WithMetric("p75FileSize", Percentile(0.75));
            }

            long version = transaction.Commit();
            DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, version, cloud);
            return new DeltaOptimizeResult(version, plan.Removes.Count, plan.Adds.Count, removedBytes, addedBytes, vectorsRemoved);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }

    private static void Check(Snapshot snapshot, IReadOnlyDictionary<string, string?>? partitionFilters, string[]? zOrderColumns)
    {
        foreach (string column in partitionFilters?.Keys ?? [])
        {
            if (!snapshot.PartitionColumns.Contains(column))
                throw new PolarsException($"'{column}' is not a partition column; optimize filters by partition columns only.");
        }
        foreach (string column in zOrderColumns ?? [])
        {
            StructField field = snapshot.Schema.Field(column) ?? throw new PolarsException($"The table has no column '{column}' to Z-order by.");
            if (snapshot.PartitionColumns.Contains(column))
                throw new PolarsException($"'{column}' is a partition column: every row of a partition has the same value, so Z-ordering by it does nothing.");
            if (field.Type is not (PrimitiveType or DecimalType) || field.Type is PrimitiveType { Name: PrimitiveType.Binary or PrimitiveType.Variant })
                throw new PolarsException($"The column '{column}' of type {field.Type} cannot be Z-ordered by.");
        }
        if (zOrderColumns is { Length: > 63 }) throw new PolarsException("Z-ordering takes at most 63 columns.");
    }

    /// <summary>
    /// Bins of one partition's files to compact into one file each: files below half the target size, or with a
    /// deletion vector, packed in size order up to 1.2 times the target. A bin worth no rewrite (a single file
    /// without a deletion vector) is dropped.
    /// </summary>
    private static IEnumerable<List<AddAction>> CompactionBins(List<AddAction> files, long target)
    {
        var candidates = files.Where(f => f.Size < target / 2 || f.DeletionVector is not null).OrderBy(f => f.Size).ToList();
        long max = target + target / 5;
        var bin = new List<AddAction>();
        long size = 0;
        foreach (AddAction file in candidates)
        {
            if (bin.Count > 0 && size + file.Size > max)
            {
                if (Worth(bin)) yield return bin;
                bin = [];
                size = 0;
            }
            bin.Add(file);
            size += file.Size;
        }
        if (Worth(bin)) yield return bin;

        static bool Worth(List<AddAction> bin) => bin.Count > 1 || bin.Any(f => f.DeletionVector is not null);
    }

    /// <summary>Replaces the files of a bin (all of one partition) with files holding their live rows.</summary>
    private static void Rewrite(DmlTable table, List<AddAction> bin, string[]? zOrderColumns, long target, DmlPlan plan)
    {
        StructType schema = table.Snapshot.Schema;
        LazyFrameHandle rows = DmlTable.Concat([.. bin.Select(f => table.ScanFile(f))])!;

        // A compacted bin is streamed into one file: bins are packed by size on disk, and splitting by the sink's
        // in-memory estimate would leave small files that the next optimize compacts again. Z-ordered rows are cut
        // in order into files of about the target size on disk, by rows.
        ulong maxRows = 0;
        if (zOrderColumns is not null)
        {
            rows = ZOrder(rows, zOrderColumns, schema, out long height);
            long inputBytes = bin.Sum(f => f.Size);
            long files = Math.Max(1, (inputBytes + target - 1) / target);
            maxRows = (ulong)Math.Max(1, (height + files - 1) / files);
        }
        foreach (AddAction add in DeltaKernelWrite.WriteFiles(table, rows, schema, plan.Now, checkNotNull: false, maxRows, approxBytesPerFile: 0))
            plan.Adds.Add(Rearranged(add));
        foreach (AddAction file in bin) plan.Remove(file, dataChange: false);
    }

    /// <summary>The same file, marked as rows rearranged rather than changed.</summary>
    private static AddAction Rearranged(AddAction add) => new()
    {
        Path = add.Path,
        PartitionValues = add.PartitionValues,
        Size = add.Size,
        ModificationTime = add.ModificationTime,
        DataChange = false,
        Stats = add.Stats,
        Tags = add.Tags,
    };

    /// <summary>
    /// Sorts rows along a Z-order curve over the columns. The engine gives each column's zero-based dense rank
    /// (nulls first) and sorts; the curve itself is <see cref="SpaceFillingCurve"/>: ranks quantized to an equal
    /// number of bits, interleaved into 128-bit keys.
    /// </summary>
    private static LazyFrameHandle ZOrder(LazyFrameHandle rows, string[] columns, StructType schema, out long height)
    {
        int k = columns.Length;
        int bits = SpaceFillingCurve.BitsPerDimension(k);
        using DataTypeHandle uint64 = PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt64);

        var ranks = new ExprHandle[k];
        for (int i = 0; i < k; i++)
        {
            ExprHandle rank = PolarsWrapper.ExprCast(
                PolarsWrapper.Rank(PolarsWrapper.Col(columns[i]), PlRankMethod.Dense, false, null),
                PolarsWrapper.DataTypeExprFromDataType(uint64), strict: true, wrapNumerical: false);
            // Zero-based, nulls sharing 0 with the smallest value.
            ranks[i] = PolarsWrapper.Alias(
                PolarsWrapper.FillNull(PolarsWrapper.Sub(rank, PolarsWrapper.Lit(1UL)), PolarsWrapper.Lit(0UL)), $"{ZPrefix}{i}");
        }
        using DataFrameHandle ranked = PolarsWrapper.LazyCollect(PolarsWrapper.LazyWithColumns(rows, ranks), PlEngine.Auto);

        int rowCount = checked((int)PolarsWrapper.DataFrameHeight(ranked));
        height = rowCount;
        var coordinates = new ReadOnlyMemory<ulong>[k];
        for (int i = 0; i < k; i++)
        {
            ulong[] ordinals = DmlTable.UInt64Column(ranked, $"{ZPrefix}{i}");
            ulong max = ordinals.Length == 0 ? 0 : ordinals.Max();
            SpaceFillingCurve.Quantize(ordinals, max, bits, ordinals);
            coordinates[i] = ordinals;
        }
        var keys = new UInt128[rowCount];
        SpaceFillingCurve.ZOrder(coordinates, bits, keys);
        ulong[] high = new ulong[rowCount], low = new ulong[rowCount];
        for (int p = 0; p < rowCount; p++)
        {
            high[p] = (ulong)(keys[p] >> 64);
            low[p] = (ulong)keys[p];
        }

        LazyFrameHandle keyed = PolarsWrapper.LazyWithColumns(PolarsWrapper.DataFrameToLazy(PolarsWrapper.CloneDataFrame(ranked)),
        [
            PolarsWrapper.Alias(PolarsWrapper.Lit(PolarsWrapper.SeriesNew(ZPrefix + "hi", high)), ZPrefix + "hi"),
            PolarsWrapper.Alias(PolarsWrapper.Lit(PolarsWrapper.SeriesNew(ZPrefix + "lo", low)), ZPrefix + "lo"),
        ]);
        keyed = PolarsWrapper.LazyFrameSort(keyed, [PolarsWrapper.Col(ZPrefix + "hi"), PolarsWrapper.Col(ZPrefix + "lo")], [false], [false], maintainOrder: true);
        return PolarsWrapper.LazySelect(keyed, [.. schema.Fields.Select(f => PolarsWrapper.Col(f.Name))]);
    }
}
