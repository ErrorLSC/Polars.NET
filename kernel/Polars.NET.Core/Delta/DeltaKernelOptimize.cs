using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Apache.Arrow;
using Kaminari.Sazanami.Curves;
using Kaminari.Sazanami.Ordering;
using Polars.NET.Core.Arrow;
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
    /// <param name="full">On a clustered table, also recluster the data clustered by earlier clustering columns (OPTIMIZE
    /// FULL); otherwise only new data is clustered.</param>
    public static DeltaOptimizeResult Optimize(
        string tablePath, long targetFileSize = 128L * 1024 * 1024, IReadOnlyDictionary<string, string?>? partitionFilters = null,
        string[]? zOrderColumns = null, CloudArgs? cloud = null, bool full = false) =>
        // Retried as a whole on a commit conflict (the files to compact are chosen anew).
        ConflictRetry.Run(() => OptimizeOnce(tablePath, targetFileSize, partitionFilters, zOrderColumns, cloud, full));

    private static DeltaOptimizeResult OptimizeOnce(
        string tablePath, long targetFileSize, IReadOnlyDictionary<string, string?>? partitionFilters,
        string[]? zOrderColumns, CloudArgs? cloud, bool full)
    {
        if (targetFileSize <= 0) throw new ArgumentOutOfRangeException(nameof(targetFileSize), "The target file size must be positive.");
        if (zOrderColumns is { Length: 0 }) zOrderColumns = null;
        try
        {
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            Snapshot snapshot = table.Snapshot;
            if (Clustering.PhysicalColumns(snapshot.DomainMetadata) is { Count: > 0 } clustering)
            {
                if (zOrderColumns is not null)
                    throw new PolarsException(
                        $"OPTIMIZE for a Delta table with clustering cannot specify ZORDER BY. Please remove ZORDER BY ({string.Join(", ", zOrderColumns)}).");
                if (partitionFilters is { Count: > 0 }) Check(snapshot, partitionFilters, null); // no partitions: always refused
                return Cluster(table, tablePath, Clustering.Fields(snapshot.Schema, clustering), targetFileSize, full, cloud);
            }
            if (full) throw new PolarsException("OPTIMIZE FULL is only supported for clustered tables.");
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
            return Commit(table, tablePath, transaction, plan, partitions.Values.Sum(p => p.Count), cloud);
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }

    /// <summary>Commits an optimize's plan with Spark's metrics, then a checkpoint when due.</summary>
    private static DeltaOptimizeResult Commit(DmlTable table, string tablePath, Transaction transaction, DmlPlan plan, int consideredFiles, CloudArgs? cloud)
    {
        foreach (RemoveAction remove in plan.Removes) transaction.Add(remove);
        foreach (AddAction add in plan.Adds) transaction.Add(add);
        long removedBytes = plan.Removes.Sum(r => r.Size ?? 0), addedBytes = plan.Adds.Sum(a => a.Size);
        int vectorsRemoved = plan.Removes.Count(r => r.DeletionVector is not null);
        transaction.WithMetric("numRemovedFiles", plan.Removes.Count)
                   .WithMetric("numAddedFiles", plan.Adds.Count)
                   .WithMetric("numRemovedBytes", removedBytes)
                   .WithMetric("numAddedBytes", addedBytes)
                   .WithMetric("numDeletionVectorsRemoved", vectorsRemoved)
                   .WithMetric("totalConsideredFiles", consideredFiles);
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
            rows = ZOrder(rows, zOrderColumns, schema, table.RowIdColumns, out long height);
            long inputBytes = bin.Sum(f => f.Size);
            long files = Math.Max(1, (inputBytes + target - 1) / target);
            maxRows = (ulong)Math.Max(1, (height + files - 1) / files);
        }
        foreach (AddAction add in DeltaKernelWrite.WriteFiles(table, rows, schema, plan.Now, checkNotNull: false, maxRows, approxBytesPerFile: 0))
            plan.Adds.Add(Rearranged(add));
        foreach (AddAction file in bin) plan.Remove(file, dataChange: false);
    }

    /// <summary>The same file, marked as rows rearranged rather than changed (clustered files with their cube).</summary>
    private static AddAction Rearranged(AddAction add, IReadOnlyDictionary<string, string>? tags = null, string? clusteringProvider = null) => new()
    {
        Path = add.Path,
        PartitionValues = add.PartitionValues,
        Size = add.Size,
        ModificationTime = add.ModificationTime,
        DataChange = false,
        Stats = add.Stats,
        Tags = tags ?? add.Tags,
        ClusteringProvider = clusteringProvider ?? add.ClusteringProvider,
    };

    // ------------------------------------------------------------------ liquid clustering

    /// <summary>The range ids a clustering column is cut into (Spark's <c>mdc.rangeId.max</c>).</summary>
    private const int RangeIds = 1000;

    /// <summary>The rows sampled for the range boundaries.</summary>
    private const int SampleRows = 100_000;

    /// <summary>A cube this large is no longer merged with new data (Spark's <c>minCubeSize.threshold</c>, 100 GiB).</summary>
    private const long MinCubeSize = 100L * 1024 * 1024 * 1024;

    private const string BucketColumn = "__seseragi_cluster_bucket";
    private const string SampleIndexColumn = "__seseragi_sample_index";

    /// <summary>
    /// Clusters the table's data by its clustering columns, as Spark's OPTIMIZE on a table with liquid clustering:
    /// new (unclustered) files, merged with the cubes clustered by the same columns that are still small, or with
    /// <paramref name="full"/> also the data clustered by other columns. Nothing new: no commit. The rows are cut
    /// into ranges of a Hilbert curve over each column's range ids (boundaries from a sample) and every range is
    /// written to its own files; rows are not sorted within a file (Spark's default). One pass over the data
    /// besides the sample; nothing is collected but the sample.
    /// </summary>
    private static DeltaOptimizeResult Cluster(
        DmlTable table, string tablePath, List<StructField[]> columns, long target, bool full, CloudArgs? cloud)
    {
        Snapshot snapshot = table.Snapshot;
        string[] names = [.. columns.Select(Clustering.LogicalName)];
        string cubeColumns = JsonText.StringArray(names);

        bool Current(AddAction f) =>
            f.ClusteringProvider == Clustering.Provider && f.Tags is { } t && t.GetValueOrDefault(Clustering.CubeColumnsTag) == cubeColumns;
        List<AddAction> fresh = [.. snapshot.Files.Where(f => f.ClusteringProvider != Clustering.Provider)];
        List<AddAction> stale = full ? [.. snapshot.Files.Where(f => f.ClusteringProvider == Clustering.Provider && !Current(f))] : [];
        if (fresh.Count == 0 && stale.Count == 0) return new DeltaOptimizeResult(null, 0, 0, 0, 0, 0);
        // Cubes of the current columns that are still small take the new data in (Spark's merge strategy).
        var smallCubes = snapshot.Files.Where(Current)
            .GroupBy(f => f.Tags![Clustering.CubeIdTag])
            .Where(cube => cube.Sum(f => f.Size) < MinCubeSize)
            .SelectMany(cube => cube);
        List<AddAction> files = [.. fresh, .. stale, .. smallCubes];

        long totalRows = files.Sum(f => Math.Max(0, (f.ParsedStats?.NumRecords ?? 0) - (f.DeletionVector?.Cardinality ?? 0)));
        long totalBytes = files.Sum(f => f.Size);
        int buckets = (int)Math.Clamp((totalBytes + target - 1) / target, 1, 1 << 20);

        // The sample: every n-th row's clustering columns.
        ExprHandle[] Keys() => [.. columns.Select((path, i) => PolarsWrapper.Alias(PathExpr(path), $"c{i}"))];
        LazyFrameHandle Rows() => DmlTable.Concat([.. files.Select(f => table.ScanFile(f))])!;
        long step = Math.Max(1, totalRows / SampleRows);
        RangeBoundaries[] boundaries;
        ulong[] bucketBoundaries;
        using (LazyFrameHandle indexed = PolarsWrapper.LazyFrameWithRowIndex(Rows(), SampleIndexColumn, 0))
        {
            LazyFrameHandle sampled = PolarsWrapper.LazyFilter(PolarsWrapper.LazyClone(indexed),
                PolarsWrapper.Eq(PolarsWrapper.Rem(PolarsWrapper.Col(SampleIndexColumn), PolarsWrapper.Lit(step)), PolarsWrapper.Lit(0L)));
            using DataFrameHandle sample = PolarsWrapper.LazyCollect(PolarsWrapper.LazySelect(sampled, Keys()), PlEngine.Streaming);
            using RecordBatch batch = ArrowFfiBridge.ExportDataFrame(sample);
            boundaries = [.. Enumerable.Range(0, columns.Count).Select(i => RangeBoundaries.FromSample(batch.Column(i), RangeIds))];
            ulong[] sampleKeys = KeysOf([.. Enumerable.Range(0, columns.Count).Select(batch.Column)], boundaries, batch.Length);
            bucketBoundaries = ClusteringKeys.BucketBoundaries(sampleKeys, buckets);
        }

        // The data: every row's bucket, from its key; the sink writes each bucket to its own files.
        ExprHandle[] keys = Keys();
        ExprHandle bucket = PolarsWrapper.Alias(PolarsWrapper.MapMany(keys[0], keys[1..], arrays =>
        {
            ulong[] rowKeys = KeysOf(arrays, boundaries, arrays[0].Length);
            var ids = new UInt32Array.Builder().Reserve(rowKeys.Length);
            foreach (ulong key in rowKeys) ids.Append(ClusteringKeys.Bucket(bucketBoundaries, key));
            return ids.Build();
        }, PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt32)), BucketColumn);
        LazyFrameHandle bucketed = PolarsWrapper.LazyWithColumns(Rows(), [bucket]);

        var plan = new DmlPlan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Files of about the target size: a bucket holds about that much; a skewed one (equal keys) is cut by rows.
        ulong maxRows = (ulong)Math.Max(1, (totalRows + buckets - 1) / buckets);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [Clustering.CubeIdTag] = Guid.NewGuid().ToString(),
            [Clustering.CubeColumnsTag] = cubeColumns,
        };
        foreach (AddAction add in DeltaKernelWrite.WriteFiles(
                     table, bucketed, snapshot.Schema, plan.Now, checkNotNull: false, maxRows, approxBytesPerFile: 0, bucketColumn: BucketColumn))
            plan.Adds.Add(Rearranged(add, tags, Clustering.Provider));
        foreach (AddAction file in files) plan.Remove(file, dataChange: false);

        var transaction = Transaction.Start(table.FileSystem, snapshot, "OPTIMIZE");
        transaction.ReadFiles(files);
        // As Spark records an OPTIMIZE of a clustered table.
        transaction.WithParameter("clusterBy", cubeColumns)
                   .WithParameter("predicate", "[]")
                   .WithParameter("auto", "false")
                   .WithParameter("isFull", full ? "true" : "false")
                   .WithParameter("zOrderBy", "[]");
        return Commit(table, tablePath, transaction, plan, files.Count, cloud);
    }

    /// <summary>The expression of a clustering column (a nested field by its path).</summary>
    private static ExprHandle PathExpr(StructField[] path)
    {
        ExprHandle expr = PolarsWrapper.Col(path[0].Name);
        foreach (StructField field in path[1..]) expr = PolarsWrapper.StructFieldByName(expr, [field.Name]);
        return expr;
    }

    /// <summary>The curve keys of rows: their range ids per column along the Hilbert curve, with a noise byte.</summary>
    private static ulong[] KeysOf(IReadOnlyList<IArrowArray> columns, RangeBoundaries[] boundaries, int rows)
    {
        int bits = ClusteringKeys.BitsFor(boundaries.Max(b => b.RangeCount));
        var ids = new ReadOnlyMemory<uint>[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            var column = new uint[rows];
            boundaries[i].Assign(columns[i], column);
            ids[i] = column;
        }
        var keys = new ulong[rows];
        ClusteringKeys.Compute(ids, bits, Random.Shared, keys);
        return keys;
    }

    /// <summary>
    /// Sorts rows along a Z-order curve over the columns. The engine gives each column's zero-based dense rank
    /// (nulls first) and sorts; the curve itself is <see cref="SpaceFillingCurve"/>: ranks quantized to an equal
    /// number of bits, interleaved into 128-bit keys.
    /// </summary>
    private static LazyFrameHandle ZOrder(LazyFrameHandle rows, string[] columns, StructType schema, string[] rowIds, out long height)
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
        return PolarsWrapper.LazySelect(keyed, [.. schema.Fields.Select(f => PolarsWrapper.Col(f.Name)), .. rowIds.Select(PolarsWrapper.Col)]);
    }
}
