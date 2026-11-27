using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Polars.NET.Core.Delta;
using Polars.NET.Core.IO.Plugins;
using static Polars.NET.Core.IO.ManagedFrames;

namespace Polars.NET.Core.IO;

/// <summary>
/// Parquet scanned by the managed reader (Seseragi.Files), through the engine's I/O plugin bridge: a file, a
/// directory, a glob, or a URL of any of them on a store, read as one dataset (the files' columns as the first file
/// has them; Hive partitions). Each run reads the columns and leading rows the query needs; its filters prune row
/// groups (statistics, Bloom filters, the page index) and skip rows, and those the reader applies exactly leave the
/// engine's plan.
/// </summary>
public static class ManagedParquet
{
    /// <summary>A lazy scan of Parquet files: listed and their schema read now, the data each time the plan runs.</summary>
    /// <param name="path">The file, directory or glob (a URL for a store).</param>
    /// <param name="rowIndexName">A UInt32 column put first numbering the rows across the files; null for none.</param>
    /// <param name="rowIndexOffset">The row index's first number.</param>
    /// <param name="includePathColumn">A string column put last holding each row's file path; null for none.</param>
    /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns.</param>
    /// <param name="maxDegreeOfParallelism">The most units decoded at once (null: every core).</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static LazyFrameHandle ScanParquet(string path, string? rowIndexName = null, uint rowIndexOffset = 0, string? includePathColumn = null,
        HivePartitioning hivePartitioning = HivePartitioning.Auto, int? maxDegreeOfParallelism = null, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        bool url = ManagedStores.IsUrl(path);
        if (!url && !File.Exists(path) && !Directory.Exists(path) && path.IndexOfAny(['*', '?', '[']) < 0)
            throw new FileNotFoundException($"Parquet file not found: {path}", path);
        CloudArgs settings = (cloud ?? CloudArgs.Local).WithProviderFrom(path);
        var (store, owner) = ManagedStores.Open([path], settings);
        ParquetDataset dataset;
        Scan scan;
        try
        {
            dataset = ParquetDataset.Open(path, new ParquetDatasetOptions
            {
                Store = store,
                FilePathColumn = includePathColumn,
                Partitioning = hivePartitioning,
            });
            try
            {
                Schema read;
                using (ParquetDatasetReader probe = dataset.OpenReader(new ParquetReadOptions { RowIndexColumn = rowIndexName })) read = probe.Schema;
                scan = new Scan(read, rowIndexName, rowIndexOffset, limit: null);
            }
            catch
            {
                dataset.Dispose();
                throw;
            }
        }
        catch
        {
            owner?.Dispose();
            throw;
        }
        int threads = maxDegreeOfParallelism is { } n and > 0 ? n : Environment.ProcessorCount;
        ulong fingerprint = new FingerprintBuilder()
            .Add("parquet").Add(path).Add(settings).Add(scan.Schema).Add(rowIndexName).Add((long)rowIndexOffset).Add(includePathColumn)
            .Add((long)hivePartitioning)
            .Value;
        return ManagedScans.Open($"parquet[{ManagedScans.Shown(path)}]", fingerprint, scan, rowIndexName, Batches, Exactly, new Owners(dataset, owner));

        IEnumerable<RecordBatch> Batches(ScanRun run)
        {
            using ParquetDatasetReader reader = dataset.OpenReader(new ParquetReadOptions
            {
                Columns = run.Columns,
                MaxRows = run.Limit,
                RowIndexColumn = run.Indexed ? rowIndexName : null,
                MaxDegreeOfParallelism = threads,
                Filter = run.Filter,
                FilterRows = run.Filter is not null,
            });
            foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
        }

        bool Exactly(ScalarExpr filter)
        {
            using ParquetDatasetReader reader = dataset.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true });
            return reader.FiltersExactly;
        }
    }

    /// <summary>The dataset and its store, released with the scan.</summary>
    private sealed class Owners(ParquetDataset dataset, IDisposable? store) : IDisposable
    {
        public void Dispose()
        {
            try
            {
                dataset.Dispose();
            }
            finally
            {
                store?.Dispose();
            }
        }
    }
}
