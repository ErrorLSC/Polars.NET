#pragma warning disable CS1573
using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Read a DataFrame from a Parquet file or multiple files via glob patterns.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanParquet and collects the result. 
    /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanParquet directly.
    /// </para>
    /// </summary>
    /// <param name="path">Path to file or glob pattern (e.g. "data/*.parquet" or "s3://bucket/data.parquet").</param>
    /// <param name="columns">Columns to select. If null, select all columns. (Optimized via projection pushdown).</param>
    /// <param name="nRows">Limit number of rows to read (optimization).</param>
    /// <param name="parallel">Parallel strategy.</param>
    /// <param name="lowMemory">Reduce memory usage at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to optimize the query plan.</param>
    /// <param name="glob">Expand glob patterns (default: true).</param>
    /// <param name="allowMissingColumns">Allow missing columns when reading multiple files.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks when reading. (default: false)</param>
    /// <param name="cache">Cache the result after reading. (default: true)</param>
    /// <param name="rowIndexName">If provided, adds a column with the row number.</param>
    /// <param name="rowIndexOffset">Offset for the row index.</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="schema">Manually specify the schema of the file(s).</param>
    /// <param name="hivePartitionSchema">Manually specify the schema for Hive partitioning columns.</param>
    /// <param name="tryParseHiveDates">Whether to try parsing dates in Hive partitioning paths (default: true).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static DataFrame ReadParquet(
        string path,
        string[]? columns = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool glob = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = LazyFrame.ScanParquet(
            path,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates,
            cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Cs.ByName([.. colsToSelect]));
        }

        return lf.Collect();
    }

    /// <summary>
    /// Read Parquet from an in-memory byte array.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanParquet and collects the result.
    /// </para>
    /// </summary>
    /// <param name="buffer">The byte array containing the parquet file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, PolarsSchema?,bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadParquet(
        byte[] buffer,
        string[]? columns = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var lf = LazyFrame.ScanParquet(
            buffer,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Cs.ByName([.. colsToSelect]));
        }

        return lf.Collect();
    }

    /// <summary>
    /// Read Parquet from a Stream.
    /// <para>
    /// Note: This method copies the stream to memory and then uses the Lazy execution engine.
    /// </para>
    /// </summary>
    /// <param name="stream">The input stream containing the parquet file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, PolarsSchema?,bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadParquet(
        Stream stream,
        string[]? columns = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ReadParquet(
            ms.ToArray(),
            columns,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates
        );
    }
    /// <summary>
    /// Read a Parquet file asynchronously.
    /// </summary>
    public static async Task<DataFrame> ReadParquetAsync(
        string path,
        string[]? columns = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool glob = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = LazyFrame.ScanParquet(
            path,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates,
            cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Cs.ByName([.. colsToSelect]));
        }

        return await lf.CollectAsync(useStreaming:true);
    }
}