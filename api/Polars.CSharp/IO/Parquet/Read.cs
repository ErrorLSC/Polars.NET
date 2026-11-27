#pragma warning disable CS1573
using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{
    /// <summary>
    /// Read a DataFrame from a Parquet file or multiple files via glob patterns.
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
    /// <param name="hivePartitioning">Enable Hive partitioning inference (default: false).</param>
    /// <param name="hivePartitionSchema">Manually specify the schema for Hive partitioning columns.</param>
    /// <param name="tryParseHiveDates">Whether to try parsing dates in Hive partitioning paths (default: true).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="ignoreExtraColumns">Leave out the columns of a file that the schema does not name, instead of raising.</param>
    /// <param name="castOptions">The casts allowed when a file's types differ from the schema (default: none, a mismatch raises).</param>
    /// <param name="hiddenFilePrefixes">Skip the files whose names begin with one of these prefixes (unstable in the engine).</param>
    /// <param name="decryption">The keys of files written with Parquet modular encryption (experimental).</param>
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        CloudOptions? cloudOptions = null,
        bool ignoreExtraColumns = false,
        ScanCastOptions? castOptions = null,
        string[]? hiddenFilePrefixes = null,
        ParquetDecryption? decryption = null)
        => SelectColumns(
            LazyFrame.ScanParquet(path, nRows, parallel, lowMemory, useStatistics, glob, allowMissingColumns, rechunk, cache,
                rowIndexName, rowIndexOffset, includePathColumn, schema, hivePartitioning, hivePartitionSchema, tryParseHiveDates,
                cloudOptions, ignoreExtraColumns, castOptions, hiddenFilePrefixes, decryption),
            columns, rowIndexName, includePathColumn).CollectEagerRead();

    /// <summary>
    /// Read Parquet from an in-memory byte array.
    /// </summary>
    /// <param name="buffer">The bytes; the engine copies them.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?,bool, IntoSchema?, bool, CloudOptions?, bool, ScanCastOptions?, string[], ParquetDecryption?)"/>
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        bool ignoreExtraColumns = false,
        ScanCastOptions? castOptions = null,
        ParquetDecryption? decryption = null)
        => SelectColumns(
            LazyFrame.ScanParquet(buffer, nRows, parallel, lowMemory, useStatistics, allowMissingColumns, rechunk, cache,
                rowIndexName, rowIndexOffset, includePathColumn, schema, hivePartitioning, hivePartitionSchema, tryParseHiveDates,
                ignoreExtraColumns, castOptions, decryption),
            columns, rowIndexName, includePathColumn).CollectEagerRead();

    /// <summary>
    /// Read Parquet from an in-memory byte array.
    /// </summary>
    /// <param name="buffer">The bytes; the engine copies them.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?,bool, IntoSchema?, bool, CloudOptions?, bool, ScanCastOptions?, string[], ParquetDecryption?)"/>
    public static DataFrame ReadParquet(
        ReadOnlySpan<byte> buffer,
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        bool ignoreExtraColumns = false,
        ScanCastOptions? castOptions = null,
        ParquetDecryption? decryption = null)
        => SelectColumns(
            LazyFrame.ScanParquet(buffer, nRows, parallel, lowMemory, useStatistics, allowMissingColumns, rechunk, cache,
                rowIndexName, rowIndexOffset, includePathColumn, schema, hivePartitioning, hivePartitionSchema, tryParseHiveDates,
                ignoreExtraColumns, castOptions, decryption),
            columns, rowIndexName, includePathColumn).CollectEagerRead();

    /// <summary>
    /// Read Parquet from an in-memory byte array.
    /// </summary>
    /// <param name="buffer">The bytes, read in place without a copy: the memory stays pinned until the last frame reading it is released, and must not change meanwhile. Wrap unmanaged memory in a <see cref="System.Buffers.MemoryManager{T}"/> and free it only once the engine unpins it (<see cref="System.Buffers.MemoryManager{T}.Unpin"/>): a query can still be finishing on a background thread right after its frames are released.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?,bool, IntoSchema?, bool, CloudOptions?, bool, ScanCastOptions?, string[], ParquetDecryption?)"/>
    public static DataFrame ReadParquet(
        ReadOnlyMemory<byte> buffer,
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        bool ignoreExtraColumns = false,
        ScanCastOptions? castOptions = null,
        ParquetDecryption? decryption = null)
        => SelectColumns(
            LazyFrame.ScanParquet(buffer, nRows, parallel, lowMemory, useStatistics, allowMissingColumns, rechunk, cache,
                rowIndexName, rowIndexOffset, includePathColumn, schema, hivePartitioning, hivePartitionSchema, tryParseHiveDates,
                ignoreExtraColumns, castOptions, decryption),
            columns, rowIndexName, includePathColumn).CollectEagerRead();

    /// <summary>
    /// Read Parquet from a Stream.
    /// </summary>
    /// <param name="stream">The input stream containing the parquet file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?,bool, IntoSchema?, bool, CloudOptions?, bool, ScanCastOptions?, string[], ParquetDecryption?)"/>
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        bool ignoreExtraColumns = false,
        ScanCastOptions? castOptions = null,
        ParquetDecryption? decryption = null)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);

        return ReadParquet(
            new ReadOnlySpan<byte>(ms.GetBuffer(), 0, (int)ms.Length),
            columns, nRows, parallel, lowMemory, useStatistics, allowMissingColumns, rechunk, cache,
            rowIndexName, rowIndexOffset, includePathColumn, schema, hivePartitioning, hivePartitionSchema, tryParseHiveDates,
            ignoreExtraColumns, castOptions, decryption);
    }

    /// <summary>
    /// Read a Parquet file asynchronously.
    /// </summary>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?,bool, IntoSchema?, bool, CloudOptions?, bool, ScanCastOptions?, string[], ParquetDecryption?)"/>
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        CloudOptions? cloudOptions = null,
        bool ignoreExtraColumns = false,
        ScanCastOptions? castOptions = null,
        string[]? hiddenFilePrefixes = null,
        ParquetDecryption? decryption = null)
        => await SelectColumns(
            LazyFrame.ScanParquet(path, nRows, parallel, lowMemory, useStatistics, glob, allowMissingColumns, rechunk, cache,
                rowIndexName, rowIndexOffset, includePathColumn, schema, hivePartitioning, hivePartitionSchema, tryParseHiveDates,
                cloudOptions, ignoreExtraColumns, castOptions, hiddenFilePrefixes, decryption),
            columns, rowIndexName, includePathColumn).CollectAsync();

    /// <summary>The requested columns, plus the row index and path columns the read adds.</summary>
    private static LazyFrame SelectColumns(LazyFrame lf, string[]? columns, string? rowIndexName, string? includePathColumn)
    {
        if (columns is not { Length: > 0 }) return lf;
        string[] selected = [.. columns, .. Added(rowIndexName), .. Added(includePathColumn)];
        return lf.Select(Cs.ByName(selected));

        string[] Added(string? name) => string.IsNullOrEmpty(name) || columns.Contains(name) ? [] : [name];
    }
}
