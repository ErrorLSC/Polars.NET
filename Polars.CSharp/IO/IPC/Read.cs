#pragma warning disable CS1573
using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    // ---------------------------------------------------------
    // Read IPC (File / Cloud)
    // ---------------------------------------------------------

    /// <summary>
    /// Read an Arrow IPC (Feather v2) file into a DataFrame.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanIpc and collects the result. 
    /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanIpc directly.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the IPC file, glob pattern, or cloud path (e.g., "s3://...").</param>
    /// <param name="columns">Columns to select. If null, select all columns. (Optimized via projection pushdown).</param>
    /// <param name="schema">Optional schema to enforce. If not provided, the schema is inferred from the file footer.</param>
    /// <param name="nRows">Limit the number of rows to scan.</param>
    /// <param name="rechunk">Make sure the DataFrame is contiguous in memory (default: false).</param>
    /// <param name="cache">Cache the result of the scan (default: true).</param>
    /// <param name="glob">Expand glob patterns (default: true).</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="hivePartitioning">Enable Hive partitioning inference (default: false).</param>
    /// <param name="hivePartitionSchema">Manually specify the schema for Hive partitioning columns.</param>
    /// <param name="tryParseHiveDates">Whether to try parsing dates in Hive partitioning paths (default: true).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static DataFrame ReadIpc(
        string path,
        string[]? columns = null,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        bool glob = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null)
    {
        var lf = LazyFrame.ScanIpc(
            path,
            schema,
            nRows,
            rechunk,
            cache,
            glob,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
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

    // ---------------------------------------------------------
    // Read IPC (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read Arrow IPC (Feather v2) from in-memory bytes.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanIpc and collects the result.
    /// </para>
    /// </summary>
    /// <param name="buffer">The byte array containing the IPC data.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadIpc(string, string[], PolarsSchema?, ulong?, bool, bool, bool, string?, uint, string?, bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadIpc(
        byte[] buffer,
        string[]? columns = null,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var lf = LazyFrame.ScanIpc(
            buffer,
            schema,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
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

    // ---------------------------------------------------------
    // Read IPC (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read Arrow IPC (Feather v2) from a Stream.
    /// <para>
    /// Note: This reads the stream fully into memory and then uses the Lazy execution engine.
    /// </para>
    /// </summary>
    /// <param name="stream">The input stream containing the IPC data.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadIpc(string, string[], PolarsSchema?, ulong?, bool, bool, bool, string?, uint, string?, bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadIpc(
        Stream stream,
        string[]? columns = null,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ReadIpc(
            ms.ToArray(),
            columns,
            schema,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates
        );
    }
}