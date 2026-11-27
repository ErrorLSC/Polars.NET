using Polars.NET.Core;
using Polars.NET.Core.Json;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Lazily read newline delimited JSON (NDJSON): a file, a directory of files, a glob, or any of them on a cloud store.
    /// The files are listed and the columns inferred now (from the first file holding records); the data is read each
    /// time the query runs: only the columns it reads and the leading rows it keeps, its filters pushed into the reader
    /// (applied exactly where the reader can; the engine applies the rest).
    /// </summary>
    /// <param name="path">The file, directory or glob (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
    /// <param name="schema">Every column and its type: nothing is inferred, fields it does not name are passed over, fields a record lacks are nulls.</param>
    /// <param name="inferSchemaLength">Rows to infer the schema from (100 by default). Null or 0 infers it from the whole input (of the first file); values above 16,777,216 do the same.</param>
    /// <param name="batchSize">Rows per batch read.</param>
    /// <param name="nRows">Limit the number of rows to read.</param>
    /// <param name="lowMemory">Accepted for compatibility; reading streams its input whatever it says.</param>
    /// <param name="rechunk">Accepted for compatibility; it changes nothing.</param>
    /// <param name="ignoreErrors">A value that does not fit its column's type is a null instead of an error (syntax errors still are errors).</param>
    /// <param name="rowIndexName">If provided, adds a first column (UInt32) numbering the rows across the files.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a last column with each row's file path.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="schemaOverrides">The types of some inferred columns (with <paramref name="schema"/>: of some of its columns).</param>
    /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns (off by default).</param>
    /// <remarks>
    /// String columns take other JSON values as their JSON text; binary columns take base64 strings. Errors are
    /// <see cref="Kaminari.Seseragi.Files.Json.JsonFormatException"/> (not newline-delimited JSON) and
    /// <see cref="Kaminari.Seseragi.Files.Json.JsonValueException"/> (a value its column cannot hold), naming the file and the line.
    /// </remarks>
    public static LazyFrame ScanNdjson(
        string path,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        CloudOptions? cloudOptions = null,
        IntoSchema? schemaOverrides = null,
        bool hivePartitioning = false)
        => ScanNdjsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ScanNdjson(path, s, o, batchSize, inferSchemaLength, nRows, ignoreErrors,
            rowIndexName, rowIndexOffset, includePathColumn, hivePartitioning, CloudOptions.ToCloudArgs(cloudOptions)));

    // ---------------------------------------------------------
    // Scan NDJSON (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The bytes are copied (into native memory the scan keeps).</remarks>
    /// <inheritdoc cref="ScanNdjson(string, IntoSchema?, ulong?, ulong?, ulong?, bool, bool, bool, string?, uint, string?, CloudOptions?, IntoSchema?, bool)"/>
    public static LazyFrame ScanNdjson(
        byte[] buffer,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        IntoSchema? schemaOverrides = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return ScanNdjson(buffer.AsSpan(), schema, inferSchemaLength, batchSize, nRows, lowMemory, rechunk, ignoreErrors, rowIndexName, rowIndexOffset, schemaOverrides);
    }

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The bytes are copied (into native memory the scan keeps).</remarks>
    /// <inheritdoc cref="ScanNdjson(string, IntoSchema?, ulong?, ulong?, ulong?, bool, bool, bool, string?, uint, string?, CloudOptions?, IntoSchema?, bool)"/>
    public static LazyFrame ScanNdjson(
        ReadOnlySpan<byte> buffer,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        IntoSchema? schemaOverrides = null)
    {
        try
        {
            return new LazyFrame(ManagedJson.ScanNdjson(buffer, schema?.Consume().Handle, schemaOverrides?.Consume().Handle, batchSize, inferSchemaLength,
                nRows, ignoreErrors, rowIndexName, rowIndexOffset));
        }
        finally
        {
            schema?.DisposeTempSchema();
            schemaOverrides?.DisposeTempSchema();
        }
    }

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The memory is read in place, without a copy: the memory stays pinned until the last frame reading it is released, and must not change meanwhile. Wrap unmanaged memory in a <see cref="System.Buffers.MemoryManager{T}"/> and free it only once it is unpinned (<see cref="System.Buffers.MemoryManager{T}.Unpin"/>): a query can still be finishing on a background thread right after its frames are released.</remarks>
    /// <inheritdoc cref="ScanNdjson(string, IntoSchema?, ulong?, ulong?, ulong?, bool, bool, bool, string?, uint, string?, CloudOptions?, IntoSchema?, bool)"/>
    public static LazyFrame ScanNdjson(
        ReadOnlyMemory<byte> buffer,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        IntoSchema? schemaOverrides = null)
        => ScanNdjsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ScanNdjson(buffer, s, o, batchSize, inferSchemaLength, nRows, ignoreErrors,
            rowIndexName, rowIndexOffset));

    // ---------------------------------------------------------
    // Scan NDJSON (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read NDJSON from a Stream: read now (from its position; the stream is left open), once, into native memory
    /// the scan keeps.
    /// </summary>
    /// <inheritdoc cref="ScanNdjson(string, IntoSchema?, ulong?, ulong?, ulong?, bool, bool, bool, string?, uint, string?, CloudOptions?, IntoSchema?, bool)"/>
    public static LazyFrame ScanNdjson(
        Stream stream,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        IntoSchema? schemaOverrides = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return ScanNdjsonWith(schema, schemaOverrides, (s, o) => ManagedJson.ScanNdjson(stream, s, o, batchSize, inferSchemaLength, nRows, ignoreErrors,
            rowIndexName, rowIndexOffset));
    }

    // The schemas given (borrowed while the scan is made; temporary ones disposed after it).
    private static LazyFrame ScanNdjsonWith(IntoSchema? schema, IntoSchema? overrides, Func<SchemaHandle?, SchemaHandle?, LazyFrameHandle> scan)
    {
        try
        {
            return new LazyFrame(scan(schema?.Consume().Handle, overrides?.Consume().Handle));
        }
        finally
        {
            schema?.DisposeTempSchema();
            overrides?.DisposeTempSchema();
        }
    }
}
