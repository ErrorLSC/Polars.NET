using System.Buffers;
using System.IO.Pipelines;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;
using JsonFormat = Kaminari.Seseragi.Files.Json.JsonFormat;

using static Polars.NET.Core.IO.ManagedFrames;

namespace Polars.NET.Core.Json;

/// <summary>
/// JSON read and written by the managed reader and writer (Seseragi.Files): frames come in and go out as Arrow record
/// batches. Reads decode the input on every core (a file mapped, a stream as it arrives); scans infer the columns once,
/// when made, and read the data each time the plan runs (the columns and leading rows the query needs, a row index
/// numbered by the reader); writes and sinks encode on every core, sinks batch by batch as the streaming engine produces
/// them. Paths with a scheme (<c>s3://</c>, <c>az://</c>, <c>gs://</c>, …) go through the managed object store clients
/// (or the native store where they cannot serve the location).
/// </summary>
/// <remarks>
/// Batches read are handed to the engine without being disposed here: exporting them pins their native buffers until
/// the engine releases them. Handles given to sinks are consumed (also when the sink fails).
/// </remarks>
public static class ManagedJson
{
    /// <summary>The inference lengths from which the whole input is taken (as the engine's reader did).</summary>
    private const ulong WholeInputFrom = 16_777_216;

    // ================================================================= options

    private static JsonFormat FormatOf(PlJsonFormat format) => format switch
    {
        PlJsonFormat.Json => JsonFormat.Document,
        PlJsonFormat.JsonLines => JsonFormat.Lines,
        _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unknown JSON format."),
    };

    /// <summary>Records the columns are inferred from: null (and 0, and very many) take the whole input.</summary>
    private static int? InferLength(ulong? rows) => rows is null or 0 or > WholeInputFrom ? null : (int)rows.Value;

    private static int BatchRows(ulong? rows) => rows is null or 0 ? JsonReadOptions.DefaultBatchSize : (int)Math.Min(rows.Value, int.MaxValue);

    private static long? RowLimit(ulong? rows) => rows is { } n ? (long)Math.Min(n, long.MaxValue) : null;

    /// <summary>The Arrow schema the engine exports for a schema (borrowed): what its frames hold.</summary>
    public static Schema ArrowSchemaOf(SchemaHandle schema) => IO.ManagedFrames.ArrowSchemaOf(schema);

    /// <summary>The partition columns a selector picks from a query's schema (the query and the selector borrowed).</summary>
    public static string[] SelectedColumns(LazyFrameHandle lf, SelectorHandle selector) => IO.ManagedFrames.SelectedColumns(lf, selector);

    // A JSON writer as the shared loops drive it.
    private static FrameWriter Frame(JsonWriter writer) => new(writer.Write, writer.WriteAsync, writer.Complete, writer.CompleteAsync, writer);

    private static JsonReadOptions ReadOptions(string[]? columns, GivenTypes given, ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors,
        PlJsonFormat format) => new()
    {
        Format = FormatOf(format),
        Columns = columns,
        Schema = given.Schema,
        SchemaOverrides = given.Overrides,
        InferSchemaLength = InferLength(inferSchemaLength),
        BatchSize = BatchRows(batchSize),
        IgnoreErrors = ignoreErrors,
        LeaveOpen = true,
    };

    // ================================================================= read

    /// <summary>Reads a JSON file (a document, or newline-delimited) into a frame.</summary>
    /// <param name="path">The file.</param>
    /// <param name="columns">The columns read (all when null).</param>
    /// <param name="schema">Every column and its type (borrowed): nothing inferred; null infers them.</param>
    /// <param name="schemaOverrides">The types of some inferred columns (borrowed).</param>
    /// <param name="inferSchemaLength">The records the columns are inferred from (null or 0: all).</param>
    /// <param name="batchSize">Rows per batch read.</param>
    /// <param name="ignoreErrors">Values that do not fit their column's type are nulls.</param>
    /// <param name="format">The layout.</param>
    public static DataFrameHandle ReadJson(string path, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? inferSchemaLength,
        ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(path);
        if (!File.Exists(path)) throw new FileNotFoundException($"JSON file not found: {path}", path);
        using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
        using JsonReader reader = JsonReader.Open(path, ReadOptions(columns, given, inferSchemaLength, batchSize, ignoreErrors, format));
        return given.CastBack(Import(reader));
    }

    /// <summary>Reads JSON bytes in place (they stay unchanged during the call).</summary>
    /// <inheritdoc cref="ReadJson(string, string[], SchemaHandle, SchemaHandle, ulong?, ulong?, bool, PlJsonFormat)"/>
    public static DataFrameHandle ReadJson(ReadOnlyMemory<byte> bytes, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
        using JsonReader reader = JsonReader.Open(bytes, ReadOptions(columns, given, inferSchemaLength, batchSize, ignoreErrors, format));
        return given.CastBack(Import(reader));
    }

    /// <summary>Reads JSON bytes in place, pinned during the call.</summary>
    /// <inheritdoc cref="ReadJson(string, string[], SchemaHandle, SchemaHandle, ulong?, ulong?, bool, PlJsonFormat)"/>
    public static unsafe DataFrameHandle ReadJson(ReadOnlySpan<byte> bytes, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        // The read is eager: it ends before this returns, the span pinned throughout.
        fixed (byte* p = bytes)
            return ReadJson(PointerMemory.Of(p, bytes.Length), columns, schema, schemaOverrides, inferSchemaLength, batchSize, ignoreErrors, format);
    }

    /// <summary>Reads a stream (from its position; left open) as it arrives.</summary>
    /// <inheritdoc cref="ReadJson(string, string[], SchemaHandle, SchemaHandle, ulong?, ulong?, bool, PlJsonFormat)"/>
    public static DataFrameHandle ReadJson(Stream stream, string[]? columns, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? inferSchemaLength, ulong? batchSize, bool ignoreErrors, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
        using JsonReader reader = JsonReader.Open(stream, ReadOptions(columns, given, inferSchemaLength, batchSize, ignoreErrors, format));
        return given.CastBack(Import(reader));
    }

    /// <summary>
    /// The reader's batches into a frame of contiguous columns (one chunk each, as eager reads return them; the reader's
    /// output owns its memory: the input may go once this returns). The batches are concatenated into one first, on
    /// every core (the long values of views shared, not copied); columns of a type that cannot be (dictionaries) or past
    /// 2 GB are imported as they come and rechunked by the engine. A read failing throws as it is
    /// (<see cref="JsonFormatException"/>, <see cref="JsonValueException"/>, …).
    /// </summary>
    /// <remarks>Batches handed to the engine are not disposed here: their buffers stay pinned until the engine releases them.</remarks>
    private static DataFrameHandle Import(JsonReader reader)
    {
        Schema schema = PolarsArrowCompat.Adapt(reader.Schema, out bool convert);
        List<RecordBatch> batches = [];
        try
        {
            foreach (RecordBatch batch in reader.ReadBatches()) batches.Add(convert ? PolarsArrowCompat.Convert(batch, schema) : batch);
            if (batches.Count > 1 && Concatenated(batches) is { } one)
            {
                foreach (RecordBatch batch in batches) batch.Dispose(); // shared view data stays with the concatenated batch
                batches = [one];
            }
        }
        catch
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
            throw;
        }
        if (batches.Count <= 1) return ArrowStreamInterop.ImportEagerStrict(batches, schema);
        using DataFrameHandle chunked = ArrowStreamInterop.ImportEagerStrict(batches, schema);
        return PolarsWrapper.DataFrameRechunk(chunked);

        static RecordBatch? Concatenated(List<RecordBatch> batches)
        {
            try
            {
                return ArrowConcat.Concatenate(batches, Environment.ProcessorCount);
            }
            catch (Exception e) when (e is NotSupportedException or OverflowException)
            {
                return null;
            }
        }
    }

    // ================================================================= scan

    /// <summary>
    /// A lazy scan of newline-delimited JSON files: a file, a directory, a glob, or a URL of any of them on a store. The
    /// files are listed and the columns inferred now (from the first file holding records); the data is read each time
    /// the plan runs.
    /// </summary>
    /// <param name="path">The file, directory or glob (a URL for a store).</param>
    /// <param name="schema">Every column and its type (borrowed): nothing inferred; null infers them.</param>
    /// <param name="schemaOverrides">The types of some inferred columns (borrowed).</param>
    /// <param name="batchSize">Rows per batch read.</param>
    /// <param name="inferSchemaLength">The records the columns are inferred from (null or 0: all of the first file's).</param>
    /// <param name="nRows">The rows read at most.</param>
    /// <param name="ignoreErrors">Values that do not fit their column's type are nulls.</param>
    /// <param name="rowIndexName">A UInt32 column put first numbering the rows across the files; null for none.</param>
    /// <param name="rowIndexOffset">The row index's first number.</param>
    /// <param name="includePathColumn">A string column put last holding each row's file path; null for none.</param>
    /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static LazyFrameHandle ScanNdjson(string path, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize, ulong? inferSchemaLength,
        ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset, string? includePathColumn, bool hivePartitioning, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        bool url = ManagedStores.IsUrl(path);
        if (!url && !File.Exists(path) && !Directory.Exists(path) && path.IndexOfAny(['*', '?', '[']) < 0)
            throw new FileNotFoundException($"NDJSON file not found: {path}", path);
        var (store, owner) = ManagedStores.Open([path], (cloud ?? CloudArgs.Local).WithProviderFrom(path));
        try
        {
            using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
            var options = new JsonDatasetOptions
            {
                Format = JsonFormat.Lines,
                Store = store,
                Schema = given.Schema,
                SchemaOverrides = given.Overrides,
                InferSchemaLength = InferLength(inferSchemaLength),
                FilePathColumn = includePathColumn,
                Partitioning = hivePartitioning ? HivePartitioning.On : HivePartitioning.Off,
            };
            JsonDataset dataset = JsonDataset.Open(path, options);
            Schema read;
            using (JsonDatasetReader probe = dataset.OpenReader(new JsonReadOptions { RowIndexColumn = rowIndexName })) read = probe.Schema;
            var scan = new Scan(read, rowIndexName, rowIndexOffset, RowLimit(nRows));
            return given.CastBack(ArrowStreamInterop.ScanStream(Batches, scan.Schema, strict: true, new Owners(dataset, owner)));

            IEnumerable<RecordBatch> Batches(StreamScanRequest request)
            {
                var (columns, indexed, limit) = scan.Ask(request);
                var read = new JsonReadOptions
                {
                    Columns = columns,
                    MaxRows = limit,
                    BatchSize = BatchRows(batchSize),
                    IgnoreErrors = ignoreErrors,
                    RowIndexColumn = indexed ? rowIndexName : null,
                };
                using JsonDatasetReader reader = dataset.OpenReader(read);
                foreach (RecordBatch batch in reader.ReadBatches()) yield return scan.Deliver(batch, indexed);
            }
        }
        catch
        {
            owner?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A lazy scan of newline-delimited JSON in memory, read in place each time the plan runs: <paramref name="owner"/>
    /// (what keeps the memory: a copy, a pin) is disposed once the engine releases the scan. The columns are inferred now.
    /// </summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(ReadOnlyMemory<byte> bytes, IDisposable owner, SchemaHandle? schema, SchemaHandle? schemaOverrides,
        ulong? batchSize, ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset)
    {
        ArgumentNullException.ThrowIfNull(owner);
        try
        {
            // The columns, decided once: every run reads with them (nothing inferred again).
            Schema full;
            using var given = new GivenTypes(schema, schemaOverrides, ignoreErrors);
            var first = new JsonReadOptions { Schema = given.Schema, SchemaOverrides = given.Overrides, InferSchemaLength = InferLength(inferSchemaLength) };
            using (JsonReader probe = JsonReader.Open(bytes, first)) full = probe.Schema;
            Schema read;
            using (JsonReader probe = JsonReader.Open(bytes, new JsonReadOptions { Schema = full, RowIndexColumn = rowIndexName })) read = probe.Schema;
            var scan = new Scan(read, rowIndexName, rowIndexOffset, RowLimit(nRows));
            return given.CastBack(ArrowStreamInterop.ScanStream(Batches, scan.Schema, strict: true, owner));

            IEnumerable<RecordBatch> Batches(StreamScanRequest request)
            {
                var (columns, indexed, limit) = scan.Ask(request);
                var options = new JsonReadOptions
                {
                    Schema = full,
                    Columns = columns,
                    MaxRows = limit,
                    BatchSize = BatchRows(batchSize),
                    IgnoreErrors = ignoreErrors,
                    RowIndexColumn = indexed ? rowIndexName : null,
                };
                using JsonReader reader = JsonReader.Open(bytes, options);
                foreach (RecordBatch batch in reader.ReadBatches()) yield return scan.Deliver(batch, indexed);
            }
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>A lazy scan of newline-delimited JSON bytes: copied once into native memory the scan keeps.</summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(ReadOnlySpan<byte> bytes, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize,
        ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset)
    {
        var copy = new NativeBufferWriter(Math.Max(1, bytes.Length));
        try
        {
            bytes.CopyTo(copy.GetSpan(bytes.Length));
            copy.Advance(bytes.Length);
        }
        catch
        {
            copy.Dispose();
            throw;
        }
        return ScanNdjson(copy.WrittenMemory, copy, schema, schemaOverrides, batchSize, inferSchemaLength, nRows, ignoreErrors, rowIndexName, rowIndexOffset);
    }

    /// <summary>
    /// A lazy scan of newline-delimited JSON in memory read in place: pinned until the engine releases the scan
    /// (<see cref="MemoryManager{T}.Unpin"/> tells), unchanged meanwhile.
    /// </summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(ReadOnlyMemory<byte> bytes, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize,
        ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset) =>
        ScanNdjson(bytes, new Pinned(bytes.Pin()), schema, schemaOverrides, batchSize, inferSchemaLength, nRows, ignoreErrors, rowIndexName, rowIndexOffset);

    /// <summary>A lazy scan of a stream (from its position; left open): read now, once, into native memory the scan keeps.</summary>
    /// <inheritdoc cref="ScanNdjson(string, SchemaHandle, SchemaHandle, ulong?, ulong?, ulong?, bool, string, uint, string, bool, CloudArgs)"/>
    public static LazyFrameHandle ScanNdjson(Stream stream, SchemaHandle? schema, SchemaHandle? schemaOverrides, ulong? batchSize,
        ulong? inferSchemaLength, ulong? nRows, bool ignoreErrors, string? rowIndexName, uint rowIndexOffset)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var copy = new NativeBufferWriter(stream.CanSeek ? (int)Math.Clamp(stream.Length - stream.Position, 1, int.MaxValue) : 1 << 16);
        try
        {
            copy.ReadFrom(stream);
        }
        catch
        {
            copy.Dispose();
            throw;
        }
        return ScanNdjson(copy.WrittenMemory, copy, schema, schemaOverrides, batchSize, inferSchemaLength, nRows, ignoreErrors, rowIndexName, rowIndexOffset);
    }
    /// <summary>A dataset and its store, released with the scan.</summary>
    private sealed class Owners(JsonDataset dataset, IDisposable? store) : IDisposable
    {
        public void Dispose()
        {
            dataset.Dispose();
            store?.Dispose();
        }
    }

    // ================================================================= write a frame

    private static JsonWriteOptions WriteOptions(JsonFormat format, PlExternalCompression compression = PlExternalCompression.Uncompressed, int level = -1) => new()
    {
        Format = format,
        Compression = compression switch
        {
            PlExternalCompression.Uncompressed => JsonCompression.None,
            PlExternalCompression.Gzip => JsonCompression.Gzip,
            PlExternalCompression.ZSTD => JsonCompression.Zstd,
            _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unknown compression."),
        },
        CompressionLevel = level < 0 || compression == PlExternalCompression.Uncompressed ? null : level,
        // The batches exported are handed over: encoded side by side, each released once encoded.
        BatchOwnership = BatchOwnership.Writer,
        LeaveOpen = true,
    };

    /// <summary>Writes a frame to a file (replaced; a failed write leaves none).</summary>
    public static void WriteJson(DataFrameHandle df, string path, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(path);
        using FileWriteStream file = LocalWrite(path, mkdir: false, sync: false);
        WriteJson(df, file, format);
        file.Commit();
    }

    /// <summary>Writes a frame to a stream (flushed, left open).</summary>
    public static void WriteJson(DataFrameHandle df, Stream stream, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(stream);
        WriteFrameAsync(df, schema => Frame(JsonWriter.Create(stream, schema, WriteOptions(FormatOf(format)))), false, default).GetAwaiter().GetResult();
    }

    /// <summary>Writes a frame to a stream with asynchronous writes only (flushed, left open).</summary>
    public static Task WriteJsonAsync(DataFrameHandle df, Stream stream, PlJsonFormat format, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return WriteFrameAsync(df, schema => Frame(JsonWriter.Create(stream, schema, WriteOptions(FormatOf(format)))), true, cancellationToken);
    }

    /// <summary>Writes a frame to a buffer writer (a <see cref="PipeWriter"/> is flushed as it fills, never completed).</summary>
    public static void WriteJson(DataFrameHandle df, IBufferWriter<byte> output, PlJsonFormat format)
    {
        ArgumentNullException.ThrowIfNull(output);
        WriteFrameAsync(df, schema => Frame(JsonWriter.Create(output, schema, WriteOptions(FormatOf(format)))), false, default).GetAwaiter().GetResult();
    }

    /// <summary>Writes a frame to a pipe with asynchronous flushes (flushed, not completed).</summary>
    public static Task WriteJsonAsync(DataFrameHandle df, PipeWriter writer, PlJsonFormat format, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return WriteFrameAsync(df, schema => Frame(JsonWriter.Create(writer, schema, WriteOptions(FormatOf(format)))), true, cancellationToken);
    }

    /// <summary>A frame as JSON bytes.</summary>
    public static byte[] WriteJsonToMemory(DataFrameHandle df, PlJsonFormat format)
    {
        using var output = new NativeBufferWriter();
        WriteJson(df, output, format);
        return output.WrittenSpan.ToArray();
    }

    /// <summary>A frame as JSON in native memory, handed over without a copy.</summary>
    public static NativeBuffer WriteJsonNativeMemory(DataFrameHandle df, PlJsonFormat format) =>
        ToNative(stream => WriteJson(df, stream, format));

    // ================================================================= sink a query

    private static Func<Schema, FrameWriter> Lines(Stream stream, PlExternalCompression compression, int level) =>
        schema => Frame(JsonWriter.Create(stream, schema, WriteOptions(JsonFormat.Lines, compression, level)));

    /// <summary>
    /// Sinks a query (consumed) as newline-delimited JSON into a file, local or on a store: published whole once
    /// written (a failed sink leaves none).
    /// </summary>
    /// <param name="lf">The query (consumed).</param>
    /// <param name="path">The file (a URL for a store).</param>
    /// <param name="compression">The file's compression.</param>
    /// <param name="compressionLevel">The level (-1: the codec's default).</param>
    /// <param name="checkExtension">Whether the path's extension must match the compression (as the engine checks it).</param>
    /// <param name="maintainOrder">Whether the rows keep the query's order.</param>
    /// <param name="syncOnClose">Whether a local file is synced to disk before it takes its name.</param>
    /// <param name="mkdir">Whether the parent directories of a local file are created.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static void SinkJson(LazyFrameHandle lf, string path, PlExternalCompression compression, int compressionLevel, bool checkExtension,
        bool maintainOrder, PlSyncOnClose syncOnClose, bool mkdir, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        FileWriteStream file;
        IDisposable? owner = null;
        try
        {
            if (checkExtension) CheckExtension(path, compression);
            if (ManagedStores.IsUrl(path))
            {
                (IWritableFileStore store, owner) = OpenStore(path, cloud);
                file = store.OpenWrite(path, createOnly: false);
            }
            else file = LocalWrite(path, mkdir, syncOnClose != PlSyncOnClose.None);
        }
        catch
        {
            lf.Dispose();
            owner?.Dispose();
            throw;
        }
        try
        {
            using (file)
            {
                SinkAsync(lf, Lines(file, compression, compressionLevel), maintainOrder, false, default).GetAwaiter().GetResult();
                file.Commit();
            }
        }
        finally
        {
            owner?.Dispose();
        }
    }

    /// <summary>Sinks a query (consumed) as newline-delimited JSON into a stream (flushed, left open).</summary>
    public static void SinkJson(LazyFrameHandle lf, Stream stream, PlExternalCompression compression, int compressionLevel, bool maintainOrder)
    {
        ArgumentNullException.ThrowIfNull(stream);
        SinkAsync(lf, Lines(stream, compression, compressionLevel), maintainOrder, false, default).GetAwaiter().GetResult();
    }

    /// <summary>Sinks a query (consumed) into a stream with asynchronous writes only, the engine read off the caller's thread.</summary>
    public static Task SinkJsonAsync(LazyFrameHandle lf, Stream stream, PlExternalCompression compression, int compressionLevel, bool maintainOrder,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return SinkAsync(lf, Lines(stream, compression, compressionLevel), maintainOrder, true, cancellationToken);
    }

    /// <summary>Sinks a query (consumed) into a pipe with asynchronous flushes (flushed, not completed).</summary>
    public static Task SinkJsonAsync(LazyFrameHandle lf, PipeWriter writer, PlExternalCompression compression, int compressionLevel, bool maintainOrder,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return SinkAsync(lf, schema => Frame(JsonWriter.Create(writer, schema, WriteOptions(JsonFormat.Lines, compression, compressionLevel))), maintainOrder, true,
            cancellationToken);
    }

    /// <summary>A query's rows (consumed) as newline-delimited JSON bytes.</summary>
    public static byte[] SinkJsonToMemory(LazyFrameHandle lf, PlExternalCompression compression, int compressionLevel, bool maintainOrder)
    {
        using var output = new NativeBufferWriter();
        SinkAsync(lf, schema => Frame(JsonWriter.Create(output, schema, WriteOptions(JsonFormat.Lines, compression, compressionLevel))), maintainOrder, false, default)
            .GetAwaiter().GetResult();
        return output.WrittenSpan.ToArray();
    }

    /// <summary>A query's rows (consumed) as newline-delimited JSON in native memory, handed over without a copy.</summary>
    public static NativeBuffer SinkJsonNativeMemory(LazyFrameHandle lf, PlExternalCompression compression, int compressionLevel, bool maintainOrder) =>
        ToNative(stream => SinkJson(lf, stream, compression, compressionLevel, maintainOrder));

    // ================================================================= partitioned

    /// <summary>
    /// Sinks a query (consumed) as a dataset of newline-delimited JSON files under <paramref name="path"/>, in Hive
    /// partitions of <paramref name="partitionBy"/> (<c>key=value</c> directories), files named
    /// <c>part-{index:D5}-{write id}.jsonl</c>, cut by rows and size, each published whole.
    /// </summary>
    /// <param name="lf">The query (consumed).</param>
    /// <param name="path">The dataset's directory (a URL for a store).</param>
    /// <param name="partitionBy">The partition columns.</param>
    /// <param name="includeKeys">Whether the partition columns are written into the files too.</param>
    /// <param name="keysPreGrouped">Whether the rows come grouped by their keys (not gathered; a partition's file closed once the next one's rows come).</param>
    /// <param name="maxRowsPerFile">The rows of a file at most (0: no limit).</param>
    /// <param name="approxBytesPerFile">The size a file closes at (0: no limit).</param>
    /// <param name="compression">The files' compression.</param>
    /// <param name="compressionLevel">The level (-1: the codec's default).</param>
    /// <param name="maintainOrder">Whether the rows keep the query's order.</param>
    /// <param name="syncOnClose">Whether local files are synced to disk before they take their names.</param>
    /// <param name="existingData">What happens to files there already.</param>
    /// <param name="cloud">The store's settings (a local path takes none).</param>
    public static void SinkJsonPartitioned(LazyFrameHandle lf, string path, string[] partitionBy, bool includeKeys, bool keysPreGrouped,
        ulong maxRowsPerFile, ulong approxBytesPerFile, PlExternalCompression compression, int compressionLevel, bool maintainOrder,
        PlSyncOnClose syncOnClose, ExistingData existingData, CloudArgs? cloud)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(partitionBy);
        IDisposable? owner = null;
        JsonDatasetWriter? writer = null;
        try
        {
            IWritableFileStore? store = null;
            if (ManagedStores.IsUrl(path)) (store, owner) = OpenStore(path, cloud);
            var options = new JsonDatasetWriteOptions
            {
                Store = store,
                PartitionBy = partitionBy,
                IncludePartitionColumns = includeKeys,
                KeysPreGrouped = keysPreGrouped,
                ExistingData = existingData,
                MaxRowsPerFile = maxRowsPerFile == 0 ? null : (long)Math.Min(maxRowsPerFile, long.MaxValue),
                ApproxBytesPerFile = approxBytesPerFile == 0 ? null : (long)Math.Min(approxBytesPerFile, long.MaxValue),
                SyncOnCommit = syncOnClose != PlSyncOnClose.None,
                Writer = WriteOptions(JsonFormat.Lines, compression, compressionLevel) with { BatchOwnership = BatchOwnership.Caller },
            };
            using CollectBatchesHandle batches = PolarsWrapper.LazyCollectBatches(lf, PlEngine.Streaming, maintainOrder);
            while (true)
            {
                using DataFrameHandle frame = PolarsWrapper.CollectBatchesNext(batches, out bool done);
                if (done || frame.IsInvalid) break;
                using IArrowArrayStream chunks = ArrowStreamInterop.ExportToStream(frame, wideIntegers: true);
                while (chunks.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } chunk)
                {
                    using (chunk)
                    {
                        writer ??= JsonDatasetWriter.Create(path, chunk.Schema, options);
                        writer.Write(chunk);
                    }
                }
            }
            writer?.Complete();
        }
        catch
        {
            writer?.Abort();
            throw;
        }
        finally
        {
            lf.Dispose();
            writer?.Dispose();
            owner?.Dispose();
        }
    }

}
