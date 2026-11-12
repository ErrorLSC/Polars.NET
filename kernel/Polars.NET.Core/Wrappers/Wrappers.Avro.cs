using Apache.Arrow;
using Apache.Arrow.Ipc;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Enums;

namespace Polars.NET.Core.Wrappers;

/// <summary>
/// Avro runs on the managed Seseragi.Files reader and writer: files stream through Arrow record batches into and
/// out of the engine.
/// </summary>
/// <remarks>
/// Batches read from a file are handed to the engine without being disposed here: exporting them pins their native
/// buffers until the engine releases them, after which the GC reclaims them (an imported Arrow buffer may not be
/// disposed while pinned).
/// </remarks>
public readonly partial struct PolarsWrapper
{
    /// <summary>Record name written when none (or the empty name) is given.</summary>
    private const string DefaultAvroRecordName = "topLevelRecord";

    private static AvroReadOptions AvroOptions(ulong? nRows, string[]? columns, int[]? projection, bool parallel) => new()
    {
        RowLimit = nRows is { } n ? (long)Math.Min(n, long.MaxValue) : null,
        Columns = columns,
        ColumnIndices = projection,
        MaxDegreeOfParallelism = parallel ? -1 : 1,
        LeaveOpen = true,
    };

    internal static IAvroCompression ToAvroCompression(PlAvroCompression compression) => compression switch
    {
        PlAvroCompression.Uncompressed => NoCompression.Instance,
        PlAvroCompression.Deflate => new DeflateCompression(),
        PlAvroCompression.Snappy => SnappyCompression.Instance,
        PlAvroCompression.Zstandard => new ZstandardCompression(),
        _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unknown Avro compression."),
    };

    private static AvroWriteOptions AvroWriteOptionsFor(PlAvroCompression compression, string? name) => new()
    {
        Compression = ToAvroCompression(compression),
        RecordName = string.IsNullOrEmpty(name) ? DefaultAvroRecordName : name,
        LeaveOpen = true,
    };

    // ---------------------------------------------------------------- read

    public static DataFrameHandle ReadAvro(string path, ulong? nRows, string[]? columns, int[]? projection, bool parallel = true)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Avro file not found: {path}", path);
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.SequentialScan);
        return ReadAvro(stream, nRows, columns, projection, parallel);
    }

    public static unsafe DataFrameHandle ReadAvro(ReadOnlySpan<byte> buffer, ulong? nRows, string[]? columns, int[]? projection, bool parallel = true)
    {
        if (buffer.Length == 0) throw new ArgumentException("Buffer cannot be null or empty", nameof(buffer));
        // The read is eager and finishes before this method returns, so the span stays pinned throughout.
        fixed (byte* p = buffer)
        {
            using var stream = new UnmanagedMemoryStream(p, buffer.Length);
            return ReadAvro(stream, nRows, columns, projection, parallel);
        }
    }

    /// <summary>Reads an Avro stream (left open) into a DataFrame, block by block.</summary>
    public static DataFrameHandle ReadAvro(Stream stream, ulong? nRows, string[]? columns, int[]? projection, bool parallel = true)
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var reader = AvroReader.Open(stream, AvroOptions(nRows, columns, projection, parallel));
        var schema = PolarsArrowCompat.Adapt(reader.Schema, out bool convert);
        var batches = reader.ReadBatches();
        return ArrowStreamInterop.ImportEagerStrict(convert ? batches.Select(b => PolarsArrowCompat.Convert(b, schema)) : batches, schema);
    }

    /// <summary>
    /// A lazy scan of an Avro file: the header is read now for the schema, the data each time the plan runs,
    /// streamed block by block.
    /// </summary>
    public static LazyFrameHandle ScanAvro(string path, ulong? nRows, string[]? columns, int[]? projection, bool parallel = true)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"Avro file not found: {path}", path);
        var options = AvroOptions(nRows, columns, projection, parallel) with { LeaveOpen = false };
        Schema arrowSchema;
        using (var probe = AvroReader.Open(path, options)) arrowSchema = probe.Schema;
        var schema = PolarsArrowCompat.Adapt(arrowSchema, out bool convert);
        return ArrowStreamInterop.ScanStream(Batches, schema, strict: true);

        IEnumerable<RecordBatch> Batches()
        {
            using var reader = AvroReader.Open(path, options);
            foreach (var b in reader.ReadBatches()) yield return convert ? PolarsArrowCompat.Convert(b, schema) : b;
        }
    }

    // ---------------------------------------------------------------- write

    /// <summary>Exports the frame batch by batch and writes each to the Avro writer.</summary>
    private static async Task WriteAvroCoreAsync(DataFrameHandle df, Stream stream, PlAvroCompression compression, string? name,
        bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanWrite) throw new ArgumentException("The destination stream is not writable.", nameof(stream));
        using var exported = ArrowStreamInterop.ExportToStream(df);
        var writer = AvroWriter.Create(stream, exported.Schema, AvroWriteOptionsFor(compression, name));
        try
        {
            while (await exported.ReadNextRecordBatchAsync(ct).ConfigureAwait(false) is { } batch)
            {
                using (batch)
                {
                    if (useAsync) await writer.WriteAsync(batch, ct).ConfigureAwait(false);
                    else writer.Write(batch);
                }
            }
            if (useAsync) await writer.CompleteAsync(ct).ConfigureAwait(false);
            else writer.Complete();
        }
        finally
        {
            if (useAsync) await writer.DisposeAsync().ConfigureAwait(false);
            else writer.Dispose();
        }
    }

    public static void WriteAvro(DataFrameHandle df, Stream stream, PlAvroCompression compression, string? name) =>
        WriteAvroCoreAsync(df, stream, compression, name, false, default).GetAwaiter().GetResult();

    public static Task WriteAvroAsync(DataFrameHandle df, Stream stream, PlAvroCompression compression, string? name,
        CancellationToken cancellationToken = default) =>
        WriteAvroCoreAsync(df, stream, compression, name, true, cancellationToken);

    /// <summary>Writes a file; a failure deletes the partial file (Avro has no footer to tell it is incomplete).</summary>
    public static void WriteAvro(DataFrameHandle df, string path, PlAvroCompression compression, string? name)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            WriteAvro(df, stream, compression, name);
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    public static byte[] WriteAvroToMemory(DataFrameHandle df, PlAvroCompression compression, string? name)
    {
        var ms = new MemoryStream();
        WriteAvro(df, ms, compression, name);
        return ms.ToArray();
    }

    /// <summary>Writes into native memory and hands it over without copying.</summary>
    public static unsafe NativeBuffer WriteAvroNativeMemory(DataFrameHandle df, PlAvroCompression compression, string? name)
    {
        var buffer = new NativeByteBuffer(64 * 1024);
        try
        {
            var sink = new NativeByteBufferStream(buffer);
            WriteAvro(df, sink, compression, name);
            int length = (int)sink.Length;
            return NativeBuffer.TakeNativeMemory(buffer.Detach(), length);
        }
        finally
        {
            buffer.Release();
        }
    }

    /// <summary>The Arrow schema the engine exports for a frame.</summary>
    public static Schema ArrowSchemaOf(DataFrameHandle df)
    {
        using var exported = ArrowStreamInterop.ExportToStream(df);
        return exported.Schema;
    }

    /// <summary>
    /// Receives the batches of a streaming sink and writes them as Avro. The writer is created with the first
    /// batch's schema (exactly what the engine exports); an empty result writes a file with the planned schema.
    /// </summary>
    public sealed class AvroBatchSink(Stream stream, PlAvroCompression compression, string? name) : IDisposable
    {
        private AvroWriter? _writer;

        public void Write(RecordBatch batch)
        {
            if (!stream.CanWrite) throw new ArgumentException("The destination stream is not writable.", nameof(stream));
            _writer ??= AvroWriter.Create(stream, batch.Schema, AvroWriteOptionsFor(compression, name));
            _writer.Write(batch);
        }

        /// <summary>Finishes the file. When no batch arrived, <paramref name="query"/> is run with no rows so that the
        /// file header gets the schema the engine exports.</summary>
        public void Complete(LazyFrameHandle query)
        {
            if (_writer is null)
            {
                using var empty = LazyCollect(LazySlice(LazyClone(query), 0, 0), PlEngine.Auto);
                _writer = AvroWriter.Create(stream, ArrowSchemaOf(empty), AvroWriteOptionsFor(compression, name));
            }
            _writer.Complete();
        }

        public void Dispose() => _writer?.Dispose();
    }
}
