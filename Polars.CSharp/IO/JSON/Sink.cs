#pragma warning disable CS1573
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd).</param>
    /// <param name="compressionLevel">Compression level.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    public void SinkJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkJson(
            CloneHandle(),
            path,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );
    }
    /// <inheritdoc cref="SinkJson(string, ExternalCompression, int, bool, bool, SyncOnClose, bool, CloudOptions?)"/>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    public void SinkJsonPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkJsonPartitioned(
            CloneHandle(),
            path,
            // --- Partition Params ---
            partitionBy.Consume().CloneHandle(), 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Alias for SinkJson with format=JsonLines.
    /// </summary>
    public void SinkNdJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        SinkJson(path,compression,compressionLevel,checkExtension, maintainOrder, syncOnClose, mkdir,cloudOptions);
    }
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file format written into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced; the whole file is never materialized in memory.
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="compression">Compression method (Gzip/Zstd).</param>
    /// <param name="compressionLevel">Compression level.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    public void SinkJson(
        Stream stream,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var handle = CloneHandle();
        NativeStreamWriter.Write(stream, target => PolarsWrapper.SinkJsonCallback(
            handle, target, compression.ToNative(), compressionLevel, checkExtension, maintainOrder));
    }

    /// <summary>
    /// Asynchronously sink the LazyFrame to a NDJSON (Newline Delimited JSON) file format written into a <see cref="Stream"/>.
    /// <para>
    /// The query runs on a background thread; chunks are written to the stream with async I/O,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="SinkJson(Stream, ExternalCompression, int, bool, bool)"/>
    /// <param name="cancellationToken">Cancels writing; the native query is aborted at its next write.</param>
    public Task SinkJsonAsync(
        Stream stream,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var handle = CloneHandle();
        return NativeStreamWriter.WriteAsync(stream, target => PolarsWrapper.SinkJsonCallback(
            handle, target, compression.ToNative(), compressionLevel, checkExtension, maintainOrder),
            cancellationToken);
    }

    /// <summary>
    /// Asynchronously sink the LazyFrame to a NDJSON (Newline Delimited JSON) file format written into a <see cref="PipeWriter"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>The writer is flushed but not completed.</para>
    /// </summary>
    /// <param name="writer">Destination pipe writer.</param>
    /// <inheritdoc cref="SinkJsonAsync(Stream, ExternalCompression, int, bool, bool, CancellationToken)"/>
    public Task SinkJsonAsync(
        PipeWriter writer,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        var handle = CloneHandle();
        return NativeStreamWriter.WriteAsync(writer, target => PolarsWrapper.SinkJsonCallback(
            handle, target, compression.ToNative(), compressionLevel, checkExtension, maintainOrder),
            cancellationToken);
    }

    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in memory.
    /// </summary>
    public byte[] SinkJsonMemory(
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkJsonMemory(
            CloneHandle(),
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder
        );
    }

    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in native memory, without copying into the managed heap.
    /// </summary>
    /// <returns>
    /// A <see cref="NativeBuffer"/> owning the serialized NDJSON data. Dispose it to release the native memory.
    /// </returns>
    public NativeBuffer SinkJsonNativeMemory(
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkJsonNativeMemory(
            CloneHandle(),
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder
        );
    }
}