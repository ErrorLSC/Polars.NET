#pragma warning disable CS1573
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    /// <summary>
    /// Write DataFrame to an IPC (Arrow/Feather) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="compression">Compression method (None, LZ4, ZSTD). Defaults to None.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage.</param>
    public void WriteIpc(
        string path,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = Lazy();

        lf.SinkIpc(
            path,
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcPartitioned"/>
    public void WriteIpcPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = Lazy();
        lf.SinkIpcPartitioned(
            path, partitionBy, includeKeys, keysPreGrouped, maxRowsPerFile, approxBytesPerFile,
            compression, compatLevel, recordBatchSize, recordBatchStatistics, maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcMemory(IpcCompression, int, int, bool, bool)"/>
    public byte[] WriteIpcMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkIpcMemory(
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpc(Stream, IpcCompression, int, int, bool, bool)"/>
    public void WriteIpc(
        Stream stream,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        var lf = Lazy();
        lf.SinkIpc(stream, compression, compatLevel, recordBatchSize, recordBatchStatistics, maintainOrder);
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcAsync(Stream, IpcCompression, int, int, bool, bool, CancellationToken)"/>
    public Task WriteIpcAsync(
        Stream stream,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        CancellationToken cancellationToken = default)
    {
        var lf = Lazy();
        return lf.SinkIpcAsync(stream, compression, compatLevel, recordBatchSize, recordBatchStatistics, maintainOrder, cancellationToken);
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcAsync(PipeWriter, IpcCompression, int, int, bool, bool, CancellationToken)"/>
    public Task WriteIpcAsync(
        PipeWriter writer,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        CancellationToken cancellationToken = default)
    {
        var lf = Lazy();
        return lf.SinkIpcAsync(writer, compression, compatLevel, recordBatchSize, recordBatchStatistics, maintainOrder, cancellationToken);
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcNativeMemory(IpcCompression, int, int, bool, bool)"/>
    public NativeBuffer WriteIpcNativeMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkIpcNativeMemory(
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder
        );
    }
    /// <summary>
    /// Write to Arrow IPC record batch stream.
    /// </summary>
    /// <param name="path">Path to the IPC stream file or a byte buffer</param>
    /// <param name="compression">Compression method. Defaults to None.</param>
    /// <param name="compatLevel">Use a specific compatibility level 
    /// when exporting Polars’ internal data structures.</param>
    public void WriteIpcStream(
        string path,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1
    )
        => PolarsWrapper.WriteIpcStream(Handle, path, compression.ToNative(), compatLevel);
    /// <inheritdoc cref="DataFrame.WriteIpcStream(string, IpcCompression, int)"/>
    public byte[] WriteIpcStreamMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1
    )
        => PolarsWrapper.WriteIpcStreamMemory(Handle, compression.ToNative(), compatLevel);
    /// <summary>
    /// Write to Arrow IPC record batch stream in native memory, without copying into the managed heap.
    /// </summary>
    /// <param name="compression">Compression method. Defaults to None.</param>
    /// <param name="compatLevel">Use a specific compatibility level
    /// when exporting Polars’ internal data structures.</param>
    /// <returns>
    /// A <see cref="NativeBuffer"/> owning the serialized bytes. Dispose it to release the native memory.
    /// </returns>
    public NativeBuffer WriteIpcStreamNativeMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1
    )
        => PolarsWrapper.WriteIpcStreamNativeMemory(Handle, compression.ToNative(), compatLevel);

    /// <summary>
    /// Write to Arrow IPC record batch stream format into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="compression">Compression method. Defaults to None.</param>
    /// <param name="compatLevel">Use a specific compatibility level
    /// when exporting Polars’ internal data structures.</param>
    public void WriteIpcStream(
        Stream stream,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1)
    {
        ArgumentNullException.ThrowIfNull(stream);
        NativeStreamWriter.Write(stream, target =>
            PolarsWrapper.WriteIpcStreamCallback(Handle, target, compression.ToNative(), compatLevel));
    }
    /// <summary>
    /// Asynchronously write to Arrow IPC record batch stream format into a <see cref="Stream"/>.
    /// <para>
    /// Serialization runs on a background thread; chunks are written to the stream with async I/O,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="WriteIpcStream(Stream, IpcCompression, int)"/>
    /// <param name="cancellationToken">Cancels writing; serialization is aborted at its next write.</param>
    public Task WriteIpcStreamAsync(
        Stream stream,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return NativeStreamWriter.WriteAsync(stream, target =>
            PolarsWrapper.WriteIpcStreamCallback(Handle, target, compression.ToNative(), compatLevel),
            cancellationToken);
    }
    /// <summary>
    /// Asynchronously write to Arrow IPC record batch stream format into a <see cref="PipeWriter"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>The writer is flushed but not completed.</para>
    /// </summary>
    /// <param name="writer">Destination pipe writer.</param>
    /// <inheritdoc cref="WriteIpcStreamAsync(Stream, IpcCompression, int, CancellationToken)"/>
    public Task WriteIpcStreamAsync(
        PipeWriter writer,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return NativeStreamWriter.WriteAsync(writer, target =>
            PolarsWrapper.WriteIpcStreamCallback(Handle, target, compression.ToNative(), compatLevel),
            cancellationToken);
    }
}
