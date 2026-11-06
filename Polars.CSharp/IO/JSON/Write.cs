#pragma warning disable CS1573
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    /// <summary>
    /// Write DataFrame to a JSON file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="format">JSON format (Json Array or JsonLines). Defaults to Json.</param>
    public void WriteJson(string path, JsonFormat format = JsonFormat.Json)
    {
        PolarsWrapper.WriteJson(Handle, path, format.ToNative());
    }
    /// <summary>
    /// Writes the DataFrame to a JSON format in memory.
    /// </summary>
    /// <param name="jsonFormat">The JSON format to use (Json or JsonLines).</param>
    /// <returns>A byte array containing the JSON data.</returns>
    public byte[] WriteJsonMemory(JsonFormat jsonFormat = JsonFormat.Json)
    {
        return PolarsWrapper.WriteJsonMemory(
            Handle,
            jsonFormat.ToNative()
        );
    }

    /// <summary>
    /// Writes the DataFrame to a JSON format in native memory, without copying into the managed heap.
    /// </summary>
    /// <param name="jsonFormat">The JSON format to use (Json or JsonLines).</param>
    /// <returns>
    /// A <see cref="NativeBuffer"/> owning the JSON data. Dispose it to release the native memory.
    /// </returns>
    public NativeBuffer WriteJsonNativeMemory(JsonFormat jsonFormat = JsonFormat.Json)
    {
        return PolarsWrapper.WriteJsonNativeMemory(
            Handle,
            jsonFormat.ToNative()
        );
    }
    /// <summary>
    /// Write the DataFrame to a JSON format into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="format">JSON format (Json Array or JsonLines). Defaults to Json.</param>
    public void WriteJson(Stream stream, JsonFormat format = JsonFormat.Json)
    {
        ArgumentNullException.ThrowIfNull(stream);
        NativeStreamWriter.Write(stream, target =>
            PolarsWrapper.WriteJsonCallback(Handle, target, format.ToNative()));
    }

    /// <summary>
    /// Asynchronously write the DataFrame to a JSON format into a <see cref="Stream"/>.
    /// <para>
    /// Serialization runs on a background thread; chunks are written to the stream with async I/O,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="WriteJson(Stream, JsonFormat)"/>
    /// <param name="cancellationToken">Cancels writing; serialization is aborted at its next write.</param>
    public Task WriteJsonAsync(
        Stream stream,
        JsonFormat format = JsonFormat.Json,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return NativeStreamWriter.WriteAsync(stream, target =>
            PolarsWrapper.WriteJsonCallback(Handle, target, format.ToNative()),
            cancellationToken);
    }

    /// <summary>
    /// Asynchronously write the DataFrame to a JSON format into a <see cref="PipeWriter"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>The writer is flushed but not completed.</para>
    /// </summary>
    /// <param name="writer">Destination pipe writer.</param>
    /// <inheritdoc cref="WriteJsonAsync(Stream, JsonFormat, CancellationToken)"/>
    public Task WriteJsonAsync(
        PipeWriter writer,
        JsonFormat format = JsonFormat.Json,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return NativeStreamWriter.WriteAsync(writer, target =>
            PolarsWrapper.WriteJsonCallback(Handle, target, format.ToNative()),
            cancellationToken);
    }

    /// <inheritdoc cref="LazyFrame.SinkJsonPartitioned"/>
    public void WriteJsonPartitioned(
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
        var lf = Lazy();
        lf.SinkJsonPartitioned(
            path, partitionBy, includeKeys, keysPreGrouped, maxRowsPerFile, approxBytesPerFile,
            compression, compressionLevel, checkExtension, maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }

    /// <summary>
    /// Write DataFrame to a Newline Delimited JSON (NDJSON) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming, compression, and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd). Defaults to None.</param>
    /// <param name="compressionLevel">Compression level. -1 for default.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    public void WriteNdJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = Lazy();

        lf.SinkJson(
            path,
            compression,
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
}