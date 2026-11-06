using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{
    /// <summary>
    /// Write the DataFrame to an Apache Avro file.
    /// </summary>
    /// <param name="path">The file path to write to.</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    public void WriteAvro(
        string path,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "")
    {
        PolarsWrapper.WriteAvro(this.Handle, path, compression.ToNative(), name);
    }
    /// <summary>
    /// Write the DataFrame to an Apache Avro format into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    public void WriteAvro(
        Stream stream,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "")
    {
        ArgumentNullException.ThrowIfNull(stream);
        NativeStreamWriter.Write(stream, target =>
            PolarsWrapper.WriteAvroCallback(Handle, target, compression.ToNative(), name));
    }

    /// <summary>
    /// Asynchronously write the DataFrame to an Apache Avro format into a <see cref="Stream"/>.
    /// <para>
    /// Serialization runs on a background thread; chunks are written to the stream with async I/O,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="WriteAvro(Stream, AvroCompression, string)"/>
    /// <param name="cancellationToken">Cancels writing; serialization is aborted at its next write.</param>
    public Task WriteAvroAsync(
        Stream stream,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "",
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return NativeStreamWriter.WriteAsync(stream, target =>
            PolarsWrapper.WriteAvroCallback(Handle, target, compression.ToNative(), name),
            cancellationToken);
    }

    /// <summary>
    /// Asynchronously write the DataFrame to an Apache Avro format into a <see cref="PipeWriter"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>The writer is flushed but not completed.</para>
    /// </summary>
    /// <param name="writer">Destination pipe writer.</param>
    /// <inheritdoc cref="WriteAvroAsync(Stream, AvroCompression, string, CancellationToken)"/>
    public Task WriteAvroAsync(
        PipeWriter writer,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "",
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return NativeStreamWriter.WriteAsync(writer, target =>
            PolarsWrapper.WriteAvroCallback(Handle, target, compression.ToNative(), name),
            cancellationToken);
    }

    /// <summary>
    /// Write the DataFrame to an Apache Avro memory buffer.
    /// </summary>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    /// <returns>A byte array containing the Avro data.</returns>
    public byte[] WriteAvroMemory(
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "")
    {
        return PolarsWrapper.WriteAvroToMemory(Handle, compression.ToNative(), name);
    }
    /// <summary>
    /// Write the DataFrame to an Apache Avro memory buffer in native memory, without copying into the managed heap.
    /// </summary>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    /// <returns>
    /// A <see cref="NativeBuffer"/> owning the Avro data. Dispose it to release the native memory.
    /// </returns>
    public NativeBuffer WriteAvroNativeMemory(
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "")
    {
        return PolarsWrapper.WriteAvroNativeMemory(Handle, compression.ToNative(), name);
    }
}