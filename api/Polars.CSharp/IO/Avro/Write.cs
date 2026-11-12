#pragma warning disable CS1573 
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
    /// <param name="name">The name of the Avro record (empty: <c>topLevelRecord</c>).</param>
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
    /// Output is written block by block as the frame is exported. The stream is flushed but not disposed.
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
        PolarsWrapper.WriteAvro(Handle, stream, compression.ToNative(), name); // completing the file flushes the stream
    }

    /// <summary>
    /// Asynchronously write the DataFrame to an Apache Avro format into a <see cref="Stream"/>.
    /// <para>
    /// Blocks are written to the stream with async I/O as they fill,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="WriteAvro(Stream, AvroCompression, string)"/>
    /// <param name="cancellationToken">Cancels writing at the next block.</param>
    public Task WriteAvroAsync(
        Stream stream,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "",
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        // Completing the file flushes the stream.
        return PolarsWrapper.WriteAvroAsync(Handle, stream, compression.ToNative(), name, cancellationToken);
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
        // The pipe's stream view writes into the pipe; completing the file flushes the pipe (it is not completed).
        return PolarsWrapper.WriteAvroAsync(Handle, writer.AsStream(leaveOpen: true), compression.ToNative(), name, cancellationToken);
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
public partial class LazyFrame
{
    /// <summary>
    /// Run the query on the streaming engine and write the result to an Avro file as batches arrive.
    /// </summary>
    /// <param name="path">The file path to write to (a failure deletes the partial file).</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record (empty: <c>topLevelRecord</c>).</param>
    public void SinkAvro(
        string path,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "")
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            SinkAvro(stream, compression, name);
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    /// <summary>
    /// Run the query on the streaming engine and write the result as Avro into a <see cref="Stream"/> as batches
    /// arrive. The stream is flushed but not disposed.
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record (empty: <c>topLevelRecord</c>).</param>
    public void SinkAvro(
        Stream stream,
        AvroCompression compression = AvroCompression.Uncompressed,
        string name = "")
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var sink = new PolarsWrapper.AvroBatchSink(stream, compression.ToNative(), name);
        SinkBatches(sink.Write);
        sink.Complete(Handle); // completing the file flushes the stream
    }
}
