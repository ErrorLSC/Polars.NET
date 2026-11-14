using System.Buffers;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// An open Parquet file: its footer is read and checked on open. The data is read at offsets, so a file can be
/// shared by several readers.
/// </summary>
public sealed class ParquetFile : IDisposable, IAsyncDisposable
{
    private const int MagicLength = 4;
    private const int TailLength = 8; // footer length + magic

    private readonly IDisposable? _owned;

    private ParquetFile(IRandomAccess source, IDisposable? owned, FileMetaData thrift, ParquetMetadata metadata)
    {
        Source = source;
        _owned = owned;
        Thrift = thrift;
        Metadata = metadata;
    }

    /// <summary>The footer: schema, row groups and column chunk statistics.</summary>
    public ParquetMetadata Metadata { get; }

    /// <summary>The full footer.</summary>
    internal FileMetaData Thrift { get; }

    private ParquetSchemaTree? _tree;
    private Apache.Arrow.Schema? _schema;

    /// <summary>The schema tree.</summary>
    /// <exception cref="ParquetFormatException">A leaf has an unknown physical type, or the tree nests too deeply.</exception>
    internal ParquetSchemaTree Tree => _tree ??= ParquetSchemaTree.Build(Thrift.Schema);

    /// <summary>
    /// The Arrow schema of the file, read with default options: Parquet types and annotations, refined by the Arrow
    /// schema the writer stored (if any), and each column's Parquet field id as <c>PARQUET:field_id</c> metadata.
    /// </summary>
    /// <exception cref="ParquetFormatException">The schema has a leaf of an unknown physical type.</exception>
    public Apache.Arrow.Schema Schema => _schema ??= ArrowMapping.Map(Tree, Thrift, new ParquetReadOptions()).Schema;

    /// <summary>Opens a reader of the file's batches.</summary>
    /// <exception cref="NotSupportedException">A selected column is nested (lists, maps, structs), which this version does not read yet.</exception>
    /// <exception cref="ArgumentException">The options name a column or row group the file does not have, or are out of range.</exception>
    public ParquetReader OpenReader(ParquetReadOptions? options = null) => new(this, options ?? new ParquetReadOptions());

    /// <summary>The bytes of the file.</summary>
    internal IRandomAccess Source { get; }

    /// <summary>Opens a file.</summary>
    /// <exception cref="ParquetFormatException">The file is not a Parquet file, is encrypted, or its footer is damaged.</exception>
    public static ParquetFile Open(string path) => Sync(OpenCoreAsync(path, useAsync: false, default));

    /// <summary>Opens a file, keeping even a small one on the file instead of in memory (tests read small files as large ones).</summary>
    internal static ParquetFile OpenOnSource(string path) => Sync(OpenCoreAsync(path, useAsync: false, default, smallInMemory: false));

    /// <summary>
    /// Opens a stream from its current position. A stream that cannot seek is first copied into native memory (up
    /// to 2 GB). Unless <paramref name="leaveOpen"/>, the stream is disposed with the file, or when opening fails.
    /// </summary>
    /// <exception cref="ParquetFormatException">The data is not a Parquet file, is encrypted, or its footer is damaged.</exception>
    public static ParquetFile Open(Stream stream, bool leaveOpen = false) => Sync(OpenCoreAsync(stream, leaveOpen, useAsync: false, default));

    /// <summary>Opens data read at offsets (say, an object in a store); the caller keeps ownership of the source.</summary>
    /// <exception cref="ParquetFormatException">The data is not a Parquet file, is encrypted, or its footer is damaged.</exception>
    public static ParquetFile Open(IRandomAccess source) => Sync(OpenCoreAsync(source, null, useAsync: false, default));

    /// <inheritdoc cref="Open(string)"/>
    public static ValueTask<ParquetFile> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(path, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(Stream, bool)"/>
    public static ValueTask<ParquetFile> OpenAsync(Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(stream, leaveOpen, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IRandomAccess)"/>
    public static ValueTask<ParquetFile> OpenAsync(IRandomAccess source, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(source, null, useAsync: true, cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _owned?.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ------------------------------------------------------------------
    // Opening: one sync and async path; the sync calls complete synchronously
    // ------------------------------------------------------------------

    private static T Sync<T>(ValueTask<T> task) => task.IsCompleted ? task.Result : task.AsTask().GetAwaiter().GetResult();

    private static ValueTask<ParquetFile> OpenCoreAsync(string path, bool useAsync, CancellationToken ct, bool smallInMemory = true)
    {
        ArgumentNullException.ThrowIfNull(path);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1,
            useAsync ? FileOptions.RandomAccess | FileOptions.Asynchronous : FileOptions.RandomAccess);
        return OpenCoreAsync(stream, leaveOpen: false, useAsync, ct, smallInMemory);
    }

    private static async ValueTask<ParquetFile> OpenCoreAsync(Stream stream, bool leaveOpen, bool useAsync, CancellationToken ct, bool smallInMemory = true)
    {
        ArgumentNullException.ThrowIfNull(stream);
        RandomAccessSource source;
        try
        {
            if (stream.CanSeek) source = new RandomAccessSource(stream, leaveOpen, null);
            else
            {
                var (memory, copy) = await NativeCopy.CopyAsync(stream, leaveOpen, useAsync, ct).ConfigureAwait(false);
                source = new RandomAccessSource(memory, false, copy);
            }
        }
        catch when (!leaveOpen)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        try
        {
            return await OpenCoreAsync(source, source, useAsync, ct, smallInMemory).ConfigureAwait(false);
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private static async ValueTask<ParquetFile> OpenCoreAsync(IRandomAccess source, IDisposable? owned, bool useAsync, CancellationToken ct, bool smallInMemory = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        long length = source.Length;
        if (length < MagicLength + TailLength) throw new ParquetFormatException($"The data ({length} bytes) is too short to be a Parquet file.");

        int tailLength = (int)Math.Min(length, ParquetMetadata.TailGuess);
        byte[] tail = ArrayPool<byte>.Shared.Rent(tailLength);
        byte[]? footer = null;
        try
        {
            await ReadExactlyAsync(source, length - tailLength, tail.AsMemory(0, tailLength), useAsync, ct).ConfigureAwait(false);
            if (tailLength == length && !tail.AsSpan(0, MagicLength).SequenceEqual("PAR1"u8) && !tail.AsSpan(0, MagicLength).SequenceEqual("PARE"u8))
                throw new ParquetFormatException("The data is not a Parquet file (no PAR1 at the start).");
            int footerLength = ParquetMetadata.FooterLength(tail.AsSpan(tailLength - TailLength, TailLength), "The data");
            if (footerLength > length - MagicLength - TailLength)
                throw new ParquetFormatException($"The Parquet footer ({footerLength} bytes) is longer than the file.");

            ReadOnlyMemory<byte> bytes;
            if (footerLength <= tailLength - TailLength) bytes = tail.AsMemory(tailLength - TailLength - footerLength, footerLength);
            else
            {
                footer = ArrayPool<byte>.Shared.Rent(footerLength);
                await ReadExactlyAsync(source, length - TailLength - footerLength, footer.AsMemory(0, footerLength), useAsync, ct).ConfigureAwait(false);
                bytes = footer.AsMemory(0, footerLength);
            }

            FileMetaData thrift = FileMetaData.Parse(bytes.Span);
            if (thrift.EncryptionAlgorithm is { } algorithm)
                throw new ParquetFormatException($"The Parquet file is encrypted ({algorithm.Name}, plaintext footer), which is not supported.");
            // A file the tail read held whole (up to 64 KiB) is read from those bytes from now on: on an object store
            // each read is a request.
            IRandomAccess data = smallInMemory && tailLength == length ? new MemoryRandomAccess(tail.AsSpan(0, tailLength).ToArray()) : source;
            return new ParquetFile(data, owned, thrift, ParquetMetadata.FromThrift(thrift));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tail);
            if (footer is not null) ArrayPool<byte>.Shared.Return(footer);
        }
    }

    /// <summary>Reads exactly <paramref name="destination"/>.Length bytes at <paramref name="offset"/>.</summary>
    internal static async ValueTask ReadExactlyAsync(IRandomAccess source, long offset, Memory<byte> destination, bool useAsync, CancellationToken ct)
    {
        int n = useAsync
            ? await source.ReadAtAsync(offset, destination, ct).ConfigureAwait(false)
            : source.ReadAt(offset, destination.Span);
        if (n != destination.Length)
            throw new ParquetFormatException($"The Parquet data ended early: {n} of {destination.Length} bytes at offset {offset}.");
    }
}
