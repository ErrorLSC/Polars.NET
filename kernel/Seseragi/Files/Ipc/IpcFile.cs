using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// An open Arrow IPC file (the file format: <c>.arrow</c>, Feather v2): its footer is read and checked on open. Batches
/// are read at offsets, so a file can be shared by several readers. Little-endian data only (as every current writer
/// writes); metadata V4 and V5; LZ4 frame and Zstandard compressed bodies.
/// </summary>
public sealed class IpcFile : IDisposable, IAsyncDisposable
{
    private const int TailLength = 10; // footer length + magic
    private const int HeadLength = 8;  // magic + padding

    private readonly IDisposable? _owned;

    private IpcFile(IRandomAccess source, IDisposable? owned, IpcFooter footer, long footerStart)
    {
        Source = source;
        _owned = owned;
        Footer = footer;
        FooterStart = footerStart;
    }

    internal IRandomAccess Source { get; }

    internal IpcFooter Footer { get; }

    /// <summary>Where the footer starts: messages lie before it.</summary>
    internal long FooterStart { get; }

    /// <summary>The schema (with the schema's custom metadata in <see cref="Schema.Metadata"/>).</summary>
    public Schema Schema => Footer.Schema.Schema;

    /// <summary>The custom metadata of the footer (the writer's file-level metadata); empty when it has none.</summary>
    public IReadOnlyDictionary<string, string> FileMetadata => ToDictionary(Footer.Metadata);

    /// <summary>The number of record batches.</summary>
    public int BatchCount => Footer.RecordBatches.Length;

    /// <summary>Opens a reader of the batches.</summary>
    /// <exception cref="ArgumentException">A column named in the options is not in the file.</exception>
    public IpcReader OpenReader(IpcReadOptions? options = null) => new(this, options ?? new IpcReadOptions());

    /// <summary>
    /// Opens a reader of the file's rows as <typeparamref name="T"/>, of the columns its members map, filtered by
    /// <paramref name="where"/> (see <see cref="IpcReader{T}"/>).
    /// </summary>
    /// <exception cref="ArgumentException">The file has no column for a member, or the options are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public IpcReader<T> OpenReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where = null,
        IpcReadOptions? options = null) => new(this, where, options);

    /// <summary>Reads the rows of the file at <paramref name="path"/> as <typeparamref name="T"/> (see <see cref="IpcReader{T}"/>).</summary>
    /// <inheritdoc cref="OpenReader{T}(Expression{Func{T, bool}}?, IpcReadOptions?)"/>
    public static List<T> ReadList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, Expression<Func<T, bool>>? where = null,
        IpcReadOptions? options = null)
    {
        using IpcFile file = Open(path);
        using IpcReader<T> reader = file.OpenReader(where, options);
        return reader.ReadList();
    }

    /// <inheritdoc cref="ReadList{T}(string, Expression{Func{T, bool}}?, IpcReadOptions?)"/>
    public static async Task<List<T>> ReadListAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path,
        Expression<Func<T, bool>>? where = null, IpcReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        await using IpcFile file = await OpenAsync(path, cancellationToken).ConfigureAwait(false);
        using IpcReader<T> reader = file.OpenReader(where, options);
        return await reader.ReadListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads batch <paramref name="index"/> (with its dictionaries).</summary>
    /// <exception cref="IpcFormatException">The batch or a dictionary is damaged or invalid.</exception>
    public RecordBatch ReadBatch(int index, IpcReadOptions? options = null)
    {
        using IpcReader reader = OpenReader(options);
        return reader.ReadBatch(index);
    }

    /// <inheritdoc cref="ReadBatch"/>
    public async ValueTask<RecordBatch> ReadBatchAsync(int index, IpcReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        using IpcReader reader = OpenReader(options);
        return await reader.ReadBatchAsync(index, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The custom metadata of batch <paramref name="index"/>'s message (empty when it has none).</summary>
    /// <exception cref="IpcFormatException">The message is damaged.</exception>
    public IReadOnlyDictionary<string, string> GetBatchMetadata(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, BatchCount);
        return ToDictionary(IpcIO.Sync(IpcIO.MessageAsync(Source, Footer.RecordBatches[index], false, default)).Metadata);
    }

    private static IReadOnlyDictionary<string, string> ToDictionary(IReadOnlyList<KeyValuePair<string, string>>? pairs)
    {
        Dictionary<string, string> map = [];
        if (pairs is not null)
        {
            foreach (var (key, value) in pairs) map[key] = value; // the last of a repeated key wins
        }
        return map;
    }

    /// <summary>Opens a file.</summary>
    /// <exception cref="IpcFormatException">The file is not an Arrow IPC file, or its footer is damaged.</exception>
    public static IpcFile Open(string path) => IpcIO.Sync(OpenCoreAsync(path, useAsync: false, default));

    /// <summary>
    /// Opens a stream from its current position, read at offsets (a stream that cannot seek is first copied into native
    /// memory, up to 2 GB). Unless <paramref name="leaveOpen"/>, the stream is disposed with the file, or when opening
    /// fails. A stream is never read through a memory mapping.
    /// </summary>
    /// <exception cref="IpcFormatException">The data is not an Arrow IPC file, or its footer is damaged.</exception>
    public static IpcFile Open(Stream stream, bool leaveOpen = false) => IpcIO.Sync(OpenCoreAsync(stream, leaveOpen, useAsync: false, default, mappable: false));

    /// <summary>Opens a file held in memory (batches are copied out of it: they never point into it).</summary>
    /// <exception cref="IpcFormatException">The data is not an Arrow IPC file, or its footer is damaged.</exception>
    public static IpcFile Open(ReadOnlyMemory<byte> bytes) => IpcIO.Sync(OpenCoreAsync(new MemoryRandomAccess(bytes), null, useAsync: false, default));

    /// <summary>Opens data read at offsets (say, an object in a store); the caller keeps ownership of the source.</summary>
    /// <exception cref="IpcFormatException">The data is not an Arrow IPC file, or its footer is damaged.</exception>
    public static IpcFile Open(IRandomAccess source) => IpcIO.Sync(OpenCoreAsync(source, null, useAsync: false, default));

    /// <inheritdoc cref="Open(string)"/>
    public static ValueTask<IpcFile> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(path, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(Stream, bool)"/>
    public static ValueTask<IpcFile> OpenAsync(Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(stream, leaveOpen, useAsync: true, cancellationToken, mappable: false);

    /// <inheritdoc cref="Open(IRandomAccess)"/>
    public static ValueTask<IpcFile> OpenAsync(IRandomAccess source, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(source, null, useAsync: true, cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _owned?.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private static ValueTask<IpcFile> OpenCoreAsync(string path, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(path);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1,
            useAsync ? FileOptions.RandomAccess | FileOptions.Asynchronous : FileOptions.RandomAccess);
        return OpenCoreAsync(stream, leaveOpen: false, useAsync, ct, mappable: true);
    }

    private static async ValueTask<IpcFile> OpenCoreAsync(Stream stream, bool leaveOpen, bool useAsync, CancellationToken ct, bool mappable)
    {
        ArgumentNullException.ThrowIfNull(stream);
        RandomAccessSource source;
        try
        {
            if (stream.CanSeek) source = new RandomAccessSource(stream, leaveOpen, null, mappable);
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
            return await OpenCoreAsync(source, source, useAsync, ct).ConfigureAwait(false);
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private static async ValueTask<IpcFile> OpenCoreAsync(IRandomAccess source, IDisposable? owned, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        long length = source.Length;
        nint tail = IpcIO.Alloc(TailLength);
        int footerLength;
        try
        {
            if (length < HeadLength + TailLength) throw await NotAFileAsync(source, length, useAsync, ct).ConfigureAwait(false);
            await IpcIO.ReadAsync(source, length - TailLength, tail, TailLength, useAsync, ct).ConfigureAwait(false);
            if (!IpcIO.Bytes(tail, TailLength)[4..].SequenceEqual(IpcFraming.FileMagic)) throw await NotAFileAsync(source, length, useAsync, ct).ConfigureAwait(false);
            footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(IpcIO.Bytes(tail, 4));
        }
        finally
        {
            IpcIO.Free(tail);
        }
        long footerStart = length - TailLength - footerLength;
        if (footerLength <= 0 || footerStart < HeadLength)
            throw new IpcFormatException($"The Arrow IPC footer length ({footerLength} bytes) does not fit the {length}-byte file.");

        nint footer = IpcIO.Alloc(footerLength);
        IpcFooter parsed;
        try
        {
            await IpcIO.ReadAsync(source, footerStart, footer, footerLength, useAsync, ct).ConfigureAwait(false);
            parsed = IpcMetadata.ParseFooter(IpcIO.Bytes(footer, footerLength));
        }
        catch (InvalidDataException e)
        {
            throw IpcIO.Format(e);
        }
        finally
        {
            IpcIO.Free(footer);
        }
        foreach (IpcBlock block in (IpcBlock[])[.. parsed.Dictionaries, .. parsed.RecordBatches])
        {
            if (block.Offset < HeadLength || block.MetadataLength < 8 || block.BodyLength < 0
                || block.Offset > footerStart - block.MetadataLength || block.BodyLength > footerStart - block.Offset - block.MetadataLength)
                throw new IpcFormatException($"An Arrow IPC footer block ({block.Offset}, {block.MetadataLength}, {block.BodyLength}) lies outside the data.");
        }
        if (source is ILocalSource { SmallReadsAreCheap: true })
        {
            // The start is checked where reading it is cheap (memory, a local file); on a store it would cost a request.
            nint head = IpcIO.Alloc(6);
            try
            {
                await IpcIO.ReadAsync(source, 0, head, 6, useAsync, ct).ConfigureAwait(false);
                if (!IpcIO.Bytes(head, 6).SequenceEqual(IpcFraming.FileMagic))
                    throw new IpcFormatException("The data does not start with ARROW1: it is not a single Arrow IPC file.");
            }
            finally
            {
                IpcIO.Free(head);
            }
        }
        return new IpcFile(source, owned, parsed, footerStart);
    }

    /// <summary>The error for data that does not end like an IPC file: says what it is when it can tell.</summary>
    private static async ValueTask<IpcFormatException> NotAFileAsync(IRandomAccess source, long length, bool useAsync, CancellationToken ct)
    {
        if (length >= 4)
        {
            nint head = IpcIO.Alloc(4);
            try
            {
                await IpcIO.ReadAsync(source, 0, head, 4, useAsync, ct).ConfigureAwait(false);
                ReadOnlySpan<byte> start = IpcIO.Bytes(head, 4);
                if (start.SequenceEqual("FEA1"u8))
                    return new IpcFormatException("The data is a Feather v1 file, which is not supported: write it again as Feather v2 (an Arrow IPC file).");
                if (IpcFraming.HasContinuation(start))
                    return new IpcFormatException("The data is an Arrow IPC stream, not a file: read it with IpcStreamReader.");
            }
            finally
            {
                IpcIO.Free(head);
            }
        }
        return new IpcFormatException($"The data ({length} bytes) is not an Arrow IPC file (no ARROW1 at the end).");
    }
}
