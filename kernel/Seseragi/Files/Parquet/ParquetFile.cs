using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Encryption;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Kaminari.Seseragi.Files.Parquet.Thrift;

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

    private ParquetFile(IRandomAccess source, IDisposable? owned, FileMetaData thrift, ParquetMetadata metadata, FileDecryptor? decryptor)
    {
        Decryptor = decryptor;
        Source = source;
        // Where the data ends: at the footer (when its length is known; else at the closing length and magic).
        DataEnd = source.Length - TailLength - (metadata.FooterSize ?? 0);
        _owned = owned;
        Thrift = thrift;
        Metadata = metadata;
    }

    /// <summary>The footer: schema, row groups and column chunk statistics.</summary>
    public ParquetMetadata Metadata { get; }

    /// <summary>The full footer.</summary>
    internal FileMetaData Thrift { get; }

    /// <summary>The file's decryption (Parquet modular encryption); null when it is not encrypted.</summary>
    internal FileDecryptor? Decryptor { get; }

    private PageIndexes? _pageIndexes;

    /// <summary>The page indexes of the column chunks, read as readers ask for them and kept for every reader.</summary>
    internal PageIndexes PageIndexes => LazyInitializer.EnsureInitialized(ref _pageIndexes, () => new PageIndexes(this));

    private BloomFilters? _bloomFilters;

    /// <summary>The Bloom filters of the column chunks, read as readers ask for them and kept for every reader.</summary>
    internal BloomFilters BloomFilters => LazyInitializer.EnsureInitialized(ref _bloomFilters, () => new BloomFilters(this));

    private SchemaShape? _shape;
    private Apache.Arrow.Schema? _schema;

    /// <summary>The file's schema and what is derived from it (shared with the files of a dataset of the same schema).</summary>
    internal SchemaShape Shape => Thrift.Shape ?? LazyInitializer.EnsureInitialized(ref _shape, () => SchemaShape.Of(Thrift));

    /// <summary>The schema tree.</summary>
    /// <exception cref="ParquetFormatException">A leaf has an unknown physical type, or the tree nests too deeply.</exception>
    internal ParquetSchemaTree Tree => Shape.Tree;

    /// <summary>
    /// The Arrow schema of the file, read with default options: Parquet types and annotations, refined by the Arrow
    /// schema the writer stored (if any), and each column's Parquet field id as <c>PARQUET:field_id</c> metadata.
    /// </summary>
    /// <exception cref="ParquetFormatException">The schema has a leaf of an unknown physical type.</exception>
    public Apache.Arrow.Schema Schema => _schema ??= Mapping(new ParquetReadOptions()).Schema;

    /// <summary>
    /// The file's columns as a reader with <paramref name="options"/> maps them (the options that shape types: views,
    /// binaries as strings, the INT96 unit), mapped once per shape of options and kept (see <see cref="Shape"/>).
    /// </summary>
    internal (Apache.Arrow.Schema Schema, ColumnMapping[] Columns) Mapping(ParquetReadOptions options) => Shape.Mapping(options);

    /// <summary>Opens a reader of the file's batches.</summary>
    /// <exception cref="NotSupportedException">A selected column is nested (lists, maps, structs), which this version does not read yet.</exception>
    /// <exception cref="ArgumentException">The options name a column or row group the file does not have, or are out of range.</exception>
    public ParquetReader OpenReader(ParquetReadOptions? options = null) => new(this, options ?? new ParquetReadOptions());

    /// <summary>
    /// Opens a reader of the file's rows as <typeparamref name="T"/>, of the columns its members map, filtered by
    /// <paramref name="where"/> (pushed into the read; see <see cref="ParquetReader{T}"/>).
    /// </summary>
    /// <exception cref="ArgumentException">The file has no column for a member, or the options are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public ParquetReader<T> OpenReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where = null,
        ParquetReadOptions? options = null) => new(this, where, options);

    /// <summary>Reads the rows of the file at <paramref name="path"/> as <typeparamref name="T"/> (see <see cref="ParquetReader{T}"/>).</summary>
    /// <inheritdoc cref="OpenReader{T}(Expression{Func{T, bool}}?, ParquetReadOptions?)"/>
    public static List<T> ReadList<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, Expression<Func<T, bool>>? where = null,
        ParquetReadOptions? options = null, ParquetDecryptionOptions? decryption = null)
    {
        using ParquetFile file = Open(path, decryption);
        using ParquetReader<T> reader = file.OpenReader(where, options);
        return reader.ReadList();
    }

    /// <inheritdoc cref="ReadList{T}(string, Expression{Func{T, bool}}?, ParquetReadOptions?, ParquetDecryptionOptions?)"/>
    public static async Task<List<T>> ReadListAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path,
        Expression<Func<T, bool>>? where = null, ParquetReadOptions? options = null, ParquetDecryptionOptions? decryption = null,
        CancellationToken cancellationToken = default)
    {
        await using ParquetFile file = await OpenAsync(path, decryption, cancellationToken).ConfigureAwait(false);
        await using ParquetReader<T> reader = file.OpenReader(where, options);
        return await reader.ReadListAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The bytes of the file.</summary>
    internal IRandomAccess Source { get; }

    /// <summary>Where the bytes the footer describes end (the footer's start): column chunks lie before it.</summary>
    internal long DataEnd { get; }

    /// <summary>Opens a file.</summary>
    /// <param name="path">The file.</param>
    /// <param name="decryption">The keys of a file written with Parquet modular encryption (ignored for a file that is not encrypted).</param>
    /// <exception cref="ParquetFormatException">The file is not a Parquet file, is encrypted and the keys do not open it, or its footer is damaged.</exception>
    public static ParquetFile Open(string path, ParquetDecryptionOptions? decryption = null) => Sync(OpenCoreAsync(path, decryption, useAsync: false, default));

    /// <summary>Opens a file, keeping even a small one on the file instead of in memory (tests read small files as large ones).</summary>
    internal static ParquetFile OpenOnSource(string path, ParquetDecryptionOptions? decryption = null) =>
        Sync(OpenCoreAsync(path, decryption, useAsync: false, default, smallInMemory: false));

    /// <summary>
    /// Opens a stream from its current position. Bytes already in memory are read in place, without a lock: a
    /// <see cref="MemoryStream"/> (its buffer visible or not; a subclass only when it shows its buffer), an
    /// <see cref="UnmanagedMemoryStream"/> made over a pointer (its memory must outlive the file); the memory must
    /// stay unchanged while the file is used. Other seekable streams are read by seeking under a lock. A stream that
    /// cannot seek is first copied into native memory (up to 2 GB). Unless <paramref name="leaveOpen"/>, the stream
    /// is disposed with the file (a copied one at once), or when opening fails. A stream is never read through a
    /// memory mapping (<see cref="ParquetReadOptions.MemoryMap"/>), even a file stream.
    /// </summary>
    /// <inheritdoc cref="Open(string, ParquetDecryptionOptions?)" path="/param[@name='decryption']"/>
    /// <exception cref="ParquetFormatException">The data is not a Parquet file, is encrypted and the keys do not open it, or its footer is damaged.</exception>
    public static ParquetFile Open(Stream stream, bool leaveOpen = false, ParquetDecryptionOptions? decryption = null) =>
        Sync(OpenCoreAsync(stream, leaveOpen, decryption, useAsync: false, default));

    /// <summary>
    /// Opens a file held in memory, read in place without a copy (columns are decoded from slices of it; the batches
    /// never point into it). The memory must stay unchanged while the file and its readers are used.
    /// </summary>
    /// <inheritdoc cref="Open(string, ParquetDecryptionOptions?)" path="/param[@name='decryption']"/>
    /// <exception cref="ParquetFormatException">The data is not a Parquet file, is encrypted and the keys do not open it, or its footer is damaged.</exception>
    public static ParquetFile Open(ReadOnlyMemory<byte> bytes, ParquetDecryptionOptions? decryption = null) =>
        Sync(OpenCoreAsync(new MemoryRandomAccess(bytes), null, decryption, useAsync: false, default));

    /// <summary>
    /// Opens a file held in memory that <paramref name="owner"/> owns (native memory, say): the file disposes the owner
    /// when it is disposed (or when opening fails).
    /// </summary>
    internal static ParquetFile Open(ReadOnlyMemory<byte> bytes, IDisposable owner)
    {
        try
        {
            return Sync(OpenCoreAsync(new MemoryRandomAccess(bytes), owner, null, useAsync: false, default));
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>Opens data read at offsets (say, an object in a store); the caller keeps ownership of the source.</summary>
    /// <inheritdoc cref="Open(string, ParquetDecryptionOptions?)" path="/param[@name='decryption']"/>
    /// <exception cref="ParquetFormatException">The data is not a Parquet file, is encrypted and the keys do not open it, or its footer is damaged.</exception>
    public static ParquetFile Open(IRandomAccess source, ParquetDecryptionOptions? decryption = null) =>
        Sync(OpenCoreAsync(source, null, decryption, useAsync: false, default));

    /// <summary>
    /// Opens a file whose footer was read before: <paramref name="metadata"/> is the <see cref="Metadata"/> of an
    /// earlier <see cref="ParquetFile"/> of the same bytes (an engine or catalog keeps it across queries). Nothing is
    /// read on open. The caller vouches that the file has not changed; reads still check every range against it.
    /// </summary>
    /// For an encrypted file, <paramref name="decryption"/> gives the keys its pages are read with (the footer's
    /// were applied when it was read).
    /// <exception cref="ArgumentException"><paramref name="metadata"/> was not read from a file.</exception>
    public static ParquetFile Open(string path, ParquetMetadata metadata, ParquetDecryptionOptions? decryption = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        FileMetaData thrift = ReadFooter(metadata);
        FileDecryptor? decryptor = DecryptorOf(metadata, decryption);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.RandomAccess);
        var source = new RandomAccessSource(stream, false, null, mappable: true);
        return new ParquetFile(source, source, thrift, metadata, decryptor);
    }

    /// <inheritdoc cref="Open(string, ParquetMetadata, ParquetDecryptionOptions?)"/>
    /// <remarks>The caller keeps ownership of <paramref name="source"/>.</remarks>
    public static ParquetFile Open(IRandomAccess source, ParquetMetadata metadata, ParquetDecryptionOptions? decryption = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new ParquetFile(source, null, ReadFooter(metadata), metadata, DecryptorOf(metadata, decryption));
    }

    /// <summary>The decryption of a file whose footer was read before (null when it is not encrypted).</summary>
    private static FileDecryptor? DecryptorOf(ParquetMetadata metadata, ParquetDecryptionOptions? decryption) =>
        metadata.Encryption is { } algorithm ? FileDecryptor.Create(algorithm, decryption) : null;

    /// <summary>
    /// Opens a source the file then owns (disposed with it when it is <see cref="IDisposable"/>, or when opening
    /// fails): from <paramref name="metadata"/> without a read when the footer was read before. When the footer is read,
    /// <paramref name="footerRead"/> (if given) gets its bytes, once they are parsed without error; with
    /// <paramref name="interner"/> it shares what it holds alike with the footers read before.
    /// </summary>
    internal static async ValueTask<ParquetFile> OpenOwnedAsync(IRandomAccess source, ParquetMetadata? metadata, ParquetDecryptionOptions? decryption,
        bool useAsync, CancellationToken ct, FooterRead? footerRead = null, FooterInterner? interner = null)
    {
        var owned = source as IDisposable;
        try
        {
            return metadata is null
                ? await OpenCoreAsync(source, owned, decryption, useAsync, ct, footerRead: footerRead, interner: interner).ConfigureAwait(false)
                : new ParquetFile(source, owned, ReadFooter(metadata), metadata, DecryptorOf(metadata, decryption));
        }
        catch
        {
            owned?.Dispose();
            throw;
        }
    }

    private static FileMetaData ReadFooter(ParquetMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return metadata.Thrift ?? throw new ArgumentException("The metadata was built by hand, not read from a Parquet file.", nameof(metadata));
    }

    /// <inheritdoc cref="Open(string, ParquetDecryptionOptions?)"/>
    public static ValueTask<ParquetFile> OpenAsync(string path, ParquetDecryptionOptions? decryption = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(path, decryption, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(Stream, bool, ParquetDecryptionOptions?)"/>
    public static ValueTask<ParquetFile> OpenAsync(Stream stream, bool leaveOpen = false, ParquetDecryptionOptions? decryption = null,
        CancellationToken cancellationToken = default) =>
        OpenCoreAsync(stream, leaveOpen, decryption, useAsync: true, cancellationToken);

    /// <inheritdoc cref="Open(IRandomAccess, ParquetDecryptionOptions?)"/>
    public static ValueTask<ParquetFile> OpenAsync(IRandomAccess source, ParquetDecryptionOptions? decryption = null, CancellationToken cancellationToken = default) =>
        OpenCoreAsync(source, null, decryption, useAsync: true, cancellationToken);

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

    private static ValueTask<ParquetFile> OpenCoreAsync(string path, ParquetDecryptionOptions? decryption, bool useAsync, CancellationToken ct,
        bool smallInMemory = true)
    {
        ArgumentNullException.ThrowIfNull(path);
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1,
            useAsync ? FileOptions.RandomAccess | FileOptions.Asynchronous : FileOptions.RandomAccess);
        return OpenCoreAsync(stream, leaveOpen: false, decryption, useAsync, ct, smallInMemory, mappable: true);
    }

    private static async ValueTask<ParquetFile> OpenCoreAsync(Stream stream, bool leaveOpen, ParquetDecryptionOptions? decryption, bool useAsync,
        CancellationToken ct, bool smallInMemory = true, bool mappable = false)
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
            return await OpenCoreAsync(source, source, decryption, useAsync, ct, smallInMemory).ConfigureAwait(false);
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private static async ValueTask<ParquetFile> OpenCoreAsync(IRandomAccess source, IDisposable? owned, ParquetDecryptionOptions? decryption, bool useAsync,
        CancellationToken ct, bool smallInMemory = true, FooterRead? footerRead = null, FooterInterner? interner = null)
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
            ReadOnlySpan<byte> last = tail.AsSpan(tailLength - TailLength, TailLength);
            bool encryptedFooter = last[MagicLength..].SequenceEqual("PARE"u8);
            int footerLength = ParquetMetadata.FooterLength(last, "The data", allowEncrypted: true);
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

            FileMetaData thrift;
            FileDecryptor? decryptor = null;
            if (encryptedFooter)
            {
                // PARE: the FileCryptoMetaData, then the footer as an encrypted module.
                if (decryption is null) throw ParquetMetadata.Encrypted("The Parquet file");
                var reader = new ThriftCompactReader(bytes.Span);
                FileCryptoMetaData crypto = FileCryptoMetaData.Read(ref reader);
                decryptor = FileDecryptor.Create(crypto.EncryptionAlgorithm, decryption);
                thrift = decryptor.DecryptFooter(bytes.Span[reader.Position..], interner);
            }
            else
            {
                thrift = FileMetaData.Parse(bytes.Span, interner);
                if (thrift.EncryptionAlgorithm is { } algorithm)
                {
                    // A plaintext footer of an encrypted file, signed: its nonce and tag follow it.
                    const int Signature = FileDecryptor.NonceLength + FileDecryptor.TagLength;
                    if (footerLength <= Signature) throw new ParquetFormatException("The plaintext footer of the encrypted Parquet file has no room for its signature.");
                    decryptor = FileDecryptor.Create(algorithm, decryption);
                    decryptor.VerifyFooterSignature(bytes.Span[..^Signature], bytes.Span[^Signature..]);
                }
            }
            decryptor?.DecryptColumnMetadata(thrift);
            // An encrypted file's chunks whose keys were not given cannot be placed: the layout is not checked there.
            if (footer is null && decryptor is null) CheckLayout(thrift, tail.AsSpan(0, tailLength - TailLength - footerLength), length - TailLength - footerLength, footerLength);
            ParquetMetadata metadata = ParquetMetadata.FromThrift(thrift, footerLength, decryptor?.Algorithm);
            // An encrypted footer is not handed over (its bytes would not parse without the keys).
            if (decryptor is null) footerRead?.Invoke(bytes.Span); // before the tail buffer is reused below
            if (tailLength < length && source is ILocalSource { SmallReadsAreCheap: true })
            {
                // The start is checked where reading it is cheap (memory, a local file); on a store it would cost a request.
                // The footer is parsed: the tail buffer is free (it is kept only for a file it holds whole).
                await ReadExactlyAsync(source, 0, tail.AsMemory(0, MagicLength), useAsync, ct).ConfigureAwait(false);
                if (!tail.AsSpan(0, MagicLength).SequenceEqual(encryptedFooter ? "PARE"u8 : "PAR1"u8))
                    throw new ParquetFormatException("The data does not start with PAR1: it is not a single Parquet file (cut at the start, or preceded by other bytes).");
            }
            // A file the tail read held whole (up to 64 KiB) is read from those bytes from now on: on an object store
            // each read is a request. Bytes already in memory need no copy. The copy is native (freed with the file:
            // batches never point into it), so that many small files read at once leave the managed heap alone.
            if (smallInMemory && tailLength == length && !InMemory(source))
            {
                var copy = new NativeByteBuffer(tailLength);
                tail.AsSpan(0, tailLength).CopyTo(copy.Span);
                return new ParquetFile(new MemoryRandomAccess(copy.Memory[..tailLength]), new Owned(owned, copy), thrift, metadata, decryptor);
            }
            return new ParquetFile(source, owned, thrift, metadata, decryptor);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tail);
            if (footer is not null) ArrayPool<byte>.Shared.Return(footer);
        }
    }

    /// <summary>
    /// Checks that the footer describes the bytes before it, as a single Parquet file's footer does. Writers leave
    /// little there that the footer does not point at: a copy of the last column's metadata (older parquet-cpp, Impala,
    /// parquet-rs: never longer than the footer itself), or zeros padding the file to a block size. When files are
    /// joined together, the last footer describes bytes of the first file and leaves the data of its own file
    /// unaccounted for: such data is refused here rather than read as garbage (a first file no longer than the footer
    /// passes for a metadata copy; reading it then fails on the shifted pages). Only the bytes the opening read holds
    /// (<paramref name="before"/>, ending at the footer) are looked at: a run of zeros reaching past them is taken for
    /// padding. With a Bloom filter of unstated length the bytes accounted for cannot be told. Column chunks outside
    /// the data are refused when read, as are page indexes and filters there ignored: here they only count where they
    /// lie within it.
    /// </summary>
    private static void CheckLayout(FileMetaData thrift, ReadOnlySpan<byte> before, long footerStart, int footerLength)
    {
        long end = MagicLength;
        foreach (RowGroup group in thrift.RowGroups)
        {
            foreach (ColumnChunk chunk in group.Columns)
            {
                if (chunk.FilePath is not null || chunk.MetaData is not { } meta) continue; // stored elsewhere, or encrypted
                if (meta.TotalCompressedSize > 0)
                {
                    long start = meta.DataPageOffset;
                    if (meta.DictionaryPageOffset is long d && d >= MagicLength && (d < start || start < MagicLength)) start = d;
                    end = Math.Max(end, Within(start, meta.TotalCompressedSize));
                }
                end = Math.Max(end, Within(chunk.ColumnIndexOffset, chunk.ColumnIndexLength));
                end = Math.Max(end, Within(chunk.OffsetIndexOffset, chunk.OffsetIndexLength));
                if (meta.BloomFilterOffset is long bloom && bloom >= MagicLength && bloom < footerStart)
                {
                    if (meta.BloomFilterLength is not int bloomLength) return;
                    end = Math.Max(end, Within(bloom, bloomLength));
                }
            }
        }
        long unaccounted = footerStart - end;
        if (unaccounted <= footerLength) return;
        ReadOnlySpan<byte> seen = before[^(int)Math.Min(unaccounted, before.Length)..];
        int zeros = seen.Length - 1 - seen.LastIndexOfAnyExcept((byte)0);
        if (zeros == seen.Length && seen.Length < unaccounted) return; // zeros as far as can be seen: padding
        long data = unaccounted - zeros;
        if (data > footerLength)
            throw new ParquetFormatException($"{data} bytes before the Parquet footer belong to no column chunk, index or filter it describes: "
                + "the data is not a single Parquet file (several files joined together?).");

        long Within(long? offset, long? length) =>
            offset is long o && length is long n && o >= MagicLength && n >= 0 && o <= footerStart - n ? o + n : MagicLength;
    }

    private static bool InMemory(IRandomAccess source) => source is ILocalSource local && local.TryGetMemory(out _);

    /// <summary>Reads exactly <paramref name="destination"/>.Length bytes at <paramref name="offset"/>.</summary>
    internal static async ValueTask ReadExactlyAsync(IRandomAccess source, long offset, Memory<byte> destination, bool useAsync, CancellationToken ct)
    {
        int n = useAsync
            ? await source.ReadAtAsync(offset, destination, ct).ConfigureAwait(false)
            : source.ReadAt(offset, destination.Span);
        if (n != destination.Length)
            throw new ParquetFormatException($"The Parquet data ended early: {n} of {destination.Length} bytes at offset {offset}.");
    }

    /// <summary>The source a file owns, and the native copy it reads instead.</summary>
    private sealed class Owned(IDisposable? source, NativeByteBuffer copy) : IDisposable
    {
        public void Dispose()
        {
            source?.Dispose();
            ((IDisposable)copy).Dispose();
        }
    }
}
