using System.Buffers.Binary;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// A file's footer as its writer wrote it: the Thrift bytes in native memory until disposed, the metadata parsed from
/// them only when asked for. A sink holds its files' footers until their statistics are read from the bytes, so no
/// object graph of them lives that long. A footer read elsewhere is held as its metadata alone.
/// </summary>
internal sealed class ParquetFooter : IDisposable
{
    private NativeBufferWriter? _bytes; // the Thrift bytes, then how the file ends (from _tail on)
    private readonly int _length;
    private readonly int _tail;
    private ParquetMetadata? _metadata;
    private static long s_held;

    private ParquetFooter(NativeBufferWriter bytes, int length, int tail = 0)
    {
        _bytes = bytes;
        _length = length;
        _tail = tail;
        Interlocked.Increment(ref s_held);
    }

    /// <summary>How many footers hold their bytes now, process-wide (each one is to be disposed).</summary>
    internal static long Held => Interlocked.Read(ref s_held);

    /// <summary>A footer read elsewhere: its metadata, no bytes.</summary>
    public ParquetFooter(ParquetMetadata metadata) => _metadata = metadata;

    /// <summary>The footer of these Thrift bytes, copied into native memory with their length and the magic after them.</summary>
    public static ParquetFooter Of(ReadOnlySpan<byte> thrift)
    {
        var bytes = new NativeBufferWriter(thrift.Length + 8);
        Span<byte> tail = bytes.GetSpan(thrift.Length + 8);
        thrift.CopyTo(tail);
        BinaryPrimitives.WriteInt32LittleEndian(tail[thrift.Length..], thrift.Length);
        "PAR1"u8.CopyTo(tail[(thrift.Length + 4)..]);
        bytes.Advance(thrift.Length + 8);
        return new ParquetFooter(bytes, thrift.Length);
    }

    /// <summary>Writes how an encrypted file ends (<paramref name="stored"/>) from its plaintext footer.</summary>
    public delegate void StoredWriter(ReadOnlySpan<byte> thrift, Span<byte> stored);

    /// <summary>
    /// The footer of an encrypted file: its plaintext Thrift bytes (the metadata is parsed from them), and how the file
    /// ends: <paramref name="storedLength"/> bytes <paramref name="writeStored"/> writes (a signed footer, or the crypto
    /// metadata and the encrypted footer), their length and <paramref name="magic"/>.
    /// </summary>
    public static ParquetFooter Encrypted(ReadOnlySpan<byte> thrift, int storedLength, StoredWriter writeStored, ReadOnlySpan<byte> magic)
    {
        int size = thrift.Length + storedLength + 8;
        var bytes = new NativeBufferWriter(size);
        Span<byte> all = bytes.GetSpan(size);
        thrift.CopyTo(all);
        writeStored(all[..thrift.Length], all.Slice(thrift.Length, storedLength));
        BinaryPrimitives.WriteInt32LittleEndian(all[(thrift.Length + storedLength)..], storedLength);
        magic.CopyTo(all[(size - 4)..]);
        bytes.Advance(size);
        return new ParquetFooter(bytes, thrift.Length, thrift.Length);
    }

    /// <summary>Whether the Thrift bytes are held (a footer as written, not disposed).</summary>
    public bool HasBytes => _bytes is not null;

    /// <summary>The Thrift bytes (none without them); valid until disposed.</summary>
    public ReadOnlySpan<byte> Thrift => _bytes is { } bytes ? bytes.WrittenSpan[.._length] : [];

    /// <summary>How the file ends: the Thrift bytes, their length and the magic.</summary>
    public ReadOnlyMemory<byte> FileTail => (_bytes ?? throw new ObjectDisposedException(nameof(ParquetFooter))).WrittenMemory[_tail..];

    /// <summary>The metadata, parsed from the bytes when first asked for.</summary>
    /// <exception cref="ObjectDisposedException">The bytes were freed before the metadata was asked for.</exception>
    public ParquetMetadata Metadata => _metadata ??= _bytes is { } bytes
        ? ParquetMetadata.FromThrift(FileMetaData.Parse(bytes.WrittenSpan[.._length]))
        : throw new ObjectDisposedException(nameof(ParquetFooter));

    /// <summary>Frees the bytes (the metadata, if parsed, stays).</summary>
    public void Dispose()
    {
        if (_bytes is null) return;
        _bytes.Dispose();
        _bytes = null;
        Interlocked.Decrement(ref s_held);
    }
}
