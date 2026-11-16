using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>Receives a footer's Thrift bytes while a file opens (valid only during the call).</summary>
internal delegate void FooterRead(ReadOnlySpan<byte> footer);

/// <summary>
/// The footers a dataset keeps between reads, as their Thrift bytes, parsed again when a file is opened again. Only
/// footers whose reading costs a request of its own are kept: those of files on a store (not a local file or memory:
/// reading a tail there costs microseconds, and later reads then see the files as they are) larger than the first read
/// of a file's tail (a smaller file comes whole in that read, data included). The bytes are copied into chunks of
/// <see cref="ChunkSize"/> bytes (a larger footer gets an array of its own), so that the collector sees a few arrays
/// without references instead of each footer's objects; up to <see cref="Limit"/> bytes in all.
/// </summary>
internal sealed class KeptFooters(int files)
{
    internal const int ChunkSize = 1 << 20;

    /// <summary>The bytes kept by default (provisional: to be set from measurements on remote stores).</summary>
    internal const long DefaultLimit = 64L << 20;

    private readonly object _lock = new();
    private readonly (int Chunk, int Offset, int Length)[] _slots = new (int, int, int)[files];
    private readonly StoredFile?[] _versions = new StoredFile?[files];
    private readonly List<byte[]> _chunks = [];
    private int _current = -1;     // the chunk small footers go to
    private int _used = ChunkSize; // of that chunk: none yet

    /// <summary>The most bytes kept; footers beyond it are read again when their files open again.</summary>
    internal long Limit { get; set; } = DefaultLimit;

    /// <summary>The footer bytes kept.</summary>
    internal long Bytes { get; private set; }

    /// <summary>The number of footers kept.</summary>
    internal int Count { get; private set; }

    /// <summary>Whether a footer read from <paramref name="source"/> is worth keeping (see the class remarks).</summary>
    internal static bool Worth(IRandomAccess source) => source is not ILocalSource && source.Length > ParquetMetadata.TailGuess;

    /// <summary>
    /// Keeps the footer of file <paramref name="index"/> unless one is kept already or the limit is reached, with the
    /// file's size and version when its source tells them (<paramref name="version"/>: the file is then opened again
    /// without a request, and its reads require that version).
    /// </summary>
    internal void Keep(int index, ReadOnlySpan<byte> footer, StoredFile? version = null)
    {
        if (footer.IsEmpty) return;
        lock (_lock)
        {
            if (_slots[index].Length != 0 || Bytes + footer.Length > Limit) return;
            if (footer.Length > ChunkSize / 4)
            {
                _chunks.Add(footer.ToArray()); // a large footer: an array of its own (the current chunk stays open)
                _slots[index] = (_chunks.Count - 1, 0, footer.Length);
            }
            else
            {
                if (ChunkSize - _used < footer.Length)
                {
                    _chunks.Add(new byte[ChunkSize]);
                    _current = _chunks.Count - 1;
                    _used = 0;
                }
                footer.CopyTo(_chunks[_current].AsSpan(_used));
                _slots[index] = (_current, _used, footer.Length);
                _used += footer.Length;
            }
            _versions[index] = version;
            Bytes += footer.Length;
            Count++;
        }
    }

    /// <summary>The size and version of file <paramref name="index"/> when its footer was kept (and its source told them).</summary>
    internal StoredFile? Version(int index)
    {
        lock (_lock) return _versions[index];
    }

    /// <summary>The footer of file <paramref name="index"/> parsed again (sharing with <paramref name="interner"/>), or null when none is kept.</summary>
    internal ParquetMetadata? Get(int index, FooterInterner? interner = null)
    {
        byte[] chunk;
        int offset, length;
        lock (_lock)
        {
            (int c, offset, length) = _slots[index];
            if (length == 0) return null;
            chunk = _chunks[c];
        }
        return ParquetMetadata.ParseFooter(chunk.AsSpan(offset, length), interner);
    }
}
