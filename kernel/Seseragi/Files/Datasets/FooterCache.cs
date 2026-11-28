using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// The footers of Parquet and Arrow IPC files shared between scans (<see cref="DatasetOptions.FooterCache"/>): a scan
/// that opens a file whose footer another scan read takes it from here instead of asking the store for it. Kept as
/// they are read (their bytes, in native memory), parsed again by each scan.
/// </summary>
/// <remarks>
/// <para>
/// Only footers whose reading costs a request of their own are kept: those of files on a store (not a local file or
/// memory, where reading a tail costs microseconds) larger than a format's first read of a file's tail (a smaller file
/// comes whole in that read, its data too). Encrypted Parquet footers are not kept.
/// </para>
/// <para>
/// An entry is a file of a store (the same store instance), by path, with its size and version (its ETag and when it
/// was last written, as far as the store tells them): a scan takes it only for a file of the same size whose version,
/// as the scan knows it (from the listing, or the store), agrees; it is then read at that version (a file replaced
/// since fails the read as "changed while it was read"). Files a scan is given without a version (a table's log of
/// its files: Delta, Iceberg, whose data files are never rewritten) match by path and size.
/// </para>
/// <para>
/// At most <see cref="Capacity"/> bytes are kept; the least recently used footers go first. Safe to share between
/// threads. Dispose it to free its memory (footers in use are freed once their scans are done with them).
/// </para>
/// </remarks>
public sealed class FooterCache : IDisposable
{
    private readonly object _lock = new();
    private readonly Dictionary<Key, Entry> _entries = [];
    private readonly LinkedList<Key> _recency = []; // most recent first
    private long _bytes;
    private bool _disposed;

    /// <summary>A cache keeping up to <paramref name="capacity"/> bytes of footers.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The capacity is negative.</exception>
    public FooterCache(long capacity = 64L << 20)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        Capacity = capacity;
    }

    /// <summary>The most bytes kept.</summary>
    public long Capacity { get; }

    /// <summary>The bytes of the footers kept.</summary>
    public long Bytes
    {
        get
        {
            lock (_lock) return _bytes;
        }
    }

    /// <summary>The number of footers kept.</summary>
    public int Count
    {
        get
        {
            lock (_lock) return _entries.Count;
        }
    }

    /// <summary>The footers scans took from here (and those they had to read), since the cache was made.</summary>
    internal long Hits, Misses;

    /// <summary>Removes every footer (those in use are freed once their scans are done with them).</summary>
    public void Clear()
    {
        lock (_lock)
        {
            foreach (Entry entry in _entries.Values) entry.Release();
            _entries.Clear();
            _recency.Clear();
            _bytes = 0;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
            Clear();
        }
    }

    /// <summary>The kind of footer: of a Parquet file, or an Arrow IPC file's (or stream's scan).</summary>
    internal enum Format : byte { Parquet, Ipc }

    internal readonly record struct Key(IFileStore Store, Format Format, string Path)
    {
        public bool Equals(Key other) => ReferenceEquals(Store, other.Store) && Format == other.Format && string.Equals(Path, other.Path, StringComparison.Ordinal);

        public override int GetHashCode() => HashCode.Combine(System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(Store), Format, Path);
    }

    /// <summary>
    /// The footer of <paramref name="path"/> when kept for a file of that <paramref name="size"/> and of a version
    /// agreeing with <paramref name="version"/> (null: none known), as a lease (dispose it when the footer is parsed);
    /// the version it was read at comes with it. An entry of another size or version is dropped.
    /// </summary>
    internal bool TryGet(IFileStore store, Format format, string path, long size, StoredFile? version, out FooterLease lease)
    {
        lease = default;
        var key = new Key(store, format, path);
        lock (_lock)
        {
            if (_disposed || !_entries.TryGetValue(key, out Entry? entry))
            {
                Misses++;
                return false;
            }
            if (entry.Version.Size != size || !Agree(entry.Version, version))
            {
                Remove(key, entry);
                Misses++;
                return false;
            }
            _recency.Remove(entry.Node);
            _recency.AddFirst(entry.Node);
            entry.Acquire();
            Hits++;
            lease = new FooterLease(entry);
            return true;
        }
    }

    /// <summary>
    /// Keeps the footer <paramref name="footer"/> of a file at <paramref name="version"/> (its size, and its version as
    /// far as known), replacing an entry of the path; nothing when it is larger than the capacity.
    /// </summary>
    internal unsafe void Put(IFileStore store, Format format, StoredFile version, ReadOnlySpan<byte> footer)
    {
        if (footer.IsEmpty || footer.Length > Capacity) return;
        var key = new Key(store, format, version.Path);
        byte* copy = NativeMemoryPool.Alloc(footer.Length);
        footer.CopyTo(new Span<byte>(copy, footer.Length));
        var entry = new Entry(copy, footer.Length, version);
        lock (_lock)
        {
            if (_disposed)
            {
                entry.Release();
                return;
            }
            if (_entries.TryGetValue(key, out Entry? old)) Remove(key, old);
            while (_bytes + footer.Length > Capacity && _recency.Last is { } oldest) Remove(oldest.Value, _entries[oldest.Value]);
            entry.Node = _recency.AddFirst(key);
            _entries[key] = entry;
            _bytes += footer.Length;
        }
    }

    // Under the lock.
    private void Remove(Key key, Entry entry)
    {
        _entries.Remove(key);
        _recency.Remove(entry.Node);
        _bytes -= entry.Length;
        entry.Release();
    }

    /// <summary>Whether what is known of two versions of a file agrees: their ETags, and when they were written, where both tell.</summary>
    private static bool Agree(StoredFile kept, StoredFile? seen)
    {
        if (seen is not { } now) return true;
        if (kept.ETag is { } a && now.ETag is { } b && !string.Equals(a, b, StringComparison.Ordinal)) return false;
        if (kept.LastModified is { } x && now.LastModified is { } y && x != y) return false;
        return true;
    }

    /// <summary>A footer's bytes in native memory, freed when neither the cache nor a scan holds it.</summary>
    internal sealed unsafe class Entry(byte* bytes, int length, StoredFile version)
    {
        private int _holders = 1; // the cache's

        public byte* Pointer => bytes;

        public int Length => length;

        public StoredFile Version => version;

        public LinkedListNode<Key> Node { get; set; } = null!;

        public void Acquire() => Interlocked.Increment(ref _holders);

        public void Release()
        {
            if (Interlocked.Decrement(ref _holders) == 0) NativeMemoryPool.Free(bytes);
        }
    }
}

/// <summary>A footer taken from a <see cref="FooterCache"/>: its bytes, valid until disposed, and the version they were read at.</summary>
internal readonly unsafe struct FooterLease(FooterCache.Entry entry) : IDisposable
{
    public ReadOnlySpan<byte> Span => new(entry.Pointer, entry.Length);

    /// <summary>The bytes as memory (valid until disposed).</summary>
    public ReadOnlyMemory<byte> Memory => PointerMemory.Of(entry.Pointer, entry.Length);

    /// <summary>The file's size and version when the footer was read.</summary>
    public StoredFile Version => entry.Version;

    public void Dispose() => entry?.Release();
}
