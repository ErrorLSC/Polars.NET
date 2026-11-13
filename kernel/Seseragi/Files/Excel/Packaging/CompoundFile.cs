using System.Buffers.Binary;
using System.Collections;
using System.Text;

namespace Kaminari.Seseragi.Files.Excel.Packaging;

/// <summary>
/// An OLE2 compound file ([MS-CFB]): the header, the FAT (located through the DIFAT), the directory and the mini
/// stream, enough to open the streams at the root of the file. Opening reads the header, the FAT and the directory;
/// the mini FAT and mini stream are read when a small stream is first opened. Counts and sector numbers from the
/// file are checked against its length before anything is allocated from them, and every chain is bounded by the
/// number of sectors, so a malformed or hostile file fails with <see cref="ExcelFormatException"/>.
/// </summary>
internal sealed class CompoundFile : IDisposable
{
    /// <summary>The first eight bytes of every compound file.</summary>
    public static ReadOnlySpan<byte> Signature => [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];

    private const uint EndOfChain = 0xFFFFFFFE;
    private const uint NoStream = 0xFFFFFFFF;
    private const int MiniShift = 6;
    private const int HeaderSize = 512;
    private const int DirectoryEntrySize = 128;

    private readonly RandomAccessSource _source;
    private readonly int _shift;
    private readonly uint[] _fat;
    private readonly uint _miniCutoff;
    private readonly uint _firstMiniFat;
    private readonly Entry[] _entries;
    private readonly Dictionary<string, int> _root;
    private readonly Dictionary<int, uint[]> _chains = []; // by directory entry, once followed
    private uint[]? _miniFat;
    private CompoundFileStream? _miniStream;

    private readonly record struct Entry(string Name, byte Type, uint Left, uint Right, uint Child, uint Start, long Size);

    private CompoundFile(RandomAccessSource source, int shift, uint[] fat, uint miniCutoff, uint firstMiniFat, Entry[] entries)
    {
        _source = source;
        _shift = shift;
        _fat = fat;
        _miniCutoff = miniCutoff;
        _firstMiniFat = firstMiniFat;
        _entries = entries;
        _root = RootChildren(entries);
    }

    /// <summary>Whether the root holds a stream or storage of this name (compared case-insensitively).</summary>
    public bool Has(string name) => _root.ContainsKey(name);

    /// <summary>Whether the root holds a stream (not a storage) of this name.</summary>
    public bool HasStream(string name) => _root.TryGetValue(name, out int id) && _entries[id].Type == 2;

    public static async ValueTask<CompoundFile> OpenAsync(RandomAccessSource source, bool useAsync, CancellationToken ct)
    {
        try
        {
            return await OpenCoreAsync(source, useAsync, ct).ConfigureAwait(false);
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private static async ValueTask<CompoundFile> OpenCoreAsync(RandomAccessSource source, bool useAsync, CancellationToken ct)
    {
        // Header (2.2).
        var header = new byte[HeaderSize];
        if (await ReadAsync(source, 0, header, useAsync, ct).ConfigureAwait(false) < HeaderSize)
            throw new ExcelFormatException("The compound file is shorter than its header.");
        if (!header.AsSpan(0, 8).SequenceEqual(Signature)) throw new ExcelFormatException("The data is not an OLE2 compound file.");
        int major = U16(header, 26);
        int shift = U16(header, 30);
        if (U16(header, 28) != 0xFFFE || !(major == 3 && shift == 9 || major == 4 && shift == 12) || U16(header, 32) != MiniShift)
            throw new ExcelFormatException($"Unsupported compound file header (version {major}, sector shift {shift}).");
        int sectorSize = 1 << shift;
        // Sectors after the header sector; a last sector cut short still counts (its stream may end inside it).
        long sectorCount = Math.Max(0, (source.Length - sectorSize + sectorSize - 1) >> shift);
        uint fatSectors = U32(header, 44);
        uint firstDirectory = U32(header, 48);
        uint miniCutoff = U32(header, 56);
        uint firstMiniFat = U32(header, 60);
        uint firstDifat = U32(header, 68);
        if (fatSectors == 0 || fatSectors > sectorCount)
            throw new ExcelFormatException($"The compound file declares {fatSectors} FAT sectors, but it has {sectorCount} sectors.");

        // The DIFAT (2.5): 109 entries in the header, then a chain of DIFAT sectors whose last entry links the next.
        var fatLocations = new uint[fatSectors];
        int found = 0;
        for (int i = 0; i < 109 && found < fatLocations.Length; i++) fatLocations[found++] = U32(header, 76 + 4 * i);
        var sector = new byte[sectorSize];
        uint next = firstDifat;
        for (long steps = 0; found < fatLocations.Length; steps++)
        {
            if (next >= sectorCount || steps >= sectorCount)
                throw new ExcelFormatException("The compound file's DIFAT chain ends before all FAT sectors are listed.");
            await ReadSectorAsync(source, next, shift, sector, useAsync, ct).ConfigureAwait(false);
            int perSector = sectorSize / 4 - 1;
            for (int i = 0; i < perSector && found < fatLocations.Length; i++) fatLocations[found++] = U32(sector, 4 * i);
            next = U32(sector, sectorSize - 4);
        }

        // The FAT (2.3).
        int perFatSector = sectorSize / 4;
        var fat = new uint[(long)fatLocations.Length * perFatSector];
        var fatBytes = new byte[sectorSize];
        for (int i = 0; i < fatLocations.Length; i++)
        {
            if (fatLocations[i] >= sectorCount) throw new ExcelFormatException($"The compound file lists FAT sector {fatLocations[i]}, past its end.");
            await ReadSectorAsync(source, fatLocations[i], shift, fatBytes, useAsync, ct).ConfigureAwait(false);
            for (int k = 0; k < perFatSector; k++) fat[(long)i * perFatSector + k] = U32(fatBytes, 4 * k);
        }

        // The directory (2.6): a chain of sectors of 128-byte entries; entry 0 is the root storage.
        var chain = Chain(fat, fat.Length, firstDirectory, null, "directory");
        // A file cut short inside its last directory sector keeps the whole entries before the cut.
        if (chain.Length == 0) throw new ExcelFormatException("The compound file has no directory.");
        long lastStart = (long)chain[^1] + 1 << shift;
        long directoryLength = ((long)chain.Length - 1 << shift) + Math.Clamp(source.Length - lastStart, 0, sectorSize);
        // A chain is bounded by the FAT, which describes more sectors than the file holds: what is read whole must fit
        // in the file.
        if (directoryLength > source.Length) throw new ExcelFormatException("The compound file's directory is larger than the file.");
        var directory = new byte[directoryLength];
        await ReadWholeAsync(new CompoundFileStream(source, chain, shift, true, directory.Length), directory, useAsync, ct).ConfigureAwait(false);
        var entries = new Entry[directory.Length / DirectoryEntrySize];
        for (int i = 0; i < entries.Length; i++)
        {
            var e = directory.AsSpan(i * DirectoryEntrySize, DirectoryEntrySize);
            int nameLength = BinaryPrimitives.ReadUInt16LittleEndian(e[64..]);
            string name = nameLength is >= 2 and <= 64 && nameLength % 2 == 0 ? Encoding.Unicode.GetString(e[..(nameLength - 2)]) : "";
            long size = (long)BinaryPrimitives.ReadUInt64LittleEndian(e[120..]);
            if (major == 3) size &= 0xFFFFFFFF; // old writers left the high half uninitialised (2.6.3)
            entries[i] = new(name, e[66], BinaryPrimitives.ReadUInt32LittleEndian(e[68..]), BinaryPrimitives.ReadUInt32LittleEndian(e[72..]),
                BinaryPrimitives.ReadUInt32LittleEndian(e[76..]), BinaryPrimitives.ReadUInt32LittleEndian(e[116..]), size);
        }
        if (entries.Length == 0 || entries[0].Type != 5) throw new ExcelFormatException("The compound file's directory has no root entry.");
        return new(source, shift, fat, miniCutoff, firstMiniFat, entries);
    }

    // The children of the root storage: a tree through the Left and Right links of the root's Child; each entry is
    // visited once.
    private static Dictionary<string, int> RootChildren(Entry[] entries)
    {
        Dictionary<string, int> children = new(StringComparer.OrdinalIgnoreCase);
        var seen = new bool[entries.Length];
        Stack<uint> pending = [];
        pending.Push(entries[0].Child);
        while (pending.Count > 0)
        {
            uint id = pending.Pop();
            // Links past the directory (a file cut short) and back to visited entries (a malformed tree) lead nowhere.
            if (id >= entries.Length || seen[id]) continue;
            seen[id] = true;
            var e = entries[id];
            if (e.Type is 1 or 2) children.TryAdd(e.Name, (int)id);
            pending.Push(e.Left);
            pending.Push(e.Right);
        }
        return children;
    }

    /// <summary>Opens a stream at the root of the file by name (case-insensitive).</summary>
    public async ValueTask<CompoundFileStream> OpenStreamAsync(string name, bool useAsync, CancellationToken ct)
    {
        if (!_root.TryGetValue(name, out int id) || _entries[id].Type != 2)
            throw new ExcelFormatException($"The compound file has no stream '{name}'.");
        var e = _entries[id];
        if (e.Size < 0) throw new ExcelFormatException($"Stream '{name}' has an invalid size.");
        if (e.Size >= _miniCutoff)
        {
            if (!_chains.TryGetValue(id, out var chain))
                _chains[id] = chain = Chain(_fat, _fat.Length, e.Start, (e.Size + (1L << _shift) - 1) >> _shift, name);
            return new CompoundFileStream(_source, chain, _shift, true, e.Size);
        }
        // A small stream lives in the mini stream (the root entry's chain), allocated by the mini FAT (2.4).
        if (_miniStream is null)
        {
            var root = _entries[0];
            var rootChain = Chain(_fat, _fat.Length, root.Start, (root.Size + (1L << _shift) - 1) >> _shift, "mini stream");
            var miniFatChain = Chain(_fat, _fat.Length, _firstMiniFat, null, "mini FAT");
            if ((long)miniFatChain.Length << _shift > _source.Length) throw new ExcelFormatException("The compound file's mini FAT is larger than the file.");
            var bytes = new byte[(long)miniFatChain.Length << _shift];
            await ReadWholeAsync(new CompoundFileStream(_source, miniFatChain, _shift, true, bytes.Length), bytes, useAsync, ct).ConfigureAwait(false);
            _miniFat = new uint[bytes.Length / 4];
            for (int i = 0; i < _miniFat.Length; i++) _miniFat[i] = U32(bytes, 4 * i);
            _miniStream = new CompoundFileStream(_source, rootChain, _shift, true, root.Size);
        }
        if (!_chains.TryGetValue(id, out var miniChain))
        {
            long miniSectors = (_miniStream.Length + (1 << MiniShift) - 1) >> MiniShift;
            _chains[id] = miniChain = Chain(_miniFat!, miniSectors, e.Start, (e.Size + (1 << MiniShift) - 1) >> MiniShift, name);
        }
        return new CompoundFileStream(_miniStream, miniChain, MiniShift, false, e.Size);
    }

    /// <summary>
    /// Follows a chain from <paramref name="start"/>: exactly <paramref name="needed"/> sectors when given (the rest
    /// of the chain is not followed), else up to ENDOFCHAIN. A sector number the allocation table does not cover, or
    /// a sector met twice (a cycle), is an error; a sector past the end of a file cut short only fails
    /// when it is read.
    /// </summary>
    private static uint[] Chain(uint[] fat, long sectors, uint start, long? needed, string what)
    {
        long limit = Math.Min(sectors, fat.Length);
        if (needed > limit) throw new ExcelFormatException($"The compound file's {what} needs more sectors than the file has.");
        List<uint> chain = needed is { } n ? new((int)n) : [];
        var seen = new BitArray((int)limit);
        uint s = start;
        while (needed is null ? s != EndOfChain : chain.Count < needed)
        {
            if (s >= limit) throw new ExcelFormatException($"The compound file's {what} chain is broken (sector {s:X}).");
            if (seen[(int)s]) throw new ExcelFormatException($"The compound file's {what} chain loops.");
            seen[(int)s] = true;
            chain.Add(s);
            s = fat[s];
        }
        return [.. chain];
    }

    private static async ValueTask ReadSectorAsync(RandomAccessSource source, uint sector, int shift, byte[] into, bool useAsync, CancellationToken ct)
    {
        if (await ReadAsync(source, (sector + 1L) << shift, into, useAsync, ct).ConfigureAwait(false) < into.Length)
            throw new ExcelFormatException("The compound file is truncated.");
    }

    private static async ValueTask<int> ReadAsync(RandomAccessSource source, long offset, byte[] into, bool useAsync, CancellationToken ct) =>
        useAsync ? await source.ReadAtAsync(offset, into, ct).ConfigureAwait(false) : source.ReadAt(offset, into);

    private static async ValueTask ReadWholeAsync(CompoundFileStream stream, byte[] into, bool useAsync, CancellationToken ct)
    {
        int n = useAsync ? await stream.ReadAtAsync(0, into, ct).ConfigureAwait(false) : stream.ReadAt(0, into);
        if (n < into.Length) throw new ExcelFormatException("The compound file is truncated.");
    }

    private static ushort U16(byte[] b, int at) => BinaryPrimitives.ReadUInt16LittleEndian(b.AsSpan(at));

    private static uint U32(byte[] b, int at) => BinaryPrimitives.ReadUInt32LittleEndian(b.AsSpan(at));

    public void Dispose() => _source.Dispose();
}
