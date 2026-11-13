using System.IO.Hashing;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Excel.Writing;

/// <summary>
/// The shared strings of a sheet being written: each distinct UTF-8 string once in a native buffer, found again by
/// an open-addressing hash table of indexes. Everything (bytes, entries, slots) lives in native memory, so a table of
/// millions of strings puts nothing on the managed heap. Indexes are assigned in order of first use, so the table can
/// be written after the sheet.
/// </summary>
internal sealed unsafe class SharedStringTable : IDisposable
{
    [StructLayout(LayoutKind.Sequential)]
    private struct Entry
    {
        public long Offset;
        public int Length;
        public uint Hash;
    }

    private NativeGrowBuffer _bytes;
    private NativeGrowBuffer _entries; // Entry per string, by index
    private int* _slots;               // index + 1, 0 = free
    private int _slotCount;
    private int _count;

    public SharedStringTable()
    {
        _slotCount = 4096;
        _slots = (int*)NativeMemory.AllocZeroed((nuint)_slotCount, sizeof(int));
    }

    /// <summary>Distinct strings.</summary>
    public int Count => _count;

    /// <summary>Cells referring to the table.</summary>
    public long References { get; private set; }

    private Entry* Entries => (Entry*)_entries.Pointer;

    public ReadOnlySpan<byte> Get(int index)
    {
        Entry e = Entries[index];
        return new(_bytes.Pointer + e.Offset, e.Length);
    }

    /// <summary>The index of a string, added if new.</summary>
    public int Add(ReadOnlySpan<byte> utf8)
    {
        References++;
        uint hash = (uint)XxHash3.HashToUInt64(utf8);
        int mask = _slotCount - 1;
        int slot = (int)(hash & (uint)mask);
        while (true)
        {
            int entry = _slots[slot];
            if (entry == 0) break;
            int index = entry - 1;
            if (Entries[index].Hash == hash && Get(index).SequenceEqual(utf8)) return index;
            slot = (slot + 1) & mask;
        }
        int added = _count;
        long offset = _bytes.Length;
        _bytes.Append(utf8);
        _entries.Append(new Entry { Offset = offset, Length = utf8.Length, Hash = hash });
        _slots[slot] = added + 1;
        _count = added + 1;
        if (_count * 2 > _slotCount) Rehash();
        return added;
    }

    private void Rehash()
    {
        int count = _slotCount * 2;
        var slots = (int*)NativeMemory.AllocZeroed((nuint)count, sizeof(int));
        int mask = count - 1;
        for (int i = 0; i < _count; i++)
        {
            int slot = (int)(Entries[i].Hash & (uint)mask);
            while (slots[slot] != 0) slot = (slot + 1) & mask;
            slots[slot] = i + 1;
        }
        NativeMemory.Free(_slots);
        _slots = slots;
        _slotCount = count;
    }

    public void Dispose()
    {
        _bytes.Free();
        _entries.Free();
        if (_slots != null) NativeMemory.Free(_slots);
        _slots = null;
        GC.SuppressFinalize(this);
    }

    ~SharedStringTable()
    {
        _bytes.Free();
        _entries.Free();
        if (_slots != null) NativeMemory.Free(_slots);
    }
}
