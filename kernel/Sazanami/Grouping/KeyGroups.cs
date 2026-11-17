using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Sazanami.Grouping;

/// <summary>
/// The distinct keys of a few key columns over a run of batches, each numbered in order of first appearance (a global
/// group id), and per batch its rows grouped: dense ids within the batch (for <see cref="Ordering.CountingSort"/>, whose
/// cost grows with the groups), mapped to the global ids. Keys are hashed and compared column by column, never
/// serialized per row; the keys seen are kept, column by column, in native memory of its own. Not thread-safe.
/// </summary>
/// <remarks>
/// Keys are equal as the IR's grouping takes them (kernel/Samidare/IR/SEMANTICS.md, Grouping keys): null equals null,
/// component by component; every floating NaN is one value and -0.0 equals 0.0; strings and binaries by their bytes,
/// whatever their layout (plain, large, view) from batch to batch; dictionary arrays by the values their entries stand
/// for; other values by what they store. <c>emptyIsNull</c> (per column) takes an empty string or binary as null, as
/// Hive partitions do: a caller's rule, off by default. A column keeps the kind and type of its first batch.
/// TODO: a probe for hash joins, <c>Find(keys, nullsEqual, groups)</c>: the group of each row without adding keys
/// (-1 when none), with the join's explicit <c>nullsEqual</c> (a backend never chooses it, SEMANTICS.md).
/// </remarks>
internal sealed class KeyGroups : IDisposable
{
    /// <summary>A row's hash before its first column (internal: tests build hash collisions).</summary>
    internal const ulong Seed = 0x243F_6A88_85A3_08D3UL;

    /// <summary>A null value's hash in a column.</summary>
    internal const ulong NullHash = 0x9E37_79B9_7F4A_7C15UL;

    private const ulong K = 0xFF51_AFD7_ED55_8CCDUL;

    private readonly bool[] _emptyIsNull;
    private readonly StoredKeys[] _stored;
    private readonly BatchKey[] _batch;
    private readonly NativeArray<nint>[] _viewBuffers;
    private readonly List<MemoryHandle> _pins = [];

    private readonly NativeArray<int> _slots = new(); // group id + 1 in each slot, 0 when empty
    private int _slotCount;
    private readonly NativeArray<ulong> _groupHashes = new();
    private readonly NativeArray<int> _localOf = new(); // per global group: its batch group + 1 in the batch being grouped, else 0
    private readonly NativeArray<ulong> _hashes = new();
    private int _count;

    /// <param name="keyCount">The key columns (at least one).</param>
    /// <param name="emptyIsNull">Per key column, whether an empty string or binary is null (none: no column).</param>
    public KeyGroups(int keyCount, ReadOnlySpan<bool> emptyIsNull = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(keyCount, 1);
        if (!emptyIsNull.IsEmpty && emptyIsNull.Length != keyCount)
            throw new ArgumentException($"{emptyIsNull.Length} emptyIsNull flags for {keyCount} key columns.", nameof(emptyIsNull));
        _emptyIsNull = emptyIsNull.IsEmpty ? new bool[keyCount] : emptyIsNull.ToArray();
        _stored = new StoredKeys[keyCount];
        _viewBuffers = new NativeArray<nint>[keyCount];
        for (int c = 0; c < keyCount; c++) (_stored[c], _viewBuffers[c]) = (new(), new());
        _batch = new BatchKey[keyCount];
        EmptyTable(1024);
    }

    /// <summary>The distinct keys seen so far, over all batches (the next new key gets this id).</summary>
    public int Count => _count;

    /// <summary>
    /// Groups the rows of one batch by its key columns. <paramref name="ids"/> receives each row's group within the
    /// batch (0.., in order of first appearance in it); <paramref name="groups"/> each batch group's global id (new keys
    /// added, numbered on); <paramref name="firstRows"/> the row where each batch group first appears. Returns the
    /// batch's group count. The spans hold at least the batch's rows.
    /// </summary>
    /// <exception cref="ArgumentException">Another column count, columns of unequal lengths, short spans, or a column whose
    /// kind or type differs from its first batch's.</exception>
    /// <exception cref="NotSupportedException">A key type not covered (nested types).</exception>
    public int Assign(IReadOnlyList<IArrowArray> keys, Span<int> ids, Span<int> groups, Span<int> firstRows)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count != _stored.Length) throw new ArgumentException($"{keys.Count} key columns; the groups take {_stored.Length}.", nameof(keys));
        int rows = keys[0].Length;
        for (int c = 1; c < keys.Count; c++)
        {
            if (keys[c].Length != rows) throw new ArgumentException($"Key column {c} has {keys[c].Length} rows; column 0 has {rows}.", nameof(keys));
        }
        if (ids.Length < rows || groups.Length < rows || firstRows.Length < rows)
            throw new ArgumentException($"The ids, groups and first rows take {rows} entries each.");

        int locals = 0;
        try
        {
            for (int c = 0; c < keys.Count; c++) Prepare(c, keys[c]);
            _hashes.Reserve(rows);
            Span<ulong> hashes = _hashes.AsSpan(0, rows);
            hashes.Fill(Seed);
            for (int c = 0; c < keys.Count; c++) Hash(in _batch[c], hashes);
            for (int row = 0; row < rows; row++)
            {
                int group = FindOrAdd(Final(hashes[row]), row);
                ref int local = ref _localOf.At(group);
                if (local == 0)
                {
                    groups[locals] = group;
                    firstRows[locals] = row;
                    local = ++locals;
                }
                ids[row] = local - 1;
            }
            return locals;
        }
        finally
        {
            for (int l = 0; l < locals; l++) _localOf.At(groups[l]) = 0;
            foreach (MemoryHandle pin in _pins) pin.Dispose();
            _pins.Clear();
            System.Array.Clear(_batch);
        }
    }

    /// <summary>Forgets every key (and the columns' kinds); keeps the memory.</summary>
    public void Clear()
    {
        _count = 0;
        _slots.AsSpan(0, _slotCount).Clear();
        foreach (StoredKeys stored in _stored) stored.Reset();
    }

    /// <summary>The native bytes it holds.</summary>
    public long ScratchBytes
    {
        get
        {
            long bytes = 4L * _slots.Capacity + 8L * _groupHashes.Capacity + 4L * _localOf.Capacity + 8L * _hashes.Capacity;
            foreach (StoredKeys stored in _stored) bytes += stored.ScratchBytes;
            foreach (NativeArray<nint> buffers in _viewBuffers) bytes += 8L * buffers.Capacity;
            return bytes;
        }
    }

    public void Dispose()
    {
        foreach (StoredKeys stored in _stored) stored.Dispose();
        foreach (NativeArray<nint> buffers in _viewBuffers) buffers.Dispose();
        _slots.Dispose();
        _groupHashes.Dispose();
        _localOf.Dispose();
        _hashes.Dispose();
    }

    /// <summary>Points the batch column at a key array, checking its kind against the column's first batch.</summary>
    private void Prepare(int c, IArrowArray array)
    {
        IArrowType type = array.Data.DataType is DictionaryType d ? d.ValueType : array.Data.DataType;
        KeyKind kind = KeyKinds.Of(type, out IArrowType family);
        StoredKeys stored = _stored[c];
        if (stored.Family is null) stored.Start(kind, family);
        else if (stored.Kind != kind || !ArrowTypes.SameType(stored.Family, family))
            throw new ArgumentException($"Key column {c} holds {ArrowTypes.Describe(type)}; its first batch held {ArrowTypes.Describe(stored.Family)}.");
        BatchKey.Prepare(ref _batch[c], array, kind, _emptyIsNull[c], _pins, _viewBuffers[c]);
    }

    /// <summary>Folds one column's value hashes into the rows' hashes.</summary>
    private static void Hash(in BatchKey key, Span<ulong> hashes)
    {
        for (int row = 0; row < hashes.Length; row++)
        {
            int at = key.Resolve(row);
            ulong value = at < 0 ? NullHash : key.Kind switch
            {
                KeyKind.Bytes => ByteHash.Of(key.Bytes(at)),
                KeyKind.Fixed128 => WideHash(key.Wide(at)),
                _ => key.Fixed(at),
            };
            hashes[row] = Fold(hashes[row], value);
        }

        static ulong WideHash(Int128 value) => (ulong)value ^ BitOperations.RotateLeft((ulong)(value >> 64) * K, 29);
    }

    /// <summary>A row's hash with one more column's value hash folded in (the hash before <see cref="Final"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Fold(ulong hash, ulong value) => BitOperations.RotateLeft(hash * K, 31) ^ value;

    /// <summary>The 64-bit finalizer of MurmurHash3: every input bit reaches every output bit.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Final(ulong x)
    {
        x ^= x >> 33;
        x *= 0xFF51_AFD7_ED55_8CCDUL;
        x ^= x >> 33;
        x *= 0xC4CE_B9FE_1A85_EC53UL;
        x ^= x >> 33;
        return x;
    }

    /// <summary>The global group of row <paramref name="row"/> (hash <paramref name="hash"/>), added when its key is new.</summary>
    private int FindOrAdd(ulong hash, int row)
    {
        int mask = _slotCount - 1;
        for (int slot = (int)hash & mask; ; slot = (slot + 1) & mask)
        {
            int entry = _slots.At(slot);
            if (entry == 0)
            {
                int group = _count;
                for (int c = 0; c < _stored.Length; c++) _stored[c].Append(group, in _batch[c], _batch[c].Resolve(row));
                _groupHashes.Grow(group + 1, group);
                _groupHashes.At(group) = hash;
                _localOf.Grow(group + 1, group);
                _localOf.At(group) = 0;
                _slots.At(slot) = ++_count;
                if (_count * 2 > _slotCount) Rehash();
                return group;
            }
            if (_groupHashes.At(entry - 1) == hash && Matches(entry - 1, row)) return entry - 1;
        }
    }

    private bool Matches(int group, int row)
    {
        for (int c = 0; c < _stored.Length; c++)
        {
            if (!_stored[c].Matches(group, in _batch[c], _batch[c].Resolve(row))) return false;
        }
        return true;
    }

    private void EmptyTable(int slots)
    {
        _slots.Reserve(slots);
        _slotCount = slots;
        _slots.AsSpan(0, slots).Clear();
    }

    private void Rehash()
    {
        EmptyTable(_slotCount * 2);
        int mask = _slotCount - 1;
        for (int group = 0; group < _count; group++)
        {
            int slot = (int)_groupHashes.At(group) & mask;
            while (_slots.At(slot) != 0) slot = (slot + 1) & mask;
            _slots.At(slot) = group + 1;
        }
    }
}
