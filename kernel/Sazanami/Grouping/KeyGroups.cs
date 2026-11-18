using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;
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
    // Per key column, of the batch being grouped: each row's value (-1: null) and image, read once by the hash pass.
    private readonly NativeArray<int>[] _at;
    private readonly NativeArray<UInt128>[] _images;
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
        _at = new NativeArray<int>[keyCount];
        _images = new NativeArray<UInt128>[keyCount];
        for (int c = 0; c < keyCount; c++) (_stored[c], _viewBuffers[c], _at[c], _images[c]) = (new(), new(), new(), new());
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
            for (int c = 0; c < keys.Count; c++)
            {
                _at[c].Reserve(rows);
                _images[c].Reserve(rows);
                Hash(in _batch[c], hashes, _at[c].AsSpan(0, rows), _images[c].AsSpan(0, rows));
            }
            // The table, the groups' hashes and their batch groups held in locals: the loop runs per row, and these
            // only change when a key is added (then read again).
            Span<int> slots = _slots.AsSpan(0, _slotCount);
            Span<ulong> groupHashes = _groupHashes.Span;
            Span<int> localOf = _localOf.Span;
            for (int row = 0; row < rows; row++)
            {
                ulong hash = Final(hashes[row]);
                int mask = slots.Length - 1, group;
                for (int slot = (int)hash & mask; ; slot = (slot + 1) & mask)
                {
                    int entry = slots[slot];
                    if (entry == 0)
                    {
                        group = Add(hash, row, slot);
                        slots = _slots.AsSpan(0, _slotCount);
                        groupHashes = _groupHashes.Span;
                        localOf = _localOf.Span;
                        break;
                    }
                    if (groupHashes[entry - 1] == hash && Matches(entry - 1, row))
                    {
                        group = entry - 1;
                        break;
                    }
                }
                ref int local = ref localOf[group];
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
            for (int c = 0; c < _at.Length; c++) bytes += 4L * _at[c].Capacity + 16L * _images[c].Capacity;
            foreach (NativeArray<nint> buffers in _viewBuffers) bytes += 8L * buffers.Capacity;
            return bytes;
        }
    }

    public void Dispose()
    {
        foreach (StoredKeys stored in _stored) stored.Dispose();
        foreach (NativeArray<nint> buffers in _viewBuffers) buffers.Dispose();
        foreach (NativeArray<int> at in _at) at.Dispose();
        foreach (NativeArray<UInt128> images in _images) images.Dispose();
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

    /// <summary>
    /// Folds one column's value hashes into the rows' hashes, keeping each row's value and image: the kind and layout
    /// chosen once per column, the loop specialized by its reader.
    /// </summary>
    private static void Hash(in BatchKey key, Span<ulong> hashes, Span<int> at, Span<UInt128> images)
    {
        switch (key.Kind)
        {
            case KeyKind.Null:
                at.Fill(-1);
                images.Clear();
                for (int row = 0; row < hashes.Length; row++) hashes[row] = Fold(hashes[row], NullHash);
                break;
            case KeyKind.Fixed8: Hash<Fixed8Reader>(in key, hashes, at, images); break;
            case KeyKind.Fixed16: Hash<Fixed16Reader>(in key, hashes, at, images); break;
            case KeyKind.Fixed32: Hash<Fixed32Reader>(in key, hashes, at, images); break;
            case KeyKind.Fixed64: Hash<Fixed64Reader>(in key, hashes, at, images); break;
            case KeyKind.Fixed128: Hash<Fixed128Reader>(in key, hashes, at, images); break;
            case KeyKind.Bytes when key.Layout == BytesLayout.Views: Hash<ViewReader>(in key, hashes, at, images); break;
            case KeyKind.Bytes: Hash<BytesReader>(in key, hashes, at, images); break;
            default: Hash<NormalizedReader>(in key, hashes, at, images); break; // booleans and floats
        }
    }

    private static void Hash<TReader>(in BatchKey key, Span<ulong> hashes, Span<int> at, Span<UInt128> images) where TReader : struct, IKeyReader
    {
        if (key.Plain)
        {
            for (int row = 0; row < hashes.Length; row++)
            {
                images[row] = TReader.Read(in key, row, out ulong value);
                at[row] = row;
                hashes[row] = Fold(hashes[row], value);
            }
            return;
        }
        for (int row = 0; row < hashes.Length; row++)
        {
            int resolved = key.Resolve(row);
            ulong value = NullHash;
            images[row] = resolved < 0 ? default : TReader.Read(in key, resolved, out value);
            at[row] = resolved;
            hashes[row] = Fold(hashes[row], value);
        }
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

    /// <summary>Adds row <paramref name="row"/>'s key (hash <paramref name="hash"/>) as a new group, in empty slot <paramref name="slot"/>.</summary>
    private int Add(ulong hash, int row, int slot)
    {
        int group = _count;
        for (int c = 0; c < _stored.Length; c++) _stored[c].Append(group, in _batch[c], _at[c].At(row), _images[c].At(row));
        _groupHashes.Grow(group + 1, group);
        _groupHashes.At(group) = hash;
        _localOf.Grow(group + 1, group);
        _localOf.At(group) = 0;
        _slots.At(slot) = ++_count;
        if (_count * 2 > _slotCount) Rehash();
        return group;
    }

    /// <summary>Whether group <paramref name="group"/>'s key is row <paramref name="row"/>'s.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Matches(int group, int row)
    {
        for (int c = 0; c < _stored.Length; c++)
        {
            if (!_stored[c].Matches(group, in _batch[c], _at[c].At(row), _images[c].At(row))) return false;
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
