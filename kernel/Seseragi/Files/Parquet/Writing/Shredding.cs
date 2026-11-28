using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Array = System.Array;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>One step on the way from a top-level column to a leaf: what a node of the path does with a row.</summary>
internal abstract record ShredStep(bool Optional, string Path);

/// <summary>A struct: null (when optional) ends the path, else its child.</summary>
internal sealed record StructStep(int Child, bool Optional, string Path) : ShredStep(Optional, Path);

/// <summary>How a list-like array lays out its items.</summary>
internal enum ListLayout { List, LargeList, FixedSize, View, LargeView }

/// <summary>A list (or map: a list of key-value structs): null or empty ends the path, else each item, repeated at <paramref name="RepLevel"/>.</summary>
internal sealed record ListStep(ListLayout Layout, int FixedSize, int RepLevel, bool Optional, string Path) : ShredStep(Optional, Path);

/// <summary>A layout that only maps rows to the values below it (a dictionary of nested values, run-end encoding): no level of its own.</summary>
internal sealed record RemapStep(bool Optional, string Path) : ShredStep(Optional, Path);

/// <summary>The leaf: a value, or null.</summary>
internal sealed record LeafStep(bool Optional, string Path) : ShredStep(Optional, Path);

/// <summary>
/// Turns rows of a nested Arrow column into the definition and repetition levels of one leaf (Dremel), and the
/// positions in the leaf array of the values present. Children are taken from the parents' raw array data (a parent's
/// offset applies to them; nulls of a parent hide whatever its children hold). A null where the schema says
/// non-nullable is refused (<see cref="Validate"/> runs the same walk without output, before anything is written).
/// The levels and positions are in native memory, freed by <see cref="Dispose"/>.
/// </summary>
internal sealed class Shredder(ShredStep[] steps) : IDisposable
{
    // The array each step walks is the same for every row of a column (a struct's child, a list's items, a
    // dictionary's values): built once per shredding.
    private readonly IArrowArray?[] _arrays = new IArrowArray?[steps.Length];
    private readonly NativeArray<byte> _def = new(Initial), _rep = new(Initial); // a byte per level (ColumnWriter.MaxLevel)
    private readonly NativeArray<int> _indices = new(Initial);
    private bool _emit;

    // The bulk walk (ShredBulk): each slot's levels so far and its position in the step's array (-1 once the path
    // ended above), and the next step's slots a list expands into.
    private readonly bool _bulk = Array.TrueForAll(steps, static step => step switch
    {
        StructStep or LeafStep => true,
        ListStep list => list.Layout is ListLayout.List or ListLayout.LargeList or ListLayout.FixedSize,
        _ => false,
    });
    private NativeArray<byte> _slotDef = new(), _slotRep = new(), _nextDef = new(), _nextRep = new();
    private NativeArray<int> _slotAt = new(), _nextAt = new();

    /// <summary>The levels and positions the buffers hold at first (4 KiB of levels each, 16 KiB of positions).</summary>
    private const int Initial = 4096;

    public ReadOnlySpan<byte> Def => _def.AsSpan(0, Levels);
    public ReadOnlySpan<byte> Rep => _rep.AsSpan(0, Levels);
    public ReadOnlySpan<int> Indices => _indices.AsSpan(0, Values);
    public int Levels { get; private set; }
    public int Values { get; private set; }

    /// <summary>Whether rows are walked one by one even where the bulk walk could take them (for tests: its oracle).</summary>
    internal bool PerRow { get; init; }

    /// <summary>The leaf array the indices point into (null when no value is present).</summary>
    public IArrowArray? Leaf { get; private set; }

    /// <summary>Shreds the rows <paramref name="offset"/> .. +<paramref name="count"/> of the top-level <paramref name="column"/>.</summary>
    public void Shred(IArrowArray column, int offset, int count)
    {
        Levels = Values = 0;
        Leaf = null;
        _emit = true;
        if (_bulk && !PerRow) ShredBulk(column, offset, count);
        else
            for (int i = offset; i < offset + count; i++) Walk(column, i, 0, 0, 0);
        Array.Clear(_arrays);
    }

    /// <summary>
    /// Walks every row without output: throws where a non-nullable node holds a null. Skipped when no non-nullable
    /// node's array holds a null at all (the walk could only meet nulls where the schema allows them).
    /// </summary>
    public void Validate(IArrowArray column)
    {
        if (!MayViolate(column.Data, 0)) return;
        _emit = false;
        for (int i = 0; i < column.Length; i++) Walk(column, i, 0, 0, 0);
        Array.Clear(_arrays);
    }

    private void Walk(IArrowArray array, int i, int s, uint def, uint rep)
    {
        ShredStep step = steps[s];
        switch (step)
        {
            case StructStep st:
                if (array.IsNull(i))
                {
                    Null(st, def, rep);
                    return;
                }
                Walk(Child(s + 1, array.Data, st.Child), array.Data.Offset + i, s + 1, def + (st.Optional ? 1u : 0u), rep);
                return;
            case ListStep list:
            {
                if (array.IsNull(i))
                {
                    Null(list, def, rep);
                    return;
                }
                uint present = def + (list.Optional ? 1u : 0u);
                var (start, length) = Items(array.Data, i, list);
                if (length == 0)
                {
                    Emit(present, rep);
                    return;
                }
                IArrowArray items = Child(s + 1, array.Data, 0);
                for (long j = 0; j < length; j++)
                    Walk(items, checked((int)(start + j)), s + 1, present + 1, j == 0 ? rep : (uint)list.RepLevel);
                return;
            }
            case RemapStep remap:
                if (array is DictionaryArray dictionary)
                {
                    if (dictionary.IsNull(i))
                    {
                        Null(remap, def, rep);
                        return;
                    }
                    Walk(dictionary.Dictionary, ArrowValues.Index(dictionary, i), s + 1, def, rep);
                }
                else
                {
                    Walk(Child(s + 1, array.Data, 1), RunOf(array.Data, array.Data.Offset + i), s + 1, def, rep);
                }
                return;
            default:
                if (ArrowValues.IsNull(array, i))
                {
                    Null(step, def, rep);
                    return;
                }
                Emit(def + (step.Optional ? 1u : 0u), rep);
                if (_emit)
                {
                    if (Values == _indices.Capacity) GrowIndices();
                    _indices.At(Values++) = i;
                    Leaf = array;
                }
                return;
        }
    }

    /// <summary>
    /// Whether a non-nullable node from step <paramref name="s"/> down holds a null anywhere in its array (a
    /// dictionary: among its indices or the values they point at; an unknown null count counts as some).
    /// </summary>
    private bool MayViolate(ArrayData data, int s)
    {
        ShredStep step = steps[s];
        if (!step.Optional && (data.NullCount != 0 || data.Dictionary is { NullCount: not 0 })) return true;
        return step switch
        {
            StructStep st => MayViolate(data.Children[st.Child], s + 1),
            ListStep => MayViolate(data.Children[0], s + 1),
            RemapStep => MayViolate(data.Dictionary ?? data.Children[1], s + 1),
            _ => false,
        };
    }

    private void Null(ShredStep step, uint def, uint rep)
    {
        if (!step.Optional) throw new ArgumentException($"'{step.Path}' is not nullable but holds a null.");
        Emit(def, rep);
    }

    private void Emit(uint def, uint rep)
    {
        if (!_emit) return;
        int at = Levels;
        // The pool may hand the two arrays different capacities: each is checked.
        if (at == _def.Capacity || at == _rep.Capacity) GrowLevels();
        _def.At(at) = (byte)def;
        _rep.At(at) = (byte)rep;
        Levels = at + 1;
    }

    // Growth stays out of line: Emit and the leaf's append are inlined into the walk, which must stay small.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowLevels()
    {
        _def.Grow(Levels + 1, Levels);
        _rep.Grow(Levels + 1, Levels);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowIndices() => _indices.Grow(Values + 1, Values);

    public void Dispose()
    {
        _def.Dispose();
        _rep.Dispose();
        _indices.Dispose();
        _slotDef.Dispose();
        _slotRep.Dispose();
        _slotAt.Dispose();
        _nextDef.Dispose();
        _nextRep.Dispose();
        _nextAt.Dispose();
    }

    /// <summary>
    /// <see cref="Shred"/> step by step instead of row by row (structs, lists laid out by offsets or a fixed size, and
    /// the leaf): every row's slot goes through a step at once, a struct turning each slot into its child's (or ending
    /// it at a null), a list into its items' (or ending it, null or empty), the leaf emitting the levels and the
    /// positions of the values present; the same levels and positions as <see cref="Walk"/>, in the same order.
    /// </summary>
    private void ShredBulk(IArrowArray column, int offset, int count)
    {
        _slotDef.Reserve(count);
        _slotRep.Reserve(count);
        _slotAt.Reserve(count);
        _slotDef.AsSpan(0, count).Clear();
        _slotRep.AsSpan(0, count).Clear();
        Span<int> first = _slotAt.AsSpan(0, count);
        for (int i = 0; i < count; i++) first[i] = offset + i;
        int slots = count;
        IArrowArray array = column;
        for (int s = 0; ; s++)
        {
            ShredStep step = steps[s];
            ArrayData data = array.Data;
            Span<byte> def = _slotDef.AsSpan(0, slots);
            Span<int> at = _slotAt.AsSpan(0, slots);
            switch (step)
            {
                case StructStep st:
                {
                    ReadOnlySpan<byte> validity = Validity(data);
                    byte present = st.Optional ? (byte)1 : (byte)0;
                    for (int k = 0; k < slots; k++)
                    {
                        int p = at[k];
                        if (p < 0) continue;
                        if (!validity.IsEmpty && !BitUtility.GetBit(validity, data.Offset + p))
                        {
                            if (!st.Optional) Refuse(st);
                            at[k] = -1;
                            continue;
                        }
                        def[k] += present;
                        at[k] = data.Offset + p;
                    }
                    array = Child(s + 1, data, st.Child);
                    continue;
                }
                case ListStep list:
                    slots = ExpandList(data, list, slots);
                    array = Child(s + 1, data, 0);
                    continue;
                default:
                    EmitLeaf(array, step, slots);
                    return;
            }
        }
    }

    /// <summary>
    /// The slots a list step turns its slots into: a null or empty list ends its slot (its levels as they are, plus the
    /// list's own when empty), a list of items gives each its slot, repeated at the list's level after the first.
    /// </summary>
    private int ExpandList(ArrayData data, ListStep list, int slots)
    {
        ReadOnlySpan<byte> validity = Validity(data);
        ReadOnlySpan<int> offsets32 = list.Layout == ListLayout.List ? Cast<int>(data.Buffers[1].Span) : default;
        ReadOnlySpan<long> offsets64 = list.Layout == ListLayout.LargeList ? Cast<long>(data.Buffers[1].Span) : default;
        Span<byte> def = _slotDef.AsSpan(0, slots), rep = _slotRep.AsSpan(0, slots);
        Span<int> at = _slotAt.AsSpan(0, slots);
        // The slots after the step: one per ended slot, one per item.
        long total = 0;
        for (int k = 0; k < slots; k++)
        {
            int p = at[k];
            if (p < 0 || !validity.IsEmpty && !BitUtility.GetBit(validity, data.Offset + p))
            {
                total++;
                continue;
            }
            long length = Range(list, offsets32, offsets64, data.Offset + p).Length;
            total += length == 0 ? 1 : length;
        }
        int next = checked((int)total);
        _nextDef.Reserve(next);
        _nextRep.Reserve(next);
        _nextAt.Reserve(next);
        Span<byte> nextDef = _nextDef.AsSpan(0, next), nextRep = _nextRep.AsSpan(0, next);
        Span<int> nextAt = _nextAt.AsSpan(0, next);
        byte optional = list.Optional ? (byte)1 : (byte)0, repeated = (byte)list.RepLevel;
        int o = 0;
        for (int k = 0; k < slots; k++)
        {
            int p = at[k];
            byte d = def[k], r = rep[k];
            if (p < 0)
            {
                (nextDef[o], nextRep[o], nextAt[o]) = (d, r, -1);
                o++;
                continue;
            }
            int row = data.Offset + p;
            if (!validity.IsEmpty && !BitUtility.GetBit(validity, row))
            {
                if (!list.Optional) Refuse(list);
                (nextDef[o], nextRep[o], nextAt[o]) = (d, r, -1);
                o++;
                continue;
            }
            byte present = (byte)(d + optional);
            var (start, length) = Range(list, offsets32, offsets64, row);
            if (length == 0)
            {
                (nextDef[o], nextRep[o], nextAt[o]) = (present, r, -1);
                o++;
                continue;
            }
            int n = (int)length;
            nextDef.Slice(o, n).Fill((byte)(present + 1));
            nextRep[o] = r;
            nextRep.Slice(o + 1, n - 1).Fill(repeated);
            int item = checked((int)start);
            Span<int> items = nextAt.Slice(o, n);
            for (int j = 0; j < n; j++) items[j] = item + j;
            o += n;
        }
        // The next step's slots become the current ones.
        (_slotDef, _nextDef) = (_nextDef, _slotDef);
        (_slotRep, _nextRep) = (_nextRep, _slotRep);
        (_slotAt, _nextAt) = (_nextAt, _slotAt);
        return next;
    }

    /// <summary>The first item and the item count of list row <paramref name="row"/> (by its offsets or its fixed size).</summary>
    private static (long Start, long Length) Range(ListStep list, ReadOnlySpan<int> offsets32, ReadOnlySpan<long> offsets64, int row) => list.Layout switch
    {
        ListLayout.List => (offsets32[row], offsets32[row + 1] - offsets32[row]),
        ListLayout.LargeList => (offsets64[row], offsets64[row + 1] - offsets64[row]),
        _ => ((long)row * list.FixedSize, list.FixedSize),
    };

    /// <summary>The leaf step: every slot's levels (a value present one level more), the positions of the values present.</summary>
    private void EmitLeaf(IArrowArray leaf, ShredStep step, int slots)
    {
        _def.Reserve(slots);
        _rep.Reserve(slots);
        _indices.Reserve(slots);
        ReadOnlySpan<byte> def = _slotDef.AsSpan(0, slots), rep = _slotRep.AsSpan(0, slots);
        ReadOnlySpan<int> at = _slotAt.AsSpan(0, slots);
        Span<byte> outDef = _def.AsSpan(0, slots);
        Span<int> indices = _indices.AsSpan(0, slots);
        rep.CopyTo(_rep.AsSpan(0, slots));
        // Nulls read from the validity bits, except where a value's nullness is more than its bit (a dictionary's
        // null values, unions and the like): those ask the array.
        bool plain = leaf is not DictionaryArray && leaf.Data.DataType.TypeId is not (ArrowTypeId.Union or ArrowTypeId.Null or ArrowTypeId.RunEndEncoded);
        ReadOnlySpan<byte> validity = plain ? Validity(leaf.Data) : default;
        byte present = step.Optional ? (byte)1 : (byte)0;
        int values = 0, dataOffset = leaf.Data.Offset;
        for (int k = 0; k < slots; k++)
        {
            int p = at[k];
            byte d = def[k];
            if (p < 0)
            {
                outDef[k] = d;
                continue;
            }
            bool isNull = plain ? !validity.IsEmpty && !BitUtility.GetBit(validity, dataOffset + p) : ArrowValues.IsNull(leaf, p);
            if (isNull)
            {
                if (!step.Optional) Refuse(step);
                outDef[k] = d;
                continue;
            }
            outDef[k] = (byte)(d + present);
            indices[values++] = p;
        }
        Levels = slots;
        Values = values;
        Leaf = values > 0 ? leaf : null;
    }

    /// <summary>The validity bits of an array that may hold nulls; empty when it holds none.</summary>
    private static ReadOnlySpan<byte> Validity(ArrayData data) =>
        data.NullCount != 0 && data.Buffers.Length > 0 ? data.Buffers[0].Span : default;

    private static void Refuse(ShredStep step) => throw new ArgumentException($"'{step.Path}' is not nullable but holds a null.");

    /// <summary>The array of step <paramref name="s"/>: a child of its parent's, built once per shredding (from the raw data: the parent's offset applies to it).</summary>
    private IArrowArray Child(int s, ArrayData parent, int index) => _arrays[s] ??= ArrowArrayFactory.BuildArray(parent.Children[index]);

    /// <summary>The first item and the item count of list row <paramref name="i"/>.</summary>
    private static (long Start, long Length) Items(ArrayData data, int i, ListStep list)
    {
        int row = data.Offset + i;
        return list.Layout switch
        {
            ListLayout.FixedSize => ((long)row * list.FixedSize, list.FixedSize),
            ListLayout.List => (Cast<int>(data.Buffers[1].Span)[row], Cast<int>(data.Buffers[1].Span)[row + 1] - Cast<int>(data.Buffers[1].Span)[row]),
            ListLayout.LargeList => (Cast<long>(data.Buffers[1].Span)[row], Cast<long>(data.Buffers[1].Span)[row + 1] - Cast<long>(data.Buffers[1].Span)[row]),
            ListLayout.View => (Cast<int>(data.Buffers[1].Span)[row], Cast<int>(data.Buffers[2].Span)[row]),
            _ => (Cast<long>(data.Buffers[1].Span)[row], Cast<long>(data.Buffers[2].Span)[row]),
        };
    }

    /// <summary>The run holding logical row <paramref name="row"/> of a run-end encoded array (the first run end above it).</summary>
    private static int RunOf(ArrayData data, int row)
    {
        ArrayData ends = data.Children[0];
        int lo = 0, hi = ends.Length - 1;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (RunEnd(ends, mid) > row) hi = mid;
            else lo = mid + 1;
        }
        return lo;
    }

    private static ReadOnlySpan<T> Cast<T>(ReadOnlySpan<byte> bytes) where T : struct => MemoryMarshal.Cast<byte, T>(bytes);

    private static long RunEnd(ArrayData ends, int k) => ends.DataType.TypeId switch
    {
        Apache.Arrow.Types.ArrowTypeId.Int16 => Cast<short>(ends.Buffers[1].Span)[ends.Offset + k],
        Apache.Arrow.Types.ArrowTypeId.Int32 => Cast<int>(ends.Buffers[1].Span)[ends.Offset + k],
        _ => Cast<long>(ends.Buffers[1].Span)[ends.Offset + k],
    };
}
