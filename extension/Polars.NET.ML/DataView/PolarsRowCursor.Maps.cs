using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;
using Kaminari.Sazanami.Data.Mapping;

namespace Polars.NET.ML.DataView;

/// <summary>Map columns: a map of numbers as a sparse vector over its key vocabulary, a map's keys or values as vectors.</summary>
internal sealed partial class PolarsRowCursor
{
    /// <summary>The array a column reads: the batch's column, or a map's keys or values as lists (the map's own offsets).</summary>
    private static IArrowArray Part(IArrowArray column, ColumnPart part) => part switch
    {
        ColumnPart.Whole => column,
        ColumnPart.MapKeys => ListOf((MapArray)column, ((MapArray)column).Keys),
        _ => ListOf((MapArray)column, ((MapArray)column).Values),
    };

    // A view over the map's buffers, owned by the batch (never disposed here).
    private static ListArray ListOf(MapArray map, IArrowArray items) =>
        new(new ListType(new Field("item", items.Data.DataType, true)), map.Length, map.ValueOffsetsBuffer, items, map.NullBitmapBuffer, map.NullCount, map.Offset);

    private abstract partial class VectorColumn
    {
        public static VectorColumn ForMap(VectorDataViewType type, MapVocabulary vocabulary)
        {
            Type item = type.ItemType.RawType;
            if (item == typeof(float)) return new SparseMapColumn<float>(vocabulary, float.NaN, missingIsStored: true);
            if (item == typeof(double)) return new SparseMapColumn<double>(vocabulary, double.NaN, missingIsStored: true);
            if (item == typeof(sbyte)) return new SparseMapColumn<sbyte>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(short)) return new SparseMapColumn<short>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(int)) return new SparseMapColumn<int>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(long)) return new SparseMapColumn<long>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(byte)) return new SparseMapColumn<byte>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(ushort)) return new SparseMapColumn<ushort>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(uint)) return new SparseMapColumn<uint>(vocabulary, 0, missingIsStored: false);
            if (item == typeof(ulong)) return new SparseMapColumn<ulong>(vocabulary, 0, missingIsStored: false);
            throw new NotSupportedException($"A map of '{item.Name}' values is no vector of numbers.");
        }

        public static VectorColumn ForMapPart(PrimitiveDataViewType item) => item switch
        {
            TextDataViewType => new TextVectorColumn(),
            BooleanDataViewType => new BooleanVectorColumn(),
            _ => Create(item.RawType) ?? throw new NotSupportedException($"A map part of '{item.RawType.Name}' items is no vector."),
        };
    }

    /// <summary>
    /// A map of numbers as a fixed-size sparse vector: each entry's value in its key's slot (keys outside the vocabulary
    /// left out, a key repeated: its first value), a null value as the missing value where one is stored (NaN),
    /// otherwise left out (0); a null map all zero.
    /// </summary>
    private sealed class SparseMapColumn<T>(MapVocabulary vocabulary, T missingValue, bool missingIsStored) : VectorColumn
        where T : unmanaged, IEquatable<T>
    {
        private MapArray? _maps;
        private PrimitiveArray<T>? _values;
        private Func<int, string?>? _keys;
        // Per row: the slots taken (by a stamp of the row, so nothing is cleared between rows) and the entries found.
        private readonly int[] _taken = new int[vocabulary.Keys.Length];
        private int _stamp;
        private readonly List<int> _slots = [];
        private readonly List<T> _found = [];

        public override void SetBatch(IArrowArray array)
        {
            _maps = (MapArray)array;
            _values = _maps.Values as PrimitiveArray<T>
                ?? throw new NotSupportedException($"Map value array '{_maps.Values.GetType().Name}' does not hold '{typeof(T).Name}' values.");
            _keys = MapKeyText.For(_maps.Keys);
        }

        public override ValueGetter<TValue> CreateGetter<TValue>(PolarsRowCursor cursor)
            => (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<T>>)((ref value) => Fill(cursor._batchRowIndex, ref value));

        private void Fill(int row, ref VBuffer<T> value)
        {
            int size = vocabulary.Keys.Length;
            MapArray maps = _maps!;
            _slots.Clear();
            _found.Clear();
            if (!maps.IsNull(row))
            {
                if (++_stamp == int.MaxValue)
                {
                    System.Array.Clear(_taken);
                    _stamp = 1;
                }
                ReadOnlySpan<int> offsets = maps.ValueOffsets;
                PrimitiveArray<T> values = _values!;
                for (int entry = offsets[row]; entry < offsets[row + 1]; entry++)
                {
                    if (_keys!(entry) is not { } key || !vocabulary.TryGetSlot(key, out int slot) || _taken[slot] == _stamp) continue;
                    _taken[slot] = _stamp;
                    if (values.IsNull(entry))
                    {
                        if (!missingIsStored) continue;
                        _slots.Add(slot);
                        _found.Add(missingValue);
                    }
                    else
                    {
                        _slots.Add(slot);
                        _found.Add(values.Values[entry]);
                    }
                }
            }

            Span<int> slots = CollectionsMarshal.AsSpan(_slots);
            Span<T> found = CollectionsMarshal.AsSpan(_found);
            slots.Sort(found);
            // Every slot taken: a dense vector (its values in slot order); else sparse, the slots as its indices.
            var editor = VBufferEditor.Create(ref value, size, slots.Length);
            found.CopyTo(editor.Values);
            if (slots.Length < size) slots.CopyTo(editor.Indices);
            value = editor.Commit();
        }
    }

    /// <summary>A list of text as a variable-length vector (null items empty text, a null list an empty vector).</summary>
    private sealed class TextVectorColumn : VectorColumn
    {
        private ListArray? _lists;
        private readonly StringViewAccessor _items = new();
        private char[] _arena = new char[65536];
        private int _arenaOffset;

        public override void SetBatch(IArrowArray array)
        {
            _lists = (ListArray)array;
            _items.SetBatch(_lists.Values);
        }

        public override ValueGetter<TValue> CreateGetter<TValue>(PolarsRowCursor cursor)
            => (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<ReadOnlyMemory<char>>>)((ref value) => Fill(cursor._batchRowIndex, ref value));

        private void Fill(int row, ref VBuffer<ReadOnlyMemory<char>> value)
        {
            ListArray lists = _lists!;
            int start = lists.IsNull(row) ? 0 : lists.ValueOffsets[row];
            int length = lists.IsNull(row) ? 0 : lists.ValueOffsets[row + 1] - start;
            var editor = VBufferEditor.Create(ref value, length);
            for (int i = 0; i < length; i++) editor.Values[i] = Text(start + i);
            value = editor.Commit();
        }

        // Decoded into an arena that is replaced, not reused, when full: memory handed out stays valid.
        private ReadOnlyMemory<char> Text(int item)
        {
            if (_items.IsNullFast(item)) return default;
            ReadOnlySpan<byte> utf8 = _items.GetBytesSpan(item);
            if (utf8.IsEmpty) return ReadOnlyMemory<char>.Empty;
            int maxChars = System.Text.Encoding.UTF8.GetMaxCharCount(utf8.Length);
            if (_arenaOffset + maxChars > _arena.Length)
            {
                _arena = new char[Math.Max(65536, maxChars)];
                _arenaOffset = 0;
            }
            int chars = System.Text.Encoding.UTF8.GetChars(utf8, _arena.AsSpan(_arenaOffset));
            var text = new ReadOnlyMemory<char>(_arena, _arenaOffset, chars);
            _arenaOffset += chars;
            return text;
        }
    }

    /// <summary>A list of booleans as a variable-length vector (null items false, a null list an empty vector).</summary>
    private sealed class BooleanVectorColumn : VectorColumn
    {
        private ListArray? _lists;

        public override void SetBatch(IArrowArray array) => _lists = (ListArray)array;

        public override ValueGetter<TValue> CreateGetter<TValue>(PolarsRowCursor cursor)
            => (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<bool>>)((ref value) => Fill(cursor._batchRowIndex, ref value));

        private void Fill(int row, ref VBuffer<bool> value)
        {
            ListArray lists = _lists!;
            var items = (BooleanArray)lists.Values;
            int start = lists.IsNull(row) ? 0 : lists.ValueOffsets[row];
            int length = lists.IsNull(row) ? 0 : lists.ValueOffsets[row + 1] - start;
            var editor = VBufferEditor.Create(ref value, length);
            for (int i = 0; i < length; i++) editor.Values[i] = items.GetValue(start + i) ?? false;
            value = editor.Commit();
        }
    }
}
