using System.Text;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// The items of one list value, read as <typeparamref name="T"/> (same rules as <see cref="ArrowRow.Get{T}(int)"/>).
/// A struct over the batch: no allocation, valid while the batch is alive.
/// </summary>
public readonly struct ArrowList<T>
{
    private readonly ArrowRowNode? _items;
    private readonly int _start;

    internal ArrowList(ArrowRowNode items, int start, int count)
    {
        _items = items;
        _start = start;
        Count = count;
    }

    /// <summary>The number of items.</summary>
    public int Count { get; }

    /// <summary>The item at <paramref name="index"/>.</summary>
    public T this[int index] => Items(index).Read<T>(_start + index);

    /// <summary>True when the item at <paramref name="index"/> is null.</summary>
    public bool IsNull(int index) => Items(index).IsNull(_start + index);

    /// <summary>The bytes of a binary or string item, without copying; empty when null.</summary>
    public ReadOnlySpan<byte> GetBytes(int index) =>
        IsNull(index) ? [] : _items!.GetBytes(_start + index);

    /// <summary>The struct item at <paramref name="index"/>; false when it is null.</summary>
    public bool TryGetStruct(int index, out ArrowRow row)
    {
        ArrowRows rows = Items(index).StructRows();
        bool isNull = _items!.IsNull(_start + index);
        row = isNull ? default : new(rows, _start + index);
        return !isNull;
    }

    /// <summary>The list item at <paramref name="index"/>; false when it is null.</summary>
    public bool TryGetList<TItem>(int index, out ArrowList<TItem> list)
    {
        (int start, int count) = Items(index).ListRange(_start + index);
        bool isNull = _items!.IsNull(_start + index);
        list = isNull ? default : new(_items.ListItems(), start, count);
        return !isNull;
    }

    /// <summary>The map item at <paramref name="index"/>; false when it is null.</summary>
    public bool TryGetMap<TKey, TValue>(int index, out ArrowMap<TKey, TValue> map)
    {
        (int start, int count) = Items(index).MapRange(_start + index);
        bool isNull = _items!.IsNull(_start + index);
        if (isNull)
        {
            map = default;
            return false;
        }
        (ArrowRowNode keys, ArrowRowNode values) = _items.MapEntries();
        map = new(keys, values, start, count);
        return true;
    }

    /// <summary>Enumerates the items.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Copies the items into a new array.</summary>
    public T[] ToArray()
    {
        T[] result = new T[Count];
        for (int i = 0; i < result.Length; i++) result[i] = this[i];
        return result;
    }

    /// <summary>True when an item equals <paramref name="value"/>.</summary>
    public bool Contains(T value)
    {
        for (int i = 0; i < Count; i++)
        {
            if (EqualityComparer<T>.Default.Equals(this[i], value)) return true;
        }
        return false;
    }

    // Index of the first item equal to a string, compared as UTF-8 without decoding the items.
    internal int IndexOfUtf8(string value)
    {
        if (Count == 0) return -1;
        Span<byte> buffer = Encoding.UTF8.GetMaxByteCount(value.Length) <= 256
            ? stackalloc byte[256]
            : new byte[Encoding.UTF8.GetByteCount(value)];
        ReadOnlySpan<byte> utf8 = buffer[..Encoding.UTF8.GetBytes(value, buffer)];
        for (int i = 0; i < Count; i++)
        {
            if (!_items!.IsNull(_start + i) && _items.GetUtf8(_start + i).SequenceEqual(utf8)) return i;
        }
        return -1;
    }

    private ArrowRowNode Items(int index)
    {
        if ((uint)index >= (uint)Count) throw new ArgumentOutOfRangeException(nameof(index));
        return _items!;
    }

    /// <summary>Enumerates the items of an <see cref="ArrowList{T}"/>.</summary>
    public struct Enumerator(ArrowList<T> list)
    {
        private int _index = -1;

        public readonly T Current => list[_index];

        public bool MoveNext() => ++_index < list.Count;
    }
}

/// <summary>
/// The entries of one map value, keys read as <typeparamref name="TKey"/> and values as <typeparamref name="TValue"/>
/// (same rules as <see cref="ArrowRow.Get{T}(int)"/>). A struct over the batch: no allocation, valid while the batch
/// is alive. Lookups are linear, which suits the small maps of file metadata.
/// </summary>
public readonly struct ArrowMap<TKey, TValue>
{
    internal ArrowMap(ArrowRowNode keys, ArrowRowNode values, int start, int count)
    {
        Keys = new(keys, start, count);
        Values = new(values, start, count);
    }

    /// <summary>The number of entries.</summary>
    public int Count => Keys.Count;

    /// <summary>The keys, in entry order.</summary>
    public ArrowList<TKey> Keys { get; }

    /// <summary>The values, in entry order.</summary>
    public ArrowList<TValue> Values { get; }

    /// <summary>The value of the first entry whose key is <paramref name="key"/>.</summary>
    public bool TryGetValue(TKey key, out TValue value)
    {
        int index = IndexOf(key);
        value = index >= 0 ? Values[index] : default!;
        return index >= 0;
    }

    /// <summary>The value of the first entry whose key is <paramref name="key"/>.</summary>
    /// <exception cref="KeyNotFoundException">No entry has that key.</exception>
    public TValue this[TKey key] => TryGetValue(key, out TValue value)
        ? value
        : throw new KeyNotFoundException($"The map has no key '{key}'.");

    /// <summary>True when an entry has the key <paramref name="key"/>.</summary>
    public bool ContainsKey(TKey key) => IndexOf(key) >= 0;

    /// <summary>The index of the first entry whose key is <paramref name="key"/>, or -1.</summary>
    public int IndexOf(TKey key) =>
        typeof(TKey) == typeof(string) && key is string s ? Keys.IndexOfUtf8(s) : IndexOfKey(key);

    /// <summary>Enumerates the entries.</summary>
    public Enumerator GetEnumerator() => new(this);

    private int IndexOfKey(TKey key)
    {
        for (int i = 0; i < Count; i++)
        {
            if (EqualityComparer<TKey>.Default.Equals(Keys[i], key)) return i;
        }
        return -1;
    }

    /// <summary>Enumerates the entries of an <see cref="ArrowMap{TKey, TValue}"/>.</summary>
    public struct Enumerator(ArrowMap<TKey, TValue> map)
    {
        private int _index = -1;

        public readonly KeyValuePair<TKey, TValue> Current => new(map.Keys[_index], map.Values[_index]);

        public bool MoveNext() => ++_index < map.Count;
    }
}
