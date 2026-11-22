using System.Buffers;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>
/// What the values of one place (a column, a struct field, a list's items) held in the records read: kinds of values,
/// the fields of objects in order of first appearance, the items of arrays. Resolved to an Arrow type as the reference dataframe engine infers
/// JSON types: integers and floats are Float64, strings with anything else String (other values as their text),
/// integers beyond Int64 Int128, objects structs of every field seen, arrays lists of their items' type, nulls alone
/// Null; booleans with numbers, or objects or arrays with numbers or each other, cannot be typed. Fields are ordered as
/// that engine merges struct types: an object with more fields than seen so far gives its order, the fields it lacks after
/// them; otherwise new fields come last.
/// </summary>
internal sealed class JsonShape(string path) : IDisposable
{
    [Flags]
    private enum Kinds
    {
        None = 0, Null = 1, Boolean = 2, Integer = 4, WideInteger = 8, Float = 16, String = 32, Object = 64, Array = 128,
    }

    private const int LinearFields = 12;

    private Kinds _kinds;
    private List<(byte[] Name, JsonShape Shape)> _fields = [];
    private readonly List<int> _order = []; // the fields of the object being read, in its order (each once)
    private int[] _stamp = [];               // per field: the object it was last seen in
    private int _objects;
    private ByteArrayDictionary? _index;
    private JsonShape? _items;

    /// <summary>The records taken in (at the top level).</summary>
    public long Records { get; set; }

    /// <summary>The place's dotted path, for errors ("" for the records themselves).</summary>
    public string Path => path;

    /// <summary>Takes in the value the reader stands on (leaving the reader on its last token).</summary>
    public void Observe(ref Utf8JsonReader reader)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.Null: _kinds |= Kinds.Null; break;
            case JsonTokenType.True or JsonTokenType.False: _kinds |= Kinds.Boolean; break;
            case JsonTokenType.String: _kinds |= Kinds.String; break;
            case JsonTokenType.Number: _kinds |= Number(ref reader, path); break;
            case JsonTokenType.StartArray:
                _kinds |= Kinds.Array;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray) (_items ??= new JsonShape(path + "[]")).Observe(ref reader);
                break;
            case JsonTokenType.StartObject:
                ObserveObject(ref reader);
                break;
        }
    }

    /// <summary>Takes in an object's fields (the reader on its start).</summary>
    public void ObserveObject(ref Utf8JsonReader reader)
    {
        _kinds |= Kinds.Object;
        int expected = 0, known = _fields.Count;
        int stamp = ++_objects;
        _order.Clear();
        Span<byte> scratch = stackalloc byte[256];
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            byte[]? rented = null;
            ReadOnlySpan<byte> name = reader.ValueIsEscaped
                ? Copy(in reader, scratch, out rented)
                : reader.ValueSpan;
            int index = expected < _fields.Count && name.SequenceEqual(_fields[expected].Name) ? expected : Find(name);
            if (index < 0) index = Add(name.ToArray());
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            if (index >= _stamp.Length) System.Array.Resize(ref _stamp, Math.Max(index + 1, _stamp.Length * 2));
            if (_stamp[index] != stamp)
            {
                _stamp[index] = stamp;
                _order.Add(index);
            }
            reader.Read();
            _fields[index].Shape.Observe(ref reader);
            expected = index + 1;
        }
        // More fields than known before: the object's order, then the fields it lacks (as the reference engine merges struct types).
        if (_order.Count > known && known > 0)
        {
            for (int i = 0; i < known; i++)
                if (_stamp[i] != stamp) _order.Add(i);
            Reorder(_order);
        }

        static ReadOnlySpan<byte> Copy(in Utf8JsonReader reader, Span<byte> scratch, out byte[]? rented)
        {
            rented = null;
            int max = reader.ValueSpan.Length;
            Span<byte> buffer = scratch;
            if (max > scratch.Length) buffer = rented = ArrayPool<byte>.Shared.Rent(max);
            return buffer[..reader.CopyString(buffer)];
        }
    }

    /// <summary>
    /// Takes in what <paramref name="other"/> took in (another input's records, after this one's): its kinds, its items,
    /// its fields as one object holding them in its order (so that fields order as when objects are merged).
    /// </summary>
    public void Merge(JsonShape other)
    {
        _kinds |= other._kinds;
        Records += other.Records;
        if (other._items is { } items) (_items ??= new JsonShape(path + "[]")).Merge(items);
        if (other._fields.Count == 0) return;
        int known = _fields.Count;
        int stamp = ++_objects;
        _order.Clear();
        foreach (var (name, shape) in other._fields)
        {
            int index = Find(name);
            if (index < 0) index = Add(name);
            if (index >= _stamp.Length) System.Array.Resize(ref _stamp, Math.Max(index + 1, _stamp.Length * 2));
            if (_stamp[index] != stamp)
            {
                _stamp[index] = stamp;
                _order.Add(index);
            }
            _fields[index].Shape.Merge(shape);
        }
        if (_order.Count > known && known > 0)
        {
            for (int i = 0; i < known; i++)
                if (_stamp[i] != stamp) _order.Add(i);
            Reorder(_order);
        }
    }

    // The fields in the order of their indices in order (a permutation of them all).
    private void Reorder(List<int> order)
    {
        _fields = [.. order.Select(i => _fields[i])];
        System.Array.Clear(_stamp);
        if (_index is not null)
        {
            _index.Dispose();
            _index = new ByteArrayDictionary();
            foreach (var (n, _) in _fields) _index.GetOrAdd(n);
        }
    }

    // A field seen for the first time.
    private int Add(byte[] name)
    {
        string text = Encoding.UTF8.GetString(name);
        _fields.Add((name, new JsonShape(path.Length == 0 ? text : path + "." + text)));
        _index?.GetOrAdd(name);
        if (_index is null && _fields.Count > LinearFields)
        {
            _index = new ByteArrayDictionary();
            foreach (var (n, _) in _fields) _index.GetOrAdd(n);
        }
        return _fields.Count - 1;
    }

    private int Find(ReadOnlySpan<byte> name)
    {
        if (_index is not null) return _index.IndexOf(name);
        for (int i = 0; i < _fields.Count; i++)
            if (name.SequenceEqual(_fields[i].Name)) return i;
        return -1;
    }

    private static Kinds Number(ref Utf8JsonReader reader, string path)
    {
        if (reader.TryGetInt64(out _)) return Kinds.Integer;
        ReadOnlySpan<byte> text = reader.ValueSpan;
        if (text.IndexOfAny(".eE"u8) < 0)
        {
            if (Int128.TryParse(text, System.Globalization.NumberStyles.AllowLeadingSign, null, out _)) return Kinds.WideInteger;
            throw new JsonValueException($"Column '{path}': the integer {Encoding.UTF8.GetString(text)} is beyond Int128.");
        }
        if (!reader.TryGetDouble(out double d) || !double.IsFinite(d))
            throw new JsonValueException($"Column '{path}': the number {Encoding.UTF8.GetString(text)} is beyond Float64.");
        return Kinds.Float;
    }

    /// <summary>The fields of the objects seen here (for the records: the columns), typed.</summary>
    public List<Field> Columns(bool views) => [.. _fields.Select(f => f.Shape.Field(Encoding.UTF8.GetString(f.Name), views))];

    private Field Field(string name, bool views)
    {
        Kinds k = _kinds & ~Kinds.Null;
        IArrowType type;
        IEnumerable<KeyValuePair<string, string>>? metadata = null;
        if ((k & Kinds.String) != 0) type = views ? StringViewType.Default : StringType.Default;
        else if (k == Kinds.None) type = NullType.Default;
        else if (k == Kinds.Boolean) type = BooleanType.Default;
        else if (k == Kinds.Object)
        {
            if (_fields.Count == 0)
                throw new JsonValueException($"Column '{path}' holds only empty objects: Apache Arrow (.NET) has no struct type without fields (give the column's type, a string for instance).");
            type = new StructType(Columns(views));
        }
        else if (k == Kinds.Array) type = new ListType(_items is null ? new Field("item", NullType.Default, true) : _items.Field("item", views));
        else if ((k & ~(Kinds.Integer | Kinds.WideInteger | Kinds.Float)) == 0)
        {
            if ((k & Kinds.Float) != 0) type = DoubleType.Default;
            else if ((k & Kinds.WideInteger) != 0)
            {
                type = WideIntegers.Type;
                metadata = [new(WideIntegers.MetadataKey, WideIntegers.Signed)];
            }
            else type = Int64Type.Default;
        }
        else throw new JsonValueException($"Column '{path}' holds {Describe(k)}: no type holds them all (give the column's type, a string for instance).");
        return new Field(name, type, true, metadata);
    }

    private static string Describe(Kinds k)
    {
        List<string> names = [];
        if ((k & Kinds.Boolean) != 0) names.Add("booleans");
        if ((k & (Kinds.Integer | Kinds.WideInteger | Kinds.Float)) != 0) names.Add("numbers");
        if ((k & Kinds.Object) != 0) names.Add("objects");
        if ((k & Kinds.Array) != 0) names.Add("arrays");
        return names.Count <= 1 ? string.Join("", names) : string.Join(", ", names[..^1]) + " and " + names[^1];
    }

    public void Dispose()
    {
        _index?.Dispose();
        foreach (var (_, shape) in _fields) shape.Dispose();
        _items?.Dispose();
    }
}
