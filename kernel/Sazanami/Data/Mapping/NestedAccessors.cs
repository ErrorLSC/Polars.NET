using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Json;
using Kaminari.Sazanami.Marshalling;

namespace Kaminari.Sazanami.Data.Mapping;

internal sealed class ListAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement> : TypedColumnAccessor<TElement[]>,
    ITypedAccessor<TElement[]>,
    ITypedAccessor<List<TElement>>,
    ITypedAccessor<IList<TElement>>,
    ITypedAccessor<ICollection<TElement>>,
    ITypedAccessor<IReadOnlyList<TElement>>,
    ITypedAccessor<IReadOnlyCollection<TElement>>,
    ITypedAccessor<IEnumerable<TElement>>,
    ITypedAccessor<FSharpList<TElement>>,
    ITypedAccessor<IOrderedEnumerable<TElement>>
{
    private IArrowArray? _array;
    private readonly ColumnAccessor _childAccessor;
    private readonly ITypedAccessor<TElement>? _typedChildAccessor;
    private readonly int _fixedListSize;

    public ListAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType, bool foldNulls = false)
    {
        TargetType = targetType;

        IArrowType innerArrowType = arrowType switch
        {
            ListType lt => lt.ValueDataType,
            LargeListType llt => llt.ValueDataType,
            FixedSizeListType fslt => fslt.ValueDataType,
            _ => throw new InvalidOperationException("Not a list type")
        };

        if (arrowType is FixedSizeListType fixedType)
        {
            _fixedListSize = fixedType.ListSize;
        }

        // The item field itself: its metadata says how to read the values (an unsigned 128-bit integer's bits).
        Field itemField = arrowType switch
        {
            ListType lt => lt.ValueField,
            LargeListType llt => llt.ValueField,
            FixedSizeListType fslt => fslt.ValueField,
            _ => new Field("item", innerArrowType, true),
        };
        _childAccessor = ColumnAccessorFactory.Create(itemField, typeof(TElement), foldNulls);
        _typedChildAccessor = _childAccessor as ITypedAccessor<TElement>;
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = array;
        if (_array is ListArray list) _childAccessor.SetBatch(list.Values);
        else if (_array is LargeListArray largeList) _childAccessor.SetBatch(largeList.Values);
        else if (_array is FixedSizeListArray fixedList) _childAccessor.SetBatch(fixedList.Values);
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    public override TElement[] GetTypedValue(int index)
    {
        IArrowArray valuesArray;
        int offset, length;

        if (_array is ListArray list)
        {
            valuesArray = list.Values;
            offset = list.ValueOffsets[index];
            length = list.ValueOffsets[index + 1] - offset;
        }
        else if (_array is LargeListArray largeList)
        {
            valuesArray = largeList.Values;
            offset = (int)largeList.ValueOffsets[index];
            length = (int)(largeList.ValueOffsets[index + 1] - largeList.ValueOffsets[index]);
        }
        else if (_array is FixedSizeListArray fixedList)
        {
            valuesArray = fixedList.Values;
            offset = (index + fixedList.Offset) * _fixedListSize;
            length = _fixedListSize;
        }
        else
        {
            throw new InvalidDataException("Invalid List Array.");
        }

        if (length == 0) return [];

        bool hasChildNulls = valuesArray.NullCount > 0;

        // Fast-Path when child array has no nulls and exact primitive match
        if (!hasChildNulls)
        {
            if (typeof(TElement) == typeof(int) && valuesArray is Int32Array i32)
                return (TElement[])(object)i32.Values.Slice(offset, length).ToArray();

            if (typeof(TElement) == typeof(long) && valuesArray is Int64Array i64)
                return (TElement[])(object)i64.Values.Slice(offset, length).ToArray();

            if (typeof(TElement) == typeof(double) && valuesArray is DoubleArray dbl)
                return (TElement[])(object)dbl.Values.Slice(offset, length).ToArray();

            if (typeof(TElement) == typeof(float) && valuesArray is FloatArray flt)
                return (TElement[])(object)flt.Values.Slice(offset, length).ToArray();
        }

        if (typeof(TElement) == typeof(string))
        {
            var res = new string[length];
            if (valuesArray is StringViewArray sv)
            {
                for (int i = 0; i < length; i++) res[i] = sv.IsNull(offset + i) ? null! : sv.GetString(offset + i);
                return (TElement[])(object)res;
            }
            if (valuesArray is LargeStringArray lsa)
            {
                for (int i = 0; i < length; i++) res[i] = lsa.IsNull(offset + i) ? null! : lsa.GetString(offset + i);
                return (TElement[])(object)res;
            }
            if (valuesArray is StringArray sa)
            {
                for (int i = 0; i < length; i++) res[i] = sa.IsNull(offset + i) ? null! : sa.GetString(offset + i);
                return (TElement[])(object)res;
            }
        }

        var result = new TElement[length];

        if (_typedChildAccessor != null)
        {
            for (int i = 0; i < length; i++)
            {
                result[i] = _childAccessor.IsNull(offset + i)
                    ? default!
                    : _typedChildAccessor.GetTypedValue(offset + i);
            }
        }
        else
        {
            for (int i = 0; i < length; i++)
            {
                result[i] = _childAccessor.IsNull(offset + i)
                    ? default!
                    : (TElement)_childAccessor.GetValue(offset + i)!;
            }
        }

        return result;
    }

    List<TElement> ITypedAccessor<List<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IList<TElement> ITypedAccessor<IList<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    ICollection<TElement> ITypedAccessor<ICollection<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IReadOnlyList<TElement> ITypedAccessor<IReadOnlyList<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IReadOnlyCollection<TElement> ITypedAccessor<IReadOnlyCollection<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IEnumerable<TElement> ITypedAccessor<IEnumerable<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    FSharpList<TElement> ITypedAccessor<FSharpList<TElement>>.GetTypedValue(int index) => ListModule.OfArray(GetTypedValue(index));
    IOrderedEnumerable<TElement> ITypedAccessor<IOrderedEnumerable<TElement>>.GetTypedValue(int index) => new PresortedOrderedEnumerable<TElement>(GetTypedValue(index));

    public override void Dispose()
    {
        _childAccessor.Dispose();
        _array = null;
        base.Dispose();
    }

    private sealed class PresortedOrderedEnumerable<TItem>(IList<TItem> source) : IOrderedEnumerable<TItem>
    {
        public IOrderedEnumerable<TItem> CreateOrderedEnumerable<TKey>(
            Func<TItem, TKey> keySelector,
            IComparer<TKey>? comparer,
            bool descending)
        {
            return descending
                ? source.OrderByDescending(keySelector, comparer)
                : source.OrderBy(keySelector, comparer);
        }

        public IEnumerator<TItem> GetEnumerator() => source.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => source.GetEnumerator();
    }
}

internal sealed class StructAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : TypedColumnAccessor<T>, ITypedAccessor<T>
{
    private StructArray? _array;
    private readonly ColumnAccessor[] _childAccessors;
    private readonly string[]? _dictFieldNames;
    private readonly CompiledArrowRowMapper<T>? _compiledMapper;

    public StructAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType, bool foldNulls = false)
    {
        TargetType = targetType;

        if (arrowType is not StructType structType)
            throw new InvalidOperationException($"Expected StructType, got {arrowType.Name}");

        if (targetType == typeof(Dictionary<string, object?>) ||
            targetType == typeof(Dictionary<string, object>) ||
            targetType == typeof(object))
        {
            int count = structType.Fields.Count;
            _childAccessors = new ColumnAccessor[count];
            _dictFieldNames = new string[count];
            for (int i = 0; i < count; i++)
            {
                var field = structType.Fields[i];
                _dictFieldNames[i] = field.Name;
                Type netType = ArrowTypeResolver.GetNetTypeFromArrowType(field.DataType);
                _childAccessors[i] = ColumnAccessorFactory.Create(field, netType, foldNulls);
            }
        }
        else
        {
            var structSchema = new Schema(structType.Fields, null);
            _compiledMapper = ArrowRowMapper<T>.GetOrCreate(structSchema, foldNulls);
            _childAccessors = _compiledMapper.CreateAccessors();
        }
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = (StructArray)array;
        if (_compiledMapper != null)
        {
            _compiledMapper.BindBatch(_childAccessors, _array.Fields);
        }
        else
        {
            for (int i = 0; i < _childAccessors.Length; i++)
            {
                _childAccessors[i].SetBatch(_array.Fields[i]);
            }
        }
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override T GetTypedValue(int index)
    {
        if (_array!.IsNull(index)) return default!;

        if (_compiledMapper != null)
        {
            return _compiledMapper.HydrateRow(_childAccessors, index);
        }

        var dict = new Dictionary<string, object?>(_childAccessors.Length);
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            dict[_dictFieldNames![i]] = _childAccessors[i].IsNull(index) ? null : _childAccessors[i].GetValue(index);
        }
        return (T)(object)dict;
    }

    T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);

    public override void Dispose()
    {
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            _childAccessors[i]?.Dispose();
        }
        _array = null;
        base.Dispose();
    }
}

internal sealed class StructToListAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement> : TypedColumnAccessor<TElement[]>,
    ITypedAccessor<TElement[]>,
    ITypedAccessor<List<TElement>>,
    ITypedAccessor<IList<TElement>>,
    ITypedAccessor<ICollection<TElement>>,
    ITypedAccessor<IReadOnlyList<TElement>>,
    ITypedAccessor<IReadOnlyCollection<TElement>>,
    ITypedAccessor<IEnumerable<TElement>>,
    ITypedAccessor<FSharpList<TElement>>
{
    private StructArray? _array;
    private readonly ColumnAccessor[] _childAccessors;
    private readonly ITypedAccessor<TElement>?[] _typedChildAccessors;

    public StructToListAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType, bool foldNulls = false)
    {
        TargetType = targetType;

        if (arrowType is not StructType structType)
            throw new InvalidOperationException($"Expected StructType, got {arrowType.Name}");

        int count = structType.Fields.Count;
        _childAccessors = new ColumnAccessor[count];
        _typedChildAccessors = new ITypedAccessor<TElement>?[count];
        for (int i = 0; i < count; i++)
        {
            var field = structType.Fields[i];
            _childAccessors[i] = ColumnAccessorFactory.Create(field, typeof(TElement), foldNulls);
            _typedChildAccessors[i] = _childAccessors[i] as ITypedAccessor<TElement>;
        }
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = (StructArray)array;
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            _childAccessors[i].SetBatch(_array.Fields[i]);
        }
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TElement[] GetTypedValue(int index)
    {
        if (_array!.IsNull(index)) return null!;

        int count = _childAccessors.Length;
        var result = new TElement[count];
        for (int i = 0; i < count; i++)
        {
            var acc = _childAccessors[i];
            if (acc.IsNull(index))
            {
                result[i] = default!;
            }
            else if (_typedChildAccessors[i] is { } typedAcc)
            {
                result[i] = typedAcc.GetTypedValue(index);
            }
            else
            {
                result[i] = (TElement)acc.GetValue(index)!;
            }
        }
        return result;
    }

    List<TElement> ITypedAccessor<List<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IList<TElement> ITypedAccessor<IList<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    ICollection<TElement> ITypedAccessor<ICollection<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IReadOnlyList<TElement> ITypedAccessor<IReadOnlyList<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IReadOnlyCollection<TElement> ITypedAccessor<IReadOnlyCollection<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IEnumerable<TElement> ITypedAccessor<IEnumerable<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    FSharpList<TElement> ITypedAccessor<FSharpList<TElement>>.GetTypedValue(int index) => ListModule.OfArray(GetTypedValue(index));

    public override void Dispose()
    {
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            _childAccessors[i]?.Dispose();
        }
        _array = null;
        base.Dispose();
    }
}

internal sealed class FSharpOptionAccessor<T>(ColumnAccessor innerAccessor, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType) : TypedColumnAccessor<FSharpOption<T>>, ITypedAccessor<FSharpOption<T>>
{
    private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => targetType;
    public override void SetBatch(IArrowArray array) => innerAccessor.SetBatch(array);
    public override bool IsNull(int index) => innerAccessor.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override FSharpOption<T> GetTypedValue(int index)
    {
        if (innerAccessor.IsNull(index)) return null!;

        if (_typedInnerAccessor != null)
        {
            T rawValue = _typedInnerAccessor.GetTypedValue(index);
            return FSharpOption<T>.Some(rawValue);
        }

        var boxedValue = innerAccessor.GetValue(index);
        return FSharpOption<T>.Some((T)boxedValue!);
    }

    FSharpOption<T> ITypedAccessor<FSharpOption<T>>.GetTypedValue(int index) => GetTypedValue(index);

    public override void Dispose()
    {
        innerAccessor.Dispose();
        base.Dispose();
    }
}

internal sealed class FSharpValueOptionAccessor<T>(ColumnAccessor innerAccessor, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType) : TypedColumnAccessor<FSharpValueOption<T>>, ITypedAccessor<FSharpValueOption<T>>
{
    private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => targetType;
    public override void SetBatch(IArrowArray array) => innerAccessor.SetBatch(array);
    public override bool IsNull(int index) => innerAccessor.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override FSharpValueOption<T> GetTypedValue(int index)
    {
        if (innerAccessor.IsNull(index)) return FSharpValueOption<T>.ValueNone;

        if (_typedInnerAccessor != null)
        {
            T rawValue = _typedInnerAccessor.GetTypedValue(index);
            return FSharpValueOption<T>.NewValueSome(rawValue);
        }

        var boxedValue = innerAccessor.GetValue(index);
        if (boxedValue == DBNull.Value || boxedValue == null) return FSharpValueOption<T>.ValueNone;

        return FSharpValueOption<T>.NewValueSome((T)boxedValue);
    }

    FSharpValueOption<T> ITypedAccessor<FSharpValueOption<T>>.GetTypedValue(int index) => GetTypedValue(index);

    public override void Dispose()
    {
        innerAccessor.Dispose();
        base.Dispose();
    }
}

internal sealed class JsonFallbackAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] T> : TypedColumnAccessor<T>, ITypedAccessor<T>
{
    private IArrowArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(T);
    public override void SetBatch(IArrowArray array) => _array = array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    public override T GetTypedValue(int index)
    {
        var rawValue = ExtractValue(_array!, index);

        if (typeof(T) == typeof(string))
        {
            var json = rawValue is string s ? s : EncodeExtractedValue(rawValue);
            return (T)(object)json;
        }

        if (rawValue is T typedValue)
        {
            return typedValue;
        }

        if (rawValue != null)
        {
            throw new NotSupportedException(
                $"Cannot read Arrow column of type '{_array!.Data.DataType.Name}' (value type '{rawValue.GetType()}') " +
                $"as '{typeof(T)}'. Only string targets fall back to a JSON representation.");
        }

        return default!;
    }

    T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "ExtractValue only yields null, numbers, booleans, strings and DateTime, which JsonValueEncoder " +
                        "writes directly; its reflection-based fallback is never reached.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "See IL2026: extracted values never reach the reflection-based fallback.")]
    private static string EncodeExtractedValue(object? value) => JsonValueEncoder.Encode(value);

    private static object? ExtractValue(IArrowArray array, int index)
    {
        return array switch
        {
            Int32Array i32 => i32.Values[index],
            Int64Array i64 => i64.Values[index],
            DoubleArray dbl => dbl.Values[index],
            FloatArray flt => flt.Values[index],
            BooleanArray b => b.Values[index],
            StringArray s => s.GetString(index),
            StringViewArray sv => sv.GetString(index),
            Date32Array d32 => d32.GetDateTime(index),
            Date64Array d64 => d64.GetDateTime(index),
            Time64Array t64 => t64.GetDateTime(index),
            TimestampArray ts => ts.GetTimestamp(index)?.DateTime,
            _ => null
        };
    }
}

/// <summary>Which .NET types read maps, and the Arrow layouts holding them.</summary>
internal static class MapAccessorTypes
{
    /// <summary>
    /// The key and value types of a dictionary type <see cref="MapAccessor{TKey, TValue}"/> fills:
    /// <see cref="Dictionary{TKey, TValue}"/>, <see cref="IDictionary{TKey, TValue}"/>,
    /// <see cref="IReadOnlyDictionary{TKey, TValue}"/> or an F# <c>Map</c>.
    /// </summary>
    public static bool TryGetKeyValue(Type type, [NotNullWhen(true)] out Type? keyType, [NotNullWhen(true)] out Type? valueType)
    {
        keyType = valueType = null;
        if (!type.IsGenericType) return false;
        Type definition = type.GetGenericTypeDefinition();
        if (definition != typeof(Dictionary<,>) && definition != typeof(IDictionary<,>)
            && definition != typeof(IReadOnlyDictionary<,>) && definition != typeof(FSharpMap<,>))
            return false;
        Type[] arguments = type.GetGenericArguments();
        (keyType, valueType) = (arguments[0], arguments[1]);
        return true;
    }

    /// <summary>
    /// The entries of a map layout: an Arrow map, or a list (or large list) of structs of two fields (how engines
    /// without a map type hold one); null for any other type.
    /// </summary>
    public static StructType? Entries(IArrowType type) => type switch
    {
        MapType map => map.KeyValueType,
        ListType { ValueDataType: StructType { Fields.Count: 2 } entries } => entries,
        LargeListType { ValueDataType: StructType { Fields.Count: 2 } entries } => entries,
        _ => null,
    };
}

/// <summary>
/// A map column (an Arrow map, or a list of key/value structs) as dictionaries: the key and value fields found by
/// name (<c>key</c> / <c>value</c>, ignoring case), else by position. A null key is refused (a .NET dictionary has
/// none), and so are nested keys (lists, structs, maps: no value equality for a dictionary to hash); a key repeated
/// keeps its first value, as a map lookup finds it.
/// </summary>
internal sealed class MapAccessor<TKey, TValue> : TypedColumnAccessor<Dictionary<TKey, TValue>>,
    ITypedAccessor<Dictionary<TKey, TValue>>,
    ITypedAccessor<IDictionary<TKey, TValue>>,
    ITypedAccessor<IReadOnlyDictionary<TKey, TValue>>,
    ITypedAccessor<FSharpMap<TKey, TValue>>
    where TKey : notnull
{
    private IArrowArray? _array;
    private readonly int _keyField, _valueField;
    private readonly ColumnAccessor _keys, _values;
    private readonly ITypedAccessor<TKey>? _typedKeys;
    private readonly ITypedAccessor<TValue>? _typedValues;

    public MapAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType, bool foldNulls = false)
    {
        TargetType = targetType;
        StructType entries = MapAccessorTypes.Entries(arrowType) ?? throw new InvalidOperationException($"{arrowType} holds no map.");
        static int Find(IReadOnlyList<Field> fields, string name, int fallback)
        {
            for (int i = 0; i < fields.Count; i++)
            {
                if (string.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
            }
            return fallback;
        }
        _keyField = Find(entries.Fields, "key", 0);
        _valueField = Find(entries.Fields, "value", 1);
        if (_keyField == _valueField) (_keyField, _valueField) = (0, 1);
        if (entries.Fields[_keyField].DataType is NestedType keyType)
            throw new NotSupportedException(
                $"A map keyed by {ArrowTypes.Describe(keyType)} cannot be read as a .NET dictionary: nested keys have no value equality to look them up by. "
                + "Read the map's entries as a list of key / value records instead (e.g. List<(K Key, V Value)>, or the map's Entries()).");
        _keys = ColumnAccessorFactory.Create(entries.Fields[_keyField], typeof(TKey), foldNulls);
        _values = ColumnAccessorFactory.Create(entries.Fields[_valueField], typeof(TValue), foldNulls);
        _typedKeys = _keys as ITypedAccessor<TKey>;
        _typedValues = _values as ITypedAccessor<TValue>;
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = array;
        var entries = (StructArray)(array switch
        {
            ListArray list => list.Values, // a MapArray is a ListArray
            LargeListArray largeList => largeList.Values,
            _ => throw new InvalidDataException($"A map column cannot be {array.Data.DataType}."),
        });
        _keys.SetBatch(entries.Fields[_keyField]);
        _values.SetBatch(entries.Fields[_valueField]);
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    public override Dictionary<TKey, TValue> GetTypedValue(int index)
    {
        var (start, end) = _array switch
        {
            ListArray list => ((long)list.ValueOffsets[index], (long)list.ValueOffsets[index + 1]),
            LargeListArray largeList => (largeList.ValueOffsets[index], largeList.ValueOffsets[index + 1]),
            _ => throw new InvalidDataException("Invalid map array."),
        };
        var result = new Dictionary<TKey, TValue>(checked((int)(end - start)));
        for (int i = checked((int)start); i < end; i++)
        {
            if (_keys.IsNull(i)) throw new InvalidDataException($"Map entry {i - start} of row {index} has a null key, which a dictionary cannot hold.");
            TKey key = _typedKeys is not null ? _typedKeys.GetTypedValue(i) : (TKey)_keys.GetValue(i)!;
            ref TValue? slot = ref CollectionsMarshal.GetValueRefOrAddDefault(result, key, out bool repeated);
            if (!repeated)
                slot = _values.IsNull(i) ? default! : _typedValues is not null ? _typedValues.GetTypedValue(i) : (TValue)_values.GetValue(i)!;
        }
        return result;
    }

    /// <summary>The map at <paramref name="index"/> as <see cref="TargetType"/>: an F# Map for an F# Map, else a dictionary.</summary>
    public override object GetValue(int index)
    {
        if (IsNull(index)) return DBNull.Value;
        return TargetType.IsGenericType && TargetType.GetGenericTypeDefinition() == typeof(FSharpMap<,>)
            ? ((ITypedAccessor<FSharpMap<TKey, TValue>>)this).GetTypedValue(index)
            : GetTypedValue(index);
    }

    Dictionary<TKey, TValue> ITypedAccessor<Dictionary<TKey, TValue>>.GetTypedValue(int index) => GetTypedValue(index);
    IDictionary<TKey, TValue> ITypedAccessor<IDictionary<TKey, TValue>>.GetTypedValue(int index) => GetTypedValue(index);
    IReadOnlyDictionary<TKey, TValue> ITypedAccessor<IReadOnlyDictionary<TKey, TValue>>.GetTypedValue(int index) => GetTypedValue(index);
    FSharpMap<TKey, TValue> ITypedAccessor<FSharpMap<TKey, TValue>>.GetTypedValue(int index) =>
        new(GetTypedValue(index).Select(static e => Tuple.Create(e.Key, e.Value)));

    public override void Dispose()
    {
        _keys.Dispose();
        _values.Dispose();
        base.Dispose();
    }
}
