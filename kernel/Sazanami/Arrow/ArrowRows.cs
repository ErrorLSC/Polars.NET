using Apache.Arrow;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// A row view over a <see cref="RecordBatch"/> or a <see cref="StructArray"/>: typed, by-ordinal or by-name access
/// to each row's values, with nested structs, lists and maps, and no allocation per row.
/// </summary>
/// <remarks>
/// <para>
/// One <see cref="ArrowRows"/> is built per batch (or struct array); the layouts of nested structs, lists and maps
/// are built on first use and cached, so reading rows afterwards allocates nothing beyond the values that are
/// themselves objects (<see cref="string"/>, <c>byte[]</c>). <see cref="ArrowRow"/>, <see cref="ArrowList{T}"/>
/// and <see cref="ArrowMap{TKey, TValue}"/> are small structs that stay valid as long as the batch is alive; they
/// are not <c>ref struct</c>s, so they can be used in async methods.
/// </para>
/// <para>
/// Ordinals are cheaper than names: resolve them once with <see cref="Ordinal"/> (or <see cref="OrdinalOfId"/>)
/// and read with the ordinal overloads. Nested layouts are shared by all rows, so an ordinal resolved on one
/// nested row (<c>row.GetStruct(x).Rows.Ordinal("y")</c>) is valid for the same struct in every row.
/// </para>
/// <para>
/// Field ids: when built with a <c>fieldIdKey</c>, the integer stored under that key in each field's Arrow
/// metadata is the field id (for example <c>PARQUET:field_id</c>, the key pyarrow writes for Parquet field ids).
/// </para>
/// <para>
/// The view is meant for one reader at a time; nested layouts are created lazily without locks.
/// </para>
/// </remarks>
public sealed class ArrowRows
{
    private readonly ArrowRowNode[] _columns;
    private readonly IArrowArray? _validity;
    private readonly Dictionary<string, int> _ordinals;
    private Dictionary<int, int>? _ordinalsById;

    internal ArrowRows(IReadOnlyList<Field> fields, IReadOnlyList<IArrowArray> columns, int count,
        IArrowArray? validity, string? fieldIdKey, string path)
    {
        if (fields.Count != columns.Count)
            throw new ArgumentException($"{fields.Count} fields but {columns.Count} columns.");
        _columns = new ArrowRowNode[columns.Count];
        _ordinals = new(columns.Count, StringComparer.Ordinal);
        for (int i = 0; i < _columns.Length; i++)
        {
            Field field = fields[i];
            _columns[i] = new ArrowRowNode(columns[i], field,
                path.Length == 0 ? field.Name : $"{path}.{field.Name}", fieldIdKey);
            // A name that appears twice cannot be looked up by name (-1 marks it ambiguous).
            if (!_ordinals.TryAdd(field.Name, i)) _ordinals[field.Name] = -1;
        }
        Fields = fields;
        Count = count;
        FieldIdKey = fieldIdKey;
        _validity = validity;
    }

    /// <summary>A view over the rows of <paramref name="batch"/>.</summary>
    /// <param name="fieldIdKey">The field-metadata key that holds field ids, or null when ids are not used.</param>
    public static ArrowRows Over(RecordBatch batch, string? fieldIdKey = null)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return new([.. batch.Schema.FieldsList], [.. batch.Arrays], batch.Length, null, fieldIdKey, "");
    }

    /// <summary>A view over the rows of <paramref name="array"/>; a null struct is a null row.</summary>
    /// <param name="fieldIdKey">The field-metadata key that holds field ids, or null when ids are not used.</param>
    public static ArrowRows Over(StructArray array, string? fieldIdKey = null)
    {
        ArgumentNullException.ThrowIfNull(array);
        return new(((Apache.Arrow.Types.StructType)array.Data.DataType).Fields, array.Fields, array.Length, array,
            fieldIdKey, "");
    }

    /// <summary>The number of rows.</summary>
    public int Count { get; }

    /// <summary>The fields, by ordinal.</summary>
    public IReadOnlyList<Field> Fields { get; }

    /// <summary>The number of fields.</summary>
    public int FieldCount => _columns.Length;

    /// <summary>The field-metadata key that holds field ids, or null.</summary>
    public string? FieldIdKey { get; }

    /// <summary>The row at <paramref name="index"/>.</summary>
    public ArrowRow this[int index]
    {
        get
        {
            if ((uint)index >= (uint)Count) throw new ArgumentOutOfRangeException(nameof(index));
            return new(this, index);
        }
    }

    /// <summary>The ordinal of the field named <paramref name="name"/>.</summary>
    /// <exception cref="KeyNotFoundException">No field, or more than one, has that name.</exception>
    public int Ordinal(string name) => TryGetOrdinal(name, out int ordinal)
        ? ordinal
        : throw new KeyNotFoundException(_ordinals.ContainsKey(name)
            ? $"More than one field is named '{name}'."
            : $"No field is named '{name}'.");

    /// <summary>Looks up the ordinal of the field named <paramref name="name"/>; false when there is none or the
    /// name is ambiguous.</summary>
    public bool TryGetOrdinal(string name, out int ordinal) =>
        _ordinals.TryGetValue(name, out ordinal) && ordinal >= 0;

    /// <summary>The ordinal of the field whose id is <paramref name="fieldId"/>.</summary>
    /// <exception cref="KeyNotFoundException">No field has that id.</exception>
    /// <exception cref="InvalidOperationException">The view was built without a field-id key.</exception>
    public int OrdinalOfId(int fieldId) => TryGetOrdinalOfId(fieldId, out int ordinal)
        ? ordinal
        : throw new KeyNotFoundException($"No field has id {fieldId}.");

    /// <summary>Looks up the ordinal of the field whose id is <paramref name="fieldId"/>.</summary>
    /// <exception cref="InvalidOperationException">The view was built without a field-id key.</exception>
    public bool TryGetOrdinalOfId(int fieldId, out int ordinal) =>
        (_ordinalsById ??= BuildIdMap()).TryGetValue(fieldId, out ordinal);

    /// <summary>The id of the field at <paramref name="ordinal"/>, or null when it has none.</summary>
    public int? FieldId(int ordinal) => _columns[ordinal].FieldId;

    /// <summary>Enumerates the rows.</summary>
    public Enumerator GetEnumerator() => new(this);

    internal ArrowRowNode Column(int ordinal) => _columns[ordinal];

    internal bool IsRowNull(int index) => _validity is not null && _validity.IsNull(index);

    private Dictionary<int, int> BuildIdMap()
    {
        if (FieldIdKey is null)
            throw new InvalidOperationException("This row view was built without a field-id metadata key.");
        Dictionary<int, int> map = new(_columns.Length);
        for (int i = 0; i < _columns.Length; i++)
        {
            if (_columns[i].FieldId is int id && !map.TryAdd(id, i))
                throw new InvalidOperationException(
                    $"Fields '{Fields[map[id]].Name}' and '{Fields[i].Name}' have the same id {id}.");
        }
        return map;
    }

    /// <summary>Enumerates the rows of an <see cref="ArrowRows"/>.</summary>
    public struct Enumerator(ArrowRows rows)
    {
        private int _index = -1;

        public readonly ArrowRow Current => new(rows, _index);

        public bool MoveNext() => ++_index < rows.Count;
    }
}
