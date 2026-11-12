namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// One row of an <see cref="ArrowRows"/> view. Values are read by ordinal or by name with
/// <see cref="Get{T}(int)"/>; nested values with <see cref="GetStruct(int)"/>, <see cref="GetList{T}(int)"/> and
/// <see cref="GetMap{TKey, TValue}(int)"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Get{T}(int)"/> reads <c>bool</c>, the integer types, <see cref="Half"/>, <c>float</c>, <c>double</c>,
/// <c>decimal</c>, <see cref="System.Data.SqlTypes.SqlDecimal"/> (38 digits), <see cref="DateOnly"/>, <see cref="DateTime"/>,
/// <see cref="DateTimeOffset"/> (zoned timestamps only: a zone-less one is a wall-clock time; <see cref="DateTime"/>
/// reads it with <see cref="DateTimeKind.Unspecified"/>), <see cref="Guid"/>, <see cref="string"/>,
/// <c>byte[]</c> and <see cref="ArrowRow"/>, each also as its nullable form. Integers and floating-point values
/// widen without loss (an <c>int</c> column reads as <c>long</c>, a <c>float</c> one as <c>double</c>); a narrowing
/// or a different kind of value throws <see cref="InvalidCastException"/> naming the field. Temporal columns read
/// as their stored integer too (days, or units since the epoch).
/// </para>
/// <para>
/// A null value reads as null for nullable value types and reference types, and throws
/// <see cref="InvalidOperationException"/> for the others. Every field of a null row reads as null.
/// </para>
/// </remarks>
public readonly struct ArrowRow
{
    internal ArrowRow(ArrowRows rows, int index)
    {
        Rows = rows;
        Index = index;
    }

    /// <summary>The view this row belongs to (its fields and ordinals).</summary>
    public ArrowRows Rows { get; }

    /// <summary>The index of the row in its view.</summary>
    public int Index { get; }

    /// <summary>True when the row itself is null (a null struct).</summary>
    public bool IsNull() => Rows.IsRowNull(Index);

    /// <summary>True when the value at <paramref name="ordinal"/> is null, or the row is null.</summary>
    public bool IsNull(int ordinal) => Rows.IsRowNull(Index) || Rows.Column(ordinal).IsNull(Index);

    /// <inheritdoc cref="IsNull(int)"/>
    public bool IsNull(string name) => IsNull(Rows.Ordinal(name));

    /// <summary>Reads the value at <paramref name="ordinal"/> as <typeparamref name="T"/>.</summary>
    public T Get<T>(int ordinal)
    {
        ArrowRowNode column = Rows.Column(ordinal);
        return Rows.IsRowNull(Index) ? column.Null<T>(Index) : column.Read<T>(Index);
    }

    /// <inheritdoc cref="Get{T}(int)"/>
    public T Get<T>(string name) => Get<T>(Rows.Ordinal(name));

    /// <summary>The value at <paramref name="ordinal"/> boxed, or null; structs box as <see cref="ArrowRow"/>.
    /// Allocates; meant for values whose type is known only at run time.</summary>
    public object? GetValue(int ordinal) => IsNull(ordinal) ? null : Rows.Column(ordinal).ReadBoxed(Index);

    /// <inheritdoc cref="GetValue(int)"/>
    public object? GetValue(string name) => GetValue(Rows.Ordinal(name));

    /// <summary>The bytes of a binary, string or fixed-size binary value, without copying; empty when null.
    /// For a decimal, its unscaled value in little-endian two's complement. Valid while the batch is alive.</summary>
    public ReadOnlySpan<byte> GetBytes(int ordinal) => IsNull(ordinal) ? [] : Rows.Column(ordinal).GetBytes(Index);

    /// <inheritdoc cref="GetBytes(int)"/>
    public ReadOnlySpan<byte> GetBytes(string name) => GetBytes(Rows.Ordinal(name));

    /// <summary>The struct at <paramref name="ordinal"/>.</summary>
    /// <exception cref="InvalidOperationException">The value is null.</exception>
    public ArrowRow GetStruct(int ordinal) => TryGetStruct(ordinal, out ArrowRow row)
        ? row
        : throw Rows.Column(ordinal).NullValue(typeof(ArrowRow), Index);

    /// <inheritdoc cref="GetStruct(int)"/>
    public ArrowRow GetStruct(string name) => GetStruct(Rows.Ordinal(name));

    /// <summary>The struct at <paramref name="ordinal"/>; false when it is null.</summary>
    public bool TryGetStruct(int ordinal, out ArrowRow row)
    {
        ArrowRowNode column = Rows.Column(ordinal);
        if (IsNull(ordinal))
        {
            column.StructRows(); // still checks the type
            row = default;
            return false;
        }
        row = new(column.StructRows(), Index);
        return true;
    }

    /// <inheritdoc cref="TryGetStruct(int, out ArrowRow)"/>
    public bool TryGetStruct(string name, out ArrowRow row) => TryGetStruct(Rows.Ordinal(name), out row);

    /// <summary>The list at <paramref name="ordinal"/>, its items read as <typeparamref name="T"/>.</summary>
    /// <exception cref="InvalidOperationException">The value is null.</exception>
    public ArrowList<T> GetList<T>(int ordinal) => TryGetList(ordinal, out ArrowList<T> list)
        ? list
        : throw Rows.Column(ordinal).NullValue(typeof(ArrowList<T>), Index);

    /// <inheritdoc cref="GetList{T}(int)"/>
    public ArrowList<T> GetList<T>(string name) => GetList<T>(Rows.Ordinal(name));

    /// <summary>The list at <paramref name="ordinal"/>; false when it is null.</summary>
    public bool TryGetList<T>(int ordinal, out ArrowList<T> list)
    {
        ArrowRowNode column = Rows.Column(ordinal);
        (int start, int count) = column.ListRange(Index);
        if (IsNull(ordinal))
        {
            list = default;
            return false;
        }
        list = new(column.ListItems(), start, count);
        return true;
    }

    /// <inheritdoc cref="TryGetList{T}(int, out ArrowList{T})"/>
    public bool TryGetList<T>(string name, out ArrowList<T> list) => TryGetList(Rows.Ordinal(name), out list);

    /// <summary>The map at <paramref name="ordinal"/>, its keys and values read as <typeparamref name="TKey"/>
    /// and <typeparamref name="TValue"/>.</summary>
    /// <exception cref="InvalidOperationException">The value is null.</exception>
    public ArrowMap<TKey, TValue> GetMap<TKey, TValue>(int ordinal) => TryGetMap(ordinal, out ArrowMap<TKey, TValue> map)
        ? map
        : throw Rows.Column(ordinal).NullValue(typeof(ArrowMap<TKey, TValue>), Index);

    /// <inheritdoc cref="GetMap{TKey, TValue}(int)"/>
    public ArrowMap<TKey, TValue> GetMap<TKey, TValue>(string name) => GetMap<TKey, TValue>(Rows.Ordinal(name));

    /// <summary>The map at <paramref name="ordinal"/>; false when it is null.</summary>
    public bool TryGetMap<TKey, TValue>(int ordinal, out ArrowMap<TKey, TValue> map)
    {
        ArrowRowNode column = Rows.Column(ordinal);
        (int start, int count) = column.MapRange(Index);
        if (IsNull(ordinal))
        {
            map = default;
            return false;
        }
        (ArrowRowNode keys, ArrowRowNode values) = column.MapEntries();
        map = new(keys, values, start, count);
        return true;
    }

    /// <inheritdoc cref="TryGetMap{TKey, TValue}(int, out ArrowMap{TKey, TValue})"/>
    public bool TryGetMap<TKey, TValue>(string name, out ArrowMap<TKey, TValue> map) =>
        TryGetMap(Rows.Ordinal(name), out map);
}
