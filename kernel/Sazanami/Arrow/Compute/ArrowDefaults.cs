using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>
/// One value of an Arrow type, as a constant column repeats it (<see cref="Builders.ConstantArrays.Of(ArrowScalar, int)"/>).
/// <see cref="Value"/> is null for a null, else by the type: a <see cref="bool"/> for booleans; the little-endian bytes
/// of one slot (<see cref="byte"/>[] of the type's width) for fixed-width types (numbers, decimals, dates, times,
/// timestamps, durations, fixed-size binaries); the bytes (UTF-8 for strings) for strings and binaries of any layout;
/// the fields' values (<see cref="ArrowScalar"/>?[], in the struct's order) for a struct; the value's
/// <see cref="ArrowScalar"/> (of the value type) for a dictionary.
/// </summary>
internal sealed record ArrowScalar(IArrowType Type, object? Value)
{
    public bool IsNull => Value is null;
}

/// <summary>
/// The values of fields a source lacks (columns or struct fields added later: a table's defaults), at one place of
/// a type: this field's own, and its fields' below it (struct fields by name; a list's elements are its own place,
/// a map's key and value are fields named <c>key</c> and <c>value</c>). A field whose default is declared may be missing
/// from a source (<see cref="ArrowConversionRules.InsertMissingFields"/> or not), and reads as that value.
/// </summary>
internal sealed class ArrowDefaults
{
    private Dictionary<string, ArrowDefaults>? _fields;

    /// <summary>Whether this field has a default (null included); an undeclared one only holds its fields'.</summary>
    public bool Declared { get; private set; }

    /// <summary>The default of a field that is not a struct (null: a null); unused for a struct.</summary>
    public ArrowScalar? Value { get; private set; }

    /// <summary>Whether the field is a struct whose default is not null: a struct of its fields' own defaults (nulls where none).</summary>
    public bool IsStruct { get; private set; }

    /// <summary>The defaults below this field, by name.</summary>
    public IReadOnlyDictionary<string, ArrowDefaults>? Fields => _fields;

    /// <summary>The defaults of field <paramref name="name"/> below this one, or null.</summary>
    public ArrowDefaults? Field(string name) => _fields?.GetValueOrDefault(name);

    /// <summary>Declares this field's default: <paramref name="value"/>, or a struct of its fields' defaults (<paramref name="isStruct"/>).</summary>
    public void Declare(ArrowScalar? value, bool isStruct)
    {
        Declared = true;
        Value = isStruct ? null : value;
        IsStruct = isStruct;
    }

    /// <summary>The defaults of field <paramref name="name"/> below this one, made when there are none yet.</summary>
    public ArrowDefaults Below(string name)
    {
        _fields ??= new(StringComparer.Ordinal);
        if (!_fields.TryGetValue(name, out ArrowDefaults? child)) _fields[name] = child = new ArrowDefaults();
        return child;
    }

    /// <summary>
    /// The field's default as a value of <paramref name="type"/> (a struct's of its fields: the type may hold fewer
    /// than the defaults name, as a selection of fields prunes it); null when none is declared or it is null.
    /// </summary>
    public ArrowScalar? ScalarFor(IArrowType type)
    {
        if (!Declared) return null;
        if (!IsStruct) return Value;
        var s = (StructType)type;
        ArrowScalar?[] fields = [.. s.Fields.Select(f => Field(f.Name)?.ScalarFor(f.DataType))];
        return new ArrowScalar(s, fields);
    }

    /// <summary>
    /// The defaults of <paramref name="over"/> laid over <paramref name="under"/>: a field <paramref name="over"/>
    /// declares takes its default, the others <paramref name="under"/>'s, at every depth.
    /// </summary>
    public static ArrowDefaults? Merge(ArrowDefaults? over, ArrowDefaults? under)
    {
        if (over is null) return under;
        if (under is null) return over;
        var merged = new ArrowDefaults();
        ArrowDefaults declared = over.Declared ? over : under;
        if (declared.Declared) merged.Declare(declared.Value, declared.IsStruct);
        HashSet<string> names = new(StringComparer.Ordinal);
        if (over._fields is { } overFields) names.UnionWith(overFields.Keys);
        if (under._fields is { } underFields) names.UnionWith(underFields.Keys);
        foreach (string name in names)
        {
            if (Merge(over.Field(name), under.Field(name)) is { } child) (merged._fields ??= new(StringComparer.Ordinal))[name] = child;
        }
        return merged;
    }
}
