using Apache.Arrow;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>Picking columns of schemas by name.</summary>
public static class ArrowProjection
{
    /// <summary>The fields of <paramref name="schema"/> named by <paramref name="names"/>, in that order.</summary>
    /// <exception cref="ArgumentException">A name is not a field of the schema.</exception>
    public static Schema Select(Schema schema, ReadOnlySpan<string> names)
    {
        var fields = new Field[names.Length];
        for (int i = 0; i < names.Length; i++)
            fields[i] = schema.GetFieldByName(names[i]) ?? throw new ArgumentException($"The schema has no field \"{names[i]}\".", nameof(names));
        return new Schema(fields, schema.Metadata);
    }
}
