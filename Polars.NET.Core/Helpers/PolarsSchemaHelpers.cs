namespace Polars.NET.Core.Helpers;

public static class PolarsSchemaExtractor
{
    /// <summary>
    /// Uniformly retrieves all column names from a LazyFrameHandle.
    /// </summary>
    public static string[] GetColumnNames(LazyFrameHandle lfHandle)
    {
        var clonedLf = PolarsWrapper.LazyClone(lfHandle);
        var schema = PolarsWrapper.GetLazySchema(clonedLf);
        var count = PolarsWrapper.GetSchemaLen(schema);
        var names = new string[count];

        for (ulong i = 0; i < count; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(schema, i, out string? name, out _);
            names[i] = name ?? string.Empty;
        }

        return names;
    }
}