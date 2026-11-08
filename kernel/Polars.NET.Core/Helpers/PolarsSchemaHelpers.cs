using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Helpers;

public static class PolarsSchemaExtractor
{
    /// <summary>
    /// Uniformly retrieves all column names from a LazyFrameHandle.
    /// </summary>
    public static string[] GetColumnNames(LazyFrameHandle lfHandle)
    {
        var schema = PolarsWrapper.GetLazySchema(lfHandle);
        var count = PolarsWrapper.GetSchemaLen(schema);
        var names = new string[count];

        for (ulong i = 0; i < count; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(schema, i, out string? name, out _);
            names[i] = name ?? string.Empty;
        }

        return names;
    }

    /// <summary>
    /// Retrieves the name and data type of every column of a LazyFrameHandle. The caller owns (and disposes)
    /// the returned type handles.
    /// </summary>
    public static (string Name, DataTypeHandle Type)[] GetFields(LazyFrameHandle lfHandle)
    {
        using var schema = PolarsWrapper.GetLazySchema(lfHandle);
        var count = PolarsWrapper.GetSchemaLen(schema);
        var fields = new (string, DataTypeHandle)[count];

        for (ulong i = 0; i < count; i++)
        {
            PolarsWrapper.GetSchemaFieldAt(schema, i, out string? name, out var type);
            fields[i] = (name ?? string.Empty, type);
        }

        return fields;
    }

    /// <summary>
    /// The categories of an Enum data type, in category order.
    /// </summary>
    public static string[] GetEnumCategories(DataTypeHandle enumType)
    {
        using var frozen = PolarsWrapper.GetEnumCategories(enumType);
        using var series = PolarsWrapper.FrozenCategoriesGetCategories(frozen);
        var categories = new string[PolarsWrapper.SeriesLen(series)];

        for (int i = 0; i < categories.Length; i++)
        {
            categories[i] = PolarsWrapper.SeriesGetStringFast(series, i) ?? string.Empty;
        }

        return categories;
    }
}