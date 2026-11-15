using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>Which <see cref="ParquetWriteOptions.Columns"/> entry applies to each leaf column, and whether the leaf can take it.</summary>
internal static class ColumnSettings
{
    /// <summary>
    /// The settings of each leaf (null: none): the entry whose path is the leaf's or the longest ancestor's, paths
    /// joined by dots.
    /// </summary>
    /// <exception cref="ArgumentException">A path names no column, or a leaf cannot take its settings.</exception>
    public static ParquetColumnOptions?[] Resolve(ColumnPlan[] plans, IReadOnlyDictionary<string, ParquetColumnOptions>? columns)
    {
        var resolved = new ParquetColumnOptions?[plans.Length];
        if (columns is null || columns.Count == 0) return resolved;
        string[] paths = [.. plans.Select(p => string.Join('.', p.Path))];
        int[] matched = new int[plans.Length];
        foreach (var (key, settings) in columns)
        {
            bool any = false;
            for (int c = 0; c < plans.Length; c++)
            {
                string path = paths[c];
                if (path != key && !(path.Length > key.Length && path.StartsWith(key, StringComparison.Ordinal) && path[key.Length] == '.')) continue;
                any = true;
                if (key.Length > matched[c])
                {
                    matched[c] = key.Length;
                    resolved[c] = settings;
                }
            }
            if (!any)
                throw new ArgumentException($"ParquetWriteOptions.Columns names '{key}', which is no column of the schema (paths join the Parquet names by dots, as in "
                                            + "'s.x', 'l.list.element', 'm.key_value.key'; the columns are " + string.Join(", ", paths.Select(p => $"'{p}'")) + ").", "options");
        }
        for (int c = 0; c < plans.Length; c++)
        {
            if (resolved[c] is not { } settings) continue;
            ParquetType type = plans[c].Leaf.Element.Type!.Value;
            if (settings.BloomFilter is not null && type == ParquetType.Boolean)
                throw new ArgumentException($"Column '{paths[c]}' is BOOLEAN, which takes no Bloom filter.", "options");
            if (!Takes(type, settings.Encoding))
                throw new ArgumentException($"Column '{paths[c]}' is {type}, which {settings.Encoding} cannot encode; it takes {string.Join(", ", Enum.GetValues<ParquetValueEncoding>().Where(e => Takes(type, e)))}.", "options");
        }
        return resolved;
    }

    /// <summary>Whether values of a physical type can be written in an encoding.</summary>
    public static bool Takes(ParquetType type, ParquetValueEncoding encoding) => encoding switch
    {
        ParquetValueEncoding.Plain => true,
        ParquetValueEncoding.DeltaBinaryPacked => type is ParquetType.Int32 or ParquetType.Int64,
        ParquetValueEncoding.DeltaLengthByteArray => type is ParquetType.ByteArray,
        ParquetValueEncoding.DeltaByteArray => type is ParquetType.ByteArray or ParquetType.FixedLenByteArray,
        ParquetValueEncoding.ByteStreamSplit => type is ParquetType.Float or ParquetType.Double or ParquetType.Int32 or ParquetType.Int64 or ParquetType.FixedLenByteArray,
        ParquetValueEncoding.Rle => type is ParquetType.Boolean,
        _ => false,
    };
}
