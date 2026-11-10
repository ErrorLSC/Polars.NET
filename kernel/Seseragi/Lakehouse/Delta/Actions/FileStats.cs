using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>
/// The statistics of a data file: row count and, per column, the smallest and largest value and the number of nulls.
/// Columns are addressed by their path of physical names; nested columns nest.
/// </summary>
public sealed class FileStats
{
    private readonly JsonElement _root;

    private FileStats(JsonElement root) => _root = root;

    /// <summary>The number of rows physically in the file, deleted ones included.</summary>
    public long? NumRecords =>
        _root.TryGetProperty("numRecords", out JsonElement n) && n.ValueKind == JsonValueKind.Number && n.TryGetInt64(out long v) ? v : null;

    /// <summary>
    /// False when the smallest and largest values are only bounds, as happens once rows are deleted through a
    /// deletion vector. Then a null count is exact only when it is 0 or equals <see cref="NumRecords"/>.
    /// </summary>
    public bool TightBounds => !_root.TryGetProperty("tightBounds", out JsonElement t) || t.ValueKind != JsonValueKind.False;

    /// <summary>Reads statistics from the JSON text of <c>add.stats</c>. Returns null for text that is not a JSON object.</summary>
    public static FileStats? Parse(string? json)
    {
        if (string.IsNullOrEmpty(json)) return null;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement.ValueKind == JsonValueKind.Object ? new FileStats(doc.RootElement.Clone()) : null;
        }
        catch (JsonException)
        {
            return null; // unreadable statistics are the same as none
        }
    }

    /// <summary>The smallest value of a column as written, if recorded.</summary>
    public bool TryGetMin(ReadOnlySpan<string> path, out JsonElement value) => TryGet("minValues", path, out value);

    /// <summary>The largest value of a column as written, if recorded.</summary>
    public bool TryGetMax(ReadOnlySpan<string> path, out JsonElement value) => TryGet("maxValues", path, out value);

    /// <summary>The number of nulls of a column, if recorded.</summary>
    public long? NullCount(ReadOnlySpan<string> path) =>
        TryGet("nullCount", path, out JsonElement v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt64(out long n) ? n : null;

    private bool TryGet(string section, ReadOnlySpan<string> path, out JsonElement value)
    {
        value = default;
        if (!_root.TryGetProperty(section, out JsonElement current)) return false;
        foreach (string segment in path)
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out current)) return false;
        }
        if (current.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined) return false;
        value = current;
        return true;
    }
}
