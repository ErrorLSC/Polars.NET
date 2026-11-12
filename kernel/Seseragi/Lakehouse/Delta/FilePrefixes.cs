using System.Globalization;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Random directory prefixes for new files, placed as Spark places them. With <c>delta.randomizeFilePrefixes</c>
/// on, or under column mapping (whose physical partition names make poor directories), a data or change data file
/// goes into a directory of random letters and digits (<c>Ab/part-…</c>) instead of the partition directories and
/// <c>_change_data</c>; the partition values are in the log only. Deletion vector files get a prefix only with
/// <c>delta.randomizeFilePrefixes</c>, carried in their descriptor. Spreading files over many prefixes avoids the
/// request rate limit an object store sets per prefix.
/// </summary>
public static class FilePrefixes
{
    /// <summary>The table property that puts new files under random prefixes.</summary>
    public const string RandomizeKey = "delta.randomizeFilePrefixes";

    /// <summary>The table property that sets the length of a prefix.</summary>
    public const string LengthKey = "delta.randomPrefixLength";

    /// <summary>The length of a prefix when the table sets none (as Spark).</summary>
    public const int DefaultLength = 2;

    private const string Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    /// <summary>The length of the prefix of new data and change data files; 0 for none (partition directories).</summary>
    /// <exception cref="DeltaException">A property holds a value it does not take.</exception>
    public static int ForDataFiles(IReadOnlyDictionary<string, string> configuration) =>
        Randomized(configuration) || TableFeatures.ColumnMappingModeOf(configuration) != ColumnMappingMode.None ? Length(configuration) : 0;

    /// <summary>The length of the prefix of new deletion vector files; 0 for none (the table root).</summary>
    /// <exception cref="DeltaException">A property holds a value it does not take.</exception>
    public static int ForDeletionVectors(IReadOnlyDictionary<string, string> configuration) =>
        Randomized(configuration) ? Length(configuration) : 0;

    /// <summary>A new random prefix of letters and digits; empty for length 0.</summary>
    public static string New(int length) => length == 0 ? "" : new string(Random.Shared.GetItems(Alphabet.AsSpan(), length));

    /// <summary>Checks a value given to one of the two properties (others pass).</summary>
    /// <exception cref="DeltaException">The value is not one the property takes.</exception>
    public static void Validate(string key, string value)
    {
        if (key == RandomizeKey && !bool.TryParse(value, out _))
            throw new DeltaException($"The property {RandomizeKey} takes true or false, not '{value}'.");
        if (key == LengthKey) ParseLength(value);
    }

    /// <summary>Checks the two properties where the table properties set them.</summary>
    /// <exception cref="DeltaException">A value is not one its property takes.</exception>
    public static void Validate(IReadOnlyDictionary<string, string> configuration)
    {
        if (configuration.TryGetValue(RandomizeKey, out string? randomize)) Validate(RandomizeKey, randomize);
        if (configuration.TryGetValue(LengthKey, out string? length)) Validate(LengthKey, length);
    }

    private static bool Randomized(IReadOnlyDictionary<string, string> configuration)
    {
        if (!configuration.TryGetValue(RandomizeKey, out string? text)) return false;
        Validate(RandomizeKey, text);
        return bool.Parse(text);
    }

    private static int Length(IReadOnlyDictionary<string, string> configuration) =>
        configuration.TryGetValue(LengthKey, out string? text) ? ParseLength(text) : DefaultLength;

    private static int ParseLength(string text) =>
        int.TryParse(text, NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite, CultureInfo.InvariantCulture, out int length) && length > 0
            ? length
            : throw new DeltaException($"The property {LengthKey} needs to be greater than 0, not '{text}'.");
}
