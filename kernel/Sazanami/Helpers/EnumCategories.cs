using System.Collections.Concurrent;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// How .NET enum types map to Polars Enum categories.
/// <para>
/// An enum is stored as a Polars Enum column whose categories are its member names, ordered by underlying value,
/// so the Enum ordering (sorting, comparisons, min / max) agrees with .NET enum comparison. Members sharing a value
/// contribute the single name <see cref="Enum.GetName(Type, object)"/> returns for it.
/// </para>
/// <para>
/// A <see cref="FlagsAttribute"/> enum is stored as its underlying integer instead: its combined values are not
/// members, so they have no category.
/// </para>
/// </summary>
public static class EnumCategories
{
    private sealed class Layout(
        bool usesEnumDataType,
        string[] categories,
        object[] values,
        Dictionary<object, string> names,
        Dictionary<object, int> indexes)
    {
        public bool UsesEnumDataType { get; } = usesEnumDataType;
        public string[] Categories { get; } = categories;
        public object[] Values { get; } = values;
        public Dictionary<object, string> Names { get; } = names;
        public Dictionary<object, int> Indexes { get; } = indexes;
    }

    private static readonly ConcurrentDictionary<Type, Layout> Layouts = new();

    /// <summary>
    /// The Arrow field metadata key under which Polars stores an Enum's categories.
    /// </summary>
    public const string PolarsEnumMetadataKey = "_PL_ENUM_VALUES2";

    /// <summary>
    /// The Arrow field metadata that makes Polars import a dictionary field as the Enum of an enum type (or
    /// Nullable / F# option of one), or null for a non-enum type or a [Flags] enum.
    /// The categories are encoded as Polars encodes them: each one prefixed with its UTF-8 byte length and ';'.
    /// </summary>
    public static Dictionary<string, string>? TryGetArrowFieldMetadata(Type type)
    {
        var core = ClrTypeHelper.UnwrapCoreType(type);
        if (!core.IsEnum || !UsesEnumDataType(core))
        {
            return null;
        }

        var encoded = new System.Text.StringBuilder();
        foreach (var category in GetLayout(core).Categories)
        {
            encoded.Append(System.Text.Encoding.UTF8.GetByteCount(category)).Append(';').Append(category);
        }

        return new Dictionary<string, string> { [PolarsEnumMetadataKey] = encoded.ToString() };
    }

    /// <summary>
    /// The position of the category holding <paramref name="value"/>, a boxed value of
    /// <paramref name="enumType"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The value is not a defined member of the enum.</exception>
    public static int GetCategoryIndex(Type enumType, object value)
    {
        if (GetLayout(enumType).Indexes.TryGetValue(value, out var index))
        {
            return index;
        }

        // Throws the undefined-member error.
        GetCategory(enumType, value);
        return -1;
    }

    /// <summary>
    /// Whether values of <paramref name="enumType"/> are stored as a Polars Enum column (false for a
    /// <see cref="FlagsAttribute"/> enum, which is stored as its underlying integer).
    /// </summary>
    public static bool UsesEnumDataType(Type enumType) => GetLayout(enumType).UsesEnumDataType;

    /// <summary>
    /// The Polars Enum categories of <paramref name="enumType"/>: its member names ordered by underlying value,
    /// one name per distinct value.
    /// </summary>
    public static string[] GetCategories(Type enumType) => [.. GetLayout(enumType).Categories];

    /// <summary>
    /// The underlying values of the categories of <paramref name="enumType"/>, boxed as the underlying integer
    /// type, in category order.
    /// </summary>
    public static object[] GetUnderlyingValues(Type enumType) => [.. GetLayout(enumType).Values];

    /// <summary>
    /// The category holding <paramref name="value"/>, a boxed value of <paramref name="enumType"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The value is not a defined member of the enum.</exception>
    public static string GetCategory(Type enumType, object value)
    {
        if (GetLayout(enumType).Names.TryGetValue(value, out var name))
        {
            return name;
        }

        throw new ArgumentException(
            $"Value '{value}' is not a defined member of enum {enumType.FullName}; a Polars Enum column holds only defined members.",
            nameof(value));
    }

    private static Layout GetLayout(Type enumType)
    {
        ArgumentNullException.ThrowIfNull(enumType);

        if (!enumType.IsEnum)
        {
            throw new ArgumentException($"Type {enumType.FullName} is not an enum.", nameof(enumType));
        }

        return Layouts.GetOrAdd(enumType, static t =>
        {
            var underlying = Enum.GetUnderlyingType(t);
            bool unsigned = underlying == typeof(byte) || underlying == typeof(ushort)
                || underlying == typeof(uint) || underlying == typeof(ulong) || underlying == typeof(char);

            // Underlying integers (Native AOT safe, unlike Enum.GetValues(Type)); boxed enum values are
            // rebuilt with Enum.ToObject below for the name/index lookups keyed by enum value.
            object[] values = [.. Enum.GetValuesAsUnderlyingType(t).Cast<object>().Distinct()];

            if (unsigned)
            {
                Array.Sort(values, static (a, b) => Convert.ToUInt64(a).CompareTo(Convert.ToUInt64(b)));
            }
            else
            {
                Array.Sort(values, static (a, b) => Convert.ToInt64(a).CompareTo(Convert.ToInt64(b)));
            }

            var names = new Dictionary<object, string>(values.Length);
            var indexes = new Dictionary<object, int>(values.Length);
            var categories = new string[values.Length];
            var underlyingValues = new object[values.Length];

            for (int i = 0; i < values.Length; i++)
            {
                object enumValue = Enum.ToObject(t, values[i]);
                categories[i] = Enum.GetName(t, enumValue)!;
                names[enumValue] = categories[i];
                indexes[enumValue] = i;
                underlyingValues[i] = values[i];
            }

            bool isFlags = t.IsDefined(typeof(FlagsAttribute), inherit: false);
            return new Layout(!isFlags, categories, underlyingValues, names, indexes);
        });
    }
}
