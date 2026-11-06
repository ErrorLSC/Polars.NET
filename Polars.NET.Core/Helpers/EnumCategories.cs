using System.Collections.Concurrent;

namespace Polars.NET.Core.Helpers;

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
    private sealed class Layout(bool usesEnumDataType, string[] categories, object[] values, Dictionary<object, string> names)
    {
        public bool UsesEnumDataType { get; } = usesEnumDataType;
        public string[] Categories { get; } = categories;
        public object[] Values { get; } = values;
        public Dictionary<object, string> Names { get; } = names;
    }

    private static readonly ConcurrentDictionary<Type, Layout> Layouts = new();

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

            object[] values = [.. Enum.GetValues(t).Cast<object>().Distinct()];

            if (unsigned)
            {
                Array.Sort(values, static (a, b) => Convert.ToUInt64(a).CompareTo(Convert.ToUInt64(b)));
            }
            else
            {
                Array.Sort(values, static (a, b) => Convert.ToInt64(a).CompareTo(Convert.ToInt64(b)));
            }

            var names = new Dictionary<object, string>(values.Length);
            var categories = new string[values.Length];
            var underlyingValues = new object[values.Length];

            for (int i = 0; i < values.Length; i++)
            {
                categories[i] = Enum.GetName(t, values[i])!;
                names[values[i]] = categories[i];
                underlyingValues[i] = Convert.ChangeType(values[i], underlying);
            }

            bool isFlags = t.IsDefined(typeof(FlagsAttribute), inherit: false);
            return new Layout(!isFlags, categories, underlyingValues, names);
        });
    }
}
