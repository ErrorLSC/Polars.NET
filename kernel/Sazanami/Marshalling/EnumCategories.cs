using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace Kaminari.Sazanami.Marshalling;

/// <summary>
/// How .NET enum types map to engine Enum categories.
/// <para>
/// An enum is stored as an engine Enum column whose categories are its member names, ordered by underlying value,
/// so the Enum ordering (sorting, comparisons, min / max) agrees with .NET enum comparison. Members sharing a value
/// contribute the single name <see cref="Enum.GetName(Type, object)"/> returns for it.
/// </para>
/// <para>
/// A <see cref="FlagsAttribute"/> enum is stored as its underlying integer instead: its combined values are not
/// members, so they have no category.
/// </para>
/// <para>
/// A column of names (an engine Enum, a dictionary of strings, strings) holds member names exactly: a value is read
/// back from its member name, an alias (another member of the same value) too, compared ordinally; any other text is
/// not a value of the enum (<see cref="TryGetMember{TEnum}"/>). Readers that take other spellings (a JSON reader
/// taking names in any case or numbers) turn them into the member names as they read them.
/// </para>
/// </summary>
public static class EnumCategories
{
    private sealed class Layout(
        bool usesEnumDataType,
        string[] categories,
        object[] values,
        Dictionary<object, string> names,
        Dictionary<object, int> indexes,
        (string Name, object Value)[] members,
        FrozenDictionary<string, object> byName)
    {
        public bool UsesEnumDataType { get; } = usesEnumDataType;
        public string[] Categories { get; } = categories;
        public object[] Values { get; } = values;
        public Dictionary<object, string> Names { get; } = names;
        public Dictionary<object, int> Indexes { get; } = indexes;
        public (string Name, object Value)[] Members { get; } = members;
        public FrozenDictionary<string, object> ByName { get; } = byName;
    }

    /// <summary>The members of <typeparamref name="TEnum"/> by name (aliases too), compared ordinally.</summary>
    private static class MembersByName<TEnum> where TEnum : struct, Enum
    {
        public static readonly FrozenDictionary<string, TEnum> Values =
            Enum.GetNames<TEnum>().Zip(Enum.GetValues<TEnum>()).ToFrozenDictionary(p => p.First, p => p.Second, StringComparer.Ordinal);
    }

    private static readonly ConcurrentDictionary<Type, Layout> Layouts = new();

    /// <summary>
    /// The Arrow field metadata key under which an Enum's categories are stored.
    /// </summary>
    public const string EnumMetadataKey = "_PL_ENUM_VALUES2";

    /// <summary>
    /// The Arrow field metadata that makes the engine import a dictionary field as the Enum of an enum type (or
    /// Nullable / F# option of one), or null for a non-enum type or a [Flags] enum.
    /// The categories are encoded as the engine expects them: each one prefixed with its UTF-8 byte length and ';'.
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

        return new Dictionary<string, string> { [EnumMetadataKey] = encoded.ToString() };
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
    /// Whether values of <paramref name="enumType"/> are stored as an engine Enum column (false for a
    /// <see cref="FlagsAttribute"/> enum, which is stored as its underlying integer).
    /// </summary>
    public static bool UsesEnumDataType(Type enumType) => GetLayout(enumType).UsesEnumDataType;

    /// <summary>
    /// The engine Enum categories of <paramref name="enumType"/>: its member names ordered by underlying value,
    /// one name per distinct value.
    /// </summary>
    public static string[] GetCategories(Type enumType) => [.. GetLayout(enumType).Categories];

    /// <summary>
    /// The underlying values of the categories of <paramref name="enumType"/>, boxed as the underlying integer
    /// type, in category order.
    /// </summary>
    public static object[] GetUnderlyingValues(Type enumType) => [.. GetLayout(enumType).Values];

    /// <summary>
    /// Every member of <paramref name="enumType"/>: its name and its underlying value (boxed as the underlying integer
    /// type), aliases (members sharing a value with another) included, ordered by their values (as unsigned numbers, as
    /// <see cref="Enum.GetNames(Type)"/> lists them).
    /// </summary>
    public static (string Name, object Value)[] GetMembers(Type enumType) => [.. GetLayout(enumType).Members];

    /// <summary>
    /// The member of <typeparamref name="TEnum"/> named <paramref name="name"/> exactly (an alias too, compared
    /// ordinally): how a column of names is read.
    /// </summary>
    public static bool TryGetMember<TEnum>(string name, out TEnum value) where TEnum : struct, Enum =>
        MembersByName<TEnum>.Values.TryGetValue(name, out value);

    /// <summary>
    /// The member of <paramref name="enumType"/> named <paramref name="name"/> exactly, boxed as the enum (see
    /// <see cref="TryGetMember{TEnum}"/>).
    /// </summary>
    public static bool TryGetMember(Type enumType, string name, [NotNullWhen(true)] out object? value) =>
        GetLayout(enumType).ByName.TryGetValue(name, out value);

    /// <summary>The error of a text that is not a member name of <paramref name="enumType"/>.</summary>
    public static FormatException NotAMemberName(Type enumType, string name) =>
        new($"'{name}' is not a member name of enum {enumType.FullName}; a column of an enum's names holds its member names exactly.");

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
            $"Value '{value}' is not a defined member of enum {enumType.FullName}; an Enum column holds only defined members.",
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

            // Every member, aliases too (GetNames and GetValuesAsUnderlyingType list them in the same order).
            string[] memberNames = Enum.GetNames(t);
            Array memberValues = Enum.GetValuesAsUnderlyingType(t);
            (string Name, object Value)[] members = [.. memberNames.Select((name, i) => (name, memberValues.GetValue(i)!))];
            FrozenDictionary<string, object> byName = members.ToFrozenDictionary(m => m.Name, m => Enum.ToObject(t, m.Value), StringComparer.Ordinal);

            bool isFlags = t.IsDefined(typeof(FlagsAttribute), inherit: false);
            return new Layout(!isFlags, categories, underlyingValues, names, indexes, members, byName);
        });
    }
}
