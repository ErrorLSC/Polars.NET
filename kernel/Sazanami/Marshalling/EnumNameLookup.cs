using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Text;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Sazanami.Marshalling;

/// <summary>
/// An enum's members found from text as readers of typed rows take it: a member's name exactly, else in any case, else
/// the member's number; each found member is given by its canonical name (the one its value lays out as). The members
/// travel in a column's metadata as text (<see cref="Members"/>), so that a reader decodes them without the type.
/// </summary>
internal sealed class EnumNameLookup : IDisposable
{
    private const int StackBytes = 256;

    // The members' names, as given (exact matches) and folded to lower case; the member of each folded name.
    private readonly ByteArrayDictionary _names = new(), _folded = new();
    private readonly List<int> _foldedMember = [];
    // The member named for each value (its first member: the canonical name).
    private readonly Dictionary<Int128, int> _byValue = [];

    /// <param name="members">The members as <see cref="Members"/> lists them.</param>
    public EnumNameLookup(string members)
    {
        Span<byte> scratch = stackalloc byte[StackBytes];
        foreach ((string name, Int128 value) in Parse(members))
        {
            int member = _names.GetOrAdd(Encoding.UTF8.GetBytes(name));
            if (_folded.GetOrAdd(Fold(Encoding.UTF8.GetBytes(name), scratch, out byte[]? rented)) == _foldedMember.Count) _foldedMember.Add(member);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            _byValue.TryAdd(value, member);
        }
    }

    /// <summary>
    /// The members of <paramref name="enumType"/> as a column's metadata lists them: canonical names first (each value's),
    /// then aliases, each as its name's length, ';', the name, its value, ';'.
    /// </summary>
    public static string Members(Type enumType)
    {
        StringBuilder text = new();
        string[] categories = EnumCategories.GetCategories(enumType);
        (string Name, object Value)[] members = EnumCategories.GetMembers(enumType);
        foreach ((string name, object value) in members.Where(m => categories.Contains(m.Name)).Concat(members.Where(m => !categories.Contains(m.Name))))
            text.Append(name.Length).Append(';').Append(name).Append(Convert.ToString(value, CultureInfo.InvariantCulture)).Append(';');
        return text.ToString();
    }

    /// <summary>The canonical name of a member.</summary>
    public ReadOnlySpan<byte> Name(int member) => _names[member];

    /// <summary>The member <paramref name="text"/> names (exactly, in any case, or by its number), or -1.</summary>
    public int Find(ReadOnlySpan<byte> text)
    {
        int member = _names.IndexOf(text);
        if (member >= 0) return member;
        Span<byte> scratch = stackalloc byte[StackBytes];
        int folded = _folded.IndexOf(Fold(text, scratch, out byte[]? rented));
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        if (folded >= 0) return _foldedMember[folded];
        if (Utf8Parser.TryParse(text, out long signed, out int used) && used == text.Length) return Find(signed);
        return Utf8Parser.TryParse(text, out ulong unsigned, out used) && used == text.Length ? Find(unsigned) : -1;
    }

    /// <summary>The member of the value <paramref name="value"/>, or -1.</summary>
    public int Find(Int128 value) => _byValue.TryGetValue(value, out int member) ? member : -1;

    private static IEnumerable<(string Name, Int128 Value)> Parse(string members)
    {
        for (int at = 0; at < members.Length;)
        {
            int semicolon = members.IndexOf(';', at);
            int length = int.Parse(members.AsSpan(at, semicolon - at), CultureInfo.InvariantCulture);
            string name = members.Substring(semicolon + 1, length);
            int end = members.IndexOf(';', semicolon + 1 + length);
            yield return (name, Int128.Parse(members.AsSpan(semicolon + 1 + length, end - semicolon - 1 - length), CultureInfo.InvariantCulture));
            at = end + 1;
        }
    }

    // Text in lower case (ASCII in place, any other through .NET's invariant lower case).
    private static ReadOnlySpan<byte> Fold(ReadOnlySpan<byte> text, Span<byte> scratch, out byte[]? rented)
    {
        rented = null;
        if (Ascii.IsValid(text))
        {
            Span<byte> lower = text.Length <= scratch.Length ? scratch : rented = ArrayPool<byte>.Shared.Rent(text.Length);
            Ascii.ToLower(text, lower, out int written);
            return lower[..written];
        }
        return Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(text).ToLowerInvariant());
    }

    public void Dispose()
    {
        _names.Dispose();
        _folded.Dispose();
    }
}
