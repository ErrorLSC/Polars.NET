using System.Globalization;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Storage.Http;

/// <summary>Strict percent-encoding of URL paths and query values.</summary>
internal static class PercentEncoding
{
    /// <summary>
    /// Letters, digits and <c>-_.~</c> stay, every other UTF-8 byte becomes <c>%XX</c> (upper case); <c>/</c> stays
    /// unless <paramref name="encodeSlash"/>.
    /// </summary>
    public static string Encode(string value, bool encodeSlash)
    {
        var builder = new StringBuilder(value.Length + 8);
        foreach (byte b in Encoding.UTF8.GetBytes(value))
        {
            char c = (char)b;
            if (c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_' or '.' or '~'
                || (c == '/' && !encodeSlash))
                builder.Append(c);
            else
                builder.Append('%').Append(b.ToString("X2", CultureInfo.InvariantCulture));
        }
        return builder.ToString();
    }
}
