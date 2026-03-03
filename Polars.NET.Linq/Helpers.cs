using System.Text.RegularExpressions;

namespace Polars.NET.Linq;

internal static partial class SqlSanitizer
{
    // Regex to remove ESCAPE
    [GeneratedRegex(@"\s+ESCAPE\s+'.'")]
    private static partial Regex EscapeRegex();

    // Regex to extract core SQL expression
    [GeneratedRegex(@"SELECT\s+(.+?)\s+FROM", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex SelectRegex();

    // Regex to remove prefix alias (t1.salary -> salary)
    [GeneratedRegex(@"\b[a-zA-Z_]\w*\.([a-zA-Z_]\w*)\b")]
    private static partial Regex AliasRegex();

    // Regex to remove suffix alias (as c1, AS cond)
    [GeneratedRegex(@"\s+[aA][sS]\s+[""\[\]\w]+$", RegexOptions.IgnoreCase, "en-US")]
    private static partial Regex TrailingAliasRegex();

    /// <summary>
    /// Clean rawSql which is not supported by Polars
    /// </summary>
    public static string Clean(string rawSql)
    {
        var sql = EscapeRegex().Replace(rawSql, "");
        return sql;
    }

    /// <summary>
    /// Extract SQL expression between SELECT to FROM
    /// </summary>
    public static string ExtractSelectSnippet(string fullSql)
    {
        var match = SelectRegex().Match(fullSql);
        if (!match.Success)
            throw new InvalidOperationException($"Expression Extraction Failed from: {fullSql}");

        return match.Groups[1].Value;
    }

    /// <summary>
    /// Remove Table alias
    /// </summary>
    public static string RemoveTableAliases(string rawSnippet)
    {
        return AliasRegex().Replace(rawSnippet, "$1");
    }

    /// <summary>
    /// Remove suffix alias
    /// </summary>
    public static string RemoveTrailingAlias(string snippet)
    {
        return TrailingAliasRegex().Replace(snippet, "").Trim();
    }
}
