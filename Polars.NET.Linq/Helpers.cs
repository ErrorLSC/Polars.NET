using System.Text.RegularExpressions;

namespace Polars.NET.Linq;

internal static partial class SqlSanitizer
{
    // Regex to remove ESCAPE
    [GeneratedRegex(@"\s+ESCAPE\s+'.'", RegexOptions.IgnoreCase, "en-US")]
    private static partial Regex EscapeRegex();

    // Regex to extract core SQL expression
    [GeneratedRegex(@"SELECT\s+(.+?)\s+FROM", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex SelectRegex();

    // Regex to remove prefix alias (t1.salary -> salary)
    [GeneratedRegex(@"\b[a-zA-Z_]\w*\.([a-zA-Z_]\w*)\b")]
    private static partial Regex PrefixAliasRegex();

    // Regex to remove suffix alias (as c1, AS cond)
    [GeneratedRegex(@"\s+[aA][sS]\s+[""\[\]\w]+$", RegexOptions.IgnoreCase, "en-US")]
    private static partial Regex TrailingAliasRegex();

    // Regex to capture the GROUP BY clause for sanitization
    [GeneratedRegex(@"(GROUP\s+BY\s+)(.+?)(HAVING|ORDER\s+BY|LIMIT|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex GroupByRegex();

    // Regex to extract table name from DML statements (DELETE/UPDATE)
    [GeneratedRegex(@"(?:DELETE\s+FROM|UPDATE)\s+""?([a-zA-Z0-9_]+)""?", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex DmlTableRegex();

    /// <summary>
    /// Clean rawSql not supported by Polars
    /// </summary>
    internal static string Clean(string rawSql)
    {
        // Console.WriteLine($"\n[Polars.NET.LINQ DEBUG] Raw SQL:\n{rawSql}\n");
        // ==========================================
        // Rule 1: Clean ESCAPE 
        // ==========================================
        var sql = EscapeRegex().Replace(rawSql, "");
        
        // ==========================================
        // Rule 2: Clean GROUP BY (Remove aliases & deduplicate)
        // ==========================================
        var groupMatch = GroupByRegex().Match(sql);
        if (groupMatch.Success)
        {
            var originalGroupBy = groupMatch.Groups[2].Value;

            // Split by comma, reuse RemoveTrailingAlias to strip 'AS xxx', distinct, and rejoin
            var distinctKeys = string.Join(", ", originalGroupBy
                .Split(',')
                .Select(RemoveTrailingAlias) 
                .Distinct());

            sql = sql.Replace(originalGroupBy, distinctKeys + " ");
        }
        // Console.WriteLine($"\n[Polars.NET.LINQ DEBUG] Clean SQL:\n{sql}\n");
        return sql;
    }

    /// <summary>
    /// Extract SQL expression between SELECT to FROM
    /// </summary>
    internal static string ExtractSelectSnippet(string fullSql)
    {
        var match = SelectRegex().Match(fullSql);
        if (!match.Success)
            throw new InvalidOperationException($"Expression Extraction Failed from: {fullSql}");

        return match.Groups[1].Value;
    }

    /// <summary>
    /// Remove Table alias
    /// </summary>
    internal static string RemoveTableAliases(string rawSnippet)
         =>PrefixAliasRegex().Replace(rawSnippet, "$1");

    /// <summary>
    /// Remove suffix alias
    /// </summary>
    internal static string RemoveTrailingAlias(string snippet)
        =>TrailingAliasRegex().Replace(snippet, "").Trim();
    
    /// <summary>
    /// Match DML statement and extract target table name
    /// </summary>
    internal static Match MatchDmlTable(string sql)
        =>DmlTableRegex().Match(sql);
}
