using System.Text.RegularExpressions;

namespace Polars.NET.Linq;

internal static partial class SqlSanitizer
{
    // ==========================================
    // Regex Generators
    // ==========================================

    [GeneratedRegex(@"\s+ESCAPE\s+'.'", RegexOptions.IgnoreCase, "en-US")]
    private static partial Regex EscapeRegex();

    [GeneratedRegex(@"\b[a-zA-Z_]\w*\.([a-zA-Z_]\w*)\b")]
    private static partial Regex PrefixAliasRegex();

    [GeneratedRegex(@"(GROUP\s+BY\s+)(.+?)(HAVING|ORDER\s+BY|LIMIT|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex GroupByRegex();

    [GeneratedRegex(@"(?:DELETE\s+FROM|UPDATE)\s+""?([a-zA-Z0-9_]+)""?", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex DmlTableRegex();

    // ==========================================
    // Clean Method
    // ==========================================

    internal static string Clean(string rawSql)
    {
        // Remove ESCAPE
        var sql = EscapeRegex().Replace(rawSql, "");

        // Deduplicate GROUP BY
        var groupMatch = GroupByRegex().Match(sql);
        if (groupMatch.Success)
        {
            var originalGroupBy = groupMatch.Groups[2].Value;
            ReadOnlySpan<char> groupSpan = originalGroupBy.AsSpan();

            // count the number of ,
            int expectedCount = groupSpan.Count(',') + 1;

            // Set a null rentedRanges array
            Range[]? rentedRanges = null;

            // If range <= 32, use stackalloc to avoid heap allocation
            // If range > 32, rent memoery from array pool
            Span<Range> ranges = expectedCount <= 32 
                ? stackalloc Range[32] 
                : (rentedRanges = System.Buffers.ArrayPool<Range>.Shared.Rent(expectedCount));

            try
            {
                int splitCount = 0;
                int parenCount = 0;
                int bracketCount = 0; // 新增：追踪 T-SQL 方括号 []
                bool inSingleQuote = false;
                bool inDoubleQuote = false; // 新增：追踪 ANSI 双引号 ""
                int lastSplitIndex = 0;

                for (int i = 0; i < groupSpan.Length; i++)
                {
                    char c = groupSpan[i];

                    // 1. 处理单引号 (字符串字面量)
                    if (c == '\'' && !inDoubleQuote) // 确保不是在双引号标识符内部
                    {
                        if (i + 1 < groupSpan.Length && groupSpan[i + 1] == '\'')
                            i++; // 采用你的完美建议：跳过转义引号
                        else
                            inSingleQuote = !inSingleQuote;
                        
                        continue; // 直接进入下一个字符，省去后续判断
                    }

                    // 2. 处理双引号 (ANSI 标识符)
                    if (c == '"' && !inSingleQuote) // 确保不是在单引号字符串内部
                    {
                        if (i + 1 < groupSpan.Length && groupSpan[i + 1] == '"')
                            i++; // 跳过转义双引号
                        else
                            inDoubleQuote = !inDoubleQuote;
                        
                        continue;
                    }

                    // 3. 如果在引号内部，直接忽略所有括号和逗号
                    if (inSingleQuote || inDoubleQuote)
                        continue;

                    // 4. 处理括号和真正的分割点
                    if (c == '[') bracketCount++;
                    else if (c == ']') bracketCount--;
                    else if (c == '(') parenCount++;
                    else if (c == ')') parenCount--;
                    else if (c == ',' && parenCount == 0 && bracketCount == 0) // 必须同时不在圆括号和方括号内！
                    {
                        if (splitCount < ranges.Length)
                        {
                            ranges[splitCount++] = new Range(lastSplitIndex, i);
                        }
                        lastSplitIndex = i + 1;
                    }
                }
                // 添加最后一段
                if (lastSplitIndex <= groupSpan.Length && splitCount < ranges.Length)
                {
                    ranges[splitCount++] = new Range(lastSplitIndex, groupSpan.Length);
                }

                // 2. 去重逻辑
                var distinctKeys = new HashSet<string>(splitCount, StringComparer.OrdinalIgnoreCase);

                for (int i = 0; i < splitCount; i++)
                {
                    // 因为是我们自定义切的，需要手动 Trim 一下前后的空格
                    var slice = groupSpan[ranges[i]].Trim();
                    if (slice.Length == 0) continue;

                    var stripped = RemoveTrailingAliasSpan(slice);
                    distinctKeys.Add(stripped.ToString());
                }

                string distinctGroupBy = string.Join(", ", distinctKeys) + " ";
                sql = sql.Replace(originalGroupBy, distinctGroupBy);
            }
            finally
            {
                if (rentedRanges != null)
                {
                    System.Buffers.ArrayPool<Range>.Shared.Return(rentedRanges);
                }
            }
        }

        return sql;
    }

    /// <summary>
    /// Extract SQL expression between SELECT and FROM
    /// </summary>
    internal static string ExtractSelectSnippet(string fullSql)
    {
        ReadOnlySpan<char> span = fullSql.AsSpan();
        
        // Find SELECT
        int selIdx = span.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
        if (selIdx == -1) throw new InvalidOperationException($"Expression Extraction Failed from: {fullSql}");

        int start = selIdx + 6;

        // Find FROM
        int fromIdx = IndexOfWordBoundary(span, "FROM", start);
        if (fromIdx == -1) throw new InvalidOperationException($"Expression Extraction Failed from: {fullSql}");

        return span[start..fromIdx].Trim().ToString();
    }

    /// <summary>
    /// Find Boundary in SQL
    /// </summary>
    private static int IndexOfWordBoundary(ReadOnlySpan<char> span, string word, int startIndex)
    {
        int searchStart = startIndex;
        while (true)
        {
            int idx = span[searchStart..].IndexOf(word, StringComparison.OrdinalIgnoreCase);
            if (idx == -1) return -1;

            int actualIdx = searchStart + idx;

            // Check whether previous char is blank
            bool prevIsSpace = actualIdx == 0 || char.IsWhiteSpace(span[actualIdx - 1]);
            
            // Check whether next char is blank
            int nextCharIdx = actualIdx + word.Length;
            bool nextIsSpace = nextCharIdx >= span.Length || char.IsWhiteSpace(span[nextCharIdx]);

            if (prevIsSpace && nextIsSpace)
            {
                return actualIdx;
            }

            searchStart = actualIdx + word.Length;
        }
    }

    /// <summary>
    /// Remove Table alias
    /// </summary>
    internal static string RemoveTableAliases(string rawSnippet)
         => PrefixAliasRegex().Replace(rawSnippet, "$1");

    /// <summary>
    /// Remove suffix alias
    /// </summary>
    internal static string RemoveTrailingAlias(string snippet)
        => RemoveTrailingAliasSpan(snippet.AsSpan()).ToString();

    /// <summary>
    /// Remove suffix by Span mode
    /// </summary>
    internal static ReadOnlySpan<char> RemoveTrailingAliasSpan(ReadOnlySpan<char> snippet)
    {
        int asIdx = snippet.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
        
        if (asIdx > 0)
        {
            return snippet[..asIdx].Trim();
        }
        return snippet.Trim();
    }

    internal static Match MatchDmlTable(string sql)
        => DmlTableRegex().Match(sql);
}