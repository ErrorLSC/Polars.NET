using System.Buffers;
using System.Text.RegularExpressions;
#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
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

    // Regex to swap Position(A in B) to STRPOS(B, A)
    [GeneratedRegex(@"Position\((.*?)\s+in\s+(.*?)\)", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex PositionRegex();

    // ==========================================
    // Clean Method
    // ==========================================

    internal static string Clean(string rawSql)
    {
        // Console.WriteLine(rawSql);
        // Remove ESCAPE
        var sql = EscapeRegex().Replace(rawSql, "");

        // ==========================================
        // Rule 0: Fix Position(A in B) Overflow 
        // ==========================================
        if (sql.Contains("Position(", StringComparison.OrdinalIgnoreCase))
        {
            sql = PositionRegex().Replace(sql, "CAST(STRPOS($2, $1) AS INT)");
        }

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
                int bracketCount = 0; 
                bool inSingleQuote = false;
                bool inDoubleQuote = false;
                int lastSplitIndex = 0;

                for (int i = 0; i < groupSpan.Length; i++)
                {
                    char c = groupSpan[i];

                    // Handle ''
                    if (c == '\'' && !inDoubleQuote) 
                    {
                        if (i + 1 < groupSpan.Length && groupSpan[i + 1] == '\'')
                            i++; 
                        else
                            inSingleQuote = !inSingleQuote;
                        
                        continue; 
                    }

                    // Handle ""
                    if (c == '"' && !inSingleQuote) 
                    {
                        if (i + 1 < groupSpan.Length && groupSpan[i + 1] == '"')
                            i++; 
                        else
                            inDoubleQuote = !inDoubleQuote;
                        
                        continue;
                    }

                    // If in '' or "", continue
                    if (inSingleQuote || inDoubleQuote)
                        continue;

                    // Handle () []
                    if (c == '[') bracketCount++;
                    else if (c == ']') bracketCount--;
                    else if (c == '(') parenCount++;
                    else if (c == ')') parenCount--;
                    else if (c == ',' && parenCount == 0 && bracketCount == 0)
                    {
                        ranges[splitCount++] = new Range(lastSplitIndex, i);
                        lastSplitIndex = i + 1;
                    }
                }
                
                if (lastSplitIndex <= groupSpan.Length && splitCount < ranges.Length)
                {
                    ranges[splitCount++] = new Range(lastSplitIndex, groupSpan.Length);
                }

                // Deduplicate
                var distinctKeys = new HashSet<string>(splitCount, StringComparer.OrdinalIgnoreCase);

                for (int i = 0; i < splitCount; i++)
                {
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
        // Console.WriteLine(sql);
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

/// <summary>
/// 真正的零分配字符串构建器 (Zero-Allocation Builder)
/// </summary>
public ref struct ValueStringBuilder
{
    private char[]? _arrayToReturnToPool;
    private Span<char> _chars;
    private int _pos;

    // 👑 灵魂所在：传入一块栈内存 (stackalloc) 作为初始底座

    public ValueStringBuilder(Span<char> initialBuffer)
    {
        _arrayToReturnToPool = null;
        _chars = initialBuffer;
        _pos = 0;
    }

    public void Append(ReadOnlySpan<char> value)
    {
        int pos = _pos;
        // 如果栈内存够用，直接极其暴力的内存拷贝！
        if (pos + value.Length <= _chars.Length)
        {
            value.CopyTo(_chars.Slice(pos));
            _pos = pos + value.Length;
        }
        else
        {
            // 装不下了，去向对象池借一块大内存
            Grow(value.Length);
            value.CopyTo(_chars.Slice(_pos));
            _pos += value.Length;
        }
    }

    private void Grow(int additionalCapacityBeyondPos)
    {
        int newCapacity = Math.Max(_chars.Length * 2, _pos + additionalCapacityBeyondPos);
        
        // 去 ArrayPool 借用字符数组，绝对不 new！
        char[] poolArray = ArrayPool<char>.Shared.Rent(newCapacity);
        
        // 把之前栈上的数据拷进新数组
        _chars.Slice(0, _pos).CopyTo(poolArray);

        // 如果之前借过数组，还回去
        if (_arrayToReturnToPool != null)
        {
            ArrayPool<char>.Shared.Return(_arrayToReturnToPool);
        }

        _arrayToReturnToPool = poolArray;
        _chars = poolArray;
    }
    public void Append(char c)
    {
        int pos = _pos;
        // 使用 (uint) 强转是一种极其底层的 C# 性能 Hack
        // 它能把越界检查和负数检查合并成一条汇编指令！
        if ((uint)pos < (uint)_chars.Length)
        {
            _chars[pos] = c;
            _pos = pos + 1;
        }
        else
        {
            Grow(1);
            _chars[_pos] = c;
            _pos += 1;
        }
    }

    public override string ToString()
    {
        // 这是全流程唯一一次产生字符串对象的地方
        string s = new(_chars[.._pos]);
        Dispose();
        return s;
    }

    public void Dispose()
    {
        char[]? toReturn = _arrayToReturnToPool;
        this = default; // 清空结构体
        if (toReturn != null)
        {
            ArrayPool<char>.Shared.Return(toReturn);
        }
    }
}