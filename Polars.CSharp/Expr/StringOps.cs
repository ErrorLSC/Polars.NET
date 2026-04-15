#pragma warning disable CS1591 
using System.Text;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp;

// ==========================================
// StringOps Helper Class
// ==========================================
/// <summary>
/// Offers multiple methods for checking, parsing, and transforming string columns.
/// Access this via <see cref="Expr.Str"/>.
/// </summary>
public readonly struct StringOps
{
    private readonly Expr _expr;
    internal StringOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));
    
    /// <summary>
    /// Convert string to uppercase.
    /// </summary>
    public Expr ToUppercase() => Wrap(PolarsWrapper.StrToUpper);
    /// <summary>
    /// Convert string to lowercase.
    /// </summary>
    public Expr ToLowercase() => Wrap(PolarsWrapper.StrToLower);
    /// <summary>
    /// Modify strings to their titlecase equivalent.
    /// <para>This is a form of case transform where the first letter of each word is capitalized, with the rest of the word in lowercase.
    /// Non-alphanumeric characters define the word boundaries.
    /// </para>
    /// </summary>
    /// <returns></returns>
    public Expr ToTitlecase() => Wrap(PolarsWrapper.StrToTitlecase);
    /// <summary>
    /// Returns string values with all regular expression meta characters escaped.
    /// </summary>
    public Expr EscapeRegex() => Wrap(PolarsWrapper.StrEscapeRegex);
    /// <summary>
    /// Get length in bytes.
    /// <para>Note: Multi-byte characters (like emojis or CJK) count as > 1 byte.</para>
    /// </summary>
    public Expr LenBytes() => Wrap(PolarsWrapper.StrLenBytes);
    /// <summary>
    /// Return the length of each string as the number of characters.
    /// </summary>
    /// <returns>Expression of data type UInt32.</returns>
    public Expr LenChars() => Wrap(PolarsWrapper.StrLenChars);
    public Expr Reverse() => Wrap(PolarsWrapper.StrReverse);
    /// <summary>
    /// Slice the string by offset and length.
    /// </summary>
    /// <param name="offset">Start index.</param>
    /// <param name="length">Length of the slice.</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     text = new[] { "Polars", "  Data  ", "Rust", null, "123-abc" }
    /// });
    /// 
    /// // 1. ToUpper
    /// // 2. Len (bytes)
    /// // 3. Slice (offset=1, len=3)
    /// df.Select(
    ///     Col("text"),
    ///     Col("text").Str.ToUpper().Alias("upper"),
    ///     Col("text").Str.Len().Alias("len_bytes"), 
    ///     Col("text").Str.Slice(1, 3).Alias("sliced") 
    /// ).Show();
    /// /* Output:
    /// shape: (5, 4)
    /// ┌──────────┬──────────┬───────────┬────────┐
    /// │ text     ┆ upper    ┆ len_bytes ┆ sliced │
    /// │ ---      ┆ ---      ┆ ---       ┆ ---    │
    /// │ str      ┆ str      ┆ u32       ┆ str    │
    /// ╞══════════╪══════════╪═══════════╪════════╡
    /// │ Polars   ┆ POLARS   ┆ 6         ┆ ola    │
    /// │   Data   ┆   DATA   ┆ 8         ┆  Da    │
    /// │ Rust     ┆ RUST     ┆ 4         ┆ ust    │
    /// │ null     ┆ null     ┆ null      ┆ null   │
    /// │ 123-abc  ┆ 123-ABC  ┆ 7         ┆ 23-    │
    /// └──────────┴──────────┴───────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Slice(IntoExpr offset, IntoExpr? length = null)
    {
        Expr offsetExpr = offset.Consume();

        Expr lengthExpr = length.HasValue ? length.Value.Consume() : Pl.LitNull();

        return new Expr(PolarsWrapper.StrSlice(
            _expr.CloneHandle(), 
            offsetExpr.CloneHandle(), 
            lengthExpr.CloneHandle()
        ));
    }
    /// <summary>
    /// Replace first matching regex/literal substring with a new string value.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="value">String that will replace the matched substring.</param>
    /// <param name="literal">Treat pattern as a literal string, not a regex.</param>
    /// <param name="n">Number of matches to replace.</param>
    /// <returns></returns>
    public Expr Replace(StringOrExpr pattern,StringOrExpr value,bool literal=false, int n=1)
        => new(PolarsWrapper.StrReplace(_expr.CloneHandle(), pattern.Expression.CloneHandle(),value.Expression.CloneHandle(),literal,n));
    /// <summary>
    /// Replace all occurrences of a pattern with a value.
    /// </summary>
    /// <param name="pattern">The pattern to search for.</param>
    /// <param name="value">The value to replace with.</param>
    /// <param name="literal">Whether to interpret the pattern as literal value(not Regex).</param>
    public Expr ReplaceAll(StringOrExpr pattern, StringOrExpr value, bool literal = false)
        => new(PolarsWrapper.StrReplaceAll(_expr.CloneHandle(), pattern.Expression.CloneHandle(),value.Expression.CloneHandle(),literal));
    /// <summary>
    /// Use the Aho-Corasick algorithm to replace many matches.
    /// </summary>
    /// <param name="patterns">Expression yielding string patterns to search and replace.</param>
    /// <param name="replaceWith">Strings to replace where a pattern was a match. Length must match the length of patterns or have length 1. This can be broadcasted, so it supports many:one and many:many.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used.</param>
    public Expr ReplaceMany(IntoExpr patterns, IntoExpr replaceWith, bool asciiCaseInsensitive = false, bool leftmost = false)
        => new(PolarsWrapper.StrReplaceMany(_expr.CloneHandle(), patterns.Consume().Handle,replaceWith.Consume().Handle,asciiCaseInsensitive,leftmost));
    /// <summary>
    /// Use the Aho-Corasick algorithm to replace many matches using a dictionary mapping.
    /// </summary>
    /// <param name="mapping">A mapping of patterns to their replacement.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used.</param>
    public Expr ReplaceMany(
        IReadOnlyDictionary<string, string> mapping, 
        bool asciiCaseInsensitive = false, 
        bool leftmost = false)
    {
        string[] keys = [.. mapping.Keys];
        string[] values = [.. mapping.Values];

        using var patSeries = Pl.Series("patterns", keys).Implode();
        using var valSeries = Pl.Series("replacements", values).Implode();

        return ReplaceMany(patSeries, valSeries, asciiCaseInsensitive, leftmost);
    }
    /// <summary>
    /// Extract the first match of a regex pattern.
    /// </summary>
    /// <param name="pattern">Regex pattern with capture groups.</param>
    /// <param name="groupIndex">The index of the capture group to extract (usually 1).</param>
    public Expr Extract(StringOrExpr pattern, int groupIndex=1) 
        => new(PolarsWrapper.StrExtract(_expr.CloneHandle(), pattern.Expression.CloneHandle(), groupIndex));
    /// <summary>
    /// Use the Aho-Corasick algorithm to extract many matches.This method supports matching on string literals only, and does not support regular expression matching.
    /// </summary>
    /// <param name="patterns">String patterns to search.Notice:string will be parsed as column name</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only.</param>
    /// <param name="overlapping">Whether matches may overlap.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used. In case there are multiple candidates for the leftmost match the pattern which comes first in patterns is used. May not be used together with overlapping = True.</param>
    public Expr ExtractMany(IntoExpr patterns,bool asciiCaseInsensitive=false,bool overlapping=false,bool leftmost=false)
        => new(PolarsWrapper.StrExtractMany(_expr.CloneHandle(),patterns.Consume().Handle,asciiCaseInsensitive,overlapping,leftmost));
    /// <summary>
    /// Extract all matches for the given regex pattern.
    /// Extract each successive non-overlapping regex match in an individual string as a list. If the haystack string is null, null is returned.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <returns>Expression of data type List(String).</returns>
    public Expr ExtractAll(StringOrExpr pattern) => new(PolarsWrapper.StrExtractAll(_expr.CloneHandle(),pattern.Expression.CloneHandle()));
    /// <summary>
    /// Extract all capture groups for the given regex pattern.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern containing at least one capture group, compatible with the regex crate.</param>
    /// <returns>Expression of data type Struct with fields of data type String.</returns>
    public Expr ExtractGroups(string pattern) => new(PolarsWrapper.StrExtractGroups(_expr.CloneHandle(),pattern));
    /// <summary>
    /// Check if the string contains a substring or regex pattern.
    /// </summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     code = new[] { "ID-001", "ID-002", "ID-999", "XX-000", "ID-ABC" },
    ///     text = new[] { "Polars", "Data", "Rust", null, "123" }
    /// });
    /// 
    /// df.Select(
    ///     Pl.Col("code"),
    ///     // Replace "-" with "_"
    ///     Pl.Col("code").Str.ReplaceAll("-", "_").Alias("replaced"),
    ///     // Extract numbers using Regex group 1
    ///     Pl.Col("code").Str.Extract(@"(\d+)", 1).Alias("extracted_num"),
    ///     // Check if text contains "a"
    ///     Pl.Col("text").Str.Contains("a").Alias("has_a")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 4)
    /// ┌────────┬──────────┬───────────────┬───────┐
    /// │ code   ┆ replaced ┆ extracted_num ┆ has_a │
    /// │ ---    ┆ ---      ┆ ---           ┆ ---   │
    /// │ str    ┆ str      ┆ str           ┆ bool  │
    /// ╞════════╪══════════╪═══════════════╪═══════╡
    /// │ ID-001 ┆ ID_001   ┆ 001           ┆ true  │
    /// │ ID-002 ┆ ID_002   ┆ 002           ┆ true  │
    /// │ ID-999 ┆ ID_999   ┆ 999           ┆ false │
    /// │ XX-000 ┆ XX_000   ┆ 000           ┆ null  │
    /// │ ID-ABC ┆ ID_ABC   ┆ null          ┆ false │
    /// └────────┴──────────┴───────────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public Expr Contains(StringOrExpr pattern,bool literal=false,bool strict=true) 
        => new(PolarsWrapper.StrContains(_expr.CloneHandle(), pattern.Expression.CloneHandle(),literal,strict));
    /// <summary>
    /// Use the Aho-Corasick algorithm to find matches.
    /// Determines if any of the patterns are contained in the string.
    /// </summary>
    /// <param name="patterns">String patterns to search.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only.</param>
    /// <returns></returns>
    public Expr ContainsAny(StringOrExpr patterns,bool asciiCaseInsensitive=false) 
        => new(PolarsWrapper.StrContainsAny(_expr.CloneHandle(),patterns.Expression.CloneHandle(),asciiCaseInsensitive));
    /// <summary>
    /// Split the string by a substring.
    /// </summary>
    /// <param name="by">Substring to split by.</param>
    /// <param name="inclusive">If True, include the split character/string in the results.</param>
    /// <param name="literal">Treat by as a literal string, not as a regular expression.</param>
    /// <param name="strict">Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value.</param>
    /// <returns>Expression/Series of data type String.</returns>
    public Expr Split(StringOrExpr by,bool inclusive=false,bool literal=true,bool strict=true)
        => new(PolarsWrapper.StrSplit(_expr.CloneHandle(), by.Expression.CloneHandle(),inclusive,literal,strict));
    /// <summary>
    /// Split the string by a substring, restricted to returning at most n items.
    /// <para>If the number of possible splits is less than n-1, the remaining field elements will be null. 
    /// If the number of possible splits is n-1 or greater, the last (nth) substring will contain the remainder of the string.
    /// </para>
    /// </summary>
    /// <param name="by">Substring to split by.</param>
    /// <param name="n">Max number of items to return.</param>
    /// <returns>Expression/Series of data type Struct with fields of data type String.</returns>
    public Expr SplitN(StringOrExpr by,int n)
        => new(PolarsWrapper.StrSplitN(_expr.CloneHandle(),by.Expression.CloneHandle(),n));
    /// <summary>
    /// Split the string by a substring using n splits.
    /// Results in a struct of n+1 fields.
    /// If it cannot make n splits, the remaining field elements will be null.
    /// </summary>
    /// <param name="by">Substring to split by.</param>
    /// <param name="n">Number of splits to make.</param>
    /// <param name="inclusive">If True, include the split character/string in the results.</param>
    /// <returns>Expression/Series of data type Struct with fields of data type String.</returns>
    public Expr SplitExact(StringOrExpr by,int n, bool inclusive=false)
        => new(PolarsWrapper.StrSplitExact(_expr.CloneHandle(),by.Expression.CloneHandle(),n,inclusive));
    /// <summary>
    /// Return the bytes offset of the first substring matching a pattern.
    /// If the pattern is not found, returns None.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="literal">Treat pattern as a literal string, not as a regular expression.</param>
    /// <param name="strict">Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value.</param>
    /// <returns></returns>
    public Expr Find(StringOrExpr pattern, bool literal=false, bool strict=true)
        => new(PolarsWrapper.StrFind(_expr.CloneHandle(),pattern.Expression.CloneHandle(),literal,strict));
    /// <summary>
    /// Use the Aho-Corasick algorithm to find many matches.This method supports matching on string literals only, and does not support regular expression matching.
    /// </summary>
    /// <param name="patterns">String patterns to search.</param>
    /// <param name="asciiCaseInsensitive">Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only.</param>
    /// <param name="overlapping">Whether matches may overlap.</param>
    /// <param name="leftmost">Guarantees in case there are overlapping matches that the leftmost match is used. In case there are multiple candidates for the leftmost match the pattern which comes first in patterns is used. May not be used together with overlapping = True.</param>
    /// <returns>The function will return the bytes offset of the start of each match. The return type will be List{UInt32}</returns>
    public Expr FindMany(StringOrExpr patterns, bool asciiCaseInsensitive=false, bool overlapping=false, bool leftmost=false)
        => new(PolarsWrapper.StrFindMany(_expr.CloneHandle(),patterns.Expression.CloneHandle(),asciiCaseInsensitive,overlapping,leftmost));
    /// <summary>
    /// Count all successive non-overlapping regex matches.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="literal">Treat pattern as a literal string, not as a regular expression.</param>
    /// <returns>Expression of data type UInt32. Returns null if the original value is null.</returns>
    public Expr CountMatches(StringOrExpr pattern,bool literal=false)
        => new(PolarsWrapper.StrCountMatches(_expr.CloneHandle(),pattern.Expression.CloneHandle(),literal));
    // ==========================================
    // Strip / Clean
    // ==========================================
    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    /// <param name="characters">The set of characters to be removed. 
    /// All combinations of this set of characters will be stripped from the start and end of the string. 
    /// If set to None (default), all leading and trailing whitespace is removed instead.</param>
    /// <example>
    /// <code>
    /// df.Select(
    ///     // "  Data  " -> "Data"
    ///     Col("text").Str.StripChars().Alias("trimmed"),
    ///     // Remove "ID-" prefix
    ///     Col("code").Str.StripPrefix("ID-").Alias("no_prefix")
    /// );
    /// </code>
    /// </example>
    public Expr StripChars(StringOrExpr? characters = null)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        
        Expr charsExpr = characters.HasValue ? characters.Value.Expression : Pl.LitNull();
        
        return new Expr(PolarsWrapper.StrStripChars(h, charsExpr.CloneHandle()));
    }
    /// <summary>
    /// Remove leading characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Expr StripCharsStart(StringOrExpr? characters = null)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        Expr charsExpr = characters.HasValue ? characters.Value.Expression : Pl.LitNull();
        return new Expr(PolarsWrapper.StrStripCharsStart(h, charsExpr.CloneHandle()));
    }
    /// <summary>
    /// Remove trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Expr StripCharsEnd(StringOrExpr? characters = null)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        Expr charsExpr = characters.HasValue ? characters.Value.Expression : Pl.LitNull();
        return new Expr(PolarsWrapper.StrStripCharsEnd(h, charsExpr.CloneHandle()));
    }
    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    public Expr StripPrefix(StringOrExpr prefix)
        => new(PolarsWrapper.StrStripPrefix(_expr.CloneHandle(), prefix.Expression.CloneHandle()));
    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    public Expr StripSuffix(StringOrExpr suffix)
        => new(PolarsWrapper.StrStripSuffix(_expr.CloneHandle(), suffix.Expression.CloneHandle()));
    
    // ==========================================
    // Boolean Checks
    // ==========================================

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    public Expr StartsWith(StringOrExpr prefix)
        => new(PolarsWrapper.StrStartsWith(_expr.CloneHandle(), prefix.Expression.CloneHandle()));
    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    public Expr EndsWith(StringOrExpr suffix)
        => new(PolarsWrapper.StrEndsWith(_expr.CloneHandle(), suffix.Expression.CloneHandle()));
    // ==========================================
    // Head & Tail
    // ==========================================
    /// <summary>
    /// Return the first n characters of each string in a String Series.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression)
    /// <para>When the n input is negative, head returns characters up to the n`th from the end of the string.
    /// For example, if `n = -3, then all characters except the last three are returned.</para></param>
    /// <returns>Expression of data type String.</returns>
    public Expr Head(IntOrExpr n) => new(PolarsWrapper.StrHead(_expr.CloneHandle(),n.Expression.CloneHandle()));
    /// <summary>
    /// Return the last n characters of each string in a String Series.
    /// </summary>
    /// <param name="n">Length of the slice (integer or expression)
    /// <para>When the n input is negative, head returns characters up to the n`th from the start of the string.
    /// For example, if `n = -3, then all characters except the first three are returned.</para></param>
    /// <returns>Expression of data type String.</returns>
    public Expr Tail(IntOrExpr n) => new(PolarsWrapper.StrTail(_expr.CloneHandle(),n.Expression.CloneHandle()));
    // ==========================================
    // JSON
    // ==========================================
    /// <summary>
    /// Extract the first match from a JSON string using the provided JSONPath.
    /// Throws errors if invalid JSON strings are encountered. All return values are cast to String, regardless of the original value.
    /// </summary>
    /// <param name="jsonPath">A valid JSONPath query string.</param>
    /// <returns>Expression of data type String. Contains null values if original value is null or the json_path returns nothing.</returns>
    public Expr JsonPathMatch(StringOrExpr jsonPath)
        => new(PolarsWrapper.StrJsonPathMatch(_expr.CloneHandle(),jsonPath.Expression.CloneHandle()));
    /// <summary>
    /// Parse string values as JSON.
    /// Throws an error if invalid JSON strings are encountered.
    /// </summary>
    /// <param name="dtype">The datatype to cast the extracted value to.</param>
    public Expr JsonDecode(IntoDataTypeExpr dtype)
        => new(PolarsWrapper.StrJsonDecode(_expr.CloneHandle(),dtype.Consume().Handle));

    // ==========================================
    // Padding
    // ==========================================
    /// <summary>
    /// Pad the start of the string with zeros until it reaches the given length.
    /// <para>A sign prefix (-) is handled by inserting the padding after the sign character rather than before.</para>
    /// <para>This method is intended for padding numeric strings. If your data contains non-ASCII characters, use pad_start() instead.</para>
    /// </summary>
    /// <param name="length">Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is.</param>
    public Expr Zfill(IntOrExpr length) => new(PolarsWrapper.StrZfill(_expr.CloneHandle(),length.Expression.CloneHandle()));
    /// <summary>
    /// Pad the start of the string until it reaches the given length.
    /// </summary>
    /// <param name="length">Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is.</param>
    /// <param name="fillChar">The character to pad the string with.</param>
    public Expr PadStart(IntOrExpr length, string fillChar=" ")
        => new(PolarsWrapper.StrPadStart(_expr.CloneHandle(),length.Expression.CloneHandle(),fillChar));
    /// <summary>
    /// Pad the end of the string until it reaches the given length.
    /// </summary>
    /// <param name="length">Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is.</param>
    /// <param name="fillChar">The character to pad the string with.</param>
    public Expr PadEnd(IntOrExpr length, string fillChar=" ")
        => new(PolarsWrapper.StrPadEnd(_expr.CloneHandle(),length.Expression.CloneHandle(),fillChar));
    
    // ==========================================
    // Join
    // ==========================================
    /// <summary>
    /// Vertically concatenate the string values in the column to a single string value.
    /// </summary>
    /// <param name="delimiter">The delimiter to insert between consecutive string values.</param>
    /// <param name="ignoreNulls">Ignore null values (default). If set to False, null values will be propagated. This means that if the column contains any null values, the output is null.</param>
    /// <returns>Expression of data type String.</returns>
    public Expr Join(string delimiter="",bool ignoreNulls = true)
        => new(PolarsWrapper.StrJoin(_expr.CloneHandle(),delimiter,ignoreNulls));
    
    // ==========================================
    // Encoding / Decoding
    // ==========================================
    /// <summary>
    /// Encode values using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>Expression of data type String.</returns>
    /// <exception cref="ArgumentOutOfRangeException">"Unsupported transfer encoding."</exception>
    public Expr Encode(TransferEncoding encoding) => encoding switch
    {
        TransferEncoding.Base64 => new Expr(PolarsWrapper.StrBase64Encode(_expr.CloneHandle())),
        TransferEncoding.Hex    => new Expr(PolarsWrapper.StrHexEncode(_expr.CloneHandle())),
        
        _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unsupported transfer encoding.")
    };
    /// <summary>
    /// Decode values using the provided encoding.
    /// </summary>
    /// <param name="encoding">The encoding to use.</param>
    /// <param name="strict">Raise an error if the underlying value cannot be decoded, otherwise mask out with a null value.</param>
    /// <returns>Expression of data type Binary.</returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public Expr Decode(TransferEncoding encoding,bool strict=true) => encoding switch
    {
        TransferEncoding.Base64 => new Expr(PolarsWrapper.StrBase64Decode(_expr.CloneHandle(),strict)),
        TransferEncoding.Hex    => new Expr(PolarsWrapper.StrHexDecode(_expr.CloneHandle(),strict)),
        
        _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unsupported transfer encoding.")
    };


    // ==========================================
    // Parsing
    // ==========================================

    /// <summary>       
    /// Convert string to Date using the specified format.
    /// <para>
    /// Strict parsing: Format mismatches result in null.
    /// Format string is similar to `strftime` (e.g. "%Y-%m-%d").
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var dateDf = DataFrame.FromColumns(new { 
    ///     raw = new[] { "2024-01-01", "2024/02/01", "invalid" } 
    /// });
    /// 
    /// dateDf.Select(
    ///     Pl.Col("raw"),
    ///     Pl.Col("raw").Str.ToDate("%Y-%m-%d").Alias("fmt_dash"), 
    ///     Pl.Col("raw").Str.ToDate("%Y/%m/%d").Alias("fmt_slash")
    /// ).Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌────────────┬────────────┬────────────┐
    /// │ raw        ┆ fmt_dash   ┆ fmt_slash  │
    /// │ ---        ┆ ---        ┆ ---        │
    /// │ str        ┆ date       ┆ date       │
    /// ╞════════════╪════════════╪════════════╡
    /// │ 2024-01-01 ┆ 2024-01-01 ┆ null       │
    /// │ 2024/02/01 ┆ null       ┆ 2024-02-01 │
    /// │ invalid    ┆ null       ┆ null       │
    /// └────────────┴────────────┴────────────┘
    /// */
    /// </code>
    /// </example>
    /// <param name="format">The parsing format (e.g., "%Y-%m-%d"). Null for auto-inference.</param>
    /// <param name="strict">If true, raises an error on parsing failure. If false, returns nulls.</param>
    /// <param name="exact">If true, requires an exact match. If false, allows matching substrings.</param>
    /// <param name="cache">Use a cache of unique converted dates to speed up parsing.</param>
    public Expr ToDate(
        string? format = null,
        bool strict = true,
        bool exact = true,
        bool cache = true)
    {
        var h = PolarsWrapper.StrToDate(
            _expr.CloneHandle(), 
            format, 
            strict, 
            exact, 
            cache
        );
        
        return new Expr(h);
    }
    /// <summary>
    /// Convert a String column into a Time column.
    /// </summary>
    /// <param name="format">The parsing format (e.g., "%H:%M:%S"). Null for auto-inference. </param>
    /// <param name="strict">Raise an error if any conversion fails.</param>
    /// <param name="exact">If true, requires an exact match. If false, allows matching substrings.</param>
    /// <param name="cache">Use a cache of unique, converted times to apply the conversion.</param>
    /// <returns></returns>
    public Expr ToTime(
        string? format = null,
        bool strict = true,
        bool exact = true,
        bool cache = true)
    {
        var h = PolarsWrapper.StrToTime(
            _expr.CloneHandle(), 
            format, 
            strict, 
            exact, 
            cache
        );
        
        return new Expr(h);
    }

    /// <summary>
    /// Convert string to Datetime. If format is null, Polars will attempt to infer it.
    /// </summary>
    /// <param name="format">The parsing format (e.g., "%Y-%m-%d"). Null for auto-inference.</param>
    /// <param name="timeUnit">Target time unit. Null to use default (usually Microseconds).</param>
    /// <param name="timeZone">Target time zone (e.g., "UTC", "Asia/Shanghai").</param>
    /// <param name="strict">If true, raises an error on parsing failure. If false, returns nulls.</param>
    /// <param name="exact">If true, requires an exact match. If false, allows matching substrings.</param>
    /// <param name="cache">Use a cache of unique converted dates to speed up parsing.</param>
    /// <param name="ambiguous">Determine how to deal with ambiguous datetimes:
    /// 'raise' (default): raise
    /// 'earliest': use the earliest datetime
    /// 'latest': use the latest datetime
    /// 'null': set to null</param>
    public Expr ToDatetime(
        string? format = null,
        TimeUnit? timeUnit = null,
        string? timeZone = null,
        bool strict = true,
        bool exact = true,
        bool cache = true,
        AmbiguousArg? ambiguous = null) 
    {
        PlTimeUnit tu = timeUnit.HasValue ? timeUnit.Value.ToNative() : PlTimeUnit.All;
        
        Expr amExpr = ambiguous.HasValue ? ambiguous.Value.Expression : Pl.Lit("raise"); 
        
        var h = PolarsWrapper.StrToDatetime(
            _expr.CloneHandle(), 
            tu, 
            timeZone, 
            format, 
            strict, 
            exact, 
            cache,
            amExpr.CloneHandle()
        );
        
        return new Expr(h);
    }
    /// <summary>
    /// Convert a String column into a Date/Datetime/Time column.
    /// </summary>
    /// <param name="dtype">The data type to convert into. Can be either Date, Datetime, or Time.</param>
    /// <param name="format">The parsing format (e.g., "%Y-%m-%d"). Null for auto-inference.</param>
    /// <param name="strict">Raise an error if any conversion fails.</param>
    /// <param name="exact">Require an exact format match. If False, allow the format to match anywhere in the target string. Conversion to the Time type is always exact.</param>
    /// <param name="cache">Use a cache of unique, converted dates to apply the datetime conversion.</param>
    /// <param name="ambiguous">Determine how to deal with ambiguous datetimes:
    /// 'raise' (default): raise
    /// 'earliest': use the earliest datetime
    /// 'latest': use the latest datetime
    /// 'null': set to null</param>
    public Expr Strptime(
        IntoDataTypeExpr dtype,
        string? format = null,
        bool strict = true,
        bool exact = true,
        bool cache = true,
        AmbiguousArg? ambiguous = null)
    {
        Expr amExpr = ambiguous.HasValue ? ambiguous.Value.Expression : Pl.Lit("raise"); 
        
        var h = PolarsWrapper.Strptime(
            _expr.CloneHandle(),  
            dtype.Consume().Handle,
            format, 
            strict, 
            exact, 
            cache,
            amExpr.CloneHandle()
        );
        
        return new Expr(h); 
    }
    /// <summary>
    /// Convert a String column into a Decimal column.
    /// </summary>
    /// <param name="scale">Number of digits after the comma to use for the decimals.</param>
    public Expr ToDecimal(int scale)
        => new(PolarsWrapper.StrToDecimal(_expr.CloneHandle(),scale));
    /// <summary>
    /// Convert a String column into an Integer column with the specified radix (base).
    /// </summary>
    /// <param name="radix">Positive integer or expression which is the base of the string we are parsing. Default is 10.</param>
    /// <param name="dtype">Integer data type to cast the result to. Default is Int64.</param>
    /// <param name="strict">If true, raises ComputeError on failure. If false, silently converts to Null.</param>
    /// <returns>Expression of destinated integer data type.</returns>
    public Expr ToInteger(
        IntOrExpr? radix = null, 
        DataType? dtype = null, 
        bool strict = true)
    {
        Expr radixExpr = radix.HasValue ? radix.Value.Expression : Pl.Lit(10);

        DataTypeHandle dtypeHandle = dtype is not null 
            ? dtype.Handle 
            : DataType.Int64.Handle;          

        var h = PolarsWrapper.StrToInteger(
            PolarsWrapper.CloneExpr(_expr.Handle), 
            radixExpr.CloneHandle(), 
            dtypeHandle, 
            strict
        );
        
        return new Expr(h);
    }
    /// <summary>
    /// Returns the Unicode normal form of the string values.
    /// </summary>
    /// <param name="form">Unicode form to use.</param>
    /// <returns></returns>
    public Expr Normalize(NormalizationForm form) => new(PolarsWrapper.StrNormalize(_expr.CloneHandle(),form));
}

public readonly struct AmbiguousArg

{
    internal readonly Expr Expression;

    private AmbiguousArg(Expr expr) 
    {
        Expression = expr;
    }

    public static implicit operator AmbiguousArg(AmbiguousStrategy strategy) 
        => new(Pl.Lit(strategy.ToString().ToLower()));

    public static implicit operator AmbiguousArg(Expr expr) 
        => new(expr);
}

public readonly struct StringOrExpr
{
    internal readonly Expr Expression;

    private StringOrExpr(Expr expr) 
    {
        Expression = expr;
    }

    public static implicit operator StringOrExpr(string value) 
        => new(Pl.Lit(value));

    public static implicit operator StringOrExpr(Expr expr) 
        => new(expr);
}

public readonly struct IntOrExpr
{
    internal readonly Expr Expression;

    private IntOrExpr(Expr expr) 
    {
        Expression = expr;
    }

    public static implicit operator IntOrExpr(int value) 
        => new(Pl.Lit(value));

    public static implicit operator IntOrExpr(Expr expr) 
        => new(expr);
}