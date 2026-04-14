#pragma warning disable CS1591 // 缺少对公共可见类型或成员的 XML 注释
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
    /// <example>
    /// <code>
    /// df.Select(Col("text").Str.ToUpper());
    /// </code>
    /// </example>
    public Expr ToUppercase() => Wrap(PolarsWrapper.StrToUpper);
    /// <summary>
    /// Convert String to LowerClass.
    /// </summary>
    public Expr ToLowercase() => Wrap(PolarsWrapper.StrToLower);
    /// <summary>
    /// Get length in bytes.
    /// <para>Note: Multi-byte characters (like emojis or CJK) count as > 1 byte.</para>
    /// </summary>
    public Expr LenBytes() => Wrap(PolarsWrapper.StrLenBytes);
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
    /// Replace all occurrences of a pattern with a value.
    /// </summary>
    /// <param name="pattern">The pattern to search for.</param>
    /// <param name="value">The value to replace with.</param>
    /// <param name="literal">Whether to interpret the pattern as literal value(not Regex).</param>
    public Expr ReplaceAll(StringOrExpr pattern, StringOrExpr value, bool literal = false)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrReplaceAll(h, pattern.Expression.CloneHandle(), value.Expression.CloneHandle(), literal));
    }
    /// <summary>
    /// Extract the first match of a regex pattern.
    /// </summary>
    /// <param name="pattern">Regex pattern with capture groups.</param>
    /// <param name="groupIndex">The index of the capture group to extract (usually 1).</param>
    public Expr Extract(StringOrExpr pattern, int groupIndex=1)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrExtract(h, pattern.Expression.CloneHandle(), groupIndex));
    }
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
    ///     Col("code"),
    ///     // Replace "-" with "_"
    ///     Col("code").Str.ReplaceAll("-", "_").Alias("replaced"),
    ///     // Extract numbers using Regex group 1
    ///     Col("code").Str.Extract(@"(\d+)", 1).Alias("extracted_num"),
    ///     // Check if text contains "a"
    ///     Col("text").Str.Contains("a").Alias("has_a")
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
    public Expr Contains(StringOrExpr pattern,bool strict=true)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrContains(h, pattern.Expression.CloneHandle(),strict));
    }
    /// <summary>
    /// Split the string by a separator. Returns a List&lt;String&gt;.
    /// </summary>
    public Expr Split(StringOrExpr separator,bool inclusive=false,bool literal=true,bool strict=true)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrSplit(h, separator.Expression.CloneHandle(),inclusive,literal,strict));
    }
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
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripPrefix(h, prefix.Expression.CloneHandle()));
    }
    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    public Expr StripSuffix(StringOrExpr suffix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStripSuffix(h, suffix.Expression.CloneHandle()));
    }

    // ==========================================
    // Boolean Checks
    // ==========================================

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    public Expr StartsWith(StringOrExpr prefix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrStartsWith(h, prefix.Expression.CloneHandle()));
    }
    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    public Expr EndsWith(StringOrExpr suffix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.StrEndsWith(h, suffix.Expression.CloneHandle()));
    }

    // ==========================================
    // Temporal Parsing
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
    ///     Col("raw"),
    ///     Col("raw").Str.ToDate("%Y-%m-%d").Alias("fmt_dash"), 
    ///     Col("raw").Str.ToDate("%Y/%m/%d").Alias("fmt_slash")
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
            PolarsWrapper.CloneExpr(_expr.Handle), 
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
            PolarsWrapper.CloneExpr(_expr.Handle), 
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