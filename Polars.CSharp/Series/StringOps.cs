namespace Polars.CSharp;

/// <summary>
/// Series String Ops
/// </summary>
public readonly struct SeriesStrOps
{
    private readonly Series _series;
    internal SeriesStrOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Transfer String to UpperClass.
    /// </summary>
    /// <returns></returns>
    public Series ToUpper() => Apply(e => e.Str.ToUpper());
    /// <summary>
    /// Transfer String to LowerClass.
    /// </summary>
    /// <returns></returns>
    public Series ToLower() => Apply(e => e.Str.ToLower());
    /// <summary>
    /// Get length in bytes.
    /// </summary>
    public Series Len() => Apply(e => e.Str.Len());
    /// <summary>
    /// Check if the string contains a substring that matches a pattern.
    /// </summary>
    /// <param name="pattern"></param>
    /// <returns></returns>
    public Series Contains(string pattern) => Apply(e => e.Str.Contains(pattern));
    /// <summary>
    /// Slice string by length.
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public Series Slice(long offset, ulong length) => Apply(e => e.Str.Slice(offset, length));
    /// <summary>
    /// Split the string by a substring.
    /// </summary>
    /// <param name="separator"></param>
    /// <returns></returns>
    public Series Split(string separator) => Apply(e => e.Str.Split(separator));
    /// <summary>
    /// Replace charaters in a string.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="value"></param>
    /// <param name="useRegex"></param>
    /// <returns></returns>
    public Series ReplaceAll(string pattern, string value, bool useRegex = false)
        => Apply(e => e.Str.ReplaceAll(pattern, value,useRegex));
    /// <summary>
    /// Extract charaters in string by Regex.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="groupIndex"></param>
    /// <returns></returns>
    public Series Extract(string pattern, uint groupIndex)
        => Apply(e => e.Str.Extract(pattern, groupIndex));
    // ==========================================
    // Strip / Clean
    // ==========================================

    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    /// <param name="matches">The set of characters to be removed.</param>
    public Series StripChars(string? matches = null)
        => Apply(e => e.Str.StripChars(matches));

    /// <summary>
    /// Remove leading characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Series StripCharsStart(string? matches = null)
        => Apply(e => e.Str.StripCharsStart(matches));

    /// <summary>
    /// Remove trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Series StripCharsEnd(string? matches = null)
        => Apply(e => e.Str.StripCharsEnd(matches));
    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    public Series StripPrefix(string prefix)
        => Apply(e => e.Str.StripPrefix(prefix));

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    public Series StripSuffix(string suffix)
        => Apply(e => e.Str.StripPrefix(suffix));

    // ==========================================
    // Boolean Checks
    // ==========================================

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    public Series StartsWith(string prefix)
        => Apply(e => e.Str.StartsWith(prefix));

    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    public Series EndsWith(string suffix)
        => Apply(e => e.Str.StripSuffix(suffix ));

    // ==========================================
    // Temporal Parsing
    // ==========================================

    /// <summary>
    /// Convert string to Date using the specified format.
    /// </summary>
    public Series ToDate(string? format = null, bool strict = true, bool exact = true, bool cache = true)
        => Apply(e => e.Str.ToDate(format,strict,exact,cache));

    /// <summary>
    /// Convert string to Datetime using the specified format.
    /// </summary>
    public Series ToDatetime(string? format = null,TimeUnit unit=TimeUnit.Microseconds,string? timeZone=null, bool strict = true, bool exact = true, bool cache = true)
        => Apply(e => e.Str.ToDatetime(format,unit,timeZone,strict,exact,cache));
}