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
    /// <inheritdoc cref="StringOps.ToUppercase()"/>
    public Series ToUppercase() => Apply(e => e.Str.ToUppercase());
    /// <inheritdoc cref="StringOps.ToLowercase()"/>
    public Series ToLowercase() => Apply(e => e.Str.ToLowercase());
    /// <inheritdoc cref="StringOps.LenBytes()"/>
    public Series Len() => Apply(e => e.Str.LenBytes());
    /// <inheritdoc cref="StringOps.Contains"/>
    public Series Contains(StringOrExpr pattern,bool strict=true ) => Apply(e => e.Str.Contains(pattern,strict));
    /// <inheritdoc cref="StringOps.Slice"/>
    public Series Slice(IntoExpr offset, IntoExpr? length=null) => Apply(e => e.Str.Slice(offset, length));
    /// <inheritdoc cref="StringOps.Split"/>
    public Series Split(StringOrExpr separator,bool inclusive=false,bool literal=true,bool strict=true) => Apply(e => e.Str.Split(separator,inclusive,literal,strict));
    /// <inheritdoc cref="StringOps.ReplaceAll"/>
    public Series ReplaceAll(StringOrExpr pattern, StringOrExpr value, bool useRegex = false)
        => Apply(e => e.Str.ReplaceAll(pattern, value,useRegex));
    /// <inheritdoc cref="StringOps.Extract"/>
    public Series Extract(StringOrExpr pattern, int groupIndex=1)
        => Apply(e => e.Str.Extract(pattern, groupIndex));
    // ==========================================
    // Strip / Clean
    // ==========================================

    /// <inheritdoc cref="StringOps.StripChars(StringOrExpr?)"/>
    public Series StripChars(StringOrExpr? characters=null)
        => Apply(e => e.Str.StripChars(characters));

    /// <inheritdoc cref="StringOps.StripCharsStart(StringOrExpr?)"/>
    public Series StripCharsStart(StringOrExpr? characters=null)
        => Apply(e => e.Str.StripCharsStart(characters));

    /// <inheritdoc cref="StringOps.StripCharsEnd(StringOrExpr?)"/>
    public Series StripCharsEnd(StringOrExpr? characters=null)
        => Apply(e => e.Str.StripCharsEnd(characters));
    /// <inheritdoc cref="StringOps.StripPrefix(StringOrExpr)"/>
    public Series StripPrefix(StringOrExpr prefix)
        => Apply(e => e.Str.StripPrefix(prefix));

    /// <inheritdoc cref="StringOps.StripSuffix(StringOrExpr)"/>
    public Series StripSuffix(StringOrExpr suffix)
        => Apply(e => e.Str.StripSuffix(suffix));

    // ==========================================
    // Boolean Checks
    // ==========================================

    /// <inheritdoc cref="StringOps.StartsWith(StringOrExpr)"/>
    public Series StartsWith(StringOrExpr prefix)
        => Apply(e => e.Str.StartsWith(prefix));

    /// <inheritdoc cref="StringOps.EndsWith(StringOrExpr)"/>
    public Series EndsWith(StringOrExpr suffix)
        => Apply(e => e.Str.EndsWith(suffix ));

    // ==========================================
    // Temporal Parsing
    // ==========================================

    /// <inheritdoc cref="StringOps.ToDate"/>
    public Series ToDate(string? format = null, bool strict = true, bool exact = true, bool cache = true)
        => Apply(e => e.Str.ToDate(format,strict,exact,cache));

    /// <inheritdoc cref="StringOps.ToDatetime"/>
    public Series ToDatetime(string? format = null,TimeUnit unit=TimeUnit.Microseconds,string? timeZone=null, bool strict = true, bool exact = true, bool cache = true,AmbiguousStrategy ambiguous=AmbiguousStrategy.Raise)
        => Apply(e => e.Str.ToDatetime(format,unit,timeZone,strict,exact,cache,ambiguous));
}