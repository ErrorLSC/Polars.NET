using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp;

/// <summary>
/// Series String Ops
/// </summary>
public readonly struct SeriesStrOps
{
    private readonly Series _series;
    internal SeriesStrOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Pl.Col(_series.Name)));
    /// <inheritdoc cref="StringOps.ToUppercase()"/>
    public Series ToUppercase() => Apply(e => e.Str.ToUppercase());
    /// <inheritdoc cref="StringOps.ToLowercase()"/>
    public Series ToLowercase() => Apply(e => e.Str.ToLowercase());
    /// <inheritdoc cref="StringOps.ToTitlecase()"/>
    public Series ToTitlecase() => Apply(e => e.Str.ToTitlecase());
    /// <inheritdoc cref="StringOps.EscapeRegex()"/>
    public Series EscapeRegex() => Apply(e => e.Str.EscapeRegex());
    /// <inheritdoc cref="StringOps.LenBytes()"/>
    public Series LenBytes() => Apply(e => e.Str.LenBytes());
    /// <inheritdoc cref="StringOps.LenChars()"/>
    public Series LenChars() => Apply(e => e.Str.LenChars());
    /// <inheritdoc cref="StringOps.Reverse()"/>
    public Series Reverse() => Apply(e => e.Str.Reverse());
    /// <inheritdoc cref="StringOps.Slice"/>
    public Series Slice(IntoExpr offset, IntoExpr? length=null) => Apply(e => e.Str.Slice(offset, length));
    /// <inheritdoc cref="StringOps.Replace"/>
    public Series Replace(string pattern, string value,bool literal=false,int n=1) 
        => Apply(e => e.Str.Replace(pattern, value,literal,n));
    /// <inheritdoc cref="StringOps.ReplaceAll"/>
    public Series ReplaceAll(string pattern, string value, bool literal = false)
        => Apply(e => e.Str.ReplaceAll(pattern, value,literal));
    /// <inheritdoc cref="StringOps.ReplaceMany(IntoExpr,IntoExpr,bool,bool)"/>
    public Series ReplaceMany(Series patterns, Series replaceWith, bool asciiCaseInsensitive = false,bool leftmost=false)
        => Apply(e => e.Str.ReplaceMany(patterns, replaceWith,asciiCaseInsensitive,leftmost));
    /// <inheritdoc cref="StringOps.ReplaceMany(IReadOnlyDictionary{string,string},bool,bool)"/>
    public Series ReplaceMany(IReadOnlyDictionary<string, string> mapping, bool asciiCaseInsensitive = false,bool leftmost=false)
        => Apply(e => e.Str.ReplaceMany(mapping,asciiCaseInsensitive,leftmost));
    /// <inheritdoc cref="StringOps.ReplaceMany(IntoExpr,IntoExpr,bool,bool)"/>
    public Series ReplaceMany(IEnumerable<string> patterns,IEnumerable<string> replaceWith, bool asciiCaseInsensitive = false,bool leftmost=false)
        => Apply(e => e.Str.ReplaceMany(Pl.Lit(patterns).Implode(),Pl.Lit(replaceWith).Implode(),asciiCaseInsensitive,leftmost));
    /// <inheritdoc cref="StringOps.Extract"/>
    public Series Extract(StringOrExpr pattern, int groupIndex=1)
        => Apply(e => e.Str.Extract(pattern, groupIndex));
    /// <inheritdoc cref="StringOps.ExtractMany"/>
    public Series ExtractMany(Series pattern, bool asciiCaseInsensitive=false,bool overlapping=false,bool leftmost=false)
        => Apply(e => e.Str.ExtractMany(pattern, asciiCaseInsensitive,overlapping,leftmost));
    /// <inheritdoc cref="StringOps.ExtractMany"/>
    public Series ExtractMany(IEnumerable<string> pattern, bool asciiCaseInsensitive=false,bool overlapping=false,bool leftmost=false)
        => Apply(e => e.Str.ExtractMany(Pl.Lit(pattern).Implode(), asciiCaseInsensitive,overlapping,leftmost));
    /// <inheritdoc cref="StringOps.ExtractAll"/>
    public Series ExtractAll(Series pattern)
        => Apply(e => e.Str.ExtractAll(Pl.Lit(pattern)));
    /// <inheritdoc cref="StringOps.ExtractAll"/>
    public Series ExtractAll(IEnumerable<string> pattern)
        => Apply(e => e.Str.ExtractAll(Pl.Lit(pattern)));
    /// <inheritdoc cref="StringOps.ExtractGroups"/>
    public Series ExtractGroups(string pattern)
        => Apply(e => e.Str.ExtractGroups(pattern));
    /// <inheritdoc cref="StringOps.Contains"/>
    public Series Contains(StringOrExpr pattern,bool literal = false,bool strict=true)
        => Apply(e => e.Str.Contains(pattern,literal,strict));
    /// <inheritdoc cref="StringOps.ContainsAny"/>
    public Series ContainsAny(Series patterns,bool asciiCaseInsensitive = false)
        => Apply(e => e.Str.ContainsAny(Pl.Lit(patterns),asciiCaseInsensitive));
    /// <inheritdoc cref="StringOps.ContainsAny"/>
    public Series ContainsAny(IEnumerable<string> patterns,bool asciiCaseInsensitive = false)
        => Apply(e => e.Str.ContainsAny(Pl.Lit(patterns),asciiCaseInsensitive));
    /// <inheritdoc cref="StringOps.Split"/>
    public Series Split(StringOrExpr by,bool inclusive=false,bool literal=true,bool strict=true)
        => Apply(e => e.Str.Split(by,inclusive,literal,strict));
    /// <inheritdoc cref="StringOps.SplitN"/>
    public Series SplitN(StringOrExpr by,int n)
        => Apply(e => e.Str.SplitN(by,n));
    /// <inheritdoc cref="StringOps.SplitExact"/>
    public Series SplitExact(StringOrExpr by,int n,bool inclusive = false)
        => Apply(e => e.Str.SplitExact(by,n,inclusive));
    /// <inheritdoc cref="StringOps.Find"/>
    public Series Find(StringOrExpr pattern,bool literal = false, bool strict = true)
        => Apply(e=>e.Str.Find(pattern,literal,strict));
    /// <inheritdoc cref="StringOps.FindMany"/>
    public Series FindMany(StringOrExpr patterns, bool asciiCaseInsensitive=false, bool overlapping=false, bool leftmost=false)
        => Apply(e=>e.Str.FindMany(patterns,asciiCaseInsensitive,overlapping,leftmost));
    /// <inheritdoc cref="StringOps.CountMatches"/>
    public Series CountMatches(Series pattern,bool literal = false)
        => Apply(e => e.Str.CountMatches(Pl.Lit(pattern),literal));
    /// <inheritdoc cref="StringOps.CountMatches"/>
    public Series CountMatches(IEnumerable<string> pattern,bool literal = false)
        => Apply(e => e.Str.CountMatches(Pl.Lit(pattern),literal));

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
    // Head & Tail
    // ==========================================
    /// <inheritdoc cref="StringOps.Head"/>
    public Series Head(IntOrExpr n) => Apply(e => e.Str.Head(n));
    /// <inheritdoc cref="StringOps.Tail"/>
    public Series Tail(IntOrExpr n) => Apply(e => e.Str.Tail(n));
    // ==========================================
    // JSON
    // ==========================================
    /// <inheritdoc cref="StringOps.JsonPathMatch"/>
    public Series JsonPathMatch(StringOrExpr jsonPath) => Apply(e => e.Str.JsonPathMatch(jsonPath));
    /// <inheritdoc cref="StringOps.JsonDecode"/>
    public Series JsonDecode(IntoDataTypeExpr dtype) => Apply(e => e.Str.JsonDecode(dtype));
    // ==========================================
    // Padding
    // ==========================================
    /// <inheritdoc cref="StringOps.Zfill"/>
    public Series Zfill(IntOrExpr length ) => Apply(e => e.Str.Zfill(length));
    /// <inheritdoc cref="StringOps.PadStart"/>
    public Series PadStart(IntOrExpr length,string fillChar=" ") => Apply(e => e.Str.PadStart(length,fillChar));
    /// <inheritdoc cref="StringOps.PadEnd"/>
    public Series PadEnd(IntOrExpr length,string fillChar=" ") => Apply(e => e.Str.PadEnd(length,fillChar));
    // ==========================================
    // Join
    // ==========================================
    /// <inheritdoc cref="StringOps.Join"/>
    public Series Join(string delimiter="",bool ignoreNulls=true) 
        => Apply(e => e.Str.Join(delimiter,ignoreNulls));
    // ==========================================
    // Encoding / Decoding
    // ==========================================
    /// <inheritdoc cref="StringOps.Encode"/>
    public Series Encode(TransferEncoding encoding) 
        => Apply(e => e.Str.Encode(encoding));
    /// <inheritdoc cref="StringOps.Decode"/>
    public Series Decode(TransferEncoding encoding,bool strict=true) 
        => Apply(e => e.Str.Decode(encoding,strict));

    // ==========================================
    // Parsing
    // ==========================================

    /// <inheritdoc cref="StringOps.ToDate"/>
    public Series ToDate(string? format = null, bool strict = true, bool exact = true, bool cache = true)
        => Apply(e => e.Str.ToDate(format,strict,exact,cache));
    /// <inheritdoc cref="StringOps.ToTime"/>
    public Series ToTime(string? format = null, bool strict = true, bool exact = true, bool cache = true)
        => Apply(e => e.Str.ToTime(format,strict,exact,cache));

    /// <inheritdoc cref="StringOps.ToDatetime"/>
    public Series ToDatetime(string? format = null,TimeUnit unit=TimeUnit.Microseconds,string? timeZone=null, bool strict = true, bool exact = true, bool cache = true,AmbiguousStrategy ambiguous=AmbiguousStrategy.Raise)
        => Apply(e => e.Str.ToDatetime(format,unit,timeZone,strict,exact,cache,ambiguous));
    /// <inheritdoc cref="StringOps.Strptime"/>
    public Series Strptime(IntoDataTypeExpr dtype, string? format = null, bool strict = true, bool exact = true, bool cache = true)
        => Apply(e => e.Str.Strptime(dtype,format,strict,exact,cache));
    /// <inheritdoc cref="StringOps.ToDecimal"/>
    public Series ToDecimal(int scale) 
        => Apply(e => e.Str.ToDecimal(scale));
    /// <inheritdoc cref="StringOps.ToInteger"/>
    public Series ToInteger(IntOrExpr? radix=null,DataType? dtype=null,bool strict=true) 
        => Apply(e => e.Str.ToInteger(radix,dtype,strict));
}