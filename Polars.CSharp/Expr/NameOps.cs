using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Offers methods for renaming columns.
/// </summary>
public readonly struct NameOps
{
    private readonly Expr _expr;
    internal NameOps(Expr expr) { _expr = expr; }
    /// <summary>
    /// Keep the original root name of the expression.
    /// </summary>
    public Expr Keep() => new(PolarsWrapper.NameKeep(_expr.CloneHandle()));

    /// <summary>
    /// Prefix the column name with a specified string.
    /// </summary>
    /// <param name="prefix"></param>
    public Expr Prefix(string prefix)
        => new(PolarsWrapper.Prefix(_expr.CloneHandle(), prefix));
    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    /// <param name="suffix"></param>
    public Expr Suffix(string suffix)
        => new(PolarsWrapper.Suffix(_expr.CloneHandle(), suffix));
    /// <summary>
    /// Add a prefix to all field names of a struct.
    /// </summary>
    /// <param name="prefix">Prefix to add to the field name.</param>
    public Expr PrefixFields(string prefix) => new(PolarsWrapper.FieldPrefix(_expr.CloneHandle(),prefix));
    /// <summary>
    /// Add a suffix to all field names of a struct.
    /// </summary>
    /// <param name="suffix">Suffix to add to the field name.</param>
    public Expr SuffixFields(string suffix) => new(PolarsWrapper.FieldSuffix(_expr.CloneHandle(),suffix));
    /// <summary>
    /// Make the root column name uppercase.
    /// </summary>
    public Expr ToUpperCase() => new(PolarsWrapper.NameToUpperCase(_expr.CloneHandle()));
    /// <summary>
    /// Make the root column name lowercase.
    /// </summary>
    public Expr ToLowerCase() => new(PolarsWrapper.NameToLowerCase(_expr.CloneHandle()));

    /// <summary>
    /// Replace matching regex/literal substring in the name with a new value.
    /// </summary>
    /// <param name="pattern">A valid regular expression pattern, compatible with the regex crate.</param>
    /// <param name="value">String that will replace the matched substring.</param>
    /// <param name="literal">Treat pattern as a literal string, not a regex.</param>
    /// <returns></returns>
    public Expr Replace(string pattern, string value,bool literal=false) => new(PolarsWrapper.NameReplace(_expr.CloneHandle(),pattern,value,literal));
    /// <summary>
    /// Rename the output of an expression by mapping a function over the root name.
    /// </summary>
    /// <param name="function">Function that maps a root name to a new name.</param>
    /// <returns></returns>
    public Expr Map(Func<string, string> function) => new(PolarsWrapper.MapName(_expr.Handle, function));
    /// <summary>
    /// Rename fields of a struct by mapping a function over the field name(s).
    /// </summary>
    /// <param name="function">Function that maps a field name to a new name.</param>
    /// <returns></returns>
    public Expr MapFields(Func<string, string> function) => new(PolarsWrapper.StructMapFields(_expr.Handle, function));
    
}