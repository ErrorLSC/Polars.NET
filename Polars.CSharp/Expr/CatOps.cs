using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Operations on Catgorical columns. Access via <see cref="Expr.Cat"/>.
/// </summary>
public readonly struct CategoricalOps
{
    private readonly Expr _expr;
    internal CategoricalOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));
    /// <summary>
    /// Get the categories stored in this data type.
    /// </summary>
    public Expr GetCategories() => Wrap(PolarsWrapper.CatGetCategories);
    /// <summary>
    /// Return the byte-length of the string representation of each value.
    /// </summary>
    /// <returns>Expression/Series of data type UInt32.</returns>
    public Expr LenBytes() => Wrap(PolarsWrapper.CatLenBytes);
    /// <summary>
    /// Return the number of characters of the string representation of each value.
    /// </summary>
    /// <returns>Expression/Series of data type UInt32.</returns>
    public Expr LenChars() => Wrap(PolarsWrapper.CatLenChars);
    /// <summary>
    /// Check if string representations of values start with a substring.
    /// </summary>
    /// <param name="prefix">Prefix substring.</param>
    public Expr StartsWith(string prefix) => new(PolarsWrapper.CatStartsWith(_expr.CloneHandle(),prefix));
    /// <summary>
    /// Check if string representations of values end with a substring.
    /// </summary>
    /// <param name="suffix">Suffix substring.</param>
    public Expr EndsWith(string suffix) => new(PolarsWrapper.CatEndsWith(_expr.CloneHandle(),suffix));
}