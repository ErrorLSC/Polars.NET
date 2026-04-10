#pragma warning disable CS1591
using System.Runtime.CompilerServices;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;
namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
/// </summary>
public readonly partial struct Polars
{
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// Example: Pl.DataFrame(Pl.Series("a", new[] {1, 2}), Pl.Series("b", new[] {3, 4}))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(params Series[] series)
        => [.. series];

    /// <summary>
    /// Create a DataFrame from a collection of strongly-typed objects (POCOs).
    /// Example: Pl.DataFrame(studentList)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame<T>(IEnumerable<T> data)
        => CSharp.DataFrame.FromRows(data);

    /// <summary>
    /// Create a DataFrame from an anonymous object where properties represent columns.
    /// Example: Pl.DataFrame(new { A = new[] { 1, 2 }, B = new[] { "x", "y" } })
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(object columns)
        => CSharp.DataFrame.FromColumns(columns);

    /// <summary>
    /// Create a DataFrame from explicitly named column tuples.
    /// Example: Pl.DataFrame(("A", new[] { 1, 2 }), ("B", new[] { "x", "y" }))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataFrame DataFrame(params (string Name, object Data)[] columns)
        => CSharp.DataFrame.FromColumns(columns);

    /// <summary>
    /// Create a Series from an IEnumerable of objects, primitives, or nested lists.
    /// Example: Pl.Series("Name", list)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, IEnumerable<T> data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series directly from an array (Fast Path).
    /// Example: Pl.Series("Age", new int[] { 25, 30 })
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, T[] data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series from a 2D matrix.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series<T>(string name, T[,] data)
        => CSharp.Series.From(name, data);

    /// <summary>
    /// Create a Series from a ReadOnlySpan (Zero allocation path).
    /// </summary>
    public static Series Series<T>(string name, ReadOnlySpan<T> data)
        => CSharp.Series.FromSpan(name, data);

    /// <summary>
    /// Materialize a single logical Expression into a physical Series.
    /// Example: Pl.Series(Pl.Lit(42).RepeatBy(5))
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Series Series(Expr expr)
        => CSharp.Series.FromExpr(expr);
}