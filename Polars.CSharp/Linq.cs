#pragma warning disable CS1591
using Polars.NET.Core;
using Polars.NET.Linq.Provider;

namespace Polars.CSharp.Linq;

/// <summary>
/// Bridges Polars.NET.Core DataFrameHandle to the strongly-typed zero-allocation C# Row Cursor.
/// </summary>
public sealed class CSharpRowCursorMaterializer : IDataFrameMaterializer
{
    public static readonly CSharpRowCursorMaterializer Instance = new();

    public IEnumerable<T> Materialize<T>(DataFrameHandle handle)
    {
        // Wrap DataFrameHandle into C# DataFrame wrapper
        var df = new DataFrame(handle);

        // Consume duck-typed DataFrameRowEnumerator<T>
        return df.Rows<T>().ToList();
    }
}

/// <summary>
/// Provides LINQ IQueryable extensions for C# Polars DataFrame and LazyFrame.
/// </summary>
public static class LinqExtensions
{
    /// <summary>
    /// Converts a LazyFrame into an IQueryable LINQ query provider.
    /// Operations will be translated to Polars Expressions and compiled into execution plans.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this LazyFrame lazyFrame)
    {
        return new PolarsQuery<T>(lazyFrame.Handle, CSharpRowCursorMaterializer.Instance);
    }

    /// <summary>
    /// Converts an eager DataFrame into an IQueryable LINQ query provider backed by LazyFrame.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this DataFrame dataFrame)
    {
        return dataFrame.Lazy().AsQueryable<T>();
    }
}
