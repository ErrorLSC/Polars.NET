#pragma warning disable CS1591
using Polars.NET.Core;
using System.Linq;
using Polars.NET.Linq.Provider;
using System.Linq.Expressions;
using System.Reflection;

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
    public T MaterializeScalar<T>(DataFrameHandle handle)
    {
        var df = new DataFrame(handle);
        if (df.Height == 0 || df.Width == 0)
        {
            return default!;
        }

        // Retrieve scalar from row 0, column 0 without creating Arrow buffers
        return df.GetValue<T>(0L, df.ColumnNames[0])!;
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
    public static IQueryable<T> AsQueryable<T>(this LazyFrame lf)
        => new PolarsQuery<T>(lf.Handle, CSharpRowCursorMaterializer.Instance);
    
    /// <summary>
    /// Converts an eager DataFrame into an IQueryable LINQ query provider backed by LazyFrame.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this DataFrame df)
        => df.Lazy().AsQueryable<T>();
        
    /// <summary>
    /// Converts a DataFrame into an IQueryable LINQ query provider,
    /// inferring the anonymous type or entity type directly from a sample sequence (e.g. source array).
    /// </summary>
    /// <typeparam name="T">The inferred row element type.</typeparam>
    /// <param name="df">The source DataFrame.</param>
    /// <param name="source">The source collection used solely for static type inference.</param>
    /// <returns>A strongly-typed PolarsQuery provider.</returns>
    public static IQueryable<T> AsQueryable<T>(this DataFrame df, IEnumerable<T> source)
        => df.Lazy().AsQueryable<T>();

    /// <summary>
    /// Converts a LazyFrame into an IQueryable LINQ query provider,
    /// inferring the anonymous type or entity type directly from a sample sequence.
    /// </summary>
    public static IQueryable<T> AsQueryable<T>(this LazyFrame lf, IEnumerable<T> source)
        => lf.AsQueryable<T>();
    
    /// <summary>
    /// Compiles the LINQ query pipeline and converts it back to a Polars LazyFrame.
    /// </summary>
    public static LazyFrame ToLazyFrame<T>(this IQueryable<T> query)
    {
        if (query is PolarsQuery<T> polarsQuery)
        {
            var lfHandle = polarsQuery.CompileToLazyFrameHandle();
            return new LazyFrame(lfHandle);
        }

        throw new NotSupportedException("ToLazyFrame can only be invoked on queries originating from Polars.NET.");
    }

    /// <summary>
    /// Compiles the LINQ query pipeline, executes Collect, and returns the resulting eager DataFrame.
    /// </summary>
    public static DataFrame ToDataFrame<T>(this IQueryable<T> query)
    {
        if (query is PolarsQuery<T> polarsQuery)
        {
            var dfHandle = polarsQuery.CompileToDataFrameHandle();
            return new(dfHandle);
        }

        throw new NotSupportedException("ToDataFrame can only be invoked on queries originating from Polars.NET.");
    }
}
