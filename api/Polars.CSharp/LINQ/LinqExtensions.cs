using System.Data;
using System.Diagnostics.CodeAnalysis;
using Polars.NET.Core;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Linq;

/// <summary>
/// Provides LINQ IQueryable extensions for C# Polars DataFrame and LazyFrame.
/// </summary>
public static partial class LinqExtensions
{
    /// <summary>
    /// Converts a LazyFrame into an IQueryable LINQ query provider.
    /// Operations will be translated to Polars Expressions and compiled into execution plans.
    /// </summary>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsQueryable<T>(this LazyFrame lf)
        => new PolarsQuery<T>(lf.Handle, CSharpRowCursorMaterializer.Instance, QueryLanguage.CSharp);

    /// <summary>
    /// Converts an eager DataFrame into an IQueryable LINQ query provider backed by LazyFrame.
    /// </summary>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
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
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsQueryable<T>(this DataFrame df, IEnumerable<T> source)
        => df.Lazy().AsQueryable<T>();

    /// <summary>
    /// Converts a LazyFrame into an IQueryable LINQ query provider,
    /// inferring the anonymous type or entity type directly from a sample sequence.
    /// </summary>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsQueryable<T>(this LazyFrame lf, IEnumerable<T> source)
        => lf.AsQueryable<T>();

    /// <summary>
    /// Converts a Series into an IQueryable LINQ query provider backed by Polars.
    /// For Struct Series, it is unnested into a multi-column DataFrame matching the struct fields.
    /// For scalar and other Series, it is converted into a single-column DataFrame.
    /// </summary>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsQueryable<T>(this Series series)
    {
        ArgumentNullException.ThrowIfNull(series);
        if (series.DataType.Kind == DataTypeKind.Struct)
        {
            return series.Unnest().AsQueryable<T>();
        }
        return series.ToFrame().AsQueryable<T>();
    }

    /// <summary>
    /// Converts a Series into an IQueryable LINQ query provider,
    /// inferring the anonymous type or entity type directly from a sample sequence.
    /// </summary>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsQueryable<T>(this Series series, IEnumerable<T> source)
        => series.AsQueryable<T>();

    /// <summary>
    /// Exposes a Series as a strongly-typed IEnumerable sequence of elements.
    /// For Struct Series, elements are materialized into objects matching the struct fields.
    /// For scalar Series, elements are materialized directly.
    /// </summary>
    public static IEnumerable<T> AsEnumerable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this Series series)
    {
        ArgumentNullException.ThrowIfNull(series);
        if (series.DataType.Kind == DataTypeKind.Struct)
        {
            return series.Unnest().AsEnumerable<T>();
        }
        return series.ToFrame().AsEnumerable<T>();
    }

    /// <summary>
    /// Compiles the LINQ query pipeline and converts it back to a Polars LazyFrame.
    /// </summary>
    public static LazyFrame ToLazyFrame<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this IQueryable<T> query)
    {
        if (query is PolarsQuery<T> polarsQuery)
        {
            var lfHandle = polarsQuery.CompileToLazyFrameHandle();
            return new(lfHandle);
        }

        // Convert other LINQ query to DataFrame then LazyFrame
        return DataFrame.FromRows([.. query]).Lazy();
    }

    /// <summary>
    /// Materializes an in-memory sequence of entities into an eager DataFrame.
    /// If the sequence is an IQueryable, native query execution and compilation is prioritized.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The sequence of elements.</param>
    /// <returns>A new DataFrame containing the elements.</returns>
    public static DataFrame ToDataFrame<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this IEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IQueryable<T> query)
            return query.ToDataFrame();

        return DataFrame.FromRows(source);
    }

    /// <summary>
    /// Converts an in-memory sequence of entities into a LazyFrame.
    /// If the sequence is an IQueryable, native query execution and compilation is prioritized.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The sequence of elements.</param>
    /// <returns>A new LazyFrame containing the elements.</returns>
    public static LazyFrame ToLazyFrame<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this IEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IQueryable<T> query)
            return query.ToLazyFrame();

        return DataFrame.FromRows(source).Lazy();
    }

    /// <summary>
    /// Materializes an in-memory sequence of entities into a Polars Series.
    /// Object collections(non-scalar collections) will be converted into a Struct Series. 
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The sequence or query of elements.</param>
    /// <param name="name">Optional name for the Series. Defaults to empty string.</param>
    /// <returns>A new Series containing the materialized values.</returns>
    public static Series ToSeries<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this IEnumerable<T> source, string name = "")
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IQueryable<T> query)
            return query.ToSeries(name);

        return Series.From(name, source);
    }

    /// <summary>
    /// Compiles a LINQ query and materializes it into a Polars Series.
    /// If the query yields a single column, returns that column directly.
    /// If the query yields multiple columns, assembles them into a native Struct Series.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="query">The query sequence.</param>
    /// <param name="name">Optional override name for the Series. If empty, the projected column name is preserved.</param>
    /// <returns>A new Series containing the query result.</returns>
    public static Series ToSeries<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this IQueryable<T> query, string name = "")
    {
        ArgumentNullException.ThrowIfNull(query);

        using var df = query.ToDataFrame();
        if (df.Width == 0)
            return Series.From(name, Array.Empty<T>());

        if (df.Width == 1)
        {
            var series = df.Column(0);
            if (!string.IsNullOrEmpty(name) && series.Name != name)
            {
                series.Rename(name);
            }
            return series;
        }

        return df.ToStruct(name);
    }

    /// <summary>
    /// Compiles the LINQ query pipeline, executes Collect, and returns the resulting eager DataFrame.
    /// </summary>
    public static DataFrame ToDataFrame<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(this IQueryable<T> query)
    {
        if (query is PolarsQuery<T> polarsQuery)
        {
            var dfHandle = polarsQuery.CompileToDataFrameHandle();
            return new(dfHandle);
        }

        // Convert other LINQ query to DataFrame
        return DataFrame.FromRows([.. query]);
    }

    /// <summary>
    /// Convert DataReader to Polars Queryable
    /// </summary>
    /// <typeparam name="T">Target Type</typeparam>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsPolarsQueryable<T>(this IDataReader reader, int batchSize = 50000)
        => DataFrame.ReadDatabase(reader, batchSize).AsQueryable<T>();

    /// <summary>
    /// Transposes an in-memory sequence into a native Polars LazyFrame and exposes it as a Polars LINQ queryable.
    /// </summary>
    /// <typeparam name="T">Row record type</typeparam>
    /// <param name="source">The in-memory collection</param>
    /// <returns>A queryable supported by the Polars native execution engine</returns>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public static IQueryable<T> AsPolarsQueryable<T>(this IEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.ToLazyFrame().AsQueryable<T>();
    }
}
