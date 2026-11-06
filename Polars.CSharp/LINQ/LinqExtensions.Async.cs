using Polars.NET.Core.Helpers;
using Polars.NET.Linq.Backend.Polars;

namespace Polars.CSharp.Linq;

public static partial class LinqExtensions
{
    /// <summary>
    /// Asynchronously compiles and executes the query, materializing the result directly into a native Polars DataFrame.
    /// </summary>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="engine">Execution engine selection (Auto, InMemory, Streaming, or Gpu).</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>A task yielding the resulting DataFrame.</returns>
    public static async Task<DataFrame> ToDataFrameAsync<T>(
        this IQueryable<T> source,
        Engine engine = Engine.Auto,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        // 1. Fast-path: Directly extract compiled plan without hydrating any C# row objects
        if (source is IPolarsPlanSource planSource)
        {
            var lfHandle = planSource.GetCompiledLazyFrameHandle();
            using var lazyDf = new LazyFrame(lfHandle);
            return await lazyDf.CollectAsync(engine, cancellationToken).ConfigureAwait(false);
        }

        // 2. Fallback path A: Stream asynchronously if the source supports IAsyncEnumerable
        if (source is IAsyncEnumerable<T> asyncStream)
        {
            var rows = new List<T>();
            await foreach (var row in asyncStream.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                rows.Add(row);
            }
            var dfHandle = DataFrameBuilder.FromRows(rows);
            return new(dfHandle);
        }

        // 3. Fallback path B: Synchronous in-memory LINQ offloaded to thread pool
        return await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rows = source.ToList();
            var dfHandle = DataFrameBuilder.FromRows(rows);
            return new DataFrame(dfHandle);
        }, cancellationToken).ConfigureAwait(false);
    }
    /// <summary>
    /// Asynchronously drains an IAsyncEnumerable sequence of objects directly into a Polars DataFrame.
    /// </summary>
    /// <typeparam name="T">The record, struct, or class type.</typeparam>
    /// <param name="source">The asynchronous stream of objects.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>A task yielding the resulting DataFrame.</returns>
    public static async Task<DataFrame> ToDataFrameAsync<T>(
        this IAsyncEnumerable<T> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        var rows = new List<T>();
        await foreach (var row in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(row);
        }

        var dfHandle = DataFrameBuilder.FromRows(rows);
        return new(dfHandle);
    }
    /// <summary>
    /// Returns the input typed as IAsyncEnumerable for streaming async enumeration.
    /// </summary>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IAsyncEnumerable<T> asyncEnumerable)
        {
            return asyncEnumerable;
        }

        throw new InvalidOperationException(
            $"The source IQueryable does not implement IAsyncEnumerable<{typeof(T).Name}>.");
    }

    /// <summary>
    /// Asynchronously enumerates the query and materializes all results into a List.
    /// </summary>
    public static async Task<List<T>> ToListAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        var list = new List<T>();
        await foreach (var item in source.AsAsyncEnumerable().WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            list.Add(item);
        }
        return list;
    }
    /// <summary>
    /// Asynchronously streams the query results and materializes them into an Array.
    /// </summary>
    public static async Task<T[]> ToArrayAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
        => [.. await source.ToListAsync(cancellationToken).ConfigureAwait(false)];
}
