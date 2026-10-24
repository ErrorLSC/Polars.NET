using System.Data;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.FSharp.Core;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Polars.NET.Linq;

namespace Polars.CSharp.Linq;

/// <summary>
/// Bridges Polars.NET.Core DataFrameHandle to the strongly-typed zero-allocation C# Row Cursor.
/// </summary>
internal sealed class CSharpRowCursorMaterializer : IDataFrameMaterializer
{
    public static readonly CSharpRowCursorMaterializer Instance = new();

    public IEnumerable<T> Materialize<T>(DataFrameHandle handle)
    {
        var df = new DataFrame(handle);

        // Return lazy, O(1) memory row stream
        return df.AsEnumerable<T>();
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
    public TAccum Aggregate<TSource, TAccum>(
        DataFrameHandle handle,
        TAccum seed,
        Func<TAccum, TSource, TAccum> folder)
    {
        var df = new DataFrame(handle);
        var enumerator = df.Rows<TSource>();

        TAccum acc = seed;
        while (enumerator.MoveNext())
        {
            acc = folder(acc, enumerator.Current);
        }
        return acc;
    }
    public IEnumerable<KeyValuePair<TKey, TAccum>> AggregateBy<TSource, TKey, TAccum>(
        DataFrameHandle handle,
        Func<TSource, TKey> keySelector,
        TAccum seed,
        Func<TAccum, TSource, TAccum> folder,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        var df = new DataFrame(handle);
        var dict = new Dictionary<TKey, TAccum>(comparer);

        var enumerator = df.Rows<TSource>();
        while (enumerator.MoveNext())
        {
            TSource current = enumerator.Current;
            TKey key = keySelector(current);

            ref TAccum? valRef = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out bool exists);
            if (!exists)
            {
                valRef = folder(seed, current);
            }
            else
            {
                valRef = folder(valRef!, current);
            }
        }

        return dict;
    }
    /// <summary>
    /// Aggregates rows grouped by key using a zero-allocation stack row enumerator and a per-key seed factory.
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, TAccum>> AggregateBy<TSource, TKey, TAccum>(
        DataFrameHandle handle,
        Func<TSource, TKey> keySelector,
        Func<TKey, TAccum> seedSelector,
        Func<TAccum, TSource, TAccum> folder,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(seedSelector);
        ArgumentNullException.ThrowIfNull(folder);

        var df = new DataFrame(handle);
        var dict = new Dictionary<TKey, TAccum>(comparer);

        if (df.Height == 0)
        {
            return dict;
        }

        // Stream rows using zero-heap ref struct enumerator
        var enumerator = df.Rows<TSource>();
        while (enumerator.MoveNext())
        {
            TSource current = enumerator.Current;
            TKey key = keySelector(current);

            ref TAccum? valRef = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out bool exists);
            if (!exists)
            {
                // Key first seen: generate initial seed via factory, then fold the first row
                TAccum initialSeed = seedSelector(key);
                valRef = folder(initialSeed, current);
            }
            else
            {
                // Key already exists: fold with the existing accumulated value
                valRef = folder(valRef!, current);
            }
        }

        return dict;
    }
    /// <summary>
    /// Slices the DataFrame into chunks of the specified size using a zero-allocation stack row enumerator.
    /// </summary>
    public IEnumerable<T[]> Chunk<T>(DataFrameHandle handle, int chunkSize)
    {
        if (chunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be greater than zero.");

        DataFrame df = new(handle);
        long totalRows = df.Height;

        if (totalRows == 0)
            yield break;

        // Iterate chunk by chunk to avoid CS4007 (ref struct across yield boundary)
        for (long offset = 0; offset < totalRows; offset += chunkSize)
        {
            int currentBatchSize = (int)Math.Min(chunkSize, totalRows - offset);
            yield return ReadChunkBatch<T>(df, offset, currentBatchSize);
        }

    }
    public long Count<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null)
    {
        var df = new DataFrame(handle);
        long height = df.Height;

        if (predicate == null)
            return height;

        if (height == 0)
            return 0L;

        var enumerator = df.Rows<TSource>();
        long count = 0L;

        while (enumerator.MoveNext())
        {
            if (predicate(enumerator.Current))
                count++;
        }

        return count;
    }

    /// <summary>
    /// Reads a single chunk batch directly into an array in a dedicated stack frame without crossing yield boundaries.
    /// </summary>
    private static T[] ReadChunkBatch<T>(DataFrame df, long offset, int count)
    {
        var chunkArray = new T[count];

        // Slice the DataFrame for the current batch or initialize enumerator for this range
        using var batchDf = df.Slice(offset, (ulong)count);
        var enumerator = batchDf.Rows<T>();

        int idx = 0;
        while (enumerator.MoveNext() && idx < count)
        {
            chunkArray[idx++] = enumerator.Current;
        }

        return chunkArray;
    }
    public bool Any<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null)
    {
        var df = new DataFrame(handle);
        if (df.Height == 0)
            return false;

        var enumerator = df.Rows<TSource>();

        // Case 1: Any() without predicate - simply checks if there is at least one row
        if (predicate == null)
            return enumerator.MoveNext();

        // Case 2: Any(predicate) - short-circuit scan on stack
        while (enumerator.MoveNext())
        {
            if (predicate(enumerator.Current))
                return true;
        }

        return false;
    }
    public bool All<TSource>(DataFrameHandle handle, Func<TSource, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var df = new DataFrame(handle);
        if (df.Height == 0)
            return true;

        var enumerator = df.Rows<TSource>();
        while (enumerator.MoveNext())
        {
            // Early exit if any row violates the predicate
            if (!predicate(enumerator.Current))
                return false;
        }

        return true;
    }
    public TSource First<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null)
    {
        var df = new DataFrame(handle);
        var enumerator = df.Rows<TSource>();

        if (predicate == null)
        {
            return enumerator.First();
        }

        while (enumerator.MoveNext())
        {
            if (predicate(enumerator.Current))
                return enumerator.Current;
        }

        throw new InvalidOperationException("Sequence contains no matching element.");
    }

    public TSource? FirstOrDefault<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null, TSource? defaultValue = default)
    {
        var df = new DataFrame(handle);
        var enumerator = df.Rows<TSource>();

        if (predicate == null)
        {
            return enumerator.FirstOrDefault(defaultValue);
        }

        while (enumerator.MoveNext())
        {
            if (predicate(enumerator.Current))
                return enumerator.Current;
        }

        return defaultValue;
    }
    public TSource Last<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null)
    {
        var df = new DataFrame(handle);

        var enumerator = df.Rows<TSource>();

        if (predicate == null)
        {
            return enumerator.Last();
        }

        bool found = false;
        TSource lastMatch = default!;

        while (enumerator.MoveNext())
        {
            if (predicate(enumerator.Current))
            {
                lastMatch = enumerator.Current;
                found = true;
            }
        }

        if (found)
            return lastMatch;

        throw new InvalidOperationException("Sequence contains no matching element.");
    }

    public TSource? LastOrDefault<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null, TSource? defaultValue = default)
    {
        var df = new DataFrame(handle);

        var enumerator = df.Rows<TSource>();

        if (predicate == null)
        {
            return enumerator.LastOrDefault(defaultValue);
        }

        bool found = false;
        TSource? lastMatch = defaultValue;

        while (enumerator.MoveNext())
        {
            if (predicate(enumerator.Current))
            {
                lastMatch = enumerator.Current;
                found = true;
            }
        }

        return found ? lastMatch : defaultValue;
    }
    public TSource Single<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null)
    {
        var df = new DataFrame(handle);
        long height = df.Height;

        if (predicate == null)
        {
            if (height == 0)
                throw new InvalidOperationException("Sequence contains no elements.");
            if (height > 1)
                throw new InvalidOperationException("Sequence contains more than one element.");

            var enumerator = df.Rows<TSource>();
            return enumerator.ElementAt(0);
        }

        if (height == 0)
            throw new InvalidOperationException("Sequence contains no matching element.");

        var scanEnumerator = df.Rows<TSource>();
        bool found = false;
        TSource match = default!;

        while (scanEnumerator.MoveNext())
        {
            if (predicate(scanEnumerator.Current))
            {
                if (found)
                    throw new InvalidOperationException("Sequence contains more than one matching element.");

                match = scanEnumerator.Current;
                found = true;
            }
        }

        if (found)
            return match;

        throw new InvalidOperationException("Sequence contains no matching element.");
    }

    public TSource? SingleOrDefault<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null, TSource? defaultValue = default)
    {
        var df = new DataFrame(handle);
        long height = df.Height;

        if (predicate == null)
        {
            if (height == 0)
                return defaultValue;
            if (height > 1)
                throw new InvalidOperationException("Sequence contains more than one element.");

            var enumerator = df.Rows<TSource>();
            return enumerator.ElementAt(0);
        }

        if (height == 0)
            return defaultValue;

        var scanEnumerator = df.Rows<TSource>();
        bool found = false;
        TSource? match = defaultValue;

        while (scanEnumerator.MoveNext())
        {
            if (predicate(scanEnumerator.Current))
            {
                if (found)
                    throw new InvalidOperationException("Sequence contains more than one matching element.");

                match = scanEnumerator.Current;
                found = true;
            }
        }

        return found ? match : defaultValue;
    }
    public TSource ElementAt<TSource>(DataFrameHandle handle, long index)
    {
        if (index < 0)
            throw new ArgumentOutOfRangeException(nameof(index), "Index was out of range.");

        var df = new DataFrame(handle);

        var enumerator = df.Rows<TSource>();
        return enumerator.ElementAt(index);
    }

    public TSource? ElementAtOrDefault<TSource>(DataFrameHandle handle, long index)
    {
        if (index < 0)
            return default;

        var df = new DataFrame(handle);

        var enumerator = df.Rows<TSource>();
        return enumerator.ElementAtOrDefault(index);
    }
    public bool Contains<TSource>(DataFrameHandle handle, TSource item, IEqualityComparer<TSource>? comparer = null)
    {
        var df = new DataFrame(handle);
        if (df.Height == 0)
            return false;

        var comp = comparer ?? EqualityComparer<TSource>.Default;
        var enumerator = df.Rows<TSource>();

        while (enumerator.MoveNext())
        {
            if (comp.Equals(enumerator.Current, item))
                return true;
        }

        return false;
    }
    public bool SequenceEqual<TSource>(DataFrameHandle handle1, DataFrameHandle handle2, IEqualityComparer<TSource>? comparer = null)
    {
        var df1 = new DataFrame(handle1);
        var df2 = new DataFrame(handle2);

        if (df1.Height != df2.Height)
            return false;

        if (df1.Height == 0)
            return true;

        var comp = comparer ?? EqualityComparer<TSource>.Default;

        var enum1 = df1.Rows<TSource>();
        var enum2 = df2.Rows<TSource>();

        while (enum1.MoveNext() && enum2.MoveNext())
        {
            if (!comp.Equals(enum1.Current, enum2.Current))
                return false;
        }

        return true;
    }

    public bool SequenceEqual<TSource>(DataFrameHandle handle, IEnumerable<TSource> second, IEqualityComparer<TSource>? comparer = null)
    {
        ArgumentNullException.ThrowIfNull(second);

        var df = new DataFrame(handle);
        var comp = comparer ?? EqualityComparer<TSource>.Default;

        if (second is ICollection<TSource> col && df.Height != col.Count)
            return false;
        if (second is IReadOnlyCollection<TSource> rCol && df.Height != rCol.Count)
            return false;

        var enum1 = df.Rows<TSource>();
        using var enum2 = second.GetEnumerator();

        while (enum1.MoveNext())
        {
            if (!enum2.MoveNext() || !comp.Equals(enum1.Current, enum2.Current))
                return false;
        }

        return !enum2.MoveNext();
    }
    private static TSource HandleEmptySequence<TSource>()
    {
        if (PolarsTypeHelper.AcceptsNull(typeof(TSource)))
            return default!;

        throw new InvalidOperationException("Sequence contains no elements.");
    }

    public TSource MinBy<TSource, TKey>(DataFrameHandle handle, Func<TSource, TKey> keySelector, IComparer<TKey>? comparer = null)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        var df = new DataFrame(handle);
        if (df.Height == 0)
            return HandleEmptySequence<TSource>();

        var comp = comparer ?? Comparer<TKey>.Default;
        var enumerator = df.Rows<TSource>();

        if (!enumerator.MoveNext())
            return HandleEmptySequence<TSource>();

        var minRow = enumerator.Current;
        var minKey = keySelector(minRow);

        while (enumerator.MoveNext())
        {
            var curRow = enumerator.Current;
            var curKey = keySelector(curRow);

            if (comp.Compare(curKey, minKey) < 0)
            {
                minKey = curKey;
                minRow = curRow;
            }
        }

        return minRow;
    }

    public TSource MaxBy<TSource, TKey>(DataFrameHandle handle, Func<TSource, TKey> keySelector, IComparer<TKey>? comparer = null)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        var df = new DataFrame(handle);
        if (df.Height == 0)
            return HandleEmptySequence<TSource>();

        var comp = comparer ?? Comparer<TKey>.Default;
        var enumerator = df.Rows<TSource>();

        if (!enumerator.MoveNext())
            return HandleEmptySequence<TSource>();

        var maxRow = enumerator.Current;
        var maxKey = keySelector(maxRow);

        while (enumerator.MoveNext())
        {
            var curRow = enumerator.Current;
            var curKey = keySelector(curRow);

            if (comp.Compare(curKey, maxKey) > 0)
            {
                maxKey = curKey;
                maxRow = curRow;
            }
        }

        return maxRow;
    }
}

internal static class MarkerExceptionHelper
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static InvalidOperationException Throw()
    {
        var frame = new System.Diagnostics.StackFrame(1);
        string methodName = frame.GetMethod()?.Name ?? "Marker";

        string message =
            $"The Polars LINQ method '{methodName}' is an expression marker and cannot be evaluated on client-side collections.\n" +
            "Cause: An earlier operation in the query could not be translated natively to Polars, causing the query to fall back to client-side evaluation.\n" +
            "Fix: Ensure native Polars operations run first, or isolate unsupported client-side methods by materializing data first (e.g., calling .ToList() / .ToDataFrame()).";

        return new InvalidOperationException(message);
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
    /// Converts a Series into an IQueryable LINQ query provider backed by Polars.
    /// For Struct Series, it is unnested into a multi-column DataFrame matching the struct fields.
    /// For scalar and other Series, it is converted into a single-column DataFrame.
    /// </summary>
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
    public static IQueryable<T> AsQueryable<T>(this Series series, IEnumerable<T> source)
        => series.AsQueryable<T>();

    /// <summary>
    /// Exposes a Series as a strongly-typed IEnumerable sequence of elements.
    /// For Struct Series, elements are materialized into objects matching the struct fields.
    /// For scalar Series, elements are materialized directly.
    /// </summary>
    public static IEnumerable<T> AsEnumerable<T>(this Series series)
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
    public static LazyFrame ToLazyFrame<T>(this IQueryable<T> query)
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
    public static DataFrame ToDataFrame<T>(this IEnumerable<T> source)
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
    public static LazyFrame ToLazyFrame<T>(this IEnumerable<T> source)
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
    public static Series ToSeries<T>(this IEnumerable<T> source, string name = "")
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
    public static Series ToSeries<T>(this IQueryable<T> query, string name = "")
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
    public static DataFrame ToDataFrame<T>(this IQueryable<T> query)
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
    /// <inheritdoc cref="System.Linq.Enumerable.ToLookup{TSource, TKey}(IEnumerable{TSource}, Func{TSource, TKey}, IEqualityComparer{TKey})"/>
    public static PolarsLookup<TKey, TSource> ToLookup<TSource, TKey>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, TKey>> keySelector,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
        => source.ToLookup(keySelector, e => e, comparer);

    /// <summary>
    /// Converts a queryable sequence into a PolarsLookup, grouping elements by a specified key selector and selecting elements with a specified selector.
    /// </summary>
    /// <typeparam name="TSource">The type of the source elements.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TElement">The type of the element.</typeparam>
    /// <param name="source">The queryable sequence to convert.</param>
    /// <param name="keySelector">The function to extract the key from each element.</param>
    /// <param name="elementSelector">The function to extract the element from each element.</param>
    /// <param name="comparer">The equality comparer to use for the keys.</param>
    /// <returns>The resulting PolarsLookup.</returns>
    public static PolarsLookup<TKey, TElement> ToLookup<TSource, TKey, TElement>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, TKey>> keySelector,
        Expression<Func<TSource, TElement>> elementSelector,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(elementSelector);

        // 1. Evaluate preceding LINQ pipeline down to a DataFrame
        var df = source.ToDataFrame();

        // 2. Resolve whether TSource is a scalar column or composite entity
        bool isScalarSource = PolarsTypeHelper.IsScalarType(typeof(TSource)) || typeof(TSource) == typeof(string);
        string? scalarCol = isScalarSource && df.Columns.Length == 1 ? df.Columns[0] : null;

        TranslationContext CreateContext(string paramName) =>
            scalarCol is not null
                ? TranslationContext.NewScalar(paramName, scalarCol)
                : TranslationContext.NewEntity(paramName);

        // 3. Fold closures and push down key selector using TranslationContext
        var evaluatedKeyBody = PartialEvaluator.eval(keySelector.Body);
        var keyParam = keySelector.Parameters[0].Name ?? "x";
        var keyCtx = CreateContext(keyParam);

        var keyOpt = ExprTranslator.tryTranslateWithContext(keyCtx, evaluatedKeyBody);
        if (!FSharpOption<ExprHandle>.get_IsSome(keyOpt))
        {
            throw new NotSupportedException($"Key selector '{keySelector}' could not be translated to a Polars expression.");
        }
        var keyExpr = new Expr(keyOpt.Value);

        // 4. Native partition via Rust engine directly to DataFrame[]
        var partitions = df.PartitionBy(keyExpr, maintainOrder: true, includeKey: true);

        // 5. Translate element selector if not identity
        var isIdentity = typeof(TSource) == typeof(TElement) && elementSelector.Body is ParameterExpression;
        Expr? elemExpr = null;
        if (!isIdentity)
        {
            var evaluatedElemBody = PartialEvaluator.eval(elementSelector.Body);
            var elemParam = elementSelector.Parameters[0].Name ?? "x";
            var elemCtx = CreateContext(elemParam);

            var elemOpt = ExprTranslator.tryTranslateWithContext(elemCtx, evaluatedElemBody);
            if (FSharpOption<ExprHandle>.get_IsSome(elemOpt))
            {
                elemExpr = new Expr(elemOpt.Value);
            }
        }

        // 6. Pre-allocate exactly one dictionary
        var groupDict = new Dictionary<TKey, PolarsGrouping<TKey, TElement>>(
            partitions.Length,
            comparer ?? EqualityComparer<TKey>.Default);

        // Fast-path: Resolve key column name from simple member access or scalar parameter
        var body = keySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
        {
            body = u.Operand;
        }

        string? singleKeyCol = body switch
        {
            MemberExpression me when me.Expression is ParameterExpression => me.Member.Name,
            ParameterExpression when scalarCol is not null => scalarCol, // Direct scalar key mapping
            _ => null
        };

        Func<DataFrame, long, TKey>? compositeKeyMapper = null;

        foreach (var partDf in partitions)
        {
            if (partDf.Height == 0L) continue;

            TKey key;
            if (singleKeyCol is not null)
            {
                // O(1) direct scalar read from Row 0 without object[] allocations
                using var col = partDf[singleKeyCol];
                key = ConvertKey<TKey>(col.GetValue<object>(0L, uncheck: true)!);
            }
            else
            {
                // Composite key fallback: compile RowMapper once
                compositeKeyMapper ??= RowMapper<TKey>.GetOrCreate(partDf);
                key = compositeKeyMapper(partDf, 0L);
            }

            var finalDf = elemExpr is not null ? partDf.Select(elemExpr) : partDf;
            groupDict[key] = new PolarsGrouping<TKey, TElement>(key, finalDf);
        }

        return new PolarsLookup<TKey, TElement>(groupDict);
    }

    private static TKey ConvertKey<TKey>(object rawKey)
    {
        if (rawKey is TKey exact)
        {
            return exact;
        }

        var targetType = typeof(TKey);
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Guarantee enum safety (e.g. DayOfWeek, StringComparison)
        if (underlying.IsEnum)
        {
            return (TKey)Enum.ToObject(underlying, rawKey);
        }

        return (TKey)Convert.ChangeType(rawKey, underlying);
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

    /// <summary>
    /// Convert DataReader to Polars Queryable
    /// </summary>
    /// <typeparam name="T">Target Type</typeparam>
    public static IQueryable<T> AsPolarsQueryable<T>(this IDataReader reader, int batchSize = 50000)
        => DataFrame.ReadDatabase(reader, batchSize).AsQueryable<T>();

    /// <summary>
    /// Transposes an in-memory sequence into a native Polars LazyFrame and exposes it as a Polars LINQ queryable.
    /// </summary>
    /// <typeparam name="T">Row record type</typeparam>
    /// <param name="source">The in-memory collection</param>
    /// <returns>A queryable supported by the Polars native execution engine</returns>
    public static IQueryable<T> AsPolarsQueryable<T>(this IEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.ToLazyFrame().AsQueryable<T>();
    }

    /// <summary>
    /// Returns the Polars native query execution plan, including warnings if parts of the query
    /// fallback to managed client-side execution.
    /// </summary>
    /// <param name="source">The source queryable.</param>
    /// <param name="optimized">Whether to display the Polars optimized logical plan or the raw plan.</param>
    public static string Explain<T>(this IQueryable<T> source, bool optimized = true)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IPolarsExplainable explainable)
        {
            return explainable.Explain(optimized);
        }

        return "[Polars.NET] Query source is not a native PolarsQueryable. In-memory execution only.";
    }
    /// <summary>
    /// Gets or sets the default plan inspector action used when none is explicitly specified.
    /// Defaults to <see cref="Console.WriteLine(string)"/>.
    /// </summary>
    public static Action<string> DefaultInspector { get; set; } = Console.WriteLine;
    /// <summary>
    /// Inspects the query plan using the default inspector (<see cref="DefaultInspector"/>)
    /// </summary>
    /// <param name="source">The source queryable.</param>
    /// <param name="optimized">Whether to display the optimized plan. Defaults to true.</param>
    public static IQueryable<T> InspectPlan<T>(
        this IQueryable<T> source,
        bool optimized = true)
        => source.InspectPlan(DefaultInspector, optimized);

    /// <summary>
    /// Inspects the query plan at the current pipeline stage and invokes a custom inspector action,
    /// </summary>
    /// <param name="source">The source queryable.</param>
    /// <param name="inspector">Action receiving the diagnostic plan string.</param>
    /// <param name="optimized">Whether to display the optimized plan. Defaults to true.</param>
    public static IQueryable<T> InspectPlan<T>(
        this IQueryable<T> source,
        Action<string> inspector,
        bool optimized = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(inspector);

        inspector(source.Explain(optimized));
        return source;
    }
    /// <summary>
    /// Shifts the values in a column by a specified offset, introducing nulls for the shifted positions.
    /// </summary>
    /// <typeparam name="T">The type of the column.</typeparam>
    /// <param name="column">The column to shift.</param>
    /// <param name="offset">The offset by which to shift the values.</param>
    /// <returns>The shifted column.</returns>
    public static T Shift<T>(this T column, long offset)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Shift{T}(T,long)"/>
    public static T Shift<T, TOffset>(this T column, TOffset offsetColumn)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Diff{T}(T,long,NullBehavior)"/>
    public static T Diff<T>(this T column)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Diff{T}(T,long,NullBehavior)"/>
    public static T Diff<T>(this T column, long n)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the difference between the current value and the value n periods ago, with a specified null behavior.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="column">The column to compute differences for.</param>
    /// <param name="n">The number of periods to look back.</param>
    /// <param name="nullBehavior">The behavior for handling null values.</param>
    /// <returns>The column with differences computed.</returns>
    public static T Diff<T>(this T column, long n, NullBehavior nullBehavior)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Over{T, TPartition, TOrder}(T, TPartition, TOrder, bool)"/>
    public static T Over<T, TPartition>(this T expr, TPartition partitionBy)
        => throw MarkerExceptionHelper.Throw();
    /// <inheritdoc cref="Over{T, TPartition, TOrder}(T, TPartition, TOrder, bool)"/>
    public static T Over<T>(this T expr, params object[] partitionBy)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes a windowed expression over a specified partition and order, with an optional descending order.
    /// </summary>
    /// <typeparam name="T">The type of the expression.</typeparam>
    /// <typeparam name="TPartition">The type of the partitioning column(s).</typeparam>
    /// <typeparam name="TOrder">The type of the ordering column(s).</typeparam>
    /// <param name="expr">The expression to compute the windowed operation for.</param>
    /// <param name="partitionBy">The column(s) to partition by.</param>
    /// <param name="orderBy">The column(s) to order by.</param>
    /// <param name="descending">Indicates whether to sort in descending order.</param>
    /// <returns>The column with the windowed expression computed.</returns>
    public static T Over<T, TPartition, TOrder>(
        this T expr,
        TPartition partitionBy,
        TOrder orderBy,
        bool descending = false)
        => throw MarkerExceptionHelper.Throw();
        
    /// <summary>
    /// Computes the rank of values using the default Average method.
    /// </summary>
    public static double Rank<T>(this T column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the rank of values with a specified sorting order.
    /// </summary>
    public static double Rank<T>(this T column, bool descending)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the rank of values using user-facing RankMethod.
    /// </summary>
    public static double Rank<T>(this T column, RankMethod method, bool descending = false)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the rank of values using user-facing RankMethod with seed.
    /// </summary>
    public static double Rank<T>(this T column, RankMethod method, bool descending, ulong? seed)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the percentage change between current and previous values.
    /// </summary>
    public static double? PctChange<T>(this T column, long n = 1)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the mean of the column.
    /// </summary>
    public static double Mean<T>(this T column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the sum of the column.
    /// </summary>
    public static T Sum<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the sum of the nullable column.
    /// </summary>
    public static T? Sum<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the mean of the nullable column.
    /// </summary>
    public static double Mean<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the average of the column.
    /// </summary>
    public static double Average<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the average of the nullable column.
    /// </summary>
    public static double Average<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Counts the number of non-null values in the column.
    /// </summary>
    public static int Count<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Counts the number of non-null values in the nullable column.
    /// </summary>
    public static int Count<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Counts the number of non-null values in the column.
    /// </summary>
    public static int Count(this string? column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Finds the minimum value in the column.
    /// </summary>
    public static T Min<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the minimum value in the nullable column.
    /// </summary>
    public static T? Min<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the minimum value in the string column.
    /// </summary>
    public static string? Min(this string? column)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the maximum value in the column.
    /// </summary>
    public static T Max<T>(this T column) where T : struct
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Finds the maximum value in the nullable column.
    /// </summary>
    public static T? Max<T>(this T? column) where T : struct
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Finds the maximum value in the string column.
    /// </summary>
    public static string? Max(this string? column)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the sample standard deviation (ddof = 1 by default).
    /// </summary>
    public static double Std<T>(this T column, byte ddof = 1)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the sample variance (ddof = 1 by default).
    /// </summary>
    public static double Var<T>(this T column, byte ddof = 1)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative sum of the column.
    /// </summary>
    public static T CumSum<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative maximum of the column.
    /// </summary>
    public static T CumMax<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative minimum of the column.
    /// </summary>
    public static T CumMin<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative product of the column.
    /// </summary>
    public static T CumProd<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the cumulative count of the column.
    /// </summary>
    public static long CumCount<T>(this T column, bool reverse = false)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Interpolates null values using the specified user-facing InterpolationMethod (defaults to Linear).
    /// </summary>
    public static T Interpolate<T>(this T column, InterpolationMethod method = InterpolationMethod.Linear)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Interpolates null values based on the values of another column (e.g., timestamps or indices).
    /// </summary>
    public static T InterpolateBy<T, TBy>(this T column, TBy byColumn)
        => throw MarkerExceptionHelper.Throw();
    /// <summary>
    /// Computes the median value of the column.
    /// </summary>
    public static double? Median<T>(this T column)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes the quantile of the column with the specified probability and method.
    /// </summary>
    public static double? Quantile<T>(
        this T column,
        double quantile,
        QuantileMethod method = QuantileMethod.Nearest)
        => throw MarkerExceptionHelper.Throw();

}

/// <summary>
/// Provides LINQ IEnumerable extensions for C# Polars DataFrame and LazyFrame.
/// </summary>
public static class GroupAggregationExtensions
{
    /// <summary>
    /// Computes sample standard deviation of a group by selector with default ddof = 1.
    /// </summary>
    public static double Std<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes standard deviation of a group by selector with explicit degree of freedom (ddof).
    /// </summary>
    public static double Std<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        byte ddof)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes sample variance of a group by selector with default ddof = 1.
    /// </summary>
    public static double Var<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes variance of a group by selector with explicit degree of freedom (ddof).
    /// </summary>
    public static double Var<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        byte ddof)
        => throw MarkerExceptionHelper.Throw();
}
