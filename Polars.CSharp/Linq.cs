#pragma warning disable CS1591
using Polars.NET.Core;
using System.Linq;
using Polars.NET.Linq.Provider;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;

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
        where TKey :notnull
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

        var df = new DataFrame(handle);
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
