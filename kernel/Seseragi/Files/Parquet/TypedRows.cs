using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Samidare;
using Kaminari.Samidare.Config;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Data.Mapping;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// What a typed read (<see cref="ParquetReader{T}"/>, <see cref="ParquetDatasetReader{T}"/>) asks of its batch reader,
/// and the conjuncts it evaluates on the rows itself.
/// </summary>
/// <param name="Options">The batch reader's options: the members' columns, the predicate's pushed conjuncts as the filter.</param>
/// <param name="Client">The conjuncts evaluated on the rows (null: none).</param>
/// <param name="MaxRows">The rows to deliver, counted on the rows when the client evaluates conjuncts (else the batch reader limits).</param>
/// <param name="Exact">Whether the options' own filter (with a predicate) is evaluated exactly; null without a predicate (the batch reader tells).</param>
internal sealed record TypedRead<T>(ParquetReadOptions Options, Func<T, bool>? Client, long? MaxRows, bool? Exact);

/// <summary>The predicate split and the rows made from batches, shared by the typed readers.</summary>
internal static class TypedRows
{
    /// <summary>
    /// The read of <typeparamref name="T"/>'s members from columns named <paramref name="names"/> (matched by name,
    /// then ignoring case), with <paramref name="where"/> translated by the query compiler (Samidare, as the LINQ
    /// provider translates it): each translated conjunct is pushed into the read; those that do not translate, or do
    /// not lower exactly over <paramref name="lowering"/> (the fields as the batch reader filters them), are evaluated
    /// on the rows as <see cref="SamidareConfig.ClientEvaluationPolicy"/> allows. <paramref name="what"/> names the
    /// source in messages ("file", "dataset").
    /// </summary>
    public static TypedRead<T> Plan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where,
        IReadOnlyList<string> names, Func<ParquetReadOptions, Apache.Arrow.Schema> lowering, ParquetReadOptions options, string what)
    {
        if (options.Columns is null && options.ColumnPaths is null)
        {
            // The columns of the members, as the source names them.
            string[] columns = [.. ArrowRowMapper<T>.ColumnNames.Select(member =>
                names.FirstOrDefault(n => n == member) ?? names.FirstOrDefault(n => string.Equals(n, member, StringComparison.OrdinalIgnoreCase))
                ?? throw new ArgumentException($"The {what} has no column for the member '{member}' of {typeof(T).Name}."))];
            options = options with { Columns = columns };
        }
        if (where is null) return new TypedRead<T>(options, null, null, null);

        // Captured values and local computations as constants first, as the LINQ provider does.
        where = (Expression<Func<T, bool>>)PartialEvaluator.eval(where);
        ParameterExpression row = where.Parameters[0];
        TranslationContext context = SubqueryDecorrelator.createContextForTypeWithCols(typeof(T), ListModule.OfSeq(names), row.Name ?? "row");
        List<ScalarExpr> pushed = [];
        List<Expression> client = [];
        // The fields as the reader reads them (its options), for whether a pushed conjunct is evaluated exactly.
        Apache.Arrow.Schema read = lowering(options);
        foreach (Expression conjunct in Conjuncts(where.Body))
        {
            FSharpOption<ScalarExpr> translated = ExprTranslator.tryTranslateToScalar(context, conjunct);
            // A conjunct over columns the reader does not filter on (a dataset's file path) is evaluated on the rows only.
            if (FSharpOption<ScalarExpr>.get_IsSome(translated)
                && ScalarExprOps.collectColumns(translated.Value).All(c => read.GetFieldIndex(c) >= 0))
            {
                ScalarExpr canonical = FilterShapes.canonical(translated.Value);
                pushed.Add(canonical);
                if (FilterLowering.lower(read, canonical).Exact) continue;
            }
            client.Add(conjunct);
        }
        Func<T, bool>? evaluated = null;
        if (client.Count > 0)
        {
            string text = string.Join(" && ", client);
            switch (SamidareConfig.ClientEvaluationPolicy)
            {
                case ClientEvaluationPolicy.Allow:
                    break;
                case ClientEvaluationPolicy.Warn:
                    Trace.TraceWarning($"[Kaminari Warning] Predicate in 'Where' ({text}) could not be pushed down and is evaluated on client.");
                    break;
                default:
                    throw new NotSupportedException($"Predicate in 'Where' ({text}) could not be pushed down to the Parquet reader "
                        + "(SamidareConfig.ClientEvaluationPolicy is Strict).");
            }
            evaluated = Expression.Lambda<Func<T, bool>>(client.Aggregate(Expression.AndAlso), row).Compile();
        }
        // The options' filter is evaluated with the predicate (rows filtered here); exactly when it lowers so.
        bool exact = true;
        ScalarExpr? filter = pushed.Count > 0 ? pushed.Aggregate((a, b) => ScalarExpr.NewBinary(BinaryOp.And, a, b)) : null;
        if (options.Filter is { } given)
        {
            exact = FilterLowering.lower(read, FilterShapes.canonical(given)).Exact;
            filter = filter is null ? given : ScalarExpr.NewBinary(BinaryOp.And, given, filter);
        }
        if (filter is not null) options = options with { Filter = filter, FilterRows = true };
        // A row limit counts the rows delivered: past the conjuncts evaluated here when there are some.
        long? maxRows = null;
        if (evaluated is not null && options.MaxRows is { } most)
        {
            maxRows = most;
            options = options with { MaxRows = null };
        }
        return new TypedRead<T>(options, evaluated, maxRows, exact);

        // The conjuncts of a predicate (its && chain, in order).
        static IEnumerable<Expression> Conjuncts(Expression body) => body is BinaryExpression { NodeType: ExpressionType.AndAlso } and
            ? [.. Conjuncts(and.Left), .. Conjuncts(and.Right)]
            : [body];
    }
}

/// <summary>
/// Rows of <typeparamref name="T"/> made from a batch reader's batches: on several threads at once when
/// <paramref name="parallelism"/> is above 1 (the rows still in order), the client conjuncts evaluated there.
/// </summary>
internal sealed class TypedRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
    Func<IEnumerable<RecordBatch>> batches, Func<CancellationToken, IAsyncEnumerable<RecordBatch>> batchesAsync,
    Func<T, bool>? client, long? maxRows, int parallelism)
{
    private readonly Func<T, bool>? _client = client;
    private readonly long? _maxRows = maxRows;
    private readonly int _parallelism = parallelism;

    /// <summary>
    /// Reads the rows, batch by batch (each batch's memory freed once its rows are made). With
    /// <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> above 1, up to that many batches are turned into rows at
    /// once on the thread pool, ahead of the consumer (the predicate's conjuncts evaluated on the rows run there too);
    /// the rows still come in the file's order, and a failure surfaces after the rows before it, as on one thread.
    /// </summary>
    public IEnumerable<T> ReadRows()
    {
        long left = _maxRows ?? long.MaxValue;
        if (left == 0) yield break;
        if (_parallelism == 1)
        {
            foreach (RecordBatch batch in batches())
            {
                using (batch)
                {
                    using ArrowBatchCursor<T> cursor = ArrowRowMapper<T>.CreateCursor(batch);
                    for (int i = 0; i < batch.Length; i++)
                    {
                        T row = cursor.ReadRow(i);
                        if (_client is not null && !_client(row)) continue;
                        yield return row;
                        if (--left == 0) yield break;
                    }
                }
            }
            yield break;
        }
        foreach (Hydrated hydrated in OrderedParallel.Map(batches(), Hydrate, _parallelism))
        {
            try
            {
                for (int i = 0; i < hydrated.Count; i++)
                {
                    yield return hydrated.Rows[i];
                    if (--left == 0) yield break;
                }
            }
            finally
            {
                hydrated.Return();
            }
            hydrated.Error?.Throw();
        }
    }

    /// <inheritdoc cref="ReadRows"/>
    public async IAsyncEnumerable<T> ReadRowsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long left = _maxRows ?? long.MaxValue;
        if (left == 0) yield break;
        if (_parallelism == 1)
        {
            await foreach (RecordBatch batch in batchesAsync(cancellationToken).ConfigureAwait(false))
            {
                using (batch)
                {
                    using ArrowBatchCursor<T> cursor = ArrowRowMapper<T>.CreateCursor(batch);
                    for (int i = 0; i < batch.Length; i++)
                    {
                        T row = cursor.ReadRow(i);
                        if (_client is not null && !_client(row)) continue;
                        yield return row;
                        if (--left == 0) yield break;
                    }
                }
            }
            yield break;
        }
        await foreach (Hydrated hydrated in OrderedParallel.MapAsync(batchesAsync(cancellationToken), Hydrate, _parallelism,
            cancellationToken).ConfigureAwait(false))
        {
            try
            {
                for (int i = 0; i < hydrated.Count; i++)
                {
                    yield return hydrated.Rows[i];
                    if (--left == 0) yield break;
                }
            }
            finally
            {
                hydrated.Return();
            }
            hydrated.Error?.Throw();
        }
    }

    /// <summary>
    /// All the rows in a list : the batches' rows kept as they are made, then copied
    /// once into a list of the exact size (no growth, no row-by-row iteration).
    /// </summary>
    public List<T> ReadList()
    {
        if (_parallelism == 1) return [.. ReadRows()];
        List<Chunk> chunks = [];
        try
        {
            long left = _maxRows ?? long.MaxValue;
            if (left > 0)
            {
                foreach (Hydrated hydrated in OrderedParallel.Map(batches(), Hydrate, _parallelism))
                    if (!Keep(chunks, hydrated, ref left)) break;
            }
            return Concatenate(chunks);
        }
        finally
        {
            foreach (Chunk chunk in chunks) chunk.Pooled?.Return();
        }
    }

    /// <inheritdoc cref="ReadList"/>
    public async Task<List<T>> ReadListAsync(CancellationToken cancellationToken)
    {
        if (_parallelism == 1)
        {
            List<T> rows = [];
            await foreach (T row in ReadRowsAsync(cancellationToken).ConfigureAwait(false)) rows.Add(row);
            return rows;
        }
        List<Chunk> chunks = [];
        try
        {
            long left = _maxRows ?? long.MaxValue;
            if (left > 0)
            {
                await foreach (Hydrated hydrated in OrderedParallel.MapAsync(batchesAsync(cancellationToken), Hydrate, _parallelism,
                    cancellationToken).ConfigureAwait(false))
                    if (!Keep(chunks, hydrated, ref left)) break;
            }
            return Concatenate(chunks);
        }
        finally
        {
            foreach (Chunk chunk in chunks) chunk.Pooled?.Return();
        }
    }

    /// <summary>
    /// Keeps a batch's rows (up to the rows left to deliver) and says whether more are wanted. Rows that fill little of
    /// their pooled array are copied out of it, so that a selective read does not hold an array per batch.
    /// </summary>
    private static bool Keep(List<Chunk> chunks, Hydrated hydrated, ref long left)
    {
        int count = (int)Math.Min(hydrated.Count, left);
        if (count < hydrated.Rows.Length / 4)
        {
            chunks.Add(new(hydrated.Rows[..count], count, null));
            hydrated.Return();
        }
        else
        {
            chunks.Add(new(hydrated.Rows, count, hydrated));
        }
        hydrated.Error?.Throw();
        left -= count;
        return left > 0;
    }

    /// <summary>Rows kept for a list: the first <paramref name="Kept"/> of <paramref name="Rows"/>, a pooled array when <paramref name="Pooled"/> is set.</summary>
    private readonly record struct Chunk(T[] Rows, int Kept, Hydrated? Pooled);

    private static List<T> Concatenate(List<Chunk> chunks)
    {
        int total = checked((int)chunks.Sum(c => (long)c.Kept));
        List<T> list = new(total);
        CollectionsMarshal.SetCount(list, total);
        Span<T> span = CollectionsMarshal.AsSpan(list);
        foreach (Chunk chunk in chunks)
        {
            chunk.Rows.AsSpan(0, chunk.Kept).CopyTo(span);
            span = span[chunk.Kept..];
        }
        return list;
    }

    /// <summary>
    /// A batch's rows (those the conjuncts evaluated on the rows keep), made on a pool thread, which frees the batch.
    /// A failure keeps the rows before it, for the consumer to have them first.
    /// </summary>
    private Hydrated Hydrate(RecordBatch batch)
    {
        using (batch)
        {
            T[] rows = ArrayPool<T>.Shared.Rent(batch.Length);
            int count = 0;
            try
            {
                using ArrowBatchCursor<T> cursor = ArrowRowMapper<T>.CreateCursor(batch);
                for (int i = 0; i < batch.Length; i++)
                {
                    T row = cursor.ReadRow(i);
                    if (_client is null || _client(row)) rows[count++] = row;
                }
                return new(rows, count, null);
            }
            catch (Exception e)
            {
                return new(rows, count, ExceptionDispatchInfo.Capture(e));
            }
        }
    }

    /// <summary>The rows of a batch, in an array of the shared pool (its first <see cref="Count"/>).</summary>
    private readonly record struct Hydrated(T[] Rows, int Count, ExceptionDispatchInfo? Error)
    {
        /// <summary>Gives the array back to the pool, the rows cleared from it (the pool must not keep them alive).</summary>
        public void Return()
        {
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) Rows.AsSpan(0, Count).Clear();
            ArrayPool<T>.Shared.Return(Rows);
        }
    }

}
