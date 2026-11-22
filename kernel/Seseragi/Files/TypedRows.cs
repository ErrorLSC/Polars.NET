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

namespace Kaminari.Seseragi.Files;

/// <summary>The predicate split and the rows made from batches, shared by the typed readers.</summary>
internal static class TypedRows
{
    /// <summary>
    /// The read of <typeparamref name="T"/>'s members from columns named <paramref name="names"/> (matched by name,
    /// then ignoring case), with <paramref name="where"/> translated by the query compiler (Samidare, as the LINQ
    /// provider translates it): each translated conjunct is pushed into the read; those that do not translate, or do
    /// not lower exactly over <paramref name="lowering"/> (the fields as the batch reader filters them, given the
    /// columns it reads), are evaluated on the rows as <see cref="SamidareConfig.ClientEvaluationPolicy"/> allows.
    /// <paramref name="what"/> names the source in messages ("file", "dataset").
    /// </summary>
    public static TypedPlan<T> Plan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Expression<Func<T, bool>>? where,
        IReadOnlyList<string> names, Func<IReadOnlyList<string>?, Apache.Arrow.Schema> lowering, TypedRequest request, string what)
    {
        IReadOnlyList<string>? columns = request.Columns;
        if (columns is null && !request.HasPaths)
        {
            // The columns of the members, as the source names them.
            columns = [.. ArrowRowMapper<T>.ColumnNames.Select(member =>
                names.FirstOrDefault(n => n == member) ?? names.FirstOrDefault(n => string.Equals(n, member, StringComparison.OrdinalIgnoreCase))
                ?? throw new ArgumentException($"The {what} has no column for the member '{member}' of {typeof(T).Name}."))];
        }
        if (where is null) return new TypedPlan<T>(columns, request.Filter, false, request.MaxRows, null, null, null);

        // Captured values and local computations as constants first, as the LINQ provider does.
        where = (Expression<Func<T, bool>>)PartialEvaluator.eval(where);
        ParameterExpression row = where.Parameters[0];
        TranslationContext context = SubqueryDecorrelator.createContextForTypeWithCols(typeof(T), ListModule.OfSeq(names), row.Name ?? "row");
        List<ScalarExpr> pushed = [];
        List<Expression> client = [];
        // The fields as the reader reads them, for whether a pushed conjunct is evaluated exactly.
        Apache.Arrow.Schema read = lowering(columns);
        // Arithmetic over a member is computed in the member's type: a column stored as another number (a long member
        // over an Int32 column) is cast to it first, as the row would be.
        // A column whose member type is not known, or another than the column's that it cannot be cast to, leaves the
        // arithmetic over it to the rows.
        Dictionary<string, Apache.Arrow.Types.IArrowType> memberTypes = new(StringComparer.Ordinal);
        HashSet<string> uncast = new(StringComparer.Ordinal);
        foreach (Apache.Arrow.Field field in read.FieldsList)
        {
            Type? member = MemberType(field.Name);
            if (member is null
                || LogicalTypes.tryOfClrType(Nullable.GetUnderlyingType(member) ?? member) is not { Value: LogicalType.Arrow { arrowType: var meant } })
            {
                uncast.Add(field.Name);
                continue;
            }
            if (FilterArithmetic.SameValues(field.DataType, meant)) continue;
            if (Kaminari.Sazanami.Expressions.ValueTypes.IsArithmetic(meant)
                && Kaminari.Sazanami.Expressions.ValueTypes.IsArithmetic(Kaminari.Sazanami.Expressions.ValueTypes.ValueType(field.DataType)))
                memberTypes[field.Name] = meant;
            else
                uncast.Add(field.Name);
        }
        foreach (Expression conjunct in Conjuncts(where.Body))
        {
            FSharpOption<ScalarExpr> translated = ExprTranslator.tryTranslateToScalar(context, conjunct);
            if (FSharpOption<ScalarExpr>.get_IsSome(translated) && uncast.Count > 0 && FilterArithmetic.Columns(translated.Value).Overlaps(uncast))
                translated = FSharpOption<ScalarExpr>.None; // computed in a type the reader does not know: on the rows
            if (memberTypes.Count > 0 && FSharpOption<ScalarExpr>.get_IsSome(translated))
            {
                HashSet<string> computed = FilterArithmetic.Columns(translated.Value);
                translated = ScalarExprOps.substitute(FuncConvert.FromFunc((string name) => computed.Contains(name) && memberTypes.TryGetValue(name, out Apache.Arrow.Types.IArrowType? meant)
                    ? FSharpOption<ScalarExpr>.Some(ScalarExpr.NewCast(ScalarExpr.NewColumn(name), LogicalType.NewArrow(meant), false))
                    : FSharpOption<ScalarExpr>.None), translated.Value);
            }
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
                    throw new NotSupportedException($"Predicate in 'Where' ({text}) could not be pushed down to the {request.Reader} reader "
                        + "(SamidareConfig.ClientEvaluationPolicy is Strict).");
            }
            evaluated = Expression.Lambda<Func<T, bool>>(client.Aggregate(Expression.AndAlso), row).Compile();
        }
        // The request's filter is evaluated with the predicate (rows filtered here); exactly when it lowers so.
        bool exact = true;
        ScalarExpr? filter = pushed.Count > 0 ? pushed.Aggregate((a, b) => ScalarExpr.NewBinary(BinaryOp.And, a, b)) : null;
        if (request.Filter is { } given)
        {
            exact = FilterLowering.lower(read, FilterShapes.canonical(given)).Exact;
            filter = filter is null ? given : ScalarExpr.NewBinary(BinaryOp.And, given, filter);
        }
        // A row limit counts the rows delivered: past the conjuncts evaluated here when there are some.
        long? readerMaxRows = request.MaxRows, clientMaxRows = null;
        if (evaluated is not null && request.MaxRows is { } most)
        {
            clientMaxRows = most;
            readerMaxRows = null;
        }
        return new TypedPlan<T>(columns, filter, filter is not null, readerMaxRows, evaluated, clientMaxRows, exact);

        // The type of the member a column binds to (by name, then ignoring case), or null.
        static Type? MemberType(string column)
        {
            System.Reflection.MemberInfo[] members = [.. typeof(T).GetProperties(), .. typeof(T).GetFields()];
            System.Reflection.MemberInfo? member = members.FirstOrDefault(m => m.Name == column)
                ?? members.FirstOrDefault(m => string.Equals(m.Name, column, StringComparison.OrdinalIgnoreCase));
            return member switch
            {
                System.Reflection.PropertyInfo p => p.PropertyType,
                System.Reflection.FieldInfo f => f.FieldType,
                _ => null,
            };
        }

        // The conjuncts of a predicate (its && chain, in order).
        static IEnumerable<Expression> Conjuncts(Expression body) => body is BinaryExpression { NodeType: ExpressionType.AndAlso } and
            ? [.. Conjuncts(and.Left), .. Conjuncts(and.Right)]
            : [body];
    }
}

/// <summary>What a typed read asks for before the predicate is split: the reader's own options.</summary>
/// <param name="Columns">The columns the options select (null: the members' columns, unless <paramref name="HasPaths"/>).</param>
/// <param name="HasPaths">Whether the options select parts of columns by path.</param>
/// <param name="Filter">The options' row filter.</param>
/// <param name="MaxRows">The options' row limit.</param>
/// <param name="Reader">The reader's name in messages ("Parquet", "Arrow IPC").</param>
internal sealed record TypedRequest(IReadOnlyList<string>? Columns, bool HasPaths, ScalarExpr? Filter, long? MaxRows, string Reader);

/// <summary>
/// What a typed read asks of its batch reader, and the conjuncts it evaluates on the rows itself.
/// </summary>
/// <param name="Columns">The columns to read (the members' columns unless the options chose).</param>
/// <param name="Filter">The batch reader's filter: the options' and the predicate's pushed conjuncts.</param>
/// <param name="FilterRows">Whether the batch reader filters rows (with a predicate's filter).</param>
/// <param name="MaxRows">The batch reader's row limit.</param>
/// <param name="Client">The conjuncts evaluated on the rows (null: none).</param>
/// <param name="ClientMaxRows">The rows to deliver, counted on the rows when the client evaluates conjuncts.</param>
/// <param name="Exact">Whether the options' own filter (with a predicate) is evaluated exactly; null without a predicate (the batch reader tells).</param>
internal sealed record TypedPlan<T>(IReadOnlyList<string>? Columns, ScalarExpr? Filter, bool FilterRows, long? MaxRows, Func<T, bool>? Client,
    long? ClientMaxRows, bool? Exact);

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
    /// Reads the rows, batch by batch (each batch's memory freed once its rows are made). With a degree of parallelism
    /// (the read options' <c>MaxDegreeOfParallelism</c>) above 1, up to that many batches are turned into rows at
    /// once on the thread pool, ahead of the consumer (the predicate's conjuncts evaluated on the rows run there too);
    /// the rows still come in the file's order, and a failure surfaces after the rows before it, as on one thread.
    /// </summary>
    public IEnumerable<T> ReadRows()
    {
        long left = _maxRows ?? long.MaxValue;
        if (left == 0) yield break;
        if (_parallelism == 1)
        {
            long start = 0;
            foreach (RecordBatch batch in batches())
            {
                using (batch)
                {
                    using ArrowBatchCursor<T> cursor = ArrowRowMapper<T>.CreateCursor(batch);
                    for (int i = 0; i < batch.Length; i++)
                    {
                        T row = Row(cursor, i, start);
                        if (_client is not null && !_client(row)) continue;
                        yield return row;
                        if (--left == 0) yield break;
                    }
                    start += batch.Length;
                }
            }
            yield break;
        }
        foreach (Hydrated hydrated in OrderedParallel.Map(Numbered(batches()), Hydrate, _parallelism))
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
            long start = 0;
            await foreach (RecordBatch batch in batchesAsync(cancellationToken).ConfigureAwait(false))
            {
                using (batch)
                {
                    using ArrowBatchCursor<T> cursor = ArrowRowMapper<T>.CreateCursor(batch);
                    for (int i = 0; i < batch.Length; i++)
                    {
                        T row = Row(cursor, i, start);
                        if (_client is not null && !_client(row)) continue;
                        yield return row;
                        if (--left == 0) yield break;
                    }
                    start += batch.Length;
                }
            }
            yield break;
        }
        await foreach (Hydrated hydrated in OrderedParallel.MapAsync(NumberedAsync(batchesAsync(cancellationToken)), Hydrate, _parallelism,
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
                foreach (Hydrated hydrated in OrderedParallel.Map(Numbered(batches()), Hydrate, _parallelism))
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
                await foreach (Hydrated hydrated in OrderedParallel.MapAsync(NumberedAsync(batchesAsync(cancellationToken)), Hydrate, _parallelism,
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
    private Hydrated Hydrate((RecordBatch Batch, long Start) numbered)
    {
        var (batch, start) = numbered;
        using (batch)
        {
            T[] rows = ArrayPool<T>.Shared.Rent(batch.Length);
            int count = 0;
            try
            {
                using ArrowBatchCursor<T> cursor = ArrowRowMapper<T>.CreateCursor(batch);
                for (int i = 0; i < batch.Length; i++)
                {
                    T row = Row(cursor, i, start);
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

    // A row of a batch: a null its member cannot hold is placed among all the rows read (the batch starting at start).
    private static T Row(ArrowBatchCursor<T> cursor, int i, long start)
    {
        try
        {
            return cursor.ReadRow(i);
        }
        catch (InvalidOperationException e) when (RowNull.At(e, start) is { } placed)
        {
            throw placed;
        }
    }

    // The batches with the number of the rows before each.
    private static IEnumerable<(RecordBatch, long)> Numbered(IEnumerable<RecordBatch> batches)
    {
        long start = 0;
        foreach (RecordBatch batch in batches)
        {
            yield return (batch, start);
            start += batch.Length;
        }
    }

    private static async IAsyncEnumerable<(RecordBatch, long)> NumberedAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        long start = 0;
        await foreach (RecordBatch batch in batches.ConfigureAwait(false))
        {
            yield return (batch, start);
            start += batch.Length;
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
