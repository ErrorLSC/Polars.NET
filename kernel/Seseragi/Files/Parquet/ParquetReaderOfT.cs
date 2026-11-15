using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Kaminari.Samidare;
using Kaminari.Samidare.Config;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Data;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The rows of a Parquet file as <typeparamref name="T"/> (records, classes, structs, F# records and tuples, mapped
/// by member name as <c>DataFrame.Rows&lt;T&gt;()</c> maps them: names matched ignoring case, nested structs, lists and
/// maps), read batch by batch. Only the columns <typeparamref name="T"/> maps are read. A <c>where</c> predicate is
/// translated by the query compiler (Samidare, as the LINQ provider translates it) and pushed into the read: row
/// groups, pages and rows are skipped by it; the conjuncts it cannot translate, or that the reader cannot evaluate,
/// are evaluated on the rows as <typeparamref name="T"/> when the compiler's client evaluation policy allows it
/// (<see cref="SamidareConfig.ClientEvaluationPolicy"/>: Strict refuses them).
/// </summary>
public sealed class ParquetReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly ParquetReader _reader;
    private readonly Func<T, bool>? _client; // the conjuncts evaluated on the rows
    private readonly long? _maxRows;         // the rows to deliver, counted here when the client evaluates conjuncts

    internal ParquetReader(ParquetFile file, Expression<Func<T, bool>>? where, ParquetReadOptions? options)
    {
        options ??= new ParquetReadOptions();
        Apache.Arrow.Schema schema = file.Schema;
        if (options.Columns is null && options.ColumnPaths is null)
        {
            // The columns of the members, as the file names them.
            var names = schema.FieldsList.Select(f => f.Name).ToArray();
            string[] columns = [.. ArrowRowMapper<T>.ColumnNames.Select(member =>
                names.FirstOrDefault(n => n == member) ?? names.FirstOrDefault(n => string.Equals(n, member, StringComparison.OrdinalIgnoreCase))
                ?? throw new ArgumentException($"The file has no column for the member '{member}' of {typeof(T).Name}."))];
            options = options with { Columns = columns };
        }
        bool exact = true;
        if (where is not null)
        {
            // Captured values and local computations as constants first, as the LINQ provider does.
            where = (Expression<Func<T, bool>>)PartialEvaluator.eval(where);
            ParameterExpression row = where.Parameters[0];
            TranslationContext context = SubqueryDecorrelator.createContextForTypeWithCols(typeof(T),
                ListModule.OfSeq(schema.FieldsList.Select(f => f.Name)), row.Name ?? "row");
            List<ScalarExpr> pushed = [];
            List<Expression> client = [];
            // The fields as the reader reads them (its options), for whether a pushed conjunct is evaluated exactly.
            var (_, all) = ArrowMapping.Map(file.Tree, file.Thrift, options);
            var read = new Apache.Arrow.Schema([.. all.Select(c => c.Field)], null);
            foreach (Expression conjunct in Conjuncts(where.Body))
            {
                FSharpOption<ScalarExpr> translated = ExprTranslator.tryTranslateToScalar(context, conjunct);
                if (FSharpOption<ScalarExpr>.get_IsSome(translated))
                {
                    ScalarExpr canonical = FilterShapes.canonical(translated.Value);
                    pushed.Add(canonical);
                    if (FilterLowering.lower(read, canonical).Exact) continue;
                }
                client.Add(conjunct);
            }
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
                _client = Expression.Lambda<Func<T, bool>>(client.Aggregate(Expression.AndAlso), row).Compile();
            }
            // The options' filter is evaluated with the predicate (rows filtered here); exactly when it lowers so.
            ScalarExpr? filter = pushed.Count > 0 ? pushed.Aggregate((a, b) => ScalarExpr.NewBinary(BinaryOp.And, a, b)) : null;
            if (options.Filter is { } given)
            {
                exact = FilterLowering.lower(read, FilterShapes.canonical(given)).Exact;
                filter = filter is null ? given : ScalarExpr.NewBinary(BinaryOp.And, given, filter);
            }
            if (filter is not null) options = options with { Filter = filter, FilterRows = true };
            // A row limit counts the rows delivered: past the conjuncts evaluated here when there are some.
            if (_client is not null && options.MaxRows is { } most)
            {
                _maxRows = most;
                options = options with { MaxRows = null };
            }
        }
        _reader = file.OpenReader(options);
        FiltersExactly = where is null ? _reader.FiltersExactly : exact;

        // The conjuncts of a predicate (its && chain, in order).
        static IEnumerable<Expression> Conjuncts(Expression body) => body is BinaryExpression { NodeType: ExpressionType.AndAlso } and
            ? [.. Conjuncts(and.Left), .. Conjuncts(and.Right)]
            : [body];
    }

    /// <summary>
    /// Whether the rows are exactly those the predicate and the options' <see cref="ParquetReadOptions.Filter"/> keep:
    /// the predicate is always applied whole (on the rows where the reader cannot); with a predicate the options' filter
    /// is applied to the rows too, exactly unless a conjunct of it cannot be evaluated by the reader; without one, as
    /// <see cref="ParquetReader.FiltersExactly"/> tells. <see cref="ParquetReadOptions.MaxRows"/> counts the rows delivered.
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The reader of the batches (tests).</summary>
    internal ParquetReader Inner => _reader;

    /// <summary>The batches' schema: the columns read for the members of <typeparamref name="T"/>.</summary>
    public Apache.Arrow.Schema Schema => _reader.Schema;

    /// <summary>Reads the rows, batch by batch (each batch's memory freed once its rows are read).</summary>
    public IEnumerable<T> ReadRows()
    {
        long left = _maxRows ?? long.MaxValue;
        if (left == 0) yield break;
        foreach (RecordBatch batch in _reader.ReadBatches())
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
    }

    /// <inheritdoc cref="ReadRows"/>
    public async IAsyncEnumerable<T> ReadRowsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long left = _maxRows ?? long.MaxValue;
        if (left == 0) yield break;
        await foreach (RecordBatch batch in _reader.ReadBatchesAsync(cancellationToken).ConfigureAwait(false))
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
    }

    /// <summary>Disposes the reader (batches being decoded are dropped).</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
