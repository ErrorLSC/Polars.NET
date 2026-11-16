using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Kaminari.Samidare.Config;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The rows of a Parquet file as <typeparamref name="T"/> (records, classes, structs, F# records and tuples, mapped
/// by member name as <c>DataFrame.Rows&lt;T&gt;()</c> maps them: names matched ignoring case, nested structs, lists and
/// maps), read batch by batch. Only the columns <typeparamref name="T"/> maps are read. A <c>where</c> predicate is
/// translated by the query compiler (Samidare, as the LINQ provider translates it) and pushed into the read: row
/// groups, pages and rows are skipped by it; the conjuncts it cannot translate, or that the reader cannot evaluate,
/// are evaluated on the rows as <typeparamref name="T"/> when the compiler's client evaluation policy allows it
/// (<see cref="SamidareConfig.ClientEvaluationPolicy"/>: Strict refuses them). Batches are turned into rows on
/// several threads at once (<see cref="ParquetReadOptions.MaxDegreeOfParallelism"/>), the rows delivered in order.
/// </summary>
public sealed class ParquetReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly ParquetReader _reader;
    private readonly TypedRows<T> _rows;

    internal ParquetReader(ParquetFile file, Expression<Func<T, bool>>? where, ParquetReadOptions? options)
    {
        options ??= new ParquetReadOptions();
        TypedRead<T> read = TypedRows.Plan(where, [.. file.Schema.FieldsList.Select(f => f.Name)],
            o => new Apache.Arrow.Schema([.. file.Mapping(o).Columns.Select(c => c.Field)], null), options, "file");
        _reader = file.OpenReader(read.Options);
        FiltersExactly = read.Exact ?? _reader.FiltersExactly;
        _rows = new TypedRows<T>(_reader.ReadBatches, _reader.ReadBatchesAsync, read.Client, read.MaxRows, options.MaxDegreeOfParallelism);
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

    /// <summary>
    /// Reads the rows, batch by batch (each batch's memory freed once its rows are made). With
    /// <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> above 1, up to that many batches are turned into rows at
    /// once on the thread pool, ahead of the consumer (the predicate's conjuncts evaluated on the rows run there too);
    /// the rows still come in the file's order, and a failure surfaces after the rows before it, as on one thread.
    /// </summary>
    public IEnumerable<T> ReadRows() => _rows.ReadRows();

    /// <inheritdoc cref="ReadRows"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => _rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list (<see cref="ParquetFile.ReadList{T}"/>), copied once into a list of the exact size.</summary>
    internal List<T> ReadList() => _rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    internal Task<List<T>> ReadListAsync(CancellationToken cancellationToken) => _rows.ReadListAsync(cancellationToken);

    /// <summary>Disposes the reader (batches being decoded are dropped).</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
