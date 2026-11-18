using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The rows of a dataset as <typeparamref name="T"/>, mapped by member name as <see cref="ParquetReader{T}"/> maps a
/// file's (names matched ignoring case; partition columns and the file path column are members too), read file
/// after file through <see cref="ParquetDatasetReader"/> (columns converted to the dataset's types). A <c>where</c>
/// predicate is translated by the query compiler and pushed into the read: conjuncts over partition members decide
/// which files are read, the others prune and filter each file; those it cannot translate, or that the reader cannot
/// evaluate exactly over the dataset's types, are evaluated on the rows as the compiler's client evaluation policy
/// allows. Batches are turned into rows on several threads at once, the rows delivered in order.
/// </summary>
public sealed class ParquetDatasetReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly ParquetDatasetReader _reader;
    private readonly TypedRows<T> _rows;

    internal ParquetDatasetReader(ParquetDataset dataset, Expression<Func<T, bool>>? where, ParquetReadOptions? options)
    {
        options ??= new ParquetReadOptions();
        string? path = dataset.Options.FilePathColumn;
        TypedRead<T> read = ParquetTypedRows.Plan(where, [.. dataset.Schema.FieldsList.Select(f => f.Name)], dataset.FilterSchema, options, "dataset");
        // The file path column is not selected: every batch carries it.
        ParquetReadOptions selected = path is not null && read.Options.Columns is { } columns && columns.Contains(path)
            ? read.Options with { Columns = [.. columns.Where(c => c != path)] }
            : read.Options;
        _reader = dataset.OpenReader(selected);
        FiltersExactly = read.Exact ?? _reader.FiltersExactly;
        _rows = new TypedRows<T>(_reader.ReadBatches, _reader.ReadBatchesAsync, read.Client, read.MaxRows, options.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="ParquetReader{T}.FiltersExactly"/>
    public bool FiltersExactly { get; }

    /// <summary>The batches' schema: the columns read for the members of <typeparamref name="T"/>.</summary>
    public Apache.Arrow.Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal ParquetDatasetReader Inner => _reader;

    /// <inheritdoc cref="ParquetReader{T}.ReadRows"/>
    public IEnumerable<T> ReadRows() => _rows.ReadRows();

    /// <inheritdoc cref="ParquetReader{T}.ReadRowsAsync"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => _rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list (<see cref="ParquetDataset.ReadList{T}"/>), copied once into a list of the exact size.</summary>
    internal List<T> ReadList() => _rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    internal Task<List<T>> ReadListAsync(CancellationToken cancellationToken) => _rows.ReadListAsync(cancellationToken);

    /// <summary>Disposes the reader.</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
