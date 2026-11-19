using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// The rows of a dataset as <typeparamref name="T"/>, mapped by member name as <see cref="IpcReader{T}"/> maps a file's
/// (names matched ignoring case; partition columns and the file path column are members too), read file after file
/// through <see cref="IpcDatasetReader"/> (columns converted to the dataset's types). A <c>where</c> predicate is
/// translated by the query compiler and pushed into the read: conjuncts over partition members decide which files are
/// read, the others filter each file's rows; those it cannot translate, or that the reader cannot evaluate exactly over
/// the dataset's types, are evaluated on the rows as the compiler's client evaluation policy allows. Batches are turned
/// into rows on several threads at once, the rows delivered in order.
/// </summary>
public sealed class IpcDatasetReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly IpcDatasetReader _reader;
    private readonly TypedRows<T> _rows;

    internal IpcDatasetReader(IpcDataset dataset, Expression<Func<T, bool>>? where, IpcReadOptions? options)
    {
        options ??= new IpcReadOptions();
        string? path = dataset.Options.FilePathColumn;
        TypedPlan<T> plan = TypedRows.Plan(where, [.. dataset.Schema.FieldsList.Select(f => f.Name)], _ => dataset.FilterSchema(),
            new TypedRequest(options.Columns, false, options.Filter, options.MaxRows, "Arrow IPC"), "dataset");
        // The file path column is not selected: every batch carries it.
        IReadOnlyList<string>? columns = path is not null && plan.Columns is { } named && named.Contains(path) ? [.. named.Where(c => c != path)] : plan.Columns;
        options = options with { Columns = columns, MaxRows = plan.MaxRows };
        if (plan.FilterRows) options = options with { Filter = plan.Filter, FilterRows = true };
        _reader = dataset.OpenReader(options);
        FiltersExactly = plan.Exact ?? _reader.FiltersExactly;
        _rows = new TypedRows<T>(_reader.ReadBatches, _reader.ReadBatchesAsync, plan.Client, plan.ClientMaxRows, options.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="IpcReader{T}.FiltersExactly"/>
    public bool FiltersExactly { get; }

    /// <summary>The batches' schema: the columns read for the members of <typeparamref name="T"/>.</summary>
    public Apache.Arrow.Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal IpcDatasetReader Inner => _reader;

    /// <inheritdoc cref="IpcReader{T}.ReadRows"/>
    public IEnumerable<T> ReadRows() => _rows.ReadRows();

    /// <inheritdoc cref="IpcReader{T}.ReadRowsAsync"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => _rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list (<see cref="IpcDataset.ReadList{T}"/>), copied once into a list of the exact size.</summary>
    internal List<T> ReadList() => _rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    internal Task<List<T>> ReadListAsync(CancellationToken cancellationToken) => _rows.ReadListAsync(cancellationToken);

    /// <summary>Disposes the reader.</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
