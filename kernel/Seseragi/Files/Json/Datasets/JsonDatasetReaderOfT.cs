using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// The rows of a JSON dataset as <typeparamref name="T"/>, mapped as <see cref="JsonReader{T}"/> maps a file's (the
/// dataset's columns are the files' and the partitions'; names matched ignoring case; partition columns and the file
/// path column are members too), read file after file through <see cref="JsonDatasetReader"/>. A <c>where</c> predicate is
/// translated by the query compiler and pushed into the read: conjuncts over partition members decide which files are
/// read, the others filter each file's rows as they are decoded; those it cannot translate, or that the reader cannot
/// evaluate exactly, are evaluated on the rows as the compiler's client evaluation policy allows. Batches are turned into
/// rows on several threads at once, the rows delivered in order.
/// </summary>
/// <remarks>
/// The columns from files are the members' (as <see cref="JsonReader{T}"/> reads a file: GUIDs from strings, enums from
/// names or numbers, dictionaries keyed by strings from objects, nothing inferred), not the dataset's own; members a
/// partition column names (ignoring case) are the paths' values.
/// </remarks>
public sealed class JsonDatasetReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly JsonDatasetReader _reader;
    private readonly TypedRows<T> _rows;

    internal JsonDatasetReader(JsonDataset dataset, Expression<Func<T, bool>>? where, JsonReadOptions? options)
    {
        options ??= new JsonReadOptions();
        if (options.RowIndexColumn is not null) throw new ArgumentException("Rows take no row index column.", nameof(options));
        // The members' columns, as JsonReader<T> reads them (GUIDs, enums, dictionaries as objects), not the dataset's own.
        dataset = dataset.Typed(JsonRowSchema<T>.Read);
        string? path = dataset.Options.FilePathColumn;
        TypedPlan<T> plan = TypedRows.Plan(where, [.. dataset.Schema.FieldsList.Select(f => f.Name)], _ => dataset.FilterSchema(),
            new TypedRequest(options.Columns, false, options.Filter, options.MaxRows, "JSON"), "dataset");
        // The file path column is not selected: every batch carries it.
        IReadOnlyList<string>? columns = path is not null && plan.Columns is { } named && named.Contains(path) ? [.. named.Where(c => c != path)] : plan.Columns;
        options = options with { Columns = columns, MaxRows = plan.MaxRows, KeysIgnoreCase = true };
        if (plan.FilterRows) options = options with { Filter = plan.Filter, FilterRows = true };
        _reader = dataset.OpenReader(options);
        FiltersExactly = plan.Exact ?? _reader.FiltersExactly;
        _rows = new TypedRows<T>(_reader.ReadBatches, _reader.ReadBatchesAsync, plan.Client, plan.ClientMaxRows, options.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="JsonReader{T}.FiltersExactly"/>
    public bool FiltersExactly { get; }

    /// <summary>The batches' schema: the columns read for the members of <typeparamref name="T"/>.</summary>
    public Apache.Arrow.Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal JsonDatasetReader Inner => _reader;

    /// <inheritdoc cref="JsonReader{T}.ReadRows"/>
    public IEnumerable<T> ReadRows() => _rows.ReadRows();

    /// <inheritdoc cref="JsonReader{T}.ReadRowsAsync"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => _rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list (<see cref="JsonDataset.ReadList{T}"/>), copied once into a list of the exact size.</summary>
    internal List<T> ReadList() => _rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    internal Task<List<T>> ReadListAsync(CancellationToken cancellationToken) => _rows.ReadListAsync(cancellationToken);

    /// <summary>Disposes the reader.</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
