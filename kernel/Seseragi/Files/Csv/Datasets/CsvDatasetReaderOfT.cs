using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// The rows of a CSV dataset as <typeparamref name="T"/>, mapped as <see cref="CsvReader{T}"/> maps a file's (the
/// dataset's columns are the members': each file's header names them, in any case, its other columns passed over and a
/// member it lacks an error; partition columns and the file path column are members too), read file after file through
/// <see cref="CsvDatasetReader"/>. A <c>where</c> predicate is translated by the query compiler and pushed into the read:
/// conjuncts over partition members decide which files are read, the others filter each file's rows as they are
/// decoded; those it cannot translate, or that the reader cannot evaluate exactly, are evaluated on the rows as the
/// compiler's client evaluation policy allows. Batches are turned into rows on several threads at once, the rows
/// delivered in order.
/// </summary>
/// <remarks>
/// The columns from files are the members' (as <see cref="CsvReader{T}"/> reads a file: GUIDs from their text, enums from
/// names or numbers, nothing inferred), not the dataset's own; members a partition column names (ignoring case) are the
/// paths' values.
/// </remarks>
public sealed class CsvDatasetReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly CsvDatasetReader _reader;
    private readonly TypedRows<T> _rows;

    /// <param name="dataset">The dataset (its columns become the members').</param>
    /// <param name="where">The rows read.</param>
    /// <param name="options">How the rows are read (no row index).</param>
    /// <param name="allowMissingColumns">Whether a file may lack a member's column (nulls); the dataset's option when null.</param>
    internal CsvDatasetReader(CsvDataset dataset, Expression<Func<T, bool>>? where, CsvReadOptions? options, bool? allowMissingColumns = null)
    {
        options ??= new CsvReadOptions();
        if (options.RowIndexColumn is not null) throw new ArgumentException("Rows take no row index column.", nameof(options));
        // The members' columns, as CsvReader<T> reads them (GUIDs, enums), not the dataset's own.
        dataset = dataset.Typed(CsvRowSchema<T>.Read, allowMissingColumns);
        string? path = dataset.Options.FilePathColumn;
        TypedPlan<T> plan = TypedRows.Plan(where, [.. dataset.Schema.FieldsList.Select(f => f.Name)], _ => dataset.Schema,
            new TypedRequest(options.Columns, false, options.Filter, options.MaxRows, "CSV"), "dataset");
        // The file path column is not selected: every batch carries it.
        IReadOnlyList<string>? columns = path is not null && plan.Columns is { } named && named.Contains(path) ? [.. named.Where(c => c != path)] : plan.Columns;
        options = options with { Columns = columns, MaxRows = plan.MaxRows };
        if (plan.FilterRows) options = options with { Filter = plan.Filter, FilterRows = true };
        _reader = dataset.OpenReader(options);
        FiltersExactly = plan.Exact ?? _reader.FiltersExactly;
        _rows = new TypedRows<T>(_reader.ReadBatches, _reader.ReadBatchesAsync, plan.Client, plan.ClientMaxRows, options.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="CsvReader{T}.FiltersExactly"/>
    public bool FiltersExactly { get; }

    /// <summary>The batches' schema: the columns read for the members of <typeparamref name="T"/>.</summary>
    public Apache.Arrow.Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal CsvDatasetReader Inner => _reader;

    /// <inheritdoc cref="CsvReader{T}.ReadRows"/>
    public IEnumerable<T> ReadRows() => _rows.ReadRows();

    /// <inheritdoc cref="CsvReader{T}.ReadRowsAsync"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => _rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list (<see cref="CsvDataset.ReadList{T}"/>), copied once into a list of the exact size.</summary>
    internal List<T> ReadList() => _rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    internal Task<List<T>> ReadListAsync(CancellationToken cancellationToken) => _rows.ReadListAsync(cancellationToken);

    /// <summary>Disposes the reader.</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
