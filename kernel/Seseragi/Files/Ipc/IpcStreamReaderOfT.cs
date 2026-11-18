using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Kaminari.Samidare.Config;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// The rows of an Arrow IPC stream as <typeparamref name="T"/>, mapped by member name as <see cref="IpcReader{T}"/>
/// maps them; only the columns <typeparamref name="T"/> maps are read (chosen once the stream's schema is read). A
/// <c>where</c> predicate is translated by the query compiler and evaluated by the reader within each batch; the
/// conjuncts it cannot evaluate are evaluated on the rows when the compiler's client evaluation policy allows it
/// (<see cref="SamidareConfig.ClientEvaluationPolicy"/>). The stream is read ahead and its batches decoded and turned
/// into rows on several threads (<see cref="IpcReadOptions.MaxDegreeOfParallelism"/>), the rows delivered in order. A
/// stream is read forward once: its rows can be read once.
/// </summary>
public sealed class IpcStreamReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly IpcStreamReader _reader;
    private readonly TypedRows<T> _rows;
    private bool _read;

    private IpcStreamReader(IpcStreamReader reader, TypedPlan<T> plan, int parallelism)
    {
        _reader = reader;
        FiltersExactly = plan.Exact ?? reader.FiltersExactly;
        _rows = new TypedRows<T>(reader.ReadBatches, reader.ReadBatchesAsync, plan.Client, plan.ClientMaxRows, parallelism);
    }

    /// <summary>
    /// Opens a stream, reading its schema message. Unless <paramref name="leaveOpen"/>, the stream is disposed with the
    /// reader, or when opening fails.
    /// </summary>
    /// <exception cref="IpcFormatException">The data is not an Arrow IPC stream, or its schema is damaged.</exception>
    /// <exception cref="ArgumentException">The stream has no column for a member, or the options are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public static IpcStreamReader<T> Open(Stream stream, Expression<Func<T, bool>>? where = null, IpcReadOptions? options = null, bool leaveOpen = false) =>
        IpcIO.Sync(OpenCoreAsync(stream, where, options, leaveOpen, false, default));

    /// <inheritdoc cref="Open"/>
    public static ValueTask<IpcStreamReader<T>> OpenAsync(Stream stream, Expression<Func<T, bool>>? where = null, IpcReadOptions? options = null,
        bool leaveOpen = false, CancellationToken cancellationToken = default) => OpenCoreAsync(stream, where, options, leaveOpen, true, cancellationToken);

    private static async ValueTask<IpcStreamReader<T>> OpenCoreAsync(Stream stream, Expression<Func<T, bool>>? where, IpcReadOptions? options,
        bool leaveOpen, bool useAsync, CancellationToken ct)
    {
        options = IpcStreamReader.Validated(stream, options, leaveOpen);
        TypedPlan<T>? plan = null;
        IpcStreamReader reader = await IpcStreamReader.OpenCoreAsync(stream, schema =>
        {
            plan = TypedRows.Plan(where, [.. schema.FieldsList.Select(f => f.Name)], _ => schema,
                new TypedRequest(options.Columns, false, options.Filter, options.MaxRows, "Arrow IPC"), "stream");
            IpcReadOptions chosen = options with { Columns = plan.Columns, MaxRows = plan.MaxRows };
            return plan.FilterRows ? chosen with { Filter = plan.Filter, FilterRows = true } : chosen;
        }, leaveOpen, useAsync, ct).ConfigureAwait(false);
        return new IpcStreamReader<T>(reader, plan!, options.MaxDegreeOfParallelism);
    }

    /// <summary>
    /// Whether the rows are exactly those the predicate and the options' <see cref="IpcReadOptions.Filter"/> keep (see
    /// <see cref="IpcReader{T}.FiltersExactly"/>).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The batches' schema: the columns read for the members of <typeparamref name="T"/>.</summary>
    public Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal IpcStreamReader Inner => _reader;

    /// <summary>
    /// Reads the rows, batch by batch (each batch's memory freed once its rows are made), in the stream's order.
    /// </summary>
    /// <exception cref="InvalidOperationException">The rows were read already (a stream is read once).</exception>
    public IEnumerable<T> ReadRows() => Once()._rows.ReadRows();

    /// <inheritdoc cref="ReadRows"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => Once()._rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list, copied once into a list of the exact size.</summary>
    /// <inheritdoc cref="ReadRows"/>
    public List<T> ReadList() => Once()._rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    public Task<List<T>> ReadListAsync(CancellationToken cancellationToken = default) => Once()._rows.ReadListAsync(cancellationToken);

    private IpcStreamReader<T> Once()
    {
        if (_read) throw new InvalidOperationException("The rows of an Arrow IPC stream can be read once.");
        _read = true;
        return this;
    }

    /// <summary>Disposes the reader (and, unless it was left open, the stream).</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
