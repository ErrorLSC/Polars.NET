using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Kaminari.Samidare.Config;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// CSV records read as rows of <typeparamref name="T"/> (records, classes, structs, F# records and tuples, mapped by
/// member name as <c>DataFrame.Rows&lt;T&gt;()</c> maps them): the columns are <typeparamref name="T"/>'s members (as
/// <see cref="CsvWriter{T}"/> writes them, nothing inferred), which the header names exactly or ignoring case; the
/// file's other columns are passed over (not decoded); a member the header does not name is an error naming it (a
/// column whose values are all empty reads as nulls). Without a header the members are the columns in their order.
/// Values as <see cref="CsvReader"/> reads them, and: GUIDs from their text, enums from their members' names (in any
/// case) or numbers, time spans from ISO 8601 (<c>PT1.5S</c>) or <c>[-][d.]hh:mm:ss[.f]</c>. A member that is a record,
/// a collection, a dictionary or bytes is refused when the reader is opened. A member of a value type (not nullable)
/// takes no empty field (declare the member nullable to receive nulls). A <c>where</c> predicate is translated by the
/// query compiler (Samidare, as the LINQ provider translates it) and evaluated by the reader on each decoded unit (the
/// columns it tests first, the others only for the rows kept); the conjuncts it cannot evaluate are evaluated on the
/// rows when the compiler's client evaluation policy allows it (<see cref="SamidareConfig.ClientEvaluationPolicy"/>:
/// Strict refuses them). Units are decoded, filtered and turned into rows on several threads at once
/// (<see cref="CsvReadOptions.MaxDegreeOfParallelism"/>), the rows delivered in order.
/// </summary>
public sealed class CsvReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly CsvReader _reader;
    private readonly TypedRows<T> _rows;

    private CsvReader(CsvReader reader, TypedPlan<T> plan, int parallelism)
    {
        _reader = reader;
        FiltersExactly = plan.Exact ?? reader.FiltersExactly;
        _rows = new TypedRows<T>(reader.ReadBatches, reader.ReadBatchesAsync, plan.Client, plan.ClientMaxRows, parallelism);
    }

    /// <summary>
    /// Whether the rows are exactly those the predicate and the options' <see cref="CsvReadOptions.Filter"/> keep: the
    /// predicate is always applied whole (on the rows where the reader cannot); with a predicate the options' filter is
    /// applied to the rows too, exactly unless a conjunct of it cannot be evaluated by the reader; without one, as
    /// <see cref="CsvReader.FiltersExactly"/> tells. <see cref="CsvReadOptions.MaxRows"/> counts the rows delivered.
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The columns read for the members of <typeparamref name="T"/>.</summary>
    public Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal CsvReader Inner => _reader;

    /// <summary>Opens a file (see <see cref="CsvReader.Open(string, CsvReadOptions?)"/>).</summary>
    /// <exception cref="ArgumentException">The options give a schema, overrides or a row index (the rows' members are the columns), or are out of range.</exception>
    /// <exception cref="NotSupportedException">A member CSV cannot hold, or a conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    /// <exception cref="CsvFormatException">The header names no column for a member.</exception>
    public static CsvReader<T> Open(string path, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null) =>
        Of(where, options, o => CsvReader.Open(path, o));

    /// <summary>Opens a stream (see <see cref="CsvReader.Open(Stream, CsvReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, CsvReadOptions?)" path="/exception"/>
    public static CsvReader<T> Open(Stream stream, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null) =>
        Of(where, options, o => CsvReader.Open(stream, o));

    /// <summary>Opens data read at offsets (see <see cref="CsvReader.Open(IRandomAccess, CsvReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, CsvReadOptions?)" path="/exception"/>
    public static CsvReader<T> Open(IRandomAccess source, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null) =>
        Of(where, options, o => CsvReader.Open(source, o));

    /// <summary>Opens CSV held in memory, read in place (see <see cref="CsvReader.Open(ReadOnlyMemory{byte}, CsvReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, CsvReadOptions?)" path="/exception"/>
    public static CsvReader<T> Open(ReadOnlyMemory<byte> bytes, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null) =>
        Of(where, options, o => CsvReader.Open(bytes, o));

    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, CsvReadOptions?)"/>
    public static async Task<CsvReader<T>> OpenAsync(string path, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (plan, chosen) = Plan(where, options);
        return new CsvReader<T>(await CsvReader.OpenAsync(path, chosen, cancellationToken).ConfigureAwait(false), plan, chosen.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="Open(Stream, Expression{Func{T, bool}}?, CsvReadOptions?)"/>
    public static async Task<CsvReader<T>> OpenAsync(Stream stream, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (plan, chosen) = Plan(where, options);
        return new CsvReader<T>(await CsvReader.OpenAsync(stream, chosen, cancellationToken).ConfigureAwait(false), plan, chosen.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="Open(IRandomAccess, Expression{Func{T, bool}}?, CsvReadOptions?)"/>
    public static async Task<CsvReader<T>> OpenAsync(IRandomAccess source, Expression<Func<T, bool>>? where = null, CsvReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (plan, chosen) = Plan(where, options);
        return new CsvReader<T>(await CsvReader.OpenAsync(source, chosen, cancellationToken).ConfigureAwait(false), plan, chosen.MaxDegreeOfParallelism);
    }

    private static CsvReader<T> Of(Expression<Func<T, bool>>? where, CsvReadOptions? options, Func<CsvReadOptions, CsvReader> open)
    {
        var (plan, chosen) = Plan(where, options);
        return new CsvReader<T>(open(chosen), plan, chosen.MaxDegreeOfParallelism);
    }

    // The batch reader's options: the members' columns (nothing inferred, matched by name), the pushed conjuncts.
    private static (TypedPlan<T> Plan, CsvReadOptions Options) Plan(Expression<Func<T, bool>>? where, CsvReadOptions? options)
    {
        options ??= new CsvReadOptions();
        if (options.Schema is not null || options.SchemaOverrides is not null)
            throw new ArgumentException("The rows' members are the columns: give no schema (nor schema overrides).", nameof(options));
        if (options.RowIndexColumn is not null) throw new ArgumentException("Rows take no row index column.", nameof(options));
        Schema schema = CsvRowSchema<T>.Read;
        TypedPlan<T> plan = TypedRows.Plan(where, [.. schema.FieldsList.Select(f => f.Name)], columns => Project(schema, columns),
            new TypedRequest(options.Columns, false, options.Filter, options.MaxRows, "CSV"), "CSV");
        CsvReadOptions chosen = options with { Schema = schema, MembersByName = true, Columns = plan.Columns, MaxRows = plan.MaxRows };
        if (plan.FilterRows) chosen = chosen with { Filter = plan.Filter, FilterRows = true };
        return (plan, chosen);
    }

    private static Schema Project(Schema schema, IReadOnlyList<string>? columns) =>
        columns is null ? schema : new Schema([.. schema.FieldsList.Where(f => columns.Contains(f.Name))], schema.Metadata);

    /// <summary>
    /// Reads the rows, unit by unit (each batch's memory freed once its rows are made). With
    /// <see cref="CsvReadOptions.MaxDegreeOfParallelism"/> above 1, up to that many batches are turned into rows at once
    /// on the thread pool, ahead of the consumer (the predicate's conjuncts evaluated on the rows run there too); the
    /// rows still come in the input's order, and a failure surfaces after the rows before it, as on one thread. A
    /// stream's rows can be read once.
    /// </summary>
    public IEnumerable<T> ReadRows() => _rows.ReadRows();

    /// <inheritdoc cref="ReadRows"/>
    public IAsyncEnumerable<T> ReadRowsAsync(CancellationToken cancellationToken = default) => _rows.ReadRowsAsync(cancellationToken);

    /// <summary>All the rows in a list, copied once into a list of the exact size.</summary>
    internal List<T> ReadList() => _rows.ReadList();

    /// <inheritdoc cref="ReadList"/>
    internal Task<List<T>> ReadListAsync(CancellationToken cancellationToken) => _rows.ReadListAsync(cancellationToken);

    /// <summary>Disposes the reader (units being decoded are dropped; a stream is closed unless left open).</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
