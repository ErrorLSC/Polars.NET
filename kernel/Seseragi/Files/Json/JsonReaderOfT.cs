using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Apache.Arrow;
using Kaminari.Samidare.Config;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// JSON records read as rows of <typeparamref name="T"/> (records, classes, structs, F# records and tuples, mapped by
/// member name as <c>DataFrame.Rows&lt;T&gt;()</c> maps them): the columns are <typeparamref name="T"/>'s members (as
/// <see cref="JsonWriter{T}"/> writes them, nothing inferred), fields matched by name ignoring case when none matches
/// exactly, other fields passed over. Values as <see cref="JsonReader"/> reads them, and: GUIDs from strings, enums from
/// their names (ignoring case) or numbers, dictionaries keyed by strings from objects, time spans from ISO 8601 or
/// <c>[-][d.]hh:mm:ss[.f]</c> strings. A member of a value type (not nullable) takes no null or missing field: an error
/// with its line (with <see cref="JsonReadOptions.IgnoreErrors"/>, the rows refuse the null, as rows of a frame do;
/// declare the member nullable to receive nulls; the error names the row among all the rows read). An object whose
/// members are all null or missing is an object of nulls. A <c>where</c> predicate is translated by the query
/// compiler (Samidare, as the LINQ provider translates it) and evaluated by the reader on each decoded chunk
/// (<see cref="JsonReadOptions.FilterRows"/>); the conjuncts it cannot evaluate are evaluated on the rows when the
/// compiler's client evaluation policy allows it (<see cref="SamidareConfig.ClientEvaluationPolicy"/>: Strict refuses
/// them). Chunks are decoded, filtered and turned into rows on several threads at once
/// (<see cref="JsonReadOptions.MaxDegreeOfParallelism"/>), the rows delivered in order.
/// </summary>
public sealed class JsonReader<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly JsonReader _reader;
    private readonly TypedRows<T> _rows;

    private JsonReader(JsonReader reader, TypedPlan<T> plan, int parallelism)
    {
        _reader = reader;
        FiltersExactly = plan.Exact ?? reader.FiltersExactly;
        _rows = new TypedRows<T>(reader.ReadBatches, reader.ReadBatchesAsync, plan.Client, plan.ClientMaxRows, parallelism);
    }

    /// <summary>
    /// Whether the rows are exactly those the predicate and the options' <see cref="JsonReadOptions.Filter"/> keep: the
    /// predicate is always applied whole (on the rows where the reader cannot); with a predicate the options' filter is
    /// applied to the rows too, exactly unless a conjunct of it cannot be evaluated by the reader; without one, as
    /// <see cref="JsonReader.FiltersExactly"/> tells. <see cref="JsonReadOptions.MaxRows"/> counts the rows delivered.
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>The columns read for the members of <typeparamref name="T"/>.</summary>
    public Schema Schema => _reader.Schema;

    /// <summary>The reader of the batches (tests).</summary>
    internal JsonReader Inner => _reader;

    /// <summary>Opens a file (see <see cref="JsonReader.Open(string, JsonReadOptions?)"/>).</summary>
    /// <exception cref="ArgumentException">The options give a schema (the rows' members are the columns), or are out of range.</exception>
    /// <exception cref="NotSupportedException">A conjunct of <paramref name="where"/> cannot be pushed down and the client evaluation policy is Strict.</exception>
    public static JsonReader<T> Open(string path, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null) =>
        Of(where, options, o => JsonReader.Open(path, o));

    /// <summary>Opens a stream (see <see cref="JsonReader.Open(Stream, JsonReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)" path="/exception"/>
    public static JsonReader<T> Open(Stream stream, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null) =>
        Of(where, options, o => JsonReader.Open(stream, o));

    /// <summary>Opens data read at offsets (see <see cref="JsonReader.Open(IRandomAccess, JsonReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)" path="/exception"/>
    public static JsonReader<T> Open(IRandomAccess source, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null) =>
        Of(where, options, o => JsonReader.Open(source, o));

    /// <summary>Opens UTF-8 JSON held in memory, read in place (see <see cref="JsonReader.Open(ReadOnlyMemory{byte}, JsonReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)" path="/exception"/>
    public static JsonReader<T> Open(ReadOnlyMemory<byte> bytes, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null) =>
        Of(where, options, o => JsonReader.Open(bytes, o));

    /// <summary>Opens JSON text (see <see cref="JsonReader.Parse(ReadOnlySpan{char}, JsonReadOptions?)"/>).</summary>
    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)" path="/exception"/>
    public static JsonReader<T> Parse(ReadOnlySpan<char> json, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null)
    {
        var (plan, chosen) = Plan(where, options);
        return new JsonReader<T>(JsonReader.Parse(json, chosen), plan, chosen.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="Open(string, Expression{Func{T, bool}}?, JsonReadOptions?)"/>
    public static async Task<JsonReader<T>> OpenAsync(string path, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (plan, chosen) = Plan(where, options);
        return new JsonReader<T>(await JsonReader.OpenAsync(path, chosen, cancellationToken).ConfigureAwait(false), plan, chosen.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="Open(Stream, Expression{Func{T, bool}}?, JsonReadOptions?)"/>
    public static async Task<JsonReader<T>> OpenAsync(Stream stream, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (plan, chosen) = Plan(where, options);
        return new JsonReader<T>(await JsonReader.OpenAsync(stream, chosen, cancellationToken).ConfigureAwait(false), plan, chosen.MaxDegreeOfParallelism);
    }

    /// <inheritdoc cref="Open(IRandomAccess, Expression{Func{T, bool}}?, JsonReadOptions?)"/>
    public static async Task<JsonReader<T>> OpenAsync(IRandomAccess source, Expression<Func<T, bool>>? where = null, JsonReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var (plan, chosen) = Plan(where, options);
        return new JsonReader<T>(await JsonReader.OpenAsync(source, chosen, cancellationToken).ConfigureAwait(false), plan, chosen.MaxDegreeOfParallelism);
    }

    private static JsonReader<T> Of(Expression<Func<T, bool>>? where, JsonReadOptions? options, Func<JsonReadOptions, JsonReader> open)
    {
        var (plan, chosen) = Plan(where, options);
        return new JsonReader<T>(open(chosen), plan, chosen.MaxDegreeOfParallelism);
    }

    // The batch reader's options: the members' columns (nothing inferred), keys ignoring case, the pushed conjuncts.
    private static (TypedPlan<T> Plan, JsonReadOptions Options) Plan(Expression<Func<T, bool>>? where, JsonReadOptions? options)
    {
        options ??= new JsonReadOptions();
        if (options.Schema is not null || options.SchemaOverrides is not null)
            throw new ArgumentException("The rows' members are the columns: give no schema (nor schema overrides).", nameof(options));
        if (options.RowIndexColumn is not null) throw new ArgumentException("Rows take no row index column.", nameof(options));
        Schema schema = JsonRowSchema<T>.Read;
        TypedPlan<T> plan = TypedRows.Plan(where, [.. schema.FieldsList.Select(f => f.Name)], columns => Project(schema, columns),
            new TypedRequest(options.Columns, false, options.Filter, options.MaxRows, "JSON"), "JSON");
        JsonReadOptions chosen = options with { Schema = schema, KeysIgnoreCase = true, Columns = plan.Columns, MaxRows = plan.MaxRows };
        if (plan.FilterRows) chosen = chosen with { Filter = plan.Filter, FilterRows = true };
        return (plan, chosen);
    }

    private static Schema Project(Schema schema, IReadOnlyList<string>? columns) =>
        columns is null ? schema : new Schema([.. schema.FieldsList.Where(f => columns.Contains(f.Name))], schema.Metadata);

    /// <summary>
    /// Reads the rows, chunk by chunk (each batch's memory freed once its rows are made). With
    /// <see cref="JsonReadOptions.MaxDegreeOfParallelism"/> above 1, up to that many batches are turned into rows at once
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

    /// <summary>Disposes the reader (chunks being decoded are dropped; a stream is closed unless left open).</summary>
    public void Dispose() => _reader.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _reader.DisposeAsync();
}
