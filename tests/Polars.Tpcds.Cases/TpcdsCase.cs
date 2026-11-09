namespace Polars.Tpcds;

/// <summary>
/// One TPC-DS query: how to run it on Polars and its DuckDB reference SQL, whose columns line up with the result
/// positionally. Row-set queries return their rows, scalar queries a single row holding the value.
/// </summary>
/// <remarks>
/// Results are exposed as <c>object</c> because most of them are anonymous types, which stay private to this assembly.
/// </remarks>
public sealed class TpcdsCase
{
    private readonly Func<string> _explain;
    private readonly Func<IReadOnlyList<object?>> _run;

    private TpcdsCase(string name, Func<string> explain, Func<IReadOnlyList<object?>> run, Func<string> sql)
    {
        Name = name;
        _explain = explain;
        _run = run;
        Sql = sql;
    }

    public string Name { get; }

    public Func<string> Sql { get; }

    /// <summary>The optimized plan of the query, or of the row set a scalar query reduces.</summary>
    public string Explain() => _explain();

    /// <summary>Builds the query, translates it, runs it on Polars and materializes the result.</summary>
    public IReadOnlyList<object?> RunPolars() => _run();

    public static TpcdsCase Rows<T>(string name, Func<IQueryable<T>> query, Func<string> sql) =>
        new(name, () => query().Explain(), () => query().ToList().Cast<object?>().ToList(), sql);

    /// <summary>A query answered by one value computed from a row set.</summary>
    public static TpcdsCase Scalar<T>(string name, Func<IQueryable<T>> query, Func<IQueryable<T>, object?> reduce, Func<string> sql) =>
        new(name, () => query().Explain(), () => [reduce(query())], sql);

    /// <summary>A query answered by the number of rows of a set query.</summary>
    public static TpcdsCase Count<T>(string name, Func<IQueryable<T>> query, Func<string> sql) =>
        Scalar(name, query, source => source.LongCount(), sql);
}
