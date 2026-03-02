using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// A context for executing SQL queries against DataFrames and LazyFrames.
/// <para>
/// Polars supports a subset of ANSI SQL. The query is converted into a logical plan 
/// and optimized/executed by the Polars engine.
/// </para>
/// </summary>
/// <example>
/// <code>
/// using var ctx = new SqlContext();
/// 
/// // Register DataFrames as tables
/// ctx.Register("df", df);
/// 
/// // Execute SQL query
/// // Note: Returns a LazyFrame, so you must call Collect() to materialize data.
/// var result = ctx.Execute("SELECT * FROM df WHERE val > 10").Collect();
/// </code>
/// </example>
public class SqlContext : IDisposable,IPolarsSqlContext
{
    internal SqlContextHandle Handle { get; }
    /// <summary>
    /// Create a new SQL Context.
    /// </summary>
    public SqlContext()
    {
        Handle = PolarsWrapper.SqlContextNew();
    }

    /// <summary>
    /// Register a LazyFrame as a table in the SQL context.
    /// </summary>
    /// <param name="tableName">The name to use in SQL queries (e.g., 'SELECT * FROM tableName')</param>
    /// <param name="lf">The LazyFrame to register.</param>
    public void Register(string tableName, LazyFrame lf)
    {
        var clonedHandle = lf.CloneHandle();
        PolarsWrapper.SqlRegister(Handle, tableName, clonedHandle);
    }

    /// <summary>
    /// Register a DataFrame as a table (convenience method).
    /// Converts DataFrame to LazyFrame internally.
    /// </summary>
    public void Register(string tableName, DataFrame df)
    {
        // DataFrame -> LazyFrame -> Clone Handle
        using var lf = df.Lazy();
        Register(tableName, lf);
    }

    void IPolarsSqlContext.Register(string tableName,IPolarsDataFrame df)
    {
        Register(tableName,(DataFrame)df);
    }
    void IPolarsSqlContext.Register(string tableName,IPolarsLazyFrame lf)
    {
        Register(tableName,(LazyFrame)lf);
    }

    /// <summary>
    /// UnRegister a LazyFrame as a table.
    /// </summary>
    public void UnRegister(string tableName)
    {
        PolarsWrapper.SqlUnRegister(Handle,tableName);
    }

    /// <summary>
    /// Execute a SQL query.
    /// </summary>
    /// <param name="query">The SQL query string.</param>
    /// <returns>A new LazyFrame representing the query result.</returns>
    public LazyFrame Execute(string query)
    {
        var lfHandle = PolarsWrapper.SqlExecute(Handle, query);
        return new LazyFrame(lfHandle);
    }
    IPolarsLazyFrame IPolarsSqlContext.Execute(string query)
    {
        return Execute(query);
    }
    /// <summary>
    /// Get the names of all registered tables, in sorted order.
    /// </summary>
    /// <returns>An array of registered table names.</returns>
    public string[] GetTables()
    {
        return PolarsWrapper.SqlGetTables(Handle);
    }
    /// <summary>
    /// Dispose the SQL Context and release resources.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
}