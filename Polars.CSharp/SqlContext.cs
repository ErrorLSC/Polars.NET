using System;
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// A SQL Context allows running SQL queries on LazyFrames.
/// </summary>
public class SqlContext : IDisposable
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
        // Wrapper 会调用 TransferOwnership，导致传入的 handle 失效。
        // 为了保护用户的 C# 对象不被意外销毁，我们 Clone 一个 handle 传进去。
        // Rust 侧会拥有这个 Clone 的所有权。
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
    /// <summary>
    /// Dispose the SQL Context and release resources.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
}