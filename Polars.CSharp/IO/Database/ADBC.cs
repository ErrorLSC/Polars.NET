using Apache.Arrow.Adbc;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Generate DataFrame from ADBC query results
    /// </summary>
    /// <param name="statement"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static DataFrame ReadAdbc(AdbcStatement statement)
    {
        ArgumentNullException.ThrowIfNull(statement);

        var result = statement.ExecuteQuery();

        if (result.Stream == null)
        {
            throw new InvalidOperationException("ADBC query executed, but returned a null Arrow stream.");
        }

        return FromArrowStream(result.Stream);
    }
    /// <summary>
    /// Executes a SQL query directly against an ADBC connection and reads the result into a zero-copy Polars DataFrame.
    /// Pure syntactic sugar: automatically manages the creation and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="sqlQuery">The SQL query string to execute.</param>
    /// <returns>A fully materialized Polars DataFrame containing the query results.</returns>
    public static DataFrame ReadAdbc(AdbcConnection connection, string sqlQuery)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (string.IsNullOrWhiteSpace(sqlQuery))
            throw new ArgumentException("SQL query cannot be null or whitespace.", nameof(sqlQuery));

        // Since Polars synchronously materializes the entire stream during FromArrowStream,
        // it is perfectly safe to dispose the statement immediately after the read completes.
        using AdbcStatement statement = connection.CreateStatement();
        statement.SqlQuery = sqlQuery;

        // Route to the core execution method
        return ReadAdbc(statement);
    }
    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database (e.g., DuckDB, SQLite).
    /// </summary>
    /// <param name="statement">An AdbcStatement configured with ingest options (e.g., target table).</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    public UpdateResult WriteToAdbc(AdbcStatement statement)
    {
        ArgumentNullException.ThrowIfNull(statement);

        try
        {
            // Delegate all unsafe pointer handling, FFI bindings, and execution to the Core layer.
            // This ensures no raw pointers leak into the managed high-level API.
            return AdbcInterop.ExecuteIngest(statement, Handle);
        }
        finally
        {
            // Crucial: Pin the DataFrame to prevent the Garbage Collector from 
            // reclaiming the underlying Rust memory while the ADBC C++ engine is actively pulling data.
            GC.KeepAlive(this);
        }
    }
    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database table.
    /// Pure syntactic sugar: automatically manages the creation, configuration, and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="tableName">The name of the target table to ingest data into.</param>
    /// <param name="ingestMode">The ingestion mode (e.g., "adbc.ingest.mode.create" or "adbc.ingest.mode.append"). Defaults to create.</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    public UpdateResult WriteToAdbc(AdbcConnection connection, string tableName,AdbcIngestMode ingestMode = AdbcIngestMode.Create)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Target table name cannot be null or whitespace.", nameof(tableName));

        // Let the framework handle the Statement lifecycle
        using AdbcStatement statement = connection.CreateStatement();
        
        // Configure ADBC bulk ingest options automatically
        statement.SetOption("adbc.ingest.target_table", tableName);
        
        string modeString = ingestMode switch
        {
            AdbcIngestMode.Create  => "adbc.ingest.mode.create",
            AdbcIngestMode.Append  => "adbc.ingest.mode.append",
            AdbcIngestMode.Replace => "adbc.ingest.mode.replace",
            _ => throw new ArgumentOutOfRangeException(nameof(ingestMode), $"Unsupported ingest mode: {ingestMode}")
        };

        statement.SetOption("adbc.ingest.mode", modeString);

        return WriteToAdbc(statement);
    }
}