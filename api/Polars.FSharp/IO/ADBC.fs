namespace Polars.FSharp

[<AutoOpen>]
module ADBC =
    open Apache.Arrow.Adbc
    open System
    type DataFrame with
        
        /// <summary>
        /// Zero-copy bulk ingest of the current DataFrame into an ADBC database table.
        /// Pure syntactic sugar: automatically manages the creation, configuration, and disposal of the underlying AdbcStatement.
        /// </summary>
        /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
        /// <param name="tableName">The name of the target table to ingest data into.</param>
        /// <param name="ingestMode">The ingestion mode. Defaults to Create.</param>
        /// <returns>The UpdateResult containing the number of rows affected.</returns>
        member this.WriteToAdbc(connection: AdbcConnection, tableName: string, ?ingestMode: AdbcIngestMode) : UpdateResult =
            ArgumentNullException.ThrowIfNull connection
            
            if String.IsNullOrWhiteSpace tableName then
                invalidArg "tableName" "Target table name cannot be null or whitespace."

            let mode = defaultArg ingestMode AdbcIngestMode.Create

            // Let the framework handle the Statement lifecycle
            use statement = connection.CreateStatement()
            
            // Configure ADBC bulk ingest options automatically
            statement.SetOption("adbc.ingest.target_table", tableName)
            
            let modeString = 
                match mode with
                | AdbcIngestMode.Create  -> "adbc.ingest.mode.create"
                | AdbcIngestMode.Append  -> "adbc.ingest.mode.append"
                | AdbcIngestMode.Replace -> "adbc.ingest.mode.replace"

            statement.SetOption("adbc.ingest.mode", modeString)

            // Route to the core execution method
            this.WriteToAdbc statement
