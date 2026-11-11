using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;
using Kaminari.Sazanami.Data;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Scan Database to LazyFrame
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanDatabase(IDataReader reader, int batchSize = 50_000)
    {
        var schema = reader.GetArrowSchema();

        var stream = reader.ToArrowBatches(batchSize).Prefetch();

        return ScanRecordBatches(stream, schema);
    }
    /// <summary>
    /// Create a LazyFrame from a database query using a reader factory.
    /// <para>
    /// <b>Recommended:</b> This is the preferred method for interacting with databases in a Lazy context.
    /// </para>
    /// <para>
    /// It accepts a factory function that creates a NEW <see cref="IDataReader"/> on demand.
    /// This allows Polars to:
    /// <list type="bullet">
    /// <item>Inspect the schema upfront (using a probe reader).</item>
    /// <item>Re-execute the query if the execution plan requires multiple passes.</item>
    /// <item>Allow you to call <see cref="Collect"/> multiple times on the same LazyFrame.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="readerFactory">A function that returns a new, open <see cref="IDataReader"/> instance each time it is called.</param>
    /// <param name="batchSize">The size of the Arrow record batch (rows). Larger values reduce overhead.</param>
    /// <returns>A new LazyFrame linked to the database stream.</returns>
    /// <example>
    /// <code>
    /// // Define a factory that returns a new Reader for the query
    /// Func&lt;IDataReader&gt; factory = () =>
    /// {
    ///     var cmd = connection.CreateCommand();
    ///     cmd.CommandText = """
    ///         SELECT name, score
    ///         FROM User
    ///         WHERE score > $min_score OR score IS NULL
    ///     """;
    ///     cmd.Parameters.AddWithValue("$min_score", 60.0);
    ///     return cmd.ExecuteReader();
    /// };
    /// 
    /// // Scan and apply transformations lazily
    /// var lf = LazyFrame.ScanDatabase(factory);
    /// 
    /// var result = lf
    ///     .WithColumns(
    ///         Col("score").FillNull(0.0).Alias("clean_score")
    ///     )
    ///     .Collect();
    ///     
    /// result.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────┬───────┬─────────────┐
    /// │ name    ┆ score ┆ clean_score │
    /// │ ---     ┆ ---   ┆ ---         │
    /// │ str     ┆ f64   ┆ f64         │
    /// ╞═════════╪═══════╪═════════════╡
    /// │ Alice   ┆ 99.5  ┆ 99.5        │
    /// │ Bob     ┆ 85.0  ┆ 85.0        │
    /// │ Charlie ┆ null  ┆ 0.0         │
    /// └─────────┴───────┴─────────────┘
    /// */
    /// </code>
    /// </example>
    public static LazyFrame ScanDatabase(Func<IDataReader> readerFactory, int batchSize = 50_000)
    {
        Schema schema;
        // Probe schema
        using (var probe = readerFactory())
        {
            schema = probe.GetArrowSchema();
        }

        // Replayable stream
        IEnumerable<RecordBatch> StreamFactory()
        {
            using var reader = readerFactory();
            // Get stream
            var stream = reader.ToArrowBatches(batchSize);

            stream = stream.Prefetch();

            foreach (var batch in stream)
                yield return batch;
        }

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(StreamFactory()),
            schema
        );
        return new LazyFrame(handle);
    }
    /// <summary>
    /// [Buffered] Create a LazyFrame from an existing DataReader.
    /// <para><b>Note:</b> This consumes the reader IMMEDIATELY into a temporary Arrow IPC file in Polars's
    /// temporary directory (<c>POLARS_TEMP_DIR</c>). The file has no name in the directory and is removed when the
    /// last frame that scans it is released.</para>
    /// </summary>
    public static LazyFrame ScanDatabaseBuffered(IDataReader reader, int batchSize = 50_000)
    {
        // A DataReader cannot be reset, so it is read now.
        var schema = reader.GetArrowSchema();
        var handle = ArrowStreamInterop.ScanStreamBuffered(
            () => EnsureStreamSafety(reader.ToArrowBatches(batchSize).Prefetch()), schema);
        return new LazyFrame(handle);
    }

}