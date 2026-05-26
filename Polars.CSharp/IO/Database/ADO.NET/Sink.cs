using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Data;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    /// <summary>
    /// Stream the result of the LazyFrame calculation into an <see cref="IDataReader"/>.
    /// <para>
    /// This allows processing huge datasets that don't fit in memory by handling them in chunks (RecordBatches).
    /// </para>
    /// <para>
    /// Common use cases:
    /// <list type="bullet">
    /// <item>Bulk inserting data into SQL Databases (using SqlBulkCopy or NpgsqlBinaryImporter).</item>
    /// <item>Streaming data to other .NET libraries that consume IDataReader.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="writerAction">
    /// A callback that receives the <see cref="IDataReader"/>. 
    /// This action executes on a separate thread (Consumer) while the Polars engine (Producer) pumps data.
    /// </param>
    /// <param name="bufferSize">
    /// The number of RecordBatches to buffer in memory. 
    /// If the buffer is full, the Polars engine will pause until the consumer reads more data (Backpressure).
    /// </param>
    /// <param name="typeOverrides">Optional schema overrides to guide the type mapping.</param>
    /// <example>
    /// <code>
    /// // Simulate a large lazy computation
    /// var lf = DataFrame.FromColumns(new { id = new[] { 0, 1, 2, 3, 4 } }).Lazy();
    /// 
    /// // Stream result to a database writer (simulated here)
    /// lf.SinkTo(reader => 
    /// {
    ///     Console.WriteLine("[DB Writer] Started receiving data...");
    ///     while (reader.Read())
    ///     {
    ///         var val = reader.GetValue(0);
    ///         Console.WriteLine($"[DB Writer] Insert row: {val}");
    ///     }
    ///     Console.WriteLine("[DB Writer] Done.");
    /// }, bufferSize: 2);
    /// 
    /// /* Output:
    /// [DB Writer] Started receiving data...
    /// [DB Writer] Insert row: 0
    /// [DB Writer] Insert row: 1
    /// ...
    /// [DB Writer] Done.
    /// */
    /// </code>
    /// </example>
    public void SinkTo(Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        // 1. Producer-Consumer buffer
        using var buffer = new BlockingCollection<RecordBatch>(boundedCapacity: bufferSize);

        // 2. Start consumer (DB Writer)
        var consumerTask = Task.Run(() => 
        {
            
            // ArrowToDbStream is responsible for disguising Buffer as DataReader
            // It automatically handles Dispose, so Batch will be released after writerAction finishes reading
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides);
            // Hand over the reader to user logic
            // Users call bulk.WriteToServer(reader) here
            writerAction(reader);
        });

        // 3. Start producer (Polars Engine - blocking execution in current thread)
        try
        {
            // Push data produced by Rust into Buffer
            // If Buffer is full, this will block, thereby automatically backpressuring the Rust engine
            SinkBatches(buffer.Add);
        }
        finally
        {
            // 4. Notify consumer: no more data
            buffer.CompleteAdding();
        }

        // 5. Wait for consumer to finish writing and throw possible exceptions
        consumerTask.Wait();
    }

}