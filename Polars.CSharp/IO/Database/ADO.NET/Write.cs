using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Data;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Common Write Interface:Transform DataFrame to IDataReader
    /// </summary>
    public void WriteTo( Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        using var buffer = new BlockingCollection<RecordBatch>(bufferSize);

        // Consumer Task
        var consumerTask = Task.Run(() => 
        {
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides);
            
            writerAction(reader);
        });

        // Producer
        try
        {
            ExportBatches(buffer.Add);
        }
        finally
        {
            buffer.CompleteAdding();
        }

        consumerTask.Wait();
    } 
}