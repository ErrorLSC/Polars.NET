using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    private static IEnumerable<RecordBatch> EnsureStreamSafety(IEnumerable<RecordBatch> source)
    {
        using var enumerator = source.GetEnumerator();

        while (enumerator.MoveNext())
        {
            var batch = enumerator.Current;
            yield return batch;
            batch.Dispose();
        }
    }
    /// <summary>
    /// Scan Enumberable As LazyFrame
    /// </summary>
    /// <returns></returns>
    public static LazyFrame ScanEnumerable<T>(
        IEnumerable<T> data, 
        Schema? schema = null, 
        int batchSize = 100_000,
        bool useBuffered = false)
    {
        // 1. Get Schema (Cached)
        schema ??= ArrowConverter.GetSchemaFromType<T>();

        // 2. Buffered Mode
        if (useBuffered)
        {
            var scope = new IpcStreamService.TempIpcScope<T>(data, batchSize); 
            var handleBuffered = ScanIpc(scope.FilePath!).Handle;
            return new ScopedLazyFrame(handleBuffered, scope);
        }

        // 3. Streaming Mode (Memory Pointer)
        IEnumerable<RecordBatch> SafeGenerator()
        {
            bool hasYielded = false;

            foreach (var batch in data.ToArrowBatches(batchSize).Prefetch())
            {
                hasYielded = true;
                yield return batch;
            }

            if (!hasYielded)
            {
                yield return ArrowConverter.GetEmptyBatch<T>();
            }
        }

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(SafeGenerator()), 
            schema
        );
        
        return new LazyFrame(handle);
    }

    /// <summary>
    /// Scan RecordBatch Stream
    /// If schema is provied, first batch won't be consumed for getting schema.
    /// </summary>
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema)
    {
        // ArgumentNullException.ThrowIfNull(nameof(schema));

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(stream),
            schema
        );
        return new LazyFrame(handle);
    }
}