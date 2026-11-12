using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Data;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
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
    public static LazyFrame ScanEnumerable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] T>(
        IEnumerable<T> data, 
        Schema? schema = null, 
        int batchSize = 100_000,
        bool useBuffered = false)
    {
        // 1. Get Schema (Cached)
        schema ??= ArrowConverter.GetSchemaFromType<T>();

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

        // Buffered: the data is read now into a temporary Arrow IPC file the engine owns.
        if (useBuffered)
            return new LazyFrame(ArrowStreamInterop.ScanStreamBuffered(() => EnsureStreamSafety(SafeGenerator()), schema));

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

        // The batches may use any standard Arrow layout, so the engine converts every column.
        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(stream),
            schema,
            strict: true
        );
        return new LazyFrame(handle);
    }
}