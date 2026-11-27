using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Data.Database;
using Kaminari.Sazanami.Data.Mapping;

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
    public static LazyFrame ScanEnumerable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        IEnumerable<T> data, 
        Schema? schema = null, 
        int batchSize = 100_000,
        bool useBuffered = false)
    {
        // 1. Get Schema (Cached)
        schema ??= ArrowRowBuilder<T>.Schema;

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
                yield return ArrowStreamingExtensions.EmptyBatch<T>();
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
    /// A lazy scan of a stream of record batches of <paramref name="schema"/>'s columns and types. A stream is read once:
    /// the first run of the query reads it (each batch is released once handed to the engine), a later run fails
    /// with an error saying so; to run a query again, collect it once and reuse the frame, or scan a source that can be
    /// opened again (<see cref="ScanDatabase(Func{System.Data.IDataReader}, int)"/>, <see cref="ScanEnumerable"/>).
    /// </summary>
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        return new LazyFrame(ArrowStreamInterop.ScanStreamOnce(() => stream, schema));
    }
}