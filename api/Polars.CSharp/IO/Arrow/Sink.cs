using Apache.Arrow;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Streaming Sink to Batchs
    /// </summary>
    public void SinkBatches(Action<RecordBatch> onBatchReceived)
    {
        using var newLfHandle = PolarsWrapper.SinkBatches(CloneHandle(), onBatchReceived);

        using var lfRes = new LazyFrame(newLfHandle);
        // The streaming engine hands batches over as they are produced; Auto would run in memory.
        using var _ = lfRes.Collect(Engine.Streaming);
    }

}