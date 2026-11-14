using Apache.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Parquet;

namespace Polars.Config.Tests;

/// <summary>
/// A consumer that stops after the first batch leaves no batch memory behind (none waits for the finalizer): the
/// batches decoded but not handed out are disposed. The count of live batch memories is process-wide, hence here,
/// in the serial project.
/// </summary>
public class ParquetEarlyStopTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void StoppingEarly_FreesTheBatchesNotTaken(int parallelism)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "duckdb_zstd.parquet");
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long before = NativeBatchMemory.Live;
        using (var file = ParquetFile.Open(path))
        using (var reader = file.OpenReader(new() { BatchSize = 2, MaxDegreeOfParallelism = parallelism }))
        {
            using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
            Assert.True(batches.MoveNext());
            batches.Current.Dispose();
            Assert.True(NativeBatchMemory.Live > before, "the row group's other batches are still held");
        }
        Assert.Equal(before, NativeBatchMemory.Live);
    }
}
