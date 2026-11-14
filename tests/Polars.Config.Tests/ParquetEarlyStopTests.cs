using Apache.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Samidare.IR;
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

    /// <summary>
    /// Reading through a filter (FilterRows) frees everything it decoded: the filter's columns, the other columns'
    /// unfiltered batches, the filtered copies' shared memory once the batches are disposed, and the rows past MaxRows.
    /// </summary>
    [Theory]
    [InlineData("Dense", 1, false)]
    [InlineData("Sparse", 1, false)]
    [InlineData("Dense", 8, true)]
    [InlineData("Auto", 8, false)]
    [InlineData("Sparse", 8, true)]
    public void FilteredReads_LeaveNoBatchMemoryBehind(string mode, int parallelism, bool limit)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "indexed_v1.parquet");
        ScalarExpr key = ScalarExpr.NewColumn("key");
        ScalarExpr filter = ScalarExpr.NewBinary(BinaryOp.Or,
            ScalarExpr.NewBinary(BinaryOp.Lt, key, ScalarExpr.NewLiteral(LiteralValue.NewInt64(700))),
            ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("cat"), ScalarExpr.NewLiteral(LiteralValue.NewString("c2"))));
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long before = NativeBatchMemory.Live;
        using (var file = ParquetFile.Open(path))
        {
            var options = new ParquetReadOptions
            {
                Filter = filter, FilterRows = true, Columns = ["key", "cat", "f"], MaxDegreeOfParallelism = parallelism, BatchSize = 64,
                MaxRows = limit ? 900 : null,
                LateMaterialization = Enum.Parse<LateMaterialization>(mode),
            };
            using var reader = file.OpenReader(options);
            long rows = 0;
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) rows += batch.Length;
            }
            Assert.True(rows > 0);
        }
        Assert.Equal(before, NativeBatchMemory.Live);
    }
}
