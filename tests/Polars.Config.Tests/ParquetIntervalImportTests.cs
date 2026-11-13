using Apache.Arrow;
using Apache.Arrow.Ipc;
using Kaminari.Seseragi.Files.Parquet;

namespace Polars.Config.Tests;

/// <summary>
/// INTERVAL columns read by the managed Parquet reader (month-day-nanosecond) import into Polars only with
/// POLARS_IMPORT_INTERVAL_AS_STRUCT, a process-wide switch: hence here, in the serial project.
/// </summary>
public class ParquetIntervalImportTests
{
    private sealed class BatchStream(Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches) : IArrowArrayStream
    {
        private readonly IEnumerator<RecordBatch> _batches = batches.GetEnumerator();
        public Apache.Arrow.Schema Schema => schema;
        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(_batches.MoveNext() ? _batches.Current : null);
        public void Dispose() => _batches.Dispose();
    }

    [Fact]
    public void IntervalColumn_ImportsAsStruct_WithTheSwitch()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "duckdb_zstd.parquet");
        using var file = ParquetFile.Open(path);
        List<(int Months, int Days, long Nanos)?> expected;
        using (var reader = file.OpenReader(new() { Columns = ["ival"] }))
        {
            expected = [.. reader.ReadBatches().SelectMany(b =>
            {
                var a = (MonthDayNanosecondIntervalArray)b.Column(0);
                return Enumerable.Range(0, a.Length).Select(i => a.GetValue(i) is { } v ? ((int, int, long)?)(v.Months, v.Days, v.Nanoseconds) : null).ToList();
            })];
        }
        Assert.Contains(expected, e => e is (> 0, > 0, > 0));

        using (var reader = file.OpenReader(new() { Columns = ["ival"] }))
            Assert.ThrowsAny<Exception>(() => DataFrame.FromArrowStream(new BatchStream(reader.Schema, reader.ReadBatches())).Dispose());

        PolarsConfig.SetEnvVar("POLARS_IMPORT_INTERVAL_AS_STRUCT", "1");
        try
        {
            using var reader = file.OpenReader(new() { Columns = ["ival"] });
            using var df = DataFrame.FromArrowStream(new BatchStream(reader.Schema, reader.ReadBatches()));
            Series ival = df["ival"];
            Assert.Equal(expected.Count, (int)ival.Length);
            int?[] months = ival.Struct.Field("months").ToArray<int?>();
            int?[] days = ival.Struct.Field("days").ToArray<int?>();
            Assert.Equal(expected.Select(e => e?.Months), months);
            Assert.Equal(expected.Select(e => e?.Days), days);
            Assert.StartsWith("duration[ns]", ival.Struct.Field("nanoseconds").DataType.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            PolarsConfig.SetEnvVar("POLARS_IMPORT_INTERVAL_AS_STRUCT", "0");
        }
    }
}
