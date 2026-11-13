using BenchmarkDotNet.Attributes;
using Polars.CSharp;

namespace Polars.Benchmarks;

// Excel (.xlsx) read and write through the engine API (DataFrame.ReadExcel / WriteExcel), 500k rows x 10 columns:
// integers, doubles, short unique and repeated strings, a boolean, a date, a datetime and a nullable string.
// Integers are Int32 so that every writer stores them as numbers.
//
// Filter: --filter '*ExcelBenchmarks*'

[MemoryDiagnoser]
[WarmupCount(1)]
[IterationCount(5)]
public class ExcelBenchmarks
{
    private const int Rows = 500_000;

    private string _dir = "";
    private DataFrame _frame = null!;

    private string ReadPath => Path.Combine(_dir, "read.xlsx");
    private string WritePath => Path.Combine(_dir, "write.xlsx");

    [GlobalSetup]
    public void Setup()
    {
        _dir = Directory.CreateTempSubdirectory("seseragi-excel-bench").FullName;
        string[] categories = [.. Enumerable.Range(0, 50).Select(i => $"category_{i}")];
        var start = new DateTime(2020, 1, 1, 0, 0, 0);
        _frame = DataFrame.FromColumns(
            ("id", Enumerable.Range(0, Rows).ToArray()),
            ("qty", Enumerable.Range(0, Rows).Select(i => i % 1000).ToArray()),
            ("price", Enumerable.Range(0, Rows).Select(i => i * 0.25 + 0.01).ToArray()),
            ("score", Enumerable.Range(0, Rows).Select(i => Math.Sqrt(i)).ToArray()),
            ("name", Enumerable.Range(0, Rows).Select(i => $"name_{i}").ToArray()),
            ("category", Enumerable.Range(0, Rows).Select(i => categories[i % categories.Length]).ToArray()),
            ("flag", Enumerable.Range(0, Rows).Select(i => i % 3 == 0).ToArray()),
            ("day", Enumerable.Range(0, Rows).Select(i => DateOnly.FromDateTime(start.AddDays(i % 3000))).ToArray()),
            ("ts", Enumerable.Range(0, Rows).Select(i => start.AddSeconds(i * 37L)).ToArray()),
            ("note", Enumerable.Range(0, Rows).Select(i => i % 4 == 0 ? null : $"note {i % 997}").ToArray()));
        _frame.WriteExcel(ReadPath);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _frame.Dispose();
        Directory.Delete(_dir, true);
    }

    [Benchmark]
    public long Read()
    {
        using var df = DataFrame.ReadExcel(ReadPath);
        return df.Height;
    }

    [Benchmark]
    public long Write()
    {
        _frame.WriteExcel(WritePath);
        return new FileInfo(WritePath).Length;
    }
}
