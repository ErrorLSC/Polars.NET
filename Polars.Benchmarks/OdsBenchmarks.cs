using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Kaminari.Seseragi.Files.Excel;
using Polars.CSharp;

namespace Polars.Benchmarks;

// OpenDocument (.ods) read through the engine API (DataFrame.ReadExcel), and listing the sheets (a pass over all of
// content.xml): 200,000 rows x 10 columns, the columns of ExcelBenchmarks. LibreOffice (soffice) converts the .xlsx
// our writer makes into the .ods.
//
// Filter: --filter '*OdsBenchmarks*'

[MemoryDiagnoser]
[WarmupCount(1)]
[IterationCount(5)]
public class OdsBenchmarks
{
    private const int Rows = 200_000;

    private string _dir = "";

    private string ReadPath => Path.Combine(_dir, "read.ods");

    [GlobalSetup]
    public void Setup()
    {
        _dir = Directory.CreateTempSubdirectory("seseragi-ods-bench").FullName;
        string[] categories = [.. Enumerable.Range(0, 50).Select(i => $"category_{i}")];
        var start = new DateTime(2020, 1, 1, 0, 0, 0);
        using var frame = DataFrame.FromColumns(
            ("id", Enumerable.Range(0, Rows).ToArray()),
            ("qty", Enumerable.Range(0, Rows).Select(i => i % 1000).ToArray()),
            ("price", Enumerable.Range(0, Rows).Select(i => i * 0.25 + 0.01).ToArray()),
            ("score", Enumerable.Range(0, Rows).Select(i => Math.Sqrt(i) + 0.5).ToArray()),
            ("name", Enumerable.Range(0, Rows).Select(i => $"name_{i}").ToArray()),
            ("category", Enumerable.Range(0, Rows).Select(i => categories[i % categories.Length]).ToArray()),
            ("flag", Enumerable.Range(0, Rows).Select(i => i % 3 == 0).ToArray()),
            ("day", Enumerable.Range(0, Rows).Select(i => DateOnly.FromDateTime(start.AddDays(i % 3000))).ToArray()),
            ("ts", Enumerable.Range(0, Rows).Select(i => start.AddSeconds(i * 37L)).ToArray()),
            ("note", Enumerable.Range(0, Rows).Select(i => i % 4 == 0 ? null : $"note {i % 997}").ToArray()));
        string xlsx = Path.Combine(_dir, "read.xlsx");
        frame.WriteExcel(xlsx);
        using var process = Process.Start(new ProcessStartInfo("soffice", ["--headless", "--convert-to", "ods", "--outdir", _dir, xlsx])
        { RedirectStandardOutput = true, RedirectStandardError = true })!;
        process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        if (!File.Exists(ReadPath)) throw new InvalidOperationException("LibreOffice (soffice) did not convert the workbook to .ods.");
    }

    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dir, true);

    [Benchmark]
    public long Read()
    {
        using var df = DataFrame.ReadExcel(ReadPath);
        return df.Height;
    }

    // Listing the sheets scans all of content.xml (reading the first sheet does not need it).
    [Benchmark]
    public int ListSheets()
    {
        using var wb = ExcelWorkbook.Open(ReadPath);
        return wb.Sheets.Count;
    }
}
