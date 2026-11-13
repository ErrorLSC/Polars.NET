using System.Diagnostics;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Polars.CSharp;

namespace Polars.Benchmarks;

// Excel 97–2003 (.xls) read through the engine API (DataFrame.ReadExcel, the managed reader), against the native
// calamine bridge (pl_read_excel, until it is removed): 65,535 rows x 10 columns, the columns of ExcelBenchmarks.
// LibreOffice (soffice) converts the .xlsx our writer makes into the .xls.
//
// Filter: --filter '*XlsBenchmarks*'

[MemoryDiagnoser]
[WarmupCount(1)]
[IterationCount(5)]
public partial class XlsBenchmarks
{
    private const int Rows = 65_535;

    private string _dir = "";

    private string ReadPath => Path.Combine(_dir, "read.xls");

    [GlobalSetup]
    public void Setup()
    {
        _dir = Directory.CreateTempSubdirectory("seseragi-xls-bench").FullName;
        string[] categories = [.. Enumerable.Range(0, 50).Select(i => $"category_{i}")];
        var start = new DateTime(2020, 1, 1, 0, 0, 0);
        using var frame = DataFrame.FromColumns(
            ("id", Enumerable.Range(0, Rows).ToArray()),
            ("qty", Enumerable.Range(0, Rows).Select(i => i % 1000).ToArray()),
            ("price", Enumerable.Range(0, Rows).Select(i => i * 0.25 + 0.01).ToArray()),
            // + 0.5: the bridge types a column by its first rows only, and Sqrt(0), Sqrt(1) are whole.
            ("score", Enumerable.Range(0, Rows).Select(i => Math.Sqrt(i) + 0.5).ToArray()),
            ("name", Enumerable.Range(0, Rows).Select(i => $"name_{i}").ToArray()),
            ("category", Enumerable.Range(0, Rows).Select(i => categories[i % categories.Length]).ToArray()),
            ("flag", Enumerable.Range(0, Rows).Select(i => i % 3 == 0).ToArray()),
            ("day", Enumerable.Range(0, Rows).Select(i => DateOnly.FromDateTime(start.AddDays(i % 3000))).ToArray()),
            ("ts", Enumerable.Range(0, Rows).Select(i => start.AddSeconds(i * 37L)).ToArray()),
            ("note", Enumerable.Range(0, Rows).Select(i => i % 4 == 0 ? null : $"note {i % 997}").ToArray()));
        string xlsx = Path.Combine(_dir, "read.xlsx");
        frame.WriteExcel(xlsx);
        using var process = Process.Start(new ProcessStartInfo("soffice", ["--headless", "--convert-to", "xls", "--outdir", _dir, xlsx])
        { RedirectStandardOutput = true, RedirectStandardError = true })!;
        process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        if (!File.Exists(ReadPath)) throw new InvalidOperationException("LibreOffice (soffice) did not convert the workbook to .xls.");
    }

    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dir, true);

    [Benchmark(Baseline = true)]
    public long ReadBridge()
    {
        IntPtr df = pl_read_excel(ReadPath, null, 0, IntPtr.Zero, true, 100, true, true);
        if (df == IntPtr.Zero) throw new InvalidOperationException("The native reader failed: " + Marshal.PtrToStringUTF8(pl_get_last_error()));
        pl_dataframe_free(df);
        return 1;
    }

    [Benchmark]
    public long Read()
    {
        using var df = DataFrame.ReadExcel(ReadPath);
        return df.Height;
    }

    [LibraryImport("native_shim", StringMarshalling = StringMarshalling.Utf8)]
    private static partial IntPtr pl_read_excel(string path, string? sheetName, UIntPtr sheetIndex, IntPtr schema,
        [MarshalAs(UnmanagedType.U1)] bool hasHeader, UIntPtr inferSchemaLen,
        [MarshalAs(UnmanagedType.U1)] bool dropEmptyRows, [MarshalAs(UnmanagedType.U1)] bool raiseIfEmpty);

    [LibraryImport("native_shim")]
    private static partial void pl_dataframe_free(IntPtr ptr);

    [LibraryImport("native_shim")]
    private static partial IntPtr pl_get_last_error();
}
