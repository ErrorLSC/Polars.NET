using System.Diagnostics;
using Kaminari.Seseragi.Files.Excel;

namespace Seseragi.Files.Tests;

/// <summary>
/// Workbooks written here are read back by openpyxl, Python Polars (calamine through fastexcel) and LibreOffice
/// (TestData/excel/interop_check.py). Opt-in: set SESERAGI_EXCEL_PYTHON to a Python with openpyxl and polars
/// installed, e.g. ~/.cache/seseragi/excel-venv/bin/python (LibreOffice is used when <c>soffice</c> is on the path).
/// Without it the test only writes the files. SESERAGI_EXCEL_KEEP keeps them (the directory is printed).
/// </summary>
[Trait("Category", "Interop")]
public class ExcelInteropTests
{
    [Fact]
    public async Task Reference_readers_read_our_files()
    {
        string dir = Directory.CreateTempSubdirectory("seseragi-excel-interop").FullName;
        try
        {
            var wide = ExcelWriteTests.Wide();
            string shared = Path.Combine(dir, "wide_shared.xlsx");
            string inline = Path.Combine(dir, "wide_inline.xlsx");
            ExcelWriter.Write(shared, wide.Schema, [wide]);
            ExcelWriter.Write(inline, wide.Schema, [wide], new() { SharedStrings = false, Autofit = true, FreezeHeader = true, SheetName = "Wide & co" });

            string? python = Environment.GetEnvironmentVariable("SESERAGI_EXCEL_PYTHON");
            if (string.IsNullOrEmpty(python)) return;
            string script = Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "interop_check.py");
            string fixtures = Path.Combine(AppContext.BaseDirectory, "TestData", "excel");
            var psi = new ProcessStartInfo(python, ["-I", script, shared, inline, fixtures]) { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(psi)!;
            string stdout = await process.StandardOutput.ReadToEndAsync();
            string stderr = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, $"interop_check.py failed ({process.ExitCode}):\n{stdout}\n{stderr}");
        }
        finally
        {
            if (Environment.GetEnvironmentVariable("SESERAGI_EXCEL_KEEP") is null) Directory.Delete(dir, true);
            else Console.WriteLine($"kept {dir}");
        }
    }
}
