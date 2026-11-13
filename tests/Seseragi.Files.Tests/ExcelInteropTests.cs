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
[Collection("soffice")] // one LibreOffice at a time: parallel instances contend for its user profile
public class ExcelInteropTests
{
    /// <summary>
    /// LibreOffice converts calamine's .xlsb fixtures (written by Excel) to .xlsx; our xlsb reader must read each
    /// sheet as our xlsx reader reads LibreOffice's copy. Runs when SESERAGI_EXCEL_PYTHON is set and <c>soffice</c>
    /// is on the path.
    /// </summary>
    [Fact]
    public async Task Xlsb_reads_as_LibreOffice_xlsx_copies()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SESERAGI_EXCEL_PYTHON"))) return;
        if (!(Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator).Any(d => File.Exists(Path.Combine(d, "soffice")))) return;
        string dir = Directory.CreateTempSubdirectory("seseragi-xlsb-interop").FullName;
        try
        {
            string[] names = ["any_sheets", "date", "date_1904", "date_rk_int", "float_values", "formula_values", "issues", "formula_references"];
            string source = Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "calamine");
            var psi = new ProcessStartInfo("soffice", ["--headless", "--convert-to", "xlsx", "--outdir", dir, .. names.Select(n => Path.Combine(source, n + ".xlsb"))])
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using (var process = Process.Start(psi)!)
            {
                await process.StandardOutput.ReadToEndAsync();
                await process.WaitForExitAsync();
            }
            foreach (string name in names)
            {
                using var xlsb = ExcelWorkbook.Open(Path.Combine(source, name + ".xlsb"));
                using var xlsx = ExcelWorkbook.Open(Path.Combine(dir, name + ".xlsx"));
                // LibreOffice has no chart sheets: its copy turns them into worksheets.
                Assert.Equal(xlsb.Sheets.Select(s => s.Name), xlsx.Sheets.Select(s => s.Name));
                foreach (var sheet in xlsb.Sheets.Where(s => s.Kind == ExcelSheetKind.Worksheet))
                {
                    var options = new ExcelReadOptions { SheetIndex = sheet.Index, HasHeader = false, RaiseIfEmpty = false };
                    using var a = xlsb.OpenSheet(options);
                    using var b = xlsx.OpenSheet(options);
                    Assert.Equal(a.Schema.FieldsList.Select(f => f.DataType.Name), b.Schema.FieldsList.Select(f => f.DataType.Name));
                    // LibreOffice has no boolean type: its copy stores some TRUE/FALSE formula results as 1/0.
                    Assert.Equal(ArrowRender.Rows(a.ReadBatches()).Select(Booleans), ArrowRender.Rows(b.ReadBatches()).Select(Booleans));
                }
            }
        }
        finally
        {
            Directory.Delete(dir, true);
        }

        static string Booleans(string row) => row.Replace("\"false\"", "\"0\"").Replace("\"true\"", "\"1\"");
    }

    /// <summary>
    /// The record layouts no Excel-written fixture shows (inline, rich and formula strings, booleans, errors, RK
    /// variants, tables) are checked against LibreOffice's own xlsb import: its .xlsx copy of a workbook built record
    /// by record must read as the original. Runs when SESERAGI_EXCEL_PYTHON is set and <c>soffice</c> is on the path.
    /// </summary>
    [Fact]
    public async Task Built_xlsb_reads_as_LibreOffice_xlsx_copy()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SESERAGI_EXCEL_PYTHON"))) return;
        if (!(Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator).Any(d => File.Exists(Path.Combine(d, "soffice")))) return;
        string dir = Directory.CreateTempSubdirectory("seseragi-xlsb-built").FullName;
        try
        {
            var b = new XlsbBuilder();
            int abc = b.SharedString("abc");
            b.Sheet("Cells")
                .Row(0).Text(0, "kind").Text(1, "value")
                .Row(1).Text(0, "st").Text(1, "inline ✓")
                .Row(2).Text(0, "isst").Shared(1, abc)
                .Row(3).Text(0, "rich").RichText(1, "rich text")
                // No formula records (LibreOffice recalculates on load, and the builder's formulas are empty) and no
                // number formats (it drops them from the builder's minimal styles.bin): Excel's own files cover both
                // (formula_values.xlsb, date_rk_int.xlsb).
                .Row(5).Text(0, "bool").Bool(1, true)
                .Row(6).Text(0, "error").Error(1)
                .Row(8).Text(0, "real").Real(1, 2.5)
                .Row(9).Text(0, "rk int").Rk(1, XlsbBuilder.RkInt(-123))
                .Row(10).Text(0, "rk int x100").Rk(1, XlsbBuilder.RkInt(12345, x100: true))
                .Row(11).Text(0, "rk float").Rk(1, XlsbBuilder.RkFloat(0.75))
                .Row(12).Text(0, "rk float x100").Rk(1, XlsbBuilder.RkFloat(1.5, x100: true))
                .EndSheet();
            b.Sheet("Tables")
                .Row(2).Text(1, "region").Text(2, "amount")
                .Row(3).Text(1, "east").Real(2, 10.5)
                .Row(4).Text(1, "total").Real(2, 10.5)
                .Table("Sales", 2, 1, 4, 2, totalsRows: 1)
                .EndSheet();
            string xlsb = Path.Combine(dir, "built.xlsb");
            using (var f = File.Create(xlsb)) b.Build().CopyTo(f);
            var psi = new ProcessStartInfo("soffice", ["--headless", "--convert-to", "xlsx", "--outdir", dir, xlsb]) { RedirectStandardOutput = true, RedirectStandardError = true };
            using (var process = Process.Start(psi)!)
            {
                await process.StandardOutput.ReadToEndAsync();
                await process.WaitForExitAsync();
            }
            using var ours = ExcelWorkbook.Open(xlsb);
            using var theirs = ExcelWorkbook.Open(Path.Combine(dir, "built.xlsx"));
            foreach (var options in new[] { new ExcelReadOptions { SheetName = "Cells" }, new ExcelReadOptions { SheetName = "Tables" } })
            {
                using var a = ours.OpenSheet(options);
                using var c = theirs.OpenSheet(options);
                Assert.Equal(ArrowRender.Rows(a.ReadBatches()).Select(Booleans), ArrowRender.Rows(c.ReadBatches()).Select(Booleans));
            }
            Assert.Equal(ours.Tables.Select(t => (t.Name, t.SheetName, t.Range, t.HeaderRowCount, t.TotalsRowCount)),
                theirs.Tables.Select(t => (t.Name, t.SheetName, t.Range, t.HeaderRowCount, t.TotalsRowCount)));
        }
        finally
        {
            Directory.Delete(dir, true);
        }

        static string Booleans(string row) => row.Replace("\"false\"", "\"0\"").Replace("\"true\"", "\"1\"");
    }

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
