using System.Diagnostics;
using System.Text.Json;
using Kaminari.Seseragi.Files.Excel;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Seseragi.Files.Tests;

/// <summary>
/// The .xls reader against independent readers: xlrd (with olefile for version 4 compound files) reads the same
/// workbooks cell by cell (TestData/excel/xls_check.py), and LibreOffice's .xlsx copies read like the originals.
/// Opt-in: set SESERAGI_EXCEL_PYTHON to a Python with xlrd and olefile installed
/// (~/.cache/seseragi/excel-venv/bin/python); the LibreOffice test also needs <c>soffice</c> on the path.
/// </summary>
[Trait("Category", "Interop")]
[Collection("soffice")] // one LibreOffice at a time: parallel instances contend for its user profile
public class ExcelXlsInteropTests
{
    private static string Fixtures => Path.Combine(AppContext.BaseDirectory, "TestData", "excel");

    // BIFF8 workbooks among the fixtures (calamine's BIFF5 and malformed files are left out; xlrd cannot read
    // incorrect_sst_unique_count.xls, whose string count is wrong).
    private static IEnumerable<string> Workbooks() =>
        Directory.GetFiles(Path.Combine(Fixtures, "calamine"), "*.xls")
            .Where(f => Path.GetFileNameWithoutExtension(f) is not ("password_protected" or "too_small" or "incorrect_sst_unique_count")
                && !Path.GetFileName(f).StartsWith("cfb_", StringComparison.Ordinal)
                && !Path.GetFileName(f).StartsWith("OOM_alloc", StringComparison.Ordinal)
                && !Path.GetFileName(f).Contains("biff5", StringComparison.Ordinal))
            .Concat(Directory.GetFiles(Fixtures, "*.xls"))
            .Where(IsBiff8)
            .Order();

    private static bool IsBiff8(string path)
    {
        try
        {
            using var wb = ExcelWorkbook.Open(path);
            return true;
        }
        catch (ExcelFormatException e) when (e.Message.Contains("BIFF5", StringComparison.Ordinal))
        {
            return false;
        }
    }

    [Fact]
    public async Task Cells_match_xlrd()
    {
        string? python = Environment.GetEnvironmentVariable("SESERAGI_EXCEL_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        string dir = Directory.CreateTempSubdirectory("seseragi-xls-xlrd").FullName;
        try
        {
            string dump = Path.Combine(dir, "cells.jsonl");
            await using (var file = File.Create(dump))
            {
                foreach (string path in Workbooks().Concat(Built(dir)).Concat(Built(dir, version: 4))) await DumpAsync(path, file);
            }
            var psi = new ProcessStartInfo(python, ["-I", Path.Combine(Fixtures, "xls_check.py"), dump]) { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(psi)!;
            string stdout = await process.StandardOutput.ReadToEndAsync();
            string stderr = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, $"xls_check.py failed ({process.ExitCode}):\n{stdout}\n{stderr}");
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// LibreOffice converts .xls workbooks (Excel's, and ours built record by record, in version 3 and 4 compound
    /// files) to .xlsx; our xls reader must read each worksheet as our xlsx reader reads LibreOffice's copy.
    /// </summary>
    [Fact]
    public async Task Xls_reads_as_LibreOffice_xlsx_copies()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SESERAGI_EXCEL_PYTHON"))) return;
        if (!(Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator).Any(d => File.Exists(Path.Combine(d, "soffice")))) return;
        string dir = Directory.CreateTempSubdirectory("seseragi-xls-lo").FullName;
        try
        {
            string calamine = Path.Combine(Fixtures, "calamine");
            string[] names = ["any_sheets", "date", "date_1904", "formula_references", "issues", "sst_continue", "xls_cross_sheet_chart",
                "continued_extended_string", "dimensions_16_bytes", "merged_range"];
            List<string> sources = [.. names.Select(n => Path.Combine(calamine, n + ".xls")),
                // No formulas in the built ones: LibreOffice recalculates on load, and the builder's formulas are empty.
                // Version 3 only: LibreOffice did not open our version 4 files (olefile and xlrd check those).
                .. Built(dir, formulas: false)];
            string outDir = Path.Combine(dir, "xlsx");
            var psi = new ProcessStartInfo("soffice", ["--headless", "--convert-to", "xlsx", "--outdir", outDir, .. sources]) { RedirectStandardOutput = true, RedirectStandardError = true };
            using (var process = Process.Start(psi)!)
            {
                await process.StandardOutput.ReadToEndAsync();
                await process.WaitForExitAsync();
            }
            foreach (string source in sources)
            {
                using var xls = ExcelWorkbook.Open(source);
                using var xlsx = ExcelWorkbook.Open(Path.Combine(outDir, Path.GetFileNameWithoutExtension(source) + ".xlsx"));
                // LibreOffice has no chart sheets: its copy turns them into worksheets.
                Assert.Equal(xls.Sheets.Select(s => s.Name), xlsx.Sheets.Select(s => s.Name));
                foreach (var sheet in xls.Sheets.Where(s => s.Kind == ExcelSheetKind.Worksheet))
                {
                    var options = new ExcelReadOptions { SheetIndex = sheet.Index, HasHeader = false, RaiseIfEmpty = false };
                    using var a = xls.OpenSheet(options);
                    using var b = xlsx.OpenSheet(options);
                    string what = $"{Path.GetFileName(source)} [{sheet.Name}]";
                    Assert.True(a.Schema.FieldsList.Select(f => f.DataType.Name).SequenceEqual(b.Schema.FieldsList.Select(f => f.DataType.Name)), what);
                    // LibreOffice has no boolean type: its copy stores some TRUE/FALSE formula results as 1/0.
                    Assert.Equal(ArrowRender.Rows(b.ReadBatches()).Select(Booleans), ArrowRender.Rows(a.ReadBatches()).Select(Booleans));
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
    /// Workbooks built record by record, for the layouts no fixture shows: strings split across Continue records
    /// (shared and formula strings, with a change of width, runs and phonetic data), every RK variant, MulRk and
    /// MulBlank, formula results, and workbooks in the mini stream and in regular sectors.
    /// </summary>
    internal static IEnumerable<string> Built(string dir, int version = 3, bool formulas = true)
    {
        // No characters beyond the BMP: xlrd counts a String record's characters as Python characters, so a surrogate
        // pair throws it off (XlsReadTests covers split pairs).
        string[] texts = ["short", "a longer string that spans records", "宽字符と Latin mixed ✓ text", "ÿ Latin-1 only ÿ", "Ωmega ≠ ascii, again Ω"];
        int[] maxRecords = [20, 37, 8224];
        foreach (int maxRecord in maxRecords)
        {
            var b = new XlsBuilder { MaxRecord = maxRecord };
            var ids = texts.Select((t, i) => b.SharedString(t, runs: i % 2 == 0 ? 2 : 0, ext: i % 3 == 0 ? 11 : 0)).ToArray();
            int date = b.Style(14);
            int custom = b.Style(170, "yyyy\\-mm\\-dd hh:mm");
            b.Sheet("Strings");
            for (int i = 0; i < texts.Length; i++)
            {
                b.LabelSst(i, 0, ids[i]);
                if (formulas) b.FormulaString(i, 1, texts[i]);
                b.Label(i, 2, texts[i]);
            }
            b.EndSheet();
            b.Sheet("Values", state: 1)
                .Number(0, 0, 2.5).Rk(0, 1, XlsBuilder.RkInt(-123)).Rk(0, 2, XlsBuilder.RkInt(12345, x100: true))
                .Rk(0, 3, XlsBuilder.RkFloat(0.75)).Rk(0, 4, XlsBuilder.RkFloat(1.5, x100: true))
                .MulRk(1, 0, (XlsBuilder.RkInt(1), date), (XlsBuilder.RkFloat(2.5), custom)).MulBlank(1, 2, 2).Number(1, 4, 45306.5, custom)
                .Bool(2, 0, true).Error(2, 1);
            if (formulas)
            {
                b.FormulaNumber(2, 2, 7).FormulaBool(2, 3, false).FormulaError(2, 4)
                    .FormulaEmpty(3, 0).FormulaString(3, 1, "after ShrFmla", sharedFormula: true);
            }
            b.EndSheet();
            if (maxRecord == 8224)
            {
                // Past 4096 bytes: the workbook stream leaves the mini stream for regular sectors.
                b.Sheet("Many");
                for (int r = 0; r < 400; r++) b.Number(r, 0, r * 1.5).Rk(r, 1, XlsBuilder.RkInt(r));
                b.EndSheet();
            }
            string path = Path.Combine(dir, $"built_{maxRecord}_v{version}.xls");
            File.WriteAllBytes(path, b.Build(version).ToArray());
            yield return path;
        }
    }

    // One line for the workbook, one per non-empty cell of each worksheet, as the sheet source produces them.
    private static async Task DumpAsync(string path, Stream into)
    {
        using var wb = ExcelWorkbook.Open(path);
        using (var w = new Utf8JsonWriter(into))
        {
            w.WriteStartObject();
            w.WriteString("file", path);
            w.WriteBoolean("date1904", wb.Date1904);
            w.WriteStartArray("sheets");
            foreach (var s in wb.Sheets)
            {
                w.WriteStartArray();
                w.WriteStringValue(s.Name);
                w.WriteStringValue(s.Kind.ToString());
                w.WriteStringValue(s.Visibility.ToString());
                w.WriteEndArray();
            }
            w.WriteEndArray();
            w.WriteEndObject();
        }
        into.WriteByte((byte)'\n');
        foreach (var sheet in wb.Sheets.Where(s => s.Kind == ExcelSheetKind.Worksheet))
        {
            using var source = await wb.Source.OpenSheetAsync(sheet.Index, false, default);
            var row = new RowBuffer();
            while (source.ReadRow(row))
            {
                foreach (ref readonly var c in row.Cells)
                {
                    using (var w = new Utf8JsonWriter(into))
                    {
                        w.WriteStartObject();
                        w.WriteNumber("sheet", sheet.Index);
                        w.WriteNumber("r", row.Row);
                        w.WriteNumber("c", c.Column);
                        switch (c.Type)
                        {
                            case CellType.Number:
                                w.WriteString("t", "n");
                                w.WriteNumber("v", c.Number);
                                break;
                            case CellType.String or CellType.SharedString:
                                w.WriteString("t", "s");
                                w.WriteString("v", row.GetText(c));
                                break;
                            case CellType.Boolean:
                                w.WriteString("t", "b");
                                w.WriteBoolean("v", c.Number != 0);
                                break;
                            default:
                                w.WriteString("t", "e");
                                w.WriteNull("v");
                                break;
                        }
                        w.WriteBoolean("date", c.Type == CellType.Number && c.Kind != ExcelValueKind.Number);
                        w.WriteEndObject();
                    }
                    into.WriteByte((byte)'\n');
                }
            }
        }
    }
}
