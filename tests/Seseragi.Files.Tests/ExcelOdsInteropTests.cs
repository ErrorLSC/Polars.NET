using System.Diagnostics;
using Kaminari.Seseragi.Files.Excel;

namespace Seseragi.Files.Tests;

/// <summary>
/// The OpenDocument reader against LibreOffice: its .xlsx copies of OpenDocument spreadsheets (calamine's, the
/// LibreOffice copies among the fixtures, flat ones, and spreadsheets built from markup) must read as our reader
/// reads the originals. Opt-in: set SESERAGI_EXCEL_PYTHON (as the other interop tests) and have <c>soffice</c> on
/// the path.
/// </summary>
[Trait("Category", "Interop")]
[Collection("soffice")] // one LibreOffice at a time: parallel instances contend for its user profile
public class ExcelOdsInteropTests
{
    private static string Fixtures => Path.Combine(AppContext.BaseDirectory, "TestData", "excel");

    [Fact]
    public async Task Ods_reads_as_LibreOffice_xlsx_copies()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SESERAGI_EXCEL_PYTHON"))) return;
        if (!(Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator).Any(d => File.Exists(Path.Combine(d, "soffice")))) return;
        string dir = Directory.CreateTempSubdirectory("seseragi-ods-lo").FullName;
        try
        {
            List<string> sources =
            [
                .. Directory.GetFiles(Path.Combine(Fixtures, "calamine"), "*.ods")
                    .Where(f => Path.GetFileNameWithoutExtension(f) is not ("password_protected" or "dos_protection")),
                .. Directory.GetFiles(Fixtures, "*.ods"),
                .. Directory.GetFiles(Fixtures, "*.fods"),
                .. Built(dir),
            ];
            string outDir = Path.Combine(dir, "xlsx");
            var psi = new ProcessStartInfo("soffice", ["--headless", "--convert-to", "xlsx", "--outdir", outDir, .. sources]) { RedirectStandardOutput = true, RedirectStandardError = true };
            using (var process = Process.Start(psi)!)
            {
                await process.StandardOutput.ReadToEndAsync();
                await process.WaitForExitAsync();
            }
            List<string> failures = [];
            foreach (string source in sources)
            {
                using var ods = ExcelWorkbook.Open(source);
                // Copies of files with the same name (an .ods and its .fods) are compared once.
                using var xlsx = ExcelWorkbook.Open(Path.Combine(outDir, Path.GetFileNameWithoutExtension(source) + ".xlsx"));
                Assert.Equal(ods.Sheets.Select(s => (s.Name, s.Visibility)), xlsx.Sheets.Select(s => (s.Name, s.Visibility)));
                foreach (var sheet in ods.Sheets)
                {
                    var options = new ExcelReadOptions { SheetIndex = sheet.Index, HasHeader = false, RaiseIfEmpty = false, InferSchemaLength = null };
                    using var a = ods.OpenSheet(options);
                    using var b = xlsx.OpenSheet(options);
                    string what = $"{Path.GetFileName(source)} [{sheet.Name}]";
                    var typesA = a.Schema.FieldsList.Select(f => f.DataType.Name).ToList();
                    var typesB = b.Schema.FieldsList.Select(f => f.DataType.Name).ToList();
                    if (!typesA.SequenceEqual(typesB)) failures.Add($"{what}: types {string.Join(",", typesA)} vs LibreOffice {string.Join(",", typesB)}");
                    var rowsA = ArrowRender.Rows(a.ReadBatches());
                    var rowsB = ArrowRender.Rows(b.ReadBatches());
                    for (int i = 0; i < Math.Max(rowsA.Count, rowsB.Count); i++)
                    {
                        string? x = i < rowsA.Count ? rowsA[i] : null, y = i < rowsB.Count ? rowsB[i] : null;
                        if (!Same(x, y) && !KnownDifference(Path.GetFileNameWithoutExtension(source), sheet.Name, i))
                            failures.Add($"{what} row {i}: {x} vs LibreOffice {y}");
                    }
                }
            }
            Assert.True(failures.Count == 0, string.Join("\n", failures.Take(60)));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    // LibreOffice's own faults in its .xlsx copies, not ours.
    private static bool KnownDifference(string file, string sheet, int row) => (file, sheet, row) switch
    {
        // Serial 1 (1899-12-31 to LibreOffice, whose dates before 1900-03-01 are a day off Excel's) reads 1900-01-01.
        ("types_openpyxl", "Types", 2) => true,
        // A table's totals row: the formula's cached value (0) is not written to the .xlsx copy.
        ("workbook_xlsxwriter", "Tables", 4) => true,
        _ => false,
    };

    // Rows alike up to LibreOffice's .xlsx copies: booleans become 1 and 0 (it has no boolean type), and date-times and
    // times lose digits past 15 significant ones (serial numbers written to 15 digits): within 10 µs.
    private static bool Same(string? ours, string? theirs)
    {
        if (ours == theirs) return true;
        if (ours is null || theirs is null) return false;
        string[] a = ours.Split(" | "), b = theirs.Split(" | ");
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            string x = Booleans(a[i]), y = Booleans(b[i]);
            if (x == y) continue;
            if (long.TryParse(x, out long p) && long.TryParse(y, out long q) && Math.Abs(p - q) <= 10) continue;
            if (DateTime.TryParse(x.Trim('"'), System.Globalization.CultureInfo.InvariantCulture, out var d)
                && DateTime.TryParse(y.Trim('"'), System.Globalization.CultureInfo.InvariantCulture, out var e)
                && Math.Abs((d - e).Ticks) <= 100) continue;
            return false;
        }
        return true;

        static string Booleans(string v) => v switch { "false" or "\"false\"" => "0", "true" or "\"true\"" => "1", "\"0\"" => "0", "\"1\"" => "1", _ => v };
    }

    // Values of every kind, as a package and as a flat document.
    private static IEnumerable<string> Built(string dir)
    {
        const string automatic = """
            <number:time-style style:name="N1"><number:hours number:style="long"/><number:text>:</number:text><number:minutes number:style="long"/></number:time-style>
            <number:time-style style:name="N2" number:truncate-on-overflow="false"><number:hours/><number:text>:</number:text><number:minutes number:style="long"/></number:time-style>
            <style:style style:name="ce1" style:family="table-cell" style:data-style-name="N1"/>
            <style:style style:name="ce2" style:family="table-cell" style:data-style-name="N2"/>
            <number:date-style style:name="N3"><number:year number:style="long"/><number:text>-</number:text><number:month number:style="long"/><number:text>-</number:text><number:day number:style="long"/></number:date-style>
            <number:date-style style:name="N4"><number:year number:style="long"/><number:text>-</number:text><number:month number:style="long"/><number:text>-</number:text><number:day number:style="long"/><number:text> </number:text><number:hours number:style="long"/><number:text>:</number:text><number:minutes number:style="long"/></number:date-style>
            <style:style style:name="ce3" style:family="table-cell" style:data-style-name="N3"/>
            <style:style style:name="ce4" style:family="table-cell" style:data-style-name="N4"/>
            <style:style style:name="ta2" style:family="table"><style:table-properties table:display="false"/></style:style>
            """;
        string spreadsheet = $"""
            <table:table table:name="Values">
             <table:table-row>{OdsBuilder.Text("number")}{OdsBuilder.Text("date")}{OdsBuilder.Text("time")}{OdsBuilder.Text("elapsed")}{OdsBuilder.Text("flag")}{OdsBuilder.Text("text")}</table:table-row>
             <table:table-row table:number-rows-repeated="2"><table:table-cell office:value-type="float" office:value="1.5"/><table:table-cell table:style-name="ce4" office:value-type="date" office:date-value="2024-02-29T06:30:00"/><table:table-cell table:style-name="ce1" office:value-type="time" office:time-value="PT13H14M"/><table:table-cell table:style-name="ce2" office:value-type="time" office:time-value="PT30H"/><table:table-cell office:value-type="boolean" office:boolean-value="true"/><table:table-cell office:value-type="string"><text:p>a<text:s text:c="2"/>b</text:p><text:p>line two</text:p></table:table-cell></table:table-row>
             <table:table-row><table:table-cell office:value-type="percentage" office:value="0.25"/><table:table-cell table:style-name="ce4" office:value-type="date" office:date-value="1999-12-31"/><table:table-cell table:style-name="ce1" office:value-type="time" office:time-value="PT0H1M"/><table:table-cell table:style-name="ce2" office:value-type="time" office:time-value="PT1H"/><table:table-cell office:value-type="boolean" office:boolean-value="false"/><table:table-cell office:value-type="string"><text:p>x<text:tab/>y</text:p></table:table-cell></table:table-row>
            </table:table>
            <table:table table:name="Hidden" table:style-name="ta2"><table:table-row>{OdsBuilder.Float("3")}</table:table-row></table:table>
            """;
        foreach (bool flat in (bool[])[false, true])
        {
            string path = Path.Combine(dir, flat ? "built_flat.fods" : "built.ods");
            File.WriteAllBytes(path, OdsBuilder.Build(flat, spreadsheet, automatic).ToArray());
            yield return path;
        }
    }
}
