using System.Buffers.Binary;
using Apache.Arrow;
using Kaminari.Seseragi.Files.Excel;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Seseragi.Files.Tests;

public class XlsReadTests
{
    private static string Calamine(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "calamine", name + ".xls");

    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", name + ".xls");

    private static (Schema Schema, List<string> Rows) Read(Stream stream, ExcelReadOptions? options = null)
    {
        using var reader = ExcelReader.Open(stream, options);
        return (reader.Schema, ArrowRender.Rows(reader.ReadBatches()));
    }

    private static (Schema Schema, List<string> Rows) Read(string path, ExcelReadOptions? options = null) => Read(File.OpenRead(path), options);

    private static List<string> Types(Schema schema) => [.. schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.Name}")];

    private static ExcelFormatException Refused(Stream stream) => Assert.Throws<ExcelFormatException>(() =>
    {
        using var reader = ExcelReader.Open(stream, new() { HasHeader = false, RaiseIfEmpty = false });
        reader.ReadBatches().ToList();
    });

    // ------------------------------------------------------------ calamine's fixtures (most written by Excel)

    [Fact]
    public void Sheets_with_visibility_and_chart_sheets()
    {
        using var wb = ExcelWorkbook.Open(Calamine("any_sheets"));
        Assert.Equal(ExcelFormat.Xls, wb.Format);
        Assert.Empty(wb.Tables);
        Assert.Equal(
        [
            new ExcelSheetInfo("Visible", 0, ExcelSheetVisibility.Visible),
            new("Hidden", 1, ExcelSheetVisibility.Hidden),
            new("VeryHidden", 2, ExcelSheetVisibility.VeryHidden),
            new("Chart", 3, ExcelSheetVisibility.Visible, ExcelSheetKind.Chartsheet),
        ], wb.Sheets);
        Assert.Contains("chart sheet", Assert.Throws<ArgumentException>(() => wb.OpenSheet(new() { SheetName = "Chart" })).Message);
        using var visible = wb.OpenSheet(new() { HasHeader = false });
        Assert.Equal(["\"1\" | 2", "\"3\" | 4", "\"5\" | 6", "\"This workbook contains 4 sheets: Visible, Hidden, VeryHidden and Chart\" | null"],
            ArrowRender.Rows(visible.ReadBatches()));
    }

    [Fact]
    public void Sheet_names_keep_workbook_order() =>
        Assert.Equal([.. Enumerable.Range(1, 8).Select(i => $"Sheet{i}")], ExcelWorkbook.Open(Calamine("sheet_names_order")).Sheets.Select(s => s.Name));

    [Theory]
    [InlineData("date", false)]
    [InlineData("date_1904", true)]
    public void Dates_in_both_systems(string name, bool date1904)
    {
        using var wb = ExcelWorkbook.Open(Calamine(name));
        Assert.Equal(date1904, wb.Date1904);
        using var reader = wb.OpenSheet(new() { HasHeader = false, Columns = ["column_1"], InferSchemaLength = 2 });
        Assert.Equal("date32", reader.Schema.FieldsList[0].DataType.Name);
        // The third value is a duration ([h]:mm:ss, 255:10:10): it does not fit the date column.
        Assert.Equal("A3", Assert.Throws<ExcelValueException>(() => reader.ReadBatches().ToList()).Cell);

        using var all = wb.OpenSheet(new() { HasHeader = false });
        Assert.Equal(["column_1:utf8", "column_2:int64"], Types(all.Schema));
        Assert.Equal(["\"2021-01-01\" | 15", "\"2021-01-02\" | 16", "\"255:10:10\" | 17"], ArrowRender.Rows(all.ReadBatches()));
    }

    [Fact]
    public async Task East_asian_built_in_formats_are_dates()
    {
        // Rows 1–19 use the built-in ids 27–36 and 50–58, which the file does not define (their codes depend on
        // the locale): dates with times. Later rows hold other formats.
        using var wb = ExcelWorkbook.Open(Calamine("date_east_asian"));
        using var source = await wb.Source.OpenSheetAsync(0, false, default);
        var row = new RowBuffer();
        List<ExcelValueKind> kinds = [];
        while (source.ReadRow(row) && row.Row < 19) kinds.AddRange(row.Cells.ToArray().Select(c => c.Kind));
        Assert.Equal(Enumerable.Repeat(ExcelValueKind.DateTime, 57), kinds);
    }

    [Fact]
    public void Shared_strings_across_continue_records()
    {
        // The last string of the table starts in a Continue record of its own.
        var (_, rows) = Read(Calamine("sst_continue"), new() { HasHeader = false });
        Assert.Equal("\"New CONTINUE block\"", rows[135]);
        Assert.Equal("\"This is a long string with a serial number at the end 0001\"", rows[0]);

        // A wrong unique-string count in the SST record: the strings are read until the records end.
        var (_, system) = Read(Calamine("incorrect_sst_unique_count"), new() { SheetName = "System Level Data", HasHeader = false });
        Assert.StartsWith("\"Revised:\" | \"11th May 2023\"", system[6]);

        // Phonetic data running on into a Continue record.
        using var wb = ExcelWorkbook.Open(Calamine("continued_extended_string"));
        using var reader = wb.OpenSheet(new() { HasHeader = false, RaiseIfEmpty = false });
        Assert.NotEmpty(reader.ReadBatches().ToList());
    }

    [Fact]
    public void Formulas_give_their_cached_values()
    {
        var (_, utf16) = Read(Calamine("formula-utf16-string"), new() { HasHeader = false });
        Assert.Equal(["\"店舗\" | 100 | 100"], utf16);
        var (_, values) = Read(Calamine("xls_formula"), new() { HasHeader = false });
        Assert.Equal(["10", "20", "110", "65"], values);
        // A formula giving "" is an empty cell (calamine keeps the empty string).
        var (_, empty) = Read(Calamine("empty-string"), new() { HasHeader = false });
        Assert.Equal(["false"], empty);
    }

    [Fact]
    public void Embedded_charts_and_pictures_are_skipped()
    {
        // The chart on the first sheet holds a data cache of Number records in a nested substream.
        var (_, report) = Read(Calamine("xls_cross_sheet_chart"), new() { HasHeader = false });
        Assert.Equal(["\"HEADER TEXT A1\" | null | null", "\"LABEL A6\" | null | \"VALUE C6\"", "\"LABEL A8\" | null | \"VALUE C8\""], report);
        using var picture = ExcelWorkbook.Open(Calamine("picture"));
        using var reader = picture.OpenSheet(new() { RaiseIfEmpty = false });
        Assert.Empty(ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void Odd_records_still_read()
    {
        // A garbled Format record (its string count does not fit), a 16-byte Dimensions record.
        var (_, rows) = Read(Calamine("dimensions_16_bytes"), new() { HasHeader = false, InferSchemaLength = 0 });
        Assert.Contains("TOTAL", rows[^1]);
        Assert.Contains(rows, r => r.Contains("\"167943\"", StringComparison.Ordinal));
        // The workbook stream named "BOOK" (the BIFF5 name) though it holds BIFF8.
        Assert.Equal("Sheet1", Assert.Single(ExcelWorkbook.Open(Calamine("capitalized_wbook_stream")).Sheets).Name);
        // A compound file cut short after the sectors the workbook needs.
        Assert.Equal("Sheet1", Assert.Single(ExcelWorkbook.Open(Calamine("cfb_dir_short_chunk_panic")).Sheets).Name);
    }

    [Theory]
    [InlineData("biff5_write")]
    [InlineData("misc_biff5_parsing")]
    [InlineData("malformed_format")]
    [InlineData("OOM_alloc2")]
    public void Older_biff_versions_are_refused_by_name(string name) =>
        Assert.Contains("BIFF5", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Calamine(name))).Message);

    [Fact]
    public void Encrypted_workbooks_are_refused_by_name()
    {
        Assert.Contains("encrypted", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Calamine("password_protected"))).Message);
        // An encrypted xlsx or xlsb is an OLE2 file too.
        string xlsb = Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "calamine", "password_protected.xlsb");
        Assert.Contains("encrypted", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(xlsb)).Message);
    }

    [Theory]
    [InlineData("cfb_difat_oom")]
    [InlineData("cfb_huge_sector_counts")]
    [InlineData("OOM_alloc3")]
    [InlineData("too_small")]
    public void Malformed_compound_files_fail_cleanly(string name) =>
        Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Calamine(name)));

    // ------------------------------------------------------------ LibreOffice's copies of our fixtures

    [Fact]
    public void LibreOffice_copies_of_the_fixtures()
    {
        var (types, rows) = Read(Fixture("types_openpyxl"));
        var (xTypes, xRows) = Read(File.OpenRead(Path.ChangeExtension(Fixture("types_openpyxl"), ".xlsx")));
        Assert.Equal(xTypes.FieldsList.Select(f => f.Name), types.FieldsList.Select(f => f.Name));
        Assert.Equal(xRows.Count, rows.Count);

        using var wb = ExcelWorkbook.Open(Fixture("date1904"));
        Assert.True(wb.Date1904);
        Assert.Equal(ExcelFormat.Xls, wb.Format);
    }

    // ------------------------------------------------------------ built record by record

    [Fact]
    public void Every_cell_record_kind()
    {
        var b = new XlsBuilder();
        int abc = b.SharedString("abc");
        int empty = b.SharedString("");
        int date = b.Style(14);
        b.Sheet("S")
            .Label(0, 0, "kind").Label(0, 1, "value")
            .Label(1, 0, "label").Label(1, 1, "inline ✓")
            .Label(2, 0, "sst").LabelSst(2, 1, abc)
            .Label(3, 0, "sst empty").LabelSst(3, 1, empty)
            .Label(4, 0, "number").Number(4, 1, 2.5)
            .Label(5, 0, "rk int").Rk(5, 1, XlsBuilder.RkInt(-123))
            .Label(6, 0, "rk int x100").Rk(6, 1, XlsBuilder.RkInt(12345, x100: true))
            .Label(7, 0, "rk float").Rk(7, 1, XlsBuilder.RkFloat(0.75))
            .Label(8, 0, "rk float x100").Rk(8, 1, XlsBuilder.RkFloat(1.5, x100: true))
            .Label(9, 0, "bool").Bool(9, 1, true)
            .Label(10, 0, "error").Error(10, 1)
            .Label(11, 0, "blank").Blank(11, 1)
            .Label(12, 0, "f number").FormulaNumber(12, 1, 7)
            .Label(13, 0, "f string").FormulaString(13, 1, "a&b")
            .Label(14, 0, "f bool").FormulaBool(14, 1, false)
            .Label(15, 0, "f error").FormulaError(15, 1)
            .Label(16, 0, "f empty").FormulaEmpty(16, 1)
            .Label(17, 0, "f shared").FormulaString(17, 1, "after ShrFmla", sharedFormula: true)
            .Label(18, 0, "date").Number(18, 1, 45306, date)
            .EndSheet();
        var (schema, rows) = Read(b.Build(), new() { InferSchemaLength = 0 });
        Assert.Equal(["kind:utf8", "value:utf8"], Types(schema));
        Assert.Equal(
        [
            "\"label\" | \"inline ✓\"", "\"sst\" | \"abc\"", "\"sst empty\" | null", "\"number\" | \"2.5\"",
            "\"rk int\" | \"-123\"", "\"rk int x100\" | \"123.45\"", "\"rk float\" | \"0.75\"", "\"rk float x100\" | \"0.015\"",
            "\"bool\" | \"true\"", "\"error\" | null", "\"blank\" | null", "\"f number\" | \"7\"", "\"f string\" | \"a&b\"",
            "\"f bool\" | \"false\"", "\"f error\" | null", "\"f empty\" | null", "\"f shared\" | \"after ShrFmla\"", "\"date\" | \"2024-01-15\"",
        ], rows);
    }

    [Fact]
    public void MulRk_and_MulBlank_spread_over_columns()
    {
        var b = new XlsBuilder();
        int pct = b.Style(10);
        b.Sheet("S")
            .MulRk(0, 1, (XlsBuilder.RkInt(1), 0), (XlsBuilder.RkFloat(2.5), 0), (XlsBuilder.RkInt(50, x100: true), pct))
            .MulBlank(1, 0, 3).Rk(1, 3, XlsBuilder.RkInt(9))
            .EndSheet();
        var (_, rows) = Read(b.Build(), new() { HasHeader = false });
        Assert.Equal(["1 | 2.5 | 0.5", "null | null | 9"], rows);
    }

    [Theory]
    [InlineData(20)]   // breaks inside the strings' characters, runs and phonetic data
    [InlineData(37)]
    [InlineData(8224)] // Excel's size: no break
    public void Strings_split_across_continue_records(int maxRecord)
    {
        string[] texts = ["short", "a longer string that spans records", "宽字符と Latin mixed ✓ text", "ÿ Latin-1 only ÿ", "", "😀 surrogate pair 😀 again 😀"];
        var b = new XlsBuilder { MaxRecord = maxRecord };
        var ids = texts.Select((t, i) => b.SharedString(t, runs: i % 2 == 0 ? 2 : 0, ext: i % 3 == 0 ? 11 : 0)).ToArray();
        b.Sheet("S");
        for (int i = 0; i < texts.Length; i++) b.LabelSst(i, 0, ids[i]).FormulaString(i, 1, texts[i]);
        b.EndSheet();
        var (_, rows) = Read(b.Build(), new() { HasHeader = false, DropEmptyRows = false });
        Assert.Equal([.. texts.Select(t => t.Length == 0 ? "null | null" : $"\"{t}\" | \"{t}\"")], rows);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    public void Compound_file_versions_and_the_mini_stream(int version)
    {
        // A small workbook lives in the mini stream; a large one in regular sectors.
        int[] sizes = [3, 2000];
        foreach (int rows in sizes)
        {
            var b = new XlsBuilder();
            b.Sheet("S").Label(0, 0, "n");
            for (int r = 1; r <= rows; r++) b.Number(r, 0, r);
            b.EndSheet();
            var bytes = b.Build(version).ToArray();
            Assert.Equal(rows < 100, b.WorkbookStream().Length < 4096);
            var (_, read) = Read(new MemoryStream(bytes));
            Assert.Equal(rows, read.Count);
            Assert.Equal($"{rows}", read[^1]);
        }
    }

    [Fact]
    public void Large_files_list_their_fat_through_difat_sectors()
    {
        // More than 109 FAT sectors (over 6.9 MB with 512-byte sectors) need DIFAT sectors.
        var b = new XlsBuilder();
        b.Sheet("S").Label(0, 0, "n").Number(1, 0, 42).EndSheet();
        var bytes = CompoundFileBuilder.Pack([("Workbook", b.WorkbookStream()), ("Padding", new byte[7_500_000])]);
        var (_, rows) = Read(new MemoryStream(bytes));
        Assert.Equal(["42"], rows);
    }

    [Fact]
    public async Task Non_seekable_streams_and_async_reads()
    {
        var b = new XlsBuilder();
        int s = b.SharedString("x");
        b.Sheet("S").Label(0, 0, "a").LabelSst(1, 0, s).EndSheet();
        var bytes = b.Build().ToArray();
        var (_, rows) = Read(new ExcelReadTests.ForwardOnlyStream(new MemoryStream(bytes)));
        Assert.Equal(["\"x\""], rows);
        await using var wb = await ExcelWorkbook.OpenAsync(new ExcelReadTests.ForwardOnlyStream(new MemoryStream(bytes)));
        using var reader = await wb.OpenSheetAsync();
        Assert.Equal(["\"x\""], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public async Task Async_reads_from_files_and_seekable_streams()
    {
        // Files go through the handle (RandomAccess), seekable streams through a gated seek and read.
        var b = new XlsBuilder();
        int s = b.SharedString("x");
        b.Sheet("S").Label(0, 0, "a");
        for (int r = 1; r <= 2000; r++) b.LabelSst(r, 0, s);
        b.EndSheet();
        var bytes = b.Build().ToArray();
        string path = Path.Combine(Directory.CreateTempSubdirectory("seseragi-xls-async").FullName, "a.xls");
        await File.WriteAllBytesAsync(path, bytes);
        try
        {
            await using (var fromPath = await ExcelReader.OpenAsync(path))
            {
                List<RecordBatch> batches = [];
                await foreach (var batch in fromPath.ReadBatchesAsync()) batches.Add(batch);
                Assert.Equal(2000, batches.Sum(x => x.Length));
            }
            await using var fromStream = await ExcelReader.OpenAsync(new MemoryStream(bytes));
            int count = 0;
            await foreach (var batch in fromStream.ReadBatchesAsync()) count += batch.Length;
            Assert.Equal(2000, count);
        }
        finally
        {
            Directory.Delete(Path.GetDirectoryName(path)!, true);
        }
    }

    [Fact]
    public void Dialog_macro_and_chart_sheets_are_not_worksheets()
    {
        var b = new XlsBuilder();
        b.Sheet("Data").Label(0, 0, "a").Number(1, 0, 1).EndSheet();
        b.Sheet("Dialog", dialog: true).EndSheet();
        b.Sheet("Macro", type: 1).EndSheet();
        b.Sheet("Chart", state: 1, type: 2).EndSheet();
        using var wb = ExcelWorkbook.Open(b.Build());
        Assert.Equal(
        [
            new ExcelSheetInfo("Data", 0, ExcelSheetVisibility.Visible),
            new("Dialog", 1, ExcelSheetVisibility.Visible, ExcelSheetKind.Other),
            new("Macro", 2, ExcelSheetVisibility.Visible, ExcelSheetKind.Other),
            new("Chart", 3, ExcelSheetVisibility.Hidden, ExcelSheetKind.Chartsheet),
        ], wb.Sheets);
        Assert.Contains("dialog", Assert.Throws<ArgumentException>(() => wb.OpenSheet(new() { SheetIndex = 1 })).Message);
    }

    [Fact]
    public void Embedded_chart_substreams_inside_a_sheet_are_skipped()
    {
        var b = new XlsBuilder();
        b.Sheet("S").Label(0, 0, "a").Number(1, 0, 1).EmbeddedChart().Number(2, 0, 2).EndSheet();
        Assert.Equal(["1", "2"], Read(b.Build()).Rows);
    }

    [Fact]
    public void Date1904_and_custom_formats()
    {
        var b = new XlsBuilder().Date1904();
        int custom = b.Style(170, "yyyy\\-mm\\-dd hh:mm");
        int elapsed = b.Style(46);
        b.Sheet("S").Label(0, 0, "at").Label(0, 1, "took").Number(1, 0, 0.5, custom).Number(1, 1, 1.25, elapsed).EndSheet();
        var (schema, rows) = Read(b.Build());
        Assert.Equal(["at:timestamp", "took:duration"], Types(schema));
        Assert.Equal(["-2082801600000000 | 108000000000"], rows); // 1904-01-01 12:00 in µs since 1970; 30 h in µs
    }

    // ------------------------------------------------------------ refused

    [Fact]
    public void Cells_out_of_order_are_refused()
    {
        var rows = new XlsBuilder();
        rows.Sheet("S").Number(2, 0, 1).Number(1, 0, 2).EndSheet();
        Assert.Contains("A2 after A3", Refused(rows.Build()).Message);
        var columns = new XlsBuilder();
        columns.Sheet("S").Number(0, 2, 1).Number(0, 1, 2).EndSheet();
        Assert.Contains("B1 after C1", Refused(columns.Build()).Message);
        var twice = new XlsBuilder();
        twice.Sheet("S").Number(0, 1, 1).Number(0, 1, 2).EndSheet();
        Assert.Contains("out of order", Refused(twice.Build()).Message);
    }

    [Fact]
    public void Undescribed_and_broken_records_are_refused()
    {
        var rstring = new XlsBuilder();
        rstring.Sheet("S").Raw(0x00D6, [0, 0, 0, 0, 0, 0, 1, 0, 0, (byte)'x', 0, 0]).EndSheet();
        Assert.Contains("RString", Refused(rstring.Build()).Message);

        var noString = new XlsBuilder();
        noString.Sheet("S").FormulaString(0, 0, "lost", omitString: true).Number(1, 0, 1).EndSheet();
        Assert.Contains("string result", Refused(noString.Build()).Message);

        var mulRk = new XlsBuilder();
        mulRk.Sheet("S").Raw(0x00BD, [0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 5, 0]).EndSheet(); // colLast 5 for one cell
        Assert.Contains("column range", Refused(mulRk.Build()).Message);

        // A sheet substream that runs past the end of the stream (no EOF).
        var truncated = new XlsBuilder();
        truncated.Sheet("S").Number(0, 0, 1).EndSheet();
        var stream = truncated.WorkbookStream();
        Assert.Contains("EOF", Refused(new MemoryStream(CompoundFileBuilder.Pack([("Workbook", stream[..^4])]))).Message);
    }

    [Fact]
    public void Encrypted_and_older_workbooks_are_refused()
    {
        var b = new XlsBuilder().Encrypted();
        b.Sheet("S").Number(0, 0, 1).EndSheet();
        Assert.Contains("encrypted", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(b.Build())).Message);

        var old = new XlsBuilder();
        old.Sheet("S").Number(0, 0, 1).EndSheet();
        var biff5 = CompoundFileBuilder.Pack([("Book", old.WorkbookStream(biffVersion: 0x0500))]);
        Assert.Contains("BIFF5", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream(biff5))).Message);

        var other = CompoundFileBuilder.Pack([("WordDocument", new byte[100])]);
        Assert.Contains("no Excel workbook", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream(other))).Message);
    }

    [Fact]
    public void Broken_compound_files_fail_cleanly()
    {
        var b = new XlsBuilder();
        b.Sheet("S").Label(0, 0, "n");
        for (int r = 1; r < 1000; r++) b.Number(r, 0, r);
        b.EndSheet();
        var stream = b.WorkbookStream();
        Assert.True(stream.Length >= 4096);
        // A cycle in the workbook stream's chain, a link past the table, a cut-off file.
        var cycle = CompoundFileBuilder.Pack([("Workbook", stream)], patchFat: fat => fat[3] = 1);
        Assert.Contains("chain", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream(cycle))).Message);
        var past = CompoundFileBuilder.Pack([("Workbook", stream)], patchFat: fat => fat[3] = 0x00FFFFFF);
        Assert.Contains("chain", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream(past))).Message);
        var whole = CompoundFileBuilder.Pack([("Workbook", stream)], noMiniStream: true);
        var cut = whole[..(whole.Length / 2)];
        Refused(new MemoryStream(cut));

        // The directory chained through every free FAT entry past the end of the file: refused before its buffer is
        // allocated (the FAT describes more sectors than the file holds).
        uint dirStart = BinaryPrimitives.ReadUInt32LittleEndian(whole.AsSpan(48));
        var longDirectory = CompoundFileBuilder.Pack([("Workbook", stream)], noMiniStream: true, patchFat: fat =>
        {
            uint at = dirStart;
            for (uint i = 0; i < fat.Length; i++)
            {
                if (fat[i] != 0xFFFFFFFF) continue;
                fat[at] = i;
                at = i;
            }
            fat[at] = 0xFFFFFFFE;
        });
        Assert.Contains("larger than the file", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream(longDirectory))).Message);

        // A directory entry that is its own sibling: the tree walk visits each entry once.
        var loop = (byte[])whole.Clone();
        BinaryPrimitives.WriteUInt32LittleEndian(loop.AsSpan((int)(dirStart + 1) * 512 + 128 + 72), 1);
        Assert.Equal("S", Assert.Single(ExcelWorkbook.Open(new MemoryStream(loop)).Sheets).Name);
    }
}
