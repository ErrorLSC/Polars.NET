using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Excel;

namespace Seseragi.Files.Tests;

public class XlsbReadTests
{
    private static string Calamine(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "calamine", name + ".xlsb");

    private static (Schema Schema, List<string> Rows) Read(Stream stream, ExcelReadOptions? options = null)
    {
        using var reader = ExcelReader.Open(stream, options);
        return (reader.Schema, ArrowRender.Rows(reader.ReadBatches()));
    }

    private static (Schema Schema, List<string> Rows) Read(string name, ExcelReadOptions? options = null) =>
        Read(File.OpenRead(Calamine(name)), options);

    private static List<string> Types(Schema schema) => [.. schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.Name}")];

    private static int Days(int y, int m, int d) => new DateOnly(y, m, d).DayNumber - 719_162;

    // ------------------------------------------------------------ calamine's fixtures (written by Excel)

    [Fact]
    public void Sheets_with_visibility_and_chart_sheets()
    {
        using var wb = ExcelWorkbook.Open(Calamine("any_sheets"));
        Assert.Equal(ExcelFormat.Xlsb, wb.Format);
        Assert.Equal(
        [
            new ExcelSheetInfo("Visible", 0, ExcelSheetVisibility.Visible),
            new("Hidden", 1, ExcelSheetVisibility.Hidden),
            new("VeryHidden", 2, ExcelSheetVisibility.VeryHidden),
            new("Chart", 3, ExcelSheetVisibility.Visible, ExcelSheetKind.Chartsheet),
        ], wb.Sheets);
        Assert.Contains("chart sheet", Assert.Throws<ArgumentException>(() => wb.OpenSheet(new() { SheetName = "Chart" })).Message);
    }

    [Fact]
    public void Sheet_names_keep_workbook_order() =>
        Assert.Equal([.. Enumerable.Range(1, 8).Select(i => $"Sheet{i}")], ExcelWorkbook.Open(Calamine("sheet_names_order")).Sheets.Select(s => s.Name));

    [Theory]
    [InlineData("date", false)]
    [InlineData("date_1904", true)]
    [InlineData("date_rk_int", false)] // dates stored as RK integers: calamine reads them as integers
    public void Dates_in_both_systems(string name, bool date1904)
    {
        using var wb = ExcelWorkbook.Open(Calamine(name));
        Assert.Equal(date1904, wb.Date1904);
        using var reader = wb.OpenSheet(new() { HasHeader = false, Columns = ["column_1"], InferSchemaLength = 2 });
        Assert.Equal("date32", reader.Schema.FieldsList[0].DataType.Name);
        // The third value is a duration ([h]:mm:ss, 255:10:10): it does not fit the date column.
        var e = Assert.Throws<ExcelValueException>(() => reader.ReadBatches().ToList());
        Assert.Equal("A3", e.Cell);

        using var all = wb.OpenSheet(new() { HasHeader = false });
        Assert.Equal(["column_1:utf8", "column_2:int64"], Types(all.Schema));
        Assert.Equal(["\"2021-01-01\" | 15", "\"2021-01-02\" | 16", "\"255:10:10\" | 17"], ArrowRender.Rows(all.ReadBatches()));
    }

    [Fact]
    public void Floats_and_strings()
    {
        var (schema, rows) = Read("float_values", new() { HasHeader = false });
        Assert.Equal(["column_1:double"], Types(schema));
        Assert.Equal(["1.23", "12.34", "123.45", "1234.56", "12345.67"], rows);

        var (_, special) = Read("issues", new() { SheetName = "spc_chrs", HasHeader = false });
        Assert.Equal(["\"&\"", "\"<\"", "\">\"", "\"aaa ' aaa\"", "\"\"\"", "\"☺\"", "\"֍\"", "\"àâéêèçöïî«»\""], special);

        var (pairs, values) = Read("issues", new() { SheetName = "issue2", HasHeader = false });
        Assert.Equal(["column_1:int64", "column_2:utf8"], Types(pairs));
        Assert.Equal(["1 | \"a\"", "2 | \"b\"", "3 | \"c\""], values);
    }

    [Fact]
    public void Formulas_give_their_cached_values()
    {
        var (_, rows) = Read("formula_values", new() { HasHeader = false });
        Assert.Equal(["\"3\"", "\"Ab\"", "\"false\""], rows); // a number, a string and a boolean: a text column
        var (schema, refs) = Read("formula_references", new() { HasHeader = false, Columns = ["column_4"] });
        Assert.Equal(["column_4:int64"], Types(schema));
        Assert.Equal(["1", "1", "1", "1", "10", "10", "10", "1", "1", "1", "1", "10", "10", "10"], refs);
    }

    [Theory]
    [InlineData("case_insensitive_part_name")]
    [InlineData("unknown_record_before_names")]
    [InlineData("unmatched_workbook_record_does_not_lose_sheet")]
    [InlineData("unmatched_workbook_record_does_not_panic")]
    [InlineData("truncated_name_record")] // a broken defined name: names are not read
    public void Odd_workbook_parts_still_list_their_sheets(string name)
    {
        using var wb = ExcelWorkbook.Open(Calamine(name));
        Assert.Equal("Sheet1", Assert.Single(wb.Sheets).Name);
    }

    [Fact]
    public void Encrypted_workbooks_are_refused_by_name() =>
        Assert.Contains("encrypted", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Calamine("password_protected"))).Message);

    // ------------------------------------------------------------ built record by record

    [Fact]
    public void Every_cell_record_kind()
    {
        var b = new XlsbBuilder();
        int abc = b.SharedString("abc");
        int empty = b.SharedString("");
        b.Sheet("S")
            .Row(0).Text(0, "kind").Text(1, "value")
            .Row(1).Text(0, "st").Text(1, "inline ✓")
            .Row(2).Text(0, "isst").Shared(1, abc)
            .Row(3).Text(0, "rich").RichText(1, "rich text")
            .Row(4).Text(0, "fmla string").FormulaText(1, "a&b")
            .Row(5).Text(0, "bool").Bool(1, true)
            .Row(6).Text(0, "fmla bool").FormulaBool(1, false)
            .Row(7).Text(0, "error").Error(1)
            .Row(8).Text(0, "fmla error").FormulaError(1)
            .Row(9).Text(0, "real").Real(1, 2.5)
            .Row(10).Text(0, "fmla num").FormulaNumber(1, -1e300)
            .Row(11).Text(0, "rk int").Rk(1, XlsbBuilder.RkInt(-123))
            .Row(12).Text(0, "rk int x100").Rk(1, XlsbBuilder.RkInt(12345, x100: true))
            .Row(13).Text(0, "rk float").Rk(1, XlsbBuilder.RkFloat(0.75))
            .Row(14).Text(0, "rk float x100").Rk(1, XlsbBuilder.RkFloat(1.5, x100: true))
            .Row(15).Text(0, "blank").Blank(1)
            .Row(16).Text(0, "empty").Text(1, "").Shared(2, empty)
            .EndSheet();
        var (schema, rows) = Read(b.Build());
        Assert.Equal(["kind:utf8", "value:utf8"], Types(schema));
        Assert.Equal(
        [
            "\"st\" | \"inline ✓\"",
            "\"isst\" | \"abc\"",
            "\"rich\" | \"rich text\"",
            "\"fmla string\" | \"a&b\"",
            "\"bool\" | \"true\"",
            "\"fmla bool\" | \"false\"",
            "\"error\" | null",
            "\"fmla error\" | null",
            "\"real\" | \"2.5\"",
            "\"fmla num\" | \"-1E+300\"",
            "\"rk int\" | \"-123\"",
            "\"rk int x100\" | \"123.45\"",
            "\"rk float\" | \"0.75\"",
            "\"rk float x100\" | \"0.015\"",
            "\"blank\" | null",
            "\"empty\" | null",
        ], rows);
    }

    [Fact]
    public void Number_formats_and_dates_of_rk_integers()
    {
        var b = new XlsbBuilder().Date1904();
        int date = b.Style(14);                        // built-in m/d/yyyy
        int dateTime = b.Style(164, "yyyy-mm-dd hh:mm");
        int elapsed = b.Style(46);                     // [h]:mm:ss
        b.Sheet("S")
            .Row(0).Text(0, "d").Text(1, "dt").Text(2, "dur")
            .Row(1).Rk(0, XlsbBuilder.RkInt(43831), date).Real(1, 43831.5, dateTime).Real(2, 1.25, elapsed)
            .EndSheet();
        var (schema, rows) = Read(b.Build());
        Assert.Equal(["d:date32", "dt:timestamp", "dur:duration"], Types(schema));
        long noon = (Days(2024, 1, 2) * 86_400L + 43_200) * 1_000_000;
        Assert.Equal([$"{Days(2024, 1, 2)} | {noon} | {30L * 3_600_000_000}"], rows);
    }

    [Fact]
    public void Records_the_reader_does_not_use_are_skipped()
    {
        var b = new XlsbBuilder();
        b.Sheet("S")
            .Row(0).Text(0, "a")
            .Raw(1000, [9, 9])         // a two-byte record type
            .Raw(37, [1, 2, 3, 4]).Raw(38, [])  // an alternate-content block
            .Row(1).Real(0, 1).Raw(60, new byte[18])
            .EndSheet();
        Assert.Equal(["1"], Read(b.Build()).Rows);
    }

    [Theory]
    [InlineData(12)]
    [InlineData(18)]
    public void Unassigned_cell_record_types_are_refused(int type)
    {
        var b = new XlsbBuilder();
        b.Sheet("S").Row(0).Text(0, "a").Row(1).Raw(type, new byte[9]).EndSheet();
        var e = Assert.Throws<ExcelFormatException>(() => Read(b.Build()));
        Assert.Contains($"type {type}", e.Message);
    }

    [Fact]
    public void Record_sizes_may_use_all_four_bytes()
    {
        // A size of 16 written in four bytes, the fourth with its (ignored) high bit set.
        var b = new XlsbBuilder();
        b.Sheet("S").Row(0).Text(0, "a").Row(1).Bytes([5, 0x90, 0x80, 0x80, 0x80, .. new byte[8], .. XlsbBuilder.F64(2.5)]).EndSheet();
        Assert.Equal(["2.5"], Read(b.Build()).Rows);
    }

    [Fact]
    public void Sheets_without_values()
    {
        // Excel's empty sheets: an error by default; without RaiseIfEmpty, no columns and no batches.
        Assert.Throws<ExcelFormatException>(() => Read("sheet_names_order"));
        var (schema, rows) = Read("sheet_names_order", new() { RaiseIfEmpty = false });
        Assert.Empty(schema.FieldsList);
        Assert.Empty(rows);
    }

    [Fact]
    public void Tables_and_sheet_states()
    {
        var b = new XlsbBuilder();
        b.Sheet("Hidden", state: 1).Row(0).Text(0, "h").EndSheet();
        b.Sheet("Data")
            .Row(0).Text(0, "title")
            .Row(2).Text(1, "region").Text(2, "amount")
            .Row(3).Text(1, "east").Real(2, 10.5)
            .Row(4).Text(1, "west").Real(2, 20.25)
            .Row(5).Text(1, "total").Real(2, 30.75)
            .Table("Sales", 2, 1, 5, 2, totalsRows: 1)
            .EndSheet();
        using var wb = ExcelWorkbook.Open(b.Build());
        Assert.Equal(ExcelSheetVisibility.Hidden, wb.Sheets[0].Visibility);
        Assert.Equal([new ExcelTableInfo("Sales", "Data", "B3:C6", 1, 1)], wb.Tables);
        using var reader = wb.OpenSheet(new() { TableName = "sales" });
        Assert.Equal(["region:utf8", "amount:double"], Types(reader.Schema));
        Assert.Equal(["\"east\" | 10.5", "\"west\" | 20.25"], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public async Task Streams_and_async_reads_match()
    {
        var b = new XlsbBuilder();
        b.Sheet("S").Row(0).Text(0, "n");
        for (int i = 1; i <= 50_000; i++) b.Row(i).Rk(0, XlsbBuilder.RkInt(i));
        var file = b.EndSheet().Build();
        await using var reader = await ExcelReader.OpenAsync(new NonSeekable(file), new() { BatchSize = 7_000 });
        long sum = 0, rows = 0;
        await foreach (var batch in reader.ReadBatchesAsync())
        {
            var col = (Int64Array)batch.Column(0);
            for (int i = 0; i < col.Length; i++) sum += col.GetValue(i)!.Value;
            rows += batch.Length;
        }
        Assert.Equal(50_000, rows);
        Assert.Equal(50_000L * 50_001 / 2, sum);
    }

    [Fact]
    public void A_sheet_part_ending_without_end_records_still_reads()
    {
        var b = new XlsbBuilder();
        b.Sheet("S").Row(0).Text(0, "n").Row(1).Real(0, 1).EndSheet(close: false);
        Assert.Equal(["1"], Read(b.Build()).Rows);
    }

    [Theory]
    [InlineData("truncated")]
    [InlineData("sst index")]
    [InlineData("cell before row")]
    [InlineData("row order")]
    [InlineData("column")]
    [InlineData("short cell")]
    public void Malformed_sheets_raise_format_errors(string what)
    {
        var b = new XlsbBuilder();
        b.Sheet("S").Row(0).Text(0, "h");
        switch (what)
        {
            case "truncated": b.Row(1).Bytes(5, 16, 0, 0); break;                          // BrtCellReal of 16 bytes, 2 given
            case "sst index": b.Row(1).Shared(0, 5); break;
            case "cell before row": b.EndSheet(close: false); b.Sheet("T").Real(0, 1); break;
            case "row order": b.Row(5).Real(0, 1).Row(3).Real(0, 2); break;
            case "column": b.Row(1).Real(20_000, 1); break;
            case "short cell": b.Row(1).Raw(5, [0, 0, 0, 0, 0, 0, 0, 0, 1]); break;         // a real with 1 byte of value
        }
        var file = b.EndSheet().Build();
        Assert.Throws<ExcelFormatException>(() =>
        {
            using var wb = ExcelWorkbook.Open(file);
            using var reader = wb.OpenSheet(new() { SheetIndex = wb.Sheets.Count - 1, HasHeader = false });
            reader.ReadBatches().ToList();
        });
    }

    private sealed class NonSeekable(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
