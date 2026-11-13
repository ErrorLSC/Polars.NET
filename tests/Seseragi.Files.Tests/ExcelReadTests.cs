using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Excel;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests;

public class ExcelReadTests
{
    internal static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", name + ".xlsx");

    private static (Schema Schema, List<string> Rows) Read(string name, ExcelReadOptions? options = null)
    {
        using var reader = ExcelReader.Open(Fixture(name), options);
        return (reader.Schema, ArrowRender.Rows(reader.ReadBatches()));
    }

    private static List<string> Types(Schema schema) => [.. schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.Name}")];

    private static long Micros(DateTime t) => (t.Ticks - DateTime.UnixEpoch.Ticks) / 10;

    private static int Days(int y, int m, int d) => new DateOnly(y, m, d).DayNumber - 719_162;

    // A workbook written by our writer from columns of strings and numbers (for error paths).
    private static MemoryStream Workbook(params (string Name, IArrowArray Array)[] columns)
    {
        var batch = Batch([.. columns.Select(c => (c.Name, c.Array, true))]);
        var ms = new MemoryStream();
        ExcelWriter.Write(ms, batch.Schema, [batch], new() { LeaveOpen = true });
        ms.Position = 0;
        return ms;
    }

    [Fact]
    public void Openpyxl_types_are_inferred()
    {
        var (schema, rows) = Read("types_openpyxl");
        Assert.Equal(["int:int64", "float:double", "mixed_num:double", "bool:bool", "text:utf8", "date:date32", "datetime:timestamp",
            "time:time64", "duration:duration", "mixed:utf8", "gaps:int64"], Types(schema));
        Assert.Equal(TimeUnit.Microsecond, ((TimestampType)schema.GetFieldByName("datetime").DataType).Unit);
        Assert.Null(((TimestampType)schema.GetFieldByName("datetime").DataType).Timezone);
        Assert.Equal(
        [
            // openpyxl stores 07:08:09.123456 as 45418.29732781777, which is .123455 to the microsecond.
            $"1 | 1.5 | 1 | true | \"a\" | {Days(2024, 1, 2)} | {Micros(new DateTime(2024, 5, 6, 7, 8, 9).AddTicks(1_234_550))} | 3723000000 | 108900000000 | \"x\" | 10",
            $"-2 | -0.25 | 2.5 | false | \"b\" | {Days(1900, 1, 1)} | {Micros(new DateTime(1999, 12, 31, 23, 59, 59))} | 86399500000 | 1000000 | \"5\" | null",
            // 9007199254740993 is not a double: Excel (and openpyxl) store 2^53. The error row (#N/A only) is dropped.
            $"9007199254740992 | 1E+300 | 3 | true | null | {Days(9999, 12, 31)} | {Micros(new DateTime(1900, 3, 1, 12, 0, 0))} | 0 | 0 | \"true\" | 30",
        ], rows);
    }

    [Fact]
    public void Shared_strings_and_special_text()
    {
        var (schema, rows) = Read("workbook_xlsxwriter");
        Assert.Equal(["id:int64", "name:utf8", "note:utf8"], Types(schema));
        Assert.Equal(
        [
            "1 | \"  padded  \" | \"tab\there\"",
            "2 | \"emoji \U0001F600\" | \"ctrl\u0001char\"",
            "3 | \"_x0041_ literal\" | \"line\nbreak\"",
            "4 | \"rich bold text\" | \"rich bold text!\"",
        ], rows);
    }

    [Fact]
    public void Workbook_lists_sheets_tables_and_date_system()
    {
        using var wb = ExcelWorkbook.Open(Fixture("workbook_xlsxwriter"));
        Assert.Equal(ExcelFormat.Xlsx, wb.Format);
        Assert.False(wb.Date1904);
        Assert.Equal(
            [new ExcelSheetInfo("Data", 0, ExcelSheetVisibility.Visible), new("Hidden", 1, ExcelSheetVisibility.Hidden), new("Tables", 2, ExcelSheetVisibility.Visible)],
            wb.Sheets);
        Assert.Equal([new ExcelTableInfo("Sales", "Tables", "B3:D6", 1, 1)], wb.Tables);

        using var hidden = wb.OpenSheet(new() { SheetName = "hidden" });
        Assert.Equal("Hidden", hidden.Sheet.Name);
        Assert.Equal(["1"], ArrowRender.Rows(hidden.ReadBatches()));
        using var byIndex = wb.OpenSheet(new() { SheetIndex = 1 });
        Assert.Equal("h", byIndex.Schema.FieldsList[0].Name);
    }

    [Fact]
    public void Chart_sheets_keep_their_tab_position_and_are_refused()
    {
        using var wb = ExcelWorkbook.Open(Fixture("chartsheet"));
        Assert.Equal(
            [new ExcelSheetInfo("Chart", 0, ExcelSheetVisibility.Visible, ExcelSheetKind.Chartsheet), new("Data", 1, ExcelSheetVisibility.Visible)],
            wb.Sheets);
        Assert.Contains("chart sheet", Assert.Throws<ArgumentException>(() => wb.OpenSheet()).Message);
        Assert.Contains("chart sheet", Assert.Throws<ArgumentException>(() => wb.OpenSheet(new() { SheetName = "chart" })).Message);
        using var data = wb.OpenSheet(new() { SheetIndex = 1 });
        Assert.Equal(["1 | 10", "2 | 20"], ArrowRender.Rows(data.ReadBatches()));
    }

    [Fact]
    public void Table_reads_its_range_without_the_totals_row()
    {
        var (schema, rows) = Read("workbook_xlsxwriter", new() { TableName = "sales" });
        Assert.Equal(["region:utf8", "amount:double", "units:int64"], Types(schema));
        Assert.Equal(["\"east\" | 10.5 | 1", "\"west\" | 20.25 | 2"], rows);
    }

    [Fact]
    public void Date1904_workbooks()
    {
        using var wb = ExcelWorkbook.Open(Fixture("date1904"));
        Assert.True(wb.Date1904);
        using var reader = wb.OpenSheet();
        Assert.Equal([$"{Days(2024, 1, 2)}", $"{Days(1904, 1, 1)}"], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void Header_names_follow_python_polars()
    {
        var (schema, rows) = Read("headers");
        // Blank → __UNNAMED__i; repeats → name_k; the unnamed empty column (F) is dropped.
        Assert.Equal(["a:double", "__UNNAMED__1:int64", "a_1:utf8", "b:utf8", "a_2:utf8", "a_1_1:int64"], Types(schema));
        Assert.Equal(["1 | 2 | \"3\" | null | \"5\" | 7", "1.5 | null | \"x\" | null | \"true\" | 8"], rows);

        var (kept, _) = Read("headers", new() { DropEmptyColumns = false });
        Assert.Equal(["a", "__UNNAMED__1", "a_1", "b", "a_2", "__UNNAMED__5", "a_1_1"], kept.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void Data_starts_at_its_first_cell_and_empty_rows_are_dropped_or_kept()
    {
        var (schema, rows) = Read("sparse");
        Assert.Equal(["k:utf8", "v:int64"], Types(schema));
        Assert.Equal(["\"a\" | 1", "\"b\" | 2"], rows);

        // Kept empty rows lie between rows with values; the styled empty row 9 is not data.
        var (_, kept) = Read("sparse", new() { DropEmptyRows = false });
        Assert.Equal(["\"a\" | 1", "null | null", "null | null", "\"b\" | 2"], kept);

        var (noHeader, raw) = Read("sparse", new() { HasHeader = false });
        Assert.Equal(["column_1:utf8", "column_2:utf8"], Types(noHeader));
        Assert.Equal(["\"k\" | \"v\"", "\"a\" | \"1\"", "\"b\" | \"2\""], raw);
    }

    [Fact]
    public void Edge_cases_of_the_xml()
    {
        using var wb = ExcelWorkbook.Open(Fixture("edge"));
        Assert.Equal("Edge & Co", Assert.Single(wb.Sheets).Name);
        using var reader = wb.OpenSheet();
        Assert.Equal(["kind:utf8", "value:utf8"], Types(reader.Schema));
        Assert.Equal(
        [
            "\"inline\" | \"rich\"",                    // runs concatenated, phonetic text left out
            "\"formula string\" | \"a&b <tag>\"",       // t="str": the cached result
            "\"iso date\" | \"2024-02-29 13:45:00\"",   // t="d"
            "\"spill\" | null",                         // any error value is null
            "\"cdata <raw> & text\" | \"esc\ttab\"",    // CDATA; _x0009_
            "\"empty v\" | null",
        ], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void Iso_date_cells_infer_their_kind()
    {
        using var wb = ExcelWorkbook.Open(Fixture("edge"));
        using var reader = wb.OpenSheet(new() { SchemaOverrides = new Dictionary<string, IArrowType> { ["value"] = new TimestampType(TimeUnit.Microsecond, (string?)null) }, Columns = ["value"], InferSchemaLength = 0 });
        var e = Assert.Throws<ExcelValueException>(() => reader.ReadBatches().ToList());
        Assert.Equal("B2", e.Cell); // "rich" is text, not a datetime
    }

    [Fact]
    public void Projection_by_name_and_index()
    {
        var (byName, rows) = Read("types_openpyxl", new() { Columns = ["text", "int"] });
        Assert.Equal(["int:int64", "text:utf8"], Types(byName)); // sheet order
        Assert.Equal(["1 | \"a\"", "-2 | \"b\"", "9007199254740992 | null"], rows);

        var (byIndex, _) = Read("types_openpyxl", new() { ColumnIndices = [10, 0] });
        Assert.Equal(["int", "gaps"], byIndex.FieldsList.Select(f => f.Name));

        Assert.Throws<ArgumentException>(() => Read("types_openpyxl", new() { Columns = ["nope"] }));
        Assert.Throws<ArgumentException>(() => Read("types_openpyxl", new() { ColumnIndices = [11] }));
        Assert.Throws<ArgumentException>(() => Read("types_openpyxl", new() { Columns = ["int"], ColumnIndices = [0] }));
    }

    [Fact]
    public void Overrides_convert_values()
    {
        var overrides = new Dictionary<string, IArrowType>
        {
            ["int"] = Int64Type.Default,
            ["mixed_num"] = StringType.Default,
            ["date"] = new TimestampType(TimeUnit.Millisecond, (string?)null),
            ["datetime"] = new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"),
            ["time"] = new Time32Type(TimeUnit.Millisecond),
            ["gaps"] = DoubleType.Default,
        };
        var (schema, rows) = Read("types_openpyxl", new() { Columns = [.. overrides.Keys], SchemaOverrides = overrides });
        Assert.Equal(["int:int64", "mixed_num:utf8", "date:timestamp", "datetime:timestamp", "time:time32", "gaps:double"], Types(schema));
        // The Tokyo wall clock 1999-12-31 23:59:59 is 14:59:59 UTC.
        Assert.Equal($"-2 | \"2.5\" | {Days(1900, 1, 1) * 86_400_000L} | {Micros(new DateTime(1999, 12, 31, 14, 59, 59))} | 86399500 | null", rows[1]);

        // 9007199254740992 does not fit Int32; 1E+300 does not fit decimal(10, 2).
        var e = Assert.Throws<ExcelValueException>(() => Read("types_openpyxl", new() { SchemaOverrides = new Dictionary<string, IArrowType> { ["int"] = Int32Type.Default } }));
        Assert.Equal("A4", e.Cell);
        Assert.Equal("Types", e.Sheet);
        e = Assert.Throws<ExcelValueException>(() => Read("types_openpyxl", new() { SchemaOverrides = new Dictionary<string, IArrowType> { ["float"] = new Decimal128Type(10, 2) } }));
        Assert.Equal("B4", e.Cell);

        Assert.Throws<ArgumentException>(() => Read("types_openpyxl", new() { SchemaOverrides = new Dictionary<string, IArrowType> { ["missing"] = Int32Type.Default } }));
    }

    [Fact]
    public void Text_overrides_parse()
    {
        using var ms = Workbook(("n", Strings(["12", " -3 ", "0.5"])), ("d", Strings(["2024-02-29", "1900-01-01", null])), ("b", Strings(["TRUE", "false", null])));
        using var reader = ExcelReader.Open(ms, new()
        {
            SchemaOverrides = new Dictionary<string, IArrowType>
            {
                ["n"] = new Decimal128Type(5, 1),
                ["d"] = Date32Type.Default,
                ["b"] = BooleanType.Default,
            },
        });
        Assert.Equal(["12.0 | 19782 | true", "-3.0 | -25567 | false", "0.5 | null | null"], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void A_value_that_does_not_fit_after_the_window_names_its_cell()
    {
        // Int64 beyond 2^53 is written as text: a late string in a column inferred as Int64.
        var batch = Batch(("a", Fixed<long>(Int64Type.Default, [1, 2, 3, 4, long.MaxValue]), true));
        var file = new MemoryStream();
        ExcelWriter.Write(file, batch.Schema, [batch], new() { LeaveOpen = true });
        file.Position = 0;
        using var reader = ExcelReader.Open(file, new() { InferSchemaLength = 3 });
        Assert.Equal("int64", reader.Schema.FieldsList[0].DataType.Name);
        var e = Assert.Throws<ExcelValueException>(() => reader.ReadBatches().ToList());
        Assert.Equal("A6", e.Cell);
        Assert.Contains("InferSchemaLength", e.Message); // the message suggests a remedy
    }

    [Fact]
    public void Whole_sheet_inference_sees_late_values()
    {
        var batch = Batch(("a", Fixed<long>(Int64Type.Default, [1, 2, 3, 4, long.MaxValue]), true));
        var file = new MemoryStream();
        ExcelWriter.Write(file, batch.Schema, [batch], new() { LeaveOpen = true });
        file.Position = 0;
        using var reader = ExcelReader.Open(file, new() { InferSchemaLength = null });
        Assert.Equal("utf8", reader.Schema.FieldsList[0].DataType.Name);
        Assert.Equal(["\"1\"", "\"2\"", "\"3\"", "\"4\"", "\"9223372036854775807\""], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void A_time_format_over_whole_serials_reads_as_datetime()
    {
        // A date-time shown with a time-only format (h:mm): Excel's clock time of a full serial.
        var batch = Batch(("t", Fixed<long>(new TimestampType(TimeUnit.Microsecond, (string?)null),
            [Micros(new DateTime(2023, 10, 1, 14, 30, 0))]), true));
        var file = new MemoryStream();
        ExcelWriter.Write(file, batch.Schema, [batch], new() { DatetimeFormat = "h:mm AM/PM", LeaveOpen = true });
        file.Position = 0;
        using var reader = ExcelReader.Open(file);
        Assert.Equal("timestamp", reader.Schema.FieldsList[0].DataType.Name);
        Assert.Equal([$"{Micros(new DateTime(2023, 10, 1, 14, 30, 0))}"], ArrowRender.Rows(reader.ReadBatches()));
    }

    // A sheet given as rows of (column, inline string) cells, row 1 first.
    private static MemoryStream Sheet(string workbookPr, params string[] rows)
    {
        var ms = new MemoryStream();
        using (var z = new System.IO.Compression.ZipArchive(ms, System.IO.Compression.ZipArchiveMode.Create, true))
        {
            void Part(string name, string text)
            {
                using var w = new StreamWriter(z.CreateEntry(name).Open());
                w.Write(text);
            }
            Part("_rels/.rels", """<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>""");
            Part("xl/workbook.xml", $"""<workbook xmlns:r="r">{workbookPr}<sheets><sheet name="S" sheetId="1" r:id="rId1"/></sheets></workbook>""");
            Part("xl/_rels/workbook.xml.rels", """<Relationships><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="s.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>""");
            Part("xl/styles.xml", """<styleSheet><cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="14"/></cellXfs></styleSheet>""");
            Part("xl/s.xml", $"""<worksheet><sheetData>{string.Concat(rows.Select((r, i) => $"<row r=\"{i + 1}\">{r}</row>"))}</sheetData></worksheet>""");
        }
        ms.Position = 0;
        return ms;
    }

    private static string Num(string cell, string value, int style = 0) => $"<c r=\"{cell}\" s=\"{style}\"><v>{value}</v></c>";

    [Fact]
    public void A_value_right_of_the_known_columns_names_its_cell()
    {
        using var ms = Sheet("", Num("A1", "1"), Num("A2", "2"), Num("A3", "3") + Num("C3", "4"));
        using var reader = ExcelReader.Open(ms, new() { HasHeader = false, InferSchemaLength = 2 });
        Assert.Single(reader.Schema.FieldsList);
        var e = Assert.Throws<ExcelValueException>(() => reader.ReadBatches().ToList());
        Assert.Equal("C3", e.Cell);

        // With a projection, cells outside it are simply not read.
        ms.Position = 0;
        using var projected = ExcelReader.Open(ms, new() { HasHeader = false, InferSchemaLength = 2, Columns = ["column_1"], LeaveOpen = true });
        Assert.Equal(["1", "2", "3"], ArrowRender.Rows(projected.ReadBatches()));
    }

    [Fact]
    public void A_value_in_a_dropped_empty_column_names_its_cell()
    {
        // Column B has no header and no value in the window, so it is dropped; a later value there is an error.
        using var ms = Sheet("", Num("A1", "1") + Num("C1", "2"), Num("A2", "3") + Num("C2", "4"), Num("A3", "5") + Num("B3", "6"));
        using var wb = ExcelWorkbook.Open(ms);
        using var reader = wb.OpenSheet(new() { InferSchemaLength = 1 });
        Assert.Equal(["1", "2"], reader.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal("B3", Assert.Throws<ExcelValueException>(() => reader.ReadBatches().ToList()).Cell);

        using var kept = wb.OpenSheet(new() { InferSchemaLength = 1, DropEmptyColumns = false });
        Assert.Equal(["1", "__UNNAMED__1", "2"], kept.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(["3 | null | 4", "5 | \"6\" | null"], ArrowRender.Rows(kept.ReadBatches())); // empty in the window: Utf8
    }

    [Fact]
    public void Overrides_honour_the_1904_date_system()
    {
        // Serial 43831 is 2024-01-02 in the 1904 system (2020-01-01 in the 1900 one).
        using var ms = Sheet("<workbookPr date1904=\"1\"/>", "<c r=\"A1\" t=\"inlineStr\"><is><t>d</t></is></c>", Num("A2", "43831"));
        using var reader = ExcelReader.Open(ms, new()
        {
            SchemaOverrides = new Dictionary<string, IArrowType> { ["d"] = new TimestampType(TimeUnit.Microsecond, (string?)null) },
        });
        Assert.Equal([$"{Micros(new DateTime(2024, 1, 2))}"], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void Empty_text_is_an_empty_cell()
    {
        // =IF(…, "", n) caches an empty string: the column stays numeric and the row has no value (as Python Polars).
        using var ms = Sheet("", "<c r=\"A1\" t=\"inlineStr\"><is><t>n</t></is></c>", Num("A2", "1"),
            "<c r=\"A3\" t=\"str\"><f>IF(TRUE,\"\",1)</f><v></v></c>", Num("A4", "3"), "<c r=\"A5\" t=\"inlineStr\"><is><t></t></is></c>");
        using var wb = ExcelWorkbook.Open(ms);
        using var reader = wb.OpenSheet();
        Assert.Equal("int64", reader.Schema.FieldsList[0].DataType.Name);
        Assert.Equal(["1", "3"], ArrowRender.Rows(reader.ReadBatches()));
        using var kept = wb.OpenSheet(new() { DropEmptyRows = false });
        Assert.Equal(["1", "null", "3"], ArrowRender.Rows(kept.ReadBatches()));
    }

    [Fact]
    public void Window_zero_reads_text()
    {
        var (schema, rows) = Read("sparse", new() { InferSchemaLength = 0 });
        Assert.Equal(["k:utf8", "v:utf8"], Types(schema));
        Assert.Equal(["\"a\" | \"1\"", "\"b\" | \"2\""], rows);
    }

    [Fact]
    public void Empty_sheets()
    {
        var batch = Batch(("a", Fixed<int>(Int32Type.Default, []), true));
        var file = new MemoryStream();
        ExcelWriter.Write(file, batch.Schema, [batch], new() { LeaveOpen = true });
        file.Position = 0;
        Assert.Throws<ExcelFormatException>(() => ExcelReader.Open(file, new() { LeaveOpen = true }));
        file.Position = 0;
        using var reader = ExcelReader.Open(file, new() { RaiseIfEmpty = false });
        Assert.Equal(["a:utf8"], Types(reader.Schema));
        Assert.Empty(reader.ReadBatches());
    }

    [Fact]
    public void Non_seekable_streams_are_buffered()
    {
        using var file = File.OpenRead(Fixture("workbook_xlsxwriter"));
        using var reader = ExcelReader.Open(new ForwardOnlyStream(file));
        Assert.Equal(4, reader.ReadBatches().Sum(b => b.Length));
    }

    [Fact]
    public void Other_containers_are_named_in_the_error()
    {
        Assert.Contains("OLE2", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream([0xD0, 0xCF, 0x11, 0xE0, 0, 0, 0, 0]))).Message);
        Assert.Contains("not an Excel workbook", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream("hello"u8.ToArray()))).Message);

        var zip = new MemoryStream();
        using (var z = new System.IO.Compression.ZipArchive(zip, System.IO.Compression.ZipArchiveMode.Create, true))
        {
            using (var w = new StreamWriter(z.CreateEntry("mimetype").Open())) w.Write("application/vnd.oasis.opendocument.spreadsheet");
            using (var w = new StreamWriter(z.CreateEntry("content.xml").Open())) w.Write("<x/>");
        }
        zip.Position = 0;
        Assert.Contains(".ods", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(zip)).Message);
    }

    [Theory]
    [InlineData("<row r=\"2\"><c r=\"A2\"><v>1</v></c>")]           // truncated row
    [InlineData("<row r=\"2\"><c r=\"A2\" t=\"zz\"><v>1</v></c></row>")] // unknown cell type
    [InlineData("<row r=\"2\"><c r=\"A2\"><v>abc</v></c></row>")]     // not a number
    [InlineData("<row r=\"2\"><c r=\"A2\" t=\"s\"><v>99</v></c></row>")] // shared string out of range
    public void Malformed_sheets_raise_format_errors(string rows)
    {
        var ms = new MemoryStream();
        using (var z = new System.IO.Compression.ZipArchive(ms, System.IO.Compression.ZipArchiveMode.Create, true))
        {
            void Part(string name, string text)
            {
                using var w = new StreamWriter(z.CreateEntry(name).Open());
                w.Write(text);
            }
            Part("_rels/.rels", """<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>""");
            Part("xl/workbook.xml", """<workbook xmlns:r="r"><sheets><sheet name="S" sheetId="1" r:id="rId1"/></sheets></workbook>""");
            Part("xl/_rels/workbook.xml.rels", """<Relationships><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="s.xml"/></Relationships>""");
            Part("xl/s.xml", $"""<worksheet><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>h</t></is></c></row>{rows}</sheetData></worksheet>""");
        }
        ms.Position = 0;
        Assert.Throws<ExcelFormatException>(() =>
        {
            using var reader = ExcelReader.Open(ms);
            reader.ReadBatches().ToList();
        });
    }

    [Fact]
    public async Task Async_open_and_read_match_sync()
    {
        await using var reader = await ExcelReader.OpenAsync(Fixture("types_openpyxl"));
        List<string> rows = [];
        await foreach (var b in reader.ReadBatchesAsync()) rows.AddRange(ArrowRender.Rows(b));
        Assert.Equal(Read("types_openpyxl").Rows, rows);
    }

    private sealed class ForwardOnlyStream(Stream inner) : Stream
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
