using Apache.Arrow;
using Kaminari.Seseragi.Files.Excel;
using static Seseragi.Files.Tests.OdsBuilder;

namespace Seseragi.Files.Tests;

public class OdsReadTests
{
    private static string Calamine(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "calamine", name + ".ods");

    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", name);

    private static (Schema Schema, List<string> Rows) Read(Stream stream, ExcelReadOptions? options = null)
    {
        using var reader = ExcelReader.Open(stream, options);
        return (reader.Schema, ArrowRender.Rows(reader.ReadBatches()));
    }

    private static (Schema Schema, List<string> Rows) Read(string path, ExcelReadOptions? options = null) => Read(File.OpenRead(path), options);

    private static List<string> Types(Schema schema) => [.. schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.Name}")];

    private static readonly ExcelReadOptions Raw = new() { HasHeader = false, InferSchemaLength = 0, DropEmptyRows = false };

    // ------------------------------------------------------------ calamine's fixtures (written by LibreOffice)

    [Fact]
    public void Sheets_with_visibility()
    {
        using var wb = ExcelWorkbook.Open(Calamine("any_sheets"));
        Assert.Equal(ExcelFormat.Ods, wb.Format);
        Assert.Empty(wb.Tables);
        Assert.False(wb.Date1904);
        // OpenDocument hides a sheet one way only; LibreOffice turned the chart sheet into a worksheet.
        Assert.Equal(
        [
            new ExcelSheetInfo("Visible", 0, ExcelSheetVisibility.Visible),
            new("Hidden", 1, ExcelSheetVisibility.Hidden),
            new("VeryHidden", 2, ExcelSheetVisibility.Hidden),
            new("Chart", 3, ExcelSheetVisibility.Visible),
        ], wb.Sheets);
        using var reader = wb.OpenSheet(new() { HasHeader = false });
        Assert.Equal(["\"1\" | 2", "\"3\" | 4", "\"5\" | 6", "\"This workbook contains 4 sheets: Visible, Hidden, VeryHidden and Chart\" | null"],
            ArrowRender.Rows(reader.ReadBatches()));
        Assert.Equal([.. Enumerable.Range(1, 8).Select(i => $"Sheet{i}")], ExcelWorkbook.Open(Calamine("sheet_names_order")).Sheets.Select(s => s.Name));
    }

    [Fact]
    public void Dates_times_and_their_fractions()
    {
        // A date, a date-time, then two times of day (one with microseconds): one timestamp column, the times on
        // the day before the 1900 epoch's day 1 (as for xlsx); the second column holds numbers.
        var (schema, rows) = Read(Calamine("date"), new() { HasHeader = false });
        Assert.Equal(["column_1:timestamp", "column_2:int64"], Types(schema));
        Assert.Equal(["1609459200000000 | 15", "1609495810000000 | 16", "-2209038590000000 | 17", "-2209038589876544 | 18"], rows);
        var (_, text) = Read(Calamine("date"), Raw);
        Assert.Equal(["\"2021-01-01\" | \"15\"", "\"2021-01-01 10:10:10\" | \"16\"", "\"10:10:10\" | \"17\"", "\"10:10:10.123456\" | \"18\""], text);
    }

    [Fact]
    public void White_space_as_LibreOffice_reads_it()
    {
        var (_, rows) = Read(Calamine("special_cells"), Raw);
        // A single trailing space is written as a plain one: kept (ODF 6.1.2 would collapse it away).
        Assert.Equal(["\"Split\nLine\"", "\"Value  With spaces\"", "\"Value   With 3 spaces\"", "\" Value   With spaces before and after \"",
            "\"  Value   With 2 spaces before and after  \""], rows);
        var (_, tabs) = Read(Calamine("ods_tabs_line_breaks"), Raw);
        Assert.Equal(["\"tab\tchar\"", "\"two\ttabs\t\"", "\"line\nbreak\"", "\"a  b\""], tabs);
    }

    [Fact]
    public void Repeated_covered_and_annotated_cells()
    {
        var (_, repeated) = Read(Calamine("number_rows_repeated"), new() { SheetName = "Sheet3", HasHeader = false });
        Assert.Equal(["\"A\" | \"B\"", "\"C\" | \"D\"", "\"C\" | \"D\"", "\"C\" | \"D\"", "\"C\" | \"D\""], repeated);
        Assert.Equal(["\"a1\"", "\"a2\"", "\"a3\""], Read(Calamine("covered"), Raw).Rows);
        Assert.Equal(["\"A\" | \"B\" | \"C\"", "\"A\" | \"B\" | \"C\"", "null | null | \"C\""], Read(Calamine("merged_cells"), Raw).Rows);
        Assert.Equal(["\"cell a.1\""], Read(Calamine("with-annotation"), Raw).Rows); // the annotation's text left out
        var (_, special) = Read(Calamine("issues"), new() { SheetName = "spc_chrs", HasHeader = false });
        Assert.Equal(["\"&\"", "\"<\"", "\">\"", "\"aaa ' aaa\"", "\"\"\"", "\"☺\"", "\"֍\"", "\"àâéêèçöïî«»\""], special);
    }

    [Fact]
    public void Error_results_and_empty_text_are_empty_cells()
    {
        // LibreOffice writes an error result as a string with calcext:value-type="error" (calamine reads its text).
        // The last one also caches the text "cached": still an error.
        using var wb = ExcelWorkbook.Open(Calamine("cached_error_text"));
        Assert.Contains("no data", Assert.Throws<ExcelFormatException>(() => wb.OpenSheet(new() { HasHeader = false })).Message);
    }

    [Fact]
    public void Refused_workbooks()
    {
        Assert.Contains("encrypted", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Calamine("password_protected"))).Message);
        using var wb = ExcelWorkbook.Open(Calamine("dos_protection"));
        Assert.Contains("past column 16384", Assert.Throws<ExcelFormatException>(() => wb.OpenSheet(new() { HasHeader = false }).ReadBatches().ToList()).Message);
    }

    [Fact]
    public void LibreOffice_copies_read_alike_as_ods_and_fods()
    {
        foreach (string name in (string[])["sparse", "types_openpyxl"])
        {
            var (a, rowsA) = Read(Fixture(name + ".ods"));
            var (b, rowsB) = Read(Fixture(name + ".fods"));
            Assert.Equal(Types(a), Types(b));
            Assert.Equal(rowsA, rowsB);
        }
        using var date1904 = ExcelWorkbook.Open(Fixture("date1904.ods"));
        Assert.True(date1904.Date1904);
    }

    // ------------------------------------------------------------ built from markup

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Every_value_type(bool flat)
    {
        const string automatic = """
            <number:time-style style:name="N1"><number:hours/></number:time-style>
            <number:time-style style:name="N2" number:truncate-on-overflow="false"><number:hours/></number:time-style>
            <style:style style:name="ce1" style:family="table-cell" style:data-style-name="N1"/>
            <style:style style:name="ce2" style:family="table-cell" style:data-style-name="N2"/>
            <style:style style:name="ce3" style:family="table-cell" style:parent-style-name="Elapsed"/>
            """;
        const string common = """<style:style style:name="Elapsed" style:family="table-cell" style:data-style-name="N2"/>""";
        string sheet = """
            <table:table table:name="S">
             <table:table-column/><table:table-column table:default-cell-style-name="ce2"/>
             <table:table-row><table:table-cell office:value-type="string" office:string-value="kind"/><table:table-cell office:value-type="string"><text:p>value</text:p></table:table-cell></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>float</text:p></table:table-cell><table:table-cell office:value-type="float" office:value="2.5"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>percentage</text:p></table:table-cell><table:table-cell office:value-type="percentage" office:value="0.25"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>currency</text:p></table:table-cell><table:table-cell office:value-type="currency" office:currency="EUR" office:value="-3"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>date</text:p></table:table-cell><table:table-cell office:value-type="date" office:date-value="2024-02-29"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>datetime</text:p></table:table-cell><table:table-cell office:value-type="date" office:date-value="2024-02-29T13:14:15.1234567+02:00"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>time</text:p></table:table-cell><table:table-cell table:style-name="ce1" office:value-type="time" office:time-value="PT13H14M15.5S"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>elapsed (column style)</text:p></table:table-cell><table:table-cell office:value-type="time" office:time-value="PT1H"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>elapsed (parent style)</text:p></table:table-cell><table:table-cell table:style-name="ce3" office:value-type="time" office:time-value="PT2H"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>past a day</text:p></table:table-cell><table:table-cell table:style-name="ce1" office:value-type="time" office:time-value="P1DT1H"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>negative</text:p></table:table-cell><table:table-cell table:style-name="ce1" office:value-type="time" office:time-value="-PT30M"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>boolean</text:p></table:table-cell><table:table-cell office:value-type="boolean" office:boolean-value="true"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>boolean 0</text:p></table:table-cell><table:table-cell office:value-type="boolean" office:boolean-value="0"/></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>error</text:p></table:table-cell><table:table-cell office:value-type="string" office:string-value="" calcext:value-type="error"><text:p>#DIV/0!</text:p></table:table-cell></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>void</text:p></table:table-cell><table:table-cell><text:p>no type</text:p></table:table-cell></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>empty text</text:p></table:table-cell><table:table-cell office:value-type="string"><text:p/></table:table-cell></table:table-row>
             <table:table-row><table:table-cell office:value-type="string"><text:p>entities</text:p></table:table-cell><table:table-cell office:value-type="string" office:string-value="a&amp;b &lt;c&gt;"/></table:table-row>
            </table:table>
            """;
        var (schema, rows) = Read(Build(flat, sheet, automatic, common), new() { InferSchemaLength = 0 });
        Assert.Equal(["kind:utf8", "value:utf8"], Types(schema));
        Assert.Equal(
        [
            "\"float\" | \"2.5\"", "\"percentage\" | \"0.25\"", "\"currency\" | \"-3\"", "\"date\" | \"2024-02-29\"",
            "\"datetime\" | \"2024-02-29 13:14:15.123457\"", "\"time\" | \"13:14:15.5\"", "\"elapsed (column style)\" | \"1:00:00\"",
            "\"elapsed (parent style)\" | \"2:00:00\"", "\"past a day\" | \"25:00:00\"", "\"negative\" | \"-0:30:00\"", "\"boolean\" | \"true\"",
            "\"boolean 0\" | \"false\"", "\"error\" | null", "\"void\" | null", "\"empty text\" | null", "\"entities\" | \"a&b <c>\"",
        ], rows);

        // Typed: one column per kind.
        string typed = $"""
            <table:table table:name="T">
             <table:table-row>{Text("d")}{Text("t")}{Text("e")}</table:table-row>
             <table:table-row><table:table-cell office:value-type="date" office:date-value="2024-01-02"/><table:table-cell table:style-name="ce1" office:value-type="time" office:time-value="PT6H"/><table:table-cell table:style-name="ce2" office:value-type="time" office:time-value="PT6H"/></table:table-row>
            </table:table>
            """;
        var (t, values) = Read(Build(flat, typed, automatic, common));
        Assert.Equal(["d:date32", "t:time64", "e:duration"], Types(t));
        Assert.Equal(["19724 | 21600000000 | 21600000000"], values);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Paragraph_text(bool flat)
    {
        string[] cells =
        [
            "<text:p>  leading,\n\t trailing  and   inner  </text:p>",
            "<text:p>kept:<text:s text:c=\"3\"/>|<text:tab/>|<text:line-break/>|</text:p>",
            "<text:p>first</text:p><text:p>second</text:p><text:h>third</text:h>",
            "<text:p>a <text:span>span</text:span> <text:a xlink:href=\"x\">link</text:a> <text:meta>meta</text:meta></text:p>",
            "<text:p>field <text:sheet-name>S</text:sheet-name>gone</text:p>",
            "<text:p>frame<draw:frame><draw:text-box><text:p>inner</text:p></draw:text-box></draw:frame> gone</text:p>",
            "<office:annotation><text:p>note</text:p></office:annotation><text:p>value</text:p>",
            "<text:p><text:ruby><text:ruby-base>base</text:ruby-base><text:ruby-text>ruby</text:ruby-text></text:ruby></text:p>",
            "<text:p>nested</text:p><table:table table:name=\"inner\"><table:table-row><table:table-cell office:value-type=\"string\"><text:p>no</text:p></table:table-cell></table:table-row></table:table>",
        ];
        string sheet = Table("S", [.. cells.Select(c => $"<table:table-cell office:value-type=\"string\">{c}</table:table-cell>")]) + Table("After", Text("second sheet"));
        using var wb = ExcelWorkbook.Open(Build(flat, sheet));
        Assert.Equal(["S", "After"], wb.Sheets.Select(s => s.Name));
        using var reader = wb.OpenSheet(Raw);
        Assert.Equal(
        [
            "\"  leading,\n\t trailing  and   inner  \"", "\"kept:   |\t|\n|\"", "\"first\nsecond\nthird\"", "\"a span link meta\"", "\"field Sgone\"",
            "\"frame gone\"", "\"value\"", "\"base\"", "\"nested\"",
        ], ArrowRender.Rows(reader.ReadBatches()));
        using var after = wb.OpenSheet(Raw with { SheetName = "After" });
        Assert.Equal(["\"second sheet\""], ArrowRender.Rows(after.ReadBatches()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Repeats_groups_and_limits(bool flat)
    {
        string sheet = """
            <table:table table:name="S">
             <table:table-columns><table:table-column table:number-columns-repeated="16384"/></table:table-columns>
             <table:table-header-rows><table:table-row><table:table-cell office:value-type="string" table:number-columns-repeated="3"><text:p>h</text:p></table:table-cell><table:table-cell table:number-columns-repeated="16381"/></table:table-row></table:table-header-rows>
             <table:table-row-group>
              <table:table-row table:number-rows-repeated="2"><table:table-cell table:number-columns-repeated="2"/><table:table-cell office:value-type="float" office:value="7"/></table:table-row>
              <table:table-row table:number-rows-repeated="5"><table:table-cell table:number-columns-repeated="16384"/></table:table-row>
             </table:table-row-group>
             <table:table-row><table:covered-table-cell/><table:covered-table-cell office:value-type="float" office:value="8"/></table:table-row>
             <table:shapes><draw:frame><draw:text-box><text:p>shape</text:p></draw:text-box></draw:frame></table:shapes>
             <table:table-row table:number-rows-repeated="1048567"><table:table-cell table:number-columns-repeated="16384"/></table:table-row>
            </table:table>
            """;
        // The five empty rows between are kept (DropEmptyRows off), the trailing padding is not.
        var (_, rows) = Read(Build(flat, sheet), Raw);
        Assert.Equal(["\"h\" | \"h\" | \"h\"", "null | null | \"7\"", "null | null | \"7\"", .. Enumerable.Repeat("null | null | null", 5), "null | \"8\" | null"], rows);

        string tooManyRows = """<table:table table:name="S"><table:table-row table:number-rows-repeated="1048570"><table:table-cell/></table:table-row><table:table-row table:number-rows-repeated="7"><table:table-cell office:value-type="float" office:value="1"/></table:table-row></table:table>""";
        using var rowsWb = ExcelWorkbook.Open(Build(flat, tooManyRows));
        Assert.Contains("past row 1048576", Assert.Throws<ExcelFormatException>(() => rowsWb.OpenSheet(Raw).ReadBatches().ToList()).Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Hidden_sheets_null_date_and_a_later_sheet(bool flat)
    {
        const string automatic = """<style:style style:name="ta2" style:family="table"><style:table-properties table:display="false"/></style:style>""";
        string spreadsheet = """
            <table:calculation-settings><table:null-date table:date-value="1904-01-01"/></table:calculation-settings>
            <table:table table:name="First"><table:table-row><table:table-cell office:value-type="float" office:value="1"/></table:table-row></table:table>
            <table:table table:name="Second" table:style-name="ta2"><table:table-row><table:table-cell office:value-type="string"><text:p>n</text:p></table:table-cell></table:table-row><table:table-row><table:table-cell office:value-type="float" office:value="0.5"/></table:table-row></table:table>
            <table:table table:name="Empty"/>
            """;
        using var wb = ExcelWorkbook.Open(Build(flat, spreadsheet, automatic));
        Assert.True(wb.Date1904);
        Assert.Equal([ExcelSheetVisibility.Visible, ExcelSheetVisibility.Hidden, ExcelSheetVisibility.Visible], wb.Sheets.Select(s => s.Visibility));
        // Serial 0.5 in the 1904 system, through a schema override.
        using var second = wb.OpenSheet(new() { SheetIndex = 1, SchemaOverrides = new Dictionary<string, Apache.Arrow.Types.IArrowType> { ["n"] = new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, (string?)null) } });
        Assert.Equal(["-2082801600000000"], ArrowRender.Rows(second.ReadBatches()));
        using var empty = wb.OpenSheet(new() { SheetName = "Empty", RaiseIfEmpty = false });
        Assert.Empty(empty.ReadBatches());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Sheets_are_found_on_demand(bool flat)
    {
        string spreadsheet = Table("A", Text("a"), Float("1")) + Table("B", Text("b"), Float("2")) + Table("C", Text("c"), Float("3"));
        List<string> Rows(ExcelReader r) => ArrowRender.Rows(r.ReadBatches());

        // The first sheet goes on from the scan; read again, from its offset.
        using (var wb = ExcelWorkbook.Open(Build(flat, spreadsheet)))
        {
            using (var a = wb.OpenSheet()) Assert.Equal(["1"], Rows(a));
            using (var again = wb.OpenSheet()) Assert.Equal(["1"], Rows(again));
            Assert.Equal(["A", "B", "C"], wb.Sheets.Select(s => s.Name));
        }
        // A later sheet first (the scan skips the ones before it), then a passed one, both open at once.
        using (var wb = ExcelWorkbook.Open(Build(flat, spreadsheet)))
        {
            using var c = wb.OpenSheet(new() { SheetName = "c" });
            using var b = wb.OpenSheet(new() { SheetIndex = 1 });
            Assert.Equal(["2"], Rows(b));
            Assert.Equal(["3"], Rows(c));
            Assert.Contains("A, B, C", Assert.Throws<ArgumentException>(() => wb.OpenSheet(new() { SheetName = "D" })).Message);
            Assert.Throws<ArgumentOutOfRangeException>(() => wb.OpenSheet(new() { SheetIndex = 3 }));
        }
        // What follows the first sheet is not read until needed: a broken second sheet does not stop the first.
        string broken = Table("A", Text("a"), Float("1")) + "<table:table table:name=\"B\"><table:table-row><table:table-cell office:value-type=\"float\" office:value=\"1\">";
        using (var wb = ExcelWorkbook.Open(Build(flat, broken)))
        {
            using (var a = wb.OpenSheet()) Assert.Equal(["1"], Rows(a));
            Assert.Throws<ExcelFormatException>(() => wb.Sheets);
        }
    }

    [Fact]
    public async Task Streams_async_and_non_seekable()
    {
        string sheet = Table("S", Text("n"), Float("1"), Float("2"));
        foreach (bool flat in (bool[])[false, true])
        {
            var bytes = Build(flat, sheet).ToArray();
            var (_, rows) = Read(new ExcelReadTests.ForwardOnlyStream(new MemoryStream(bytes)));
            Assert.Equal(["1", "2"], rows);
            await using var reader = await ExcelReader.OpenAsync(new MemoryStream(bytes));
            int count = 0;
            await foreach (var batch in reader.ReadBatchesAsync()) count += batch.Length;
            Assert.Equal(2, count);
        }
    }

    [Fact]
    public void Detection()
    {
        string sheet = Table("S", Text("n"), Float("1"));
        // No mimetype part: the manifest names the package's type. A template is a spreadsheet too.
        Assert.Equal(ExcelFormat.Ods, ExcelWorkbook.Open(Package(sheet, mediaType: null)).Format);
        Assert.Equal(ExcelFormat.Ods, ExcelWorkbook.Open(Package(sheet, mediaType: Spreadsheet + "-template")).Format);
        Assert.Contains("not a spreadsheet", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Package(sheet, mediaType: "application/vnd.oasis.opendocument.text"))).Message);
        Assert.Contains("not a spreadsheet", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Flat(sheet, mediaType: "application/vnd.oasis.opendocument.text"))).Message);
        Assert.Contains("encrypted", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(Package(sheet, encrypted: true))).Message);
        Assert.Contains("not an Excel workbook", Assert.Throws<ExcelFormatException>(() => ExcelWorkbook.Open(new MemoryStream("<?xml version=\"1.0\"?><html/>"u8.ToArray()))).Message);
        // A flat document cut short inside a sheet: found when the sheet is read, or when all sheets are listed (opening
        // reads only up to the first sheet).
        var cut = Flat(sheet).ToArray();
        using var truncated = ExcelWorkbook.Open(new MemoryStream(cut[..(cut.Length - 80)]));
        Assert.Contains("truncated", Assert.Throws<ExcelFormatException>(() => truncated.OpenSheet(Raw).ReadBatches().ToList()).Message);
        using var listed = ExcelWorkbook.Open(new MemoryStream(cut[..(cut.Length - 80)]));
        Assert.Contains("truncated", Assert.Throws<ExcelFormatException>(() => listed.Sheets).Message);
    }

    [Theory]
    [InlineData("""<table:table-cell office:value-type="float" office:value="x"/>""", "number")]
    [InlineData("""<table:table-cell office:value-type="date" office:date-value="2024-02-30"/>""", "date")]
    [InlineData("""<table:table-cell office:value-type="time" office:time-value="P1M"/>""", "time")]
    [InlineData("""<table:table-cell table:number-columns-repeated="0"/>""", "number-columns-repeated")]
    public void Invalid_values_are_refused(string cell, string what)
    {
        using var wb = ExcelWorkbook.Open(Package($"""<table:table table:name="S"><table:table-row>{cell}</table:table-row></table:table>"""));
        var e = Assert.Throws<ExcelFormatException>(() => wb.OpenSheet(Raw).ReadBatches().ToList());
        Assert.Contains(what, e.Message);
    }
}
