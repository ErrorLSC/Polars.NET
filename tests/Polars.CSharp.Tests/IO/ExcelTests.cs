#nullable enable
using MiniExcelLibs;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class ExcelTests
{
    private static string TempFile() => Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

    [Fact]
    public void Read_a_workbook_written_by_MiniExcel()
    {
        string path = TempFile();
        try
        {
            MiniExcel.SaveAs(path, (List<Person>)
            [
                new(1, "Alice", new DateTime(2022, 1, 1), 99.5, true, 13800138000),
                new(2, "Bob", new DateTime(2023, 5, 20), 88.0, false, 13900139000),
                new(3, "Charlie", new DateTime(2024, 12, 31), 60.0, true, 13700137000),
            ]);

            using (var df = DataFrame.ReadExcel(path))
            {
                Assert.Equal(3, df.Height);
                // Whole numbers are Int64; MiniExcel formats DateTime cells as dates, which read as Date.
                Assert.Equal(DataType.Int64, df.Schema["Id"]);
                Assert.Equal(DataType.Date, df.Schema["JoinDate"]);
                Assert.Equal(new DateOnly(2022, 1, 1), df.GetValue<DateOnly>(0, "JoinDate"));
                Assert.Equal(99.5, df.GetValue<double>(0, "Score"));
                Assert.True(df.GetValue<bool>(0, "IsActive"));
                Assert.Equal(13800138000L, df.GetValue<long>(0, "Phone"));
            }

            using var overridden = DataFrame.ReadExcel(path, schema: PolarsSchema.From<PhoneAsText>());
            Assert.Equal(DataType.String, overridden.Schema["Phone"]);
            Assert.Equal("13800138000", overridden.GetValue<string>(0, "Phone"));
            Assert.Equal(DataType.Float64, overridden.Schema["Score"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Empty_rows_are_dropped_or_kept()
    {
        string path = TempFile();
        try
        {
            MiniExcel.SaveAs(path, (List<Sparse>) [new("A", 1), new(null, null), new("C", 3)]);

            using var dropped = DataFrame.ReadExcel(path, dropEmptyRows: true);
            Assert.Equal(2, dropped.Height);
            Assert.Equal("A", dropped.GetValue<string>(0, "Col1"));
            Assert.Equal("C", dropped.GetValue<string>(1, "Col1"));

            using var kept = DataFrame.ReadExcel(path, dropEmptyRows: false);
            Assert.Equal(3, kept.Height);
            Assert.Null(kept.GetValue<string>(1, "Col1"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Write_keeps_big_integers_and_formats()
    {
        string path = TempFile();
        try
        {
            using var df = DataFrame.From<Logged>([new(1, ulong.MaxValue, new DateTime(2023, 10, 1), new DateTime(2023, 10, 1, 14, 30, 0))]);
            df.WriteExcel(path, sheetName: "Data", dateFormat: "dd/mm/yyyy", datetimeFormat: "hh:mm AM/PM");

            // UInt64 beyond 2^53 is text; a datetime shown with a time-only format still reads as a datetime.
            using var read = DataFrame.ReadExcel(path, sheetName: "Data", schema: PolarsSchema.From<BigIdAsText>());
            Assert.Equal(ulong.MaxValue.ToString(), read.GetValue<string>(0, "BigId"));
            Assert.Equal(new DateTime(2023, 10, 1), read.GetValue<DateTime>(0, "JoinDate"));
            Assert.Equal(new DateTime(2023, 10, 1, 14, 30, 0), read.GetValue<DateTime>(0, "LogTime"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Round_trip_through_streams_keeps_types()
    {
        using var df = DataFrame.From<Measure>(
        [
            new(1, 1.5f, 0.1m, "x", new DateOnly(2024, 2, 29), new DateTime(2024, 5, 6, 7, 8, 9).AddTicks(1_234_560), new TimeOnly(1, 2, 3), TimeSpan.FromHours(30)),
            new(-(1L << 53), float.NaN, null, null, null, null, null, TimeSpan.FromMilliseconds(-1500)),
        ]);
        var ms = new MemoryStream();
        df.WriteExcel(ms);
        ms.Position = 0;
        using var read = DataFrame.ReadExcel(ms);

        Assert.Equal(DataType.Int64, read.Schema["Id"]);
        Assert.Equal(-(1L << 53), read.GetValue<long>(1, "Id"));
        Assert.Equal(1.5, read.GetValue<double>(0, "Ratio"));
        Assert.Null(read.GetValue<double?>(1, "Ratio")); // NaN is a #NUM! error, read as null
        Assert.Equal(0.1, read.GetValue<double>(0, "Price"));
        Assert.Equal(new DateOnly(2024, 2, 29), read.GetValue<DateOnly>(0, "Day"));
        Assert.Equal(new DateTime(2024, 5, 6, 7, 8, 9).AddTicks(1_234_560), read.GetValue<DateTime>(0, "At"));
        Assert.Equal(new TimeOnly(1, 2, 3), read.GetValue<TimeOnly>(0, "Clock"));
        Assert.Equal(TimeSpan.FromHours(30), read.GetValue<TimeSpan>(0, "Elapsed"));
        Assert.Equal(TimeSpan.FromMilliseconds(-1500), read.GetValue<TimeSpan>(1, "Elapsed"));
    }

    [Fact]
    public void Zoned_datetimes_are_written_as_wall_clock_time()
    {
        using var utc = DataFrame.From<Instant>([new(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc))]);
        using var tokyo = utc.WithColumns(Col("At").Dt.ConvertTimeZone("Asia/Tokyo"));
        var ms = new MemoryStream();
        tokyo.WriteExcel(ms);
        ms.Position = 0;
        using var read = DataFrame.ReadExcel(ms);
        Assert.Equal(new DateTime(2024, 1, 1, 9, 0, 0), read.GetValue<DateTime>(0, "At"));
    }

    [Fact]
    public void Columns_scan_and_sink()
    {
        string path = TempFile();
        string sunk = TempFile();
        try
        {
            using (var df = DataFrame.From<Sale>([new("east", 10.5, 1), new("west", 20.25, 2), new("east", 1, 3)]))
                df.WriteExcel(path, autofit: true, freezeHeader: true, compressionLevel: System.IO.Compression.CompressionLevel.Fastest);

            using var picked = DataFrame.ReadExcel(path, columns: ["Units", "Region"]);
            Assert.Equal(["Region", "Units"], picked.Columns);

            using var scanned = LazyFrame.ScanExcel(path)
                .Filter(Col("Region") == Lit("east"))
                .Select(Col("Amount").Sum())
                .Collect();
            Assert.Equal(11.5, scanned.GetValue<double>(0, "Amount"));

            LazyFrame.ScanExcel(path).Filter(Col("Units") > Lit(1L)).SinkExcel(sunk, sheetName: "Big");
            using var back = DataFrame.ReadExcel(sunk, sheetName: "Big");
            Assert.Equal(2, back.Height);
            Assert.Equal("west", back.GetValue<string>(0, "Region"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(sunk);
        }
    }

    [Fact]
    public void Sinking_an_empty_result_writes_the_header()
    {
        using var df = DataFrame.From<Sale>([new("east", 10.5, 1)]);
        var ms = new MemoryStream();
        df.Lazy().Filter(Col("Units") > Lit(5L)).SinkExcel(ms);
        ms.Position = 0;
        using var read = DataFrame.ReadExcel(ms, raiseIfEmpty: false);
        Assert.Equal(["Region", "Amount", "Units"], read.Columns);
        Assert.Equal(0, read.Height);
    }

    [Fact]
    public void Columns_without_a_cell_form_are_refused()
    {
        using var df = DataFrame.From<Tagged>([new(1, ["a", "b"])]);
        var e = Assert.Throws<NotSupportedException>(() => df.WriteExcel(new MemoryStream()));
        Assert.Contains("Tags", e.Message);
    }

    [Fact]
    public void Binary_workbooks_read_through_the_managed_reader()
    {
        string Calamine(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "calamine", $"{name}.xlsb");

        using var pairs = DataFrame.ReadExcel(Calamine("issues"), sheetName: "issue2", hasHeader: false);
        Assert.Equal(DataType.Int64, pairs.Schema["column_1"]);
        Assert.Equal("c", pairs.GetValue<string>(2, "column_2"));

        using (var stream = File.OpenRead(Calamine("float_values")))
        using (var floats = DataFrame.ReadExcel(stream, hasHeader: false))
            Assert.Equal(12345.67, floats.GetValue<double>(4, "column_1"));

        // Dates stored as RK integers read as dates (1904 system included).
        using var dates = DataFrame.ReadExcel(Calamine("date_1904"), hasHeader: false, columns: ["column_1"], inferSchemaLen: 2, raiseIfEmpty: false,
            schema: PolarsSchema.From<DayColumn>());
        Assert.Equal(new DateTime(2021, 1, 1), dates.GetValue<DateTime>(0, "column_1"));

        using var scanned = LazyFrame.ScanExcel(Calamine("formula_references"), sheetName: "Sheet2", hasHeader: false)
            .Select(Col("column_1").Sum())
            .Collect();
        Assert.Equal(4L, scanned.GetValue<long>(0, "column_1"));

        Assert.Contains("chart sheet", Assert.Throws<ArgumentException>(() => DataFrame.ReadExcel(Calamine("any_sheets"), sheetIndex: 3)).Message);

        // An empty sheet without RaiseIfEmpty: an empty frame without columns.
        using (var empty = DataFrame.ReadExcel(Calamine("sheet_names_order"), raiseIfEmpty: false))
        {
            Assert.Equal(0, empty.Height);
            Assert.Empty(empty.Columns);
        }
        using (var emptyScan = LazyFrame.ScanExcel(Calamine("sheet_names_order"), raiseIfEmpty: false).Collect())
            Assert.Equal(0, emptyScan.Height);
        Assert.ThrowsAny<Exception>(() => DataFrame.ReadExcel(Calamine("sheet_names_order")));

        // A table of a binary workbook (built record by record: no open-source writer produces .xlsb).
        var b = new Seseragi.Files.Tests.XlsbBuilder();
        b.Sheet("Data")
            .Row(0).Text(0, "title")
            .Row(2).Text(1, "region").Text(2, "amount")
            .Row(3).Text(1, "east").Real(2, 10.5)
            .Row(4).Text(1, "total").Real(2, 10.5)
            .Table("Sales", 2, 1, 4, 2, totalsRows: 1)
            .EndSheet();
        using var table = DataFrame.ReadExcel(b.Build(), tableName: "Sales");
        Assert.Equal(["region", "amount"], table.Columns);
        Assert.Equal(1, table.Height);
    }

    [Theory]
    [InlineData("xls")]
    [InlineData("ods")]
    public void Other_formats_still_read_natively(string extension)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "excel", $"sparse.{extension}");
        using var df = DataFrame.ReadExcel(path);
        Assert.Equal(["k", "v"], df.Columns);
        Assert.Equal(2, df.Height);
        Assert.Equal("b", df.GetValue<string>(1, "k"));
        Assert.Throws<NotSupportedException>(() => DataFrame.ReadExcel(path, columns: ["k"]));
        Assert.Throws<NotSupportedException>(() => DataFrame.ReadExcel(path, tableName: "T"));
    }

    [Fact]
    public void Sheet_index_counts_chart_sheets_and_refuses_them()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "excel", "chartsheet.xlsx");
        var e = Assert.Throws<ArgumentException>(() => DataFrame.ReadExcel(path));
        Assert.Contains("chart sheet", e.Message);
        using var df = DataFrame.ReadExcel(path, sheetIndex: 1);
        Assert.Equal(20L, df.GetValue<long>(1, "y"));
    }

    private sealed record DayColumn(DateTime column_1);

    private sealed record Person(int Id, string Name, DateTime JoinDate, double Score, bool IsActive, long Phone);

    private sealed record PhoneAsText(string Phone);

    private sealed record BigIdAsText(string BigId);

    private sealed record Sparse(string? Col1, int? Col2);

    private sealed record Logged(int Id, ulong BigId, DateTime JoinDate, DateTime LogTime);

    private sealed record Measure(long Id, float Ratio, decimal? Price, string? Name, DateOnly? Day, DateTime? At, TimeOnly? Clock, TimeSpan Elapsed);

    private sealed record Instant(DateTime At);

    private sealed record Sale(string Region, double Amount, long Units);

    private sealed record Tagged(int Id, string[] Tags);
}
