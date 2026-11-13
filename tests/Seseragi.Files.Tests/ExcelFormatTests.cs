using Kaminari.Seseragi.Files.Excel;

namespace Seseragi.Files.Tests;

/// <summary>Number format classification (cases from calamine's and openpyxl's date-format tests, MIT) and serial
/// date arithmetic.</summary>
public class ExcelFormatTests
{
    [Theory]
    // calamine formats.rs test_is_date_format (ported from openpyxl), with the finer kinds this package tells apart.
    [InlineData("DD/MM/YY", "Date")]
    [InlineData("H:MM:SS;@", "Time")]
    [InlineData("#,##0\\ [$\\u20bd-46D]", "Number")]
    [InlineData("m\"M\"d\"D\";@", "Date")]
    [InlineData("[h]:mm:ss", "Duration")]
    [InlineData("\"Y: \"0.00\"m\";\"Y: \"-0.00\"m\";\"Y: <num>m\";@", "Number")]
    [InlineData("#,##0\\ [$''u20bd-46D]", "Number")]
    [InlineData("\"$\"#,##0_);[Red](\"$\"#,##0)", "Number")]
    [InlineData("[$-404]e\"\\xfc\"m\"\\xfc\"d\"\\xfc\"", "Date")]
    [InlineData("0_ ;[Red]\\-0\\ ", "Number")]
    [InlineData("\\Y000000", "Number")]
    [InlineData("#,##0.0####\" YMD\"", "Number")]
    [InlineData("[h]", "Duration")]
    [InlineData("[ss]", "Duration")]
    [InlineData("[s].000", "Duration")]
    [InlineData("[m]", "Duration")]
    [InlineData("[mm]", "Duration")]
    [InlineData("[Blue]\\+[h]:mm;[Red]\\-[h]:mm;[Green][h]:mm", "Duration")]
    [InlineData("[>=100][Magenta][s].00", "Duration")]
    [InlineData("[h]:mm;[=0]\\-", "Duration")]
    [InlineData("[>=100][Magenta].00", "Number")]
    [InlineData("[>=100][Magenta]General", "Number")]
    [InlineData("ha/p\\\\m", "Time")]
    [InlineData("#,##0.00\\ _M\"H\"_);[Red]#,##0.00\\ _M\"S\"_)", "Number")]
    [InlineData("#,##0*y", "Number")]
    [InlineData("0\"x\"*d", "Number")]
    [InlineData("*-#,##0", "Number")]
    [InlineData("*-yyyy-mm-dd", "Date")]
    // Date, date-time, time: 'm' is minutes after an hour or before seconds, months otherwise.
    [InlineData("yyyy-mm-dd", "Date")]
    [InlineData("yyyy-mm-dd hh:mm:ss", "DateTime")]
    [InlineData("m/d/yy h:mm", "DateTime")]
    [InlineData("d-mmm-yy", "Date")]
    [InlineData("mmm-yy", "Date")]
    [InlineData("mmmm", "Date")]
    [InlineData("h:mm AM/PM", "Time")]
    [InlineData("hh:mm:ss.000", "Time")]
    [InlineData("mm:ss", "Time")]
    [InlineData("mmss.0", "Time")]
    [InlineData("[$-F400]h:mm:ss\\ AM/PM", "Time")]
    [InlineData("[$-411]ggge\"年\"m\"月\"d\"日\"", "Date")]
    [InlineData("yyyy\"年\"m\"月\"d\"日\" h\"時\"mm\"分\"", "DateTime")]
    [InlineData("General", "Number")]
    [InlineData("@", "Number")]
    [InlineData("0.00E+00", "Number")]
    [InlineData("0%", "Number")]
    [InlineData("# ?/?", "Number")]
    public void Custom_formats(string format, string kind) =>
        Assert.Equal(kind, ExcelNumberFormat.Classify(format).ToString());

    [Theory]
    [InlineData(0, "Number")]
    [InlineData(14, "Date")]
    [InlineData(17, "Date")]
    [InlineData(18, "Time")]
    [InlineData(21, "Time")]
    [InlineData(22, "DateTime")]
    [InlineData(27, "DateTime")]
    [InlineData(36, "DateTime")]
    [InlineData(45, "Time")]
    [InlineData(46, "Duration")]
    [InlineData(47, "Time")]
    [InlineData(49, "Number")]
    [InlineData(58, "DateTime")]
    [InlineData(59, "Number")]
    public void Built_in_formats(int id, string kind) =>
        Assert.Equal(kind, ExcelNumberFormat.BuiltIn(id).ToString());

    [Theory]
    [InlineData(1, false, "1900-01-01")]
    [InlineData(59, false, "1900-02-28")]
    [InlineData(60, false, "1900-02-28")] // the fictitious 1900-02-29
    [InlineData(61, false, "1900-03-01")]
    [InlineData(0, false, "1899-12-31")]
    [InlineData(45293, false, "2024-01-02")]
    [InlineData(2958465, false, "9999-12-31")]
    [InlineData(0, true, "1904-01-01")]
    [InlineData(43831, true, "2024-01-02")]
    public void Serial_days(long serial, bool date1904, string date) =>
        Assert.Equal(date, DateOnly.FromDayNumber((int)(ExcelDates.UnixDays(serial, date1904) + 719_162)).ToString("yyyy-MM-dd"));

    [Theory]
    [InlineData("1900-01-01", 1)]
    [InlineData("1900-02-28", 59)]
    [InlineData("1900-03-01", 61)]
    [InlineData("2024-01-02", 45293)]
    [InlineData("9999-12-31", 2958465)]
    public void Dates_to_serials(string date, double serial)
    {
        int days = DateOnly.Parse(date).DayNumber - 719_162;
        Assert.True(ExcelDates.TryToSerial(days, 0, out double s));
        Assert.Equal(serial, s);
    }

    [Theory]
    [InlineData("1899-12-31")]
    [InlineData("0001-01-01")]
    public void Dates_before_1900_have_no_serial(string date) =>
        Assert.False(ExcelDates.TryToSerial(DateOnly.Parse(date).DayNumber - 719_162, 0, out _));

    [Theory]
    [InlineData(-1.0)]
    [InlineData(double.NaN)]
    [InlineData(2958466.0)] // 10000-01-01
    public void Serials_outside_excel_dates_are_rejected(double serial) =>
        Assert.False(ExcelDates.TryToUnixMicros(serial, false, out _));

    [Fact]
    public void A_fraction_just_below_midnight_rounds_to_the_next_day()
    {
        Assert.True(ExcelDates.TrySplit(1.9999999999999998, false, out long days, out long ofDay));
        Assert.Equal(DateOnly.Parse("1900-01-02").DayNumber - 719_162, days);
        Assert.Equal(0, ofDay);
    }
}
