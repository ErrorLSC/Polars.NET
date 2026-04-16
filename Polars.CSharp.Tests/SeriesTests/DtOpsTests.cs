using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesDtOpsTests
{
    [Fact]
    [Trait("Series", "DateTimeComponents")]
    public void Test_Series_Dt_Components()
    {
        DateTime?[] data = [
            new DateTime(2000, 1, 1),
            new DateTime(2001, 1, 1),
            new DateTime(2024, 5, 15),
            new DateTime(1999, 12, 31),
            null
        ];

        using Series s = Pl.Series("dates", data);

        using Series millennium = s.Dt.Millennium();
        Assert.Equal([2, 3, 3, 2, null], millennium.ToArray<int?>());

        using Series century = s.Dt.Century();
        Assert.Equal([20, 21, 21, 20, null], century.ToArray<int?>());

        using Series year = s.Dt.Year();
        Assert.Equal([2000, 2001, 2024, 1999, null], year.ToArray<int?>());

        using Series isoYear = s.Dt.IsoYear();
        Assert.Equal([1999, 2001, 2024, 1999, null], isoYear.ToArray<int?>());

        using Series quarter = s.Dt.Quarter();
        Assert.Equal([1, 1, 2, 4, null], quarter.ToArray<int?>());

        using Series month = s.Dt.Month();
        Assert.Equal([1, 1, 5, 12, null], month.ToArray<int?>());

        using Series day = s.Dt.Day();
        Assert.Equal([1, 1, 15, 31, null], day.ToArray<int?>());
    }
    [Fact]
    [Trait("Series", "DateTimeComponentsTimeAndExtended")]
    public void Test_Series_Dt_Time_And_Extended_Components()
    {
        DateTime?[] data = [
            new DateTime(2024, 2, 15, 8, 30, 45),   
            new DateTime(2023, 2, 28, 23, 59, 59),  
            new DateTime(2024, 12, 31, 0, 0, 0),    
            null
        ];

        using Series s = Pl.Series("datetimes", data);

        using Series daysInMonth = s.Dt.DaysInMonth();
        Assert.Equal([29, 28, 31, null], daysInMonth.ToArray<int?>());

        using Series ordinalDay = s.Dt.OrdinalDay();
        Assert.Equal([46, 59, 366, null], ordinalDay.ToArray<int?>());

        using Series weekday = s.Dt.Weekday();
        Assert.Equal([4, 2, 2, null], weekday.ToArray<int?>());

        using Series hour = s.Dt.Hour();
        Assert.Equal([8, 23, 0, null], hour.ToArray<int?>());

        using Series minute = s.Dt.Minute();
        Assert.Equal([30, 59, 0, null], minute.ToArray<int?>());

        using Series second = s.Dt.Second();
        Assert.Equal([45, 59, 0, null], second.ToArray<int?>());
    }
    [Fact]
    [Trait("Series", "DateTimeComponentsSubSecond")]
    public void Test_Series_Dt_SubSecond_And_Fractional_Second()
    {
        var dt1 = new DateTime(2024, 1, 1, 12, 30, 45).AddTicks(1234560); 
        
        var dt2 = new DateTime(2024, 1, 1, 12, 30, 0);
        
        var dt3 = new DateTime(2024, 1, 1, 12, 30, 59).AddTicks(9990000); 

        DateTime?[] data = [dt1, dt2, dt3, null];
        using Series s = Pl.Series("sub_seconds", data);

        using Series second = s.Dt.Second();
        Assert.Equal([45, 0, 59, null], second.ToArray<int?>());

        using Series ms = s.Dt.Millisecond();
        Assert.Equal([123, 0, 999, null], ms.ToArray<int?>());

        using Series us = s.Dt.Microsecond();
        Assert.Equal([123456, 0, 999000, null], us.ToArray<int?>());

        using Series ns = s.Dt.Nanosecond();
        Assert.Equal([123456000, 0, 999000000, null], ns.ToArray<int?>());

        using Series fractionalSecond = s.Dt.Second(fractional: true);
        
        Assert.Equal(DataType.Float64, fractionalSecond.DataType);

        var actualFractional = fractionalSecond.ToArray<double?>();
        Assert.Equal(4, actualFractional.Length);

        Assert.Equal(45.123456, actualFractional[0].Value, precision: 6);
        Assert.Equal(0.0,        actualFractional[1].Value, precision: 6);
        Assert.Equal(59.999000, actualFractional[2].Value, precision: 6);
        Assert.Null(actualFractional[3]);
    }
    [Fact]
    [Trait("Series", "DateTimeLeapAndMonthBounds")]
    public void Test_Series_Dt_LeapYear_And_Month_Bounds()
    {
        DateTime?[] data = [
            new DateTime(2024, 2, 15),  
            new DateTime(2023, 2, 15),  
            new DateTime(2000, 2, 15),  
            new DateTime(1900, 2, 15),  
            null
        ];

        using Series s = Pl.Series("dates", data);

        using Series isLeapYear = s.Dt.IsLeapYear();
        Assert.Equal(DataType.Boolean, isLeapYear.DataType);
        Assert.Equal([true, false, true, false, null], isLeapYear.ToArray<bool?>());

        using Series monthStart = s.Dt.MonthStart();
        Assert.Equal([
            new DateTime(2024, 2, 1),
            new DateTime(2023, 2, 1),
            new DateTime(2000, 2, 1),
            new DateTime(1900, 2, 1),
            null
        ], monthStart.ToArray<DateTime?>());

        using Series monthEnd = s.Dt.MonthEnd();
        Assert.Equal([
            new DateTime(2024, 2, 29), // 24年闰月
            new DateTime(2023, 2, 28), // 23年平月
            new DateTime(2000, 2, 29), // 2000世纪闰月
            new DateTime(1900, 2, 28), // 1900世纪平月
            null
        ], monthEnd.ToArray<DateTime?>());
    }
    [Fact]
    [Trait("Series", "DateTimeOffsetsTzAware")]
    public void Test_Series_Dt_Offsets_Timezone_Aware()
    {
        DateTime?[] data = [
            new DateTime(2024, 1, 1, 12, 0, 0), // EST: UTC-5
            new DateTime(2024, 7, 1, 12, 0, 0), // EDT: UTC-4 = Base -5 + DST +1
            null
        ];

        using Series naiveSeries = Pl.Series("naive_dates", data);
        
        using Series tzSeries = naiveSeries.Dt.ReplaceTimeZone("America/New_York");

        using Series baseOffset = tzSeries.Dt.BaseUtcOffset();
        using Series dstOffset = tzSeries.Dt.DstOffset();

        var baseVals = baseOffset.ToArray<TimeSpan?>();
        var dstVals = dstOffset.ToArray<TimeSpan?>();

        Assert.Equal([
            TimeSpan.FromHours(-5), 
            TimeSpan.FromHours(-5), 
            null
        ], baseVals);

        Assert.Equal([
            TimeSpan.Zero, 
            TimeSpan.FromHours(1), 
            null
        ], dstVals);
    }
    [Fact]
    [Trait("Series", "DateTimeExtraction")]
    public void Test_Series_Dt_Date_Time_Timestamp()
    {
        DateTime?[] data = [
            new DateTime(2024, 5, 15, 14, 30, 45, 123, DateTimeKind.Utc), 
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),          
            new DateTime(1969, 12, 31, 23, 59, 59, DateTimeKind.Utc),     
            null
        ];

        using Series s = Pl.Series("datetime_col", data);

        using Series dateSeries = s.Dt.Date();
        Assert.Equal(DataType.Date, dateSeries.DataType);
        Assert.Equal([
            new DateOnly(2024, 5, 15),
            new DateOnly(1970, 1, 1),
            new DateOnly(1969, 12, 31),
            null
        ], dateSeries.ToArray<DateOnly?>());

        using Series timeSeries = s.Dt.Time();
        Assert.Equal(DataType.Time, timeSeries.DataType);
        Assert.Equal([
            new TimeOnly(14, 30, 45, 123),
            new TimeOnly(0, 0, 0),
            new TimeOnly(23, 59, 59),
            null
        ], timeSeries.ToArray<TimeOnly?>());

        using Series tsMicro = s.Dt.Timestamp(); 
        Assert.Equal(DataType.Int64, tsMicro.DataType);
        long?[] microVals = tsMicro.ToArray<long?>();
        
        long expectedMicro1 = new DateTimeOffset(data[0].Value).ToUnixTimeMilliseconds() * 1000;
        Assert.Equal(expectedMicro1, microVals[0]);
        Assert.Equal(0L, microVals[1]);           // 1970-01-01 = 0 us
        Assert.Equal(-1_000_000L, microVals[2]);  //  -1,000,000 us
        Assert.Null(microVals[3]);

        // Milliseconds
        using Series tsMilli = s.Dt.Timestamp(TimeUnit.Milliseconds);
        long?[] milliVals = tsMilli.ToArray<long?>();
        
        long expectedMilli1 = new DateTimeOffset(data[0].Value).ToUnixTimeMilliseconds();
        Assert.Equal(expectedMilli1, milliVals[0]);
        Assert.Equal(0L, milliVals[1]);           // 1970-01-01 = 0 ms
        Assert.Equal(-1000L, milliVals[2]);       //  -1000 ms
        Assert.Null(milliVals[3]);
    }
    [Fact]
    [Trait("Series", "DurationTotals")]
    public void Test_Series_Dt_Duration_Totals()
    {
        TimeSpan ts1 = new(days: 1, hours: 12, minutes: 30, seconds: 0); 
        
        TimeSpan ts2 = new(days: 0, hours: 0, minutes: 45, seconds: 30);
        
        TimeSpan ts3 = TimeSpan.FromTicks(11234560); 

        TimeSpan?[] data = [ts1, ts2, ts3, null];
        using Series s = Pl.Series("durations", data);

        // TotalDays
        using Series daysInt = s.Dt.TotalDays();
        Assert.Equal(DataType.Int64, daysInt.DataType);
        Assert.Equal([1L, 0L, 0L, null], daysInt.ToArray<long?>());

        // TotalHours
        using Series hoursInt = s.Dt.TotalHours();
        Assert.Equal([36L, 0L, 0L, null], hoursInt.ToArray<long?>());

        // TotalMinutes
        using Series minsInt = s.Dt.TotalMinutes();
        Assert.Equal([2190L, 45L, 0L, null], minsInt.ToArray<long?>());

        // TotalHours
        using Series hoursFrac = s.Dt.TotalHours(fractional: true);
        Assert.Equal(DataType.Float64, hoursFrac.DataType);
        var hoursFracArr = hoursFrac.ToArray<double?>();
        Assert.Equal(36.5, hoursFracArr[0].Value, precision: 5);
        Assert.Equal(45.5 / 60.0, hoursFracArr[1].Value, precision: 5); 

        using Series minsFrac = s.Dt.TotalMinutes(fractional: true);
        var minsFracArr = minsFrac.ToArray<double?>();
        Assert.Equal(2190.0, minsFracArr[0].Value, precision: 5);
        Assert.Equal(45.5, minsFracArr[1].Value, precision: 5);


        // TotalMilliseconds
        using Series msInt = s.Dt.TotalMilliseconds();
        Assert.Equal([131400000L, 2730000L, 1123L, null], msInt.ToArray<long?>());

        // TotalMicroseconds
        using Series usInt = s.Dt.TotalMicroseconds();
        Assert.Equal([131400000000L, 2730000000L, 1123456L, null], usInt.ToArray<long?>());

        // TotalNanoseconds
        using Series nsInt = s.Dt.TotalNanoseconds();
        Assert.Equal([131400000000000L, 2730000000000L, 1123456000L, null], nsInt.ToArray<long?>());
    }
    [Fact]
    [Trait("Series", "DateTimeFormatting")]
    public void Test_Series_Dt_ToString_And_Strftime()
    {
        DateTime?[] data = [
            new DateTime(2024, 5, 15, 14, 30, 45), 
            new DateTime(2000, 1, 1, 0, 0, 0),     
            null
        ];

        using Series s = Pl.Series("datetimes", data);

        string customFormat = "%Y-%m-%d %H:%M:%S";
        
        using Series customToString = s.Dt.ToString(customFormat);
        using Series customStrftime = s.Dt.Strftime(customFormat);

        Assert.Equal(DataType.String, customToString.DataType);
        Assert.Equal(DataType.String, customStrftime.DataType);

        string[] expectedCustom = [
            "2024-05-15 14:30:45",
            "2000-01-01 00:00:00",
            null
        ];

        var actualToStringCustom = customToString.ToArray<string>();
        var actualStrftimeCustom = customStrftime.ToArray<string>();

        Assert.Equal(expectedCustom, actualToStringCustom);
        Assert.Equal(actualToStringCustom, actualStrftimeCustom);

        using Series defaultToString = s.Dt.ToString();
        using Series defaultStrftime = s.Dt.Strftime();

        var actualToStringDef = defaultToString.ToArray<string>();
        var actualStrftimeDef = defaultStrftime.ToArray<string>();

        Assert.Equal(actualToStringDef, actualStrftimeDef);

        Assert.NotNull(actualToStringDef[0]);
        Assert.StartsWith("2024-05-15 14:30:45", actualToStringDef[0]);
        Assert.StartsWith("2000-01-01 00:00:00", actualToStringDef[1]);
        Assert.Null(actualToStringDef[2]);
    }
    [Fact]
    [Trait("Series", "DateTimeTruncateAndRound")]
    public void Test_Series_Dt_Truncate_And_Round()
    {
        DateTime?[] data = [
            new DateTime(2024, 5, 15, 14, 10, 0), 
            new DateTime(2024, 5, 15, 14, 50, 0), 
            null
        ];

        using Series s = Pl.Series("datetimes", data);

        using Series truncString = s.Dt.Truncate("1h");
        
        Assert.Equal([
            new DateTime(2024, 5, 15, 14, 0, 0), // 14:10 -> 14:00
            new DateTime(2024, 5, 15, 14, 0, 0), // 14:50 -> 14:00 
            null
        ], truncString.ToArray<DateTime?>());

        using Series roundTimeSpan = s.Dt.Round(TimeSpan.FromHours(1));
        
        Assert.Equal([
            new DateTime(2024, 5, 15, 14, 0, 0), 
            new DateTime(2024, 5, 15, 15, 0, 0), 
            null
        ], roundTimeSpan.ToArray<DateTime?>());

        using Expr expr30m = Pl.Lit("30m");
        using Series truncExpr = s.Dt.Truncate(expr30m);
        
        Assert.Equal([
            new DateTime(2024, 5, 15, 14, 0, 0),  
            new DateTime(2024, 5, 15, 14, 30, 0), 
            null
        ], truncExpr.ToArray<DateTime?>());
        
        using Series roundExpr = s.Dt.Round(expr30m);
        
        Assert.Equal([
            new DateTime(2024, 5, 15, 14, 0, 0), 
            new DateTime(2024, 5, 15, 15, 0, 0),  
            null
        ], roundExpr.ToArray<DateTime?>());
    }
    [Fact]
    [Trait("Series", "DateTimeOffsetBy")]
    public void Test_Series_Dt_OffsetBy()
    {
        DateTime?[] data = [
            new DateTime(2024, 1, 31, 12, 0, 0), 
            new DateTime(2023, 1, 31, 12, 0, 0), 
            null
        ];

        using Series s = Pl.Series("datetimes", data);

        // ==========================================
        using Series offsetString = s.Dt.OffsetBy("1mo");
        
        Assert.Equal([
            new DateTime(2024, 2, 29, 12, 0, 0), 
            new DateTime(2023, 2, 28, 12, 0, 0), 
            null
        ], offsetString.ToArray<DateTime?>());

        using Series offsetStringNegative = s.Dt.OffsetBy("-1y");
        
        Assert.Equal([
            new DateTime(2023, 1, 31, 12, 0, 0), 
            new DateTime(2022, 1, 31, 12, 0, 0), 
            null
        ], offsetStringNegative.ToArray<DateTime?>());

        TimeSpan ts = new(hours: 2, minutes: 30, seconds: 0);
        using Series offsetTimeSpan = s.Dt.OffsetBy(ts);
        
        Assert.Equal([
            new DateTime(2024, 1, 31, 14, 30, 0), // 12:00 + 2.5h = 14:30
            new DateTime(2023, 1, 31, 14, 30, 0), 
            null
        ], offsetTimeSpan.ToArray<DateTime?>());

        using Expr expr1w = Pl.Lit("1w");
        using Series offsetExpr = s.Dt.OffsetBy(expr1w);
        
        Assert.Equal([
            new DateTime(2024, 2, 7, 12, 0, 0), 
            new DateTime(2023, 2, 7, 12, 0, 0), 
            null
        ], offsetExpr.ToArray<DateTime?>());
    }
    [Fact]
    [Trait("Series", "DateTimeCombine")]
    public void Test_Series_Dt_Combine()
    {
        DateOnly?[] dates = [
            new DateOnly(2024, 1, 1),   
            new DateOnly(2024, 6, 15),  
            null
        ];

        using Series s = Pl.Series("dates", dates);

        TimeOnly fixedTime = new(14, 30, 45);
        
        using Series combinedWithExpr = s.Dt.Combine(fixedTime, TimeUnit.Microseconds);

        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds, null), combinedWithExpr.DataType);
        
        Assert.Equal([
            new DateTime(2024, 1, 1, 14, 30, 45),
            new DateTime(2024, 6, 15, 14, 30, 45),
            null
        ], combinedWithExpr.ToArray<DateTime?>());

        TimeOnly?[] times = [
            new TimeOnly(8, 15, 0),   
            new TimeOnly(20, 45, 30), 
            null
        ];
        using Series timeSeries = Pl.Series("times", times);

        using Series combinedWithSeries = s.Dt.Combine(timeSeries, TimeUnit.Microseconds);

        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds, null), combinedWithSeries.DataType);
        
        Assert.Equal([
            new DateTime(2024, 1, 1, 8, 15, 0),
            new DateTime(2024, 6, 15, 20, 45, 30),
            null
        ], combinedWithSeries.ToArray<DateTime?>());
    }
    [Fact]
    [Trait("Series", "TimeZoneConversion")]
    public void Test_Series_Dt_Convert_And_Replace_TimeZone()
    {
        DateTime?[] data = [
            new DateTime(2024, 1, 1, 12, 0, 0), 
            new DateTime(2024, 7, 1, 12, 0, 0), 
            null
        ];

        using Series s = Pl.Series("naive_dates", data);

        using Series tzUtc = s.Dt.ReplaceTimeZone("UTC");
        
        using Series utcHours = tzUtc.Dt.Hour(); 
        Assert.Equal([12, 12, null], utcHours.ToArray<int?>());

        using Series tzNyConverted = tzUtc.Dt.ConvertTimeZone("America/New_York");
        using Series nyConvertedHours = tzNyConverted.Dt.Hour();
        
        Assert.Equal([7, 8, null], nyConvertedHours.ToArray<int?>());

        using Series tzNyReplaced = tzUtc.Dt.ReplaceTimeZone("America/New_York");
        using Series nyReplacedHours = tzNyReplaced.Dt.Hour();
        
        Assert.Equal([12, 12, null], nyReplacedHours.ToArray<int?>());

        using Series dstOffset = tzNyReplaced.Dt.DstOffset();
        Assert.Equal([
            TimeSpan.Zero,        
            TimeSpan.FromHours(1),
            null
        ], dstOffset.ToArray<TimeSpan?>());
    }
    [Fact]
    [Trait("Series", "BusinessDays")]
    public void Test_Series_Dt_Business_Days()
    {
        DateTime?[] dates = [
            new DateTime(2024, 5, 1),
            new DateTime(2024, 5, 2),
            new DateTime(2024, 5, 3),
            new DateTime(2024, 5, 4),
            null
        ];

        using Series s = Pl.Series("dates", dates);
        
        DateOnly[] holidaysArray = [new DateOnly(2024, 5, 2)];
        
        using Series holidaysSeries = Pl.Series("holidays_series", holidaysArray);

        using Series isBizEnum = s.Dt.IsBusinessDay(holidaysArray);
        Assert.Equal(DataType.Boolean, isBizEnum.DataType);
        Assert.Equal([true, false, true, false, null], isBizEnum.ToArray<bool?>());

        using Series isBizSeries = s.Dt.IsBusinessDay(holidaysSeries);
        Assert.Equal([true, false, true, false, null], isBizSeries.ToArray<bool?>());

        bool[] fourDayWorkWeek = [true, true, true, true, false, false, false];
        using Series isBizCustom = s.Dt.IsBusinessDay(holidaysArray, fourDayWorkWeek);
        Assert.Equal([true, false, false, false, null], isBizCustom.ToArray<bool?>());

        DateTime?[] validStartDates = [
            new DateTime(2024, 5, 1), 
            new DateTime(2024, 5, 3), 
            null
        ];
        using Series sValid = Pl.Series("valid_dates", validStartDates);
        
        using Expr nExpr = Pl.Lit(1); 
        
        using Series addedDays = sValid.Dt.AddBusinessDays(nExpr, holidaysArray);
        
        Assert.Equal([
            new DateTime(2024, 5, 3),
            new DateTime(2024, 5, 6),
            null
        ], addedDays.ToArray<DateTime?>());
    }
    [Fact]
    [Trait("Series", "DateTimeReplace_WithSeries")]
    public void Test_Series_Dt_Replace_With_Series()
    {
        DateTime?[] data = [
            new DateTime(2024, 5, 15, 14, 30, 45), 
            new DateTime(2023, 1, 1, 8, 0, 0),     
            null                                   
        ];
        using Series s = Pl.Series("datetimes", data);

        // ==========================================
        int?[] newYears = [2030, 2040, null];
        using Series yearSeries = Pl.Series("new_years", newYears);

        using Series replacedYears = s.Dt.Replace(year: yearSeries);
        
        Assert.Equal([
            new DateTime(2030, 5, 15, 14, 30, 45), 
            new DateTime(2040, 1, 1, 8, 0, 0),     
            null
        ], replacedYears.ToArray<DateTime?>());

        int?[] newHours = [10, 20, null];
        int?[] newMinutes = [15, 45, null];
        using Series hourSeries = Pl.Series("new_hours", newHours);
        using Series minuteSeries = Pl.Series("new_minutes", newMinutes);

        using Series replacedTime = s.Dt.Replace(
            hour: hourSeries, 
            minute: minuteSeries
        );
        
        Assert.Equal([
            new DateTime(2024, 5, 15, 10, 15, 45), 
            new DateTime(2023, 1, 1, 20, 45, 0),   
            null
        ], replacedTime.ToArray<DateTime?>());

        int?[] dynamicMonths = [11, 12, null];
        using Series monthSeries = Pl.Series("dynamic_months", dynamicMonths);

        using Series replacedMixed = s.Dt.Replace(
            year: 2099,          
            month: monthSeries  
        );

        Assert.Equal([
            new DateTime(2099, 11, 15, 14, 30, 45), 
            new DateTime(2099, 12, 1, 8, 0, 0),     
            null
        ], replacedMixed.ToArray<DateTime?>());
    }
    [Fact]
    [Trait("Series", "DateTimeEpoch")]
    public void Test_Series_Dt_Epoch()
    {
        DateTime?[] data = [
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), 
            new DateTime(1970, 1, 2, 0, 0, 0, DateTimeKind.Utc),   
            new DateTime(2024, 5, 15, 14, 30, 45, DateTimeKind.Utc), 
            null
        ];

        using Series s = Pl.Series("datetimes", data);

        long epochSec1 = 0;
        long epochSec2 = 86400; 
        long epochSec3 = new DateTimeOffset(data[2].Value).ToUnixTimeSeconds();

        using Series epochDays = s.Dt.Epoch(TimeUnit.Day);
        Assert.Equal(DataType.Int32, epochDays.DataType);
        Assert.Equal([
            0,
            1,
            (int)(epochSec3 / 86400), 
            null
        ], epochDays.ToArray<int?>());

        using Series epochSeconds = s.Dt.Epoch(TimeUnit.Second);
        Assert.Equal(DataType.Int64, epochSeconds.DataType);
        Assert.Equal([
            epochSec1,
            epochSec2,
            epochSec3,
            null
        ], epochSeconds.ToArray<long?>());

        using Series epochMilli = s.Dt.Epoch(TimeUnit.Milliseconds);
        Assert.Equal([
            epochSec1 * 1_000L,
            epochSec2 * 1_000L,
            epochSec3 * 1_000L,
            null
        ], epochMilli.ToArray<long?>());

        using Series epochMicro = s.Dt.Epoch(); 
        Assert.Equal([
            epochSec1 * 1_000_000L,
            epochSec2 * 1_000_000L,
            epochSec3 * 1_000_000L,
            null
        ], epochMicro.ToArray<long?>());

        using Series epochNano = s.Dt.Epoch(TimeUnit.Nanoseconds);
        Assert.Equal([
            epochSec1 * 1_000_000_000L,
            epochSec2 * 1_000_000_000L,
            epochSec3 * 1_000_000_000L,
            null
        ], epochNano.ToArray<long?>());
    }
}