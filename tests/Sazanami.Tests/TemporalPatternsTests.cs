using System.Text;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ValueInference"/> and <see cref="TemporalPatterns"/> against Polars' native CSV reader (0.55): a sample of
/// the differential corpus with the engine's answers (TestData/temporal/native.tsv), the lenient chrono forms, the
/// regex gate, columns keeping their pattern group, and the decided deviations (finer than the unit, out of range, times).
/// </summary>
public class TemporalPatternsTests
{
    private static readonly DateTime Epoch = DateTime.UnixEpoch;

    private static long Micros(DateTime t) => (t - Epoch).Ticks / 10;

    private static int Days(int year, int month, int day) => new DateOnly(year, month, day).DayNumber - DateOnly.FromDateTime(Epoch).DayNumber;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Every cell of the native table, inferred kinds and values at every unit, equals ours, except the decided deviations,
    /// each of which the sample exercises: a fraction finer than the unit is TooFine (native truncates), an inferred
    /// datetime needing nanoseconds is datetime[ns] (native: μs), a fraction past nanoseconds infers as a string, and a
    /// datetime[ns] outside its range is OutOfRange (native wraps).
    /// </summary>
    [Fact]
    public void NativeTable()
    {
        Dictionary<string, int> deviations = [];
        int rows = 0;
        foreach (string line in File.ReadLines(Path.Combine(AppContext.BaseDirectory, "TestData", "temporal", "native.tsv")))
        {
            if (line.StartsWith('#')) continue;
            string[] native = line.Split('\t');
            byte[] value = Convert.FromHexString(native[0]);
            string[] ours = Ours(value);
            rows++;
            for (int i = 1; i < native.Length; i++)
            {
                string expected = native[i], actual = ours[i - 1];
                if (expected == actual || (i == 7 && expected == "-")) continue;
                string? deviation = (i, expected, actual) switch
                {
                    (1 or 2, _, _) when expected.StartsWith("datetime[μs") && actual == expected.Replace("μs", "ns") => "ns unit",
                    (1 or 2, _, "str") when expected.StartsWith("datetime") || expected == "time" => "past ns",
                    (_, _, _) when actual == $"TOOFINE:{expected}" => "too fine",
                    (6, _, "RANGE") => "ns range",
                    _ => null,
                };
                Assert.True(deviation is not null, $"'{Encoding.UTF8.GetString(value)}' column {i}: native {expected}, ours {actual}");
                deviations[deviation!] = deviations.GetValueOrDefault(deviation!) + 1;
            }
        }
        Assert.True(rows > 2900);
        Assert.All(["ns unit", "past ns", "too fine", "ns range"], d => Assert.True(deviations.GetValueOrDefault(d) > 0, d));
    }

    /// <summary>The cells of a native table row, ours: inferred kind (plain, decimal comma), date, datetime μs / ms / ns, time.</summary>
    private static string[] Ours(byte[] value)
    {
        TemporalPattern? group = TemporalPatterns.Infer(value);
        string date = group is TemporalPattern.DateDmy or TemporalPattern.DateYmd && TemporalPatterns.ParseDate(value, group.Value, out int days) == TemporalOutcome.Parsed
            ? days.ToString() : "null";
        string time = group == TemporalPattern.Time ? Outcome(TemporalPatterns.ParseTime(value, out long t), t) : "-";
        return [Kind(value, false), Kind(value, true), date, Datetime(TimeUnit.Microsecond), Datetime(TimeUnit.Millisecond), Datetime(TimeUnit.Nanosecond), time];

        string Datetime(TimeUnit unit) =>
            group is null or TemporalPattern.Time ? "null" : Outcome(TemporalPatterns.ParseDatetime(value, group.Value, unit, out long v), v);

        static string Outcome(TemporalOutcome outcome, long v) => outcome switch
        {
            TemporalOutcome.Parsed => v.ToString(),
            TemporalOutcome.TooFine => $"TOOFINE:{v}",
            TemporalOutcome.OutOfRange => "RANGE",
            _ => "null",
        };
    }

    private static string Kind(byte[] value, bool decimalComma) => ValueInference.Infer(value, true, decimalComma, out bool ns) switch
    {
        TextKind.Boolean => "bool",
        TextKind.Int64 => "i64",
        TextKind.Int128 => "i128",
        TextKind.Float64 => "f64",
        TextKind.Date => "date",
        TextKind.Datetime => ns ? "datetime[ns]" : "datetime[μs]",
        TextKind.DatetimeUtc => ns ? "datetime[ns, UTC]" : "datetime[μs, UTC]",
        TextKind.Time => "time",
        _ => "str",
    };

    [Theory]
    [InlineData("2021-01-15", nameof(TextKind.Date))]
    [InlineData("15.01.2021", nameof(TextKind.Date))]
    [InlineData(" 2021-01-15", nameof(TextKind.Date))] // numbers skip leading whitespace
    [InlineData(" 2021-01-15", nameof(TextKind.Date))] // Unicode whitespace too
    [InlineData("2021- 01-15", nameof(TextKind.Date))]
    [InlineData("2021 -01-15", nameof(TextKind.String))] // a literal does not
    [InlineData("2021-01-15 ", nameof(TextKind.String))]
    [InlineData("21-01-01", nameof(TextKind.Date))] // 1 to 4 digit years: 0021-01-01
    [InlineData("1.5.5", nameof(TextKind.Date))]
    [InlineData("99999-01-01", nameof(TextKind.String))]
    [InlineData("+12345-01-01", nameof(TextKind.Date))] // signed years take any count of digits
    [InlineData("2021-02-29", nameof(TextKind.String))]
    [InlineData("0000-02-29", nameof(TextKind.Date))] // proleptic Gregorian, year 0 is a leap year
    [InlineData("2021-01-1510:00", nameof(TextKind.Datetime))] // a pattern space matches no whitespace too
    [InlineData("2021-01-15t10:00:00", nameof(TextKind.String))]
    [InlineData("2021-01-15T10:00:60", nameof(TextKind.Datetime))] // leap second
    [InlineData("2021-01-15T24:00:00", nameof(TextKind.String))]
    [InlineData("20210115T100000", nameof(TextKind.Datetime))]
    [InlineData("2021-01-15T10:00:00+01", nameof(TextKind.DatetimeUtc))]
    [InlineData("2021-01-15T10:00:00−01:00", nameof(TextKind.DatetimeUtc))] // MINUS SIGN
    [InlineData("2021-01-15T10:00:00+25:00", nameof(TextKind.DatetimeUtc))] // inference ignores the offset's range
    [InlineData("2021-01-15T10:00:00+01:60", nameof(TextKind.String))]
    [InlineData("2021-01-15T10:00:00UTC", nameof(TextKind.DatetimeUtc))] // %+
    [InlineData("1:2", nameof(TextKind.Time))]
    [InlineData("24:00", nameof(TextKind.String))]
    [InlineData("10:00:00.1234567891", nameof(TextKind.String))] // finer than nanoseconds (native: time, truncated)
    [InlineData("20210115", nameof(TextKind.Int64))]
    [InlineData("１２３", nameof(TextKind.Int128))] // Unicode \d: no Int64, so Int128
    [InlineData("123456789012345678901234567890123456789012345", nameof(TextKind.Int128))] // whatever its size
    [InlineData("+1", nameof(TextKind.String))]
    [InlineData("1.５", nameof(TextKind.Float64))]
    [InlineData("inf", nameof(TextKind.Float64))]
    [InlineData("Inf", nameof(TextKind.String))]
    [InlineData("NaN", nameof(TextKind.Float64))]
    [InlineData("nan", nameof(TextKind.String))]
    [InlineData("5.", nameof(TextKind.Float64))]
    [InlineData("1.e5", nameof(TextKind.String))]
    [InlineData("falſe", nameof(TextKind.Boolean))] // Unicode case folding: 'ſ' is an 's'
    [InlineData("ＴＲＵＥ", nameof(TextKind.String))]
    [InlineData("t", nameof(TextKind.String))]
    public void Infers(string text, string kind) => Assert.Equal(Enum.Parse<TextKind>(kind), ValueInference.Infer(U(text), true, false, out _));

    [Theory]
    [InlineData("1,5", nameof(TextKind.Float64))]
    [InlineData("5,", nameof(TextKind.Float64))]
    [InlineData("1.5", nameof(TextKind.String))]
    [InlineData("1.234,56", nameof(TextKind.String))]
    public void Infers_DecimalComma(string text, string kind) => Assert.Equal(Enum.Parse<TextKind>(kind), ValueInference.Infer(U(text), true, true, out _));

    [Fact]
    public void Infers_WithoutDates_AndQuoted()
    {
        Assert.Equal(TextKind.String, ValueInference.Infer(U("2021-01-15"), false, false, out _));
        Assert.Equal(TextKind.Int64, ValueInference.Infer(U("15"), false, false, out _));
        Assert.Equal(TextKind.String, ValueInference.Infer(U("\"15\""), true, false, out _)); // quoted: temporal or string
        Assert.Equal(TextKind.Date, ValueInference.Infer(U("\"2021-01-15\""), true, false, out _));
        Assert.Equal(TextKind.Datetime, ValueInference.Infer(U("2021-01-15T10:00:00.123456789"), true, false, out bool ns));
        Assert.True(ns);
        Assert.Equal(TextKind.Datetime, ValueInference.Infer(U("2021-01-15T10:00:00.123456000"), true, false, out ns));
        Assert.False(ns);
    }

    [Theory]
    [InlineData("2021-01-15T10:00:00", nameof(TemporalPattern.DatetimeYmd), 2021, 1, 15, 10, 0, 0)]
    [InlineData("2021-01-15 10:00", nameof(TemporalPattern.DatetimeYmd), 2021, 1, 15, 10, 0, 0)]
    [InlineData("2021-01-15", nameof(TemporalPattern.DateYmd), 2021, 1, 15, 0, 0, 0)]
    [InlineData("2021/1/5 10:00", nameof(TemporalPattern.DatetimeYmd), 2021, 1, 5, 10, 0, 0)]
    [InlineData("1.1.2021 1:2", nameof(TemporalPattern.DatetimeDmy), 2021, 1, 1, 1, 2, 0)]
    [InlineData("20210115T100000", nameof(TemporalPattern.DatetimeYmd), 2021, 1, 15, 10, 0, 0)]
    [InlineData("2021-01-15T10:00:60", nameof(TemporalPattern.DatetimeYmd), 2021, 1, 15, 10, 1, 0)] // leap second: one more
    [InlineData("2021-01-15T10:00:00Z", nameof(TemporalPattern.DatetimeYmdZ), 2021, 1, 15, 10, 0, 0)]
    [InlineData("2021-01-15T10:00:00+01:00", nameof(TemporalPattern.DatetimeYmdZ), 2021, 1, 15, 9, 0, 0)]
    [InlineData("2021-01-15T10:00:00 +01:00", nameof(TemporalPattern.DatetimeYmdZ), 2021, 1, 15, 9, 0, 0)] // the group's first pattern skips it
    [InlineData("2021-01-15T10:00:00+01:", nameof(TemporalPattern.DatetimeYmdZ), 2021, 1, 15, 9, 0, 0)]
    [InlineData("2021-01-15T10:00+0100", nameof(TemporalPattern.DatetimeYmdZ), 2021, 1, 15, 9, 0, 0)]
    [InlineData("2021-01-15 10:00:00-05:30", nameof(TemporalPattern.DatetimeYmdZ), 2021, 1, 15, 15, 30, 0)]
    public void ReadsDatetimes(string text, string group, int year, int month, int day, int hour, int minute, int second)
    {
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U(text), Enum.Parse<TemporalPattern>(group), TimeUnit.Microsecond, out long value));
        Assert.Equal(Micros(new DateTime(year, month, day, hour, minute, second)), value);
    }

    /// <summary>
    /// Past the group's first pattern only what the engine's regex gate admits reads: 4+ digit years, 'T' or a space, no
    /// whitespace, Z or ±hh[[:]mm]; and the month the gate's first match captures must be 1 to 12.
    /// </summary>
    [Theory]
    [InlineData("2021-01-1510:00", nameof(TemporalPattern.DatetimeYmd))]
    [InlineData("2021-01-15  10:00", nameof(TemporalPattern.DatetimeYmd))]
    [InlineData(" 2021-01-15", nameof(TemporalPattern.DatetimeYmd))]
    [InlineData("21-01-01", nameof(TemporalPattern.DatetimeDmy))]
    [InlineData("20210105T100000", nameof(TemporalPattern.DatetimeYmd))] // the first match captures month "0"
    [InlineData("20211005T100000", nameof(TemporalPattern.DatetimeYmd))]
    [InlineData("2021-01-15 10:00:00.1234567891", nameof(TemporalPattern.DatetimeYmd))] // the gate takes 1 to 9 fraction digits
    [InlineData("2021-01-15 10:00:00+01:", nameof(TemporalPattern.DatetimeYmdZ))]
    [InlineData("2021-01-15 10:00:00z", nameof(TemporalPattern.DatetimeYmdZ))]
    [InlineData("2021-01-15T10:00:00UTC", nameof(TemporalPattern.DatetimeYmdZ))]
    [InlineData("2021-01-15T10:00:00+25:00", nameof(TemporalPattern.DatetimeYmdZ))] // no such offset
    [InlineData("2021-01-15T10:00:00+", nameof(TemporalPattern.DatetimeYmdZ))] // an offset cut short at the end (fuzz)
    [InlineData("2021-01-15T10:00:00+0", nameof(TemporalPattern.DatetimeYmdZ))]
    [InlineData("2021-01-15 10:00-1", nameof(TemporalPattern.DatetimeYmdZ))]
    [InlineData("2021-01-15T10:00:00.5-", nameof(TemporalPattern.DatetimeYmdZ))]
    [InlineData("2021-01-15T10:00:00Z", nameof(TemporalPattern.DatetimeYmd))] // a naive group takes no offset
    [InlineData("15-01-2021 10:00", nameof(TemporalPattern.DatetimeYmd))]
    [InlineData("2021-01/15 10:00", nameof(TemporalPattern.DatetimeYmd))] // the gate admits mixed separators, no pattern does
    [InlineData("15/01-2021T10:00:00", nameof(TemporalPattern.DatetimeDmy))]
    [InlineData("2021-01/15T10:00:00Z", nameof(TemporalPattern.DatetimeYmdZ))]
    public void RefusesDatetimes(string text, string group) =>
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDatetime(U(text), Enum.Parse<TemporalPattern>(group), TimeUnit.Microsecond, out _));

    /// <summary>A huge field is refused, never deep in the regex gate (it recurses per repeated digit).</summary>
    [Fact]
    public void LongFields_AreRefused()
    {
        byte[] digits = [.. Enumerable.Repeat((byte)'1', 200_000)];
        foreach (TemporalPattern group in (TemporalPattern[])[TemporalPattern.DatetimeDmy, TemporalPattern.DatetimeYmd, TemporalPattern.DatetimeYmdZ])
            Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDatetime(digits, group, TimeUnit.Microsecond, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(digits, TemporalPattern.DateYmd, out _));
        Assert.Equal(TextKind.Int128, ValueInference.Infer(digits, true, false, out _));
        Assert.Equal(TextKind.String, ValueInference.Infer([.. digits, (byte)'-', (byte)'1'], true, false, out _));
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("2021-01-15T10:00:00.123456789+01:00"), TemporalPattern.DatetimeYmdZ, TimeUnit.Nanosecond, out _));
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("2021-01-15 10:00:00.123456789+01:00"), TemporalPattern.DatetimeYmdZ, TimeUnit.Nanosecond, out _));
    }

    [Fact]
    public void FinerThanTheUnit_OrOutOfRange()
    {
        Assert.Equal(TemporalOutcome.TooFine, TemporalPatterns.ParseDatetime(U("2021-01-15T10:00:00.1234567"), TemporalPattern.DatetimeYmd, TimeUnit.Microsecond, out long us));
        Assert.Equal(Micros(new DateTime(2021, 1, 15, 10, 0, 0)) + 123_456, us); // the value without the digits it cannot hold
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("2021-01-15T10:00:00.1234560"), TemporalPattern.DatetimeYmd, TimeUnit.Microsecond, out _));
        Assert.Equal(TemporalOutcome.TooFine, TemporalPatterns.ParseDatetime(U("2021-01-15T10:00:00.5"), TemporalPattern.DatetimeYmd, TimeUnit.Second, out long s));
        Assert.Equal(Micros(new DateTime(2021, 1, 15, 10, 0, 0)) / 1_000_000, s);
        Assert.Equal(TemporalOutcome.TooFine, TemporalPatterns.ParseDatetime(U("2021-01-15T10:00:00.1234567891"), TemporalPattern.DatetimeYmd, TimeUnit.Nanosecond, out long ns));
        Assert.Equal(Micros(new DateTime(2021, 1, 15, 10, 0, 0)) * 1000 + 123_456_789, ns);
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("2021-01-15T10:00:00.1234567890"), TemporalPattern.DatetimeYmd, TimeUnit.Nanosecond, out _));
        Assert.Equal(TemporalOutcome.OutOfRange, TemporalPatterns.ParseDatetime(U("0001-01-01"), TemporalPattern.DateYmd, TimeUnit.Nanosecond, out _));
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("0001-01-01"), TemporalPattern.DateYmd, TimeUnit.Microsecond, out _));
        // The edges of datetime[ns].
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("1677-09-21T00:12:43.145224192"), TemporalPattern.DatetimeYmd, TimeUnit.Nanosecond, out ns));
        Assert.Equal(long.MinValue, ns);
        Assert.Equal(TemporalOutcome.OutOfRange, TemporalPatterns.ParseDatetime(U("1677-09-21T00:12:43.145224191"), TemporalPattern.DatetimeYmd, TimeUnit.Nanosecond, out _));
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDatetime(U("2262-04-11T23:47:16.854775807"), TemporalPattern.DatetimeYmd, TimeUnit.Nanosecond, out ns));
        Assert.Equal(long.MaxValue, ns);
        Assert.Equal(TemporalOutcome.OutOfRange, TemporalPatterns.ParseDatetime(U("2262-04-11T23:47:16.854775808"), TemporalPattern.DatetimeYmd, TimeUnit.Nanosecond, out _));
        Assert.Equal(TemporalOutcome.OutOfRange, TemporalPatterns.ParseDatetime(U("2262-04-11T23:47:16.854775807-00:01"), TemporalPattern.DatetimeYmdZ, TimeUnit.Nanosecond, out _));
    }

    [Fact]
    public void ReadsDates()
    {
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDate(U("15/01/2021"), TemporalPattern.DateDmy, out int days));
        Assert.Equal(Days(2021, 1, 15), days);
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDate(U("-0001-01-01"), TemporalPattern.DateYmd, out days));
        Assert.Equal(Days(1, 1, 1) - 366 - 365, days); // year 0 is a leap year, year −1 is not
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDate(U("+262142-12-31"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(U("+262143-01-01"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDate(U("-262143-01-01"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(U("-262144-12-31"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(U("2021-01-15 10:00"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(U("1900-02-29"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(U("2021-01/15"), TemporalPattern.DateYmd, out _));
        Assert.Equal(TemporalOutcome.Invalid, TemporalPatterns.ParseDate(U("15.01-2021"), TemporalPattern.DateDmy, out _));
        Assert.Equal(TemporalOutcome.Parsed, TemporalPatterns.ParseDate(U("2000-02-29"), TemporalPattern.DateYmd, out _));
    }

    /// <summary>Times: H:M:S[.f] or H:M per value (R4, native: the first value's format only); a leap second is one more.</summary>
    [Theory]
    [InlineData("10:00:00", 36_000_000_000_000)]
    [InlineData("12:00", 43_200_000_000_000)]
    [InlineData("1:2:3.4", 3_723_400_000_000)]
    [InlineData(" 10:00", 36_000_000_000_000)]
    [InlineData("17:00:60", 61_260_000_000_000)]
    [InlineData("23:59:60", -1)]
    [InlineData("24:00", -1)]
    [InlineData("10:00:00 ", -1)]
    [InlineData("10:00:00.", -1)]
    [InlineData("10:00:00Z", -1)]
    public void ReadsTimes(string text, long nanoseconds)
    {
        TemporalOutcome outcome = TemporalPatterns.ParseTime(U(text), out long value);
        Assert.Equal(nanoseconds < 0 ? TemporalOutcome.Invalid : TemporalOutcome.Parsed, outcome);
        if (nanoseconds >= 0) Assert.Equal(nanoseconds, value);
    }

    /// <summary>
    /// A column's group is fixed by the first value that reads (pairs.txt, native): later values of another order are
    /// refused, ambiguous ones read in the column's order, a value that does not read fixes nothing.
    /// </summary>
    [Fact]
    public void Column_KeepsTheGroupOfTheFirstValueThatReads()
    {
        var dates = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Parsed, dates.ReadDate(U("2021-05-06"), out _));
        Assert.Equal(TemporalOutcome.Parsed, dates.ReadDate(U("01-02-03"), out int days));
        Assert.Equal(Days(1, 2, 3), days);
        Assert.Equal(TemporalOutcome.Invalid, dates.ReadDate(U("15-01-2021"), out _));

        dates = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Parsed, dates.ReadDate(U("15-01-2021"), out _));
        Assert.Equal(TemporalOutcome.Parsed, dates.ReadDate(U("01-02-03"), out days));
        Assert.Equal(Days(3, 2, 1), days);
        Assert.Equal(TemporalOutcome.Invalid, dates.ReadDate(U("2021-01-15"), out _));

        dates = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Invalid, dates.ReadDate(U("2021-01-15 10:00"), out _)); // a datetime: no group for a date column
        Assert.Equal(TemporalOutcome.Invalid, dates.ReadDate(U("abc"), out _));
        Assert.Null(dates.Group);
        Assert.Equal(TemporalOutcome.Parsed, dates.ReadDate(U("2021-01-15"), out _));
        Assert.Equal(TemporalPattern.DateYmd, dates.Group);

        var stamps = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Parsed, stamps.ReadDatetime(U("2021-01-15T10:00:00Z"), TimeUnit.Microsecond, out _));
        Assert.Equal(TemporalOutcome.Invalid, stamps.ReadDatetime(U("2021-01-15 10:00"), TimeUnit.Microsecond, out _));

        stamps = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Parsed, stamps.ReadDatetime(U("2021-01-15"), TimeUnit.Microsecond, out _)); // a date: its datetime list
        Assert.Equal(TemporalOutcome.Parsed, stamps.ReadDatetime(U("2021-01-15 10:00"), TimeUnit.Microsecond, out long us));
        Assert.Equal(Micros(new DateTime(2021, 1, 15, 10, 0, 0)), us);
        Assert.Equal(TemporalOutcome.Invalid, stamps.ReadDatetime(U("15-01-2021 10:00"), TimeUnit.Microsecond, out _));

        // Every value at the column's unit (native converts some at μs whatever the unit: 1970-01-19 for datetime[ns]).
        stamps = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Parsed, stamps.ReadDatetime(U("2021-01-15 10:00:00"), TimeUnit.Nanosecond, out _));
        Assert.Equal(TemporalOutcome.Parsed, stamps.ReadDatetime(U("2021-01-15 10:00"), TimeUnit.Nanosecond, out long ns));
        Assert.Equal(Micros(new DateTime(2021, 1, 15, 10, 0, 0)) * 1000, ns);

        stamps = new TemporalColumn();
        Assert.Equal(TemporalOutcome.Invalid, stamps.ReadDatetime(U("10:00"), TimeUnit.Microsecond, out _)); // a time fixes nothing
        Assert.Null(stamps.Group);
        Assert.Equal(TemporalOutcome.Invalid, stamps.ReadDatetime(U("2021-01-1510:00"), TimeUnit.Microsecond, out _)); // infers YMD, reads not
        Assert.Null(stamps.Group);
        Assert.Equal(TemporalOutcome.Parsed, stamps.ReadDatetime(U("15-01-2021 10:00"), TimeUnit.Microsecond, out _));
        Assert.Equal(TemporalPattern.DatetimeDmy, stamps.Group);
    }
}
