using System.Text;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// A column mixing kinds is inferred by one rule, for a file and (C3) across a dataset's files: integers with floats are
/// floats, dates with naive datetimes of the same day-month-year order are datetimes (a date read as its midnight),
/// every other mix is a string (nothing guessed: no zone given to naive text, no date given to a time, no serial number
/// read as a date).
/// </summary>
public class CsvInferenceMixTests
{
    [Theory]
    [InlineData("2024-01-02\n2024-01-02 03:04:05", "t:datetime[μs] | 2024-01-02T00:00:00 | 2024-01-02T03:04:05")]
    [InlineData("2024-01-02 03:04:05\n2024-01-02", "t:datetime[μs] | 2024-01-02T03:04:05 | 2024-01-02T00:00:00")]
    [InlineData("02/01/2024\n02/01/2024 03:04", "t:datetime[μs] | 2024-01-02T00:00:00 | 2024-01-02T03:04:00")]
    [InlineData("2024-01-02\n2024-01-02T03:04:05.123456789", "t:datetime[ns] | 2024-01-02T00:00:00 | 2024-01-02T03:04:05.1234567")] // the renderer shows ticks
    [InlineData("1\n1.5", "t:f64 | 1 | 1.5")]
    public void LosslessMixes_TakeTheWiderType(string values, string expected) => Assert.Equal(expected, Read(values));

    [Theory]
    [InlineData("02/01/2024\n2024-01-02 03:04:05")]           // dates and datetimes in different orders
    [InlineData("2024-01-02\n2024-01-02T03:04:05Z")]          // a date has no zone: UTC midnight would be a guess
    [InlineData("2024-01-02 03:04:05\n2024-01-02T03:04:05Z")] // naive with zoned
    [InlineData("1\ntrue")]
    [InlineData("2024-01-02\n45292")]                         // an Excel serial number is not read as a date
    [InlineData("03:04:05\n2024-01-02")]
    [InlineData("03:04:05\n2024-01-02 03:04:05")]
    [InlineData("1.5\nfalse")]
    [InlineData("true\n2024-01-02")]
    public void OtherMixes_AreStrings(string values)
    {
        string got = Read(values);
        Assert.StartsWith("t:str |", got);
        Assert.Equal(values.Split('\n').Select(v => $"\"{v}\""), got.Split(" | ")[1..]);
    }

    /// <summary>Units decoded side by side before the column's format is known read the dates of a datetime column alike.</summary>
    [Fact]
    public void DatesOfADatetimeColumn_ReadAlikeInAnyUnits()
    {
        var text = new StringBuilder("t\n2024-01-02\n2024-01-03 04:05:06\n");
        for (int i = 0; i < 400; i++) text.Append(i % 3 == 0 ? $"2024-02-{1 + i % 28:D2}\n" : $"2024-02-{1 + i % 28:D2} 01:02:03\n");
        byte[] bytes = Encoding.UTF8.GetBytes(text.ToString());
        string whole = CsvBehaviourTests.Read(bytes, new CsvReadOptions { MaxDegreeOfParallelism = 1 }, CsvBehaviourTests.CsvSource.Memory);
        Assert.StartsWith("t:datetime[μs] |", whole);
        foreach (int unit in (int[])[1, 7, 64])
            Assert.Equal(whole, CsvBehaviourTests.Read(bytes, new CsvReadOptions { UnitBytes = unit, MaxDegreeOfParallelism = 8, InferSchemaLength = 2 },
                CsvBehaviourTests.CsvSource.Memory));
    }

    private static string Read(string values) =>
        CsvBehaviourTests.Read(Encoding.UTF8.GetBytes("t\n" + values + "\n"), new CsvReadOptions(), CsvBehaviourTests.CsvSource.Memory);
}
