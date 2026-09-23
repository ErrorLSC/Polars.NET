using Polars.CSharp.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public record Employee(string Name, int Age, int Salary);

public class LinqTests
{
    [Fact]
    [Trait("LINQ", "WhereAndTake")]
    public void Test_Linq_Where_Take()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [25, 17, 30, 15]),
            Series.From("Salary", [5000, 3000, 7000, 2000])
        ]);

        var threshold = 18;
        var query = df.AsQueryable<Employee>()
                      .Where(e => e.Age >= threshold && e.Salary > 4000)
                      .Take(10)
                      .ToList();

        Assert.Equal(2, query.Count);
        Assert.Equal("Alice", query[0].Name);
        Assert.Equal("Charlie", query[1].Name);
    }
    private static bool CustomMagicAudit(string name, int salary)
    {
        // Polars expr cannot translate string length modular arithmetic or arbitrary managed code
        return name.Length % 2 != 0 && (salary ^ 0x5A) > 4000;
    }

    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_WithUnsupportedCSharpMethod()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [25, 17, 30, 15]),
            Series.From("Salary", [5000, 3000, 7000, 2000])
        ]);

        // 1. Where(Age >= 18) should push down to Polars Native (Filtering Bob and David out)
        // 2. Where(CustomMagicAudit) must gracefully fallback to client-side cursor evaluation
        var query = df.AsQueryable<Employee>()
                      .Where(e => e.Age >= 18)
                      .Where(e => CustomMagicAudit(e.Name, e.Salary))
                      .ToList();

        // Qualified adults:
        // Alice: Name.Length = 5 (odd), (5000 ^ 90) > 4000 -> True
        // Charlie: Name.Length = 7 (odd), (7000 ^ 90) > 4000 -> True
        // Eve: Name.Length = 3 (odd), (6200 ^ 90) > 4000 -> True
        Assert.Equal(3, query.Count);
        Assert.All(query, e => Assert.True(CustomMagicAudit(e.Name, e.Salary)));
    }
    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_ToDataFrame_And_ToLazyFrame()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [25, 17, 30, 15]),
            Series.From("Salary", [5000, 3000, 7000, 2000])
        ]);

        // Even with client fallback, ToDataFrame() must re-transpose back into a native columnar DataFrame
        using DataFrame dfResult = df.AsQueryable<Employee>()
                                     .Where(e => e.Age >= 18)
                                     .Where(e => CustomMagicAudit(e.Name, e.Salary))
                                     .ToDataFrame();

        Assert.Equal(3, dfResult.Height);
        Assert.Equal(3, dfResult.Width);
        Assert.True(dfResult.ColumnNames.SequenceEqual(["Name", "Age", "Salary"]));

        // Verify that continuing from ToLazyFrame works natively
        using LazyFrame lfResumed = df.AsQueryable<Employee>()
                                      .Where(e => e.Age >= 18)
                                      .Where(e => CustomMagicAudit(e.Name, e.Salary))
                                      .ToLazyFrame();

        using DataFrame dfCollected = lfResumed.Collect();
        Assert.Equal(3, dfCollected.Height);
    }
}
