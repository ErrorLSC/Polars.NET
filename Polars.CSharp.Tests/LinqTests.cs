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
    /// <summary>
    /// External static lookup table simulating a pure managed security/audit cache.
    /// This cannot be translated to any Polars native ExprHandle, guaranteeing a client fallback.
    /// </summary>
    private static readonly HashSet<string> ApprovedStaffRegistry = new(StringComparer.OrdinalIgnoreCase)
    {
        "Alice", "Charlie"
    };

    /// <summary>
    /// Custom business method checking managed state that Polars native engine cannot translate.
    /// </summary>
    private static bool CheckSecurityClearance(string name, int salary)
    {
        // Polars expr cannot execute an in-memory HashSet.Contains or arbitrary C# control flow
        return ApprovedStaffRegistry.Contains(name) && salary > 4500;
    }

    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_WithUnsupportedCSharpMethod()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("Age", [25, 17, 30, 15, 28]),
            Series.From("Salary", [5000, 3000, 7000, 2000, 6200])
        ]);

        // 1. Where(Age >= 18) should push down to Polars Native (Filtering Bob and David out)
        // 2. Where(CustomMagicAudit) must gracefully fallback to client-side cursor evaluation
        var query = df.AsQueryable<Employee>()
                      .Where(e => e.Age >= 18)
                      .Where(e => CheckSecurityClearance(e.Name, e.Salary))
                      .ToList();
        
        // Qualified:
        // Alice: Age 25 >= 18, in approved set, salary 5000 > 4500 -> True
        // Charlie: Age 30 >= 18, in approved set, salary 7000 > 4500 -> True
        // Eve: Age 28 >= 18, NOT in approved set -> False
        Assert.Equal(2, query.Count);
        Assert.Equal("Alice", query[0].Name);
        Assert.Equal("Charlie", query[1].Name);
    }
    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_ToDataFrame_And_ToLazyFrame()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("Age", [25, 17, 30, 15, 28]),
            Series.From("Salary", [5000, 3000, 7000, 2000, 6200])
        ]);

        // back into a native columnar DataFrame via DataFrameBuilder.FromRows<T>()
        using DataFrame dfResult = df.AsQueryable<Employee>()
                                     .Where(e => e.Age >= 18)
                                     .Where(e => CheckSecurityClearance(e.Name, e.Salary))
                                     .ToDataFrame();

        Assert.Equal(2, dfResult.Height);
        Assert.Equal(3, dfResult.Width);
        Assert.True(dfResult.ColumnNames.SequenceEqual(["Name", "Age", "Salary"]));

        // Verify that continuing from ToLazyFrame works natively on top of the transposed DataFrame
        using LazyFrame lfResumed = df.AsQueryable<Employee>()
                                      .Where(e => e.Age >= 18)
                                      .Where(e => CheckSecurityClearance(e.Name, e.Salary))
                                      .ToLazyFrame();

        using DataFrame dfCollected = lfResumed.Collect();
        Assert.Equal(2, dfCollected.Height);
    }
}
