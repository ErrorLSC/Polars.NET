using Polars.CSharp.Linq;

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
}
