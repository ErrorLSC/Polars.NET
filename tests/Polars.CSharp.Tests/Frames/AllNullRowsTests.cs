namespace Polars.CSharp.Tests;

/// <summary>
/// A frame's row whose mapped columns are all null is an object of nulls (rows are data); only a query's results fold
/// such objects to null, as an outer join leaves a missing side.
/// </summary>
public class AllNullRowsTests
{
    [Fact]
    public void ARowOfNulls_IsAnObjectOfNulls_NestedOnesToo()
    {
        Sparse[] rows = [new(1, "a", new(2, "b")), new(null, null, null), new(null, null, new(null, null))];
        using var df = DataFrame.From(rows);
        Sparse[] read = df.Rows<Sparse>().ToArray();
        Assert.All(read, Assert.NotNull);
        Assert.Equal(rows, read);
    }

    public sealed record Part(int? A, string B);

    public sealed record Sparse(int? Id, string Name, Part Nested);
}
