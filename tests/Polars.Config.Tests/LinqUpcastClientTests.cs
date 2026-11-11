using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public record UcRow(int Id, string Name);

/// <summary>
/// A client step after an upcast reads the actual elements, not empty instances of the upcast type.
/// </summary>
public class LinqUpcastClientTests
{
    private static readonly UcRow[] Rows = [new(1, "a"), new(2, "bb"), new(3, "ccc")];

    private static bool IsLong(object value) => value.ToString()!.Length > 1;

    [Fact]
    public void Client_Predicate_After_An_Upcast()
    {
        using var _ = Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope();
        using var df = DataFrame.FromRows(Rows);

        Assert.Equal(
            Rows.Select(r => (object)r.Name).Where(IsLong).ToList(),
            [.. df.AsQueryable<UcRow>().Select(r => (object)r.Name).Where(x => IsLong(x))]);
    }
}
