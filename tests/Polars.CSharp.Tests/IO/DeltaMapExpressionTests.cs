#nullable enable
using Kaminari.Seseragi.Lakehouse.Delta;
using Polars.NET.Core.Exceptions;

namespace Polars.CSharp.Tests;

/// <summary>
/// Spark expressions over map columns in Delta tables, as Spark 4.2 and Delta 4.4 take them: m['k'] and the map
/// functions in CHECK constraints (a missing key is null, which violates), element_at / size in generated columns
/// (where Delta refuses a subscript), maps compared refused; a violation shows a map as Spark shows one.
/// </summary>
public class DeltaMapExpressionTests
{
    public sealed record Scored(int Id, Dictionary<string, int>? M);
    public sealed record WithFirst(int Id, Dictionary<string, int>? M, int? K, int? N, List<string>? Keys);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_map_expr_{name}_{Guid.NewGuid():N}");
        try
        {
            body(path);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    private static void Write<T>(string path, IEnumerable<T> rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
    }

    private static DeltaConstraintViolationException Violation(Action write) =>
        Assert.IsType<DeltaConstraintViolationException>(Assert.Throws<PolarsException>(write).InnerException);

    [Fact]
    public void A_check_reads_a_maps_value_and_a_missing_key_violates() => InTable("check", path =>
    {
        Write(path, [new Scored(1, new() { ["k"] = 5 })]);
        Delta.AddConstraint(path, "pos", "M['k'] > 0 AND size(M) < 3 AND map_contains_key(M, 'k')");
        Write(path, [new Scored(2, new() { ["k"] = 1, ["j"] = 2 })]);

        var negative = Violation(() => Write(path, [new Scored(3, new() { ["k"] = -1, ["j"] = 2 })]));
        Assert.Equal("{k -> -1, j -> 2}", negative.Values.Single(v => v.Key == "M").Value);
        // The key missing: M['k'] is null, which Spark counts as a violation.
        Violation(() => Write(path, [new Scored(4, new() { ["j"] = 2 })]));
        Violation(() => Write(path, [new Scored(5, null)]));

        using var read = DataFrame.ReadDelta(path).Sort("Id");
        Assert.Equal([1, 2], read["Id"].ToArray<int>());
    });

    [Fact]
    public void A_generated_column_takes_element_at_and_size_but_no_subscript() => InTable("generated", path =>
    {
        using var schema = PolarsSchema.From<WithFirst>();
        Delta.CreateTable(path, schema, generatedColumns: [new("K", "element_at(M, 'k')"), new("N", "size(M)"), new("Keys", "map_keys(M)")]);
        Write(path, [new Scored(1, new() { ["k"] = 7, ["j"] = 1 }), new Scored(2, new() { ["j"] = 1 }), new Scored(3, null)]);

        using var read = DataFrame.ReadDelta(path).Sort("Id");
        Assert.Equal([7, null, null], read["K"].ToArray<int?>());
        Assert.Equal([2, 1, null], read["N"].ToArray<int?>());
        Assert.Equal([["k", "j"], ["j"], null], read["Keys"].ToArray<List<string>?>());

        string other = path + "_subscript";
        try
        {
            var error = Assert.ThrowsAny<Exception>(() => Delta.CreateTable(other, schema, generatedColumns: [new("K", "M['k']")]));
            Assert.Contains("generated column", error.Message);
        }
        finally
        {
            if (Directory.Exists(other)) Directory.Delete(other, true);
        }
    });

    [Fact]
    public void Maps_are_not_compared_and_map_builds_one() => InTable("compare", path =>
    {
        Write(path, [new Scored(1, new() { ["k"] = 5 })]);
        var compared = Assert.ThrowsAny<Exception>(() => Delta.AddConstraint(path, "same", "M = map('k', 5)"));
        Assert.Contains("cannot be compared", compared.Message);
        Assert.Contains("cannot be compared", Assert.ThrowsAny<Exception>(() => Delta.AddConstraint(path, "in", "M IN (map('k', 5))")).Message);
        // Computed keys could be equal in a row, where Spark fails the write: refused.
        Assert.Contains("literal keys", Assert.ThrowsAny<Exception>(() => Delta.AddConstraint(path, "computed", "map(cast(Id AS STRING), 1, 'x', 2)['x'] = 2")).Message);
        var repeated = Assert.ThrowsAny<Exception>(() => Delta.AddConstraint(path, "dup", "map('a', 1, 'a', 2)['a'] = 1"));
        Assert.Contains("DUPLICATED_MAP_KEY", repeated.Message);

        Delta.AddConstraint(path, "built", "map('a', Id, 'b', 2)['a'] = Id");
        Write(path, [new Scored(2, new() { ["k"] = 1 })]);
    });
}
