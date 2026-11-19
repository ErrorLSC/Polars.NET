namespace Polars.CSharp.Tests;

/// <summary>
/// Struct columns through every way rows enter and leave the engine, again and again with collections in between: a
/// buffer freed twice or used after it is freed shows up as a crash or as values that changed.
/// </summary>
public class StructRoundTripStressTests
{
    public sealed record Leaf(int A, string B, Int128 Big);

    public sealed record Branch(Leaf One, List<Leaf> Many, Dictionary<string, Leaf> ByName);

    public sealed record Tree(long Id, Branch Branch, List<Branch> Branches);

    private static Tree[] Trees(int count) => [.. Enumerable.Range(0, count).Select(i => new Tree(i,
        i % 5 == 0 ? null : new Branch(new Leaf(i, i % 3 == 0 ? null : $"b{i}", (Int128)i << 90), [new Leaf(-i, "m", -i), null],
            i % 4 == 0 ? null : new Dictionary<string, Leaf> { ["k"] = new Leaf(i, "v", i), ["z"] = null }),
        i % 7 == 0 ? null : [null, new Branch(new Leaf(i, null, 0), null, null)]))]; // an all-null record reads back as null (the row mapper folds it)

    [Fact]
    public void StructColumns_SurviveRepeatedRoundTrips()
    {
        Tree[] trees = Trees(3000);
        for (int round = 0; round < 30; round++)
        {
            using (var df = DataFrame.From(trees))
            {
                Tree[] back = [.. df.Rows<Tree>()];
                Assert.Equal(Show(trees), Show(back));
                using var again = DataFrame.From(back);
                Assert.True(df.Equals(again));
                using var mapped = df.MapRows<Tree, Tree>(t => t);
                Assert.True(df.Equals(mapped));
            }
            using (var streamed = DataFrame.FromEnumerable(trees, batchSize: 700))
            using (var scanned = LazyFrame.ScanEnumerable(trees, batchSize: 500).Collect())
                Assert.True(streamed.Equals(scanned));
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static string Show(Tree[] trees) => string.Join("\n", trees.Select(t =>
        $"{t.Id}|{Show(t.Branch)}|{(t.Branches is null ? "null" : string.Join(";", t.Branches.Select(Show)))}"));

    private static string Show(Branch b) => b is null ? "null"
        : $"{Show(b.One)}/{(b.Many is null ? "null" : string.Join(",", b.Many.Select(Show)))}/{(b.ByName is null ? "null" : string.Join(",", b.ByName.OrderBy(e => e.Key).Select(e => e.Key + "=" + Show(e.Value))))}";

    private static string Show(Leaf l) => l is null ? "null" : $"{l.A}:{l.B ?? "~"}:{l.Big}";
}
