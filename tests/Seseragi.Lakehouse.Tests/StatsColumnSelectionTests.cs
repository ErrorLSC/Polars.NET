using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Which columns get statistics, checked against what delta-spark 4.4 writes for the same table properties.</summary>
public class StatsColumnSelectionTests
{
    // a INT, s STRUCT<x INT, y STRING>, b STRING, c DOUBLE, arr ARRAY<INT>, d DATE
    private static readonly StructField[] Fields =
    [.. StructType.Parse("""
        {"type":"struct","fields":[
          {"name":"a","type":"integer","nullable":true,"metadata":{}},
          {"name":"s","type":{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}},{"name":"y","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},
          {"name":"b","type":"string","nullable":true,"metadata":{}},
          {"name":"c","type":"double","nullable":true,"metadata":{}},
          {"name":"arr","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":true,"metadata":{}},
          {"name":"d","type":"date","nullable":true,"metadata":{}}]}
        """).Fields];

    private static string[] Selected(params (string Key, string Value)[] properties) =>
        [.. FileStatistics.IndexedLeaves(Fields, properties.ToDictionary(p => p.Key, p => p.Value)).Select(p => string.Join('.', p.Select(f => f.Name)))];

    [Fact]
    public void LeadingLeaves_AreCountedAsSpark()
    {
        Assert.Equal(["a", "s.x", "s.y", "b", "c", "d"], Selected()); // 32 by default; arr counted, no statistics written for it
        Assert.Equal(["a"], Selected((FileStatistics.NumIndexedColsKey, "1")));
        Assert.Equal(["a", "s.x"], Selected((FileStatistics.NumIndexedColsKey, "2")));
        Assert.Equal(["a", "s.x", "s.y"], Selected((FileStatistics.NumIndexedColsKey, "3")));
        Assert.Equal(["a", "s.x", "s.y", "b"], Selected((FileStatistics.NumIndexedColsKey, "4")));
        Assert.Equal(["a", "s.x", "s.y", "b", "c"], Selected((FileStatistics.NumIndexedColsKey, "6"))); // arr is the 6th leaf
        Assert.Empty(Selected((FileStatistics.NumIndexedColsKey, "0")));
        Assert.Equal(["a", "s.x", "s.y", "b", "c", "d"], Selected((FileStatistics.NumIndexedColsKey, "-1")));
    }

    [Fact]
    public void NamedColumns_TakePrecedence()
    {
        Assert.Equal(["s.y", "c"], Selected((FileStatistics.StatsColumnsKey, "c,s.y")));
        Assert.Equal(["d"], Selected((FileStatistics.StatsColumnsKey, "d"), (FileStatistics.NumIndexedColsKey, "1")));
        Assert.Equal(["b", "c"], Selected((FileStatistics.StatsColumnsKey, " c , `b` ")));
        Assert.Equal(["s.x", "s.y"], Selected((FileStatistics.StatsColumnsKey, "s")));
        Assert.Equal(["s.x"], Selected((FileStatistics.StatsColumnsKey, "`s`.`x`")));
        // Set but empty (what Spark leaves after dropping the only named column): no column statistics at all.
        Assert.Empty(Selected((FileStatistics.StatsColumnsKey, "")));
        Assert.Empty(Selected((FileStatistics.StatsColumnsKey, ""), (FileStatistics.NumIndexedColsKey, "5")));
    }

    [Fact]
    public void FooterStatistics_CoverOnlyTheSelectedColumns()
    {
        // nullCount and bounds follow the selection; numRecords is always there.
        Assert.Empty(FileStatistics.IndexedLeaves(Fields, new Dictionary<string, string> { [FileStatistics.NumIndexedColsKey] = "0" }));
    }
}
