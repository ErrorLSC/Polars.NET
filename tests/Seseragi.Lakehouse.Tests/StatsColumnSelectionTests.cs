using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Which columns get statistics, checked against what delta-spark 4.1 writes for the same table properties.</summary>
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
        [.. FileStatistics.IndexedFields(Fields, properties.ToDictionary(p => p.Key, p => p.Value)).Select(f => f.Name)];

    [Fact]
    public void LeadingLeaves_AreCountedAsSpark()
    {
        Assert.Equal(["a", "b", "c", "d"], Selected()); // 32 by default
        Assert.Equal(["a"], Selected((FileStatistics.NumIndexedColsKey, "1")));
        Assert.Equal(["a"], Selected((FileStatistics.NumIndexedColsKey, "2")));   // Spark: a, s.x
        Assert.Equal(["a"], Selected((FileStatistics.NumIndexedColsKey, "3")));   // Spark: a, s.x, s.y
        Assert.Equal(["a", "b"], Selected((FileStatistics.NumIndexedColsKey, "4")));
        Assert.Equal(["a", "b", "c"], Selected((FileStatistics.NumIndexedColsKey, "6"))); // arr is the 6th leaf
        Assert.Empty(Selected((FileStatistics.NumIndexedColsKey, "0")));
        Assert.Equal(["a", "b", "c", "d"], Selected((FileStatistics.NumIndexedColsKey, "-1")));
    }

    [Fact]
    public void NamedColumns_TakePrecedence()
    {
        Assert.Equal(["c"], Selected((FileStatistics.StatsColumnsKey, "c,s.y")));        // Spark: s.y, c
        Assert.Equal(["d"], Selected((FileStatistics.StatsColumnsKey, "d"), (FileStatistics.NumIndexedColsKey, "1")));
        Assert.Equal(["b", "c"], Selected((FileStatistics.StatsColumnsKey, " c , `b` ")));
        Assert.Empty(Selected((FileStatistics.StatsColumnsKey, "s")));                   // Spark: s.x, s.y
    }

    [Fact]
    public void FooterStatistics_CoverOnlyTheSelectedColumns()
    {
        // nullCount and bounds follow the selection; numRecords is always there.
        Assert.Empty(FileStatistics.IndexedFields(Fields, new Dictionary<string, string> { [FileStatistics.NumIndexedColsKey] = "0" }));
    }
}
