using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Delta.Skipping;
using Kaminari.Seseragi.Lakehouse.Skipping;
using P = Kaminari.Seseragi.Lakehouse.Skipping.Predicate;

namespace Seseragi.Lakehouse.Tests;

public class FileFilterTests
{
    private static readonly StructType s_schema = StructType.Parse("""
        {"type":"struct","fields":[
          {"name":"id","type":"long","nullable":true,"metadata":{}},
          {"name":"name","type":"string","nullable":true,"metadata":{}},
          {"name":"price","type":"double","nullable":true,"metadata":{}},
          {"name":"amount","type":"decimal(10,2)","nullable":true,"metadata":{}},
          {"name":"day","type":"date","nullable":true,"metadata":{}},
          {"name":"at","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"flag","type":"boolean","nullable":true,"metadata":{}},
          {"name":"blob","type":"binary","nullable":true,"metadata":{}},
          {"name":"region","type":"string","nullable":true,"metadata":{}},
          {"name":"year","type":"integer","nullable":true,"metadata":{}},
          {"name":"nested","type":{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}
        ]}
        """);

    private static readonly string[] s_partitions = ["region", "year"];

    private static AddAction File(string? stats = null, string? region = "east", string? year = "2024") => new()
    {
        Path = "f.parquet",
        Stats = stats,
        PartitionValues = new Dictionary<string, string?> { ["region"] = region, ["year"] = year },
    };

    // ids 10..20, no nulls except three in name; all of blob-free columns filled.
    private static readonly AddAction s_file = File("""
        {"numRecords":100,
         "minValues":{"id":10,"name":"bob","price":1.5,"amount":2.50,"day":"2024-01-10","at":"2024-01-10T00:00:00.123Z","nested":{"x":5}},
         "maxValues":{"id":20,"name":"dave","price":9.5,"amount":7.25,"day":"2024-01-20","at":"2024-01-20T00:00:00.123Z","nested":{"x":8}},
         "nullCount":{"id":0,"name":3,"price":0,"amount":0,"day":0,"at":0,"flag":100,"nested":{"x":0}}}
        """);

    private static bool Might(Predicate predicate, AddAction? file = null) =>
        new FileFilter(s_schema, s_partitions, predicate).MightMatch(file ?? s_file);

    [Theory]
    [InlineData(CompareOp.Eq, 9L, false)]
    [InlineData(CompareOp.Eq, 10L, true)]
    [InlineData(CompareOp.Eq, 21L, false)]
    [InlineData(CompareOp.Lt, 10L, false)]
    [InlineData(CompareOp.Lt, 11L, true)]
    [InlineData(CompareOp.Le, 9L, false)]
    [InlineData(CompareOp.Le, 10L, true)]
    [InlineData(CompareOp.Gt, 20L, false)]
    [InlineData(CompareOp.Gt, 19L, true)]
    [InlineData(CompareOp.Ge, 21L, false)]
    [InlineData(CompareOp.Ge, 20L, true)]
    [InlineData(CompareOp.Ne, 15L, true)]
    public void IntegerBounds(CompareOp op, long value, bool expected) =>
        Assert.Equal(expected, Might(P.Compare("id", op, value)));

    [Fact]
    public void OtherTypes_UseTheirOwnOrder()
    {
        Assert.False(Might(P.Lt("name", "bob")));
        Assert.True(Might(P.Eq("name", "carol")));
        Assert.False(Might(P.Gt("name", "dave")));
        Assert.False(Might(P.Gt("price", 9.5)));
        Assert.True(Might(P.Ge("price", 9)));
        Assert.False(Might(P.Eq("amount", 7.26m)));
        Assert.True(Might(P.Eq("amount", 7.25m)));
        Assert.False(Might(P.Lt("day", new DateOnly(2024, 1, 10))));
        Assert.True(Might(P.Le("day", new DateOnly(2024, 1, 10))));
        Assert.False(Might(P.Eq(new ColumnPath("nested", "x"), 9)));
        Assert.True(Might(P.Eq(new ColumnPath("nested", "x"), 8)));
    }

    [Fact]
    public void TimestampMaximum_AllowsForTheCutMicroseconds()
    {
        var max = new DateTimeOffset(2024, 1, 20, 0, 0, 0, 123, TimeSpan.Zero);

        // The statistics say .123, the true maximum may be up to .123999.
        Assert.True(Might(P.Gt("at", max.AddTicks(9980))));    // .123998
        Assert.True(Might(P.Eq("at", max.AddTicks(9990))));    // .123999
        Assert.False(Might(P.Gt("at", max.AddTicks(9990))));
        Assert.False(Might(P.Ge("at", max.AddMilliseconds(1))));
        Assert.False(Might(P.Lt("at", new DateTime(2024, 1, 10, 0, 0, 0, 123))));
    }

    [Fact]
    public void Nulls()
    {
        Assert.False(Might(P.IsNull("id")));
        Assert.True(Might(P.IsNull("name")));
        Assert.True(Might(P.IsNotNull("name")));
        Assert.False(Might(P.IsNotNull("flag")));     // null in all 100 rows
        Assert.False(Might(P.Eq("flag", true)));
        Assert.True(Might(P.Not(P.IsNull("name"))));
        Assert.False(Might(P.Not(P.IsNotNull("id"))));
    }

    [Fact]
    public void Ne_SkipsOnlyWhenEveryValueIsTheLiteral()
    {
        AddAction constant = File("""{"numRecords":5,"minValues":{"id":7,"name":"x"},"maxValues":{"id":7,"name":"x"},"nullCount":{"id":0,"name":0}}""");

        Assert.False(Might(P.Ne("id", 7), constant));
        Assert.True(Might(P.Ne("id", 8), constant));
        Assert.False(Might(P.Not(P.Eq("id", 7)), constant));
        Assert.True(Might(P.Ne("name", "x"), constant)); // strings may be truncated
    }

    [Fact]
    public void PartitionValues_AreExact()
    {
        Assert.True(Might(P.Eq("region", "east")));
        Assert.False(Might(P.Eq("region", "west")));
        Assert.False(Might(P.Ne("region", "east")));
        Assert.True(Might(P.Gt("year", 2023)));
        Assert.False(Might(P.Gt("year", 2024)));
        Assert.False(Might(P.Not(P.Le("year", 2024))));

        AddAction nullRegion = File(region: null);
        Assert.False(Might(P.Eq("region", "east"), nullRegion));
        Assert.False(Might(P.Ne("region", "east"), nullRegion));
        Assert.True(Might(P.IsNull("region"), nullRegion));
        Assert.False(Might(P.IsNotNull("region"), nullRegion));
        Assert.False(Might(P.IsNull("region")));
    }

    [Fact]
    public void And_Or_Not_Combine()
    {
        Assert.False(Might(P.And(P.Eq("region", "east"), P.Gt("id", 20))));
        Assert.True(Might(P.Or(P.Eq("region", "west"), P.Le("id", 10))));
        Assert.False(Might(P.Or(P.Eq("region", "west"), P.Gt("id", 20))));
        // not (region = 'west' or id <= 20)  ==  region <> 'west' and id > 20
        Assert.False(Might(P.Not(P.Or(P.Eq("region", "west"), P.Le("id", 20)))));
        // not (region = 'east' and id > 20)  ==  region <> 'east' or id <= 20
        Assert.True(Might(P.Not(P.And(P.Eq("region", "east"), P.Gt("id", 20)))));
    }

    [Fact]
    public void WhateverIsUnknown_KeepsTheFile()
    {
        Assert.True(Might(P.Eq("id", 999), File(stats: null)));
        Assert.True(Might(P.Eq("id", 999), File(stats: "not json")));
        Assert.True(Might(P.Eq("id", 999), File(stats: """{"numRecords":1}""")));
        Assert.True(Might(P.Eq("missing_column", 1)));
        Assert.True(Might(P.Eq("blob", "x")));
        Assert.True(Might(P.Eq("id", "a string for a long column")));
        Assert.True(Might(P.Eq("price", double.NaN)));
        Assert.True(Might(P.Eq(new ColumnPath("id", "x"), 1)));
        Assert.True(Might(P.Gt("year", 2024), File(year: "not a number")));
    }

    [Fact]
    public void ColumnMapping_LooksUpPhysicalNames()
    {
        StructType mapped = StructType.Parse("""
            {"type":"struct","fields":[
              {"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}},
              {"name":"region","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b"}}
            ]}
            """);
        var file = new AddAction
        {
            Path = "f.parquet",
            Stats = """{"numRecords":3,"minValues":{"col-a":1},"maxValues":{"col-a":5},"nullCount":{"col-a":0}}""",
            PartitionValues = new Dictionary<string, string?> { ["col-b"] = "east" },
        };

        Assert.False(new FileFilter(mapped, ["region"], P.Gt("id", 5)).MightMatch(file));
        Assert.True(new FileFilter(mapped, ["region"], P.Eq("id", 5)).MightMatch(file));
        Assert.False(new FileFilter(mapped, ["region"], P.Eq("region", "west")).MightMatch(file));
    }

    [Fact]
    public void FileStats_ReadsTheSample()
    {
        FileStats stats = s_file.ParsedStats!;
        Assert.Equal(100, stats.NumRecords);
        Assert.True(stats.TightBounds);
        Assert.Equal(3, stats.NullCount(["name"]));
        Assert.True(stats.TryGetMax(["nested", "x"], out var max));
        Assert.Equal(8, max.GetInt32());
        Assert.False(stats.TryGetMin(["flag"], out _));
        Assert.False(FileStats.Parse("""{"numRecords":1,"tightBounds":false}""")!.TightBounds);
    }
}

public class TimeTravelTests
{
    private static readonly DateTimeOffset s_t0 = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static MemoryFileSystem Table() => new MemoryFileSystem()
        .Put(LogFile.CommitPath(0), string.Join("\n", Lines.Protocol, Lines.Metadata(), Lines.Add("a.parquet")), s_t0)
        .Put(LogFile.CommitPath(1), Lines.Add("b.parquet"), s_t0.AddHours(1))
        .Put(LogFile.CommitPath(2), Lines.Add("c.parquet"), s_t0.AddHours(2));

    [Fact]
    public void LoadAsOf_PicksTheLatestVersionNotAfterTheTime()
    {
        MemoryFileSystem fs = Table();

        Assert.Equal(0, Snapshot.LoadAsOf(fs, s_t0).Version);
        Assert.Equal(0, Snapshot.LoadAsOf(fs, s_t0.AddMinutes(59)).Version);
        Assert.Equal(1, Snapshot.LoadAsOf(fs, s_t0.AddHours(1)).Version);
        Assert.Equal(2, Snapshot.LoadAsOf(fs, s_t0.AddDays(30)).Version);
        Assert.Contains("before the earliest", Assert.Throws<DeltaException>(() => Snapshot.LoadAsOf(fs, s_t0.AddSeconds(-1))).Message);
    }

    [Fact]
    public void FileTimesThatDoNotIncrease_AreMadeTo()
    {
        // Version 1 was written "before" version 0 according to the file system clock.
        var fs = new MemoryFileSystem()
            .Put(LogFile.CommitPath(0), string.Join("\n", Lines.Protocol, Lines.Metadata()), s_t0)
            .Put(LogFile.CommitPath(1), Lines.Add("b.parquet"), s_t0.AddMinutes(-5))
            .Put(LogFile.CommitPath(2), Lines.Add("c.parquet"), s_t0.AddMinutes(-5));

        CommitTimeline timeline = CommitTimeline.Load(fs);

        Assert.Equal(s_t0.AddMilliseconds(1), timeline.TimestampOf(1));
        Assert.Equal(s_t0.AddMilliseconds(2), timeline.TimestampOf(2));
        Assert.Equal(0, timeline.VersionAsOf(s_t0));
        Assert.Equal(2, timeline.VersionAsOf(s_t0.AddMilliseconds(2)));
    }

    [Fact]
    public void VersionAtOrAfter_FindsTheFirstCommitFromATime()
    {
        CommitTimeline timeline = CommitTimeline.Load(Table());

        Assert.Equal(0, timeline.VersionAtOrAfter(s_t0.AddDays(-1)));
        Assert.Equal(1, timeline.VersionAtOrAfter(s_t0.AddMinutes(1)));
        Assert.Equal(2, timeline.VersionAtOrAfter(s_t0.AddHours(2)));
        Assert.Null(timeline.VersionAtOrAfter(s_t0.AddHours(3)));
        Assert.Equal((0L, 2L), (timeline.EarliestVersion, timeline.LatestVersion));
        Assert.Throws<DeltaException>(() => timeline.TimestampOf(3));
    }

    [Fact]
    public void InCommitTimestamps_ReplaceFileTimes_FromTheEnablementVersion()
    {
        long ict1 = s_t0.AddDays(10).ToUnixTimeMilliseconds();
        long ict2 = s_t0.AddDays(11).ToUnixTimeMilliseconds();
        string metadata = Lines.Metadata(configuration: "\"delta.enableInCommitTimestamps\":\"true\",\"delta.inCommitTimestampEnablementVersion\":\"1\"");
        // File times say everything happened within minutes; the commits themselves say days apart.
        var fs = new MemoryFileSystem()
            .Put(LogFile.CommitPath(0), string.Join("\n", Lines.Protocol, Lines.Metadata()), s_t0)
            .Put(LogFile.CommitPath(1), string.Join("\n", "{\"commitInfo\":{\"inCommitTimestamp\":" + ict1 + "}}", metadata), s_t0.AddMinutes(1))
            .Put(LogFile.CommitPath(2), string.Join("\n", "{\"commitInfo\":{\"inCommitTimestamp\":" + ict2 + "}}", Lines.Add("c.parquet")), s_t0.AddMinutes(2));

        CommitTimeline timeline = CommitTimeline.Load(fs);

        Assert.Equal(s_t0, timeline.TimestampOf(0));
        Assert.Equal(s_t0.AddDays(10), timeline.TimestampOf(1));
        Assert.Equal(0, timeline.VersionAsOf(s_t0.AddDays(5)));
        Assert.Equal(1, timeline.VersionAsOf(s_t0.AddDays(10)));
        Assert.Equal(2, Snapshot.LoadAsOf(fs, s_t0.AddDays(12)).Version);
    }

    [Fact]
    public void FilesMatching_FiltersTheSnapshot()
    {
        var fs = new MemoryFileSystem().Commit(0, Lines.Protocol, Lines.Metadata(),
            """{"add":{"path":"low.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":0}}"}}""",
            """{"add":{"path":"high.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"id\":6},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":0}}"}}""",
            Lines.Add("nostats.parquet"));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(["high.parquet", "nostats.parquet"], snapshot.FilesMatching(Predicate.Gt("id", 5)).Select(f => f.Path));
    }
}
