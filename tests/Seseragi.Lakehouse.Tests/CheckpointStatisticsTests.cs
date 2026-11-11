using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Seseragi.Lakehouse.Tests;

/// <summary>The typed checkpoint columns (values checked against delta-spark 4.1 in SparkCheckpointStatisticsTests).</summary>
public class CheckpointStatisticsTests
{
    private const string Schema =
        """
        {"type":"struct","fields":[
          {"name":"i","type":"integer","nullable":true,"metadata":{}},
          {"name":"big","type":"decimal(25,3)","nullable":true,"metadata":{}},
          {"name":"small","type":"decimal(5,2)","nullable":true,"metadata":{}},
          {"name":"tn","type":"timestamp_ntz","nullable":true,"metadata":{}},
          {"name":"ts","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"dbl","type":"double","nullable":true,"metadata":{}},
          {"name":"b","type":"boolean","nullable":true,"metadata":{}},
          {"name":"st","type":{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},
          {"name":"arr","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":true,"metadata":{}},
          {"name":"pd","type":"date","nullable":true,"metadata":{}}]}
        """;

    private static CheckpointStatistics Statistics(Dictionary<string, string>? configuration = null, bool deletionVectors = false) =>
        CheckpointStatistics.For(
            new MetadataAction { Id = "t", SchemaString = Schema, PartitionColumns = ["pd"], Configuration = configuration ?? [] },
            new ProtocolAction
            {
                MinReaderVersion = 3, MinWriterVersion = 7,
                ReaderFeatures = deletionVectors ? ["deletionVectors"] : [], WriterFeatures = deletionVectors ? ["deletionVectors"] : [],
            })!;

    private static string Names(ParquetNode node) => node switch
    {
        ParquetStruct s => $"{s.Name}<{string.Join(",", s.Children.Select(Names))}>",
        _ => node.Name,
    };

    [Fact]
    public void Schema_FollowsSpark_BoundsForTypesWithThem_NullCountsForEveryIndexedLeaf()
    {
        Assert.Equal("stats_parsed<numRecords,minValues<i,big,small,tn,ts,dbl,st<x>>,maxValues<i,big,small,tn,ts,dbl,st<x>>,nullCount<i,big,small,tn,ts,dbl,b,st<x>,arr>>",
            Names(Statistics().StatsNode()));
        Assert.Equal("partitionValues_parsed<pd>", Names(Statistics().PartitionNode()!));
        Assert.EndsWith(",tightBounds>", Names(Statistics(deletionVectors: true).StatsNode()));
        // No indexed column: the row count only (Spark leaves the empty structs out).
        Assert.Equal("stats_parsed<numRecords>", Names(Statistics(new() { [FileStatistics.NumIndexedColsKey] = "0" }).StatsNode()));
        Assert.Null(CheckpointStatistics.For(
            new MetadataAction { Id = "t", SchemaString = Schema, Configuration = new Dictionary<string, string> { [CheckpointStatistics.WriteStructKey] = "false" } },
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }));
    }

    [Fact]
    public void Values_AreTheStatisticsInTheColumnsTypes()
    {
        var value = Statistics().StatsValue(
            """
            {"numRecords":3,
             "minValues":{"i":-1,"big":-0.001,"small":"12.5","tn":"2024-01-02T03:04:05.123","ts":"1960-06-01T00:00:00.000Z","dbl":-0.0},
             "maxValues":{"i":7,"big":1234567890123456789012.345,"small":999.999},
             "nullCount":{"i":0,"b":3,"st":{"x":1},"arr":2}}
            """)!;
        var min = (Dictionary<string, object?>)value["minValues"]!;
        var max = (Dictionary<string, object?>)value["maxValues"]!;
        Assert.Equal(3L, value["numRecords"]);
        Assert.Equal(-1, min["i"]);
        Assert.Equal(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, min["big"]); // -1 unscaled, 11 bytes
        Assert.Equal(1250, min["small"]);
        Assert.Equal(1704164645123000L, min["tn"]);   // the local date-time as microseconds since the epoch
        Assert.Equal(-302486400000000L, min["ts"]);   // before the epoch
        Assert.Equal(0d, min["dbl"]);
        Assert.False(double.IsNegative((double)min["dbl"]!)); // negative zero as zero, as Spark
        Assert.Null(min["st"]);                          // not in the JSON: null as a whole
        Assert.Null(max["small"]);                       // more fraction digits than the scale: not the column's value
        var nulls = (Dictionary<string, object?>)value["nullCount"]!;
        Assert.Equal((3L, 2L, 1L), ((long)nulls["b"]!, (long)nulls["arr"]!, (long)((Dictionary<string, object?>)nulls["st"]!)["x"]!));
        Assert.Null(Statistics().StatsValue(null));
        Assert.Null(Statistics().StatsValue("not json"));
    }

    [Fact]
    public void PartitionValues_AreTyped_EmptyOrMissingIsNull()
    {
        CheckpointStatistics statistics = Statistics();
        Assert.Equal(18294, statistics.PartitionValue(new AddAction { Path = "a", PartitionValues = new Dictionary<string, string?> { ["pd"] = "2020-02-02" } })!["pd"]);
        Assert.Null(statistics.PartitionValue(new AddAction { Path = "a", PartitionValues = new Dictionary<string, string?> { ["pd"] = null } })!["pd"]);
        Assert.Null(statistics.PartitionValue(new AddAction { Path = "a", PartitionValues = new Dictionary<string, string?>() })!["pd"]);
    }
}
