using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// Classic checkpoints built straight into native Arrow batches must be byte for byte the reference ones (rows as
/// dictionaries through ParquetNodeArrow, the same batch boundaries), over every column type, partition type and kind
/// of statistics text, well-formed or not, across batch boundaries.
/// </summary>
public class NativeCheckpointTests
{
    private const string Schema =
        """
        {"type":"struct","fields":[
          {"name":"by","type":"byte","nullable":true,"metadata":{}},
          {"name":"sh","type":"short","nullable":true,"metadata":{}},
          {"name":"i","type":"integer","nullable":true,"metadata":{}},
          {"name":"l","type":"long","nullable":true,"metadata":{}},
          {"name":"f","type":"float","nullable":true,"metadata":{}},
          {"name":"d","type":"double","nullable":true,"metadata":{}},
          {"name":"dec5","type":"decimal(5,2)","nullable":true,"metadata":{}},
          {"name":"dec15","type":"decimal(15,3)","nullable":true,"metadata":{}},
          {"name":"dec25","type":"decimal(25,3)","nullable":true,"metadata":{}},
          {"name":"s","type":"string","nullable":true,"metadata":{}},
          {"name":"bin","type":"binary","nullable":true,"metadata":{}},
          {"name":"b","type":"boolean","nullable":true,"metadata":{}},
          {"name":"dt","type":"date","nullable":true,"metadata":{}},
          {"name":"ts","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"tn","type":"timestamp_ntz","nullable":true,"metadata":{}},
          {"name":"st","type":{"type":"struct","fields":[
            {"name":"x","type":"integer","nullable":true,"metadata":{}},
            {"name":"deep","type":{"type":"struct","fields":[{"name":"y","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},
            "nullable":true,"metadata":{}},
          {"name":"arr","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":true,"metadata":{}},
          {"name":"p_s","type":"string","nullable":true,"metadata":{}},
          {"name":"p_i","type":"integer","nullable":true,"metadata":{}},
          {"name":"p_l","type":"long","nullable":true,"metadata":{}},
          {"name":"p_d","type":"date","nullable":true,"metadata":{}},
          {"name":"p_dec","type":"decimal(10,2)","nullable":true,"metadata":{}},
          {"name":"p_ts","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"p_b","type":"boolean","nullable":true,"metadata":{}},
          {"name":"p_f","type":"float","nullable":true,"metadata":{}},
          {"name":"p_dbl","type":"double","nullable":true,"metadata":{}},
          {"name":"p_by","type":"byte","nullable":true,"metadata":{}},
          {"name":"p_bin","type":"binary","nullable":true,"metadata":{}}]}
        """;

    private static readonly string[] PartitionColumns = ["p_s", "p_i", "p_l", "p_d", "p_dec", "p_ts", "p_b", "p_f", "p_dbl", "p_by", "p_bin"];

    /// <summary>Statistics texts of every shape: valid, partial, mistyped, malformed, escaped, out of range.</summary>
    private static readonly string?[] StatsTexts =
    [
        null,
        "",
        "not json",
        "[1,2]",
        "{\"numRecords\":3} trailing",
        "{}",
        """{"numRecords":"3","tightBounds":"yes"}""",
        """{"numRecords":12,"tightBounds":true,"minValues":null,"maxValues":5,"nullCount":[1]}""",
        """
        {"numRecords":3,"tightBounds":false,
         "minValues":{"by":-5,"sh":-300,"i":-1,"l":-9000000000,"f":-0.0,"d":-0.0,"dec5":"12.5","dec15":-123456789012.345,
                      "dec25":-0.001,"s":"aé\"q\\n","bin":"AAE=","b":true,"dt":"2020-02-29","ts":"1960-06-01T00:00:00.000Z",
                      "tn":"2024-01-02T03:04:05.123","st":{"x":1,"deep":{"y":"z"}},"arr":[1]},
         "maxValues":{"by":127,"sh":32767,"i":2147483647,"l":9000000000,"f":3.5,"d":1e300,"dec5":999.99,"dec15":"1.5e2",
                      "dec25":1234567890123456789012.345,"s":"zz","dt":"2024-13-01","ts":"2024-01-02 03:04:05","tn":"2024-01-02",
                      "st":{"x":"7","deep":5}},
         "nullCount":{"by":0,"i":0,"b":3,"st":{"x":1,"deep":{"y":2}},"arr":2,"s":"x","dec5":1.5}}
        """,
        """{"minValues":{"dec5":"+003.10","dec15":"0000.000","dec25":"-","i":3000000000,"f":"1.5","d":"NaN","s":"\ud800x"},"nullCount":{"st":null}}""",
        """{"minValues":{"dec5":"1234.5","dec15":"1.0005","dec25":"1e30","dt":20200101,"ts":1700000000,"tn":"bad","st":{"deep":{"y":7}}}}""",
        """{"maxValues":{"s":"__LONG__","d":"1.25","dec5":"-1e-2","dec15":12345678901234567890,"st":[]},"unknown":{"a":[1,{"b":2}]}}""",
        """{ "numRecords" : 1 , "nullCount" : { "i" : 1 , "i2" : 4 } , "minValues" : { } }""",
        // Names given twice take their last value; escaped names are names too.
        """{"numRecords":1,"numRecords":5,"minValues":{"i":1,"i":2,"st":{"x":1},"st":{"x":3,"x":4}},"maxValues":{"l":1},"maxValues":{"d":4.5}}""",
        """{"minValues":{"\u0069":9,"i":10,"s":"\u00e9"},"nullC\u006funt":{"i":3}}""",
    ];

    private static readonly IReadOnlyDictionary<string, string?>[] PartitionValueSets =
    [
        new Dictionary<string, string?>
        {
            ["p_s"] = "é東\"x", ["p_i"] = "-7", ["p_l"] = "9000000000", ["p_d"] = "2020-02-02", ["p_dec"] = "-12.30",
            ["p_ts"] = "2024-01-02 03:04:05.123456", ["p_b"] = "true", ["p_f"] = "-0.0", ["p_dbl"] = "1e10", ["p_by"] = "12", ["p_bin"] = "\\x00",
        },
        new Dictionary<string, string?> { ["p_s"] = "", ["p_i"] = null, ["p_d"] = "nope", ["p_dec"] = "1.234", ["p_b"] = "maybe", ["p_f"] = "x" },
        new Dictionary<string, string?>(),
        new Dictionary<string, string?> { ["p_ts"] = "2024-01-02T03:04:05Z", ["p_dec"] = "99999999.99", ["p_i"] = "1.5", ["p_dbl"] = "NaN" },
    ];

    private static Snapshot Table(int files, Dictionary<string, string> configuration, bool partitioned, bool deletionVectors)
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3, MinWriterVersion = 7,
            ReaderFeatures = deletionVectors ? ["deletionVectors"] : [],
            WriterFeatures = deletionVectors ? ["deletionVectors", "domainMetadata"] : ["domainMetadata"],
        };
        var metadata = new MetadataAction
        {
            Id = "native", Name = "t", Description = "the \"table\"", SchemaString = Schema,
            PartitionColumns = partitioned ? PartitionColumns : [],
            Configuration = configuration, FormatOptions = new Dictionary<string, string> { ["k"] = "v" }, CreatedTime = 1_700_000_000_000,
        };
        string longText = new('w', 400);
        List<DeltaAction> actions =
        [
            protocol, metadata,
            new TxnAction { AppId = "app-1", Version = 3, LastUpdated = 17 },
            new TxnAction { AppId = "app-2", Version = 9 },
            new DomainMetadataAction { Domain = "delta.clustering", Configuration = """{"clusteringColumns":[]}""" },
            new DomainMetadataAction { Domain = "user.x", Configuration = "{}", Removed = false },
        ];
        for (int n = 0; n < files; n++)
        {
            string? stats = StatsTexts[n % StatsTexts.Length]?.Replace("__LONG__", longText, StringComparison.Ordinal);
            actions.Add(new AddAction
            {
                Path = $"p={n % 5}/part-{n:D6}-é.parquet",
                PartitionValues = partitioned ? PartitionValueSets[n % PartitionValueSets.Length] : new Dictionary<string, string?>(),
                Size = 1000 + n,
                ModificationTime = 1_700_000_000_000 + n,
                Stats = stats,
                Tags = n % 3 == 0 ? new Dictionary<string, string?> { ["INSERTION_TIME"] = $"{n}", ["empty"] = null } : null,
                DeletionVector = n % 4 == 0
                    ? new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = $"ab^{n}", Offset = n % 8 == 0 ? null : 1, SizeInBytes = 34, Cardinality = n }
                    : null,
                BaseRowId = n % 2 == 0 ? n * 10L : null,
                DefaultRowCommitVersion = n % 2 == 0 ? 1 : null,
                ClusteringProvider = n % 5 == 0 ? "liquid" : null,
            });
        }
        for (int n = 0; n < 37; n++)
        {
            actions.Add(new RemoveAction
            {
                Path = $"gone-{n}.parquet", DeletionTimestamp = n % 2 == 0 ? 1_800_000_000_000 : null, ExtendedFileMetadata = n % 3 == 0,
                PartitionValues = n % 2 == 0 ? new Dictionary<string, string?> { ["p_s"] = "x" } : null, Size = n % 4 == 0 ? null : n,
                DeletionVector = n % 5 == 0 ? new DeletionVectorDescriptor { StorageType = "i", PathOrInlineDv = "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{0", SizeInBytes = 2, Cardinality = 1 } : null,
                BaseRowId = n, DefaultRowCommitVersion = n % 2 == 0 ? 2 : null,
            });
        }
        var fs = new MemoryFileSystem();
        Assert.True(fs.TryCreate("_delta_log/00000000000000000000.json", ActionWriter.WriteCommit(actions)));
        return Snapshot.Load(fs);
    }

    private static void SameAsTheReference(Snapshot snapshot)
    {
        var now = new DateTimeOffset(2026, 11, 17, 0, 0, 0, TimeSpan.Zero);
        var fs = new MemoryFileSystem();
        string path = Checkpoints.Write(fs, snapshot, now);
        byte[] written = fs.ReadAll(path)!;
        byte[] reference = Checkpoints.ReferenceParquet(snapshot, now);
        Assert.Equal(reference.Length, written.Length);
        Assert.True(reference.AsSpan().SequenceEqual(written), "the native checkpoint differs from the reference one");
        string hint = Encoding.UTF8.GetString(fs.ReadAll("_delta_log/_last_checkpoint")!);
        Assert.Contains($"\"sizeInBytes\":{written.Length},", hint);
    }

    [Theory]
    [InlineData(40_000, true, true)]       // crosses two batch boundaries; every partition type; tightBounds
    [InlineData(CheckpointBatches_Rows - 6, true, false)] // exactly one full batch (6 rows before the adds)
    [InlineData(25, false, false)]
    public void TheNativeCheckpoint_IsTheReferenceOne(int files, bool partitioned, bool deletionVectors) =>
        SameAsTheReference(Table(files, [], partitioned, deletionVectors));

    private const int CheckpointBatches_Rows = 16_384;

    /// <summary>
    /// A wide table (more columns than any fixed bound on names per object), every column indexed, with a non-ASCII
    /// column name given twice, escaped once: statistics objects of 40 names, repeats among them.
    /// </summary>
    [Fact]
    public void TheNativeCheckpoint_IsTheReferenceOne_ForWideStatistics_AndRepeatedNonAsciiNames()
    {
        string[] names = [.. Enumerable.Range(0, 39).Select(i => $"c{i}"), "é"];
        string schema = """{"type":"struct","fields":[""" + string.Join(",", names.Select(n => """{"name":""" + $"\"{n}\"" + ""","type":"long","nullable":true,"metadata":{}}""")) + "]}";
        var metadata = new MetadataAction { Id = "wide", SchemaString = schema, Configuration = new Dictionary<string, string> { [FileStatistics.NumIndexedColsKey] = "-1" } };
        string Section(int i) => "{" + string.Join(",", names.Select((n, k) => $"\"{n}\":{i + k}")) + "}";
        string?[] texts =
        [
            $$"""{"numRecords":1,"minValues":{{Section(0)}},"maxValues":{{Section(100)}},"nullCount":{{Section(0)}}}""",
            $$"""{"numRecords":2,"minValues":{"é":1,"\u00e9":2,"c0":5},"maxValues":{{Section(7)}}}""",
            $$"""{"numRecords":3,"nullCount":{"c1":1,"c1":9},"minValues":{{Section(3)}}}""",
        ];
        List<DeltaAction> actions = [new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata];
        for (int n = 0; n < 900; n++) actions.Add(new AddAction { Path = $"w-{n}.parquet", Size = n, Stats = texts[n % texts.Length] });
        var fs = new MemoryFileSystem();
        Assert.True(fs.TryCreate("_delta_log/00000000000000000000.json", ActionWriter.WriteCommit(actions)));
        SameAsTheReference(Snapshot.Load(fs));
    }

    [Theory]
    [InlineData(CheckpointStatistics.WriteJsonKey, "false")]
    [InlineData(CheckpointStatistics.WriteStructKey, "false")]
    [InlineData(FileStatistics.NumIndexedColsKey, "0")]
    [InlineData(FileStatistics.NumIndexedColsKey, "3")]
    public void TheNativeCheckpoint_IsTheReferenceOne_WhateverTheStatisticsSettings(string key, string value) =>
        SameAsTheReference(Table(300, new() { [key] = value }, partitioned: true, deletionVectors: true));
}
