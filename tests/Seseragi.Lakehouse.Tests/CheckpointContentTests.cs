using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// What classic checkpoints hold, read back with the kernel's Parquet reader: every action and field, across batch
/// boundaries; the typed statistics of every kind of statistics text (well-formed or not, repeated names, wide
/// objects) and the typed partition values of every partition type. The expected values were checked against the
/// earlier implementation (rows as dictionaries through ParquetNodeArrow: byte for byte the same files) and its
/// semantics against delta-spark (SparkCheckpointStatisticsTests).
/// </summary>
public class CheckpointContentTests
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

    private static readonly DateTimeOffset Now = new(2026, 11, 17, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Writes the checkpoint, checks the hint names its size, and returns its bytes.</summary>
    private static byte[] Checkpoint(Snapshot snapshot)
    {
        var fs = new MemoryFileSystem();
        string path = Checkpoints.Write(fs, snapshot, Now);
        byte[] written = fs.ReadAll(path)!;
        string hint = Encoding.UTF8.GetString(fs.ReadAll("_delta_log/_last_checkpoint")!);
        Assert.Contains($"\"sizeInBytes\":{written.Length},", hint);
        return written;
    }

    /// <summary>Prints actual values instead of checking them (CHECKPOINT_GOLDEN=print), to review and paste.</summary>
    private static void Expect(string name, IReadOnlyList<string?> expected, IReadOnlyList<string?> actual)
    {
        if (Environment.GetEnvironmentVariable("CHECKPOINT_GOLDEN") == "print")
        {
            Console.WriteLine($"// {name}");
            foreach (string? value in actual) Console.WriteLine(value is null ? "GOLDEN null," : $"GOLDEN \"\"\"{value}\"\"\",");
            return;
        }
        Assert.Equal(expected, actual);
    }

    private static string Json(DeltaAction action) =>
        Encoding.UTF8.GetString(ActionWriter.WriteCommit([action])).Replace("\"dataChange\":true", "\"dataChange\":false", StringComparison.Ordinal);

    [Theory]
    [InlineData(40_000, true, true)]       // crosses two batch boundaries; every partition type; tightBounds
    [InlineData(BatchRows - 6, true, false)] // exactly one full batch (6 rows before the adds)
    [InlineData(25, false, false)]
    public void TheCheckpoint_HoldsTheTable_AndIsTheSameEveryTime(int files, bool partitioned, bool deletionVectors)
    {
        Snapshot snapshot = Table(files, [], partitioned, deletionVectors);
        byte[] written = Checkpoint(snapshot);
        Assert.Equal(written, Checkpoint(snapshot));

        // The table as the checkpoint alone holds it (files never change data in a checkpoint).
        var fs = new MemoryFileSystem();
        fs.TryCreate("_delta_log/00000000000000000000.checkpoint.parquet", written);
        fs.TryCreate("_delta_log/_last_checkpoint", Encoding.UTF8.GetBytes($$"""{"version":0,"size":{{files + 43}}}"""));
        Snapshot read = Snapshot.Load(fs);
        Assert.Equal(snapshot.Files.Select(Json), read.Files.Select(Json));
        Assert.Equal(snapshot.Tombstones.Select(Json).Order(StringComparer.Ordinal), read.Tombstones.Select(Json).Order(StringComparer.Ordinal));
        Assert.Equal(snapshot.AppTransactions.Values.Select(Json).Order(StringComparer.Ordinal), read.AppTransactions.Values.Select(Json).Order(StringComparer.Ordinal));
        Assert.Equal(snapshot.DomainMetadata.Values.Select(Json).Order(StringComparer.Ordinal), read.DomainMetadata.Values.Select(Json).Order(StringComparer.Ordinal));
        Assert.Equal(Json(snapshot.Metadata), Json(read.Metadata));
        Assert.Equal(Json(snapshot.Protocol), Json(read.Protocol));
    }

    private const int BatchRows = 16_384;

    [Fact]
    public void TypedStatistics_OfEveryKindOfText()
    {
        byte[] written = Checkpoint(Table(StatsTexts.Length, [], partitioned: true, deletionVectors: true));
        List<string?> stats = CheckpointRead.Column(written, "add", "stats_parsed");
        // The rows before the files: protocol, metaData, two txns, two domains.
        Expect(nameof(StatsExpected), StatsExpected, stats.Skip(6).Take(StatsTexts.Length).ToList());
    }

    [Fact]
    public void TypedPartitionValues_OfEveryPartitionType()
    {
        byte[] written = Checkpoint(Table(PartitionValueSets.Length, [], partitioned: true, deletionVectors: false));
        Expect(nameof(PartitionsExpected), PartitionsExpected, CheckpointRead.Column(written, "add", "partitionValues_parsed").Skip(6).Take(PartitionValueSets.Length).ToList());
    }

    /// <summary>
    /// A wide table (more names per statistics object than any fixed bound), every column indexed, with a non-ASCII
    /// column name given twice, escaped once, and repeated names: a repeated name takes its last value.
    /// </summary>
    [Fact]
    public void TypedStatistics_OfWideObjects_AndRepeatedNames()
    {
        string[] names = [.. Enumerable.Range(0, 39).Select(i => $"c{i}"), "é"];
        string schema = """{"type":"struct","fields":[""" + string.Join(",", names.Select(n => """{"name":""" + $"\"{n}\"" + ""","type":"long","nullable":true,"metadata":{}}""")) + "]}";
        var metadata = new MetadataAction { Id = "wide", SchemaString = schema, Configuration = new Dictionary<string, string> { [FileStatistics.NumIndexedColsKey] = "-1" } };
        string Section(int i) => "{" + string.Join(",", names.Select((n, k) => $"\"{n}\":{i + k}")) + "}";
        string?[] texts =
        [
            $$"""{"numRecords":1,"minValues":{{Section(0)}},"maxValues":{{Section(100)}},"nullCount":{{Section(0)}}}""",
            $$"""{"numRecords":2,"minValues":{"é":1,"\u00e9":2,"c0":5},"maxValues":{{Section(7)}}}""",
            $$"""{"numRecords":3,"nullCount":{"c1":1,"c1":9},"minValues":{{Section(3)}},"numRecords":4}""",
        ];
        List<DeltaAction> actions = [new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata];
        for (int n = 0; n < texts.Length; n++) actions.Add(new AddAction { Path = $"w-{n}.parquet", Size = n, Stats = texts[n] });
        var fs = new MemoryFileSystem();
        Assert.True(fs.TryCreate("_delta_log/00000000000000000000.json", ActionWriter.WriteCommit(actions)));
        byte[] written = Checkpoint(Snapshot.Load(fs));
        Expect(nameof(WideExpected), WideExpected, CheckpointRead.Column(written, "add", "stats_parsed").Skip(2).ToList());
    }

    [Theory]
    [InlineData(CheckpointStatistics.WriteJsonKey, "false", "path,partitionValues,size,modificationTime,dataChange,tags,deletionVector,baseRowId,defaultRowCommitVersion,clusteringProvider,partitionValues_parsed,stats_parsed")]
    [InlineData(CheckpointStatistics.WriteStructKey, "false", "path,partitionValues,size,modificationTime,dataChange,tags,deletionVector,baseRowId,defaultRowCommitVersion,clusteringProvider,stats")]
    [InlineData(FileStatistics.NumIndexedColsKey, "0", "path,partitionValues,size,modificationTime,dataChange,tags,deletionVector,baseRowId,defaultRowCommitVersion,clusteringProvider,stats,partitionValues_parsed,stats_parsed")]
    public void TheStatisticsSettings_ChooseTheColumns(string key, string value, string addFields)
    {
        byte[] written = Checkpoint(Table(300, new() { [key] = value }, partitioned: true, deletionVectors: true));
        Assert.Equal(addFields, string.Join(",", CheckpointRead.Fields(written, "add")));
        if (key == FileStatistics.NumIndexedColsKey)
            Assert.Equal("{numRecords:12,tightBounds:true}", CheckpointRead.Column(written, "add", "stats_parsed")[6 + 7]);
    }

    private static readonly string Long = new('w', 400);

    private static readonly string?[] StatsExpected =
    [
        null,
        null,
        null,
        null,
        null,
        """{}""",
        """{}""",
        """{numRecords:12,tightBounds:true}""",
        """{numRecords:3,minValues:{by:-5,sh:-300,i:-1,l:-9000000000,f:0,d:0,dec5:12.50,dec15:-123456789012.345,dec25:-0.001,s:"aé\"q\\n",dt:18321d,ts:-302486400000000us,tn:1704164645123000us,st:{x:1,deep:{y:"z"}}},maxValues:{by:127,sh:32767,i:2147483647,l:9000000000,f:3.5,d:1E+300,dec5:999.99,dec15:150.000,dec25:1234567890123456789012.345,s:"zz",ts:1704164645000000us,tn:1704153600000000us,st:{}},nullCount:{by:0,i:0,b:3,st:{x:1,deep:{y:2}},arr:2},tightBounds:false}""",
        """{minValues:{d:NaN,dec5:3.10,dec15:0.000,dec25:0.000},nullCount:{}}""",
        """{minValues:{st:{deep:{}}}}""",
        $$$$"""{maxValues:{d:1.25,dec5:-0.01,s:"{{{{Long}}}}"}}""",
        """{numRecords:1,minValues:{},nullCount:{i:1}}""",
        """{numRecords:5,minValues:{i:2,st:{x:4}},maxValues:{d:4.5}}""",
        """{minValues:{i:10,s:"é"},nullCount:{i:3}}""",
    ];
    private static readonly string?[] PartitionsExpected =
    [
        """{p_s:"é東\"x",p_i:-7,p_l:9000000000,p_d:18294d,p_dec:-12.30,p_ts:1704164645123456us,p_b:true,p_f:-0,p_dbl:10000000000,p_by:12}""",
        """{}""",
        """{}""",
        """{p_dec:99999999.99,p_ts:1704164645000000us,p_dbl:NaN}""",
    ];
    private static readonly string?[] WideExpected =
    [
        """{numRecords:1,minValues:{c0:0,c1:1,c2:2,c3:3,c4:4,c5:5,c6:6,c7:7,c8:8,c9:9,c10:10,c11:11,c12:12,c13:13,c14:14,c15:15,c16:16,c17:17,c18:18,c19:19,c20:20,c21:21,c22:22,c23:23,c24:24,c25:25,c26:26,c27:27,c28:28,c29:29,c30:30,c31:31,c32:32,c33:33,c34:34,c35:35,c36:36,c37:37,c38:38,é:39},maxValues:{c0:100,c1:101,c2:102,c3:103,c4:104,c5:105,c6:106,c7:107,c8:108,c9:109,c10:110,c11:111,c12:112,c13:113,c14:114,c15:115,c16:116,c17:117,c18:118,c19:119,c20:120,c21:121,c22:122,c23:123,c24:124,c25:125,c26:126,c27:127,c28:128,c29:129,c30:130,c31:131,c32:132,c33:133,c34:134,c35:135,c36:136,c37:137,c38:138,é:139},nullCount:{c0:0,c1:1,c2:2,c3:3,c4:4,c5:5,c6:6,c7:7,c8:8,c9:9,c10:10,c11:11,c12:12,c13:13,c14:14,c15:15,c16:16,c17:17,c18:18,c19:19,c20:20,c21:21,c22:22,c23:23,c24:24,c25:25,c26:26,c27:27,c28:28,c29:29,c30:30,c31:31,c32:32,c33:33,c34:34,c35:35,c36:36,c37:37,c38:38,é:39}}""",
        """{numRecords:2,minValues:{c0:5,é:2},maxValues:{c0:7,c1:8,c2:9,c3:10,c4:11,c5:12,c6:13,c7:14,c8:15,c9:16,c10:17,c11:18,c12:19,c13:20,c14:21,c15:22,c16:23,c17:24,c18:25,c19:26,c20:27,c21:28,c22:29,c23:30,c24:31,c25:32,c26:33,c27:34,c28:35,c29:36,c30:37,c31:38,c32:39,c33:40,c34:41,c35:42,c36:43,c37:44,c38:45,é:46}}""",
        """{numRecords:4,minValues:{c0:3,c1:4,c2:5,c3:6,c4:7,c5:8,c6:9,c7:10,c8:11,c9:12,c10:13,c11:14,c12:15,c13:16,c14:17,c15:18,c16:19,c17:20,c18:21,c19:22,c20:23,c21:24,c22:25,c23:26,c24:27,c25:28,c26:29,c27:30,c28:31,c29:32,c30:33,c31:34,c32:35,c33:36,c34:37,c35:38,c36:39,c37:40,c38:41,é:42},nullCount:{c1:9}}""",
    ];
}
