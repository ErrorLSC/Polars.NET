using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Lakehouse.Tests.CheckpointParquet;

namespace Seseragi.Lakehouse.Tests;

public class CheckpointTests
{
    private const string Log = "_delta_log/";

    // Checkpoint rows in their JSON shape, written as Parquet: every action column present, the unused ones null or a
    // struct of nulls, maps as objects or as arrays of key/value entries.
    private static string AddRow(string path, string partitionValue = "x") =>
        "{\"txn\":null,\"add\":{\"path\":\"" + path + "\",\"partitionValues\":[{\"key\":\"p\",\"value\":\"" + partitionValue + "\"}],\"size\":10,\"modificationTime\":5,\"dataChange\":false,\"stats\":null,\"deletionVector\":null},\"remove\":{\"path\":null,\"deletionTimestamp\":null},\"metaData\":null,\"protocol\":null}";

    private const string RemoveRow = """{"add":null,"remove":{"path":"gone.parquet","deletionTimestamp":3,"dataChange":false},"metaData":null,"protocol":{"minReaderVersion":null,"minWriterVersion":null}}""";
    // An add of nulls first: a reader that looks only at whether the struct is null takes it for an add.
    private const string ProtocolRow = """{"add":{"path":null,"size":null,"deletionVector":null},"remove":null,"metaData":null,"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""";

    private static string MetadataRow => Lines.Metadata("\"p\"");

    private const string Checkpoint2 = Log + "00000000000000000002.checkpoint.parquet";

    private static MemoryFileSystem TableWithCheckpointAt2() => new MemoryFileSystem()
        .Commit(0, Lines.Protocol, Lines.Metadata("\"p\""), Lines.Add("a.parquet"), Lines.Add("gone.parquet"))
        .Commit(1, Lines.Remove("gone.parquet"))
        .Commit(2, Lines.Add("b.parquet"))
        .PutCheckpoint(Checkpoint2, ProtocolRow, MetadataRow, AddRow("a.parquet"), AddRow("b.parquet", "y"), RemoveRow)
        .Commit(3, Lines.Add("c.parquet"), Lines.Remove("a.parquet"));

    /// <summary>The checkpoint files read (by range), in first-read order.</summary>
    private static List<string> CheckpointReads(MemoryFileSystem fs) => [.. fs.RangeReads.Select(r => r.Path).Distinct()];

    [Fact]
    public void ClassicCheckpoint_PlusLaterCommits_GivesTheState()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2();

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(3, snapshot.Version);
        Assert.Equal(["b.parquet", "c.parquet"], snapshot.Files.Select(f => f.Path).Order());
        Assert.Equal("y", snapshot.Files.Single(f => f.Path == "b.parquet").PartitionValues["p"]);
        Assert.False(snapshot.Files.Single(f => f.Path == "b.parquet").DataChange); // as the checkpoint has it
        Assert.Equal(["p"], snapshot.PartitionColumns);
        Assert.Equal([Checkpoint2], CheckpointReads(fs));
        Assert.DoesNotContain(Checkpoint2, fs.Reads); // read by ranges, never whole
    }

    [Fact]
    public void Checkpoint_GivesTheSameStateAsTheCommits()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2();

        Snapshot viaCheckpoint = Snapshot.Load(fs, 2);
        Snapshot viaCommits = Snapshot.Load(fs, 2, useCheckpoints: false);

        Assert.Equal(viaCommits.Files.Select(f => (f.Path, f.Size)).Order(), viaCheckpoint.Files.Select(f => (f.Path, f.Size)).Order());
        Assert.Equal(viaCommits.Metadata.SchemaString, viaCheckpoint.Metadata.SchemaString);
        Assert.Equal(viaCommits.Metadata.PartitionColumns, viaCheckpoint.Metadata.PartitionColumns);
        Assert.Equal(viaCommits.Protocol.MinWriterVersion, viaCheckpoint.Protocol.MinWriterVersion);
        Assert.Equal(["gone.parquet"], viaCheckpoint.Tombstones.Select(t => t.Path));
        Assert.Equal(3, viaCheckpoint.Tombstones[0].DeletionTimestamp);
    }

    [Fact]
    public void Reads_ASmallCheckpointInOneRequest_AndALargeOneByFooterAndRowGroup()
    {
        // Up to 64 KiB the footer read holds the whole file, which is then read from memory.
        MemoryFileSystem small = TableWithCheckpointAt2();
        Snapshot.Load(small, 2);
        Assert.Equal(1, small.RangeReads.Count(r => r.Path == Checkpoint2));

        // A larger one: the footer, then the row group (its column chunks coalesced into one read).
        const string Large = Log + "00000000000000000002.checkpoint.parquet";
        MemoryFileSystem large = new MemoryFileSystem().PutCheckpoint(Large,
            [ProtocolRow, MetadataRow, .. Enumerable.Range(0, 2_000).Select(i => AddRow($"part-{i:D5}-a-long-file-name-to-make-the-column-big.parquet"))]);
        Assert.True(large.ReadAll(Large)!.Length > 64 * 1024);
        large.Reads.Clear();
        Assert.Equal(2_000, Snapshot.Load(large).Files.Count);
        Assert.Equal(2, large.RangeReads.Count(r => r.Path == Large));
    }

    [Fact]
    public void ProtocolAndMetadataOnly_SkipsTheFileActions()
    {
        // Many files: a load of the table's head reads far less of the checkpoint than a full load.
        string[] adds = [.. Enumerable.Range(0, 2_000).Select(i => AddRow($"part-{i:D5}-a-long-file-name-to-make-the-column-big.parquet"))];
        MemoryFileSystem Build() => new MemoryFileSystem()
            .PutCheckpoint(Log + "00000000000000000004.checkpoint.parquet", [ProtocolRow, MetadataRow, .. adds])
            .Commit(5, Lines.Add("z.parquet"));

        MemoryFileSystem full = Build();
        Assert.Equal(2_001, Snapshot.Load(full).Files.Count);
        MemoryFileSystem head = Build();
        TableHead h = Snapshot.LoadHead(head);

        Assert.Equal(5, h.Version);
        Assert.Equal(["p"], h.Metadata.PartitionColumns);
        Assert.Equal(1, h.Protocol.MinReaderVersion);
        // After the footer (the same tail read for both), the data: only the two small columns for the head.
        static long DataRead(MemoryFileSystem fs) => fs.RangeReads.Skip(1).Sum(r => (long)r.Length);
        Assert.True(DataRead(head) * 50 < DataRead(full), $"{DataRead(head)} vs {DataRead(full)}");
    }

    [Fact]
    public void StatsParsed_StandsInForTheStatsText()
    {
        // delta.checkpoint.writeStatsAsJson = false: the checkpoint has no stats column, only the struct, with a null
        // for a column that has none.
        ParquetStruct statsParsed = Struct("stats_parsed", I64("numRecords"),
            Struct("minValues", I64("id"), Str("p")), Struct("maxValues", I64("id"), Str("p")), Struct("nullCount", I64("id"), I64("p")));
        static string Row(string path, string stats) => AddRow(path).Replace("\"stats\":null", "\"stats_parsed\":" + stats);
        var fs = new MemoryFileSystem().PutBytes(Log + "00000000000000000000.checkpoint.parquet", Write(Classic(statsText: false, statsParsed),
            ProtocolRow, MetadataRow,
            Row("low.parquet", """{"numRecords":10,"minValues":{"id":1,"p":null},"maxValues":{"id":10,"p":null},"nullCount":{"id":0,"p":null}}"""),
            Row("high.parquet", """{"numRecords":10,"minValues":{"id":11,"p":null},"maxValues":{"id":20,"p":null},"nullCount":{"id":0,"p":null}}"""),
            Row("none.parquet", "null")));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal("""{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":10},"nullCount":{"id":0}}""",
            snapshot.Files.Single(f => f.Path == "low.parquet").Stats);
        Assert.Null(snapshot.Files.Single(f => f.Path == "none.parquet").ParsedStats);
        Assert.Equal(["high.parquet", "none.parquet"],
            snapshot.FilesMatching(Kaminari.Seseragi.Lakehouse.Skipping.Predicate.Gt("id", 15L)).Select(f => f.Path).Order());
    }

    [Fact]
    public void StatsParsed_WithTheTextToo_IsNotRead()
    {
        ParquetStruct statsParsed = Struct("stats_parsed", I64("numRecords"));
        var fs = new MemoryFileSystem().PutBytes(Log + "00000000000000000000.checkpoint.parquet", Write(Classic(statsText: true, statsParsed),
            ProtocolRow, MetadataRow,
            AddRow("text.parquet").Replace("\"stats\":null", "\"stats\":" + """ "{\"numRecords\":7}","stats_parsed":{"numRecords":99}""".Trim()),
            AddRow("none.parquet").Replace("\"stats\":null", "\"stats_parsed\":{\"numRecords\":99}")));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal("""{"numRecords":7}""", snapshot.Files.Single(f => f.Path == "text.parquet").Stats);
        Assert.Null(snapshot.Files.Single(f => f.Path == "none.parquet").Stats);
    }

    [Fact]
    public void StatsParsed_BecomesTheTextSparkWrites()
    {
        // The typed statistics of every kind of bound, read back into the text a Spark commit holds.
        ParquetLeaf Ts(string name, bool utc) =>
            new(name, Repetition.Optional, PhysicalType.Int64, Annotation: utc ? Annotation.TimestampMicros : Annotation.LocalTimestampMicros);
        ParquetStruct Bounds(string name) => Struct(name, I64("i"), new ParquetLeaf("b", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Int8),
            new ParquetLeaf("x", Repetition.Optional, PhysicalType.Double), new ParquetLeaf("f", Repetition.Optional, PhysicalType.Float),
            new ParquetLeaf("d", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Date),
            Ts("ts", utc: true), Ts("ntz", utc: false),
            new ParquetLeaf("m", Repetition.Optional, PhysicalType.Int64, Annotation: Annotation.Decimal, Precision: 12, Scale: 2),
            new ParquetLeaf("small", Repetition.Optional, PhysicalType.Int32, Annotation: Annotation.Decimal, Precision: 5, Scale: 3),
            Str("s"), Struct("nested", I64("k")));
        ParquetStruct statsParsed = Struct("stats_parsed", I64("numRecords"), Bounds("minValues"), Bounds("maxValues"),
            Struct("nullCount", I64("i"), I64("s")), Bool("tightBounds"));
        // 2023-05-31T18:58:33.633999Z: cut to milliseconds. -1 µs before the epoch: floored, not rounded towards zero.
        string row = AddRow("f.parquet").Replace("\"stats\":null", "\"stats_parsed\":" + """
            {"numRecords":3,
             "minValues":{"i":-5,"b":-128,"x":1.5,"f":0.1,"d":19508,"ts":1685559513633999,"ntz":-1,"m":-150,"small":7,"s":"a\"b","nested":{"k":1}},
             "maxValues":{"i":null,"b":null,"x":null,"f":null,"d":null,"ts":null,"ntz":null,"m":null,"small":null,"s":null,"nested":null},
             "nullCount":{"i":0,"s":3},"tightBounds":false}
            """);
        var fs = new MemoryFileSystem().PutBytes(Log + "00000000000000000000.checkpoint.parquet",
            Write(Classic(statsText: false, statsParsed), ProtocolRow, MetadataRow, row));

        string? stats = Snapshot.Load(fs).Files.Single().Stats;

        Assert.Equal(
            """{"numRecords":3,"minValues":{"i":-5,"b":-128,"x":1.5,"f":0.1,"d":"2023-05-31","ts":"2023-05-31T18:58:33.633Z","ntz":"1969-12-31T23:59:59.999","m":-1.50,"small":0.007,"s":"a\"b","nested":{"k":1}},"maxValues":{},"nullCount":{"i":0,"s":3},"tightBounds":false}""",
            stats);
        FileStats parsed = FileStats.Parse(stats)!;
        Assert.True(parsed.TryGetMin(["s"], out var s));
        Assert.Equal("a\"b", s.GetString());
        Assert.False(parsed.TightBounds);
    }

    [Fact]
    public void StatsParsed_NonFiniteFloats_AreStrings()
    {
        ParquetStruct statsParsed = Struct("stats_parsed", I64("numRecords"),
            Struct("minValues", new ParquetLeaf("x", Repetition.Optional, PhysicalType.Double), new ParquetLeaf("y", Repetition.Optional, PhysicalType.Double)));
        string row = AddRow("f.parquet").Replace("\"stats\":null", "\"stats_parsed\":" + """{"numRecords":1,"minValues":{"x":"-Infinity","y":"NaN"}}""");
        var fs = new MemoryFileSystem().PutBytes(Log + "00000000000000000000.checkpoint.parquet",
            Write(Classic(statsText: false, statsParsed), ProtocolRow, MetadataRow, row));

        Assert.Equal("""{"numRecords":1,"minValues":{"x":"-Infinity","y":"NaN"}}""", Snapshot.Load(fs).Files.Single().Stats);
    }

    [Fact]
    public void Checkpoint_IsEnough_WhenOlderCommitsAreGone()
    {
        var fs = new MemoryFileSystem()
            .PutCheckpoint(Checkpoint2, ProtocolRow, MetadataRow, AddRow("a.parquet"))
            .Commit(2, Lines.Add("ignored-already-in-checkpoint.parquet"))
            .Commit(3, Lines.Add("c.parquet"));

        Assert.Equal(["a.parquet", "c.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path).Order());
        Assert.Equal(["a.parquet"], Snapshot.Load(fs, 2).Files.Select(f => f.Path));

        // Without checkpoints the table cannot be read, and the error says why.
        Assert.Contains("needs a checkpoint", Assert.Throws<DeltaException>(() => Snapshot.Load(fs, null, useCheckpoints: false)).Message);
    }

    [Fact]
    public void VersionBeforeTheCheckpoint_ReplaysFromTheStart()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", """{"version":2,"size":5}""");

        Assert.Equal(["a.parquet"], Snapshot.Load(fs, 1).Files.Select(f => f.Path));
        Assert.Empty(fs.RangeReads);
    }

    [Fact]
    public void LastCheckpointHint_IsUsed_AndAStaleOrCorruptOneIsHarmless()
    {
        MemoryFileSystem good = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", """{"version":2,"size":5}""");
        Assert.Equal(3, Snapshot.Load(good).Version);

        // Points at a checkpoint that does not exist: the whole log is listed instead.
        MemoryFileSystem stale = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", """{"version":3,"size":5}""");
        Assert.Equal(["b.parquet", "c.parquet"], Snapshot.Load(stale).Files.Select(f => f.Path).Order());

        MemoryFileSystem corrupt = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", "{oops");
        Assert.Equal(3, Snapshot.Load(corrupt).Version);
    }

    [Fact]
    public void DamagedCheckpoint_IsAnError_NamingTheFile()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2();
        byte[] bytes = fs.ReadAll(Checkpoint2)!;
        fs.PutBytes(Checkpoint2, bytes[..^20]); // the footer cut off

        DeltaException e = Assert.Throws<DeltaException>(() => Snapshot.Load(fs));
        Assert.Contains(Checkpoint2, e.Message);
        Assert.IsType<ParquetFormatException>(e.InnerException);
    }

    [Fact]
    public void MultiPartCheckpoint_NeedsAllItsParts()
    {
        const string Part1 = Log + "00000000000000000001.checkpoint.0000000001.0000000002.parquet";
        const string Part2 = Log + "00000000000000000001.checkpoint.0000000002.0000000002.parquet";
        static MemoryFileSystem Base() => new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata("\"p\""), Lines.Add("a.parquet"))
            .Commit(1, Lines.Add("b.parquet"))
            .PutCheckpoint(Part1, ProtocolRow, MetadataRow, AddRow("a.parquet"));

        MemoryFileSystem whole = Base().PutCheckpoint(Part2, AddRow("b.parquet"));
        Assert.Equal(["a.parquet", "b.parquet"], Snapshot.Load(whole).Files.Select(f => f.Path).Order());
        Assert.Equal([Part1, Part2], CheckpointReads(whole));

        // One part missing: the checkpoint is ignored and the commits are replayed.
        MemoryFileSystem partial = Base();
        Assert.Equal(["a.parquet", "b.parquet"], Snapshot.Load(partial).Files.Select(f => f.Path).Order());
        Assert.Empty(partial.RangeReads);
    }

    [Fact]
    public void V2Checkpoint_ReadsItsSidecars()
    {
        const string uuid = "80a083e8-7026-4e79-81be-64bd76c43a11";
        byte[] s1 = Write(Classic(), AddRow("a.parquet"));
        byte[] s2 = Write(Classic(), AddRow("b.parquet"), RemoveRow);
        var fs = new MemoryFileSystem()
            .Put(Log + $"00000000000000000004.checkpoint.{uuid}.json", string.Join("\n",
                """{"checkpointMetadata":{"version":4}}""",
                """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint"]}}""",
                Lines.Metadata(),
                $$$"""{"sidecar":{"path":"s1.parquet","sizeInBytes":{{{s1.Length}}},"modificationTime":1}}""",
                // A sidecar may also be an absolute URI, which the file system resolves; without a size it is read whole.
                """{"sidecar":{"path":"s3://elsewhere/side/s2.parquet","modificationTime":1}}"""))
            .PutBytes(Log + "_sidecars/s1.parquet", s1)
            .PutBytes("s3://elsewhere/side/s2.parquet", s2)
            .Commit(5, Lines.Remove("a.parquet"));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(5, snapshot.Version);
        Assert.Equal(["b.parquet"], snapshot.Files.Select(f => f.Path));
        Assert.Equal([Log + "_sidecars/s1.parquet"], CheckpointReads(fs));
        Assert.Contains("s3://elsewhere/side/s2.parquet", fs.Reads);

        // The protocol and metadata are in the top-level file: no sidecar is read for them.
        fs.RangeReads.Clear();
        fs.Reads.Clear();
        Assert.Equal(5, Snapshot.LoadHead(fs).Version);
        Assert.Empty(fs.RangeReads);
        Assert.DoesNotContain("s3://elsewhere/side/s2.parquet", fs.Reads);
    }

    [Fact]
    public void V2ParquetCheckpoint_ReadsItsSidecars_ButNotForTheHead()
    {
        const string Top = Log + "00000000000000000004.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet";
        byte[] side = Write(Classic(), AddRow("a.parquet"), AddRow("b.parquet"));
        const string V2Protocol = """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint"]}}""";
        MemoryFileSystem Build() => new MemoryFileSystem()
            .PutBytes(Top, Write(V2(), """{"checkpointMetadata":{"version":4,"tags":null}}""", V2Protocol, MetadataRow,
                $$$"""{"sidecar":{"path":"side.parquet","sizeInBytes":{{{side.Length}}},"modificationTime":1}}"""))
            .PutBytes(Log + "_sidecars/side.parquet", side);

        MemoryFileSystem fs = Build();
        Assert.Equal(["a.parquet", "b.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path).Order());
        MemoryFileSystem head = Build();
        Assert.Equal(4, Snapshot.LoadHead(head).Version);
        Assert.Equal([Top], CheckpointReads(head));
    }

    [Fact]
    public void Maps_NullAndEmpty_AndMissingColumns()
    {
        // A checkpoint without txn and domainMetadata columns, and add without deletion vectors or row tracking.
        List<ParquetNode> schema =
        [
            Struct("add", Str("path"), Map("partitionValues"), I64("size"), I64("modificationTime"), Bool("dataChange"), Map("tags"), Str("stats")),
            Struct("metaData", Str("id"), Struct("format", Str("provider"), Map("options")), Str("schemaString"), Strings("partitionColumns"),
                Map("configuration")),
            Struct("protocol", I32("minReaderVersion"), I32("minWriterVersion")),
        ];
        var fs = new MemoryFileSystem().PutBytes(Log + "00000000000000000000.checkpoint.parquet", Write(schema,
            ProtocolRow,
            Lines.Metadata("\"p\"", "\"k\":\"v\""),
            """{"add":{"path":"null-value.parquet","partitionValues":{"p":null},"size":1,"modificationTime":1,"dataChange":false,"tags":{}}}""",
            """{"add":{"path":"null-map.parquet","partitionValues":null,"size":1,"modificationTime":1,"dataChange":false,"tags":null}}"""));

        Snapshot snapshot = Snapshot.Load(fs);

        AddAction nullValue = snapshot.Files.Single(f => f.Path == "null-value.parquet");
        Assert.True(nullValue.PartitionValues.ContainsKey("p"));
        Assert.Null(nullValue.PartitionValues["p"]);
        Assert.NotNull(nullValue.Tags);
        Assert.Empty(nullValue.Tags);
        AddAction nullMap = snapshot.Files.Single(f => f.Path == "null-map.parquet");
        Assert.Empty(nullMap.PartitionValues);
        Assert.Null(nullMap.Tags);
        Assert.Null(nullMap.DeletionVector);
        Assert.Equal("v", snapshot.Metadata.Configuration["k"]);
        Assert.Empty(snapshot.AppTransactions);
    }

    [Fact]
    public void CheckpointAtTheLatestVersion_NeedsNoCommit()
    {
        var fs = new MemoryFileSystem()
            .PutCheckpoint(Log + "00000000000000000007.checkpoint.parquet", ProtocolRow, MetadataRow, AddRow("a.parquet"))
            .Touch(Log + "00000000000000000007.checkpoint.parquet", DateTimeOffset.UnixEpoch.AddDays(1));

        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Equal(7, snapshot.Version);
        Assert.Equal(DateTimeOffset.UnixEpoch.AddDays(1), snapshot.CommitFileTime);
    }
}
