using System.Text;
using Kaminari.Seseragi.Delta;
using Kaminari.Seseragi.Delta.Engine;

namespace Seseragi.Delta.Tests;

/// <summary>Serves "Parquet" files that are kept as JSON lines in the memory file system, and counts the reads.</summary>
internal sealed class FakeParquetReader(MemoryFileSystem fs) : IParquetReader
{
    public List<string> Reads { get; } = [];

    public byte[] ReadAsJsonLines(string path)
    {
        Reads.Add(path);
        return fs.ReadAll(path) ?? throw new FileNotFoundException(path);
    }
}

public class CheckpointTests
{
    private const string Log = "_delta_log/";

    // Checkpoint rows the way a Parquet reader hands them over: every action column present, the unused ones
    // null or an object of nulls, maps as arrays of key/value entries.
    private static string AddRow(string path, string partitionValue = "x") =>
        "{\"txn\":null,\"add\":{\"path\":\"" + path + "\",\"partitionValues\":[{\"key\":\"p\",\"value\":\"" + partitionValue + "\"}],\"size\":10,\"modificationTime\":5,\"dataChange\":false,\"stats\":null,\"deletionVector\":null},\"remove\":{\"path\":null,\"deletionTimestamp\":null},\"metaData\":null,\"protocol\":null}";

    private const string RemoveRow = """{"add":null,"remove":{"path":"gone.parquet","deletionTimestamp":3,"dataChange":false},"metaData":null,"protocol":{"minReaderVersion":null,"minWriterVersion":null}}""";
    private const string ProtocolRow = """{"add":null,"remove":null,"metaData":null,"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""";

    private static string MetadataRow =>
        Lines.Metadata("\"p\"").Replace("\"configuration\":{}", "\"configuration\":[]").Replace("\"options\":{}", "\"options\":[]");

    private static MemoryFileSystem TableWithCheckpointAt2() => new MemoryFileSystem()
        .Commit(0, Lines.Protocol, Lines.Metadata("\"p\""), Lines.Add("a.parquet"), Lines.Add("gone.parquet"))
        .Commit(1, Lines.Remove("gone.parquet"))
        .Commit(2, Lines.Add("b.parquet"))
        .Put(Log + "00000000000000000002.checkpoint.parquet", string.Join("\n", ProtocolRow, MetadataRow, AddRow("a.parquet"), AddRow("b.parquet", "y"), RemoveRow))
        .Commit(3, Lines.Add("c.parquet"), Lines.Remove("a.parquet"));

    [Fact]
    public void ClassicCheckpoint_PlusLaterCommits_GivesTheState()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2();
        var parquet = new FakeParquetReader(fs);

        Snapshot snapshot = Snapshot.Load(fs, parquet: parquet);

        Assert.Equal(3, snapshot.Version);
        Assert.Equal(["b.parquet", "c.parquet"], snapshot.Files.Select(f => f.Path).Order());
        Assert.Equal("y", snapshot.Files.Single(f => f.Path == "b.parquet").PartitionValues["p"]);
        Assert.Equal(["p"], snapshot.PartitionColumns);
        Assert.Equal([Log + "00000000000000000002.checkpoint.parquet"], parquet.Reads);
    }

    [Fact]
    public void Checkpoint_IsEnough_WhenOlderCommitsAreGone()
    {
        var fs = new MemoryFileSystem()
            .Put(Log + "00000000000000000002.checkpoint.parquet", string.Join("\n", ProtocolRow, MetadataRow, AddRow("a.parquet")))
            .Commit(2, Lines.Add("ignored-already-in-checkpoint.parquet"))
            .Commit(3, Lines.Add("c.parquet"));

        Assert.Equal(["a.parquet", "c.parquet"], Snapshot.Load(fs, parquet: new FakeParquetReader(fs)).Files.Select(f => f.Path).Order());
        Assert.Equal(["a.parquet"], Snapshot.Load(fs, 2, new FakeParquetReader(fs)).Files.Select(f => f.Path));

        // Without a Parquet reader the table cannot be read, and the error says why.
        Assert.Contains("Parquet reader", Assert.Throws<DeltaException>(() => Snapshot.Load(fs)).Message);
    }

    [Fact]
    public void WithoutParquetReader_TheCommitsAloneAreUsed()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2();

        Assert.Equal(["b.parquet", "c.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path).Order());
    }

    [Fact]
    public void VersionBeforeTheCheckpoint_ReplaysFromTheStart()
    {
        MemoryFileSystem fs = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", """{"version":2,"size":5}""");
        var parquet = new FakeParquetReader(fs);

        Assert.Equal(["a.parquet"], Snapshot.Load(fs, 1, parquet).Files.Select(f => f.Path));
        Assert.Empty(parquet.Reads);
    }

    [Fact]
    public void LastCheckpointHint_IsUsed_AndAStaleOrCorruptOneIsHarmless()
    {
        MemoryFileSystem good = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", """{"version":2,"size":5}""");
        Assert.Equal(3, Snapshot.Load(good, parquet: new FakeParquetReader(good)).Version);

        // Points at a checkpoint that does not exist: the whole log is listed instead.
        MemoryFileSystem stale = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", """{"version":3,"size":5}""");
        Assert.Equal(["b.parquet", "c.parquet"], Snapshot.Load(stale, parquet: new FakeParquetReader(stale)).Files.Select(f => f.Path).Order());

        MemoryFileSystem corrupt = TableWithCheckpointAt2().Put(Log + "_last_checkpoint", "{oops");
        Assert.Equal(3, Snapshot.Load(corrupt, parquet: new FakeParquetReader(corrupt)).Version);
    }

    [Fact]
    public void MultiPartCheckpoint_NeedsAllItsParts()
    {
        MemoryFileSystem Base() => new MemoryFileSystem()
            .Commit(0, Lines.Protocol, Lines.Metadata("\"p\""), Lines.Add("a.parquet"))
            .Commit(1, Lines.Add("b.parquet"))
            .Put(Log + "00000000000000000001.checkpoint.0000000001.0000000002.parquet", string.Join("\n", ProtocolRow, MetadataRow, AddRow("a.parquet")));

        MemoryFileSystem whole = Base().Put(Log + "00000000000000000001.checkpoint.0000000002.0000000002.parquet", AddRow("b.parquet"));
        var parquet = new FakeParquetReader(whole);
        Assert.Equal(["a.parquet", "b.parquet"], Snapshot.Load(whole, parquet: parquet).Files.Select(f => f.Path).Order());
        Assert.Equal(2, parquet.Reads.Count);

        // One part missing: the checkpoint is ignored and the commits are replayed.
        MemoryFileSystem partial = Base();
        var unused = new FakeParquetReader(partial);
        Assert.Equal(["a.parquet", "b.parquet"], Snapshot.Load(partial, parquet: unused).Files.Select(f => f.Path).Order());
        Assert.Empty(unused.Reads);
    }

    [Fact]
    public void V2Checkpoint_ReadsItsSidecars()
    {
        const string uuid = "80a083e8-7026-4e79-81be-64bd76c43a11";
        var fs = new MemoryFileSystem()
            .Put(Log + $"00000000000000000004.checkpoint.{uuid}.json", string.Join("\n",
                """{"checkpointMetadata":{"version":4}}""",
                """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint"]}}""",
                Lines.Metadata(),
                """{"sidecar":{"path":"s1.parquet","sizeInBytes":1,"modificationTime":1}}""",
                """{"sidecar":{"path":"s2.parquet","sizeInBytes":1,"modificationTime":1}}"""))
            .Put(Log + "_sidecars/s1.parquet", AddRow("a.parquet"))
            .Put(Log + "_sidecars/s2.parquet", string.Join("\n", AddRow("b.parquet"), RemoveRow))
            .Commit(5, Lines.Remove("a.parquet"));
        var parquet = new FakeParquetReader(fs);

        Snapshot snapshot = Snapshot.Load(fs, parquet: parquet);

        Assert.Equal(5, snapshot.Version);
        Assert.Equal(["b.parquet"], snapshot.Files.Select(f => f.Path));
        Assert.Equal([Log + "_sidecars/s1.parquet", Log + "_sidecars/s2.parquet"], parquet.Reads);
    }

    [Fact]
    public void CheckpointAtTheLatestVersion_NeedsNoCommit()
    {
        var fs = new MemoryFileSystem()
            .Put(Log + "00000000000000000007.checkpoint.parquet", string.Join("\n", ProtocolRow, MetadataRow, AddRow("a.parquet")),
                DateTimeOffset.UnixEpoch.AddDays(1));

        Snapshot snapshot = Snapshot.Load(fs, parquet: new FakeParquetReader(fs));

        Assert.Equal(7, snapshot.Version);
        Assert.Equal(DateTimeOffset.UnixEpoch.AddDays(1), snapshot.CommitFileTime);
    }
}
