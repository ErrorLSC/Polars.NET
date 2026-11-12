using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

public class LogCleanupTests
{
    private static (MemoryFileSystem Fs, Snapshot Latest) TableWithCheckpoints(params (string Key, string Value)[] configuration) =>
        TableWithCheckpoints(new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, configuration);

    private static (MemoryFileSystem Fs, Snapshot Latest) TableWithCheckpoints(ProtocolAction protocol, params (string Key, string Value)[] configuration)
    {
        var fs = new MemoryFileSystem();
        var metadata = new MetadataAction
        {
            Id = "t",
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
            Configuration = configuration.Append((Key: Checkpoints.IntervalKey, Value: "2")).ToDictionary(c => c.Key, c => c.Value),
        };
        Transaction.Create(fs, protocol, metadata).Commit();
        for (int v = 1; v <= 7; v++)
        {
            Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = $"f{v}.parquet" }).Commit();
            Snapshot s = Snapshot.Load(fs);
            if (Checkpoints.IsDue(s)) Checkpoints.Write(fs, s);
        }
        return (fs, Snapshot.Load(fs));
    }

    private static string[] Log(MemoryFileSystem fs) => [.. fs.List("_delta_log").Select(f => f.Path["_delta_log/".Length..])];

    [Fact]
    public void ExpiredLog_IsRemovedUpToTheNewestExpiredCheckpoint()
    {
        var (fs, latest) = TableWithCheckpoints();
        Assert.Equal(["00000000000000000002.checkpoint.parquet", "00000000000000000004.checkpoint.parquet", "00000000000000000006.checkpoint.parquet"],
            Log(fs).Where(n => n.Contains("checkpoint.parquet")));

        // Within the retention (30 days by default) nothing goes.
        Assert.Empty(LogCleanup.Run(fs, latest));

        // A month later every commit is old: the newest checkpoint (6) is the boundary.
        IReadOnlyList<string> deleted = LogCleanup.Run(fs, latest, DateTimeOffset.UtcNow.AddDays(31));
        Assert.Equal(8, deleted.Count); // commits 0-5 and checkpoints 2 and 4
        Assert.Equal(["00000000000000000006.checkpoint.parquet", "00000000000000000006.json", "00000000000000000007.json", "_last_checkpoint"], Log(fs));
        // Running again finds nothing more.
        Assert.Empty(LogCleanup.Run(fs, latest, DateTimeOffset.UtcNow.AddDays(31)));
    }

    [Fact]
    public void CheckpointProtection_KeepsHistoryBeforeTheBoundary_UntilItCanAllGoAtOnce()
    {
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["appendOnly", "invariants", "checkpointProtection"] };

        // Protected before version 7: the newest expired checkpoint (6) does not reach it, so nothing goes, not even
        // the history before checkpoint 2 that the protection would let go only together with the rest.
        var (fs, latest) = TableWithCheckpoints(protocol, (TableFeatures.RequireCheckpointProtectionBeforeVersionKey, "7"));
        Assert.Empty(LogCleanup.Run(fs, latest, DateTimeOffset.UtcNow.AddDays(31)));
        Assert.Equal(12, Log(fs).Length); // commits 0-7, checkpoints 2, 4, 6, _last_checkpoint

        // Protected before version 5: the cleanup reaches past it, so everything before checkpoint 6 goes in one run.
        var (reached, latestReached) = TableWithCheckpoints(protocol, (TableFeatures.RequireCheckpointProtectionBeforeVersionKey, "5"));
        Assert.Equal(8, LogCleanup.Run(reached, latestReached, DateTimeOffset.UtcNow.AddDays(31)).Count);
        Assert.Equal(["00000000000000000006.checkpoint.parquet", "00000000000000000006.json", "00000000000000000007.json", "_last_checkpoint"], Log(reached));

        // The property without the feature protects nothing (as the protocol reads it).
        var (unprotected, latestUnprotected) = TableWithCheckpoints((TableFeatures.RequireCheckpointProtectionBeforeVersionKey, "7"));
        Assert.Equal(8, LogCleanup.Run(unprotected, latestUnprotected, DateTimeOffset.UtcNow.AddDays(31)).Count);
    }

    [Fact]
    public void Cleanup_CanBeTurnedOff_AndFollowsTheRetentionProperty()
    {
        var (off, latestOff) = TableWithCheckpoints((LogCleanup.EnabledKey, "false"));
        Assert.Empty(LogCleanup.Run(off, latestOff, DateTimeOffset.UtcNow.AddDays(365)));

        var (shortRetention, latest) = TableWithCheckpoints((LogCleanup.RetentionKey, "interval 1 hour"));
        Assert.Empty(LogCleanup.Run(shortRetention, latest, DateTimeOffset.UtcNow.AddMinutes(30)));
        Assert.NotEmpty(LogCleanup.Run(shortRetention, latest, DateTimeOffset.UtcNow.AddHours(2)));
    }

    [Fact]
    public void V2Checkpoints_AreBoundaries_AndUnreferencedOldSidecarsGo()
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = ["v2Checkpoint"], WriterFeatures = ["v2Checkpoint"] };
        var metadata = new MetadataAction
        {
            Id = "t",
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
            Configuration = new Dictionary<string, string> { [Checkpoints.IntervalKey] = "2" },
        };
        Transaction.Create(fs, protocol, metadata).Commit();
        string? latestCheckpoint = null;
        for (int v = 1; v <= 5; v++)
        {
            Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = $"f{v}.parquet" }).Commit();
            Snapshot s = Snapshot.Load(fs);
            if (Checkpoints.IsDue(s)) latestCheckpoint = Checkpoints.Write(fs, s);
        }
        // A sidecar a remaining checkpoint refers to, and two nobody refers to: one old, one recent.
        string referencing = "_delta_log/00000000000000000004.checkpoint.11111111-1111-1111-1111-111111111111.json";
        fs.Put(referencing, """{"checkpointMetadata":{"version":4}}""" + "\n" + """{"sidecar":{"path":"kept.parquet","sizeInBytes":1,"modificationTime":1}}""");
        fs.Put("_delta_log/_sidecars/kept.parquet", "x");
        fs.Put("_delta_log/_sidecars/orphan.parquet", "x");
        fs.Put("_delta_log/_sidecars/fresh.parquet", "x", DateTimeOffset.UtcNow.AddDays(40));

        IReadOnlyList<string> deleted = LogCleanup.Run(fs, Snapshot.Load(fs), DateTimeOffset.UtcNow.AddDays(31));

        string[] log = [.. fs.List("_delta_log").Select(f => f.Path)];
        Assert.Contains(latestCheckpoint!, log);           // the boundary (version 4) stays
        Assert.Contains(referencing, log);
        Assert.DoesNotContain(log, p => p.Contains(".checkpoint.") && p.Contains("00000000000000000002"));
        Assert.DoesNotContain("_delta_log/00000000000000000003.json", log);
        Assert.Contains("_delta_log/00000000000000000004.json", log);
        string[] sidecars = [.. fs.List("_delta_log/_sidecars").Select(f => f.Path)];
        Assert.Equal(["_delta_log/_sidecars/fresh.parquet", "_delta_log/_sidecars/kept.parquet"], sidecars);
        Assert.Contains("_delta_log/_sidecars/orphan.parquet", deleted);
    }
}
