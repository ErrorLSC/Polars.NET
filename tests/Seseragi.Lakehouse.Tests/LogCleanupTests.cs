using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

public class LogCleanupTests
{
    private static (MemoryFileSystem Fs, Snapshot Latest) TableWithCheckpoints(params (string Key, string Value)[] configuration)
    {
        var fs = new MemoryFileSystem();
        var metadata = new MetadataAction
        {
            Id = "t",
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
            Configuration = configuration.Append((Key: Checkpoints.IntervalKey, Value: "2")).ToDictionary(c => c.Key, c => c.Value),
        };
        Transaction.Create(fs, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata).Commit();
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
    public void Cleanup_CanBeTurnedOff_AndFollowsTheRetentionProperty()
    {
        var (off, latestOff) = TableWithCheckpoints((LogCleanup.EnabledKey, "false"));
        Assert.Empty(LogCleanup.Run(off, latestOff, DateTimeOffset.UtcNow.AddDays(365)));

        var (shortRetention, latest) = TableWithCheckpoints((LogCleanup.RetentionKey, "interval 1 hour"));
        Assert.Empty(LogCleanup.Run(shortRetention, latest, DateTimeOffset.UtcNow.AddMinutes(30)));
        Assert.NotEmpty(LogCleanup.Run(shortRetention, latest, DateTimeOffset.UtcNow.AddHours(2)));
    }
}
