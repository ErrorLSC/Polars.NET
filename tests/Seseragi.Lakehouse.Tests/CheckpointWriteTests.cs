using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

public class CheckpointWriteTests
{
    private static MetadataAction Metadata(params (string Key, string Value)[] configuration) => new()
    {
        Id = "t",
        SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
        Configuration = configuration.ToDictionary(c => c.Key, c => c.Value),
    };

    [Theory]
    [InlineData("interval 1 week", 7 * 24)]
    [InlineData("interval 2 days", 48)]
    [InlineData("INTERVAL 36 HOURS", 36)]
    [InlineData("interval 90 minutes", 1.5)]
    [InlineData("nonsense", 7 * 24)]
    public void TombstoneRetention_ReadsTheIntervalProperty(string value, double hours) =>
        Assert.Equal(TimeSpan.FromHours(hours), Checkpoints.Retention(Metadata((Checkpoints.TombstoneRetentionKey, value))));

    [Fact]
    public void Checkpoints_AreDueEveryInterval_AndTombstonesExpire()
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 };
        var day = new DateTimeOffset(2026, 1, 10, 0, 0, 0, TimeSpan.Zero);
        Transaction.Create(fs, protocol, Metadata((Checkpoints.IntervalKey, "2"), (Checkpoints.TombstoneRetentionKey, "interval 1 day")))
            .Add(new AddAction { Path = "a.parquet" }).Add(new AddAction { Path = "b.parquet" }).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "DELETE")
            .Add(new RemoveAction { Path = "a.parquet", DeletionTimestamp = day.AddDays(-3).ToUnixTimeMilliseconds() })
            .Add(new RemoveAction { Path = "b.parquet", DeletionTimestamp = day.AddHours(-1).ToUnixTimeMilliseconds() })
            .Commit();
        Snapshot v1 = Snapshot.Load(fs);
        Assert.False(Checkpoints.IsDue(v1));
        Assert.Equal(["a.parquet", "b.parquet"], v1.Tombstones.Select(t => t.Path).Order());

        Transaction.Start(fs, v1, "WRITE").Add(new AddAction { Path = "c.parquet" }).Commit();
        Snapshot v2 = Snapshot.Load(fs);
        Assert.True(Checkpoints.IsDue(v2));
        Checkpoints.Write(fs, v2, day);

        // Read back through the fake reader would need Parquet; the bytes are a Parquet file and the hint points at it.
        byte[] file = fs.ReadAll("_delta_log/00000000000000000002.checkpoint.parquet")!;
        Assert.Equal("PAR1"u8.ToArray(), file[..4]);
        Assert.Equal("PAR1"u8.ToArray(), file[^4..]);
        string hint = System.Text.Encoding.UTF8.GetString(fs.ReadAll("_delta_log/_last_checkpoint")!);
        // protocol + metaData + one add + the one unexpired tombstone
        Assert.Contains("\"version\":2,\"size\":4", hint);

        // V2 checkpoint tables are not checkpointed by this kernel.
        var v2Protocol = new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = ["v2Checkpoint"], WriterFeatures = ["v2Checkpoint"] };
        var other = new MemoryFileSystem();
        Transaction.Create(other, v2Protocol, Metadata((Checkpoints.IntervalKey, "1"))).Commit();
        Transaction.Start(other, Snapshot.Load(other), "WRITE").Commit();
        Snapshot withV2 = Snapshot.Load(other);
        Assert.False(Checkpoints.IsDue(withV2));
        Assert.Throws<DeltaProtocolException>(() => Checkpoints.Write(other, withV2));
    }
}
