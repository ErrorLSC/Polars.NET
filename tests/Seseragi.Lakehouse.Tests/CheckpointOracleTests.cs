using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// Checkpoints written by Spark and Databricks, read with the managed Parquet reader, against the commits they were
/// made from: a snapshot from the checkpoint must equal the one replayed from commit 0.
/// </summary>
public class CheckpointOracleTests
{
    public static TheoryData<string> Tables => new(
        "app-txn-checkpoint",
        "v1-multi-part-struct-stats-only",
        "v2-json-sidecars-struct-stats-only",
        "v2-parquet-sidecars-struct-stats-only",
        "with_checkpoint_no_last_checkpoint",
        "dat/out/reader_tests/generated/no_stats/delta",
        "dat/out/reader_tests/generated/stats_as_struct/delta",
        "dat/out/reader_tests/generated/with_checkpoint/delta");

    [Theory]
    [MemberData(nameof(Tables))]
    public void Checkpoint_EqualsTheCommits(string table)
    {
        string root = Path.Combine(AppContext.BaseDirectory, "TestData", table);
        if (!Directory.Exists(root)) return; // dat/ is fetched separately (see TestData/README.md)
        var fs = new LocalFileSystem(root);

        Snapshot checkpoint = Snapshot.Load(fs);
        Snapshot commits = Snapshot.Load(fs, null, useCheckpoints: false);

        SnapshotAssert.Same(commits, checkpoint);
    }

    [Fact]
    public void NoReplay_ReadsFromItsCheckpointAlone()
    {
        string root = Path.Combine(AppContext.BaseDirectory, "TestData", "dat/out/reader_tests/generated/no_replay/delta");
        if (!Directory.Exists(root)) return;

        Snapshot snapshot = Snapshot.Load(new LocalFileSystem(root));

        Assert.NotEmpty(snapshot.Files);
        Assert.All(snapshot.Files, f => Assert.True(f.Size > 0));
    }
}
