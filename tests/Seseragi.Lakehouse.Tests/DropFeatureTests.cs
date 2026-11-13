using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Seseragi.Lakehouse.Tests;

/// <summary>DROP FEATURE, checked against what delta-spark 4.4 commits for the same tables.</summary>
public class DropFeatureTests
{
    private const string Schema = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""";

    private static ProtocolAction P(int reader, int writer, string[]? readerFeatures = null, string[]? writerFeatures = null) =>
        new() { MinReaderVersion = reader, MinWriterVersion = writer, ReaderFeatures = readerFeatures, WriterFeatures = writerFeatures };

    private static void AssertProtocol(ProtocolAction actual, int reader, int writer, string[]? readerFeatures, string[]? writerFeatures)
    {
        Assert.Equal((reader, writer), (actual.MinReaderVersion, actual.MinWriterVersion));
        Assert.Equal(readerFeatures?.Order(), actual.ReaderFeatures?.Order());
        Assert.Equal(writerFeatures?.Order(), actual.WriterFeatures?.Order());
    }

    private static MemoryFileSystem Table(ProtocolAction protocol, Dictionary<string, string>? configuration = null, int files = 2)
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, protocol, new MetadataAction { Id = "t", SchemaString = Schema, Configuration = configuration ?? [] }).Commit();
        for (int i = 0; i < files; i++)
            Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = $"f{i}.parquet", Size = 1, Stats = "{\"numRecords\":1}" }).Commit();
        return fs;
    }

    private static long Drop(MemoryFileSystem fs, string feature) => DropFeature.Run(fs, () => Snapshot.Load(fs), feature);

    /// <summary>Each commit from a version on: its operation and the actions it holds.</summary>
    private static List<(string Operation, string Actions)> Commits(MemoryFileSystem fs, long from)
    {
        List<(string, string)> commits = [];
        for (long v = from; fs.ReadAll(LogFile.CommitPath(v)) is byte[] bytes; v++)
        {
            string operation = "";
            List<string> kinds = [];
            foreach (string line in Encoding.UTF8.GetString(bytes).Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                using JsonDocument doc = JsonDocument.Parse(line);
                JsonProperty action = doc.RootElement.EnumerateObject().First();
                if (action.Name == "commitInfo") operation = action.Value.GetProperty("operation").GetString()!;
                else kinds.Add(action.Name);
            }
            commits.Add((operation, string.Join(",", kinds.Order(StringComparer.Ordinal)))); // order within a commit means nothing
        }
        return commits;
    }

    private static bool HasCheckpoint(MemoryFileSystem fs, long version) => fs.ReadAll($"_delta_log/{version:D20}.checkpoint.parquet") is not null;

    // ------------------------------------------------------------------ the protocol

    [Fact]
    public void WithoutFeature_GivesTheProtocolsSparkGives()
    {
        // Spark 4.2 (probe outputs): deletion vectors from 3/7 → 1/7 with checkpoint protection.
        AssertProtocol(TableFeatures.WithoutFeature(P(3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants"]), "deletionVectors", true),
            1, 7, null, ["appendOnly", "invariants", "checkpointProtection"]);
        // Column mapping from legacy 2/5: every feature 5 implied listed, but column mapping.
        AssertProtocol(TableFeatures.WithoutFeature(P(2, 5), "columnMapping", true),
            1, 7, null, ["appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "checkpointProtection"]);
        // A writer feature whose drop leaves exactly what a legacy version implies: that version.
        AssertProtocol(TableFeatures.WithoutFeature(P(1, 7, null, ["inCommitTimestamp", "appendOnly", "invariants"]), "inCommitTimestamp", false), 1, 2, null, null);
        AssertProtocol(TableFeatures.WithoutFeature(P(1, 7, null, ["checkConstraints", "appendOnly", "invariants"]), "checkConstraints", false), 1, 2, null, null);
        // Row tracking leaves domain metadata.
        AssertProtocol(TableFeatures.WithoutFeature(P(1, 7, null, ["domainMetadata", "rowTracking", "appendOnly", "invariants"]), "rowTracking", false),
            1, 7, null, ["domainMetadata", "appendOnly", "invariants"]);
        // Deletion vectors from a column-mapped table: reader 2, column mapping implied by it.
        AssertProtocol(TableFeatures.WithoutFeature(
                P(3, 7, ["columnMapping", "deletionVectors"], ["columnMapping", "deletionVectors", "appendOnly", "invariants"]), "deletionVectors", true),
            2, 7, null, ["columnMapping", "appendOnly", "invariants", "checkpointProtection"]);
        // A reader feature left: reader 3 lists it.
        AssertProtocol(TableFeatures.WithoutFeature(
                P(3, 7, ["deletionVectors", "typeWidening"], ["deletionVectors", "typeWidening", "appendOnly", "invariants", "checkpointProtection"]), "typeWidening", true),
            3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants", "checkpointProtection"]);
    }

    // ------------------------------------------------------------------ reader features: barriers and protection

    [Fact]
    public void ReaderFeature_IsDroppedBehindThreeCheckpointedEmptyCommits_UnderCheckpointProtection()
    {
        MemoryFileSystem fs = Table(P(3, 7, ["vacuumProtocolCheck"], ["vacuumProtocolCheck", "appendOnly", "invariants"]));
        long version = Drop(fs, "VacuumProtocolCheck"); // any case

        // Spark: v3-5 Empty Commit (each checkpointed), v6 DROP FEATURE with metaData and protocol, checkpointed too.
        Assert.Equal(6, version);
        Assert.Equal([("Empty Commit", ""), ("Empty Commit", ""), ("Empty Commit", ""), ("DROP FEATURE", "metaData,protocol")], Commits(fs, 3));
        Assert.All([3L, 4L, 5L, 6L], v => Assert.True(HasCheckpoint(fs, v)));
        Snapshot table = Snapshot.Load(fs);
        AssertProtocol(table.Protocol, 1, 7, null, ["appendOnly", "invariants", "checkpointProtection"]);
        Assert.Equal(6, TableFeatures.CheckpointProtectionBoundary(table.Protocol, table.Metadata));
        string drop = Encoding.UTF8.GetString(fs.ReadAll(LogFile.CommitPath(6))!);
        Assert.Contains("\"operationParameters\":{\"featureName\":\"vacuumProtocolCheck\",\"truncateHistory\":false}", drop);
    }

    [Fact]
    public void V2Checkpoint_GoesBackToTheClassicPolicy_SoTheBarriersAreClassic()
    {
        MemoryFileSystem fs = Table(P(3, 7, ["v2Checkpoint"], ["v2Checkpoint", "appendOnly", "invariants"]),
            new Dictionary<string, string> { [Checkpoints.PolicyKey] = "v2" });
        Assert.Equal(7, Drop(fs, "v2Checkpoint"));
        Assert.Equal(("SET TBLPROPERTIES", "metaData"), Commits(fs, 3)[0]);
        Assert.All([4L, 5L, 6L, 7L], v => Assert.True(HasCheckpoint(fs, v)));
        Assert.Equal("classic", Snapshot.Load(fs).Metadata.Configuration[Checkpoints.PolicyKey]);
    }

    // ------------------------------------------------------------------ writer features: one commit

    [Fact]
    public void InCommitTimestamps_AreUnsetThenDropped_ToTheLegacyProtocol()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["inCommitTimestamp", "appendOnly", "invariants"]),
            new Dictionary<string, string> { [CommitTimeline.InCommitTimestampsKey] = "true" });
        Assert.Equal(4, Drop(fs, "inCommitTimestamp"));
        Assert.Equal([("UNSET TBLPROPERTIES", "metaData"), ("DROP FEATURE", "metaData,protocol")], Commits(fs, 3));
        Assert.Contains("\"properties\":\"[\\\"delta.enableInCommitTimestamps\\\"]\",\"ifExists\":true", Encoding.UTF8.GetString(fs.ReadAll(LogFile.CommitPath(3))!));
        Snapshot table = Snapshot.Load(fs);
        AssertProtocol(table.Protocol, 1, 2, null, null);
        Assert.Equal("false", table.Metadata.Configuration[CommitTimeline.InCommitTimestampsKey]); // as Spark leaves it
        Assert.False(HasCheckpoint(fs, 4));
    }

    [Fact]
    public void RowTracking_IsSuspended_TheIdsUnbackfilled_AndTheDomainRemoved()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["domainMetadata", "rowTracking", "appendOnly", "invariants"]),
            new Dictionary<string, string>(RowTracking.WithMaterializedColumnNames(new Dictionary<string, string>())) { [RowTracking.EnableKey] = "true" });
        Assert.All(Snapshot.Load(fs).Files, f => Assert.NotNull(f.BaseRowId));

        Assert.Equal(5, Drop(fs, "rowTracking"));
        Assert.Equal([("SET TBLPROPERTIES", "metaData"), ("ROW TRACKING UNBACKFILL", "add,add"), ("DROP FEATURE", "domainMetadata,metaData,protocol")], Commits(fs, 3));
        Snapshot table = Snapshot.Load(fs);
        Assert.All(table.Files, f => Assert.Null(f.BaseRowId));
        Assert.DoesNotContain(table.Metadata.Configuration.Keys, k => k.Contains("rowTracking", StringComparison.OrdinalIgnoreCase) || k == RowTracking.EnableKey);
        Assert.False(table.DomainMetadata.ContainsKey(RowTracking.Domain)); // removed by the drop commit
        AssertProtocol(table.Protocol, 1, 7, null, ["domainMetadata", "appendOnly", "invariants"]);
    }

    [Fact]
    public void AnInterruptedDrop_IsFinishedByRunningItAgain()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["domainMetadata", "rowTracking", "appendOnly", "invariants"]),
            new Dictionary<string, string>(RowTracking.WithMaterializedColumnNames(new Dictionary<string, string>())) { [RowTracking.EnableKey] = "true" });
        // The first step happened (row tracking suspended), then the drop stopped.
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { [RowTracking.EnableKey] = "false", [RowTracking.SuspendedKey] = "true" });
        Drop(fs, "rowTracking");
        Assert.Equal([("SET TBLPROPERTIES", "metaData"), ("ROW TRACKING UNBACKFILL", "add,add"), ("DROP FEATURE", "domainMetadata,metaData,protocol")], Commits(fs, 3));
    }

    // ------------------------------------------------------------------ TRUNCATE HISTORY

    private static long Truncate(MemoryFileSystem fs, string feature, DateTimeOffset? now = null) =>
        DropFeature.Run(fs, () => Snapshot.Load(fs), feature, now: now, truncateHistory: true);

    [Fact]
    public void TruncateHistory_DropsAReaderFeatureWithoutTraces_InOneRun_WithoutCheckpointProtection()
    {
        MemoryFileSystem fs = Table(P(3, 7, ["vacuumProtocolCheck"], ["vacuumProtocolCheck", "appendOnly", "invariants"]));

        Assert.Equal(4, Truncate(fs, "vacuumProtocolCheck"));

        Assert.Equal([("Empty Commit", ""), ("DROP FEATURE", "metaData,protocol")], Commits(fs, 3));
        Assert.True(HasCheckpoint(fs, 3));
        Assert.False(HasCheckpoint(fs, 4));
        AssertProtocol(Snapshot.Load(fs).Protocol, 1, 2, null, null);
        Assert.Contains("\"truncateHistory\":true", Encoding.UTF8.GetString(fs.ReadAll(LogFile.CommitPath(4))!));
    }

    // The runs that delete history and then read the log from a checkpoint are checked against Spark in the
    // integration tests (SparkDropFeatureTests.TruncateHistory_*).

    [Fact]
    public void TruncateHistory_FirstRemovesTraces_ThenWaitsForTheRetention()
    {
        MemoryFileSystem fs = Table(P(3, 7, ["v2Checkpoint"], ["v2Checkpoint", "appendOnly", "invariants"]),
            new Dictionary<string, string> { [Checkpoints.PolicyKey] = "v2" });

        // Run 1: the policy goes back to classic, an empty commit is checkpointed, and the drop asks to wait.
        Assert.Contains("partially successful", Assert.Throws<DeltaException>(() => Truncate(fs, "v2Checkpoint")).Message);
        Assert.Equal([("SET TBLPROPERTIES", "metaData"), ("Empty Commit", "")], Commits(fs, 3));
        Assert.True(HasCheckpoint(fs, 4));

        // Run 2 at once: no traces left, but the young history still has the policy: nothing deleted, refused again.
        Assert.Contains("historical versions", Assert.Throws<DeltaException>(() => Truncate(fs, "v2Checkpoint")).Message);
        Assert.NotNull(fs.ReadAll(LogFile.CommitPath(0)));
        Assert.Equal([("Empty Commit", "")], Commits(fs, 5));
    }

    [Fact]
    public void TruncateHistory_OfCheckpointProtection_WaitsWhileTheProtectedHistoryIsYoung()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["appendOnly", "invariants", "checkpointProtection"]),
            new Dictionary<string, string> { [TableFeatures.RequireCheckpointProtectionBeforeVersionKey] = "2" });

        Assert.Contains("CheckpointProtection cannot be dropped", Assert.Throws<DeltaException>(() => Truncate(fs, "checkpointProtection")).Message);
        Assert.Equal([("Empty Commit", "")], Commits(fs, 3));
        Assert.True(HasCheckpoint(fs, 3));
        Assert.NotNull(fs.ReadAll(LogFile.CommitPath(0)));
    }

    [Fact]
    public void TruncateHistory_OfCheckpointProtectionWithNothingProtected_UnsetsAndDropsInOneRun()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["appendOnly", "invariants", "checkpointProtection"]));
        long dropped = Truncate(fs, "checkpointProtection");
        Assert.Equal([("UNSET TBLPROPERTIES", "metaData"), ("DROP FEATURE", "metaData,protocol")], Commits(fs, dropped - 1));
        AssertProtocol(Snapshot.Load(fs).Protocol, 1, 2, null, null);
    }

    [Fact]
    public void TruncateHistory_OnlyForReaderFeaturesAndCheckpointProtection()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["appendOnly", "invariants", "domainMetadata", "inCommitTimestamp"]));
        Assert.Contains("does not require history truncation", Assert.Throws<DeltaException>(() => Truncate(fs, "domainMetadata")).Message);
        Assert.Contains("does not require history truncation", Assert.Throws<DeltaException>(() => Truncate(fs, "inCommitTimestamp")).Message);
        Assert.Equal(2, Snapshot.Load(fs).Version);
    }

    // ------------------------------------------------------------------ refusals

    [Fact]
    public void Refusals_FollowSpark_AndChangeNothing()
    {
        MemoryFileSystem fs = Table(P(1, 7, null, ["appendOnly", "invariants", "changeDataFeed", "domainMetadata", "clustering", "checkpointProtection"]));
        string Refused(string feature) => Assert.Throws<DeltaException>(() => Drop(fs, feature)).Message;

        Assert.Contains("not currently present", Refused("deletionVectors"));
        Assert.Contains("not a table feature", Refused("noSuchFeature"));
        Assert.Contains("not supported", Refused("changeDataFeed"));
        Assert.Contains("not supported", Refused("appendOnly"));
        Assert.Contains("not supported", Refused("clustering"));
        Assert.Contains("depend on", Refused("domainMetadata"));        // clustering keeps its columns in a domain
        Assert.Contains("TRUNCATE HISTORY", Refused("checkpointProtection"));
        Assert.Equal(2, Snapshot.Load(fs).Version);

        // A table with check constraints (which this kernel does not write, so the log is written by hand).
        var constrained = new MemoryFileSystem();
        constrained.Put(LogFile.CommitPath(0), Encoding.UTF8.GetString(ActionWriter.WriteCommit(
        [
            P(1, 7, null, ["appendOnly", "invariants", "checkConstraints"]),
            new MetadataAction { Id = "t", SchemaString = Schema, Configuration = new Dictionary<string, string> { ["delta.constraints.positive"] = "id > 0" } },
        ])), DateTimeOffset.UtcNow);
        Assert.Contains("check constraints", Assert.Throws<DeltaException>(() => Drop(constrained, "checkConstraints")).Message);

        MemoryFileSystem dv = Table(P(3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants"]));
        Assert.Contains("engine", Assert.Throws<DeltaException>(() => Drop(dv, "deletionVectors")).Message); // rewrites need the engine
    }
}
