using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Protocols and commits checked against what delta-spark 4.4 writes for the same ALTER TABLE.</summary>
public class AlterTableTests
{
    private const string Schema =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"s","type":{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}""";

    private static ProtocolAction P(int reader, int writer, string[]? readerFeatures = null, string[]? writerFeatures = null) =>
        new() { MinReaderVersion = reader, MinWriterVersion = writer, ReaderFeatures = readerFeatures, WriterFeatures = writerFeatures };

    private static MemoryFileSystem Table(ProtocolAction? protocol = null, Dictionary<string, string>? configuration = null)
    {
        var fs = new MemoryFileSystem();
        var metadata = new MetadataAction { Id = "t", SchemaString = Schema, Configuration = configuration ?? [] };
        Transaction.Create(fs, protocol ?? P(1, 2), metadata).Commit(DateTimeOffset.FromUnixTimeMilliseconds(1_000));
        return fs;
    }

    private static string Commit(MemoryFileSystem fs, long version) =>
        Encoding.UTF8.GetString(fs.ReadAll(LogFile.CommitPath(version))!);

    private static void AssertProtocol(ProtocolAction actual, int reader, int writer, string[]? readerFeatures, string[]? writerFeatures)
    {
        Assert.Equal((reader, writer), (actual.MinReaderVersion, actual.MinWriterVersion));
        Assert.Equal(readerFeatures?.Order(), actual.ReaderFeatures?.Order());
        Assert.Equal(writerFeatures?.Order(), actual.WriterFeatures?.Order());
    }

    // ------------------------------------------------------------------ protocol rules

    [Fact]
    public void WithFeatures_UpgradesAsSpark()
    {
        // Spark 4.2, ALTER TABLE on a 1/2 table:
        AssertProtocol(TableFeatures.WithFeatures(P(1, 2), [TableFeatures.DeletionVectors]),
            3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants"]);
        AssertProtocol(TableFeatures.WithFeatures(P(1, 2), [TableFeatures.ChangeDataFeed]),
            1, 7, null, ["changeDataFeed", "appendOnly", "invariants"]);
        AssertProtocol(TableFeatures.WithFeatures(P(1, 2), [TableFeatures.ColumnMapping]),
            2, 7, null, ["columnMapping", "appendOnly", "invariants"]);
        AssertProtocol(TableFeatures.WithFeatures(P(1, 2), [TableFeatures.InCommitTimestamp]),
            1, 7, null, ["inCommitTimestamp", "appendOnly", "invariants"]);
        // Then deletion vectors on the column-mapped 2/7 table: reader 3 lists column mapping too.
        AssertProtocol(TableFeatures.WithFeatures(P(2, 7, null, ["columnMapping", "appendOnly", "invariants"]), [TableFeatures.DeletionVectors]),
            3, 7, ["columnMapping", "deletionVectors"], ["columnMapping", "deletionVectors", "appendOnly", "invariants"]);
        // And the change data feed on a 3/7 table: appended to the writer features only.
        AssertProtocol(TableFeatures.WithFeatures(P(3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants"]), [TableFeatures.ChangeDataFeed]),
            3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants", "changeDataFeed"]);
    }

    [Fact]
    public void WithFeatures_KeepsAProtocolThatAlreadySupportsThem()
    {
        ProtocolAction legacy = P(1, 4);
        Assert.Same(legacy, TableFeatures.WithFeatures(legacy, [TableFeatures.AppendOnly, TableFeatures.ChangeDataFeed]));
        ProtocolAction mapped = P(2, 5);
        Assert.Same(mapped, TableFeatures.WithFeatures(mapped, [TableFeatures.ColumnMapping]));
        ProtocolAction features = P(3, 7, ["deletionVectors"], ["deletionVectors"]);
        Assert.Same(features, TableFeatures.WithFeatures(features, [TableFeatures.DeletionVectors]));
    }

    [Fact]
    public void WithVersions_RaisesOnly_AndListsWhatTheOldVersionImplied()
    {
        AssertProtocol(TableFeatures.WithVersions(P(1, 2), null, 4), 1, 4, null, null);
        AssertProtocol(TableFeatures.WithVersions(P(1, 4), null, 7), 1, 7, null,
            ["appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns"]);
        AssertProtocol(TableFeatures.WithVersions(P(2, 5), 3, null), 3, 7, ["columnMapping"],
            ["appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "columnMapping"]);
        ProtocolAction high = P(1, 4);
        Assert.Same(high, TableFeatures.WithVersions(high, 1, 2));
        Assert.Throws<DeltaProtocolException>(() => TableFeatures.WithVersions(high, null, 8));
    }

    [Fact]
    public void Union_TakesTheOldestProtocols()
    {
        // Restoring to a 1/1 table must still work.
        ProtocolAction oldest = P(1, 1);
        Assert.Same(oldest, TableFeatures.Union(oldest, P(1, 1)));
        AssertProtocol(TableFeatures.Union(P(1, 2), oldest), 1, 2, null, null);
        AssertProtocol(TableFeatures.Union(oldest, P(1, 2)), 1, 2, null, null);
    }

    [Fact]
    public void Union_OfALegacyAndAFeatureProtocol_KeepsWhatTheLegacyVersionImplied()
    {
        ProtocolAction union = TableFeatures.Union(P(1, 4), P(3, 7, ["deletionVectors"], ["deletionVectors"]));
        AssertProtocol(union, 3, 7, ["deletionVectors"],
            ["appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "deletionVectors"]);
    }

    // ------------------------------------------------------------------ SetProperties

    [Fact]
    public void SetProperties_TurningDeletionVectorsOn_BringsTheFeature()
    {
        MemoryFileSystem fs = Table();
        long? version = AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "true" });

        Assert.Equal(1, version);
        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants"]);
        Assert.Equal("true", after.Metadata.Configuration["delta.enableDeletionVectors"]);
        string commit = Commit(fs, 1);
        Assert.Contains("\"operation\":\"SET TBLPROPERTIES\"", commit);
        Assert.Contains("\"properties\":\"{\\\"delta.enableDeletionVectors\\\":\\\"true\\\"}\"", commit);
    }

    [Fact]
    public void SetProperties_FeatureProperty_ChangesOnlyTheProtocol()
    {
        MemoryFileSystem fs = Table();
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.feature.changeDataFeed"] = "supported" });

        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 1, 7, null, ["changeDataFeed", "appendOnly", "invariants"]);
        Assert.Empty(after.Metadata.Configuration);
        string commit = Commit(fs, 1);
        Assert.Contains("delta.feature.changedatafeed", commit); // lower-cased, as Spark records it
        Assert.DoesNotContain("\"metaData\"", commit);
    }

    [Fact]
    public void SetProperties_PlainProperties_ChangeOnlyTheMetadata()
    {
        MemoryFileSystem fs = Table();
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string>
        {
            ["my.custom"] = "x",
            ["DELTA.deletedFileRetentionDuration"] = "interval 1 hour", // canonical spelling stored
            ["delta.appendOnly"] = "true", // writer 2 implies the feature
        });

        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 1, 2, null, null);
        Assert.Equal("x", after.Metadata.Configuration["my.custom"]);
        Assert.Equal("interval 1 hour", after.Metadata.Configuration["delta.deletedFileRetentionDuration"]);
        Assert.Equal("true", after.Metadata.Configuration["delta.appendOnly"]);
        Assert.DoesNotContain("\"protocol\"", Commit(fs, 1));
    }

    [Fact]
    public void SetProperties_Versions_RaiseTheProtocol_WithoutBeingStored()
    {
        MemoryFileSystem fs = Table();
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.minWriterVersion"] = "4" });
        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 1, 4, null, null);
        Assert.Empty(after.Metadata.Configuration);
    }

    [Fact]
    public void SetProperties_NothingChanges_CommitsNothing()
    {
        MemoryFileSystem fs = Table(configuration: new() { ["delta.appendOnly"] = "false" });
        Assert.Null(AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.appendOnly"] = "false" }));
        Assert.Null(AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.feature.appendOnly"] = "supported" }));
        Assert.Equal(0, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void SetProperties_RefusesWhatItCannotSet()
    {
        MemoryFileSystem fs = Table();
        Snapshot table = Snapshot.Load(fs);
        static Dictionary<string, string> One(string key, string value) => new() { [key] = value };

        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.noSuchThing", "x")));
        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.constraints.pos", "id > 0")));
        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.columnMapping.mode", "id")));
        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.enableDeletionVectors", "yes")));
        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.minReaderVersion", "4")));
        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.minWriterVersion", "1")));
        Assert.Throws<DeltaException>(() => AlterTable.SetProperties(fs, table, One("delta.feature.variantType", "supported")));
        Assert.Equal(0, Snapshot.Load(fs).Version);

        // Unknown delta. properties are stored when not refused.
        AlterTable.SetProperties(fs, table, One("delta.noSuchThing", "x"), refuseUnknown: false);
        Assert.Equal("x", Snapshot.Load(fs).Metadata.Configuration["delta.noSuchThing"]);
    }

    [Fact]
    public void SetProperties_ColumnMappingByName_KeepsTheNamesAsPhysicalNames()
    {
        MemoryFileSystem fs = Table();
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.columnMapping.mode"] = "name" });

        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 2, 7, null, ["columnMapping", "appendOnly", "invariants"]);
        Assert.Equal(ColumnMappingMode.Name, after.ColumnMappingMode);
        Assert.Equal("3", after.Metadata.Configuration["delta.columnMapping.maxColumnId"]);
        StructField id = after.Schema.Field("id")!;
        StructField x = ((StructType)after.Schema.Field("s")!.Type).Field("x")!;
        Assert.Equal(("id", 1), (id.PhysicalName, id.FieldId));
        Assert.Equal(("x", 3), (x.PhysicalName, x.FieldId));
        Assert.Contains("\"delta.columnMapping.id\":1", after.Metadata.SchemaString); // a number, as Spark writes it

        // Back to none would rewrite every file: refused.
        Assert.Throws<DeltaException>(() =>
            AlterTable.SetProperties(fs, after, new Dictionary<string, string> { ["delta.columnMapping.mode"] = "none" }));
    }

    [Fact]
    public void SetProperties_InCommitTimestampsOnAnExistingTable_StartWithThisCommit()
    {
        MemoryFileSystem fs = Table();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "a.parquet", Size = 1 }).Commit(DateTimeOffset.FromUnixTimeMilliseconds(2_000));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { ["delta.enableInCommitTimestamps"] = "true" }, now: now);

        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 1, 7, null, ["inCommitTimestamp", "appendOnly", "invariants"]);
        Assert.Equal("2", after.Metadata.Configuration[CommitTimeline.EnablementVersionKey]);
        long enabledAt = long.Parse(after.Metadata.Configuration[CommitTimeline.EnablementTimestampKey]);
        Assert.Contains($"\"inCommitTimestamp\":{enabledAt}", Commit(fs, 2));
        Assert.Equal(enabledAt, CommitTimeline.Load(fs, after.Metadata).TimestampOf(2).ToUnixTimeMilliseconds());

        // The next commit follows the in-commit timeline.
        Transaction.Start(fs, after, "WRITE").Add(new AddAction { Path = "b.parquet", Size = 1 }).Commit(DateTimeOffset.FromUnixTimeMilliseconds(5));
        Assert.Contains($"\"inCommitTimestamp\":{enabledAt + 1}", Commit(fs, 3));
    }

    // ------------------------------------------------------------------ AddFeatures

    [Fact]
    public void AddFeatures_DeletionVectors_AlsoTurnsThemOn()
    {
        MemoryFileSystem fs = Table();
        Assert.Equal(1, AlterTable.AddFeatures(fs, Snapshot.Load(fs), ["DeletionVectors"]));

        Snapshot after = Snapshot.Load(fs);
        AssertProtocol(after.Protocol, 3, 7, ["deletionVectors"], ["deletionVectors", "appendOnly", "invariants"]);
        Assert.Equal("true", after.Metadata.Configuration["delta.enableDeletionVectors"]);
        Assert.Contains("delta.feature.deletionvectors", Commit(fs, 1));

        // Present and on already: nothing to commit.
        Assert.Null(AlterTable.AddFeatures(fs, after, [TableFeatures.DeletionVectors]));
    }

    [Fact]
    public void AddFeatures_KeepsAnExplicitlyOffProperty()
    {
        MemoryFileSystem fs = Table(configuration: new() { ["delta.enableChangeDataFeed"] = "false" });
        AlterTable.AddFeatures(fs, Snapshot.Load(fs), [TableFeatures.ChangeDataFeed]);
        Snapshot after = Snapshot.Load(fs);
        Assert.Contains("changeDataFeed", after.Protocol.WriterFeatures!);
        Assert.Equal("false", after.Metadata.Configuration["delta.enableChangeDataFeed"]);
    }

    [Fact]
    public void AddFeatures_TimestampWithoutTimezone_IsTimestampNtz()
    {
        MemoryFileSystem fs = Table();
        AlterTable.AddFeatures(fs, Snapshot.Load(fs), ["TimestampWithoutTimezone"]);
        AssertProtocol(Snapshot.Load(fs).Protocol, 3, 7, ["timestampNtz"], ["timestampNtz", "appendOnly", "invariants"]);
    }

    [Fact]
    public void AddFeatures_RefusesUnknownFeatures_AndARaiseThatIsNotAllowed()
    {
        MemoryFileSystem fs = Table();
        Snapshot table = Snapshot.Load(fs);
        Assert.Throws<DeltaException>(() => AlterTable.AddFeatures(fs, table, ["IcebergCompatV1"]));
        Assert.Throws<DeltaException>(() => AlterTable.AddFeatures(fs, table, ["noSuchFeature"]));
        Assert.Throws<DeltaException>(() => AlterTable.AddFeatures(fs, table, [TableFeatures.DeletionVectors], allowProtocolIncrease: false));
        Assert.Equal(0, Snapshot.Load(fs).Version);

        // At 3/7 no version is raised: allowed.
        MemoryFileSystem modern = Table(P(3, 7, [], ["appendOnly", "invariants"]));
        AlterTable.AddFeatures(modern, Snapshot.Load(modern), [TableFeatures.DeletionVectors], allowProtocolIncrease: false);
        Assert.Contains("deletionVectors", Snapshot.Load(modern).Protocol.ReaderFeatures!);
    }
}
