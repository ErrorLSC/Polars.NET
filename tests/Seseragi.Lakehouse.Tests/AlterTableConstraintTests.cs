using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// ADD / DROP CONSTRAINT and ALTER COLUMN SET / DROP DEFAULT as metadata commits, checked against what delta-spark
/// 4.4 writes for them; a new constraint commits only on the version its caller checked.
/// </summary>
public class AlterTableConstraintTests
{
    private const string Schema =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"a","type":"integer","nullable":true,"metadata":{}}]}""";

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

    [Fact]
    public void AddConstraint_KeysTheNameLowerCased_AndRaisesALegacyWriterTo3()
    {
        MemoryFileSystem fs = Table();
        Assert.Equal(1, AlterTable.AddConstraint(fs, Snapshot.Load(fs), "My C", " a > -5 "));
        Snapshot table = Snapshot.Load(fs);
        Assert.Equal("a > -5", table.Metadata.Configuration["delta.constraints.my c"]);
        Assert.Equal((1, 3, null), (table.Protocol.MinReaderVersion, table.Protocol.MinWriterVersion, table.Protocol.WriterFeatures));
        string commit = Commit(fs, 1);
        Assert.Contains("\"operation\":\"ADD CONSTRAINT\"", commit, StringComparison.Ordinal);
        Assert.Contains("\"operationParameters\":{\"name\":\"My C\",\"expr\":\"a > -5\"}", commit, StringComparison.Ordinal);
    }

    [Fact]
    public void AddConstraint_KeepsAProtocolThatHasTheFeature_AndListsItOnTableFeatures()
    {
        MemoryFileSystem legacy = Table(P(1, 4));
        AlterTable.AddConstraint(legacy, Snapshot.Load(legacy), "c", "a > 0");
        Assert.Equal(4, Snapshot.Load(legacy).Protocol.MinWriterVersion);
        Assert.DoesNotContain("\"protocol\"", Commit(legacy, 1), StringComparison.Ordinal);

        // Spark 4.2 on a 3/7 table: checkConstraints appended to the writer features.
        MemoryFileSystem features = Table(P(3, 7, ["deletionVectors"], ["deletionVectors", "invariants", "appendOnly"]));
        AlterTable.AddConstraint(features, Snapshot.Load(features), "c", "a > 0");
        Assert.Equal(["deletionVectors", "invariants", "appendOnly", "checkConstraints"], Snapshot.Load(features).Protocol.WriterFeatures);
    }

    [Fact]
    public void AddConstraint_RefusesATakenOrEmptyName()
    {
        MemoryFileSystem fs = Table(configuration: new() { ["delta.constraints.c1"] = "a > 0" });
        Assert.Equal("Constraint 'c1' already exists. Please delete the old constraint first.",
            Assert.Throws<DeltaException>(() => AlterTable.AddConstraint(fs, Snapshot.Load(fs), "C1", "a > 1")).Message);
        Assert.Throws<DeltaException>(() => AlterTable.AddConstraint(fs, Snapshot.Load(fs), "", "a > 1"));
        Assert.Throws<DeltaException>(() => AlterTable.AddConstraint(fs, Snapshot.Load(fs), "c2", "  "));
        Assert.Equal(0, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void AddConstraint_OnAVersionAnotherWriterMovedPast_Conflicts()
    {
        // The rows the caller checked are those of the version it read: rows appended since were not checked.
        MemoryFileSystem fs = Table();
        Snapshot checkedVersion = Snapshot.Load(fs);
        Transaction.Start(fs, checkedVersion, "WRITE")
            .Add(new AddAction { Path = "part-0.parquet", Size = 1, ModificationTime = 1, DataChange = true, PartitionValues = new Dictionary<string, string?>() })
            .Commit();
        Assert.Throws<DeltaCommitConflictException>(() => AlterTable.AddConstraint(fs, checkedVersion, "c", "a > 0"));
        Assert.Equal(1, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void DropConstraint_MatchesAnyCase_RecordsWhatSparkRecords_AndSkipsAMissingOneOnlyWhenAsked()
    {
        MemoryFileSystem fs = Table(P(1, 3), new() { ["delta.constraints.c1"] = "a > 0", ["delta.constraints.c2"] = "a < 9" });
        Assert.Equal(1, AlterTable.DropConstraint(fs, Snapshot.Load(fs), "C1"));
        Snapshot table = Snapshot.Load(fs);
        Assert.Equal(["delta.constraints.c2"], table.Metadata.Configuration.Keys);
        Assert.Equal(3, table.Protocol.MinWriterVersion);
        Assert.Contains("\"operationParameters\":{\"name\":\"C1\",\"expr\":\"a > 0\",\"existed\":\"true\"}", Commit(fs, 1), StringComparison.Ordinal);

        Assert.Throws<DeltaException>(() => AlterTable.DropConstraint(fs, Snapshot.Load(fs), "c1"));
        Assert.Null(AlterTable.DropConstraint(fs, Snapshot.Load(fs), "c1", ifExists: true));
        Assert.Equal(1, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void SetColumnDefault_StoresTheTextTrimmed_AndAddsTheFeature()
    {
        MemoryFileSystem fs = Table();
        AlterTable.SetColumnDefault(fs, Snapshot.Load(fs), "A", "  '5'  ");
        Snapshot table = Snapshot.Load(fs);
        Assert.Equal("'5'", table.Schema.Field("a")!.Metadata[Constraints.CurrentDefaultKey]);
        // Spark 4.2, the feature on a 1/2 table: 1/7 with the features 2 implies.
        Assert.Equal((1, 7), (table.Protocol.MinReaderVersion, table.Protocol.MinWriterVersion));
        Assert.Equal(["allowColumnDefaults", "appendOnly", "invariants"], table.Protocol.WriterFeatures!.Order());
        string commit = Commit(fs, 1);
        Assert.Contains("\"operation\":\"CHANGE COLUMN\"", commit, StringComparison.Ordinal);
        Assert.Contains("\\\"CURRENT_DEFAULT\\\":\\\"'5'\\\"", commit, StringComparison.Ordinal);

        // Set again: no protocol change.
        AlterTable.SetColumnDefault(fs, Snapshot.Load(fs), "a", "6");
        Assert.DoesNotContain("\"protocol\"", Commit(fs, 2), StringComparison.Ordinal);
    }

    [Fact]
    public void DropColumnDefault_CommitsOnlyWhenTheColumnHasOne()
    {
        MemoryFileSystem fs = Table();
        Assert.Null(AlterTable.DropColumnDefault(fs, Snapshot.Load(fs), "a"));
        AlterTable.SetColumnDefault(fs, Snapshot.Load(fs), "a", "6");
        Assert.Equal(2, AlterTable.DropColumnDefault(fs, Snapshot.Load(fs), "A"));
        Assert.False(Snapshot.Load(fs).Schema.Field("a")!.Metadata.ContainsKey(Constraints.CurrentDefaultKey));
        Assert.Throws<DeltaException>(() => AlterTable.DropColumnDefault(fs, Snapshot.Load(fs), "zz"));
    }
}
