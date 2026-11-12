using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Row tracking and identity columns, checked against what delta-spark 4.4 writes.</summary>
public class RowTrackingAndIdentityTests
{
    private const string Schema =
        """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"v","type":"string","nullable":true,"metadata":{}}]}""";

    private static ProtocolAction Tracking => new()
    {
        MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = [TableFeatures.DomainMetadata, RowTracking.Feature, "appendOnly", "invariants"],
    };

    private static MetadataAction Metadata(string schema = Schema, Dictionary<string, string>? configuration = null) =>
        new() { Id = "t", SchemaString = schema, Configuration = configuration ?? [] };

    private static AddAction Add(string path, long rows, bool dataChange = true) =>
        new() { Path = path, Size = 10, ModificationTime = 1, DataChange = dataChange, Stats = $$"""{"numRecords":{{rows}}}""" };

    private static List<DeltaAction> Commit(MemoryFileSystem fs, long version) =>
        ActionParser.ParseCommit(fs.ReadAll(LogFile.CommitPath(version))!);

    private static MemoryFileSystem TrackedTable()
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, Tracking, Metadata(configuration: RowTracking.WithMaterializedColumnNames(new Dictionary<string, string> { [RowTracking.EnableKey] = "true" }))).Commit();
        return fs;
    }

    // ------------------------------------------------------------------ row tracking

    [Fact]
    public void NewFiles_GetConsecutiveRowIds_AndTheCommitVersion_AsSpark()
    {
        MemoryFileSystem fs = TrackedTable();
        // Spark 4.2: two files of 1 and 2 rows at version 1 get base ids 0 and 1, high water mark 2.
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 1)).Add(Add("b", 2)).Commit();
        var adds = Commit(fs, 1).OfType<AddAction>().ToList();
        Assert.Equal([0L, 1L], adds.Select(a => a.BaseRowId!.Value));
        Assert.All(adds, a => Assert.Equal(1L, a.DefaultRowCommitVersion));
        var domain = Commit(fs, 1).OfType<DomainMetadataAction>().Single();
        Assert.Equal(RowTracking.Domain, domain.Domain);
        Assert.Equal("""{"rowIdHighWaterMark":2,"domainName":"delta.rowTracking"}""", domain.Configuration);

        // The next file continues after the mark; the snapshot knows it.
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("c", 1)).Commit();
        Assert.Equal(3L, Commit(fs, 2).OfType<AddAction>().Single().BaseRowId);
        Assert.Equal(3L, RowTracking.HighWaterMark(Snapshot.Load(fs).DomainMetadata));

        // A file that keeps its ids (re-added with a deletion vector, say) is left alone, and needs no domain.
        var keep = new AddAction { Path = "a", Size = 10, Stats = """{"numRecords":1}""", BaseRowId = 0, DefaultRowCommitVersion = 1 };
        Transaction.Start(fs, Snapshot.Load(fs), "DELETE").Add(keep).Commit();
        Assert.Equal((0L, 1L), Commit(fs, 3).OfType<AddAction>().Select(a => (a.BaseRowId!.Value, a.DefaultRowCommitVersion!.Value)).Single());
        Assert.Empty(Commit(fs, 3).OfType<DomainMetadataAction>());
    }

    [Fact]
    public void ConcurrentAppends_TakeIdsAfterTheWinner_WithoutConflict()
    {
        MemoryFileSystem fs = TrackedTable();
        Snapshot read = Snapshot.Load(fs);
        Transaction first = Transaction.Start(fs, read, "WRITE").Add(Add("a", 5));
        Transaction second = Transaction.Start(fs, read, "WRITE").Add(Add("b", 2));
        Assert.Equal(1, first.Commit());
        Assert.Equal(2, second.Commit());

        AddAction b = Commit(fs, 2).OfType<AddAction>().Single();
        Assert.Equal(5L, b.BaseRowId);              // after the winner's 0..4
        Assert.Equal(2L, b.DefaultRowCommitVersion); // the version it landed at, not the one it first tried
        Assert.Equal(6L, RowTracking.HighWaterMark(Snapshot.Load(fs).DomainMetadata));
    }

    [Fact]
    public void RowTracking_RefusesFilesWithoutRowCounts_AndAHandWrittenMark()
    {
        MemoryFileSystem fs = TrackedTable();
        var noStats = new AddAction { Path = "a", Size = 10 };
        Assert.Throws<DeltaProtocolException>(() => Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(noStats).Commit());
        Assert.Throws<ArgumentException>(() => Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(RowTracking.DomainFor(9)));
        Assert.Equal(0, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void EnablingRowTracking_OnAnExistingTable_BackfillsTheFilesInOneCommit()
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, Metadata()).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 2)).Add(Add("b", 1)).Commit();

        long? version = AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { [RowTracking.EnableKey] = "true" });
        Assert.Equal(2, version);
        Snapshot table = Snapshot.Load(fs);
        Assert.True(RowTracking.IsEnabled(table.Protocol, table.Metadata));
        Assert.Contains(TableFeatures.DomainMetadata, table.Protocol.WriterFeatures!);
        var (rowId, commitVersion) = RowTracking.MaterializedColumns(table.Metadata)!.Value;
        Assert.StartsWith("_row-id-col-", rowId);
        Assert.StartsWith("_row-commit-version-col-", commitVersion);

        // Spark's backfill: the files again, data unchanged, with ids and the backfill's version.
        var readded = Commit(fs, 2).OfType<AddAction>().ToList();
        Assert.Equal(2, readded.Count);
        Assert.All(readded, a => Assert.False(a.DataChange));
        Assert.Equal([0L, 2L], readded.Select(a => a.BaseRowId!.Value).Order());
        Assert.All(table.Files, f => Assert.Equal(2L, f.DefaultRowCommitVersion));
        Assert.Equal(2L, RowTracking.HighWaterMark(table.DomainMetadata));
    }

    [Fact]
    public void AddingTheFeature_OnlyGivesNewFilesIds()
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, Metadata()).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 2)).Commit();
        AlterTable.AddFeatures(fs, Snapshot.Load(fs), [RowTracking.Feature]);
        Snapshot table = Snapshot.Load(fs);
        Assert.True(RowTracking.IsSupported(table.Protocol));
        Assert.False(RowTracking.IsEnabled(table.Protocol, table.Metadata));
        Assert.NotNull(RowTracking.MaterializedColumns(table.Metadata)); // named as Spark names them
        Assert.Null(table.Files.Single().BaseRowId);

        Transaction.Start(fs, table, "WRITE").Add(Add("b", 4)).Commit();
        Assert.Equal(0L, Snapshot.Load(fs).Files.Single(f => f.Path == "b").BaseRowId);
    }

    [Fact]
    public void SuspendedRowTracking_GivesNewFilesNoIds_AndResumesAfterTheMark_AsSpark()
    {
        MemoryFileSystem fs = TrackedTable();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 2)).Commit(); // ids 0..1

        // Spark 4.2: suspending while tracking is on is DELTA_ROW_TRACKING_ILLEGAL_PROPERTY_COMBINATION.
        var both = Assert.ThrowsAny<DeltaException>(() =>
            AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { [RowTracking.SuspendedKey] = "true" }));
        Assert.Contains("both set to true", both.Message);
        Assert.ThrowsAny<DeltaException>(() =>
            AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { [RowTracking.SuspendedKey] = "yes" }));

        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string>
        {
            [RowTracking.EnableKey] = "false", [RowTracking.SuspendedKey] = "true",
        });
        // Suspended: appends (and rewrites) add files without ids, and no domain.
        long appended = Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("b", 3)).Commit();
        AddAction b = Commit(fs, appended).OfType<AddAction>().Single();
        Assert.Null(b.BaseRowId);
        Assert.Null(b.DefaultRowCommitVersion);
        Assert.Empty(Commit(fs, appended).OfType<DomainMetadataAction>());
        Assert.Equal(1L, RowTracking.HighWaterMark(Snapshot.Load(fs).DomainMetadata));
        // Turning tracking on while suspended is refused.
        Assert.ThrowsAny<DeltaException>(() =>
            AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { [RowTracking.EnableKey] = "true" }));

        // Lifting the suspension and turning tracking on in one change backfills the file in that commit (Spark
        // needs two changes here), after the mark.
        long resumed = AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string>
        {
            [RowTracking.EnableKey] = "true", [RowTracking.SuspendedKey] = "false",
        })!.Value;
        AddAction backfilled = Commit(fs, resumed).OfType<AddAction>().Single();
        Assert.Equal(("b", 2L, resumed, false), (backfilled.Path, backfilled.BaseRowId!.Value, backfilled.DefaultRowCommitVersion!.Value, backfilled.DataChange));
        Snapshot table = Snapshot.Load(fs);
        Assert.True(RowTracking.IsEnabled(table.Protocol, table.Metadata));
        Assert.Equal(4L, RowTracking.HighWaterMark(table.DomainMetadata));
        Assert.All(table.Files, f => Assert.NotNull(f.BaseRowId));
    }

    [Fact]
    public void BothPropertiesOn_AreRefused_AndATableHoldingBoth_CanBeRepaired()
    {
        var configuration = RowTracking.WithMaterializedColumnNames(new Dictionary<string, string>
        {
            [RowTracking.EnableKey] = "true", [RowTracking.SuspendedKey] = "true",
        });
        Assert.ThrowsAny<DeltaException>(() => Transaction.Create(new MemoryFileSystem(), Tracking, Metadata(configuration: configuration)).Commit());

        // Spark 4.2 creates such a table, then refuses every commit to it, the repairing ALTER included; here the
        // ALTER that sets either property to false goes through.
        var fs = new MemoryFileSystem();
        Assert.True(fs.TryCreate(LogFile.CommitPath(0), ActionWriter.WriteCommit([Tracking, Metadata(configuration: configuration)])));
        Assert.ThrowsAny<DeltaException>(() => Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 1)).Commit());
        AlterTable.SetProperties(fs, Snapshot.Load(fs), new Dictionary<string, string> { [RowTracking.SuspendedKey] = "false" });
        long version = Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 1)).Commit();
        Assert.Equal(0L, Commit(fs, version).OfType<AddAction>().Single().BaseRowId);
    }

    // ------------------------------------------------------------------ identity columns

    private const string IdentitySchema =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.identity.step":5,"delta.identity.allowExplicitInsert":false,"delta.identity.start":10}},{"name":"v","type":"string","nullable":true,"metadata":{}}]}""";

    [Fact]
    public void IdentityColumns_AreRead_AndTheirMarksWrittenAsNumbers()
    {
        IdentityColumn id = IdentityColumns.Of(StructType.Parse(IdentitySchema)).Single();
        Assert.Equal(new IdentityColumn("id", 10, 5, null, false), id);
        Assert.Equal(10, id.NextValue);

        MetadataAction marked = IdentityColumns.WithHighWaterMarks(Metadata(IdentitySchema), new Dictionary<string, long> { ["id"] = 20 });
        Assert.Contains("\"delta.identity.highWaterMark\":20", marked.SchemaString);
        Assert.Contains("\"delta.identity.start\":10", marked.SchemaString);
        IdentityColumn after = IdentityColumns.Of(StructType.Parse(marked.SchemaString)).Single();
        Assert.Equal(20, after.HighWaterMark);
        Assert.Equal(25, after.NextValue);

        var down = new IdentityColumn("d", -1, -2, -3, true);
        Assert.Equal(-5, down.NextValue);
        Assert.Throws<DeltaException>(() => new IdentityColumn("x", 1, 1, long.MaxValue, false).NextValue);
    }

    [Fact]
    public void IdentityTables_AreWritable_UnlessTheirMetadataIsBroken()
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = [TableFeatures.IdentityColumns, "appendOnly", "invariants"] };
        Transaction.Create(fs, protocol, Metadata(IdentitySchema)).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(Add("a", 1)).Commit();

        string broken = IdentitySchema.Replace("\"type\":\"long\"", "\"type\":\"integer\"");
        Assert.Throws<DeltaProtocolException>(() => Transaction.Create(new MemoryFileSystem(), protocol, Metadata(broken)).Commit());
    }

    [Fact]
    public void Restore_KeepsTheIdentityHighWaterMark_AsSpark()
    {
        var fs = new MemoryFileSystem();
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = [TableFeatures.IdentityColumns, "appendOnly", "invariants"] };
        MetadataAction v0 = IdentityColumns.WithHighWaterMarks(Metadata(IdentitySchema), new Dictionary<string, long> { ["id"] = 15 });
        Transaction.Create(fs, protocol, v0).Commit();
        MetadataAction v1 = IdentityColumns.WithHighWaterMarks(v0, new Dictionary<string, long> { ["id"] = 40 });
        v1 = new MetadataAction { Id = v1.Id, SchemaString = v1.SchemaString, Configuration = new Dictionary<string, string> { ["x"] = "1" } };
        Transaction.Start(fs, Snapshot.Load(fs), "SET TBLPROPERTIES").SetMetadata(v1).Commit();

        Restore.Run(fs, Snapshot.Load(fs), Snapshot.Load(fs, 0));
        Snapshot restored = Snapshot.Load(fs);
        Assert.Empty(restored.Metadata.Configuration);   // the old metadata is back…
        Assert.Equal(40, IdentityColumns.Of(restored.Schema).Single().HighWaterMark); // …but values given stay given
    }
}
