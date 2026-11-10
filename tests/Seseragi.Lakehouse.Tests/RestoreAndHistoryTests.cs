using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

public class RestoreAndHistoryTests
{
    private const string Schema = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""";

    /// <summary>v0 create, v1 adds a and b, v2 replaces a by c, v3 sets a property and turns deletion vectors on.</summary>
    private static MemoryFileSystem Table()
    {
        var fs = new MemoryFileSystem();
        var metadata = new MetadataAction { Id = "t", SchemaString = Schema };
        Transaction.Create(fs, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "a.parquet", Size = 1 }).Add(new AddAction { Path = "b.parquet", Size = 2 }).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "DELETE")
            .Add(new RemoveAction { Path = "a.parquet", DeletionTimestamp = 1 }).Add(new AddAction { Path = "c.parquet", Size = 3 }).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "SET TBLPROPERTIES")
            .SetProtocol(new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = [TableFeatures.DeletionVectors], WriterFeatures = [TableFeatures.DeletionVectors] })
            .SetMetadata(new MetadataAction { Id = "t", SchemaString = Schema, Configuration = new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "true" } })
            .Commit();
        foreach (string file in new[] { "a.parquet", "b.parquet", "c.parquet" }) fs.PutBytes(file, [1]);
        return fs;
    }

    [Fact]
    public void Restore_BringsBackFilesAndMetadata_KeepingTheProtocol()
    {
        MemoryFileSystem fs = Table();
        Snapshot current = Snapshot.Load(fs);

        RestoreResult result = Restore.Run(fs, current, Snapshot.Load(fs, 1));

        Assert.Equal(4, result.Version);
        Assert.Equal((1, 1), (result.RestoredFiles, result.RemovedFiles));
        Snapshot after = Snapshot.Load(fs);
        Assert.Equal(["a.parquet", "b.parquet"], after.Files.Select(f => f.Path).Order());
        Assert.Empty(after.Metadata.Configuration);
        // Not downgraded: deletion vectors stay supported.
        Assert.Equal(7, after.Protocol.MinWriterVersion);
        string commit = System.Text.Encoding.UTF8.GetString(fs.ReadAll("_delta_log/00000000000000000004.json")!);
        Assert.Contains("\"operation\":\"RESTORE\"", commit);
        Assert.Contains("\"version\":\"1\"", commit);
        Assert.DoesNotContain("\"protocol\"", commit);

        // Already as the target: nothing to commit.
        Assert.Null(Restore.Run(fs, after, Snapshot.Load(fs, 1)).Version);
    }

    [Fact]
    public void Restore_CanDowngradeTheProtocol_WhenAllowed()
    {
        MemoryFileSystem fs = Table();
        Restore.Run(fs, Snapshot.Load(fs), Snapshot.Load(fs, 2), protocolDowngradeAllowed: true);
        Snapshot after = Snapshot.Load(fs);
        Assert.Equal((1, 2), (after.Protocol.MinReaderVersion, after.Protocol.MinWriterVersion));
        Assert.Equal(["b.parquet", "c.parquet"], after.Files.Select(f => f.Path).Order());
    }

    [Fact]
    public void Restore_RefusesVacuumedFiles_UnlessToldToIgnoreThem()
    {
        MemoryFileSystem fs = Table();
        fs.Delete("a.parquet");
        Snapshot current = Snapshot.Load(fs);

        Assert.Contains("no longer exist", Assert.Throws<DeltaException>(() => Restore.Run(fs, current, Snapshot.Load(fs, 1))).Message);
        Assert.Equal(current.Version, Snapshot.Load(fs).Version);

        RestoreResult result = Restore.Run(fs, current, Snapshot.Load(fs, 1), ignoreMissingFiles: true);
        Assert.Equal(["a.parquet"], result.MissingFiles);
        Assert.Equal(["b.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path));
    }

    [Fact]
    public void History_IsNewestFirst_WithOperationsAndTimes()
    {
        MemoryFileSystem fs = Table();

        List<HistoryEntry> all = History.Read(fs);

        Assert.Equal([3L, 2, 1, 0], all.Select(e => e.Version));
        Assert.Equal(["SET TBLPROPERTIES", "DELETE", "WRITE", "CREATE TABLE"], all.Select(e => e.CommitInfo!.Operation));
        Assert.True(all.Zip(all.Skip(1)).All(p => p.First.Timestamp > p.Second.Timestamp));
        Assert.Equal([3L, 2], History.Read(fs, limit: 2).Select(e => e.Version));
    }
}
