using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

public class TrackingFileSystemTests
{
    private const string Schema = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""";

    private static MemoryFileSystem Table()
    {
        var fs = new MemoryFileSystem();
        Transaction.Create(fs, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, new MetadataAction { Id = "t", SchemaString = Schema }).Commit();
        return fs;
    }

    [Fact]
    public void ARefusedCommit_DeletesTheFilesWritten()
    {
        MemoryFileSystem fs = Table();
        var tracking = new TrackingFileSystem(fs);
        Snapshot read = Snapshot.Load(fs);
        Assert.True(tracking.TryCreate("part-1.parquet", new byte[] { 1 }));
        // Another writer changes the metadata first: a conflict, so this commit certainly did not land.
        Transaction.Start(fs, read, "SET TBLPROPERTIES").SetMetadata(new MetadataAction { Id = "t", SchemaString = Schema, Description = "x" }).Commit();

        Assert.Throws<DeltaCommitConflictException>(() =>
            Transaction.Start(tracking, read, "WRITE").Add(new AddAction { Path = "part-1.parquet" }).Commit(maxRetries: 0));

        Assert.Equal(["part-1.parquet"], tracking.Rollback());
        Assert.Null(fs.ReadAll("part-1.parquet"));
    }

    [Fact]
    public void ALandedCommit_KeepsItsFiles_AndAFailureBeforeCommittingDoesNot()
    {
        MemoryFileSystem fs = Table();
        var committed = new TrackingFileSystem(fs);
        committed.TryCreate("kept.parquet", new byte[] { 1 });
        Transaction.Start(committed, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "kept.parquet" }).Commit();
        Assert.Empty(committed.Rollback());
        Assert.NotNull(fs.ReadAll("kept.parquet"));

        var failed = new TrackingFileSystem(fs);
        failed.TryCreate("never.parquet", new byte[] { 1 });
        failed.TryCreate("deletion_vector_x.bin", new byte[] { 1 });
        Assert.Equal(["never.parquet", "deletion_vector_x.bin"], failed.Rollback());
        Assert.Null(fs.ReadAll("never.parquet"));
        Assert.NotNull(fs.ReadAll("kept.parquet"));
    }

    [Fact]
    public void ACommitWhoseFateIsUnknown_KeepsTheFiles()
    {
        MemoryFileSystem fs = Table();
        var tracking = new TrackingFileSystem(new FailingCommits(fs));
        tracking.TryCreate("maybe.parquet", new byte[] { 1 });

        Assert.Throws<IOException>(() =>
            Transaction.Start(tracking, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "maybe.parquet" }).Commit());

        Assert.Empty(tracking.Rollback());
        Assert.NotNull(fs.ReadAll("maybe.parquet"));
    }

    /// <summary>A store whose commit writes time out: the put may or may not have happened.</summary>
    private sealed class FailingCommits(MemoryFileSystem inner) : IWritableFileSystem
    {
        public bool TryCreate(string path, ReadOnlyMemory<byte> content) =>
            path.StartsWith("_delta_log/", StringComparison.Ordinal) ? throw new IOException("timeout") : inner.TryCreate(path, content);
        public void Overwrite(string path, ReadOnlyMemory<byte> content) => inner.Overwrite(path, content);
        public bool Delete(string path) => inner.Delete(path);
        public IReadOnlyList<FileEntry> ListAll(string directory) => inner.ListAll(directory);
        public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null) => inner.List(directory, startFrom);
        public byte[]? ReadAll(string path) => inner.ReadAll(path);
        public byte[]? ReadRange(string path, long offset, int length) => inner.ReadRange(path, offset, length);
    }
}
