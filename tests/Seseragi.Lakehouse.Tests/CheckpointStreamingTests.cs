using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Checkpoints go to the store as a stream: a failure on the way leaves no checkpoint and no hint.</summary>
public class CheckpointStreamingTests
{
    /// <summary>A store whose streamed files fail after some bytes (as a connection dropping mid-upload), counting aborts and bytes.</summary>
    private sealed class FailingStore(MemoryFileSystem inner, long failAfter) : IWritableFileSystem
    {
        public int Aborts;
        public long Written;

        public FileWriteStream OpenWrite(string path, bool createOnly) => new Failing(((IWritableFileSystem)inner).OpenWrite(path, createOnly), failAfter, this);

        private sealed class Failing(FileWriteStream inner, long failAfter, FailingStore owner) : FileWriteStream
        {
            private long _written;

            protected override void WriteCore(ReadOnlySpan<byte> buffer)
            {
                if (_written + buffer.Length > failAfter) throw new IOException("injected: the connection dropped");
                _written += buffer.Length;
                owner.Written += buffer.Length;
                inner.Write(buffer);
            }

            protected override bool CommitCore() => inner.Commit();

            protected override void Abort()
            {
                owner.Aborts++;
                inner.Dispose();
            }

            protected override void Release() => inner.Dispose();
        }

        public bool TryCreate(string path, ReadOnlyMemory<byte> content) => inner.TryCreate(path, content);
        public void Overwrite(string path, ReadOnlyMemory<byte> content) => inner.Overwrite(path, content);
        public bool Delete(string path) => inner.Delete(path);
        public IReadOnlyList<FileEntry> ListAll(string directory) => inner.ListAll(directory);
        public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null) => inner.List(directory, startFrom);
        public byte[]? ReadAll(string path) => inner.ReadAll(path);
        public byte[]? ReadRange(string path, long offset, int length) => inner.ReadRange(path, offset, length);
    }

    private static MetadataAction Metadata(params (string Key, string Value)[] configuration) => new()
    {
        Id = "t",
        SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
        Configuration = configuration.ToDictionary(c => c.Key, c => c.Value),
    };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AFailureMidCheckpoint_LeavesNoCheckpoint_NoHint_AndAbortsTheStream(bool v2)
    {
        var memory = new MemoryFileSystem();
        ProtocolAction protocol = v2
            ? new() { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = ["v2Checkpoint"], WriterFeatures = ["v2Checkpoint"] }
            : new() { MinReaderVersion = 1, MinWriterVersion = 2 };
        Transaction transaction = Transaction.Create(memory, protocol, v2 ? Metadata((Checkpoints.PolicyKey, "v2")) : Metadata());
        for (int i = 0; i < 40_000; i++)
            transaction.Add(new AddAction { Path = $"part-{i:D6}.parquet", Size = i, Stats = $$$"""{"numRecords":{{{i}}},"minValues":{"id":{{{i}}}},"maxValues":{"id":{{{i}}}}}""" });
        transaction.Commit();
        Snapshot snapshot = Snapshot.Load(memory);
        string[] before = [.. memory.ListAll("").Select(e => e.Path)];

        var store = new FailingStore(memory, failAfter: 200_000); // well into the file, past the first batch
        var e = Assert.Throws<IOException>(() => Checkpoints.Write(store, snapshot));
        Assert.Contains("injected", e.Message);
        Assert.Equal(1, store.Aborts);
        Assert.Equal(before, memory.ListAll("").Select(f => f.Path)); // no checkpoint, no _last_checkpoint

        // The same write without the failure lands.
        string path = Checkpoints.Write(memory, snapshot);
        Assert.NotNull(memory.ReadAll(path));
        Assert.NotNull(memory.ReadAll("_delta_log/_last_checkpoint"));
    }

    [Fact]
    public void AFailureOutsideTheWriter_SendsNoRowGroup_AndAbortsTheStream()
    {
        // The last add's partition value does not fit its byte column: the checkpoint fails while building its rows,
        // the others buffered in its one row group. The writer is aborted: nothing of that row group (nor a footer) is
        // encoded and sent.
        var memory = new MemoryFileSystem();
        var metadata = new MetadataAction
        {
            Id = "t",
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"p","type":"byte","nullable":true,"metadata":{}}]}""",
            PartitionColumns = ["p"],
        };
        Transaction transaction = Transaction.Create(memory, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata);
        for (int i = 0; i < 40_000; i++)
            transaction.Add(new AddAction { Path = $"p=1/part-{i:D6}.parquet", PartitionValues = new Dictionary<string, string?> { ["p"] = "1" }, Size = i, Stats = $$$"""{"numRecords":{{{i}}},"minValues":{"id":{{{i}}}},"maxValues":{"id":{{{i}}}}}""" });
        transaction.Add(new AddAction { Path = "p=1000/part-wide.parquet", PartitionValues = new Dictionary<string, string?> { ["p"] = "1000" }, Size = 1 });
        transaction.Commit();
        Snapshot snapshot = Snapshot.Load(memory);
        string[] before = [.. memory.ListAll("").Select(e => e.Path)];

        var store = new FailingStore(memory, failAfter: long.MaxValue);
        Assert.Throws<OverflowException>(() => Checkpoints.Write(store, snapshot));
        Assert.Equal(1, store.Aborts);
        Assert.InRange(store.Written, 0, 4); // at most the leading magic
        Assert.Equal(before, memory.ListAll("").Select(f => f.Path));
    }
}
