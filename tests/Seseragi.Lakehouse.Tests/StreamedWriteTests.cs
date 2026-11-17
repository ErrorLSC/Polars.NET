using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Gcs;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Files written as streams: to object stores in parts (fakes with tiny parts), to the local disk, in memory.</summary>
public class StreamedWriteTests
{
    private static readonly RetrySettings Fast = new(3, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5));

    private const int Part = 1000;
    private const int Quantum = 256 * 1024;

    private static byte[] Bytes(int length, int seed)
    {
        var bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    private static CloudFileSystem Root(ObjectStoreClient client, string scheme, int partSize = Part, int concurrency = 8, int maxParts = 10_000)
    {
        HashSet<string> schemes = [scheme];
        return new CloudFileSystem($"{scheme}://bucket/t", "bucket", "t", client, schemes, _ => throw new NotSupportedException())
        {
            UploadPartSize = partSize, UploadConcurrency = concurrency, MaxUploadParts = maxParts,
        };
    }

    private static CloudFileSystem S3(FakeS3 store, int concurrency = 8, int maxParts = 10_000) =>
        Root(new S3Client(store.BucketUrl, "us-east-1", new StaticCredential<AwsCredential>(new("AKID", "secret", null)), Fast), "s3",
            concurrency: concurrency, maxParts: maxParts);

    private static CloudFileSystem Gcs(FakeGcs store, int partSize) =>
        Root(new GcsClient(new GcsSettings(store.Url, new StaticCredential<string>("token")), "bucket", Fast), "gs", partSize);

    /// <summary>Writes the content in uneven pieces (sync or async), so pieces straddle part boundaries.</summary>
    private static async Task Write(FileWriteStream stream, byte[] content, bool async)
    {
        int[] pieces = [1, 7, 333, 999, 1000, 1001, 2500];
        for (int offset = 0, i = 0; offset < content.Length; i++)
        {
            int count = Math.Min(pieces[i % pieces.Length], content.Length - offset);
            if (async) await stream.WriteAsync(content.AsMemory(offset, count));
            else stream.Write(content, offset, count);
            offset += count;
        }
    }

    private static int Count(FakeUploadStore store, string what) => store.Seen.Count(s => s.Contains(what, StringComparison.Ordinal));

    // ---- object stores ----

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task S3_ALargeFile_GoesInEqualParts_AndAppearsOnlyAtCommit(bool async, bool createOnly)
    {
        using var store = new FakeS3();
        byte[] content = Bytes(3 * Part + 456, 1);
        await using (FileWriteStream stream = S3(store).OpenWrite("dir/f.bin", createOnly))
        {
            await Write(stream, content, async);
            Assert.Equal(content.Length, stream.Position);
            Assert.False(store.Objects.ContainsKey("t/dir/f.bin"));
            Assert.True(async ? await stream.CommitAsync() : stream.Commit());
        }
        Assert.Equal(content, store.Objects["t/dir/f.bin"]);
        Assert.Equal(4, Count(store, "partNumber="));
        Assert.Equal(1, Count(store, "?uploads="));
        Assert.Equal(0, store.OpenUploads);
        Assert.All(store.Server.Requests.Where(r => r.PathAndQuery.Contains("partNumber=") && !r.PathAndQuery.Contains("partNumber=4&")),
            r => Assert.Equal(Part, r.Content.Length));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(10)]
    [InlineData(Part)] // exactly one part: still one request
    public async Task S3_AFileOfOnePartAtMost_IsOneRequest(int length)
    {
        using var store = new FakeS3();
        byte[] content = Bytes(length, 2);
        await using (FileWriteStream stream = S3(store).OpenWrite("f", createOnly: true))
        {
            await Write(stream, content, async: true);
            Assert.True(await stream.CommitAsync());
        }
        Assert.Equal(content, store.Objects["t/f"]);
        Assert.Equal(["PUT /bucket/t/f"], store.Seen);
        Assert.Equal("*", store.Server.Requests.Single().Headers["If-None-Match"]);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(5 * Part)]
    public void S3_CreatingAnExistingFile_CommitsFalse_LeavingItAsItWas(int length)
    {
        using var store = new FakeS3();
        store.Objects["t/f"] = [1, 2, 3];
        using (FileWriteStream stream = S3(store).OpenWrite("f", createOnly: true))
        {
            stream.Write(Bytes(length, 3));
            Assert.False(stream.Commit());
        }
        Assert.Equal([1, 2, 3], store.Objects["t/f"]);
        Assert.Equal(0, store.OpenUploads);
    }

    [Fact]
    public async Task S3_DisposingWithoutACommit_AbortsTheUpload_AndLeavesNothing()
    {
        using var store = new FakeS3 { Delay = TimeSpan.FromMilliseconds(20) };
        await using (FileWriteStream stream = S3(store).OpenWrite("f", createOnly: false))
            await Write(stream, Bytes(5 * Part + 1, 4), async: true);
        Assert.Empty(store.Objects);
        Assert.Equal(0, store.OpenUploads);
        Assert.StartsWith("DELETE /bucket/t/f?uploadId=", store.Seen[^1]);
        Assert.Equal(0, Count(store, "POST /bucket/t/f?uploadId="));
    }

    [Fact]
    public void S3_AnUncommittedSmallFile_SendsNothing()
    {
        using var store = new FakeS3();
        using (FileWriteStream stream = S3(store).OpenWrite("f", createOnly: false)) stream.Write(Bytes(10, 5));
        Assert.Empty(store.Seen);
    }

    [Fact]
    public void S3_PartsLargerThanTheFirstBuffer_KeepTheirBytes_AsTheBufferGrows()
    {
        using var store = new FakeS3();
        HashSet<string> schemes = ["s3"];
        var fs = new CloudFileSystem("s3://bucket/t", "bucket", "t",
            new S3Client(store.BucketUrl, "us-east-1", new StaticCredential<AwsCredential>(new("AKID", "secret", null)), Fast), schemes,
            _ => throw new NotSupportedException()) { UploadPartSize = 300_000 };
        byte[] content = Bytes(700_001, 21);
        using (FileWriteStream stream = fs.OpenWrite("f", createOnly: false))
        {
            for (int offset = 0; offset < content.Length; offset += 10_000)
                stream.Write(content.AsSpan(offset, Math.Min(10_000, content.Length - offset)));
            Assert.True(stream.Commit());
        }
        Assert.Equal(content, store.Objects["t/f"]);
        Assert.Equal(3, Count(store, "partNumber="));
    }

    [Fact]
    public void S3_PartsGoAFewAtATime()
    {
        using var store = new FakeS3 { Delay = TimeSpan.FromMilliseconds(30) };
        using (FileWriteStream stream = S3(store, concurrency: 3).OpenWrite("f", createOnly: false))
        {
            stream.Write(Bytes(12 * Part + 1, 6));
            Assert.True(stream.Commit());
        }
        Assert.Equal(3, store.MostInFlight);
        Assert.Equal(13, Count(store, "partNumber="));
    }

    [Fact]
    public void S3_AFailedPart_FailsTheWrite_AndDisposingAbortsTheUpload()
    {
        using var store = new FakeS3();
        store.Refuse(r => r.PathAndQuery.Contains("partNumber=2&", StringComparison.Ordinal), times: 100);
        using (FileWriteStream stream = S3(store, concurrency: 1).OpenWrite("f", createOnly: false))
        {
            var e = Assert.Throws<IOException>(() => stream.Write(Bytes(5 * Part, 7)));
            Assert.Contains("part", e.Message);
        }
        Assert.Empty(store.Objects);
        Assert.Equal(0, store.OpenUploads);
    }

    [Fact]
    public void S3_AFailedLastPart_FailsTheCommit()
    {
        using var store = new FakeS3();
        store.Refuse(r => r.PathAndQuery.Contains("partNumber=2&", StringComparison.Ordinal), times: 100);
        using (FileWriteStream stream = S3(store).OpenWrite("f", createOnly: false))
        {
            stream.Write(Bytes(2 * Part, 8));
            Assert.Throws<IOException>(() => stream.Commit());
        }
        Assert.Empty(store.Objects);
        Assert.Equal(0, store.OpenUploads);
    }

    [Fact]
    public void S3_MoreThanTheMostParts_IsRefusedClearly()
    {
        using var store = new FakeS3();
        using (FileWriteStream stream = S3(store, maxParts: 3).OpenWrite("f", createOnly: false))
        {
            stream.Write(Bytes(3 * Part, 9)); // three parts: the most allowed
            var e = Assert.Throws<IOException>(() => stream.WriteByte(1));
            Assert.Contains("more than 3 parts", e.Message);
        }
        Assert.Equal(0, store.OpenUploads);
        Assert.Empty(store.Objects);
    }

    [Fact]
    public void ACommittedStream_TakesNoMoreBytes_NorASecondCommit()
    {
        using var store = new FakeS3();
        using FileWriteStream stream = S3(store).OpenWrite("f", createOnly: false);
        stream.Write([1]);
        Assert.True(stream.Commit());
        Assert.False(stream.CanWrite);
        Assert.Throws<InvalidOperationException>(() => stream.Write([2]));
        Assert.Throws<InvalidOperationException>(() => stream.Commit());
        Assert.Equal([1], store.Objects["t/f"]);
    }

    [Fact]
    public async Task Gcs_ACreatingStream_IsOneResumableUpload_OnePartAtATime()
    {
        using var store = new FakeGcs { Delay = TimeSpan.FromMilliseconds(5) };
        byte[] content = Bytes(3 * Quantum + 5, 10);
        await using (FileWriteStream stream = Gcs(store, Quantum).OpenWrite("f", createOnly: true))
        {
            await stream.WriteAsync(content);
            Assert.True(await stream.CommitAsync());
        }
        Assert.Equal(content, store.Objects["t/f"]);
        Assert.Equal(1, store.MostInFlight);
    }

    [Fact]
    public void Gcs_AReplacingStream_IsAnXmlMultipartUpload()
    {
        using var store = new FakeGcs();
        byte[] content = Bytes(3 * Part + 5, 11);
        using (FileWriteStream stream = Gcs(store, Part).OpenWrite("f", createOnly: false))
        {
            stream.Write(content);
            Assert.True(stream.Commit());
        }
        Assert.Equal(content, store.Objects["t/f"]);
        Assert.Equal(4, Count(store, "partNumber="));
    }

    [Fact]
    public void Azure_AStream_IsBlocksAndABlockList()
    {
        using var store = new FakeAzureBlocks();
        var keys = new Dictionary<string, string>
        {
            ["account_name"] = "acct", ["access_key"] = Kaminari.Seseragi.Lakehouse.Storage.Azure.AzureSettings.EmulatorKey,
            ["endpoint"] = store.Url, ["allow_http"] = "true",
        };
        CloudFileSystem fs = CloudStorage.TryOpen("az://c/t", keys, Fast, new Dictionary<string, string>(), null, out string? reason)
            ?? throw new InvalidOperationException(reason);
        fs.UploadPartSize = Part;
        byte[] content = Bytes(2 * Part + 5, 12);
        using (FileWriteStream stream = fs.OpenWrite("f", createOnly: true))
        {
            stream.Write(content);
            Assert.True(stream.Commit());
        }
        Assert.Equal(content, store.Objects["t/f"]);
        Assert.Equal(3, store.BlockIdsSeen.Count);
    }

    // ---- local disk ----

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Local_AStream_IsAHiddenFileUntilCommitted(bool async)
    {
        string root = Directory.CreateTempSubdirectory("seseragi-stream").FullName;
        try
        {
            var fs = new LocalFileSystem(root);
            byte[] content = Bytes(100_000, 13);
            await using (FileWriteStream stream = fs.OpenWrite("a/b/f.bin", createOnly: true))
            {
                await Write(stream, content, async);
                Assert.Null(fs.ReadAll("a/b/f.bin"));
                Assert.All(fs.ListAll(""), e => Assert.StartsWith("a/b/.f.bin.", e.Path));
                Assert.True(async ? await stream.CommitAsync() : stream.Commit());
            }
            Assert.Equal(content, fs.ReadAll("a/b/f.bin"));
            Assert.Equal(["a/b/f.bin"], fs.ListAll("").Select(e => e.Path));

            await using (FileWriteStream again = fs.OpenWrite("a/b/f.bin", createOnly: true))
            {
                again.Write([1]);
                Assert.False(async ? await again.CommitAsync() : again.Commit());
            }
            await using (FileWriteStream replace = fs.OpenWrite("a/b/f.bin", createOnly: false))
            {
                replace.Write([2]);
                Assert.True(replace.Commit());
            }
            Assert.Equal([2], fs.ReadAll("a/b/f.bin"));
            Assert.Single(fs.ListAll(""));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Local_AnUncommittedStream_LeavesNothing_AndOfTwoCreatorsOneWins()
    {
        string root = Directory.CreateTempSubdirectory("seseragi-stream").FullName;
        try
        {
            var fs = new LocalFileSystem(root);
            using (FileWriteStream stream = fs.OpenWrite("f", createOnly: false)) stream.Write([1, 2]);
            Assert.Empty(fs.ListAll(""));

            using FileWriteStream first = fs.OpenWrite("g", createOnly: true), second = fs.OpenWrite("g", createOnly: true);
            first.Write([1]);
            second.Write([2]);
            Assert.True(second.Commit());
            Assert.False(first.Commit());
            Assert.Equal([2], fs.ReadAll("g"));
            Assert.Equal(["g"], fs.ListAll("").Select(e => e.Path));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    // ---- the default (in memory) and the tracking wrapper ----

    [Fact]
    public void ByDefault_TheBytesAreWrittenAtCommit_WithOneCall()
    {
        IWritableFileSystem fs = new MemoryFileSystem();
        using (FileWriteStream stream = fs.OpenWrite("f", createOnly: true))
        {
            stream.Write([1, 2, 3]);
            Assert.Null(fs.ReadAll("f"));
            Assert.True(stream.Commit());
        }
        using (FileWriteStream stream = fs.OpenWrite("f", createOnly: true))
        {
            stream.Write([4]);
            Assert.False(stream.Commit());
        }
        using (FileWriteStream stream = fs.OpenWrite("g", createOnly: false)) stream.Write([5]);
        Assert.Equal([1, 2, 3], fs.ReadAll("f"));
        Assert.Null(fs.ReadAll("g"));
    }

    [Fact]
    public void Tracking_RecordsStreamedFilesOnceCreated_AndNeverAnotherWritersFile()
    {
        var inner = new MemoryFileSystem();
        byte[] theirs = [9];
        inner.TryCreate("theirs.parquet", theirs);
        var tracking = new TrackingFileSystem(inner);
        using (FileWriteStream stream = tracking.OpenWrite("mine.parquet", createOnly: true))
        {
            stream.Write([1]);
            Assert.Empty(tracking.Created);
            Assert.True(stream.Commit());
        }
        using (FileWriteStream stream = tracking.OpenWrite("theirs.parquet", createOnly: true))
        {
            stream.Write([2]);
            Assert.False(stream.Commit());
        }
        using (FileWriteStream stream = tracking.OpenWrite("dropped.parquet", createOnly: false)) stream.Write([3]);
        Assert.Equal(["mine.parquet"], tracking.Created);
        Assert.Equal(["mine.parquet"], tracking.Rollback());
        Assert.NotNull(inner.ReadAll("theirs.parquet"));
    }

    [Fact]
    public void Tracking_ACommitWrittenAsAStream_MarksTheOperationCommitted()
    {
        var tracking = new TrackingFileSystem(new MemoryFileSystem());
        tracking.Track("part-0.parquet");
        using (FileWriteStream stream = tracking.OpenWrite("_delta_log/00000000000000000000.json", createOnly: true))
        {
            stream.Write([1]);
            Assert.True(stream.Commit());
        }
        Assert.Empty(tracking.Rollback());
    }

    [Fact]
    public void SyncOnCommit_ReachesTheStoresStream_ThroughTheTrackingWrapper()
    {
        // Data files written without a sync request turn it off on the stream they get: the tracking wrapper must pass
        // it on to the store's stream, which syncs (or not) at commit. A local file commits either way.
        string dir = Path.Combine(Path.GetTempPath(), $"sync_{Guid.NewGuid():N}");
        try
        {
            var local = new LocalFileSystem(dir);
            var tracking = new TrackingFileSystem(local);
            using (FileWriteStream stream = tracking.OpenWrite("a.bin", createOnly: true))
            {
                Assert.True(stream.SyncOnCommit);
                stream.SyncOnCommit = false;
                Assert.False(stream.SyncOnCommit);
                stream.Write("abc"u8);
                Assert.True(stream.Commit());
            }
            Assert.Equal("abc"u8.ToArray(), local.ReadAll("a.bin"));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }
}
