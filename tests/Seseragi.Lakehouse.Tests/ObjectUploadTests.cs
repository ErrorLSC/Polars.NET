using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Azure;
using Kaminari.Seseragi.Lakehouse.Storage.Gcs;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Uploads in parts against in-memory stores that play each protocol, with failures injected.</summary>
public class ObjectUploadTests
{
    private static readonly RetrySettings Fast = new(3, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5));

    private static byte[] Bytes(int length, byte seed) => [.. Enumerable.Range(0, length).Select(i => (byte)(seed + i * 7))];

    private static S3Client S3(FakeS3 store) =>
        new(store.BucketUrl, "us-east-1", new StaticCredential<AwsCredential>(new("AKID", "secret", null)), Fast);

    private static ObjectStoreClient Azure(FakeAzureBlocks store)
    {
        var keys = new Dictionary<string, string>
        {
            ["account_name"] = "acct", ["access_key"] = AzureSettings.EmulatorKey, ["endpoint"] = store.Url, ["allow_http"] = "true",
        };
        return (CloudStorage.TryOpen("az://c/t", keys, Fast, new Dictionary<string, string>(), null, out string? reason)
            ?? throw new InvalidOperationException(reason)).Client;
    }

    private static GcsClient Gcs(FakeGcs store) => new(new GcsSettings(store.Url, new StaticCredential<string>("token")), "bucket", Fast);

    /// <summary>Uploads the parts (all at once where the upload allows it), then completes with the last bytes.</summary>
    private static async Task<bool> Upload(ObjectStoreClient client, string key, bool create, byte[][] parts, byte[] last)
    {
        ObjectUpload upload = await client.StartUploadAsync(key, create, CancellationToken.None);
        UploadedPart[] done;
        if (upload.MaxConcurrency > 1)
        {
            done = await Task.WhenAll(parts.Select((part, i) => upload.PutPartAsync(i, part, CancellationToken.None)));
        }
        else
        {
            done = new UploadedPart[parts.Length];
            for (int i = 0; i < parts.Length; i++) done[i] = await upload.PutPartAsync(i, parts[i], CancellationToken.None);
        }
        return await upload.CompleteAsync(done, last, CancellationToken.None);
    }

    // ---- S3 ----

    [Fact]
    public async Task S3_PartsAndTheLastBytes_MakeTheObject_NumberedFromOne()
    {
        using var store = new FakeS3();
        byte[] a = Bytes(1000, 1), b = Bytes(1000, 2), c = Bytes(10, 3);
        Assert.True(await Upload(S3(store), "dir/x y.bin", create: false, [a, b], c));
        Assert.Equal([.. a, .. b, .. c], store.Objects["dir/x y.bin"]);
        Assert.Equal(0, store.OpenUploads);
        var seen = store.Seen;
        Assert.StartsWith("POST /bucket/dir/x%20y.bin?uploads=", seen[0]);
        Assert.Contains(seen, s => s.StartsWith("PUT /bucket/dir/x%20y.bin?partNumber=3&uploadId=", StringComparison.Ordinal));
        Assert.StartsWith("POST /bucket/dir/x%20y.bin?uploadId=", seen[^1]);
        Assert.Equal(5, seen.Count);
    }

    [Fact]
    public async Task S3_AnEmptyObject_IsOneEmptyPart_AndAnExactMultiple_NoExtraPart()
    {
        using var store = new FakeS3();
        Assert.True(await Upload(S3(store), "empty", create: false, [], []));
        Assert.Empty(store.Objects["empty"]);
        Assert.Single(store.Seen, s => s.StartsWith("PUT /bucket/empty?partNumber=1&", StringComparison.Ordinal));

        byte[] a = Bytes(64, 5);
        Assert.True(await Upload(S3(store), "full", create: false, [a], []));
        Assert.Equal(a, store.Objects["full"]);
        Assert.Single(store.Seen, s => s.StartsWith("PUT /bucket/full?", StringComparison.Ordinal));
    }

    [Fact]
    public async Task S3_EveryStepIsRetried()
    {
        using var store = new FakeS3();
        store.Refuse(r => r.PathAndQuery.EndsWith("?uploads=", StringComparison.Ordinal));
        store.Refuse(r => r.PathAndQuery.Contains("partNumber=1&", StringComparison.Ordinal), times: 2);
        store.Refuse(r => r.PathAndQuery.Contains("partNumber=2&", StringComparison.Ordinal));
        store.Refuse(r => r.Method == "POST" && r.PathAndQuery.Contains("?uploadId=", StringComparison.Ordinal));
        byte[] a = Bytes(100, 1), b = Bytes(7, 2);
        Assert.True(await Upload(S3(store), "k", create: true, [a], b));
        Assert.Equal([.. a, .. b], store.Objects["k"]);
        Assert.Equal(4 + 5, store.Seen.Count);
    }

    [Fact]
    public async Task S3_ACompletionAnswered200WithAnErrorDocument_IsSentAgain()
    {
        using var store = new FakeS3 { ErrorBodies = 1 };
        Assert.True(await Upload(S3(store), "k", create: false, [Bytes(10, 1)], Bytes(3, 2)));
        Assert.Equal(2, store.Seen.Count(s => s.StartsWith("POST /bucket/k?uploadId=", StringComparison.Ordinal)));
        Assert.Equal(13, store.Objects["k"].Length);
    }

    [Theory]
    [InlineData(false, false)] // AWS: the completion sent again is answered 200
    [InlineData(true, false)]  // MinIO, replacing: 404 NoSuchUpload, the upload is gone
    [InlineData(true, true)]   // MinIO, creating: 412 (the object is there), the upload is gone
    public async Task S3_ACompletionWhoseAnswerWasLost_IsRecognizedAsOurs(bool forgetCompleted, bool create)
    {
        using var store = new FakeS3 { ForgetCompleted = forgetCompleted };
        store.Lose(r => r.Method == "POST" && r.PathAndQuery.Contains("?uploadId=", StringComparison.Ordinal));
        byte[] a = Bytes(50, 1), b = Bytes(5, 9);
        Assert.True(await Upload(S3(store), "k", create, [a], b));
        Assert.Equal([.. a, .. b], store.Objects["k"]);
        Assert.Equal(forgetCompleted, store.Seen.Any(s => s.StartsWith("GET /bucket/k?", StringComparison.Ordinal) && s.Contains("max-parts=1")));
    }

    [Fact]
    public async Task S3_CreatingAnExistingObject_IsRefused_AndTheUploadAborted()
    {
        using var store = new FakeS3();
        store.Objects["k"] = [1, 2, 3];
        Assert.False(await Upload(S3(store), "k", create: true, [Bytes(10, 1)], Bytes(2, 2)));
        Assert.Equal([1, 2, 3], store.Objects["k"]);
        Assert.Equal(0, store.OpenUploads);
        Assert.StartsWith("DELETE /bucket/k?uploadId=", store.Seen[^1]);
        Assert.Contains("POST", store.Seen[^2]);
    }

    [Fact]
    public async Task S3_AnotherCreateInProgress_409UntilTheRetriesRunOut_IsRefused()
    {
        using var store = new FakeS3 { ConditionalAnswer = 409 };
        Assert.False(await Upload(S3(store), "k", create: true, [], Bytes(2, 2)));
        Assert.Equal(1 + Fast.MaxRetries, store.Seen.Count(s => s.StartsWith("POST /bucket/k?uploadId=", StringComparison.Ordinal)));
        Assert.Equal(0, store.OpenUploads);
        Assert.False(store.Objects.ContainsKey("k"));
    }

    [Fact]
    public async Task S3_AStoreWithoutConditionalCompletions_FailsClearly_AndAborts()
    {
        using var store = new FakeS3 { ConditionalAnswer = 501 };
        var e = await Assert.ThrowsAsync<StorageException>(() => Upload(S3(store), "k", create: true, [], Bytes(2, 2)));
        Assert.Contains("conditional", e.Message);
        Assert.Equal(0, store.OpenUploads);
    }

    [Fact]
    public async Task S3_Abort_RemovesTheUpload_AndTwiceIsHarmless()
    {
        using var store = new FakeS3();
        S3Client client = S3(store);
        ObjectUpload upload = await client.StartUploadAsync("k", create: false, CancellationToken.None);
        await upload.PutPartAsync(0, Bytes(10, 1), CancellationToken.None);
        Assert.Equal(1, store.OpenUploads);
        await upload.AbortAsync(CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        Assert.Equal(0, store.OpenUploads);
        Assert.Empty(store.Objects);
    }

    [Fact]
    public async Task S3_PartsGivenOutOfOrder_AreRefused()
    {
        using var store = new FakeS3();
        ObjectUpload upload = await S3(store).StartUploadAsync("k", create: false, CancellationToken.None);
        UploadedPart part = await upload.PutPartAsync(1, Bytes(10, 1), CancellationToken.None);
        await Assert.ThrowsAsync<ArgumentException>(() => upload.CompleteAsync([part], ReadOnlyMemory<byte>.Empty, CancellationToken.None));
    }

    // ---- Azure ----

    [Fact]
    public async Task Azure_BlocksAndTheBlockList_MakeTheBlob_WithIdsOfOneLength()
    {
        using var store = new FakeAzureBlocks();
        store.Refuse(r => r.PathAndQuery.Contains("comp=block&", StringComparison.Ordinal)); // the retry reuses the block's id
        byte[] a = Bytes(1000, 1), b = Bytes(1000, 2), c = Bytes(10, 3);
        Assert.True(await Upload(Azure(store), "t/x y.bin", create: false, [a, b], c));
        Assert.Equal([.. a, .. b, .. c], store.Objects["t/x y.bin"]);
        Assert.Equal(3, store.BlockIdsSeen.Distinct().Count());
        Assert.Single(store.BlockIdsSeen.Select(id => id.Length).Distinct());
        Assert.Contains("comp=blocklist", store.Seen[^1]);
    }

    [Fact]
    public async Task Azure_AnEmptyObject_IsAnEmptyBlockList()
    {
        using var store = new FakeAzureBlocks();
        Assert.True(await Upload(Azure(store), "t/e", create: true, [], []));
        Assert.Empty(store.Objects["t/e"]);
        Assert.Single(store.Seen);
    }

    [Fact]
    public async Task Azure_CreatingAnExistingBlob_IsRefused()
    {
        using var store = new FakeAzureBlocks();
        store.PutByOther("t/k", [1, 2]);
        Assert.False(await Upload(Azure(store), "t/k", create: true, [Bytes(10, 1)], Bytes(2, 2)));
        Assert.Equal([1, 2], store.Objects["t/k"]);
    }

    [Theory]
    [InlineData(true)]  // the commit sent again fails its condition; the committed blocks are ours
    [InlineData(false)] // the commit sent again names committed blocks (Latest) and lands again
    public async Task Azure_ACommitWhoseAnswerWasLost_IsRecognizedAsOurs(bool create)
    {
        using var store = new FakeAzureBlocks();
        store.Lose(r => r.PathAndQuery.Contains("comp=blocklist", StringComparison.Ordinal));
        byte[] a = Bytes(50, 1), b = Bytes(5, 9);
        Assert.True(await Upload(Azure(store), "t/k", create, [a], b));
        Assert.Equal([.. a, .. b], store.Objects["t/k"]);
        Assert.Equal(create, store.Seen.Any(s => s.StartsWith("GET", StringComparison.Ordinal) && s.Contains("blocklisttype=committed")));
    }

    [Fact]
    public async Task Azure_ACreateCommittedByAnotherWriterFirst_IsRefused_NotAnError()
    {
        // Another writer's commit discards our uncommitted blocks: our commit then names blocks that do not exist.
        using var store = new FakeAzureBlocks();
        ObjectStoreClient client = Azure(store);
        ObjectUpload upload = await client.StartUploadAsync("t/k", create: true, CancellationToken.None);
        UploadedPart part = await upload.PutPartAsync(0, Bytes(10, 1), CancellationToken.None);
        store.PutByOther("t/k", [9]);
        Assert.False(await upload.CompleteAsync([part], Bytes(2, 2), CancellationToken.None));
        Assert.Equal([9], store.Objects["t/k"]);
    }

    [Fact]
    public async Task Azure_AnotherCreateOfAMissingBlobWithoutOurBlocks_IsAnError()
    {
        using var store = new FakeAzureBlocks();
        ObjectUpload upload = await Azure(store).StartUploadAsync("t/k", create: true, CancellationToken.None);
        var e = await Assert.ThrowsAsync<StorageException>(() => upload.CompleteAsync([new UploadedPart(0, "bm9uZQ==")], ReadOnlyMemory<byte>.Empty, CancellationToken.None));
        Assert.Equal("InvalidBlockList", e.Code);
    }

    // ---- GCS: XML multipart (replacing) ----

    [Fact]
    public async Task Gcs_Replacing_IsAnXmlMultipartUpload_WithRawETags()
    {
        using var store = new FakeGcs();
        byte[] a = Bytes(1000, 1), b = Bytes(1000, 2), c = Bytes(10, 3);
        store.Refuse(r => r.Method == "POST" && r.PathAndQuery.Contains("?uploadId=", StringComparison.Ordinal));
        Assert.True(await Upload(Gcs(store), "dir/x y.bin", create: false, [a, b], c));
        Assert.Equal([.. a, .. b, .. c], store.Objects["dir/x y.bin"]);
        Assert.Equal(0, store.OpenUploads);
        Assert.StartsWith("POST /bucket/dir/x%20y.bin?uploads", store.Seen[0]);
        SeenRequest complete = store.Server.Requests.Last();
        Assert.Contains("<ETag>\"", complete.Body);
        Assert.Equal("application/xml", complete.Headers["Content-Type"]);
        Assert.Equal("Bearer token", complete.Headers["Authorization"]);
    }

    [Fact]
    public async Task Gcs_ReplacingWithNothing_AbortsAndWritesAnEmptyObject()
    {
        using var store = new FakeGcs();
        store.Objects["k"] = [1];
        Assert.True(await Upload(Gcs(store), "k", create: false, [], []));
        Assert.Empty(store.Objects["k"]);
        Assert.Equal(0, store.OpenUploads);
        Assert.Contains(store.Seen, s => s.StartsWith("DELETE /bucket/k?uploadId=", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Gcs_AbortingAMultipartUpload_RemovesIt()
    {
        using var store = new FakeGcs();
        ObjectUpload upload = await Gcs(store).StartUploadAsync("k", create: false, CancellationToken.None);
        await upload.PutPartAsync(0, Bytes(5, 1), CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        Assert.Equal(0, store.OpenUploads);
        Assert.Empty(store.Objects);
    }

    // ---- GCS: JSON resumable (creating) ----

    private const int Quantum = 256 * 1024;

    [Fact]
    public async Task Gcs_Creating_IsAResumableUpload_InOrder_ConditionCheckedAtTheEnd()
    {
        using var store = new FakeGcs();
        byte[] a = Bytes(Quantum, 1), b = Bytes(Quantum, 2), c = Bytes(10, 3);
        Assert.True(await Upload(Gcs(store), "dir/k", create: true, [a, b], c));
        Assert.Equal([.. a, .. b, .. c], store.Objects["dir/k"]);
        var requests = store.Server.Requests.ToList();
        Assert.Equal("POST /upload/storage/v1/b/bucket/o?uploadType=resumable&name=dir%2Fk&ifGenerationMatch=0", store.Seen[0]);
        Assert.Equal(
            [$"bytes 0-{Quantum - 1}/*", $"bytes {Quantum}-{2 * Quantum - 1}/*", $"bytes {2 * Quantum}-{2 * Quantum + 9}/{2 * Quantum + 10}"],
            requests.Skip(1).Select(r => r.Headers["Content-Range"]));
    }

    [Fact]
    public async Task Gcs_CreatingAnExistingObject_IsRefused()
    {
        using var store = new FakeGcs();
        store.Objects["k"] = [7];
        Assert.False(await Upload(Gcs(store), "k", create: true, [Bytes(Quantum, 1)], Bytes(3, 2)));
        Assert.Equal([7], store.Objects["k"]);
        Assert.Equal(0, store.OpenUploads);
    }

    [Fact]
    public async Task Gcs_BytesTheServiceDidNotKeep_AreSentAgain()
    {
        // The service keeps whole quanta: half of a two-quantum chunk, then none of the rest once.
        using var store = new FakeGcs();
        store.KeepAtMost.Enqueue(Quantum);
        store.KeepAtMost.Enqueue(0);
        byte[] a = Bytes(2 * Quantum, 1), b = Bytes(2 * Quantum, 2), c = Bytes(10, 3);
        Assert.True(await Upload(Gcs(store), "k", create: true, [a, b], c));
        Assert.Equal([.. a, .. b, .. c], store.Objects["k"]);
        Assert.Equal(2, store.Server.Requests.Count(r => r.Headers.GetValueOrDefault("Content-Range") == $"bytes {Quantum}-{2 * Quantum - 1}/*"));
    }

    [Fact]
    public async Task Gcs_AChunkTheServiceNeverKeeps_FailsAfterAFewTries()
    {
        using var store = new FakeGcs();
        for (int i = 0; i < 10; i++) store.KeepAtMost.Enqueue(0);
        await Assert.ThrowsAsync<StorageException>(() => Upload(Gcs(store), "k", create: true, [Bytes(Quantum, 1)], []));
    }

    [Fact]
    public async Task Gcs_AFinishTheServiceNeverCompletes_FailsAfterAFewTries()
    {
        using var store = new FakeGcs { NeverFinish = true };
        await Assert.ThrowsAsync<StorageException>(() => Upload(Gcs(store), "k", create: true, [Bytes(Quantum, 1)], Bytes(3, 2)));
        Assert.True(store.Seen.Count < 10);
    }

    [Fact]
    public async Task Gcs_ALostFinalAnswer_AndRetriedChunks_StillLand()
    {
        using var store = new FakeGcs();
        store.Lose(r => r.Headers.GetValueOrDefault("Content-Range")?.StartsWith("bytes 0-", StringComparison.Ordinal) == true);
        store.Lose(r => r.Headers.GetValueOrDefault("Content-Range")?.EndsWith($"/{Quantum + 4}", StringComparison.Ordinal) == true);
        byte[] a = Bytes(Quantum, 1), b = Bytes(4, 3);
        Assert.True(await Upload(Gcs(store), "k", create: true, [a], b));
        Assert.Equal([.. a, .. b], store.Objects["k"]);
    }

    [Fact]
    public async Task Gcs_AnEmptyCreate_FinishesWithTheSizeOnly()
    {
        using var store = new FakeGcs();
        Assert.True(await Upload(Gcs(store), "k", create: true, [], []));
        Assert.Empty(store.Objects["k"]);
        Assert.Equal("bytes */0", store.Server.Requests.Last().Headers["Content-Range"]);
    }

    [Fact]
    public async Task Gcs_AResumableUpload_TakesOnePartAtATime_InOrder_AndAborts()
    {
        using var store = new FakeGcs();
        ObjectUpload upload = await Gcs(store).StartUploadAsync("k", create: true, CancellationToken.None);
        Assert.Equal(1, upload.MaxConcurrency);
        await Assert.ThrowsAsync<InvalidOperationException>(() => upload.PutPartAsync(1, Bytes(Quantum, 1), CancellationToken.None));
        await upload.PutPartAsync(0, Bytes(Quantum, 1), CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        Assert.Equal(0, store.OpenUploads);
        Assert.Empty(store.Objects);
    }
}
