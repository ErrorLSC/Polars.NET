using System.Net.Http.Headers;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Storage;

namespace Polars.Integration.Tests;

/// <summary>
/// Uploads in parts on the real stores, at the storage layer (managed clients only: the native store has no such
/// requests): parts of the stores' minimum size (5 MiB) and a short tail, replacing and creating, a create refused
/// because the object exists or another writer created it meanwhile, and an aborted upload that leaves nothing.
/// </summary>
internal static class RealUploads
{
    /// <summary>A checkpoint of about 13 MiB written in parts to the store below <paramref name="root"/>, which is deleted after.</summary>
    public static void LargeCheckpoint(string root, IReadOnlyDictionary<string, string> keys)
    {
        CloudFileSystem fs = CloudStorage.TryOpen(root, keys, null, out string? reason) ?? throw new InvalidOperationException(reason);
        try
        {
            DeltaKernelLargeCheckpointTests.Check(fs);
        }
        finally
        {
            foreach (var file in fs.ListAll("")) fs.Delete(file.Path);
        }
    }

    public const int PartSize = 5 * 1024 * 1024;

    public static byte[] Bytes(int length, int seed)
    {
        var bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    /// <summary>Uploads two full parts (at once where the upload allows it) and a tail.</summary>
    public static async Task<(bool Done, byte[] Content)> UploadAsync(ObjectStoreClient client, string key, bool create, int seed)
    {
        byte[] a = Bytes(PartSize, seed), b = Bytes(PartSize, seed + 1), tail = Bytes(1234, seed + 2);
        ObjectUpload upload = await client.StartUploadAsync(key, create, CancellationToken.None);
        UploadedPart[] parts = upload.MaxConcurrency > 1
            ? await Task.WhenAll(upload.PutPartAsync(0, a, CancellationToken.None), upload.PutPartAsync(1, b, CancellationToken.None))
            : [await upload.PutPartAsync(0, a, CancellationToken.None), await upload.PutPartAsync(1, b, CancellationToken.None)];
        bool done = await upload.CompleteAsync(parts, tail, CancellationToken.None);
        return (done, [.. a, .. b, .. tail]);
    }

    /// <summary>
    /// Replaces, creates, refuses, races and aborts on one store, checking what the store holds after each; deletes
    /// what it wrote below <paramref name="prefix"/> (and only that: the cloud's other test classes share the run's
    /// prefix and run alongside, so its cleanup fixture must not run when this class ends).
    /// </summary>
    public static async Task Check(CloudFileSystem fs, string prefix, bool abortRemovesTheUpload, Func<Task>? more = null)
    {
        try
        {
            await CheckCore(fs.Client, prefix, abortRemovesTheUpload);
            if (more is not null) await more();
        }
        finally
        {
            foreach (ObjectInfo info in fs.Client.List(prefix + "/", delimited: false, startAfter: null).ToList()) fs.Client.Delete(info.Key);
        }
    }

    private static async Task CheckCore(ObjectStoreClient client, string prefix, bool abortRemovesTheUpload)
    {

        // Replacing an existing object, and creating a new one.
        string replaced = $"{prefix}/replaced.bin";
        client.Put(replaced, "old"u8.ToArray());
        var (done, content) = await UploadAsync(client, replaced, create: false, seed: 1);
        Assert.True(done);
        Assert.Equal(content, client.Get(replaced));

        string created = $"{prefix}/created.bin";
        (done, content) = await UploadAsync(client, created, create: true, seed: 2);
        Assert.True(done);
        Assert.Equal(content, client.Get(created));

        // Creating it again is refused; the object stays as it was.
        (done, _) = await UploadAsync(client, created, create: true, seed: 3);
        Assert.False(done);
        Assert.Equal(content, client.Get(created));

        // Another writer creates the key while our upload is open: ours is refused, theirs stays.
        string raced = $"{prefix}/raced.bin";
        ObjectUpload upload = await client.StartUploadAsync(raced, create: true, CancellationToken.None);
        UploadedPart part = await upload.PutPartAsync(0, Bytes(PartSize, 4), CancellationToken.None);
        Assert.True(client.PutIfAbsent(raced, "theirs"u8.ToArray()));
        Assert.False(await upload.CompleteAsync([part], Bytes(10, 5), CancellationToken.None));
        Assert.Equal("theirs"u8.ToArray(), client.Get(raced));

        // An aborted upload leaves no object, and (where the store keeps parts) no upload to complete.
        string aborted = $"{prefix}/aborted.bin";
        upload = await client.StartUploadAsync(aborted, create: false, CancellationToken.None);
        part = await upload.PutPartAsync(0, Bytes(PartSize, 6), CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        await upload.AbortAsync(CancellationToken.None);
        if (abortRemovesTheUpload)
            await Assert.ThrowsAsync<StorageException>(() => upload.CompleteAsync([part], Bytes(10, 7), CancellationToken.None));
        Assert.Null(client.Head(aborted));
    }
}

[Trait("RealCloud", "Aws")]
[Collection(RealAws.Collection)]
public sealed class RealCloudAwsUploadTests
{
    [AwsFact]
    public async Task Uploads_InParts()
    {
        string prefix = $"{RealAws.RunPrefix}/uploads-{Guid.NewGuid():N}";
        await RealUploads.Check(Open(), prefix, abortRemovesTheUpload: true);
        // Refused creates and the abort leave no upload behind (S3 would keep billing its parts).
        Assert.Equal(0, RealAwsApi.OpenUploads(prefix + "/"));
    }

    [AwsFact]
    public void ALargeCheckpoint_GoesInParts()
    {
        string prefix = $"{RealAws.RunPrefix}/checkpoint-{Guid.NewGuid():N}";
        RealUploads.LargeCheckpoint($"s3://{RealAws.Bucket}/{prefix}", RealAws.Keys());
        Assert.Equal(0, RealAwsApi.OpenUploads(prefix + "/"));
    }

    private static CloudFileSystem Open() =>
        CloudStorage.TryOpen($"s3://{RealAws.Bucket}", RealAws.Keys(), null, out string? reason) ?? throw new InvalidOperationException(reason);
}

[Trait("RealCloud", "Azure")]
[Collection(RealAzure.Collection)]
public sealed class RealCloudAzureUploadTests
{
    [AzureTheory]
    [InlineData(AzureAccountKind.Blob, AzureSignIn.AccountKey)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.AccountKey)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.ClientSecret)]
    public async Task Uploads_InParts(AzureAccountKind kind, AzureSignIn signIn)
    {
        CloudFileSystem fs = CloudStorage.TryOpen(RealAzure.Url(kind, ""), RealAzure.Keys(kind, signIn), null, out string? reason)
            ?? throw new InvalidOperationException(reason);
        // Uncommitted blocks cannot be removed: an abort leaves them to expire.
        await RealUploads.Check(fs, $"{RealAzure.RunPrefix}/uploads-{Guid.NewGuid():N}", abortRemovesTheUpload: false);
    }

    [AzureTheory]
    [InlineData(AzureAccountKind.Blob)]
    [InlineData(AzureAccountKind.Adls)]
    public void ALargeCheckpoint_GoesInParts(AzureAccountKind kind) =>
        RealUploads.LargeCheckpoint(RealAzure.Url(kind, $"{RealAzure.RunPrefix}/checkpoint-{Guid.NewGuid():N}"), RealAzure.Keys(kind, AzureSignIn.AccountKey));
}

[Trait("RealCloud", "Gcs")]
[Collection(RealGcs.Collection)]
public sealed class RealCloudGcsUploadTests
{
    [GcsFact]
    public async Task Uploads_InParts()
    {
        CloudFileSystem fs = CloudStorage.TryOpen($"gs://{RealGcs.Bucket}", RealGcs.Keys(), null, out string? reason)
            ?? throw new InvalidOperationException(reason);
        string prefix = $"{RealGcs.RunPrefix}/uploads-{Guid.NewGuid():N}";
        // Both kinds of upload make plain binary objects: the completion's XML body does not become the type.
        await RealUploads.Check(fs, prefix, abortRemovesTheUpload: true, async () =>
        {
            Assert.Equal("application/octet-stream", await ContentType($"{prefix}/replaced.bin"));
            Assert.Equal("application/octet-stream", await ContentType($"{prefix}/created.bin"));
        });
    }

    [GcsFact]
    public void ALargeCheckpoint_GoesInParts() =>
        RealUploads.LargeCheckpoint($"gs://{RealGcs.Bucket}/{RealGcs.RunPrefix}/checkpoint-{Guid.NewGuid():N}", RealGcs.Keys());

    private static async Task<string?> ContentType(string key)
    {
        using var http = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Get,
            $"https://storage.googleapis.com/storage/v1/b/{RealGcs.Bucket}/o/{Uri.EscapeDataString(key)}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", RealGcs.AccessToken());
        using HttpResponseMessage response = await http.SendAsync(request);
        response.EnsureSuccessStatusCode();
        using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return document.RootElement.TryGetProperty("contentType", out JsonElement type) ? type.GetString() : null;
    }
}
