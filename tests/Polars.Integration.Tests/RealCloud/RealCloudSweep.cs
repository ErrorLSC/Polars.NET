using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Delta;

namespace Polars.Integration.Tests;

/// <summary>A fact that runs only on request (SESERAGI_SWEEP=1), when no other real-cloud run is going on.</summary>
public sealed class SweepFactAttribute : FactAttribute
{
    public SweepFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("SESERAGI_SWEEP") != "1")
            Skip = "Deletes every test run's leftovers in the real clouds: set SESERAGI_SWEEP=1, with no other real-cloud run going on.";
    }
}

/// <summary>
/// Deletes what interrupted real-cloud runs left below <c>seseragi-tests/</c> (a run's own cleanup only runs when the
/// run ends), and aborts S3 uploads left open there. Every cloud that is configured is swept.
/// </summary>
[Trait("RealCloud", "Sweep")]
public sealed class RealCloudSweep
{
    private const string Prefix = "seseragi-tests";

    [SweepFact]
    public void Sweep_DeletesEveryRunsLeftovers()
    {
        List<string> swept = [];
        if (RealAws.Available)
        {
            var open = RealAwsApi.OpenUploadList(Prefix + "/");
            foreach (var (key, uploadId) in open) RealAwsApi.AbortUpload(key, uploadId);
            using var fs = new ObjectStoreFileSystem($"s3://{RealAws.Bucket}/{Prefix}", RealAws.Args(), ObjectStoreBackend.Managed);
            swept.Add($"aws: {DeleteAll(fs)} files, {open.Count} uploads aborted");
            Assert.Empty(fs.ListAll(""));
            Assert.Equal(0, RealAwsApi.OpenUploads(Prefix + "/"));
        }
        if (RealAzure.Available)
        {
            foreach (AzureAccountKind kind in Enum.GetValues<AzureAccountKind>())
            {
                BlobContainerClient container = RealAzure.ContainerClient(kind);
                // On ADLS the listing holds the directories as well, and a directory goes only once it is empty:
                // deepest first, pass after pass until a pass deletes nothing. Aborted block uploads leave blobs of
                // uncommitted blocks (invisible to plain listings, gone after a week) that keep directories too.
                int deleted = 0;
                for (int pass = 0, progress = 1; progress > 0 && pass < 10; pass++)
                {
                    progress = 0;
                    var names = container.GetBlobs(BlobTraits.None, BlobStates.Uncommitted, Prefix + "/", default).Select(b => b.Name).Append(Prefix)
                        .OrderByDescending(n => n.Count(c => c == '/')).ThenByDescending(n => n.Length).ToList();
                    foreach (string name in names)
                    {
                        try
                        {
                            if (container.GetBlobClient(name).DeleteIfExists()) progress++;
                        }
                        catch (Azure.RequestFailedException e) when (e.ErrorCode == "DirectoryIsNotEmpty")
                        {
                            // Emptied by a later pass.
                        }
                    }
                    deleted += progress;
                }
                swept.Add($"azure {kind}: {deleted}");
                Assert.Empty(container.GetBlobs(BlobTraits.None, BlobStates.Uncommitted, Prefix + "/", default));
            }
        }
        if (RealGcs.Available)
        {
            using var fs = new ObjectStoreFileSystem($"gs://{RealGcs.Bucket}/{Prefix}", RealGcs.Args(), ObjectStoreBackend.Managed);
            swept.Add($"gcs: {DeleteAll(fs)}");
            Assert.Empty(fs.ListAll(""));
        }
        Console.WriteLine("Swept " + string.Join(", ", swept));
    }

    private static int DeleteAll(ObjectStoreFileSystem fs)
    {
        IReadOnlyList<FileEntry> files = fs.ListAll("");
        foreach (FileEntry file in files) fs.Delete(file.Path);
        return files.Count;
    }
}
