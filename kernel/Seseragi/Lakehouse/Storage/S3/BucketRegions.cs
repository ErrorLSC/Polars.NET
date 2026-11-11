using System.Collections.Concurrent;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.S3;

/// <summary>
/// The regions of AWS buckets, asked of S3 itself: any request to a bucket's global host is answered with an
/// <c>x-amz-bucket-region</c> header, whether or not the request is allowed. Found regions are kept for the process.
/// </summary>
internal static class BucketRegions
{
    private static readonly ConcurrentDictionary<string, string> Known = new(StringComparer.Ordinal);

    /// <summary>The bucket's region, or null when S3 does not say.</summary>
    public static string? Find(string bucket, RetrySettings retry)
    {
        if (Known.TryGetValue(bucket, out string? region)) return region;
        try
        {
            StoreResponse response = HttpTransport.Send(
                () => new HttpRequestMessage(HttpMethod.Head, $"https://{bucket}.s3.amazonaws.com"), retry);
            region = response.Header("x-amz-bucket-region");
        }
        catch (StorageException)
        {
            return null;
        }
        if (string.IsNullOrEmpty(region)) return null;
        Known[bucket] = region;
        return region;
    }
}
