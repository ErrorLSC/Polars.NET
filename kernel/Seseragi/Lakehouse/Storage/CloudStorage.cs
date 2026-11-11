using System.Collections;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>Opens tables on object stores with the managed clients.</summary>
public static class CloudStorage
{
    private static readonly IReadOnlySet<string> S3Schemes = new HashSet<string>(StringComparer.Ordinal) { "s3", "s3a" };

    /// <summary>
    /// A file system rooted at <paramref name="url"/>, or null with the reason when the managed clients cannot serve
    /// the location with these settings (a scheme or a setting they do not implement); the caller then uses another
    /// implementation. Configuration keys are those of the Rust <c>object_store</c> library, such as
    /// <c>aws_access_key_id</c>, <c>aws_region</c> or <c>aws_endpoint</c>; environment variables apply below them.
    /// </summary>
    public static CloudFileSystem? TryOpen(
        string url, IReadOnlyDictionary<string, string> options, RetrySettings? retry, out string? reason) =>
        TryOpen(url, options, retry ?? RetrySettings.Default, ProcessEnvironment(), AwsConfigFile(), out reason,
            bucket => BucketRegions.Find(bucket, retry ?? RetrySettings.Default));

    internal static CloudFileSystem? TryOpen(
        string url, IReadOnlyDictionary<string, string> options, RetrySettings retry,
        IReadOnlyDictionary<string, string> environment, string? awsConfigFile, out string? reason,
        Func<string, string?>? regionLookup = null)
    {
        ArgumentNullException.ThrowIfNull(url);
        int schemeEnd = url.IndexOf("://", StringComparison.Ordinal);
        string scheme = schemeEnd < 0 ? "" : url[..schemeEnd].ToLowerInvariant();
        if (!S3Schemes.Contains(scheme))
        {
            reason = $"the scheme of '{url}' is not served by a managed client";
            return null;
        }

        string rest = url[(schemeEnd + 3)..].TrimEnd('/');
        int slash = rest.IndexOf('/');
        string bucket = slash < 0 ? rest : rest[..slash];
        string prefix = slash < 0 ? "" : rest[(slash + 1)..];
        if (bucket.Length == 0)
        {
            reason = $"'{url}' names no bucket";
            return null;
        }

        S3Client? Open(string name, out string? why) =>
            S3Settings.Resolve(name, options, environment, awsConfigFile, out why, regionLookup) is { } settings
                ? new S3Client(settings.BucketEndpoint, settings.Region, settings.Credential, retry)
                : null;

        if (Open(bucket, out reason) is not { } client) return null;
        return new CloudFileSystem(url, bucket, prefix, client, S3Schemes, other =>
            Open(other, out string? why) ?? throw new StorageException($"Cannot open bucket '{other}': {why}."));
    }

    private static Dictionary<string, string> ProcessEnvironment()
    {
        var variables = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
            if (entry.Key is string name && entry.Value is string value) variables[name] = value;
        return variables;
    }

    private static string? AwsConfigFile()
    {
        try
        {
            string home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            string path = Path.Combine(home, ".aws", "config");
            return home.Length > 0 && File.Exists(path) ? File.ReadAllText(path) : null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }
}
