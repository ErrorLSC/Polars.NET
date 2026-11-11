using System.Collections;
using Kaminari.Seseragi.Lakehouse.Storage.Azure;
using Kaminari.Seseragi.Lakehouse.Storage.Gcs;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>Opens tables on object stores with the managed clients.</summary>
public static class CloudStorage
{
    private static readonly IReadOnlySet<string> S3Schemes = new HashSet<string>(StringComparer.Ordinal) { "s3", "s3a" };

    private static readonly IReadOnlySet<string> GcsSchemes = new HashSet<string>(StringComparer.Ordinal) { "gs" };

    private static readonly IReadOnlySet<string> AzureSchemes =
        new HashSet<string>(StringComparer.Ordinal) { "az", "azure", "adl", "abfs", "abfss" };

    /// <summary>
    /// A file system rooted at <paramref name="url"/>, or null with the reason when the managed clients cannot serve
    /// the location with these settings (a scheme or a setting they do not implement); the caller then uses another
    /// implementation. Configuration keys are those of the Rust <c>object_store</c> library, such as
    /// <c>aws_access_key_id</c>, <c>azure_storage_account_name</c> or <c>azure_storage_sas_token</c> or <c>google_service_account_key</c>;
    /// environment variables (<c>AWS_*</c>, <c>AZURE_*</c>, <c>GOOGLE_*</c>) apply below them.
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
        string rest = schemeEnd < 0 ? "" : url[(schemeEnd + 3)..].TrimEnd('/');
        int slash = rest.IndexOf('/');
        string authority = slash < 0 ? rest : rest[..slash];
        string prefix = slash < 0 ? "" : rest[(slash + 1)..];
        if (authority.Length == 0 && (S3Schemes.Contains(scheme) || AzureSchemes.Contains(scheme) || GcsSchemes.Contains(scheme)))
        {
            reason = $"'{url}' names no bucket or container";
            return null;
        }

        if (S3Schemes.Contains(scheme))
        {
            S3Client? Open(string bucket, out string? why) =>
                S3Settings.Resolve(bucket, options, environment, awsConfigFile, out why, regionLookup) is { } settings
                    ? new S3Client(settings.BucketEndpoint, settings.Region, settings.Credential, retry)
                    : null;

            if (Open(authority, out reason) is not { } client) return null;
            return new CloudFileSystem(url, authority, prefix, client, S3Schemes, other =>
                Open(other, out string? why) ?? throw new StorageException($"Cannot open bucket '{other}': {why}."));
        }

        if (AzureSchemes.Contains(scheme))
        {
            AzureClient? Open(string location, out string? why)
            {
                if (ParseAzureAuthority(scheme, location, out why) is not { } parsed) return null;
                return AzureSettings.Resolve(parsed.Account, options, environment, out why) is { } settings
                    ? new AzureClient(settings, parsed.Container, retry)
                    : null;
            }

            if (Open(authority, out reason) is not { } client) return null;
            return new CloudFileSystem(url, authority, prefix, client, AzureSchemes, other =>
                Open(other, out string? why) ?? throw new StorageException($"Cannot open container '{other}': {why}."));
        }

        if (GcsSchemes.Contains(scheme))
        {
            // One settings for every bucket: the credentials do not depend on it.
            if (GcsSettings.Resolve(options, environment, ReadFile, GcloudCredentials(environment), out reason) is not { } settings)
                return null;
            return new CloudFileSystem(url, authority, prefix, new GcsClient(settings, authority, retry), GcsSchemes,
                other => new GcsClient(settings, other, retry));
        }

        reason = $"the scheme of '{url}' is not served by a managed client";
        return null;
    }

    /// <summary>
    /// The container and account (null: from the settings) of an Azure location's authority, as the engine's object
    /// store reads them: <c>container</c>, or <c>container@account.dfs.core.windows.net</c> (or <c>blob.</c>) for
    /// <c>abfs[s]</c> and <c>az</c>.
    /// </summary>
    internal static (string Container, string? Account)? ParseAzureAuthority(string scheme, string authority, out string? reason)
    {
        reason = null;
        int at = authority.IndexOf('@');
        if (at >= 0 && scheme is "az" or "abfs" or "abfss")
        {
            string container = authority[..at];
            string host = authority[(at + 1)..].ToLowerInvariant();
            int dot = host.IndexOf('.');
            string account = dot < 0 ? host : host[..dot];
            string domain = dot < 0 ? "" : host[(dot + 1)..];
            if (domain is "dfs.core.windows.net" or "blob.core.windows.net" && !container.Contains('.') && account.Length > 0)
                return (container, account);
            reason = domain is "dfs.fabric.microsoft.com" or "blob.fabric.microsoft.com"
                ? "Microsoft Fabric locations are not supported by the managed Azure client"
                : $"the Azure location '{authority}' is not recognised";
            return null;
        }
        if (at >= 0 || authority.Contains('.'))
        {
            reason = $"the Azure location '{authority}' is not recognised";
            return null;
        }
        return (authority, null);
    }

    /// <summary>gcloud's application default credentials below the home directory the environment names.</summary>
    private static string? GcloudCredentials(IReadOnlyDictionary<string, string> environment)
    {
        if (OperatingSystem.IsWindows())
            return environment.GetValueOrDefault("APPDATA") is { Length: > 0 } appData
                ? Path.Combine(appData, "gcloud", "application_default_credentials.json")
                : null;
        return environment.GetValueOrDefault("HOME") is { Length: > 0 } home ? Path.Combine(home, GcsSettings.WellKnownCredentials) : null;
    }

    private static string? ReadFile(string path)
    {
        try
        {
            return File.Exists(path) ? File.ReadAllText(path) : null;
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
