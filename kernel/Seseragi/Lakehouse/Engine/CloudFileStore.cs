using System.Collections.Frozen;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Lakehouse.Storage;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>The files of an object store (S3, Azure Blob Storage, Google Cloud Storage) as a store datasets read and write.</summary>
public static class CloudFileStore
{
    /// <summary>
    /// The files below <paramref name="url"/> (a bucket or container, or a directory of one: <c>s3://bucket</c>,
    /// <c>az://container/dir</c>, <c>gs://bucket</c>), through the managed object store clients. Datasets name its files
    /// by URLs below it (<c>s3://bucket/data/**/*.parquet</c>). Configuration keys are those of
    /// <c>CloudStorage.TryOpen</c> (the Rust <c>object_store</c> library's), environment variables below them. Dataset
    /// writers write through it too (files uploaded in parts, each appearing whole when its upload completes).
    /// </summary>
    /// <exception cref="ArgumentException">The managed clients cannot serve the location with these settings (the reason says why).</exception>
    public static FileSystemStore Open(string url, IReadOnlyDictionary<string, string>? options = null, RetrySettings? retry = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(url);
        CloudFileSystem fileSystem = CloudStorage.TryOpen(url, options ?? FrozenDictionary<string, string>.Empty, retry, out string? reason)
            ?? throw new ArgumentException($"'{url}' cannot be opened: {reason}", nameof(url));
        return new FileSystemStore(fileSystem, url);
    }
}
