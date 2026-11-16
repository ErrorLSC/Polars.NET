using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>An object as a store reports it.</summary>
/// <param name="Key">The full key inside the bucket or container.</param>
/// <param name="Size">The size in bytes.</param>
/// <param name="LastModified">The modification time.</param>
internal readonly record struct ObjectInfo(string Key, long Size, DateTimeOffset LastModified);

/// <summary>A ranged read: the bytes copied, and what the answer told of the object (its size, its version).</summary>
internal readonly record struct RangeRead(int Count, long? Size, string? Version);

/// <summary>An object's last bytes, its size and its version (when the store tells it).</summary>
internal sealed record ObjectTail(byte[] Tail, long Size, string? Version);

/// <summary>
/// The requests a file system needs from one bucket (or container) of an object store. Keys are plain text, never
/// percent-encoded; a client encodes them for its protocol.
/// </summary>
internal abstract class ObjectStoreClient
{
    /// <summary>The object's size and time, or null when there is none.</summary>
    public abstract ObjectInfo? Head(string key);

    /// <summary>The whole object, or null when there is none.</summary>
    public abstract byte[]? Get(string key);

    /// <summary>
    /// <paramref name="length"/> bytes from <paramref name="offset"/> (fewer when the object ends first, none when it
    /// ends before the offset), or null when there is no object.
    /// </summary>
    public abstract byte[]? GetRange(string key, long offset, int length);

    /// <summary>
    /// The last <paramref name="length"/> bytes of the object (all of it when shorter) and its size, or null when
    /// there is none. By default its properties, then a ranged read; stores with suffix ranges do it in one request.
    /// </summary>
    public virtual (byte[] Tail, long Size)? GetTail(string key, int length)
    {
        if (Head(key) is not { } info) return null;
        int count = (int)Math.Min(length, info.Size);
        if (count == 0) return ([], info.Size);
        byte[]? tail = GetRange(key, info.Size - count, count);
        return tail is null ? null : (tail, info.Size);
    }

    /// <summary>
    /// The tail and size from an answer to a suffix range request (<c>bytes=-N</c>): 206 with
    /// <c>Content-Range: bytes first-last/size</c>, or 200 with the whole object; null when the answer is neither.
    /// </summary>
    internal static (byte[] Tail, long Size)? SuffixAnswer(System.Net.HttpStatusCode status, string? contentRange, byte[] body, int length)
    {
        if (status == System.Net.HttpStatusCode.OK)
            return (body.Length <= length ? body : body[^length..], body.Length);
        if (status != System.Net.HttpStatusCode.PartialContent || contentRange is null) return null;
        int slash = contentRange.LastIndexOf('/');
        if (slash < 0 || !long.TryParse(contentRange.AsSpan(slash + 1), System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out long size))
            return null;
        return (body, size);
    }

    /// <summary>
    /// Reads the bytes of the object from <paramref name="offset"/> into <paramref name="destination"/> (fewer when it
    /// ends first, none when it ends before the offset), requiring its version to be <paramref name="version"/> when
    /// given; null when there is no object. The answer tells the object's size and version when the store sends them.
    /// By default a synchronous ranged read, unversioned.
    /// </summary>
    /// <exception cref="StorageException">The store failed; with status 412 when the object is not of that version.</exception>
    public virtual Task<RangeRead?> ReadRangeAsync(string key, long offset, Memory<byte> destination, string? version, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (GetRange(key, offset, destination.Length) is not { } bytes) return Task.FromResult<RangeRead?>(null);
        bytes.CopyTo(destination);
        return Task.FromResult<RangeRead?>(new RangeRead(bytes.Length, null, null));
    }

    /// <summary>
    /// The last <paramref name="length"/> bytes of the object (all of it when shorter), its size and version (when the
    /// store tells it), or null when there is none. By default <see cref="GetTail"/>, unversioned.
    /// </summary>
    public virtual Task<ObjectTail?> GetTailAsync(string key, int length, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<ObjectTail?>(GetTail(key, length) is var (tail, size) ? new ObjectTail(tail, size, null) : null);
    }

    /// <summary>
    /// Reads a ranged answer into <paramref name="destination"/>: 206 (its bytes), 200 (the whole object sent: the range
    /// cut from it), 416 (the object ends before the offset: none); null for 404; <paramref name="failure"/> otherwise.
    /// An answer of another version (<paramref name="version"/>) than <paramref name="required"/> fails with 412 too:
    /// a store (or an emulator) that does not check the condition is checked here.
    /// </summary>
    internal static async Task<RangeRead?> ReadRangeAnswer(HttpResponseMessage response, long offset, Memory<byte> destination,
        string? version, string? required, Func<StoreResponse, StorageException> failure, CancellationToken ct)
    {
        if (required is not null && version is not null && version != required && response.IsSuccessStatusCode)
            throw new StorageException($"Failed to read: the object is at version {version}, not {required}.", System.Net.HttpStatusCode.PreconditionFailed);
        switch (response.StatusCode)
        {
            case System.Net.HttpStatusCode.NotFound:
                return null;
            case System.Net.HttpStatusCode.RequestedRangeNotSatisfiable:
                return new RangeRead(0, response.Content.Headers.ContentRange?.Length, version);
            case System.Net.HttpStatusCode.PartialContent:
            {
                int count = (int)Math.Min(destination.Length, response.Content.Headers.ContentLength ?? destination.Length);
                Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                await using (body.ConfigureAwait(false))
                    count = await body.ReadAtLeastAsync(destination[..count], count, throwOnEndOfStream: true, ct).ConfigureAwait(false);
                return new RangeRead(count, response.Content.Headers.ContentRange?.Length, version);
            }
            case System.Net.HttpStatusCode.OK:
            {
                // The store ignored the range and sends everything: skip to the offset.
                Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                await using (body.ConfigureAwait(false))
                {
                    byte[] skip = new byte[(int)Math.Min(offset, 81920)];
                    for (long left = offset; left > 0;)
                    {
                        int n = await body.ReadAsync(skip.AsMemory(0, (int)Math.Min(left, skip.Length)), ct).ConfigureAwait(false);
                        if (n == 0) return new RangeRead(0, response.Content.Headers.ContentLength, version);
                        left -= n;
                    }
                    int count = await body.ReadAtLeastAsync(destination, destination.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
                    return new RangeRead(count, response.Content.Headers.ContentLength, version);
                }
            }
            default:
            {
                byte[] body = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
                throw failure(new StoreResponse(response.StatusCode, response.Headers, response.Content.Headers, body, retried: false));
            }
        }
    }

    /// <summary>Creates the object only when none exists, atomically; false when one exists.</summary>
    public abstract bool PutIfAbsent(string key, ReadOnlyMemory<byte> content);

    /// <summary>Writes the object, replacing any existing one.</summary>
    public abstract void Put(string key, ReadOnlyMemory<byte> content);

    /// <summary>
    /// Starts writing an object in parts (see <see cref="ObjectUpload"/>); with <paramref name="create"/>, completing
    /// it only creates the object when none exists.
    /// </summary>
    /// <exception cref="StorageException">The store refused to start the upload.</exception>
    public abstract Task<ObjectUpload> StartUploadAsync(string key, bool create, CancellationToken cancellationToken);

    /// <summary>Deletes the object; a missing object is not an error.</summary>
    public abstract void Delete(string key);

    /// <summary>
    /// Objects whose key starts with <paramref name="prefix"/>, in the store's order. <paramref name="delimited"/>
    /// keeps only those without a further <c>/</c> after the prefix; <paramref name="startAfter"/> keeps only keys
    /// greater than it.
    /// </summary>
    public abstract IEnumerable<ObjectInfo> List(string prefix, bool delimited, string? startAfter);
}
