namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>An object as a store reports it.</summary>
/// <param name="Key">The full key inside the bucket or container.</param>
/// <param name="Size">The size in bytes.</param>
/// <param name="LastModified">The modification time.</param>
internal readonly record struct ObjectInfo(string Key, long Size, DateTimeOffset LastModified);

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

    /// <summary>Creates the object only when none exists, atomically; false when one exists.</summary>
    public abstract bool PutIfAbsent(string key, ReadOnlyMemory<byte> content);

    /// <summary>Writes the object, replacing any existing one.</summary>
    public abstract void Put(string key, ReadOnlyMemory<byte> content);

    /// <summary>Deletes the object; a missing object is not an error.</summary>
    public abstract void Delete(string key);

    /// <summary>
    /// Objects whose key starts with <paramref name="prefix"/>, in the store's order. <paramref name="delimited"/>
    /// keeps only those without a further <c>/</c> after the prefix; <paramref name="startAfter"/> keeps only keys
    /// greater than it.
    /// </summary>
    public abstract IEnumerable<ObjectInfo> List(string prefix, bool delimited, string? startAfter);
}
