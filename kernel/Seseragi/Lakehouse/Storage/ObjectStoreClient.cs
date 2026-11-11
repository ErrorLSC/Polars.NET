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
