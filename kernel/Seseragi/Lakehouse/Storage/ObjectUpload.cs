namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>A part of an upload as the store acknowledged it: its index (from 0) and the store's name for it.</summary>
/// <param name="Index">The part's position in the object, from 0.</param>
/// <param name="Id">What the store calls the part (an S3 ETag, an Azure block id); empty when the store needs none.</param>
internal readonly record struct UploadedPart(int Index, string Id);

/// <summary>
/// An object written in parts (S3 and GCS multipart uploads, Azure blocks, GCS resumable uploads): nothing is visible
/// at the key until <see cref="CompleteAsync"/> lands, and then all of it. Parts are uploaded with
/// <see cref="PutPartAsync"/>, up to <see cref="MaxConcurrency"/> at once, by index from 0 without gaps; the last
/// bytes go to <see cref="CompleteAsync"/>, so every part before them is full-sized (stores refuse small parts that
/// are not the last).
/// </summary>
/// <remarks>
/// An upload that is neither completed nor aborted may keep its parts on the store (S3 and GCS bill them until a
/// lifecycle rule removes them; Azure drops uncommitted blocks after a week): a writer that gives up calls
/// <see cref="AbortAsync"/>. Every request is retried like any other; completing is safe to retry (a completion whose
/// answer was lost is recognized as ours).
/// </remarks>
internal abstract class ObjectUpload(string key, bool create)
{
    /// <summary>The object's key.</summary>
    public string Key { get; } = key;

    /// <summary>Whether the object is created only when none exists (put-if-absent), rather than replaced.</summary>
    public bool Create { get; } = create;

    /// <summary>How many parts may be uploaded at once; 1 when the store takes them only in order.</summary>
    public virtual int MaxConcurrency => int.MaxValue;

    /// <summary>Uploads the part at <paramref name="index"/> (not the last one: see <see cref="CompleteAsync"/>).</summary>
    /// <exception cref="StorageException">The store refused the part or could not be reached.</exception>
    public abstract Task<UploadedPart> PutPartAsync(int index, ReadOnlyMemory<byte> content, CancellationToken cancellationToken);

    /// <summary>
    /// Uploads <paramref name="last"/> (possibly empty) after <paramref name="parts"/> and makes the object visible.
    /// False when <see cref="Create"/> and an object already exists at the key; the upload is then aborted and the
    /// existing object is untouched.
    /// </summary>
    /// <param name="parts">Every part uploaded so far, in index order without gaps.</param>
    /// <param name="last">The object's last bytes.</param>
    /// <param name="cancellationToken">Cancels the requests.</param>
    /// <exception cref="StorageException">The store failed; whether the object was written is then unknown.</exception>
    public abstract Task<bool> CompleteAsync(IReadOnlyList<UploadedPart> parts, ReadOnlyMemory<byte> last, CancellationToken cancellationToken);

    /// <summary>Gives the upload up: its parts are removed where the store keeps them. Aborting twice is harmless.</summary>
    public abstract Task AbortAsync(CancellationToken cancellationToken);

    /// <summary>Checks that <paramref name="parts"/> run from 0 without gaps.</summary>
    protected static void CheckOrder(IReadOnlyList<UploadedPart> parts)
    {
        for (int i = 0; i < parts.Count; i++)
        {
            if (parts[i].Index != i)
                throw new ArgumentException($"The parts of an upload must be given in order from 0; part {i} is {parts[i].Index}.", nameof(parts));
        }
    }
}
