using System.Buffers;
using System.Numerics;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// The values of one data page in their physical type, taken from the column writer once the page is full (the writer
/// fills another): mapped to a dictionary of the page's own (<see cref="Map"/>, on any thread) before the writer merges
/// that into the chunk's, in page order (<see cref="ColumnWriter"/>); then encoded, plainly or as the chunk's indices,
/// as the page is compressed, and let go (its memory back to the pool). One thread uses it at a time: the page's
/// work items hand it on.
/// </summary>
internal abstract class PageValues : IDisposable
{
    private readonly NativeArray<uint> _local = new(), _remap = new();
    private bool _mapped, _pays = true;

    /// <summary>The page's values (nulls are levels only).</summary>
    public abstract int Count { get; }

    /// <summary>The bytes the values take plainly encoded.</summary>
    public abstract long PlainBytes { get; }

    /// <summary>Whether every value is mapped to the page's dictionary.</summary>
    public bool Mapped => _mapped;

    /// <summary>For the page judging the dictionary: whether a dictionary of its values makes it smaller (false when mapping stopped part way).</summary>
    public bool Pays => _pays;

    /// <summary>The distinct values of the page's dictionary, in order of first appearance.</summary>
    protected abstract int LocalCount { get; }

    /// <summary>The bytes the page's dictionary takes plainly encoded (as the chunk's dictionary page would hold them).</summary>
    protected abstract long LocalPlainBytes { get; }

    /// <summary>Adds the values <paramref name="first"/> .. + indices' length to the page's dictionary and gives their indices in it.</summary>
    protected abstract void MapRange(Span<uint> indices, int first);

    protected abstract void ClearLocal();

    /// <summary>
    /// Maps the values to a dictionary of the page's own. Judging (<paramref name="worth"/>: the bytes the dictionary
    /// and its indices must stay below), in steps: the dictionary and its indices' width only grow, so one that does not
    /// pay part way never will, and the rest is not mapped (<see cref="Pays"/> false).
    /// </summary>
    public void Map(long? worth)
    {
        ClearLocal();
        int count = Count;
        _local.Reserve(count);
        Span<uint> indices = _local.AsSpan(0, count);
        _mapped = false;
        _pays = true;
        if (worth is long limit)
        {
            for (int first = 0; first < count; first += JudgeStep)
            {
                MapRange(indices.Slice(first, Math.Min(JudgeStep, count - first)), first);
                if (LocalPlainBytes + 1 + RleBitPackedEncoder.MaxBytes(count, Width(LocalCount)) >= limit)
                {
                    _pays = false;
                    return;
                }
            }
        }
        else MapRange(indices, 0);
        _mapped = true;
    }

    /// <summary>The values of the judging page mapped before it is judged again.</summary>
    public const int JudgeStep = 1024;

    /// <summary>The bits of a dictionary index among <paramref name="entries"/> (none for one entry).</summary>
    public static int Width(int entries) => entries <= 1 ? 0 : 32 - BitOperations.LeadingZeroCount((uint)entries - 1);

    /// <summary>
    /// Where the writer merging the page's dictionary into the chunk's puts each entry's index there (by its index
    /// here: the entries in order of first appearance).
    /// </summary>
    public Span<uint> Remap()
    {
        int entries = LocalCount;
        _remap.Reserve(entries);
        return _remap.AsSpan(0, entries);
    }

    /// <summary>The values as the chunk dictionary's indices (once merged), bit-packed after their width.</summary>
    public void EncodeIndices(int bitWidth, IBufferWriter<byte> output)
    {
        int count = Count;
        output.GetSpan(1 + RleBitPackedEncoder.MaxBytes(count, bitWidth)); // the room at once
        Span<uint> indices = _local.AsSpan(0, count);
        if (count > 0)
        {
            // Mapped (a page of nulls is not: its dictionary may still be the last page's).
            ReadOnlySpan<uint> remap = _remap.AsSpan(0, LocalCount);
            for (int i = 0; i < count; i++) indices[i] = remap[(int)indices[i]];
        }
        output.GetSpan(1)[0] = (byte)bitWidth;
        output.Advance(1);
        RleBitPackedEncoder.Encode<uint>(indices, bitWidth, output);
    }

    /// <summary>Gathers what the page's statistics need beyond its values' (on the page's thread, after <see cref="Map"/>).</summary>
    public virtual void Prepare() { }

    /// <summary>Whether <see cref="Prepare"/> has anything to do.</summary>
    public virtual bool Prepares => false;

    public abstract void EncodePlain(IBufferWriter<byte> output);

    /// <summary>Encodes the values in <paramref name="encoding"/> (one the type takes: <see cref="ColumnSettings.Takes"/>).</summary>
    public abstract void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output);

    /// <summary>
    /// Empties the page for another (its buffers kept; its dictionary emptied only when the page is mapped again:
    /// a large one, of a page judging the dictionary, would be cleared for every page after it).
    /// </summary>
    public virtual void Clear()
    {
        _mapped = false;
        _pays = true;
    }

    /// <summary>The native bytes it holds.</summary>
    public virtual long ScratchBytes => 4L * (_local.Capacity + _remap.Capacity);

    /// <summary>
    /// Returns its memory to the pool between row groups (grown again as values come): the page's indices into its
    /// dictionary and their remapping, as a subclass returns its values and dictionary.
    /// </summary>
    public virtual void Trim()
    {
        _local.Dispose();
        _remap.Dispose();
    }

    public virtual void Dispose()
    {
        _local.Dispose();
        _remap.Dispose();
    }
}

/// <summary>
/// A page's work before the writer takes it in order: its values mapped to a dictionary of their own (when the chunk
/// may still be dictionary-encoded), and what its statistics need gathered. Any thread may run it; the writer runs it
/// itself when no helper did.
/// </summary>
internal sealed class PagePreparation(PageValues page, bool map, long? worth) : ClaimedWork
{
    /// <summary>Whether there is anything to do (else the page is laid out at once).</summary>
    public bool HasWork { get; } = map || page.Prepares;

    protected override void Execute()
    {
        if (map) page.Map(worth);
        page.Prepare();
    }
}
