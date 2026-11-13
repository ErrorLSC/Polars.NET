using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>
/// A workbook's shared string table as read: one native UTF-8 buffer with an offset per string (no managed string
/// per entry). Loaders append a string's bytes, then <see cref="EndString"/>.
/// </summary>
internal sealed unsafe class SharedStringStore : ISharedStrings, IDisposable
{
    private NativeGrowBuffer _bytes;
    private NativeGrowBuffer _offsets; // long per string boundary: string i spans [i] .. [i + 1]
    private int _count;

    public SharedStringStore() => _offsets.Append(0L);

    public int Count => _count;

    public ReadOnlySpan<byte> Get(int index)
    {
        if ((uint)index >= (uint)_count)
            throw new ExcelFormatException($"A cell refers to shared string {index}, but the table holds {_count}.");
        long* offsets = (long*)_offsets.Pointer;
        long start = offsets[index];
        return new(_bytes.Pointer + start, (int)(offsets[index + 1] - start));
    }

    /// <summary>Bytes appended so far (where the string being built started, when taken before it).</summary>
    public long Length => _bytes.Length;

    /// <summary>Room for <paramref name="bytes"/> more bytes; shrink back with <see cref="Truncate"/>.</summary>
    public Span<byte> Reserve(int bytes) => new(_bytes.Reserve(bytes), bytes);

    public void Truncate(long length) => _bytes.SetLength(length);

    /// <summary>The bytes appended since <paramref name="start"/> (for decoding in place).</summary>
    public Span<byte> Since(long start) => new(_bytes.Pointer + start, checked((int)(_bytes.Length - start)));

    /// <summary>Ends the string being built.</summary>
    public void EndString()
    {
        _offsets.Append(_bytes.Length);
        _count++;
    }

    public void Dispose()
    {
        _bytes.Free();
        _offsets.Free();
        GC.SuppressFinalize(this);
    }

    ~SharedStringStore()
    {
        _bytes.Free();
        _offsets.Free();
    }
}
