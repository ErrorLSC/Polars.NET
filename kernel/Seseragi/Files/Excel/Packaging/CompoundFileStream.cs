namespace Kaminari.Seseragi.Files.Excel.Packaging;

/// <summary>
/// A stream of a compound file: a chain of sectors (of the file, or mini sectors of the mini stream) read in order.
/// Runs of consecutive sectors are read at once. Reads go to <see cref="ReadAt"/> at absolute offsets, so several
/// streams over the same data do not disturb each other.
/// </summary>
internal sealed class CompoundFileStream : Stream, IRandomAccess
{
    private readonly IRandomAccess _parent;
    private readonly uint[] _sectors;
    private readonly int _shift;
    private readonly long _headerSectors; // 1 when sectors are numbered after the file header, 0 in the mini stream
    private long _position;

    /// <param name="parent">The file, or the mini stream.</param>
    /// <param name="sectors">The chain, covering at least <paramref name="length"/> bytes.</param>
    /// <param name="shift">The sector size as a power of 2.</param>
    /// <param name="afterHeader">Sector n starts at (n + 1) sectors (file sectors) instead of n (mini sectors).</param>
    /// <param name="length">The stream size.</param>
    public CompoundFileStream(IRandomAccess parent, uint[] sectors, int shift, bool afterHeader, long length)
    {
        _parent = parent;
        _sectors = sectors;
        _shift = shift;
        _headerSectors = afterHeader ? 1 : 0;
        Length = length;
    }

    public override long Length { get; }

    public override bool CanRead => true;

    public override bool CanSeek => true;

    public override bool CanWrite => false;

    public override long Position
    {
        get => _position;
        set => _position = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value));
    }

    public int ReadAt(long offset, Span<byte> destination)
    {
        int count = (int)Math.Clamp(Length - offset, 0, destination.Length);
        int total = 0;
        while (total < count)
        {
            var (at, run) = Locate(offset + total, count - total);
            if (_parent.ReadAt(at, destination.Slice(total, run)) < run) throw Truncated();
            total += run;
        }
        return count;
    }

    public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        int count = (int)Math.Clamp(Length - offset, 0, destination.Length);
        int total = 0;
        while (total < count)
        {
            var (at, run) = Locate(offset + total, count - total);
            if (await _parent.ReadAtAsync(at, destination.Slice(total, run), cancellationToken).ConfigureAwait(false) < run) throw Truncated();
            total += run;
        }
        return count;
    }

    // Where the byte at offset sits in the parent, and how many of the wanted bytes follow it contiguously.
    private (long At, int Run) Locate(long offset, int wanted)
    {
        int size = 1 << _shift;
        long index = offset >> _shift;
        int inner = (int)(offset & (size - 1));
        long end = index + 1;
        long bytes = size - inner;
        while (bytes < wanted && end < _sectors.Length && _sectors[end] == _sectors[end - 1] + 1)
        {
            end++;
            bytes += size;
        }
        return ((_sectors[index] + _headerSectors << _shift) + inner, (int)Math.Min(bytes, wanted));
    }

    private static ExcelFormatException Truncated() => new("The compound file is truncated: a stream's sectors lie past its end.");

    public override int Read(Span<byte> buffer)
    {
        int n = ReadAt(_position, buffer);
        _position += n;
        return n;
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int n = await ReadAtAsync(_position, buffer, cancellationToken).ConfigureAwait(false);
        _position += n;
        return n;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override long Seek(long offset, SeekOrigin origin) => Position = origin switch
    {
        SeekOrigin.Begin => offset,
        SeekOrigin.Current => _position + offset,
        SeekOrigin.End => Length + offset,
        _ => throw new ArgumentOutOfRangeException(nameof(origin)),
    };

    public override void Flush() { }

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
