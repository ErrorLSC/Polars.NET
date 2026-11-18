using System.Buffers;
using System.IO.Hashing;
using System.Runtime.ExceptionServices;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// The threads a writer may keep busy: the column writers running (counted while they run) and the helpers that
/// compress their pages, at most <see cref="Max"/> together.
/// </summary>
internal sealed class WorkBudget(int max)
{
    private int _busy;

    public int Max => max;

    public void Enter() => Interlocked.Increment(ref _busy);

    public void Exit() => Interlocked.Decrement(ref _busy);

    /// <summary>Takes a thread for a helper when one is left.</summary>
    public bool TryEnter()
    {
        while (true)
        {
            int busy = Volatile.Read(ref _busy);
            if (busy >= max) return false;
            if (Interlocked.CompareExchange(ref _busy, busy + 1, busy) == busy) return true;
        }
    }
}

/// <summary>
/// A data page encoded but not yet compressed: its levels and values (<see cref="Raw"/>; format 2 keeps the levels,
/// the first <see cref="LevelsLength"/> bytes, uncompressed), and its header without sizes and checksum. Compressing
/// it (<see cref="Run"/>) depends on nothing else, so any thread may: the column writer claims a page it needs and
/// has not seen taken, or waits for the helper that took it. Bytes come out the same whichever thread compressed.
/// </summary>
internal sealed class PageJob(PageSettings settings)
{
    private readonly object _lock = new();
    private int _state; // 0 waiting, 1 running, 2 done
    private ManualResetEventSlim? _done;
    private Exception? _error;

    public NativeBufferWriter Raw { get; } = new(1 << 16);
    public int LevelsLength { get; set; }
    /// <summary>The page's header without sizes and checksum (set once its values are encoded).</summary>
    public PageHeader Header { get; set; } = null!;
    /// <summary>The page as the page index describes it (its place in the chunk set once committed).</summary>
    public required PageRecord Record { get; set; }

    /// <summary>The upper bound of the compressed page in the file (header included), before it is compressed.</summary>
    public long Bound { get; set; }

    public byte[] HeaderBytes { get; private set; } = [];

    /// <summary>The compressed page (levels first in format 2), in native memory until <see cref="Release"/>.</summary>
    public NativeBufferWriter Body { get; } = new(1 << 16);

    public int BodyLength => Body.WrittenCount;
    public int Uncompressed { get; private set; }

    public bool TryClaim() => Interlocked.CompareExchange(ref _state, 1, 0) == 0;

    /// <summary>Compresses and checksums the page (once claimed); its buffer goes back to the pool.</summary>
    public void Run()
    {
        try
        {
            Compress();
        }
        catch (Exception e)
        {
            _error = e;
        }
        finally
        {
            lock (_lock)
            {
                Volatile.Write(ref _state, 2);
                _done?.Set();
            }
        }
    }

    /// <summary>The page compressed: by this thread when no helper took it, else when the helper is done.</summary>
    public void Complete()
    {
        if (TryClaim()) Run();
        else
        {
            ManualResetEventSlim? wait = null;
            lock (_lock)
            {
                if (Volatile.Read(ref _state) != 2) wait = _done ??= new ManualResetEventSlim(false);
            }
            wait?.Wait();
        }
        _done?.Dispose();
        if (_error is { } e) ExceptionDispatchInfo.Throw(e);
    }

    private void Compress()
    {
        ReadOnlySpan<byte> raw = Raw.WrittenSpan;
        bool v1 = Header.Type == PageType.DataPage;
        int levels = v1 ? 0 : LevelsLength;
        // Format 1 compresses levels and values together (an uncompressed codec copies them); format 2 only the
        // values, and only with a codec.
        bool compressed = v1 || settings.Codec != CompressionCodec.Uncompressed;
        ReadOnlySpan<byte> values = raw[levels..];
        Span<byte> body = Body.GetSpan(levels + (compressed ? PageCodecs.MaxCompressedLength(settings.Codec, values.Length) : values.Length));
        raw[..levels].CopyTo(body);
        int length = levels + (compressed ? PageCodecs.Compress(settings.Codec, values, body[levels..], settings.Level) : Copy(values, body[levels..]));
        Body.Advance(length);
        PageHeader header = new()
        {
            Type = Header.Type,
            UncompressedPageSize = raw.Length,
            CompressedPageSize = length,
            Crc = settings.Checksums ? (int)Crc32.HashToUInt32(body[..length]) : null,
            DataPageHeader = Header.DataPageHeader,
            DataPageHeaderV2 = Header.DataPageHeaderV2,
        };
        ThriftCompactWriter thrift = ThriftCompactWriter.ThreadScratch();
        header.Write(thrift);
        HeaderBytes = thrift.ToArray();
        Uncompressed = raw.Length;
        Raw.Dispose();

        static int Copy(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
    }

    /// <summary>Returns the compressed body to the pool (after the page is in the file).</summary>
    public void Release()
    {
        Body.Dispose();
        Raw.Dispose();
    }
}
