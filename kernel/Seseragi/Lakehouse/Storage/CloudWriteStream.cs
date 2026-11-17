using System.Globalization;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>
/// A file written to an object store in parts. The bytes fill a buffer of one part; a file that fits in it is written
/// with one request at commit (a plain or a conditional upload). Once the buffer overflows, an upload in parts starts
/// (<see cref="ObjectUpload"/>) and every full buffer goes out as a part while the next one fills, a few at a time;
/// the commit sends the rest with the completion. Disposing without a commit cancels the parts in flight and aborts
/// the upload.
/// </summary>
/// <remarks>
/// Every part but the last has the same size (stores such as R2 require it). Beyond <c>maxParts</c> parts the write is
/// refused (S3 and GCS take 10 000). Buffers are native memory from <see cref="NativeMemoryPool"/>, handed to the HTTP
/// requests as they are; the first part's buffer grows as bytes come, so a small file holds little.
/// </remarks>
internal sealed class CloudWriteStream(ObjectStoreClient client, string key, bool createOnly, int partSize, int concurrency, int maxParts)
    : FileWriteStream
{
    private const int FirstBuffer = 64 * 1024;

    private NativeByteBuffer? _buffer; // the part being filled
    private int _filled;
    private ObjectUpload? _upload;
    private readonly List<Task<UploadedPart>> _parts = [];
    private readonly List<Task<UploadedPart>> _running = [];
    private readonly CancellationTokenSource _abort = new();

    protected override void WriteCore(ReadOnlySpan<byte> buffer)
    {
        while (!buffer.IsEmpty)
        {
            if (_filled == partSize) SendPartAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
            buffer = buffer[Copy(buffer)..];
        }
    }

    protected override async ValueTask WriteCoreAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
        while (!buffer.IsEmpty)
        {
            if (_filled == partSize) await SendPartAsync(cancellationToken).ConfigureAwait(false);
            buffer = buffer[Copy(buffer.Span)..];
        }
    }

    /// <summary>Copies what fits of <paramref name="source"/> into the part being filled; returns how much.</summary>
    private int Copy(ReadOnlySpan<byte> source)
    {
        int count = Math.Min(source.Length, partSize - _filled);
        int needed = _filled + count;
        if (_buffer is null) _buffer = new NativeByteBuffer(Math.Min(partSize, Math.Max(needed, FirstBuffer)), pooled: true);
        else if (needed > _buffer.Capacity) _buffer.EnsureCapacity(Math.Min(partSize, Math.Max(needed, 2L * _buffer.Capacity))); // keeps the bytes
        source[..count].CopyTo(_buffer.Span[_filled..]);
        _filled = needed;
        return count;
    }

    /// <summary>Sends the full buffer as the next part (starting the upload first), once fewer parts than allowed are in flight.</summary>
    private async ValueTask SendPartAsync(CancellationToken cancellationToken)
    {
        ThrowIfAPartFailed();
        // The part sent now is followed by one more at least (the bytes that overflowed).
        if (_parts.Count + 2 > maxParts)
        {
            throw new IOException(string.Create(CultureInfo.InvariantCulture,
                $"'{key}' is too large to upload in parts: more than {maxParts} parts of {partSize / 1048576.0:0.##} MiB ({(double)maxParts * partSize / (1L << 30):0.#} GiB)."));
        }
        try
        {
            _upload ??= await client.StartUploadAsync(key, createOnly, cancellationToken).ConfigureAwait(false);
        }
        catch (StorageException e)
        {
            throw new IOException(e.Message, e);
        }

        int limit = Math.Max(1, Math.Min(concurrency, _upload.MaxConcurrency));
        _running.RemoveAll(static part => part.IsCompleted);
        while (_running.Count >= limit)
        {
            await Task.WhenAny(_running).WaitAsync(cancellationToken).ConfigureAwait(false);
            _running.RemoveAll(static part => part.IsCompleted);
        }
        ThrowIfAPartFailed();

        Task<UploadedPart> part = PutAsync(_upload, _parts.Count, _buffer!, _filled);
        _buffer = null;
        _filled = 0;
        _parts.Add(part);
        _running.Add(part);
    }

    /// <summary>Uploads a part, then gives its buffer back to the pool.</summary>
    private async Task<UploadedPart> PutAsync(ObjectUpload upload, int index, NativeByteBuffer buffer, int length)
    {
        try
        {
            return await upload.PutPartAsync(index, buffer.Memory[..length], _abort.Token).ConfigureAwait(false);
        }
        finally
        {
            buffer.Release();
        }
    }

    private void ThrowIfAPartFailed()
    {
        foreach (Task<UploadedPart> part in _parts)
        {
            if (part.IsFaulted)
            {
                Exception e = part.Exception!.InnerException!;
                throw new IOException($"Failed to upload a part of '{key}': {e.Message}", e);
            }
        }
    }

    protected override bool CommitCore()
    {
        try
        {
            return _upload is null ? PutWhole() : CompleteAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            ReturnBuffer();
        }
    }

    protected override async ValueTask<bool> CommitCoreAsync(CancellationToken cancellationToken)
    {
        try
        {
            return _upload is null
                ? await Task.Run(PutWhole, cancellationToken).ConfigureAwait(false)
                : await CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnBuffer();
        }
    }

    /// <summary>The whole file in one request.</summary>
    private bool PutWhole()
    {
        ReadOnlyMemory<byte> content = _buffer is null ? ReadOnlyMemory<byte>.Empty : _buffer.Memory[.._filled];
        try
        {
            if (createOnly) return client.PutIfAbsent(key, content);
            client.Put(key, content);
            return true;
        }
        catch (StorageException e)
        {
            throw new IOException(e.Message, e);
        }
    }

    /// <summary>Waits for the parts, then completes the upload with the bytes left.</summary>
    private async ValueTask<bool> CompleteAsync(CancellationToken cancellationToken)
    {
        UploadedPart[] parts;
        try
        {
            parts = await Task.WhenAll(_parts).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            ThrowIfAPartFailed();
            throw;
        }
        try
        {
            ReadOnlyMemory<byte> last = _buffer is null ? ReadOnlyMemory<byte>.Empty : _buffer.Memory[.._filled];
            return await _upload!.CompleteAsync(parts, last, cancellationToken).ConfigureAwait(false);
        }
        catch (StorageException e)
        {
            throw new IOException(e.Message, e);
        }
    }

    protected override void Abort() => AbortAsync().AsTask().GetAwaiter().GetResult();

    protected override async ValueTask AbortAsync()
    {
        try
        {
            // Parts still in flight finish (cancelled) first: one landing after the abort would stay on the store.
            await _abort.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(_parts).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Failed or cancelled: either way no longer in flight.
            }
            if (_upload is not null) await _upload.AbortAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            Release();
        }
    }

    protected override void Release()
    {
        ReturnBuffer();
        _abort.Dispose();
    }

    private void ReturnBuffer()
    {
        _buffer?.Release();
        _buffer = null;
        _filled = 0;
    }
}
