namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// A file being written, as a write-only stream: the bytes go in with <c>Write</c>, and the file appears when
/// <see cref="Commit"/> succeeds, with its whole content at once (readers never see a part of it). Disposing the
/// stream without committing gives the file up: nothing appears, and what was stored on the way (a temporary file,
/// the parts of an upload) is removed. A writer unwinding on an exception therefore never publishes half a file.
/// </summary>
/// <remarks>
/// <see cref="Stream.Flush()"/> publishes nothing. After <see cref="Commit"/> (whatever its outcome) the stream takes no
/// more bytes. <see cref="Stream.Position"/> is the number of bytes written.
/// </remarks>
public abstract class FileWriteStream : Stream
{
    private const int Open = 0, Committing = 1, Committed = 2;

    private int _state;
    private int _disposed;
    private long _written;

    /// <summary>Opens a stream that holds the bytes in memory and writes them with one call at commit.</summary>
    /// <param name="fileSystem">The file system written to: <see cref="IWritableFileSystem.TryCreate"/> or <see cref="IWritableFileSystem.Overwrite"/>.</param>
    /// <param name="path">The file's path.</param>
    /// <param name="createOnly">Whether the file is created only when none exists.</param>
    public static FileWriteStream Buffered(IWritableFileSystem fileSystem, string path, bool createOnly) =>
        new BufferedFileWriteStream(fileSystem, path, createOnly);

    /// <summary>
    /// Publishes the file. Returns false when the stream was opened create-only and a file already exists at the path
    /// (the existing file is untouched, nothing of this stream remains); true otherwise.
    /// </summary>
    /// <exception cref="InvalidOperationException">The stream was committed already, or disposed.</exception>
    /// <exception cref="IOException">The storage failed; whether the file appeared is then unknown.</exception>
    public bool Commit()
    {
        StartCommit();
        bool done = CommitCore();
        Volatile.Write(ref _state, Committed);
        return done;
    }

    /// <inheritdoc cref="Commit"/>
    public async ValueTask<bool> CommitAsync(CancellationToken cancellationToken = default)
    {
        StartCommit();
        bool done = await CommitCoreAsync(cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _state, Committed);
        return done;
    }

    private void StartCommit()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Interlocked.CompareExchange(ref _state, Committing, Open) != Open)
            throw new InvalidOperationException("The file was committed already.");
    }

    /// <summary>Writes the bytes on their way to the file.</summary>
    protected abstract void WriteCore(ReadOnlySpan<byte> buffer);

    /// <summary>Writes the bytes on their way to the file, asynchronously; by default as <see cref="WriteCore"/>.</summary>
    protected virtual ValueTask WriteCoreAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        WriteCore(buffer.Span);
        return ValueTask.CompletedTask;
    }

    /// <summary>Publishes the file (see <see cref="Commit"/>).</summary>
    protected abstract bool CommitCore();

    /// <summary>Publishes the file asynchronously; by default as <see cref="CommitCore"/>.</summary>
    protected virtual ValueTask<bool> CommitCoreAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(CommitCore());
    }

    /// <summary>
    /// Gives the file up: called once, when the stream is disposed without a successful commit (or after a commit
    /// failed). Removes what was stored on the way, best effort; releases what the stream holds.
    /// </summary>
    protected abstract void Abort();

    /// <summary>Gives the file up asynchronously; by default as <see cref="Abort"/>.</summary>
    protected virtual ValueTask AbortAsync()
    {
        Abort();
        return ValueTask.CompletedTask;
    }

    /// <summary>Releases what a committed stream still holds; called once when it is disposed.</summary>
    protected virtual void Release()
    {
    }

    private void ThrowIfNotWritable()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _state) != Open) throw new InvalidOperationException("The file was committed: the stream takes no more bytes.");
    }

    /// <inheritdoc/>
    public sealed override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfNotWritable();
        WriteCore(buffer);
        _written += buffer.Length;
    }

    /// <inheritdoc/>
    public sealed override void Write(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        Write(buffer.AsSpan(offset, count));
    }

    /// <inheritdoc/>
    public sealed override void WriteByte(byte value) => Write([value]);

    /// <inheritdoc/>
    public sealed override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ThrowIfNotWritable();
        await WriteCoreAsync(buffer, cancellationToken).ConfigureAwait(false);
        _written += buffer.Length;
    }

    /// <inheritdoc/>
    public sealed override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <inheritdoc/>
    public override bool CanRead => false;

    /// <inheritdoc/>
    public override bool CanSeek => false;

    /// <inheritdoc/>
    public override bool CanWrite => Volatile.Read(ref _disposed) == 0 && Volatile.Read(ref _state) == Open;

    /// <inheritdoc/>
    public override long Length => throw new NotSupportedException("A file being written has no length to read; Position counts the bytes written.");

    /// <inheritdoc/>
    public override long Position
    {
        get => _written;
        set => throw new NotSupportedException("A file being written cannot seek.");
    }

    /// <summary>Publishes nothing: only <see cref="Commit"/> does.</summary>
    public override void Flush()
    {
    }

    /// <inheritdoc/>
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException("The stream is write-only.");

    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException("A file being written cannot seek.");

    /// <inheritdoc/>
    public override void SetLength(long value) => throw new NotSupportedException("A file being written cannot seek.");

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (disposing && Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            if (Volatile.Read(ref _state) == Committed) Release();
            else AbortQuietly();
        }
        base.Dispose(disposing);
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            if (Volatile.Read(ref _state) == Committed) Release();
            else
            {
                try
                {
                    await AbortAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Warn(e);
                }
            }
        }
        await base.DisposeAsync().ConfigureAwait(false);
    }

    // Disposing usually happens while an exception unwinds: a failure to clean up must not replace it.
    private void AbortQuietly()
    {
        try
        {
            Abort();
        }
        catch (Exception e)
        {
            Warn(e);
        }
    }

    private static void Warn(Exception e) =>
        System.Diagnostics.Trace.TraceWarning($"Could not remove what an uncommitted file left: {e.Message}");
}

/// <summary>Holds the bytes in memory; at commit, one <see cref="IWritableFileSystem.TryCreate"/> or <see cref="IWritableFileSystem.Overwrite"/>.</summary>
internal sealed class BufferedFileWriteStream(IWritableFileSystem fileSystem, string path, bool createOnly) : FileWriteStream
{
    private MemoryStream? _bytes = new();

    protected override void WriteCore(ReadOnlySpan<byte> buffer) => _bytes!.Write(buffer);

    protected override bool CommitCore()
    {
        var content = new ReadOnlyMemory<byte>(_bytes!.GetBuffer(), 0, (int)_bytes.Length);
        if (createOnly) return fileSystem.TryCreate(path, content);
        fileSystem.Overwrite(path, content);
        return true;
    }

    protected override void Abort() => _bytes = null;

    protected override void Release() => _bytes = null;
}
