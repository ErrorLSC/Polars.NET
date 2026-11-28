namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// A file a writer creates or overwrites, as a write-only stream. The file is opened at once (a bad path, a denied
/// access or a file in use fail there), but an existing file's old bytes are cut off on a pool thread: truncating a
/// large file blocks in the file system for tens of milliseconds (about 20 ms for 76 MB on btrfs), which a writer
/// then spends encoding instead of waiting. The first write (or any use of the stream but closing it) waits for the
/// truncation and rethrows its failure; closing waits for it too (without rethrowing: a write surfaced it), so that a
/// file given up unwritten is empty, as after <see cref="FileMode.Create"/>.
/// </summary>
internal sealed class OverwrittenFile : Stream
{
    private readonly FileStream _file;
    private Task? _truncation;

    private OverwrittenFile(FileStream file)
    {
        _file = file;
        // A new file is empty already; a device or a pipe has no bytes to cut.
        if (file.CanSeek && file.Length > 0) _truncation = Task.Run(() => file.SetLength(0));
    }

    /// <summary>Opens <paramref name="path"/> to be written from its start, as <see cref="FileMode.Create"/> does.</summary>
    public static OverwrittenFile Open(string path, int bufferSize = 1)
    {
        var file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, bufferSize, FileOptions.Asynchronous);
        try
        {
            return new OverwrittenFile(file);
        }
        catch
        {
            file.Dispose();
            throw;
        }
    }

    /// <summary>The file, once its old bytes are gone.</summary>
    private FileStream File
    {
        get
        {
            if (_truncation is { } truncation)
            {
                truncation.GetAwaiter().GetResult();
                _truncation = null;
            }
            return _file;
        }
    }

    private async ValueTask<FileStream> FileAsync()
    {
        if (_truncation is { } truncation)
        {
            await truncation.ConfigureAwait(false);
            _truncation = null;
        }
        return _file;
    }

    public override bool CanRead => false;
    public override bool CanSeek => _file.CanSeek;
    public override bool CanWrite => true;
    public override long Length => File.Length;

    public override long Position
    {
        get => File.Position;
        set => File.Position = value;
    }

    public override long Seek(long offset, SeekOrigin origin) => File.Seek(offset, origin);
    public override void SetLength(long value) => File.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => File.Write(buffer, offset, count);
    public override void Write(ReadOnlySpan<byte> buffer) => File.Write(buffer);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        await (await FileAsync().ConfigureAwait(false)).WriteAsync(buffer, cancellationToken).ConfigureAwait(false);

    public override void Flush() => File.Flush();

    public override async Task FlushAsync(CancellationToken cancellationToken) =>
        await (await FileAsync().ConfigureAwait(false)).FlushAsync(cancellationToken).ConfigureAwait(false);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException("The stream is write-only.");

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _truncation?.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
            _truncation = null;
            _file.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (_truncation is { } truncation) await truncation.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        _truncation = null;
        await _file.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
