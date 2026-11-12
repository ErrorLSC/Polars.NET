namespace Kaminari.Sazanami.Helpers;

/// <summary>A write-only stream that appends to a <see cref="NativeByteBuffer"/>, growing it as needed.</summary>
internal sealed class NativeByteBufferStream(NativeByteBuffer buffer) : Stream
{
    private int _written;

    public NativeByteBuffer Buffer { get; } = buffer;

    public override long Length => _written;

    public override long Position
    {
        get => _written;
        set => throw new NotSupportedException();
    }

    public override void Write(ReadOnlySpan<byte> data)
    {
        if ((long)_written + data.Length > Array.MaxLength) throw new InvalidOperationException("A native buffer cannot exceed 2 GB.");
        Buffer.EnsureCapacity((long)_written + data.Length);
        data.CopyTo(Buffer.Span[_written..]);
        _written += data.Length;
    }

    public override void Write(byte[] data, int offset, int count) => Write(data.AsSpan(offset, count));

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Write(data.Span);
        return ValueTask.CompletedTask;
    }

    public override Task WriteAsync(byte[] data, int offset, int count, CancellationToken cancellationToken)
    {
        Write(data.AsSpan(offset, count));
        return Task.CompletedTask;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override void Flush() { }
    public override int Read(byte[] data, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
