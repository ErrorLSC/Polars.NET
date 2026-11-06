using System.Buffers;
using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

/// <summary>
/// A byte buffer allocated by the native (Rust) side and handed to .NET without copying.
/// <para>
/// Implements <see cref="IMemoryOwner{T}"/> (via <see cref="MemoryManager{T}"/>), so it can be passed
/// to <c>PipeWriter</c>, <c>Stream.WriteAsync(ReadOnlyMemory&lt;byte&gt;)</c>, sockets, gRPC etc.
/// </para>
/// <para>
/// The native memory is released on <see cref="IDisposable.Dispose"/> (or by the finalizer as a fallback).
/// Any <see cref="Span{T}"/> or <see cref="Memory{T}"/> obtained from this buffer must not be used after disposal.
/// Pinned handles (<see cref="Memory{T}.Pin"/>) and streams from <see cref="AsStream"/> keep the memory alive
/// until they are disposed themselves, even if the buffer is disposed first.
/// </para>
/// </summary>
public sealed unsafe class NativeBuffer : MemoryManager<byte>
{
    private readonly FfiBufferHandle _handle;
    private readonly int _length;

    private NativeBuffer(FfiBufferHandle handle, int length)
    {
        _handle = handle;
        _length = length;
    }

    /// <summary>
    /// Takes ownership of a native <see cref="FfiBuffer"/>. The buffer is freed even if this method throws.
    /// </summary>
    internal static NativeBuffer Take(FfiBuffer buffer)
    {
        var handle = new FfiBufferHandle(buffer);
        if (buffer.Length > int.MaxValue)
        {
            handle.Dispose();
            throw new InvalidOperationException(
                $"Native buffer of {buffer.Length} bytes exceeds the 2 GB limit of in-memory output. " +
                "Write to a file path instead.");
        }
        return new NativeBuffer(handle, (int)buffer.Length);
    }

    /// <summary>
    /// Takes ownership of a native <see cref="FfiBuffer"/>, copies it into a managed array and frees it.
    /// </summary>
    internal static byte[] TakeToArray(FfiBuffer buffer)
    {
        using var nb = Take(buffer);
        return nb.ToArray();
    }

    /// <summary>Number of bytes in the buffer.</summary>
    public int Length => _length;

    /// <summary>Whether the buffer has been disposed.</summary>
    public bool IsDisposed => _handle.IsClosed;

    /// <inheritdoc/>
    public override Span<byte> GetSpan()
    {
        ObjectDisposedException.ThrowIf(_handle.IsClosed, this);
        return _length == 0
            ? []
            : new Span<byte>((void*)_handle.DangerousGetHandle(), _length);
    }

    /// <inheritdoc/>
    /// <remarks>The pin holds a reference on the native memory until the returned handle is disposed.</remarks>
    public override MemoryHandle Pin(int elementIndex = 0)
    {
        if ((uint)elementIndex > (uint)_length)
            throw new ArgumentOutOfRangeException(nameof(elementIndex));

        bool added = false;
        _handle.DangerousAddRef(ref added); // throws ObjectDisposedException if already released
        byte* ptr = (byte*)_handle.DangerousGetHandle() + elementIndex;
        return new MemoryHandle(ptr, default, this);
    }

    /// <inheritdoc/>
    public override void Unpin() => _handle.DangerousRelease();

    /// <summary>Copies the contents into a new managed array.</summary>
    public byte[] ToArray() => GetSpan().ToArray();

    /// <summary>
    /// Returns a read-only <see cref="Stream"/> over the native memory (no copy).
    /// The stream keeps the memory alive until the stream itself is disposed.
    /// </summary>
    public Stream AsStream()
    {
        ObjectDisposedException.ThrowIf(_handle.IsClosed, this);
        if (_length == 0) return new MemoryStream([], writable: false);

        bool added = false;
        _handle.DangerousAddRef(ref added);
        return new OwnedUnmanagedStream(_handle, _length);
    }

    /// <summary>Writes the whole buffer to <paramref name="destination"/>.</summary>
    public void WriteTo(Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        destination.Write(GetSpan());
    }

    /// <summary>Asynchronously writes the whole buffer to <paramref name="destination"/>.</summary>
    /// <remarks>The buffer must not be disposed until the returned task completes.</remarks>
    public ValueTask WriteToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ObjectDisposedException.ThrowIf(_handle.IsClosed, this);
        return destination.WriteAsync(Memory, cancellationToken);
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        // SafeHandle frees the memory once all outstanding pins / streams are released.
        // When called from the finalizer path (disposing == false) the SafeHandle's own
        // critical finalizer takes care of it.
        if (disposing) _handle.Dispose();
    }

    private sealed class OwnedUnmanagedStream : UnmanagedMemoryStream
    {
        private FfiBufferHandle? _handle;

        public OwnedUnmanagedStream(FfiBufferHandle handle, int length)
            : base((byte*)handle.DangerousGetHandle(), length, length, FileAccess.Read)
        {
            _handle = handle;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            Interlocked.Exchange(ref _handle, null)?.DangerousRelease();
        }
    }
}

/// <summary>Owns a Rust-allocated <c>Vec&lt;u8&gt;</c> exposed as <see cref="FfiBuffer"/>.</summary>
internal sealed class FfiBufferHandle : SafeHandle
{
    private readonly nuint _length;
    private readonly nuint _capacity;

    public FfiBufferHandle(FfiBuffer buffer) : base(IntPtr.Zero, true)
    {
        _length = buffer.Length;
        _capacity = buffer.Capacity;
        SetHandle((IntPtr)buffer.Data);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_free_ffi_buffer(new FfiBuffer
        {
            Data = (nuint)handle,
            Length = _length,
            Capacity = _capacity,
        });
        return true;
    }
}
