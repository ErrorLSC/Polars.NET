using System.Buffers;
using Polars.NET.Core;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Tests for the zero-copy <c>*NativeMemory</c> writers and <see cref="NativeBuffer"/> semantics.
/// Format-specific round-trips live in <c>NativeMemoryTests.{Format}.cs</c>.
/// </summary>
public partial class NativeMemoryTests
{
    private static DataFrame SampleFrame() => DataFrame.FromColumns(new
    {
        id = new[] { 1, 2, 3, 4, 5 },
        name = new[] { "Alice", "Bob", null, "David", "Eve" },
        score = new[] { 1.5, 2.5, 3.5, 4.5, 5.5 },
    });

    // ---------------- IPC ----------------

    [Fact]
    public void WriteIpcStreamNativeMemory_RoundTrip_MatchesByteArray()
    {
        using var df = SampleFrame();

        using var native = df.WriteIpcStreamNativeMemory(IpcCompression.LZ4);
        byte[] managed = df.WriteIpcStreamMemory(IpcCompression.LZ4);

        Assert.True(native.Length > 0);
        Assert.Equal(managed, native.ToArray());

        using var back = DataFrame.ReadIpcStream(native.GetSpan());
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void WriteIpcNativeMemory_RoundTrip()
    {
        using var df = SampleFrame();

        using var native = df.WriteIpcNativeMemory();
        using var back = DataFrame.ReadIpc(native.AsStream());

        Assert.True(df.Equals(back));
    }

    [Fact]
    public void SinkIpcNativeMemory_RoundTrip()
    {
        using var df = SampleFrame();

        using var native = df.Lazy().Filter(Col("id") > 2).SinkIpcNativeMemory();
        using var back = DataFrame.ReadIpc(native.ToArray());

        Assert.Equal(3, back.Height);
        Assert.Equal(3, back.GetValue<int>(0, "id"));
    }

    // ---------------- NativeBuffer semantics ----------------

    [Fact]
    public void NativeBuffer_IsMemoryOwner()
    {
        using var df = SampleFrame();
        using IMemoryOwner<byte> owner = df.WriteIpcStreamNativeMemory();

        Memory<byte> mem = owner.Memory;
        Assert.Equal(((NativeBuffer)owner).Length, mem.Length);
        Assert.Equal(((NativeBuffer)owner).ToArray(), mem.ToArray());
    }

    [Fact]
    public void NativeBuffer_DisposeIsIdempotent_AndAccessAfterDisposeThrows()
    {
        using var df = SampleFrame();
        var buf = df.WriteIpcStreamNativeMemory();

        ((IDisposable)buf).Dispose();
        ((IDisposable)buf).Dispose();

        Assert.True(buf.IsDisposed);
        Assert.Throws<ObjectDisposedException>(() => buf.GetSpan());
        Assert.Throws<ObjectDisposedException>(() => buf.Memory);
        Assert.Throws<ObjectDisposedException>(() => buf.AsStream());
        Assert.Throws<ObjectDisposedException>(() => buf.Pin());
    }

    [Fact]
    public unsafe void NativeBuffer_PinKeepsMemoryAliveAfterDispose()
    {
        using var df = SampleFrame();
        var buf = df.WriteIpcStreamNativeMemory();
        byte[] expected = buf.ToArray();

        using (var pin = buf.Memory.Pin())
        {
            ((IDisposable)buf).Dispose();
            var span = new ReadOnlySpan<byte>(pin.Pointer, expected.Length);
            Assert.Equal(expected, span.ToArray());
        }
    }

    [Fact]
    public void NativeBuffer_StreamKeepsMemoryAliveAfterDispose()
    {
        using var df = SampleFrame();
        var buf = df.WriteIpcStreamNativeMemory();
        byte[] expected = buf.ToArray();

        using var stream = buf.AsStream();
        ((IDisposable)buf).Dispose();

        Assert.False(stream.CanWrite);
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task NativeBuffer_WriteTo_And_WriteToAsync()
    {
        using var df = SampleFrame();
        using var buf = df.WriteIpcStreamNativeMemory();

        using var ms1 = new MemoryStream();
        buf.WriteTo(ms1);

        using var ms2 = new MemoryStream();
        await buf.WriteToAsync(ms2);

        Assert.Equal(buf.ToArray(), ms1.ToArray());
        Assert.Equal(buf.ToArray(), ms2.ToArray());
    }

    [Fact]
    public void NativeBuffer_PinOutOfRangeThrows()
    {
        using var df = SampleFrame();
        using var buf = df.WriteIpcStreamNativeMemory();

        Assert.Throws<ArgumentOutOfRangeException>(() => buf.Pin(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => buf.Pin(buf.Length + 1));
    }
}
