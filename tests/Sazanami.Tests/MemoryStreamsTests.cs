using Kaminari.Sazanami.Buffers;

namespace Sazanami.Tests;

/// <summary><see cref="MemoryStreams.TryGetBytes"/>: a memory stream's bytes from its origin, visible or hidden; subclasses only when visible.</summary>
public class MemoryStreamsTests
{
    private static readonly byte[] Array = [.. Enumerable.Range(0, 200).Select(i => (byte)i)];

    [Fact]
    public void AHiddenBuffer_IsReadFromTheOrigin()
    {
        var stream = new MemoryStream(Array, 30, 100, writable: false) { Position = 7 };
        Assert.False(stream.TryGetBuffer(out _));
        Assert.True(MemoryStreams.TryGetBytes(stream, out ArraySegment<byte> bytes));
        Assert.Same(Array, bytes.Array);
        Assert.Equal(30, bytes.Offset);
        Assert.Equal(100, bytes.Count);
        Assert.Equal(7, stream.Position);
    }

    [Fact]
    public void AVisibleBuffer_IsTheStreamsOwn()
    {
        var stream = new MemoryStream(Array, 10, 50, writable: false, publiclyVisible: true);
        Assert.True(MemoryStreams.TryGetBytes(stream, out ArraySegment<byte> bytes));
        Assert.Same(Array, bytes.Array);
        Assert.Equal((10, 50), (bytes.Offset, bytes.Count));
    }

    [Fact]
    public void AnExpandableStream_GivesWhatWasWritten()
    {
        var stream = new MemoryStream();
        stream.Write([1, 2, 3]);
        Assert.True(MemoryStreams.TryGetBytes(stream, out ArraySegment<byte> bytes));
        Assert.Equal([1, 2, 3], bytes.ToArray());
    }

    [Fact]
    public void ASubclassHidingItsBuffer_GivesNothing()
    {
        Assert.False(MemoryStreams.TryGetBytes(new Derived(Array), out _));
        Assert.True(MemoryStreams.TryGetBytes(new DerivedVisible(Array), out ArraySegment<byte> visible));
        Assert.Equal(Array.Length, visible.Count);
    }

    private sealed class Derived(byte[] bytes) : MemoryStream(bytes, writable: false);

    private sealed class DerivedVisible(byte[] bytes) : MemoryStream(bytes, 0, bytes.Length, writable: false, publiclyVisible: true);
}
