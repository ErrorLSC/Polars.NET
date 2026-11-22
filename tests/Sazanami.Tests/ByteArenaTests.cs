using Kaminari.Sazanami.Buffers;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ByteArena"/>: values written in place keep their bytes whatever is written after them (in the same
/// chunk, in later chunks, or in arrays of their own), the room not taken is used by the next value, and chunks start
/// small and double up to the largest size.
/// </summary>
public class ByteArenaTests
{
    [Fact]
    public void Values_KeepTheirBytes_AcrossChunksAndOwnArrays()
    {
        var arena = new ByteArena(firstChunkBytes: 64, maxChunkBytes: 64);
        var random = new Random(5);
        List<(ReadOnlyMemory<byte> Value, byte[] Expected)> values = [];
        for (int i = 0; i < 2000; i++)
        {
            // Mostly small values, some over a quarter of a chunk; room asked for larger than what is written.
            byte[] bytes = new byte[random.Next(5) == 0 ? random.Next(17, 100) : random.Next(0, 16)];
            random.NextBytes(bytes);
            ReadOnlyMemory<byte> value;
            if (i % 2 == 0) value = arena.Add(bytes);
            else
            {
                Span<byte> room = arena.GetSpan(bytes.Length + random.Next(0, 8));
                bytes.CopyTo(room);
                value = arena.Take(bytes.Length);
            }
            values.Add((value, bytes));
        }
        foreach (var (value, expected) in values) Assert.Equal(expected, value.ToArray());
    }

    [Fact]
    public void RoomNotTaken_IsTheNextValues()
    {
        var arena = new ByteArena(firstChunkBytes: 64, maxChunkBytes: 64);
        ReadOnlyMemory<byte> first = arena.Add([1, 2, 3]);
        arena.GetSpan(10).Fill(9);
        ReadOnlyMemory<byte> second = arena.Take(2);
        ReadOnlyMemory<byte> third = arena.Add([7]);
        Assert.Equal([1, 2, 3], first.ToArray());
        Assert.Equal([9, 9], second.ToArray());
        Assert.Equal([7], third.ToArray());
        // Consecutive in one chunk: the second value starts where the first ends, the third where the second does.
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(first, out ArraySegment<byte> a));
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(third, out ArraySegment<byte> c));
        Assert.Same(a.Array, c.Array);
        Assert.Equal(5, c.Offset);
    }

    [Fact]
    public void TakingMoreThanTheRoom_IsRefused()
    {
        var arena = new ByteArena(firstChunkBytes: 64, maxChunkBytes: 64);
        arena.GetSpan(4);
        Assert.ThrowsAny<ArgumentOutOfRangeException>(() => arena.Take(65));
    }

    [Fact]
    public void Chunks_StartSmall_AndDoubleUpToTheLargest()
    {
        var arena = new ByteArena(firstChunkBytes: 16, maxChunkBytes: 256);
        List<int> sizes = [];
        byte[]? last = null;
        for (int i = 0; i < 200; i++)
        {
            Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(arena.Add([1, 2, 3, 4, 5, 6, 7, 8]), out ArraySegment<byte> segment));
            if (!ReferenceEquals(segment.Array, last)) sizes.Add(segment.Array!.Length);
            last = segment.Array;
        }
        Assert.Equal([16, 32, 64, 128, 256, 256, 256], sizes[..7]);
        // A first value larger than the first chunk gets a chunk that holds it.
        var other = new ByteArena(firstChunkBytes: 4, maxChunkBytes: 256);
        Assert.Equal(new byte[40], other.Add(new byte[40]).ToArray());
    }
}
