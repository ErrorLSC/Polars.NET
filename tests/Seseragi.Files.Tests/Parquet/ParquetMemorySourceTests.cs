using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Files held in memory: <see cref="ParquetFile.Open(ReadOnlyMemory{byte})"/> reads the memory in place, memory
/// streams with a visible buffer and unmanaged memory streams over a pointer are sliced, memory streams that hide their
/// buffer are copied once; every way reads what the file read by path holds, from any position of the stream.
/// </summary>
public class ParquetMemorySourceTests
{
    /// <summary>Larger than what opening reads at once (64 KiB), several row groups and pages.</summary>
    private static readonly string Large = ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet");

    /// <summary>Nested columns, smaller than 64 KiB (kept whole from the tail read when on a file or a store).</summary>
    private static readonly string Small = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet-pushdown", "nested_v2_pages.parquet");

    public static TheoryData<string> Files => [Large, Small];

    private static List<string> Rows(ParquetFile file, int parallelism)
    {
        using var reader = file.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = parallelism, BatchSize = 97 });
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))));
            }
        }
        return rows;
    }

    private static List<string> Expected(string path)
    {
        using var file = ParquetFile.Open(path);
        return Rows(file, 1);
    }

    private static ChunkReading Reading(ParquetFile file)
    {
        using var reader = file.OpenReader();
        return reader.EffectiveReading;
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void Memory_IsReadInPlace(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        // The file inside a larger buffer: the slice is what is read, never copied.
        byte[] padded = [.. new byte[37], .. bytes, .. new byte[11]];
        ReadOnlyMemory<byte> memory = padded.AsMemory(37, bytes.Length);
        using var file = ParquetFile.Open(memory);
        Assert.Equal(ChunkReading.Slice, Reading(file));
        Assert.True(((ILocalSource)file.Source).TryGetMemory(out ReadOnlyMemory<byte> held));
        Assert.True(MemoryMarshal.TryGetArray(held, out ArraySegment<byte> segment));
        Assert.Same(padded, segment.Array);
        Assert.Equal(37, segment.Offset);
        List<string> expected = Expected(path);
        Assert.Equal(expected, Rows(file, 1));
        Assert.Equal(expected, Rows(file, 8));
    }

    [Fact]
    public void Memory_ThatIsNotParquet_IsRefused()
    {
        Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(new byte[100].AsMemory()));
        Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(ReadOnlyMemory<byte>.Empty));
    }

    /// <summary>
    /// A plain memory stream that hides its buffer (<c>new MemoryStream(bytes)</c>) is read in place too: its array
    /// from the stream's origin and position, the stream left open until the file is disposed.
    /// </summary>
    [Theory]
    [MemberData(nameof(Files))]
    public async Task AMemoryStreamHidingItsBuffer_IsReadInPlace_FromItsOriginAndPosition(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        List<string> expected = Expected(path);
        foreach (bool useAsync in (bool[])[false, true])
        {
            // Origin 100 in the array, position 50 in the stream: the file starts at 150.
            byte[] padded = [.. new byte[150], .. bytes, .. new byte[7]];
            var stream = new MemoryStream(padded, 100, 50 + bytes.Length, writable: false) { Position = 50 };
            Assert.False(stream.TryGetBuffer(out _));
            ParquetFile file = useAsync ? await ParquetFile.OpenAsync(stream) : ParquetFile.Open(stream);
            using (file)
            {
                Assert.Equal(ChunkReading.Slice, Reading(file));
                Assert.True(((ILocalSource)file.Source).TryGetMemory(out ReadOnlyMemory<byte> held));
                Assert.True(MemoryMarshal.TryGetArray(held, out ArraySegment<byte> segment));
                Assert.Same(padded, segment.Array);
                Assert.Equal(150, segment.Offset);
                Assert.Equal(bytes.Length, segment.Count);
                Assert.Equal(expected, Rows(file, 1));
                Assert.Equal(expected, Rows(file, 8));
                Assert.True(stream.CanRead);
                Assert.Equal(50, stream.Position);
            }
            Assert.False(stream.CanRead);
        }
    }

    /// <summary>A subclass of <see cref="MemoryStream"/> that hides its buffer may keep its bytes elsewhere: read through the stream.</summary>
    [Fact]
    public void AMemoryStreamSubclassHidingItsBuffer_IsReadThroughTheStream()
    {
        byte[] bytes = File.ReadAllBytes(Large);
        using var file = ParquetFile.Open(new DerivedStream(bytes));
        Assert.Equal(ChunkReading.Fetch, Reading(file));
        Assert.False(((ILocalSource)file.Source).TryGetMemory(out _));
        Assert.Equal(Expected(Large), Rows(file, 8));
    }

    private sealed class DerivedStream(byte[] bytes) : MemoryStream(bytes, writable: false);

    [Fact]
    public void AMemoryStreamShowingItsBuffer_IsSliced_FromItsPosition()
    {
        byte[] bytes = File.ReadAllBytes(Large);
        byte[] padded = [.. new byte[100], .. bytes];
        var stream = new MemoryStream(padded, 0, padded.Length, writable: false, publiclyVisible: true) { Position = 100 };
        using var file = ParquetFile.Open(stream);
        Assert.True(stream.CanRead);
        Assert.Equal(ChunkReading.Slice, Reading(file));
        Assert.True(((ILocalSource)file.Source).TryGetMemory(out ReadOnlyMemory<byte> held));
        Assert.True(MemoryMarshal.TryGetArray(held, out ArraySegment<byte> segment));
        Assert.Same(padded, segment.Array);
        Assert.Equal(100, segment.Offset);
        Assert.Equal(Expected(Large), Rows(file, 8));
        using (var reader = file.OpenReader(new ParquetReadOptions { Reading = ChunkReading.Fetch }))
            Assert.Equal(Expected(Large).Count, reader.ReadBatches().Sum(b => { using (b) return b.Length; }));
        // Read at offsets from the buffer, never by seeking the stream.
        Assert.Equal(100, stream.Position);
    }

    [Theory]
    [MemberData(nameof(Files))]
    public unsafe void AnUnmanagedMemoryStreamOverAPointer_IsSliced_FromItsPosition(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        byte* native = (byte*)NativeMemory.Alloc((nuint)(bytes.Length + 64));
        try
        {
            bytes.CopyTo(new Span<byte>(native + 64, bytes.Length));
            using var stream = new UnmanagedMemoryStream(native, bytes.Length + 64) { Position = 64 };
            using var file = ParquetFile.Open(stream, leaveOpen: true);
            Assert.Equal(ChunkReading.Slice, Reading(file));
            Assert.True(((ILocalSource)file.Source).TryGetMemory(out ReadOnlyMemory<byte> held));
            fixed (byte* at = held.Span) Assert.True(at == native + 64, "the memory is the stream's, not a copy");
            List<string> expected = Expected(path);
            Assert.Equal(expected, Rows(file, 1));
            Assert.Equal(expected, Rows(file, 8));
        }
        finally
        {
            NativeMemory.Free(native);
        }
    }

    /// <summary>An unmanaged memory stream over a <see cref="SafeBuffer"/> has no pointer to give: read under the gate, correctly.</summary>
    [Fact]
    public void AnUnmanagedMemoryStreamOverASafeBuffer_IsReadThroughTheStream()
    {
        byte[] bytes = File.ReadAllBytes(Large);
        using var buffer = new NativeSafeBuffer((nuint)bytes.Length);
        buffer.WriteSpan(0, bytes.AsSpan());
        using var stream = new UnmanagedMemoryStream(buffer, 0, bytes.Length);
        using var file = ParquetFile.Open(stream, leaveOpen: true);
        Assert.Equal(ChunkReading.Fetch, Reading(file));
        Assert.False(((ILocalSource)file.Source).TryGetMemory(out _));
        Assert.Equal(Expected(Large), Rows(file, 8));
    }

    private sealed unsafe class NativeSafeBuffer : SafeBuffer
    {
        public NativeSafeBuffer(nuint length) : base(ownsHandle: true)
        {
            SetHandle((nint)NativeMemory.Alloc(length));
            Initialize(length);
        }

        protected override bool ReleaseHandle()
        {
            NativeMemory.Free((void*)handle);
            return true;
        }
    }
}
