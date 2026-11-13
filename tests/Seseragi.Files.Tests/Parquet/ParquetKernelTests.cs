using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Compression;
using Kaminari.Sazanami.Encodings;
using Kaminari.Seseragi.Files.Parquet.Encodings;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The fast decoding kernels against their bounds-checked reference paths, over every width, offset and length.</summary>
public class ParquetKernelTests
{
    [Fact]
    public void BitUnpack_EqualsTheCheckedPath_ForEveryWidthOffsetAndLength()
    {
        var random = new Random(7);
        for (int width = 0; width <= 32; width++)
        {
            foreach (int bytes in (int[])[0, 1, 7, 9, 64, 300])
            {
                byte[] packed = new byte[bytes];
                random.NextBytes(packed);
                long capacity = bytes * 8L / Math.Max(1, width);
                foreach (int first in (int[])[0, 1, 5, 8, 13, 16])
                {
                    for (int length = 0; length <= 70 && first + length <= Math.Max(capacity, width == 0 ? 100 : 0); length += length < 20 ? 1 : 7)
                    {
                        Check<int>(packed, first, width, length);
                        Check<uint>(packed, first, width, length);
                        if (width <= 8) Check<byte>(packed, first, width, length);
                    }
                }
            }
        }

        static void Check<T>(byte[] packed, int first, int width, int length) where T : unmanaged, System.Numerics.IBinaryInteger<T>
        {
            var fast = new T[length];
            var reference = new T[length];
            BitUnpacking.Unpack(packed, first, width, fast.AsSpan());
            BitUnpacking.UnpackChecked(packed, first, width, reference.AsSpan());
            Assert.True(fast.AsSpan().SequenceEqual(reference), $"width {width}, first {first}, length {length}, {packed.Length} bytes, {typeof(T).Name}");
        }
    }

    [Fact]
    public unsafe void Bitmap_AppendMaskAndAppendBits_EqualBitByBitAppends()
    {
        var random = new Random(11);
        for (int before = 0; before < 10; before++)
        {
            foreach (int length in (int[])[0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 64, 100, 257])
            {
                byte[] mask = new byte[length];
                for (int i = 0; i < length; i++) mask[i] = (byte)(random.Next(3) == 0 ? 0 : random.Next(1, 256));
                byte[] packed = new byte[(length + 16) / 8 + 2];
                random.NextBytes(packed);
                foreach (int firstBit in (int[])[0, 3, 8, 13])
                {
                    if (firstBit + length > packed.Length * 8) continue;
                    var fast = new NativeBitmap();
                    var slow = new NativeBitmap();
                    try
                    {
                        for (int i = 0; i < before; i++)
                        {
                            fast.Append(i % 3 == 0);
                            slow.Append(i % 3 == 0);
                        }
                        fast.AppendMask(mask);
                        foreach (byte m in mask) slow.Append(m != 0);
                        fast.AppendBits(packed, firstBit, length);
                        for (int i = 0; i < length; i++) slow.Append((packed[(firstBit + i) >> 3] >> ((firstBit + i) & 7) & 1) != 0);
                        Assert.Equal(slow.Length, fast.Length);
                        Assert.Equal(slow.UnsetCount, fast.UnsetCount);
                        long bytes = (slow.Length + 7) / 8;
                        byte* f = fast.Detach(out long fb);
                        byte* s = slow.Detach(out long sb);
                        Assert.True(new ReadOnlySpan<byte>(f, (int)bytes).SequenceEqual(new ReadOnlySpan<byte>(s, (int)bytes)), $"before {before}, length {length}, first {firstBit}");
                        System.Runtime.InteropServices.NativeMemory.AlignedFree(f);
                        System.Runtime.InteropServices.NativeMemory.AlignedFree(s);
                    }
                    finally
                    {
                        fast.Free();
                        slow.Free();
                    }
                }
            }
        }
    }

    [Fact]
    public void SnappyBlock_DecodesWhatSnappierEncodes_AndRejectsDamage()
    {
        var random = new Random(5);
        List<byte[]> inputs = [[], [42], "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc"u8.ToArray()];
        foreach (int size in (int[])[1, 7, 15, 16, 17, 63, 64, 65, 1000, 70_000, 300_000])
        {
            byte[] noise = new byte[size];
            random.NextBytes(noise);
            inputs.Add(noise);
            byte[] text = new byte[size];
            for (int i = 0; i < size; i++) text[i] = (byte)"lorem ipsum dolor sit amet "[(i * 7 + i / 13) % 27];
            inputs.Add(text);
            byte[] runs = new byte[size];
            for (int i = 0; i < size; i++) runs[i] = (byte)(i / (1 + i % 9) % 3); // short periods: overlapping copies
            inputs.Add(runs);
        }
        foreach (byte[] input in inputs)
        {
            byte[] compressed = new byte[Snappier.Snappy.GetMaxCompressedLength(input.Length)];
            int n = Snappier.Snappy.Compress(input, compressed);
            byte[] output = new byte[input.Length];
            Assert.Equal(input.Length, SnappyBlock.UncompressedLength(compressed.AsSpan(0, n)));
            Assert.Equal(input.Length, SnappyBlock.Decompress(compressed.AsSpan(0, n), output));
            Assert.True(output.AsSpan().SequenceEqual(input), $"{input.Length} bytes");

            if (n < 2) continue;
            for (int round = 0; round < 50; round++)
            {
                byte[] damaged = compressed[..n];
                damaged[random.Next(n)] ^= (byte)random.Next(1, 256);
                int cut = random.Next(2) == 0 ? n : random.Next(n);
                try
                {
                    SnappyBlock.Decompress(damaged.AsSpan(0, cut), new byte[input.Length]);
                }
                catch (InvalidDataException)
                {
                }
            }
        }
    }

    [Fact]
    public unsafe void ExportedBatch_KeepsItsMemory_UntilTheExportIsReleased_ThoughDisposed()
    {
        using var file = Kaminari.Seseragi.Files.Parquet.ParquetFile.Open(ParquetFixtures.FullPath("writers/pyarrow.parquet"));
        using var reader = file.OpenReader(new() { MaxDegreeOfParallelism = 1 });
        Apache.Arrow.RecordBatch batch = reader.ReadBatches().First();
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(batch.Column(0).Data.Buffers[1].Memory, out NativeBufferMemory? memory));
        NativeBatchMemory owner = memory!.Owner;

        Apache.Arrow.C.CArrowArray* exported = Apache.Arrow.C.CArrowArray.Create();
        Apache.Arrow.C.CArrowArrayExporter.ExportRecordBatch(batch, exported);
        batch.Dispose();
        Assert.False(owner.IsFreed); // the export's pins hold it
        Apache.Arrow.C.CArrowArray.Free(exported); // releases the export
        Assert.True(owner.IsFreed);

        // A pin alone (what any consumer handing the memory on takes) keeps it too.
        Apache.Arrow.RecordBatch pinned = reader.ReadBatches().First();
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(pinned.Column(0).Data.Buffers[1].Memory, out NativeBufferMemory? pinnedMemory));
        System.Buffers.MemoryHandle handle = pinned.Column(0).Data.Buffers[1].Memory.Pin();
        pinned.Dispose();
        Assert.False(pinnedMemory!.Owner.IsFreed);
        handle.Dispose();
        Assert.True(pinnedMemory.Owner.IsFreed);

        // Without an export, disposing frees it at once.
        Apache.Arrow.RecordBatch other = reader.ReadBatches().First();
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(other.Column(0).Data.Buffers[1].Memory, out NativeBufferMemory? otherMemory));
        other.Dispose();
        Assert.True(otherMemory!.Owner.IsFreed);
    }
}
