using Apache.Arrow;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.Parquet;

namespace Polars.Config.Tests;

/// <summary>
/// The fuzzer's guard pages (process-wide switch, hence here): blocks of the native pool and guarded copies are usable
/// to their full size and freed cleanly, blocks made either side of the switch are freed by their own kind, and a
/// Parquet file reads the same with guards on. (A read past a block's end kills the process — the fuzzer's selftest
/// shows that in a child process.)
/// </summary>
public sealed unsafe class GuardedMemoryTests : IDisposable
{
    public void Dispose() => GuardedMemory.Enabled = false;

    [Fact]
    public void GuardedBlocks_AreUsableToTheirEnd_AndFreedEitherSideOfTheSwitch()
    {
        byte* before = NativeMemoryPool.Alloc(100_000);
        GuardedMemory.Enabled = true;
        foreach (long size in (long[])[0, 1, 63, 64, 65, 4095, 4096, 4097, 100_000, 3 << 20])
        {
            byte* p = NativeMemoryPool.Alloc(size);
            Assert.Equal(0, (nint)p % NativeMemoryPool.Alignment);
            long usable = NativeMemoryPool.UsableSize(p);
            Assert.InRange(usable - Math.Max(size, 8), 0, NativeMemoryPool.Alignment - 1);
            new Span<byte>(p, (int)usable).Fill(0xA5); // the last usable byte sits right before the guard page
            NativeMemoryPool.Free(p);
        }
        using (var block = GuardedMemory.Block.Copy([1, 2, 3]))
        {
            Assert.Equal((byte[])[1, 2, 3], block.GetSpan().ToArray());
        }
        byte* during = NativeMemoryPool.Alloc(100_000);
        GuardedMemory.Enabled = false;
        NativeMemoryPool.Free(before); // an ordinary block, freed while guards were on and after
        NativeMemoryPool.Free(during); // a guarded block, freed after the switch went off
    }

    [Fact]
    public void ParquetFiles_ReadTheSame_WithGuardsOn()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "duckdb_zstd.parquet");
        string plain = Render(path);
        GuardedMemory.Enabled = true;
        Assert.Equal(plain, Render(path));

        static string Render(string path)
        {
            using var file = ParquetFile.Open(path);
            using var reader = file.OpenReader(new() { MaxDegreeOfParallelism = 2 });
            List<string> rows = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    for (int c = 0; c < batch.ColumnCount; c++)
                        rows.Add($"{batch.Schema.GetFieldByIndex(c).Name}: {string.Join(",", Enumerable.Range(0, batch.Length).Select(i => Value(batch.Column(c), i)))}");
                }
            }
            return string.Join("\n", rows);
        }

        static string Value(IArrowArray array, int i) => array.IsNull(i) ? "null" : array switch
        {
            Int32Array a => $"{a.GetValue(i)}",
            Int64Array a => $"{a.GetValue(i)}",
            DoubleArray a => $"{a.GetValue(i)}",
            StringViewArray a => a.GetString(i),
            StringArray a => a.GetString(i),
            BooleanArray a => $"{a.GetValue(i)}",
            _ => array.GetType().Name,
        };
    }
}
