using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
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

    /// <summary>
    /// Typed rows written as Arrow IPC with the writer owning their batches (compressed side by side, after Write
    /// returns): the same bytes with guards on, where a block is unmapped once freed, so a batch freed before the
    /// writer is done with it (by the row batcher, say) ends the process instead of passing by luck.
    /// </summary>
    [Fact]
    public void TypedIpcRows_WriteTheSame_WithGuardsOn()
    {
        Row[] rows = [.. Enumerable.Range(0, 60_000).Select(i => new Row(i, $"name {i % 777}", i % 3 == 0 ? null : i * 0.5))];
        byte[] plain = Write(rows, long.MaxValue);
        GuardedMemory.Enabled = true;
        Assert.Equal(plain, Write(rows, long.MaxValue));
        Assert.Equal(plain, Write(rows, 0)); // every record batch written as it is compressed, each buffer freed once written

        static byte[] Write(Row[] rows, long streamBodyFrom)
        {
            using var memory = new MemoryStream();
            IpcWriter.WriteRows(memory, rows, new IpcWriteOptions { Compression = Lz4FrameCompression.Instance, MaxDegreeOfParallelism = 4, LeaveOpen = true,
                StreamBodyFrom = streamBodyFrom });
            return memory.ToArray();
        }
    }

    public sealed record Row(long Id, string Name, double? Value);

    /// <summary>
    /// An Arrow IPC dataset read through its window (files and batches side by side, rows numbered and filtered, a read
    /// stopped early): the same rows with guards on, every block freed only once nothing reads it.
    /// </summary>
    [Fact]
    public void IpcDatasets_ReadTheSame_WithGuardsOn()
    {
        string root = Directory.CreateTempSubdirectory("guarded-ipc-dataset-").FullName;
        try
        {
            Row[] rows = [.. Enumerable.Range(0, 30_000).Select(i => new Row(i, $"name {i % 777}", i % 3 == 0 ? null : i * 0.5))];
            for (int f = 0; f < 12; f++)
            {
                string dir = Path.Combine(root, $"p={f % 3}");
                Directory.CreateDirectory(dir);
                IpcWriter.WriteRows(Path.Combine(dir, $"f{f:D2}.arrow"), rows.Skip(f * 2500).Take(2500),
                    new IpcWriteOptions { Compression = f % 2 == 0 ? Lz4FrameCompression.Instance : NoCompression.Instance });
            }
            Kaminari.Samidare.IR.ScalarExpr filter = Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Lt,
                Kaminari.Samidare.IR.ScalarExpr.NewColumn("Id"), Kaminari.Samidare.IR.ScalarExpr.NewLiteral(Kaminari.Samidare.IR.LiteralValue.NewInt64(20_000)));
            var options = new IpcReadOptions { MaxDegreeOfParallelism = 8, RowIndexColumn = "row", Filter = filter, FilterRows = true };
            string plain = Read(root, options, stopAfter: int.MaxValue);
            string early = Read(root, options, stopAfter: 3);
            GuardedMemory.Enabled = true;
            Assert.Equal(plain, Read(root, options, stopAfter: int.MaxValue));
            Assert.Equal(early, Read(root, options, stopAfter: 3));
        }
        finally
        {
            GuardedMemory.Enabled = false;
            Directory.Delete(root, recursive: true);
        }

        static string Read(string root, IpcReadOptions options, int stopAfter)
        {
            using var dataset = IpcDataset.Open(root);
            using IpcDatasetReader reader = dataset.OpenReader(options);
            List<string> lines = [];
            int taken = 0;
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    for (int i = 0; i < batch.Length; i++)
                        lines.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => batch.Column(c).IsNull(i) ? "null" : Value(batch.Column(c), i))));
                }
                if (++taken == stopAfter) break;
            }
            return string.Join('\n', lines);
        }

        static string Value(IArrowArray array, int i) => array switch
        {
            Int64Array a => a.GetValue(i)!.Value.ToString(),
            DoubleArray a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            StringArray a => a.GetString(i),
            StringViewArray a => a.GetString(i),
            _ => array.GetType().Name,
        };
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
