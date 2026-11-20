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

    /// <summary>
    /// Typed rows written as Parquet, the writer owning them: it takes the row batcher's batches and encodes row groups side by side
    /// after Write returns. The same bytes with guards on, so a batch freed while a row group still encodes it ends the
    /// process instead of passing by luck.
    /// </summary>
    [Fact]
    public void TypedParquetRows_WriteTheSame_WithGuardsOn()
    {
        Row[] rows = [.. Enumerable.Range(0, 200_000).Select(i => new Row(i, $"name {i % 777}", i % 3 == 0 ? null : i * 0.5))];
        byte[] plain = Write(rows);
        GuardedMemory.Enabled = true;
        Assert.Equal(plain, Write(rows));

        static byte[] Write(Row[] rows)
        {
            using var memory = new MemoryStream();
            ParquetWriter.WriteRows(memory, rows, new ParquetWriteOptions
            {
                MaxDegreeOfParallelism = 8, RowGroupRows = 20_000, LeaveOpen = true, BatchOwnership = BatchOwnership.Writer,
            });
            return memory.ToArray();
        }
    }

    public sealed record Row(long Id, string Name, double? Value);

    /// <summary>
    /// Typed rows written as datasets: the row batcher frees each batch once the write returns, while an unpartitioned
    /// Parquet file keeps slices of it for row groups encoded side by side, and an unpartitioned IPC file writes it as
    /// record batches; partitioned files take gathered copies. The same bytes with guards on, so a batch freed while
    /// still referenced ends the process instead of passing by luck.
    /// </summary>
    [Fact]
    public void TypedDatasets_WriteTheSame_WithGuardsOn()
    {
        Row[] rows = [.. Enumerable.Range(0, 300_000).Select(i => new Row(i, $"name {i % 777}", i % 3 == 0 ? null : i * 0.5))];
        string root = Directory.CreateTempSubdirectory("guarded-typed-datasets-").FullName;
        try
        {
            Dictionary<string, byte[]> plain = Write("plain");
            Assert.Equal(6, plain.Count);
            GuardedMemory.Enabled = true;
            Dictionary<string, byte[]> guarded = Write("guarded");
            GuardedMemory.Enabled = false;
            Assert.Equal(plain.Keys.Order(), guarded.Keys.Order());
            foreach ((string name, byte[] bytes) in plain) Assert.True(bytes.AsSpan().SequenceEqual(guarded[name]), name);
        }
        finally
        {
            GuardedMemory.Enabled = false;
            Directory.Delete(root, recursive: true);
        }

        Dictionary<string, byte[]> Write(string run)
        {
            string at = Path.Combine(root, run);
            static string Name(DatasetFileName n) => $"f-{n.Index}{n.Extension}";
            ParquetDatasetWriter.WriteRows($"{at}/parquet", rows, new ParquetDatasetWriteOptions { FileName = Name, MaxDegreeOfParallelism = 4 });
            IpcDatasetWriter.WriteRows($"{at}/ipc", rows, new IpcDatasetWriteOptions
            {
                FileName = Name, MaxDegreeOfParallelism = 4, Writer = new IpcWriteOptions { Compression = Lz4FrameCompression.Instance },
            });
            ParquetDatasetWriter.WriteRows($"{at}/parts", rows.Select(r => r with { Id = r.Id % 4 }),
                new ParquetDatasetWriteOptions { FileName = Name, PartitionBy = [nameof(Row.Id)], MaxDegreeOfParallelism = 4 });
            return Directory.GetFiles(at, "*", SearchOption.AllDirectories).ToDictionary(f => Path.GetRelativePath(at, f), File.ReadAllBytes);
        }
    }

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

    /// <summary>
    /// Files of mixed types (dictionaries of other index types and value layouts, lists and list views) read as one
    /// dataset with unified dictionaries, the batches kept and read after the read and the reader are gone: the same
    /// with guards on (the dictionaries' blocks shared by the batches, list views gathered by intermediates held).
    /// </summary>
    [Fact]
    public void IpcDatasets_WithUnifiedDictionariesAndConversions_ReadTheSame_WithGuardsOn()
    {
        string root = Directory.CreateTempSubdirectory("guarded-ipc-types-").FullName;
        try
        {
            var random = new Random(1123);
            // Enough distinct values (some long) for the unified dictionaries to outgrow their buffers while earlier
            // batches still point into the old ones.
            string[] words = [.. Enumerable.Range(0, 2000).Select(i => i % 3 == 0 ? $"w{i}" : $"a value longer than twelve bytes, number {i}")];
            for (int f = 0; f < 6; f++)
            {
                bool odd = f % 2 == 1;
                var dictionaryType = new Apache.Arrow.Types.DictionaryType(odd ? Apache.Arrow.Types.Int32Type.Default : Apache.Arrow.Types.Int8Type.Default,
                    odd ? Apache.Arrow.Types.StringViewType.Default : Apache.Arrow.Types.StringType.Default, false);
                var listType = odd
                    ? (Apache.Arrow.Types.IArrowType)new Apache.Arrow.Types.ListViewType(new Apache.Arrow.Field("item", Apache.Arrow.Types.Int64Type.Default, true))
                    : new Apache.Arrow.Types.ListType(new Apache.Arrow.Field("item", Apache.Arrow.Types.Int64Type.Default, true));
                var schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("d", dictionaryType, true), new Apache.Arrow.Field("l", listType, true)], null);
                using IpcWriter writer = IpcWriter.Create(Path.Combine(root, $"f{f}.arrow"), schema);
                // A dictionary of some words (in another order per file: one per file), indices into it.
                string[] entries = [.. words.OrderBy(_ => random.Next()).Take(random.Next(100, 120))];
                for (int b = 0; b < 3; b++)
                {
                    int length = random.Next(1, 400);
                    IArrowArray dictionary = odd
                        ? new StringViewArray.Builder().AppendRange(entries).Build()
                        : new StringArray.Builder().AppendRange(entries).Build();
                    IArrowArray indices = odd
                        ? new Int32Array.Builder().AppendRange(Enumerable.Range(0, length).Select(_ => random.Next(entries.Length))).Build()
                        : new Int8Array.Builder().AppendRange(Enumerable.Range(0, length).Select(_ => (sbyte)random.Next(Math.Min(entries.Length, 127)))).Build();
                    var column = new DictionaryArray(dictionaryType, indices, dictionary);
                    // Lists of 0..3 numbers; as views, each row's items placed after the others in reverse order.
                    long[][] rows = [.. Enumerable.Range(0, length).Select(_ => Enumerable.Range(0, random.Next(4)).Select(_ => (long)random.Next(1000)).ToArray())];
                    var items = new Int64Array.Builder().AppendRange(odd ? rows.Reverse().SelectMany(r => r) : rows.SelectMany(r => r)).Build();
                    var bounds = new List<int>();
                    int at = 0;
                    if (odd)
                    {
                        int total = rows.Sum(r => r.Length);
                        int[] starts = new int[length];
                        int end = total;
                        for (int i = 0; i < length; i++)
                        {
                            end -= rows[i].Length;
                            starts[i] = end;
                        }
                        bounds.AddRange(starts);
                    }
                    else
                    {
                        foreach (long[] row in rows) { bounds.Add(at); at += row.Length; }
                        bounds.Add(at);
                    }
                    ArrowBuffer Ints(IEnumerable<int> values)
                    {
                        byte[] bytes = [.. values.SelectMany(BitConverter.GetBytes)];
                        return new ArrowBuffer(bytes);
                    }
                    ArrayData listData = odd
                        ? new ArrayData(listType, length, 0, 0, [ArrowBuffer.Empty, Ints(bounds), Ints(rows.Select(r => r.Length))], [items.Data])
                        : new ArrayData(listType, length, 0, 0, [ArrowBuffer.Empty, Ints(bounds)], [items.Data]);
                    using var batch = new RecordBatch(schema, [column, ArrowArrayFactory.BuildArray(listData)], length);
                    writer.Write(batch);
                }
                writer.Complete();
            }
            var options = new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName, UnifyDictionaries = true };
            string plain = ReadAfter(root, options);
            GuardedMemory.Enabled = true;
            Assert.Equal(plain, ReadAfter(root, options));
        }
        finally
        {
            GuardedMemory.Enabled = false;
            Directory.Delete(root, recursive: true);
        }

        // Every batch read, then rendered once the reader and the dataset are gone.
        static string ReadAfter(string root, IpcDatasetOptions options)
        {
            List<RecordBatch> batches;
            using (var dataset = IpcDataset.Open(root, options))
            using (IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = 8 }))
                batches = [.. reader.ReadBatches()];
            List<string> lines = [];
            foreach (RecordBatch batch in batches)
            {
                using (batch)
                {
                    var d = (DictionaryArray)batch.Column(0);
                    var l = (ListArray)batch.Column(1);
                    var indices = (Int32Array)d.Indices;
                    for (int i = 0; i < batch.Length; i++)
                    {
                        string value = d.Dictionary switch
                        {
                            StringArray a => a.GetString(indices.GetValue(i)!.Value),
                            StringViewArray a => a.GetString(indices.GetValue(i)!.Value),
                            var other => other.GetType().Name,
                        };
                        var items = (Int64Array)l.Values;
                        lines.Add($"{indices.GetValue(i)}:{value}|[{string.Join(',', Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(k => items.GetValue(k)))}]");
                    }
                }
            }
            return string.Join('\n', lines);
        }
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
