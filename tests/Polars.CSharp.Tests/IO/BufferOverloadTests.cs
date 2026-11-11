using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace Polars.CSharp.Tests;

/// <summary>
/// In-memory scans and reads over byte[] (copied), ReadOnlySpan (copied) and ReadOnlyMemory (shared, pinned until
/// the engine drops it) (#35).
/// </summary>
public class BufferOverloadTests
{
    private static DataFrame Sample() => DataFrame.FromColumns([
        Series.From("id", Enumerable.Range(0, 1000).ToArray()),
        Series.From("name", Enumerable.Range(0, 1000).Select(i => $"n{i}").ToArray()),
    ]);

    private static byte[] Bytes(string format)
    {
        using var df = Sample();
        var path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.{format}");
        try
        {
            switch (format)
            {
                case "parquet": df.WriteParquet(path); break;
                case "ipc": df.WriteIpc(path); break;
                case "avro": df.WriteAvro(path); break;
                case "csv": return Encoding.UTF8.GetBytes("id,name\n" + string.Concat(Enumerable.Range(0, 1000).Select(i => $"{i},n{i}\n")));
                case "ndjson": return Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, 1000).Select(i => $"{{\"id\":{i},\"name\":\"n{i}\"}}\n")));
                default: throw new ArgumentOutOfRangeException(nameof(format));
            }
            return File.ReadAllBytes(path);
        }
        finally { File.Delete(path); }
    }

    private static LazyFrame ScanBytes(string format, byte[] bytes) => format switch
    {
        "parquet" => LazyFrame.ScanParquet(bytes),
        "ipc" => LazyFrame.ScanIpc(bytes),
        "csv" => LazyFrame.ScanCsv(bytes),
        _ => LazyFrame.ScanNdjson(bytes),
    };

    private static LazyFrame ScanSpan(string format, ReadOnlySpan<byte> bytes) => format switch
    {
        "parquet" => LazyFrame.ScanParquet(bytes),
        "ipc" => LazyFrame.ScanIpc(bytes),
        "csv" => LazyFrame.ScanCsv(bytes),
        _ => LazyFrame.ScanNdjson(bytes),
    };

    private static LazyFrame ScanMemory(string format, ReadOnlyMemory<byte> bytes) => format switch
    {
        "parquet" => LazyFrame.ScanParquet(bytes),
        "ipc" => LazyFrame.ScanIpc(bytes),
        "csv" => LazyFrame.ScanCsv(bytes),
        _ => LazyFrame.ScanNdjson(bytes),
    };

    private static void AssertSample(DataFrame df)
    {
        Assert.Equal(1000, df.Height);
        Assert.Equal(999L, Convert.ToInt64(df[999, "id"]));
        Assert.Equal("n999", df[999, "name"]);
    }

    public static TheoryData<string> ScanFormats => ["parquet", "ipc", "csv", "ndjson"];

    [Theory]
    [MemberData(nameof(ScanFormats))]
    public void Every_Overload_Reads_The_Same_Rows(string format)
    {
        var bytes = Bytes(format);

        // A slice of a larger array: the memory overloads read from its offset.
        var padded = new byte[bytes.Length + 64];
        bytes.CopyTo(padded, 32);
        var slice = new ReadOnlyMemory<byte>(padded, 32, bytes.Length);

        using (var lf = ScanBytes(format, bytes)) using (var df = lf.Collect()) AssertSample(df);
        using (var lf = ScanSpan(format, slice.Span)) using (var df = lf.Collect()) AssertSample(df);
        using (var lf = ScanMemory(format, slice)) using (var df = lf.Collect()) AssertSample(df);
        using (var unmanaged = new TrackingMemory(bytes))
        using (var lf = ScanMemory(format, unmanaged.Memory))
        using (var df = lf.Collect())
            AssertSample(df);
    }

    [Theory]
    [MemberData(nameof(ScanFormats))]
    public void Byte_Array_And_Span_Are_Copied_Memory_Is_Shared(string format)
    {
        var bytes = Bytes(format);

        // Copied: changing the caller's bytes after the scan does not matter.
        var copied = (byte[])bytes.Clone();
        using (var lf = ScanBytes(format, copied))
        {
            Array.Clear(copied);
            using var df = lf.Collect();
            AssertSample(df);
        }
        var spanned = (byte[])bytes.Clone();
        using (var lf = ScanSpan(format, spanned))
        {
            Array.Clear(spanned);
            using var df = lf.Collect();
            AssertSample(df);
        }

        // Shared: the engine reads the caller's memory when the query runs.
        var shared = (byte[])bytes.Clone();
        using (var lf = ScanMemory(format, shared))
        {
            Array.Clear(shared);
            Assert.ThrowsAny<Exception>(() =>
            {
                using var df = lf.Collect();
                AssertSample(df);
            });
        }
    }

    [Theory]
    [MemberData(nameof(ScanFormats))]
    public void Shared_Memory_Is_Unpinned_When_The_Last_Frame_Is_Released(string format)
    {
        using var memory = new TrackingMemory(Bytes(format));

        var lf = ScanMemory(format, memory.Memory);
        var clone = lf.Clone();
        Assert.Equal(1, memory.Pins);

        lf.Dispose();
        Assert.Equal(0, memory.Unpins); // the clone still reads it
        using (var df = clone.Collect()) AssertSample(df);

        clone.Dispose();
        Assert.Equal(1, memory.Unpins);
    }

    /// <summary>Waits (bounded) for the engine to unpin: a background query may still hold the plan briefly.</summary>
    private static async Task AssertAllUnpinned(TrackingMemory memory)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (memory.Unpins < memory.Pins && sw.ElapsedMilliseconds < 5000) await Task.Delay(10);
        Assert.Equal(memory.Pins, memory.Unpins);
    }

    [Theory]
    [MemberData(nameof(ScanFormats))]
    public async Task Shared_Memory_Is_Unpinned_After_Streaming_And_An_Early_Break(string format)
    {
        using var memory = new TrackingMemory(Bytes(format));
        using (var lf = ScanMemory(format, memory.Memory))
        using (var df = lf.Collect(Engine.Streaming))
            AssertSample(df);
        Assert.Equal(1, memory.Unpins);

        // The batch runner may still hold the plan on its thread right after the break.
        using (var lf = ScanMemory(format, memory.Memory))
            await foreach (var batch in lf.CollectBatchesAsync(10, Engine.Streaming)) { batch.Dispose(); break; }
        await AssertAllUnpinned(memory);
    }

    [Fact]
    public void Options_Reach_The_Engine_Through_Every_Overload()
    {
        var csv = Encoding.UTF8.GetBytes("a;b\n1;x\n2;y\n3;z\n");
        foreach (var lf in new[]
        {
            LazyFrame.ScanCsv(csv, separator: ';', nRows: 2, rowIndexName: "idx"),
            LazyFrame.ScanCsv(csv.AsSpan(), separator: ';', nRows: 2, rowIndexName: "idx"),
            LazyFrame.ScanCsv(csv.AsMemory(), separator: ';', nRows: 2, rowIndexName: "idx"),
        })
        {
            using (lf)
            using (var df = lf.Collect())
            {
                Assert.Equal(["idx", "a", "b"], df.Columns);
                Assert.Equal(2, df.Height);
                Assert.Equal("y", df[1, "b"]);
            }
        }

        var parquet = Bytes("parquet");
        LazyFrame[] lfs = [
            LazyFrame.ScanParquet(parquet, nRows: 5, rowIndexName: "idx", rowIndexOffset: 10),
            LazyFrame.ScanParquet(parquet.AsSpan(), nRows: 5, rowIndexName: "idx", rowIndexOffset: 10),
            LazyFrame.ScanParquet(parquet.AsMemory(), nRows: 5, rowIndexName: "idx", rowIndexOffset: 10),
        ];
        foreach (var lf in lfs)
        {
            using (lf)
            using (var df = lf.Collect())
            {
                Assert.Equal(5, df.Height);
                Assert.Equal(10L, Convert.ToInt64(df[0, "idx"]));
            }
        }
    }

    [Fact]
    public void A_Collected_Frame_Keeps_Shared_Memory_It_References()
    {
        // An IPC scan may hand out the buffer itself; the result keeps the memory pinned while it lives.
        using var memory = new TrackingMemory(Bytes("ipc"));
        DataFrame df;
        using (var lf = LazyFrame.ScanIpc(memory.Memory)) df = lf.Collect();
        using (df)
            AssertSample(df);
        Assert.Equal(memory.Pins, memory.Unpins);
    }

    [Fact]
    public void Empty_Buffers_Are_Refused_Without_Pinning()
    {
        using var memory = new TrackingMemory([]);
        Assert.Throws<ArgumentException>(() => LazyFrame.ScanParquet(memory.Memory));
        Assert.Throws<ArgumentException>(() => LazyFrame.ScanCsv([]));
        Assert.Equal(0, memory.Pins);
    }

    [Fact]
    public void Eager_Reads_Take_Every_Buffer_Kind()
    {
        string[] formats = ["parquet", "ipc", "csv", "avro"];
        foreach (var format in formats)
        {
            var bytes = Bytes(format);
            var memory = new ReadOnlyMemory<byte>(bytes);
            DataFrame[] frames = format switch
            {
                "parquet" => [DataFrame.ReadParquet(bytes), DataFrame.ReadParquet(memory.Span), DataFrame.ReadParquet(memory)],
                "ipc" => [DataFrame.ReadIpc(bytes), DataFrame.ReadIpc(memory.Span), DataFrame.ReadIpc(memory)],
                "csv" => [DataFrame.ReadCsv(bytes), DataFrame.ReadCsv(memory.Span), DataFrame.ReadCsv(memory)],
                _ => [DataFrame.ReadAvro(bytes), DataFrame.ReadAvro(memory.Span), DataFrame.ReadAvro(memory)],
            };
            foreach (var df in frames)
                using (df)
                    AssertSample(df);
        }

        var json = Encoding.UTF8.GetBytes("[" + string.Join(",", Enumerable.Range(0, 1000).Select(i => $"{{\"id\":{i},\"name\":\"n{i}\"}}")) + "]");
        DataFrame[] dfs = [DataFrame.ReadJson(json), DataFrame.ReadJson(json.AsSpan()), DataFrame.ReadJson(json.AsMemory())];
        foreach (var df in dfs)
            using (df)
                AssertSample(df);
    }

    /// <summary>Unmanaged memory that counts how often the engine pins and unpins it.</summary>
    private sealed unsafe class TrackingMemory : MemoryManager<byte>
    {
        private readonly byte* _pointer;
        private readonly int _length;
        private int _pins;
        private int _unpins;

        public TrackingMemory(byte[] data)
        {
            _length = data.Length;
            _pointer = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, data.Length));
            data.CopyTo(new Span<byte>(_pointer, _length));
        }

        public int Pins => Volatile.Read(ref _pins);
        public int Unpins => Volatile.Read(ref _unpins);

        public override Span<byte> GetSpan() => new(_pointer, _length);

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            Interlocked.Increment(ref _pins);
            return new MemoryHandle(_pointer + elementIndex, default, this);
        }

        public override void Unpin() => Interlocked.Increment(ref _unpins);

        protected override void Dispose(bool disposing) => NativeMemory.Free(_pointer);
    }
}
