using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// Every IPC test file (TestData/ipc) read here against pyarrow and Python Polars (values.json): each column's values
/// hash like the oracle's, batch by batch, in both formats and every reading mode. Each file falls in exactly one
/// bucket — compared, or refused for a named reason — and the bucket sizes are asserted, so no file is skipped silently.
/// </summary>
public class IpcValueTests
{
    internal static readonly string Root = Path.Combine(AppContext.BaseDirectory, "TestData", "ipc");

    internal static readonly Lazy<JsonDocument> Oracle = new(() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Root, "values.json"))));

    /// <summary>Files refused, with a fragment of the error each must give.</summary>
    internal static readonly Dictionary<string, string> Refused = new()
    {
        ["apache/integration/1.0.0-bigendian/generated_dictionary.arrow_file"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_dictionary.stream"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_null_trivial.arrow_file"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_null_trivial.stream"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_primitive.arrow_file"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_primitive.stream"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_primitive_zerolength.arrow_file"] = "Big-endian",
        ["apache/integration/1.0.0-bigendian/generated_primitive_zerolength.stream"] = "Big-endian",
        ["pyarrow/feather_v1.feather"] = "Feather v1",
    };

    /// <summary>Files pyarrow cannot read, compared with Polars (Int128 and UInt128, which Polars alone writes).</summary>
    private static readonly string[] PolarsOnly = ["polars/int128.arrow"];

    internal static string[] Names => [.. Oracle.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name).Where(n => !n.StartsWith("apache/fuzz/"))];

    public static TheoryData<string> Files => [.. Names];

    [Fact]
    public void EveryFile_FallsInOneBucket()
    {
        string[] all = Names;
        string[] compared = [.. all.Except(Refused.Keys)];
        Assert.Equal(all.Length, compared.Length + Refused.Count);
        Assert.Equal((190, 181, 9), (all.Length, compared.Length, Refused.Count));
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void File_ReadsLikeTheOracles(string name)
    {
        if (Refused.TryGetValue(name, out string? reason))
        {
            var e = Assert.ThrowsAny<IpcFormatException>(() => Read(name, new IpcReadOptions()));
            Assert.Contains(reason, e.Message);
            return;
        }
        JsonElement expected = Oracle.Value.RootElement.GetProperty("files").GetProperty(name);
        bool polars = PolarsOnly.Contains(name);
        JsonElement oracle = expected.GetProperty(polars ? "polars" : "pyarrow");
        Assert.False(oracle.TryGetProperty("error", out JsonElement error), $"{name}: the oracle failed: {error}");
        var (schema, batches) = Read(name, new IpcReadOptions());
        List<string> diffs = [];
        if (!polars)
        {
            // Batch by batch: rows and the hash of the batch's column hashes.
            JsonElement[] expectedBatches = [.. oracle.GetProperty("batches").EnumerateArray()];
            if (expectedBatches.Length != batches.Count) diffs.Add($"{batches.Count} batches, oracle {expectedBatches.Length}");
            else
            {
                for (int b = 0; b < batches.Count; b++)
                {
                    int rows = expectedBatches[b][0].GetInt32();
                    string hash = CanonicalRender.Hash(batches[b].Select(CanonicalRender.Hash));
                    if (rows != batches[b][0].Count || hash != expectedBatches[b][1].GetString()) diffs.Add($"batch {b}: {batches[b][0].Count} rows, oracle {rows}; hash {hash} vs {expectedBatches[b][1]}");
                }
            }
        }
        for (int c = 0; c < schema.FieldsList.Count; c++)
        {
            Field field = schema.FieldsList[c];
            JsonElement column = oracle.GetProperty("columns")[c];
            List<string> values = [.. batches.SelectMany(b => b[c])];
            if (polars && field.DataType is Decimal128Type or Decimal256Type) values = [.. values.Select(v => v == "null" ? v : v[2..^3])]; // integers: d:<v>e-0
            if (field.Name != column.GetProperty("name").GetString()) diffs.Add($"column {c}: {field.Name}, oracle {column.GetProperty("name")}");
            if (CanonicalRender.Hash(values) != column.GetProperty("hash").GetString())
                diffs.Add($"{field.Name}: ours {string.Join(" ", values.Take(3))} vs {string.Join(" ", column.GetProperty("sample").EnumerateArray().Select(s => s.GetString()))}");
            if (values.Count != column.GetProperty("rows").GetInt32()) diffs.Add($"{field.Name}: {values.Count} rows, oracle {column.GetProperty("rows")}");
            if (values.Count(v => v == "null") != column.GetProperty("nulls").GetInt32()) diffs.Add($"{field.Name}: {values.Count(v => v == "null")} nulls, oracle {column.GetProperty("nulls")}");
        }
        Assert.True(diffs.Count == 0, $"{name}:\n{string.Join('\n', diffs)}");
    }

    /// <summary>Mapped or not, sync or async, file or bytes in memory: the same values, or the same error.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public async Task ReadingModes_ReadAlike(string name)
    {
        IpcReadOptions[] ways = [new() { MemoryMap = IpcMemoryMap.Off }, new() { MemoryMap = IpcMemoryMap.On }, new() { MemoryMap = IpcMemoryMap.Auto }];
        if (Refused.ContainsKey(name))
        {
            string reference = Assert.ThrowsAny<IpcFormatException>(() => Read(name, new IpcReadOptions())).Message;
            foreach (IpcReadOptions way in ways) Assert.Equal(reference, Assert.ThrowsAny<IpcFormatException>(() => Read(name, way)).Message);
            Assert.Equal(reference, (await Assert.ThrowsAnyAsync<IpcFormatException>(() => ReadAsync(name))).Message);
            Assert.Equal(reference, Assert.ThrowsAny<IpcFormatException>(() => Read(name, new IpcReadOptions(), fromMemory: true)).Message);
            return;
        }
        var (_, reference2) = Read(name, new IpcReadOptions());
        foreach (IpcReadOptions way in ways) Assert.Equal(reference2, Read(name, way).Batches);
        Assert.Equal(reference2, (await ReadAsync(name)).Batches);
        Assert.Equal(reference2, Read(name, new IpcReadOptions(), fromMemory: true).Batches);
    }

    /// <summary>Selecting columns reads them alike, in schema order; a row limit cuts the rows.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void ColumnsAndRows_ReadAsSlicesOfTheWhole(string name)
    {
        if (Refused.ContainsKey(name)) return;
        var (schema, whole) = Read(name, new IpcReadOptions());
        if (schema.FieldsList.Count == 0) return;
        // Every other column, by name (unique names only: a shared name selects all its columns).
        string[] names = [.. schema.FieldsList.Select(f => f.Name)];
        int[] picked = [.. Enumerable.Range(0, names.Length).Where(i => i % 2 == 1 && names.Count(n => n == names[i]) == 1)];
        if (picked.Length > 0)
        {
            var (projected, part) = Read(name, new IpcReadOptions { Columns = [.. picked.Select(i => names[i])] });
            Assert.Equal(picked.Select(i => names[i]), projected.FieldsList.Select(f => f.Name));
            for (int b = 0; b < whole.Count; b++)
            {
                for (int k = 0; k < picked.Length; k++) Assert.Equal(whole[b][picked[k]], part[b][k]);
            }
        }
        long total = whole.Sum(b => (long)b[0].Count);
        long limit = total / 2 + 1;
        var (_, cut) = Read(name, new IpcReadOptions { MaxRows = limit });
        Assert.Equal(Math.Min(limit, total), cut.Sum(b => (long)b[0].Count));
        List<string> wholeFirst = [.. whole.SelectMany(b => b[0]).Take((int)limit)];
        Assert.Equal(wholeFirst, cut.SelectMany(b => b[0]));
    }

    internal static bool IsFile(string name)
    {
        using var f = File.OpenRead(Path.Combine(Root, name));
        Span<byte> head = stackalloc byte[6];
        return f.Read(head) == 6 && head.SequenceEqual("ARROW1"u8);
    }

    /// <summary>The schema and, per batch, per column, the canonical values.</summary>
    internal static (Schema Schema, List<List<string>[]> Batches) Read(string name, IpcReadOptions options, bool fromMemory = false)
    {
        string path = Path.Combine(Root, name);
        List<List<string>[]> batches = [];
        if (IsFile(name))
        {
            using IpcFile file = fromMemory ? IpcFile.Open(File.ReadAllBytes(path)) : IpcFile.Open(path);
            using IpcReader reader = file.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) batches.Add(Render(batch));
            }
            return (reader.Schema, batches);
        }
        using IpcStreamReader stream = fromMemory ? IpcStreamReader.Open(new MemoryStream(File.ReadAllBytes(path)), options) : IpcStreamReader.Open(File.OpenRead(path), options);
        foreach (RecordBatch batch in stream.ReadBatches())
        {
            using (batch) batches.Add(Render(batch));
        }
        return (stream.Schema, batches);
    }

    private static async Task<(Schema Schema, List<List<string>[]> Batches)> ReadAsync(string name)
    {
        string path = Path.Combine(Root, name);
        List<List<string>[]> batches = [];
        if (IsFile(name))
        {
            await using IpcFile file = await IpcFile.OpenAsync(path);
            using IpcReader reader = file.OpenReader();
            await foreach (RecordBatch batch in reader.ReadBatchesAsync())
            {
                using (batch) batches.Add(Render(batch));
            }
            return (reader.Schema, batches);
        }
        await using IpcStreamReader stream = await IpcStreamReader.OpenAsync(new NonSeekable(File.OpenRead(path)));
        await foreach (RecordBatch batch in stream.ReadBatchesAsync())
        {
            using (batch) batches.Add(Render(batch));
        }
        return (stream.Schema, batches);
    }

    private static List<string>[] Render(RecordBatch batch) =>
        [.. Enumerable.Range(0, batch.ColumnCount).Select(c => Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(c), i)).ToList())];

    /// <summary>A stream that cannot seek (a socket, a pipe).</summary>
    internal sealed class NonSeekable(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => inner.Read(buffer);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
