using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// The IPC writer: every corpus file read and written again (both formats, every codec) reads back the same, batch by
/// batch; slices write their own rows; dictionaries are written once, then as deltas or replacements; the bytes do not
/// depend on the degree of parallelism; Apache.Arrow's reader reads what is written.
/// </summary>
public class IpcWriterTests
{
    /// <summary>Corpus files that cannot be written again as they are, with the reason.</summary>
    private static readonly Dictionary<string, string> NotWritable = new()
    {
        // pyarrow's stream replaces its dictionary: the file format refuses that (the stream format writes it).
        ["pyarrow/dictionary_replacement.arrows"] = "file",
    };

    /// <summary>The corpus files that can be read (and so written again).</summary>
    internal static string[] Names => [.. IpcValueTests.Oracle.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name)
        .Where(n => !n.StartsWith("apache/fuzz/") && !n.Contains("bigendian") && !n.EndsWith("feather_v1.feather"))];

    public static TheoryData<string> Files => [.. Names];

    private static readonly (IpcFormat Format, IIpcCompression Codec)[] Ways =
    [
        (IpcFormat.File, NoCompression.Instance), (IpcFormat.Stream, Lz4FrameCompression.Instance), (IpcFormat.File, new ZstandardCompression()),
        (IpcFormat.Stream, NoCompression.Instance),
    ];

    [Theory]
    [MemberData(nameof(Files))]
    public void CorpusFile_WrittenAgain_ReadsTheSame(string name)
    {
        var (schema, expected) = IpcValueTests.Read(name, new IpcReadOptions());
        foreach (var (format, codec) in Ways)
        {
            var options = new IpcWriteOptions { Format = format, Compression = codec, MaxDegreeOfParallelism = 4 };
            if (NotWritable.TryGetValue(name, out string? refused) && refused == "file" && format == IpcFormat.File)
            {
                Assert.Contains("allows no replacement", Assert.Throws<InvalidOperationException>(() => Rewrite(name, options)).Message);
                continue;
            }
            byte[] written = Rewrite(name, options);
            var (readSchema, actual) = ReadBytes(written, format);
            Assert.Equal(schema.FieldsList.Select(f => f.Name), readSchema.FieldsList.Select(f => f.Name));
            Assert.Equal(expected.Count, actual.Count);
            for (int b = 0; b < expected.Count; b++)
            {
                for (int c = 0; c < expected[b].Length; c++) Assert.True(expected[b][c].SequenceEqual(actual[b][c]), $"{name} {format} {codec}: batch {b}, column {readSchema.FieldsList[c].Name}");
            }
        }
    }

    /// <summary>Reads a corpus file with our reader and writes its batches with our writer.</summary>
    internal static byte[] Rewrite(string name, IpcWriteOptions options, Func<RecordBatch, RecordBatch>? change = null)
    {
        string path = Path.Combine(IpcValueTests.Root, name);
        var output = new MemoryStream();
        if (IpcValueTests.IsFile(name))
        {
            using IpcFile file = IpcFile.Open(path);
            using IpcReader reader = file.OpenReader();
            using IpcWriter writer = IpcWriter.Create(output, reader.Schema, options with { LeaveOpen = true });
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) WriteOne(writer, batch, change);
            }
        }
        else
        {
            using IpcStreamReader reader = IpcStreamReader.Open(File.OpenRead(path));
            using IpcWriter writer = IpcWriter.Create(output, reader.Schema, options with { LeaveOpen = true });
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) WriteOne(writer, batch, change);
            }
        }
        return output.ToArray();

        static void WriteOne(IpcWriter writer, RecordBatch batch, Func<RecordBatch, RecordBatch>? change)
        {
            if (change is null)
            {
                writer.Write(batch);
                return;
            }
            using RecordBatch changed = change(batch);
            writer.Write(changed);
        }
    }

    internal static (Schema Schema, List<List<string>[]> Batches) ReadBytes(byte[] bytes, IpcFormat format)
    {
        string path = Path.Combine(Path.GetTempPath(), $"ipc-{Guid.NewGuid():N}.{(format == IpcFormat.File ? "arrow" : "arrows")}");
        File.WriteAllBytes(path, bytes);
        try
        {
            // A path, so that IpcValueTests.Read tells the formats apart by their magic; mapped reads are exercised too.
            string relative = Path.GetRelativePath(IpcValueTests.Root, path);
            return IpcValueTests.Read(relative, new IpcReadOptions());
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Slices of every batch (from odd offsets: bitmaps shifted, offsets rebased, run ends cut) read back as those rows.</summary>
    [Theory]
    [InlineData("pyarrow/primitives.arrow")]
    [InlineData("pyarrow/strings.arrow")]
    [InlineData("pyarrow/nested.arrow")]
    [InlineData("pyarrow/unions.arrow")]
    [InlineData("pyarrow/ree.arrow")]
    [InlineData("pyarrow/dictionary.arrow")]
    [InlineData("pyarrow/extension.arrow")]
    [InlineData("polars/frame_uncompressed.arrow")]
    [InlineData("apache/integration/cpp-21.0.0/generated_list_view.arrow_file")]
    [InlineData("apache/integration/cpp-21.0.0/generated_binary_view.arrow_file")]
    [InlineData("apache/integration/cpp-21.0.0/generated_run_end_encoded.arrow_file")]
    [InlineData("apache/integration/cpp-21.0.0/generated_union.arrow_file")]
    [InlineData("apache/integration/cpp-21.0.0/generated_nested.arrow_file")]
    [InlineData("apache/integration/cpp-21.0.0/generated_map.arrow_file")]
    public void Slices_WriteTheirOwnRows(string name)
    {
        var (_, whole) = IpcValueTests.Read(name, new IpcReadOptions());
        foreach (int start in (int[])[1, 3, 9])
        {
            foreach (var (format, codec) in (IReadOnlyList<(IpcFormat, IIpcCompression)>)[(IpcFormat.File, NoCompression.Instance), (IpcFormat.Stream, Lz4FrameCompression.Instance)])
            {
                byte[] written = Rewrite(name, new IpcWriteOptions { Format = format, Compression = codec },
                    batch => batch.Slice(Math.Min(start, batch.Length), Math.Max(0, Math.Min(batch.Length - start, 2 * start + 1))));
                var (_, actual) = ReadBytes(written, format);
                for (int b = 0; b < whole.Count; b++)
                {
                    int from = Math.Min(start, whole[b][0].Count), count = Math.Max(0, Math.Min(whole[b][0].Count - start, 2 * start + 1));
                    for (int c = 0; c < whole[b].Length; c++) Assert.Equal(whole[b][c].Skip(from).Take(count), actual[b][c]);
                }
            }
        }
    }

    [Theory]
    [InlineData(IpcFormat.File)]
    [InlineData(IpcFormat.Stream)]
    public void Dictionaries_AreWrittenOnce_ThenAsDeltas(IpcFormat format)
    {
        // pyarrow's delta stream: three batches whose dictionaries extend each other.
        byte[] written = Rewrite("pyarrow/dictionary_delta.arrows", new IpcWriteOptions { Format = format });
        var kinds = Messages(written, format);
        Assert.Equal(["Schema", "Dictionary", "Batch", "Dictionary(delta)", "Batch", "Dictionary(delta)", "Batch"], kinds);
        // The same dictionaries in both batches (the second a slice of the first's rows): each written once.
        List<string> same = Messages(Rewrite("pyarrow/dictionary.arrow", new IpcWriteOptions { Format = format }), format);
        Assert.Equal(CountDictionaryFields("pyarrow/dictionary.arrow"), same.Count(k => k == "Dictionary"));
        Assert.DoesNotContain("Dictionary(delta)", same);
    }

    [Fact]
    public void AStream_ReplacesADictionaryThatChanges()
    {
        byte[] written = Rewrite("pyarrow/dictionary_replacement.arrows", new IpcWriteOptions { Format = IpcFormat.Stream });
        Assert.Equal(["Schema", "Dictionary", "Batch", "Dictionary", "Batch", "Dictionary", "Batch"], Messages(written, IpcFormat.Stream));
        var (_, batches) = ReadBytes(written, IpcFormat.Stream);
        Assert.Equal(["u:41", "u:42", "u:42", "u:41", "u:43", "null", "u:5a", "u:5a"], batches.SelectMany(b => b[0]));
    }

    private static int CountDictionaryFields(string name)
    {
        using IpcFile file = IpcFile.Open(Path.Combine(IpcValueTests.Root, name));
        return Count(file.Schema.FieldsList);

        static int Count(IEnumerable<Field> fields) => fields.Sum(f => (f.DataType is DictionaryType ? 1 : 0)
            + Count(f.DataType is DictionaryType d ? d.ValueType is NestedType n ? n.Fields : [] : f.DataType is NestedType m ? m.Fields : []));
    }

    /// <summary>The kinds of the messages written, in order.</summary>
    private static List<string> Messages(byte[] bytes, IpcFormat format)
    {
        List<string> kinds = [];
        int at = format == IpcFormat.File ? 8 : 0;
        while (at + 8 <= bytes.Length)
        {
            int length = BitConverter.ToInt32(bytes, at + 4);
            if (length == 0) break;
            var message = Kaminari.Sazanami.Arrow.Ipc.IpcMetadata.ParseMessage(bytes.AsSpan(at + 8, length));
            kinds.Add(message.Kind switch
            {
                Kaminari.Sazanami.Arrow.Ipc.IpcMessageKind.Schema => "Schema",
                Kaminari.Sazanami.Arrow.Ipc.IpcMessageKind.RecordBatch => "Batch",
                _ => message.DictionaryBatch!.IsDelta ? "Dictionary(delta)" : "Dictionary",
            });
            at += 8 + length + (int)message.BodyLength;
        }
        return kinds;
    }

    [Theory]
    [InlineData("pyarrow/many_batches.arrow")]
    [InlineData("polars/frame_uncompressed.arrow")]
    [InlineData("pyarrow/big_lz4.arrow")]
    public void Bytes_DoNotDependOnParallelism(string name)
    {
        foreach (IIpcCompression codec in (IIpcCompression[])[Lz4FrameCompression.Instance, new ZstandardCompression(5), NoCompression.Instance])
        {
            byte[] one = Rewrite(name, new IpcWriteOptions { Compression = codec, MaxDegreeOfParallelism = 1 });
            byte[] many = Rewrite(name, new IpcWriteOptions { Compression = codec, MaxDegreeOfParallelism = 16 });
            Assert.Equal(one, many);
        }
    }

    [Fact]
    public void Buffers_AreAligned_AndIncompressibleOnesStoredRaw()
    {
        byte[] written = Rewrite("pyarrow/lz4.arrow", new IpcWriteOptions { Compression = Lz4FrameCompression.Instance });
        int at = 8, raw = 0, compressed = 0;
        while (at + 8 <= written.Length)
        {
            int length = BitConverter.ToInt32(written, at + 4);
            if (length == 0) break;
            var message = Kaminari.Sazanami.Arrow.Ipc.IpcMetadata.ParseMessage(written.AsSpan(at + 8, length));
            long body = at + 8 + length;
            Assert.Equal(0, body % 64);
            var header = message.RecordBatch ?? message.DictionaryBatch?.Data;
            foreach (var buffer in header?.Buffers ?? [])
            {
                Assert.Equal(0, buffer.Offset % 64);
                if (buffer.Length == 0) continue;
                long prefix = BitConverter.ToInt64(written, (int)(body + buffer.Offset));
                if (prefix == -1) raw++;
                else compressed++;
            }
            at = (int)(body + message.BodyLength);
        }
        Assert.True(raw > 0, "the noise column is stored raw");
        Assert.True(compressed > 0);
    }

    /// <summary>Apache.Arrow's reader reads every uncompressed file we write, value for value.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void ApacheReader_ReadsWhatIsWritten(string name)
    {
        if (name is "polars/int128.arrow") return; // written as decimals: compared by our own reader above
        foreach (IpcFormat format in (IpcFormat[])[IpcFormat.File, IpcFormat.Stream])
        {
            if (NotWritable.ContainsKey(name) && format == IpcFormat.File) continue;
            if (name.StartsWith("pyarrow/dictionary_delta") && format == IpcFormat.Stream) continue; // Apache.Arrow 23 fails on stream deltas (its concatenation)
            byte[] written = Rewrite(name, new IpcWriteOptions { Format = format });
            var (_, ours) = ReadBytes(written, format);
            using var stream = new MemoryStream(written);
            using ArrowStreamReader reader = format == IpcFormat.File ? new ArrowFileReader(stream) : new ArrowStreamReader(stream);
            int b = 0;
            while (reader.ReadNextRecordBatch() is { } batch)
            {
                using (batch)
                {
                    for (int c = 0; c < batch.ColumnCount; c++)
                        Assert.Equal(ours[b][c], Enumerable.Range(0, batch.Length).Select(i => CanonicalRender.Value(batch.Column(c), i)));
                }
                b++;
            }
            Assert.Equal(ours.Count, b);
        }
    }

    [Fact]
    public void Metadata_IsWrittenAtEveryLevel()
    {
        using IpcFile source = IpcFile.Open(Path.Combine(IpcValueTests.Root, "pyarrow/metadata.arrow"));
        var output = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(output, source.Schema, new IpcWriteOptions { LeaveOpen = true, FileMetadata = new Dictionary<string, string> { ["file:k"] = "file v" } }))
        {
            for (int i = 0; i < source.BatchCount; i++)
            {
                using RecordBatch batch = source.ReadBatch(i);
                writer.Write(batch, new Dictionary<string, string> { ["batch"] = i.ToString(System.Globalization.CultureInfo.InvariantCulture) });
            }
        }
        using IpcFile file = IpcFile.Open(output.ToArray());
        Assert.Equal("schema v", file.Schema.Metadata["schema:k"]);
        Assert.Equal("field v", file.Schema.FieldsList[0].Metadata["field:k"]);
        Assert.Equal("inner v", ((StructType)file.Schema.FieldsList[1].DataType).Fields[0].Metadata["inner:k"]);
        Assert.False(file.Schema.FieldsList[0].IsNullable);
        Assert.Equal("file v", file.FileMetadata["file:k"]);
        Assert.Equal("2", file.GetBatchMetadata(2)["batch"]);
    }

    [Fact]
    public void Lifecycle_FollowsTheParquetWriter()
    {
        var schema = new Schema([new Field("a", Int32Type.Default, true)], null);
        using var batch = new RecordBatch(schema, [new Int32Array.Builder().Append(1).Append(2).Build()], 2);
        // No batch: a valid file of no batches.
        var empty = new MemoryStream();
        using (IpcWriter.Create(empty, schema, new IpcWriteOptions { LeaveOpen = true })) { }
        using (IpcFile file = IpcFile.Open(empty.ToArray())) Assert.Equal(0, file.BatchCount);
        // Aborted: no footer.
        var aborted = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(aborted, schema, new IpcWriteOptions { LeaveOpen = true }))
        {
            writer.Write(batch);
            writer.Abort();
            Assert.Throws<InvalidOperationException>(() => writer.Write(batch));
        }
        Assert.Throws<IpcFormatException>(() => IpcFile.Open(aborted.ToArray()));
        // A batch of another schema is refused and leaves the output valid.
        var output = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(output, schema, new IpcWriteOptions { LeaveOpen = true }))
        {
            writer.Write(batch);
            var other = new Schema([new Field("a", StringType.Default, true)], null);
            using var wrong = new RecordBatch(other, [new StringArray.Builder().Append("x").Build()], 1);
            Assert.Contains("holds utf8 values", Assert.Throws<ArgumentException>(() => writer.Write(wrong)).Message);
            writer.Write(batch);
            Assert.True(writer.BytesWritten > 0);
        }
        using (IpcFile file = IpcFile.Open(output.ToArray())) Assert.Equal(2, file.BatchCount);
        Assert.Throws<ArgumentException>(() => IpcWriter.Create(new MemoryStream(), schema,
            new IpcWriteOptions { Format = IpcFormat.Stream, FileMetadata = new Dictionary<string, string>() }));
    }
}
