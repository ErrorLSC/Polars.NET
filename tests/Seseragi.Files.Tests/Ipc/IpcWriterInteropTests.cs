using System.Diagnostics;
using System.Text.Json;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// Every corpus file written again by our writer (file and stream formats, uncompressed, LZ4 frame and Zstandard, also
/// with buffers split into several Zstandard frames, as large buffers are) and
/// read by pyarrow (fully validated) and Python Polars: they read what they read from the original
/// (TestData/ipc/writer_interop.py against values.json). Opt-in: SESERAGI_IPC_PYTHON (pyarrow 26 and Polars 2.1, e.g.
/// ~/.cache/seseragi/avro-venv/bin/python); without it the files are only written.
/// </summary>
[Trait("Category", "Interop")]
public sealed class IpcWriterInteropTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"ipc-interop-{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    [Fact]
    public void OracleReaders_ReadOurFiles_AsTheyReadTheOriginals()
    {
        Directory.CreateDirectory(_dir);
        (IpcFormat Format, IIpcCompression Codec, string Tag, int Frames)[] ways =
        [
            (IpcFormat.File, NoCompression.Instance, "none.arrow", 32 << 20), (IpcFormat.File, Lz4FrameCompression.Instance, "lz4.arrow", 32 << 20),
            (IpcFormat.Stream, new ZstandardCompression(), "zstd.arrows", 32 << 20),
            (IpcFormat.File, new ZstandardCompression(), "zstd-frames.arrow", 4096), // every buffer over 4 KiB several frames
        ];
        Dictionary<string, string> manifest = [];
        int n = 0;
        foreach (string name in IpcWriterTests.Names)
        {
            if (name == "polars/int128.arrow") continue; // written as decimals: Polars reads them back as decimals, not Int128
            // Year-month and day-time intervals: pyarrow cannot hand their arrays out (the corpus oracle reads golden JSON);
            // our reader compares them in IpcWriterTests.
            if (name.EndsWith("generated_interval.arrow_file") || name.EndsWith("generated_interval.stream")) continue;
            foreach (var (format, codec, tag, frames) in ways)
            {
                if (name == "pyarrow/dictionary_replacement.arrows" && format == IpcFormat.File) continue; // refused by the file format
                string path = Path.Combine(_dir, $"{n++:D4}.{tag}");
                File.WriteAllBytes(path, IpcWriterTests.Rewrite(name, new IpcWriteOptions { Format = format, Compression = codec, ZstdFrameBytes = frames }));
                manifest[path] = name;
            }
        }
        string? python = Environment.GetEnvironmentVariable("SESERAGI_IPC_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        string manifestPath = Path.Combine(_dir, "manifest.json");
        File.WriteAllText(manifestPath, JsonSerializerManifest(manifest));
        var start = new ProcessStartInfo(python) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string arg in (string[])["-I", Path.Combine(IpcValueTests.Root, "writer_interop.py"), manifestPath]) start.ArgumentList.Add(arg);
        using Process process = Process.Start(start)!;
        string output = process.StandardOutput.ReadToEnd();
        string errors = process.StandardError.ReadToEnd();
        process.WaitForExit();
        Assert.True(process.ExitCode == 0, errors);
        using JsonDocument diffs = JsonDocument.Parse(output);
        if (diffs.RootElement.GetArrayLength() > 0) File.WriteAllText(Path.Combine(Path.GetTempPath(), "ipc-interop-diffs.json"), output);
        Assert.True(diffs.RootElement.GetArrayLength() == 0, output.Length > 4000 ? output[..4000] : output);
    }

    /// <summary>
    /// Slices of string views and list views (their views rebased or kept, data and child cut to what they use, null
    /// views emptied) written by our writer: pyarrow validates them fully and reads our values.
    /// </summary>
    [Fact]
    public void Pyarrow_ReadsSlicesOfViews()
    {
        string? python = Environment.GetEnvironmentVariable("SESERAGI_IPC_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        Directory.CreateDirectory(_dir);
        Apache.Arrow.RecordBatch whole = IpcWriterTests.LargeViews();
        foreach (int start in (int[])[0, 150_000, 99_950])
        {
            Apache.Arrow.RecordBatch slice = whole.Slice(start, 100); // shares the batch's buffers: not disposed
            string path = Path.Combine(_dir, $"views-{start}.arrow");
            IpcWriter.Write(path, whole.Schema, [slice], new IpcWriteOptions { Compression = Lz4FrameCompression.Instance });
            var info = new ProcessStartInfo(python) { RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (string arg in (string[])["-I", "-c", Script, path]) info.ArgumentList.Add(arg);
            using Process process = Process.Start(info)!;
            string output = process.StandardOutput.ReadToEnd();
            string errors = process.StandardError.ReadToEnd();
            process.WaitForExit();
            Assert.True(process.ExitCode == 0, errors);
            using JsonDocument read = JsonDocument.Parse(output);
            var strings = (Apache.Arrow.StringViewArray)slice.Column(0);
            var lists = (Apache.Arrow.ListViewArray)slice.Column(1);
            JsonElement s = read.RootElement.GetProperty("s"), l = read.RootElement.GetProperty("l");
            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(strings.IsNull(i) ? null : strings.GetString(i), s[i].ValueKind == JsonValueKind.Null ? null : s[i].GetString());
                if (lists.IsNull(i))
                {
                    Assert.Equal(JsonValueKind.Null, l[i].ValueKind);
                    continue;
                }
                var items = (Apache.Arrow.Int32Array)lists.GetSlicedValues(i);
                Assert.Equal(Enumerable.Range(0, items.Length).Select(k => items.GetValue(k)!.Value), l[i].EnumerateArray().Select(e => e.GetInt32()));
            }
        }
    }

    private const string Script = """
        import json, sys
        import pyarrow.ipc as ipc
        table = ipc.open_file(sys.argv[1]).read_all()
        table.validate(full=True)
        print(json.dumps({"s": table.column("s").to_pylist(), "l": table.column("l").to_pylist()}))
        """;

    /// <summary>The manifest as JSON (written by hand: no reflection serializer).</summary>
    private static string JsonSerializerManifest(Dictionary<string, string> manifest)
    {
        var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (path, name) in manifest) writer.WriteString(path, name);
            writer.WriteEndObject();
        }
        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}
