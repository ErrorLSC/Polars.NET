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
