using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Avro;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Excel;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// Writers creating a file by its path overwrite an existing one as <see cref="FileMode.Create"/> does, though its old
/// bytes are cut off on a pool thread (<see cref="OverwrittenFile"/>, the text writers' file sink): the file holds the
/// new bytes alone, whatever it held before.
/// </summary>
public sealed class OverwrittenFileTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("overwritten-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static readonly Schema Schema = new([new Field("id", Int64Type.Default, false), new Field("s", StringType.Default, true)], null);

    private static RecordBatch Batch()
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < 300; i++)
        {
            ids.Append(i);
            if (i % 7 == 0) names.AppendNull();
            else names.Append($"name {i}");
        }
        return new RecordBatch(Schema, [ids.Build(), names.Build()], 300);
    }

    /// <summary>The writers, by name: whether their bytes are the same on every write (Avro's sync marker is random, a workbook's entries are timestamped).</summary>
    public static TheoryData<string, bool> Writers => new()
    {
        { "parquet", true }, { "ipc", true }, { "csv", true }, { "json", true }, { "avro", false }, { "excel", false },
    };

    private static void Write(string writer, string path)
    {
        using RecordBatch batch = Batch();
        RecordBatch[] batches = [batch];
        switch (writer)
        {
            case "parquet": ParquetWriter.Write(path, Schema, batches); break;
            case "ipc": IpcWriter.Write(path, Schema, batches); break;
            case "csv": CsvWriter.Write(path, Schema, batches); break;
            case "json": JsonWriter.Write(path, Schema, batches); break;
            case "avro": AvroWriter.Write(path, Schema, batches); break;
            default: ExcelWriter.Write(path, Schema, batches); break;
        }
    }

    [Theory]
    [MemberData(nameof(Writers))]
    public void ALargerFileOverwritten_HoldsTheNewBytesAlone(string writer, bool deterministic)
    {
        string fresh = Path.Combine(_dir, $"fresh.{writer}"), overwritten = Path.Combine(_dir, $"old.{writer}");
        Write(writer, fresh);
        byte[] old = new byte[4 << 20];
        old.AsSpan().Fill(0xAB);
        File.WriteAllBytes(overwritten, old);
        Write(writer, overwritten);
        byte[] expected = File.ReadAllBytes(fresh), actual = File.ReadAllBytes(overwritten);
        Assert.Equal(expected.Length, actual.Length);
        if (deterministic) Assert.Equal(expected, actual);
        // Twice over the same file: nothing of the first write is left either.
        Write(writer, overwritten);
        Assert.Equal(expected.Length, new FileInfo(overwritten).Length);
    }

    [Fact]
    public void AFileOpenedAndClosedUnwritten_IsEmpty()
    {
        string path = Path.Combine(_dir, "unwritten");
        File.WriteAllBytes(path, new byte[1 << 20]);
        using (OverwrittenFile.Open(path)) { }
        Assert.Equal(0, new FileInfo(path).Length);
    }

    [Fact]
    public async Task AFileOpenedAndClosedUnwrittenAsynchronously_IsEmpty()
    {
        string path = Path.Combine(_dir, "unwritten-async");
        await File.WriteAllBytesAsync(path, new byte[1 << 20]);
        await using (OverwrittenFile.Open(path)) { }
        Assert.Equal(0, new FileInfo(path).Length);
    }

    [Fact]
    public async Task WritesAndSeeks_StartFromAnEmptyFile()
    {
        string path = Path.Combine(_dir, "seeks");
        File.WriteAllBytes(path, new byte[1 << 20]);
        await using (OverwrittenFile stream = OverwrittenFile.Open(path))
        {
            Assert.Equal(0, stream.Length);
            await stream.WriteAsync("abcdef"u8.ToArray());
            stream.Seek(1, SeekOrigin.Begin);
            stream.Write("XY"u8);
            Assert.Equal(3, stream.Position);
            await stream.FlushAsync();
        }
        Assert.Equal("aXYdef"u8.ToArray(), File.ReadAllBytes(path));
    }

    [Fact]
    public void ADeviceWithoutLength_IsWritten()
    {
        if (!OperatingSystem.IsLinux()) return; // a Linux device
        Write("parquet", "/dev/null");
        Write("csv", "/dev/null");
    }
}
