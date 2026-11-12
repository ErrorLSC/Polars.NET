using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests;

/// <summary>
/// Files written here are read back by fastavro, Python polars and (when installed) DuckDB
/// (TestData/avro/interop_check.py).
/// Opt-in: set SESERAGI_AVRO_PYTHON to a Python with fastavro, cramjam and polars installed,
/// e.g. ~/.cache/seseragi/avro-venv/bin/python. Without it the test only writes the files.
/// </summary>
[Trait("Category", "Interop")]
public class AvroInteropTests
{
    /// <summary>The types the Python polars Avro reader supports, with the values interop_check.py expects.</summary>
    internal static RecordBatch PolarsSubset()
    {
        var bin = new BinaryArray.Builder().Append([0]).Append((ReadOnlySpan<byte>)[]).AppendNull().Build();
        var listValues = Fixed(Int64Type.Default, [(long?)1, null]);
        var list = List(new ListType(new Field("item", Int64Type.Default, true)), [0, 2, 2, 2], listValues, [true, false, true]);
        var stType = new StructType([new Field("x", Int32Type.Default, false), new Field("y", StringType.Default, true)]);
        var st = Struct(stType, [Fixed(Int32Type.Default, [(int?)1, 2, 3]), Strings(["p", null, null])], [true, false, true]);
        const long ts = 1_704_164_645_000_006; // 2024-01-02T03:04:05.000006
        return Batch(
            ("i", Fixed(Int32Type.Default, [(int?)1, null, -3]), true),
            ("l", Fixed(Int64Type.Default, [(long?)1_000_000_000_000, 2, null]), true),
            ("d", Fixed(DoubleType.Default, [(double?)0.5, null, -2.25]), true),
            ("f", Fixed(FloatType.Default, [(float?)1.5f, 2f, null]), true),
            ("b", new BooleanArray.Builder().Append(true).Append(false).AppendNull().Build(), true),
            ("s", Strings(["a", null, "雷"]), true),
            ("bin", bin, true),
            ("date", Fixed(Date32Type.Default, [(int?)19724, null, -1]), true),
            ("ts", Fixed(new TimestampType(TimeUnit.Microsecond, (string?)null), [(long?)ts, null, 0]), true),
            ("tsz", Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), [(long?)ts, null, 0]), true),
            ("tsk", Fixed(new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), [(long?)ts, null, 0]), true),
            ("dec", new Decimal128Array.Builder(new Decimal128Type(10, 2)).Append(1.25m).AppendNull().Append(-3.5m).Build(), true),
            ("list", list, true),
            ("st", st, true));
    }

    /// <summary>Maps with non-string keys, with the values interop_check.py expects.</summary>
    internal static RecordBatch Maps()
    {
        var mType = new MapType(new Field("key", Int32Type.Default, false), new Field("value", StringType.Default, true));
        var m = new MapArray.Builder(mType);
        m.Append(); ((Int32Array.Builder)m.KeyBuilder).Append(1).Append(2); ((StringArray.Builder)m.ValueBuilder).Append("a").AppendNull();
        m.AppendNull();
        m.Append();
        var bigType = new MapType(new Field("key", Int64Type.Default, false), new Field("value", Int64Type.Default, false));
        var big = new MapArray.Builder(bigType);
        big.Append(); ((Int64Array.Builder)big.KeyBuilder).Append(5); ((Int64Array.Builder)big.ValueBuilder).Append(6);
        big.Append(); ((Int64Array.Builder)big.KeyBuilder).Append(long.MinValue); ((Int64Array.Builder)big.ValueBuilder).Append(0);
        big.Append();
        return Batch(("m", m.Build(), true), ("big", big.Build(), false));
    }

    [Fact]
    public async Task Reference_readers_read_our_files()
    {
        string dir = Directory.CreateTempSubdirectory("seseragi-avro-interop").FullName;
        try
        {
            string wide = Path.Combine(dir, "wide.avro");
            string polars = Path.Combine(dir, "polars.avro");
            var w = AvroWriteTests.Wide(0, 300);
            AvroWriter.Write(wide, w.Schema, [w.Slice(0, 100), w.Slice(100, 200)], new() { Compression = SnappyCompression.Instance, BlockSize = 1000 });
            var p = PolarsSubset();
            await AvroWriter.WriteAsync(polars, p.Schema, ((RecordBatch[])[p]).ToAsyncEnumerable(), new() { Compression = new DeflateCompression() });
            string maps = Path.Combine(dir, "maps.avro");
            var mb = Maps();
            AvroWriter.Write(maps, mb.Schema, [mb]);

            string? python = Environment.GetEnvironmentVariable("SESERAGI_AVRO_PYTHON");
            if (string.IsNullOrEmpty(python)) return;
            string script = Path.Combine(AppContext.BaseDirectory, "TestData", "avro", "interop_check.py");
            var psi = new ProcessStartInfo(python, ["-I", script, wide, polars, maps]) { RedirectStandardOutput = true, RedirectStandardError = true };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0, stdout + stderr);
            Assert.Contains("interop ok", stdout);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }
}
