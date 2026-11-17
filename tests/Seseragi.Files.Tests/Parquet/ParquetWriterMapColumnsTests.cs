using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriteOptions.MapColumns"/>: a list of key/value structs (how Polars holds a map) written as a
/// Parquet MAP is the file the same entries write as an Arrow map, byte for byte, at the top, inside a struct and as a
/// list's element, in 32- and 64-bit list layouts; a null key is refused, as a map's keys are required.
/// </summary>
public sealed class ParquetWriterMapColumnsTests
{
    private const int Rows = 3000;

    /// <summary>Entries of every row (0–4 each, some rows null), keys distinct strings, values nullable ints.</summary>
    private static (int[] Offsets, bool[] Valid, StructArray Entries) Entries(int seed, bool nullKey = false)
    {
        var random = new Random(seed);
        List<int> offsets = [0];
        var valid = new bool[Rows];
        var keys = new StringArray.Builder();
        var values = new Int32Array.Builder();
        for (int r = 0; r < Rows; r++)
        {
            valid[r] = random.Next(7) != 0;
            int n = valid[r] ? random.Next(5) : 0;
            for (int e = 0; e < n; e++)
            {
                if (nullKey && r == Rows / 2 && e == 0) keys.AppendNull();
                else keys.Append($"k{e}-{random.Next(50)}");
                if (random.Next(4) == 0) values.AppendNull();
                else values.Append(random.Next(1000));
            }
            offsets.Add(offsets[^1] + n);
        }
        StringArray k = keys.Build();
        Int32Array v = values.Build();
        var type = new StructType([new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true)]);
        return ([.. offsets], valid, new StructArray(type, k.Length, [k, v], ArrowBuffer.Empty, 0));
    }

    private static ArrowBuffer Validity(bool[] valid)
    {
        var bits = new byte[(valid.Length + 7) / 8];
        for (int i = 0; i < valid.Length; i++) if (valid[i]) bits[i >> 3] |= (byte)(1 << (i & 7));
        return new ArrowBuffer(bits);
    }

    private static int Nulls(bool[] valid) => valid.Count(v => !v);

    private static IArrowArray AsMap(int[] offsets, bool[] valid, StructArray entries) =>
        new MapArray(new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true)), valid.Length,
            new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), entries, Validity(valid), Nulls(valid));

    private static IArrowArray AsList(int[] offsets, bool[] valid, StructArray entries, bool large)
    {
        var item = new Field("entries", entries.Data.DataType, false);
        return large
            ? new LargeListArray(new LargeListType(item), valid.Length,
                new ArrowBuffer(MemoryMarshal.AsBytes(offsets.Select(o => (long)o).ToArray().AsSpan()).ToArray()), entries, Validity(valid), Nulls(valid))
            : new ListArray(new ListType(item), valid.Length, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), entries, Validity(valid), Nulls(valid));
    }

    /// <summary>The column at <paramref name="layout"/>: top-level, a struct's field, or the element of a one-item list per row.</summary>
    private static IArrowArray Placed(IArrowArray column, string layout)
    {
        switch (layout)
        {
            case "struct":
            {
                var type = new StructType([new Field("m", column.Data.DataType, true)]);
                return new StructArray(type, column.Length, [column], ArrowBuffer.Empty, 0);
            }
            case "list":
            {
                int[] offsets = [.. Enumerable.Range(0, column.Length + 1)];
                return new ListArray(new ListType(new Field("item", column.Data.DataType, true)), column.Length,
                    new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), column, ArrowBuffer.Empty, 0);
            }
            default:
                return column;
        }
    }

    private static byte[] Written(IArrowArray column, ParquetWriteOptions options)
    {
        var schema = new Schema([new Field("m", column.Data.DataType, true)], null);
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, [new RecordBatch(schema, [column], column.Length)], options with { StoreArrowSchema = false, LeaveOpen = true });
        return stream.ToArray();
    }

    public static TheoryData<string, bool> Layouts() => new()
    {
        { "top", false }, { "top", true }, { "struct", false }, { "struct", true }, { "list", false }, { "list", true },
    };

    [Theory]
    [MemberData(nameof(Layouts))]
    public void AListOfKeyValueStructs_WritesTheFileOfTheMap(string layout, bool large)
    {
        var (offsets, valid, entries) = Entries(11);
        string path = layout switch { "struct" => "m.m", "list" => "m[]", _ => "m" };
        byte[] expected = Written(Placed(AsMap(offsets, valid, entries), layout), new ParquetWriteOptions());
        byte[] actual = Written(Placed(AsList(offsets, valid, entries, large), layout), new ParquetWriteOptions { MapColumns = new HashSet<string>([path]) });
        Assert.Equal(expected, actual);

        // Without the option the same column is a list.
        Assert.NotEqual(expected, Written(Placed(AsList(offsets, valid, entries, large), layout), new ParquetWriteOptions()));
    }

    [Fact]
    public void ANullKey_IsRefused()
    {
        var (offsets, valid, entries) = Entries(12, nullKey: true);
        var e = Assert.Throws<ArgumentException>(() =>
            Written(AsList(offsets, valid, entries, large: true), new ParquetWriteOptions { MapColumns = new HashSet<string>(["m"]) }));
        Assert.Contains("m.key", e.Message);
    }

    [Fact]
    public void AColumnThatIsNoListOfPairs_IsRefused()
    {
        var ints = new Int32Array.Builder().Append(1).Build();
        int[] offsets = [0, 1];
        IArrowArray list = new ListArray(new ListType(new Field("item", Int32Type.Default, true)), 1,
            new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), ints, ArrowBuffer.Empty, 0);
        Assert.Contains("key/value", Assert.Throws<ArgumentException>(() =>
            Written(list, new ParquetWriteOptions { MapColumns = new HashSet<string>(["m"]) })).Message);
    }
}
