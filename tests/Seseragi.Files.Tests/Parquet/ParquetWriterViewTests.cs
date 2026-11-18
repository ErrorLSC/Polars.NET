using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// View columns (string_view, binary_view: what Polars exports) are copied straight from their views and data buffers
/// (read as Sazanami's <c>ArrowStringView</c>):
/// the file is the one their values write as plain strings or binary, byte for byte, with values inline (up to 12 bytes)
/// and in several data buffers, nulls, slices of the array and views under a list. Inline values find their dictionary
/// entries by their views: the dictionary (its entries and their order) must stay the one their bytes give.
/// </summary>
public sealed class ParquetWriterViewTests
{
    /// <summary>Values of every kind: null, empty, inline, exactly 12 bytes, just over, long, multi-byte UTF-8.</summary>
    private static string?[] Values(int seed, int count)
    {
        var random = new Random(seed);
        return [.. Enumerable.Range(0, count).Select(i => random.Next(9) switch
        {
            0 => null,
            1 => "",
            2 => $"k{random.Next(40)}",
            3 => new string('a', 12),
            4 => new string('b', 13),
            5 => $"雷雨-{random.Next(1000)}-" + new string('x', random.Next(30)),
            6 => $"cat-{random.Next(8)}",
            _ => $"value-{random.Next(100_000):D6}-{new string('z', random.Next(60))}",
        })];
    }

    /// <summary>
    /// A view array of the values in native memory (Sazanami's view builder): long values in the builder's own data
    /// buffer, every third one in a data buffer of its own, so that views point into many buffers in turn.
    /// </summary>
    private static IArrowArray Views(IArrowType type, string?[] values)
    {
        using var builder = new NativeBinaryViewBuilder(type);
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is not { } text)
            {
                builder.AppendNull();
                continue;
            }
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(text);
            if (bytes.Length > NativeBinaryViewBuilder.MaxInline && i % 3 == 0)
                builder.AppendViewSlots(1)[0] = NativeBinaryViewBuilder.View(bytes, builder.AddBuffer(bytes), 0);
            else builder.Append(bytes);
        }
        return builder.ExportArray();
    }

    private static IArrowArray Plain(bool binary, string?[] values)
    {
        if (!binary)
        {
            var strings = new StringArray.Builder();
            foreach (string? v in values) strings.Append(v);
            return strings.Build();
        }
        var bytes = new BinaryArray.Builder();
        foreach (string? v in values)
        {
            if (v is null) bytes.AppendNull();
            else bytes.Append(System.Text.Encoding.UTF8.GetBytes(v).AsSpan());
        }
        return bytes.Build();
    }

    private static byte[] Written(IArrowArray column, ParquetWriteOptions options, int offset = 0, int? length = null)
    {
        var schema = new Schema([new Field("s", column.Data.DataType, true)], null);
        RecordBatch batch = new RecordBatch(schema, [column], column.Length).Slice(offset, length ?? column.Length - offset);
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, [batch], options with { StoreArrowSchema = false, LeaveOpen = true });
        return stream.ToArray();
    }

    public static TheoryData<bool, string> Cases()
    {
        TheoryData<bool, string> data = [];
        foreach (bool binary in (bool[])[false, true])
            foreach (string layout in (string[])["dictionary", "plain", "delta", "small_pages", "dictionary_overflow", "row_groups"])
                data.Add(binary, layout);
        return data;
    }

    private static ParquetWriteOptions Options(string layout) => layout switch
    {
        "plain" => new() { Dictionary = false },
        "delta" => new() { Columns = new Dictionary<string, ParquetColumnOptions> { ["s"] = new() { Encoding = ParquetValueEncoding.DeltaByteArray, Dictionary = false } } },
        "small_pages" => new() { DataPageRows = 333, DataPageBytes = 4000, MaxDegreeOfParallelism = 4 },
        // The dictionary outgrows its page mid-chunk: the rest of the chunk goes plain.
        "dictionary_overflow" => new() { DictionaryPageBytes = 3000, DataPageRows = 500 },
        // A dictionary per row group: the views' entries start over with each.
        "row_groups" => new() { RowGroupRows = 1700, DataPageRows = 400 },
        _ => new(),
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void AViewColumn_WritesTheFileItsValuesWriteAsPlainColumns(bool binary, string layout)
    {
        string?[] values = Values(91, 20_000);
        using IArrowArray view = Views(binary ? BinaryViewType.Default : StringViewType.Default, values);
        IArrowArray plain = Plain(binary, values);
        ParquetWriteOptions options = Options(layout);
        Assert.Equal(Written(plain, options), Written(view, options));
        // A slice starting mid-array (the views' offset applies).
        Assert.Equal(Written(plain, options, 777, 12_345), Written(view, options, 777, 12_345));
    }

    [Fact]
    public void ViewsUnderAList_WriteTheFileTheirValuesWriteAsPlainStrings()
    {
        string?[] values = Values(92, 30_000);
        var random = new Random(93);
        List<int> offsets = [0];
        while (offsets[^1] < values.Length) offsets.Add(Math.Min(values.Length, offsets[^1] + random.Next(6)));
        int rows = offsets.Count - 1;
        ArrowBuffer listOffsets = new(MemoryMarshal.AsBytes(offsets.ToArray().AsSpan()).ToArray());
        IArrowArray List(IArrowArray items) => new ListArray(new ListType(new Field("item", items.Data.DataType, true)), rows, listOffsets, items, ArrowBuffer.Empty, 0);
        using IArrowArray items = Views(StringViewType.Default, values);
        IArrowArray view = List(items);
        IArrowArray plain = List(Plain(binary: false, values));
        var options = new ParquetWriteOptions();
        Assert.Equal(Written(plain, options), Written(view, options));
        Assert.Equal(Written(plain, options, 101, rows - 300), Written(view, options, 101, rows - 300));
    }
}
