using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// UUID columns (FIXED_LEN_BYTE_ARRAY(16) annotated UUID) hold their bytes in RFC 4122 (big-endian) order: typed rows
/// read them as the <see cref="Guid"/> other engines show, and the writer annotates Arrow <c>arrow.uuid</c> fields.
/// </summary>
public class ParquetGuidTests
{
    private static string Matrix(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "matrix", name);

    [Fact]
    public void UuidColumns_ReadAsTheGuidsOtherEnginesShow()
    {
        // pyarrow: the first rows of the uuid column, as pyarrow and DuckDB print them.
        List<PyarrowRow> pyarrow = ParquetFile.ReadList<PyarrowRow>(Matrix("pa_plain_v1.parquet"));
        Assert.Equal(Guid.Parse("00000000-0123-4567-890a-bcdef1234567"), pyarrow[1].uuid);
        Assert.Equal(Guid.Parse("00000000-0246-8acf-1215-79bde2468ace"), pyarrow[2].uuid);

        List<DuckRow> duck = ParquetFile.ReadList<DuckRow>(Matrix("duckdb_snappy.parquet"));
        Assert.Equal([.. Enumerable.Range(0, 3).Select(i => Guid.Parse($"00000000-0000-0000-0000-{i:x12}"))], duck.Take(3).Select(r => r.id));
    }

    [Fact]
    public void ArrowUuidFields_AreWrittenAsUuidColumns()
    {
        var g = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var bytes = new byte[16];
        g.TryWriteBytes(bytes, bigEndian: true, out _);
        var field = new Field("id", new FixedSizeBinaryType(16), false,
            [new("ARROW:extension:name", "arrow.uuid"), new("ARROW:extension:metadata", "")]);
        var schema = new Schema([field, new Field("plain", new FixedSizeBinaryType(16), false)], null);
        var column = new FixedSizeBinaryArray(new ArrayData(new FixedSizeBinaryType(16), 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        using var batch = new RecordBatch(schema, [column, column], 1);
        using var stream = new MemoryStream();
        ParquetWriter.Write(stream, schema, [batch], new ParquetWriteOptions { LeaveOpen = true });

        stream.Position = 0;
        using var file = ParquetFile.Open(stream);
        Assert.Equal(LogicalTypeKind.Uuid, file.Metadata.Thrift!.Schema[1].LogicalType?.Kind);
        Assert.Null(file.Metadata.Thrift!.Schema[2].LogicalType);
        using ParquetReader<WrittenRow> reader = file.OpenReader<WrittenRow>();
        Assert.Equal(g, Assert.Single(reader.ReadRows()).id);
    }

    private sealed record PyarrowRow(Guid? uuid);

    private sealed record DuckRow(Guid id);

    private sealed record WrittenRow(Guid id);
}
