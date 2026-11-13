using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;
using static Kaminari.Seseragi.Files.Avro.AvroField;
using S = Kaminari.Seseragi.Files.Avro.AvroSchema;

namespace Polars.Benchmarks;

// Many small files of one schema, as an Iceberg table's manifests: a header of a few KB (the Avro schema plus the
// table's own metadata), a deflate block of a few rows with nested statistics maps. Each read opens a file from
// memory and reads it whole, with the manifest reader schema matched by field id; the cache skips parsing and
// resolving the schema again.
//
// Filter: --filter '*AvroSmallFileBenchmarks*'

[MemoryDiagnoser]
public class AvroSmallFileBenchmarks
{
    private const int Files = 200;
    private const int RowsPerFile = 3;

    private readonly List<byte[]> _files = [];
    private AvroRecordSchema _reader = null!;
    private AvroReadCache _cache = null!;

    private static AvroRecordSchema Manifest() => S.Record("manifest_entry",
    [
        Required("status", S.Int, 0), Optional("snapshot_id", S.Long, 1), Optional("sequence_number", S.Long, 3),
        Optional("file_sequence_number", S.Long, 4),
        Required("data_file", S.Record("r2",
        [
            Required("content", S.Int, 134, defaultJson: "0"), Required("file_path", S.String, 100), Required("file_format", S.String, 101),
            Required("partition", S.Record("r102", [Optional("dt", S.Date, 1000), Optional("id_bucket", S.Int, 1001)]), 102),
            Required("record_count", S.Long, 103), Required("file_size_in_bytes", S.Long, 104),
            Optional("column_sizes", S.ArrayMap("k117_v118", S.Int, S.Long, 117, 118), 108),
            Optional("value_counts", S.ArrayMap("k119_v120", S.Int, S.Long, 119, 120), 109),
            Optional("null_value_counts", S.ArrayMap("k121_v122", S.Int, S.Long, 121, 122), 110),
            Optional("nan_value_counts", S.ArrayMap("k138_v139", S.Int, S.Long, 138, 139), 137),
            Optional("lower_bounds", S.ArrayMap("k126_v127", S.Int, S.Bytes, 126, 127), 125),
            Optional("upper_bounds", S.ArrayMap("k129_v130", S.Int, S.Bytes, 129, 130), 128),
            Optional("key_metadata", S.Bytes, 131), Optional("split_offsets", S.Array(S.Long, 133), 132),
            Optional("equality_ids", S.Array(S.Int, 136), 135), Optional("sort_order_id", S.Int, 140),
            Optional("referenced_data_file", S.String, 143),
        ]), 2),
    ]);

    [GlobalSetup]
    public void Setup()
    {
        var schema = Manifest();
        Schema arrow = schema.ToArrowSchema();
        var dataFileType = (StructType)arrow.GetFieldByName("data_file").DataType;
        var metadata = new Dictionary<string, byte[]>
        {
            // Stand-ins for the table metadata Iceberg puts in every manifest header (a few KB of JSON).
            ["schema"] = Encoding.UTF8.GetBytes(new string('s', 2000)),
            ["partition-spec"] = Encoding.UTF8.GetBytes(new string('p', 1500)),
            ["format-version"] = "2"u8.ToArray(),
            ["content"] = "data"u8.ToArray(),
        };
        for (int f = 0; f < Files; f++)
        {
            var batch = new RecordBatch(arrow, [Status(), Nullable(), Nullable(), Nullable(), DataFile(dataFileType, f)], RowsPerFile);
            var ms = new MemoryStream();
            AvroWriter.Write(ms, arrow, [batch], new() { WriterSchema = schema, Metadata = metadata, Compression = new DeflateCompression() });
            _files.Add(ms.ToArray());
        }
        _reader = Manifest();
        _cache = new AvroReadCache();

        static IArrowArray Status() => new Int32Array.Builder().AppendRange([1, 1, 1]).Build();
        static IArrowArray Nullable() => new Int64Array.Builder().AppendNull().AppendNull().AppendNull().Build();
    }

    private static IArrowArray DataFile(StructType type, int file)
    {
        var children = new List<IArrowArray>();
        foreach (var field in type.Fields)
        {
            children.Add(field.Name switch
            {
                "content" => new Int32Array.Builder().AppendRange([0, 0, 0]).Build(),
                "file_path" => new StringArray.Builder().AppendRange([$"s3://bucket/t/data/{file}-0.parquet", $"s3://bucket/t/data/{file}-1.parquet", $"s3://bucket/t/data/{file}-2.parquet"]).Build(),
                "file_format" => new StringArray.Builder().AppendRange(["PARQUET", "PARQUET", "PARQUET"]).Build(),
                "record_count" or "file_size_in_bytes" => new Int64Array.Builder().AppendRange([1000, 2000, 3000]).Build(),
                "partition" => new StructArray(field.DataType, RowsPerFile,
                    [new Date32Array.Builder().Append(new DateTime(2024, 1, 1)).Append(new DateTime(2024, 1, 2)).Append(new DateTime(2024, 1, 3)).Build(),
                     new Int32Array.Builder().AppendRange([0, 1, 2]).Build()], ArrowBuffer.Empty, 0, 0),
                "column_sizes" or "value_counts" or "null_value_counts" or "nan_value_counts" => LongMap((MapType)field.DataType),
                "lower_bounds" or "upper_bounds" => BytesMap((MapType)field.DataType),
                _ => NullOf(field.DataType),
            });
        }
        return new StructArray(type, RowsPerFile, children, ArrowBuffer.Empty, 0, 0);
    }

    // Twelve columns of statistics per data file.
    private static IArrowArray LongMap(MapType type)
    {
        var b = new MapArray.Builder(type);
        for (int r = 0; r < RowsPerFile; r++)
        {
            b.Append();
            for (int c = 1; c <= 12; c++)
            {
                ((Int32Array.Builder)b.KeyBuilder).Append(c);
                ((Int64Array.Builder)b.ValueBuilder).Append(c * 100L + r);
            }
        }
        return b.Build();
    }

    private static IArrowArray BytesMap(MapType type)
    {
        var b = new MapArray.Builder(type);
        for (int r = 0; r < RowsPerFile; r++)
        {
            b.Append();
            for (int c = 1; c <= 12; c++)
            {
                ((Int32Array.Builder)b.KeyBuilder).Append(c);
                ((BinaryArray.Builder)b.ValueBuilder).Append((ReadOnlySpan<byte>)[(byte)c, (byte)r, 0, 0]);
            }
        }
        return b.Build();
    }

    // An all-null column of a type (only the optional scalar and list fields reach here).
    private static IArrowArray NullOf(IArrowType type) => type switch
    {
        BinaryType => new BinaryArray.Builder().AppendNull().AppendNull().AppendNull().Build(),
        StringType => new StringArray.Builder().AppendNull().AppendNull().AppendNull().Build(),
        Int32Type => new Int32Array.Builder().AppendNull().AppendNull().AppendNull().Build(),
        ListType l => NullLists(l),
        _ => throw new NotSupportedException(type.Name),
    };

    private static ListArray NullLists(ListType type)
    {
        var b = new ListArray.Builder(type.ValueDataType);
        for (int r = 0; r < RowsPerFile; r++) b.AppendNull();
        return b.Build();
    }

    private long ReadAll(AvroReadCache? cache)
    {
        long rows = 0;
        foreach (byte[] file in _files)
        {
            using var reader = AvroReader.Open(new MemoryStream(file), new() { ReaderSchema = _reader, FieldMatching = AvroFieldMatching.ByFieldId, Cache = cache });
            foreach (var b in reader.ReadBatches())
            {
                rows += b.Length;
                b.Dispose();
            }
        }
        return rows;
    }

    [Benchmark(Baseline = true)]
    public long ReadWithoutCache() => ReadAll(null);

    [Benchmark]
    public long ReadWithCache() => ReadAll(_cache);
}
