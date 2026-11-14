using Apache.Arrow;
using Apache.Arrow.Ipc;
using AT = Apache.Arrow.Types;
using ArrowField = Apache.Arrow.Field;
using Kaminari.Seseragi.Files.Parquet;

namespace Polars.CSharp.Tests.IO;

/// <summary>
/// The managed Parquet reader, imported into Polars, against Polars' own Parquet reader: the same frames, file by
/// file, with Polars' known faults listed by name.
/// </summary>
public class ParquetManagedPipelineTests
{
    private static readonly string Root = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet");

    /// <summary>Files the managed reader refuses (ALP, values that do not fit): checked in Seseragi.Files.Tests.</summary>
    private static readonly HashSet<string> ManagedRefuses = ["apache/data/alp_extended.zstd.parquet", "apache/data/flba12_timestamp.parquet"];

    /// <summary>Files Polars' reader refuses while the managed reader reads them (pyarrow and DuckDB agree with it).</summary>
    private static readonly HashSet<string> PolarsRefuses =
    [
        "apache/data/concatenated_gzip_members.parquet",   // "failed to fill whole buffer": one GZIP member only
        "apache/data/dict-page-offset-zero.parquet",       // SortingColumn.descending missing (an old field id)
        "apache/data/int32_with_uuid_logical_type.parquet", // refuses the annotation instead of ignoring it
        "apache/data/int96_timestamp_order.parquet",       // a ColumnOrder member it does not know
        "apache/data/nation.dict-malformed.parquet",       // PARQUET-816 padding missing
        "apache/data/unknown-logical-type.parquet",        // a LogicalType member it does not know
        // A file with an INTERVAL column cannot be read at all, whatever the columns selected:
        "matrix/duckdb_brotli.parquet", "matrix/duckdb_gzip.parquet", "matrix/duckdb_lz4.parquet", "matrix/duckdb_snappy.parquet",
        "matrix/duckdb_uncompressed.parquet", "matrix/duckdb_zstd.parquet", "writers/duckdb.parquet",
        "nested/spark_variant.parquet",                    // the VARIANT logical type
        // DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT on FIXED_LEN_BYTE_ARRAY columns: "not yet supported".
        "matrix/enc_dba_small_pages.parquet", "matrix/enc_dba_v1_none.parquet", "matrix/enc_dba_v1_zstd.parquet",
        "matrix/enc_dba_v2_snappy.parquet", "matrix/enc_dba_v2_zstd.parquet", "matrix/enc_spark_v2.parquet",
        "matrix/enc_bss_small_pages.parquet", "matrix/enc_bss_v1_none.parquet", "matrix/enc_bss_v1_zstd.parquet",
        "matrix/enc_bss_v2_snappy.parquet", "matrix/enc_bss_v2_zstd.parquet", "apache/data/byte_stream_split_extended.gzip.parquet",
    ];

    /// <summary>Files Polars reads wrongly (the managed reader agrees with pyarrow and DuckDB there).</summary>
    private static readonly HashSet<string> PolarsMisreads =
    [
        "apache/data/old_list_structure.parquet",          // two-level lists named "array": a list of structs instead of a list of lists
        "apache/data/repeated_primitive_no_list.parquet",  // repeated fields without LIST: empty lists instead of the values
        "apache/data/nested_structs.rust.parquet",         // drops the UTC zone of timestamps inside structs
    ];

    /// <summary>Files too large for a test run (a 2 GiB string chunk).</summary>
    private static readonly HashSet<string> TooLarge = ["apache/data/large_string_map.brotli.parquet"];

    public static TheoryData<string> Files => [.. Directory.EnumerateFiles(Root, "*.parquet", SearchOption.AllDirectories)
        .Select(p => Path.GetRelativePath(Root, p).Replace('\\', '/')).Where(n => !ManagedRefuses.Contains(n) && !PolarsMisreads.Contains(n) && !TooLarge.Contains(n)).Order(StringComparer.Ordinal)];

    private sealed class BatchStream(Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches) : IArrowArrayStream
    {
        private readonly IEnumerator<RecordBatch> _batches = batches.GetEnumerator();
        public Apache.Arrow.Schema Schema => schema;
        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(_batches.MoveNext() ? _batches.Current : null);
        public void Dispose() => _batches.Dispose();
    }

    /// <summary>Columns Polars cannot import (Decimal256) or imports only with a global option (intervals, see the Config tests), at any depth.</summary>
    private static bool Importable(ArrowField f) => Importable(f.DataType);

    private static bool Importable(AT.IArrowType type) => type switch
    {
        AT.Decimal256Type or AT.IntervalType => false,
        AT.ListType l => Importable(l.ValueDataType),
        AT.FixedSizeListType l => Importable(l.ValueDataType),
        AT.MapType m => Importable(m.KeyField.DataType) && Importable(m.ValueField.DataType),
        AT.StructType s => s.Fields.All(Importable),
        _ => true,
    };

    [Theory]
    [MemberData(nameof(Files))]
    public void ManagedRead_EqualsPolarsRead(string name)
    {
        string path = Path.Combine(Root, name);
        using var file = ParquetFile.Open(path);
        string[] columns = [.. file.Schema.FieldsList.Where(Importable).Select(f => f.Name)];
        if (columns.Length == 0) return;
        // Small batches through the owning stream: each batch is disposed as soon as Polars has imported it, so the
        // frame lives on the pins of the export (freed memory would be reused by the later batches and show here).
        using var reader = file.OpenReader(new() { Columns = columns, BatchSize = 1_000 });
        using var managed = DataFrame.FromArrowStream(reader.ReadArrowStream());
        if (PolarsRefuses.Contains(name))
        {
            Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(path, columns: columns).Dispose());
            Assert.Equal(file.Metadata.RowCount, managed.Height);
            return;
        }
        using var native = DataFrame.ReadParquet(path, columns: columns);
        if (name == "apache/data/repeated_no_annotation.parquet")
        {
            Assert.Equal(0, native.Height); // Polars reads no rows when only the flat column beside a repeated group is selected
            Assert.Equal(6, managed.Height);
            return;
        }

        List<Series> ours = [], theirs = [];
        foreach (string column in columns)
        {
            Series a = managed[column], b = native[column];
            string ta = a.DataType.ToString(), tb = b.DataType.ToString();
            if (ta == tb) { }
            else if (ta.StartsWith("datetime[μs", StringComparison.Ordinal) && tb.StartsWith("datetime[ns", StringComparison.Ordinal))
            {
                // INT96: read in microseconds (Spark's semantics); Polars reads nanoseconds, clamping what lies outside
                // 1677-2262 (its known fault). Compared in microseconds over the range nanoseconds hold.
                using Series inRange = a.IsBetween(new DateTime(1677, 9, 22), new DateTime(2262, 4, 11));
                using Series ourPart = a.Filter(inRange), theirPart = b.Cast(a.DataType).Filter(inRange);
                Assert.True(ourPart.Equals(theirPart), $"{name}.{column}");
                continue;
            }
            else if ((ta, tb) is ("f16", "binary") or ("str", "binary")) continue; // Polars reads FLOAT16 and JSON as binary
            else if (ta == "null" && b.NullCount == b.Length) continue; // UNKNOWN: Polars keeps the physical type without an Arrow schema
            else Assert.Fail($"{name}.{column}: ours {ta}, Polars {tb}");
            ours.Add(a);
            theirs.Add(b);
        }
        if (ours.Count == 0) return;
        using var left = new DataFrame([.. ours]);
        using var right = new DataFrame([.. theirs]);
        Assert.True(left == right, $"{name}:\n{left}\n{right}");
    }
}
