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
        "apache/data/dict-page-offset-zero.parquet",       // SortingColumn.descending missing (an old field id)
        "apache/data/incorrect_map_schema.parquet",        // since Polars 2.x: a MAP whose key is optional
        "apache/data/int32_with_uuid_logical_type.parquet", // refuses the annotation instead of ignoring it
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
        .Select(p => Path.GetRelativePath(Root, p).Replace('\\', '/')).Where(n => !ManagedRefuses.Contains(n) && !PolarsMisreads.Contains(n) && !TooLarge.Contains(n))
        .Where(n => !n.StartsWith("encrypted/", StringComparison.Ordinal) || n == "encrypted/plain.parquet") // the others need keys: KeyedFiles
        .Order(StringComparer.Ordinal)];

    private sealed class BatchStream(Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches) : IArrowArrayStream
    {
        private readonly IEnumerator<RecordBatch> _batches = batches.GetEnumerator();
        public Apache.Arrow.Schema Schema => schema;
        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
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

    private static Dictionary<string, ReadOnlyMemory<byte>> Keys(params (string Column, string Key)[] keys) =>
        keys.ToDictionary(k => k.Column, k => (ReadOnlyMemory<byte>)System.Text.Encoding.ASCII.GetBytes(k.Key));

    /// <summary>Encrypted files both readers read (AES_GCM_V1, 16- or 32-byte keys), with their keys.</summary>
    private static readonly Dictionary<string, ParquetDecryptionOptions> Keyed = BuildKeyed();

    private static Dictionary<string, ParquetDecryptionOptions> BuildKeyed()
    {
        byte[] k16 = "0123456789012345"u8.ToArray(), k32 = "01234567890123456789012345678901"u8.ToArray();
        var ours = Keys(("i64", "1234567890123450"), ("s.a", "1234567890123451"));
        var apache = Keys(("double_field", "1234567890123450"), ("float_field", "1234567890123451"));
        var apache256 = Keys(("double_field", "12345678901234567890123456789012"), ("float_field", "12345678901234567890123456789013"),
            ("boolean_field", "12345678901234567890123456789014"), ("int32_field", "12345678901234567890123456789015"),
            ("ba_field", "12345678901234567890123456789016"), ("flba_field", "12345678901234567890123456789017"),
            ("int64_field.list.element", "12345678901234567890123456789018"), ("int96_field", "12345678901234567890123456789019"));
        byte[] tester = "tester"u8.ToArray();
        return new()
        {
            ["encrypted/pa_gcm128.parquet"] = new(k16),
            ["encrypted/pa_gcm128_v2.parquet"] = new(k16),
            ["encrypted/pa_gcm128_plaintext_footer.parquet"] = new(k16),
            ["encrypted/pa_gcm128_aad_supplied.parquet"] = new(k16) { AadPrefix = "table-7"u8.ToArray() },
            ["encrypted/pa_gcm256_aad_stored.parquet"] = new(k32),
            ["encrypted/pl_columns.parquet"] = new(k16) { ColumnKeys = ours },
            ["encrypted/pl_columns_plaintext_footer.parquet"] = new(k16) { ColumnKeys = ours },
            ["apache/data/uniform_encryption.parquet.encrypted"] = new(k16),
            ["apache/data/encrypt_columns_and_footer.parquet.encrypted"] = new(k16) { ColumnKeys = apache },
            ["apache/data/encrypt_columns_plaintext_footer.parquet.encrypted"] = new(k16) { ColumnKeys = apache },
            ["apache/data/encrypt_columns_and_footer_aad.parquet.encrypted"] = new(k16) { ColumnKeys = apache, AadPrefix = tester },
            ["apache/data/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted"] = new(k16) { ColumnKeys = apache, AadPrefix = tester },
            ["apache/data/encrypt_columns_and_footer_bloom_filter.parquet.encrypted"] = new(k16) { ColumnKeys = apache },
            ["apache/data/aes256/uniform_encryption.parquet.encrypted"] = new(k32),
            ["apache/data/aes256/encrypt_columns_and_footer.parquet.encrypted"] = new(k32) { ColumnKeys = apache256 },
            ["apache/data/aes256/encrypt_columns_plaintext_footer.parquet.encrypted"] = new(k32) { ColumnKeys = apache256 },
            ["apache/data/aes256/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted"] = new(k32) { ColumnKeys = apache256, AadPrefix = tester },
        };
    }

    public static TheoryData<string> KeyedFiles => [.. Keyed.Keys.Order(StringComparer.Ordinal)];

    [Theory]
    [MemberData(nameof(Files))]
    public void ManagedRead_EqualsPolarsRead(string name) => AssertSameRead(name, null);

    /// <summary>The managed reader and the engine decrypt alike (the managed one with Seseragi's options, the engine with Polars.NET's).</summary>
    [Theory]
    [MemberData(nameof(KeyedFiles))]
    public void ManagedRead_EqualsPolarsRead_OfEncryptedFiles(string name) => AssertSameRead(name, Keyed[name]);

    private static void AssertSameRead(string name, ParquetDecryptionOptions keys)
    {
        string path = Path.Combine(Root, name);
        ParquetDecryption engineKeys = keys is null ? null
            : new ParquetDecryption(keys.FooterKey) { ColumnKeys = keys.ColumnKeys, AadPrefix = keys.AadPrefix };
        using var file = ParquetFile.Open(path, keys);
        string[] columns = [.. file.Schema.FieldsList.Where(Importable).Select(f => f.Name)];
        if (columns.Length == 0) return;
        // Small batches through the owning stream: each batch is disposed as soon as Polars has imported it, so the
        // frame lives on the pins of the export (freed memory would be reused by the later batches and show here).
        using var reader = file.OpenReader(new() { Columns = columns, BatchSize = 1_000 });
        using var managed = DataFrame.FromArrowStream(reader.ReadArrowStream());
        if (PolarsRefuses.Contains(name))
        {
            Assert.ThrowsAny<Exception>(() => DataFrame.ReadParquet(path, columns: columns, decryption: engineKeys).Dispose());
            Assert.Equal(file.Metadata.RowCount, managed.Height);
            return;
        }
        using var native = DataFrame.ReadParquet(path, columns: columns, decryption: engineKeys);
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
