using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>Footers of the Parquet test files, against pyarrow (footers.json) and against themselves.</summary>
public class ParquetFooterTests
{
    public static TheoryData<string> Files => ParquetFixtures.AllData;

    [Theory]
    [MemberData(nameof(Files))]
    public void Footer_MatchesPyarrow(string name)
    {
        JsonElement expected = ParquetFixtures.Expected(name);
        if (expected.TryGetProperty("error", out _))
        {
            // pyarrow could not read it: encrypted files and damaged footers. Ours must fail cleanly, or open.
            Exception? e = Record.Exception(() => ParquetFile.Open(ParquetFixtures.FullPath(name)).Dispose());
            if (e is not null) Assert.IsType<ParquetFormatException>(e);
            return;
        }

        using ParquetFile file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        FileMetaData meta = file.Thrift;
        List<string> diffs = [];
        void Check<T>(string what, T want, T got)
        {
            if (!EqualityComparer<T>.Default.Equals(want, got)) diffs.Add($"{what}: pyarrow {want}, ours {got}");
        }

        Check("created_by", expected.GetProperty("created_by").GetString() ?? "", meta.CreatedBy ?? "");
        Check("num_rows", expected.GetProperty("num_rows").GetInt64(), meta.NumRows);
        Check("footer length", expected.GetProperty("serialized_size").GetInt64(), (long)ParquetFixtures.FooterBytes(name).Length);
        Dictionary<string, string> kv = [];
        foreach (var (key, value) in file.Metadata.KeyValueMetadata) kv[key] = value ?? "";
        Check("key_value", Render(expected.GetProperty("key_value").EnumerateObject().Select(p => (p.Name, p.Value.GetString()!))),
            Render(kv.Select(p => (p.Key, p.Value))));

        List<Leaf> leaves = Leaves(meta.Schema, file.Metadata.Schema);
        JsonElement[] wantLeaves = [.. expected.GetProperty("leaves").EnumerateArray()];
        Check("leaf count", wantLeaves.Length, leaves.Count);
        for (int i = 0; i < Math.Min(wantLeaves.Length, leaves.Count); i++)
        {
            JsonElement w = wantLeaves[i];
            Leaf l = leaves[i];
            string at = $"leaf {i} ({w.GetProperty("path").GetString()})";
            Check($"{at} path", w.GetProperty("path").GetString(), l.Path);
            Check($"{at} physical", w.GetProperty("physical").GetString(), PhysicalName(l.Element.Type));
            Check($"{at} converted", w.GetProperty("converted").GetString(), ConvertedName(l.Element));
            Check($"{at} length", w.GetProperty("length").GetInt32(), l.Element.Type == ParquetType.FixedLenByteArray ? l.Element.TypeLength ?? 0 : 0);
            if (w.GetProperty("converted").GetString() == "DECIMAL")
            {
                Check($"{at} precision", w.GetProperty("precision").GetInt32(), l.View.DecimalPrecision ?? -1);
                Check($"{at} scale", w.GetProperty("scale").GetInt32(), l.View.DecimalScale ?? -1);
            }
            Check($"{at} max_def", w.GetProperty("max_def").GetInt32(), l.MaxDefinition);
            Check($"{at} max_rep", w.GetProperty("max_rep").GetInt32(), l.MaxRepetition);
        }

        JsonElement[] groups = [.. expected.GetProperty("row_groups").EnumerateArray()];
        Check("row groups", groups.Length, meta.RowGroups.Length);
        for (int g = 0; g < Math.Min(groups.Length, meta.RowGroups.Length); g++)
        {
            RowGroup group = meta.RowGroups[g];
            Check($"group {g} num_rows", groups[g].GetProperty("num_rows").GetInt64(), group.NumRows);
            Check($"group {g} total_byte_size", groups[g].GetProperty("total_byte_size").GetInt64(), group.TotalByteSize);
            JsonElement[] columns = [.. groups[g].GetProperty("columns").EnumerateArray()];
            for (int c = 0; c < columns.Length; c++)
            {
                JsonElement w = columns[c];
                ColumnChunk chunk = group.Columns[c];
                ColumnMetaData m = chunk.MetaData!;
                string at = $"group {g} column {c}";
                Check($"{at} path", w.GetProperty("path").GetString(), string.Join('.', m.PathInSchema));
                Check($"{at} physical", w.GetProperty("physical").GetString(), PhysicalName(m.Type));
                Check($"{at} compression", w.GetProperty("compression").GetString(), CodecName(m.Codec));
                Check($"{at} encodings", string.Join(',', w.GetProperty("encodings").EnumerateArray().Select(e => e.GetString())),
                    string.Join(',', m.Encodings.Select(EncodingName)));
                Check($"{at} num_values", w.GetProperty("num_values").GetInt64(), m.NumValues);
                Check($"{at} total_compressed", w.GetProperty("total_compressed").GetInt64(), m.TotalCompressedSize);
                Check($"{at} total_uncompressed", w.GetProperty("total_uncompressed").GetInt64(), m.TotalUncompressedSize);
                Check($"{at} file_offset", w.GetProperty("file_offset").GetInt64(), chunk.FileOffset);
                Check($"{at} data_page_offset", w.GetProperty("data_page_offset").GetInt64(), m.DataPageOffset);
                Check($"{at} dictionary_page_offset", OptionalLong(w, "dictionary_page_offset"), m.DictionaryPageOffset);
                Check($"{at} has_offset_index", w.GetProperty("has_offset_index").GetBoolean(), chunk.OffsetIndexOffset is not null);
                Check($"{at} has_column_index", w.GetProperty("has_column_index").GetBoolean(), chunk.ColumnIndexOffset is not null);
                Check($"{at} bloom_filter_offset", OptionalLong(w, "bloom_filter_offset"), m.BloomFilterOffset);
                Check($"{at} bloom_filter_length", OptionalLong(w, "bloom_filter_length"), (long?)m.BloomFilterLength);
                if (OptionalLong(w, "null_count") is long nulls) Check($"{at} null_count", nulls, m.Statistics?.NullCount);
                if (OptionalLong(w, "distinct_count") is long distinct) Check($"{at} distinct_count", distinct, m.Statistics?.DistinctCount);
            }
        }
        // Arrow C++ cannot apply a UUID annotation to INT32 and reports it as an undefined type of length 16.
        if (name == "apache/data/int32_with_uuid_logical_type.parquet")
            diffs.RemoveAll(d => d is "leaf 0 (int32_uuid) converted: pyarrow UNKNOWN, ours NONE" or "leaf 0 (int32_uuid) length: pyarrow 16, ours 0");
        Assert.True(diffs.Count == 0, $"{name}:\n{string.Join('\n', diffs.Take(40))}");
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void Footer_ReencodesToAFixedPoint(string name)
    {
        if (!Opens(name, out FileMetaData? meta)) return;
        byte[] once = Encode(meta);
        byte[] twice = Encode(FileMetaData.Parse(once));
        Assert.Equal(once, twice);
    }

    /// <summary>The footer written back is the original byte for byte, except where a writer differs from the canonical encoding.</summary>
    [Fact]
    public void Footer_ReencodesToTheOriginalBytes()
    {
        List<string> differ = [];
        foreach (string name in ParquetFixtures.All)
        {
            if (!Opens(name, out FileMetaData? meta)) continue;
            if (!ParquetFixtures.FooterBytes(name).AsSpan().SequenceEqual(Encode(meta))) differ.Add(name);
        }
        Assert.Equal(KnownReencodingDifferences, differ.Order(StringComparer.Ordinal));
    }

    /// <summary>Footers not written back byte for byte, each for a reason found by decoding it.</summary>
    private static readonly string[] KnownReencodingDifferences =
    [
        "apache/bad_data/ARROW-GH-41317.parquet",  // a list<Encoding> declared with i16 elements; written back as i32
        "apache/data/dict-page-offset-zero.parquet", // ColumnMetaData field 15 as a list of structs (today an i32): skipped
    ];

    /// <summary>
    /// The page-level structs found through the footer (first page headers, column and offset indexes, bloom filter
    /// headers) parse and write back byte for byte; counts how many of each were seen, so the test cannot pass empty.
    /// </summary>
    [Fact]
    public void PageStructs_ReencodeToTheOriginalBytes()
    {
        int pages = 0, dataV2 = 0, dictionaries = 0, columnIndexes = 0, offsetIndexes = 0, blooms = 0;
        List<string> failures = [];
        foreach (string name in ParquetFixtures.All)
        {
            if (!Opens(name, out FileMetaData? meta) || name.StartsWith("apache/bad_data/", StringComparison.Ordinal)) continue;
            byte[] file = File.ReadAllBytes(ParquetFixtures.FullPath(name));
            foreach (RowGroup group in meta.RowGroups)
            {
                foreach (ColumnChunk chunk in group.Columns)
                {
                    ColumnMetaData m = chunk.MetaData!;
                    // column_chunk_key_value_metadata.parquet has data_page_offset 0 after its dictionary page.
                    long first = m.DictionaryPageOffset is > 0 and var d && (d < m.DataPageOffset || m.DataPageOffset < 4) ? d : m.DataPageOffset;
                    Check("page header", first, null, span =>
                    {
                        PageHeader header = PageHeader.Parse(span, out int length);
                        pages++;
                        if (header.DataPageHeaderV2 is not null) dataV2++;
                        if (header.DictionaryPageHeader is not null) dictionaries++;
                        return (length, Write(header.Write));
                    });
                    if (chunk.ColumnIndexOffset is long ci)
                        Check("column index", ci, chunk.ColumnIndexLength, span =>
                        {
                            columnIndexes++;
                            // Writers differ on the element type of list<bool> (null_pages, field 1): Java and C++ write
                            // 1, arrow-rs 2. Ours is 1; compare the rest.
                            if (span.Length > 1 && span[0] == 0x19 && (span[1] & 0x0F) == ThriftType.BoolFalse) span[1] = (byte)((span[1] & 0xF0) | ThriftType.BoolTrue);
                            return (span.Length, Write(ColumnIndex.Parse(span).Write));
                        });
                    if (chunk.OffsetIndexOffset is long oi)
                        Check("offset index", oi, chunk.OffsetIndexLength, span => { offsetIndexes++; return (span.Length, Write(OffsetIndex.Parse(span).Write)); });
                    if (m.BloomFilterOffset is long bf)
                        Check("bloom filter header", bf, null, span =>
                        {
                            BloomFilterHeader header = BloomFilterHeader.Parse(span, out int length);
                            blooms++;
                            return (length, Write(header.Write));
                        });

                    void Check(string what, long offset, int? length, Func<byte[], (int Length, byte[] Again)> parse)
                    {
                        byte[] original = file.AsSpan((int)offset, length ?? (int)Math.Min(1 << 16, file.Length - offset)).ToArray();
                        try
                        {
                            var (used, again) = parse(original);
                            if (!original.AsSpan(0, used).SequenceEqual(again)) failures.Add($"{name}: {what} at {offset} differs");
                        }
                        catch (ParquetFormatException e)
                        {
                            failures.Add($"{name}: {what} at {offset}: {e.Message}");
                        }
                    }
                }
            }
        }
        Assert.True(failures.Count == 0, string.Join('\n', failures.Take(30)));
        Assert.True(pages > 1000 && dataV2 > 0 && dictionaries > 100 && columnIndexes > 50 && offsetIndexes > 50 && blooms >= 2,
            $"pages {pages}, v2 {dataV2}, dictionaries {dictionaries}, column indexes {columnIndexes}, offset indexes {offsetIndexes}, blooms {blooms}");

        static byte[] Write(Action<ThriftCompactWriter> write)
        {
            var writer = new ThriftCompactWriter();
            write(writer);
            return writer.ToArray();
        }
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void PublicView_MatchesTheFormerParser(string name)
    {
        if (ParquetFixtures.IsEncrypted(name)) return;
        byte[] footer;
        try
        {
            footer = ParquetFixtures.FooterBytes(name);
        }
        catch (ParquetFormatException)
        {
            return;
        }
        ParquetMetadata legacy;
        try
        {
            legacy = LegacyParquetFooter.Parse(footer);
        }
        catch (InvalidDataException)
        {
            return; // the former parser refused it; the new one is checked against pyarrow
        }
        ParquetMetadata current;
        try
        {
            current = ParquetMetadata.Parse(footer);
        }
        catch (ParquetFormatException e)
        {
            Assert.Fail($"{name}: the former parser read it, the new one refuses: {e.Message}");
            return;
        }
        Assert.Equal(RenderView(legacy), RenderView(current));
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void EncryptedFiles_AreRefusedByName(string name)
    {
        if (!ParquetFixtures.IsEncrypted(name)) return;
        var e = Assert.Throws<ParquetFormatException>(() => ParquetFile.Open(ParquetFixtures.FullPath(name)));
        Assert.Contains("encrypted", e.Message);
    }

    [Fact]
    public void BadData_FailsOnlyWithParquetFormatException()
    {
        foreach (string name in ParquetFixtures.All.Where(n => n.StartsWith("apache/bad_data/", StringComparison.Ordinal)))
        {
            Exception? e = Record.Exception(() => ParquetFile.Open(ParquetFixtures.FullPath(name)).Dispose());
            if (e is not null) Assert.True(e is ParquetFormatException, $"{name}: {e}");
        }
    }

    /// <summary>Damaged footers (bytes flipped, cut short) fail with ParquetFormatException or parse; nothing else.</summary>
    [Theory]
    [InlineData("apache/data/alltypes_plain.parquet")]
    [InlineData("apache/data/nested_maps.snappy.parquet")]
    [InlineData("apache/data/data_index_bloom_encoding_stats.parquet")]
    [InlineData("writers/pyarrow.parquet")]
    [InlineData("writers/spark_delta.parquet")]
    public void DamagedFooters_FailCleanly(string name)
    {
        byte[] footer = ParquetFixtures.FooterBytes(name);
        var random = new Random(20261113);
        for (int i = 0; i < 2000; i++)
        {
            byte[] damaged = (byte[])footer.Clone();
            int flips = random.Next(1, 4);
            for (int f = 0; f < flips; f++) damaged[random.Next(damaged.Length)] = (byte)random.Next(256);
            ParseOrFail(damaged, $"{name} damaged #{i}");
        }
        for (int cut = 0; cut < footer.Length; cut++) ParseOrFail(footer.AsSpan(0, cut).ToArray(), $"{name} cut at {cut}");

        static void ParseOrFail(byte[] bytes, string what)
        {
            Exception? e = Record.Exception(() => ParquetMetadata.Parse(bytes));
            if (e is not null) Assert.True(e is ParquetFormatException, $"{what}: {e}");
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static bool Opens(string name, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out FileMetaData? meta)
    {
        meta = null;
        if (ParquetFixtures.IsEncrypted(name)) return false;
        try
        {
            using ParquetFile file = ParquetFile.Open(ParquetFixtures.FullPath(name));
            meta = file.Thrift;
            return true;
        }
        catch (ParquetFormatException)
        {
            return false;
        }
    }

    internal static byte[] Encode(FileMetaData meta)
    {
        var writer = new ThriftCompactWriter();
        meta.Write(writer);
        return writer.ToArray();
    }

    private static long? OptionalLong(JsonElement e, string property) =>
        e.GetProperty(property) is { ValueKind: JsonValueKind.Number } v ? v.GetInt64() : null;

    private static string Render(IEnumerable<(string Key, string Value)> pairs) =>
        string.Join(";", pairs.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => $"{p.Key}={p.Value}"));

    private sealed record Leaf(string Path, SchemaElement Element, ParquetSchemaElement View, int MaxDefinition, int MaxRepetition);

    /// <summary>The leaves in order, with their dotted paths and maximum levels.</summary>
    private static List<Leaf> Leaves(SchemaElement[] schema, IReadOnlyList<ParquetSchemaElement> view)
    {
        List<Leaf> leaves = [];
        int at = 1;
        for (int i = 0; i < (schema[0].NumChildren ?? 0); i++) Walk(null, 0, 0);
        return leaves;

        void Walk(string? prefix, int def, int rep)
        {
            int index = at++;
            SchemaElement e = schema[index];
            if (e.Repetition == FieldRepetition.Optional) def++;
            if (e.Repetition == FieldRepetition.Repeated) { def++; rep++; }
            string path = prefix is null ? e.Name : $"{prefix}.{e.Name}";
            if (SchemaTree.IsLeaf(e)) leaves.Add(new Leaf(path, e, view[index], def, rep));
            else for (int c = 0; c < (e.NumChildren ?? 0); c++) Walk(path, def, rep);
        }
    }

    private static string PhysicalName(ParquetType? type) => type switch
    {
        ParquetType.Boolean => "BOOLEAN",
        ParquetType.Int32 => "INT32",
        ParquetType.Int64 => "INT64",
        ParquetType.Int96 => "INT96",
        ParquetType.Float => "FLOAT",
        ParquetType.Double => "DOUBLE",
        ParquetType.ByteArray => "BYTE_ARRAY",
        ParquetType.FixedLenByteArray => "FIXED_LEN_BYTE_ARRAY",
        _ => $"?{type}",
    };

    /// <summary>The converted type as Arrow C++ reports it: the one the logical type implies, else the written one.</summary>
    private static string ConvertedName(SchemaElement e)
    {
        if (e.LogicalType is null && e.ConvertedType is { } converted) return converted switch
        {
            ConvertedType.Utf8 => "UTF8",
            ConvertedType.Map => "MAP",
            ConvertedType.MapKeyValue => "MAP_KEY_VALUE",
            ConvertedType.List => "LIST",
            ConvertedType.Enum => "ENUM",
            ConvertedType.Decimal => "DECIMAL",
            ConvertedType.Date => "DATE",
            ConvertedType.TimeMillis => "TIME_MILLIS",
            ConvertedType.TimeMicros => "TIME_MICROS",
            ConvertedType.TimestampMillis => "TIMESTAMP_MILLIS",
            ConvertedType.TimestampMicros => "TIMESTAMP_MICROS",
            ConvertedType.UInt8 => "UINT_8",
            ConvertedType.UInt16 => "UINT_16",
            ConvertedType.UInt32 => "UINT_32",
            ConvertedType.UInt64 => "UINT_64",
            ConvertedType.Int8 => "INT_8",
            ConvertedType.Int16 => "INT_16",
            ConvertedType.Int32 => "INT_32",
            ConvertedType.Int64 => "INT_64",
            ConvertedType.Json => "JSON",
            ConvertedType.Bson => "BSON",
            ConvertedType.Interval => "INTERVAL",
            _ => $"?{converted}",
        };
        return e.LogicalType switch
        {
            null => "NONE",
            // Arrow keeps the written converted type of a decimal whose logical type it could not apply.
            { Kind: LogicalTypeKind.String } => "UTF8",
            { Kind: LogicalTypeKind.Map } => "MAP",
            { Kind: LogicalTypeKind.List } => "LIST",
            { Kind: LogicalTypeKind.Enum } => "ENUM",
            { Kind: LogicalTypeKind.Decimal } => "DECIMAL",
            { Kind: LogicalTypeKind.Date } => "DATE",
            { Kind: LogicalTypeKind.Time, IsAdjustedToUtc: true, Unit: ParquetTimeUnit.Millis } => "TIME_MILLIS",
            { Kind: LogicalTypeKind.Time, IsAdjustedToUtc: true, Unit: ParquetTimeUnit.Micros } => "TIME_MICROS",
            { Kind: LogicalTypeKind.Timestamp, IsAdjustedToUtc: true, Unit: ParquetTimeUnit.Millis } => "TIMESTAMP_MILLIS",
            { Kind: LogicalTypeKind.Timestamp, IsAdjustedToUtc: true, Unit: ParquetTimeUnit.Micros } => "TIMESTAMP_MICROS",
            { Kind: LogicalTypeKind.Integer } i => $"{(i.IsSigned ? "INT" : "UINT")}_{i.BitWidth}",
            { Kind: LogicalTypeKind.Json } => "JSON",
            { Kind: LogicalTypeKind.Bson } => "BSON",
            { Kind: var kind } when !Enum.IsDefined(kind) => "UNKNOWN", // a member Arrow C++ does not know
            _ => "NONE",
        };
    }

    /// <summary>Codec names as pyarrow prints them: LZ4_RAW is its "LZ4", the Hadoop-framed LZ4 is unknown to it.</summary>
    private static string CodecName(CompressionCodec codec) => codec switch
    {
        CompressionCodec.Uncompressed => "UNCOMPRESSED",
        CompressionCodec.Snappy => "SNAPPY",
        CompressionCodec.Gzip => "GZIP",
        CompressionCodec.Lzo => "LZO",
        CompressionCodec.Brotli => "BROTLI",
        CompressionCodec.Lz4 => "UNKNOWN",
        CompressionCodec.Zstd => "ZSTD",
        CompressionCodec.Lz4Raw => "LZ4",
        _ => $"?{codec}",
    };

    private static string EncodingName(ParquetEncoding encoding) => encoding switch
    {
        ParquetEncoding.Plain => "PLAIN",
        ParquetEncoding.PlainDictionary => "PLAIN_DICTIONARY",
        ParquetEncoding.Rle => "RLE",
        ParquetEncoding.BitPacked => "BIT_PACKED",
        ParquetEncoding.DeltaBinaryPacked => "DELTA_BINARY_PACKED",
        ParquetEncoding.DeltaLengthByteArray => "DELTA_LENGTH_BYTE_ARRAY",
        ParquetEncoding.DeltaByteArray => "DELTA_BYTE_ARRAY",
        ParquetEncoding.RleDictionary => "RLE_DICTIONARY",
        ParquetEncoding.ByteStreamSplit => "BYTE_STREAM_SPLIT",
        _ => "UNKNOWN", // ALP and later
    };

    /// <summary>The public view as text: schema, row count, row groups with paths, counts and statistics.</summary>
    internal static string RenderView(ParquetMetadata m)
    {
        var s = new StringBuilder();
        s.AppendLine($"rows {m.RowCount}");
        foreach (ParquetSchemaElement e in m.Schema) s.AppendLine(e.ToString());
        foreach (ParquetRowGroup g in m.RowGroups)
        {
            s.AppendLine($"group {g.RowCount}");
            foreach (ParquetColumnChunk c in g.Columns)
            {
                ParquetColumnStatistics? st = c.Statistics;
                s.AppendLine($"  {string.Join('.', c.Path)} {c.ValueCount} " +
                    (st is null ? "no stats" : $"min {Hex(st.Min)} max {Hex(st.Max)} nulls {st.NullCount}"));
            }
        }
        return s.ToString();

        static string Hex(byte[]? b) => b is null ? "-" : Convert.ToHexString(b);
    }
}
