using Kaminari.Seseragi.Files;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Microsoft.FSharp.Collections;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetDataset"/> schema modes: the first file's columns, every file's united by name, or a given
/// schema; files' columns converted (lossless widenings, lossy ones when allowed), missing columns and struct fields,
/// extra ones left out; filters over converted and missing columns.
/// </summary>
public sealed class ParquetDatasetSchemaTests
{
    private readonly MemoryFileStore _store = new();

    // ---- writing --------------------------------------------------------------------------------------------------

    /// <summary>Writes a file of the columns given (name, array), each array one row group of its rows.</summary>
    private void Put(string path, params (string Name, IArrowArray Values)[] columns) => Put(path, rowGroupRows: 1000, columns);

    private void Put(string path, int rowGroupRows, params (string Name, IArrowArray Values)[] columns) =>
        Put(path, new ParquetWriteOptions { RowGroupRows = rowGroupRows }, columns);

    private void Put(string path, ParquetWriteOptions options, params (string Name, IArrowArray Values)[] columns)
    {
        var schema = new Apache.Arrow.Schema([.. columns.Select(c => new Field(c.Name, c.Values.Data.DataType, true))], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, options))
        {
            writer.Write(new RecordBatch(schema, [.. columns.Select(c => c.Values)], columns[0].Values.Length));
            writer.Complete();
        }
        _store.Add(path, stream.ToArray());
    }

    private static Int64Array Longs(params long?[] values)
    {
        var b = new Int64Array.Builder();
        foreach (long? v in values) b.Append(v);
        return b.Build();
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (int? v in values) b.Append(v);
        return b.Build();
    }

    private static Int8Array Bytes(params sbyte[] values) => new Int8Array.Builder().AppendRange(values).Build();

    private static DoubleArray Doubles(params double[] values) => new DoubleArray.Builder().AppendRange(values).Build();

    private static StringArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? v in values) b.Append(v);
        return b.Build();
    }

    private static StructArray Struct((string Name, IArrowArray Values)[] fields) =>
        new(new StructType([.. fields.Select(f => new Field(f.Name, f.Values.Data.DataType, true))]), fields[0].Values.Length, [.. fields.Select(f => f.Values)], ArrowBuffer.Empty, 0);

    // ---- reading --------------------------------------------------------------------------------------------------

    private ParquetDataset Open(ParquetDatasetOptions? options = null, string path = "d/") =>
        ParquetDataset.Open(path, (options ?? new ParquetDatasetOptions()) with { Store = _store });

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        return Rows(reader.ReadBatches());
    }

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => Render(batch.Column(c), i))));
            }
        }
        return rows;
    }

    private static string Render(IArrowArray array, int i) => array.IsNull(i) ? "null" : array switch
    {
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        DoubleArray d => d.GetValue(i)!.Value.ToString("R", CultureInfo.InvariantCulture),
        FloatArray f => ((double)f.GetValue(i)!.Value).ToString("R", CultureInfo.InvariantCulture),
        DictionaryArray d => Render(d.Dictionary, d.Indices is Int32Array ix ? ix.GetValue(i)!.Value : throw new NotSupportedException()),
        StructArray s => "{" + string.Join(",", ((StructType)s.Data.DataType).Fields.Select((f, k) => $"{f.Name}:{Render(s.Fields[k], i)}")) + "}",
        ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(k => Render(l.Values, k))) + "]",
        _ => CanonicalRender.Value(array, i),
    };

    private static ScalarExpr Compare(BinaryOp op, string column, LiteralValue value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(value));

    // ---- first file -----------------------------------------------------------------------------------------------

    [Fact]
    public void FirstFile_WidensNarrowerColumnsOfLaterFiles()
    {
        Put("d/a.parquet", ("x", Longs(1, null)));
        Put("d/b.parquet", ("x", Ints(int.MinValue)));
        Put("d/c.parquet", ("x", Bytes(-7)));
        using ParquetDataset dataset = Open();
        Assert.IsType<Int64Type>(dataset.Schema.GetFieldByName("x").DataType);
        Assert.Equal(["1", "null", "-2147483648", "-7"], Read(dataset));
        // Row numbers run across the converted files.
        Assert.Equal(["0|1", "1|null", "2|-2147483648", "3|-7"], Read(dataset, new ParquetReadOptions { RowIndexColumn = "n" }));
    }

    [Fact]
    public void FirstFile_AWiderLaterFile_IsAnError_ThatNamesTheWayOut()
    {
        Put("d/a.parquet", ("x", Ints(1)));
        Put("d/b.parquet", ("x", Longs(2)));
        using ParquetDataset dataset = Open();
        var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
        Assert.Contains("d/b.parquet", error.Message);
        Assert.Contains("'x'", error.Message);
        Assert.Contains("int64 cannot be read as int32", error.Message);
        Assert.Contains("UnionByName", error.Message);
        // United by name, the dataset's column is the wider one.
        using ParquetDataset united = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.Equal(["1", "2"], Read(united));
    }

    [Fact]
    public void LossyConversions_NeedTheSwitch()
    {
        Put("d/a.parquet", ("x", Doubles(0.5)));
        Put("d/b.parquet", ("x", Longs((1L << 53) + 1)));
        using (ParquetDataset dataset = Open())
        {
            var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
            Assert.Contains("d/b.parquet", error.Message);
            Assert.Contains("lossy", error.Message);
            Assert.Contains("AllowLossyCasts", error.Message);
        }
        using (ParquetDataset lossy = Open(new ParquetDatasetOptions { AllowLossyCasts = true }))
            Assert.Equal(["0.5", "9007199254740992"], Read(lossy));
    }

    [Fact]
    public void MissingColumns_AreAnError_OrNulls_AndExtraColumnsAreLeftOut()
    {
        Put("d/a.parquet", ("x", Longs(1)), ("y", Strings("a")));
        Put("d/b.parquet", ("z", Doubles(9)), ("x", Longs(2)));
        using (ParquetDataset dataset = Open())
        {
            Assert.Equal(["x", "y"], dataset.Schema.FieldsList.Select(f => f.Name));
            var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
            Assert.Contains("d/b.parquet", error.Message);
            Assert.Contains("no column 'y'", error.Message);
            Assert.Contains("AllowMissingColumns", error.Message);
            // Not read, not missed.
            Assert.Equal(["1", "2"], Read(dataset, new ParquetReadOptions { Columns = ["x"] }));
        }
        using (ParquetDataset allowed = Open(new ParquetDatasetOptions { AllowMissingColumns = true }))
        {
            Assert.Equal(["1|a", "2|null"], Read(allowed));
            Assert.Equal(["null"], Read(allowed, new ParquetReadOptions { Columns = ["y"] }).Skip(1));
        }
    }

    // ---- united by name -------------------------------------------------------------------------------------------

    [Fact]
    public void UnionByName_UnitesColumnsInFirstAppearanceOrder_WithTheNarrowestCommonTypes()
    {
        Put("d/a.parquet", ("x", Ints(1)));
        Put("d/b.parquet", ("y", Strings("b")), ("x", Longs(2)));
        Put("d/c.parquet", ("z", Doubles(3.5)), ("y", Strings("c")));
        var counting = new CountingStore(_store);
        using ParquetDataset dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = counting, SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.Equal(3, counting.Opened); // every footer, when the dataset opens
        Assert.Equal(["x", "y", "z"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<Int64Type>(dataset.Schema.FieldsList[0].DataType);
        Assert.Equal(["1|null|null", "2|b|null", "null|c|3.5"], Read(dataset));
        Assert.Equal(["null", "null", "3.5"], Read(dataset, new ParquetReadOptions { Columns = ["z"] }));
    }

    /// <summary>Another layout unites the files again: footers not kept (memory: none is) are read again.</summary>
    [Fact]
    public void UnionByName_OtherLayouts_ReadTheFootersAgain()
    {
        Put("d/a.parquet", ("x", Ints(1)), ("s", Strings("a")));
        Put("d/b.parquet", ("s", Strings("b")), ("x", Longs(2)));
        var counting = new CountingStore(_store);
        using ParquetDataset dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = counting, SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.Equal(2, counting.Opened);
        using var plain = dataset.OpenReader(new ParquetReadOptions { ViewTypes = false });
        Assert.Equal(4, counting.Opened);
        Assert.IsType<StringType>(plain.Schema.FieldsList[1].DataType);
        Assert.Equal(["1|a", "2|b"], Rows(plain.ReadBatches()));
    }

    [Fact]
    public void UnionByName_TypesWithoutACommonOne_AreNamed()
    {
        Put("d/a.parquet", ("x", Longs(1)));
        Put("d/b.parquet", ("x", Strings("b")));
        var error = Assert.Throws<ParquetFormatException>(() => Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }));
        Assert.Contains("d/b.parquet", error.Message);
        Assert.Contains("d/a.parquet", error.Message);
        Assert.Contains("no type holds both", error.Message);

        _store.Remove("d/b.parquet");
        Put("d/b.parquet", ("x", Doubles(2.5)));
        error = Assert.Throws<ParquetFormatException>(() => Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }));
        Assert.Contains("AllowLossyCasts", error.Message);
        using ParquetDataset lossy = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName, AllowLossyCasts = true });
        Assert.IsType<DoubleType>(lossy.Schema.FieldsList[0].DataType);
        Assert.Equal(["1", "2.5"], Read(lossy));
    }

    [Fact]
    public void UnionByName_UnitesStructFields()
    {
        Put("d/a.parquet", ("s", Struct([("a", Ints(1))])));
        Put("d/b.parquet", ("s", Struct([("b", Strings("q")), ("a", Longs(2))])));
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.Equal(["{a:1,b:null}", "{a:2,b:q}"], Read(dataset));
    }

    // ---- given schema ---------------------------------------------------------------------------------------------

    [Fact]
    public void AGivenSchema_ReadsNoFileToOpen_AndShapesTheBatches()
    {
        Put("d/a.parquet", ("x", Ints(1)), ("extra", Doubles(0)), ("y", Strings("a")));
        Put("d/b.parquet", ("y", Strings("b")), ("x", Longs(2)));
        var given = new Apache.Arrow.Schema([new Field("y", LargeStringType.Default, false), new Field("x", Int64Type.Default, false)], null);
        var counting = new CountingStore(_store);
        using ParquetDataset dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = counting, Schema = given });
        Assert.Equal(0, counting.Opened);
        Assert.Equal(["y", "x"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<StringViewType>(dataset.Schema.FieldsList[0].DataType); // the reader's layout
        Assert.All(dataset.Schema.FieldsList, f => Assert.True(f.IsNullable));
        Assert.Equal(["a|1", "b|2"], Read(dataset));
        using var plain = dataset.OpenReader(new ParquetReadOptions { ViewTypes = false });
        Assert.IsType<StringType>(plain.Schema.FieldsList[0].DataType);
        Assert.Equal(["a|1", "b|2"], Rows(plain.ReadBatches()));
    }

    [Fact]
    public void AGivenSchema_RefusesUnionByName_AndDuplicateNames()
    {
        var given = new Apache.Arrow.Schema([new Field("x", Int64Type.Default, true)], null);
        Assert.Throws<ArgumentException>(() => Open(new ParquetDatasetOptions { Schema = given, SchemaMode = DatasetSchemaMode.UnionByName }));
        var twice = new Apache.Arrow.Schema([new Field("x", Int64Type.Default, true), new Field("x", Int32Type.Default, true)], null);
        Assert.Throws<ArgumentException>(() => Open(new ParquetDatasetOptions { Schema = twice }));
    }

    [Fact]
    public void AGivenSchema_TypesThePartitionKeysItNames()
    {
        Put("h/year=2024/a.parquet", ("x", Ints(1)));
        Put("h/year=2025/a.parquet", ("x", Ints(2)));
        var given = new Apache.Arrow.Schema([new Field("year", Int16Type.Default, true), new Field("x", Int64Type.Default, true)], null);
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { Schema = given }, "h/");
        Assert.Equal(["year", "x"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<Int16Type>(dataset.Schema.FieldsList[0].DataType);
        Assert.Equal(["2024|1", "2025|2"], Read(dataset));
        Assert.Equal(["2025|2"], Read(dataset, new ParquetReadOptions { Filter = Compare(BinaryOp.Gt, "year", LiteralValue.NewInt16(2024)) }));
    }

    // ---- nested ---------------------------------------------------------------------------------------------------

    [Fact]
    public void StructFields_MatchByName_MissingOnesNamed_OrNull()
    {
        Put("d/a.parquet", ("s", Struct([("a", Longs(1)), ("b", Strings("x"))])));
        Put("d/b.parquet", ("s", Struct([("c", Doubles(1)), ("a", Ints(2))])));
        using (ParquetDataset dataset = Open())
        {
            var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
            Assert.Contains("d/b.parquet", error.Message);
            Assert.Contains("no field 's.b'", error.Message);
            Assert.Contains("AllowMissingColumns", error.Message);
        }
        using (ParquetDataset allowed = Open(new ParquetDatasetOptions { AllowMissingColumns = true }))
        {
            Assert.Equal(["{a:1,b:x}", "{a:2,b:null}"], Read(allowed));
            // Paths into the struct: a file lacking a field selected reads it as nulls.
            Assert.Equal(["{b:x}", "{b:null}"], Read(allowed, new ParquetReadOptions { ColumnPaths = [["s", "b"]] }));
            Assert.Equal(["{a:1}", "{a:2}"], Read(allowed, new ParquetReadOptions { ColumnPaths = [["s", "a"]] }));
        }
    }

    [Fact]
    public void ListElements_Widen()
    {
        var narrow = new ListArray.Builder(Int32Type.Default);
        narrow.Append();
        ((Int32Array.Builder)narrow.ValueBuilder).Append(1).Append(2);
        narrow.AppendNull();
        var wide = new ListArray.Builder(Int64Type.Default);
        wide.Append();
        ((Int64Array.Builder)wide.ValueBuilder).Append(3);
        Put("d/a.parquet", ("l", wide.Build()));
        Put("d/b.parquet", ("l", narrow.Build()));
        using ParquetDataset dataset = Open();
        Assert.Equal(["[3]", "[1,2]", "null"], Read(dataset));
    }

    [Fact]
    public void AGivenSchema_ReadsDictionaries_AsAReaderHoldsThem()
    {
        var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        Put("d/a.parquet", ("x", new DictionaryArray(dictionaryType, Ints(1, 0), Strings("p", "q"))));
        var given = new Apache.Arrow.Schema([new Field("x", new DictionaryType(Int32Type.Default, LargeStringType.Default, false), true)], null);
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { Schema = given });
        Assert.Equal(["q", "p"], Read(dataset));
    }

    [Fact]
    public void Dictionaries_AndPlainStrings_ReadIntoEachOther()
    {
        var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        Put("d/a.parquet", ("x", new DictionaryArray(dictionaryType, Ints(1, 0, null), Strings("p", "q"))));
        Put("d/b.parquet", ("x", Strings("r", null)));
        using (ParquetDataset dataset = Open())
        {
            Assert.IsType<DictionaryType>(dataset.Schema.FieldsList[0].DataType);
            Assert.Equal(["q", "p", "null", "r", "null"], Read(dataset));
        }
        using (ParquetDataset united = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }))
        {
            Assert.IsType<StringViewType>(united.Schema.FieldsList[0].DataType);
            Assert.Equal(["q", "p", "null", "r", "null"], Read(united));
        }
    }

    // ---- filters --------------------------------------------------------------------------------------------------

    [Fact]
    public void Filters_PruneWidenedFiles_OnTheirOwnTypes()
    {
        Put("d/a.parquet", ("x", Longs(1, 2)));
        Put("d/b.parquet", rowGroupRows: 2, ("x", Ints(10, 11, 200, 201)));
        using ParquetDataset dataset = Open();
        using var reader = dataset.OpenReader(new ParquetReadOptions { Filter = Compare(BinaryOp.GtEq, "x", LiteralValue.NewInt64(100)), FilterRows = true });
        Assert.True(reader.FiltersExactly);
        Assert.Equal(["200", "201"], Rows(reader.ReadBatches()));
        Assert.Equal(0, reader.FilesFilteredAfter);
        // A literal beyond the narrower type.
        using var beyond = dataset.OpenReader(new ParquetReadOptions { Filter = Compare(BinaryOp.Lt, "x", LiteralValue.NewInt64(1L << 40)), FilterRows = true });
        Assert.Equal(6, Rows(beyond.ReadBatches()).Count);
    }

    [Fact]
    public void EqualityFilters_OverWidenedFiles_UseTheirStatisticsAndBloomFilters_WithoutLosingRows()
    {
        // Bloom filters hash the file's values in the file's type: a wider literal must still find them.
        var bloom = new ParquetWriteOptions { Columns = new Dictionary<string, ParquetColumnOptions> { ["x"] = new() { BloomFilter = new ParquetBloomFilterOptions() } } };
        Put("d/a.parquet", ("x", Longs(1)), ("d", new Decimal128Array.Builder(new Decimal128Type(12, 3)).Append(9.999m).Build()));
        Put("d/b.parquet", bloom, ("x", Ints(10, 20, 30)), ("d", new Decimal128Array.Builder(new Decimal128Type(9, 2)).Append(1.25m).Append(2.5m).Append(3m).Build()));
        using ParquetDataset dataset = Open();
        foreach (bool rows in (bool[])[true, false])
        {
            List<string> Ids(ScalarExpr filter)
            {
                using var reader = dataset.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = rows, Columns = ["x"] });
                return Rows(reader.ReadBatches());
            }
            Assert.Contains("20", Ids(Compare(BinaryOp.Eq, "x", LiteralValue.NewInt64(20))));
            Assert.DoesNotContain("10", Ids(Compare(BinaryOp.Eq, "x", LiteralValue.NewInt64(15))).Where(_ => rows));
            Assert.Empty(Ids(Compare(BinaryOp.Eq, "x", LiteralValue.NewInt64(1L << 40))));
            Assert.Contains("20", Ids(ScalarExpr.NewInList(ScalarExpr.NewColumn("x"),
                InListSource.NewLiterals(ListModule.OfSeq([LiteralValue.NewInt64(20), LiteralValue.NewInt64(1L << 40)])), false, false)));
            // Decimals of another scale: 1.250 is the file's 1.25.
            Assert.Contains("10", Ids(Compare(BinaryOp.Eq, "d", LiteralValue.NewDecimal(1.250m))));
            Assert.Contains("20", Ids(Compare(BinaryOp.GtEq, "d", LiteralValue.NewDecimal(2.500m))));
        }
    }

    [Fact]
    public void Filters_OverAMissingColumn_SeeNulls()
    {
        Put("d/a.parquet", ("x", Longs(1, 2)), ("y", Longs(5, null)));
        Put("d/b.parquet", ("x", Longs(3)));
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { AllowMissingColumns = true });
        ScalarExpr isNull = ScalarExpr.NewUnary(UnaryOp.IsNull, ScalarExpr.NewColumn("y"));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = isNull, FilterRows = true, Columns = ["x"] }))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal(["2", "3"], Rows(reader.ReadBatches()));
            Assert.Equal(0, reader.FilesFilteredAfter);
        }
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = Compare(BinaryOp.Eq, "y", LiteralValue.NewInt64(5)), FilterRows = true }))
            Assert.Equal(["1|5"], Rows(reader.ReadBatches()));
        ScalarExpr either = ScalarExpr.NewBinary(BinaryOp.Or, Compare(BinaryOp.Eq, "y", LiteralValue.NewInt64(5)), Compare(BinaryOp.Eq, "x", LiteralValue.NewInt64(3)));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = either, FilterRows = true }))
            Assert.Equal(["1|5", "3|null"], Rows(reader.ReadBatches()));
        // Without the switch, a file lacking a column the filter reads is named.
        using ParquetDataset strict = Open();
        var error = Assert.Throws<ParquetFormatException>(() => Rows(strict.OpenReader(new ParquetReadOptions { Filter = isNull, Columns = ["x"] }).ReadBatches()));
        Assert.Contains("filters on", error.Message);
    }

    [Fact]
    public void Filters_OverLossilyConvertedColumns_AreAppliedAfterConverting()
    {
        // 2^53 + 1 converts to 2^53: the filter x = 2^53 keeps it, though the file's value differs.
        Put("d/a.parquet", ("x", Doubles(0.5)), ("s", Struct([("k", Ints(0))])));
        Put("d/b.parquet", ("x", Longs(7, 1L << 53, (1L << 53) + 1)), ("s", Struct([("k", Ints(3, 1, 2))])));
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { AllowLossyCasts = true });
        using var reader = dataset.OpenReader(new ParquetReadOptions { Filter = Compare(BinaryOp.Eq, "x", LiteralValue.NewFloat64(9007199254740992.0)), FilterRows = true });
        Assert.True(reader.FiltersExactly);
        Assert.Equal(["9007199254740992|{k:1}", "9007199254740992|{k:2}"], Rows(reader.ReadBatches()));
        Assert.Equal(1, reader.FilesFilteredAfter);
        // Limits count the rows the dataset kept; row numbers are the files'.
        using var limited = dataset.OpenReader(new ParquetReadOptions
        {
            Filter = Compare(BinaryOp.Eq, "x", LiteralValue.NewFloat64(9007199254740992.0)), FilterRows = true, MaxRows = 1, RowIndexColumn = "n", Columns = ["x"],
        });
        Assert.Equal(["2|9007199254740992"], Rows(limited.ReadBatches()));
        // The filter's column is read for the dataset's filtering even when not selected.
        using var other = dataset.OpenReader(new ParquetReadOptions
        {
            Filter = Compare(BinaryOp.Eq, "x", LiteralValue.NewFloat64(9007199254740992.0)), FilterRows = true, Columns = ["s"],
        });
        Assert.Equal(["{k:1}", "{k:2}"], Rows(other.ReadBatches()));
    }

    [Fact]
    public void Filters_OverNarrowedFloats_DoNotPruneOnTheFilesValues()
    {
        // The double 0.1 reads as the float 0.1f: x = 0.1f keeps it, though the file's value differs from 0.1f.
        Put("d/a.parquet", ("x", new FloatArray.Builder().Append(0.5f).Build()));
        Put("d/b.parquet", ("x", Doubles(0.1, 0.7)));
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { AllowLossyCasts = true });
        ScalarExpr filter = Compare(BinaryOp.Eq, "x", LiteralValue.NewFloat32(0.1f));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true }))
            Assert.Equal([((double)0.1f).ToString("R", CultureInfo.InvariantCulture)], Rows(reader.ReadBatches()));
        using (var pruning = dataset.OpenReader(new ParquetReadOptions { Filter = filter }))
            Assert.Contains(((double)0.1f).ToString("R", CultureInfo.InvariantCulture), Rows(pruning.ReadBatches()));
    }

    [Theory]
    [InlineData("float")]
    [InlineData("half")]
    public void Filters_OverNarrowerFloats_FilterTheWidenedValues(string narrow)
    {
        // 0.1 as a float is 0.100000001490116…, not the double 0.1: the filter compares the widened values.
        IArrowArray values = narrow == "float"
            ? new FloatArray.Builder().AppendRange([0.1f, 0.15f, 0.25f]).Build()
            : new HalfFloatArray.Builder().AppendRange([(Half)0.1, (Half)0.15, (Half)0.25]).Build();
        double[] widened = narrow == "float" ? [0.1f, 0.15f, 0.25f] : [(double)(Half)0.1, (double)(Half)0.15, (double)(Half)0.25];
        Put("d/a.parquet", ("x", Doubles(0.1)));
        Put("d/b.parquet", ("x", values));
        using ParquetDataset dataset = Open();
        (BinaryOp Op, double Literal, Func<double, double, bool> Keeps)[] filters =
        [
            (BinaryOp.Eq, 0.1, (v, l) => v == l), (BinaryOp.NotEq, 0.1, (v, l) => v != l), (BinaryOp.Gt, 0.1, (v, l) => v > l),
            (BinaryOp.Lt, 0.2, (v, l) => v < l), (BinaryOp.GtEq, widened[1], (v, l) => v >= l), (BinaryOp.LtEq, 0.15, (v, l) => v <= l),
        ];
        foreach (var (op, literal, keeps) in filters)
        {
            List<string> expected = [.. ((double[])[0.1, .. widened]).Where(v => keeps(v, literal)).Select(v => v.ToString("R", CultureInfo.InvariantCulture))];
            ScalarExpr filter = Compare(op, "x", LiteralValue.NewFloat64(literal));
            using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true }))
            {
                Assert.True(reader.FiltersExactly);
                Assert.Equal(expected, Rows(reader.ReadBatches()));
            }
            // Pruning alone keeps at least those rows.
            using var pruning = dataset.OpenReader(new ParquetReadOptions { Filter = filter });
            Assert.Superset(expected.ToHashSet(), Rows(pruning.ReadBatches()).ToHashSet());
        }
    }

    [Fact]
    public void Filters_OverAMissingColumn_ThatDoNotFold_KeepTheirRows()
    {
        Put("d/a.parquet", ("x", Longs(1, 2)), ("y", Longs(5, null)));
        Put("d/b.parquet", ("x", Longs(3)));
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { AllowMissingColumns = true });
        // coalesce(y, x) > 2: y is null in b, so x decides there.
        ScalarExpr coalesce = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewCoalesce(ListModule.OfSeq([ScalarExpr.NewColumn("y"), ScalarExpr.NewColumn("x")])),
            ScalarExpr.NewLiteral(LiteralValue.NewInt64(2)));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = coalesce }))
            Assert.Contains("3|null", Rows(reader.ReadBatches()));
        ScalarExpr sum = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewBinary(BinaryOp.Add, ScalarExpr.NewColumn("y"), ScalarExpr.NewColumn("x")),
            ScalarExpr.NewLiteral(LiteralValue.NewInt64(0)));
        // y + x > 0 folds to null where y is missing (arithmetic with a null is null): exact, b's row left out.
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = sum, FilterRows = true }))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal(["1|5"], Rows(reader.ReadBatches()));
        }
    }

    /// <summary>
    /// Arithmetic over a column a file stores narrower than the dataset is the dataset's: an Int32 file's
    /// int.MaxValue + int.MaxValue is positive in the Int64 dataset (it would wrap in Int32), a Float32 file's x * x is
    /// computed in Float64.
    /// </summary>
    [Fact]
    public void Arithmetic_OverWidenedColumns_IsTheDatasets()
    {
        Put("d/a.parquet", ("a", Longs(1, -5)), ("x", Doubles(0.1, 0.2)));
        Put("d/b.parquet", ("a", Ints(int.MaxValue, -1)), ("x", new FloatArray.Builder().Append(0.1f).Append(3f).Build()));
        using ParquetDataset dataset = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.IsType<Int64Type>(dataset.Schema.GetFieldByName("a").DataType);
        Assert.IsType<DoubleType>(dataset.Schema.GetFieldByName("x").DataType);
        ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);
        ScalarExpr sum = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewBinary(BinaryOp.Add, Col("a"), Col("a")), ScalarExpr.NewLiteral(LiteralValue.NewInt64(0)));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = sum, FilterRows = true, Columns = ["a"] }))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal(["1", "2147483647"], Rows(reader.ReadBatches()));
        }
        // (double)0.1f squared differs from 0.1f squared in Float32: the dataset's Float64 product is meant.
        double wide = (double)0.1f * (double)0.1f;
        ScalarExpr square = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewBinary(BinaryOp.Multiply, Col("x"), Col("x")), ScalarExpr.NewLiteral(LiteralValue.NewFloat64(wide)));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = square, FilterRows = true, Columns = ["a"] }))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal(["2147483647"], Rows(reader.ReadBatches()));
        }
    }

    /// <summary>
    /// Arithmetic over a column a file holds as a type that is not cast up (a struct field, an integer of a decimal
    /// dataset) is evaluated after conversion: no row the dataset's filter keeps is lost.
    /// </summary>
    [Fact]
    public void Arithmetic_OverColumnsNotCastUp_IsEvaluatedAfterConversion()
    {
        Put("d/a.parquet", ("s", Struct([("a", Longs(1, -5))])));
        Put("d/b.parquet", ("s", Struct([("a", Ints(int.MaxValue, -1))])));
        using (ParquetDataset dataset = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }))
        {
            ScalarExpr a = ScalarExpr.NewFunction(ScalarFunction.StructFieldByName,
                Microsoft.FSharp.Collections.ListModule.OfSeq([ScalarExpr.NewColumn("s"), ScalarExpr.NewLiteral(LiteralValue.NewString("a"))]), LogicalType.Unknown);
            ScalarExpr sum = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewBinary(BinaryOp.Add, a, a), ScalarExpr.NewLiteral(LiteralValue.NewInt64(0)));
            using var reader = dataset.OpenReader(new ParquetReadOptions { Filter = sum, FilterRows = true, RowIndexColumn = "n" });
            Assert.True(reader.FiltersExactly);
            Assert.Equal(["0", "2"], Rows(reader.ReadBatches()).Select(r => r.Split('|')[0]));
        }
        Put("e/a.parquet", ("x", new Decimal128Array.Builder(new Decimal128Type(20, 0)).Append(1m).Build()));
        Put("e/b.parquet", ("x", Ints(int.MaxValue)));
        using (ParquetDataset dataset = Open(new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }, "e/"))
        {
            Assert.IsType<Decimal128Type>(dataset.Schema.GetFieldByName("x").DataType);
            ScalarExpr x = ScalarExpr.NewColumn("x");
            ScalarExpr sum = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewBinary(BinaryOp.Add, x, x), ScalarExpr.NewLiteral(LiteralValue.NewInt64(0)));
            // Decimal arithmetic is the engine's: nothing exact here, yet no row may be lost.
            using var reader = dataset.OpenReader(new ParquetReadOptions { Filter = sum, FilterRows = true, RowIndexColumn = "n" });
            Assert.Equal(["0", "1"], Rows(reader.ReadBatches()).Select(r => r.Split('|')[0]));
        }
    }

    [Fact]
    public void Filters_OverDatesWidenedToTimestamps_StayExact()
    {
        var day = new DateTime(2024, 5, 6);
        Put("d/a.parquet", ("t", new TimestampArray.Builder(TimeUnit.Microsecond).Append(new DateTimeOffset(day.AddHours(3), TimeSpan.Zero)).Build()));
        Put("d/b.parquet", ("t", new Date32Array.Builder().Append(day).Append(day.AddDays(1)).Build()));
        using ParquetDataset dataset = Open();
        // A timestamp literal against a date column: the file's reader cannot decide it; the dataset filters after.
        ScalarExpr after = Compare(BinaryOp.Gt, "t", LiteralValue.NewDateTime(day.AddHours(1)));
        using var reader = dataset.OpenReader(new ParquetReadOptions { Filter = after, FilterRows = true, RowIndexColumn = "n" });
        Assert.True(reader.FiltersExactly);
        List<string> rows = Rows(reader.ReadBatches());
        Assert.Equal(["0", "2"], rows.Select(r => r.Split('|')[0]));
    }

    // ---- times ----------------------------------------------------------------------------------------------------

    [Fact]
    public void FinerUnits_FailOnValuesBeyondTheirRange_NamingTheFile()
    {
        Put("d/a.parquet", ("t", new TimestampArray.Builder(TimeUnit.Nanosecond).Append(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build()));
        Put("d/b.parquet", ("t", new TimestampArray.Builder(TimeUnit.Millisecond).Append(new DateTimeOffset(2300, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build()));
        using ParquetDataset dataset = Open();
        var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
        Assert.Contains("d/b.parquet", error.Message);
        Assert.Contains("beyond the range", error.Message);
    }

    // ---- helpers --------------------------------------------------------------------------------------------------

    /// <summary>A store counting the files opened.</summary>
    private sealed class CountingStore(IFileStore inner) : IFileStore
    {
        private int _opened;

        public int Opened => Volatile.Read(ref _opened);

        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _opened);
            return inner.OpenAsync(file, cancellationToken);
        }

        public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _opened);
            return inner.OpenAsync(path, cancellationToken);
        }
    }
}
