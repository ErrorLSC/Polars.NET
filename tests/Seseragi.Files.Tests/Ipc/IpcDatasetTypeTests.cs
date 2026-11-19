using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// Datasets of files whose types differ in layout and dictionary encoding (TestData/ipc/datasets, written by pyarrow
/// and Polars with write_datasets.py), against their rows as pyarrow reads each file: strings and lists in their
/// plain, large and view layouts, dictionaries of several index types beside each other and beside plain values,
/// Polars Categoricals and Enums, nested; both schema modes, dictionaries unified or not, one thread or many.
/// </summary>
public sealed class IpcDatasetTypeTests
{
    private static string Dir(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "ipc", "datasets", name);

    private static readonly Lazy<JsonDocument> Expected = new(() => JsonDocument.Parse(File.ReadAllBytes(Dir("expected.json"))));

    private static List<string> ExpectedRows(string name) => [.. Expected.Value.RootElement.GetProperty(name).EnumerateArray().Select(e => e.GetString()!)];

    private static readonly IpcReadOptions[] Ways = [new() { MaxDegreeOfParallelism = 1 }, new() { MaxDegreeOfParallelism = 16 }];

    /// <summary>The rows of every way of reading, after the read is over (dictionaries shared by batches outlive it).</summary>
    private static List<List<string>> ReadAll(IpcDataset dataset)
    {
        List<List<string>> all = [];
        foreach (IpcReadOptions way in Ways)
        {
            List<RecordBatch> batches;
            using (IpcDatasetReader reader = dataset.OpenReader(way)) batches = [.. reader.ReadBatches()];
            all.Add(ArrowRender.Rows(batches));
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
        return all;
    }

    public static TheoryData<string, DatasetSchemaMode, bool, bool> Cases()
    {
        TheoryData<string, DatasetSchemaMode, bool, bool> cases = [];
        foreach (string name in (string[])["strings", "lists", "dicts", "dictplain", "nested", "enum_same"])
        {
            foreach (DatasetSchemaMode mode in (DatasetSchemaMode[])[DatasetSchemaMode.FirstFile, DatasetSchemaMode.UnionByName])
            {
                foreach (bool unify in (bool[])[false, true])
                {
                    // The first file's narrower dictionary indices cannot take a later file's (unified ones can).
                    bool refused = mode == DatasetSchemaMode.FirstFile && !unify && name is "dicts" or "nested";
                    cases.Add(name, mode, unify, refused);
                }
            }
        }
        return cases;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void MixedTypes_ReadAsPyarrowReadsEachFile(string name, DatasetSchemaMode mode, bool unify, bool refused)
    {
        var options = new IpcDatasetOptions { SchemaMode = mode, UnifyDictionaries = unify };
        if (refused)
        {
            Exception error = Assert.ThrowsAny<Exception>(() => ReadAll(IpcDataset.Open(Dir(name), options)));
            Assert.Contains("cannot be read as", error.Message);
            return;
        }
        using var dataset = IpcDataset.Open(Dir(name), options);
        foreach (List<string> rows in ReadAll(dataset)) Assert.Equal(ExpectedRows(name), rows);
        if (unify)
        {
            Assert.All(Dictionaries(dataset.Schema.FieldsList[0].DataType), d => Assert.IsType<Int32Type>(d.IndexType));
        }
    }

    private static IEnumerable<DictionaryType> Dictionaries(IArrowType type) => type switch
    {
        DictionaryType d => [d],
        StructType s => s.Fields.SelectMany(f => Dictionaries(f.DataType)),
        _ => [],
    };

    [Fact]
    public void UnitedTypes_KeepTheFirstFilesLayout_AndWidenDictionaryIndices()
    {
        var union = new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName };
        using (var strings = IpcDataset.Open(Dir("strings"), union)) Assert.IsType<StringType>(strings.Schema.FieldsList[0].DataType);
        using (var lists = IpcDataset.Open(Dir("lists"), union))
        {
            var list = Assert.IsType<ListType>(lists.Schema.FieldsList[0].DataType);
            Assert.IsType<Int64Type>(list.ValueDataType);
        }
        using (var dicts = IpcDataset.Open(Dir("dicts"), union))
        {
            var d = Assert.IsType<DictionaryType>(dicts.Schema.FieldsList[0].DataType);
            Assert.IsType<Int64Type>(d.IndexType); // Int8, Int32 and UInt32: Int64 holds them all
            Assert.IsType<StringType>(d.ValueType);
        }
        using (var plain = IpcDataset.Open(Dir("dictplain"), union)) Assert.IsType<StringType>(plain.Schema.FieldsList[0].DataType);
        using (var nested = IpcDataset.Open(Dir("nested"), union))
        {
            var s = Assert.IsType<StructType>(nested.Schema.FieldsList[0].DataType);
            Assert.IsType<Int16Type>(Assert.IsType<DictionaryType>(s.Fields[0].DataType).IndexType);
            Assert.IsType<ListType>(s.Fields[1].DataType);
        }
        using (var enums = IpcDataset.Open(Dir("enum_same"), union))
            Assert.True(enums.Schema.FieldsList[0].Metadata?.ContainsKey("_PL_ENUM_VALUES2"));
    }

    [Theory]
    [InlineData("enum_diff", "an Enum of")]
    [InlineData("enum_plain", "no Enum")]
    public void Enums_OfOtherCategories_OrBesideOtherColumns_AreRefused(string name, string said)
    {
        foreach (DatasetSchemaMode mode in (DatasetSchemaMode[])[DatasetSchemaMode.FirstFile, DatasetSchemaMode.UnionByName])
        {
            Exception error = Assert.ThrowsAny<Exception>(() => ReadAll(IpcDataset.Open(Dir(name), new IpcDatasetOptions { SchemaMode = mode })));
            Assert.Contains("Enum", error.Message);
            Assert.Contains(said, error.Message);
            Assert.Contains("b.arrow", error.Message);
        }
    }

    /// <summary>
    /// Unified dictionaries: one value one index across files and batches, each batch's dictionary a prefix of the
    /// next's, the same whatever the parallelism, and still readable once the read is over.
    /// </summary>
    [Fact]
    public void UnifiedDictionaries_GiveEachValueOneIndex()
    {
        using var dataset = IpcDataset.Open(Dir("dicts"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName, UnifyDictionaries = true });
        List<List<string>>? previous = null;
        foreach (IpcReadOptions way in Ways)
        {
            List<RecordBatch> batches;
            using (IpcDatasetReader reader = dataset.OpenReader(way)) batches = [.. reader.ReadBatches()];
            List<List<string>> dictionaries = [];
            Dictionary<string, int> indexOf = [];
            foreach (RecordBatch batch in batches)
            {
                var column = (DictionaryArray)batch.Column(0);
                Assert.IsType<Int32Type>(((DictionaryType)column.Data.DataType).IndexType);
                List<string> entries = [.. Enumerable.Range(0, column.Dictionary.Length).Select(i => ArrowRender.Value(column.Dictionary, i))];
                if (dictionaries.Count > 0) Assert.Equal(dictionaries[^1], entries.Take(dictionaries[^1].Count));
                dictionaries.Add(entries);
                for (int i = 0; i < column.Length; i++)
                {
                    if (column.IsNull(i)) continue;
                    int index = ((Int32Array)column.Indices).GetValue(i)!.Value;
                    string value = entries[index];
                    Assert.Equal(index, indexOf.TryGetValue(value, out int known) ? known : indexOf[value] = index);
                }
            }
            Assert.Equal(dictionaries[^1].Count, dictionaries[^1].Distinct().Count());
            if (previous is not null) Assert.Equal(previous, dictionaries);
            previous = dictionaries;
            Assert.Equal(ExpectedRows("dicts"), ArrowRender.Rows(batches));
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }

    /// <summary>
    /// A given schema's dictionary index types are kept by unified dictionaries (at any depth, in Parquet too); more
    /// distinct values than they reach are an error naming the column; undeclared dictionaries are indexed by Int32.
    /// </summary>
    [Fact]
    public void UnifiedDictionaries_KeepAGivenSchemasIndexTypes()
    {
        var bytes = new DictionaryType(UInt8Type.Default, StringType.Default, false);
        using (var dataset = IpcDataset.Open(Dir("dicts"), new IpcDatasetOptions { Schema = new Schema([new Field("d", bytes, true)], null), UnifyDictionaries = true }))
        {
            Assert.IsType<UInt8Type>(((DictionaryType)dataset.Schema.FieldsList[0].DataType).IndexType);
            foreach (IpcReadOptions way in Ways)
            {
                List<RecordBatch> batches;
                using (IpcDatasetReader reader = dataset.OpenReader(way))
                {
                    Assert.IsType<UInt8Type>(((DictionaryType)reader.Schema.FieldsList[0].DataType).IndexType);
                    batches = [.. reader.ReadBatches()];
                }
                Dictionary<string, int> indexOf = [];
                foreach (RecordBatch batch in batches)
                {
                    var column = (DictionaryArray)batch.Column(0);
                    var indices = Assert.IsType<UInt8Array>(column.Indices);
                    for (int i = 0; i < column.Length; i++)
                    {
                        if (column.IsNull(i)) continue;
                        int index = indices.GetValue(i)!.Value;
                        string value = ArrowRender.Value(column.Dictionary, index);
                        Assert.Equal(index, indexOf.TryGetValue(value, out int known) ? known : indexOf[value] = index);
                    }
                }
                Assert.Equal(ExpectedRows("dicts"), ArrowRender.Rows(batches));
                foreach (RecordBatch batch in batches) batch.Dispose();
            }
        }

        // Nested: the struct's dictionary as declared (Int16); a dictionary the given schema leaves out of a list is Int32.
        var nested = new StructType([new Field("c", new DictionaryType(Int16Type.Default, StringType.Default, false), true),
            new Field("l", new ListType(new Field("item", StringType.Default, true)), true)]);
        using (var dataset = IpcDataset.Open(Dir("nested"), new IpcDatasetOptions { Schema = new Schema([new Field("s", nested, true)], null), UnifyDictionaries = true }))
        {
            List<string> rows = [];
            using (IpcDatasetReader reader = dataset.OpenReader())
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch)
                    {
                        var c = (DictionaryArray)((StructArray)batch.Column(0)).Fields[0];
                        Assert.IsType<Int16Array>(c.Indices);
                        rows.AddRange(ArrowRender.Rows(batch));
                    }
                }
            }
            Assert.Equal(ExpectedRows("nested"), rows);
        }

        // Parquet: a dictionary per row group, unified into the declared Int16 indices.
        var shorts = new Schema([new Field("d", new DictionaryType(Int16Type.Default, StringType.Default, false), true)], null);
        using (var parquet = ParquetDataset.Open(Dir("parquet_dicts"), new ParquetDatasetOptions { Schema = shorts, UnifyDictionaries = true }))
        {
            List<RecordBatch> batches;
            using (ParquetDatasetReader reader = parquet.OpenReader()) batches = [.. reader.ReadBatches()];
            Assert.All(batches, b => Assert.IsType<Int16Array>(((DictionaryArray)b.Column(0)).Indices));
            Assert.Equal(ExpectedRows("parquet_dicts"), ArrowRender.Rows(batches));
            foreach (RecordBatch batch in batches) batch.Dispose();
        }

        // 200 distinct values: UInt8 indices reach them, Int8 ones do not.
        string root = Directory.CreateTempSubdirectory("seseragi-ipc-dict-overflow-").FullName;
        try
        {
            for (int f = 0; f < 2; f++)
            {
                var values = new StringArray.Builder().AppendRange(Enumerable.Range(0, 100).Select(i => $"v{f * 100 + i}")).Build();
                var keys = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
                var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
                var schema = new Schema([new Field("d", type, true)], null);
                using IpcWriter writer = IpcWriter.Create(Path.Combine(root, $"f{f}.arrow"), schema);
                using (var batch = new RecordBatch(schema, [new DictionaryArray(type, keys, values)], 100)) writer.Write(batch);
                writer.Complete();
            }
            foreach (IpcReadOptions way in Ways)
            {
                var wide = new Schema([new Field("d", new DictionaryType(UInt8Type.Default, StringType.Default, false), true)], null);
                using var fits = IpcDataset.Open(root, new IpcDatasetOptions { Schema = wide, UnifyDictionaries = true });
                using (IpcDatasetReader reader = fits.OpenReader(way)) Assert.Equal(200, Drain(reader));
                var narrow = new Schema([new Field("d", new DictionaryType(Int8Type.Default, StringType.Default, false), true)], null);
                using var overflows = IpcDataset.Open(root, new IpcDatasetOptions { Schema = narrow, UnifyDictionaries = true });
                Exception error = Assert.ThrowsAny<Exception>(() =>
                {
                    using IpcDatasetReader reader = overflows.OpenReader(way);
                    Drain(reader);
                });
                Assert.Contains("'d'", error.Message);
                Assert.Contains("200 distinct", error.Message);
                Assert.Contains("wider index type", error.Message);
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }

        static int Drain(IpcDatasetReader reader)
        {
            int rows = 0;
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) rows += batch.Length;
            }
            return rows;
        }
    }

    /// <summary>Parquet datasets unify their dictionaries too: one per row group in the files, one for the dataset.</summary>
    [Fact]
    public void ParquetDictionaries_AreUnifiedAcrossRowGroupsAndFiles()
    {
        foreach (bool unify in (bool[])[false, true])
        {
            using var dataset = ParquetDataset.Open(Dir("parquet_dicts"), new ParquetDatasetOptions { UnifyDictionaries = unify });
            List<RecordBatch> batches;
            using (ParquetDatasetReader reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 4 })) batches = [.. reader.ReadBatches()];
            Assert.Equal(ExpectedRows("parquet_dicts"), ArrowRender.Rows(batches));
            // Unified: the last batch carries the dataset's dictionary (p, q, LONG, z), not its row group's own.
            if (unify) Assert.Equal(4, ((DictionaryArray)batches[^1].Column(0)).Dictionary.Length);
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }

    /// <summary>
    /// Enums nested in lists (two layouts, the Enum on the item field) unite and keep their categories; Enums of other
    /// categories in a struct field are refused, naming the field; Polars' Parquet Enums are compared too.
    /// </summary>
    [Fact]
    public void NestedEnums_AndParquetEnums_FollowTheSameRules()
    {
        using (var lists = IpcDataset.Open(Dir("enum_list"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }))
        {
            Assert.All(ReadAll(lists), rows => Assert.Equal(ExpectedRows("enum_list"), rows));
            var list = Assert.IsType<LargeListType>(lists.Schema.FieldsList[0].DataType);
            Assert.True(list.ValueField.Metadata?.ContainsKey("_PL_ENUM_VALUES2"));
        }
        Exception nested = Assert.ThrowsAny<Exception>(() => ReadAll(IpcDataset.Open(Dir("enum_struct"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName })));
        Assert.Contains("'s.e'", nested.Message);
        Exception parquet = Assert.ThrowsAny<Exception>(() =>
        {
            using var dataset = ParquetDataset.Open(Dir("parquet_enum_diff"), new ParquetDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
            using ParquetDatasetReader reader = dataset.OpenReader();
            foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
        });
        Assert.Contains("Enum", parquet.Message);
    }

    /// <summary>A given schema declaring an Enum column as plain values reads every file's Enum as its values (the explicit downgrade).</summary>
    [Fact]
    public void GivenPlainColumns_DowngradeEnums()
    {
        var plain = new Schema([new Field("e", StringViewType.Default, true)], null);
        foreach (var (name, expected) in (ValueTuple<string, string[]>[])[("enum_diff", ["\"lo\"", "\"mid\""]), ("enum_plain", ["\"lo\"", "\"lo\""])])
        {
            using var dataset = IpcDataset.Open(Dir(name), new IpcDatasetOptions { Schema = plain });
            Assert.All(ReadAll(dataset), rows => Assert.Equal(expected, rows));
        }
        using var parquet = ParquetDataset.Open(Dir("parquet_enum_diff"), new ParquetDatasetOptions { Schema = plain });
        using ParquetDatasetReader reader = parquet.OpenReader();
        Assert.Equal(["\"lo\"", "\"mid\""], ArrowRender.Rows(reader.ReadBatches()));
    }

    /// <summary>
    /// Unified dictionaries on every way of reading: Parquet on one thread and many, synchronously and not, IPC
    /// asynchronously, a row limit cutting a batch, and a reader read twice (the same indices both times).
    /// </summary>
    [Fact]
    public async Task UnifiedDictionaries_OnEveryWayOfReading()
    {
        using var parquet = ParquetDataset.Open(Dir("parquet_dicts"), new ParquetDatasetOptions { UnifyDictionaries = true });
        List<string>? first = null;
        foreach (int dop in (int[])[1, 4])
        {
            using ParquetDatasetReader reader = parquet.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = dop });
            foreach (bool async in (bool[])[false, true])
            {
                List<RecordBatch> batches = [];
                if (async) await foreach (RecordBatch batch in reader.ReadBatchesAsync()) batches.Add(batch);
                else batches.AddRange(reader.ReadBatches());
                List<string> indices = [.. batches.SelectMany(b => Indices((DictionaryArray)b.Column(0)))];
                Assert.Equal(ExpectedRows("parquet_dicts"), ArrowRender.Rows(batches));
                first ??= indices;
                Assert.Equal(first, indices); // the same numbering on every reading
                foreach (RecordBatch batch in batches) batch.Dispose();
            }
            using ParquetDatasetReader limited = parquet.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = dop, MaxRows = 3 });
            List<RecordBatch> cut = [.. limited.ReadBatches()];
            Assert.Equal(ExpectedRows("parquet_dicts").Take(3), ArrowRender.Rows(cut));
            Assert.Equal(first!.Take(3), cut.SelectMany(b => Indices((DictionaryArray)b.Column(0))));
            foreach (RecordBatch batch in cut) batch.Dispose();
        }
        using var ipc = IpcDataset.Open(Dir("dicts"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName, UnifyDictionaries = true });
        foreach (IpcReadOptions way in Ways)
        {
            using IpcDatasetReader reader = ipc.OpenReader(way with { MaxRows = 6 });
            List<RecordBatch> batches = [];
            await foreach (RecordBatch batch in reader.ReadBatchesAsync()) batches.Add(batch);
            Assert.Equal(ExpectedRows("dicts").Take(6), ArrowRender.Rows(batches));
            foreach (RecordBatch batch in batches) batch.Dispose();
        }

        static IEnumerable<string> Indices(DictionaryArray column) =>
            Enumerable.Range(0, column.Length).Select(i => column.IsNull(i) ? "null" : ((Int32Array)column.Indices).GetValue(i)!.Value.ToString());
    }
}
