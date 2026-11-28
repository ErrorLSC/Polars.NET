using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// Names matched ignoring case (<see cref="DatasetOptions.CaseInsensitiveNames"/>), as Spark (by default) and DuckDB
/// match them: columns and struct fields at any depth (in lists and maps too) read under the dataset's spelling (a
/// given schema's, the first file's, the first seen when united); filters and statistics through the dataset's names;
/// an exact name first, two names of a file matching one ignoring case refused, names differing in case alone in the
/// dataset refused; every format; off by default.
/// </summary>
public sealed class DatasetCaseInsensitiveTests
{
    private readonly MemoryFileStore _store = new();

    private static Field F(string name, IArrowType type) => new(name, type, true);

    private static Int64Array Longs(params long[] values) => new Int64Array.Builder().AppendRange(values).Build();

    private static StructArray Struct(StructType type, int length, params IArrowArray[] fields) => new(type, length, fields, ArrowBuffer.Empty, 0);

    private void Put(string path, Field[] fields, IArrowArray[] columns, int rowGroupRows = 1000)
    {
        var schema = new Apache.Arrow.Schema(fields, null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows }))
        {
            writer.Write(new RecordBatch(schema, columns, columns[0].Length));
            writer.Complete();
        }
        _store.Add(path, stream.ToArray());
    }

    /// <summary>"Id" and struct "S" { "Inner" } in one file, "id" and "s" { "inner" } in another.</summary>
    private void PutUpperAndLower(string dir = "d")
    {
        var upper = new StructType([F("Inner", Int64Type.Default)]);
        var lower = new StructType([F("inner", Int64Type.Default)]);
        Put($"{dir}/a.parquet", [F("Id", Int64Type.Default), F("S", upper)], [Longs(1, 2), Struct(upper, 2, Longs(10, 20))]);
        Put($"{dir}/b.parquet", [F("id", Int64Type.Default), F("s", lower)], [Longs(3), Struct(lower, 1, Longs(30))]);
    }

    private static Apache.Arrow.Schema Given => new([F("ID", Int64Type.Default), F("s", new StructType([F("INNER", Int64Type.Default)]))], null);

    private ParquetDataset Open(string path, Apache.Arrow.Schema? schema = null, bool ignoreCase = true, DatasetSchemaMode mode = DatasetSchemaMode.FirstFile,
        bool allowMissing = false) =>
        ParquetDataset.Open(path, new ParquetDatasetOptions
        {
            Store = _store, Schema = schema, CaseInsensitiveNames = ignoreCase, SchemaMode = mode, AllowMissingColumns = allowMissing,
        });

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        return ArrowRender.Rows(reader.ReadBatches());
    }

    private static ScalarExpr Compare(BinaryOp op, string column, long value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(LiteralValue.NewInt64(value)));

    [Fact]
    public void AGivenSchema_ReadsColumnsAndStructFieldsOfAnyCase_UnderItsNames()
    {
        PutUpperAndLower();
        using ParquetDataset dataset = Open("d/*.parquet", Given);
        Assert.Equal(["ID", "s"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(["1 | {INNER: 10}", "2 | {INNER: 20}", "3 | {INNER: 30}"], Read(dataset));
        // Filters, selections and statistics through the dataset's names.
        Assert.Equal(["2 | {INNER: 20}", "3 | {INNER: 30}"], Read(dataset, new ParquetReadOptions { Filter = Compare(BinaryOp.Gt, "ID", 1), FilterRows = true }));
        Assert.Equal(["3 | {INNER: 30}"], Read(dataset, new ParquetReadOptions { Filter = Compare(BinaryOp.Gt, "ID", 2) })); // the first file pruned by its statistics
        Assert.Equal(["{INNER: 10}", "{INNER: 20}", "{INNER: 30}"], Read(dataset, new ParquetReadOptions { ColumnPaths = [["s", "INNER"]] }));
        // Off by default: the names do not match.
        using ParquetDataset exact = Open("d/*.parquet", Given, ignoreCase: false, allowMissing: true);
        Assert.Equal(["null | null", "null | null", "null | {INNER: null}"], Read(exact));
    }

    [Fact]
    public void WithoutASchema_TheFirstFileSpells_OrTheFirstSeenWhenUnited()
    {
        PutUpperAndLower();
        using ParquetDataset first = Open("d/*.parquet");
        Assert.Equal(["Id", "S"], first.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(["1 | {Inner: 10}", "2 | {Inner: 20}", "3 | {Inner: 30}"], Read(first));
        var lower = new StructType([F("inner", Int64Type.Default), F("more", Int64Type.Default)]);
        Put("d/c.parquet", [F("ID", Int64Type.Default), F("s", lower), F("Extra", Int64Type.Default)], [Longs(4), Struct(lower, 1, Longs(40), Longs(41)), Longs(7)]);
        Put("d/e.parquet", [F("extra", Int64Type.Default)], [Longs(8)]);
        using ParquetDataset united = Open("d/*.parquet", mode: DatasetSchemaMode.UnionByName);
        Assert.Equal(["Id", "S", "Extra"], united.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(["1 | {Inner: 10, more: null} | null", "2 | {Inner: 20, more: null} | null", "3 | {Inner: 30, more: null} | null",
            "4 | {Inner: 40, more: 41} | 7", "null | null | 8"], Read(united));
    }

    [Fact]
    public void FieldsInListsAndMaps_MatchIgnoringCase()
    {
        var item = new StructType([F("a", Int64Type.Default)]);
        var mapType = new MapType(new Field("key", StringType.Default, false), F("value", item));
        var items = new ListArray(new ListType(F("element", item)), 1, new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(),
            Struct(item, 2, Longs(1, 2)), ArrowBuffer.Empty, 0);
        var entries = new StructArray(mapType.KeyValueType, 1, [new StringArray.Builder().Append("k").Build(), Struct(item, 1, Longs(5))], ArrowBuffer.Empty, 0);
        var map = new MapArray(mapType, 1, new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), entries, ArrowBuffer.Empty, 0);
        Put("d/a.parquet", [F("items", new ListType(F("element", item))), F("m", mapType)], [items, map]);
        var upperItem = new StructType([F("A", Int64Type.Default)]);
        var schema = new Apache.Arrow.Schema([F("Items", new ListType(F("element", upperItem))),
            F("M", new MapType(new Field("key", StringType.Default, false), F("value", upperItem)))], null);
        using ParquetDataset dataset = Open("d/*.parquet", schema);
        Assert.Equal(["[{A: 1}, {A: 2}] | {\"k\": {A: 5}}"], Read(dataset));
    }

    [Fact]
    public void AnExactNameComesFirst_TwoOthersMatchingOneAreRefused()
    {
        Put("d/a.parquet", [F("id", Int64Type.Default), F("ID", Int64Type.Default)], [Longs(4), Longs(5)]);
        using ParquetDataset exact = Open("d/*.parquet", new([F("ID", Int64Type.Default)], null));
        Assert.Equal(["5"], Read(exact));
        using ParquetDataset ambiguous = Open("d/*.parquet", new([F("Id", Int64Type.Default)], null));
        var e = Assert.Throws<ParquetFormatException>(() => Read(ambiguous));
        Assert.Contains("d/a.parquet", e.Message);
        Assert.Contains("'id' and 'ID'", e.Message);
        Assert.Contains("'Id'", e.Message);
        // The first file's columns become the dataset's: they may not differ in case alone.
        Assert.Throws<ArgumentException>(() => Open("d/*.parquet"));
    }

    [Fact]
    public void TheOptionsAreChecked()
    {
        PutUpperAndLower();
        Assert.Contains("'a' and 'A'", Assert.Throws<ArgumentException>(() => Open("d/*.parquet", new([F("a", Int64Type.Default), F("A", Int64Type.Default)], null))).Message);
        Assert.Throws<ArgumentException>(() => Open("d/*.parquet", new([F("s", new StructType([F("x", Int64Type.Default), F("X", Int64Type.Default)]))], null)));
        var e = Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/*.parquet", new ParquetDatasetOptions
        {
            Store = _store, Schema = Given, CaseInsensitiveNames = true, FieldMatching = ParquetFieldMatching.ByFieldId,
        }));
        Assert.Contains("CaseInsensitiveNames", e.Message);
    }

    [Fact]
    public void AHivePartitionKey_IsTheColumnItEqualsIgnoringCase()
    {
        Put("p/ID=7/a.parquet", [F("id", Int64Type.Default), F("v", Int64Type.Default)], [Longs(1), Longs(2)]);
        using ParquetDataset dataset = Open("p/");
        Assert.Equal(["id", "v"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<Int64Type>(dataset.Schema.GetFieldByName("id").DataType);
        Assert.Equal(["7 | 2"], Read(dataset)); // the path's value wins, as for an equal key
    }

    [Fact]
    public void IpcFiles_MatchIgnoringCase()
    {
        var deep = new StructType([F("x", Int64Type.Default)]);
        var lower = new StructType([F("inner", Int64Type.Default), F("deep", deep)]);
        var cat = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var schema = new Apache.Arrow.Schema([F("id", Int64Type.Default), F("s", lower), F("cat", cat)], null);
        var dictionary = new DictionaryArray(cat, new Int32Array.Builder().Append(0).Append(1).Build(), new StringArray.Builder().Append("x").Append("y").Build());
        using (var stream = new MemoryStream())
        {
            using (IpcWriter writer = IpcWriter.Create(stream, schema))
            {
                using var batch = new RecordBatch(schema, [Longs(1, 2), Struct(lower, 2, Longs(10, 20), Struct(deep, 2, Longs(7, 8))), dictionary], 2);
                writer.Write(batch);
                writer.Complete();
            }
            _store.Add("i/a.arrow", stream.ToArray());
        }
        var given = new Apache.Arrow.Schema([F("ID", Int64Type.Default),
            F("S", new StructType([F("INNER", Int64Type.Default), F("Deep", new StructType([F("X", Int64Type.Default)]))])), F("Cat", cat)], null);
        // A second file spelled otherwise: without a schema, the first file's names.
        var upperSchema = new Apache.Arrow.Schema([F("ID", Int64Type.Default), F("Extra", Int64Type.Default), F("iD2", Int64Type.Default)], null);
        using (var stream = new MemoryStream())
        {
            using (IpcWriter writer = IpcWriter.Create(stream, upperSchema))
            {
                using var batch = new RecordBatch(upperSchema, [Longs(3), Longs(9), Longs(0)], 1);
                writer.Write(batch);
                writer.Complete();
            }
            _store.Add("i/b.arrow", stream.ToArray());
        }
        using (var first = IpcDataset.Open("i/*.arrow", new IpcDatasetOptions { Store = _store, CaseInsensitiveNames = true, AllowMissingColumns = true }))
        {
            Assert.Equal(["id", "s", "cat"], first.Schema.FieldsList.Select(f => f.Name));
            using IpcDatasetReader all = first.OpenReader(new IpcReadOptions { Columns = ["id"] });
            Assert.Equal(["1", "2", "3"], ArrowRender.Rows(all.ReadBatches()));
        }
        _store.Remove("i/b.arrow");
        using var dataset = IpcDataset.Open("i/*.arrow", new IpcDatasetOptions { Store = _store, Schema = given, CaseInsensitiveNames = true });
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { Filter = Compare(BinaryOp.Gt, "ID", 1), FilterRows = true });
        Assert.Equal(["2 | {INNER: 20, Deep: {X: 8}} | \"y\""], ArrowRender.Rows(reader.ReadBatches()));

        // Two of a file's names matching one of the dataset's ignoring case.
        var twins = new Apache.Arrow.Schema([F("Id", Int64Type.Default), F("iD", Int64Type.Default)], null);
        using (var stream = new MemoryStream())
        {
            using (IpcWriter writer = IpcWriter.Create(stream, twins))
            {
                using var batch = new RecordBatch(twins, [Longs(1), Longs(2)], 1);
                writer.Write(batch);
                writer.Complete();
            }
            _store.Add("w/a.arrow", stream.ToArray());
        }
        using var twice = IpcDataset.Open("w/*.arrow", new IpcDatasetOptions { Store = _store, Schema = new([F("ID", Int64Type.Default)], null), CaseInsensitiveNames = true });
        using IpcDatasetReader failing = twice.OpenReader();
        var e = Assert.Throws<IpcFormatException>(() => ArrowRender.Rows(failing.ReadBatches()));
        Assert.Contains("w/a.arrow", e.Message);
        Assert.Contains("'Id' and 'iD'", e.Message);
    }

    [Fact]
    public void CsvFiles_MatchTheirHeadersIgnoringCase()
    {
        _store.Add("c/a.csv", "Id,Name\n1,a\n"u8.ToArray());
        _store.Add("c/b.csv", "NAME,id\nb,2\n"u8.ToArray());
        using var inferred = CsvDataset.Open("c/*.csv", new CsvDatasetOptions { Store = _store, CaseInsensitiveNames = true });
        Assert.Equal(["Id", "Name"], inferred.Schema.FieldsList.Select(f => f.Name));
        using (CsvDatasetReader reader = inferred.OpenReader())
            Assert.Equal(["1 | \"a\"", "2 | \"b\""], ArrowRender.Rows(reader.ReadBatches()));
        var schema = new Apache.Arrow.Schema([F("ID", Int64Type.Default), F("name", StringType.Default)], null);
        using var given = CsvDataset.Open("c/*.csv", new CsvDatasetOptions { Store = _store, Schema = schema, CaseInsensitiveNames = true });
        using (CsvDatasetReader reader = given.OpenReader(new CsvReadOptions { Filter = Compare(BinaryOp.Gt, "ID", 1), FilterRows = true }))
            Assert.Equal(["2 | \"b\""], ArrowRender.Rows(reader.ReadBatches()));
        // Off: the second file lacks the first file's columns.
        Assert.ThrowsAny<Exception>(() => CsvDataset.Open("c/*.csv", new CsvDatasetOptions { Store = _store }));
        // Two of a file's names matching one of the dataset's ignoring case.
        _store.Add("w/a.csv", "Id,iD\n1,2\n"u8.ToArray());
        using var twice = CsvDataset.Open("w/*.csv", new CsvDatasetOptions { Store = _store, Schema = schema, CaseInsensitiveNames = true, AllowMissingColumns = true });
        using (CsvDatasetReader reader = twice.OpenReader())
            Assert.Contains("'Id' and 'iD'", Assert.Throws<CsvFormatException>(() => ArrowRender.Rows(reader.ReadBatches())).Message);
        // A file whose header names differ in case alone, its columns the dataset's.
        _store.Add("t/a.csv", "x,X\n1,2\n"u8.ToArray());
        Assert.Throws<CsvFormatException>(() => CsvDataset.Open("t/*.csv", new CsvDatasetOptions { Store = _store, CaseInsensitiveNames = true }));
    }

    [Fact]
    public void JsonFiles_MatchTheirKeysIgnoringCase()
    {
        _store.Add("j/a.json", "{\"Id\": 1, \"S\": {\"Inner\": 10}}\n"u8.ToArray());
        _store.Add("j/b.json", "{\"id\": 2, \"s\": {\"inner\": 20}}\n"u8.ToArray());
        using var inferred = JsonDataset.Open("j/*.json", new JsonDatasetOptions { Store = _store, CaseInsensitiveNames = true, SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.Equal(["Id", "S"], inferred.Schema.FieldsList.Select(f => f.Name));
        using (JsonDatasetReader reader = inferred.OpenReader())
            Assert.Equal(["1 | {Inner: 10}", "2 | {Inner: 20}"], ArrowRender.Rows(reader.ReadBatches()));
        using var given = JsonDataset.Open("j/*.json", new JsonDatasetOptions { Store = _store, Schema = Given, CaseInsensitiveNames = true });
        using (JsonDatasetReader reader = given.OpenReader(new JsonReadOptions { Filter = Compare(BinaryOp.Gt, "ID", 1), FilterRows = true }))
            Assert.Equal(["2 | {INNER: 20}"], ArrowRender.Rows(reader.ReadBatches()));
        // A record naming a field twice ignoring case: its first is read (as of a repeated key); inferred, one column.
        _store.Add("k/a.json", "{\"Id\": 1, \"iD\": 2}\n"u8.ToArray());
        using var twice = JsonDataset.Open("k/*.json", new JsonDatasetOptions { Store = _store, CaseInsensitiveNames = true });
        Assert.Equal(["Id"], twice.Schema.FieldsList.Select(f => f.Name));
        using (JsonDatasetReader reader = twice.OpenReader())
            Assert.Equal(["1"], ArrowRender.Rows(reader.ReadBatches()));
    }
}
