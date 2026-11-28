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
using Microsoft.FSharp.Collections;

namespace Seseragi.Files.Tests;

/// <summary>
/// Defaults of missing columns and struct fields (<see cref="DatasetOptions.Defaults"/>, <see cref="DatasetFile.Defaults"/>),
/// as Iceberg's <c>initial-default</c> reads them: only where a file lacks the field (its own nulls stay), at any depth
/// (struct fields, list elements' fields, map values' fields), structs as Iceberg's <c>{}</c> (their fields' own
/// defaults), every value type, filters folding the default (a file ruled out by its default not read), a file's own
/// defaults over the dataset's (files of one shape still read alike), field-id matching, CSV and IPC; the paths and
/// values checked, JSON refused.
/// </summary>
public sealed class DatasetDefaultTests
{
    private readonly MemoryFileStore _store = new();

    private static Field F(string name, IArrowType type) => new(name, type, true);

    private static Apache.Arrow.Schema Schema(params Field[] fields) => new(fields, null);

    private void Put(string path, Field[] fields, IArrowArray[] columns)
    {
        var schema = Schema(fields);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema))
        {
            writer.Write(new RecordBatch(schema, columns, columns[0].Length));
            writer.Complete();
        }
        _store.Add(path, stream.ToArray());
    }

    private ParquetDataset Open(Apache.Arrow.Schema schema, IReadOnlyList<ColumnDefault>? defaults, string path = "d/*.parquet", bool allowMissing = false) =>
        ParquetDataset.Open(path, new ParquetDatasetOptions { Store = _store, Schema = schema, Defaults = defaults, AllowMissingColumns = allowMissing });

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) rows.AddRange(ArrowRender.Rows(batch));
        }
        return rows;
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (int? v in values) b.Append(v);
        return b.Build();
    }

    private static StringArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? v in values) b.Append(v);
        return b.Build();
    }

    private static StructArray Struct(StructType type, int length, params IArrowArray[] fields) => new(type, length, fields, ArrowBuffer.Empty, 0);

    private static ScalarExpr Compare(BinaryOp op, string column, LiteralValue value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(value));

    [Fact]
    public void AMissingColumn_ReadsItsDefault_AFilesOwnNullsStayNull()
    {
        Put("d/a.parquet", [F("id", Int32Type.Default), F("n", Int32Type.Default)], [Ints(1, 2), Ints(null, 5)]);
        Put("d/b.parquet", [F("id", Int32Type.Default)], [Ints(3)]);
        var schema = Schema(F("id", Int32Type.Default), F("n", Int32Type.Default), F("added", Int32Type.Default));
        // A default lets the column be missing without AllowMissingColumns.
        using ParquetDataset dataset = Open(schema, [new("n", 7), new("added", 42)]);
        Assert.Equal(["1 | null | 42", "2 | 5 | 42", "3 | 7 | 42"], Read(dataset));
        // A null default only declares the column may be missing.
        using ParquetDataset nulls = Open(schema, [new("n", 7), new("added", null)]);
        Assert.Equal(["1 | null | null", "2 | 5 | null", "3 | 7 | null"], Read(nulls));
        // A missing column without one is still an error, which names both ways out.
        using ParquetDataset strict = Open(schema, [new("n", 7)]);
        var e = Assert.Throws<ParquetFormatException>(() => Read(strict));
        Assert.Contains("d/a.parquet", e.Message);
        Assert.Contains("'added'", e.Message);
        Assert.Contains("AllowMissingColumns", e.Message);
        Assert.Contains("Defaults", e.Message);
    }

    [Fact]
    public void EveryValueType_IsReadExactly()
    {
        Put("d/a.parquet", [F("id", Int32Type.Default)], [Ints(1)]);
        var dictionary = new DictionaryType(Int32Type.Default, StringType.Default, false);
        Field[] fields =
        [
            F("id", Int32Type.Default),
            F("b", BooleanType.Default), F("i8", Int8Type.Default), F("u64", UInt64Type.Default), F("f", FloatType.Default), F("d", DoubleType.Default),
            F("dec", new Decimal128Type(38, 10)), F("dec256", new Decimal256Type(40, 2)), F("sci", new Decimal128Type(22, 0)),
            F("day", Date32Type.Default), F("clock", new Time64Type(TimeUnit.Nanosecond)), F("wall", new TimestampType(TimeUnit.Microsecond, (string?)null)),
            F("instant", new TimestampType(TimeUnit.Microsecond, "UTC")), F("wait", DurationType.Millisecond),
            F("s", StringType.Default), F("long", StringType.Default), F("bin", BinaryType.Default), F("uuid", new FixedSizeBinaryType(16)),
            F("cat", dictionary),
        ];
        Guid uuid = Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7");
        ColumnDefault[] defaults =
        [
            new("b", true), new("i8", (sbyte)-5), new("u64", ulong.MaxValue), new("f", 1.5f), new("d", "2.25"),
            new("dec", "12345678901234567890.0123456789"), new("dec256", 14.2m), new("sci", "2E+20"),
            new("day", new DateOnly(2024, 2, 29)), new("clock", "12:34:56.123456789"), new("wall", "2017-11-16T22:31:08.123456"),
            new("instant", new DateTimeOffset(2017, 11, 16, 23, 31, 8, TimeSpan.FromHours(1))), new("wait", TimeSpan.FromSeconds(1.5)),
            new("s", "hé"), new("long", "a string longer than twelve bytes"), new("bin", (byte[])[0, 1, 0xFF]), new("uuid", uuid),
            new("cat", "x"),
        ];
        // Strings as views (the default) and as plain strings.
        foreach (bool views in (bool[])[true, false])
        {
            using ParquetDataset dataset = Open(Schema(fields), defaults);
            Assert.Equal(
            [
                "1 | true | -5 | 18446744073709551615 | 1.5 | 2.25 | 12345678901234567890.0123456789 | 14.20 | 200000000000000000000 | 19782 | 45296123456789 "
                + "| 1510871468123456 | 1510871468000000 | 1500 | \"hé\" | \"a string longer than twelve bytes\" | 0x0001FF | 0xF79C3E09677C4BBDA4793F349CB785E7 | \"x\"",
            ], Read(dataset, new ParquetReadOptions { ViewTypes = views }));
        }
    }

    [Fact]
    public void StructDefaults_FollowIceberg()
    {
        // point { x, y } with x and y defaulting to 0: as the spec's table of a struct whose default is null or {}.
        var full = new StructType([F("x", Int32Type.Default), F("y", Int32Type.Default)]);
        var onlyX = new StructType([F("x", Int32Type.Default)]);
        Put("d/a.parquet", [F("id", Int32Type.Default), F("point", onlyX)], [Ints(1), Struct(onlyX, 1, Ints(3))]);
        Put("d/b.parquet", [F("id", Int32Type.Default)], [Ints(2)]);
        var schema = Schema(F("id", Int32Type.Default), F("point", full));
        using ParquetDataset nullDefault = Open(schema, [new("point", null), new(["point", "x"], 0), new(["point", "y"], 0)]);
        Assert.Equal(["1 | {x: 3, y: 0}", "2 | null"], Read(nullDefault));
        using ParquetDataset structDefault = Open(schema, [new("point", ColumnDefault.Struct), new(["point", "x"], 0), new(["point", "y"], 0)]);
        Assert.Equal(["1 | {x: 3, y: 0}", "2 | {x: 0, y: 0}"], Read(structDefault));
        // A field without a default inside a struct that has one: null.
        using ParquetDataset partial = Open(schema, [new("point", ColumnDefault.Struct), new(["point", "y"], 9)], allowMissing: false);
        Assert.Equal(["1 | {x: 3, y: 9}", "2 | {x: null, y: 9}"], Read(partial));
        // Fields selected: the struct's default pruned alike.
        Assert.Equal(["{y: 0}", "{y: 0}"], Read(structDefault, new ParquetReadOptions { ColumnPaths = [["point", "y"]] }));
        // A struct field missing without a default is an error (without AllowMissingColumns).
        using ParquetDataset strict = Open(schema, [new(["point", "x"], 0)]);
        var e = Assert.Throws<ParquetFormatException>(() => Read(strict));
        Assert.Contains("y", e.Message);
    }

    [Fact]
    public void FieldsOfListElementsAndMapValues_TakeTheirDefaults()
    {
        var oldItem = new StructType([F("a", Int32Type.Default)]);
        var newItem = new StructType([F("a", Int32Type.Default), F("b", StringType.Default)]);
        var oldMap = new MapType(new Field("key", StringType.Default, false), F("value", oldItem));
        var newMap = new MapType(new Field("key", StringType.Default, false), F("value", newItem));
        var items = new ListArray(new ListType(F("element", oldItem)), 1, new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(),
            Struct(oldItem, 2, Ints(1, 2)), ArrowBuffer.Empty, 0);
        var entries = new StructArray(oldMap.KeyValueType, 1, [Strings("k"), Struct(oldItem, 1, Ints(5))], ArrowBuffer.Empty, 0);
        var map = new MapArray(oldMap, 1, new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), entries, ArrowBuffer.Empty, 0);
        Put("d/a.parquet", [F("items", new ListType(F("element", oldItem))), F("m", oldMap)], [items, map]);
        var schema = Schema(F("items", new ListType(F("element", newItem))), F("m", newMap));
        using ParquetDataset dataset = Open(schema, [new(["items", "b"], "new"), new(["m", "value", "b"], "v")]);
        Assert.Equal(["[{a: 1, b: \"new\"}, {a: 2, b: \"new\"}] | {\"k\": {a: 5, b: \"v\"}}"], Read(dataset));
    }

    [Fact]
    public void Filters_ReadAMissingColumnAsItsDefault()
    {
        var point = new StructType([F("x", Int32Type.Default)]);
        var wall = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var times = new TimestampArray.Builder(wall).Append(new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero)).Append(new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build();
        Put("d/a.parquet", [F("id", Int32Type.Default), F("added", Int32Type.Default), F("point", point), F("at", wall)],
            [Ints(1, 2), Ints(42, 41), Struct(point, 2, Ints(1, 2)), times]);
        Put("d/b.parquet", [F("id", Int32Type.Default)], [Ints(3)]);
        var schema = Schema(F("id", Int32Type.Default), F("added", Int32Type.Default), F("point", point), F("at", wall));
        using ParquetDataset dataset = Open(schema, [new("added", 42), new("point", ColumnDefault.Struct), new(["point", "x"], 7),
            new("at", "2020-01-01T00:00:00")], allowMissing: true);
        ParquetReadOptions Where(ScalarExpr filter, bool rows) => new() { Columns = ["id"], Filter = filter, FilterRows = rows };
        // The file without the column is read when its default passes, left out (unread) when it does not.
        Assert.Equal(["1", "3"], Read(dataset, Where(Compare(BinaryOp.Eq, "added", LiteralValue.NewInt32(42)), rows: true)));
        Assert.Equal(["1", "2"], Read(dataset, Where(Compare(BinaryOp.Lt, "added", LiteralValue.NewInt32(42)), rows: false)));
        Assert.Equal(["2"], Read(dataset, Where(Compare(BinaryOp.Lt, "added", LiteralValue.NewInt32(42)), rows: true)));
        // A default of no literal form (a struct, a timestamp): its conjuncts filter the rows after the default is put in.
        ScalarExpr field = ScalarExpr.NewFunction(ScalarFunction.StructFieldByName,
            ListModule.OfSeq<ScalarExpr>([ScalarExpr.NewColumn("point"), ScalarExpr.NewLiteral(LiteralValue.NewString("x"))]), LogicalType.Unknown);
        var x = ScalarExpr.NewBinary(BinaryOp.Eq, field, ScalarExpr.NewLiteral(LiteralValue.NewInt32(7)));
        Assert.Equal(["3"], Read(dataset, Where(x, rows: true)));
        var at = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("at"), ScalarExpr.NewLiteral(LiteralValue.NewDateTime(new DateTime(2020, 1, 1))));
        Assert.Equal(["3"], Read(dataset, Where(at, rows: true)));
    }

    [Fact]
    public void AFilesOwnDefaults_ComeFirst_FilesOfOneShapeStillReadAlike()
    {
        Put("d/a.parquet", [F("id", Int32Type.Default)], [Ints(1)]);
        Put("d/b.parquet", [F("id", Int32Type.Default)], [Ints(2)]);
        Put("d/c.parquet", [F("id", Int32Type.Default), F("region", StringType.Default)], [Ints(3), Strings("jp")]);
        Put("d/e.parquet", [F("id", Int32Type.Default)], [Ints(4)]);
        var schema = Schema(F("id", Int32Type.Default), F("region", StringType.Default), F("extra", Int32Type.Default));
        ParquetDatasetFile[] files =
        [
            new("d/a.parquet", Defaults: [new("region", "eu"), new("extra", 1)]),   // its own, first of its shape
            new("d/b.parquet"),                                                     // the same shape: the dataset's
            new("d/c.parquet", Defaults: [new("region", "eu")]),                    // has the column: its own values
            new("d/e.parquet", Defaults: [new("extra", 5)]),                        // its own lets "extra" be missing
        ];
        using var dataset = ParquetDataset.Open(files, new ParquetDatasetOptions { Store = _store, Schema = schema, Defaults = [new("region", "us"), new("extra", null)] });
        Assert.Equal(["1 | \"eu\" | 1", "2 | \"us\" | null", "3 | \"jp\" | null", "4 | \"us\" | 5"], Read(dataset));
        // Without the dataset's default for "extra", a file lacking it without its own is an error.
        using var strict = ParquetDataset.Open(files, new ParquetDatasetOptions { Store = _store, Schema = schema, Defaults = [new("region", "us")] });
        var e = Assert.Throws<ParquetFormatException>(() => Read(strict));
        Assert.Contains("d/b.parquet", e.Message);
        Assert.Contains("'extra'", e.Message);
    }

    [Fact]
    public void ByFieldId_AnIdAFileLacks_ReadsItsDefault()
    {
        static Field Id(string name, IArrowType type, int id) => ArrowFieldIds.With(F(name, type), id);
        Put("d/a.parquet", [Id("old_name", Int32Type.Default, 1)], [Ints(1)]);
        using var dataset = ParquetDataset.Open("d/*.parquet", new ParquetDatasetOptions
        {
            Store = _store,
            Schema = Schema(Id("key", Int32Type.Default, 1), Id("added", StringType.Default, 2)),
            FieldMatching = ParquetFieldMatching.ByFieldId,
            Defaults = [new("added", "x")],
        });
        Assert.Equal(["1 | \"x\""], Read(dataset));
    }

    [Fact]
    public void PathsAndValues_AreChecked()
    {
        Put("d/k=1/a.parquet", [F("id", Int32Type.Default)], [Ints(1)]);
        var schema = Schema(F("id", Int32Type.Default), F("s", new StructType([F("x", Int8Type.Default)])),
            F("l", new ListType(F("element", Int32Type.Default))), F("m", new MapType(new Field("key", StringType.Default, false), F("value", Int32Type.Default))),
            F("dec", new Decimal128Type(5, 2)), F("wall", new TimestampType(TimeUnit.Microsecond, (string?)null)), F("day", Date32Type.Default));
        // Refused when the dataset opens, before any read.
        string Refused(ColumnDefault[] defaults) => Assert.Throws<ArgumentException>(() =>
            ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = _store, Schema = schema, Defaults = defaults, AllowMissingColumns = true })).Message;
        Assert.Contains("no column 'nope'", Refused([new("nope", 1)]));
        Assert.Contains("'s' has no field 'y'", Refused([new(["s", "y"], 1)]));
        Assert.Contains("'key' and 'value', not 'v'", Refused([new(["m", "v"], 1)]));
        Assert.Contains("'id' is int32: it has no field 'x'", Refused([new(["id", "x"], 1)]));
        Assert.Contains("two defaults", Refused([new("id", 1), new(["id"], 2)]));
        Assert.Contains("partition column", Refused([new("k", 1)]));
        Assert.Contains("ColumnDefault.Struct", Refused([new("s", 1)]));
        Assert.Contains("not a value of int32", Refused([new("id", ColumnDefault.Struct)]));
        Assert.Contains("lists and maps take no default but null", Refused([new("l", 1)]));
        Assert.Contains("lists and maps take no default but null", Refused([new("m", 1)]));
        Assert.Contains("not a value of int8", Refused([new(["s", "x"], 300)]));
        Assert.Contains("not a value of decimal128(5, 2)", Refused([new("dec", "1.234")]));
        Assert.Contains("not a value of decimal128(5, 2)", Refused([new("dec", 1000m)]));
        Assert.Contains("not a value of timestamp[us]", Refused([new("wall", "2020-01-01T00:00:00+01:00")]));
        Assert.Contains("not a value of timestamp[us]", Refused([new("wall", DateTimeOffset.UnixEpoch)]));
        Assert.Contains("not a value of date32", Refused([new("day", "yesterday")]));
        Assert.Contains("not a value of int32", Refused([new("id", "x")]));
        // Null defaults of lists and maps are fine.
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = _store, Schema = schema, Defaults = [new("l", null), new("m", null)], AllowMissingColumns = true });
        Assert.Single(Read(dataset));
        // A file's own defaults are checked alike.
        var e = Assert.Throws<ArgumentException>(() => ParquetDataset.Open([new ParquetDatasetFile("d/k=1/a.parquet", Defaults: [new("id", "x")])],
            new ParquetDatasetOptions { Store = _store, Schema = schema, AllowMissingColumns = true }));
        Assert.Contains("d/k=1/a.parquet", e.Message);
        // An empty path is refused when the options are.
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = _store, Defaults = [new([], 1)] }));
    }

    [Fact]
    public void ByFieldId_ANestedIdAFileLacks_ReadsItsDefault_AlsoThroughANameMapping()
    {
        static Field Id(string name, IArrowType type, int? id) => ArrowFieldIds.With(F(name, type), id);
        // The file's struct (id 2) is renamed in the schema and gained a field (id 4) the file lacks.
        var stored = new StructType([Id("a", Int32Type.Default, 3)]);
        Put("d/a.parquet", [Id("k", Int32Type.Default, 1), Id("s", stored, 2)], [Ints(1), Struct(stored, 1, Ints(5))]);
        var plain = new StructType([F("a", Int32Type.Default)]);
        Put("m/a.parquet", [F("k", Int32Type.Default), F("s", plain)], [Ints(2), Struct(plain, 1, Ints(6))]);
        var schema = Schema(Id("key", Int32Type.Default, 1), Id("st", new StructType([Id("A", Int32Type.Default, 3), Id("B", StringType.Default, 4)]), 2));
        ParquetDatasetOptions Options(NameMapping? mapping) => new()
        {
            Store = _store,
            Schema = schema,
            FieldMatching = ParquetFieldMatching.ByFieldId,
            NameMapping = mapping,
            Defaults = [new(["st", "B"], "b")],
        };
        using (var dataset = ParquetDataset.Open("d/*.parquet", Options(null)))
            Assert.Equal(["1 | {A: 5, B: \"b\"}"], Read(dataset));
        NameMapping mapping = NameMapping.Parse("""
            [ { "field-id": 1, "names": ["k"] }, { "field-id": 2, "names": ["s"], "fields": [ { "field-id": 3, "names": ["a"] } ] } ]
            """);
        using (var mapped = ParquetDataset.Open("m/*.parquet", Options(mapping)))
            Assert.Equal(["2 | {A: 6, B: \"b\"}"], Read(mapped));
    }

    [Fact]
    public void AWiderLiteral_AndUnifiedDictionaries_MeetADefault()
    {
        Put("d/a.parquet", [F("id", Int32Type.Default)], [Ints(1)]);
        var cat = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var schema = Schema(F("id", Int32Type.Default), F("added", Int32Type.Default), F("cat", cat));
        using var dataset = ParquetDataset.Open("d/*.parquet", new ParquetDatasetOptions
        {
            Store = _store, Schema = schema, UnifyDictionaries = true, Defaults = [new("added", 42), new("cat", "x")],
        });
        Assert.Equal(["1 | 42 | \"x\""], Read(dataset));
        Assert.Equal(["1"], Read(dataset, new ParquetReadOptions { Columns = ["id"], Filter = Compare(BinaryOp.Eq, "added", LiteralValue.NewInt64(42)), FilterRows = true }));
        Assert.Empty(Read(dataset, new ParquetReadOptions { Columns = ["id"], Filter = Compare(BinaryOp.Eq, "added", LiteralValue.NewInt64(41)), FilterRows = true }));
    }

    private sealed record CsvRow(long x);

    [Fact]
    public void CsvFiles_LackingAColumn_ReadItsDefault()
    {
        _store.Add("c/a.csv", "x,y\n1,a\n"u8.ToArray());
        _store.Add("c/b.csv", "x\n2\n"u8.ToArray());
        using var inferred = CsvDataset.Open("c/*.csv", new CsvDatasetOptions { Store = _store, Defaults = [new("y", "z")] });
        using (CsvDatasetReader reader = inferred.OpenReader())
            Assert.Equal(["1 | \"a\"", "2 | \"z\""], ArrowRender.Rows(reader.ReadBatches()));
        var schema = Schema(F("x", Int64Type.Default), F("y", StringType.Default), F("n", Int64Type.Default));
        using var given = CsvDataset.Open("c/*.csv", new CsvDatasetOptions { Store = _store, Schema = schema, Defaults = [new("y", "z"), new("n", 9L)] });
        using (CsvDatasetReader reader = given.OpenReader())
            Assert.Equal(["1 | \"a\" | 9", "2 | \"z\" | 9"], ArrowRender.Rows(reader.ReadBatches()));
        // Typed rows read the members' columns: a default of another column is passed over.
        Assert.Equal([new CsvRow(1), new CsvRow(2)], CsvDataset.ReadList<CsvRow>("c/*.csv", datasetOptions: new CsvDatasetOptions { Store = _store, Defaults = [new("y", "z")] }));
    }

    [Fact]
    public void IpcFiles_LackingAStructField_ReadItsDefault()
    {
        var old = new StructType([F("a", Int32Type.Default)]);
        var schema = Schema(F("s", old));
        using (var stream = new MemoryStream())
        {
            using (IpcWriter writer = IpcWriter.Create(stream, schema))
            {
                using var batch = new RecordBatch(schema, [Struct(old, 1, Ints(1))], 1);
                writer.Write(batch);
                writer.Complete();
            }
            _store.Add("i/a.arrow", stream.ToArray());
        }
        var wide = Schema(F("s", new StructType([F("a", Int32Type.Default), F("b", Int64Type.Default)])));
        using var dataset = IpcDataset.Open("i/*.arrow", new IpcDatasetOptions { Store = _store, Schema = wide, Defaults = [new(["s", "b"], 8L)] });
        using IpcDatasetReader reader = dataset.OpenReader();
        Assert.Equal(["{a: 1, b: 8}"], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void JsonDatasets_TakeNoDefaults()
    {
        _store.Add("j/a.json", "{\"x\": 1}\n"u8.ToArray());
        var e = Assert.Throws<ArgumentException>(() => JsonDataset.Open("j/*.json", new JsonDatasetOptions { Store = _store, Defaults = [new("y", 1)] }));
        Assert.Contains("defaults do not apply", e.Message);
        e = Assert.Throws<ArgumentException>(() => JsonDataset.Open([new JsonDatasetFile("j/a.json") { Defaults = [new("y", 1)] }], new JsonDatasetOptions { Store = _store }));
        Assert.Contains("defaults do not apply", e.Message);
    }
}
