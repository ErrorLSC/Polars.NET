using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Partitioned datasets: Hive keys read from paths as Polars reads them (when, types, values, collisions), partition
/// values given with files, the glob switch, and filters over partition columns deciding which files are read.
/// </summary>
public sealed class ParquetHiveTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-hive-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    /// <summary>Writes a file of an Int64 column <c>id</c> (<paramref name="rows"/> rows from <paramref name="first"/>), and more columns when given.</summary>
    private string Write(string relative, long first, int rows, params (Field Field, IArrowArray Array)[] more)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var ids = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) ids.Append(first + i);
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), .. more.Select(m => m.Field)], null);
        using var writer = ParquetWriter.Create(path, schema);
        writer.Write(new RecordBatch(schema, [ids.Build(), .. more.Select(m => m.Array)], rows));
        writer.Complete();
        return path;
    }

    private static string Render(IArrowArray array, int i) => array.IsNull(i) ? "null" : array switch
    {
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        DoubleArray a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        BooleanArray a => a.GetValue(i)!.Value ? "true" : "false",
        Date32Array a => a.GetDateOnly(i)!.Value.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
        TimestampArray a => a.GetTimestamp(i)!.Value.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture),
        Decimal128Array a => a.GetSqlDecimal(i)!.Value.ToString(),
        _ => CanonicalRender.Value(array, i),
    };

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                Assert.Equal(reader.Schema.FieldsList.Count, batch.ColumnCount);
                for (int i = 0; i < batch.Length; i++) rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => Render(batch.Column(c), i))));
            }
        }
        return rows;
    }

    private static string[] Names(Apache.Arrow.Schema schema) => [.. schema.FieldsList.Select(f => f.Name)];

    private static ArrowTypeId TypeOf(Apache.Arrow.Schema schema, string name) => schema.GetFieldByName(name).DataType.TypeId;

    [Fact]
    public void ADirectory_GivesItsKeysAsColumns_AfterTheFilesColumns()
    {
        Write("d/year=2024/month=02/p.parquet", 2, 1);
        Write("d/year=2023/month=12/p.parquet", 0, 2);
        Write("d/year=2024/month=01/p.parquet", 1, 1);
        using var dataset = ParquetDataset.Open(At("d"));
        Assert.Equal(["id", "year", "month"], Names(dataset.Schema));
        Assert.Equal(["year", "month"], Names(dataset.PartitionSchema!));
        Assert.Equal(ArrowTypeId.Int64, TypeOf(dataset.Schema, "month"));
        Assert.Equal(["0|2023|12", "1|2023|12", "1|2024|1", "2|2024|2"], Read(dataset));
        // The partition keys of the input directory itself are not columns.
        using var below = ParquetDataset.Open(At("d/year=2024"));
        Assert.Equal(["id", "month"], Names(below.Schema));
    }

    [Fact]
    public void Globs_AndFiles_HavePartitionsOnlyWhenAsked_FromTheirWholePath()
    {
        Write("g/k=a/p.parquet", 0, 1);
        Write("g/k=b/p.parquet", 1, 1);
        Assert.Null(ParquetDataset.Open(At("g/**/*.parquet")).PartitionSchema);
        var on = new ParquetDatasetOptions { Partitioning = HivePartitioning.On };
        Assert.Equal(["0|a", "1|b"], Read(ParquetDataset.Open(At("g/**/*.parquet"), on)));
        Assert.Equal(["1|b"], Read(ParquetDataset.Open(At("g/k=b/p.parquet"), on)));
        Assert.Equal(["0|a", "1|b"], Read(ParquetDataset.Open([At("g/k=a/p.parquet"), At("g/k=b/p.parquet")], on)));
        Assert.Null(ParquetDataset.Open(At("g"), new ParquetDatasetOptions { Partitioning = HivePartitioning.Off }).PartitionSchema);
    }

    [Fact]
    public void Values_AreDecoded_NullMarkersRead_StringsKeepTheirEmptyValue()
    {
        Write("s/s=x/p.parquet", 0, 1);
        Write("s/s=/p.parquet", 1, 1);
        Write("s/s=__HIVE_DEFAULT_PARTITION__/p.parquet", 2, 1);
        Write("s/s=a%2Fb%20c/p.parquet", 3, 1);
        Write("s/x=1=2/q.parquet", 4, 1); // not a key: two '='
        Write("i/k=1/p.parquet", 0, 1);
        Write("i/k=/p.parquet", 1, 1);
        Assert.Throws<InvalidOperationException>(() => ParquetDataset.Open(At("s")));
        File.Delete(At("s/x=1=2/q.parquet"));
        using var strings = ParquetDataset.Open(At("s"));
        Assert.Equal(["1|", "2|null", "3|a/b c", "0|x"], Read(strings));
        Assert.Equal(["1|null", "0|1"], Read(ParquetDataset.Open(At("i"))));
    }

    [Theory]
    [InlineData("1,1.5", "Double")]
    [InlineData("1,true", "String")]
    [InlineData("True,false", "Boolean")]
    [InlineData("inf,NaN,-1e3", "Double")]
    [InlineData("007,-5", "Int64")]
    [InlineData("+5", "String")]
    [InlineData("99999999999999999999", "Decimal128")]
    [InlineData("2024-01-15,2024-02-29", "Date32")]
    [InlineData("15-01-2024", "Date32")]
    [InlineData("2024-01-15 10%3A00%3A00,2024-01-15T10%3A00%3A00.5", "Timestamp")]
    [InlineData("2024-01-15T10%3A00%3A00Z", "Timestamp")]
    [InlineData("2024-01-15,2024-01-15 10%3A00", "String")]
    [InlineData("10%3A00%3A00", "String")]
    public void Types_AreInferredAsPolarsInfersThem(string values, string type)
    {
        string[] each = values.Split(',');
        for (int i = 0; i < each.Length; i++) Write($"t/k={each[i]}/p{i}.parquet", i, 1);
        using var dataset = ParquetDataset.Open(At("t"));
        IArrowType inferred = dataset.Schema.GetFieldByName("k").DataType;
        Assert.Equal(type, inferred is StringViewType ? "String" : inferred.TypeId.ToString());
        if (values.Contains('Z')) Assert.Equal("UTC", ((TimestampType)inferred).Timezone);
        Assert.Equal(each.Length, Read(dataset).Count);
    }

    [Fact]
    public void Dates_AreStrings_WhenNotParsed()
    {
        Write("n/d=2024-01-15/p.parquet", 0, 1);
        Write("n/t=x/q.parquet", 1, 1);
        File.Delete(At("n/t=x/q.parquet"));
        var options = new ParquetDatasetOptions { ParseHiveDates = false };
        using var dataset = ParquetDataset.Open(At("n"), options);
        Assert.Equal(ArrowTypeId.StringView, TypeOf(dataset.Schema, "d"));
        Assert.Equal(["0|2024-01-15"], Read(dataset));
        using var parsed = ParquetDataset.Open(At("n"));
        Assert.Equal(ArrowTypeId.Date32, TypeOf(parsed.Schema, "d"));
        Assert.Equal(["0|2024-01-15"], Read(parsed));
    }

    /// <summary>Directories that are no keys (two '=', a '*' in the value) are passed over.</summary>
    [Fact]
    public void PartsThatAreNoKeys_ArePassedOver()
    {
        Write("e/x=1=2/v=a*b/k=a/p.parquet", 0, 1);
        Write("e/x=1=2/v=a*b/k=b/p.parquet", 1, 1);
        using var dataset = ParquetDataset.Open(At("e"));
        Assert.Equal(["k"], Names(dataset.PartitionSchema!));
        Assert.Equal(["0|a", "1|b"], Read(dataset));
    }

    /// <summary>A key the files hold as a date column: its values are strings when dates are not parsed (Polars).</summary>
    [Fact]
    public void AKeyTheFilesHoldAsADate_IsAString_WhenDatesAreNotParsed()
    {
        var dates = new Date32Array.Builder().Append(new DateTime(2020, 1, 1)).Build();
        Write("dc/d=2024-01-01/p.parquet", 0, 1, (new Field("d", Date32Type.Default, true), dates));
        Assert.Equal(ArrowTypeId.Date32, TypeOf(ParquetDataset.Open(At("dc")).Schema, "d"));
        using var strings = ParquetDataset.Open(At("dc"), new ParquetDatasetOptions { ParseHiveDates = false });
        Assert.Equal(ArrowTypeId.StringView, TypeOf(strings.Schema, "d"));
        Assert.Equal(["0|2024-01-01"], Read(strings));
    }

    /// <summary>A key the files also hold as a column: the path's value, the file's type, in the file column's place.</summary>
    [Fact]
    public void AKeyTheFilesHold_TakesThePathsValue_InTheFilesPlace()
    {
        var names = new StringArray.Builder().Append("file").Build();
        var years = new Int32Array.Builder().Append(1999).Build();
        Write("c/year=2024/n=7/p.parquet", 0, 1, (new Field("year", Int32Type.Default, true), years), (new Field("n", StringType.Default, true), names));
        using var dataset = ParquetDataset.Open(At("c"));
        Assert.Equal(["id", "year", "n"], Names(dataset.Schema));
        Assert.Equal(ArrowTypeId.Int32, TypeOf(dataset.Schema, "year"));
        Assert.Equal(["0|2024|7"], Read(dataset));
        Assert.Equal(["7"], Read(dataset, new ParquetReadOptions { Columns = ["n"] }));
    }

    [Fact]
    public void APartitionSchema_SetsTheTypes()
    {
        Write("p/a=1/b=x/p.parquet", 0, 1);
        var schema = new Apache.Arrow.Schema([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true), new Field("c", Int8Type.Default, true)], null);
        using var dataset = ParquetDataset.Open(At("p"), new ParquetDatasetOptions { PartitionSchema = schema });
        Assert.Equal(ArrowTypeId.Int32, TypeOf(dataset.Schema, "a"));
        Assert.Equal(ArrowTypeId.String, TypeOf(dataset.Schema, "b")); // as given, whatever the layout
        Assert.Equal(["0|1|x"], Read(dataset));
        var partial = new Apache.Arrow.Schema([new Field("a", Int32Type.Default, true)], null);
        Assert.Throws<InvalidOperationException>(() => ParquetDataset.Open(At("p"), new ParquetDatasetOptions { PartitionSchema = partial }));
        Write("q/a=x/p.parquet", 0, 1);
        Assert.Throws<InvalidDataException>(() => ParquetDataset.Open(At("q"), new ParquetDatasetOptions { PartitionSchema = partial }));
        var dated = new Apache.Arrow.Schema([new Field("a", Date32Type.Default, true)], null);
        Write("r/a=2024-01-15/p.parquet", 0, 1);
        Assert.Equal(ArrowTypeId.StringView, TypeOf(ParquetDataset.Open(At("r"), new ParquetDatasetOptions { PartitionSchema = dated, ParseHiveDates = false }).Schema, "a"));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open(At("r"),
            new ParquetDatasetOptions { PartitionSchema = new Apache.Arrow.Schema([new Field("a", new ListType(Int32Type.Default), true)], null) }));
    }

    [Fact]
    public void PathsDisagreeingOnTheirKeys_AreRefused()
    {
        Write("m/a=1/b=x/p.parquet", 0, 1);
        Write("m/a=2/p.parquet", 1, 1);
        Assert.Throws<InvalidOperationException>(() => ParquetDataset.Open(At("m")));
        Write("o/a=1/b=x/p.parquet", 0, 1);
        Write("o/b=y/a=2/p.parquet", 1, 1);
        Assert.Equal(["0|1|x", "1|2|y"], Read(ParquetDataset.Open(At("o"))));
    }

    [Fact]
    public void AFilterOverPartitionColumns_DecidesTheFilesRead()
    {
        Write("f/year=2023/p.parquet", 0, 3);
        Write("f/year=2024/p.parquet", 3, 2);
        Write("f/year=2025/p.parquet", 5, 4);
        using var dataset = ParquetDataset.Open(At("f"));
        ScalarExpr year2025 = Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2025));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = year2025, FilterRows = true }))
        {
            Assert.Equal([false, false, true], reader.FilesRead);
            Assert.True(reader.FiltersExactly);
        }
        Assert.Equal(["5|2025", "6|2025", "7|2025", "8|2025"], Read(dataset, new ParquetReadOptions { Filter = year2025 }));
        // The row index counts the rows of the files left out (Polars: 5..8).
        Assert.Equal(["5|5|2025", "6|6|2025", "7|7|2025", "8|8|2025"], Read(dataset, new ParquetReadOptions { Filter = year2025, RowIndexColumn = "n" }));
        // Conjuncts over files and partitions together: the partitions' values put in.
        ScalarExpr mixed = ScalarExpr.NewBinary(BinaryOp.Or,
            ScalarExpr.NewBinary(BinaryOp.And, Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2024)), Binary(BinaryOp.GtEq, "id", LiteralValue.NewInt64(4))),
            ScalarExpr.NewBinary(BinaryOp.And, Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2025)), Binary(BinaryOp.Lt, "id", LiteralValue.NewInt64(7))));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = mixed, FilterRows = true }))
            Assert.True(reader.FiltersExactly);
        Assert.Equal(["4|2024", "5|2025", "6|2025"], Read(dataset, new ParquetReadOptions { Filter = mixed, FilterRows = true }));
        // A null partition value passes no comparison.
        Write("f/year=__HIVE_DEFAULT_PARTITION__/p.parquet", 9, 1);
        using var withNull = ParquetDataset.Open(At("f"));
        Assert.Equal(["9|null"], Read(withNull, new ParquetReadOptions { Filter = ScalarExpr.NewUnary(UnaryOp.IsNull, ScalarExpr.NewColumn("year")) }));
        Assert.Equal(9, Read(withNull, new ParquetReadOptions { Filter = Binary(BinaryOp.NotEq, "year", LiteralValue.NewInt64(1)) }).Count);
    }

    /// <summary>Each file reads its own filter: exactness holds only if every file's filter is exact.</summary>
    [Fact]
    public async Task FiltersExactly_HoldsForEveryFile_NotTheFirstOnly()
    {
        Write("x/year=2023/p.parquet", 0, 3);
        Write("x/year=__HIVE_DEFAULT_PARTITION__/p.parquet", 3, 3);
        using var dataset = ParquetDataset.Open(At("x"));
        // A null partition value cannot stand in the mixed conjunct: that file is filtered less.
        ScalarExpr mixed = ScalarExpr.NewBinary(BinaryOp.Or,
            ScalarExpr.NewBinary(BinaryOp.And, Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2023)), Binary(BinaryOp.GtEq, "id", LiteralValue.NewInt64(1))),
            Binary(BinaryOp.Lt, "id", LiteralValue.NewInt64(0)));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Filter = mixed, FilterRows = true }))
            Assert.False(reader.FiltersExactly);
        // The first file folds to true; the other keeps a conjunct the reader cannot evaluate.
        Write("y/year=2023/p.parquet", 0, 3);
        Write("y/year=2024/p.parquet", 3, 3);
        using var other = ParquetDataset.Open(At("y"));
        ScalarExpr residue = ScalarExpr.NewBinary(BinaryOp.Or, Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2023)),
            ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewBinary(BinaryOp.Add, ScalarExpr.NewColumn("id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(1))),
                ScalarExpr.NewLiteral(LiteralValue.NewInt64(5))));
        using (var reader = other.OpenReader(new ParquetReadOptions { Filter = residue, FilterRows = true }))
            Assert.False(reader.FiltersExactly);
        ScalarExpr exact = ScalarExpr.NewBinary(BinaryOp.Or, Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2023)), Binary(BinaryOp.Gt, "id", LiteralValue.NewInt64(4)));
        using (var reader = other.OpenReader(new ParquetReadOptions { Filter = exact, FilterRows = true }))
        {
            Assert.True(reader.FiltersExactly);
            List<long> ids = [];
            await foreach (RecordBatch batch in reader.ReadBatchesAsync())
            {
                using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
            }
            Assert.Equal([0L, 1, 2, 5], ids);
        }
    }

    /// <summary>The asynchronous read leaves the same files out and numbers rows the same way.</summary>
    [Fact]
    public async Task TheAsynchronousRead_PrunesAndNumbersAlike()
    {
        Write("a/year=2023/p.parquet", 0, 3);
        Write("a/year=2024/p.parquet", 3, 2);
        Write("a/year=2025/p.parquet", 5, 4);
        using var dataset = ParquetDataset.Open(At("a"));
        var options = new ParquetReadOptions { Filter = Binary(BinaryOp.Eq, "year", LiteralValue.NewInt64(2025)), RowIndexColumn = "n" };
        using var reader = dataset.OpenReader(options);
        List<string> rows = [];
        await foreach (RecordBatch batch in reader.ReadBatchesAsync())
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++) rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => Render(batch.Column(c), i))));
            }
        }
        Assert.Equal(["5|5|2025", "6|6|2025", "7|7|2025", "8|8|2025"], rows);
        Assert.Equal(Read(dataset, options), rows);
    }

    /// <summary>A string key the files hold follows the reader's layout, as other strings do.</summary>
    [Fact]
    public void AStringKeyTheFilesHold_FollowsTheLayout()
    {
        var names = new StringArray.Builder().Append("file").Build();
        Write("v/n=x/p.parquet", 0, 1, (new Field("n", StringType.Default, true), names));
        using var dataset = ParquetDataset.Open(At("v"));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { ViewTypes = false })) Assert.Equal(ArrowTypeId.String, TypeOf(reader.Schema, "n"));
        using (var reader = dataset.OpenReader()) Assert.Equal(ArrowTypeId.StringView, TypeOf(reader.Schema, "n"));
        Assert.Equal(["0|x"], Read(dataset, new ParquetReadOptions { ViewTypes = false }));
    }

    [Fact]
    public void Columns_SelectPartitionsToo()
    {
        Write("s/k=a/p.parquet", 0, 2);
        using var dataset = ParquetDataset.Open(At("s"), new ParquetDatasetOptions { FilePathColumn = "file" });
        Assert.Equal(["a", "a"], Read(ParquetDataset.Open(At("s")), new ParquetReadOptions { Columns = ["k"] }));
        using (var reader = dataset.OpenReader(new ParquetReadOptions { Columns = ["k", "id"], RowIndexColumn = "n", ViewTypes = false }))
        {
            Assert.Equal(["n", "id", "k", "file"], Names(reader.Schema));
            Assert.Equal(ArrowTypeId.String, TypeOf(reader.Schema, "k"));
        }
        Assert.Throws<ArgumentException>(() => dataset.OpenReader(new ParquetReadOptions { ColumnPaths = [["k", "x"]] }));
        Assert.Throws<ArgumentException>(() => dataset.OpenReader(new ParquetReadOptions { Columns = ["nope"] }));
        Assert.Throws<ArgumentException>(() => dataset.OpenReader(new ParquetReadOptions { RowIndexColumn = "k" }));
    }

    [Fact]
    public void ValuesGivenWithTheFiles_AreTyped_ByThePartitionSchema()
    {
        string a = Write("v/a.parquet", 0, 2);
        string b = Write("v/b.parquet", 2, 1);
        var schema = new Apache.Arrow.Schema([new Field("d", Date32Type.Default, true), new Field("n", Int32Type.Default, true), new Field("s", StringType.Default, true)], null);
        using var dataset = ParquetDataset.Open(
        [
            new ParquetDatasetFile(a, PartitionValues: new Dictionary<string, object?> { ["d"] = new DateOnly(2024, 1, 15), ["n"] = 3, ["s"] = "x" }),
            new ParquetDatasetFile(b, PartitionValues: new Dictionary<string, object?> { ["d"] = "2024-02-01", ["n"] = "4", ["s"] = null }),
        ], new ParquetDatasetOptions { PartitionSchema = schema });
        Assert.Equal(["0|2024-01-15|3|x", "1|2024-01-15|3|x", "2|2024-02-01|4|null"], Read(dataset));
        Assert.Equal(["2|2024-02-01|4|null"], Read(dataset, new ParquetReadOptions { Filter = Binary(BinaryOp.Gt, "n", LiteralValue.NewInt32(3)) }));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open([new ParquetDatasetFile(a, PartitionValues: new Dictionary<string, object?> { ["d"] = null })]));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open([new ParquetDatasetFile(a, PartitionValues: new Dictionary<string, object?> { ["d"] = null })],
            new ParquetDatasetOptions { PartitionSchema = schema }));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open([new ParquetDatasetFile(a, PartitionValues: new Dictionary<string, object?> { ["d"] = 1.5, ["n"] = 1, ["s"] = "" })],
            new ParquetDatasetOptions { PartitionSchema = schema }));
    }

    [Fact]
    public void WithoutGlobs_WildcardsAreNames()
    {
        string odd = Write("w/a[1].parquet", 0, 1);
        Write("w/a1.parquet", 1, 1);
        Assert.Equal(["1"], Read(ParquetDataset.Open(At("w/a[1].parquet"))));
        Assert.Equal(["0"], Read(ParquetDataset.Open(At("w/a[1].parquet"), new ParquetDatasetOptions { Glob = false })));
        Assert.Equal(odd, ParquetDataset.Open(odd, new ParquetDatasetOptions { Glob = false }).Files.Single().Path);
    }

    private static ScalarExpr Binary(BinaryOp op, string column, LiteralValue value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(value));
}
