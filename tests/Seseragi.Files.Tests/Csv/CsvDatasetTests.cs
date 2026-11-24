using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// <see cref="CsvDataset"/> against the behaviour of Python Polars 2.1's multi-file scan_csv (C3a,
/// csv-ref/datasets/RULE-C3.md) and the decisions taken where ours differs: columns matched by name, extra columns
/// refused unless ignored, the first files' kinds united as one file's, header, skips and comments per file, row limits
/// and numbers across files, empty files left out of listings, errors naming the file; the same rows whatever the
/// threads and chunks.
/// </summary>
public sealed class CsvDatasetTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-csv-dataset-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private string Write(string relative, string text)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        using FileStream file = File.Create(path);
        using Stream output = path.EndsWith(".gz", StringComparison.Ordinal) ? new GZipStream(file, CompressionLevel.Fastest)
            : path.EndsWith(".zst", StringComparison.Ordinal) ? new ZstdSharp.CompressionStream(file) : file;
        output.Write(bytes);
        return path;
    }

    private static string Type(IArrowType type) => type switch
    {
        Int64Type => "i64",
        Int32Type => "i32",
        DoubleType => "f64",
        BooleanType => "bool",
        StringViewType or StringType => "str",
        Date32Type => "date",
        TimestampType t => $"datetime[{t.Unit}{(t.Timezone is { } z ? ", " + z : "")}]",
        _ => type.Name,
    };

    /// <summary>"name:type,… | row | row …" of what a read gives.</summary>
    private static string Show(CsvDataset dataset, CsvReadOptions? options = null)
    {
        using CsvDatasetReader reader = dataset.OpenReader(options);
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
            using (batch)
                rows.AddRange(ArrowRender.Rows(batch));
        return string.Join(" | ", [string.Join(",", reader.Schema.FieldsList.Select(f => $"{f.Name}:{Type(f.DataType)}")), .. rows]);
    }

    private string Show(string pathOrGlob, CsvDatasetOptions? options = null, CsvReadOptions? read = null)
    {
        using CsvDataset dataset = CsvDataset.Open(At(pathOrGlob), options);
        return Show(dataset, read);
    }

    private void AB()
    {
        Write("d/a.csv", "x,y\n1,a\n2,b\n");
        Write("d/b.csv", "x,y\n3,c\n4,d\n");
    }

    // ---------------------------------------------------------------- files

    [Fact]
    public void AGlob_ADirectory_AndAList_ReadTheFilesInOrder()
    {
        AB();
        string expected = "x:i64,y:str | 1 | \"a\" | 2 | \"b\" | 3 | \"c\" | 4 | \"d\"";
        Assert.Equal((expected), (Show("d/*.csv")));
        Assert.Equal((expected), (Show("d")));
        using CsvDataset listed = CsvDataset.Open([At("d/b.csv"), At("d/a.csv")]);
        Assert.Equal(("x:i64,y:str | 3 | \"c\" | 4 | \"d\" | 1 | \"a\" | 2 | \"b\""), (Show(listed)));
    }

    [Fact]
    public void AGlobMatchingNothing_IsNotFound()
    {
        AB();
        Assert.Throws<FileNotFoundException>(() => CsvDataset.Open(At("d/*.tsv")));
    }

    [Fact]
    public void GlobOrder_IsThePaths()
    {
        Write("g/b.csv", "x\n2\n");
        Write("g/a.csv", "x\n1\n");
        Write("g/sub/c.csv", "x\n3\n");
        Write("g/A.csv", "x\n0\n");
        Assert.Equal("x:i64 | 0 | 1 | 2 | 3", Show("g/**/*.csv"));
    }

    [Fact]
    public void EmptyFiles_AreLeftOutOfListings_HeaderOnlyFilesHoldNoRows()
    {
        Write("e/a.csv", "x\n1\n");
        Write("e/b.csv", "");
        Write("e/c.csv", "x\n");
        Write("e/d.csv", "x\n3\n");
        Assert.Equal("x:i64 | 1 | 3", Show("e/*.csv"));
    }

    [Fact]
    public void AnEmptyFileNamed_IsAnErrorNamingIt()
    {
        string a = Write("n/a.csv", "x\n1\n"), b = Write("n/b.csv", "");
        var e = Assert.Throws<CsvFormatException>(() => Show(CsvDataset.Open([a, b])));
        Assert.Contains(b, e.Message);
    }

    [Fact]
    public void CompressedFiles_AndByteOrderMarks_AreReadAsTheOthers()
    {
        Write("z/a.csv", "x\n1\n");
        Write("z/b.csv.gz", "﻿x\n2\n");
        Write("z/c.csv.zst", "x\n3\n");
        Assert.Equal("x:i64 | 1 | 2 | 3", Show("z/*"));
    }

    [Fact]
    public void FilePaths_AreTheListedPaths()
    {
        AB();
        string got = Show("d/*.csv", new CsvDatasetOptions { FilePathColumn = "path" });
        Assert.Equal(($"x:i64,y:str,path:str | 1 | \"a\" | \"{At("d/a.csv")}\" | 2 | \"b\" | \"{At("d/a.csv")}\" | 3 | \"c\" | \"{At("d/b.csv")}\" | 4 | \"d\" | \"{At("d/b.csv")}\""), (got));
    }

    // ---------------------------------------------------------------- columns

    [Fact]
    public void Columns_AreMatchedByName_InAnyOrder()
    {
        Write("o/a.csv", "x,y\n1,a\n");
        Write("o/b.csv", "y,x\nc,3\n");
        Assert.Equal(("x:i64,y:str | 1 | \"a\" | 3 | \"c\""), (Show("o/*.csv")));
    }

    [Fact]
    public void AMissingColumn_IsAnErrorNamingTheFile_OrNullsWhenAllowed()
    {
        Write("m/a.csv", "x,y\n1,a\n");
        string b = Write("m/b.csv", "x\n3\n");
        var e = Assert.Throws<CsvFormatException>(() => Show("m/*.csv"));
        Assert.Contains(b, e.Message);
        Assert.Contains("'y'", e.Message);
        Assert.Contains("AllowMissingColumns", e.Message);
        Assert.Equal(("x:i64,y:str | 1 | \"a\" | 3 | null"), (Show("m/*.csv", new CsvDatasetOptions { AllowMissingColumns = true })));
    }

    [Fact]
    public void AnExtraColumn_IsAnErrorNamingTheFile_LeftOutWhenIgnored_UnitedWhenMissingAllowed()
    {
        Write("x/a.csv", "x\n1\n");
        string b = Write("x/b.csv", "x,z\n3,q\n");
        var e = Assert.Throws<CsvFormatException>(() => Show("x/*.csv"));
        Assert.Contains(b, e.Message);
        Assert.Contains("'z'", e.Message);
        Assert.Equal("x:i64 | 1 | 3", Show("x/*.csv", new CsvDatasetOptions { IgnoreExtraColumns = true }));
        Assert.Equal(("x:i64,z:str | 1 | null | 3 | \"q\""), (Show("x/*.csv", new CsvDatasetOptions { AllowMissingColumns = true })));
    }

    [Fact]
    public void AnExtraColumnOfAFileNotInferred_IsAnError_UnlessIgnored()
    {
        Write("xl/a.csv", "x\n1\n");
        string b = Write("xl/b.csv", "x,z\n3,q\n");
        var options = new CsvDatasetOptions { InferSchemaFiles = 1, AllowMissingColumns = true };
        var e = Assert.Throws<CsvFormatException>(() => Show("xl/*.csv", options));
        Assert.Contains(b, e.Message);
        Assert.Equal("x:i64 | 1 | 3", Show("xl/*.csv", options with { IgnoreExtraColumns = true }));
    }

    [Fact]
    public void AGivenSchema_MatchesByName_TypesTheColumns()
    {
        Write("s/a.csv", "x,y\n1,a\n");
        Write("s/b.csv", "y,x\nc,3\n");
        var schema = new Schema([new Field("x", Int32Type.Default, true), new Field("y", StringType.Default, true)], null);
        Assert.Equal(("x:i32,y:str | 1 | \"a\" | 3 | \"c\""), (Show("s/*.csv", new CsvDatasetOptions { Schema = schema })));
    }

    [Fact]
    public void Overrides_TypeInferredColumns()
    {
        AB();
        var options = new CsvDatasetOptions { Format = new CsvReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["x"] = DoubleType.Default } } };
        Assert.Equal(("x:f64,y:str | 1 | \"a\" | 2 | \"b\" | 3 | \"c\" | 4 | \"d\""), (Show("d/*.csv", options)));
    }

    [Fact]
    public void DuplicateNames_AreRenamedPerFile_ThenMatched()
    {
        Write("dup/a.csv", "x,x\n1,2\n");
        Write("dup/b.csv", "x,x\n3,4\n");
        Assert.Equal(("x:i64,x_duplicated_0:i64 | 1 | 2 | 3 | 4"), (Show("dup/*.csv")));
    }

    [Fact]
    public void WithoutAHeader_ColumnsAreNumbered_AndFilesOfOtherWidthsMissColumns()
    {
        Write("h/a.csv", "1,a\n");
        Write("h/b.csv", "2,b\n");
        var options = new CsvDatasetOptions { Format = new CsvReadOptions { HasHeader = false } };
        Assert.Equal(("column_1:i64,column_2:str | 1 | \"a\" | 2 | \"b\""), (Show("h/*.csv", options)));
        Write("h2/a.csv", "1,a\n");
        Write("h2/b.csv", "2\n");
        Assert.Throws<CsvFormatException>(() => Show("h2/*.csv", options));
        Assert.Equal(("column_1:i64,column_2:str | 1 | \"a\" | 2 | null"), (Show("h2/*.csv", options with { AllowMissingColumns = true })));
    }

    [Fact]
    public void WithoutAHeader_AGivenSchema_NamesTheColumnsByPosition()
    {
        Write("hp/a.csv", "1,a\n");
        Write("hp/b.csv", "2,b\n");
        var schema = new Schema([new Field("id", Int32Type.Default, true), new Field("name", StringType.Default, true)], null);
        Assert.Equal("id:i32,name:str | 1 | \"a\" | 2 | \"b\"",
            Show("hp/*.csv", new CsvDatasetOptions { Schema = schema, Format = new CsvReadOptions { HasHeader = false } }));
    }

    [Fact]
    public void OverridesOfColumnsTheFilesLack_ArePassedOver_AsOneFilesReaderDoes()
    {
        AB();
        var options = new CsvDatasetOptions { Format = new CsvReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["nope"] = DoubleType.Default } } };
        Assert.StartsWith("x:i64,y:str", Show("d/*.csv", options));
    }

    // ---------------------------------------------------------------- inference

    [Theory]
    [InlineData("1", "1.5", "f64")]
    [InlineData("1.5", "1", "f64")]
    [InlineData("1", "abc", "str")]
    [InlineData("1", "true", "str")]
    [InlineData("2024-01-02", "45292", "str")]
    [InlineData("2024-01-02", "2024-01-02 03:04:05", "datetime[Microsecond]")]
    [InlineData("2024-01-02 03:04:05", "2024-01-02T03:04:05Z", "str")]
    [InlineData("2024-01-02T03:04:05Z", "2024-01-02T03:04:05+09:00", "datetime[Microsecond, UTC]")]
    [InlineData("03:04:05", "2024-01-02", "str")]
    [InlineData("", "7", "i64")]
    public void TheFirstFilesKinds_AreUnitedAsOneFilesAre(string first, string second, string type)
    {
        string dir = $"k{Math.Abs((first + second).GetHashCode()) % 100000}";
        Write($"{dir}/a.csv", $"t\n{first}\n");
        Write($"{dir}/b.csv", $"t\n{second}\n");
        using CsvDataset dataset = CsvDataset.Open(At($"{dir}/*.csv"));
        Assert.Equal(type, Type(dataset.Schema.FieldsList[0].DataType));
        // As one file of both values.
        using (CsvReader one = CsvReader.Parse($"t\n{first}\n{second}\n")) Assert.Equal(type, Type(one.Schema.FieldsList[0].DataType));
        Show(dataset); // every value reads in the type
    }

    [Fact]
    public void AFileBeyondThoseInferred_WhoseValuesDoNotFit_IsAnErrorNamingIt()
    {
        for (int i = 0; i < 12; i++) Write($"many/f{i:D2}.csv", i < 11 ? "x\n1\n" : "x\n2.5\n");
        var e = Assert.Throws<CsvValueException>(() => Show("many/*.csv"));
        Assert.Contains(At("many/f11.csv"), e.Message);
        Assert.Contains("SchemaOverrides", e.Message);
        Assert.StartsWith("x:f64", Show("many/*.csv", new CsvDatasetOptions { InferSchemaFiles = 12 }));
        Assert.StartsWith("x:f64", Show("many/*.csv", new CsvDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName }));
    }

    /// <summary>
    /// The heads of the files inferred are read side by side: when several fail, the failure is the first file's (as if
    /// they were read in order), however fast the others fail.
    /// </summary>
    [Fact]
    public void WhenSeveralHeadsFail_TheFirstFilesFailureIsTheOne()
    {
        Write("hf/a.csv", "x\n" + string.Concat(Enumerable.Range(0, 99).Select(i => $"{i}\n")) + "\"open\n");
        Write("hf/b.csv", "x\n1\"a\n");
        for (int i = 0; i < 20; i++)
        {
            var e = Assert.Throws<CsvFormatException>(() => CsvDataset.Open(At("hf/*.csv")));
            Assert.StartsWith(At("hf/a.csv") + ": ", e.Message);
        }
    }

    [Fact]
    public void InferenceOff_ReadsStrings()
    {
        AB();
        Assert.StartsWith("x:str,y:str", Show("d/*.csv", new CsvDatasetOptions { Format = new CsvReadOptions { InferSchemaLength = 0 } }));
    }

    // ---------------------------------------------------------------- per file, across files

    [Fact]
    public void SkipsAndComments_ApplyToEveryFile()
    {
        Write("sk/a.csv", "junk\nx,y\nskip,me\n1,a\n");
        Write("sk/b.csv", "junk\nx,y\nskip,me\n3,c\n");
        var options = new CsvDatasetOptions { Format = new CsvReadOptions { SkipRows = 1, SkipRowsAfterHeader = 1 } };
        Assert.Equal(("x:i64,y:str | 1 | \"a\" | 3 | \"c\""), (Show("sk/*.csv", options)));
        Write("c/a.csv", "#c\nx,y\n1,a\n");
        Write("c/b.csv", "x,y\n#c\n3,c\n");
        Assert.Equal(("x:i64,y:str | 1 | \"a\" | 3 | \"c\""), (Show("c/*.csv", new CsvDatasetOptions { Format = new CsvReadOptions { CommentPrefix = "#" } })));
    }

    [Fact]
    public void RowLimits_AndRowNumbers_RunAcrossFiles()
    {
        Write("r/a.csv", "x\n1\n2\n");
        Write("r/b.csv", "x\n3\n4\n");
        Write("r/c.csv", "x\n5\n6\n");
        Assert.Equal("x:i64 | 1 | 2 | 3", Show("r/*.csv", read: new CsvReadOptions { MaxRows = 3 }));
        Assert.Equal(("i:i64,x:i64 | 10 | 1 | 11 | 2 | 12 | 3 | 13 | 4 | 14 | 5 | 15 | 6"),
            (Show("r/*.csv", read: new CsvReadOptions { RowIndexColumn = "i", RowIndexOffset = 10 })));
    }

    [Fact]
    public void AValueError_NamesTheFile_TheRowAndTheLine()
    {
        Write("err/a.csv", "x\n1\n");
        string b = Write("err/b.csv", "x\n2\nzz\n");
        var schema = new Schema([new Field("x", Int64Type.Default, true)], null);
        var e = Assert.Throws<CsvValueException>(() => Show("err/*.csv", new CsvDatasetOptions { Schema = schema }));
        Assert.StartsWith(b + ": ", e.Message);
        Assert.Equal(2, e.Row);
        Assert.Equal(3, e.Line);
    }

    [Fact]
    public void KeyValueDirectories_ArePartitions_WhenADirectoryIsGivenAlone()
    {
        Write("hive/k=1/a.csv", "x\n1\n");
        Write("hive/k=2/b.csv", "x\n2\n");
        Assert.Equal("x:i64,k:i64 | 1 | 1 | 2 | 2", Show("hive"));
        Assert.Equal("x:i64 | 1 | 2", Show("hive", new CsvDatasetOptions { Partitioning = HivePartitioning.Off }));
        // A filter over the partition column reads only the files it keeps.
        using CsvDataset dataset = CsvDataset.Open(At("hive"));
        var filter = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("k"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(2)));
        using CsvDatasetReader reader = dataset.OpenReader(new CsvReadOptions { Filter = filter, FilterRows = true });
        Assert.Equal([false, true], reader.FilesRead);
        Assert.Equal(["2 | 2"], ArrowRender.Rows(reader.ReadBatches()));
    }

    /// <summary>
    /// A chunk that read a datetime column otherwise than its file (its first value a date alone, the file's format learned
    /// from a datetime) is decoded again by the consumer, then made the dataset's like the others (the file path added).
    /// </summary>
    [Fact]
    public void AChunkDecodedAgainInTheFilesFormat_IsMadeTheDatasetsToo()
    {
        StringBuilder text = new("x,t\n");
        for (int r = 0; r < 150; r++) text.Append($"{r},\n");            // no format known from the first rows
        text.Append("150,2024-01-02 03:04:05\n");
        for (int r = 151; r < 400; r++) text.Append($"{r},2024-01-{1 + r % 28:D2}\n"); // dates alone: midnight
        string path = Write("redo/a.csv", text.ToString());
        var schema = new Schema([new Field("x", Int64Type.Default, true), new Field("t", new TimestampType(TimeUnit.Microsecond, (string?)null), true)], null);
        using CsvDataset dataset = CsvDataset.Open(At("redo"), new CsvDatasetOptions { Schema = schema, FilePathColumn = "path" });
        long before = CsvReader.Redecoded;
        string got = Show(dataset, new CsvReadOptions { MaxDegreeOfParallelism = 8, UnitBytes = 64, BatchSize = 7 });
        Assert.True(CsvReader.Redecoded > before, "no chunk was decoded again");
        Assert.Equal(Show(dataset, new CsvReadOptions { MaxDegreeOfParallelism = 1 }), got);
        Assert.Contains($"\"{path}\"", got);
        Assert.Contains(" 1704326400000000 ", got); // 2024-01-04 (a date alone) at midnight, in μs
    }

    [Fact]
    public void ManyFiles_ReadTheSame_WhateverTheThreadsAndChunks()
    {
        StringBuilder all = new("x,t,s\n");
        for (int f = 0; f < 23; f++)
        {
            StringBuilder text = new(f % 2 == 0 ? "x,t,s\n" : "s,x,t\n");
            for (int r = 0; r < 300 + f * 7; r++)
            {
                string t = r % 5 == 0 ? $"2024-03-{1 + r % 28:D2}" : $"2024-03-{1 + r % 28:D2} 0{r % 10}:00:00";
                string s = r % 3 == 0 ? "\"q,\"\"x\"\"\"" : $"s{r}";
                text.Append(f % 2 == 0 ? $"{f * 1000 + r},{t},{s}\n" : $"{s},{f * 1000 + r},{t}\n");
            }
            Write($"many2/f{f:D2}.csv", text.ToString());
        }
        using CsvDataset dataset = CsvDataset.Open(At("many2"));
        string whole = Show(dataset, new CsvReadOptions { MaxDegreeOfParallelism = 1 });
        Assert.StartsWith("x:i64,t:datetime[Microsecond],s:str", whole);
        foreach (int threads in (int[])[2, 8])
            foreach (int unit in (int[])[1, 64, 4096])
                Assert.Equal(whole, Show(dataset, new CsvReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = unit, BatchSize = 37 }));
    }
}
