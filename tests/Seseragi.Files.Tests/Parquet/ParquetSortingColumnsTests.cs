using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Sort orders in Parquet footers (<c>sorting_columns</c>): what row groups declare (pyarrow's files: leaf ordinals,
/// declarations that say nothing), what the footer proves of the whole file (<see cref="ParquetMetadata.FileOrder"/>:
/// boundaries by statistics, ties, nulls, NaN, missing statistics, declarations not alike), the writer's declaration
/// (<see cref="ParquetWriteOptions.SortingColumns"/>, checked when created) and its check of the rows
/// (<see cref="ParquetWriteOptions.VerifySortingColumns"/>: within and across batches, later keys under ties, row
/// groups ended, threads, typed rows), and dataset files (partition keys left out).
/// </summary>
public sealed class ParquetSortingColumnsTests
{
    private static readonly string Fixtures = Path.Combine(AppContext.BaseDirectory, "TestData", "parquet-sorting");

    private static ParquetSortingColumn Asc(string column, bool nullsFirst = false) => new(column, false, nullsFirst);
    private static ParquetSortingColumn Desc(string column, bool nullsFirst = false) => new(column, true, nullsFirst);

    private static Int64Array Longs(params long?[] values)
    {
        var b = new Int64Array.Builder();
        foreach (long? v in values) b.Append(v);
        return b.Build();
    }

    private static DoubleArray Doubles(params double?[] values)
    {
        var b = new DoubleArray.Builder();
        foreach (double? v in values) b.Append(v);
        return b.Build();
    }

    private static StringArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? v in values) b.Append(v);
        return b.Build();
    }

    private static Apache.Arrow.Schema SchemaOf(params (string Name, IArrowType Type)[] fields) =>
        new([.. fields.Select(f => new Field(f.Name, f.Type, true))], null);

    /// <summary>A file of the batches (each its own row group with <paramref name="rowGroupRows"/> at least its rows).</summary>
    private static byte[] Write(Apache.Arrow.Schema schema, ParquetWriteOptions options, params IArrowArray[][] batches)
    {
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, options with { LeaveOpen = true }))
        {
            foreach (IArrowArray[] columns in batches)
            {
                writer.Write(new RecordBatch(schema, columns, columns[0].Length));
                writer.EndRowGroup();
            }
            writer.Complete();
        }
        return stream.ToArray();
    }

    /// <summary>The footer of a file of two-column (a, b) row groups, one per batch, declaring <paramref name="keys"/>.</summary>
    private static ParquetMetadata Groups(ParquetSortingColumn[] keys, bool statistics = true, (long?[] A, long?[] B)[]? groups = null)
    {
        var schema = SchemaOf(("a", Int64Type.Default), ("b", Int64Type.Default));
        var options = new ParquetWriteOptions { SortingColumns = keys, Statistics = statistics };
        return ParquetMetadata.FromFile(Write(schema, options, [.. groups!.Select(g => (IArrowArray[])[Longs(g.A), Longs(g.B)])]));
    }

    private static (long?[], long?[]) G(long?[] a, long?[]? b = null) => (a, b ?? [.. a.Select(_ => (long?)0)]);

    // ------------------------------------------------------------------
    // Declarations read
    // ------------------------------------------------------------------

    [Fact]
    public void PyarrowDeclarationsNameLeavesByColumnChunkOrdinal()
    {
        // s{x, y} comes first, so a and b are column chunks 2 and 3 (their top-level positions are 1 and 2).
        ParquetMetadata meta = ParquetMetadata.FromFile(File.ReadAllBytes(Path.Combine(Fixtures, "struct_first.parquet")));
        ParquetSortingColumn[] expected = [Asc("a"), Desc("b", nullsFirst: true)];
        Assert.Equal(2, meta.RowGroups.Count);
        foreach (ParquetRowGroup group in meta.RowGroups) Assert.Equal(expected, group.SortingColumns);
        // a: 1, 2 | 3, 4 follows strictly at the boundary, so both keys hold for the file.
        Assert.Equal(expected, meta.FileOrder);
    }

    [Theory]
    [InlineData("bad_index.parquet")] // column chunk 9 of 2
    [InlineData("list_leaf.parquet")] // the element of a list
    public void DeclarationsNamingNoRowColumnSayNothing(string file)
    {
        ParquetMetadata meta = ParquetMetadata.FromFile(File.ReadAllBytes(Path.Combine(Fixtures, file)));
        Assert.All(meta.RowGroups, g => Assert.Empty(g.SortingColumns));
        Assert.Empty(meta.FileOrder);
    }

    [Fact]
    public void ADeclarationNamingALeafTwiceSaysNothing()
    {
        ParquetMetadata written = Groups([Asc("a")], groups: [G([1, 2])]);
        FileMetaData meta = Rewrite(written, (g, _) => [new SortingColumn(0, false, false), new SortingColumn(0, true, false)]);
        ParquetMetadata view = ParquetMetadata.FromThrift(meta);
        Assert.Empty(view.RowGroups[0].SortingColumns);
        Assert.Empty(view.FileOrder);
    }

    /// <summary>The footer with each row group's declaration replaced (by its ordinal).</summary>
    private static FileMetaData Rewrite(ParquetMetadata written, Func<RowGroup, int, SortingColumn[]?> sorting)
    {
        FileMetaData meta = written.Thrift!;
        return new FileMetaData
        {
            Version = meta.Version, Schema = meta.Schema, NumRows = meta.NumRows, KeyValueMetadata = meta.KeyValueMetadata,
            CreatedBy = meta.CreatedBy, ColumnOrders = meta.ColumnOrders,
            RowGroups = [.. meta.RowGroups.Select((g, i) => new RowGroup
            {
                Columns = g.Columns, TotalByteSize = g.TotalByteSize, NumRows = g.NumRows, FileOffset = g.FileOffset,
                TotalCompressedSize = g.TotalCompressedSize, Ordinal = g.Ordinal, SortingColumns = sorting(g, i),
            })],
        };
    }

    private static FileMetaData GetThrift(ParquetMetadata meta) => meta.Thrift!;

    // ------------------------------------------------------------------
    // The file's order
    // ------------------------------------------------------------------

    [Fact]
    public void FileOrderHoldsWholeWhereTheFirstKeyOrdersEveryBoundaryStrictly()
    {
        ParquetSortingColumn[] keys = [Asc("a"), Asc("b")];
        Assert.Equal(keys, Groups(keys, groups: [G([1, 2], [5, 6]), G([3, 3], [0, 1]), G([4], [9])]).FileOrder);
        // One row group: its declaration (believed, never checked against the rows).
        Assert.Equal(keys, Groups(keys, groups: [G([1, 2], [5, 6])]).FileOrder);
    }

    [Fact]
    public void ATieAtABoundaryProvesTheFirstKeyAlone()
    {
        ParquetSortingColumn[] keys = [Asc("a"), Asc("b")];
        // a: 1, 2 | 2, 3 ties at the boundary: whether b goes on in order there the statistics cannot tell.
        Assert.Equal([Asc("a")], Groups(keys, groups: [G([1, 2], [0, 9]), G([2, 3], [1, 2])]).FileOrder);
    }

    [Fact]
    public void RowGroupsOrderedOnlyWithinThemselvesProveNothing()
    {
        // Each row group sorted by a (as declared), the file not: 1, 3 | 2, 4.
        ParquetMetadata meta = Groups([Asc("a")], groups: [G([1, 3]), G([2, 4])]);
        Assert.All(meta.RowGroups, g => Assert.Equal([Asc("a")], g.SortingColumns));
        Assert.Empty(meta.FileOrder);
    }

    [Fact]
    public void DescendingKeysCompareTheOtherWay()
    {
        Assert.Equal([Desc("a")], Groups([Desc("a")], groups: [G([4, 3]), G([2, 1])]).FileOrder);
        Assert.Empty(Groups([Desc("a")], groups: [G([2, 1]), G([4, 3])]).FileOrder);
        // Ascending would prove the second file, not the first.
        Assert.Empty(Groups([Asc("a")], groups: [G([4, 3]), G([2, 1])]).FileOrder);
        // A row group's last row is its smallest: 5, 1 | 3, 2 is out of order though 5 is above 3.
        Assert.Empty(Groups([Desc("a")], groups: [G([5, 1]), G([3, 2])]).FileOrder);
        // 3, 2 | 2, 1 ties at the boundary: the later key is not proven.
        Assert.Equal([Desc("a")], Groups([Desc("a"), Asc("b")], groups: [G([3, 2]), G([2, 1])]).FileOrder);
    }

    [Fact]
    public void NullsLastMayOnlyBeFollowedByNulls()
    {
        ParquetSortingColumn[] keys = [Asc("a"), Asc("b")];
        Assert.Equal(keys, Groups(keys, groups: [G([1, 2]), G([3, null])]).FileOrder);
        Assert.Equal(keys, Groups(keys, groups: [G([1, 2]), G([null, null])]).FileOrder); // values, then nulls alone
        Assert.Equal([Asc("a")], Groups(keys, groups: [G([1, null]), G([null, null])]).FileOrder); // null against null: a tie
        Assert.Empty(Groups(keys, groups: [G([1, null]), G([2, 3])]).FileOrder);
    }

    [Fact]
    public void NullsFirstMayOnlyFollowNulls()
    {
        ParquetSortingColumn[] keys = [Asc("a", nullsFirst: true), Asc("b")];
        Assert.Equal(keys, Groups(keys, groups: [G([null, 1]), G([2, 3])]).FileOrder);
        Assert.Equal(keys, Groups(keys, groups: [G([null, null]), G([1, 2])]).FileOrder); // nulls alone, then values
        Assert.Equal([keys[0]], Groups(keys, groups: [G([null, null]), G([null, 1])]).FileOrder);
        Assert.Empty(Groups(keys, groups: [G([1, 2]), G([null, 3])]).FileOrder);
        // Declared nulls last, the same file proves nothing.
        Assert.Empty(Groups([Asc("a"), Asc("b")], groups: [G([null, 1]), G([2, 3])]).FileOrder);
    }

    [Fact]
    public void WithoutStatisticsNothingIsProvenAcrossRowGroups()
    {
        Assert.Empty(Groups([Asc("a")], statistics: false, groups: [G([1, 2]), G([3, 4])]).FileOrder);
        Assert.Equal([Asc("a")], Groups([Asc("a")], statistics: false, groups: [G([1, 2])]).FileOrder);
    }

    [Fact]
    public void WithoutNullCountsNothingIsProven()
    {
        ParquetSortingColumn[] keys = [Asc("a")];
        Assert.Equal(keys, Groups(keys, groups: [G([1, 2]), G([3, null])]).FileOrder);
        // The same file, the first row group's null count left out: whether it ends in nulls is not known.
        ParquetMetadata meta = Groups(keys, groups: [G([1, 2]), G([3, null])]);
        Statistics statistics = meta.Thrift!.RowGroups[0].Columns[0].MetaData!.Statistics!;
        typeof(Statistics).GetProperty(nameof(Statistics.NullCount))!.SetValue(statistics, null); // init-only: a footer without it
        Assert.Empty(meta.FileOrder);
    }

    [Fact]
    public void RowGroupsDeclaringOtherKeysProveNothing()
    {
        ParquetMetadata written = Groups([Asc("a"), Asc("b")], groups: [G([1, 2]), G([3, 4])]);
        Assert.Equal([Asc("a"), Asc("b")], written.FileOrder);
        // The second row group declares a alone, then a descending, then nothing.
        foreach (SortingColumn[]? second in (SortingColumn[]?[])[[new(0, false, false)], [new(0, true, false), new(1, false, false)], null])
        {
            ParquetMetadata view = ParquetMetadata.FromThrift(Rewrite(written, (g, i) => i == 0 ? g.SortingColumns : second));
            Assert.Empty(view.FileOrder);
        }
    }

    [Fact]
    public void FloatKeysNeedToBeKnownFreeOfNaN()
    {
        var schema = SchemaOf(("d", DoubleType.Default));
        var options = new ParquetWriteOptions { SortingColumns = [Asc("d")] };
        Assert.Equal([Asc("d")], ParquetMetadata.FromFile(Write(schema, options, [Doubles(-0.0, 1.5)], [Doubles(2, 3)])).FileOrder);
        // NaN sorts last, so 1, NaN | 2 is out of order; and a row group holding NaN has no bounds to tell anyway.
        Assert.Empty(ParquetMetadata.FromFile(Write(schema, options, [Doubles(1, double.NaN)], [Doubles(2, 3)])).FileOrder);
        Assert.Empty(ParquetMetadata.FromFile(Write(schema, options, [Doubles(1, 2)], [Doubles(3, double.NaN)])).FileOrder);
    }

    [Fact]
    public void BoundsLeavingNaNOutProveNothingWithoutANaNCountOfZero()
    {
        // pyarrow: 1, NaN | 2, 3 with bounds 1..1 in the first row group; NaN sorts last, so the file is not in order.
        ParquetMetadata meta = ParquetMetadata.FromFile(File.ReadAllBytes(Path.Combine(Fixtures, "nan_bounds.parquet")));
        Assert.All(meta.RowGroups, g => Assert.Equal([Asc("d")], g.SortingColumns));
        Assert.Empty(meta.FileOrder);
    }

    [Fact]
    public void TruncatedStringBoundsProveOnlyWhatTheyBound()
    {
        var schema = SchemaOf(("s", StringType.Default));
        var options = new ParquetWriteOptions { SortingColumns = [Asc("s")] }; // bounds cut to 64 bytes
        string stem = new('k', 100);
        // The two row groups differ after byte 64 alone: their cut bounds overlap, so nothing is proven.
        Assert.Empty(ParquetMetadata.FromFile(Write(schema, options, [Strings(stem + "a", stem + "b")], [Strings(stem + "c")])).FileOrder);
        Assert.Equal([Asc("s")], ParquetMetadata.FromFile(Write(schema, options, [Strings("a" + stem, "b" + stem)], [Strings("c" + stem)])).FileOrder);
    }

    // ------------------------------------------------------------------
    // The writer's declaration
    // ------------------------------------------------------------------

    [Fact]
    public void TheWriterDeclaresTheKeysInEveryRowGroupByLeafOrdinal()
    {
        var sType = new StructType([new Field("x", Int64Type.Default, true), new Field("y", Int64Type.Default, true)]);
        var schema = new Apache.Arrow.Schema([new Field("s", sType, true), new Field("a", Int64Type.Default, true)], null);
        IArrowArray S(long x) => TestBatches.Struct(sType, [Longs(x, x), Longs(0, 0)], [true, true]);
        ParquetSortingColumn[] keys = [Asc("a"), new(["s", "y"], true, true)];
        byte[] file = Write(schema, new ParquetWriteOptions { SortingColumns = keys, VerifySortingColumns = true }, [S(1), Longs(1, 2)], [S(2), Longs(3, 4)]);
        ParquetMetadata meta = ParquetMetadata.FromFile(file);
        Assert.Equal(2, meta.RowGroups.Count);
        Assert.All(meta.RowGroups, g => Assert.Equal(keys, g.SortingColumns));
        FileMetaData thrift = GetThrift(meta);
        Assert.All(thrift.RowGroups, g => Assert.Equal([new SortingColumn(2, false, false), new SortingColumn(1, true, true)], g.SortingColumns!));
        Assert.Equal(keys, meta.FileOrder); // a orders the boundary strictly (2 | 3): the later key holds too
    }

    [Fact]
    [Trait("Category", "Interop")]
    public async Task PyarrowAndPolarsReadTheDeclaration()
    {
        // Opt-in: SESERAGI_PARQUET_PYTHON, a Python with pyarrow and polars (as the writer interop tests).
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        var sType = new StructType([new Field("x", Int64Type.Default, true), new Field("y", Int64Type.Default, true)]);
        var schema = new Apache.Arrow.Schema([new Field("s", sType, true), new Field("a", Int64Type.Default, true)], null);
        var options = new ParquetWriteOptions { SortingColumns = [Asc("a"), new(["s", "y"], true, true)], VerifySortingColumns = true };
        string path = Path.Combine(Directory.CreateTempSubdirectory("seseragi-sorting").FullName, "sorted.parquet");
        try
        {
            File.WriteAllBytes(path, Write(schema, options, [TestBatches.Struct(sType, [Longs(1, 1), Longs(0, 0)], [true, true]), Longs(1, 2)]));
            var psi = new System.Diagnostics.ProcessStartInfo(python, ["-I", Path.Combine(Fixtures, "read_sorting.py"), path])
                { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = System.Diagnostics.Process.Start(psi)!;
            Task<string> stdout = process.StandardOutput.ReadToEndAsync(), stderr = process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, await stderr);
            using var read = System.Text.Json.JsonDocument.Parse(await stdout);
            // a is column chunk 2 (s.x and s.y come first), s.y chunk 1.
            Assert.Equal("[[[2,false,false],[1,true,true]]]", read.RootElement.GetProperty("pyarrow").GetRawText().Replace(" ", ""));
            Assert.True(read.RootElement.GetProperty("polars").GetProperty("a").GetBoolean()); // Polars marks the first key sorted
        }
        finally
        {
            Directory.Delete(Path.GetDirectoryName(path)!, recursive: true);
        }
    }

    [Fact]
    public void WithoutADeclarationNoneIsWritten()
    {
        var schema = SchemaOf(("a", Int64Type.Default));
        FileMetaData thrift = GetThrift(ParquetMetadata.FromFile(Write(schema, new ParquetWriteOptions(), [Longs(1)])));
        Assert.Null(thrift.RowGroups[0].SortingColumns);
    }

    [Fact]
    public void DeclarationsTheWriterCannotMakeAreRefusedWhenItIsCreated()
    {
        var listType = new ListType(new Field("element", Int64Type.Default, true));
        var schema = new Apache.Arrow.Schema(
        [
            new Field("a", Int64Type.Default, true), new Field("l", listType, true), new Field("t", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
        ], null);
        string Refused(ParquetWriteOptions options) =>
            Assert.ThrowsAny<ArgumentException>(() => ParquetWriter.Create(new MemoryStream(), schema, options)).Message;

        Assert.Contains("'b', which is no leaf column", Refused(new() { SortingColumns = [Asc("b")] }));
        Assert.Contains("'l', which is no leaf column of the schema", Refused(new() { SortingColumns = [Asc("l")] }));
        Assert.Contains("the columns are 'a', 't')", Refused(new() { SortingColumns = [Asc("l")] })); // a list's leaf is none to sort by
        Assert.Contains("lies in a list or a map", Refused(new() { SortingColumns = [new(["l", "list", "element"])] }));
        Assert.Contains("INT96", Refused(new() { SortingColumns = [Asc("t")], Int96Timestamps = true }));
        Assert.Contains("twice", Refused(new() { SortingColumns = [Asc("a"), Desc("a")] }));
        Assert.Contains("SortingColumns declares", Refused(new() { VerifySortingColumns = true }));
        Assert.Contains("SortingColumns declares", Refused(new() { VerifySortingColumns = true, SortingColumns = [] }));
        Assert.Contains("path of names", Refused(new() { SortingColumns = [new([])] }));
        Assert.Throws<ArgumentNullException>(() => ParquetWriter.Create(new MemoryStream(), schema, new() { SortingColumns = [null!] }));
        // Declared alone, a timestamp in microseconds is fine.
        ParquetWriter.Create(new MemoryStream(), schema, new() { SortingColumns = [Asc("t"), Desc("a")] }).Dispose();
    }

    [Fact]
    public void KeysTheCheckCannotOrderAreRefusedWhenTheRowsAreToBeChecked()
    {
        var schema = SchemaOf(("n", NullType.Default));
        var declared = new ParquetWriteOptions { SortingColumns = [Asc("n")] };
        ParquetWriter.Create(new MemoryStream(), schema, declared).Dispose();
        var error = Assert.Throws<ArgumentException>(() => ParquetWriter.Create(new MemoryStream(), schema, declared with { VerifySortingColumns = true }));
        Assert.Contains("cannot check the order of 'n'", error.Message);
    }

    // ------------------------------------------------------------------
    // The writer's check
    // ------------------------------------------------------------------

    private static readonly Apache.Arrow.Schema AB = SchemaOf(("a", Int64Type.Default), ("b", StringType.Default));

    /// <summary>Writes the batches with the rows checked; the first failure's message, or null when the file completed.</summary>
    private static string? Checked(ParquetSortingColumn[] keys, IEnumerable<(long?[] A, string?[] B)> batches, bool endEach = false,
        BatchOwnership ownership = BatchOwnership.Caller, int threads = 1)
    {
        var options = new ParquetWriteOptions { SortingColumns = keys, VerifySortingColumns = true, BatchOwnership = ownership, MaxDegreeOfParallelism = threads };
        using var writer = ParquetWriter.Create(new MemoryStream(), AB, options);
        foreach (var (a, b) in batches)
        {
            try
            {
                writer.Write(new RecordBatch(AB, [Longs(a), Strings(b)], a.Length));
            }
            catch (InvalidDataException e)
            {
                // The file is given up.
                Assert.Throws<InvalidOperationException>(() => writer.Complete());
                return e.Message;
            }
            if (endEach) writer.EndRowGroup();
        }
        writer.Complete();
        return null;
    }

    [Fact]
    public void SortedRowsPassAcrossBatches()
    {
        Assert.Null(Checked([Asc("a"), Desc("b")], [([1, 1, 2], ["z", "y", "q"]), ([2, 3], ["q", "a"]), ([3], ["a"]), ([], [])]));
        Assert.Null(Checked([Asc("a", nullsFirst: true)], [([null, null], [null, null]), ([null, 1, 1], ["", "", ""])]));
        Assert.Null(Checked([Desc("b"), Asc("a")], [([5, 6], ["x", "x"]), ([1], ["w"]), ([0], [null])]));
    }

    [Fact]
    public void ARowOutOfOrderInABatchFailsTheFileNamingRowKeyAndValues()
    {
        string? error = Checked([Asc("a")], [([1, 2], ["", ""]), ([3, 5, 4, 6], ["", "", "", ""])]);
        Assert.NotNull(error);
        Assert.Contains("Row 4 sorts before the row above it by 'a': 4 after 5", error);
        Assert.Contains("declares 'a' ascending, nulls last", error);
    }

    [Fact]
    public void ARowOutOfOrderAtABatchBoundaryIsFound()
    {
        Assert.Contains("Row 2 sorts before the row above it by 'a': 1 after 2", Checked([Asc("a")], [([1, 2], ["", ""]), ([1], [""])]));
        // A null after values, nulls last: fine; values after it are not.
        Assert.Null(Checked([Asc("a")], [([1, 2], ["", ""]), ([null], [""])]));
        Assert.Contains("Row 3 sorts before the row above it by 'a': 3 after null", Checked([Asc("a")], [([1, 2], ["", ""]), ([null], [""]), ([3], [""])]));
    }

    [Fact]
    public void LaterKeysOrderOnlyRowsTheKeysBeforeThemTie()
    {
        ParquetSortingColumn[] keys = [Asc("a"), Asc("b")];
        Assert.Null(Checked(keys, [([1, 2, 3], ["c", "b", "a"])]));
        string? error = Checked(keys, [([1, 2, 2], ["a", "c", "b"])]);
        Assert.Contains("Row 2 sorts before the row above it by 'b' (the keys before it tie: 'a'): \"b\" after \"c\"", error);
        // The same across a batch boundary.
        Assert.Contains("Row 1 sorts before the row above it by 'b'", Checked(keys, [([2], ["c"]), ([2], ["b"])]));
    }

    [Fact]
    public void EndingARowGroupStartsTheCheckOver()
    {
        Assert.Null(Checked([Asc("a")], [([3, 4], ["", ""]), ([1, 2], ["", ""])], endEach: true));
        Assert.NotNull(Checked([Asc("a")], [([3, 4], ["", ""]), ([1, 2], ["", ""])]));
        // Rows still count from the file's first.
        Assert.Contains("Row 3 sorts", Checked([Asc("a")], [([3, 4], ["", ""]), ([1, 0], ["", ""])], endEach: true));
    }

    [Fact]
    public void TheCheckIsTheSameWithTheWriterOwningTheBatches()
    {
        Assert.Null(Checked([Asc("a")], [([1, 2], ["", ""]), ([3], [""])], ownership: BatchOwnership.Writer, threads: 4));
        Assert.Contains("Row 2 sorts before the row above it by 'a': 0 after 2",
            Checked([Asc("a")], [([1, 2], ["", ""]), ([0], [""])], ownership: BatchOwnership.Writer, threads: 4));
    }

    [Theory]
    [InlineData(BatchOwnership.Caller)]
    [InlineData(BatchOwnership.Writer)]
    public void ABatchOfManyRowsIsCheckedSideBySide(BatchOwnership ownership)
    {
        // 40 000 rows on 4 threads: segments checked side by side; the first break of several is the one named.
        long?[] a = [.. Enumerable.Range(0, 40_000).Select(i => (long?)i)];
        string?[] b = [.. a.Select(_ => "")];
        Assert.Null(Checked([Asc("a")], [(a, b)], ownership: ownership, threads: 4));
        a[30_001] = 0;
        a[25_000] = 7;
        Assert.Contains("Row 25000 sorts before the row above it by 'a': 7 after 24999", Checked([Asc("a")], [(a, b)], ownership: ownership, threads: 4));
    }

    [Fact]
    public void TheFileWrittenWithTheCheckReadsBackDeclared()
    {
        var options = new ParquetWriteOptions { SortingColumns = [Asc("a")], VerifySortingColumns = true };
        ParquetMetadata meta = ParquetMetadata.FromFile(Write(AB, options, [Longs(1, 2), Strings("", "")], [Longs(2, 3), Strings("", "")]));
        Assert.Equal([Asc("a")], meta.FileOrder);
    }

    [Fact]
    public void AStructFieldKeyIsCarriedAloneAcrossBatches()
    {
        // s: {x, lv (a list view: rows of it are not copied)}; the last row of the first batch has s null (nulls last),
        // so the second batch may hold only nulls at s.x.
        var lvType = new ListViewType(Int32Type.Default);
        var sType = new StructType([new Field("x", Int64Type.Default, true), new Field("lv", lvType, true)]);
        var schema = new Apache.Arrow.Schema([new Field("s", sType, true)], null);
        static byte[] Ints(params int[] values) => [.. values.SelectMany(BitConverter.GetBytes)];
        IArrowArray S(long?[] x, bool[] valid)
        {
            var items = new ArrayData(Int32Type.Default, 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(Ints(7))]);
            var lv = new ListViewArray(new ArrayData(lvType, x.Length, 0, 0,
                [ArrowBuffer.Empty, new ArrowBuffer(Ints([.. x.Select(_ => 0)])), new ArrowBuffer(Ints([.. x.Select(_ => 1)]))], [items]));
            return TestBatches.Struct(sType, [Longs(x), lv], valid);
        }
        var options = new ParquetWriteOptions { SortingColumns = [new(["s", "x"])], VerifySortingColumns = true };
        using (var writer = ParquetWriter.Create(new MemoryStream(), schema, options))
        {
            writer.Write(new RecordBatch(schema, [S([1, 2], [true, false])], 2));
            writer.Write(new RecordBatch(schema, [S([null, 0], [true, false])], 2));
            writer.Complete();
        }
        using var failing = ParquetWriter.Create(new MemoryStream(), schema, options);
        failing.Write(new RecordBatch(schema, [S([1, 2], [true, false])], 2));
        var error = Assert.Throws<InvalidDataException>(() => failing.Write(new RecordBatch(schema, [S([3], [true])], 1)));
        Assert.Contains("Row 2 sorts before the row above it by 's.x': 3 after null", error.Message);
    }

    public sealed record Reading(long Id, string Name);

    [Fact]
    public void TypedRowsAreCheckedToo()
    {
        var options = new ParquetWriteOptions { SortingColumns = [Asc("Id")], VerifySortingColumns = true };
        using var stream = new MemoryStream();
        using var writer = ParquetWriter<Reading>.Create(stream, options);
        writer.WriteRows([new(1, "a"), new(3, "b"), new(2, "c")]);
        var error = Assert.Throws<InvalidDataException>(writer.Complete);
        Assert.Contains("Row 2 sorts before the row above it by 'Id': 2 after 3", error.Message);
    }

    // ------------------------------------------------------------------
    // Dataset files
    // ------------------------------------------------------------------

    [Fact]
    public void DatasetFilesLeavePartitionKeysOut()
    {
        var store = new MemoryFileStore();
        var schema = SchemaOf(("p", StringType.Default), ("a", Int64Type.Default));
        var options = new ParquetDatasetWriteOptions
        {
            Store = store, PartitionBy = ["p"],
            Writer = new ParquetWriteOptions { SortingColumns = [Asc("p"), Desc("a")], VerifySortingColumns = true },
        };
        DatasetWriteResult<ParquetWrittenFile> result;
        using (var writer = ParquetDatasetWriter.Create("d", schema, options))
        {
            writer.Write(new RecordBatch(schema, [Strings("x", "x", "y"), Longs(5, 4, 9)], 3));
            result = writer.Complete();
        }
        Assert.Equal(2, result.Files.Count);
        Assert.All(result.Files, f => Assert.Equal([Desc("a")], f.Metadata.RowGroups[0].SortingColumns));

        // Only partition keys: nothing is declared (or checked).
        options = options with { Writer = options.Writer with { SortingColumns = [Asc("p")] }, ExistingData = ExistingData.Append };
        using (var writer = ParquetDatasetWriter.Create("d", schema, options))
        {
            writer.Write(new RecordBatch(schema, [Strings("x"), Longs(1)], 1));
            Assert.All(writer.Complete().Files, f => Assert.Empty(f.Metadata.RowGroups[0].SortingColumns));
        }

        var error = Assert.Throws<ArgumentException>(() => ParquetDatasetWriter.Create("e", schema, options with { Writer = options.Writer with { SortingColumns = [Asc("q")] } }));
        Assert.Contains("'q' is no column of the schema", error.Message);
    }
}
