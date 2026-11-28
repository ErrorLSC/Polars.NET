using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;
using Microsoft.FSharp.Collections;

namespace Seseragi.Files.Tests;

/// <summary>
/// Each row's number in its own file (<see cref="DatasetOptions.FileRowIndexColumn"/>), as Iceberg's <c>_pos</c>,
/// Spark's <c>_metadata.row_index</c> and DuckDB's <c>file_row_number</c> give it: from 0 in every file, the physical
/// position kept through filters and deletion vectors, beside the row index across the files; filters over it (Parquet
/// files' rows selected before they are read, other shapes and formats filtered after), row limits, files left out by
/// partition filters never opened, every format (rows numbered across units), names checked.
/// </summary>
public sealed class DatasetFileRowIndexTests
{
    private readonly MemoryFileStore _store = new();

    private static Field F(string name, IArrowType type) => new(name, type, true);

    private static Int32Array Ints(IEnumerable<int> values) => new Int32Array.Builder().AppendRange(values).Build();

    private void Put(string path, int first, int rows, int rowGroupRows = 1000)
    {
        var schema = new Apache.Arrow.Schema([F("x", Int32Type.Default)], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows }))
        {
            writer.Write(new RecordBatch(schema, [Ints(Enumerable.Range(first, rows))], rows));
            writer.Complete();
        }
        _store.Add(path, stream.ToArray());
    }

    private ParquetDataset Open(string path = "d/*.parquet", string? filePath = null) =>
        ParquetDataset.Open(path, new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "pos", FilePathColumn = filePath });

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        return ArrowRender.Rows(reader.ReadBatches());
    }

    private static ScalarExpr Pos => ScalarExpr.NewColumn("pos");

    private static ScalarExpr Lit(long value) => ScalarExpr.NewLiteral(LiteralValue.NewInt64(value));

    private static ScalarExpr Op(BinaryOp op, ScalarExpr a, ScalarExpr b) => ScalarExpr.NewBinary(op, a, b);

    /// <summary>Every row's x and its number in its file, with rows filtered (and the given filter).</summary>
    private static ParquetReadOptions Where(ScalarExpr filter, bool rows = true, long? maxRows = null, int threads = 1) =>
        new() { Filter = filter, FilterRows = rows, MaxRows = maxRows, MaxDegreeOfParallelism = threads };

    private static async Task<List<string>> ReadAsync(ParquetDataset dataset, ParquetReadOptions options)
    {
        await using var reader = dataset.OpenReader(options);
        List<string> rows = [];
        await foreach (RecordBatch batch in reader.ReadBatchesAsync()) rows.AddRange(ArrowRender.Rows([batch]));
        return rows;
    }

    [Fact]
    public void EveryFileIsNumberedFrom0_LastAfterTheFilePath_BesideTheRowIndexAcrossFiles()
    {
        Put("d/a.parquet", 10, 3);
        Put("d/b.parquet", 20, 2);
        using ParquetDataset dataset = Open(filePath: "file");
        Assert.Equal(["x", "file", "pos"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.False(dataset.Schema.GetFieldByName("pos").IsNullable);
        Assert.Equal(["10 | \"d/a.parquet\" | 0", "11 | \"d/a.parquet\" | 1", "12 | \"d/a.parquet\" | 2", "20 | \"d/b.parquet\" | 0", "21 | \"d/b.parquet\" | 1"],
            Read(dataset));
        foreach (int threads in (int[])[1, 8])
        {
            Assert.Equal(["0 | 10 | \"d/a.parquet\" | 0", "1 | 11 | \"d/a.parquet\" | 1", "2 | 12 | \"d/a.parquet\" | 2", "3 | 20 | \"d/b.parquet\" | 0", "4 | 21 | \"d/b.parquet\" | 1"],
                Read(dataset, new ParquetReadOptions { RowIndexColumn = "i", MaxDegreeOfParallelism = threads }));
        }
        // Selected by name like the dataset's other columns (always delivered, as the file path is).
        Assert.Equal(["0", "1", "2", "0", "1"], Read(dataset, new ParquetReadOptions { Columns = ["pos"] }).Select(r => r.Split(" | ")[^1]));
    }

    [Fact]
    public void ARowKeepsItsPosition_ThroughFiltersAndDeletionVectors()
    {
        Put("d/a.parquet", 0, 10, rowGroupRows: 4);
        ParquetDatasetFile[] files = [new("d/a.parquet", Rows: RowRanges.FromIndices([1, 4, 5]).Complement(10))]; // rows 1, 4, 5 deleted
        using var dataset = ParquetDataset.Open(files, new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "pos" });
        Assert.Equal(["0 | 0", "2 | 2", "3 | 3", "6 | 6", "7 | 7", "8 | 8", "9 | 9"], Read(dataset));
        ScalarExpr even = Op(BinaryOp.Eq, Op(BinaryOp.Modulo, ScalarExpr.NewColumn("x"), ScalarExpr.NewLiteral(LiteralValue.NewInt32(2))),
            ScalarExpr.NewLiteral(LiteralValue.NewInt32(0)));
        Assert.Equal(["0 | 0", "2 | 2", "6 | 6", "8 | 8"], Read(dataset, Where(even)));
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(8, false)]
    [InlineData(1, true)]
    [InlineData(8, true)]
    public void FiltersOverThePosition_SelectAParquetFilesRowsBeforeTheyAreRead(int threads, bool async)
    {
        Put("d/a.parquet", 0, 10, rowGroupRows: 3);
        Put("d/b.parquet", 100, 4);
        using ParquetDataset dataset = Open();
        List<string> X(ScalarExpr filter, bool rows = true, long? maxRows = null)
        {
            ParquetReadOptions options = Where(filter, rows, maxRows, threads);
            List<string> read = async ? ReadAsync(dataset, options).GetAwaiter().GetResult() : Read(dataset, options);
            return [.. read.Select(r => r.Split(" | ")[0])];
        }
        ScalarExpr range = Op(BinaryOp.And, Op(BinaryOp.GtEq, Pos, Lit(2)), Op(BinaryOp.Lt, Pos, Lit(5)));
        Assert.Equal(["2", "3", "4", "102", "103"], X(range));
        // Selected before the rows are read: exact without filtering rows (row groups and pages outside are not read).
        Assert.Equal(["2", "3", "4", "102", "103"], X(range, rows: false));
        // Literal first, Or, Not, NotEq, In (of other integer types), a list of nothing in range.
        Assert.Equal(["7", "8", "9"], X(Op(BinaryOp.Lt, Lit(6), Pos), rows: false));
        Assert.Equal(["0", "9", "100"], X(Op(BinaryOp.Or, Op(BinaryOp.Eq, Pos, Lit(0)), Op(BinaryOp.Eq, Pos, Lit(9))), rows: false));
        Assert.Equal(["0", "100"], X(ScalarExpr.NewUnary(UnaryOp.Not, Op(BinaryOp.GtEq, Pos, Lit(1))), rows: false));
        Assert.Equal(["0", "2", "3", "4", "5", "6", "7", "8", "9", "100", "102", "103"], X(Op(BinaryOp.NotEq, Pos, Lit(1)), rows: false));
        ScalarExpr inList = ScalarExpr.NewInList(Pos, InListSource.NewLiterals(ListModule.OfSeq([LiteralValue.NewInt32(3), LiteralValue.NewUInt8(1), LiteralValue.NewInt64(42)])), false, false);
        Assert.Equal(["1", "3", "101", "103"], X(inList, rows: false));
        // A shape not modelled: filtered after when rows are filtered, else all rows (pruning only).
        ScalarExpr odd = Op(BinaryOp.Eq, Op(BinaryOp.Modulo, Pos, Lit(2)), Lit(1));
        Assert.Equal(["1", "3", "5", "7", "9", "101", "103"], X(odd));
        Assert.Equal(14, X(odd, rows: false).Count);
        // A row limit counts the rows the position keeps (the files' readers are not cut before).
        Assert.Equal(["7", "9"], X(Op(BinaryOp.And, odd, Op(BinaryOp.GtEq, Pos, Lit(6))), maxRows: 2));
        Assert.Equal(["5", "7"], X(Op(BinaryOp.And, odd, Op(BinaryOp.GtEq, Pos, Lit(4))), maxRows: 2));
        // Counted alike; with a filter on a data column too.
        ScalarExpr bigX = Op(BinaryOp.GtEq, ScalarExpr.NewColumn("x"), ScalarExpr.NewLiteral(LiteralValue.NewInt32(3)));
        Assert.Equal(["3", "4", "102", "103"], X(Op(BinaryOp.And, bigX, range)));
        using ParquetDatasetReader counting = dataset.OpenReader(Where(range, threads: threads));
        Assert.Equal(5, counting.CountRows());
    }

    [Fact]
    public void AConjunctOverThePositionAndAnotherColumn_IsRefusedWhenRowsAreFiltered()
    {
        Put("d/a.parquet", 0, 4);
        using ParquetDataset dataset = Open();
        ScalarExpr both = Op(BinaryOp.Or, Op(BinaryOp.Eq, Pos, Lit(0)), Op(BinaryOp.Eq, ScalarExpr.NewColumn("x"), ScalarExpr.NewLiteral(LiteralValue.NewInt32(3))));
        var e = Assert.Throws<ArgumentException>(() => Read(dataset, Where(both)));
        Assert.Contains("'pos'", e.Message);
        Assert.Contains("x", e.Message);
        Assert.Equal(4, Read(dataset, Where(both, rows: false)).Count); // pruning only: left out
    }

    [Fact]
    public void FilesLeftOutByAPartitionFilter_AreNotOpened_UnlessTheRowIndexAcrossFilesCountsThem()
    {
        Put("p/k=1/a.parquet", 0, 3);
        _store.Add("p/k=2/broken.parquet", "not a parquet file"u8.ToArray());
        using var dataset = ParquetDataset.Open("p/", new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "pos", Schema = new([F("x", Int32Type.Default)], null) });
        ScalarExpr k1 = Op(BinaryOp.Eq, ScalarExpr.NewColumn("k"), Lit(1));
        Assert.Equal(["0 | 1 | 0", "1 | 1 | 1", "2 | 1 | 2"], Read(dataset, new ParquetReadOptions { Filter = k1 }));
        Assert.ThrowsAny<Exception>(() => Read(dataset, new ParquetReadOptions { Filter = k1, RowIndexColumn = "i" }));
    }

    [Fact]
    public void NamesAreChecked()
    {
        Put("d/a.parquet", 0, 1);
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/*.parquet", new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "x" }));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/*.parquet", new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "" }));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/*.parquet", new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "f", FilePathColumn = "f" }));
        using ParquetDataset dataset = Open();
        Assert.Throws<ArgumentException>(() => Read(dataset, new ParquetReadOptions { RowIndexColumn = "pos" }));
    }

    [Fact]
    public void CsvAndJsonFiles_AreNumberedAcrossTheirUnits()
    {
        string Lines(int first, int rows, Func<int, string> line) => string.Concat(Enumerable.Range(first, rows).Select(i => line(i) + "\n"));
        _store.Add("c/a.csv", System.Text.Encoding.UTF8.GetBytes("x\n" + Lines(0, 300, i => $"{i}")));
        _store.Add("c/b.csv", System.Text.Encoding.UTF8.GetBytes("x\n" + Lines(1000, 200, i => $"{i}")));
        _store.Add("j/a.json", System.Text.Encoding.UTF8.GetBytes(Lines(0, 300, i => $$"""{"x": {{i}}}""")));
        _store.Add("j/b.json", System.Text.Encoding.UTF8.GetBytes(Lines(1000, 200, i => $$"""{"x": {{i}}}""")));
        ScalarExpr window = Op(BinaryOp.And, Op(BinaryOp.GtEq, Pos, Lit(198)), Op(BinaryOp.Lt, Pos, Lit(202)));
        // With a filter on the data too: the positions are the records' in their files, those the data filter left out counted.
        ScalarExpr evenX = Op(BinaryOp.Eq, Op(BinaryOp.Modulo, ScalarExpr.NewColumn("x"), Lit(2)), Lit(0));
        ScalarExpr both = Op(BinaryOp.And, evenX, Op(BinaryOp.Lt, Pos, Lit(6)));
        string[] expected = ["198 | 198 | 198", "199 | 199 | 199", "200 | 200 | 200", "201 | 201 | 201", "1198 | 498 | 198", "1199 | 499 | 199"];
        foreach (int threads in (int[])[1, 8])
        {
            using var csv = CsvDataset.Open("c/*.csv", new CsvDatasetOptions { Store = _store, FileRowIndexColumn = "pos" });
            using (CsvDatasetReader reader = csv.OpenReader(new CsvReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 64, BatchSize = 7, RowIndexColumn = "i", Filter = window, FilterRows = true }))
                Assert.Equal(expected, ArrowRender.Rows(reader.ReadBatches()).Select(Reorder));
            using var json = JsonDataset.Open("j/*.json", new JsonDatasetOptions { Store = _store, FileRowIndexColumn = "pos" });
            using (JsonDatasetReader reader = json.OpenReader(new JsonReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 64, BatchSize = 7, RowIndexColumn = "i", Filter = window, FilterRows = true }))
                Assert.Equal(expected, ArrowRender.Rows(reader.ReadBatches()).Select(Reorder));
            using (CsvDatasetReader reader = csv.OpenReader(new CsvReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 64, BatchSize = 7, Filter = both, FilterRows = true }))
                Assert.Equal(["0 | 0", "2 | 2", "4 | 4", "1000 | 0", "1002 | 2", "1004 | 4"], ArrowRender.Rows(reader.ReadBatches()));
            using (JsonDatasetReader reader = json.OpenReader(new JsonReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 64, BatchSize = 7, Filter = both, FilterRows = true }))
                Assert.Equal(["0 | 0", "2 | 2", "4 | 4", "1000 | 0", "1002 | 2", "1004 | 4"], ArrowRender.Rows(reader.ReadBatches()));
            // Without the row index across files, and a limit.
            using (CsvDatasetReader reader = csv.OpenReader(new CsvReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 64, BatchSize = 7, Filter = window, FilterRows = true, MaxRows = 5 }))
                Assert.Equal(["198 | 198", "199 | 199", "200 | 200", "201 | 201", "1198 | 198"], ArrowRender.Rows(reader.ReadBatches()));
        }

        // "i | x | pos" as "x | i | pos".
        static string Reorder(string row) => row.Split(" | ") is var p ? $"{p[1]} | {p[0]} | {p[2]}" : row;
    }

    [Fact]
    public void IpcFiles_AreNumberedAcrossTheirBatches()
    {
        var schema = new Apache.Arrow.Schema([F("x", Int32Type.Default)], null);
        foreach (var (path, first) in (ValueTuple<string, int>[])[("i/a.arrow", 0), ("i/b.arrow", 100)])
        {
            using var stream = new MemoryStream();
            using (IpcWriter writer = IpcWriter.Create(stream, schema))
            {
                for (int b = 0; b < 3; b++)
                {
                    using var batch = new RecordBatch(schema, [Ints(Enumerable.Range(first + 4 * b, 4))], 4);
                    writer.Write(batch);
                }
                writer.Complete();
            }
            _store.Add(path, stream.ToArray());
        }
        using var dataset = IpcDataset.Open("i/*.arrow", new IpcDatasetOptions { Store = _store, FileRowIndexColumn = "pos" });
        ScalarExpr filter = Op(BinaryOp.And, Op(BinaryOp.GtEq, Pos, Lit(3)), Op(BinaryOp.Lt, Pos, Lit(6)));
        foreach (int threads in (int[])[1, 8])
        {
            using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads, Filter = filter, FilterRows = true, RowIndexColumn = "i" });
            Assert.Equal(["3 | 3 | 3", "4 | 4 | 4", "5 | 5 | 5", "15 | 103 | 3", "16 | 104 | 4", "17 | 105 | 5"], ArrowRender.Rows(reader.ReadBatches()));
            ScalarExpr odd = Op(BinaryOp.Eq, Op(BinaryOp.Modulo, ScalarExpr.NewColumn("x"), ScalarExpr.NewLiteral(LiteralValue.NewInt32(2))), ScalarExpr.NewLiteral(LiteralValue.NewInt32(1)));
            using IpcDatasetReader both = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads, Filter = Op(BinaryOp.And, odd, filter), FilterRows = true });
            Assert.Equal(["3 | 3", "5 | 5", "103 | 3", "105 | 5"], ArrowRender.Rows(both.ReadBatches()));
        }
    }

    private sealed record Plain(int x);

    private sealed record Positioned(int x, long pos);

    [Fact]
    public void TypedRows_TakeThePositionByName_OrLeaveItOut()
    {
        Put("d/a.parquet", 7, 2);
        _store.Add("c/a.csv", "x\n7\n8\n"u8.ToArray());
        var parquet = new ParquetDatasetOptions { Store = _store, FileRowIndexColumn = "pos" };
        Assert.Equal([new Plain(7), new Plain(8)], ParquetDataset.ReadList<Plain>("d/*.parquet", datasetOptions: parquet));
        Assert.Equal([new Positioned(7, 0), new Positioned(8, 1)], ParquetDataset.ReadList<Positioned>("d/*.parquet", datasetOptions: parquet));
        var csv = new CsvDatasetOptions { Store = _store, FileRowIndexColumn = "pos" };
        Assert.Equal([new Plain(7), new Plain(8)], CsvDataset.ReadList<Plain>("c/*.csv", datasetOptions: csv));
        Assert.Equal([new Positioned(7, 0), new Positioned(8, 1)], CsvDataset.ReadList<Positioned>("c/*.csv", datasetOptions: csv));
    }
}
