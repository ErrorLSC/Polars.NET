#nullable enable
using System.Linq.Expressions;
using Kaminari.Seseragi.Files.Parquet;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests.IO;

/// <summary>
/// <see cref="ParquetReader{T}"/> against Polars: the rows as <c>DataFrame.Rows&lt;T&gt;()</c> maps them, and a predicate's
/// rows as the LINQ provider gives them for the same expression (one translator, the same semantics).
/// </summary>
public sealed class ParquetRowReaderTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"parquet-rows-{Guid.NewGuid():N}");
    private readonly string _path;
    private readonly Row[] _rows;

    public ParquetRowReaderTests()
    {
        Directory.CreateDirectory(_dir);
        _path = Path.Combine(_dir, "rows.parquet");
        string[] words = ["a", "b", "apple", "banana", "c"];
        var start = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        _rows = [.. Enumerable.Range(0, 5000).Select(i => new Row(
            i,
            i % 7 == 0 ? null : i % 10,
            i % 5 == 0 ? null : i % 40 / 8.0,
            i % 3 == 0 ? null : words[i % words.Length],
            i % 2 == 0,
            i % 11 == 0 ? null : start.AddHours(i)))];
        using var df = DataFrame.From(_rows);
        df.WriteParquet(_path, statistics: true, rowGroupSize: 1000);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static List<T> PolarsRows<T>(string path)
    {
        using var df = DataFrame.ReadParquet(path);
        using var rows = df.Rows<T>();
        List<T> list = [];
        while (rows.MoveNext()) list.Add(rows.Current);
        return list;
    }

    [Fact]
    public void AllRows_AreTheRowsPolarsMaps()
    {
        Assert.Equal(PolarsRows<Row>(_path), ParquetFile.ReadList<Row>(_path));
        Assert.Equal(_rows, ParquetFile.ReadList<Row>(_path));
    }

    [Fact]
    public void OnlyTheMembersColumns_AreRead_MatchedIgnoringCase()
    {
        using var file = ParquetFile.Open(_path);
        using ParquetReader<Narrow> reader = file.OpenReader<Narrow>();
        Assert.Equal(["Id", "S"], reader.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(_rows.Select(r => new Narrow(r.Id, r.S)), reader.ReadRows());
    }

    [Fact]
    public void AMemberWithoutAColumn_IsRefused()
    {
        using var file = ParquetFile.Open(_path);
        Assert.Contains("'Missing'", Assert.Throws<ArgumentException>(() => file.OpenReader<WithMissing>()).Message);
    }

    private static readonly long Low = 1234;
    private static readonly long[] Ids = [3, 30, 300, 4999, 7000];
    private static readonly List<long> IdList = [3, 30, 300, 4999, 7000];
    private static readonly HashSet<string> Words = ["apple", "c", "zzz"];
    private static readonly string?[] WordsAndNull = ["a", null];
    private static readonly int?[] NsAndNull = [1, 4, null];

    /// <summary>Predicates of every kind the translator pushes down: comparisons, nulls, strings, booleans, IN, captured values.</summary>
    private static readonly Expression<Func<Row, bool>>[] Predicates =
    [
        r => r.Id > 4000,
        r => r.N == 3,
        r => r.N != null && r.N < 5,
        r => r.N == null,
        r => r.S == "apple",
        r => r.S != "a",
        r => r.S!.StartsWith("b"),
        r => r.D > 2.5 && r.B,
        r => !r.B || r.Id < 10,
        r => r.T > new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc),
        r => r.Id >= Low && r.Id < Low + 100,
        r => Ids.Contains(r.Id),
        r => IdList.Contains(r.Id),
        r => Words.Contains(r.S!),
        r => WordsAndNull.Contains(r.S),
        r => NsAndNull.Contains(r.N),
        r => !Ids.Contains(r.Id),
        r => r.Id < 0,
    ];

    public static TheoryData<int> PredicateIndexes => [.. Enumerable.Range(0, Predicates.Length)];

    [Theory]
    [MemberData(nameof(PredicateIndexes))]
    public void APredicate_KeepsTheRowsTheLinqProviderKeeps(int index)
    {
        Expression<Func<Row, bool>> where = Predicates[index];
        using var df = DataFrame.ReadParquet(_path);
        List<Row> expected = [.. df.AsQueryable<Row>().Where(where)];

        using var file = ParquetFile.Open(_path);
        using ParquetReader<Row> reader = file.OpenReader(where);
        Assert.True(reader.FiltersExactly);
        Assert.Equal(expected, reader.ReadRows());
        Assert.Equal(expected, ParquetFile.ReadList(_path, where));
    }

    [Fact]
    public async Task ReadingAsynchronously_GivesTheSameRows()
    {
        Expression<Func<Row, bool>> where = r => r.N == 4 && r.S != null;
        Assert.Equal(ParquetFile.ReadList(_path, where), await ParquetFile.ReadListAsync(_path, where));
        await using var file = await ParquetFile.OpenAsync(_path);
        await using ParquetReader<Row> reader = file.OpenReader<Row>();
        List<Row> all = [];
        await foreach (Row row in reader.ReadRowsAsync()) all.Add(row);
        Assert.Equal(_rows, all);
    }

    [Fact]
    public void TheOptionsFilter_IsAppliedWithThePredicate()
    {
        var options = new ParquetReadOptions
        {
            Filter = Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Lt, Kaminari.Samidare.IR.ScalarExpr.NewColumn("Id"),
                Kaminari.Samidare.IR.ScalarExpr.NewLiteral(Kaminari.Samidare.IR.LiteralValue.NewInt64(100))),
            FilterRows = true,
        };
        Assert.Equal(_rows.Where(r => r.Id < 100 && r.B), ParquetFile.ReadList<Row>(_path, r => r.B, options));
    }

    [Fact]
    public void AFileWithoutRows_GivesNoRows()
    {
        string path = Path.Combine(_dir, "empty.parquet");
        Row[] none = [];
        using (var df = DataFrame.From(none)) df.WriteParquet(path);
        Assert.Empty(ParquetFile.ReadList<Row>(path));
        Assert.Empty(ParquetFile.ReadList<Row>(path, r => r.Id > 0));
    }

    public sealed record Row(long Id, int? N, double? D, string? S, bool B, DateTime? T);

    public sealed record Narrow(long id, string? s);

    public sealed record WithMissing(long Id, int Missing);
}
