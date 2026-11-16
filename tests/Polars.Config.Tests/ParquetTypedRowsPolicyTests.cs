#nullable enable
using System.Linq.Expressions;
using Kaminari.Seseragi.Files.Parquet;
using Polars.NET.Core.Enums;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// <see cref="ParquetReader{T}"/> under the process-global client evaluation policy of the LINQ provider (hence the
/// serial project): conjuncts it cannot push down (untranslatable, or translatable but not evaluated by the reader)
/// are refused when Strict and evaluated on the rows otherwise, while the others are still pushed down.
/// </summary>
public sealed class ParquetTypedRowsPolicyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"parquet-policy-{Guid.NewGuid():N}");
    private readonly string _path;
    private readonly Row[] _rows = [.. Enumerable.Range(0, 1000).Select(i => new Row(i, i % 7))];

    public ParquetTypedRowsPolicyTests()
    {
        Directory.CreateDirectory(_dir);
        _path = Path.Combine(_dir, "rows.parquet");
        using var df = DataFrame.From(_rows);
        df.WriteParquet(_path, statistics: true, rowGroupSize: 100);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static bool Odd(long value) => value % 2 == 1;

    public static TheoryData<string> Cases => ["untranslatable", "mixed", "betweenColumns"];

    private static Expression<Func<Row, bool>> Where(string name) => name switch
    {
        "untranslatable" => r => Odd(r.Id),
        "mixed" => r => r.Id < 150 && Odd(r.Id),
        _ => r => r.M > r.Id,
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Strict_RefusesWhatCannotBePushedDown(string name)
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict).BeginScope())
        {
            using var file = ParquetFile.Open(_path);
            Assert.Contains("could not be pushed down", Assert.Throws<NotSupportedException>(() => file.OpenReader(Where(name))).Message);
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void AllowAndWarn_EvaluateItOnTheRows_AndPushTheRestDown(string name)
    {
        Expression<Func<Row, bool>> where = Where(name);
        Row[] expected = [.. _rows.Where(where.Compile())];
        foreach (ClientEvaluationPolicy policy in (ClientEvaluationPolicy[])[ClientEvaluationPolicy.Allow, ClientEvaluationPolicy.Warn])
        {
            using (Pl.Config.SetClientEvaluationPolicy(policy).BeginScope())
            {
                using var file = ParquetFile.Open(_path);
                using ParquetReader<Row> reader = file.OpenReader(where);
                Assert.True(reader.FiltersExactly);
                Assert.Equal(expected, reader.ReadRows());
                // r.Id < 150 is still pushed down: two of the ten row groups are read.
                if (name == "mixed") Assert.Equal([0, 1], reader.Inner.RowGroupsRead);
            }
        }
    }

    [Fact]
    public void ARowLimit_CountsTheRowsDelivered_PastTheConjunctsEvaluatedOnTheRows()
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var file = ParquetFile.Open(_path);
            using ParquetReader<Row> reader = file.OpenReader<Row>(r => r.Id < 150 && Odd(r.Id), new ParquetReadOptions { MaxRows = 10 });
            Assert.Equal(Enumerable.Range(0, 10).Select(i => 2L * i + 1), reader.ReadRows().Select(r => r.Id));
        }
    }

    /// <summary>
    /// Conjuncts evaluated on the rows run on the threads that make them (several batches at once): the rows come in
    /// order, a row limit still counts the rows delivered, and a conjunct failing mid-batch fails the read after the
    /// rows before it, as on one thread.
    /// </summary>
    [Fact]
    public void ConjunctsOnTheRows_UnderParallelHydration_KeepTheOrder_TheLimit_AndTheRowsBeforeAFailure()
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var file = ParquetFile.Open(_path);
            var options = new ParquetReadOptions { BatchSize = 16, MaxDegreeOfParallelism = 8 };
            using (ParquetReader<Row> reader = file.OpenReader<Row>(r => Odd(r.Id), options))
                Assert.Equal(_rows.Where(r => Odd(r.Id)), reader.ReadRows());
            using (ParquetReader<Row> reader = file.OpenReader<Row>(r => Odd(r.Id), options with { MaxRows = 123 }))
                Assert.Equal(_rows.Where(r => Odd(r.Id)).Take(123), reader.ReadRows());
            using (ParquetReader<Row> reader = file.OpenReader<Row>(r => Odd(r.Id), options with { MaxRows = 123 }))
                Assert.Equal(_rows.Where(r => Odd(r.Id)).Take(123), reader.ReadRowsAsync().ToBlockingEnumerable());
            Assert.Equal(_rows.Where(r => Odd(r.Id)).Take(123), ParquetFile.ReadList<Row>(_path, r => Odd(r.Id), options with { MaxRows = 123 }));
            Assert.Equal(_rows.Where(r => Odd(r.Id)).Take(123),
                ParquetFile.ReadListAsync<Row>(_path, r => Odd(r.Id), options with { MaxRows = 123 }).GetAwaiter().GetResult());
            Assert.Equal("537", Assert.Throws<InvalidOperationException>(() => ParquetFile.ReadList<Row>(_path, r => FailsAt537(r.Id), options)).Message);
            foreach (int parallelism in (int[])[1, 8])
            {
                using ParquetReader<Row> reader = file.OpenReader<Row>(r => FailsAt537(r.Id), options with { MaxDegreeOfParallelism = parallelism });
                List<long> ids = [];
                var error = Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (Row row in reader.ReadRows()) ids.Add(row.Id);
                });
                Assert.Equal("537", error.Message);
                Assert.Equal(_rows.Where(r => r.Id < 537 && Odd(r.Id)).Select(r => r.Id), ids);
            }
        }
    }

    private static bool FailsAt537(long id) => id == 537 ? throw new InvalidOperationException("537") : Odd(id);

    [Fact]
    public void TheOptionsFilter_FiltersTheRows_WhenThePredicateIsAllEvaluatedOnTheRows()
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            var hint = new ParquetReadOptions
            {
                Filter = Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Lt, Kaminari.Samidare.IR.ScalarExpr.NewColumn("Id"),
                    Kaminari.Samidare.IR.ScalarExpr.NewLiteral(Kaminari.Samidare.IR.LiteralValue.NewInt64(50))),
            };
            using var file = ParquetFile.Open(_path);
            using ParquetReader<Row> reader = file.OpenReader<Row>(r => Odd(r.Id), hint);
            Assert.True(reader.FiltersExactly);
            Assert.Equal(_rows.Where(r => r.Id < 50 && Odd(r.Id)), reader.ReadRows()); // mid row group: rows filtered, not only groups
        }
    }

    public sealed record Row(long Id, long M);

    /// <summary>A dataset's typed rows follow the policy too, conjuncts over the dataset's columns pushed down.</summary>
    [Fact]
    public void DatasetRows_FollowThePolicy()
    {
        string[] paths = [Path.Combine(_dir, "rows.parquet")];
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict).BeginScope())
        {
            using var dataset = ParquetDataset.Open(paths);
            Assert.Contains("could not be pushed down", Assert.Throws<NotSupportedException>(() => dataset.OpenReader(Where("mixed"))).Message);
        }
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var dataset = ParquetDataset.Open(paths);
            using ParquetDatasetReader<Row> reader = dataset.OpenReader(Where("mixed"), new ParquetReadOptions { MaxRows = 10, MaxDegreeOfParallelism = 8 });
            Assert.True(reader.FiltersExactly);
            Assert.Equal(_rows.Where(Where("mixed").Compile()).Take(10), reader.ReadRows());
        }
    }

    private sealed record Located(long Id, string File);

    /// <summary>
    /// A conjunct over a dataset's file path is evaluated on the rows (no file holds the column): refused when Strict,
    /// the rows filtered all the same otherwise, the other conjuncts still pushed down.
    /// </summary>
    [Fact]
    public void DatasetRows_FilterTheFilePath_OnTheRows()
    {
        string[] paths = [Path.Combine(_dir, "rows.parquet")];
        var options = new ParquetDatasetOptions { FilePathColumn = "File" };
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict).BeginScope())
        {
            using var dataset = ParquetDataset.Open(paths, options);
            Assert.Throws<NotSupportedException>(() => dataset.OpenReader<Located>(r => r.File == paths[0]));
        }
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var dataset = ParquetDataset.Open(paths, options);
            using ParquetDatasetReader<Located> reader = dataset.OpenReader<Located>(r => r.File == paths[0] && r.Id < 5);
            Assert.Equal([.. Enumerable.Range(0, 5).Select(i => new Located(i, paths[0]))], reader.ReadRows());
            using ParquetDatasetReader<Located> none = dataset.OpenReader<Located>(r => r.File == "elsewhere" && r.Id < 5);
            Assert.Empty(none.ReadRows());
        }
    }
}
