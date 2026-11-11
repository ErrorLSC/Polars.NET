using BenchmarkDotNet.Attributes;
using Polars.CSharp;

namespace Polars.Benchmarks;

// Managed rows and columns into a native DataFrame, through each ingestion path.
//
// Filter: --filter '*RowIngestion*'

public class RowIngestionRow
{
    public int Id { get; set; }
    public long Amount { get; set; }
    public double Price { get; set; }
    public string? Name { get; set; }
    public bool Flag { get; set; }
    public DateTime At { get; set; }
    public int? Optional { get; set; }
}

[MemoryDiagnoser]
public class RowIngestionBenchmarks
{
    private RowIngestionRow[] _rows = [];
    private List<Dictionary<string, object?>> _dicts = [];

    [Params(100_000)]
    public int N { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var start = new DateTime(2024, 1, 1);
        _rows = [.. Enumerable.Range(0, N).Select(i => new RowIngestionRow
        {
            Id = i,
            Amount = i * 7L,
            Price = i / 3.0,
            Name = i % 10 == 0 ? null : "name" + (i % 100),
            Flag = i % 2 == 0,
            At = start.AddMinutes(i),
            Optional = i % 5 == 0 ? null : i,
        })];

        _dicts = [.. _rows.Select(r => new Dictionary<string, object?>
        {
            ["Id"] = r.Id,
            ["Amount"] = r.Amount,
            ["Price"] = r.Price,
            ["Name"] = r.Name,
            ["Flag"] = r.Flag,
        })];
    }

    private IEnumerable<RowIngestionRow> Stream()
    {
        foreach (var row in _rows) yield return row;
    }

    [Benchmark]
    public long FromRows_Array()
    {
        using var df = DataFrame.FromRows(_rows);
        return df.Height;
    }

    [Benchmark]
    public long FromRows_Streaming()
    {
        using var df = DataFrame.FromRows(Stream());
        return df.Height;
    }

    [Benchmark]
    public long FromDicts()
    {
        using var df = DataFrame.FromDicts(_dicts);
        return df.Height;
    }

    [Benchmark]
    public long FromColumns_Soa()
    {
        using var df = DataFrame.FromColumns(new
        {
            Id = _rows.Select(r => r.Id).ToArray(),
            Price = _rows.Select(r => r.Price).ToArray(),
            Name = _rows.Select(r => r.Name).ToArray(),
        });
        return df.Height;
    }
}
