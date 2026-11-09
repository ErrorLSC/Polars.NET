using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Linq.Tests;

/// <summary>Queries that the relational optimizer rewrites return what LINQ to Objects returns, in the same order.</summary>
public class LinqRelationalRewriteParityTests
{
    public record Sale(long Customer, int Date, long Ticket, long? Quantity, decimal Amount);
    public record Return(long ReturnedTicket);
    public record Customer(long CustomerId, string Name);
    public record Day(int DayId, int Year);

    private static readonly Sale[] Sales =
    [
        .. Enumerable.Range(0, 400).Select(i => new Sale(i % 7, i % 11, i % 13, i % 5 == 0 ? null : i % 9, i * 0.25m)),
    ];
    private static readonly Return[] Returns = [new(1), new(4), new(9)];
    private static readonly Customer[] Customers = [new(5, "eve"), new(1, "ann"), new(3, "bob"), new(6, "ann"), new(0, "cid")];
    private static readonly Day[] Days = [.. Enumerable.Range(0, 11).Select(i => new Day(i, 2000 + i % 3))];

    [Fact]
    public void Count_of_non_null_values_guards_a_sum()
    {
        var rows = Sales.Concat([new Sale(99, 0, 0, null, 0m), new Sale(99, 1, 0, null, 0m)]).ToArray();
        using var df = DataFrame.FromRows(rows);

        var native = df.AsQueryable(rows)
            .GroupBy(s => s.Customer)
            .Select(g => new { g.Key, N = g.Count(s => s.Quantity != null), Total = g.Count(s => s.Quantity != null) > 0 ? g.Sum(s => s.Quantity) : null })
            .ToList();
        var expected = rows
            .GroupBy(s => s.Customer)
            .Select(g => new { g.Key, N = g.Count(s => s.Quantity != null), Total = g.Count(s => s.Quantity != null) > 0 ? g.Sum(s => s.Quantity) : null })
            .ToList();

        Assert.Equal(expected, native);
        Assert.Contains(native, x => x.Key == 99 && x.N == 0 && x.Total == null);
    }

    [Fact]
    public void Unmatched_rows_of_a_left_join_before_a_filtered_join()
    {
        using var dfS = DataFrame.FromRows(Sales);
        using var dfR = DataFrame.FromRows(Returns);
        using var dfD = DataFrame.FromRows(Days);

        var query = from s in dfS.AsQueryable(Sales)
                      join r in dfR.AsQueryable(Returns) on s.Ticket equals r.ReturnedTicket into rs
                      from r in rs.DefaultIfEmpty()
                      join d in dfD.AsQueryable(Days).Where(d => d.Year == 2001) on s.Date equals d.DayId
                      where r == null
                      group s by new { d.Year, s.Customer } into g
                      select new { g.Key.Year, g.Key.Customer, N = g.LongCount() };
        var expected = (from s in Sales
                        join r in Returns on s.Ticket equals r.ReturnedTicket into rs
                        from r in rs.DefaultIfEmpty()
                        join d in Days.Where(d => d.Year == 2001) on s.Date equals d.DayId
                        where r == null
                        group s by new { d.Year, s.Customer } into g
                        select new { g.Key.Year, g.Key.Customer, N = g.LongCount() }).ToList();

        Assert.Contains("ANTI JOIN", query.Explain());
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Grouping_on_keys_of_two_dimensions_keeps_first_appearance_order()
    {
        using var dfS = DataFrame.FromRows(Sales);
        using var dfC = DataFrame.FromRows(Customers);
        using var dfD = DataFrame.FromRows(Days);

        var query = from c in dfC.AsQueryable(Customers)
                      join s in dfS.AsQueryable(Sales) on c.CustomerId equals s.Customer
                      join d in dfD.AsQueryable(Days).Where(d => d.Year >= 2001) on s.Date equals d.DayId
                      group s by new { c.Name, d.Year } into g
                      select new { g.Key.Name, g.Key.Year, Total = g.Sum(s => s.Amount) };
        var expected = (from c in Customers
                        join s in Sales on c.CustomerId equals s.Customer
                        join d in Days.Where(d => d.Year >= 2001) on s.Date equals d.DayId
                        group s by new { c.Name, d.Year } into g
                        select new { g.Key.Name, g.Key.Year, Total = g.Sum(s => s.Amount) }).ToList();

        Assert.Equal(2, System.Text.RegularExpressions.Regex.Count(query.Explain(), "AGGREGATE"));
        Assert.Equal(expected, query.ToList());
    }
}
