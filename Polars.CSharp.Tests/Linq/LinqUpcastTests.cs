using System.Linq.Expressions;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public interface IUpNamed { string Name { get; } }
public record UpRow(int Id, string Name, int Age) : IUpNamed;

/// <summary>
/// Queries whose elements are upcast (to object, an interface) keep the actual values, as LINQ to Objects does:
/// they run narrowed to the actual element type, and the elements are then boxed or upcast.
/// </summary>
public class LinqUpcastTests
{
    private static readonly UpRow[] Rows = [new(1, "a", 30), new(2, "b", 25), new(3, "c", 40)];

    private static void AssertMatches<T>(Func<IQueryable<UpRow>, IQueryable<T>> query)
    {
        using var df = DataFrame.FromRows(Rows);
        Assert.Equal(query(Rows.AsQueryable()).ToList(), query(df.AsQueryable<UpRow>()).ToList());
    }

    private static void AssertTerminal<T>(Expression<Func<IQueryable<UpRow>, T>> terminal)
    {
        using var df = DataFrame.FromRows(Rows);
        var run = terminal.Compile();
        Assert.Equal(run(Rows.AsQueryable()), run(df.AsQueryable<UpRow>()));
    }

    [Fact]
    public void Elements_Upcast_To_Object_Keep_Their_Values()
    {
        AssertMatches(q => q.Select(r => (object)r));
        AssertMatches(q => q.Select(r => (object)r.Id));
        AssertMatches(q => q.Select(r => (object)r.Name));
        AssertMatches(q => q.Select(r => (object)new { r.Id, r.Name }));
        AssertMatches(q => q.Select(r => new { r.Id, r.Name }).Select(x => (object)x));
        AssertMatches(q => q.Select(r => (object)Tuple.Create(r.Id, r.Name)));
        AssertMatches(q => q.Select(r => r as object));
    }

    [Fact]
    public void Cast_And_Interface_Upcasts()
    {
        AssertMatches(q => q.Cast<object>());
        AssertMatches(q => q.Select(r => r.Name).Cast<object>());
        AssertMatches(q => q.Select(r => (IUpNamed)r));
        AssertMatches(q => q.Select(r => (IUpNamed)r).Select(x => (object)x));
    }

    [Fact]
    public void Operators_After_An_Upcast_Run_Over_The_Actual_Elements()
    {
        AssertMatches(q => q.Select(r => (object)r.Id).Where(x => x != null));
        AssertMatches(q => q.Select(r => (object)r).Take(2));
        AssertMatches(q => q.Select(r => (object)r.Name).Select(x => x.ToString()));
    }

    [Fact]
    public void Single_Element_Terminals()
    {
        AssertTerminal(q => q.Select(r => (object)r).First());
        AssertTerminal(q => q.Where(r => r.Id == 2).Select(r => (object)r.Name).Single());
        AssertTerminal(q => q.Select(r => (object)r.Age).ElementAt(2));
        AssertTerminal(q => q.Select(r => (object)new { r.Id }).Last());
        AssertTerminal(q => q.Select(r => (object)r.Id).FirstOrDefault());
        AssertTerminal(q => q.Where(r => r.Id > 99).Select(r => (object)r.Id).FirstOrDefault());
        AssertTerminal(q => q.Where(r => r.Id > 99).Select(r => (object)r).FirstOrDefault());
        AssertTerminal(q => q.Select(r => (object)r).Count());
    }

    [Fact]
    public async Task Frames_And_Async_Enumeration()
    {
        using var df = DataFrame.FromRows(Rows);

        using var frame = df.AsQueryable<UpRow>().Select(r => (object)new { r.Id, r.Name }).ToDataFrame();
        Assert.Equal(["Id", "Name"], frame.Columns);
        Assert.Equal(3, frame.Height);

        Assert.Equal([1, 2, 3], await df.AsQueryable<UpRow>().Select(r => (object)r.Id).ToListAsync());
    }
}
