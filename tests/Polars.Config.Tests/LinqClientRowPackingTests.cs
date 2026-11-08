using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Polars.NET.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public record CrEmp(string Name, string Dept, int Salary);
public record CrDept(string Dept, string Name, string Floor);

/// <summary>
/// A client row step (predicate or projection) over anonymous rows runs on rows materialized from the native
/// plan, with the entities of a join's transparent identifier packed first, instead of falling back entirely.
/// </summary>
public class LinqClientRowPackingTests
{
    private static readonly CrEmp[] Emps = [new("a", "X", 1), new("b", "Y", 2), new("c", "Y", 4)];
    private static readonly CrDept[] Depts = [new("X", "dx", "1"), new("Y", "dy", "2")];

    private static bool IsEven(int value) => value % 2 == 0;

    private static string Label(CrEmp emp, CrDept dept) => $"{emp.Name}/{dept.Name}@{dept.Floor}";

    private static IDisposable AllowClientEvaluation() =>
        Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope();

    private static void AssertRowStep<T>(IQueryable<T> query, IEnumerable<T> expected)
    {
        var inspection = ((IPolarsExplainable)query).InspectPlan(true);
        Assert.False(inspection.IsFullFallback, inspection.FormatReport());
        Assert.Equal(expected.Select(r => r!.ToString()).Order(), query.ToList().Select(r => r!.ToString()).Order());
    }

    [Fact]
    public void Client_Predicate_Over_Join_Pairs()
    {
        using var _ = AllowClientEvaluation();
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);

        AssertRowStep(
            e.AsQueryable<CrEmp>().Join(d.AsQueryable<CrDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Where(t => IsEven(t.x.Salary)),
            Emps.Join(Depts, x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Where(t => IsEven(t.x.Salary)));
    }

    [Fact]
    public void Client_Projection_Over_Join_Pairs()
    {
        using var _ = AllowClientEvaluation();
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);

        AssertRowStep(
            e.AsQueryable<CrEmp>().Join(d.AsQueryable<CrDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Select(t => Label(t.x, t.y)),
            Emps.Join(Depts, x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Select(t => Label(t.x, t.y)));
    }

    [Fact]
    public void Client_Predicate_Over_Left_Join_Pairs_Sees_Null_Entities()
    {
        using var _ = AllowClientEvaluation();
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts.Take(1).ToArray());

        AssertRowStep(
            e.AsQueryable<CrEmp>().LeftJoin(d.AsQueryable<CrDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Where(t => t.y == null || IsEven(t.x.Salary)),
            Emps.LeftJoin(Depts.Take(1), x => x.Dept, y => y.Dept, (x, y) => new { x, y }).Where(t => t.y == null || IsEven(t.x.Salary)));
    }

    [Fact]
    public void Client_Predicate_Over_Tuple_Pairs()
    {
        using var _ = AllowClientEvaluation();
        using var udf = Pl.Config.SetDisallowUdfPushdown(true).BeginScope();
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);

        AssertRowStep(
            e.AsQueryable<CrEmp>().Join(d.AsQueryable<CrDept>(), x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y)).Where(t => IsEven(t.Item1.Salary)),
            Emps.Join(Depts, x => x.Dept, y => y.Dept, (x, y) => Tuple.Create(x, y)).Where(t => IsEven(t.Item1.Salary)));
    }

    private enum Parity { Even, Odd }

    private static Parity ParityOf(int value) => value % 2 == 0 ? Parity.Even : Parity.Odd;

    [Fact]
    public void Client_Predicate_Over_An_Enum_Returning_Method()
    {
        using var _ = AllowClientEvaluation();
        using var e = DataFrame.FromRows(Emps);

        AssertRowStep(
            e.AsQueryable<CrEmp>().Where(x => ParityOf(x.Salary) == Parity.Even).Select(x => x.Name),
            Emps.Where(x => ParityOf(x.Salary) == Parity.Even).Select(x => x.Name));
    }

    [Fact]
    public void Client_Projected_Enum_Lands_In_An_Enum_Column()
    {
        using var _ = AllowClientEvaluation();
        using var e = DataFrame.FromRows(Emps);

        using var result = e.AsQueryable<CrEmp>().Select(x => new { x.Name, Parity = ParityOf(x.Salary) }).ToDataFrame();

        Assert.True(result.Schema["Parity"].IsEnum);
        Assert.Equal(
            Emps.Select(x => ParityOf(x.Salary).ToString()).ToArray(),
            result["Parity"].Cast(DataType.String).ToArray<string>());
    }

    [Fact]
    public void Client_Predicate_Over_Flat_Anonymous_Rows()
    {
        using var _ = AllowClientEvaluation();
        using var e = DataFrame.FromRows(Emps);

        AssertRowStep(
            e.AsQueryable<CrEmp>().Select(x => new { x.Name, x.Salary }).Where(r => IsEven(r.Salary)),
            Emps.Select(x => new { x.Name, x.Salary }).Where(r => IsEven(r.Salary)));
    }
}
