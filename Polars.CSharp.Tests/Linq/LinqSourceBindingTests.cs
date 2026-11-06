using Polars.CSharp.Linq;
using Polars.NET.Linq.IR;

namespace Polars.CSharp.Tests;

public record BindEmp(string Name, string Dept, int Salary);
public record BindDept(string Dept, string Region);

/// <summary>
/// Compiled LogicalPlans reference their sources by slot only; the bindings travel beside the plan.
/// Grafted sub-plans and scalar terminals renumber slots so that sources never collide.
/// </summary>
public class LinqSourceBindingTests
{
    private static readonly BindEmp[] EmpsA =
    [
        new("Alice", "Eng", 100),
        new("Bob", "Ops", 80),
    ];

    private static readonly BindEmp[] EmpsB =
    [
        new("Carol", "Eng", 120),
        new("Dan", "Sales", 70),
        new("Erin", "Ops", 90),
    ];

    private static readonly BindDept[] Depts =
    [
        new("Eng", "EU"),
        new("Ops", "US"),
    ];

    /// Slot indices of every Scan in the plan, in tree order.
    private static List<int> ScanSlots(LogicalPlan plan)
    {
        var slots = new List<int>();
        void Walk(LogicalPlan p)
        {
            if (p is LogicalPlan.Scan { source: LogicalPlanSource.Slot slot })
                slots.Add(slot.index);
            foreach (var child in LogicalPlanOps.getInputs(p))
                Walk(child);
        }
        Walk(plan);
        return slots;
    }

    private static CompiledLogicalQuery Compile<T>(IQueryable<T> query) =>
        query.CompileLogicalQuery() ?? throw new InvalidOperationException("Not a Polars query.");

    [Fact]
    public void Root_Source_Is_Slot_Zero()
    {
        using var df = DataFrame.FromRows(EmpsA);
        var compiled = Compile(df.AsQueryable<BindEmp>().Where(e => e.Salary > 90).Select(e => e.Name));

        Assert.Equal([0], ScanSlots(compiled.Plan));
        Assert.True(Assert.Single(compiled.Sources).IsTable);
    }

    [Fact]
    public void InMemory_Join_Source_Gets_Its_Own_Slot()
    {
        using var df = DataFrame.FromRows(EmpsB);
        var depts = Depts.ToList();
        var query = df.AsQueryable<BindEmp>()
            .Join(depts, e => e.Dept, d => d.Dept, (e, d) => new { e.Name, d.Region });

        var compiled = Compile(query);
        Assert.Equal([0, 1], ScanSlots(compiled.Plan).Order());
        Assert.True(compiled.Sources[0].IsTable);
        Assert.True(compiled.Sources[1].IsInMemory);

        var expected = EmpsB.Join(Depts, e => e.Dept, d => d.Dept, (e, d) => $"{e.Name}:{d.Region}").Order();
        Assert.Equal(expected, query.AsEnumerable().Select(x => $"{x.Name}:{x.Region}").Order());
    }

    [Fact]
    public void Grafted_Subplans_Are_Renumbered_Without_Collisions()
    {
        using var dfA = DataFrame.FromRows(EmpsA);
        using var dfB = DataFrame.FromRows(EmpsB);
        using var dfDepts = DataFrame.FromRows(Depts);

        // b's own plan scans two sources (slots 0 and 1); grafted after a's root they become 1 and 2.
        var b = dfB.AsQueryable<BindEmp>()
            .Join(dfDepts.AsQueryable<BindDept>(), e => e.Dept, d => d.Dept, (e, d) => new BindEmp(e.Name, d.Region, e.Salary));
        var query = dfA.AsQueryable<BindEmp>().Concat(b);

        var compiled = Compile(query);
        Assert.Equal([0, 1, 2], ScanSlots(compiled.Plan).Order());
        Assert.Equal(3, compiled.Sources.Length);
        Assert.All(compiled.Sources, s => Assert.True(s.IsTable));

        var expected = EmpsA.Concat(EmpsB.Join(Depts, e => e.Dept, d => d.Dept, (e, d) => new BindEmp(e.Name, d.Region, e.Salary)))
            .Select(e => e.ToString()).Order();
        Assert.Equal(expected, query.AsEnumerable().Select(e => e.ToString()).Order());
    }

    [Fact]
    public void Same_Query_Shape_Over_Different_Frames_Compiles_To_Identical_Plans()
    {
        using var dfA = DataFrame.FromRows(EmpsA);
        using var dfB = DataFrame.FromRows(EmpsB);

        static IQueryable<string> Shape(IQueryable<BindEmp> q) =>
            q.Where(e => e.Salary >= 90).OrderBy(e => e.Name).Select(e => e.Name);

        var compiledA = Compile(Shape(dfA.AsQueryable<BindEmp>()));
        var compiledB = Compile(Shape(dfB.AsQueryable<BindEmp>()));

        // The plans differ only in what slot 0 is bound to.
        Assert.Equal(LogicalPlanOps.formatPlan(compiledA.Plan), LogicalPlanOps.formatPlan(compiledB.Plan));
        Assert.NotSame(compiledA.Sources[0], compiledB.Sources[0]);

        Assert.Equal(["Alice"], Shape(dfA.AsQueryable<BindEmp>()).ToList());
        Assert.Equal(["Carol", "Erin"], Shape(dfB.AsQueryable<BindEmp>()).ToList());
    }

    [Fact]
    public void Scalar_Terminals_Combine_Sources_Of_Both_Operands()
    {
        using var dfA = DataFrame.FromRows(EmpsA);
        using var dfA2 = DataFrame.FromRows(EmpsA);
        using var dfB = DataFrame.FromRows(EmpsB);

        var a = dfA.AsQueryable<BindEmp>();
        Assert.True(a.SequenceEqual(dfA2.AsQueryable<BindEmp>()));
        Assert.False(a.SequenceEqual(dfB.AsQueryable<BindEmp>()));
        Assert.Equal(5, a.Concat(dfB.AsQueryable<BindEmp>()).Count());
        Assert.Equal(390, a.Concat(dfB.AsQueryable<BindEmp>()).Where(e => e.Salary > 75).Sum(e => e.Salary));
    }
}
