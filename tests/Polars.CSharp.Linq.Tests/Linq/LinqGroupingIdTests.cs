#nullable enable
using System.Linq.Expressions;
using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests.Linq;

/// <summary>
/// GROUPING_ID and Grouping() in ROLLUP / CUBE / GROUPING SETS selectors, alone and mixed with keys and aggregates.
/// Results are compared in order with a LINQ to Objects reference: one block per level, groups in first-key order.
/// </summary>
public class LinqGroupingIdTests
{
    private const string NativePushdown = "100% Native Pushdown";

    public record GsRow(string Dept, string? Team, int Amount);

    // A real null Team group in "Eng" tells a null key apart from a rolled-up one.
    private static readonly GsRow[] Rows =
    [
        new("Sales", "A", 10),
        new("Eng", "B", 30),
        new("Sales", "B", 20),
        new("Eng", null, 40),
        new("Sales", "A", 5),
    ];

    private record RefRow(string? Dept, string? Team, int Sum, int Count, int GDept, int GTeam);

    /// <summary>One block per level (key subset over Dept = 0, Team = 1), groups in first-key order.</summary>
    private static List<RefRow> Reference(params int[][] levels) =>
        levels.SelectMany(level =>
        {
            bool dept = level.Contains(0), team = level.Contains(1);
            return Rows
                .GroupBy(r => (Dept: dept ? r.Dept : null, Team: team ? r.Team : null))
                .Select(g => new RefRow(g.Key.Dept, g.Key.Team, g.Sum(r => r.Amount), g.Count(), dept ? 0 : 1, team ? 0 : 1));
        }).ToList();

    private static readonly int[][] RollupLevels = [[0, 1], [0], []];
    private static readonly int[][] CubeLevels = [[0, 1], [0], [1], []];

    [Fact]
    public void GroupingId_Over_All_Keys_In_Rollup()
    {
        using var df = DataFrame.From(Rows);
        var query = df.AsQueryable<GsRow>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp((key, g) => new { key.Dept, key.Team, Total = g.Sum(x => x.Amount), Gid = key.GroupingId() });

        Assert.Contains(NativePushdown, query.Explain());
        var expected = Reference(RollupLevels).Select(r => new { r.Dept, r.Team, Total = r.Sum, Gid = r.GDept * 2 + r.GTeam });
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void GroupingId_With_Selected_Columns_Orders_Bits_By_The_Selector()
    {
        using var df = DataFrame.From(Rows);
        var query = df.AsQueryable<GsRow>()
            .GroupBy(x => new { x.Dept, x.Team })
            .Cube()
            .Select(g => new
            {
                g.Key.Dept,
                g.Key.Team,
                All = g.Key.GroupingId(),
                Reversed = g.Key.GroupingId(k => new { k.Team, k.Dept }),
                TeamOnly = g.Key.GroupingId(k => k.Team),
                None = g.Key.GroupingId(k => new { }),
            });

        Assert.Contains(NativePushdown, query.Explain());
        var expected = Reference(CubeLevels).Select(r => new
        {
            r.Dept,
            r.Team,
            All = r.GDept * 2 + r.GTeam,
            Reversed = r.GTeam * 2 + r.GDept,
            TeamOnly = r.GTeam,
            None = 0,
        });
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void GroupingId_In_GroupingSets_And_Filtered_Downstream()
    {
        using var df = DataFrame.From(Rows);
        var query = df.AsQueryable<GsRow>()
            .GroupBy(x => new { x.Dept, x.Team })
            .GroupingSets(
                [k => new { k.Team }, k => new { k.Dept }, k => new { }],
                (key, g) => new { key.Dept, key.Team, Count = g.Count(), Gid = key.GroupingId() })
            .Where(r => r.Gid != 3)
            .OrderBy(r => r.Gid);

        Assert.Contains(NativePushdown, query.Explain());
        var expected = Reference([1], [0], [])
            .Select(r => new { r.Dept, r.Team, r.Count, Gid = r.GDept * 2 + r.GTeam })
            .Where(r => r.Gid != 3)
            .OrderBy(r => r.Gid);
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Grouping_Mixes_With_Keys_And_Aggregates_In_One_Member()
    {
        using var df = DataFrame.From(Rows);
        var query = df.AsQueryable<GsRow>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp((key, g) => new
            {
                // Hierarchy parent as in TPC-DS Q36/Q70/Q86, computed in the selector itself.
                Parent = key.Team.Grouping() == 0 ? key.Dept : null,
                Lochierarchy = key.Dept.Grouping() + key.Team.Grouping(),
                // An aggregate no other member projects, and one another member projects.
                Weighted = g.Sum(x => x.Amount) * (1 + key.GroupingId()),
                Mean = g.Sum(x => x.Amount) / g.Count(),
                Total = g.Sum(x => x.Amount),
                Label = key.GroupingId(k => k.Team) == 1 ? "subtotal" : key.Dept + "/" + (key.Team ?? "-"),
            });

        Assert.Contains(NativePushdown, query.Explain());
        var expected = Reference(RollupLevels).Select(r => new
        {
            Parent = r.GTeam == 0 ? r.Dept : null,
            Lochierarchy = r.GDept + r.GTeam,
            Weighted = r.Sum * (1 + r.GDept * 2 + r.GTeam),
            Mean = r.Sum / r.Count,
            Total = r.Sum,
            Label = r.GTeam == 1 ? "subtotal" : r.Dept + "/" + (r.Team ?? "-"),
        });
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Grouping_Inside_A_Window_Partition_Of_The_Selector()
    {
        using var df = DataFrame.From(Rows);
        var query = df.AsQueryable<GsRow>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp((key, g) => new
            {
                key.Dept,
                key.Team,
                Total = g.Sum(x => x.Amount),
                Rank = (long)g.Sum(x => x.Amount).Rank(RankMethod.Min, true)
                    .Over(new { lo = key.Dept.Grouping() + key.Team.Grouping(), parent = key.Team.Grouping() == 0 ? key.Dept : null }),
                Share = g.Sum(x => x.Amount) * 100 / g.Sum(x => x.Amount).Sum().Over(key.GroupingId()),
            });

        Assert.Contains(NativePushdown, query.Explain());
        var reference = Reference(RollupLevels);
        var expected = reference.Select(r => new
        {
            r.Dept,
            r.Team,
            Total = r.Sum,
            Rank = 1L + reference.Count(o =>
                o.GDept + o.GTeam == r.GDept + r.GTeam
                && (o.GTeam == 0 ? o.Dept : null) == (r.GTeam == 0 ? r.Dept : null)
                && o.Sum > r.Sum),
            Share = r.Sum * 100 / reference.Where(o => o.GDept == r.GDept && o.GTeam == r.GTeam).Sum(o => o.Sum),
        });
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void GroupingId_On_A_Scalar_Key()
    {
        using var df = DataFrame.From(Rows);
        var twoParam = df.AsQueryable<GsRow>()
            .GroupBy(x => x.Team)
            .RollUp((team, g) => new { Team = team, Gid = team.GroupingId(), G = team.Grouping(), Tagged = team.Grouping() == 1 ? "all" : team });
        var oneParam = df.AsQueryable<GsRow>()
            .GroupBy(x => x.Team)
            .Rollup()
            .Select(g => new { Team = g.Key, Gid = g.Key.GroupingId(), G = g.Key.Grouping(), Tagged = g.Key.Grouping() == 1 ? "all" : g.Key });

        var expected = Rows.GroupBy(r => r.Team).Select(g => new { Team = g.Key, Gid = 0, G = 0, Tagged = g.Key })
            .Append(new { Team = (string?)null, Gid = 1, G = 1, Tagged = (string?)"all" });
        foreach (var query in new[] { twoParam, oneParam })
        {
            Assert.Contains(NativePushdown, query.Explain());
            Assert.Equal(expected, query.ToList());
        }
    }

    /// <summary>
    /// The parameterless operators type-check only before a Select; a hand-built tree that continues them with
    /// anything else is rejected, never run with the operator dropped (the subtotal levels lost) or reordered.
    /// </summary>
    [Fact]
    public void A_Grouping_Sets_Operator_Not_Followed_By_Select_Is_Rejected()
    {
        using var df = DataFrame.From(Rows);
        var grouped = df.AsQueryable<GsRow>().GroupBy(x => new { x.Dept, x.Team });

        // The operator's own node, as the typed API builds it (the argument of its Select).
        static Expression OperatorNode<T>(IQueryable<T> selected) => ((MethodCallExpression)selected.Expression).Arguments[0];
        (string, Expression)[] operators = [
            ("RollUp", OperatorNode(grouped.RollUp().Select(g => g.Key))),
            ("Cube", OperatorNode(grouped.Cube().Select(g => g.Key))),
            ("GroupingSets", OperatorNode(grouped.GroupingSets(k => new { k.Dept }, k => new { }).Select(g => g.Key))),
            ("GroupingSets", OperatorNode(grouped.GroupingSets([["Dept"], []]).Select(g => g.Key))),
        ];

        foreach (var (name, node) in operators)
        {
            var elem = node.Type.GetGenericArguments()[0];

            // A terminal straight after the operator.
            var count = Assert.Throws<InvalidOperationException>(() => grouped.Provider.Execute<int>(
                Expression.Call(typeof(Queryable), nameof(Queryable.Count), [elem], node)));
            Assert.Contains($"'{name}()' must be followed immediately by '.Select(...)'", count.Message);

            // A Where between the operator and its Select.
            var g = Expression.Parameter(elem, "g");
            var where = Expression.Call(typeof(Queryable), nameof(Queryable.Where), [elem], node,
                Expression.Quote(Expression.Lambda(Expression.Constant(true), g)));
            var select = Expression.Call(typeof(Queryable), nameof(Queryable.Select), [elem, typeof(int)], where,
                Expression.Quote(Expression.Lambda(Expression.Constant(1), g)));
            var reordered = Assert.Throws<InvalidOperationException>(() => grouped.Provider.CreateQuery<int>(select).ToList());
            Assert.Contains($"'{name}()' must be followed immediately by '.Select(...)'", reordered.Message);
        }
    }

    [Fact]
    public void Query_Syntax_Selects_From_A_Grouping_Sets_Operator()
    {
        using var df = DataFrame.From(Rows);
        var query = from g in df.AsQueryable<GsRow>().GroupBy(x => new { x.Dept, x.Team }).Cube()
                    select new { g.Key.Dept, g.Key.Team, Total = g.Sum(x => x.Amount), Gid = g.Key.GroupingId() };

        Assert.Contains(NativePushdown, query.Explain());
        var expected = Reference(CubeLevels).Select(r => new { r.Dept, r.Team, Total = r.Sum, Gid = r.GDept * 2 + r.GTeam });
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void A_Key_Without_A_Member_Does_Not_Clash_With_A_Member_Of_Its_Name()
    {
        using var df = DataFrame.From(Rows);
        // Team is a key no member holds, and a member named Team holds an aggregate.
        var query = df.AsQueryable<GsRow>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp((key, g) => new { key.Dept, Team = g.Count(), Gid = key.GroupingId() });

        Assert.Contains(NativePushdown, query.Explain());
        var expected = Reference(RollupLevels).Select(r => new { r.Dept, Team = r.Count, Gid = r.GDept * 2 + r.GTeam });
        Assert.Equal(expected, query.ToList());
    }
}
