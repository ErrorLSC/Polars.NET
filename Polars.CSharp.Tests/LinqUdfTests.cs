#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core;
using Xunit;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public static partial class TestUdfMethods
{
    public static bool IsSenior(int age) => age >= 30;

    public static bool AgeAbove(int age, int limit) => age >= limit;

    public static string MaskName(string name) => string.IsNullOrEmpty(name) ? "" : name[..1] + "***";

    public static bool Qualifies(int age, int salary) => age >= 25 && salary >= 50000;

    public static double ComputeRatio(int age, int salary) => (double)salary / age;

    public static double TotalComp(int salary, double bonusRate, int age) =>
        salary * (1.0 + bonusRate) + (age >= 30 ? 5000.0 : 0.0);

    public static string Shout(this string s) => s.ToUpperInvariant() + "!";

    public static int SafeLen(string? s) => s?.Length ?? 0;

    private static readonly HashSet<string> ApprovedStaffRegistry = ["Alice", "Charlie"];

    public static bool CheckSecurity(string name, int salary) =>
        ApprovedStaffRegistry.Contains(name) && salary > 45000;

    public static bool CheckSecurityEntity(UdfEmployee e) =>
        CheckSecurity(e.Name, e.Salary);

    [GeneratedRegex(@"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).+$")]
    public static partial Regex ComplexLookaroundRegex();
}

public record UdfEmployee(string Name, int Age, int Salary);
public record UdfNullableRecord(string? Description, int? Score);
public record UdfUserAccount(string User, string Password);

public class LinqUdfTests
{
    private static DataFrame CreateSampleDf()
    {
        return DataFrame.FromRows([
            new UdfEmployee("Alice", 30, 60000),
            new UdfEmployee("Bob", 25, 45000),
            new UdfEmployee("Charlie", 40, 80000),
            new UdfEmployee("David", 22, 35000)
        ]);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_SingleColumn_Udf_In_Where()
    {
        using var df = CreateSampleDf();

        // Where with custom single-column static method
        var results = df.AsQueryable<UdfEmployee>()
            .Where(x => TestUdfMethods.IsSenior(x.Age))
            .OrderBy(x => x.Name)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_SingleColumn_Udf_With_Captured_Variable_In_Where()
    {
        using var df = CreateSampleDf();

        int threshold = 35;
        var results = df.AsQueryable<UdfEmployee>()
            .Where(x => TestUdfMethods.AgeAbove(x.Age, threshold))
            .ToList();

        Assert.Single(results);
        Assert.Equal("Charlie", results[0].Name);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_SingleColumn_Udf_In_Select()
    {
        using var df = CreateSampleDf();

        using var resultDf = df.AsQueryable<UdfEmployee>()
            .Select(x => new
            {
                Masked = TestUdfMethods.MaskName(x.Name),
                x.Salary
            })
            .OrderByDescending(x => x.Salary)
            .ToDataFrame();

        Assert.Equal(4, resultDf.Height);
        Assert.Equal("C***", resultDf.Column("Masked").GetValue<string>(0));
        Assert.Equal("A***", resultDf.Column("Masked").GetValue<string>(1));
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_MultiColumn_Udf_In_Where()
    {
        using var df = CreateSampleDf();

        // Same-table multi-column UDF: age >= 25 && salary >= 50000
        var results = df.AsQueryable<UdfEmployee>()
            .Where(x => TestUdfMethods.Qualifies(x.Age, x.Salary))
            .OrderBy(x => x.Age)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);   // 30, 60k
        Assert.Equal("Charlie", results[1].Name); // 40, 80k
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_MultiColumn_Udf_In_Select()
    {
        using var df = CreateSampleDf();

        using var resultDf = df.AsQueryable<UdfEmployee>()
            .Where(x => x.Name == "Alice")
            .Select(x => new
            {
                x.Name,
                Ratio = TestUdfMethods.ComputeRatio(x.Age, x.Salary)
            })
            .ToDataFrame();

        Assert.Equal(1, resultDf.Height);
        Assert.Equal("Alice", resultDf.Column("Name").GetValue<string>(0));
        Assert.Equal(2000.0, resultDf.Column("Ratio").GetValue<double>(0)); // 60000 / 30
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_MultiColumn_Udf_With_Captured_Closure()
    {
        using var df = CreateSampleDf();

        double bonusRate = 0.10;
        using var resultDf = df.AsQueryable<UdfEmployee>()
            .Where(x => x.Name == "Alice")
            .Select(x => new
            {
                x.Name,
                Comp = TestUdfMethods.TotalComp(x.Salary, bonusRate, x.Age)
            })
            .ToDataFrame();

        Assert.Equal(1, resultDf.Height);
        // Alice: 60000 * 1.1 + 5000 = 66000 + 5000 = 71000
        Assert.Equal(71000.0, resultDf.Column("Comp").GetValue<double>(0));
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_Extension_Method_Udf()
    {
        using var df = CreateSampleDf();

        var results = df.AsQueryable<UdfEmployee>()
            .Where(x => x.Name == "Bob")
            .Select(x => new { Shouted = x.Name.Shout() })
            .ToList();

        Assert.Single(results);
        Assert.Equal("BOB!", results[0].Shouted);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_Delegate_Invocation_Udf()
    {
        using var df = CreateSampleDf();

        Func<int, int, bool> filterFunc = (age, salary) => age > 25 && salary > 50000;

        var results = df.AsQueryable<UdfEmployee>()
            .Where(x => filterFunc(x.Age, x.Salary))
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "Alice");
        Assert.Contains(results, r => r.Name == "Charlie");
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_Udf_Nullable_Handling()
    {
        using var df = DataFrame.FromRows([
            new UdfNullableRecord("Hello", 10),
            new UdfNullableRecord(null, null),
            new UdfNullableRecord("World", 20)
        ]);

        var results = df.AsQueryable<UdfNullableRecord>()
            .Select(x => new
            {
                Len = TestUdfMethods.SafeLen(x.Description),
                x.Score
            })
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(5, results[0].Len);
        Assert.Equal(10, results[0].Score);
        Assert.Equal(0, results[1].Len);
        Assert.Null(results[1].Score);
        Assert.Equal(5, results[2].Len);
        Assert.Equal(20, results[2].Score);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_Udf_In_OrderBy()
    {
        using var df = CreateSampleDf();

        // Order by multi-column UDF ratio: Bob (45000/25 = 1800) < Alice (60000/30 = 2000)
        var results = df.AsQueryable<UdfEmployee>()
            .OrderBy(x => TestUdfMethods.ComputeRatio(x.Age, x.Salary))
            .ToList();

        Assert.Equal(4, results.Count);
        // David: 35000/22 ~ 1590.9
        // Bob: 45000/25 = 1800
        // Alice: 60000/30 = 2000
        // Charlie: 80000/40 = 2000
        Assert.Equal("David", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_Udf_In_GroupBy()
    {
        using var df = CreateSampleDf();

        var results = df.AsQueryable<UdfEmployee>()
            .GroupBy(x => TestUdfMethods.IsSenior(x.Age))
            .Select(g => new
            {
                IsSenior = g.Key,
                Count = g.Count()
            })
            .OrderBy(x => x.IsSenior)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.False(results[0].IsSenior);
        Assert.Equal(2, results[0].Count); // Bob (25), David (22)
        Assert.True(results[1].IsSenior);
        Assert.Equal(2, results[1].Count);  // Alice (30), Charlie (40)
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_Udf_Under_Strict_Policy_Pushes_Down()
    {
        using var df = CreateSampleDf();

        // Ensure strict policy is default and active
        Assert.Equal(ClientEvaluationPolicy.Strict, Pl.Config.ClientEvaluationPolicy);

        var query = df.AsQueryable<UdfEmployee>()
            .Where(x => TestUdfMethods.IsSenior(x.Age));

        // Explain must produce a valid native plan string
        string plan = query.Explain();
        Assert.NotEmpty(plan);

        // Execution succeeds under Strict policy because UDF is pushed down to Rust LazyFrame
        var results = query.ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_WholeEntity_Blackbox_Udf_Requires_Client_Eval_Policy()
    {
        using var df = CreateSampleDf();

        // 1. In default Strict mode: passing the whole entity to a custom method CANNOT be pushed down -> throws
        var query = df.AsQueryable<UdfEmployee>()
            .Where(e => TestUdfMethods.CheckSecurityEntity(e));

        var ex = Assert.Throws<PolarsPushdownException>(() => query.ToList());
        Assert.Contains("CheckSecurityEntity", ex.ExpressionText);

        // 2. Under Allow mode: gracefully falls back to client row cursor (Hybrid Execution)
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            var fallbackQuery = df.AsQueryable<UdfEmployee>()
                .Where(e => TestUdfMethods.CheckSecurityEntity(e));

            Assert.Contains("Hybrid Execution", fallbackQuery.Explain());
            var results = fallbackQuery.ToList();
            Assert.Equal(2, results.Count);
            Assert.Equal("Alice", results[0].Name);
            Assert.Equal("Charlie", results[1].Name);
        }
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_MultiColumn_Udf_Pushes_Down_Under_Strict_Policy()
    {
        using var df = CreateSampleDf();

        // Multi-column UDF passes columns (e.Name, e.Salary) -> fully pushed down natively in Strict mode
        var query = df.AsQueryable<UdfEmployee>()
            .Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary));

        Assert.Contains("100% Native Pushdown", query.Explain());
        var results = query.ToList();
        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_DisallowUdfPushdown_Blocks_MultiColumn_Udf()
    {
        using var df = CreateSampleDf();

        // Default: DisallowUdfPushdown is false
        Assert.False(Pl.Config.DisallowUdfPushdown);

        // When DisallowUdfPushdown is true:
        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        {
            Assert.True(Pl.Config.DisallowUdfPushdown);

            var query = df.AsQueryable<UdfEmployee>()
                .Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary));

            // Under Strict: throws PolarsPushdownException because UDF pushdown is disallowed
            var ex = Assert.Throws<PolarsPushdownException>(() => query.ToList());
            Assert.Contains("CheckSecurity", ex.ExpressionText);

            // Under Allow: falls back to client evaluation (Hybrid)
            using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
            {
                var fallbackQuery = df.AsQueryable<UdfEmployee>()
                    .Where(e => TestUdfMethods.CheckSecurity(e.Name, e.Salary));

                Assert.Contains("Hybrid Execution", fallbackQuery.Explain());
                var results = fallbackQuery.ToList();
                Assert.Equal(2, results.Count);
            }
        }

        // Scope restored: DisallowUdfPushdown is false again
        Assert.False(Pl.Config.DisallowUdfPushdown);
    }

    [Fact]
    [Trait("Category", "LINQ_UDF")]
    public void Test_Linq_DisallowUdfPushdown_Blocks_Regex_Lookaround_Udf()
    {
        using var df = DataFrame.FromColumns([
            Series.From("User", ["Alice", "Bob"]),
            Series.From("Password", ["P@ssw0rd1", "weakpass"])
        ]);

        // Default: Regex with complex lookahead pushes down via MapStringPredicate UDF
        var defaultQuery = df.AsQueryable<UdfUserAccount>()
            .Where(x => TestUdfMethods.ComplexLookaroundRegex().IsMatch(x.Password));

        var defaultResults = defaultQuery.ToList();
        Assert.Single(defaultResults);
        Assert.Equal("Alice", defaultResults[0].User);

        // When DisallowUdfPushdown is true:
        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        {
            var blockedQuery = df.AsQueryable<UdfUserAccount>()
                .Where(x => TestUdfMethods.ComplexLookaroundRegex().IsMatch(x.Password));

            // Under Strict: throws PolarsPushdownException
            var ex = Assert.Throws<PolarsPushdownException>(() => blockedQuery.ToList());
            Assert.Contains("IsMatch", ex.ExpressionText);

            // Under Allow: falls back to client evaluation
            using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
            {
                var fallbackQuery = df.AsQueryable<UdfUserAccount>()
                    .Where(x => TestUdfMethods.ComplexLookaroundRegex().IsMatch(x.Password));

                var fallbackResults = fallbackQuery.ToList();
                Assert.Single(fallbackResults);
                Assert.Equal("Alice", fallbackResults[0].User);
            }
        }
    }
}
