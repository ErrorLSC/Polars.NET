using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// LINQ tests that switch the process-global client evaluation policy or UDF pushdown flag.
/// They live in this serial test project so they cannot leak config into concurrently running tests.
/// </summary>
public class LinqClientEvaluationPolicyTests
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

    private static DataFrame CreateDepartmentDf()
    {
        return DataFrame.FromRows([
            new UdfDepartmentEmployee("Alice", "Eng", 30, 60000),
            new UdfDepartmentEmployee("Bob", "Eng", 25, 45000),
            new UdfDepartmentEmployee("Charlie", "Eng", 40, 80000),
            new UdfDepartmentEmployee("David", "Sales", 22, 35000),
            new UdfDepartmentEmployee("Eve", "Sales", 35, 70000)
        ]);
    }

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        Assert.Contains(expectedPlan, query.Explain());
    }

    private const string HybridPushdown = "Hybrid Execution";

    private static readonly HashSet<string> ApprovedStaffRegistry = new(StringComparer.OrdinalIgnoreCase)
    {
        "Alice", "Charlie"
    };

    private static bool CheckSecurityClearance(string name, int salary) =>
        ApprovedStaffRegistry.Contains(name) && salary > 4500;

    private static bool CheckSecurityClearance(Employee e) => CheckSecurityClearance(e.Name, e.Salary);

    [Fact]
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_Agg_DisallowUdfPushdown()
    {
        using var df = CreateDepartmentDf();

        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        {
            var blockedQuery = df.AsQueryable<UdfDepartmentEmployee>()
                .GroupBy(x => x.Department)
                .Select(g => new
                {
                    Dept = g.Key,
                    SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Salary))
                });

            // Under Strict: throws PolarsPushdownException because group UDF pushdown is disallowed
            var ex = Assert.Throws<PolarsPushdownException>(() => blockedQuery.ToList());
            Assert.Contains("CalculateRange", ex.ExpressionText);

            // Under Allow: falls back to client evaluation
            using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
            {
                var fallbackQuery = df.AsQueryable<UdfDepartmentEmployee>()
                    .GroupBy(x => x.Department)
                    .Select(g => new
                    {
                        Dept = g.Key,
                        SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Salary))
                    })
                    .OrderBy(x => x.Dept);

                var fallbackResults = fallbackQuery.ToList();
                Assert.Equal(2, fallbackResults.Count);
                Assert.Equal(35000.0, fallbackResults[0].SalaryRange);
                Assert.Equal(35000.0, fallbackResults[1].SalaryRange);
            }
        }
    }

    private static bool CustomSalaryPredicate(Employee emp) => emp.Salary > 4000;
    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_ClientFallback_Where_FollowedBy_TypeChanging_Select()
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var df = DataFrame.FromColumns([
                Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
                Series.From("Age", [25, 17, 30, 15, 28]),
                Series.From("Salary", [5000, 3000, 7000, 2000, 6200])
            ]);

            var query = df.AsQueryable<Employee>()
                          .Where(e => CustomSalaryPredicate(e))
                          .Select(e => e.Name);

            AssertPlan(query, HybridPushdown);

            var result = query.ToList();

            Assert.Equal(3, result.Count);
            Assert.Equal("Alice", result[0]);
            Assert.Equal("Charlie", result[1]);
            Assert.Equal("Eve", result[2]);
        }
    }

    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_WithUnsupportedCSharpMethod()
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var df = DataFrame.FromColumns([
                Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
                Series.From("Age", [25, 17, 30, 15, 28]),
                Series.From("Salary", [5000, 3000, 7000, 2000, 6200])
            ]);

            // 1. Where(Age >= 18) should push down to Polars Native (Filtering Bob and David out)
            // 2. Where(CustomMagicAudit) must gracefully fallback to client-side cursor evaluation
            var query = df.AsQueryable<Employee>()
                          .Where(e => e.Age >= 18)
                          .Where(e => CheckSecurityClearance(e));

            AssertPlan(query, HybridPushdown);
            var result = query.ToList();

            // Qualified:
            // Alice: Age 25 >= 18, in approved set, salary 5000 > 4500 -> True
            // Charlie: Age 30 >= 18, in approved set, salary 7000 > 4500 -> True
            // Eve: Age 28 >= 18, NOT in approved set -> False
            Assert.Equal(2, result.Count);
            Assert.Equal("Alice", result[0].Name);
            Assert.Equal("Charlie", result[1].Name);
        }
    }

    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_ToDataFrame_And_ToLazyFrame()
    {
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            using var df = DataFrame.FromColumns([
                Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
                Series.From("Age", [25, 17, 30, 15, 28]),
                Series.From("Salary", [5000, 3000, 7000, 2000, 6200])
            ]);

            // back into a native columnar DataFrame via DataFrameBuilder.FromRows<T>()
            using DataFrame dfResult = df.AsQueryable<Employee>()
                                         .Where(e => e.Age >= 18)
                                         .Where(e => CheckSecurityClearance(e))
                                         .ToDataFrame();

            Assert.Equal(2, dfResult.Height);
            Assert.Equal(3, dfResult.Width);
            Assert.True(dfResult.ColumnNames.SequenceEqual(["Name", "Age", "Salary"]));

            // Verify that continuing from ToLazyFrame works natively on top of the transposed DataFrame
            using LazyFrame lfResumed = df.AsQueryable<Employee>()
                                          .Where(e => e.Age >= 18)
                                          .Where(e => CheckSecurityClearance(e))
                                          .ToLazyFrame();

            using DataFrame dfCollected = lfResumed.Collect();
            Assert.Equal(2, dfCollected.Height);
        }
    }
}
