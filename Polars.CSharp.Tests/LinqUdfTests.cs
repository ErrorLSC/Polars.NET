#nullable enable

using System.Text.RegularExpressions;
using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
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

    public static double CalculateRange(IEnumerable<int> values)
    {
        var list = values.ToList();
        return list.Count == 0 ? 0 : list.Max() - list.Min();
    }

    public static double CustomSpread(this IEnumerable<int> values)
    {
        var list = values.ToList();
        return list.Count == 0 ? 0 : list.Max() - list.Min();
    }

    public static double WeightedAverage(IEnumerable<int> salaries, IEnumerable<int> weights)
    {
        var salList = salaries.ToList();
        var wtList = weights.ToList();
        double sumProduct = 0;
        double sumWeight = 0;
        for (int i = 0; i < salList.Count; i++)
        {
            sumProduct += (double)salList[i] * wtList[i];
            sumWeight += wtList[i];
        }
        return sumWeight == 0 ? 0 : sumProduct / sumWeight;
    }

    public static int PeakToTrough(int[] values)
    {
        if (values.Length == 0) return 0;
        return values.Max() - values.Min();
    }

    public static int MedianInt(List<int> values)
    {
        if (values.Count == 0) return 0;
        values.Sort();
        return values[values.Count / 2];
    }

    public static double TotalPayroll(IEnumerable<UdfDepartmentEmployee> employees)
    {
        return employees.Sum(e => (double)e.Salary);
    }

    public static bool ShouldUpdatePrice(double targetPrice, double sourcePrice) => sourcePrice > targetPrice;
    public static double BlendPrice(double targetPrice, double sourcePrice) => (targetPrice + sourcePrice) / 2.0;
    public static bool IsEligibleNewProduct(string category, double price) => category != "Disallowed" && price > 0.0;
    public static double DiscountPrice(double price) => price * 0.85;
    public static bool ShouldRetireSeasonal(string category) => category == "Seasonal";
}

public record UdfEmployee(string Name, int Age, int Salary);
public record UdfNullableRecord(string? Description, int? Score);
public record UdfUserAccount(string User, string Password);
public record UdfDepartmentEmployee(string Name, string Department, int Age, int Salary);
public record UdfRollupRecord(string Dept, string Team, int Amount);
public record UdfRollupMultiColRecord(string Dept, string Team, int Salary, int Weight);
public record UdfMergeItem(int Id, string Category, double Price, bool IsDiscontinued);

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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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
    [Trait("LINQ", "UDF")]
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

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_SingleColumn_Aggregation()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Salary))
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        // Eng: 80000 - 45000 = 35000
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(35000.0, results[0].SalaryRange);
        // Sales: 70000 - 35000 = 35000
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(35000.0, results[1].SalaryRange);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_ExtensionMethod_Aggregation()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                Spread = g.Select(x => x.Salary).CustomSpread()
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(35000.0, results[0].Spread);
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(35000.0, results[1].Spread);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_MultiColumn_Aggregation()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                WeightedAvg = TestUdfMethods.WeightedAverage(g.Select(x => x.Salary), g.Select(x => x.Age))
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        // Eng: (60000*30 + 45000*25 + 80000*40) / (30 + 25 + 40) = (1800000 + 1125000 + 3200000) / 95 = 6125000 / 95 ≈ 64473.68
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(6125000.0 / 95.0, results[0].WeightedAvg, 2);

        // Sales: (35000*22 + 70000*35) / (22 + 35) = (770000 + 2450000) / 57 = 3220000 / 57 ≈ 56491.23
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(3220000.0 / 57.0, results[1].WeightedAvg, 2);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_Array_And_List_Aggregation()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                Peak = TestUdfMethods.PeakToTrough(g.Select(x => x.Salary).ToArray()),
                Median = TestUdfMethods.MedianInt(g.Select(x => x.Salary).ToList())
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        // Eng salaries: [45000, 60000, 80000] -> Peak: 35000, Median: 60000
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(35000, results[0].Peak);
        Assert.Equal(60000, results[0].Median);

        // Sales salaries: [35000, 70000] -> Peak: 35000, Median: 70000
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(35000, results[1].Peak);
        Assert.Equal(70000, results[1].Median);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Aggregate_Linq_Method()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                AggSum = g.Select(x => x.Salary).Aggregate((acc, x) => acc + x)
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        // Eng: 60000 + 45000 + 80000 = 185000
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(185000, results[0].AggSum);

        // Sales: 35000 + 70000 = 105000
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(105000, results[1].AggSum);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_Agg_Combined_With_Native_Agg()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                Count = g.Count(),
                TotalSalary = g.Sum(x => x.Salary),
                SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Salary))
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(3, results[0].Count);
        Assert.Equal(185000, results[0].TotalSalary);
        Assert.Equal(35000.0, results[0].SalaryRange);

        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(2, results[1].Count);
        Assert.Equal(105000, results[1].TotalSalary);
        Assert.Equal(35000.0, results[1].SalaryRange);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_Agg_Composite_Arithmetic()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                ScaledRange = TestUdfMethods.CalculateRange(g.Select(x => x.Salary)) * 1.5 + 100.0
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        // 35000 * 1.5 + 100 = 52600
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(52600.0, results[0].ScaledRange);
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(52600.0, results[1].ScaledRange);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_Agg_With_Having()
    {
        using var df = CreateDepartmentDf();

        // Single group filter via Having: only keep groups where Range > 30000
        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Salary))
            })
            .Where(x => x.SalaryRange > 30000.0)
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal("Sales", results[1].Dept);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    public void Test_Linq_GroupBy_Custom_Agg_On_WholeGroup()
    {
        using var df = CreateDepartmentDf();

        var results = df.AsQueryable<UdfDepartmentEmployee>()
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Dept = g.Key,
                Payroll = TestUdfMethods.TotalPayroll(g)
            })
            .OrderBy(x => x.Dept)
            .ToList();

        Assert.Equal(2, results.Count);
        // Eng: 60000 + 45000 + 80000 = 185000
        Assert.Equal("Eng", results[0].Dept);
        Assert.Equal(185000.0, results[0].Payroll);
        // Sales: 35000 + 70000 = 105000
        Assert.Equal("Sales", results[1].Dept);
        Assert.Equal(105000.0, results[1].Payroll);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    [Trait("LINQ", "RollUp")]
    public void Test_Linq_GroupBy_Rollup_Custom_Udf_Aggregation_SchemeA()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Dept", ["Sales", "Sales", "Sales", "Sales", "Eng", "Eng", "Eng", "Eng"]),
            Series.From("Team", ["A", "A", "B", "B", "A", "A", "B", "B"]),
            Series.From("Amount", [10, 30, 20, 50, 100, 120, 200, 250])
        ]);

        var query = df.AsQueryable<UdfRollupRecord>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp(
                (key, g) => new
                {
                    key.Dept,
                    key.Team,
                    TotalAmount = g.Sum(x => x.Amount),
                    SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Amount)),
                    Count = g.Count()
                }
            );

        var plan = query.Explain();
        Assert.Contains("100% Native Pushdown", plan);
        Assert.Contains("with C# UDF Callback", plan);
        Assert.Contains("CalculateRange", plan);
        using var resultDf = query.ToDataFrame();

        // 7 rows: 4 at level 0, 2 at level 1, 1 at level 2
        Assert.Equal(7, resultDf.Height);

        // Check level 0 (Sales, A)
        var salesA = resultDf.Filter(Pl.Col("Dept") == "Sales" & Pl.Col("Team") == "A");
        Assert.Equal(1, salesA.Height);
        Assert.Equal(40, salesA.GetValue<int>(0, "TotalAmount"));
        Assert.Equal(20.0, salesA.GetValue<double>(0, "SalaryRange"));

        // Check level 1 (Sales, null)
        var salesSubtotal = resultDf.Filter(Pl.Col("Dept") == "Sales" & Pl.Col("Team").IsNull());
        Assert.Equal(1, salesSubtotal.Height);
        Assert.Equal(110, salesSubtotal.GetValue<int>(0, "TotalAmount"));
        // Sales amounts: 10, 30, 20, 50 -> Range: 50 - 10 = 40
        Assert.Equal(40.0, salesSubtotal.GetValue<double>(0, "SalaryRange"));

        // Check level 1 (Eng, null)
        var engSubtotal = resultDf.Filter(Pl.Col("Dept") == "Eng" & Pl.Col("Team").IsNull());
        Assert.Equal(1, engSubtotal.Height);
        Assert.Equal(670, engSubtotal.GetValue<int>(0, "TotalAmount"));
        // Eng amounts: 100, 120, 200, 250 -> Range: 250 - 100 = 150
        Assert.Equal(150.0, engSubtotal.GetValue<double>(0, "SalaryRange"));

        // Check Grand Total: (null, null)
        var grandTotal = resultDf.Filter(Pl.Col("Dept").IsNull() & Pl.Col("Team").IsNull());
        Assert.Equal(1, grandTotal.Height);
        Assert.Equal(780, grandTotal.GetValue<int>(0, "TotalAmount"));
        Assert.Equal(8, grandTotal.GetValue<int>(0, "Count"));
        // Range across all amounts [10 .. 250] is 250 - 10 = 240
        Assert.Equal(240.0, grandTotal.GetValue<double>(0, "SalaryRange"));

        // Also verify ToList() object hydration
        var listResults = query.ToList();
        Assert.Equal(7, listResults.Count);
        var grandTotalRow = listResults.Single(r => r.Dept == null && r.Team == null);
        Assert.Equal(780, grandTotalRow.TotalAmount);
        Assert.Equal(8, grandTotalRow.Count);
        Assert.Equal(240.0, grandTotalRow.SalaryRange);
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    [Trait("LINQ", "RollUp")]
    public void Test_Linq_GroupBy_Rollup_Custom_Udf_Aggregation_SchemeB()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Dept", ["Sales", "Sales", "Sales", "Sales", "Eng", "Eng", "Eng", "Eng"]),
            Series.From("Team", ["A", "A", "B", "B", "A", "A", "B", "B"]),
            Series.From("Amount", [10, 30, 20, 50, 100, 120, 200, 250])
        ]);

        var query = df.AsQueryable<UdfRollupRecord>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp()
            .Select(g => new
            {
                g.Key.Dept,
                g.Key.Team,
                TotalAmount = g.Sum(x => x.Amount),
                SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Amount)),
                Count = g.Count()
            });

        var plan = query.Explain();
        Assert.Contains("100% Native Pushdown", plan);
        Assert.Contains("with C# UDF Callback", plan);
        Assert.Contains("CalculateRange", plan);
        using var resultDf = query.ToDataFrame();
        Assert.Equal(7, resultDf.Height);

        var grandTotal = resultDf.Filter(Pl.Col("Dept").IsNull() & Pl.Col("Team").IsNull());
        Assert.Equal(1, grandTotal.Height);
        Assert.Equal(780, grandTotal.GetValue<int>(0, "TotalAmount"));
        Assert.Equal(8, grandTotal.GetValue<int>(0, "Count"));
        Assert.Equal(240.0, grandTotal.GetValue<double>(0, "SalaryRange"));
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    [Trait("LINQ", "RollUp")]
    public void Test_Linq_GroupBy_Rollup_Custom_MultiColumn_Udf()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Dept", ["Sales", "Sales", "Eng", "Eng"]),
            Series.From("Team", ["A", "B", "A", "B"]),
            Series.From("Salary", [1000, 2000, 3000, 4000]),
            Series.From("Weight", [1, 2, 3, 4])
        ]);

        var query = df.AsQueryable<UdfRollupMultiColRecord>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp(
                (key, g) => new
                {
                    key.Dept,
                    key.Team,
                    WeightedAvg = TestUdfMethods.WeightedAverage(g.Select(x => x.Salary), g.Select(x => x.Weight))
                }
            );

        var plan = query.Explain();
        Assert.Contains("100% Native Pushdown", plan);
        Assert.Contains("with C# UDF Callback", plan);
        Assert.Contains("WeightedAverage", plan);
        using var resultDf = query.ToDataFrame();
        Assert.Equal(7, resultDf.Height);

        // Grand total: (1000*1 + 2000*2 + 3000*3 + 4000*4) / (1 + 2 + 3 + 4) = 30000 / 10 = 3000
        var grandTotal = resultDf.Filter(Pl.Col("Dept").IsNull() & Pl.Col("Team").IsNull());
        Assert.Equal(1, grandTotal.Height);
        Assert.Equal(3000.0, grandTotal.GetValue<double>(0, "WeightedAvg"));
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    [Trait("LINQ", "RollUp")]
    public void Test_Linq_GroupBy_Rollup_Custom_Udf_With_Grouping_Property()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Dept", ["Sales", "Sales", "Eng", "Eng"]),
            Series.From("Team", ["A", "B", "A", "B"]),
            Series.From("Amount", [10, 30, 100, 150])
        ]);

        var query = df.AsQueryable<UdfRollupRecord>()
            .GroupBy(x => new { x.Dept, x.Team })
            .RollUp(
                (key, g) => new
                {
                    key.Dept,
                    key.Team,
                    SalaryRange = TestUdfMethods.CalculateRange(g.Select(x => x.Amount)),
                    G_Dept = key.Dept.Grouping(),
                    G_Team = key.Grouping(k => k.Team)
                }
            );

        var plan = query.Explain();
        Assert.Contains("100% Native Pushdown", plan);
        Assert.Contains("with C# UDF Callback", plan);
        Assert.Contains("CalculateRange", plan);
        using var resultDf = query.ToDataFrame();
        Assert.Equal(7, resultDf.Height);

        var grandTotal = resultDf.Filter(Pl.Col("Dept").IsNull() & Pl.Col("Team").IsNull());
        Assert.Equal(1, grandTotal.Height);
        Assert.Equal(1, grandTotal.GetValue<int>(0, "G_Dept"));
        Assert.Equal(1, grandTotal.GetValue<int>(0, "G_Team"));
        // Range across all amounts [10, 30, 100, 150] is 150 - 10 = 140
        Assert.Equal(140.0, grandTotal.GetValue<double>(0, "SalaryRange"));
    }

    [Fact]
    [Trait("LINQ", "UDF")]
    [Trait("LINQ", "TypedMerge")]
    public void Test_Linq_TypedMerge_Supports_Custom_Udf_Pushdown()
    {
        var targetDf = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3, 4, 5 },
            Category = new[] { "Seasonal", "Core", "Core", "Tech", "Tech" },
            Price = new[] { 10.0, 20.0, 30.0, 100.0, 200.0 },
            IsDiscontinued = new[] { false, false, false, false, false }
        });

        var sourceDf = DataFrame.FromColumns(new
        {
            Id = new[] { 3, 4, 5, 6, 7 },
            Category = new[] { "Core", "Tech", "Tech", "New", "Disallowed" },
            Price = new[] { 30.0, 150.0, 180.0, 50.0, 80.0 },
            IsDiscontinued = new[] { true, false, false, false, false }
        });

        var targetQuery = targetDf.AsQueryable<UdfMergeItem>();
        var sourceQuery = sourceDf.AsQueryable<UdfMergeItem>();

        var mergeBuilder = targetQuery.Merge(sourceQuery, t => t.Id, s => s.Id)
            // 1. Matched: Delete if discontinued
            .WhenMatched(
                (t, s) => s.IsDiscontinued,
                then => then.Delete()
            )
            // 2. Matched: Update price using UDF condition & UDF setter
            .WhenMatched(
                (t, s) => TestUdfMethods.ShouldUpdatePrice(t.Price, s.Price),
                then => then.Update(set => set.Set(t => t.Price, (t, s) => TestUdfMethods.BlendPrice(t.Price, s.Price)))
            )
            // 3. Not matched by target: Insert if UDF condition matches, overriding price with UDF setter
            .WhenNotMatchedByTarget(
                s => TestUdfMethods.IsEligibleNewProduct(s.Category, s.Price),
                then => then.Insert(set => set
                    .Set(t => t.Id, s => s.Id)
                    .Set(t => t.Category, s => s.Category)
                    .Set(t => t.Price, s => TestUdfMethods.DiscountPrice(s.Price))
                    .Set(t => t.IsDiscontinued, s => s.IsDiscontinued))
            )
            // 4. Not matched by source: Delete if UDF condition matches
            .WhenNotMatchedBySource(
                t => TestUdfMethods.ShouldRetireSeasonal(t.Category),
                then => then.Delete()
            );

        // Verify logical merge plan
        string mergePlan = mergeBuilder.ToMergePlanString();
        Assert.NotEmpty(mergePlan);

        // Execute merge
        using var resultDf = mergeBuilder.ToDataFrame().Sort("Id");

        var idArray = resultDf["Id"].ToArray<int>();
        var priceArray = resultDf["Price"].ToArray<double>();

        // Assert:
        // Id 1: Target Seasonal retired via WhenNotMatchedBySource UDF -> Deleted
        // Id 2: Target Core kept -> Kept with 20.0
        // Id 3: Matched & Discontinued -> Deleted
        // Id 4: Matched & Source Price(150) > Target Price(100) -> BlendPrice(100, 150) = 125.0
        // Id 5: Matched & Source Price(180) < Target Price(200) -> Kept with 200.0
        // Id 6: Source New item eligible -> Inserted with DiscountPrice(50 * 0.85) = 42.5
        // Id 7: Source Disallowed item -> Rejected by UDF condition -> Not inserted
        Assert.Equal([2, 4, 5, 6], idArray);
        Assert.Equal([20.0, 125.0, 200.0, 42.5], priceArray);

        // Also test ToList() materialization directly to POCO/records
        var entityList = mergeBuilder.ToList().OrderBy(x => x.Id).ToList();
        Assert.Equal(4, entityList.Count);
        Assert.Equal(2, entityList[0].Id);
        Assert.Equal(20.0, entityList[0].Price);
        Assert.Equal(4, entityList[1].Id);
        Assert.Equal(125.0, entityList[1].Price);
        Assert.Equal(5, entityList[2].Id);
        Assert.Equal(200.0, entityList[2].Price);
        Assert.Equal(6, entityList[3].Id);
        Assert.Equal(42.5, entityList[3].Price);

        // Also test re-entering LINQ query pipeline from merge result
        var highPricedItems = mergeBuilder.AsQueryable()
            .Where(x => x.Price > 30.0)
            .OrderBy(x => x.Id)
            .ToList();

        Assert.Equal(3, highPricedItems.Count);
        Assert.Equal(4, highPricedItems[0].Id);
        Assert.Equal(5, highPricedItems[1].Id);
        Assert.Equal(6, highPricedItems[2].Id);
    }
}
