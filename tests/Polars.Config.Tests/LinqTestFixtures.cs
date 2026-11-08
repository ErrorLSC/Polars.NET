using System.Text.RegularExpressions;

namespace Polars.Config.Tests;

/// <summary>
/// Managed helpers that Polars cannot translate natively, used to force UDF pushdown or client evaluation.
/// Mirrors the subset of Polars.CSharp.Tests fixtures needed by the serial LINQ tests in this project.
/// </summary>
public static partial class TestUdfMethods
{
    private static readonly HashSet<string> ApprovedStaffRegistry = ["Alice", "Charlie"];

    public static string MaskName(string name) => string.IsNullOrEmpty(name) ? "" : name[..1] + "***";

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
}

public record Employee(string Name, int Age, int Salary);
public record UdfEmployee(string Name, int Age, int Salary);
public record UdfUserAccount(string User, string Password);
public record UdfDepartmentEmployee(string Name, string Department, int Age, int Salary);
