using System.Text;
using Polars.CSharp.Linq;
using System.Linq;
using Pl = Polars.CSharp.Polars;
using System.Text.RegularExpressions;

namespace Polars.CSharp.Tests;

public record Employee(string Name, int Age, int Salary);
public readonly record struct EmployeeDept(string Name, string Department, int Age, int Salary);
public readonly record struct DeptEmpCount(int DeptId, string DeptName, int EmployeeCount);
public record struct DepartmentRecord(string Department,int Salary);
public readonly record struct Person(int Id, string Name, int DeptId);
public readonly record struct Department(int Id, string DeptName);
public readonly record struct PersonDeptJoined(int Id, string Name, int DeptId, string DeptName);
public record DeptSummary(string Department, int TotalSalary);
public record struct EmployeeDeptRecord(int Id, string Name, string DeptName);
public readonly record struct DeptPersonSummary(int DeptId, long MemberCount, int MinId, int MaxId);
public readonly record struct PersonWithTags(int Id, string Name, string[] Tags);
public readonly record struct PersonTagPair(int Id, string Name, string Tag);
public readonly record struct PersonTagDetailed(int PersonId, string TagName);
public record struct StudentScores(int Id, string Name, int[] Scores);
public readonly record struct AdvancedDeptSummary(
    int DeptId,
    long TotalMembers,
    long HighIdMembers,
    int FirstMemberId,
    int MaxMemberId
);

public interface IWorker
{
    string Name { get; }
    int Salary { get; }
}

public record struct WorkerRecord(string Name, int Age, int Salary) : IWorker;
public readonly record struct DeptMetricSummary(
    int DeptId,
    string DeptName,
    long MemberCount,
    int TotalSalary,
    int MaxSalary
);

public readonly record struct DeptMinMaxBySummary(
    int DeptId,
    int YoungestMemberId,
    int HighestPaidMemberId
);

public readonly record struct MemberRecord(
    int Id,
    string Name,
    int DeptId,
    int Age,
    int Salary
);
public readonly record struct DeptAggResult(int DeptId, long TotalCount, int MaxId);
public readonly record struct OrderRecord(
    int Id,
    string CustomerName,
    DateTime OrderDate,
    double Amount
);
public record struct DeveloperProfile(int Id, string[] Skills, string[] DesiredSkills);
public readonly record struct UserInfo(int Id, string Email, string Password);
public readonly record struct LogEntry(int Id, string Message);
public record struct AddressInfo(string City, string ZipCode);
public record struct CustomerProfile(int Id, string CustomerName, AddressInfo Address);
public record struct UserProfile(int Id, string Nickname, string FallbackName);
public record OrderItem(string ItemName,decimal Price,decimal DiscountRate);
public readonly record struct ProductMetric(
    string Name,
    double Factor
);
public partial class LinqTests
{
    [Fact]
    [Trait("LINQ", "SingleColumn")]
    public void Test_CSharp_Linq_Scalar_Projection_ToUpper()
    {
        var emps = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000)
        };

        using var dfEmps = DataFrame.FromRows(emps);
        var empQuery = dfEmps.AsQueryable(emps);

        IQueryable<string> upperQuery = empQuery.Select(e => e.Name.ToUpper());

        var firstName = upperQuery.First();

        var dfResult = upperQuery.ToDataFrame();

        Assert.Equal(1L, dfResult.Width);

        Assert.Equal("ALICE", firstName);

    }
    [Fact]
    [Trait("LINQ", "Scalar_Projection")]
    public void Test_CSharp_Linq_Scalar_Projection_Any_All()
    {
        var emps = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000)
        };

        using var dfEmps = DataFrame.FromRows(emps);
        var empQuery = dfEmps.AsQueryable(emps);

        var upperQuery = empQuery.Select(e => e.Name.ToUpper());
        upperQuery.ToDataFrame().Show();
        Assert.True(upperQuery.Any(name => name == "ALICE"));
        Assert.False(upperQuery.Any(name => name == "Alice")); 

        Assert.True(upperQuery.All(name => name.Length == 3 || name.Length == 5));
        Assert.False(upperQuery.All(name => name == "ALICE"));
    }
    [Fact]
    [Trait("LINQ", "Scalar_Last")]
    public void Test_Linq_Last_And_LastOrDefault_Flow()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [25, 30, 35, 30]),
            Series.From("Salary", [50000L, 60000L, 70000L, 80000L])
        ]);

        var query = df.AsQueryable<Employee>();

        Assert.Equal("David", query.Last().Name);
        Assert.Equal("David", query.LastOrDefault().Name);

        var lastThirty = query.Last(e => e.Age == 30);
        Assert.Equal("David", lastThirty.Name);
        Assert.Equal(80000L, lastThirty.Salary);

        Assert.Throws<InvalidOperationException>(() => query.Last(e => e.Age > 100));

        var upperLast = query.Select(e => e.Name.ToUpper()).Last(name => name.StartsWith('C') || name.StartsWith('D'));
        Assert.Equal("DAVID", upperLast);
    }

    [Fact]
    [Trait("LINQ", "WhereAndTake")]
    public void Test_Linq_Where_Take()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [25, 17, 30, 15]),
            Series.From("Salary", [5000, 3000, 7000, 2000])
        ]);

        var threshold = 18;
        var query = df.AsQueryable<Employee>()
                      .Where(e => e.Age >= threshold && e.Salary > 4000)
                      .Take(10)
                      .ToList();

        Assert.Equal(2, query.Count);
        Assert.Equal("Alice", query[0].Name);
        Assert.Equal("Charlie", query[1].Name);
    }
    /// <summary>
    /// External static lookup table simulating a pure managed security/audit cache.
    /// This cannot be translated to any Polars native ExprHandle, guaranteeing a client fallback.
    /// </summary>
    private static readonly HashSet<string> ApprovedStaffRegistry = new(StringComparer.OrdinalIgnoreCase)
    {
        "Alice", "Charlie"
    };

    /// <summary>
    /// Custom business method checking managed state that Polars native engine cannot translate.
    /// </summary>
    private static bool CheckSecurityClearance(string name, int salary)
    {
        // Polars expr cannot execute an in-memory HashSet.Contains or arbitrary C# control flow
        return ApprovedStaffRegistry.Contains(name) && salary > 4500;
    }

    [Fact]
    [Trait("LINQ", "Skip")]
    public void Test_Linq_Skip_And_Skip_Take_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5, 6]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"])
        ]);

        // 1. Single Skip
        var skipResult = df.AsQueryable<Person>()
            .OrderBy(p => p.Id)
            .Skip(3)
            .ToList();

        Assert.Equal(3, skipResult.Count);
        Assert.Equal(4, skipResult[0].Id);
        Assert.Equal(6, skipResult[2].Id);

        // 2. Skip + Take Fusion (Paging scenario)
        var pagedResult = df.AsQueryable<Person>()
            .OrderBy(p => p.Id)
            .Skip(2)
            .Take(2)
            .ToList();

        Assert.Equal(2, pagedResult.Count);
        Assert.Equal(3, pagedResult[0].Id);
        Assert.Equal(4, pagedResult[1].Id);
    }

    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_WithUnsupportedCSharpMethod()
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
                      .Where(e => CheckSecurityClearance(e.Name, e.Salary))
                      .ToList();
        
        // Qualified:
        // Alice: Age 25 >= 18, in approved set, salary 5000 > 4500 -> True
        // Charlie: Age 30 >= 18, in approved set, salary 7000 > 4500 -> True
        // Eve: Age 28 >= 18, NOT in approved set -> False
        Assert.Equal(2, query.Count);
        Assert.Equal("Alice", query[0].Name);
        Assert.Equal("Charlie", query[1].Name);
    }
    [Fact]
    [Trait("LINQ", "Fallback")]
    public void Test_Linq_Fallback_ToDataFrame_And_ToLazyFrame()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("Age", [25, 17, 30, 15, 28]),
            Series.From("Salary", [5000, 3000, 7000, 2000, 6200])
        ]);

        // back into a native columnar DataFrame via DataFrameBuilder.FromRows<T>()
        using DataFrame dfResult = df.AsQueryable<Employee>()
                                     .Where(e => e.Age >= 18)
                                     .Where(e => CheckSecurityClearance(e.Name, e.Salary))
                                     .ToDataFrame();

        Assert.Equal(2, dfResult.Height);
        Assert.Equal(3, dfResult.Width);
        Assert.True(dfResult.ColumnNames.SequenceEqual(["Name", "Age", "Salary"]));

        // Verify that continuing from ToLazyFrame works natively on top of the transposed DataFrame
        using LazyFrame lfResumed = df.AsQueryable<Employee>()
                                      .Where(e => e.Age >= 18)
                                      .Where(e => CheckSecurityClearance(e.Name, e.Salary))
                                      .ToLazyFrame();

        using DataFrame dfCollected = lfResumed.Collect();
        Assert.Equal(2, dfCollected.Height);
    }
    [Fact]
    [Trait("LINQ", "Join")]
    public void Test_Linq_MultiColumn_OrderBy_ThenByDescending()
    {
        using var df = DataFrame.FromColumns(
            [
                Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
                Series.From("Department", ["IT", "HR", "IT", "Finance", "HR", "IT"]),
                Series.From("Age", [25, 17, 30, 15, 28, 22]),
                Series.From("Salary", [5000, 3000, 7000, 2000, 6200, 5000])
            ]);

        // Filter adults with salary > 3500, then sort by Department ascending, and Salary descending
        var query = df.AsQueryable<EmployeeDept>()
                      .Where(e => -e.Age <= -18 && e.Salary / 1000.0f > 3.5f)
                      .OrderBy(e => e.Department)
                      .ThenByDescending(e => e.Salary)
                      .ToList();

        // Qualified:
        // Eve: HR, 28, 6200
        // Charlie: IT, 30, 7000
        // Alice: IT, 25, 5000
        // Frank: IT, 22, 5000
        Assert.Equal(4, query.Count);

        // Department "HR" comes first
        Assert.Equal("Eve", query[0].Name);
        Assert.Equal("HR", query[0].Department);
        Assert.Equal(6200, query[0].Salary);

        // Department "IT": Charlie (7000) has higher salary than Alice (5000) and Frank (5000)
        Assert.Equal("Charlie", query[1].Name);
        Assert.Equal("IT", query[1].Department);
        Assert.Equal(7000, query[1].Salary);

        Assert.Equal("IT", query[2].Department);
        Assert.Equal(5000, query[2].Salary);

        Assert.Equal("IT", query[3].Department);
        Assert.Equal(5000, query[3].Salary);
    }
    [Fact]
    [Trait("LINQ", "Join")]
    public void Test_Linq_MultiKey_Chained_Join_Pushdown()
    {
        // 1. Left Table: Persons (Id, Name, DeptId)
        using var leftDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 20, 10, 30])
        ]);

        // 2. Middle Table: Departments (Id, DeptName)
        // Primary key matches (p.DeptId == d.Id)
        using var middleDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20, 30]),
            Series.From("DeptName", ["IT", "HR", "Finance"])
        ]);

        // 3. Right Table: DeptEmpCounts (DeptId, DeptName, EmployeeCount)
        // Multi-column composite key: matches (j.DeptId == c.DeptId && j.DeptName == c.DeptName)
        using var rightDf = DataFrame.FromColumns(
        [
            Series.From("DeptId", [10, 20, 40]),
            Series.From("DeptName", ["IT", "HR", "Legal"]),
            Series.From("EmployeeCount", [2, 1, 5])
        ]);

        // First Join: Single key (p.DeptId == d.Id) -> PersonDeptJoined
        // Second Join: Composite Multi-Key (new { j.DeptId, j.DeptName } == new { c.DeptId, c.DeptName })
        var query = leftDf.AsQueryable<Person>()
            .Join(
                middleDf.AsQueryable<Department>(),
                p => p.DeptId,
                d => d.Id,
                (p, d) => new PersonDeptJoined(p.Id, p.Name, p.DeptId, d.DeptName)
            )
            .Join(
                rightDf.AsQueryable<DeptEmpCount>(),
                j => new { j.DeptId, j.DeptName },
                c => new { c.DeptId, c.DeptName },
                (j, c) => new
                {
                    j.Id,
                    j.Name,
                    j.DeptId,
                    j.DeptName,
                    c.EmployeeCount
                }
            )
            .OrderBy(r => r.Id)
            .ToList();

        // Verification:
        // Alice (1) -> DeptId 10, IT, Count 2
        // Bob (2)   -> DeptId 20, HR, Count 1
        // Charlie (3) -> DeptId 10, IT, Count 2
        // David (4) is DeptId 30 (Finance), which doesn't exist in rightDf -> inner join excludes David
        Assert.Equal(3, query.Count);

        Assert.Equal(1, query[0].Id);
        Assert.Equal("Alice", query[0].Name);
        Assert.Equal(10, query[0].DeptId);
        Assert.Equal("IT", query[0].DeptName);
        Assert.Equal(2, query[0].EmployeeCount);

        Assert.Equal(2, query[1].Id);
        Assert.Equal("Bob", query[1].Name);
        Assert.Equal(20, query[1].DeptId);
        Assert.Equal("HR", query[1].DeptName);
        Assert.Equal(1, query[1].EmployeeCount);

        Assert.Equal(3, query[2].Id);
        Assert.Equal("Charlie", query[2].Name);
        Assert.Equal(10, query[2].DeptId);
        Assert.Equal("IT", query[2].DeptName);
        Assert.Equal(2, query[2].EmployeeCount);
    }
    [Fact]
    [Trait("LINQ", "Join")]
    public void Test_Linq_Join_Two_LazyFrames()
    {
        // 1. Prepare Left DataFrame
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 20, 10, 30])
        ]);

        // 2. Prepare Right DataFrame
        using var deptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20]),
            Series.From("DeptName", ["Engineering", "HR"])
        ]);

        // 3. Native Pushdown Join
        // Both sides are LazyFrames / PolarsQuery instances
        using var resultDf = personsDf.AsQueryable<Person>()
            .Where(p => p.Id <= 3) // Alice (10), Bob (20), Charlie (10)
            .Join(
                deptsDf.AsQueryable<Department>(),
                person => person.DeptId,
                dept => dept.Id,
                (person, dept) => new PersonDeptJoined(person.Id, person.Name, person.DeptId, dept.DeptName)
            )
            .OrderBy(r => r.Id)
            .ToDataFrame();

        // Qualified matches:
        // Alice: Id=1, DeptId=10 -> Engineering
        // Bob: Id=2, DeptId=20 -> HR
        // Charlie: Id=3, DeptId=10 -> Engineering
        Assert.Equal(3, resultDf.Height);

        var names = resultDf["Name"].ToArray<string>();
        var deptNames = resultDf["DeptName"].ToArray<string>();

        Assert.Equal(["Alice", "Bob", "Charlie"], names);
        Assert.Equal(["Engineering", "HR", "Engineering"], deptNames);
    }

    [Fact]
    [Trait("LINQ", "Join")]
    public void Test_Linq_Join_With_InMemory_Collection()
    {
        // Left side is a Native Polars DataFrame
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [101, 102, 999]) // 999 does not exist in right table
        ]);

        // Right side is a pure C# in-memory List<Department>
        // Tests DataFrameBuilder.FromRows<Department> dynamic transposition!
        var inMemoryDepts = new List<Department>
        {
            new(101, "Finance"),
            new(102, "Marketing")
        };

        var query = personsDf.AsQueryable<Person>()
            .Join(
                inMemoryDepts,
                p => p.DeptId,
                d => d.Id,
                (p, d) => new PersonDeptJoined(p.Id, p.Name, p.DeptId, d.DeptName)
            )
            .OrderBy(r => r.Id)
            .ToList();

        Assert.Equal(2, query.Count);
        Assert.Equal("Alice", query[0].Name);
        Assert.Equal("Finance", query[0].DeptName);

        Assert.Equal("Bob", query[1].Name);
        Assert.Equal("Marketing", query[1].DeptName);
    }
    [Fact]
    [Trait("LINQ", "Join")]
    public void Test_Linq_LeftJoin_Two_LazyFrames()
    {
        // 1. Left dataset: 4 employees across 3 departments
        using var employeesDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 20, 10, 99]) // 99 does not exist in departmentsDf
        ]);

        // 2. Right dataset: Only Dept 10 exists
        using var departmentsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10]),
            Series.From("DeptName", ["Engineering"])
        ]);

        // 3. Perform LeftJoin via the .NET 8 extension method
        var results = employeesDf.AsQueryable<Person>()
            .LeftJoin(
                departmentsDf.AsQueryable<Department>(),
                emp => emp.DeptId,
                dept => dept.Id,
                (emp, dept) => new EmployeeDeptRecord(emp.Id, emp.Name, dept.DeptName)
            )
            .OrderBy(r => r.Id)
            .ToList();

        // All 4 left rows must be preserved in a Left Outer Join
        Assert.Equal(4, results.Count);

        // Matched rows (DeptId 10 -> "Engineering")
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Engineering", results[0].DeptName);

        Assert.Equal("Charlie", results[2].Name);
        Assert.Equal("Engineering", results[2].DeptName);

        // Unmatched rows (DeptId 20 and 99 have no match in right table -> DeptName defaults to null)
        Assert.Equal("Bob", results[1].Name);
        Assert.Null(results[1].DeptName);

        Assert.Equal("David", results[3].Name);
        Assert.Null(results[3].DeptName);
    }

    [Fact]
    [Trait("LINQ", "Join")]
    public void Test_Linq_LeftJoin_ToDataFrame_PreservesNulls()
    {
        using var employeesDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("DeptId", [10, 99])
        ]);

        using var departmentsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10]),
            Series.From("DeptName", ["Engineering"])
        ]);

        // Execute LeftJoin directly to native DataFrame
        using DataFrame joinedDf = employeesDf.AsQueryable<Person>()
            .LeftJoin(
                departmentsDf.AsQueryable<Department>(),
                emp => emp.DeptId,
                dept => dept.Id,
                (emp, dept) => new EmployeeDeptRecord(emp.Id, emp.Name, dept.DeptName)
            )
            .OrderBy(r => r.Id)
            .ToDataFrame();

        Assert.Equal(2, joinedDf.Height);

        // Check Native Series Null handling
        var deptNameCol = joinedDf["DeptName"];
        Assert.Equal("Engineering", deptNameCol.GetValue<string>(0));
        Assert.Null(deptNameCol.GetValue<string>(1));
    }

    [Fact]
    [Trait("LINQ", "GroupBy")]
    public void Test_Linq_GroupBy_With_Aggregations_On_Existing_Person_Record()
    {
        // 1. Prepare Left DataFrame using Person columns (Id, Name, DeptId)
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5, 6]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
            Series.From("DeptId", [10, 20, 10, 30, 20, 10])
        ]);

        // Distribution:
        // Dept 10: Alice (1), Charlie (3), Frank (6) -> Count=3, MinId=1, MaxId=6
        // Dept 20: Bob (2), Eve (5)                  -> Count=2, MinId=2, MaxId=5
        // Dept 30: David (4)                         -> Count=1, MinId=4, MaxId=4

        // 2. Perform native GroupBy + Aggregations pushdown via LINQ
        var summaries = personsDf.AsQueryable<Person>()
            .Where(p => p.Id <= 5) // Exclude Frank (6) -> Dept 10 now has 2 members
            .GroupBy(
                p => p.DeptId,
                (deptId, group) => new DeptPersonSummary(
                    deptId,
                    group.Count(),
                    group.Min(x => x.Id),
                    group.Max(x => x.Id)
                )
            )
            .OrderBy(s => s.DeptId)
            .ToList();

        // 3. Verify results
        Assert.Equal(3, summaries.Count);

        // Dept 10 (Alice=1, Charlie=3)
        Assert.Equal(10, summaries[0].DeptId);
        Assert.Equal(2, summaries[0].MemberCount);
        Assert.Equal(1, summaries[0].MinId);
        Assert.Equal(3, summaries[0].MaxId);

        // Dept 20 (Bob=2, Eve=5)
        Assert.Equal(20, summaries[1].DeptId);
        Assert.Equal(2, summaries[1].MemberCount);
        Assert.Equal(2, summaries[1].MinId);
        Assert.Equal(5, summaries[1].MaxId);

        // Dept 30 (David=4)
        Assert.Equal(30, summaries[2].DeptId);
        Assert.Equal(1, summaries[2].MemberCount);
        Assert.Equal(4, summaries[2].MinId);
        Assert.Equal(4, summaries[2].MaxId);
    }

    [Fact]
    [Trait("LINQ", "GroupBy")]
    public void Test_Linq_GroupBy_Advanced_Aggregations()
    {
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5, 6]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
            Series.From("DeptId", [10, 20, 10, 30, 20, 10])
        ]);

        var summaries = personsDf.AsQueryable<Person>()
            .GroupBy(
                p => p.DeptId,
                (deptId, group) => new AdvancedDeptSummary(
                    deptId,
                    group.LongCount(),
                    group.Count(x => x.Id > 2),
                    group.Select(x => x.Id).First(), // Correct LINQ syntax: maps to ExprFirst
                    group.Max(x => x.Id)
                )
            )
            .OrderBy(s => s.DeptId)
            .ToList();

        Assert.Equal(3, summaries.Count);

        // Dept 10: Alice (1), Charlie (3), Frank (6)
        Assert.Equal(10, summaries[0].DeptId);
        Assert.Equal(3, summaries[0].TotalMembers);
        Assert.Equal(2, summaries[0].HighIdMembers);
        Assert.Equal(1, summaries[0].FirstMemberId);
        Assert.Equal(6, summaries[0].MaxMemberId);

        // Dept 20: Bob (2), Eve (5)
        Assert.Equal(20, summaries[1].DeptId);
        Assert.Equal(2, summaries[1].TotalMembers);
        Assert.Equal(1, summaries[1].HighIdMembers);
        Assert.Equal(2, summaries[1].FirstMemberId);
        Assert.Equal(5, summaries[1].MaxMemberId);

        // Dept 30: David (4)
        Assert.Equal(30, summaries[2].DeptId);
        Assert.Equal(1, summaries[2].TotalMembers);
        Assert.Equal(1, summaries[2].HighIdMembers);
        Assert.Equal(4, summaries[2].FirstMemberId);
        Assert.Equal(4, summaries[2].MaxMemberId);
    }

    [Fact]
    [Trait("LINQ", "GroupBy")]
    public void Test_Linq_GroupBy_MinBy_MaxBy_Aggregations()
    {
        // 1. Prepare dataset with Department, Age and Salary
        using var membersDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5, 6]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
            Series.From("DeptId", [10, 20, 10, 30, 20, 10]),
            Series.From("Age", [25, 45, 35, 29, 22, 40]),
            Series.From("Salary", [5000, 9000, 7500, 6000, 8000, 12000])
        ]);

        // 2. Pushdown MinBy & MaxBy within GroupBy using standard LINQ syntax:
        // group.MinBy(x => x.Age).Id
        var summaries = membersDf.AsQueryable<MemberRecord>()
            .GroupBy(
                m => m.DeptId,
                (deptId, group) => new DeptMinMaxBySummary(
                    deptId,
                    group.MinBy(x => x.Age).Id,      // Member Id with min Age
                    group.MaxBy(x => x.Salary).Id   // Member Id with max Salary
                )
            )
            .OrderBy(s => s.DeptId)
            .ToList();

        Assert.Equal(3, summaries.Count);

        // Dept 10:
        // Alice: Id=1, Age=25, Salary=5000
        // Charlie: Id=3, Age=35, Salary=7500
        // Frank: Id=6, Age=40, Salary=12000
        // Youngest is Alice (Id 1), Highest paid is Frank (Id 6)
        Assert.Equal(10, summaries[0].DeptId);
        Assert.Equal(1, summaries[0].YoungestMemberId);
        Assert.Equal(6, summaries[0].HighestPaidMemberId);

        // Dept 20:
        // Bob: Id=2, Age=45, Salary=9000
        // Eve: Id=5, Age=22, Salary=8000
        // Youngest is Eve (Id 5), Highest paid is Bob (Id 2)
        Assert.Equal(20, summaries[1].DeptId);
        Assert.Equal(5, summaries[1].YoungestMemberId);
        Assert.Equal(2, summaries[1].HighestPaidMemberId);

        // Dept 30:
        // David: Id=4, Age=29, Salary=6000
        Assert.Equal(30, summaries[2].DeptId);
        Assert.Equal(4, summaries[2].YoungestMemberId);
        Assert.Equal(4, summaries[2].HighestPaidMemberId);
    }
    [Fact]
    [Trait("LINQ","GroupBy")]
    public void Test_Linq_GroupBy_Without_Agg_Returns_Grouping_Sequence()
    {
        // 1. Prepare sample DataFrame with multiple persons per department
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("DeptId", [10, 20, 10, 30, 20])
        ]);

        // 2. Execute bare GroupBy without immediate aggregation
        // Expected signature: IEnumerable<IGrouping<int, Person>>
        var groupingList = personsDf.AsQueryable<Person>()
            .Where(p => p.Id <= 4) // Alice (10), Bob (20), Charlie (10), David (30)
            .GroupBy(p => p.DeptId)
            .OrderBy(g => g.Key)
            .ToList();

        // 3. Verify top-level group count
        // Dept 10, 20, 30 are expected
        Assert.Equal(3, groupingList.Count);

        // Group 1: DeptId = 10 (Alice, Charlie)
        var dept10 = groupingList[0];
        Assert.Equal(10, dept10.Key);
        var dept10Persons = dept10.OrderBy(p => p.Id).ToList();
        Assert.Equal(2, dept10Persons.Count);
        Assert.Equal("Alice", dept10Persons[0].Name);
        Assert.Equal("Charlie", dept10Persons[1].Name);

        // Group 2: DeptId = 20 (Bob)
        var dept20 = groupingList[1];
        Assert.Equal(20, dept20.Key);
        var dept20Persons = dept20.ToList();
        Assert.Single(dept20Persons);
        Assert.Equal("Bob", dept20Persons[0].Name);

        // Group 3: DeptId = 30 (David)
        var dept30 = groupingList[2];
        Assert.Equal(30, dept30.Key);
        var dept30Persons = dept30.ToList();
        Assert.Single(dept30Persons);
        Assert.Equal("David", dept30Persons[0].Name);
    }

    [Fact]
    [Trait("LINQ","GroupBy")]
    public void Test_Linq_GroupBy_FollowedBy_Select_Pushdown()
    {
        // 1. Prepare sample DataFrame with existing Person struct
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [101, 102, 103, 104, 105]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("DeptId", [1, 2, 1, 3, 2])
        ]);

        // 2. Standard C# LINQ syntax: GroupBy followed by Select projection
        // Should compile into a single native LazyGroupByAgg call
        var results = personsDf.AsQueryable<Person>()
            .Where(p => p.Id >= 102) // Filter out Alice (101, Dept 1) -> Dept 1 now only has Charlie (103)
            .GroupBy(p => p.DeptId)
            .Select(g => new DeptAggResult(
                g.Key,
                g.Count(),
                g.Max(x => x.Id)
            ))
            .OrderBy(r => r.DeptId)
            .ToList();

        // 3. Verify correctness
        Assert.Equal(3, results.Count);

        // Dept 1: Charlie (103)
        Assert.Equal(1, results[0].DeptId);
        Assert.Equal(1, results[0].TotalCount);
        Assert.Equal(103, results[0].MaxId);

        // Dept 2: Bob (102), Eve (105)
        Assert.Equal(2, results[1].DeptId);
        Assert.Equal(2, results[1].TotalCount);
        Assert.Equal(105, results[1].MaxId);

        // Dept 3: David (104)
        Assert.Equal(3, results[2].DeptId);
        Assert.Equal(1, results[2].TotalCount);
        Assert.Equal(104, results[2].MaxId);
    }

    [Fact]
    [Trait("LINQ","GroupBy")]
    public void Test_Linq_GroupBy_Aggregations_On_Empty_Dataset_Pushdown()
    {
        // 1. Prepare dataset where Where filter eliminates all records
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [10, 20, 10])
        ]);

        // 2. Pushdown aggregation on zero rows (Id > 999 eliminates everything)
        var results = personsDf.AsQueryable<Person>()
            .Where(p => p.Id > 999)
            .GroupBy(
                p => p.DeptId,
                (deptId, group) => new DeptPersonSummary(
                    deptId,
                    group.Count(),
                    group.Min(x => x.Id),
                    group.Max(x => x.Id)
                )
            )
            .ToList();

        // 3. Native engine must return an empty sequence gracefully without null-pointer exceptions
        Assert.Empty(results);
    }

    [Fact]
    [Trait("LINQ", "GroupBy_4Args_Having")]
    public void Test_Linq_GroupBy_With_ElementSelector_And_Having()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "HR", "IT", "Finance", "IT", "HR"]),
            Series.From("Salary", [50000, 60000, 70000, 80000, 90000, 65000])
        ]);

        var query = df.AsQueryable<DepartmentRecord>();

        // GroupBy with 4 arguments:
        // 1. keySelector: d => d.Department
        // 2. elementSelector: d => d.Salary
        // 3. resultSelector: (dept, salaries) => new DeptSummary(dept, salaries.Sum())
        // Followed by Where (HAVING): TotalSalary > 100000
        var results = query
            .GroupBy(
                d => d.Department,
                d => d.Salary,
                (dept, salaries) => new DeptSummary(dept, salaries.Sum()))
            .Where(s => s.TotalSalary > 100000)
            .ToList();

        // results.ToDataFrame().Show();
        // IT total = 210000, HR total = 125000, Finance = 80000 (filtered out)
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Department == "IT" && r.TotalSalary == 210000);
        Assert.Contains(results, r => r.Department == "HR" && r.TotalSalary == 125000);
    }
    [Fact]
    [Trait("LINQ","Complex")]
    public void Test_Linq_Join_FollowedBy_GroupBy_Aggregations_Pushdown()
    {
        // 1. Prepare Persons (left table)
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("DeptId", [10, 20, 10, 10, 20])
        ]);

        // 2. Prepare Departments (right table)
        using var departmentsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20]),
            Series.From("DeptName", ["Engineering", "HumanResources"])
        ]);

        // 3. Pipeline: Inner Join -> GroupBy DeptId -> Vectorized Aggregations
        // Pipeline pushes down: pl_lazy_join -> pl_lazy_groupby_agg -> sort
        var summaries = personsDf.AsQueryable<Person>()
            .Join(
                departmentsDf.AsQueryable<Department>(),
                p => p.DeptId,
                d => d.Id,
                (p, d) => new PersonDeptJoined(p.Id, p.Name, p.DeptId, d.DeptName)
            )
            .GroupBy(
                pd => pd.DeptId,
                (deptId, group) => new DeptPersonSummary(
                    deptId,
                    group.Count(),
                    group.Min(x => x.Id),
                    group.Max(x => x.Id)
                )
            )
            .OrderBy(s => s.DeptId)
            .ToList();

        // 4. Assert correctness
        Assert.Equal(2, summaries.Count);

        // Dept 10 (Engineering): Alice (1), Charlie (3), David (4)
        Assert.Equal(10, summaries[0].DeptId);
        Assert.Equal(3, summaries[0].MemberCount);
        Assert.Equal(1, summaries[0].MinId);
        Assert.Equal(4, summaries[0].MaxId);

        // Dept 20 (HumanResources): Bob (2), Eve (5)
        Assert.Equal(20, summaries[1].DeptId);
        Assert.Equal(2, summaries[1].MemberCount);
        Assert.Equal(2, summaries[1].MinId);
        Assert.Equal(5, summaries[1].MaxId);
    }

    [Fact]
    [Trait("LINQ","Complex")]
    public void Test_Linq_Complex_Filter_Join_OrderBy_And_Aggregations()
    {
        // 1. Source tables
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5, 6]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
            Series.From("DeptId", [10, 20, 10, 30, 20, 10])
        ]);

        using var deptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20]),
            Series.From("DeptName", ["Engineering", "HR"])
        ]);

        // 2. High-complexity pipeline:
        // Filter Left -> Inner Join Right -> Two-stage GroupBy + Select -> Multi-column Sort -> Take Limit
        var results = personsDf.AsQueryable<Person>()
            .Where(p => p.Id >= 2) // Eliminates Alice(1)
            .Join(
                deptsDf.AsQueryable<Department>(),
                p => p.DeptId,
                d => d.Id,
                (p, d) => new PersonDeptJoined(p.Id, p.Name, p.DeptId, d.DeptName)
            )
            .GroupBy(pd => pd.DeptId)
            .Select(g => new DeptAggResult(
                g.Key,
                g.Count(),
                g.Max(x => x.Id)
            ))
            .OrderByDescending(r => r.TotalCount)
            .ThenBy(r => r.DeptId)
            .Take(2)
            .ToList();

        // Left rows matching:
        // Bob(2, Dept 20), Charlie(3, Dept 10), Eve(5, Dept 20), Frank(6, Dept 10)
        // Dept 10: Count=2, MaxId=6
        // Dept 20: Count=2, MaxId=5
        Assert.Equal(2, results.Count);

        // Both have TotalCount=2, ThenBy DeptId ensures Dept 10 comes first
        Assert.Equal(10, results[0].DeptId);
        Assert.Equal(2, results[0].TotalCount);
        Assert.Equal(6, results[0].MaxId);

        Assert.Equal(20, results[1].DeptId);
        Assert.Equal(2, results[1].TotalCount);
        Assert.Equal(5, results[1].MaxId);
    }

    [Fact]
    [Trait("LINQ","Complex")]
    public void Test_Linq_LeftJoin_FollowedBy_GroupBy_Aggregations()
    {
        // 1. Employees with an unmatched department (Dept 99)
        using var employeesDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 10, 20, 99])
        ]);

        // 2. Only Dept 10 and 20 exist
        using var departmentsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20]),
            Series.From("DeptName", ["Engineering", "HR"])
        ]);

        // 3. LeftJoin preserves David(4, Dept 99) -> Group by DeptId -> Aggregate Min/Max Id
        var summaries = employeesDf.AsQueryable<Person>()
            .LeftJoin(
                departmentsDf.AsQueryable<Department>(),
                emp => emp.DeptId,
                dept => dept.Id,
                (emp, dept) => new PersonDeptJoined(emp.Id, emp.Name, emp.DeptId, dept.DeptName)
            )
            .GroupBy(
                joined => joined.DeptId,
                (deptId, g) => new DeptPersonSummary(
                    deptId,
                    g.Count(),
                    g.Min(x => x.Id),
                    g.Max(x => x.Id)
                )
            )
            .OrderBy(s => s.DeptId)
            .ToList();

        // Expecting 3 groups: 10, 20, 99
        Assert.Equal(3, summaries.Count);

        // Dept 10 (Alice, Bob)
        Assert.Equal(10, summaries[0].DeptId);
        Assert.Equal(2, summaries[0].MemberCount);
        Assert.Equal(1, summaries[0].MinId);
        Assert.Equal(2, summaries[0].MaxId);

        // Dept 20 (Charlie)
        Assert.Equal(20, summaries[1].DeptId);
        Assert.Equal(1, summaries[1].MemberCount);
        Assert.Equal(3, summaries[1].MinId);
        Assert.Equal(3, summaries[1].MaxId);

        // Dept 99 (David - unmatched DeptName preserved)
        Assert.Equal(99, summaries[2].DeptId);
        Assert.Equal(1, summaries[2].MemberCount);
        Assert.Equal(4, summaries[2].MinId);
        Assert.Equal(4, summaries[2].MaxId);
    }
    
    [Fact]
    [Trait("LINQ", "RightJoin")]
    public void Test_Linq_RightJoin_Materialize_Rows()
    {
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("DeptId", [10, 20])
        ]);

        using var deptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 30]),
            Series.From("DeptName", ["Engineering", "Marketing"])
        ]);

        var result = personsDf.AsQueryable<Person>()
            .RightJoin(
                deptsDf.AsQueryable<Department>(),
                p => p.DeptId,
                d => d.Id,
                (p, d) => new EmployeeDeptRecord(d.Id, p.Name, d.DeptName)
            )
            .OrderBy(r => r.DeptName)
            .ToList();

        Assert.Equal(2, result.Count);
        // Marketing has no matching person in left table
        Assert.Equal("Engineering", result[0].DeptName);
        Assert.Equal("Marketing", result[1].DeptName);
        Assert.Null(result[1].Name);
    }

    [Fact]
    [Trait("LINQ", "Intersect")]
    public void Test_Linq_Intersect_FullRow_Pushdown()
    {
        // Intersect maps to Semi Join matching all schema columns
        using var leftDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 20, 30, 40])
        ]);

        using var rightDf = DataFrame.FromColumns(
        [
            Series.From("Id", [2, 4, 5]),
            Series.From("Name", ["Bob", "David", "Eve"]),
            Series.From("DeptId", [20, 40, 50])
        ]);

        var result = leftDf.AsQueryable<Person>()
            .Intersect(rightDf.AsQueryable<Person>())
            .OrderBy(p => p.Id)
            .ToList();

        // Bob (2) and David (4) exist in both datasets
        Assert.Equal(2, result.Count);
        Assert.Equal(2, result[0].Id);
        Assert.Equal("Bob", result[0].Name);

        Assert.Equal(4, result[1].Id);
        Assert.Equal("David", result[1].Name);
    }

    [Fact]
    [Trait("LINQ", "IntersectBy")]
    public void Test_Linq_IntersectBy_KeySelector_Pushdown()
    {
        // IntersectBy matches rows based only on DeptId key
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [10, 20, 99])
        ]);

        using var validDeptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 30]),
            Series.From("DeptName", ["Engineering", "Legal"])
        ]);

        var result = personsDf.AsQueryable<Person>()
            .IntersectBy(validDeptsDf.AsQueryable<Department>().Select(d => d.Id), p => p.DeptId)
            .ToList();

        // Only Alice matches DeptId = 10
        Assert.Single(result);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal(10, result[0].DeptId);
    }

    [Fact]
    [Trait("LINQ", "Except")]
    public void Test_Linq_Except_FullRow_Pushdown()
    {
        // Except maps to Anti Join matching all schema columns
        using var leftDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [10, 20, 30])
        ]);

        using var rightDf = DataFrame.FromColumns(
        [
            Series.From("Id", [2]),
            Series.From("Name", ["Bob"]),
            Series.From("DeptId", [20])
        ]);

        var result = leftDf.AsQueryable<Person>()
            .Except(rightDf.AsQueryable<Person>())
            .OrderBy(p => p.Id)
            .ToList();

        // Bob (2) should be excluded
        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Alice", result[0].Name);

        Assert.Equal(3, result[1].Id);
        Assert.Equal("Charlie", result[1].Name);
    }

    [Fact]
    [Trait("LINQ", "ExceptBy")]
    public void Test_Linq_ExceptBy_KeySelector_Pushdown()
    {
        // ExceptBy matches and excludes rows based on DeptId
        using var personsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [10, 20, 30])
        ]);

        using var excludedDeptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 30]),
            Series.From("DeptName", ["Engineering", "Research"])
        ]);

        var result = personsDf.AsQueryable<Person>()
            .ExceptBy(excludedDeptsDf.AsQueryable<Department>().Select(d => d.Id), p => p.DeptId)
            .ToList();

        // Only Bob (DeptId = 20) should remain
        Assert.Single(result);
        Assert.Equal("Bob", result[0].Name);
        Assert.Equal(20, result[0].DeptId);
    }
    [Fact]
    [Trait("LINQ", "Distinct")]
    public void Test_Linq_Distinct_FullRow_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 1, 3, 2]),
            Series.From("Name", ["Alice", "Bob", "Alice", "Charlie", "Bob"]),
            Series.From("DeptId", [10, 20, 10, 30, 20])
        ]);

        var result = df.AsQueryable<Person>()
            .Distinct()
            .OrderBy(p => p.Id)
            .ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal([1, 2, 3], result.Select(r => r.Id).ToArray());
    }

    [Fact]
    [Trait("LINQ", "DistinctBy")]
    public void Test_Linq_DistinctBy_KeySelector_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 10, 20, 20])
        ]);

        // DistinctBy DeptId: should preserve only the first record for each DeptId
        var result = df.AsQueryable<Person>()
            .DistinctBy(p => p.DeptId)
            .OrderBy(p => p.DeptId)
            .ToList();

        Assert.Equal(2, result.Count);
        Assert.Equal(10, result[0].DeptId);
        Assert.Equal("Alice", result[0].Name);

        Assert.Equal(20, result[1].DeptId);
        Assert.Equal("Charlie", result[1].Name);
    }
    [Fact]
    [Trait("LINQ", "Concat")]
    public void Test_Linq_Concat_Two_LazyFrames()
    {
        using var leftDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("DeptId", [10, 20])
        ]);

        using var rightDf = DataFrame.FromColumns(
        [
            Series.From("Id", [3, 4]),
            Series.From("Name", ["Charlie", "David"]),
            Series.From("DeptId", [30, 40])
        ]);

        var result = leftDf.AsQueryable<Person>()
            .Concat(rightDf.AsQueryable<Person>())
            .OrderBy(p => p.Id)
            .ToList();

        Assert.Equal(4, result.Count);
        Assert.Equal([1, 2, 3, 4], [.. result.Select(r => r.Id)]);
    }

    [Fact]
    [Trait("LINQ", "Union")]
    public void Test_Linq_Union_FullRow_Pushdown()
    {
        using var leftDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("DeptId", [10, 20])
        ]);

        using var rightDf = DataFrame.FromColumns(
        [
            Series.From("Id", [2, 3]),
            Series.From("Name", ["Bob", "Charlie"]),
            Series.From("DeptId", [20, 30])
        ]);

        // Row (2, "Bob", 20) is duplicated across tables and should appear once
        var result = leftDf.AsQueryable<Person>()
            .Union(rightDf.AsQueryable<Person>())
            .OrderBy(p => p.Id)
            .ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal([1, 2, 3], [.. result.Select(r => r.Id)]);
    }
    [Fact]
    [Trait("LINQ", "Union")]
    public void Test_Linq_Union_And_UnionBy_Pushdown()
    {
        // 1. Arrange: Two DataFrames with overlapping Person records
        using var df1 = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [10, 20, 30])
        ]);

        using var df2 = DataFrame.FromColumns(
        [
            Series.From("Name", ["Charlie", "David", "Eve"]),
            Series.From("Id", [3, 4, 5]),
            Series.From("DeptId", [30, 40, 50])
        ]);

        // 2. Act (Case A): Full-row Union (Id 3 is identical in both -> deduplicated)
        var unionResults = df1.AsQueryable<Person>()
            .Union(df2.AsQueryable<Person>())
            .OrderBy(p => p.Id)
            .ToList();

        // 3. Assert (Case A): Total 5 unique elements (1, 2, 3, 4, 5)
        Assert.Equal(5, unionResults.Count);
        Assert.Equal(1, unionResults[0].Id);
        Assert.Equal(2, unionResults[1].Id);
        Assert.Equal(3, unionResults[2].Id);
        Assert.Equal(4, unionResults[3].Id);
        Assert.Equal(5, unionResults[4].Id);

        // 4. Act (Case B): UnionBy specific key (e.g. DeptId)
        using var df3 = DataFrame.FromColumns(
        [
            Series.From("Id", [101, 102]),
            Series.From("Name", ["BobClone", "Frank"]),
            Series.From("DeptId", [20, 60]) // DeptId 20 duplicates df1's Bob
        ]);

        var unionByResults = df1.AsQueryable<Person>()
            .UnionBy(df3.AsQueryable<Person>(), p => p.DeptId)
            .OrderBy(p => p.DeptId)
            .ToList();

        // 5. Assert (Case B): DeptId 20 should keep df1's Bob, Frank with DeptId 60 is added
        Assert.Equal(4, unionByResults.Count);
        Assert.Equal(10, unionByResults[0].DeptId);
        Assert.Equal(20, unionByResults[1].DeptId);
        Assert.Equal("Bob", unionByResults[1].Name); // First keep strategy retains original
        Assert.Equal(30, unionByResults[2].DeptId);
        Assert.Equal(60, unionByResults[3].DeptId);
        Assert.Equal("Frank", unionByResults[3].Name);
    }

    [Fact]
    [Trait("LINQ", "UnionBy")]
    public void Test_Linq_UnionBy_KeySelector_Pushdown()
    {
        using var leftDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("DeptId", [10, 20])
        ]);

        using var rightDf = DataFrame.FromColumns(
        [
            Series.From("Id", [3, 4]),
            Series.From("Name", ["Robert", "Charlie"]),
            Series.From("DeptId", [20, 30]) // DeptId 20 duplicates leftDf's DeptId 20
        ]);

        // Distinct on DeptId: only the first record with DeptId 20 ("Bob") is preserved
        var result = leftDf.AsQueryable<Person>()
            .UnionBy(rightDf.AsQueryable<Person>(), p => p.DeptId)
            .OrderBy(p => p.DeptId)
            .ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal(10, result[0].DeptId);
        Assert.Equal(20, result[1].DeptId);
        Assert.Equal("Bob", result[1].Name);
        Assert.Equal(30, result[2].DeptId);
    }

    [Fact]
    [Trait("LINQ", "SelectMany")]
    public void Test_Linq_SelectMany_Explode_Pushdown()
    {
        // 1. Prepare DataFrame with List/Array column
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Tags", new string[][] 
            {
                ["Rust", "DotNet"],
                ["Polars"]
            })
        ]);

        // 2. Execute SelectMany with ResultSelector
        var results = df.AsQueryable<PersonWithTags>()
            .SelectMany(p => p.Tags, (p, tag) => new PersonTagPair(p.Id, p.Name, tag))
            .OrderBy(r => r.Id)
            .ToList();

        // 3. Expected:
        // Alice -> Rust
        // Alice -> DotNet
        // Bob -> Polars
        Assert.Equal(3, results.Count);

        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Rust", results[0].Tag);

        Assert.Equal(1, results[1].Id);
        Assert.Equal("Alice", results[1].Name);
        Assert.Equal("DotNet", results[1].Tag);

        Assert.Equal(2, results[2].Id);
        Assert.Equal("Bob", results[2].Name);
        Assert.Equal("Polars", results[2].Tag);
    }

    [Fact]
    [Trait("LINQ", "SelectManyCrossJoin")]
    public void Test_Linq_SelectMany_CrossJoin_ExistingRecords()
    {
        // 1. Prepare Left DataFrame: 2 Employees
        using var empDf = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Age", [28, 35]),
            Series.From("Salary", [7000, 9500])
        ]);

        // 2. Prepare Right DataFrame: 2 Departments
        using var deptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20]),
            Series.From("DeptName", ["Engineering", "Finance"])
        ]);

        // 3. Cartesian Product (2 x 2 = 4 records)
        // Projects to existing readonly record struct EmployeeDept
        var results = empDf.AsQueryable<Employee>()
            .SelectMany(
                _ => deptsDf.AsQueryable<Department>(),
                (e, d) => new EmployeeDept(e.Name, d.DeptName, e.Age, e.Salary)
            )
            .OrderBy(r => r.Name)
            .ThenBy(r => r.Department)
            .ToList();

        Assert.Equal(4, results.Count);

        // Alice x Engineering
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Engineering", results[0].Department);
        Assert.Equal(28, results[0].Age);
        Assert.Equal(7000, results[0].Salary);

        // Alice x Finance
        Assert.Equal("Alice", results[1].Name);
        Assert.Equal("Finance", results[1].Department);
        Assert.Equal(28, results[1].Age);
        Assert.Equal(7000, results[1].Salary);

        // Bob x Engineering
        Assert.Equal("Bob", results[2].Name);
        Assert.Equal("Engineering", results[2].Department);
        Assert.Equal(35, results[2].Age);
        Assert.Equal(9500, results[2].Salary);

        // Bob x Finance
        Assert.Equal("Bob", results[3].Name);
        Assert.Equal("Finance", results[3].Department);
        Assert.Equal(35, results[3].Age);
        Assert.Equal(9500, results[3].Salary);
    }
    [Fact]
    [Trait("LINQ", "DistinctBy")]
    public void Test_Linq_DistinctBy_With_Convert_And_Complex_Expressions()
    {
        // 1. Prepare data with duplicate values
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("DeptId", [10, 10, 20, 20, 30])
        ]);

        // Case A: Explicit cast to (long) inside keySelector: p => (long)p.DeptId
        // Verifies ExpressionPatterns.(|ExtractColumnName|_|) stripping Convert nodes recursively
        var distinctLongKey = df.AsQueryable<Person>()
            .DistinctBy(p => (long)p.DeptId)
            .OrderBy(p => p.DeptId)
            .ToList();

        Assert.Equal(3, distinctLongKey.Count);
        Assert.Equal(10, distinctLongKey[0].DeptId);
        Assert.Equal(20, distinctLongKey[1].DeptId);
        Assert.Equal(30, distinctLongKey[2].DeptId);

        // Case B: Boxed cast to (object): p => (object)p.DeptId
        var distinctBoxed = df.AsQueryable<Person>()
            .DistinctBy(p => (object)p.DeptId)
            .OrderBy(p => p.DeptId)
            .ToList();

        Assert.Equal(3, distinctBoxed.Count);
        Assert.Equal("Alice", distinctBoxed[0].Name);
        Assert.Equal("Charlie", distinctBoxed[1].Name);
        Assert.Equal("Eve", distinctBoxed[2].Name);

        // Case C: Multi-column anonymous object projection: p => new { p.DeptId, p.Name }
        var distinctCompound = df.AsQueryable<Person>()
            .DistinctBy(p => new { p.DeptId, p.Name })
            .OrderBy(p => p.Id)
            .ToList();

        Assert.Equal(5, distinctCompound.Count);
    }

    [Fact]
    [Trait("LINQ", "SelectMany")]
    public void Test_Linq_SelectMany_Explode_Pushdown_With_Renaming()
    {
        // 1. Prepare DataFrame with List/Array column
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Tags", new string[][] 
            {
                ["Rust", "DotNet"],
                ["Polars"]
            })
        ]);

        // Case A: Map Tags -> Tag (Single letter truncation rename)
        var results = df.AsQueryable<PersonWithTags>()
            .SelectMany(p => p.Tags, (p, tag) => new PersonTagPair(p.Id, p.Name, tag))
            .OrderBy(r => r.Id)
            .ThenBy(r => r.Tag)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("DotNet", results[0].Tag);

        Assert.Equal(1, results[1].Id);
        Assert.Equal("Alice", results[1].Name);
        Assert.Equal("Rust", results[1].Tag);

        Assert.Equal(2, results[2].Id);
        Assert.Equal("Bob", results[2].Name);
        Assert.Equal("Polars", results[2].Tag);

        // Case B: Map Id -> PersonId and Tags -> TagName (Dual property rename test)
        var detailedResults = df.AsQueryable<PersonWithTags>()
            .SelectMany(p => p.Tags, (p, t) => new PersonTagDetailed(p.Id, t))
            .OrderBy(r => r.PersonId)
            .ThenBy(r => r.TagName)
            .ToList();

        Assert.Equal(3, detailedResults.Count);
        Assert.Equal(1, detailedResults[0].PersonId);
        Assert.Equal("DotNet", detailedResults[0].TagName);
        Assert.Equal(2, detailedResults[2].PersonId);
        Assert.Equal("Polars", detailedResults[2].TagName);
    }
    [Fact]
    [Trait("LINQ", "TakeWhile")]
    public void Test_Linq_TakeWhile_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 10, 4, 5]),
            Series.From("Name", ["A", "B", "C", "D", "E", "F"])
        ]);

        // Take elements while Id < 10.
        // Even though 4 and 5 are < 10, they appear after 10, so they MUST NOT be included.
        var results = df.AsQueryable<Person>()
            .TakeWhile(p => p.Id < 10)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal([1, 2, 3], [.. results.Select(r => r.Id)]);
    }

    [Fact]
    [Trait("LINQ", "SkipWhile")]
    public void Test_Linq_SkipWhile_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 10, 2, 1]),
            Series.From("Name", ["A", "B", "C", "D", "E", "F"])
        ]);

        // Skip while Id < 10.
        // Starts keeping from 10 onwards, preserving subsequent elements even if < 10.
        var results = df.AsQueryable<Person>()
            .SkipWhile(p => p.Id < 10)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal([10, 2, 1], [.. results.Select(r => r.Id)]);
    }

    [Fact]
    [Trait("LINQ", "GroupJoin")]
    public void Test_Linq_GroupJoin_LeftOuterJoin_Pushdown()
    {
        // 1. Prepare Departments: HR (10), IT (20), Sales (30 - has no employees)
        using var deptsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20, 30]),
            Series.From("DeptName", ["HR", "IT", "Sales"])
        ]);

        // 2. Prepare Employees: 3 in HR, 1 in IT
        using var empsDf = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 10, 10, 20])
        ]);

        // 3. GroupJoin: Departments with their Employees
        var results = deptsDf.AsQueryable<Department>()
            .GroupJoin(
                empsDf.AsQueryable<Person>(),
                d => d.Id,
                e => e.DeptId,
                (dept, empGroup) => new DeptEmpCount(dept.Id, dept.DeptName, empGroup.Count())
            )
            .OrderBy(r => r.DeptId)
            .ToList();

        Assert.Equal(3, results.Count);

        // HR -> 3 employees
        Assert.Equal(10, results[0].DeptId);
        Assert.Equal("HR", results[0].DeptName);
        Assert.Equal(3, results[0].EmployeeCount);

        // IT -> 1 employee
        Assert.Equal(20, results[1].DeptId);
        Assert.Equal("IT", results[1].DeptName);
        Assert.Equal(1, results[1].EmployeeCount);

        // Sales -> 0 employees (Left Outer Join behavior preserved)
        Assert.Equal(30, results[2].DeptId);
        Assert.Equal("Sales", results[2].DeptName);
        Assert.Equal(0, results[2].EmployeeCount);
    }
    [Fact]
    [Trait("LINQ", "Zip")]
    public void Test_Linq_Zip_HorizontalConcat_ExistingRecords()
    {
        // 1. Left DataFrame: Employees with Name, Age, Salary
        using var empDf = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [28, 35, 42]),
            Series.From("Salary", [75000, 92000, 110000])
        ]);

        // 2. Right DataFrame: Departments
        using var deptDf = DataFrame.FromColumns(
        [
            Series.From("DeptName", ["Engineering", "Product", "Executive"])
        ]);

        // 3. Zip side-by-side into existing EmployeeDept record struct:
        // public readonly record struct EmployeeDept(string Name, string Department, int Age, int Salary);
        var results = empDf.AsQueryable<Employee>()
            .Zip(
                deptDf.AsQueryable<Department>(),
                (e, d) => new EmployeeDept(e.Name, d.DeptName, e.Age, e.Salary)
            )
            .OrderBy(ed => ed.Age)
            .ToList();

        Assert.Equal(3, results.Count);

        // Alice (Engineering, Age 28)
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Engineering", results[0].Department);
        Assert.Equal(28, results[0].Age);
        Assert.Equal(75000, results[0].Salary);

        // Bob (Product, Age 35)
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Product", results[1].Department);
        Assert.Equal(35, results[1].Age);
        Assert.Equal(92000, results[1].Salary);

        // Charlie (Executive, Age 42)
        Assert.Equal("Charlie", results[2].Name);
        Assert.Equal("Executive", results[2].Department);
        Assert.Equal(42, results[2].Age);
        Assert.Equal(110000, results[2].Salary);
    }
    [Fact]
    [Trait("LINQ", "Zip3")]
    public void Test_Linq_Zip3_HorizontalConcat()
    {
        // 1. First DataFrame: Member IDs & basic identifiers
        using var df1 = DataFrame.FromColumns(
        [
            Series.From("Id", [101, 102, 103]),
            Series.From("DeptId", [10, 20, 30])
        ]);

        // 2. Second DataFrame: Employees (Name, Age, Salary)
        using var df2 = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [28, 35, 42]),
            Series.From("Salary", [75000, 90000, 120000])
        ]);

        // 3. Third DataFrame: Departments (DeptName)
        using var df3 = DataFrame.FromColumns(
        [
            Series.From("DeptName", ["HR", "IT", "Finance"])
        ]);

        // 4. Three-way positional Zip: (TFirst, TSecond, TThird)
        var results = df1.AsQueryable<MemberRecord>()
            .Zip(
                df2.AsQueryable<Employee>(),
                df3.AsQueryable<Department>()
            )
            .OrderBy(tuple => tuple.Second.Age)
            .ToList();

        Assert.Equal(3, results.Count);

        // First row: Alice (Id 101, Age 28, HR)
        Assert.Equal(101, results[0].First.Id);
        Assert.Equal("Alice", results[0].Second.Name);
        Assert.Equal(28, results[0].Second.Age);
        Assert.Equal("HR", results[0].Third.DeptName);

        // Second row: Bob (Id 102, Age 35, IT)
        Assert.Equal(102, results[1].First.Id);
        Assert.Equal("Bob", results[1].Second.Name);
        Assert.Equal(35, results[1].Second.Age);
        Assert.Equal("IT", results[1].Third.DeptName);

        // Third row: Charlie (Id 103, Age 42, Finance)
        Assert.Equal(103, results[2].First.Id);
        Assert.Equal("Charlie", results[2].Second.Name);
        Assert.Equal(42, results[2].Second.Age);
        Assert.Equal(120000, results[2].Second.Salary);
        Assert.Equal("Finance", results[2].Third.DeptName);
    }
    [Fact]
    [Trait("LINQ", "Chunk")]
    public void Test_Linq_Chunk_Batching()
    {
        // 1. Prepare 7 records
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4, 5, 6, 7]),
            Series.From("Name", ["A", "B", "C", "D", "E", "F", "G"]),
            Series.From("DeptId", [10, 10, 20, 20, 30, 30, 40]),
            Series.From("Age", [21, 22, 23, 24, 25, 26, 27]),
            Series.From("Salary", [3000, 3500, 4000, 4500, 5000, 5500, 6000])
        ]);

        // 2. Chunk by batch size of 3
        var chunks = df.AsQueryable<MemberRecord>()
            .OrderBy(m => m.Id)
            .Chunk(3)
            .ToList();

        // 7 items divided by 3 -> 3 chunks: [3, 3, 1]
        Assert.Equal(3, chunks.Count);

        // Chunk 0: [1, 2, 3]
        Assert.Equal(3, chunks[0].Length);
        Assert.Equal(1, chunks[0][0].Id);
        Assert.Equal(2, chunks[0][1].Id);
        Assert.Equal(3, chunks[0][2].Id);

        // Chunk 1: [4, 5, 6]
        Assert.Equal(3, chunks[1].Length);
        Assert.Equal(4, chunks[1][0].Id);
        Assert.Equal(5, chunks[1][1].Id);
        Assert.Equal(6, chunks[1][2].Id);

        // Chunk 2: [7] (Remainder chunk)
        Assert.Single(chunks[2]);
        Assert.Equal(7, chunks[2][0].Id);
    }
    [Fact]
    [Trait("LINQ", "TakeLast")]
    public void Test_Linq_TakeLast_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("Age", [20, 25, 30, 35, 40]),
            Series.From("Salary", [5000, 6000, 7000, 8000, 9000])
        ]);

        var results = df.AsQueryable<Employee>()
            .TakeLast(2)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("David", results[0].Name);
        Assert.Equal("Eve", results[1].Name);
    }
    [Fact]
    [Trait("LINQ", "SkipLast")]
    public void Test_Linq_SkipLast_Pushdown()
    {
        // 1. Prepare 5 employee records
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve"]),
            Series.From("Age", [25, 30, 35, 40, 45]),
            Series.From("Salary", [50000, 60000, 70000, 80000, 90000])
        ]);

        // 2. Skip the last 2 records natively via LINQ (should retain Alice, Bob, Charlie)
        var results = df.AsQueryable<Employee>()
            .SkipLast(2)
            .ToList();

        // 3. Assert count and elements
        Assert.Equal(3, results.Count);

        // Row 0: Alice
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(25, results[0].Age);
        Assert.Equal(50000, results[0].Salary);

        // Row 1: Bob
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal(30, results[1].Age);
        Assert.Equal(60000, results[1].Salary);

        // Row 2: Charlie
        Assert.Equal("Charlie", results[2].Name);
        Assert.Equal(35, results[2].Age);
        Assert.Equal(70000, results[2].Salary);
    }

    [Fact]
    [Trait("LINQ", "SkipLast")]
    public void Test_Linq_SkipLast_ExceedingCount_ReturnsEmpty()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Age", [25, 30]),
            Series.From("Salary", [50000, 60000])
        ]);

        // Skipping count >= total rows returns an empty sequence
        var results = df.AsQueryable<Employee>()
            .SkipLast(5)
            .ToList();

        Assert.Empty(results);
    }
    [Fact]
    [Trait("LINQ", "AppendPrepend")]
    public void Test_Linq_Append_And_Prepend_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Bob"]),
            Series.From("Age", [30]),
            Series.From("Salary", [6000])
        ]);

        var first = new Employee("Alice", 25, 5000);
        var last = new Employee("Charlie", 35, 7000);

        var results = df.AsQueryable<Employee>()
            .Prepend(first)
            .Append(last)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Charlie", results[2].Name);
    }
    [Fact]
    [Trait("LINQ", "Reverse")]
    public void Test_Linq_Reverse_Pushdown()
    {
        // 1. Prepare 3 employee records
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        // 2. Reverse row order natively via LINQ
        var results = df.AsQueryable<Employee>()
            .Reverse()
            .ToList();

        Assert.Equal(3, results.Count);

        // Row 0: Charlie
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal(35, results[0].Age);
        Assert.Equal(70000, results[0].Salary);

        // Row 1: Bob
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal(30, results[1].Age);
        Assert.Equal(60000, results[1].Salary);

        // Row 2: Alice
        Assert.Equal("Alice", results[2].Name);
        Assert.Equal(25, results[2].Age);
        Assert.Equal(50000, results[2].Salary);
    }
    [Fact]
    [Trait("LINQ", "Shuffle")]
    public void Test_Linq_Shuffle_PreservesCountAndElements()
    {
        // 1. Prepare 10 records
        var ids = Enumerable.Range(1, 10).ToArray();
        var ages = ids.Select(i => 20 + i).ToArray();
        var salaries = ids.Select(i => 5000 + i * 100).ToArray();
        var names = ids.Select(i => $"User_{i}").ToArray();

        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", ids),
            Series.From("Name", names),
            Series.From("DeptId", ids.Select(_ => 1).ToArray()),
            Series.From("Age", ages),
            Series.From("Salary", salaries)
        ]);

        // 2. Shuffle rows natively
        var results = df.AsQueryable<MemberRecord>()
            .Shuffle()
            .ToList();

        // 3. Row count and distinct elements must remain identical
        Assert.Equal(10, results.Count);
        Assert.Equal(10, results.Select(r => r.Id).Distinct().Count());
        Assert.All(results, r => Assert.Contains(r.Id, ids));
    }
    [Fact]
    [Trait("LINQ", "Scalar")]
    public void Test_Linq_Scalar_Aggregations_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 1. Count
        Assert.Equal(3, query.Count());
        Assert.Equal(2, query.Count(e => e.Age >= 30));

        // 2. Any
        Assert.True(query.Any(e => e.Salary > 65000));
        Assert.False(query.Any(e => e.Salary > 100000));

        // 3. First & Last
        var first = query.OrderBy(e => e.Age).First();
        Assert.Equal("Alice", first.Name);

        var last = query.OrderBy(e => e.Age).Last();
        Assert.Equal("Charlie", last.Name);

        // 4. Max, Min, Sum, Average
        Assert.Equal(70000, query.Max(e => e.Salary));
        Assert.Equal(50000, query.Min(e => e.Salary));
        Assert.Equal(180000, query.Sum(e => e.Salary));
        Assert.Equal(30.0, query.Average(e => e.Age), 2);
    }

    [Fact]
    [Trait("LINQ", "Cast_OfType")]
    public void Test_Linq_Cast_And_OfType_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Age", [28, 35]),
            Series.From("Salary", [75000, 90000])
        ]);

        var query = df.AsQueryable<WorkerRecord>();

        // 1. OfType with compatible interface type -> Retains all records
        var workers = query.OfType<WorkerRecord>().ToList();
        Assert.Equal(2, workers.Count);
        Assert.Equal("Alice", workers[0].Name);

        // 2. OfType with completely incompatible type -> Filtered to empty
        var emptyDepartments = query.OfType<Department>().ToList();
        Assert.Empty(emptyDepartments);

        // 3. Cast preserves element mappings
        var castedWorkers = query.Cast<WorkerRecord>().ToList();
        Assert.Equal(2, castedWorkers.Count);
        Assert.Equal("Bob", castedWorkers[1].Name);
    }
    [Fact]
    [Trait("LINQ", "Scalar_FirstLastOrDefault")]
    public void Test_Linq_FirstOrDefault_And_LastOrDefault_Pushdown()
    {
        // 1. Prepare populated dataset
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 2. Normal matches
        var first = query.OrderBy(e => e.Age).FirstOrDefault();
        Assert.Equal("Alice", first.Name);
        Assert.Equal(25, first.Age);

        var last = query.OrderBy(e => e.Age).LastOrDefault();
        Assert.Equal("Charlie", last.Name);
        Assert.Equal(35, last.Age);

        // 3. Filtered matches that yield no rows (should return default Employee)
        var missingFirst = query.Where(e => e.Age > 100).FirstOrDefault();
        Assert.Equal(default, missingFirst);

        var missingLast = query.Where(e => e.Salary < 1000).LastOrDefault();
        Assert.Equal(default, missingLast);
    }
    [Fact]
    [Trait("LINQ", "Scalar_First")]
    public void Test_Linq_First_And_FirstOrDefault_ShortCircuit()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000L, 60000L, 70000L])
        ]);

        var query = df.AsQueryable<Employee>();

        Assert.Equal("Alice", query.First().Name);
        Assert.Equal("Alice", query.FirstOrDefault().Name);

        var bob = query.First(e => e.Age == 30);
        Assert.Equal("Bob", bob.Name);
        Assert.Equal(60000L, bob.Salary);

        Assert.Throws<InvalidOperationException>(() => query.First(e => e.Age > 100));
    }

    [Fact]
    [Trait("LINQ", "Scalar_FirstLastOrDefault")]
    public void Test_Linq_FirstLastOrDefault_OnEmptyTable_ReturnsDefault()
    {
        // Empty table scenario
        using var emptyDf = DataFrame.FromColumns(
        [
            Series.From("Name", Array.Empty<string>()),
            Series.From("Age", Array.Empty<int>()),
            Series.From("Salary", Array.Empty<int>())
        ]);

        var query = emptyDf.AsQueryable<Employee>();

        var first = query.FirstOrDefault();
        Assert.Equal(default, first);

        var last = query.LastOrDefault();
        Assert.Equal(default, last);
    }
    [Fact]
    [Trait("LINQ", "Scalar_Single")]
    public void Test_Linq_Single_And_SingleOrDefault_Pushdown()
    {
        // 1. Prepare 3 employee records
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 2. Exactly one match
        var singleAlice = query.Single(e => e.Name == "Alice");
        Assert.Equal("Alice", singleAlice.Name);
        Assert.Equal(25, singleAlice.Age);

        var singleOrDefaultBob = query.SingleOrDefault(e => e.Age == 30);
        Assert.Equal("Bob", singleOrDefaultBob.Name);

        // 3. Zero match
        var missingDefault = query.SingleOrDefault(e => e.Salary > 200000);
        Assert.Equal(default, missingDefault);

        Assert.Throws<InvalidOperationException>(() =>
        {
            query.Single(e => e.Salary > 200000);
        });

        // 4. More than one match -> Both Single and SingleOrDefault MUST throw
        Assert.Throws<InvalidOperationException>(() =>
        {
            query.Single(e => e.Age >= 25);
        });

        Assert.Throws<InvalidOperationException>(() =>
        {
            query.SingleOrDefault(e => e.Age >= 25);
        });
    }
    [Fact]
    [Trait("LINQ", "Scalar_ElementAt")]
    public void Test_Linq_ElementAt_And_ElementAtOrDefault_Pushdown()
    {
        // 1. Prepare 3 employee records
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 2. Valid indexes within bounds
        var row0 = query.ElementAt(0);
        Assert.Equal("Alice", row0.Name);

        var row1 = query.ElementAtOrDefault(1);
        Assert.Equal("Bob", row1.Name);

        var row2 = query.ElementAt(2);
        Assert.Equal("Charlie", row2.Name);

        // 3. Out-of-bounds (index >= count)
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            query.ElementAt(3);
        });

        var outOfBoundsDefault = query.ElementAtOrDefault(5);
        Assert.Equal(default, outOfBoundsDefault);

        // 4. Negative index (< 0)
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            query.ElementAt(-1);
        });

        var negativeDefault = query.ElementAtOrDefault(-1);
        Assert.Equal(default, negativeDefault);
    }
    [Fact]
    [Trait("LINQ", "DefaultIfEmpty")]
    public void Test_Linq_DefaultIfEmpty()
    {
        // 1. Non-empty DataFrame -> returns original elements
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Age", [25, 30]),
            Series.From("Salary", [50000, 60000])
        ]);

        var nonEmptyResults = df.AsQueryable<Employee>()
            .DefaultIfEmpty()
            .ToList();

        Assert.Equal(2, nonEmptyResults.Count);
        Assert.Equal("Alice", nonEmptyResults[0].Name);

        // 2. Filtered to empty -> returns single default(Employee)
        var emptyResults = df.AsQueryable<Employee>()
            .Where(e => e.Age > 100)
            .DefaultIfEmpty()
            .ToList();

        Assert.Single(emptyResults);
        Assert.Equal(default, emptyResults[0]);

        // 3. Custom defaultValue provided
        var customDefault = new Employee("Unknown", 0, 0);
        var customResults = df.AsQueryable<Employee>()
            .Where(e => e.Age > 100)
            .DefaultIfEmpty(customDefault)
            .ToList();

        Assert.Single(customResults);
        Assert.Equal("Unknown", customResults[0].Name);
    }

    [Fact]
    [Trait("LINQ", "Contains")]
    public void Test_Linq_Contains_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Age", [25, 30]),
            Series.From("Salary", [50000, 60000])
        ]);

        var query = df.AsQueryable<Employee>();

        var existingAlice = new Employee("Alice", 25, 50000);
        var missingCharlie = new Employee("Charlie", 35, 70000);

        Assert.True(query.Contains(existingAlice));
        Assert.False(query.Contains(missingCharlie));
    }
    [Fact]
    [Trait("LINQ", "Contains")]
    public void Test_Linq_Contains_FullRecord_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 20, 10, 30])
        ]);

        var query = df.AsQueryable<Person>();

        // 1. Existing person: Bob (2, "Bob", 20) -> should be true
        var existingPerson = new Person(2, "Bob", 20);
        var containsExisting = query.Contains(existingPerson);
        Assert.True(containsExisting);

        // 2. Non-existing person (wrong DeptId): Bob with DeptId 99 -> should be false
        var mismatchPerson = new Person(2, "Bob", 99);
        var containsMismatch = query.Contains(mismatchPerson);
        Assert.False(containsMismatch);

        // 3. Completely non-existing person
        var stranger = new Person(99, "Eve", 50);
        var containsStranger = query.Contains(stranger);
        Assert.False(containsStranger);
    }

    [Fact]
    [Trait("LINQ", "Contains")]
    public void Test_Linq_Contains_SingleColumn_Scalar_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("DeptId", [10, 20, 10, 30])
        ]);

        // Scalar projection + Contains pushdown
        var nameQuery = df.AsQueryable<Person>().Select(p => p.Name);

        Assert.True(nameQuery.Contains("Alice"));
        Assert.True(nameQuery.Contains("David"));
        Assert.False(nameQuery.Contains("Frank"));

        // Scalar int projection + Contains pushdown
        var idQuery = df.AsQueryable<Person>().Select(p => p.Id);

        Assert.True(idQuery.Contains(1));
        Assert.True(idQuery.Contains(4));
        Assert.False(idQuery.Contains(42));
    }
    [Fact]
    [Trait("LINQ", "Scalar_SequenceEqual")]
    public void Test_Linq_SequenceEqual_Pushdown()
    {
        using var df1 = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        using var df2Identical = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        using var df3DifferentOrder = DataFrame.FromColumns(
        [
            Series.From("Name", ["Bob", "Alice", "Charlie"]),
            Series.From("Age", [30, 25, 35]),
            Series.From("Salary", [60000, 50000, 70000])
        ]);

        using var df4DifferentLength = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Age", [25, 30]),
            Series.From("Salary", [50000, 60000])
        ]);

        var q1 = df1.AsQueryable<Employee>();
        var q2 = df2Identical.AsQueryable<Employee>();
        var q3 = df3DifferentOrder.AsQueryable<Employee>();
        var q4 = df4DifferentLength.AsQueryable<Employee>();

        // 1. Identical sequences in same order -> true
        Assert.True(q1.SequenceEqual(q2));

        // 2. Different row order -> false
        Assert.False(q1.SequenceEqual(q3));

        // 3. Different row counts -> false
        Assert.False(q1.SequenceEqual(q4));

        // 4. SequenceEqual against in-memory sequence fallback
        var inMemoryList = new List<Employee>
        {
            new("Alice", 25, 50000),
            new("Bob", 30, 60000),
            new("Charlie", 35, 70000)
        };
        Assert.True(q1.SequenceEqual(inMemoryList));
    }
    [Fact]
    [Trait("LINQ", "Scalar_All")]
    public void Test_Linq_All_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 1. All match -> true
        Assert.True(query.All(e => e.Age >= 20));
        Assert.True(query.All(e => e.Salary > 40000));

        // 2. Some match, but not all -> false
        Assert.False(query.All(e => e.Age > 25));
        Assert.False(query.All(e => e.Salary >= 60000));

        // 3. None match -> false
        Assert.False(query.All(e => e.Age < 20));
    }

    [Fact]
    [Trait("LINQ", "Scalar_All")]
    public void Test_Linq_All_OnEmptyTable_ReturnsTrue()
    {
        // Vacuum truth: All elements of an empty set satisfy any condition
        using var emptyDf = DataFrame.FromColumns(
        [
            Series.From("Name", Array.Empty<string>()),
            Series.From("Age", Array.Empty<int>()),
            Series.From("Salary", Array.Empty<int>())
        ]);

        var query = emptyDf.AsQueryable<Employee>();

        Assert.True(query.All(e => e.Age > 100));
    }

    [Fact]
    [Trait("LINQ", "Scalar_Any")]
    public void Test_Linq_Any_Pushdown_And_ShortCircuit()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000L, 60000L, 70000L])
        ]);

        var query = df.AsQueryable<Employee>();

        // ==========================================
        // 1. Any() without predicate (Existence check)
        // ==========================================
        // Non-empty DataFrame -> true
        Assert.True(query.Any());

        // Empty DataFrame via filtered query -> false
        Assert.False(query.Where(e => e.Age > 100).Any());

        // ==========================================
        // 2. Any(predicate) with condition (Short-circuit scan)
        // ==========================================
        // Case A: Matches first row directly (Early exit on first element) -> true
        Assert.True(query.Any(e => e.Age == 25));

        // Case B: Matches intermediate or last row -> true
        Assert.True(query.Any(e => e.Name == "Charlie"));
        Assert.True(query.Any(e => e.Salary >= 60000L));

        // Case C: Matches all rows -> true
        Assert.True(query.Any(e => e.Age >= 20));

        // Case D: Matches no rows -> false
        Assert.False(query.Any(e => e.Age < 20));
        Assert.False(query.Any(e => e.Salary > 100000L));
        Assert.False(query.Any(e => e.Name == "NonExistent"));
    }

    [Fact]
    [Trait("LINQ", "CountBy")]
    public void Test_Linq_CountBy_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "HR", "IT", "Finance", "IT", "HR"]),
            Series.From("Salary", [50000, 60000, 70000, 80000, 90000, 65000])
        ]);

        var query = df.AsQueryable<DepartmentRecord>();

        // Native CountBy pushdown: groups by Department, counts occurrences
        var counts = query.CountBy(d => d.Department).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        Assert.Equal(3, counts["IT"]);
        Assert.Equal(2, counts["HR"]);
        Assert.Equal(1, counts["Finance"]);
    }
    [Fact]
    [Trait("LINQ", "Scalar_MinBy_MaxBy")]
    public void Test_Linq_MinBy_And_MaxBy_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Age", [28, 35, 22, 40]),
            Series.From("Salary", [75000, 90000, 50000, 110000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 1. MinBy: Employee with youngest Age (Charlie, 22)
        var youngest = query.MinBy(e => e.Age);
        Assert.Equal("Charlie", youngest.Name);
        Assert.Equal(22, youngest.Age);

        // 2. MaxBy: Employee with highest Salary (David, 110000)
        var highestEarner = query.MaxBy(e => e.Salary);
        Assert.Equal("David", highestEarner.Name);
        Assert.Equal(110000, highestEarner.Salary);

        // 3. Filtered to empty: returns default(Employee)
        var missingMin = query.Where(e => e.Age > 100).MinBy(e => e.Salary);
        Assert.Equal(default, missingMin);

        var missingMax = query.Where(e => e.Salary < 1000).MaxBy(e => e.Age);
        Assert.Equal(default, missingMax);
    }
    [Fact]
    [Trait("LINQ", "Aggregate")]
    public void Test_Linq_Aggregate()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // 1. Simple Accumulation: Sum of salaries using seed
        var totalSalary = query.Aggregate(0, (acc, e) => acc + e.Salary);
        Assert.Equal(180000, totalSalary);

        // 2. String Concatenation with Result Selector
        var summary = query.Aggregate(
            new StringBuilder(),
            (sb, e) => sb.Append(e.Name[0]),
            sb => sb.ToString());

        Assert.Equal("ABC", summary);
    }

    [Fact]
    [Trait("LINQ", "AggregateBy")]
    public void Test_Linq_AggregateBy()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "HR", "IT", "Finance", "IT", "HR"]),
            Series.From("Salary", [50000, 60000, 70000, 80000, 90000, 65000])
        ]);

        var query = df.AsQueryable<DepartmentRecord>();

        // Aggregate total salary by department: KeyValuePair<string, int>
        var deptSalaries = query
            .AggregateBy(
                d => d.Department,
                seed: 0,
                (acc, d) => acc + d.Salary)
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        Assert.Equal(210000, deptSalaries["IT"]);       // 50000 + 70000 + 90000
        Assert.Equal(125000, deptSalaries["HR"]);       // 60000 + 65000
        Assert.Equal(80000, deptSalaries["Finance"]);   // 80000
    }
    [Fact]
    [Trait("LINQ", "Aggregate_Overloads")]
    public void Test_Linq_Aggregate_All_Overloads()
    {
        // 1. Prepare test dataset
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [20, 30, 40]),
            Series.From("Salary", [50000, 60000, 70000])
        ]);

        var query = df.AsQueryable<Employee>();

        // Overload 1: Aggregate(source, func) -> Fold without seed
        // Picks the person with the longest Name
        var longestNameEmployee = query.Aggregate((longest, current) =>
            current.Name.Length > longest.Name.Length ? current : longest);
        Assert.Equal("Charlie", longestNameEmployee.Name);

        // Overload 2: Aggregate(source, seed, func) -> Seeded fold
        // Sum total salaries
        var totalSalary = query.Aggregate(10000, (acc, e) => acc + e.Salary);
        Assert.Equal(190000, totalSalary); // 10000 + (50000 + 60000 + 70000)

        // Overload 3: Aggregate(source, seed, func, resultSelector) -> Fold with projection
        // Accumulate average age description
        var ageReport = query.Aggregate(
            0,
            (acc, e) => acc + e.Age,
            totalAge => $"Average Age: {totalAge / 3.0:F1}");
        Assert.Equal("Average Age: 30.0", ageReport);
    }

    [Fact]
    [Trait("LINQ", "AggregateBy_Overloads")]
    public void Test_Linq_AggregateBy_All_Overloads()
    {
        // 1. Prepare test dataset with departments
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "HR", "IT", "Finance", "IT", "HR"]),
            Series.From("Salary", [50000, 60000, 70000, 80000, 90000, 65000])
        ]);

        var query = df.AsQueryable<DepartmentRecord>();

        // Overload 1: AggregateBy(keySelector, seed, func) -> Static seed
        var staticSeedResults = query
            .AggregateBy(
                d => d.Department,
                seed: 1000,
                (acc, d) => acc + d.Salary)
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        Assert.Equal(211000, staticSeedResults["IT"]);       // 1000 + (50000 + 70000 + 90000)
        Assert.Equal(126000, staticSeedResults["HR"]);       // 1000 + (60000 + 65000)
        Assert.Equal(81000, staticSeedResults["Finance"]);   // 1000 + 80000

        // Overload 2: AggregateBy(keySelector, seedSelector, func) -> Seed factory delegate
        // Seed starts with 100 for "IT", 200 for "HR", and 300 for others
        var seedFactoryResults = query
            .AggregateBy(
                d => d.Department,
                seedSelector: dept => dept == "IT" ? 100 : (dept == "HR" ? 200 : 300),
                (acc, d) => acc + d.Salary)
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        Assert.Equal(210100, seedFactoryResults["IT"]);       // 100 + 210000
        Assert.Equal(125200, seedFactoryResults["HR"]);       // 200 + 125000
        Assert.Equal(80300, seedFactoryResults["Finance"]);   // 300 + 80000
    }
    [Fact]
    [Trait("LINQ", "Index")]
    public void Test_Linq_Index_WithRowIndexPushdown()
    {
        // 1. Arrange: Sample Employees
        using var df = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [25, 30, 35]),
            Series.From("Salary", [60000, 80000, 95000])
        ]);

        // 2. Act: Call .NET 9 Index()
        // Index() yields tuples of (Index, Item)
        var results = df.AsQueryable<Employee>()
            .Index()
            .ToList();
            
        // Assert: Verify 0-based index and element mapping
        Assert.Equal(3, results.Count);

        Assert.Equal(0, results[0].Index);
        Assert.Equal("Alice", results[0].Item.Name);
        Assert.Equal(25, results[0].Item.Age);

        Assert.Equal(1, results[1].Index);
        Assert.Equal("Bob", results[1].Item.Name);
        Assert.Equal(30, results[1].Item.Age);

        Assert.Equal(2, results[2].Index);
        Assert.Equal("Charlie", results[2].Item.Name);
        Assert.Equal(35, results[2].Item.Age);
    }

    [Fact]
    [Trait("LINQ", "Order")]
    public void Test_Linq_Order_And_OrderDescending()
    {
        // 1. Arrange: Single column scalar sequence of IDs
        using var df = DataFrame.FromColumns(
        [
            Series.From("Id", [40, 10, 30, 20])
        ]);

        // 2. Act: Test .NET 7 Order() (Ascending)
        var ascResults = df.AsQueryable<int>()
            .Order()
            .ToList();

        // 3. Act: Test .NET 7 OrderDescending()
        var descResults = df.AsQueryable<int>()
            .OrderDescending()
            .ToList();

        // 4. Assert
        Assert.Equal([10, 20, 30, 40], ascResults);
        Assert.Equal([40, 30, 20, 10], descResults);
    }

    [Fact]
    [Trait("LINQ", "Zip3")]
    public void Test_Linq_Zip3_And_Project()
    {
        // 1. First DataFrame: Identifiers & Department references
        using var df1 = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("DeptId", [10, 20, 30])
        ]);

        // 2. Second DataFrame: Employee details
        using var df2 = DataFrame.FromColumns(
        [
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Age", [28, 35, 42]),
            Series.From("Salary", [75000, 90000, 120000])
        ]);

        // 3. Third DataFrame: Departments
        using var df3 = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20, 30]),
            Series.From("DeptName", ["HR", "IT", "Finance"])
        ]);

        // 4. BCL Standard: Zip 3 sequences to tuple, then project to EmployeeDeptRecord
        var results = df1.AsQueryable<Person>()
            .Zip(
                df2.AsQueryable<Employee>(),
                df3.AsQueryable<Department>()
            )
            .Select(t => new EmployeeDeptRecord(t.First.Id, t.Second.Name, t.Third.DeptName))
            .OrderBy(record => record.Id)
            .ToList();

        // 5. Assert: Verify merged and projected columns
        Assert.Equal(3, results.Count);

        // Row 1: Alice (Id: 1, Dept: HR)
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("HR", results[0].DeptName);

        // Row 2: Bob (Id: 2, Dept: IT)
        Assert.Equal(2, results[1].Id);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("IT", results[1].DeptName);

        // Row 3: Charlie (Id: 3, Dept: Finance)
        Assert.Equal(3, results[2].Id);
        Assert.Equal("Charlie", results[2].Name);
        Assert.Equal("Finance", results[2].DeptName);
    }
    [Fact]
    [Trait("LINQ", "ZipDuplicateColumns")]
    public void Test_Linq_Zip_WithDuplicateColumns_DisambiguatesAndProjects()
    {
        // 1. Arrange: Both DataFrames contain colliding column names: "Id" and "DeptId"
        // First DataFrame: Person records with Id, Name, DeptId
        using var df1 = DataFrame.FromColumns(
        [
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("DeptId", [10, 20, 30])
        ]);

        // Second DataFrame: Department records with Id, DeptName
        using var df2 = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20, 30]),
            Series.From("DeptName", ["HR", "IT", "Finance"])
        ]);

        // 2. Act (Case A): 2-way Zip into ValueTuple (Person First, Department Second)
        var tupleResults = df1.AsQueryable<Person>()
            .Zip(df2.AsQueryable<Department>())
            .ToList();

        // 3. Assert (Case A): Tuple elements must resolve their respective 'Id' correctly
        Assert.Equal(3, tupleResults.Count);

        // Row 1
        Assert.Equal(1, tupleResults[0].First.Id);
        Assert.Equal("Alice", tupleResults[0].First.Name);
        Assert.Equal(10, tupleResults[0].Second.Id);
        Assert.Equal("HR", tupleResults[0].Second.DeptName);

        // Row 2
        Assert.Equal(2, tupleResults[1].First.Id);
        Assert.Equal("Bob", tupleResults[1].First.Name);
        Assert.Equal(20, tupleResults[1].Second.Id);
        Assert.Equal("IT", tupleResults[1].Second.DeptName);

        // Row 3
        Assert.Equal(3, tupleResults[2].First.Id);
        Assert.Equal("Charlie", tupleResults[2].First.Name);
        Assert.Equal(30, tupleResults[2].Second.Id);
        Assert.Equal("Finance", tupleResults[2].Second.DeptName);

        // 4. Act (Case B): 2-way Zip with custom resultSelector projecting to PersonDeptJoined
        var projectedResults = df1.AsQueryable<Person>()
            .Zip(
                df2.AsQueryable<Department>(),
                (p, d) => new PersonDeptJoined(p.Id, p.Name, p.DeptId, d.DeptName)
            )
            .OrderBy(p => p.Id)
            .ToList();

        // 5. Assert (Case B): Projected DTO correctly maps fields without column collision exception
        Assert.Equal(3, projectedResults.Count);

        Assert.Equal(new PersonDeptJoined(1, "Alice", 10, "HR"), projectedResults[0]);
        Assert.Equal(new PersonDeptJoined(2, "Bob", 20, "IT"), projectedResults[1]);
        Assert.Equal(new PersonDeptJoined(3, "Charlie", 30, "Finance"), projectedResults[2]);
    }
    [Fact]
    [Trait("LINQ", "ComplexEndToEnd")]
    public void Test_Linq_Complex_EndToEnd_Aggregation_Pipeline()
    {
        // -------------------------------------------------------------
        // 1. Arrange: Prepare mock relational tables using existing records
        // -------------------------------------------------------------

        // Left Table: Persons (Id, Name, DeptId)
        using var personDf = DataFrame.FromColumns(
        [
            Series.From("Id", [101, 102, 201, 202, 203, 301, 302, 999]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi"]),
            Series.From("DeptId", [10, 10, 20, 20, 20, 30, 30, 99]) // Dept 99 has no dept match
        ]);

        // Right Table: Departments (Id, DeptName)
        using var deptDf = DataFrame.FromColumns(
        [
            Series.From("Id", [10, 20, 30, 40]),
            Series.From("DeptName", ["Engineering", "Finance", "HR", "Marketing"])
        ]);

        // Archive Table: Historical Summary for Union testing (DeptPersonSummary)
        using var archiveDf = DataFrame.FromColumns(
        [
            Series.From("DeptId", [40]),
            Series.From("MemberCount", [1L]),
            Series.From("MinId", [401]),
            Series.From("MaxId", [401])
        ]);

        // -------------------------------------------------------------
        // 2. Act: Construct and execute complex composite query plan
        // -------------------------------------------------------------

        // Pipeline A: Join -> Filter -> GroupBy(key, elem, res) -> Having
        var activeDeptSummary = personDf.AsQueryable<Person>()
            // 2.1 Inner Join Persons with Departments on DeptId == Id
            .Join(
                deptDf.AsQueryable<Department>(),
                p => p.DeptId,
                d => d.Id,
                (p, d) => new EmployeeDeptRecord(p.Id, p.Name, d.DeptName)
            )
            // 2.2 Native Filter: Only consider valid positive employee IDs > 101
            .Where(x => x.Id > 101)
            // 2.3 GroupBy with element & result selectors: Group by DeptId (mapped from Id / Dept range)
            // Here we group by (x.Id / 10 * 10) to map to DeptId, selecting x.Id for aggregations
            .GroupBy(
                x => x.Id / 10,
                x => x.Id,
                (deptId, ids) => new DeptPersonSummary(
                    deptId,
                    ids.LongCount(),
                    ids.Min(),
                    ids.Max()
                )
            )
            // 2.4 Native Having pushdown: Only departments with MemberCount >= 2
            .Where(summary => summary.MemberCount >= 2L);

        // Pipeline B: Union with archive -> OrderBy -> Slicing -> .NET 9 Index()
        var finalRankedResults = activeDeptSummary
            // 2.5 Vertical Concat + Deduplication via Union
            .Union(archiveDf.AsQueryable<DeptPersonSummary>())
            // 2.6 Multi-column ordering: Sort by MemberCount descending, then DeptId ascending
            .OrderByDescending(s => s.MemberCount)
            .ThenBy(s => s.DeptId)
            // 2.7 Slice: Skip 0, Take top 2
            .Skip(0)
            .Take(2)
            // 2.8 Attach 0-based native rank using .NET 9 Index()
            .Index()
            .ToList();
        // -------------------------------------------------------------
        // 3. Assert: Verify end-to-end data integrity
        // -------------------------------------------------------------

        // Filter: Id > 101 filters out Alice(101)
        // Group breakdown:
        // - Dept 20 (Finance): Charlie(201), David(202), Eve(203) -> MemberCount: 3, MinId: 201, MaxId: 203
        // - Dept 30 (HR): Frank(301), Grace(302)                 -> MemberCount: 2, MinId: 301, MaxId: 302
        // - Dept 10 (Engineering): Bob(102)                      -> MemberCount: 1 (Filtered out by Having >= 2L!)
        // Archive Group:
        // - Dept 40 (Marketing): MemberCount: 1, Min: 401, Max: 401
        
        // Sorted By MemberCount Descending:
        // Rank 0: Dept 20 (Finance) - MemberCount: 3
        // Rank 1: Dept 30 (HR)      - MemberCount: 2
        // Rank 2: Dept 40 (Archive) - MemberCount: 1 (Sliced out by Take(2))

        Assert.Equal(2, finalRankedResults.Count);

        // Rank 0 Check
        var rank0 = finalRankedResults[0];
        Assert.Equal(0, rank0.Index);
        Assert.Equal(20, rank0.Item.DeptId);
        Assert.Equal(3L, rank0.Item.MemberCount);
        Assert.Equal(201, rank0.Item.MinId);
        Assert.Equal(203, rank0.Item.MaxId);

        // Rank 1 Check
        var rank1 = finalRankedResults[1];
        Assert.Equal(1, rank1.Index);
        Assert.Equal(30, rank1.Item.DeptId);
        Assert.Equal(2L, rank1.Item.MemberCount);
        Assert.Equal(301, rank1.Item.MinId);
        Assert.Equal(302, rank1.Item.MaxId);
    }
    [Fact]
    [Trait("LINQ", "Expr_TernaryConditional")]
    public void Test_Linq_TernaryConditional_Pushdown()
    {
        using var df = DataFrame.FromColumns(
        [
            Series.From("Department", ["IT", "HR", "IT", "Finance"]),
            Series.From("Salary", [50000, 60000, 70000, 80000])
        ]);

        var query = df.AsQueryable<DepartmentRecord>();

        // 1. Conditional in Where filter:
        // If IT, check Salary > 60000; otherwise check Salary >= 80000
        var filtered = query
            .Where(d => d.Department == "IT" ? d.Salary > 60000 : d.Salary >= 80000)
            .ToList();

        Assert.Equal(2, filtered.Count);
        Assert.Contains(filtered, d => d.Department == "IT" && d.Salary == 70000);
        Assert.Contains(filtered, d => d.Department == "Finance" && d.Salary == 80000);

        // 2. Chained nested conditional with ToDataFrame()
        // Bonus calculation: IT gets 10000, HR gets 5000, others get 2000
        var projected = query
            .Select(d => new
            {
                d.Department,
                Bonus = d.Department == "IT" ? 10000 : (d.Department == "HR" ? 5000 : 2000)
            })
            .ToDataFrame();

        // Verifies the Bonus column is generated entirely within Native Polars
        var bonusCol = projected["Bonus"].ToArray<int>();
        Assert.Equal([10000, 5000, 10000, 2000], bonusCol);
    }
    [Fact]
    [Trait("LINQ", "AnonymousType")]
    public void Test_Linq_AnonymousType_Sequence_Inference()
    {
        var employees = new[]
        {
            new { Id = 1, Name = "Alice",   Age = 25, Salary = 5000 },
            new { Id = 2, Name = "Bob",     Age = 17, Salary = 3000 },
            new { Id = 3, Name = "Charlie", Age = 30, Salary = 7000 },
            new { Id = 4, Name = "David",   Age = 15, Salary = 2000 }
        };

        using var df = DataFrame.FromRows(employees);

        var query = df.AsQueryable(employees);

        var threshold = 18;
        var results = query.Where(e => e.Age >= threshold && e.Salary > 4000)
                           .OrderBy(e => e.Age)
                           .Take(10)
                           .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(25, results[0].Age);
        Assert.Equal(5000, results[0].Salary);

        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal(30, results[1].Age);
        Assert.Equal(7000, results[1].Salary);
    }

    [Fact]
    [Trait("LINQ", "AnonymousType")]
    public void Test_Linq_AnonymousType_FromDataArray_Aggregation()
    {
        var data = new[]
        {
            new { Dept = "IT",      Salary = 60000, Bonus = 5000 },
            new { Dept = "HR",      Salary = 45000, Bonus = 3000 },
            new { Dept = "IT",      Salary = 80000, Bonus = 7000 },
            new { Dept = "Finance", Salary = 55000, Bonus = 4000 }
        };

        // DataFrame created from array of anonymous types
        using var df = DataFrame.FromRows(data);

        // Directly infer type T from the source array
        var query = df.AsQueryable(data);

        // 1. Where + Count
        var itCount = query.Where(x => x.Dept == "IT").Count();
        Assert.Equal(2, itCount);

        // 2. MaxBy equivalent (OrderByDescending + First)
        var highestEarner = query.OrderByDescending(x => x.Salary).First();
        Assert.Equal("IT", highestEarner.Dept);
        Assert.Equal(80000, highestEarner.Salary);

        // 3. Projection to a new anonymous shape
        var summaries = query.Where(x => x.Bonus >= 4000)
                             .Select(x => new { DepartmentName = x.Dept, TotalCompensation = x.Salary + x.Bonus })
                             .OrderByDescending(x => x.TotalCompensation)
                             .ToList();

        Assert.Equal(3, summaries.Count);
        Assert.Equal("IT", summaries[0].DepartmentName);
        Assert.Equal(87000, summaries[0].TotalCompensation);
    }
    [Fact]
    [Trait("LINQ", "Scalar_Count")]
    public void Test_CSharp_Linq_Scalar_Count_And_LongCount()
    {
        var employees = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000),
            new Employee("Charlie", 35, 70000),
            new Employee("David", 30, 80000)
        };

        using var df = DataFrame.FromRows(employees);
        var query = df.AsQueryable(employees);

        int totalCount = query.Count();
        long totalLongCount = query.LongCount();
        Assert.Equal(4, totalCount);
        Assert.Equal(4L, totalLongCount);

        int countAge30 = query.Count(e => e.Age == 30);
        long longCountAge30 = query.LongCount(e => e.Age == 30);
        Assert.Equal(2, countAge30);
        Assert.Equal(2L, longCountAge30);

        int countNone = query.Count(e => e.Salary > 1000000L);
        Assert.Equal(0, countNone);

        var upperQuery = query.Select(e => e.Name.ToUpper());
        int countStartsWithA = upperQuery.Count(name => name.StartsWith("A"));
        long longCountLength5 = upperQuery.LongCount(name => name.Length == 5); // ALICE (5), DAVID (5)

        Assert.Equal(1, countStartsWithA);
        Assert.Equal(2L, longCountLength5);
    }
    [Fact]
    [Trait("LINQ", "Scalar_Single")]
    public void Test_CSharp_Linq_Scalar_Single_And_SingleOrDefault()
    {
        var emps = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000),
            new Employee("Charlie", 35, 70000)
        };

        using var df = DataFrame.FromRows(emps);
        var query = df.AsQueryable(emps);

        Assert.Throws<InvalidOperationException>(() => query.Single());
        Assert.Throws<InvalidOperationException>(() => query.SingleOrDefault());

        var alice = query.Single(e => e.Age == 25);
        Assert.Equal("Alice", alice.Name);

        var aliceOrDef = query.SingleOrDefault(e => e.Age == 25);
        Assert.Equal("Alice", aliceOrDef.Name);

        Assert.Throws<InvalidOperationException>(() => query.Single(e => e.Age == 99));

        var upperQuery = query.Select(e => e.Name.ToUpper());
        Assert.Throws<InvalidOperationException>(() => upperQuery.Single(name => name.StartsWith('B') || name.StartsWith('C')));

        var uniqueBob = upperQuery.Single(name => name == "BOB");
        Assert.Equal("BOB", uniqueBob);
    }
    [Fact]
    [Trait("LINQ", "Scalar_Empty_DefaultValue")]
    public void Test_CSharp_Linq_Scalar_Empty_Collection_With_DefaultValues()
    {
        var fallbackEmployee = new Employee("Fallback", 99, 99999);
        Employee[] emptyList = [];

        using var df = DataFrame.FromRows(emptyList);
        var query = df.AsQueryable(emptyList);

        // ==========================================
        // 1. First / FirstOrDefault
        // ==========================================
        // 1.1 Empty with First
        Assert.Throws<InvalidOperationException>(() => query.First());
        Assert.Throws<InvalidOperationException>(() => query.First(e => e.Age > 20));

        // 1.2 FirstOrDefault return null
        Assert.Null(query.FirstOrDefault());
        Assert.Null(query.FirstOrDefault(e => e.Age > 20));

        // 1.3 defaultValue
        Assert.Equal(fallbackEmployee, query.FirstOrDefault(fallbackEmployee));
        Assert.Equal(fallbackEmployee, query.FirstOrDefault(e => e.Age > 20, fallbackEmployee));

        // ==========================================
        // 2. Last / LastOrDefault
        // ==========================================
        // 2.1 Empty with Last
        Assert.Throws<InvalidOperationException>(() => query.Last());
        Assert.Throws<InvalidOperationException>(() => query.Last(e => e.Age > 20));

        // 2.2 LastOrDefault return null
        Assert.Null(query.LastOrDefault());
        Assert.Null(query.LastOrDefault(e => e.Age > 20));

        // 2.3 .defaultValue 
        Assert.Equal(fallbackEmployee, query.LastOrDefault(fallbackEmployee));
        Assert.Equal(fallbackEmployee, query.LastOrDefault(e => e.Age > 20, fallbackEmployee));

        // ==========================================
        // 3. Single / SingleOrDefault
        // ==========================================
        // 3.1 Empty with Single
        Assert.Throws<InvalidOperationException>(() => query.Single());
        Assert.Throws<InvalidOperationException>(() => query.Single(e => e.Age > 20));

        // 3.2 SingleOrDefault return null
        Assert.Null(query.SingleOrDefault());
        Assert.Null(query.SingleOrDefault(e => e.Age > 20));

        // 3.3 defaultValue
        Assert.Equal(fallbackEmployee, query.SingleOrDefault(fallbackEmployee));
        Assert.Equal(fallbackEmployee, query.SingleOrDefault(e => e.Age > 20, fallbackEmployee));

        var upperQuery = query.Select(e => e.Name.ToUpper());
        Assert.Equal("DEFAULT_NAME", upperQuery.FirstOrDefault("DEFAULT_NAME"));
        Assert.Equal("DEFAULT_NAME", upperQuery.LastOrDefault("DEFAULT_NAME"));
        Assert.Equal("DEFAULT_NAME", upperQuery.SingleOrDefault("DEFAULT_NAME"));
        Assert.Equal("DEFAULT_NAME", upperQuery.SingleOrDefault(name => name.StartsWith('Z'), "DEFAULT_NAME"));
    }
    [Fact]
    [Trait("LINQ", "Scalar_ElementAt_Index")]
    public void Test_CSharp_Linq_Scalar_ElementAt_And_Index()
    {
        var emps = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000),
            new Employee("Charlie", 35, 70000)
        };

        using var df = DataFrame.FromRows(emps);
        var query = df.AsQueryable(emps);

        Assert.Equal("Alice", query.ElementAt(0).Name);
        Assert.Equal("Bob", query.ElementAt(1).Name);
        Assert.Equal("Charlie", query.ElementAt(2).Name);

        Assert.Equal("Charlie", query.ElementAt(^1).Name);
        Assert.Equal("Bob", query.ElementAt(^2).Name);
        Assert.Equal("Alice", query.ElementAt(^3).Name);

        Assert.Throws<ArgumentOutOfRangeException>(() => query.ElementAt(3));
        Assert.Throws<ArgumentOutOfRangeException>(() => query.ElementAt(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => query.ElementAt(^4));

        Assert.Null(query.ElementAtOrDefault(-1));
        Assert.Null(query.ElementAtOrDefault(3));
        Assert.Null(query.ElementAtOrDefault(^4));
        Assert.Equal("Charlie", query.ElementAtOrDefault(^1)?.Name);

        var upperQuery = query.Select(e => e.Name.ToUpper());
        Assert.Equal("CHARLIE", upperQuery.ElementAt(^1));
        Assert.Equal("BOB", upperQuery.ElementAt(^2));
        Assert.Null(upperQuery.ElementAtOrDefault(^10));
    }
    [Fact]
    [Trait("LINQ", "Scalar_Contains")]
    public void Test_CSharp_Linq_Scalar_Contains_Flow()
    {
        var alice = new Employee("Alice", 25, 50000);
        var bob = new Employee("Bob", 30, 60000);
        var charlie = new Employee("Charlie", 35, 70000);
        var stranger = new Employee("Stranger", 99, 99999);

        var emps = new[] { alice, bob, charlie };

        using var df = DataFrame.FromRows(emps);
        var query = df.AsQueryable(emps);

        Assert.True(query.Contains(alice));
        Assert.True(query.Contains(bob));
        Assert.False(query.Contains(stranger));

        var upperNames = query.Select(e => e.Name.ToUpper());
        Assert.True(upperNames.Contains("ALICE"));
        Assert.True(upperNames.Contains("BOB"));
        Assert.False(upperNames.Contains("Alice")); 
        Assert.False(upperNames.Contains("DAVID"));

        Employee[] emptyEmps = [];
        using var emptyDf = DataFrame.FromRows(emptyEmps);
        var emptyQuery = emptyDf.AsQueryable(emptyEmps);
        Assert.False(emptyQuery.Contains(alice));
    }
    [Fact]
    [Trait("LINQ", "Scalar_SequenceEqual")]
    public void Test_CSharp_Linq_Scalar_SequenceEqual_Flow()
    {
        var emps1 = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000)
        };

        var emps2 = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000)
        };

        var empsMismatch = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 31, 60000)
        };

        using var df1 = DataFrame.FromRows(emps1);
        using var df2 = DataFrame.FromRows(emps2);
        using var dfMismatch = DataFrame.FromRows(empsMismatch);

        var q1 = df1.AsQueryable(emps1);
        var q2 = df2.AsQueryable(emps2);
        var qMismatch = dfMismatch.AsQueryable(empsMismatch);

        Assert.True(q1.SequenceEqual(q2));
        Assert.False(q1.SequenceEqual(qMismatch));

        Assert.True(q1.SequenceEqual(emps2));
        Assert.False(q1.SequenceEqual(empsMismatch));

        var empsExtra = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 30, 60000),
            new Employee("Charlie", 35, 70000)
        };
        Assert.False(q1.SequenceEqual(empsExtra));
    }
    [Fact]
    [Trait("LINQ", "Scalar_MinBy_MaxBy")]
    public void Test_CSharp_Linq_Scalar_MinBy_And_MaxBy_Flow()
    {
        var emps = new[]
        {
            new Employee("Alice", 25, 50000),
            new Employee("Bob", 40, 90000),
            new Employee("Charlie", 30, 75000)
        };

        using var df = DataFrame.FromRows(emps);
        var query = df.AsQueryable(emps);

        var youngest = query.MinBy(e => e.Age);
        var oldest = query.MaxBy(e => e.Age);
        Assert.Equal("Alice", youngest.Name);
        Assert.Equal("Bob", oldest.Name);

        var lowestCalculated = query.MinBy(e => e.Salary / (e.Age + 1));
        Assert.Equal("Alice", lowestCalculated.Name);

        var upperQuery = query.Select(e => e.Name.ToUpper());
        var minName = upperQuery.MinBy(name => name.Length);
        var maxName = upperQuery.MaxBy(name => name);
        Assert.Equal("BOB", minName);
        Assert.Equal("CHARLIE", maxName);

        Employee[] emptyEmps = [];
        using var emptyDf = DataFrame.FromRows(emptyEmps);
        var emptyQuery = emptyDf.AsQueryable(emptyEmps);
        Assert.Null(emptyQuery.MinBy(e => e.Age));
        Assert.Null(emptyQuery.MaxBy(e => e.Age));
    }
    [Fact]
    [Trait("LINQ", "Scalar_Numeric_Aggregations")]
    public void Test_CSharp_Linq_Scalar_Numeric_Aggregations_Flow()
    {
        var emps = new[]
        {
            new Employee("Alice", 20, 1000),
            new Employee("Bob", 30, 2000),
            new Employee("Charlie", 40, 3000)
        };

        using var df = DataFrame.FromRows(emps);
        var query = df.AsQueryable(emps);

        Assert.Equal(6000, query.Sum(e => e.Salary));
        Assert.Equal(2000.0, query.Average(e => e.Salary));
        Assert.Equal(1000, query.Min(e => e.Salary));
        Assert.Equal(3000, query.Max(e => e.Salary));

        var salaries = query.Select(e => e.Salary);
        Assert.Equal(6000, salaries.Sum());
        Assert.Equal(2000.0, salaries.Average());
        Assert.Equal(1000, salaries.Min());
        Assert.Equal(3000, salaries.Max());

        Assert.Equal(6120, query.Sum(e => e.Salary + (e.Age > 25 ? 50 : 20)));

        Employee[] emptyEmps = [];
        using var emptyDf = DataFrame.FromRows(emptyEmps);
        var emptyQuery = emptyDf.AsQueryable(emptyEmps);

        Assert.Equal(0, emptyQuery.Sum(e => e.Salary));
        Assert.Throws<InvalidOperationException>(() => emptyQuery.Average(e => e.Salary));
        Assert.Throws<InvalidOperationException>(() => emptyQuery.Min(e => e.Salary));
        Assert.Throws<InvalidOperationException>(() => emptyQuery.Max(e => e.Salary));
    }
    private static DataFrame CreateSampleOrdersDf()
    {
        return DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3, 4]),
            Series.From("CustomerName", ["  Alice Smith  ", "bob_jones", "CHARLIE", "david"]),
            Series.From("OrderDate", [
                new DateTime(2026, 1, 15, 10, 30, 0),
                new DateTime(2025, 6, 20, 14, 45, 0),
                new DateTime(2026, 8, 5, 9, 15, 0),
                new DateTime(2024, 12, 1, 18, 0, 0)
            ]),
            Series.From("Amount", [120.55, -45.0, 999.4, 25.8])
        ]);
    }

    [Fact]
    [Trait("LINQ", "StringVectorized")]
    public void Test_Linq_String_Vectorized_Ops()
    {
        using var df = CreateSampleOrdersDf();

        // Tests: Trim(), ToLower(), Contains(), StartsWith(), Length
        var query = df.AsQueryable<OrderRecord>()
                      .Where(o => o.CustomerName.Trim().ToLower().Contains("li") || o.CustomerName.StartsWith("  A"))
                      .Select(o => new {
                          CleanName = o.CustomerName.Trim().ToLower(),
                          Len = o.CustomerName.Length
                      })
                      .ToList();

        Assert.Equal(2, query.Count);
        Assert.Equal("alice smith", query[0].CleanName);
        Assert.Equal("charlie", query[1].CleanName);
    }

    [Fact]
    [Trait("LINQ", "DateTimeVectorized")]
    public void Test_Linq_DateTime_Vectorized_Ops()
    {
        using var df = CreateSampleOrdersDf();

        // Tests: .Year, .Month, .Day, ToString(chrono format), AddDays
        var query = df.AsQueryable<OrderRecord>()
                      .Where(o => o.OrderDate.Year == 2026 && o.OrderDate.Month >= 1)
                      .Select(o => new {
                          o.Id,
                          Year = o.OrderDate.Year,
                          Month = o.OrderDate.Month,
                          Day = o.OrderDate.Day,
                          Hour = o.OrderDate.Hour,
                          Formatted = o.OrderDate.ToString("yyyy-MM-dd")
                      })
                      .ToList();

        Assert.Equal(2, query.Count);
        
        Assert.Equal(1, query[0].Id);
        Assert.Equal(2026, query[0].Year);
        Assert.Equal(1, query[0].Month);
        Assert.Equal(15, query[0].Day);
        Assert.Equal(10, query[0].Hour);
        Assert.Equal("2026-01-15", query[0].Formatted);

        Assert.Equal(3, query[1].Id);
        Assert.Equal(2026, query[1].Year);
        Assert.Equal(8, query[1].Month);
        Assert.Equal(5, query[1].Day);
        Assert.Equal("2026-08-05", query[1].Formatted);
    }

    [Fact]
    [Trait("LINQ", "MathVectorized")]
    public void Test_Linq_Math_Vectorized_Ops()
    {
        using var df = CreateSampleOrdersDf();

        // Tests: Math.Abs, Math.Round, Math.Sqrt, Math.Max
        var query = df.AsQueryable<OrderRecord>()
                      .Where(o => Math.Abs(o.Amount) > 50.0)
                      .Select(o => new {
                          o.Id,
                          AbsAmount = Math.Abs(o.Amount),
                          Rounded = Math.Round(o.Amount, 1),
                          CeilVal = Math.Ceiling(o.Amount),
                          MaxVal = Math.Max(o.Amount, 100.0)
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Record 1: 120.55
        Assert.Equal(1, query[0].Id);
        Assert.Equal(120.55, query[0].AbsAmount);
        Assert.Equal(120.6, query[0].Rounded);
        Assert.Equal(121.0, query[0].CeilVal);
        Assert.Equal(120.55, query[0].MaxVal);

        // Record 3: 999.4
        Assert.Equal(3, query[1].Id);
        Assert.Equal(999.4, query[1].AbsAmount);
        Assert.Equal(999.4, query[1].Rounded);
        Assert.Equal(1000.0, query[1].CeilVal);
        Assert.Equal(999.4, query[1].MaxVal);
    }

    [Fact]
    [Trait("LINQ", "CombinedVectorizedPushdown")]
    public void Test_Linq_Combined_Complex_Pushdown()
    {
        using var df = CreateSampleOrdersDf();

        // Complex combined filter + projection without hitting fallback
        var result = df.AsQueryable<OrderRecord>()
                       .Where(o => o.OrderDate.Year >= 2025 && 
                                   Math.Abs(o.Amount) > 30.0 && 
                                   !string.IsNullOrEmpty(o.CustomerName))
                       .Select(o => new {
                           UpperName = o.CustomerName.Trim().ToUpper(),
                           SubName = o.CustomerName.Trim().Substring(0, 3),
                           o.OrderDate.Year,
                           SqrtAmount = Math.Sqrt(Math.Abs(o.Amount))
                       })
                       .ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal("ALICE SMITH", result[0].UpperName);
        Assert.Equal("Ali", result[0].SubName);
        Assert.Equal("BOB_JONES", result[1].UpperName);
        Assert.Equal("bob", result[1].SubName);
        Assert.Equal("CHARLIE", result[2].UpperName);
        Assert.Equal("CHA", result[2].SubName);
    }
    // Complex lookahead regex: Rust engine does NOT support this -> routes to MapStringPredicate UDF
    [GeneratedRegex(@"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).+$")]
    private static partial Regex StrongPasswordRegex();

    [Fact]
    [Trait("LINQ", "Regex")]
    public void Test_Linq_Regex_Native_And_UDF_Pushdown()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Email", ["alice@example.com", "bob_invalid", "charlie@polars.net"]),
            Series.From("Password", ["P@ssw0rd1", "weakpass", "Admin2026"])
        ]);
        
        // Scenario 1: Standard regex -> Validated -> Natively pushed down to Rust StrContains
        var validEmailPattern = @"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$";
        var nativeQuery = df.AsQueryable<UserInfo>()
                            .Where(u => Regex.IsMatch(u.Email, validEmailPattern))
                            .ToList();

        Assert.Equal(2, nativeQuery.Count);
        Assert.Equal("alice@example.com", nativeQuery[0].Email);
        Assert.Equal("charlie@polars.net", nativeQuery[1].Email);

        // Scenario 2: SG Regex with Lookaround -> Rejected by Rust -> Pushed down to MapStringPredicate UDF
        var udfQuery = df.AsQueryable<UserInfo>()
                         .Where(u => StrongPasswordRegex().IsMatch(u.Password))
                         .ToList();

        Assert.Equal(2, udfQuery.Count);
        Assert.Equal("P@ssw0rd1", udfQuery[0].Password);
        Assert.Equal("Admin2026", udfQuery[1].Password);

        // Scenario 3: Mixed Where conditions (Native + UDF combined)
        var combinedQuery = df.AsQueryable<UserInfo>()
                              .Where(u => Regex.IsMatch(u.Email, validEmailPattern) && StrongPasswordRegex().IsMatch(u.Password))
                              .ToList();

        Assert.Equal(2, combinedQuery.Count);
        Assert.Equal("alice@example.com", combinedQuery[0].Email);
        Assert.Equal("Admin2026", combinedQuery[1].Password);
    }
    // Source Generated Regex targeting numbers in string
    [GeneratedRegex(@"\d+")]
    private static partial Regex NumberRegex();

    // Source Generated Regex targeting bracketed tokens: [tag]
    [GeneratedRegex(@"\[([a-zA-Z]+)\]")]
    private static partial Regex TagRegex();

    [Fact]
    [Trait("LINQ", "RegexMatchEvaluator")]
    public void Test_Linq_Regex_Replace_With_MatchEvaluator_Static()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Message", ["Item 10 costs 20", "Score 5 bonus 100", "Zero 0 none"])
        ]);

        // Static Regex.Replace with MatchEvaluator: double each numeric value
        MatchEvaluator doubleNumbers = m => (int.Parse(m.Value) * 2).ToString();

        var query = df.AsQueryable<LogEntry>()
                      .Select(e => new {
                          e.Id,
                          DoubledMessage = NumberRegex().Replace(e.Message, doubleNumbers)
                      })
                      .ToList();

        Assert.Equal(3, query.Count);
        Assert.Equal("Item 20 costs 40", query[0].DoubledMessage);
        Assert.Equal("Score 10 bonus 200", query[1].DoubledMessage);
        Assert.Equal("Zero 0 none", query[2].DoubledMessage);
    }

    [Fact]
    [Trait("LINQ", "RegexMatchEvaluator")]
    public void Test_Linq_Regex_Replace_With_MatchEvaluator_Instance_And_SG()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Message", ["Status: [info] at server", "Error: [critical] failure"])
        ]);

        // Instance / SG Regex.Replace with MatchEvaluator: uppercase inside brackets
        MatchEvaluator uppercaseTag = m => $"[{m.Groups[1].Value.ToUpperInvariant()}]";

        var query = df.AsQueryable<LogEntry>()
                      .Select(e => new {
                          e.Id,
                          NormalizedMessage = TagRegex().Replace(e.Message, uppercaseTag)
                      })
                      .ToList();

        Assert.Equal(2, query.Count);
        Assert.Equal("Status: [INFO] at server", query[0].NormalizedMessage);
        Assert.Equal("Error: [CRITICAL] failure", query[1].NormalizedMessage);
    }
    // SG Regex with capture groups for extracting severity and numeric code: e.g. "[ERROR:404]"
    [GeneratedRegex(@"\[(INFO|WARN|ERROR):(\d+)\]")]
    private static partial Regex LogHeaderRegex();

    // Lookahead SG Regex to extract thread numbers preceding "ms" latency (Rust engine cannot do lookahead)
    [GeneratedRegex(@"\d+(?=\s*ms)")]
    private static partial Regex LatencyLookaheadRegex();

    [Fact]
    [Trait("LINQ", "RegexMatch")]
    public void Test_Linq_Regex_Match_Value_And_Groups()
    {
        // Reusing existing record struct LogEntry(int Id, string Message)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Message", [
                "[INFO:200] Request processed in 45 ms",
                "[ERROR:500] Database timeout after 1200 ms",
                "Malformed message without brackets"
            ])
        ]);

        var query = df.AsQueryable<LogEntry>()
                      .Select(e => new {
                          e.Id,
                          // 1. Static Regex.Match(s, pat).Value -> Rust Native StrExtract (Group 0)
                          FullTag = Regex.Match(e.Message,@"\[(INFO|WARN|ERROR):(\d+)\]").Value,
                          // 2. SG Regex Match with Groups[1] -> Rust Native StrExtract (Group 1: Severity)
                          Severity = LogHeaderRegex().Match(e.Message).Groups[1].Value,
                          // 3. SG Regex Match with Groups[2] -> Rust Native StrExtract (Group 2: Code)
                          StatusCode = LogHeaderRegex().Match(e.Message).Groups[2].Value,
                          // 4. Lookahead SG Regex -> Fallback to MapStringTransform UDF in Arrow Chunk
                          Latency = LatencyLookaheadRegex().Match(e.Message).Value
                      })
                      .ToList();

        Assert.Equal(3, query.Count);

        // Record 1: Normal processing
        Assert.Equal(1, query[0].Id);
        Assert.Equal("[INFO:200]", query[0].FullTag);
        Assert.Equal("INFO", query[0].Severity);
        Assert.Equal("200", query[0].StatusCode);
        Assert.Equal("45", query[0].Latency);

        // Record 2: Error log
        Assert.Equal(2, query[1].Id);
        Assert.Equal("[ERROR:500]", query[1].FullTag);
        Assert.Equal("ERROR", query[1].Severity);
        Assert.Equal("500", query[1].StatusCode);
        Assert.Equal("1200", query[1].Latency);

        // Record 3: No match
        Assert.Equal(3, query[2].Id);
        Assert.Equal(string.Empty, query[2].FullTag);
        Assert.Equal(string.Empty, query[2].Severity);
        Assert.Equal(string.Empty, query[2].StatusCode);
        Assert.Equal(string.Empty,query[2].Latency);
    }

    // Lookahead SG Regex targeting numbers that precede "ms"
    [GeneratedRegex(@"\d+(?=\s*ms)")]
    private static partial Regex LookaheadMsRegex();

    [Fact]
    [Trait("LINQ", "RegexMatches")]
    public void Test_Linq_Regex_Matches_Count_And_Extraction_Using_LogEntry()
    {
        // Reusing existing record struct LogEntry(int Id, string Message)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Message", [
                "CPU 20%, Memory 80%, Disk 45%",
                "Step took 10 ms then 200 ms",
                "No numbers here"
            ])
        ]);

        var query = df.AsQueryable<LogEntry>()
                      .Select(e => new {
                          e.Id,
                          // 1. Static Regex.Matches(s, pat).Count -> Rust StrExtractAll + ListLengths
                          DigitCount = Regex.Matches(e.Message,@"\d+").Count,
                          // 2. SG Regex rx.Matches(s).Count -> Native
                          NumberGroupCount = NumberRegex().Count(e.Message),
                          // 3. Lookahead SG Regex Matches(s).Count -> UDF Fallback (Int32 Array)
                          LookaheadCount = LookaheadMsRegex().Count(e.Message)
                      })
                      .ToList();

        Assert.Equal(3, query.Count);

        // Record 1: "20", "80", "45" -> 3 numbers, 0 "ms"
        Assert.Equal(1, query[0].Id);
        Assert.Equal(3, query[0].DigitCount);
        Assert.Equal(3, query[0].NumberGroupCount);
        Assert.Equal(0, query[0].LookaheadCount);

        // Record 2: "10 ms", "200 ms" -> 2 numbers, 2 "ms"
        Assert.Equal(2, query[1].Id);
        Assert.Equal(2, query[1].DigitCount);
        Assert.Equal(2, query[1].NumberGroupCount);
        Assert.Equal(2, query[1].LookaheadCount);

        // Record 3: No matches
        Assert.Equal(3, query[2].Id);
        Assert.Equal(0, query[2].DigitCount);
        Assert.Equal(0, query[2].NumberGroupCount);
        Assert.Equal(0, query[2].LookaheadCount);
    }
    // SG Regex targeting individual word tokens: e.g. "CPU", "Memory", "Disk"
    [GeneratedRegex(@"\b[A-Za-z]+\b")]
    private static partial Regex WordTokensRegex();

    // Lookahead SG Regex targeting numeric IDs preceding "ms" (Forces UDF Fallback -> MapStringToList)
    [GeneratedRegex(@"\d+(?=\s*ms)")]
    private static partial Regex LookaheadMsRegexExtractor();

    [Fact]
    [Trait("LINQ", "RegexMatchesToList")]
    public void Test_Linq_Regex_Matches_Select_Value_ToList_Using_LogEntry()
    {
        // Reusing existing record struct LogEntry(int Id, string Message)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Message", [
                "Server alpha port 8080 latency 12 ms",
                "Backup beta delay 250 ms and 300 ms",
                "Idle"
            ])
        ]);

        var query = df.AsQueryable<LogEntry>()
                      .Select(e => new {
                          e.Id,
                          // 1. Static Regex.Matches + Select(m => m.Value).ToList() -> Native StrExtractAll
                          StaticTokens = Regex.Matches(e.Message,@"\b[A-Za-z]+\b")
                                              .Select(m => m.Value)
                                              .ToList(),

                          // 2. SG Regex rx.Matches + Select(m => m.Value).ToList() -> Native StrExtractAll
                          SgTokens = WordTokensRegex().Matches(e.Message)
                                                      .Select(m => m.Value)
                                                      .ToList(),

                          // 3. Lookahead SG Regex + Select(m => m.Value).ToList() -> Arrow LargeList UDF Fallback
                          LookaheadLatencies = LookaheadMsRegexExtractor().Matches(e.Message)
                                                                         .Select(m => m.Value)
                                                                         .ToList()
                      })
                      .ToList();

        Assert.Equal(3, query.Count);

        // Record 1: "Server alpha port 8080 latency 12 ms"
        Assert.Equal(1, query[0].Id);
        Assert.Equal(["Server", "alpha", "port", "latency", "ms"], query[0].StaticTokens);
        Assert.Equal(["Server", "alpha", "port", "latency", "ms"], query[0].SgTokens);
        Assert.Equal(["12"], query[0].LookaheadLatencies);

        // Record 2: "Backup beta delay 250 ms and 300 ms"
        Assert.Equal(2, query[1].Id);
        Assert.Equal(["Backup", "beta", "delay", "ms", "and", "ms"], query[1].StaticTokens);
        Assert.Equal(["Backup", "beta", "delay", "ms", "and", "ms"], query[1].SgTokens);
        Assert.Equal(["250", "300"], query[1].LookaheadLatencies);

        // Record 3: "Idle"
        Assert.Equal(3, query[2].Id);
        Assert.Equal(["Idle"], query[2].StaticTokens);
        Assert.Equal(["Idle"], query[2].SgTokens);
        Assert.Empty(query[2].LookaheadLatencies);
    }
    // SG Regex: delimiter is one or more whitespace, comma, or semicolon
    [GeneratedRegex(@"[\s,;]+")]
    private static partial Regex DelimiterRegex();

    // Lookahead SG Regex: split before uppercase letters (Rust engine rejects lookahead -> triggers UDF)
    [GeneratedRegex(@"(?=[A-Z])")]
    private static partial Regex CamelCaseSplitRegex();

    [Fact]
    [Trait("LINQ", "RegexSplit")]
    public void Test_Linq_Regex_Split_Native_And_UDF_Using_LogEntry()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Message", [
                "apple,banana;orange grape",
                "SplitCamelCaseText"
            ])
        ]);

        var query = df.AsQueryable<LogEntry>()
                      .Select(e => new {
                          e.Id,
                          // 1. Static Regex.Split -> Rust Native StrSplit (literal: false)
                          StaticWords = Regex.Split(e.Message, @"[\s,;]+"),
                          // 2. SG Regex rx.Split -> Rust Native StrSplit (literal: false)
                          SgWords = DelimiterRegex().Split(e.Message),
                          // 3. Lookahead SG Regex -> Arrow LargeList UDF Fallback
                          Tokens = CamelCaseSplitRegex().Split(e.Message)
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Record 1: Delimiter split
        Assert.Equal(1, query[0].Id);
        Assert.Equal(["apple", "banana", "orange", "grape"], query[0].StaticWords);
        Assert.Equal(["apple", "banana", "orange", "grape"], query[0].SgWords);

        // Record 2: CamelCase split via UDF Fallback
        Assert.Equal(2, query[1].Id);
        // Note: Regex.Split("SplitCamelCaseText", @"(?=[A-Z])") may yield an empty leading string before the first capital letter
        var cleanedTokens = query[1].Tokens.Where(s => !string.IsNullOrEmpty(s)).ToList();
        Assert.Equal(["Split", "Camel", "Case", "Text"], cleanedTokens);
    }
    [Fact]
    [Trait("LINQ", "ListOperators")]
    public void Test_Linq_List_Operators_Pushdown_Using_PersonWithTags()
    {
        // PersonWithTags(int Id, string Name, string[] Tags)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Tags", [
                new[] { "admin", "dev", "ops" },
                ["guest"],
                ["dev", "tester"]
            ])
        ]);

        // 1. Test Where with List.Contains
        var adminUsers = df.AsQueryable<PersonWithTags>()
                           .Where(p => p.Tags.Contains("admin"))
                           .ToList();

        Assert.Single(adminUsers);
        Assert.Equal("Alice", adminUsers[0].Name);

        // 2. Test Select with Length, First, Last, Indexer
        var projected = df.AsQueryable<PersonWithTags>()
                          .Select(p => new {
                              p.Id,
                              TagCount = p.Tags.Length,
                              FirstTag = p.Tags.First(),
                              LastTag = p.Tags.Last(),
                              SecondTag = p.Tags[1]
                          })
                          .ToList();

        Assert.Equal(3, projected.Count);

        // Alice: ["admin", "dev", "ops"]
        Assert.Equal(3, projected[0].TagCount);
        Assert.Equal("admin", projected[0].FirstTag);
        Assert.Equal("ops", projected[0].LastTag);
        Assert.Equal("dev", projected[0].SecondTag);

        // Bob: ["guest"] (Length 1, index 1 is out of bounds -> returns null/default)
        Assert.Equal(1, projected[1].TagCount);
        Assert.Equal("guest", projected[1].FirstTag);
        Assert.Equal("guest", projected[1].LastTag);
        Assert.Null(projected[1].SecondTag);

        // Charlie: ["dev", "tester"]
        Assert.Equal(2, projected[2].TagCount);
        Assert.Equal("dev", projected[2].FirstTag);
        Assert.Equal("tester", projected[2].LastTag);
        Assert.Equal("tester", projected[2].SecondTag);
    }
    [Fact]
    [Trait("LINQ", "ListJoin")]
    public void Test_Linq_List_Join_Pushdown_Using_PersonWithTags()
    {
        // Reusing existing record struct PersonWithTags(int Id, string Name, string[] Tags)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Tags", [
                ["admin", "dev", "ops"],
                ["guest"],
                new[] { "dev", "tester" }
            ])
        ]);

        var query = df.AsQueryable<PersonWithTags>()
                      .Select(p => new {
                          p.Id,
                          JoinedTags = string.Join(";", p.Tags),
                          CommaTags = string.Join(", ", p.Tags)
                      })
                      .ToList();

        Assert.Equal(3, query.Count);

        // Record 1
        Assert.Equal("admin;dev;ops", query[0].JoinedTags);
        Assert.Equal("admin, dev, ops", query[0].CommaTags);

        // Record 2
        Assert.Equal("guest", query[1].JoinedTags);
        Assert.Equal("guest", query[1].CommaTags);

        // Record 3
        Assert.Equal("dev;tester", query[2].JoinedTags);
        Assert.Equal("dev, tester", query[2].CommaTags);
    }
    [Fact]
    [Trait("LINQ", "ListTransformations")]
    public void Test_Linq_List_Transformations_Pushdown_Using_PersonWithTags()
    {
        // Reusing existing record struct PersonWithTags(int Id, string Name, string[] Tags)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Tags", [
                ["dev", "ops", "dev", "qa"],
                new[] { "a", "b", "c" }
            ])
        ]);

        var query = df.AsQueryable<PersonWithTags>()
                      .Select(p => new {
                          p.Id,
                          // 1. Take(2) -> ListHead
                          TopTags = p.Tags.Take(2),
                          // 2. Skip(1) -> ListSlice
                          SkippedTags = p.Tags.Skip(1),
                          // 3. Distinct() -> ListUnique
                          UniqueTags = p.Tags.Distinct(),
                          // 4. Reverse() -> ListReverse
                          ReversedTags = p.Tags.Reverse()
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Record 1: ["dev", "ops", "dev", "qa"]
        Assert.Equal(["dev", "ops"], query[0].TopTags);
        Assert.Equal(["ops", "dev", "qa"], query[0].SkippedTags);
        Assert.Equal(["dev", "ops", "qa"], query[0].UniqueTags);
        Assert.Equal(["qa", "dev", "ops", "dev"], query[0].ReversedTags);

        // Record 2: ["a", "b", "c"]
        Assert.Equal(["a", "b"], query[1].TopTags);
        Assert.Equal(["b", "c"], query[1].SkippedTags);
        Assert.Equal(["a", "b", "c"], query[1].UniqueTags);
        Assert.Equal(["c", "b", "a"], query[1].ReversedTags);
    }
    [Fact]
    [Trait("LINQ", "ListEvalTransformations")]
    public void Test_Linq_List_Distinct_And_Reverse_Using_PersonWithTags()
    {
        // Reusing existing record struct PersonWithTags(int Id, string Name, string[] Tags)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Tags", [
                new[] { "dev", "ops", "dev", "qa" },
                new[] { "a", "b", "c" }
            ])
        ]);

        var query = df.AsQueryable<PersonWithTags>()
                      .Select(p => new {
                          p.Id,
                          // Distinct() -> list().eval(col("").unique())
                          UniqueTags = p.Tags.Distinct(),
                          // Reverse() -> list().eval(col("").reverse())
                          ReversedTags = p.Tags.Reverse()
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Record 1: ["dev", "ops", "dev", "qa"]
        Assert.Equal(1, query[0].Id);
        // Note: Polars unique maintains original encounter order or set uniqueness
        Assert.Equal(3, query[0].UniqueTags.Count());
        Assert.Contains("dev", query[0].UniqueTags);
        Assert.Contains("ops", query[0].UniqueTags);
        Assert.Contains("qa", query[0].UniqueTags);
        Assert.Equal(["qa", "dev", "ops", "dev"], query[0].ReversedTags);

        // Record 2: ["a", "b", "c"]
        Assert.Equal(2, query[1].Id);
        Assert.Equal(["a", "b", "c"], query[1].UniqueTags);
        Assert.Equal(["c", "b", "a"], query[1].ReversedTags);
    }
    [Fact]
    [Trait("LINQ", "ListTakeLast")]
    public void Test_Linq_List_TakeLast_Pushdown_Using_PersonWithTags()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Tags", [
                new[] { "dev", "ops", "qa", "sec" },
                new[] { "a", "b" }
            ])
        ]);

        var query = df.AsQueryable<PersonWithTags>()
                      .Select(p => new {
                          p.Id,
                          // TakeLast(2) -> list().tail(2)
                          LastTwoTags = p.Tags.TakeLast(2)
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Record 1: ["dev", "ops", "qa", "sec"] -> Last 2: ["qa", "sec"]
        Assert.Equal(["qa", "sec"], query[0].LastTwoTags);

        // Record 2: ["a", "b"] -> Last 2: ["a", "b"]
        Assert.Equal(["a", "b"], query[1].LastTwoTags);
    }
    [Fact]
    [Trait("LINQ", "ListSetAndSort")]
    public void Test_Linq_List_Set_Operations_And_Sort()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Skills", [
                new[] { "rust", "csharp", "fsharp" },
                new[] { "python", "sql" }
            ]),
            Series.From("DesiredSkills", [
                new[] { "csharp", "go" },
                new[] { "sql", "rust" }
            ])
        ]);

        var query = df.AsQueryable<DeveloperProfile>()
                      .Select(d => new {
                          d.Id,
                          // 1. Sort ascending
                          SortedSkills = d.Skills.OrderBy(s => s),
                          // 2. Set Intersection: Skills ∩ DesiredSkills
                          MatchedSkills = d.Skills.Intersect(d.DesiredSkills),
                          // 3. Set Difference: Skills - DesiredSkills
                          UnneededSkills = d.Skills.Except(d.DesiredSkills)
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Record 1
        Assert.Equal(["csharp", "fsharp", "rust"], query[0].SortedSkills);
        Assert.Equal(["csharp"], query[0].MatchedSkills);
        Assert.Equal(["rust", "fsharp"], query[0].UnneededSkills);

        // Record 2
        Assert.Equal(["python", "sql"], query[1].SortedSkills);
        Assert.Equal(["sql"], query[1].MatchedSkills);
        Assert.Equal(["python"], query[1].UnneededSkills);
    }
    [Fact]
    [Trait("LINQ", "ListPredicates")]
    public void Test_Linq_List_Any_And_All_Pushdown()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Name", ["Alice", "Bob", "Charlie"]),
            Series.From("Scores", [
                new[] { 95, 88, 92 },
                [50, 59, 45],
                []
            ])
        ]);

        var query = df.AsQueryable<StudentScores>()
                      .Select(s => new {
                          s.Id,
                          // Has any score recorded
                          HasScores = s.Scores.Any(),
                          // Has any score >= 90
                          HasHighScore = s.Scores.Any(x => x >= 90),
                          // Passed all exams (all >= 60)
                          PassedAll = s.Scores.All(x => x >= 60)
                      })
                      .ToList();

        Assert.Equal(3, query.Count);

        // Alice: [95, 88, 92]
        Assert.True(query[0].HasScores);
        Assert.True(query[0].HasHighScore);
        Assert.True(query[0].PassedAll);

        // Bob: [50, 59, 45]
        Assert.True(query[1].HasScores);
        Assert.False(query[1].HasHighScore);
        Assert.False(query[1].PassedAll);

        // Charlie: []
        Assert.False(query[2].HasScores);
        Assert.False(query[2].HasHighScore);
        Assert.True(query[2].PassedAll); // Vacation truth: All on empty set is true in BCL
    }
    [Fact]
    [Trait("LINQ", "ListAnyAll")]
    public void Test_Linq_List_Any_And_All_Eval_Pushdown()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Scores", [
                new[] { 95, 88, 92 },
                new[] { 50, 59, 45 }
            ])
        ]);

        var query = df.AsQueryable<StudentScores>()
                      .Select(s => new {
                          s.Id,
                          // Any() without predicate
                          HasScores = s.Scores.Any(),
                          // Any(x => x >= 90)
                          HasHonor = s.Scores.Any(x => x >= 90),
                          // All(x => x >= 60)
                          PassedAll = s.Scores.All(x => x >= 60)
                      })
                      .ToList();

        Assert.Equal(2, query.Count);

        // Alice: [95, 88, 92]
        Assert.True(query[0].HasScores);
        Assert.True(query[0].HasHonor);
        Assert.True(query[0].PassedAll);

        // Bob: [50, 59, 45]
        Assert.True(query[1].HasScores);
        Assert.False(query[1].HasHonor);
        Assert.False(query[1].PassedAll);
    }
    [Fact]
    [Trait("LINQ", "ListSelect")]
    public void Test_Linq_List_Inline_Select_Projection_Using_PersonWithTags()
    {
        // Reusing existing record struct PersonWithTags(int Id, string Name, string[] Tags)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2]),
            Series.From("Name", ["Alice", "Bob"]),
            Series.From("Tags", [
                new[] { "admin", "dev" },
                new[] { "guest" }
            ])
        ]);

        var query = df.AsQueryable<PersonWithTags>()
                      .Select(p => new {
                          p.Id,
                          // tags.Select(t => t.ToUpper()) -> list().eval(col("").str().to_uppercase())
                          UpperTags = p.Tags.Select(t => t.ToUpper()).ToArray()
                      })
                      .ToList();

        Assert.Equal(2, query.Count);
        Assert.Equal(["ADMIN", "DEV"], query[0].UpperTags);
        Assert.Equal(["GUEST"], query[1].UpperTags);
    }
    [Fact]
    [Trait("LINQ", "StructFieldPushdown")]
    public void Test_Linq_Nested_Struct_Field_Pushdown_Using_CustomerProfile()
    {
        // 1. Arrange: Create a DataFrame with a nested Struct column "Address"
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("CustomerName", ["Alice", "Bob", "Charlie"]),
            Series.From("Address", [
                new AddressInfo("Tokyo", "100-0001"),
                new AddressInfo("Yokohama", "220-0012"),
                new AddressInfo("Osaka", "530-0001")
            ])
        ]);

        // 2. Act & Assert: Where filter pushdown on nested struct field: c.Address.City == "Yokohama"
        var yokohamaCustomers = df.AsQueryable<CustomerProfile>()
                                  .Where(c => c.Address.City == "Yokohama")
                                  .ToList();

        Assert.Single(yokohamaCustomers);
        Assert.Equal("Bob", yokohamaCustomers[0].CustomerName);
        Assert.Equal("Yokohama", yokohamaCustomers[0].Address.City);
        Assert.Equal("220-0012", yokohamaCustomers[0].Address.ZipCode);

        // 3. Act & Assert: Select projection pushdown extracting nested struct fields
        var projected = df.AsQueryable<CustomerProfile>()
                          .Select(c => new {
                              c.Id,
                              CityName = c.Address.City,
                              Postal = c.Address.ZipCode
                          })
                          .OrderBy(x => x.Id)
                          .ToList();

        Assert.Equal(3, projected.Count);
        
        Assert.Equal(1, projected[0].Id);
        Assert.Equal("Tokyo", projected[0].CityName);
        Assert.Equal("100-0001", projected[0].Postal);

        Assert.Equal(2, projected[1].Id);
        Assert.Equal("Yokohama", projected[1].CityName);
        Assert.Equal("220-0012", projected[1].Postal);

        Assert.Equal(3, projected[2].Id);
        Assert.Equal("Osaka", projected[2].CityName);
        Assert.Equal("530-0001", projected[2].Postal);
    }
    [Fact]
    [Trait("LINQ", "NullCoalescing")]
    public void Test_Linq_Null_Coalescing_Pushdown()
    {
        // Construct DataFrame with nullable strings
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3]),
            Series.From("Nickname", ["Neo", null, "Trinity"]),
            Series.From("FallbackName", ["Thomas", "Guest", "Agent"])
        ]);

        // Act: Test ?? operator inside Select projection
        var query = df.AsQueryable<UserProfile>()
                      .Select(u => new {
                          u.Id,
                          DisplayName = u.Nickname ?? u.FallbackName
                      })
                      .OrderBy(u => u.Id)
                      .ToList();

        Assert.Equal(3, query.Count);

        // Record 1: Nickname is not null -> "Neo"
        Assert.Equal(1, query[0].Id);
        Assert.Equal("Neo", query[0].DisplayName);

        // Record 2: Nickname is null -> "Guest"
        Assert.Equal(2, query[1].Id);
        Assert.Equal("Guest", query[1].DisplayName);

        // Record 3: Nickname is not null -> "Trinity"
        Assert.Equal(3, query[2].Id);
        Assert.Equal("Trinity", query[2].DisplayName);
    }
    [Fact]
    [Trait("LINQ", "DecimalMethods")]
    public void Test_Linq_Decimal_Methods_Pushdown()
    {
        using var df = DataFrame.FromColumns([
            Series.From("ItemName", ["Widget A", "Widget B", "Widget C", "Widget D"]),
            Series.From("Price", [12.3456m, -15.75m, 100.00m, 5.49m]),
            Series.From("DiscountRate", [0.15m, 0.20m, 0.05m, 0.50m])
        ]);

        var filtered = df.AsQueryable<OrderItem>()
                         .Where(o => decimal.Abs(o.Price) > 10.0m && decimal.Floor(o.Price) >= 12.0m)
                         .ToList();

        Assert.Equal(2, filtered.Count);
        Assert.Equal("Widget A", filtered[0].ItemName);
        Assert.Equal("Widget C", filtered[1].ItemName);

        var projected = df.AsQueryable<OrderItem>()
                          .Select(o => new {
                              o.ItemName,
                              RoundedPrice = decimal.Round(o.Price, 2),
                              CeilPrice = decimal.Ceiling(o.Price),
                              FloorPrice = decimal.Floor(o.Price),
                              AbsPrice = decimal.Abs(o.Price),
                              ClampedPrice = decimal.Clamp(decimal.Abs(o.Price), 10.0m, 50.0m),
                              MinPrice = decimal.Min(decimal.Abs(o.Price), 20.0m)
                          })
                          .ToList();

        Assert.Equal(4, projected.Count);

        // Widget A (Price: 12.3456m)
        Assert.Equal(12.35m, projected[0].RoundedPrice);
        Assert.Equal(13.0m, projected[0].CeilPrice);
        Assert.Equal(12.0m, projected[0].FloorPrice);
        Assert.Equal(12.3456m, projected[0].AbsPrice);
        Assert.Equal(12.3456m, projected[0].ClampedPrice);
        Assert.Equal(12.3456m, projected[0].MinPrice);

        // Widget B (Price: -15.75m)
        Assert.Equal(-15.75m, projected[1].RoundedPrice);
        Assert.Equal(-15.0m, projected[1].CeilPrice);
        Assert.Equal(-16.0m, projected[1].FloorPrice);
        Assert.Equal(15.75m, projected[1].AbsPrice);
        Assert.Equal(15.75m, projected[1].ClampedPrice);
        Assert.Equal(15.75m, projected[1].MinPrice);

        // Widget C (Price: 100.00m)
        Assert.Equal(100.00m, projected[2].RoundedPrice);
        Assert.Equal(50.0m, projected[2].ClampedPrice);
        Assert.Equal(20.0m, projected[2].MinPrice);
    }
    [Fact]
    [Trait("LINQ", "DecimalMethods")]
    public void Test_Linq_Decimal_Methods_And_Conversions_Pushdown()
    {
        // decimal DataFrame
        using var df = DataFrame.FromColumns([
            Series.From("ItemName", ["Item A", "Item B", "Item C", "Item D"]),
            Series.From("Price", [12.3456m, -15.75m, 100.50m, 5.50m]),
            Series.From("DiscountRate", [0.15m, 0.20m, 0.05m, 0.50m])
        ]);

        // 1. Where (decimal.Abs, decimal.Floor, decimal.Truncate)
        var filtered = df.AsQueryable<OrderItem>()
                         .Where(o => decimal.Abs(o.Price) >= 10.0m && decimal.Floor(o.Price) > 5.0m)
                         .ToList();

        Assert.Equal(2, filtered.Count);
        Assert.Equal("Item A", filtered[0].ItemName);
        Assert.Equal("Item C", filtered[1].ItemName);

        // 2. Select (MidpointRounding, Clamp, Min, Max, Truncate)
        var projected = df.AsQueryable<OrderItem>()
                          .Select(o => new {
                              o.ItemName,
                              // HalfToEven
                              RoundDefault = decimal.Round(o.Price, 2),
                              // HalfAwayFromZero: 5.50m to 6.0m
                              RoundAwayFromZero = decimal.Round(o.Price, 0, MidpointRounding.AwayFromZero),
                              // ToEven: 5.50m to 6.0m，100.50m to 100.0m
                              RoundToEven = decimal.Round(o.Price, 0, MidpointRounding.ToEven),
                              // ToZero
                              RoundToZero = decimal.Round(o.Price, 1, MidpointRounding.ToZero),
                              // Basic Operators
                              Floor = decimal.Floor(o.Price),
                              Ceil = decimal.Ceiling(o.Price),
                              Trunc = decimal.Truncate(o.Price),
                              Abs = decimal.Abs(o.Price),
                              // Clamp and Binary
                              Clamped = decimal.Clamp(decimal.Abs(o.Price), 10.0m, 50.0m),
                              MinVal = decimal.Min(decimal.Abs(o.Price), 20.0m),
                              MaxVal = decimal.Max(decimal.Abs(o.Price), 20.0m),
                              // Cast:ToInt32, ToDouble
                              PriceAsInt = decimal.ToInt32(decimal.Floor(decimal.Abs(o.Price))),
                              PriceAsDouble = decimal.ToDouble(o.Price),
                              // ToString with format string
                              StrPrice = o.Price.ToString(),
                              StrPriceF2 = o.Price.ToString("F2")
                          })
                          .ToList();

        Assert.Equal(4, projected.Count);

        // Item A: Price = 12.3456m
        var a = projected[0];
        Assert.Equal(12.35m, a.RoundDefault);
        Assert.Equal(12.0m, a.Floor);
        Assert.Equal(13.0m, a.Ceil);
        Assert.Equal(12.0m, a.Trunc);
        Assert.Equal(12.3456m, a.Abs);
        Assert.Equal(12.3456m, a.Clamped);
        Assert.Equal(12.3456m, a.MinVal);
        Assert.Equal(20.0m, a.MaxVal);
        Assert.Equal(12, a.PriceAsInt);
        Assert.Equal(12.3456, a.PriceAsDouble, precision: 4);
        Assert.Equal("12.35", a.StrPriceF2);

        // Item B: Price = -15.75m 
        var b = projected[1];
        Assert.Equal(-15.75m, b.RoundDefault);
        Assert.Equal(-16.0m, b.Floor);
        Assert.Equal(-15.0m, b.Ceil);
        Assert.Equal(-15.0m, b.Trunc);
        Assert.Equal(15.75m, b.Abs);
        Assert.Equal(-15.7m, b.RoundToZero); 
        Assert.Equal(15, b.PriceAsInt);

        // Item C: Price = 100.50m (ToEven / HalfToEven)
        var c = projected[2];
        Assert.Equal(101.0m, c.RoundAwayFromZero); // AwayFromZero -> 101
        Assert.Equal(100.0m, c.RoundToEven);       // ToEven -> even 100
        Assert.Equal(50.0m, c.Clamped);            // Clamp 
        Assert.Equal(20.0m, c.MinVal);             // Min 
        Assert.Equal(100.50m, c.MaxVal);           // Max 
        Assert.Equal("100.50", c.StrPriceF2);

        // Item D: Price = 5.50m
        var d = projected[3];
        Assert.Equal(6.0m, d.RoundAwayFromZero);
        Assert.Equal(6.0m, d.RoundToEven);
        Assert.Equal(10.0m, d.Clamped);            
    }
    public class StockPrice
    {
        public string Symbol { get; set; } = string.Empty;
        public DateTime TradeDate { get; set; }
        public double? Price { get; set; }
    }
    [Fact]
    [Trait("LINQ", "WindowFunctions")]
    public void Test_Linq_Shift_Diff_Over_Pushdown()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Symbol", ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT"]),
            Series.From("TradeDate", [
                new DateTime(2026, 1, 1),
                new DateTime(2026, 1, 2),
                new DateTime(2026, 1, 3),
                new DateTime(2026, 1, 1),
                new DateTime(2026, 1, 2)
            ]),
            Series.From("Price", [150.0, 155.0, 160.0, 300.0, 310.0])
        ]);

        var query = df.AsQueryable<StockPrice>()
                    .Select(s => new {
                        s.Symbol,
                        s.TradeDate,
                        s.Price,
                        PrevPrice = s.Price.Shift(1),
                        PriceDiff = s.Price.Diff(1),
                        GroupedPrevPrice = s.Price.Shift(1).Over(s.Symbol)
                    })
                    .ToList();

        Assert.Equal(5, query.Count);
        
        Assert.Equal(150.0, query[0].Price);
        Assert.Null(query[0].PrevPrice);
        Assert.Null(query[0].PriceDiff);
        Assert.Null(query[0].GroupedPrevPrice);

        Assert.Equal(155.0, query[1].Price);
        Assert.Equal(150.0, query[1].PrevPrice);
        Assert.Equal(5.0, query[1].PriceDiff);
        Assert.Equal(150.0, query[1].GroupedPrevPrice);

        Assert.Equal(160.0, query[2].Price);
        Assert.Equal(155.0, query[2].PrevPrice);
        Assert.Equal(5.0, query[2].PriceDiff);
        Assert.Equal(155.0, query[2].GroupedPrevPrice);

        Assert.Equal(300.0, query[3].Price);

        Assert.Equal(160.0, query[3].PrevPrice);
        Assert.Equal(140.0, query[3].PriceDiff); // 300.0 - 160.0

        Assert.Null(query[3].GroupedPrevPrice);

        Assert.Equal(310.0, query[4].Price);
        Assert.Equal(300.0, query[4].PrevPrice);
        Assert.Equal(10.0, query[4].PriceDiff);
        Assert.Equal(300.0, query[4].GroupedPrevPrice);

    }
    [Fact]
    [Trait("LINQ", "WindowFunctions")]
    public void Test_Linq_Rank_And_Over_Pushdown_Native_Style()
    {
        // MemberRecord: (int Id, string Name, int DeptId, int Age, int Salary)
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eva"]),
            Series.From("DeptId", [1, 1, 1, 2, 2]),
            Series.From("Age", [25, 30, 35, 28, 40]),
            Series.From("Salary", [5000, 8000, 8000, 4000, 6000])
        ]);

        var results = df.AsQueryable<MemberRecord>()
            .Select(e => new
            {
                e.Name,
                e.DeptId,
                e.Salary,

                // SQL: RANK() OVER (PARTITION BY DeptId ORDER BY Salary DESC)
                // Dept 1: Bob(8000)=1, Charlie(8000)=1, Alice(5000)=3
                DeptRank = e.Salary.Rank(RankMethod.Min, descending: true).Over(e.DeptId),

                // SQL: SUM(Salary) OVER (PARTITION BY DeptId)
                // Dept 1: 5000 + 8000 + 8000 = 21000
                // Dept 2: 4000 + 6000 = 10000
                DeptTotalSalary = e.Salary.Sum().Over(e.DeptId),

                DeptSalaryPctChange = e.Salary.PctChange(1).Over(e.DeptId)
            })
            .ToList();

        Assert.Equal(5, results.Count);

        var dept1 = results.Where(r => r.DeptId == 1).ToList();
        Assert.Equal(3, dept1.Count);

        Assert.All(dept1, r => Assert.Equal(21000, r.DeptTotalSalary));

        // Alice (Salary 5000)
        var alice = dept1.Single(r => r.Name == "Alice");
        Assert.Equal(3.0, alice.DeptRank);

        // Bob (Salary 8000) 
        var bob = dept1.Single(r => r.Name == "Bob");
        Assert.Equal(1.0, bob.DeptRank);

        // Charlie (Salary 8000) 
        var charlie = dept1.Single(r => r.Name == "Charlie");
        Assert.Equal(1.0, charlie.DeptRank);

        var dept2 = results.Where(r => r.DeptId == 2).ToList();
        Assert.Equal(2, dept2.Count);

        // Dept 2 4000 + 6000 = 10000
        Assert.All(dept2, r => Assert.Equal(10000, r.DeptTotalSalary));

        // Eva (Salary 6000) 
        var eva = dept2.Single(r => r.Name == "Eva");
        Assert.Equal(1.0, eva.DeptRank);

        // David (Salary 4000) 
        var david = dept2.Single(r => r.Name == "David");
        Assert.Equal(2.0, david.DeptRank);
        Assert.Null(david.DeptSalaryPctChange); 
    }
    [Fact]
    [Trait("LINQ", "WindowFunctions")]
    public void Test_Linq_Multi_Column_Over_Anonymous_And_Params_Pushdown()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eva"]),
            Series.From("DeptId", [1, 1, 1, 2, 2]),
            Series.From("Age", [25, 30, 30, 28, 40]),
            Series.From("Salary", [5000, 8000, 8000, 4000, 6000])
        ]);

        var results = df.AsQueryable<MemberRecord>()
            .Select(e => new
            {
                e.Name,
                e.DeptId,
                e.Age,
                e.Salary,

                // Over(new { e.DeptId, e.Age })
                AnonTotalSalary = e.Salary.Sum().Over(new { e.DeptId, e.Age }),
                AnonRank = e.Salary.Rank(RankMethod.Min, descending: true).Over(new { e.DeptId, e.Age }),

                // Over(e.DeptId, e.Age)
                ParamsTotalSalary = e.Salary.Sum().Over(e.DeptId, e.Age),
                ParamsRank = e.Salary.Rank(RankMethod.Min, descending: true).Over(e.DeptId, e.Age)
            })
            .ToList();

        Assert.Equal(5, results.Count);

        Assert.All(results, r =>
        {
            Assert.Equal(r.AnonTotalSalary, r.ParamsTotalSalary);
            Assert.Equal(r.AnonRank, r.ParamsRank);
        });

        // DeptId = 1, Age = 30: Bob & Charlie
        var group1_30 = results.Where(r => r.DeptId == 1 && r.Age == 30).ToList();
        Assert.Equal(2, group1_30.Count);
        Assert.All(group1_30, r =>
        {
            Assert.Equal(16000, r.AnonTotalSalary);
            Assert.Equal(1.0, r.AnonRank);
        });

        // DeptId = 1, Age = 25: Alice
        var alice = results.Single(r => r.Name == "Alice");
        Assert.Equal(5000, alice.AnonTotalSalary);
        Assert.Equal(1.0, alice.AnonRank);

        // DeptId = 2: David (28), Eva (40)
        var david = results.Single(r => r.Name == "David");
        Assert.Equal(4000, david.AnonTotalSalary);
        Assert.Equal(1.0, david.AnonRank);

        var eva = results.Single(r => r.Name == "Eva");
        Assert.Equal(6000, eva.AnonTotalSalary);
        Assert.Equal(1.0, eva.AnonRank);
    }
    [Fact]
    [Trait("LINQ", "DispersionAggregations")]
    public void Test_Linq_Std_And_Var_Direct_GroupBy_And_Over()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eva"]),
            Series.From("DeptId", [1, 1, 1, 2, 2]),
            Series.From("Age", [25, 30, 30, 28, 40]),
            Series.From("Salary", [5000, 8000, 11000, 4000, 6000])
        ]);

        // ---------------------------------------------------------------------
        // 1. Direct / Scalar Aggregate on Single Column
        // ---------------------------------------------------------------------
        // Salaries: [5000, 8000, 11000, 4000, 6000]
        // Mean = 6800
        // Deviations = [-1800, 1200, 4200, -2800, -800]
        // Sum of squares = 3,240,000 + 1,440,000 + 17,640,000 + 7,840,000 + 640,000 = 30,800,000
        // Sample Var (ddof = 1): 30,800,000 / 4 = 7,700,000
        // Sample Std (ddof = 1): sqrt(7,700,000) ≈ 2774.887385
        // Pop Var (ddof = 0): 30,800,000 / 5 = 6,160,000
        var sampleVar = df.AsQueryable<MemberRecord>()
            .Select(x => new
            {
                SampleVar = x.Salary.Var(),
                SampleStd = x.Salary.Std(),
                PopVar = x.Salary.Var(0)
            })
            .ToList();

        Assert.Single(sampleVar);
        Assert.Equal(7700000.0, sampleVar[0].SampleVar, precision: 4);
        Assert.Equal(Math.Sqrt(7700000.0), sampleVar[0].SampleStd, precision: 4);
        Assert.Equal(6160000.0, sampleVar[0].PopVar, precision: 4);

        // ---------------------------------------------------------------------
        // 2. GroupBy Reductions (Std & Var)
        // ---------------------------------------------------------------------
        // DeptId 1 (Alice 5000, Bob 8000, Charlie 11000):
        //   Mean = 8000, Dev = [-3000, 0, 3000], SumSq = 18,000,000
        //   ddof = 1: Var = 9,000,000, Std = 3000.0
        // DeptId 2 (David 4000, Eva 6000):
        //   Mean = 5000, Dev = [-1000, 1000], SumSq = 2,000,000
        //   ddof = 1: Var = 2,000,000, Std = sqrt(2,000,000) ≈ 1414.21356
        var groupResults = df.AsQueryable<MemberRecord>()
            .GroupBy(x => x.DeptId)
            .Select(g => new
            {
                DeptId = g.Key,
                DeptSalaryStd = g.Std(x => x.Salary),
                DeptSalaryVar = g.Var(x => x.Salary),
                DeptSalaryVarPop = g.Var(x => x.Salary, 0)
            })
            .ToList();

        Assert.Equal(2, groupResults.Count);

        var dept1 = groupResults.Single(g => g.DeptId == 1);
        Assert.Equal(3000.0, dept1.DeptSalaryStd, precision: 4);
        Assert.Equal(9000000.0, dept1.DeptSalaryVar, precision: 4);
        Assert.Equal(6000000.0, dept1.DeptSalaryVarPop, precision: 4);

        var dept2 = groupResults.Single(g => g.DeptId == 2);
        Assert.Equal(Math.Sqrt(2000000.0), dept2.DeptSalaryStd, precision: 4);
        Assert.Equal(2000000.0, dept2.DeptSalaryVar, precision: 4);
        Assert.Equal(1000000.0, dept2.DeptSalaryVarPop, precision: 4);

        // ---------------------------------------------------------------------
        // 3. Window Function (Over) with Anon & Params Keys
        // ---------------------------------------------------------------------
        var windowResults = df.AsQueryable<MemberRecord>()
            .Select(e => new
            {
                e.Name,
                e.DeptId,
                e.Age,
                e.Salary,

                // Over(e.DeptId) with default ddof = 1
                DeptSalaryStd = e.Salary.Std().Over(e.DeptId),
                DeptSalaryVar = e.Salary.Var().Over(e.DeptId),

                // Over(e.DeptId) with explicit ddof = 0
                DeptSalaryVarPop = e.Salary.Var(0).Over(e.DeptId),

                // Multi-partition: Over(new { e.DeptId, e.Age })
                // Dept 1, Age 30: Bob(8000), Charlie(11000) -> Mean = 9500, Diff = 1500, SumSq = 4,500,000, Var(1) = 4,500,000
                AnonSubGroupVar = e.Salary.Var().Over(new { e.DeptId, e.Age }),
                ParamsSubGroupVar = e.Salary.Var().Over(e.DeptId, e.Age)
            })
            .ToList();

        Assert.Equal(5, windowResults.Count);

        // Verification for DeptId = 1 window projections
        var dept1Members = windowResults.Where(r => r.DeptId == 1).ToList();
        Assert.Equal(3, dept1Members.Count);
        Assert.All(dept1Members, r =>
        {
            Assert.Equal(3000.0, r.DeptSalaryStd, precision: 4);
            Assert.Equal(9000000.0, r.DeptSalaryVar, precision: 4);
            Assert.Equal(6000000.0, r.DeptSalaryVarPop, precision: 4);
        });

        // Verification for Multi-column partition Over: DeptId = 1, Age = 30 (Bob & Charlie)
        var bobAndCharlie = windowResults.Where(r => r.DeptId == 1 && r.Age == 30).ToList();
        Assert.Equal(2, bobAndCharlie.Count);
        Assert.All(bobAndCharlie, r =>
        {
            Assert.Equal(r.AnonSubGroupVar, r.ParamsSubGroupVar);
            Assert.Equal(4500000.0, r.AnonSubGroupVar, precision: 4);
        });
    }
    [Fact]
    [Trait("LINQ", "CumulativeOperations")]
    public void Test_Linq_Cumulative_Operators_Direct_And_Over()
    {
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3, 4, 5]),
            Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eva"]),
            Series.From("DeptId", [1, 1, 1, 2, 2]),
            Series.From("Age", [25, 30, 30, 28, 40]),
            Series.From("Salary", [5000, 8000, 11000, 4000, 6000])
        ]);

        // ---------------------------------------------------------------------
        // 1. Direct Projection: Global Cumulative Operations (Forward & Reverse)
        // ---------------------------------------------------------------------
        // Salaries: [5000, 8000, 11000, 4000, 6000]
        // CumSum forward:  [5000, 13000, 24000, 28000, 34000]
        // CumSum reverse:  [34000, 29000, 21000, 10000, 6000]
        // CumMax forward:  [5000, 8000, 11000, 11000, 11000]
        // CumMin forward:  [5000, 5000, 5000, 4000, 4000]
        // CumCount:        [1, 2, 3, 4, 5]
        var directResults = df.AsQueryable<MemberRecord>()
            .Select(e => new
            {
                e.Name,
                e.Salary,
                RunningSum = e.Salary.CumSum(),
                ReverseRunningSum = e.Salary.CumSum(reverse: true),
                RunningMax = e.Salary.CumMax(),
                RunningMin = e.Salary.CumMin(),
                RunningCount = e.Salary.CumCount()
            })
            .ToList();

        Assert.Equal(5, directResults.Count);

        // Verify forward CumSum
        Assert.Equal([5000, 13000, 24000, 28000, 34000], directResults.Select(r => r.RunningSum));

        // Verify reverse CumSum
        Assert.Equal([34000, 29000, 21000, 10000, 6000], directResults.Select(r => r.ReverseRunningSum));

        // Verify CumMax & CumMin
        Assert.Equal([5000, 8000, 11000, 11000, 11000], directResults.Select(r => r.RunningMax));
        Assert.Equal([5000, 5000, 5000, 4000, 4000], directResults.Select(r => r.RunningMin));

        // Verify CumCount
        Assert.Equal([1L, 2L, 3L, 4L, 5L], directResults.Select(r => r.RunningCount));

        // ---------------------------------------------------------------------
        // 2. Window Projection: Cumulative Operations with Over(e.DeptId)
        // ---------------------------------------------------------------------
        // DeptId 1 (Alice 5000, Bob 8000, Charlie 11000):
        //   CumSum: [5000, 13000, 24000]
        //   CumMax: [5000, 8000, 11000]
        //   CumCount: [1, 2, 3]
        // DeptId 2 (David 4000, Eva 6000):
        //   CumSum: [4000, 10000]
        //   CumMax: [4000, 6000]
        //   CumCount: [1, 2]
        var windowResults = df.AsQueryable<MemberRecord>()
            .Select(e => new
            {
                e.Name,
                e.DeptId,
                e.Salary,
                DeptRunningSum = e.Salary.CumSum().Over(e.DeptId),
                DeptRunningSumRev = e.Salary.CumSum(reverse: true).Over(e.DeptId),
                DeptRunningMax = e.Salary.CumMax().Over(e.DeptId),
                DeptRunningCount = e.Salary.CumCount().Over(e.DeptId)
            })
            .ToList();

        Assert.Equal(5, windowResults.Count);

        // Verify DeptId = 1 window results
        var dept1 = windowResults.Where(r => r.DeptId == 1).ToList();
        Assert.Equal([5000, 13000, 24000], dept1.Select(r => r.DeptRunningSum));
        Assert.Equal([24000, 19000, 11000], dept1.Select(r => r.DeptRunningSumRev));
        Assert.Equal([5000, 8000, 11000], dept1.Select(r => r.DeptRunningMax));
        Assert.Equal([1L, 2L, 3L], dept1.Select(r => r.DeptRunningCount));

        // Verify DeptId = 2 window results
        var dept2 = windowResults.Where(r => r.DeptId == 2).ToList();
        Assert.Equal([4000, 10000], dept2.Select(r => r.DeptRunningSum));
        Assert.Equal([10000, 6000], dept2.Select(r => r.DeptRunningSumRev));
        Assert.Equal([4000, 6000], dept2.Select(r => r.DeptRunningMax));
        Assert.Equal([1L, 2L], dept2.Select(r => r.DeptRunningCount));
    }
    [Fact]
    [Trait("LINQ", "CumulativeOperations")]
    public void Test_Linq_CumProd_Decimal_Throws_Unsupported_Exception()
    {
        using var df = DataFrame.FromColumns([
            Series.From("ItemName", ["Mouse", "Keyboard", "Monitor"]),
            Series.From("Price", [100.0m, 200.0m, 1000.0m]),
            Series.From("DiscountRate", [0.9m, 0.8m, 0.5m])
        ]);

        // Polars doesn't support decimal prod
        var ex = Assert.Throws<NET.Core.PolarsException>(() =>
        {
            _ = df.AsQueryable<OrderItem>()
                .Select(x => new
                {
                    x.ItemName,
                    EffectiveFactor = x.DiscountRate.CumProd()
                })
                .ToList();
        });

        Assert.Contains("cum_prod", ex.Message);
        Assert.Contains("not supported", ex.Message);
    }
    [Fact]
    [Trait("LINQ", "CumulativeOperations")]
    public void Test_Linq_CumProd_Double_Direct_And_Reverse_Pushdown()
    {
        // [0.5, 2.0, 3.0, 0.25]
        // CumProd forward:
        //   Row 0: 0.5
        //   Row 1: 0.5 * 2.0 = 1.0
        //   Row 2: 1.0 * 3.0 = 3.0
        //   Row 3: 3.0 * 0.25 = 0.75
        // CumProd reverse (reverse: true):
        //   Row 0: 0.5 * 2.0 * 3.0 * 0.25 = 0.75
        //   Row 1: 2.0 * 3.0 * 0.25 = 1.5
        //   Row 2: 3.0 * 0.25 = 0.75
        //   Row 3: 0.25
        using var df = DataFrame.FromColumns([
            Series.From("Name", ["A", "B", "C", "D"]),
            Series.From("Factor", [0.5, 2.0, 3.0, 0.25])
        ]);

        var results = df.AsQueryable<ProductMetric>()
            .Select(x => new
            {
                x.Name,
                x.Factor,
                RunningProd = x.Factor.CumProd(),
                ReverseRunningProd = x.Factor.CumProd(reverse: true)
            })
            .ToList();

        Assert.Equal(4, results.Count);

        Assert.Equal(0.5, results[0].RunningProd, precision: 4);
        Assert.Equal(1.0, results[1].RunningProd, precision: 4);
        Assert.Equal(3.0, results[2].RunningProd, precision: 4);
        Assert.Equal(0.75, results[3].RunningProd, precision: 4);

        Assert.Equal(0.75, results[0].ReverseRunningProd, precision: 4);
        Assert.Equal(1.5, results[1].ReverseRunningProd, precision: 4);
        Assert.Equal(0.75, results[2].ReverseRunningProd, precision: 4);
        Assert.Equal(0.25, results[3].ReverseRunningProd, precision: 4);
    }
}
