using Polars.CSharp.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public record struct Employee(string Name, int Age, int Salary);
public readonly record struct EmployeeDept(string Name, string Department, int Age, int Salary);

public class LinqTests
{
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
    public readonly record struct Person(int Id, string Name, int DeptId);
    public readonly record struct Department(int Id, string DeptName);
    public readonly record struct PersonDeptJoined(int Id, string Name, int DeptId, string DeptName);
    public record struct EmployeeDeptRecord(int Id, string Name, string DeptName);
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
    // Specialized summary record struct for GroupBy test verification
    public readonly record struct DeptPersonSummary(int DeptId, long MemberCount, int MinId, int MaxId);

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
    public readonly record struct AdvancedDeptSummary(
        int DeptId,
        long TotalMembers,
        long HighIdMembers,
        int FirstMemberId,
        int MaxMemberId
    );

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
    // Supporting projection record struct for two-stage GroupBy->Select testing
    public readonly record struct DeptAggResult(int DeptId, long TotalCount, int MaxId);

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
    public readonly record struct PersonWithTags(int Id, string Name, string[] Tags);
    public readonly record struct PersonTagPair(int Id, string Name, string Tag);
    public readonly record struct PersonTagDetailed(int PersonId, string TagName);

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
    [Trait("LINQ", "SelectMany")]
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

    public readonly record struct DeptEmpCount(int DeptId, string DeptName, int EmployeeCount);

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
    public interface IWorker
    {
        string Name { get; }
        int Salary { get; }
    }

    public record struct WorkerRecord(string Name, int Age, int Salary) : IWorker;

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
}
