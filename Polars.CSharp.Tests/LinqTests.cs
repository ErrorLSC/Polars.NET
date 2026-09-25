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
    
}
