using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

// Class-typed rows for the .NET 11 FullJoin/RightJoin null-folding tests.
public record Net11Bonus(string Name, int Amount);

/// <summary>
/// Tests for the LINQ operators added in .NET 11 (System.Linq.Queryable):
///   Join / LeftJoin / RightJoin / FullJoin returning (TOuter Outer, TInner Inner) tuples,
///   FullJoin with a result selector, and GroupJoin returning IGrouping&lt;TOuter, TInner&gt;.
/// The first block was moved (uncommented) from LinqTests.cs; the *_MatchesLinqToObjects tests
/// at the end compare the Polars provider against LINQ-to-Objects over the same arrays.
/// The provider does not support these operators yet: failures here are the backlog.
/// </summary>
public class LinqNet11OperatorTests
{
    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleJoin_Inner()
    {
        Person[] persons = [
            new Person(1, "Alice", 10),
            new Person(2, "Bob", 20),
            new Person(3, "Charlie", 99) // Unmatched DeptId
        ];

        Department[] depts = [
            new Department(10, "Engineering"),
            new Department(20, "Sales"),
            new Department(30, "HR") // Unmatched Id
        ];

        using var dfPerson = DataFrame.FromRows(persons);
        using var dfDept = DataFrame.FromRows(depts);

        var personQuery = dfPerson.AsQueryable(persons);
        var deptQuery = dfDept.AsQueryable(depts);

        // .NET 11 Join returning (Person Outer, Department Inner)
        var results = personQuery
            .Join(deptQuery, p => p.DeptId, d => d.Id)
            .OrderBy(pair => pair.Outer.Id)
            .ToList();

        Assert.Equal(2, results.Count);

        Assert.Equal(1, results[0].Outer.Id);
        Assert.Equal("Alice", results[0].Outer.Name);
        Assert.Equal(10, results[0].Inner.Id);
        Assert.Equal("Engineering", results[0].Inner.DeptName);

        Assert.Equal(2, results[1].Outer.Id);
        Assert.Equal("Bob", results[1].Outer.Name);
        Assert.Equal(20, results[1].Inner.Id);
        Assert.Equal("Sales", results[1].Inner.DeptName);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleJoin_Chained_Filter_And_Projection()
    {
        Person[] persons = [
            new Person(1, "Alice", 10),
            new Person(2, "Bob", 10),
            new Person(3, "Charlie", 20)
        ];

        Department[] depts = [
            new Department(10, "Engineering"),
            new Department(20, "Sales")
        ];

        using var dfPerson = DataFrame.FromRows(persons);
        using var dfDept = DataFrame.FromRows(depts);

        var personQuery = dfPerson.AsQueryable(persons);
        var deptQuery = dfDept.AsQueryable(depts);

        var query = personQuery
            .Join(deptQuery, p => p.DeptId, d => d.Id)
            .Where(pair => pair.Inner.DeptName == "Engineering" && pair.Outer.Id > 1)
            .Select(pair => new PersonDeptJoined(
                pair.Outer.Id,
                pair.Outer.Name,
                pair.Outer.DeptId,
                pair.Inner.DeptName));

        var plan = query.Explain();
        Assert.Contains("100% Native Pushdown", plan);

        var results = query.ToList();

        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal(10, results[0].DeptId);
        Assert.Equal("Engineering", results[0].DeptName);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleJoin_LeftJoin_Class_NullFolding()
    {
        Department[] depts = [
            new Department(1, "Engineering"),
            new Department(2, "EmptyDept")
        ];

        Employee[] emps = [
            new Employee("Engineering", 25, 75000)
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfEmp = DataFrame.FromRows(emps);

        var deptQuery = dfDept.AsQueryable(depts);
        var empQuery = dfEmp.AsQueryable(emps);

        // Employee is a class record -> Inner is Employee?
        var results = deptQuery
            .LeftJoin(empQuery, d => d.DeptName, e => e.Name)
            .OrderBy(pair => pair.Outer.Id)
            .ToList();

        Assert.Equal(2, results.Count);

        // Row 1: Matched class instance
        Assert.Equal("Engineering", results[0].Outer.DeptName);
        Assert.NotNull(results[0].Inner);
        Assert.Equal("Engineering", results[0].Inner.Name);
        Assert.Equal(75000, results[0].Inner.Salary);

        // Row 2: Unmatched class instance -> true null folding
        Assert.Equal("EmptyDept", results[1].Outer.DeptName);
        Assert.Null(results[1].Inner);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleJoin_LeftJoin_Struct_DefaultFolding()
    {
        Person[] persons = [
            new Person(1, "Alice", 10),
            new Person(2, "Ghost", 99) // No department with Id = 99
        ];

        Department[] depts = [
            new Department(10, "Engineering")
        ];

        using var dfPerson = DataFrame.FromRows(persons);
        using var dfDept = DataFrame.FromRows(depts);

        var personQuery = dfPerson.AsQueryable(persons);
        var deptQuery = dfDept.AsQueryable(depts);

        // Department is a struct -> pair.Inner is Department (defaults to default(Department) when unmatched)
        var results = personQuery
            .LeftJoin(deptQuery, p => p.DeptId, d => d.Id)
            .OrderBy(pair => pair.Outer.Id)
            .ToList();

        Assert.Equal(2, results.Count);

        // Row 1: Matched struct
        Assert.Equal("Alice", results[0].Outer.Name);
        Assert.Equal("Engineering", results[0].Inner.DeptName);
        Assert.Equal(10, results[0].Inner.Id);

        // Row 2: Unmatched struct -> folds to default(Department) where DeptName is null and Id is 0
        Assert.Equal("Ghost", results[1].Outer.Name);
        Assert.Null(results[1].Inner.DeptName);
        Assert.Equal(0, results[1].Inner.Id);
    }
    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_FullJoin_WithResultSelector()
    {
        Department[] depts = [
            new Department(1, "Engineering"),
            new Department(2, "Sales"),
            new Department(3, "HR") // Dept only
        ];

        DepartmentRecord[] records = [
            new DepartmentRecord("Engineering", 150000),
            new DepartmentRecord("Sales", 90000),
            new DepartmentRecord("Legal", 80000) // Record only
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfRecord = DataFrame.FromRows(records);

        var deptQuery = dfDept.AsQueryable(depts);
        var recordQuery = dfRecord.AsQueryable(records);

        // Check struct presence via field nullability (DeptName / Department)
        var results = deptQuery
            .FullJoin(
                recordQuery,
                d => d.DeptName,
                r => r.Department,
                (d, r) => new DepartmentRecord(
                    d.DeptName ?? r.Department,
                    r.Department != null ? r.Salary : 0))
            .OrderBy(r => r.Department)
            .ToList();

        Assert.Equal(4, results.Count);

        Assert.Equal("Engineering", results[0].Department);
        Assert.Equal(150000, results[0].Salary);

        Assert.Equal("HR", results[1].Department);
        Assert.Equal(0, results[1].Salary);

        Assert.Equal("Legal", results[2].Department);
        Assert.Equal(80000, results[2].Salary);

        Assert.Equal("Sales", results[3].Department);
        Assert.Equal(90000, results[3].Salary);
    }
    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_Tuple_FullJoin_Struct_DefaultFolding()
    {
        Department[] depts = [
            new Department(1, "Engineering"),
            new Department(2, "HR") // Dept only
        ];

        DepartmentRecord[] records = [
            new DepartmentRecord("Engineering", 120000),
            new DepartmentRecord("Security", 60000) // Record only
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfRecord = DataFrame.FromRows(records);

        var deptQuery = dfDept.AsQueryable(depts);
        var recordQuery = dfRecord.AsQueryable(records);

        var results = deptQuery
            .FullJoin(recordQuery, d => d.DeptName, r => r.Department)
            .OrderBy(pair => pair.Outer.DeptName ?? pair.Inner.Department)
            .ToList();

        Assert.Equal(3, results.Count);

        // Row 1: Engineering (Matched on both sides)
        Assert.Equal("Engineering", results[0].Outer.DeptName);
        Assert.Equal(120000, results[0].Inner.Salary);

        // Row 2: HR (Left side only -> Inner is default(DepartmentRecord))
        Assert.Equal("HR", results[1].Outer.DeptName);
        Assert.Null(results[1].Inner.Department);
        Assert.Equal(0, results[1].Inner.Salary);

        // Row 3: Security (Right side only -> Outer is default(Department))
        Assert.Null(results[2].Outer.DeptName);
        Assert.Equal(0, results[2].Outer.Id);
        Assert.Equal("Security", results[2].Inner.Department);
        Assert.Equal(60000, results[2].Inner.Salary);
    }
    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_Tuple_FullJoin_Explicit_NullableStruct()
    {
        Department[] depts = [
            new Department(1, "Engineering"),
            new Department(2, "HR")
        ];

        DepartmentRecord[] records = [
            new DepartmentRecord("Engineering", 120000),
            new DepartmentRecord("Security", 60000)
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfRecord = DataFrame.FromRows(records);

        // Cast query source element type to Nullable<TStruct>
        var deptNullableQuery = dfDept.AsQueryable(depts).Select(d => (Department?)d);
        var recordNullableQuery = dfRecord.AsQueryable(records).Select(r => (DepartmentRecord?)r);

        var results = deptNullableQuery
            .FullJoin(recordNullableQuery, d => d.Value.DeptName, r => r.Value.Department)
            .OrderBy(pair => pair.Outer.HasValue ? pair.Outer.Value.DeptName : pair.Inner.Value.Department).InspectPlan()
            .ToList();

        Assert.Equal(3, results.Count);

        // Row 1: Matched
        Assert.True(results[0].Outer.HasValue);
        Assert.True(results[0].Inner.HasValue);
        Assert.Equal("Engineering", results[0].Outer!.Value.DeptName);
        Assert.Equal(120000, results[0].Inner!.Value.Salary);

        // Row 2: Left only -> Inner is actual Nullable null
        Assert.True(results[1].Outer.HasValue);
        Assert.False(results[1].Inner.HasValue);
        Assert.Equal("HR", results[1].Outer!.Value.DeptName);

        // Row 3: Right only -> Outer is actual Nullable null
        Assert.False(results[2].Outer.HasValue);
        Assert.True(results[2].Inner.HasValue);
        Assert.Equal("Security", results[2].Inner!.Value.Department);
        Assert.Equal(60000, results[2].Inner!.Value.Salary);
    }
    [Fact]
    [Trait("LINQ", "Net11_GroupJoin")]
    public void Test_Linq_Net11_Standalone_GroupJoin_Materialization()
    {
        // Arrange
        // Department: readonly record struct Department(int Id, string DeptName)
        // Employee: record Employee(string Name, int Age, int Salary)
        Department[] depts = [
            new Department(10, "Engineering"),
            new Department(20, "Sales"),
            new Department(30, "HR") // Dept 30 has NO matching employees
        ];

        Employee[] emps = [
            new Employee("Alice", 10, 80000),   // Age is used as DeptId for join
            new Employee("Charlie", 10, 50000), // Age is used as DeptId for join
            new Employee("Bob", 20, 45000)      // Age is used as DeptId for join
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfEmp = DataFrame.FromRows(emps);

        var deptQuery = dfDept.AsQueryable(depts);
        var empQuery = dfEmp.AsQueryable(emps);

        // Act: .NET 11 Standalone GroupJoin returning IQueryable<IGrouping<Department, Employee>>
        var results = deptQuery
            .GroupJoin(empQuery, d => d.Id, e => e.Age)
            .ToList();

        // Assert
        Assert.Equal(3, results.Count);

        // Group 0: Engineering -> 2 employees (Alice, Charlie)
        var grp0 = results[0];
        Assert.Equal(10, grp0.Key.Id);
        Assert.Equal("Engineering", grp0.Key.DeptName);
        var grp0Emps = grp0.ToList();
        Assert.Equal(2, grp0Emps.Count);
        Assert.Equal("Alice", grp0Emps[0].Name);
        Assert.Equal("Charlie", grp0Emps[1].Name);

        // Group 1: Sales -> 1 employee (Bob)
        var grp1 = results[1];
        Assert.Equal(20, grp1.Key.Id);
        Assert.Equal("Sales", grp1.Key.DeptName);
        var grp1Emps = grp1.ToList();
        Assert.Single(grp1Emps);
        Assert.Equal("Bob", grp1Emps[0].Name);

        // Group 2: HR -> 0 employees (empty group, not null!)
        var grp2 = results[2];
        Assert.Equal(30, grp2.Key.Id);
        Assert.Equal("HR", grp2.Key.DeptName);
        Assert.Empty(grp2);
    }

    [Fact]
    [Trait("LINQ", "Net11_GroupJoin")]
    public void Test_Linq_Net11_GroupJoin_With_Chained_Projection_Pushdown()
    {
        Department[] depts = [
            new Department(10, "Engineering"),
            new Department(20, "Sales"),
            new Department(30, "HR")
        ];

        Employee[] emps = [
            new Employee("Alice", 10, 80000),
            new Employee("Charlie", 10, 50000),
            new Employee("Bob", 20, 45000)
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfEmp = DataFrame.FromRows(emps);

        var deptQuery = dfDept.AsQueryable(depts);
        var empQuery = dfEmp.AsQueryable(emps);

        // Act: .NET 11 GroupJoin followed by Select with g.Key and g.Count()
        var query = deptQuery
            .GroupJoin(empQuery, d => d.Id, e => e.Age)
            .Select(g => new DeptEmpCount(g.Key.Id, g.Key.DeptName, g.Count()))
            .OrderBy(r => r.DeptId);

        // Verify native pushdown
        var plan = query.Explain();
        Assert.Contains("100% Native Pushdown", plan);

        var results = query.ToList();

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Equal(2, results[0].EmployeeCount); // Engineering
        Assert.Equal(1, results[1].EmployeeCount); // Sales
        Assert.Equal(0, results[2].EmployeeCount); // HR
    }

    [Fact]
    [Trait("LINQ", "Net11_GroupJoin")]
    public void Test_Linq_Net11_GroupJoin_Struct_Inner_Elements()
    {
        // Testing with struct Person as TInner
        Department[] depts = [
            new Department(10, "Engineering"),
            new Department(99, "EmptyDept")
        ];

        Person[] persons = [
            new Person(1, "Alice", 10),
            new Person(2, "Charlie", 10)
        ];

        using var dfDept = DataFrame.FromRows(depts);
        using var dfPerson = DataFrame.FromRows(persons);

        var deptQuery = dfDept.AsQueryable(depts);
        var personQuery = dfPerson.AsQueryable(persons);

        var results = deptQuery
            .GroupJoin(personQuery, d => d.Id, p => p.DeptId)
            .ToList();

        Assert.Equal(2, results.Count);

        // Engineering has 2 persons
        Assert.Equal(2, results[0].Count());
        Assert.Equal("Alice", results[0].First().Name);

        // EmptyDept has 0 persons
        Assert.Empty(results[1]);
    }

    // ------------------------------------------------------------------
    // Parity tests: Polars provider vs LINQ-to-Objects over the same data
    // ------------------------------------------------------------------

    private static readonly Person[] ParityPersons =
    [
        new Person(1, "Alice", 10),
        new Person(2, "Bob", 20),
        new Person(3, "Charlie", 10),
        new Person(4, "Ghost", 99),
    ];

    private static readonly Department[] ParityDepts =
    [
        new Department(10, "Engineering"),
        new Department(20, "Sales"),
        new Department(30, "HR"),
    ];

    private static readonly Employee[] ParityEmps =
    [
        new Employee("Alice", 30, 80000),
        new Employee("Bob", 40, 45000),
        new Employee("Dora", 50, 60000),
    ];

    private static readonly Net11Bonus[] ParityBonuses =
    [
        new Net11Bonus("Alice", 1000),
        new Net11Bonus("Erin", 500),
    ];

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleJoin_MatchesLinqToObjects()
    {
        using var dfPerson = DataFrame.FromRows(ParityPersons);
        using var dfDept = DataFrame.FromRows(ParityDepts);

        var expected = ParityPersons
            .Join(ParityDepts, p => p.DeptId, d => d.Id)
            .Select(pair => new { pair.Outer.Id, pair.Outer.Name, pair.Inner.DeptName })
            .OrderBy(r => r.Id)
            .ToList();

        var actual = dfPerson.AsQueryable(ParityPersons)
            .Join(dfDept.AsQueryable(ParityDepts), p => p.DeptId, d => d.Id)
            .Select(pair => new { pair.Outer.Id, pair.Outer.Name, pair.Inner.DeptName })
            .OrderBy(r => r.Id)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleLeftJoin_MatchesLinqToObjects()
    {
        using var dfPerson = DataFrame.FromRows(ParityPersons);
        using var dfDept = DataFrame.FromRows(ParityDepts);

        // Department is a struct: unmatched rows fold to default(Department).
        var expected = ParityPersons
            .LeftJoin(ParityDepts, p => p.DeptId, d => d.Id)
            .Select(pair => new { pair.Outer.Id, pair.Inner.DeptName, DeptId = pair.Inner.Id })
            .OrderBy(r => r.Id)
            .ToList();

        var actual = dfPerson.AsQueryable(ParityPersons)
            .LeftJoin(dfDept.AsQueryable(ParityDepts), p => p.DeptId, d => d.Id)
            .Select(pair => new { pair.Outer.Id, pair.Inner.DeptName, DeptId = pair.Inner.Id })
            .OrderBy(r => r.Id)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleRightJoin_MatchesLinqToObjects()
    {
        using var dfPerson = DataFrame.FromRows(ParityPersons);
        using var dfDept = DataFrame.FromRows(ParityDepts);

        // Person is a struct: the HR department has no persons -> Outer folds to default(Person).
        var expected = ParityPersons
            .RightJoin(ParityDepts, p => p.DeptId, d => d.Id)
            .Select(pair => new { DeptId = pair.Inner.Id, pair.Inner.DeptName, PersonId = pair.Outer.Id, pair.Outer.Name })
            .OrderBy(r => r.DeptId).ThenBy(r => r.PersonId)
            .ToList();

        var actual = dfPerson.AsQueryable(ParityPersons)
            .RightJoin(dfDept.AsQueryable(ParityDepts), p => p.DeptId, d => d.Id)
            .Select(pair => new { DeptId = pair.Inner.Id, pair.Inner.DeptName, PersonId = pair.Outer.Id, pair.Outer.Name })
            .OrderBy(r => r.DeptId).ThenBy(r => r.PersonId)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleRightJoin_Class_NullFolding_MatchesLinqToObjects()
    {
        using var dfEmp = DataFrame.FromRows(ParityEmps);
        using var dfBonus = DataFrame.FromRows(ParityBonuses);

        // Employee is a class: the "Erin" bonus has no employee -> Outer is null.
        var expected = ParityEmps
            .RightJoin(ParityBonuses, e => e.Name, b => b.Name)
            .Select(pair => new { BonusName = pair.Inner.Name, pair.Inner.Amount, EmpName = pair.Outer == null ? null : pair.Outer.Name })
            .OrderBy(r => r.BonusName)
            .ToList();

        var actual = dfEmp.AsQueryable(ParityEmps)
            .RightJoin(dfBonus.AsQueryable(ParityBonuses), e => e.Name, b => b.Name)
            .Select(pair => new { BonusName = pair.Inner.Name, pair.Inner.Amount, EmpName = pair.Outer == null ? null : pair.Outer.Name })
            .OrderBy(r => r.BonusName)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_TupleFullJoin_Class_NullFolding_MatchesLinqToObjects()
    {
        using var dfEmp = DataFrame.FromRows(ParityEmps);
        using var dfBonus = DataFrame.FromRows(ParityBonuses);

        // Both sides are classes: unmatched sides fold to null.
        var expected = ParityEmps
            .FullJoin(ParityBonuses, e => e.Name, b => b.Name)
            .Select(pair => new
            {
                EmpName = pair.Outer == null ? null : pair.Outer.Name,
                BonusName = pair.Inner == null ? null : pair.Inner.Name,
                Amount = pair.Inner == null ? 0 : pair.Inner.Amount,
            })
            .OrderBy(r => r.EmpName ?? r.BonusName)
            .ToList();

        var actual = dfEmp.AsQueryable(ParityEmps)
            .FullJoin(dfBonus.AsQueryable(ParityBonuses), e => e.Name, b => b.Name)
            .Select(pair => new
            {
                EmpName = pair.Outer == null ? null : pair.Outer.Name,
                BonusName = pair.Inner == null ? null : pair.Inner.Name,
                Amount = pair.Inner == null ? 0 : pair.Inner.Amount,
            })
            .OrderBy(r => r.EmpName ?? r.BonusName)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_FullJoin_ResultSelector_MatchesLinqToObjects()
    {
        using var dfPerson = DataFrame.FromRows(ParityPersons);
        using var dfDept = DataFrame.FromRows(ParityDepts);

        var expected = ParityPersons
            .FullJoin(ParityDepts, p => p.DeptId, d => d.Id,
                (p, d) => new { PersonId = p.Id, p.Name, DeptId = d.Id, d.DeptName })
            .OrderBy(r => r.DeptId).ThenBy(r => r.PersonId)
            .ToList();

        var actual = dfPerson.AsQueryable(ParityPersons)
            .FullJoin(dfDept.AsQueryable(ParityDepts), p => p.DeptId, d => d.Id,
                (p, d) => new { PersonId = p.Id, p.Name, DeptId = d.Id, d.DeptName })
            .OrderBy(r => r.DeptId).ThenBy(r => r.PersonId)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_GroupJoin")]
    public void Test_Linq_Net11_GroupJoin_Aggregates_MatchesLinqToObjects()
    {
        using var dfDept = DataFrame.FromRows(ParityDepts);
        using var dfPerson = DataFrame.FromRows(ParityPersons);

        var expected = ParityDepts
            .GroupJoin(ParityPersons, d => d.Id, p => p.DeptId)
            .Select(g => new { g.Key.Id, g.Key.DeptName, Count = g.Count(), MaxPersonId = g.Select(p => p.Id).DefaultIfEmpty().Max() })
            .OrderBy(r => r.Id)
            .ToList();

        var actual = dfDept.AsQueryable(ParityDepts)
            .GroupJoin(dfPerson.AsQueryable(ParityPersons), d => d.Id, p => p.DeptId)
            .Select(g => new { g.Key.Id, g.Key.DeptName, Count = g.Count(), MaxPersonId = g.Select(p => p.Id).DefaultIfEmpty().Max() })
            .OrderBy(r => r.Id)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("LINQ", "Net11_GroupJoin")]
    public void Test_Linq_Net11_GroupJoin_Materialized_Groups_MatchLinqToObjects()
    {
        using var dfDept = DataFrame.FromRows(ParityDepts);
        using var dfPerson = DataFrame.FromRows(ParityPersons);

        var expected = ParityDepts
            .GroupJoin(ParityPersons, d => d.Id, p => p.DeptId)
            .OrderBy(g => g.Key.Id)
            .Select(g => (g.Key, Members: g.Select(p => p.Name).Order().ToList()))
            .ToList();

        var actual = dfDept.AsQueryable(ParityDepts)
            .GroupJoin(dfPerson.AsQueryable(ParityPersons), d => d.Id, p => p.DeptId)
            .OrderBy(g => g.Key.Id)
            .ToList()
            .Select(g => (g.Key, Members: g.Select(p => p.Name).Order().ToList()))
            .ToList();

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Key, actual[i].Key);
            Assert.Equal(expected[i].Members, actual[i].Members);
        }
    }

    // Nullable<struct> sides (`Select(x => (T?)x)`): HasValue is the side's presence, natively.
    private static readonly Department[] NsDepts = [new(1, "Eng"), new(2, "HR")];
    private static readonly DepartmentRecord[] NsRecs = [new("Eng", 100), new("Sec", 60)];

    [Fact]
    [Trait("LINQ", "Net11_Joins")]
    public void Test_Linq_Net11_NullableStruct_HasValue_In_Projection_And_Filter_MatchesLinqToObjects()
    {
        using var dfDept = DataFrame.FromRows(NsDepts);
        using var dfRec = DataFrame.FromRows(NsRecs);

        IQueryable<object> Build(IQueryable<Department> d, IQueryable<DepartmentRecord> r) =>
            d.Select(x => (Department?)x)
             .FullJoin(r.Select(y => (DepartmentRecord?)y), x => x.Value.DeptName, y => y.Value.Department)
             .Where(p => !p.Inner.HasValue || p.Inner.Value.Salary > 50)
             .Select(p => new { HasDept = p.Outer.HasValue, HasRec = p.Inner.HasValue, Name = p.Outer.HasValue ? p.Outer.Value.DeptName : p.Inner.Value.Department })
             .OrderBy(x => x.Name)
             .Select(x => (object)x);

        var expected = Build(NsDepts.AsQueryable(), NsRecs.AsQueryable()).ToList();
        var actualQuery = Build(dfDept.AsQueryable(NsDepts), dfRec.AsQueryable(NsRecs));
        Assert.Equal(expected, actualQuery.ToList());

        var native = dfDept.AsQueryable(NsDepts).Select(x => (Department?)x)
            .LeftJoin(dfRec.AsQueryable(NsRecs).Select(y => (DepartmentRecord?)y), x => x.Value.DeptName, y => y.Value.Department)
            .Select(p => new { p.Outer.Value.Id, Matched = p.Inner.HasValue });
        Assert.Contains("100% Native Pushdown", native.Explain());
        Assert.Equal([new { Id = 1, Matched = true }, new { Id = 2, Matched = false }], native.OrderBy(x => x.Id).ToList());
    }
}
