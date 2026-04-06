using Polars.CSharp;
using Pl = Polars.CSharp.Polars;
using Polars.NET.Linq.CSharpExtensions;
using LinqToDB;
using LinqToDB.Async;
using Polars.NET.Linq;
using LinqToDB.Mapping; 

namespace Polars.Integration.Tests;

public class LinqProviderTests
{
    public class Person
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public double Sales { get; set; }
    }

    public class PersonDto
    {
        public string Name { get; set; } = string.Empty;
        public double Sales { get; set; }
    }

    public class Department
    {
        public int DeptId { get; set; }
        public string DeptName { get; set; } = string.Empty;
    }

    public class Employee
    {
        public string Name { get; set; } = string.Empty;
        public int DeptId { get; set; }
    }

    public class EmpDeptDto
    {
        public string EmpName { get; set; } = string.Empty;
        public string DepartmentName { get; set; } = string.Empty;
    }

    public class EmployeeSalary
    {
        public string Name { get; set; } = string.Empty;
        public int DeptId { get; set; }
        public double Salary { get; set; }
    }

    public class DeptStatsDto
    {
        public int DeptId { get; set; }
        public double TotalSalary { get; set; }
        public int EmployeeCount { get; set; }
    }
    public class SimpleUser
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
    [Fact]
    [Trait("Linq", "Where")]
    public void Test_Polars_Linq_Where_And_OrderBy()
    {
        var data = new[]
        {
            new Person { Name = "Alice",   Age = 25, Sales = 100.0 },
            new Person { Name = "Bob",     Age = 30, Sales = 200.0 },
            new Person { Name = "Charlie", Age = 35, Sales = 300.0 },
            new Person { Name = "David",   Age = 18, Sales =  50.0 }
        };

        using var df = DataFrame.From(data);

        int ageLimit = 20;
        string excludeName = "Alice";

        var query = df.AsQueryable<Person>()
                      .Where(p => p.Age > ageLimit && p.Name != excludeName)
                      .OrderByDescending(p => p.Sales);

        // SELECT
        //         p."Name",
        //         p."Age",
        //         p."Sales"
        // FROM
        //         "Person" p
        // WHERE
        //         p."Age" > 20 AND (p."Name" <> 'Alice' OR p."Name" IS NULL)
        // ORDER BY
        //         p."Sales" DESC

        // shape: (2, 3)
        // ┌─────────┬─────┬───────┐
        // │ Name    ┆ Age ┆ Sales │
        // │ ---     ┆ --- ┆ ---   │
        // │ str     ┆ i32 ┆ f64   │
        // ╞═════════╪═════╪═══════╡
        // │ Charlie ┆ 35  ┆ 300.0 │
        // │ Bob     ┆ 30  ┆ 200.0 │
        // └─────────┴─────┴───────┘
        var results = query.ToList();
        Assert.NotNull(results);
        Assert.Equal(2, results.Count); 

        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal(35, results[0].Age);
        Assert.Equal(300.0, results[0].Sales);

        Assert.Equal("Bob", results[1].Name);
        Assert.Equal(30, results[1].Age);
        Assert.Equal(200.0, results[1].Sales);
    }

    [Fact]
    [Trait("Linq", "Select")]
    public void Test_Polars_Linq_Select_Projection()
    {
        var data = new[]
        {
            new Person { Name = "Alice", Age = 25, Sales = 100.0 },
            new Person { Name = "Bob",   Age = 30, Sales = 200.0 },
            new Person { Name = "Charlie", Age = 35, Sales = 300.0 }
        };

        using var df = DataFrame.From(data);

        var query = df.AsQueryable<Person>()
                      .Where(p => p.Sales > 150)
                      .Select(p => new PersonDto 
                      { 
                          Name = p.Name, 
                          Sales = p.Sales 
                      });
        // SELECT
        //         p."Name",
        //         p."Sales"
        // FROM
        //         "Person" p
        // WHERE
        //         p."Sales" > 150

        // shape: (2, 2)
        // ┌─────────┬───────┐
        // │ Name    ┆ Sales │
        // │ ---     ┆ ---   │
        // │ str     ┆ f64   │
        // ╞═════════╪═══════╡
        // │ Bob     ┆ 200.0 │
        // │ Charlie ┆ 300.0 │
        // └─────────┴───────┘

        var results = query.ToList();
        Assert.Equal(2, results.Count);
        
        Assert.IsType<List<PersonDto>>(results);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal(200.0, results[0].Sales);
    }
    [Fact]
    [Trait("Linq","Join")]
    public void Test_Polars_Linq_Inner_Join()
    {
        var depts = new[]
        {
            new Department { DeptId = 1, DeptName = "Engineering" },
            new Department { DeptId = 2, DeptName = "Sales" }
        };

        var emps = new[]
        {
            new Employee { Name = "Alice", DeptId = 1 },
            new Employee { Name = "Bob", DeptId = 2 },
            new Employee { Name = "Charlie", DeptId = 1 }
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(Pl.Sql(),ownsContext:true);
        var deptQuery = dfDepts.AsQueryable<Department>(db);
        var empQuery = dfEmps.AsQueryable<Employee>(db);

        var query = from e in empQuery
                    join d in deptQuery on e.DeptId equals d.DeptId
                    where e.Name != "Bob"
                    orderby d.DeptName,e.Name
                    select new EmpDeptDto
                    {
                        EmpName = e.Name,
                        DepartmentName = d.DeptName
                    };
        // SELECT
        //         e."Name",
        //         d."DeptName"
        // FROM
        //         "Employee" e
        //                 INNER JOIN "Department" d ON e."DeptId" = d."DeptId"
        // WHERE
        //         e."Name" <> 'Bob' OR e."Name" IS NULL
        // ORDER BY
        //         d."DeptName",
        //         e."Name"
        // shape: (2, 2)
        // ┌─────────┬─────────────┐
        // │ Name    ┆ DeptName    │
        // │ ---     ┆ ---         │
        // │ str     ┆ str         │
        // ╞═════════╪═════════════╡
        // │ Alice   ┆ Engineering │
        // │ Charlie ┆ Engineering │
        // └─────────┴─────────────┘

        var results = query.ToList();
        Assert.Equal(2, results.Count);
        
        Assert.Equal("Alice", results[0].EmpName);
        Assert.Equal("Engineering", results[0].DepartmentName);

        Assert.Equal("Charlie", results[1].EmpName);
        Assert.Equal("Engineering", results[1].DepartmentName);
    }
    [Fact]
    [Trait("Linq","GroupByHaving")]
    public void Test_Polars_Linq_GroupBy_Aggregation_With_Having()
    {
        var emps = new[]
        {
            new EmployeeSalary { Name = "Alice", DeptId = 1, Salary = 5000.0 },
            new EmployeeSalary { Name = "Bob",   DeptId = 2, Salary = 4000.0 },
            new EmployeeSalary { Name = "Charlie", DeptId = 1, Salary = 6000.0 },
            new EmployeeSalary { Name = "David", DeptId = 2, Salary = 4500.0 },
            new EmployeeSalary { Name = "Eve",   DeptId = 3, Salary = 3000.0 }
        };

        using var dfEmps = DataFrame.From(emps);
        
        var empQuery = dfEmps.AsQueryable<EmployeeSalary>();

        var query = from e in empQuery
                    group e by e.DeptId into g
                    where g.Sum(x => x.Salary) > 5000.0 
                    orderby g.Key
                    select new DeptStatsDto
                    {
                        DeptId = g.Key,
                        TotalSalary = g.Sum(x => x.Salary),
                        EmployeeCount = g.Count()
                    };

        var results = query.ToList();

        // SELECT
        //         e."Name",
        //         d."DeptName"
        // FROM
        //         "Employee" e
        // INNER JOIN "Department" d ON e."DeptId" = d."DeptId"
        // WHERE
        //         e."Name" <> 'Bob' OR e."Name" IS NULL
        // ORDER BY
        //         d."DeptName",
        //         e."Name"
        // shape: (2, 2)
        // ┌─────────┬─────────────┐
        // │ Name    ┆ DeptName    │
        // │ ---     ┆ ---         │
        // │ str     ┆ str         │
        // ╞═════════╪═════════════╡
        // │ Alice   ┆ Engineering │
        // │ Charlie ┆ Engineering │
        // └─────────┴─────────────┘

        Assert.Equal(2, results.Count); 

        Assert.Equal(1, results[0].DeptId);
        Assert.Equal(11000.0, results[0].TotalSalary); // 5000 + 6000
        Assert.Equal(2, results[0].EmployeeCount);

        Assert.Equal(2, results[1].DeptId);
        Assert.Equal(8500.0, results[1].TotalSalary); // 4000 + 4500
        Assert.Equal(2, results[1].EmployeeCount);
        
        Assert.DoesNotContain(results, r => r.DeptId == 3);
    }
    [Fact]
    [Trait("Linq", "ScalarAndFirst")]
    public void Test_Polars_Linq_Scalar_And_First()
    {     
        var data = new[]
        {
            new { Id = 1, Name = "Alice", Score = 80 },
            new { Id = 2, Name = "Bob", Score = 90 },
            new { Id = 3, Name = "Charlie", Score = 85 }
        };

        var query = DataFrame.From(data).AsQueryable(data);

        int highScorersCount = query.Where(s => s.Score > 82).Count();
        
        Assert.Equal(2, highScorersCount); 

        // linq2db : SELECT MAX(Score) FROM students
        int maxScore = query.Max(s => s.Score);
        Assert.Equal(90, maxScore);

        // linq2db : SELECT Id, Name, Score FROM students WHERE Score > 82 ORDER BY Score DESC LIMIT 1
        var topStudent = query.Where(s => s.Score > 82)
                              .OrderByDescending(s => s.Score)
                              .First();
                              
        Assert.NotNull(topStudent);
        Assert.Equal("Bob", topStudent.Name);
        Assert.Equal(90, topStudent.Score);
    }
    public record Product(int Id, string Name, string Category, double Price);
    [Fact]
    [Trait("Linq", "AdvancedFilters")]
    public void Test_Polars_Linq_In_And_String_Like()
    {
        var data = new[]
        {
            new Product(1, "Apple", "Fruit", 1.2),
            new Product(2, "Banana", "Fruit", 0.8),
            new Product(3, "Carrot", "Vegetable", 0.5),
            new Product(4, "Avocado", "Vegetable", 2.0),
            new Product(5, "Beef", "Meat", 5.0)
        };

        using var df = DataFrame.From(data);

        var query = df.AsQueryable<Product>();

        // linq2db : SELECT ... FROM products WHERE Category IN ('Fruit', 'Meat')
        var targetCategories = new[] { "Fruit", "Meat" };
        var inResult = query.Where(p => targetCategories.Contains(p.Category)).ToList();
        
        Assert.Equal(3, inResult.Count); // Apple, Banana, Beef
        Assert.DoesNotContain(inResult, p => p.Category == "Vegetable");

        // linq2db : SELECT ... FROM products WHERE Name LIKE 'A%' ESCAPE '~' (ESCAPE will be removed)
        // ==========================================
        var likeResult = query.Where(p => p.Name.StartsWith("A")).ToList();
        
        Assert.Equal(2, likeResult.Count); // Apple, Avocado
        Assert.Contains(likeResult, p => p.Name == "Apple");
        Assert.Contains(likeResult, p => p.Name == "Avocado");

        var complexResult = query.Where(p => 
            targetCategories.Contains(p.Category) && 
            p.Name.Contains('e') && 
            p.Price > 1.0
        ).ToList();

        Assert.Equal(2, complexResult.Count); 
    }
    [Fact]
    [Trait("Linq", "PaginationAndDistinct")]
    public void Test_Polars_Linq_Skip_Take_And_Distinct()
    {
        var data = new[]
        {
            new Product(1, "Apple", "Fruit", 1.2),
            new Product(2, "Banana", "Fruit", 0.8),
            new Product(3, "Cherry", "Fruit", 1.5),
            new Product(4, "Beef", "Meat", 5.0),
            new Product(5, "Pork", "Meat", 4.0),
            new Product(6, "Apple", "Fruit", 1.2) // Duplicated
        };

        using var df = DataFrame.From(data);
        var query = df.AsQueryable<Product>();

        // linq2db : SELECT DISTINCT p."Category" FROM products p
        var distinctCategories = query.Select(p => p.Category).Distinct().ToList();
        
        Assert.Equal(2, distinctCategories.Count);
        Assert.Contains("Fruit", distinctCategories);
        Assert.Contains("Meat", distinctCategories);

        var distinctProducts = query.Distinct().ToList();
        Assert.Equal(6, distinctProducts.Count); 

        // linq2db : SELECT ... FROM products ORDER BY p."Id" LIMIT 2 OFFSET 2
        var pagedResult = query.OrderBy(p => p.Id)
                               .Skip(2)
                               .Take(2)
                               .ToList();
                               
        Assert.Equal(2, pagedResult.Count);
        Assert.Equal(3, pagedResult[0].Id); 
        Assert.Equal(4, pagedResult[1].Id);
    }
    [Fact]
    [Trait("Linq", "Subquery")]
    public void Test_Polars_Linq_Subquery_Any()
    {
        var depts = new[]
        {
            new { DeptId = 1, DeptName = "Engineering" },
            new { DeptId = 2, DeptName = "Sales" },
            new { DeptId = 3, DeptName = "HR" }
        };

        var emps = new[]
        {
            new { Name = "Alice", DeptId = 1, Salary = 6000.0 }, 
            new { Name = "Bob",   DeptId = 2, Salary = 4000.0 }, 
            new { Name = "Charlie", DeptId = 1, Salary = 4500.0 }
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(Pl.Sql(),true);
        var deptQuery = dfDepts.AsQueryable(depts,db);
        var empQuery = dfEmps.AsQueryable(emps,db);

        var query = deptQuery
            .Join(
                empQuery.Where(e => e.Salary > 5000), 
                d => d.DeptId, 
                e => e.DeptId, 
                (d, e) => d 
            )
            .Distinct();
        var result = query.ToList();
        
        Assert.Single(result);
        Assert.Equal("Engineering", result[0].DeptName);
    }
    public class DeptDto
    {
        public int DeptId { get; set; }
        public string DeptName { get; set; } = string.Empty;
    }

    public class EmpDto
    {
        public string Name { get; set; } = string.Empty;
        public int DeptId { get; set; }
    }

    public class LeftJoinResult
    {
        public string DeptName { get; set; } = string.Empty;
        public string? EmployeeName { get; set; }
    }
    [Fact]
    [Trait("Linq", "LeftJoin")]
    public void Test_Polars_Linq_Left_Join()
    {
        // Arrange
        var depts = new[]
        {
            new DeptDto { DeptId = 1, DeptName = "Engineering" },
            new DeptDto { DeptId = 2, DeptName = "Sales" },
            new DeptDto { DeptId = 3, DeptName = "HR" } 
        };

        var emps = new[]
        {
            new EmpDto { Name = "Alice", DeptId = 1 },
            new EmpDto { Name = "Bob",   DeptId = 2 },
            new EmpDto { Name = "Charlie", DeptId = 1 }
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var deptQuery = db.RegisterTable<DeptDto>(dfDepts);
        var empQuery = db.RegisterTable<EmpDto>(dfEmps);

        // Classic LINQ Left Join
        var query = from d in deptQuery
                    join e in empQuery on d.DeptId equals e.DeptId into empGroup
                    from e in empGroup.DefaultIfEmpty() 
                    orderby d.DeptId, e.Name
                    select new LeftJoinResult
                    {
                        DeptName = d.DeptName,
                        EmployeeName = e != null ? e.Name : "NO_EMPLOYEE" 
                    };

        var results = query.ToList();
        // SELECT
        //         d."DeptName",
        //         CASE
        //                 WHEN e."DeptId" IS NOT NULL THEN e."Name"
        //                 ELSE 'NO_EMPLOYEE'
        //         END as "EmployeeName"
        // FROM
        //         "DeptDto" d
        // LEFT JOIN "EmpDto" e ON d."DeptId" = e."DeptId"
        // ORDER BY
        //         d."DeptId",
        //         e."Name"
        // shape: (4, 2)
        // ┌─────────────┬──────────────┐
        // │ DeptName    ┆ EmployeeName │
        // │ ---         ┆ ---          │
        // │ str         ┆ str          │
        // ╞═════════════╪══════════════╡
        // │ Engineering ┆ Alice        │
        // │ Engineering ┆ Charlie      │
        // │ Sales       ┆ Bob          │
        // │ HR          ┆ NO_EMPLOYEE  │
        // └─────────────┴──────────────┘

        Assert.Equal(4, results.Count);

        Assert.Equal("Engineering", results[0].DeptName);
        Assert.Equal("Alice", results[0].EmployeeName);
        
        Assert.Equal("Engineering", results[1].DeptName);
        Assert.Equal("Charlie", results[1].EmployeeName);

        Assert.Equal("Sales", results[2].DeptName);
        Assert.Equal("Bob", results[2].EmployeeName);

        Assert.Equal("HR", results[3].DeptName);
        Assert.Equal("NO_EMPLOYEE", results[3].EmployeeName);
    }
    [Fact]
    [Trait("Linq", "UnionAndCrossJoin")]
    public void Test_Polars_Linq_Union_And_CrossJoin()
    {
        var depts = new[]
        {
            new DeptDto { DeptId = 1, DeptName = "Engineering" },
            new DeptDto { DeptId = 2, DeptName = "Sales" }
        };

        var emps = new[]
        {
            new EmpDto { Name = "Alice", DeptId = 1 },
            new EmpDto { Name = "Bob",   DeptId = 2 }
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(Pl.Sql(), ownsContext: true);
        var deptQuery = db.RegisterTable<DeptDto>(dfDepts);
        var empQuery = db.RegisterTable<EmpDto>(dfEmps);

        // linq2db : SELECT ... FROM departments d CROSS JOIN employees e
        var crossJoinQuery = from d in deptQuery
                             from e in empQuery
                             select new { d.DeptName, e.Name };

        var crossResult = crossJoinQuery.ToList();
        
        Assert.Equal(4, crossResult.Count);
        Assert.Contains(crossResult, x => x.DeptName == "Engineering" && x.Name == "Alice");
        Assert.Contains(crossResult, x => x.DeptName == "Sales" && x.Name == "Bob");

        // linq2db : SELECT ... FROM employees WHERE ... UNION ALL SELECT ... FROM employees WHERE ...
        var query1 = empQuery.Where(e => e.DeptId == 1);
        var query2 = empQuery.Where(e => e.DeptId == 2);
        
        var unionResult = query1.Concat(query2).ToList();

        Assert.Equal(2, unionResult.Count);
        Assert.Contains(unionResult, x => x.Name == "Alice");
        Assert.Contains(unionResult, x => x.Name == "Bob");

        var upperResult = empQuery.Select(e => e.Name.ToUpper()).ToList();
        
        Assert.Contains("ALICE", upperResult);
        Assert.Contains("BOB", upperResult);
    }
    public record EmpSalaryDto(string Name, int DeptId, double Salary);

    [Fact]
    [Trait("Linq", "AdvancedSetsAndLet")]
    public void Test_Polars_Linq_Except_Intersect_And_Let()
    {
        var emps = new[]
        {
            new EmpSalaryDto("Alice", 1, 6000.0),
            new EmpSalaryDto("Bob", 2, 4000.0),
            new EmpSalaryDto("Charlie", 1, 4500.0),
            new EmpSalaryDto("David", 3, 8000.0),
            new EmpSalaryDto("Eve", 2, 5500.0)
        };

        using var dfEmps = DataFrame.From(emps);
        var empQuery = dfEmps.AsQueryable<EmpSalaryDto>();

        // linq2db: SELECT ... INTERSECT SELECT ...
        var q1 = empQuery.Where(e => e.DeptId == 1);
        var q2 = empQuery.Where(e => e.Salary > 4000);
        
        var intersectResult = q1.Intersect(q2).ToList();
        
        Assert.Equal(2, intersectResult.Count); // Alice , Charlie
        Assert.Contains(intersectResult, e => e.Name == "Alice");
        Assert.Contains(intersectResult, e => e.Name == "Charlie");

        // linq2db : SELECT ... EXCEPT SELECT ...
        var exceptResult = q2.Except(q1).ToList();
        
        Assert.Equal(2, exceptResult.Count); // David (8000, 3) , Eve (5500, 2)
        Assert.DoesNotContain(exceptResult, e => e.DeptId == 1);

        var letQuery = from e in empQuery
                       let bonus = e.Salary * 1.5
                       where bonus > 8000
                       select new 
                       { 
                           e.Name, 
                           Bonus = bonus 
                       };

        var letResult = letQuery.ToList();

        Assert.Equal(3, letResult.Count);
        Assert.Contains(letResult, x => x.Name == "Alice" && x.Bonus == 9000.0);
        Assert.Contains(letResult, x => x.Name == "David" && x.Bonus == 12000.0);
    }
    [Fact]
    [Trait("Linq", "WindowFunctions")]
    public void Test_Polars_Linq_Window_Functions()
    {
        var emps = new[]
        {
            new EmpSalaryDto("Alice", 1, 6000.0),
            new EmpSalaryDto("Bob", 2, 4000.0),
            new EmpSalaryDto("Charlie", 1, 4500.0),
            new EmpSalaryDto("David", 3, 8000.0),
            new EmpSalaryDto("Eve", 2, 5500.0)
        };

        using var dfEmps = DataFrame.From(emps);
        var empQuery = dfEmps.AsQueryable<EmpSalaryDto>();

        var query = from e in empQuery
                    select new
                    {
                        e.Name,
                        e.DeptId,
                        e.Salary,
                        // RANK() OVER (PARTITION BY DeptId ORDER BY Salary DESC)
                        DeptRank = LinqToDB.Sql.Ext.Rank()
                                         .Over()
                                         .PartitionBy(e.DeptId)
                                         .OrderByDesc(e.Salary)
                                         .ToValue(),
                                         
                        // SUM(Salary) OVER (PARTITION BY DeptId)
                        DeptTotalSalary = LinqToDB.Sql.Ext.Sum(e.Salary)
                                                .Over()
                                                .PartitionBy(e.DeptId)
                                                .ToValue()
                    };

        var results = query.ToList();

        // SELECT
        //         e."Name" AS "Name",
        //         e."DeptId" AS "DeptId",
        //         e."Salary" AS "Salary",
        //         RANK() OVER(PARTITION BY e."DeptId" ORDER BY e."Salary" DESC) AS "DeptRank",
        //         SUM(e."Salary") OVER(PARTITION BY e."DeptId") AS "DeptTotalSalary"
        // FROM
        //         "EmpSalaryDto" e

        //  shape: (5, 5)
        // ┌─────────┬────────┬────────┬──────────┬─────────────────┐
        // │ Name    ┆ DeptId ┆ Salary ┆ DeptRank ┆ DeptTotalSalary │
        // │ ---     ┆ ---    ┆ ---    ┆ ---      ┆ ---             │
        // │ str     ┆ i32    ┆ f64    ┆ u32      ┆ f64             │
        // ╞═════════╪════════╪════════╪══════════╪═════════════════╡
        // │ Alice   ┆ 1      ┆ 6000.0 ┆ 1        ┆ 10500.0         │
        // │ Bob     ┆ 2      ┆ 4000.0 ┆ 2        ┆ 9500.0          │
        // │ Charlie ┆ 1      ┆ 4500.0 ┆ 2        ┆ 10500.0         │
        // │ David   ┆ 3      ┆ 8000.0 ┆ 1        ┆ 8000.0          │
        // │ Eve     ┆ 2      ┆ 5500.0 ┆ 1        ┆ 9500.0          │
        // └─────────┴────────┴────────┴──────────┴─────────────────┘
        Assert.Equal(5, results.Count);

        var alice = results.First(r => r.Name == "Alice");
        Assert.Equal(1, alice.DeptRank); 
        Assert.Equal(10500.0, alice.DeptTotalSalary); 

        var charlie = results.First(r => r.Name == "Charlie");
        Assert.Equal(2, charlie.DeptRank); 
        Assert.Equal(10500.0, charlie.DeptTotalSalary); 

        var eve = results.First(r => r.Name == "Eve");
        Assert.Equal(1, eve.DeptRank);
        Assert.Equal(9500.0, eve.DeptTotalSalary);

        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(2, bob.DeptRank);
        Assert.Equal(9500.0, bob.DeptTotalSalary);

        var david = results.First(r => r.Name == "David");
        Assert.Equal(1, david.DeptRank);
        Assert.Equal(8000.0, david.DeptTotalSalary);
    }
    [Fact]
    [Trait("Linq", "CaseWhenAndCte")]
    public void Test_Polars_Linq_CaseWhen_And_Cte()
    {
        var emps = new[]
        {
            new EmpSalaryDto("Alice", 1, 6000.0),
            new EmpSalaryDto("Bob", 2, 4000.0),
            new EmpSalaryDto("Charlie", 1, 4500.0),
            new EmpSalaryDto("David", 3, 8000.0),
            new EmpSalaryDto("Eve", 2, 5500.0)
        };

        using var dfEmps = DataFrame.From(emps);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var empQuery = db.RegisterTable<EmpSalaryDto>(dfEmps);

        // ==========================================
        // CASE WHEN 
        // ==========================================
        var caseWhenQueryRaw = empQuery.Select(e => new
        {
            e.Name,
            SalaryTier = e.Salary >= 6000 ? "High" : (e.Salary >= 4500 ? "Medium" : "Low")
        });
        var caseWhenQuery = caseWhenQueryRaw.ToList();
        // SELECT
        //         e."Name" AS "Name",
        //         CASE
        //                 WHEN e."Salary" >= 6000 THEN 'High'
        //                 WHEN e."Salary" >= 4500 THEN 'Medium'
        //                 ELSE 'Low'
        //         END AS "SalaryTier"
        // FROM
        //         "EmpSalaryDto" e

        // shape: (5, 2)
        // ┌─────────┬────────────┐
        // │ Name    ┆ SalaryTier │
        // │ ---     ┆ ---        │
        // │ str     ┆ str        │
        // ╞═════════╪════════════╡
        // │ Alice   ┆ High       │
        // │ Bob     ┆ Low        │
        // │ Charlie ┆ Medium     │
        // │ David   ┆ High       │
        // │ Eve     ┆ Medium     │
        // └─────────┴────────────┘

        Assert.Equal(5, caseWhenQuery.Count);
        Assert.Equal("High", caseWhenQuery.First(e => e.Name == "Alice").SalaryTier);   // 6000
        Assert.Equal("High", caseWhenQuery.First(e => e.Name == "David").SalaryTier);   // 8000
        Assert.Equal("Medium", caseWhenQuery.First(e => e.Name == "Eve").SalaryTier);     // 5500
        Assert.Equal("Medium", caseWhenQuery.First(e => e.Name == "Charlie").SalaryTier); // 4500
        Assert.Equal("Low", caseWhenQuery.First(e => e.Name == "Bob").SalaryTier);        // 4000

        // ==========================================
        // CTE 
        // ==========================================
        
        var cte = empQuery.Where(e => e.Salary > 5000).AsCte("HighEarners");

        var cteQuery = from c in cte
                         where c.DeptId == 1 || c.DeptId == 3
                         select c;
        var cteResult = cteQuery.ToList();

        // WITH "HighEarners" ("DeptId", "Name", "Salary")
        // AS
        // (
        //         SELECT
        //                 e."DeptId",
        //                 e."Name",
        //                 e."Salary"
        //         FROM
        //                 "EmpSalaryDto" e
        //         WHERE
        //                 e."Salary" > 5000
        // )
        // SELECT
        //         c_1."Name",
        //         c_1."DeptId",
        //         c_1."Salary"
        // FROM
        //         "HighEarners" c_1
        // WHERE
        //         c_1."DeptId" = 1 OR c_1."DeptId" = 3

        // shape: (2, 3)
        // ┌───────┬────────┬────────┐
        // │ Name  ┆ DeptId ┆ Salary │
        // │ ---   ┆ ---    ┆ ---    │
        // │ str   ┆ i32    ┆ f64    │
        // ╞═══════╪════════╪════════╡
        // │ Alice ┆ 1      ┆ 6000.0 │
        // │ David ┆ 3      ┆ 8000.0 │
        // └───────┴────────┴────────┘
        Assert.Equal(2, cteResult.Count);
        Assert.Contains(cteResult, e => e.Name == "Alice");
        Assert.Contains(cteResult, e => e.Name == "David");
    }
    public record OrderDto(int OrderId, DateTime OrderDate, string Region, double Revenue);

    [Fact]
    [Trait("Linq", "TimeSeriesAndMultiGroup")]
    public void Test_Polars_Linq_Time_Series_And_MultiGroup()
    {
        var orders = new[]
        {
            new OrderDto(1, new DateTime(2023, 1, 15), "North", 100.0),
            new OrderDto(2, new DateTime(2023, 1, 20), "North", 150.0),
            new OrderDto(3, new DateTime(2023, 2, 10), "South", 200.0),
            new OrderDto(4, new DateTime(2024, 1, 5),  "North", 300.0),
            new OrderDto(5, new DateTime(2023, 2, 25), "South", 250.0)
        };

        using var dfOrders = DataFrame.From(orders);
        var orderQuery = dfOrders.AsQueryable<OrderDto>();

        // ==========================================
        // Multi-key GroupBy
        // ==========================================
        var multiGroupQuery = from o in orderQuery
                              group o by new { o.OrderDate.Year, o.Region } into g
                              orderby g.Key.Year, g.Key.Region
                              select new
                              {
                                  g.Key.Year,
                                  g.Key.Region,
                                  TotalRevenue = g.Sum(x => x.Revenue)
                              };

        var multiGroupResult = multiGroupQuery.ToList();

        // SELECT
        //         g_2."Year_1" AS "Year",
        //         g_2."Region" AS "Region",
        //         SUM(g_2."Revenue") AS "TotalRevenue"
        // FROM
        //         (
        //                 SELECT
        //                         Floor(Extract(year From g_1."OrderDate"))::Int as "Year_1",
        //                         g_1."Region",
        //                         g_1."Revenue"
        //                 FROM
        //                         "OrderDto" g_1
        //         ) g_2
        // GROUP BY
        //         g_2."Year_1", g_2."Region" ORDER BY
        //         g_2."Year_1",
        //         g_2."Region"

        // shape: (3, 3)
        // ┌──────┬────────┬──────────────┐
        // │ Year ┆ Region ┆ TotalRevenue │
        // │ ---  ┆ ---    ┆ ---          │
        // │ i32  ┆ str    ┆ f64          │
        // ╞══════╪════════╪══════════════╡
        // │ 2023 ┆ North  ┆ 250.0        │
        // │ 2023 ┆ South  ┆ 450.0        │
        // │ 2024 ┆ North  ┆ 300.0        │
        // └──────┴────────┴──────────────┘
        Assert.Equal(3, multiGroupResult.Count);
        
        Assert.Equal(2023, multiGroupResult[0].Year);
        Assert.Equal("North", multiGroupResult[0].Region);
        Assert.Equal(250.0, multiGroupResult[0].TotalRevenue);

        Assert.Equal(2023, multiGroupResult[1].Year);
        Assert.Equal("South", multiGroupResult[1].Region);
        Assert.Equal(450.0, multiGroupResult[1].TotalRevenue);

        // ==========================================
        // Date Functions
        // ==========================================
        var febOrdersQuery = orderQuery
            .Where(o => o.OrderDate.Year == 2023 && o.OrderDate.Month == 2);
        var febOrders = febOrdersQuery.ToList();
        // SELECT
        //         o."OrderId",
        //         o."OrderDate",
        //         o."Region",
        //         o."Revenue"
        // FROM
        //         "OrderDto" o
        // WHERE
        //         Floor(Extract(year From o."OrderDate"))::Int = 2023 AND
        //         Floor(Extract(month From o."OrderDate"))::Int = 2

        // shape: (2, 4)
        // ┌─────────┬─────────────────────┬────────┬─────────┐
        // │ OrderId ┆ OrderDate           ┆ Region ┆ Revenue │
        // │ ---     ┆ ---                 ┆ ---    ┆ ---     │
        // │ i32     ┆ datetime[μs]        ┆ str    ┆ f64     │
        // ╞═════════╪═════════════════════╪════════╪═════════╡
        // │ 3       ┆ 2023-02-10 00:00:00 ┆ South  ┆ 200.0   │
        // │ 5       ┆ 2023-02-25 00:00:00 ┆ South  ┆ 250.0   │
        // └─────────┴─────────────────────┴────────┴─────────┘

        Assert.Equal(2, febOrders.Count); // OrderId 3 和 5
        Assert.Contains(febOrders, o => o.OrderId == 3);
        Assert.Contains(febOrders, o => o.OrderId == 5);
    }
    public record NullableEmpDto(
        [property: Column(CanBeNull = true)] string? Name, 
        int DeptId, 
        double Salary
    );

    [Fact]
    [Trait("Linq", "SubqueryInAndFunctions")]
    public void Test_Polars_Linq_SubqueryIn_And_Functions()
    {
        var depts = new[]
        {
            new DeptDto { DeptId = 1, DeptName = "Engineering" },
            new DeptDto { DeptId = 2, DeptName = "Sales" },
            new DeptDto { DeptId = 3, DeptName = "HR" }
        };

        var emps = new[]
        {
            new NullableEmpDto("Alice", 1, 6000.0),
            new NullableEmpDto(null, 2, 4000.0), 
            new NullableEmpDto("Charlie", 1, 4500.0),
            new NullableEmpDto("David", 3, 8000.0)
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(Pl.Sql(),true);
        var deptQuery = dfDepts.AsQueryable<DeptDto>(db);
        var empQuery = db.RegisterTable<NullableEmpDto>(dfEmps);

        // ==========================================
        // IN Subquery
        // ==========================================
        
        var highPaidDeptIds = empQuery.Where(e => e.Salary > 5000).Select(e => e.DeptId);
        
        var richDeptsQuery = deptQuery.Where(d => highPaidDeptIds.Contains(d.DeptId));
        var richDepts = richDeptsQuery.ToList();
        // SELECT
        //         d."DeptId",
        //         d."DeptName"
        // FROM
        //         "DeptDto" d
        // WHERE
        //         d."DeptId" IN (
        //                 SELECT
        //                         e."DeptId"
        //                 FROM
        //                         "NullableEmpDto" e
        //                 WHERE
        //                         e."Salary" > 5000
        //         )

        // shape: (2, 2)
        // ┌────────┬─────────────┐
        // │ DeptId ┆ DeptName    │
        // │ ---    ┆ ---         │
        // │ i32    ┆ str         │
        // ╞════════╪═════════════╡
        // │ 1      ┆ Engineering │
        // │ 3      ┆ HR          │
        // └────────┴─────────────┘

        Assert.Equal(2, richDepts.Count); 
        Assert.Contains(richDepts, d => d.DeptName == "Engineering");
        Assert.Contains(richDepts, d => d.DeptName == "HR");

        // ==========================================
        // ?? -> COALESCE
        // ==========================================
        var coalesceQuery = empQuery.Select(e => new
        {
            SafeName = LinqToDB.Sql.AsSql(e.Name ?? "Unknown")
        });
        var coalesceResult = coalesceQuery.ToList();
        // SELECT
        //         Coalesce(e."Name", 'Unknown') AS "SafeName"
        // FROM
        //         "NullableEmpDto" e

        // shape: (4, 1)
        // ┌──────────┐
        // │ SafeName │
        // │ ---      │
        // │ str      │
        // ╞══════════╡
        // │ Alice    │
        // │ Unknown  │
        // │ Charlie  │
        // │ David    │
        // └──────────┘

        Assert.Equal(4, coalesceResult.Count);
        Assert.Contains(coalesceResult, e => e.SafeName == "Unknown"); 

        // ==========================================
        // Substring / Concat
        // ==========================================
        var stringQuery = empQuery
            .Where(e => e.Name != null)
            .Select(e => new
            {
                e.Name,
                ShortName = e.Name!.Substring(0, 3)
            });
        var stringResult = stringQuery.ToList();
        // SELECT
        //         e."Name" AS "Name",
        //         Substring(e."Name", 1, 3) AS "ShortName"
        // FROM
        //         "NullableEmpDto" e
        // WHERE
        //         e."Name" IS NOT NULL

        // shape: (3, 2)
        // ┌─────────┬───────────┐
        // │ Name    ┆ ShortName │
        // │ ---     ┆ ---       │
        // │ str     ┆ str       │
        // ╞═════════╪═══════════╡
        // │ Alice   ┆ Ali       │
        // │ Charlie ┆ Cha       │
        // │ David   ┆ Dav       │
        // └─────────┴───────────┘

        Assert.Equal(3, stringResult.Count);
        Assert.Equal("Ali", stringResult.First(e => e.Name == "Alice").ShortName);
        Assert.Equal("Cha", stringResult.First(e => e.Name == "Charlie").ShortName);
        // ==========================================
        // String Length
        // ==========================================
        var lengthQuery = empQuery
            .Where(e => e.Name != null && e.Name.Length > 5)
            .Select(e => e.Name);

        var lengthResult = lengthQuery.ToList();
        // SELECT
        //         e."Name"
        // FROM
        //         "NullableEmpDto" e
        // WHERE
        //         e."Name" IS NOT NULL AND Length(e."Name") > 5

        // shape: (1, 1)
        // ┌─────────┐
        // │ Name    │
        // │ ---     │
        // │ str     │
        // ╞═════════╡
        // │ Charlie │
        // └─────────┘

        Assert.Single(lengthResult);
        Assert.Equal("Charlie", lengthResult[0]); 

        // ==========================================
        // ToLower
        // ==========================================
        var caseQuery = empQuery
            .Where(e => e.Name == "Alice")
            .Select(e => e.Name!.ToLower());
        var caseResult = caseQuery.ToList();
        // SELECT
        //         Lower(e."Name") as c1
        // FROM
        //         "NullableEmpDto" e
        // WHERE
        //         e."Name" = 'Alice'

        // shape: (1, 1)
        // ┌───────┐
        // │ c1    │
        // │ ---   │
        // │ str   │
        // ╞═══════╡
        // │ alice │
        // └───────┘

        Assert.Equal("alice", caseResult[0]);

        // ==========================================
        // StartsWith / EndsWith / Contains
        // ==========================================
        var likeQuery = empQuery
            .Where(e => e.Name != null && (e.Name.StartsWith('D') || e.Name.Contains("lic")))
            .OrderBy(e => e.Name)
            .Select(e => e.Name);

        // SELECT
        //         e."Name"
        // FROM
        //         "NullableEmpDto" e
        // WHERE
        //         e."Name" IS NOT NULL AND (e."Name" LIKE 'D%' OR e."Name" LIKE '%lic%')
        // ORDER BY
        //         e."Name"

        // shape: (2, 1)
        // ┌───────┐
        // │ Name  │
        // │ ---   │
        // │ str   │
        // ╞═══════╡
        // │ Alice │
        // │ David │
        // └───────┘

        var likeResult = likeQuery.ToList();

        Assert.Equal(2, likeResult.Count);
        Assert.Equal("Alice", likeResult[0]);
        Assert.Equal("David", likeResult[1]);
    }
    public record SalesData(string Category, string ProductName, double Revenue, double Discount);

    [Fact]
    [Trait("Linq", "MathStringAndConditionalAgg")]
    public void Test_Polars_Linq_Math_String_And_ConditionalAgg()
    {
        var sales = new[]
        {
            new SalesData("Tech", "Laptop", 1000.5, 50.0),
            new SalesData("Tech", "Mouse", -20.0, 0.0), 
            new SalesData("Office", "Desk", 500.2, 10.0),
            new SalesData("Office", "Chair", 150.8, 5.0)
        };

        var salesQuery = DataFrame.From(sales).AsQueryable<SalesData>();

        // ==========================================
        // String CONCAT and Math
        // ==========================================
        var scalarQuery = salesQuery.Select(s => new
        {
            FullName = s.Category + " - " + s.ProductName,
            NetRevenue = Math.Round(Math.Abs(s.Revenue) - s.Discount, 2)
        });
        // SELECT
        //         s."Category" || ' - ' || s."ProductName" AS "FullName",
        //         Abs(s."Revenue") - s."Discount" AS "NetRevenue"
        // FROM
        //         "SalesData" s

        // shape: (4, 2)
        // ┌────────────────┬────────────┐
        // │ FullName       ┆ NetRevenue │
        // │ ---            ┆ ---        │
        // │ str            ┆ f64        │
        // ╞════════════════╪════════════╡
        // │ Tech - Laptop  ┆ 950.5      │
        // │ Tech - Mouse   ┆ 20.0       │
        // │ Office - Desk  ┆ 490.2      │
        // │ Office - Chair ┆ 145.8      │
        // └────────────────┴────────────┘
        var scalarResults = scalarQuery.ToList();

        Assert.Equal(4, scalarResults.Count);
        Assert.Contains(scalarResults, s => s.FullName == "Tech - Laptop" && s.NetRevenue == 950.5);
        Assert.Contains(scalarResults, s => s.FullName == "Tech - Mouse" && s.NetRevenue == 20.0);

        // ==========================================
        // Conditional Aggregation / Pivot
        // ==========================================
        var aggQuery = salesQuery
            .GroupBy(s => 1) 
            .Select(g => new
            {
                Total = g.Sum(x => Math.Abs(x.Revenue)),
                TechTotal = g.Sum(x => x.Category == "Tech" ? Math.Abs(x.Revenue) : 0),
                OfficeTotal = g.Sum(x => x.Category == "Office" ? Math.Abs(x.Revenue) : 0)
            });

        // SELECT
        //         SUM(Abs(g_1."Revenue")) AS "Total",
        //         SUM(CASE
        //                 WHEN g_1."Category" = 'Tech' THEN Abs(g_1."Revenue")
        //                 ELSE 0
        //         END) AS "TechTotal",
        //         SUM(CASE
        //                 WHEN g_1."Category" = 'Office' THEN Abs(g_1."Revenue")
        //                 ELSE 0
        //         END) AS "OfficeTotal"
        // FROM
        //         "SalesData" g_1
        // shape: (1, 3)
        // ┌────────┬───────────┬─────────────┐
        // │ Total  ┆ TechTotal ┆ OfficeTotal │
        // │ ---    ┆ ---       ┆ ---         │
        // │ f64    ┆ f64       ┆ f64         │
        // ╞════════╪═══════════╪═════════════╡
        // │ 1671.5 ┆ 1020.5    ┆ 651.0       │
        // └────────┴───────────┴─────────────┘

        var aggResult = aggQuery.ToList();

        Assert.Single(aggResult);
        Assert.Equal(1671.5, aggResult[0].Total); // 1000.5 + 20 + 500.2 + 150.8
        Assert.Equal(1020.5, aggResult[0].TechTotal);
        Assert.Equal(651.0, aggResult[0].OfficeTotal);
    }
    public record StockPrice(string Ticker, DateTime Date, double Price);

    [Fact]
    [Trait("Linq", "LeadLagAndNestedList")]
    public void Test_Polars_Linq_LeadLag_And_NestedList()
    {
        var stocks = new[]
        {
            new StockPrice("AAPL", new DateTime(2024, 1, 1), 150.0),
            new StockPrice("AAPL", new DateTime(2024, 1, 2), 155.0),
            new StockPrice("AAPL", new DateTime(2024, 1, 3), 152.0),
            new StockPrice("MSFT", new DateTime(2024, 1, 1), 300.0),
            new StockPrice("MSFT", new DateTime(2024, 1, 2), 305.0)
        };

        var depts = new[] { new DeptDto { DeptId = 1, DeptName = "Tech" }, new DeptDto { DeptId = 2, DeptName = "Sales" } };
        var emps = new[] { new EmpDto { Name = "Alice", DeptId = 1 }, new EmpDto { Name = "Bob", DeptId = 1 }, new EmpDto { Name = "Charlie", DeptId = 2 } };

        using var dfStocks = DataFrame.From(stocks);
        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var stockQuery = db.RegisterTable<StockPrice>(dfStocks);
        var deptQuery = db.RegisterTable<DeptDto>(dfDepts);
        var empQuery = db.RegisterTable<EmpDto>(dfEmps);

        // ==========================================
        // Window Lag 
        // ==========================================
        var lagQuery = from s in stockQuery
                       select new
                       {
                           s.Ticker,
                           s.Date,
                           s.Price,
                           PrevPrice = LinqToDB.Sql.Ext.Lag(s.Price)
                                              .Over()
                                              .PartitionBy(s.Ticker)
                                              .OrderBy(s.Date)
                                              .ToValue()
                       };

        var lagResult = lagQuery.ToList();

        Assert.Equal(5, lagResult.Count);
        // SELECT
        //         s."Ticker" AS "Ticker",
        //         s."Date" AS "Date",
        //         s."Price" AS "Price",
        //         LAG(s."Price") OVER(PARTITION BY s."Ticker" ORDER BY s."Date") AS "PrevPrice"
        // FROM
        //         "StockPrice" s
        // shape: (5, 4)
        // ┌────────┬─────────────────────┬───────┬───────────┐
        // │ Ticker ┆ Date                ┆ Price ┆ PrevPrice │
        // │ ---    ┆ ---                 ┆ ---   ┆ ---       │
        // │ str    ┆ datetime[μs]        ┆ f64   ┆ f64       │
        // ╞════════╪═════════════════════╪═══════╪═══════════╡
        // │ AAPL   ┆ 2024-01-01 00:00:00 ┆ 150.0 ┆ null      │
        // │ AAPL   ┆ 2024-01-02 00:00:00 ┆ 155.0 ┆ 150.0     │
        // │ AAPL   ┆ 2024-01-03 00:00:00 ┆ 152.0 ┆ 155.0     │
        // │ MSFT   ┆ 2024-01-01 00:00:00 ┆ 300.0 ┆ null      │
        // │ MSFT   ┆ 2024-01-02 00:00:00 ┆ 305.0 ┆ 300.0     │
        // └────────┴─────────────────────┴───────┴───────────┘

        var aaplDay2 = lagResult.First(s => s.Ticker == "AAPL" && s.Date.Day == 2);
        Assert.Equal(155.0, aaplDay2.Price);
        Assert.Equal(150.0, aaplDay2.PrevPrice); 

        // ==========================================
        // STRING_AGG
        // ==========================================
        var nestedQuery = from d in deptQuery
                          join e in empQuery on d.DeptId equals e.DeptId into empGroup
                          from e in empGroup.DefaultIfEmpty() 
                          group e by d.DeptName into g      
                          select new
                          {
                              DeptName = g.Key,
                              Employees = g.ListAgg(x => x.Name)
                          };
        // SELECT
        //         g_1."DeptName" AS "DeptName",
        //         ARRAY_TO_STRING(ARRAY_AGG(e."Name"), ', ') AS "Employees"
        // FROM
        //         "DeptDto" g_1
        //                 LEFT JOIN "EmpDto" e ON g_1."DeptId" = e."DeptId"
        // GROUP BY
        //         g_1."DeptName" 

        // shape: (2, 2)
        // ┌──────────┬────────────┐
        // │ DeptName ┆ Employees  │
        // │ ---      ┆ ---        │
        // │ str      ┆ str        │
        // ╞══════════╪════════════╡
        // │ Sales    ┆ Charlie    │
        // │ Tech     ┆ Alice, Bob │
        // └──────────┴────────────┘
        var nestedResult = nestedQuery.ToList();

        Assert.Equal(2, nestedResult.Count);
        
        var techDept = nestedResult.First(d => d.DeptName == "Tech");

        Assert.Contains("Alice", techDept.Employees);
        Assert.Contains("Bob", techDept.Employees);

        var salesDept = nestedResult.First(d => d.DeptName == "Sales");

        Assert.Equal("Charlie", salesDept.Employees);
    }
    [Fact]
    [Trait("Linq", "UnifiedCRUD")]
    public void Test_Polars_Linq_Unified_CRUD_UX()
    {
        var emps = new[]
        {
            new EmployeeSalary { Name = "Alice", DeptId = 1, Salary = 5000.0 },
            new EmployeeSalary { Name = "Bob",   DeptId = 2, Salary = 4000.0 },
            new EmployeeSalary { Name = "Eve",   DeptId = 3, Salary = 3000.0 }
        };

        using var dfEmps = DataFrame.From(emps);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        
        var table = db.RegisterTable<EmployeeSalary>(dfEmps);

        // ==========================================
        // SELECT
        // ==========================================
        var richEmps = table.Where(e => e.Salary >= 5000).ToList();
        Assert.Single(richEmps);
        Assert.Equal("Alice", richEmps[0].Name);

        // ==========================================
        // UPDATE (NOT SUPPORTED)
        // ==========================================
        try
        {
            table.Where(e => e.DeptId == 1)
                 .Set(e => e.Salary, e => e.Salary + 1000)
                 .Update();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Expected Update Error: {ex.Message}");
        }

        // ==========================================
        // Delete
        // ==========================================
        int deleted = table.Where(e => e.DeptId == 3).Delete();
        Assert.True(deleted >= 0);
    }
    public record StaffRecord(string name, int age, int salary);
    [Fact]
    [Trait("Linq", "LazyIO")]
    public void Test_Polars_Linq_Lazy_Csv_Scan_And_Pushdown()
    {
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_lazy_data.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            using var schema = PolarsSchema.From<StaffRecord>();

            using var lf = LazyFrame.ScanCsv(fileName,schema:schema);
            
            var results = lf.AsQueryable<StaffRecord>()
                          .Where(e => e.age > 30)
                          .Select(e => new 
                          { 
                              e.name, 
                              e.salary 
                          }).ToList();

            Assert.NotNull(results);
            Assert.Equal(2, results.Count); 

            Assert.Equal("Charlie", results[0].name);
            Assert.Equal(70000.0, results[0].salary);

            Assert.Equal("David", results[1].name);
            Assert.Equal(80000.0, results[1].salary);
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    [Fact]
    [Trait("Linq", "SeriesToList")]
    public void Test_Polars_Linq_Series_List()
    {
        using var series = Series.From("my_numbers", Enumerable.Range(1, 100).ToArray());
        
        var query = series.AsQueryable<int>().Where(x => x > 90)
                           .OrderByDescending(x => x)
                           .Take(5)
                           .Skip(1);
        // SELECT
        //         row_1.value as "Value_1"
        // FROM
        //         my_numbers row_1
        // WHERE
        //         row_1.value > 90
        // ORDER BY
        //         row_1.value DESC
        // LIMIT 4 OFFSET 1 

        var results = query.ToList();
        Assert.Equal(4, results.Count);
        Assert.Equal(99, results[0]);
    }
    [Fact]
    [Trait("Linq", "SeriesScalar")]
    public void Test_Polars_Linq_Series_Scalar()
    {
        using var series = Series.From("my_numbers", Enumerable.Range(1, 100).ToArray());
        
        var result = series.AsQueryable<int>().Where(x => x > 98)
                           .Sum();
        Assert.Equal(199, result);
    }
    [Fact]
    [Trait("Linq", "SeriesScalarAsync")]
    public async Task Test_Polars_Linq_Series_Scalar_Async() 
    {
        using var series = Series.From("my_numbers", Enumerable.Range(1, 100).ToArray());
        
        var result = await series.AsQueryable<int>()
                                 .Where(x => x > 98)
                                 .SumAsync()
                                 .ConfigureAwait(true);
                                 
        Assert.Equal(199, result);
    }
    [Fact]
    [Trait("Linq", "ToSeries")]
    public void Test_Polars_Linq_Series()
    {
        using var series = Series.From("my_numbers", Enumerable.Range(1, 100).ToArray());
        
        var result = series.AsQueryable<int>().Where(x => x > 90)
                           .OrderByDescending(x => x)
                           .Take(5)
                           .Skip(1)
                           .ToSeries("New Series");
        
        Assert.Equal("my_numbers",series.Name);
        Assert.Equal("New Series",result.Name);
        Assert.Equal(4,result.Length);
        Assert.Equal(100,series.Length);
    }
    
    [Table("employees")]
    public record StaffRecordWithBonus(
        [property: Column("name")] string name, 
        [property: Column("age")] int age, 
        [property: Column("salary")] int salary, 
        [property: Column("bonus")] double bonus);

    [Fact]
    [Trait("Linq", "HybridLazy")]
    public void Test_Polars_Linq_Hybrid_Native_And_Linq_Pushdown_With_Sugar()
    {
        var csvContent = @"
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_hybrid_lazy_data_sugar.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            using var schema = PolarsSchema.From<StaffRecord>();

            using var lf = LazyFrame.ScanCsv(fileName, schema: schema,hasHeader:false);
            
            using var lfWithBonus = lf.WithColumns((Pl.Col("salary") * 0.1).Alias("bonus"));

            var query = lfWithBonus.AsQueryable<StaffRecordWithBonus>()
                                   .Where(e => e.age > 30 && e.bonus >= 7000.0) 
                                   .Select(e => new 
                                   { 
                                       e.name, 
                                       TotalCompensation = e.salary + e.bonus 
                                   });
            // SELECT
            //         e.name AS "name",
            //         e.salary::Float + e.bonus AS "TotalCompensation"
            // FROM
            //         employees e
            // WHERE
            //         e.age > 30 AND e.bonus >= 7000
            // shape: (2, 2)
            // ┌─────────┬───────────────────┐
            // │ name    ┆ TotalCompensation │
            // │ ---     ┆ ---               │
            // │ str     ┆ f64               │
            // ╞═════════╪═══════════════════╡
            // │ Charlie ┆ 77000.0           │
            // │ David   ┆ 88000.0           │
            // └─────────┴───────────────────┘

            var results = query.ToList();

            Assert.NotNull(results);
            Assert.Equal(2, results.Count);

            Assert.Equal("Charlie", results[0].name);
            Assert.Equal(77000.0, results[0].TotalCompensation); 

            Assert.Equal("David", results[1].name);
            Assert.Equal(88000.0, results[1].TotalCompensation); 
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    [Fact]
    [Trait("Linq","Sandwich")]
    public void Test_Polars_Double_Hybrid_Sandwich()
    {
        using var schema = PolarsSchema.From<StaffRecord>();
        string path = "/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv";

        using var rawLf = LazyFrame.ScanCsv(path,schema:schema);
        
        var query = rawLf.AsQueryable<StaffRecord>()
                      .Where(e => e.salary > 5000)
                      .Select(e => new { e.name, e.salary });
        string plan1 = query.Explain(true);
        Console.WriteLine(plan1);
        // Csv SCAN [/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv]
        // PROJECT 2/3 COLUMNS
        // SELECTION: [(col("salary")) > (5000)]


        using LazyFrame lfWithLinq = query.ToLazyFrame();

        using var finalLf = lfWithLinq.WithColumns(Pl.Col("salary").Std().Alias("salary_std"));
        string plan2 = finalLf.Explain(true);
        Console.WriteLine(plan2);
        // WITH_COLUMNS:
        //  [col("salary").std().alias("salary_std")] 
        //   Csv SCAN [/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv]
        //   PROJECT 2/3 COLUMNS
        //   SELECTION: [(col("salary")) > (5000)]

        using var df = finalLf.Collect();

        df.Show();
        // shape: (4, 3)
        // ┌─────────┬────────┬──────────────┐
        // │ name    ┆ salary ┆ salary_std   │
        // │ ---     ┆ ---    ┆ ---          │
        // │ str     ┆ i32    ┆ f64          │
        // ╞═════════╪════════╪══════════════╡
        // │ Alice   ┆ 50000  ┆ 12909.944487 │
        // │ Bob     ┆ 60000  ┆ 12909.944487 │
        // │ Charlie ┆ 70000  ┆ 12909.944487 │
        // │ David   ┆ 80000  ┆ 12909.944487 │
        // └─────────┴────────┴──────────────┘
      
        Assert.True(df.Height > 0);
    }
    [Fact]
    [Trait("Linq", "SqlTranslator")]
    public void Test_PolarsSqlTranslator_Borrowing_Linq2db()
    {
        string snippet1 = PolarsExpr.ToSql<StaffRecord, int>(e => (int)Math.Pow(e.salary, 2));
        // Power(salary::Float, 2)
        Assert.Contains("Power(salary::Float, 2)", snippet1, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("t1.salary", snippet1); 

        // Power(salary::Float, 2) AS "salary_sq"
        string snippet2 = PolarsExpr.ToSql<StaffRecord, object>(e => new { salary_sq = Math.Pow(e.salary, 2) });
        Assert.Contains("salary_sq", snippet2, StringComparison.OrdinalIgnoreCase);
        // Power(salary::Float, 2) AS "salary_sq",salary * 2 AS "salary_dbl"
        string[] multiSnippets = PolarsExpr.ToSqls<StaffRecord, object>(e => new 
        { 
            salary_sq = Math.Pow(e.salary, 2),
            salary_dbl = e.salary * 2
        });

        Assert.Equal(2, multiSnippets.Length);
        Assert.Contains("AS \"salary_sq\"", multiSnippets[0], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AS \"salary_dbl\"", multiSnippets[1], StringComparison.OrdinalIgnoreCase);

        using var df = DataFrame.FromColumns(new
        {
            salary = new[] { 10, 20, 30 }
        });

        using var resultDf = df.Select(multiSnippets.Select(Pl.SqlExpr).ToArray());
        resultDf.Show();
        // shape: (3, 2)
        // ┌───────────┬────────────┐
        // │ salary_sq ┆ salary_dbl │
        // │ ---       ┆ ---        │
        // │ f64       ┆ i32        │
        // ╞═══════════╪════════════╡
        // │ 100.0     ┆ 20         │
        // │ 400.0     ┆ 40         │
        // │ 900.0     ┆ 60         │
        // └───────────┴────────────┘
        
        var sqArr = resultDf["salary_sq"].ToArray<double>();
        var dblArr = resultDf["salary_dbl"].ToArray<int>(); 
        
        Assert.Equal(3, sqArr.Length);
        
        Assert.Equal(100.0, sqArr[0]); 
        Assert.Equal(900.0, sqArr[2]); 

        Assert.Equal(20, dblArr[0]);
        Assert.Equal(60, dblArr[2]);
    }
    public record SalaryRecord
    {
        public double salary { get; set; }
    }

    [Fact]
    [Trait("Linq", "SyntaxSugar")]
    public void Test_Ultimate_StrongTyped_Select_Sugar()
    {
        using var df = DataFrame.FromColumns(new
        {
            salary = new[] { 10.0, 20.0, 30.0 }
        });

        using var resultDf = df.Select(Pl.SqlExprs(
            PolarsExpr.ToSqls<SalaryRecord, object>(e => new 
            { 
                salary_sq = Math.Pow(e.salary, 2), 
                salary_dbl = e.salary * 2,          
                is_high = e.salary > 15            
            })
        ));

        // shape: (3, 3)
        // ┌───────────┬────────────┬─────────┐
        // │ salary_sq ┆ salary_dbl ┆ is_high │
        // │ ---       ┆ ---        ┆ ---     │
        // │ f64       ┆ f64        ┆ bool    │
        // ╞═══════════╪════════════╪═════════╡
        // │ 100.0     ┆ 20.0       ┆ false   │
        // │ 400.0     ┆ 40.0       ┆ true    │
        // │ 900.0     ┆ 60.0       ┆ true    │
        // └───────────┴────────────┴─────────┘

        var sqArr = resultDf["salary_sq"].ToArray<double>();
        var dblArr = resultDf["salary_dbl"].ToArray<double>(); 
        var isHighArr = resultDf["is_high"].ToArray<bool>();

        Assert.Equal(3, sqArr.Length);

        Assert.Equal(100.0, sqArr[0]);
        Assert.Equal(20.0, dblArr[0]);
        Assert.False(isHighArr[0]); 

        Assert.Equal(900.0, sqArr[2]);
        Assert.Equal(60.0, dblArr[2]);
        Assert.True(isHighArr[2]);  
    }
    [Fact]
    [Trait("Linq", "Async")]
    public async Task Test_Polars_Linq_ToListAsync_Support()
    {
        var users = new[]
        {
            new SimpleUser { Id = 1, Name = "Alice" },
            new SimpleUser { Id = 2, Name = "Bob" },
            new SimpleUser { Id = 3, Name = "Charlie" }
        };

        using var df = DataFrame.From(users);

        var query = df.AsQueryable<SimpleUser>().Where(u => u.Id > 1).OrderByDescending(u => u.Id);
        
        var results = await query.AsAsyncEnumerable().ToListAsync();

        Assert.NotNull(results);
        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }
    public class TrafficRecord
    {
        public int Id { get; set; }
        public string Region { get; set; } = "";
        public double Latency { get; set; }
    }

    [Fact]
    [Trait("Linq", "AsyncStress")]
    public async Task Test_Polars_Linq_High_Concurrency_Async_Stress()
    {
        int recordCount = 100_000;
        var mockData = Enumerable.Range(0, recordCount).Select(i => new TrafficRecord
        {
            Id = i,
            Region = $"Region_{i % 50}",
            Latency = Random.Shared.NextDouble() * 100.0
        }).ToArray();

        using var df = DataFrame.From(mockData);

        async Task<int> SimulateWebRequestAsync(int workerId)
        {
            using var db = new PolarsDataContext(Pl.Sql(),true);
            
            var table = db.RegisterTable<TrafficRecord>(df);

            string targetRegion = $"Region_{workerId % 50}";

            var query = table.Where(t => t.Region == targetRegion && t.Latency > 10.0)
                             .OrderBy(t => t.Id);

            var results = await query.AsAsyncEnumerable().ToListAsync();
            
            return results.Count;
        }

        int concurrencyLevel = 100;
        var tasks = new List<Task<int>>();

        for (int i = 0; i < concurrencyLevel; i++)
        {
            tasks.Add(SimulateWebRequestAsync(i));
        }

        var finalResults = await Task.WhenAll(tasks);

        Assert.Equal(concurrencyLevel, finalResults.Length);

        foreach (var count in finalResults)
        {

            Assert.True(count > 0 && count <= 2000);
        }
        
        Console.WriteLine($"[Polars.NET] Finished {concurrencyLevel} Concurrnet LINQ Queries");
    }
    [Fact]
    [Trait("Linq", "AsyncStressToDataFrame")]
    public async Task Test_Polars_Linq_High_Concurrency_ToDataFrameAsync_Stress()
    {

        int recordCount = 100_000;
        var mockData = Enumerable.Range(0, recordCount).Select(i => new
        {
            Id = i,
            Region = $"Region_{i % 50}", 
            Latency = Random.Shared.NextDouble() * 100.0
        }).ToArray();

        using var df = DataFrame.From(mockData);


        async Task<long> SimulateDataFrameQueryAsync(int workerId)
        {
            using var ctx = new SqlContext();
            using var db = new PolarsDataContext(ctx);
            
            var table = db.RegisterTable(df, mockData);

            string targetRegion = $"Region_{workerId % 50}";

            var query = table.Where(t => t.Region == targetRegion && t.Latency > 10.0)
                             .OrderBy(t => t.Id);

            using DataFrame resultDf = await query.ToDataFrameAsync();

            return resultDf.Height; 
        }

        int concurrencyLevel = 1000;
        var tasks = new List<Task<long>>();

        for (int i = 0; i < concurrencyLevel; i++)
        {
            tasks.Add(SimulateDataFrameQueryAsync(i));
        }

        var finalHeights = await Task.WhenAll(tasks);

        Assert.Equal(concurrencyLevel, finalHeights.Length);

        foreach (var height in finalHeights)
        {
            Assert.True(height > 0 && height <= 2000);
        }
        
        Console.WriteLine($"[Polars.NET] ToDataFrameAsync Finished {concurrencyLevel} Concurrent Queries");
    }
    public class SalesRecord
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
        public double Revenue { get; set; }
        public DateTime SaleDate { get; set; }
    }
    [Fact]
    [Trait("Linq", "MathDate")]
    public void Test_Polars_Linq_Math_Date_And_Aggregations()
    {
        var sales = new[]
        {
            new SalesRecord { Id = 1, Category = "Tech", Revenue = -150.5, SaleDate = new DateTime(2023, 1, 15) },
            new SalesRecord { Id = 2, Category = "Tech", Revenue = 200.2, SaleDate = new DateTime(2023, 5, 20) },
            new SalesRecord { Id = 3, Category = "Food", Revenue = 99.9,  SaleDate = new DateTime(2024, 2, 10) }
        };

        using var dfSales = DataFrame.From(sales);
        var table = dfSales.AsQueryable<SalesRecord>();

        // ==========================================
        // Math
        // ==========================================
        var mathQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                Absolute = Math.Abs(x.Revenue),
                Rounded = Math.Round(x.Revenue),
                Ceiled = Math.Ceiling(x.Revenue) 
            });

        var mathResult = mathQuery.ToList();

        Assert.Equal(3, mathResult.Count);
        Assert.Equal(150.5, mathResult[0].Absolute);
        Assert.Equal(-150.0, mathResult[0].Rounded); 
        Assert.Equal(201.0, mathResult[1].Ceiled);   

        // ==========================================
        // Date
        // ==========================================
        var dateQuery = table
            .Where(x => x.SaleDate.Year == 2023)
            .Select(x => x.Id);
        var dateResult = dateQuery.ToList();
        // SELECT
        //         x."Id"
        // FROM
        //         "SalesRecord" x
        // WHERE
        //         Floor(Extract(year From x."SaleDate"))::Int = 2023
        // shape: (2, 1)
        // ┌─────┐
        // │ Id  │
        // │ --- │
        // │ i32 │
        // ╞═════╡
        // │ 1   │
        // │ 2   │
        // └─────┘

        Assert.Equal(2, dateResult.Count); 
        Assert.Contains(1, dateResult);
    }
    public class WindowStatsRecord
    {
        public int DeptId { get; set; }
        public string EmpName { get; set; } = "";
        public double Salary { get; set; }
    }

    [Fact]
    [Trait("Linq", "WindowAndStats")]
    public void Test_Polars_Linq_Window_And_Stats()
    {
        var data = new[]
        {
            new WindowStatsRecord { DeptId = 1, EmpName = "Alice",   Salary = 5000 },
            new WindowStatsRecord { DeptId = 1, EmpName = "Bob",     Salary = 6000 },
            new WindowStatsRecord { DeptId = 1, EmpName = "Charlie", Salary = 7000 },
            new WindowStatsRecord { DeptId = 2, EmpName = "Dave",    Salary = 4000 },
            new WindowStatsRecord { DeptId = 2, EmpName = "Eve",     Salary = 4000 }, 
            new WindowStatsRecord { DeptId = 2, EmpName = "Frank",   Salary = 8000 }
        };

        using var dfData = DataFrame.From(data);
        var table = dfData.AsQueryable<WindowStatsRecord>();

        // ==========================================
        // Stats
        // ==========================================
        var statsQuery = table
            .GroupBy(x => x.DeptId)
            .Select(g => new
            {
                DeptId = g.Key,
                MedianSalary = g.Median(x => x.Salary),
                StdDevSalary = g.StdDev(x => x.Salary)
            })
            .OrderBy(x => x.DeptId);
        
        var statsResult = statsQuery.ToList();

        Assert.Equal(2, statsResult.Count);
        
        Assert.Equal(1, statsResult[0].DeptId);
        Assert.Equal(6000.0, statsResult[0].MedianSalary);
        Assert.True(statsResult[0].StdDevSalary > 0); 

        // ==========================================
        // Complex Window Functions
        // ==========================================
        var windowQuery = table
            .Select(x => new
            {
                x.DeptId,
                x.EmpName,
                x.Salary,
                RowNum = LinqToDB.Sql.Ext.RowNumber().Over().PartitionBy(x.DeptId).OrderByDesc(x.Salary).ToValue(),
                Rank = LinqToDB.Sql.Ext.Rank().Over().PartitionBy(x.DeptId).OrderByDesc(x.Salary).ToValue()
            })
            .OrderBy(x => x.DeptId)
            .ThenByDescending(x => x.Salary)
            .ThenBy(x => x.EmpName);
        var windowResult = windowQuery.ToList();
        // SELECT
        //         x."DeptId" AS "DeptId",
        //         x."EmpName" AS "EmpName",
        //         x."Salary" AS "Salary",
        //         ROW_NUMBER() OVER(PARTITION BY x."DeptId" ORDER BY x."Salary" DESC) AS "RowNum",
        //         RANK() OVER(PARTITION BY x."DeptId" ORDER BY x."Salary" DESC) AS "Rank"
        // FROM
        //         "WindowStatsRecord" x
        // ORDER BY
        //         x."DeptId",
        //         x."Salary" DESC,
        //         x."EmpName"
        // shape: (6, 5)
        // ┌────────┬─────────┬────────┬────────┬──────┐
        // │ DeptId ┆ EmpName ┆ Salary ┆ RowNum ┆ Rank │
        // │ ---    ┆ ---     ┆ ---    ┆ ---    ┆ ---  │
        // │ i32    ┆ str     ┆ f64    ┆ u32    ┆ u32  │
        // ╞════════╪═════════╪════════╪════════╪══════╡
        // │ 1      ┆ Charlie ┆ 7000.0 ┆ 1      ┆ 1    │
        // │ 1      ┆ Bob     ┆ 6000.0 ┆ 2      ┆ 2    │
        // │ 1      ┆ Alice   ┆ 5000.0 ┆ 3      ┆ 3    │
        // │ 2      ┆ Frank   ┆ 8000.0 ┆ 1      ┆ 1    │
        // │ 2      ┆ Dave    ┆ 4000.0 ┆ 2      ┆ 2    │
        // │ 2      ┆ Eve     ┆ 4000.0 ┆ 3      ┆ 2    │
        // └────────┴─────────┴────────┴────────┴──────┘
        Assert.Equal(6, windowResult.Count);

        var charlie = windowResult.First(x => x.EmpName == "Charlie");
        Assert.Equal(1, charlie.RowNum);
        Assert.Equal(1, charlie.Rank);

        var frank = windowResult.First(x => x.EmpName == "Frank");
        Assert.Equal(1, frank.Rank); 

        var dave = windowResult.First(x => x.EmpName == "Dave");
        var eve = windowResult.First(x => x.EmpName == "Eve");
        
        Assert.Equal(2, dave.Rank);
        Assert.Equal(2, eve.Rank);
        
        Assert.True(dave.RowNum != eve.RowNum);
        Assert.True(dave.RowNum == 2 || dave.RowNum == 3);
    }
    public class StatRecord
    {
        public int GroupId { get; set; }
        public double Value { get; set; }
    }

    [Fact]
    [Trait("Linq", "DataScienceStats")]
    public void Test_Polars_Linq_Quantiles_And_Variance()
    {
        var data = new[]
        {
            new StatRecord { GroupId = 1, Value = 10.0 },
            new StatRecord { GroupId = 1, Value = 20.0 },
            new StatRecord { GroupId = 1, Value = 30.0 },
            new StatRecord { GroupId = 1, Value = 40.0 }
        };

        using var dfData = DataFrame.From(data);
        var table = dfData.AsQueryable<StatRecord>();

        // ==========================================
        // Variance, Quantile
        // ==========================================
        var statsQuery = table
            .GroupBy(x => x.GroupId)
            .Select(g => new
            {
                GroupId = g.Key,
                Var = PolarsSql.Variance(g, x => x.Value),
                Q50_Cont = PolarsSql.QuantileCont(g, x => x.Value, 0.5),
                Q50_Disc = PolarsSql.QuantileDisc(g, x => x.Value, 0.5),
                Q99_Cont = PolarsSql.QuantileCont(g, x => x.Value, 0.99)
            });
        var statsResult = statsQuery.ToList();
        // SELECT
        //         g_1."GroupId" AS "GroupId",
        //         VARIANCE(g_1."Value") AS "Var",
        //         QUANTILE_CONT(g_1."Value", 0.5) AS "Q50_Cont",
        //         QUANTILE_DISC(g_1."Value", 0.5) AS "Q50_Disc",
        //         QUANTILE_CONT(g_1."Value", 0.98999999999999999) AS "Q99_Cont"
        // FROM
        //         tmp_f7a9ea08e6bb4f2e9dfa946f06abcc68 g_1
        // GROUP BY
        //         g_1."GroupId" 
        // shape: (1, 5)
        // ┌─────────┬────────────┬──────────┬──────────┬──────────┐
        // │ GroupId ┆ Var        ┆ Q50_Cont ┆ Q50_Disc ┆ Q99_Cont │
        // │ ---     ┆ ---        ┆ ---      ┆ ---      ┆ ---      │
        // │ i32     ┆ f64        ┆ f64      ┆ f64      ┆ f64      │
        // ╞═════════╪════════════╪══════════╪══════════╪══════════╡
        // │ 1       ┆ 166.666667 ┆ 25.0     ┆ 20.0     ┆ 39.7     │
        // └─────────┴────────────┴──────────┴──────────┴──────────┘
        Assert.Single(statsResult);
        var result = statsResult[0];

        // (ddof=1) = ((15^2) + (5^2) + (5^2) + (15^2)) / 3 = (225+25+25+225)/3 = 500/3 ≈ 166.666...
        Assert.Equal(166.6666, result.Var, precision: 3);

        // (20 + 30) / 2 = 25.0
        Assert.Equal(25.0, result.Q50_Cont);

        Assert.True(result.Q50_Disc == 20.0 || result.Q50_Disc == 30.0, $"Actual Q50_Disc was {result.Q50_Disc}");
        Assert.NotEqual(25.0, result.Q50_Disc); 

        Assert.True(result.Q99_Cont > 39.0);
    }
    public class BitwiseRecord
    {
        public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    [Fact]
    [Trait("Linq", "Bitwise")]
    public void Test_Polars_Linq_Native_Bitwise_Operators()
    {

        // 5 : 0101
        // 3 : 0011
        // 12 : 1100
        // 10 : 1010
        var data = new[]
        {
            new BitwiseRecord { Id = 1, A = 5,  B = 3 },
            new BitwiseRecord { Id = 2, A = 12, B = 10 }
        };

        var firstQuery = DataFrame.From(data).AsQueryable<BitwiseRecord>()
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                // Bitwise AND
                AndResult = x.A & x.B, 
                // Bitwise OR
                OrResult  = x.A | x.B, 
                // Bitwise XOR
                XorResult  = PolarsSql.BitXor(x.A, x.B),
                // Bitwise NOT
                NotResult = ~x.A,       
                // Bitwise COUNT
                CountResult = PolarsSql.BitCount(x.A)
            });

        var bitQuery = firstQuery.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         x."A" & x."B" AS "AndResult",
        //         x."A" | x."B" AS "OrResult",
        //         BIT_XOR(x."A", x."B") AS "XorResult",
        //         BIT_NOT(x."A") AS "NotResult",
        //         BIT_COUNT(x."A") AS "CountResult"
        // FROM
        //         "BitwiseRecord" x
        // ORDER BY
        //         x."Id"
        // shape: (2, 6)
        // ┌─────┬───────────┬──────────┬───────────┬───────────┬─────────────┐
        // │ Id  ┆ AndResult ┆ OrResult ┆ XorResult ┆ NotResult ┆ CountResult │
        // │ --- ┆ ---       ┆ ---      ┆ ---       ┆ ---       ┆ ---         │
        // │ i32 ┆ i32       ┆ i32      ┆ i32       ┆ i32       ┆ u32         │
        // ╞═════╪═══════════╪══════════╪═══════════╪═══════════╪═════════════╡
        // │ 1   ┆ 1         ┆ 7        ┆ 6         ┆ -6        ┆ 2           │
        // │ 2   ┆ 8         ┆ 14       ┆ 6         ┆ -13       ┆ 2           │
        // └─────┴───────────┴──────────┴───────────┴───────────┴─────────────┘
        Assert.Equal(2, bitQuery.Count);

        // Verify 5 and 3

        // 5 & 3 = 1 (0001)
        // 5 | 3 = 7 (0111)
        // 5 ^ 3 = 6 (0110)
        // ~5    = -6
        // Bitcount 5 = 2
        var row1 = bitQuery[0];
        Assert.Equal(1, row1.AndResult);
        Assert.Equal(7, row1.OrResult);
        Assert.Equal(6, row1.XorResult);
        Assert.Equal(~5, row1.NotResult); 
        Assert.Equal(2,row1.CountResult);

        // Verify 12 and 10

        // 12 & 10 = 8 (1000)
        // 12 | 10 = 14 (1110)
        // 12 ^ 10 = 6 (0110)
        // ~12     = -13
        // Bitcount 12 = 2
        var row2 = bitQuery[1];
        Assert.Equal(8, row2.AndResult);
        Assert.Equal(14, row2.OrResult);
        Assert.Equal(6, row2.XorResult);
        Assert.Equal(~12, row2.NotResult);
        Assert.Equal(2,row2.CountResult);
    }
    public class TemporalRecord
    {
        public int Id { get; set; }
        public DateTime EventTime { get; set; }
    }

    [Fact]
    [Trait("Linq", "Temporal")]
    public void Test_Polars_Linq_Native_Temporal_Functions()
    {
        var data = new[]
        {
            new TemporalRecord { Id = 1, EventTime = new DateTime(2024, 3, 15, 14, 30, 0) },
            new TemporalRecord { Id = 2, EventTime = new DateTime(2025, 12, 1, 9, 15, 45) }
        };

        using var df = DataFrame.From(data);
        var table = df.AsQueryable<TemporalRecord>();

        var timeQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                x.EventTime.Year,
                x.EventTime.Month,
                x.EventTime.Day,
                x.EventTime.Hour,
                
                FormattedDate = x.EventTime.ToPolarsString("%Y-%m-%d")
            });
        var timeResult = timeQuery.ToList();
        // SELECT
        //         x."Id",
        //         Floor(Extract(year From x."EventTime"))::Int as "Year_1",
        //         Floor(Extract(month From x."EventTime"))::Int as "Month_1",
        //         Floor(Extract(day From x."EventTime"))::Int as "Day_1",
        //         Floor(Extract(hour From x."EventTime"))::Int as "Hour_1",
        //         strftime(x."EventTime", '%Y-%m-%d') as "FormattedDate"
        // FROM
        //         "TemporalRecord" x
        // ORDER BY
        //         x."Id"
        // shape: (2, 6)
        // ┌─────┬────────┬─────────┬───────┬────────┬───────────────┐
        // │ Id  ┆ Year_1 ┆ Month_1 ┆ Day_1 ┆ Hour_1 ┆ FormattedDate │
        // │ --- ┆ ---    ┆ ---     ┆ ---   ┆ ---    ┆ ---           │
        // │ i32 ┆ i32    ┆ i32     ┆ i32   ┆ i32    ┆ str           │
        // ╞═════╪════════╪═════════╪═══════╪════════╪═══════════════╡
        // │ 1   ┆ 2024   ┆ 3       ┆ 15    ┆ 14     ┆ 2024-03-15    │
        // │ 2   ┆ 2025   ┆ 12      ┆ 1     ┆ 9      ┆ 2025-12-01    │
        // └─────┴────────┴─────────┴───────┴────────┴───────────────┘

        Assert.Equal(2, timeResult.Count);
        
        Assert.Equal(2024, timeResult[0].Year);
        Assert.Equal(3, timeResult[0].Month);
        Assert.Equal(15, timeResult[0].Day);
        Assert.Equal(14, timeResult[0].Hour);
        Assert.StartsWith("2024", timeResult[0].FormattedDate);
    }
    public class StringNativeRecord
    {
        public int Id { get; set; }
        public string Text1 { get; set; } = "";
        public string Text2 { get; set; } = "";
        public string TimeStr {get; set;} ="";
    }

    [Fact]
    [Trait("Linq", "String")]
    public void Test_Polars_Linq_Native_String_Functions()
    {
        var data = new[]
        {
            new StringNativeRecord { Id = 1, Text1 = "  Hello  ", Text2 = "World", TimeStr = "2024-03-15 14:30:00" },
            new StringNativeRecord { Id = 2, Text1 = "Polars",    Text2 = "Data",  TimeStr = "2025-12-01 09:15:45" }
        };

        using var df = DataFrame.From(data);
        var table = df.AsQueryable<StringNativeRecord>();

        var strQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                ConcatStr = x.Text1 + " " + x.Text2,
                
                LTrimStr = x.Text1.TrimStart(),
                RTrimStr = x.Text1.TrimEnd(),
                
                ReplaceStr = x.Text2.Replace("r", "x"),
                
                PosStr = x.Text2.IndexOf('o') ,
                ParsedTime = x.TimeStr.ParsePolarsDate("%Y-%m-%d %H:%M:%S")

            });
        
        var strResult =strQuery.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         x."Text1" || ' ' || x."Text2" AS "ConcatStr",
        //         LTRIM(x."Text1") AS "LTrimStr",
        //         RTRIM(x."Text1") AS "RTrimStr",
        //         Replace(x."Text2", 'r', 'x') AS "ReplaceStr",
        //         CAST(STRPOS(x."Text2", 'o') AS INT) - 1 AS "PosStr",
        //         STRPTIME(x."TimeStr", '%Y-%m-%d %H:%M:%S') AS "ParsedTime"
        // FROM
        //         "StringNativeRecord" x
        // ORDER BY
        //         x."Id"
        // shape: (2, 7)
        // ┌─────┬─────────────────┬──────────┬──────────┬────────────┬────────┬─────────────────────┐
        // │ Id  ┆ ConcatStr       ┆ LTrimStr ┆ RTrimStr ┆ ReplaceStr ┆ PosStr ┆ ParsedTime          │
        // │ --- ┆ ---             ┆ ---      ┆ ---      ┆ ---        ┆ ---    ┆ ---                 │
        // │ i32 ┆ str             ┆ str      ┆ str      ┆ str        ┆ i32    ┆ datetime[μs]        │
        // ╞═════╪═════════════════╪══════════╪══════════╪════════════╪════════╪═════════════════════╡
        // │ 1   ┆   Hello   World ┆ Hello    ┆   Hello  ┆ Woxld      ┆ 1      ┆ 2024-03-15 14:30:00 │
        // │ 2   ┆ Polars Data     ┆ Polars   ┆ Polars   ┆ Data       ┆ -1     ┆ 2025-12-01 09:15:45 │
        // └─────┴─────────────────┴──────────┴──────────┴────────────┴────────┴─────────────────────┘
        Assert.Equal(2, strResult.Count);
        
        var row1 = strResult[0];
        Assert.Equal("  Hello   World", row1.ConcatStr);
        Assert.Equal("Hello  ", row1.LTrimStr);
        Assert.Equal("  Hello", row1.RTrimStr);
        Assert.Equal("Woxld", row1.ReplaceStr); 
        Assert.Equal(1, row1.PosStr); 
        Assert.Equal(new DateTime(2024, 3, 15, 14, 30, 0), row1.ParsedTime);

        var row2 = strResult[1];
        Assert.Equal("Polars Data", row2.ConcatStr);
        Assert.Equal("Polars", row2.LTrimStr);
        Assert.Equal("Polars", row2.RTrimStr);
        Assert.Equal("Data", row2.ReplaceStr); 
        Assert.Equal(-1, row2.PosStr); 
        Assert.Equal(new DateTime(2025, 12, 1, 9, 15, 45), row2.ParsedTime);
    }
    [Fact]
    [Trait("Linq", "ControlFlow")]
    public void Test_Polars_Linq_Native_Control_Flow_Functions()
    {       
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        var mockData = new[] 
        { 
            new { Id = 1, Val1 = (int?)10, Val2 = (int?)20 },
            new { Id = 2, Val1 = (int?)null, Val2 = (int?)30 },
            new { Id = 3, Val1 = (int?)40, Val2 = (int?)40 }
        };
        
        var table = db.RegisterTable(DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                
                CoalesceVal = LinqToDB.Sql.AsSql(x.Val1 ?? x.Val2 ?? 0),
                
                MaxVal = LinqToDB.Sql.AsSql(Math.Max(x.Val1 ?? 0, x.Val2 ?? 0)),
                
                MinVal = LinqToDB.Sql.AsSql(Math.Min(x.Val1 ?? 0, x.Val2 ?? 0)),

                IfStr = x.Val1 > 15 ? "Big" : "Small",
                
                NullIfVal = x.Val1 == x.Val2 ? null : x.Val1
            });

        var result = query.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         Coalesce(x."Val1", x."Val2", 0) AS "CoalesceVal",
        //         CASE
        //                 WHEN Coalesce(x."Val1", 0) >= Coalesce(x."Val2", 0) THEN Coalesce(x."Val1", 0)
        //                 ELSE Coalesce(x."Val2", 0)
        //         END AS "MaxVal",
        //         CASE
        //                 WHEN Coalesce(x."Val1", 0) <= Coalesce(x."Val2", 0) THEN Coalesce(x."Val1", 0)
        //                 ELSE Coalesce(x."Val2", 0)
        //         END AS "MinVal",
        //         CASE
        //                 WHEN x."Val1" > 15 THEN 'Big'
        //                 ELSE 'Small'
        //         END AS "IfStr",
        //         NULLIF(x."Val1", x."Val2") AS "NullIfVal"
        // FROM
        //         tmp_88d373fc60994f3daf6aad6456b65d30 x
        // ORDER BY
        //         x."Id"
        // shape: (3, 6)
        // ┌─────┬─────────────┬────────┬────────┬───────┬───────────┐
        // │ Id  ┆ CoalesceVal ┆ MaxVal ┆ MinVal ┆ IfStr ┆ NullIfVal │
        // │ --- ┆ ---         ┆ ---    ┆ ---    ┆ ---   ┆ ---       │
        // │ i32 ┆ i32         ┆ i32    ┆ i32    ┆ str   ┆ i32       │
        // ╞═════╪═════════════╪════════╪════════╪═══════╪═══════════╡
        // │ 1   ┆ 10          ┆ 20     ┆ 10     ┆ Small ┆ 10        │
        // │ 2   ┆ 30          ┆ 30     ┆ 0      ┆ Small ┆ null      │
        // │ 3   ┆ 40          ┆ 40     ┆ 40     ┆ Big   ┆ null      │
        // └─────┴─────────────┴────────┴────────┴───────┴───────────┘

        Assert.Equal(3, result.Count);
        
        Assert.Equal(30, result[1].CoalesceVal); // null ?? 30 = 30
        Assert.Equal(20, result[0].MaxVal);      // Max(10, 20) = 20
        Assert.Null(result[2].NullIfVal);        // 40 == 40 -> null        
    }
    [Fact]
    [Trait("Linq", "MathTrig")]
    public void Test_Polars_Linq_Native_Math_Trig_Functions()
    {
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        var mockData = new[] 
        { 
            new { Id = 1, V1 = 0.5, V2 = 1.0 },
            new { Id = 2, V1 = 0.0, V2 = -0.5 },
            new { Id = 3, V1 = -1.0, V2 = 0.5 }
        };
        
        var table = db.RegisterTable(DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                SinVal = Math.Sin(x.V1),
                CosVal = Math.Cos(x.V1),
                TanVal = Math.Tan(x.V1),
                
                AsinVal = Math.Asin(x.V1),
                AcosVal = Math.Acos(x.V1),
                AtanVal = Math.Atan(x.V1),
                

                Atan2Val = Math.Atan2(x.V1, x.V2)
            });
        var result = query.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         Sin(x."V1") AS "SinVal",
        //         Cos(x."V1") AS "CosVal",
        //         Tan(x."V1") AS "TanVal",
        //         Asin(x."V1") AS "AsinVal",
        //         Acos(x."V1") AS "AcosVal",
        //         Atan(x."V1") AS "AtanVal",
        //         Atan2(x."V1", x."V2") AS "Atan2Val"
        // FROM
        //         tmp_d1ae0ee866264e84954a7f8bcff04e2e x
        // ORDER BY
        //         x."Id"
        // shape: (3, 8)
        // ┌─────┬───────────┬──────────┬───────────┬───────────┬──────────┬───────────┬───────────┐
        // │ Id  ┆ SinVal    ┆ CosVal   ┆ TanVal    ┆ AsinVal   ┆ AcosVal  ┆ AtanVal   ┆ Atan2Val  │
        // │ --- ┆ ---       ┆ ---      ┆ ---       ┆ ---       ┆ ---      ┆ ---       ┆ ---       │
        // │ i32 ┆ f64       ┆ f64      ┆ f64       ┆ f64       ┆ f64      ┆ f64       ┆ f64       │
        // ╞═════╪═══════════╪══════════╪═══════════╪═══════════╪══════════╪═══════════╪═══════════╡
        // │ 1   ┆ 0.479426  ┆ 0.877583 ┆ 0.546302  ┆ 0.523599  ┆ 1.047198 ┆ 0.463648  ┆ 0.463648  │
        // │ 2   ┆ 0.0       ┆ 1.0      ┆ 0.0       ┆ 0.0       ┆ 1.570796 ┆ 0.0       ┆ 3.141593  │
        // │ 3   ┆ -0.841471 ┆ 0.540302 ┆ -1.557408 ┆ -1.570796 ┆ 3.141593 ┆ -0.785398 ┆ -1.107149 │
        // └─────┴───────────┴──────────┴───────────┴───────────┴──────────┴───────────┴───────────┘

        Assert.Equal(3, result.Count);
        
        Assert.True(Math.Abs(result[0].SinVal - Math.Sin(0.5)) < 1e-6);

    }
    [Fact]
    [Trait("Linq", "MathTrigExtension")]
    public void Test_Polars_Linq_Specific_Math_Functions()
    {
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        // 构造测试数据
        // Ratio 用于反三角函数输入 (-1 到 1)
        // Deg 用于基于角度的三角函数输入 (如 30度, 90度)
        // Rad 用于弧度和余切输入
        var mockData = new[] 
        { 
            new { Id = 1, Ratio = 0.5, Deg = 30.0, Rad = 0.523, Y = 1.0, X = 1.0 },
            new { Id = 2, Ratio = 1.0, Deg = 90.0, Rad = 1.570, Y = -1.0, X = 0.0 }
        };
        
        var table = db.RegisterTable(DataFrame.From(mockData),mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                
                DegVal = PolarsSql.Degrees(x.Rad),
                RadVal = PolarsSql.Radians(x.Deg),
                
                SindVal = PolarsSql.Sind(x.Deg),
                CosdVal = PolarsSql.Cosd(x.Deg),
                TandVal = PolarsSql.Tand(x.Deg),
                
                CotVal = PolarsSql.Cot(x.Rad),
                CotdVal = PolarsSql.Cotd(x.Deg),
                
                AsindVal = PolarsSql.Asind(x.Ratio), 
                AcosdVal = PolarsSql.Acosd(x.Ratio),
                AtandVal = PolarsSql.Atand(x.Ratio),
                
                Atan2dVal = PolarsSql.Atan2d(x.Y, x.X)
            });
        var result = query.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         DEGREES(x."Rad") AS "DegVal",
        //         RADIANS(x."Deg") AS "RadVal",
        //         SIND(x."Deg") AS "SindVal",
        //         COSD(x."Deg") AS "CosdVal",
        //         TAND(x."Deg") AS "TandVal",
        //         COT(x."Rad") AS "CotVal",
        //         COTD(x."Deg") AS "CotdVal",
        //         ASIND(x."Ratio") AS "AsindVal",
        //         ACOSD(x."Ratio") AS "AcosdVal",
        //         ATAND(x."Ratio") AS "AtandVal",
        //         ATAN2D(x."Y", x."X") AS "Atan2dVal"
        // FROM
        //         tmp_256ffae51d3c4de297125531acac31f6 x
        // ORDER BY
        //         x."Id"
        // shape: (2, 12)
        // ┌─────┬───────────┬──────────┬─────────┬───┬──────────┬──────────┬───────────┬───────────┐
        // │ Id  ┆ DegVal    ┆ RadVal   ┆ SindVal ┆ … ┆ AsindVal ┆ AcosdVal ┆ AtandVal  ┆ Atan2dVal │
        // │ --- ┆ ---       ┆ ---      ┆ ---     ┆   ┆ ---      ┆ ---      ┆ ---       ┆ ---       │
        // │ i32 ┆ f64       ┆ f64      ┆ f64     ┆   ┆ f64      ┆ f64      ┆ f64       ┆ f64       │
        // ╞═════╪═══════════╪══════════╪═════════╪═══╪══════════╪══════════╪═══════════╪═══════════╡
        // │ 1   ┆ 29.965693 ┆ 0.523599 ┆ 0.5     ┆ … ┆ 30.0     ┆ 60.0     ┆ 26.565051 ┆ 45.0      │
        // │ 2   ┆ 89.954374 ┆ 1.570796 ┆ 1.0     ┆ … ┆ 90.0     ┆ 0.0      ┆ 45.0      ┆ -90.0     │
        // └─────┴───────────┴──────────┴─────────┴───┴──────────┴──────────┴───────────┴───────────┘
        
        Assert.Equal(2, result.Count);
        
        Assert.True(Math.Abs(result[0].SindVal - 0.5) < 1e-6);

    }
    [Fact]
    [Trait("Linq", "MathGeneral")]
    public void Test_Polars_Linq_Native_Math_General_Functions()
    {
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        var mockData = new[] 
        { 
            new { Id = 1, V1 = 2.0, V2 = 3.0, IntVal = 10, Divisor = 3 }
        };
        
        var table = db.RegisterTable(DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                
                AbsVal = Math.Abs(x.V2 * -1),                   
                ModVal = PolarsSql.Mod(x.IntVal, x.Divisor),     
                DivOpVal = x.IntVal / x.Divisor,                 
                DivFuncVal = PolarsSql.Div(x.IntVal, x.Divisor),

                CeilVal = PolarsSql.Ceil(x.V1 + 0.5),          
                FloorVal = Math.Floor(x.V1 + 0.5),               
                RoundVal = PolarsSql.Round(x.V1 + 0.54, 1),     
                
                PowVal = Math.Pow(x.V1, x.V2),              
                SqrtVal = Math.Sqrt(x.V1),                      
                CbrtVal = PolarsSql.Cbrt(x.V1),              
                ExpVal = Math.Exp(x.V1),                       

                LnVal = Math.Log(x.V1),                       
                Log10Val = PolarsSql.Log10(x.V1),               
                Log2Val = PolarsSql.Log2(x.V1),                 
                Log1pVal = PolarsSql.Log1p(x.V1),               

                SignVal = Math.Sign(x.V1 - 5.0),                
                
                PiFunc = PolarsSql.Pi()                         
            });

        var df = query.ToDataFrame();
        // SELECT
        //         x."Id" AS "Id",
        //         Abs(x."V2" * -1) AS "AbsVal",
        //         MOD(x."IntVal", x."Divisor") AS "ModVal",
        //         x."IntVal" / x."Divisor" AS "DivOpVal",
        //         DIV(x."IntVal"::BigInt, x."Divisor"::BigInt) AS "DivFuncVal",
        //         CEIL((x."V1" + 0.5)) AS "CeilVal",
        //         Floor(x."V1" + 0.5) AS "FloorVal",
        //         ROUND((x."V1" + 0.54000000000000004), 1) AS "RoundVal",
        //         Power(x."V1", x."V2") AS "PowVal",
        //         Sqrt(x."V1") AS "SqrtVal",
        //         CBRT(x."V1") AS "CbrtVal",
        //         Exp(x."V1") AS "ExpVal",
        //         Ln(x."V1") AS "LnVal",
        //         LOG10(x."V1") AS "Log10Val",
        //         LOG2(x."V1") AS "Log2Val",
        //         LOG1P(x."V1") AS "Log1pVal",
        //         Sign(x."V1" - 5) AS "SignVal",
        //         PI() AS "PiFunc"
        // FROM
        //         tmp_1edac8e4c27e46488e4932c6d2ad1831 x
        // ORDER BY
        //         x."Id"
        // shape: (1, 18)
        // ┌─────┬────────┬────────┬──────────┬───┬─────────┬──────────┬─────────┬──────────┐
        // │ Id  ┆ AbsVal ┆ ModVal ┆ DivOpVal ┆ … ┆ Log2Val ┆ Log1pVal ┆ SignVal ┆ PiFunc   │
        // │ --- ┆ ---    ┆ ---    ┆ ---      ┆   ┆ ---     ┆ ---      ┆ ---     ┆ ---      │
        // │ i32 ┆ f64    ┆ i32    ┆ i32      ┆   ┆ f64     ┆ f64      ┆ f64     ┆ f64      │
        // ╞═════╪════════╪════════╪══════════╪═══╪═════════╪══════════╪═════════╪══════════╡
        // │ 1   ┆ 3.0    ┆ 1      ┆ 3        ┆ … ┆ 1.0     ┆ 1.098612 ┆ -1.0    ┆ 3.141593 │
        // └─────┴────────┴────────┴──────────┴───┴─────────┴──────────┴─────────┴──────────┘
        Assert.Equal(18L, df.Width);

    }
    [Fact]
    [Trait("Linq", "ArrayFunctionsBatch1")]
    public void Test_Polars_Linq_Array_Batch1()
    {
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx, ownsContext: true);

        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            DeptId = new[] { 10, 10, 20 },
            Name = new[] { "Alice", "Bob", "Charlie" },
            Tags = new[] 
            { 
                new[] { "admin", "user" }, 
                ["user"], 
                ["guest", "user"] 
            },
            Scores = new[] 
            { 
                new[] { 90, 85, 95 }, 
                [70], 
                [60, 65] 
            }
        });
        var prototype = new[] 
        { 
            new { Id = 0, DeptId = 0, Name = "", Tags = new string[0], Scores = new int[0] } 
        };
        var table = db.RegisterTable(df,prototype);

        var scalarQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                TagsCount = PolarsSql.ArrayLength(x.Tags),
                
                IsAdmin = PolarsSql.ArrayContains(x.Tags, "admin"),
                
                FirstScore = PolarsSql.ArrayGet(x.Scores, 1) 
            });

        var scalarResult = scalarQuery.ToList();

        // SELECT
        //         x."Id" AS "Id",
        //         ARRAY_LENGTH(x."Tags") AS "TagsCount",
        //         ARRAY_CONTAINS(x."Tags", 'admin') AS "IsAdmin",
        //         ARRAY_GET(x."Scores", 1) AS "FirstScore"
        // FROM
        //         tmp_bd15bd83867a40689d26cbc29b1e66c4 x
        // ORDER BY
        //         x."Id"
        // shape: (3, 4)
        // ┌─────┬───────────┬─────────┬────────────┐
        // │ Id  ┆ TagsCount ┆ IsAdmin ┆ FirstScore │
        // │ --- ┆ ---       ┆ ---     ┆ ---        │
        // │ i32 ┆ u32       ┆ bool    ┆ i32        │
        // ╞═════╪═══════════╪═════════╪════════════╡
        // │ 1   ┆ 2         ┆ true    ┆ 90         │
        // │ 2   ┆ 1         ┆ false   ┆ 70         │
        // │ 3   ┆ 2         ┆ false   ┆ 60         │
        // └─────┴───────────┴─────────┴────────────┘
        Assert.Equal(3, scalarResult.Count);
        
        // Alice
        Assert.Equal(2, scalarResult[0].TagsCount);
        Assert.True(scalarResult[0].IsAdmin);
        Assert.Equal(90, scalarResult[0].FirstScore);

        // Bob
        Assert.Equal(1, scalarResult[1].TagsCount);
        Assert.False(scalarResult[1].IsAdmin);
        Assert.Equal(70, scalarResult[1].FirstScore);

        // ==========================================
        // ARRAY_AGG
        // ==========================================
        var aggQuery = table
            .GroupBy(x => x.DeptId)
            .Select(g => new
            {
                DeptId = g.Key,
                EmployeeNames = g.ArrayAgg(x => x.Name) 
            })
            .OrderBy(x => x.DeptId);
        // SELECT
        //         g_1."DeptId" AS "DeptId",
        //         ARRAY_AGG(g_1."Name") AS "EmployeeNames"
        // FROM
        //         tmp_9351839e2563421f9110998373cab3b4 g_1
        // GROUP BY
        //         g_1."DeptId" ORDER BY
        //         g_1."DeptId"
        // shape: (2, 2)
        // ┌────────┬──────────────────┐
        // │ DeptId ┆ EmployeeNames    │
        // │ ---    ┆ ---              │
        // │ i32    ┆ list[str]        │
        // ╞════════╪══════════════════╡
        // │ 10     ┆ ["Alice", "Bob"] │
        // │ 20     ┆ ["Charlie"]      │
        // └────────┴──────────────────┘
        var aggResult = aggQuery.ToList();

        Assert.Equal(2, aggResult.Count);
        
        // Dept 10 (Alice, Bob)
        Assert.Equal(10, aggResult[0].DeptId);
        var dept10Names = aggResult[0].EmployeeNames.ToArray();
        Assert.Equal(2, dept10Names.Length);
        Assert.Contains("Alice", dept10Names);
        Assert.Contains("Bob", dept10Names);

        // Dept 20 (Charlie)
        Assert.Equal(20, aggResult[1].DeptId);
        var dept20Names = aggResult[1].EmployeeNames.ToArray();
        Assert.Single(dept20Names);
        Assert.Equal("Charlie", dept20Names[0]);
    }
    [Fact]
    [Trait("Linq", "ArrayFunctionsBatch2")]
    public void Test_Polars_Linq_Array_Batch2()
    {
        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2 },
            Words = new[] 
            { 
                ["Hello", "World"], 
                new[] { "POLARS", "net" } 
            },
            Values = new[] 
            { 
                [10, 20, 30],
                new[] { 5, 15 }
            }
        });

        var prototype = new[] 
        { 
            new { Id = 0, Words = new string[0], Values = new int[0] } 
        };
        
        var table = df.AsQueryable(prototype);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                MinWord = PolarsSql.ArrayMin(x.Words),
                MaxWord = PolarsSql.ArrayMax(x.Words),
                
                MeanVal = PolarsSql.ArrayMean(x.Values),
                SumVal = PolarsSql.ArraySum(x.Values)
            });
        var result = query.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         ARRAY_LOWER(x."Words") AS "MinWord",
        //         ARRAY_UPPER(x."Words") AS "MaxWord",
        //         ARRAY_MEAN(x."Values") AS "MeanVal",
        //         ARRAY_SUM(x."Values") AS "SumVal"
        // FROM
        //         tmp_b1db87a032364f2d99ef1ab96a908a99 x
        // ORDER BY
        //         x."Id"
        // shape: (2, 5)
        // ┌─────┬─────────┬─────────┬─────────┬────────┐
        // │ Id  ┆ MinWord ┆ MaxWord ┆ MeanVal ┆ SumVal │
        // │ --- ┆ ---     ┆ ---     ┆ ---     ┆ ---    │
        // │ i32 ┆ str     ┆ str     ┆ f64     ┆ i32    │
        // ╞═════╪═════════╪═════════╪═════════╪════════╡
        // │ 1   ┆ Hello   ┆ World   ┆ 20.0    ┆ 60     │
        // │ 2   ┆ POLARS  ┆ net     ┆ 10.0    ┆ 20     │
        // └─────┴─────────┴─────────┴─────────┴────────┘
        Assert.Equal(2, result.Count);

        Assert.Equal("Hello", result[0].MinWord); 
        Assert.Equal("World", result[0].MaxWord);
        Assert.Equal(20.0, result[0].MeanVal); 
        Assert.Equal(60, result[0].SumVal);    

        Assert.Equal("POLARS", result[1].MinWord); 
        Assert.Equal("net", result[1].MaxWord);
        Assert.Equal(10.0, result[1].MeanVal); 
        Assert.Equal(20, result[1].SumVal);
    }
    [Fact]
    [Trait("Linq", "ArrayFunctionsBatch3")]
    public void Test_Polars_Linq_Array_Batch3()
    {
        var mockData = new[] 
        { 
            new { Id = 1, Tags = new[] { "apple", "banana", "apple" } },
            new { Id = 2, Tags = new[] { "dog", "cat" } }
        };
        using var df = DataFrame.From(mockData);
        var table = df.AsQueryable(mockData);

        // ==========================================
        // Array Reverse, Unique, ToString
        // ==========================================
        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                Reversed = PolarsSql.ArrayReverse(x.Tags),
                Unique = PolarsSql.ArrayUnique(x.Tags),
                Joined = PolarsSql.ArrayToString(x.Tags, "-")
            });
        var result = query.ToList();
        // SELECT
        //         x."Id" AS "Id",
        //         ARRAY_REVERSE(x."Tags") AS "Reversed",
        //         ARRAY_UNIQUE(x."Tags") AS "Unique",
        //         ARRAY_TO_STRING(x."Tags", '-') AS "Joined"
        // FROM
        //         tmp_351733abc7bb48ae85ffdf4fd975b0ea x
        // ORDER BY
        //         x."Id"
        // shape: (2, 4)
        // ┌─────┬──────────────────────────────┬─────────────────────┬────────────────────┐
        // │ Id  ┆ Reversed                     ┆ Unique              ┆ Joined             │
        // │ --- ┆ ---                          ┆ ---                 ┆ ---                │
        // │ i32 ┆ list[str]                    ┆ list[str]           ┆ str                │
        // ╞═════╪══════════════════════════════╪═════════════════════╪════════════════════╡
        // │ 1   ┆ ["apple", "banana", "apple"] ┆ ["apple", "banana"] ┆ apple-banana-apple │
        // │ 2   ┆ ["cat", "dog"]               ┆ ["dog", "cat"]      ┆ dog-cat            │
        // └─────┴──────────────────────────────┴─────────────────────┴────────────────────┘
        Assert.Equal(2, result.Count);

        // Row 1
        Assert.Equal("apple-banana-apple", result[0].Joined);
        
        var uniqueTags = result[0].Unique;
        Assert.Equal(2, uniqueTags.Length); 
        Assert.Contains("apple", uniqueTags);
        Assert.Contains("banana", uniqueTags);

        // Row 2
        Assert.Equal(new[] { "cat", "dog" }, result[1].Reversed); // dog, cat -> cat, dog
        Assert.Equal("dog-cat", result[1].Joined);

        // ==========================================
        // UNNEST
        // ==========================================
        var unnestQuery = table
            .Select(x => new
            {
                x.Id,
                SingleTag = PolarsSql.Unnest(x.Tags) 
            })
            .OrderBy(x => x.Id).ThenBy(x => x.SingleTag);
        var unnestResult = unnestQuery.ToList();
        // SELECT
        //         t1."Id" AS "Id",
        //         t1."SingleTag" AS "SingleTag"
        // FROM
        //         (
        //                 SELECT
        //                         x."Id",
        //                         UNNEST(x."Tags") as "SingleTag"
        //                 FROM
        //                         tmp_0c6c4b69acca4a38a8a78eedc1ee8575 x
        //         ) t1
        // ORDER BY
        //         t1."Id",
        //         t1."SingleTag"
        // shape: (5, 2)
        // ┌─────┬───────────┐
        // │ Id  ┆ SingleTag │
        // │ --- ┆ ---       │
        // │ i32 ┆ str       │
        // ╞═════╪═══════════╡
        // │ 1   ┆ apple     │
        // │ 1   ┆ apple     │
        // │ 1   ┆ banana    │
        // │ 2   ┆ cat       │
        // │ 2   ┆ dog       │
        // └─────┴───────────┘

        Assert.Equal(5, unnestResult.Count);

        Assert.Equal(1, unnestResult[0].Id); Assert.Equal("apple", unnestResult[0].SingleTag);
        Assert.Equal(1, unnestResult[1].Id); Assert.Equal("apple", unnestResult[1].SingleTag);
        Assert.Equal(1, unnestResult[2].Id); Assert.Equal("banana", unnestResult[2].SingleTag);

        Assert.Equal(2, unnestResult[3].Id); Assert.Equal("cat", unnestResult[3].SingleTag);
        Assert.Equal(2, unnestResult[4].Id); Assert.Equal("dog", unnestResult[4].SingleTag);
    }
    public class OrderDetail {
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    [Fact(Skip = "This is the feature boundary")]
    [Trait("Linq", "ArrayUnnestStruct")]
    public void Test_Polars_Linq_Unnest_StructArray()
    {
        var orders = new[]
        {
            new { 
                OrderId = 101, 
                Details = new[] { 
                    new OrderDetail { Sku = "Apple", Qty = 5 }, 
                    new OrderDetail { Sku = "Banana", Qty = 2 } 
                } 
            },
            new { 
                OrderId = 102, 
                Details = new[] { 
                    new OrderDetail { Sku = "Cherry", Qty = 10 } 
                } 
            }
        };

        using var df = DataFrame.FromRows(orders);
        // Here is the polars way to do such query:
        // df.Show();
        var result = df.Explode("Details").Unnest("Details");

    }
    public class JoinResult
    {
        public string DeptName { get; set; } = string.Empty;
        public string? EmployeeName { get; set; } 
    }
    [Fact]
    [Trait("Linq", "LeftJoinNew")]
    public void Test_Polars_Linq_LeftJoin_Net10()
    {
        // Arrange
        var depts = new[]
        {
            new DeptDto { DeptId = 1, DeptName = "Engineering" },
            new DeptDto { DeptId = 2, DeptName = "Sales" },
            new DeptDto { DeptId = 3, DeptName = "HR" } 
        };

        var emps = new[]
        {
            new EmpDto { Name = "Alice", DeptId = 1 },
            new EmpDto { Name = "Bob",   DeptId = 2 },
            new EmpDto { Name = "Charlie", DeptId = 1 }
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var deptQuery = db.RegisterTable<DeptDto>(dfDepts);
        var empQuery = db.RegisterTable<EmpDto>(dfEmps);

        var query = deptQuery
            .LeftJoin(
                empQuery,
                d => d.DeptId,
                e => e.DeptId,
                (d, e) => new 
                {
                    d.DeptId,
                    d.DeptName,
                    EmployeeName = e != null ? e.Name : "NO_EMPLOYEE" 
                })
            .OrderBy(x => x.DeptId)
            .ThenBy(x => x.EmployeeName)
            .Select(x => new JoinResult
            {
                DeptName = x.DeptName,
                EmployeeName = x.EmployeeName
            });

        var results = query.ToList();
        // SELECT
        //         x."DeptName",
        //         x."EmployeeName"
        // FROM
        //         (
        //                 SELECT
        //                         d."DeptId",
        //                         CASE
        //                                 WHEN e."DeptId" IS NOT NULL THEN e."Name"
        //                                 ELSE 'NO_EMPLOYEE'
        //                         END as "EmployeeName",
        //                         d."DeptName"
        //                 FROM
        //                         "DeptDto" d
        //                                 LEFT JOIN "EmpDto" e ON d."DeptId" = e."DeptId"
        //         ) x
        // ORDER BY
        //         x."DeptId",
        //         x."EmployeeName"
        // shape: (4, 2)
        // ┌─────────────┬──────────────┐
        // │ DeptName    ┆ EmployeeName │
        // │ ---         ┆ ---          │
        // │ str         ┆ str          │
        // ╞═════════════╪══════════════╡
        // │ Engineering ┆ Alice        │
        // │ Engineering ┆ Charlie      │
        // │ Sales       ┆ Bob          │
        // │ HR          ┆ NO_EMPLOYEE  │
        // └─────────────┴──────────────┘

        // Assert
        Assert.Equal(4, results.Count);

        Assert.Equal("Engineering", results[0].DeptName);
        Assert.Equal("Alice", results[0].EmployeeName);
        Assert.Equal("Engineering", results[1].DeptName);
        Assert.Equal("Charlie", results[1].EmployeeName);

        Assert.Equal("Sales", results[2].DeptName);
        Assert.Equal("Bob", results[2].EmployeeName);

        Assert.Equal("HR", results[3].DeptName);
        Assert.Equal("NO_EMPLOYEE", results[3].EmployeeName);
    }

    [Fact]
    [Trait("Linq", "RightJoin")]
    public void Test_Polars_Linq_RightJoin_Net10()
    {
        var depts = new[]
        {
            new DeptDto { DeptId = 1, DeptName = "Engineering" },
            new DeptDto { DeptId = 2, DeptName = "Sales" },
            new DeptDto { DeptId = 3, DeptName = "HR" }
        };

        var emps = new[]
        {
            new EmpDto { Name = "Alice", DeptId = 1 },
            new EmpDto { Name = "Bob",   DeptId = 2 },
            new EmpDto { Name = "Charlie", DeptId = 1 },
            new EmpDto { Name = "David", DeptId = 99 } 
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var deptQuery = db.RegisterTable<DeptDto>(dfDepts);
        var empQuery = db.RegisterTable<EmpDto>(dfEmps);

        var query = empQuery
            .RightJoin(
                deptQuery,
                e => e.DeptId,
                d => d.DeptId,
                (e, d) => new 
                {
                    d.DeptId,
                    d.DeptName,

                    EmployeeName = e != null ? e.Name : "NO_EMPLOYEE" 
                })
            .OrderBy(x => x.DeptId)
            .ThenBy(x => x.EmployeeName)
            .Select(x => new JoinResult
            {
                DeptName = x.DeptName,
                EmployeeName = x.EmployeeName
            });

        var results = query.ToList();
        // SELECT
        //         x."DeptName",
        //         x."EmployeeName"
        // FROM
        //         (
        //                 SELECT
        //                         d."DeptId",
        //                         CASE
        //                                 WHEN e."DeptId" IS NOT NULL THEN e."Name"
        //                                 ELSE 'NO_EMPLOYEE'
        //                         END as "EmployeeName",
        //                         d."DeptName"
        //                 FROM
        //                         "EmpDto" e
        //                                 RIGHT JOIN "DeptDto" d ON e."DeptId" = d."DeptId"
        //         ) x
        // ORDER BY
        //         x."DeptId",
        //         x."EmployeeName"
        // shape: (4, 2)
        // ┌─────────────┬──────────────┐
        // │ DeptName    ┆ EmployeeName │
        // │ ---         ┆ ---          │
        // │ str         ┆ str          │
        // ╞═════════════╪══════════════╡
        // │ Engineering ┆ Alice        │
        // │ Engineering ┆ Charlie      │
        // │ Sales       ┆ Bob          │
        // │ HR          ┆ NO_EMPLOYEE  │
        // └─────────────┴──────────────┘

        Assert.Equal(4, results.Count);
        Assert.Equal("HR", results[3].DeptName);
        Assert.Equal("NO_EMPLOYEE", results[3].EmployeeName);
    }
    public class SaleRecord
    {
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public double Amount { get; set; }
    }

    [Fact]
    [Trait("Linq", "CountByAggregateBy")]
    public void Test_Polars_Linq_New_Aggregations_Net10()
    {
        // Arrange
        var sales = new[]
        {
            new SaleRecord { Id = 1, Category = "Electronics", Amount = 1200.50 },
            new SaleRecord { Id = 2, Category = "Electronics", Amount = 800.00 },
            new SaleRecord { Id = 3, Category = "Clothing",    Amount = 150.00 },
            new SaleRecord { Id = 4, Category = "Clothing",    Amount = 200.00 },
            new SaleRecord { Id = 5, Category = "Clothing",    Amount = 50.00 },
            new SaleRecord { Id = 6, Category = "Books",       Amount = 45.00 }
        };

        using var df = DataFrame.From(sales);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var salesQuery = db.RegisterTable<SaleRecord>(df);

        var countQuery = salesQuery
            .CountBy(x => x.Category)
            .OrderBy(x => x.Key);
        var countResult = countQuery.ToList();
        // SELECT
        //         g_1."Category" as "Key_1",
        //         COUNT(*) as "Count_1"
        // FROM
        //         "SaleRecord" g_1
        // GROUP BY
        //         g_1."Category" ORDER BY
        //         g_1."Category"
        // shape: (3, 2)
        // ┌─────────────┬─────────┐
        // │ Key_1       ┆ Count_1 │
        // │ ---         ┆ ---     │
        // │ str         ┆ u32     │
        // ╞═════════════╪═════════╡
        // │ Books       ┆ 1       │
        // │ Clothing    ┆ 3       │
        // │ Electronics ┆ 2       │
        // └─────────────┴─────────┘

        Assert.Equal(3, countResult.Count);
        
        // Books: 1, Clothing: 3, Electronics: 2
        Assert.Equal("Books", countResult[0].Key);
        Assert.Equal(1, countResult[0].Value);

        Assert.Equal("Clothing", countResult[1].Key);
        Assert.Equal(3, countResult[1].Value);

        Assert.Equal("Electronics", countResult[2].Key);
        Assert.Equal(2, countResult[2].Value);

        var sumQuery = salesQuery
            .GroupBy(x => x.Category)
            .Select(g => new 
            {
                Key = g.Key, 
                Value = g.Sum(x => x.Amount) 
            })
            .OrderBy(x => x.Key);
        // SELECT
        //         g_1."Category" AS "Key",
        //         SUM(g_1."Amount") AS "Value"
        // FROM
        //         "SaleRecord" g_1
        // GROUP BY
        //         g_1."Category" ORDER BY
        //         g_1."Category"
        // shape: (3, 2)
        // ┌─────────────┬────────┐
        // │ Key         ┆ Value  │
        // │ ---         ┆ ---    │
        // │ str         ┆ f64    │
        // ╞═════════════╪════════╡
        // │ Books       ┆ 45.0   │
        // │ Clothing    ┆ 400.0  │
        // │ Electronics ┆ 2000.5 │
        // └─────────────┴────────┘
        var sumResult = sumQuery.ToList();

        Assert.Equal(3, sumResult.Count);

        // Books: 45.0
        Assert.Equal("Books", sumResult[0].Key);
        Assert.Equal(45.0, sumResult[0].Value);

        // Clothing: 150 + 200 + 50 = 400.0
        Assert.Equal("Clothing", sumResult[1].Key);
        Assert.Equal(400.0, sumResult[1].Value);

        // Electronics: 1200.5 + 800 = 2000.5
        Assert.Equal("Electronics", sumResult[2].Key);
        Assert.Equal(2000.5, sumResult[2].Value);
    }
}