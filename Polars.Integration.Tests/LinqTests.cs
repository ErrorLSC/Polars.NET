using Polars.CSharp;
using static Polars.CSharp.Polars;
using Polars.NET.Linq.CSharpExtensions;
using LinqToDB;
using DataType = Polars.CSharp.DataType;
using LinqToDB.Async;
using Polars.NET.Linq; // 引入我们刚才写的扩展

namespace Polars.Integration.Tests;

public class LinqProviderTests
{
    // 1. 定义一个用于映射的强类型 POCO
    // 注意：底层由于复用了 DataFrame.Rows<T>()，要求实体类必须有无参构造函数 (new())
    public class Person
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public double Sales { get; set; }
    }

    // 专门用于 Select 投影的 DTO
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

    // 定义包含外键的员工类
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
        // Arrange: 准备模拟数据
        var data = new[]
        {
            new Person { Name = "Alice",   Age = 25, Sales = 100.0 },
            new Person { Name = "Bob",     Age = 30, Sales = 200.0 },
            new Person { Name = "Charlie", Age = 35, Sales = 300.0 },
            new Person { Name = "David",   Age = 18, Sales =  50.0 }
        };

        // 创建 Polars 内存 DataFrame
        using var df = DataFrame.From(data);

        // 【新写法】：一键创建自带 SqlContext 的极简数据上下文
        using var db = new PolarsDataContext(Sql(), ownsContext: true);

        // 定义外部闭包变量，测试 linq2db 能否将它们内联为纯文本 SQL
        int ageLimit = 20;
        string excludeName = "Alice";

        // Act: 注册表并开启 LINQ 查询链 (此时只是拼接表达式树，不会有任何计算)
        // 【新写法】：直接调用 db.RegisterTable
        var query = db.RegisterTable<Person>("people", df)
                      .Where(p => p.Age > ageLimit && p.Name != excludeName)
                      .OrderByDescending(p => p.Sales);

        // 触发物化：调用 ToList() 时生成 SQL 交给 Rust 引擎
        var results = query.ToList();

        // Assert: 验证结果
        Assert.NotNull(results);
        Assert.Equal(2, results.Count); // 应该只剩下 Bob 和 Charlie

        // 验证 OrderByDescending (Sales 降序，Charlie 应该在第一位)
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
        
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);

        // Act: 注册表、仅查询部分列，并映射到全新的 DTO 类中
        var query = db.RegisterTable<Person>("people", df)
                      .Where(p => p.Sales > 150)
                      .Select(p => new PersonDto 
                      { 
                          Name = p.Name, 
                          Sales = p.Sales 
                      });

        var results = query.ToList();

        // Assert
        Assert.Equal(2, results.Count);
        
        // 验证生成的集合类型
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

        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);
        var deptQuery = db.RegisterTable<Department>("departments", dfDepts);
        var empQuery = db.RegisterTable<Employee>("employees", dfEmps);

        // Act: 经典的 LINQ Join 语法
        var query = from e in empQuery
                    join d in deptQuery on e.DeptId equals d.DeptId
                    where e.Name != "Bob"
                    orderby d.DeptName,e.Name
                    select new EmpDeptDto
                    {
                        EmpName = e.Name,
                        DepartmentName = d.DeptName
                    };

        var results = query.ToList();
        // Assert
        Assert.Equal(2, results.Count);
        
        // 应该只有 Alice 和 Charlie (因为 Bob 被过滤了)，且按部门名称排序
        Assert.Equal("Alice", results[0].EmpName);
        Assert.Equal("Engineering", results[0].DepartmentName);

        Assert.Equal("Charlie", results[1].EmpName);
        Assert.Equal("Engineering", results[1].DepartmentName);
    }
    [Fact]
    [Trait("Linq","GroupByHaving")]
    public void Test_Polars_Linq_GroupBy_Aggregation_With_Having()
    {
        // Arrange: 准备数据
        var emps = new[]
        {
            new EmployeeSalary { Name = "Alice", DeptId = 1, Salary = 5000.0 },
            new EmployeeSalary { Name = "Bob",   DeptId = 2, Salary = 4000.0 },
            new EmployeeSalary { Name = "Charlie", DeptId = 1, Salary = 6000.0 },
            new EmployeeSalary { Name = "David", DeptId = 2, Salary = 4500.0 },
            new EmployeeSalary { Name = "Eve",   DeptId = 3, Salary = 3000.0 }
        };

        using var dfEmps = DataFrame.From(emps);
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);
        
        var empQuery = db.RegisterTable<EmployeeSalary>("employees_salary", dfEmps);

        // Act: LINQ GroupBy + Having 语法
        // 预期 SQL: GROUP BY e."DeptId" HAVING SUM(e."Salary") > 5000
        var query = from e in empQuery
                    group e by e.DeptId into g
                    // 【核心升级】：这里的 where 会被翻译成 HAVING
                    where g.Sum(x => x.Salary) > 5000.0 
                    orderby g.Key
                    select new DeptStatsDto
                    {
                        DeptId = g.Key,
                        TotalSalary = g.Sum(x => x.Salary),
                        EmployeeCount = g.Count()
                    };

        var results = query.ToList();

        // Assert: 验证聚合和过滤结果
        // 一共 3 个部门，但 Dept 3 (3000) 被 HAVING 过滤掉了，只剩下 2 个
        Assert.Equal(2, results.Count); 

        // 验证 Dept 1 (Alice + Charlie)
        Assert.Equal(1, results[0].DeptId);
        Assert.Equal(11000.0, results[0].TotalSalary); // 5000 + 6000
        Assert.Equal(2, results[0].EmployeeCount);

        // 验证 Dept 2 (Bob + David)
        Assert.Equal(2, results[1].DeptId);
        Assert.Equal(8500.0, results[1].TotalSalary); // 4000 + 4500
        Assert.Equal(2, results[1].EmployeeCount);
        
        // 确保 Dept 3 不存在
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

        using var df = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        // 或者沿用你上面写的 ctx.RegisterTable<T> 方式
        var query = db.RegisterTable("students", df,data);

        // ==========================================
        // 测试 1：标量聚合 (Scalar Aggregation)
        // linq2db 预期生成: SELECT COUNT(*) FROM students WHERE Score > 82
        // ==========================================
        int highScorersCount = query.Where(s => s.Score > 82).Count();
        Assert.Equal(2, highScorersCount); // Bob 和 Charlie

        // ==========================================
        // 测试 2：求最大值 (Max)
        // linq2db 预期生成: SELECT MAX(Score) FROM students
        // ==========================================
        int maxScore = query.Max(s => s.Score);
        Assert.Equal(90, maxScore);

        // ==========================================
        // 测试 3：单行查询 (First / Take 1)
        // linq2db 预期生成: SELECT Id, Name, Score FROM students WHERE Score > 82 ORDER BY Score DESC LIMIT 1
        // ==========================================
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
        // Arrange: 准备测试数据
        var data = new[]
        {
            new Product(1, "Apple", "Fruit", 1.2),
            new Product(2, "Banana", "Fruit", 0.8),
            new Product(3, "Carrot", "Vegetable", 0.5),
            new Product(4, "Avocado", "Vegetable", 2.0),
            new Product(5, "Beef", "Meat", 5.0)
        };

        using var df = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);

        var query = db.RegisterTable<Product>("products", df);

        // ==========================================
        // 测试 1：集合包含 (映射为 IN 子句)
        // linq2db 预期生成: SELECT ... FROM products WHERE Category IN ('Fruit', 'Meat')
        // ==========================================
        var targetCategories = new[] { "Fruit", "Meat" };
        var inResult = query.Where(p => targetCategories.Contains(p.Category)).ToList();
        
        Assert.Equal(3, inResult.Count); // Apple, Banana, Beef
        Assert.DoesNotContain(inResult, p => p.Category == "Vegetable");

        // ==========================================
        // 测试 2：字符串前缀匹配 (映射为 LIKE 'A%')
        // linq2db 预期生成: SELECT ... FROM products WHERE Name LIKE 'A%' ESCAPE '~' (或者没 ESCAPE)
        // ==========================================
        var likeResult = query.Where(p => p.Name.StartsWith("A")).ToList();
        
        Assert.Equal(2, likeResult.Count); // Apple, Avocado
        Assert.Contains(likeResult, p => p.Name == "Apple");
        Assert.Contains(likeResult, p => p.Name == "Avocado");

        // ==========================================
        // 测试 3：组合拳！IN + LIKE + 复杂条件
        // ==========================================
        var complexResult = query.Where(p => 
            targetCategories.Contains(p.Category) && 
            p.Name.Contains('e') && 
            p.Price > 1.0
        ).ToList();

        // 只有 Apple 和 Beef 满足：类别是水果或肉，名字包含 e，且价格大于 1.0
        Assert.Equal(2, complexResult.Count); 
    }
    [Fact]
    [Trait("Linq", "PaginationAndDistinct")]
    public void Test_Polars_Linq_Skip_Take_And_Distinct()
    {
        // Arrange
        var data = new[]
        {
            new Product(1, "Apple", "Fruit", 1.2),
            new Product(2, "Banana", "Fruit", 0.8),
            new Product(3, "Cherry", "Fruit", 1.5),
            new Product(4, "Beef", "Meat", 5.0),
            new Product(5, "Pork", "Meat", 4.0),
            new Product(6, "Apple", "Fruit", 1.2) // 故意加一个重复的苹果
        };

        using var df = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var query = db.RegisterTable<Product>("products", df);

        // ==========================================
        // 测试 1：投影去重 (Distinct)
        // linq2db 预期生成: SELECT DISTINCT p."Category" FROM products p
        // ==========================================
        var distinctCategories = query.Select(p => p.Category).Distinct().ToList();
        
        Assert.Equal(2, distinctCategories.Count);
        Assert.Contains("Fruit", distinctCategories);
        Assert.Contains("Meat", distinctCategories);

        // 我们还可以测试整行去重
        var distinctProducts = query.Distinct().ToList();
        // 因为 Id 不同 (1 和 6)，所以它们不算完全重复，总数还是 6
        Assert.Equal(6, distinctProducts.Count); 

        // ==========================================
        // 测试 2：分页与切片 (Skip & Take)
        // linq2db 预期生成: SELECT ... FROM products ORDER BY p."Id" LIMIT 2 OFFSET 2
        // ==========================================
        var pagedResult = query.OrderBy(p => p.Id)
                               .Skip(2)
                               .Take(2)
                               .ToList();
                               
        Assert.Equal(2, pagedResult.Count);
        // 跳过 Apple(1) 和 Banana(2)，应该取到 Cherry(3) 和 Beef(4)
        Assert.Equal(3, pagedResult[0].Id); 
        Assert.Equal(4, pagedResult[1].Id);
    }
    // [Fact]
    // [Trait("Linq", "Subquery")]
    // public void Test_Polars_Linq_Subquery_Any()
    // {
    //     // Arrange: 准备部门和员工数据
    //     var depts = new[]
    //     {
    //         new { DeptId = 1, DeptName = "Engineering" },
    //         new { DeptId = 2, DeptName = "Sales" },
    //         new { DeptId = 3, DeptName = "HR" }
    //     };

    //     var emps = new[]
    //     {
    //         new { Name = "Alice", DeptId = 1, Salary = 6000.0 }, // Engineering 达标 (> 5000)
    //         new { Name = "Bob",   DeptId = 2, Salary = 4000.0 }, // Sales 不达标
    //         new { Name = "Charlie", DeptId = 1, Salary = 4500.0 }
    //     };

    //     using var dfDepts = DataFrame.From(depts);
    //     using var dfEmps = DataFrame.From(emps);

    //     using var ctx = new SqlContext();
    //     var deptQuery = ctx.RegisterTable("departments", dfDepts, depts);
    //     var empQuery = ctx.RegisterTable("employees", dfEmps, emps);

    //     // ==========================================
    //     // 测试：相关子查询 Any
    //     // 业务需求：找出“至少拥有一个薪水大于 5000 的员工”的部门
    //     // ==========================================
    //     var result = deptQuery.Where(d => 
    //         empQuery.Any(e => e.DeptId == d.DeptId && e.Salary > 5000)
    //     ).ToList();

    //     // Assert: 只有 Engineering 部门满足条件
    //     Assert.Single(result);
    //     Assert.Equal("Engineering", result[0].DeptName);
    // }
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
        public string? EmployeeName { get; set; } // 左连接可能没有员工，允许为空
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
            new DeptDto { DeptId = 3, DeptName = "HR" } // 注意：HR 部门没有员工！
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
        var deptQuery = db.RegisterTable<DeptDto>("departments", dfDepts);
        var empQuery = db.RegisterTable<EmpDto>("employees", dfEmps);

        // Act: 经典的 LINQ Left Join 语法
        var query = from d in deptQuery
                    join e in empQuery on d.DeptId equals e.DeptId into empGroup
                    from e in empGroup.DefaultIfEmpty() // DefaultIfEmpty 触发 Left Join
                    orderby d.DeptId, e.Name
                    select new LeftJoinResult
                    {
                        DeptName = d.DeptName,
                        // 如果 e 是 null（没有匹配的员工），我们给个默认值或者保留 null
                        EmployeeName = e != null ? e.Name : "NO_EMPLOYEE" 
                    };

        var results = query.ToList();

        // Assert
        // Engineering有2人，Sales有1人，HR有0人(但因为是Left Join，HR也会出现1次)，总共应该返回4条记录
        Assert.Equal(4, results.Count);

        // 验证 Engineering 部门 (Alice, Charlie)
        Assert.Equal("Engineering", results[0].DeptName);
        Assert.Equal("Alice", results[0].EmployeeName);
        
        Assert.Equal("Engineering", results[1].DeptName);
        Assert.Equal("Charlie", results[1].EmployeeName);

        // 验证 Sales 部门 (Bob)
        Assert.Equal("Sales", results[2].DeptName);
        Assert.Equal("Bob", results[2].EmployeeName);

        // 验证 HR 部门 (无员工，应该触发空值处理)
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

        using var db = new PolarsDataContext(Sql(), ownsContext: true);
        var deptQuery = db.RegisterTable<DeptDto>("departments", dfDepts);
        var empQuery = db.RegisterTable<EmpDto>("employees", dfEmps);

        // ==========================================
        // 测试 1：交叉连接 (Cross Join / Cartesian Product)
        // 语法：连续使用多个 from
        // linq2db 预期生成: SELECT ... FROM departments d CROSS JOIN employees e
        // ==========================================
        var crossJoinQuery = from d in deptQuery
                             from e in empQuery
                             select new { d.DeptName, e.Name };

        var crossResult = crossJoinQuery.ToList();
        
        // 2 个部门 * 2 个员工 = 4 条记录
        Assert.Equal(4, crossResult.Count);
        Assert.Contains(crossResult, x => x.DeptName == "Engineering" && x.Name == "Alice");
        Assert.Contains(crossResult, x => x.DeptName == "Sales" && x.Name == "Bob");

        // ==========================================
        // 测试 2：集合拼接 (Concat -> UNION ALL)
        // 业务场景：将两批相同结构的数据合并
        // linq2db 预期生成: SELECT ... FROM employees WHERE ... UNION ALL SELECT ... FROM employees WHERE ...
        // ==========================================
        var query1 = empQuery.Where(e => e.DeptId == 1);
        var query2 = empQuery.Where(e => e.DeptId == 2);
        
        // Concat 对应 UNION ALL (不去重), Union 对应 UNION (去重)
        var unionResult = query1.Concat(query2).ToList();

        Assert.Equal(2, unionResult.Count);
        Assert.Contains(unionResult, x => x.Name == "Alice");
        Assert.Contains(unionResult, x => x.Name == "Bob");

        // ==========================================
        // 测试 3：内置函数映射 (String / Math Functions)
        // linq2db 预期将 C# 的 string.ToUpper() 翻译为 SQL 的 UPPER()
        // ==========================================
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
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var empQuery = db.RegisterTable<EmpSalaryDto>("employees", dfEmps);

        // ==========================================
        // 测试 1：交集 (Intersect)
        // 找出：既是 1 部门，又薪水大于 4000 的员工
        // linq2db 预期: SELECT ... INTERSECT SELECT ...
        // ==========================================
        var q1 = empQuery.Where(e => e.DeptId == 1);
        var q2 = empQuery.Where(e => e.Salary > 4000);
        
        var intersectResult = q1.Intersect(q2).ToList();
        
        Assert.Equal(2, intersectResult.Count); // Alice 和 Charlie
        Assert.Contains(intersectResult, e => e.Name == "Alice");
        Assert.Contains(intersectResult, e => e.Name == "Charlie");

        // ==========================================
        // 测试 2：差集 (Except)
        // 找出：薪水大于 4000，但【排除】1 部门的员工
        // linq2db 预期: SELECT ... EXCEPT SELECT ...
        // ==========================================
        var exceptResult = q2.Except(q1).ToList();
        
        Assert.Equal(2, exceptResult.Count); // David (8000, 3) 和 Eve (5500, 2)
        Assert.DoesNotContain(exceptResult, e => e.DeptId == 1);

        // ==========================================
        // 测试 3：Let 关键字与派生计算
        // 场景：计算年终奖（薪水 * 1.5），并筛选出年终奖大于 8000 的人
        // linq2db 可能的生成结果：内联乘法，或者生成嵌套的 SELECT 子查询
        // ==========================================
        var letQuery = from e in empQuery
                       let bonus = e.Salary * 1.5
                       where bonus > 8000
                       select new 
                       { 
                           e.Name, 
                           Bonus = bonus 
                       };

        var letResult = letQuery.ToList();

        // 只有 Alice (9000), David (12000), Eve (8250) 的年终奖 > 8000
        Assert.Equal(3, letResult.Count);
        Assert.Contains(letResult, x => x.Name == "Alice" && x.Bonus == 9000.0);
        Assert.Contains(letResult, x => x.Name == "David" && x.Bonus == 12000.0);
    }
    [Fact]
    [Trait("Linq", "WindowFunctions")]
    public void Test_Polars_Linq_Window_Functions()
    {
        LinqToDB.Common.Configuration.Sql.GenerateFinalAliases = true;
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
        var empQuery = db.RegisterTable<EmpSalaryDto>("employees", dfEmps);

        // ==========================================
        // 测试：窗口函数 (Window Functions)
        // 业务场景：计算每个人在自己部门内的薪水排名，以及该部门的总薪水
        // ==========================================
        var query = from e in empQuery
                    select new
                    {
                        e.Name,
                        e.DeptId,
                        e.Salary,
                        // 1. 排名窗口函数: RANK() OVER (PARTITION BY DeptId ORDER BY Salary DESC)
                        DeptRank = LinqToDB.Sql.Ext.Rank()
                                         .Over()
                                         .PartitionBy(e.DeptId)
                                         .OrderByDesc(e.Salary)
                                         .ToValue(),
                                         
                        // 2. 聚合窗口函数: SUM(Salary) OVER (PARTITION BY DeptId)
                        DeptTotalSalary = LinqToDB.Sql.Ext.Sum(e.Salary)
                                                .Over()
                                                .PartitionBy(e.DeptId)
                                                .ToValue()
                    };

        var results = query.ToList();

        // Assert
        Assert.Equal(5, results.Count);

        // 验证 1 部门 (Alice: 6000, Charlie: 4500)
        var alice = results.First(r => r.Name == "Alice");
        Assert.Equal(1, alice.DeptRank); // Alice 薪水最高，排名第 1
        Assert.Equal(10500.0, alice.DeptTotalSalary); // 1部门总薪水 10500

        var charlie = results.First(r => r.Name == "Charlie");
        Assert.Equal(2, charlie.DeptRank); // Charlie 排名第 2
        Assert.Equal(10500.0, charlie.DeptTotalSalary); // 窗口聚合，总薪水也是 10500

        // 验证 2 部门 (Eve: 5500, Bob: 4000)
        var eve = results.First(r => r.Name == "Eve");
        Assert.Equal(1, eve.DeptRank);
        Assert.Equal(9500.0, eve.DeptTotalSalary);

        var bob = results.First(r => r.Name == "Bob");
        Assert.Equal(2, bob.DeptRank);
        Assert.Equal(9500.0, bob.DeptTotalSalary);

        // 验证 3 部门 (David: 8000)
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
        var empQuery = db.RegisterTable("employees", dfEmps, emps);

        // ==========================================
        // 测试 1：CASE WHEN (数据分箱/条件分支)
        // 业务需求：按照薪水划分等级
        // linq2db 预期: CASE WHEN e."Salary" >= 6000 THEN 'High' WHEN ... ELSE 'Low' END
        // ==========================================
        var caseWhenQuery = empQuery.Select(e => new
        {
            e.Name,
            // 嵌套的三元运算符
            SalaryTier = e.Salary >= 6000 ? "High" : (e.Salary >= 4500 ? "Medium" : "Low")
        }).ToList();

        Assert.Equal(5, caseWhenQuery.Count);
        Assert.Equal("High", caseWhenQuery.First(e => e.Name == "Alice").SalaryTier);   // 6000
        Assert.Equal("High", caseWhenQuery.First(e => e.Name == "David").SalaryTier);   // 8000
        Assert.Equal("Medium", caseWhenQuery.First(e => e.Name == "Eve").SalaryTier);     // 5500
        Assert.Equal("Medium", caseWhenQuery.First(e => e.Name == "Charlie").SalaryTier); // 4500
        Assert.Equal("Low", caseWhenQuery.First(e => e.Name == "Bob").SalaryTier);        // 4000

        // ==========================================
        // 测试 2：CTE (Common Table Expression / WITH 语句)
        // 业务需求：先圈出一批高薪人群作为 CTE，然后再和自己或其他表进行后续复杂查询
        // linq2db 预期: WITH "HighEarners" AS (SELECT ... ) SELECT * FROM "HighEarners" c WHERE ...
        // ==========================================
        
        // 1. 定义 CTE (此时不执行，只是声明)
        var cte = empQuery.Where(e => e.Salary > 5000).AsCte("HighEarners");

        // 2. 基于 CTE 进行查询
        var cteResult = (from c in cte
                         where c.DeptId == 1 || c.DeptId == 3
                         select c).ToList();

        // 薪水 > 5000 的有 Alice(1,6000), David(3,8000), Eve(2,5500)
        // 在这三人中，部门是 1 或 3 的只有 Alice, David
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
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var orderQuery = db.RegisterTable<OrderDto>("orders", dfOrders);

        // ==========================================
        // 测试 1：多维分组 (Multi-key GroupBy)
        // 业务需求：按年份和地区统计总营收
        // linq2db 预期: GROUP BY EXTRACT(YEAR FROM o."OrderDate"), o."Region"
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

        // 预期结果：2023-North (250), 2023-South (450), 2024-North (300)
        Assert.Equal(3, multiGroupResult.Count);
        
        Assert.Equal(2023, multiGroupResult[0].Year);
        Assert.Equal("North", multiGroupResult[0].Region);
        Assert.Equal(250.0, multiGroupResult[0].TotalRevenue);

        Assert.Equal(2023, multiGroupResult[1].Year);
        Assert.Equal("South", multiGroupResult[1].Region);
        Assert.Equal(450.0, multiGroupResult[1].TotalRevenue);

        // ==========================================
        // 测试 2：时间序列筛选 (Date Functions)
        // 业务需求：找出 2023 年 2 月的所有订单
        // ==========================================
        var febOrders = orderQuery
            .Where(o => o.OrderDate.Year == 2023 && o.OrderDate.Month == 2)
            .ToList();

        Assert.Equal(2, febOrders.Count); // OrderId 3 和 5
        Assert.Contains(febOrders, o => o.OrderId == 3);
        Assert.Contains(febOrders, o => o.OrderId == 5);
    }
    public record NullableEmpDto(string? Name, int DeptId, double Salary);

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
            new NullableEmpDto(null, 2, 4000.0), // 故意塞一个 null 名字
            new NullableEmpDto("Charlie", 1, 4500.0),
            new NullableEmpDto("David", 3, 8000.0)
        };

        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var deptQuery = db.RegisterTable("departments", dfDepts, depts);
        var empQuery = db.RegisterTable("employees", dfEmps, emps);

        // ==========================================
        // 测试 1：非相关子查询 (IN Subquery)
        // 业务需求：找出有员工薪水大于 5000 的部门信息
        // linq2db 预期: SELECT ... FROM departments d WHERE d."DeptId" IN (SELECT e."DeptId" FROM employees e WHERE e."Salary" > 5000)
        // ==========================================
        
        // 1. 构建一个只查 DeptId 的内部查询
        var highPaidDeptIds = empQuery.Where(e => e.Salary > 5000).Select(e => e.DeptId);
        
        // 2. 在外部查询中使用 .Contains() 传入这个内部查询
        var richDepts = deptQuery.Where(d => highPaidDeptIds.Contains(d.DeptId)).ToList();

        Assert.Equal(2, richDepts.Count); // Engineering (Alice) 和 HR (David)
        Assert.Contains(richDepts, d => d.DeptName == "Engineering");
        Assert.Contains(richDepts, d => d.DeptName == "HR");

        // ==========================================
        // 测试 2：空值合并运算符 (?? -> COALESCE)
        // 业务需求：如果员工名字为 null，则显示 "Unknown"
        // linq2db 预期: SELECT COALESCE(e."Name", 'Unknown') FROM employees e
        // ==========================================
        var coalesceQuery = empQuery.Select(e => new
        {
            SafeName = e.Name ?? "Unknown"
        }).ToList();

        Assert.Equal(4, coalesceQuery.Count);
        Assert.Contains(coalesceQuery, e => e.SafeName == "Unknown"); // 替补了原本是 null 的 Bob

        // ==========================================
        // 测试 3：字符串截取与拼接 (Substring / Concat)
        // 业务需求：取名字的前 3 个字母（如果有的话）
        // linq2db 预期: SUBSTRING(e."Name", 1, 3) 
        // ==========================================
        var stringQuery = empQuery
            .Where(e => e.Name != null)
            .Select(e => new
            {
                e.Name,
                ShortName = e.Name!.Substring(0, 3)
            }).ToList();

        Assert.Equal(3, stringQuery.Count);
        Assert.Equal("Ali", stringQuery.First(e => e.Name == "Alice").ShortName);
        Assert.Equal("Cha", stringQuery.First(e => e.Name == "Charlie").ShortName);
        // ==========================================
        // 测试 4：字符串长度 (Length)
        // 业务需求：找出名字长度大于 5 的员工
        // linq2db 预期: LENGTH(e."Name") > 5 
        // ==========================================
        var lengthQuery = empQuery
            .Where(e => e.Name != null && e.Name.Length > 5)
            .Select(e => e.Name)
            .ToList();

        Assert.Single(lengthQuery);
        Assert.Equal("Charlie", lengthQuery[0]); // Charlie 长度是 7

        // ==========================================
        // 测试 5：大小写转换 (ToLower / ToUpper)
        // 业务需求：将名字转为全大写
        // linq2db 预期: UPPER(e."Name")
        // ==========================================
        var caseQuery = empQuery
            .Where(e => e.Name == "Alice")
            .Select(e => e.Name!.ToUpper())
            .ToList();

        Assert.Equal("ALICE", caseQuery[0]);

        // ==========================================
        // 测试 6：前后缀与包含 (StartsWith / EndsWith / Contains)
        // 业务需求：找以 D 开头，或者包含 'lic' 的名字
        // linq2db 预期: e."Name" LIKE 'D%' OR e."Name" LIKE '%lic%'
        // ==========================================
        var likeQuery = empQuery
            .Where(e => e.Name != null && (e.Name.StartsWith('D') || e.Name.Contains("lic")))
            .OrderBy(e => e.Name)
            .Select(e => e.Name)
            .ToList();

        Assert.Equal(2, likeQuery.Count);
        Assert.Equal("Alice", likeQuery[0]);
        Assert.Equal("David", likeQuery[1]);
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

        using var dfSales = DataFrame.From(sales);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var salesQuery = db.RegisterTable("sales", dfSales, sales);

        // ==========================================
        // 测试 1：字符串拼接 (+) 与 数学函数 (Math)
        // linq2db 预期: CONCAT(s."Category", ' - ', s."ProductName")
        // linq2db 预期: ROUND(ABS(s."Revenue") - s."Discount", 2)
        // ==========================================
        var scalarQuery = salesQuery.Select(s => new
        {
            // 字符串拼接
            FullName = s.Category + " - " + s.ProductName,
            // 嵌套的数学计算
            NetRevenue = Math.Round(Math.Abs(s.Revenue) - s.Discount, 2)
        }).ToList();

        Assert.Equal(4, scalarQuery.Count);
        Assert.Contains(scalarQuery, s => s.FullName == "Tech - Laptop" && s.NetRevenue == 950.5);
        Assert.Contains(scalarQuery, s => s.FullName == "Tech - Mouse" && s.NetRevenue == 20.0);

        // ==========================================
        // 测试 2：条件聚合 (Conditional Aggregation / Pivot)
        // 业务需求：在一次查询中，同时算出总营收，以及 Tech 和 Office 分别的营收
        // linq2db 预期: SUM(CASE WHEN s."Category" = 'Tech' THEN ABS(s."Revenue") ELSE 0 END)
        // ==========================================
        var aggQuery = salesQuery
            .GroupBy(s => 1) // 假分组，为了聚合全表
            .Select(g => new
            {
                Total = g.Sum(x => Math.Abs(x.Revenue)),
                // 核心：在 Sum 内部使用三元运算符！
                TechTotal = g.Sum(x => x.Category == "Tech" ? Math.Abs(x.Revenue) : 0),
                OfficeTotal = g.Sum(x => x.Category == "Office" ? Math.Abs(x.Revenue) : 0)
            }).ToList();

        Assert.Single(aggQuery);
        Assert.Equal(1671.5, aggQuery[0].Total); // 1000.5 + 20 + 500.2 + 150.8
        Assert.Equal(1020.5, aggQuery[0].TechTotal);
        Assert.Equal(651.0, aggQuery[0].OfficeTotal);
    }
    public record StockPrice(string Ticker, DateTime Date, double Price);

    [Fact]
    [Trait("Linq", "LeadLagAndNestedList")]
    public void Test_Polars_Linq_LeadLag_And_NestedList()
    {
        // 准备时间序列数据
        var stocks = new[]
        {
            new StockPrice("AAPL", new DateTime(2024, 1, 1), 150.0),
            new StockPrice("AAPL", new DateTime(2024, 1, 2), 155.0),
            new StockPrice("AAPL", new DateTime(2024, 1, 3), 152.0),
            new StockPrice("MSFT", new DateTime(2024, 1, 1), 300.0),
            new StockPrice("MSFT", new DateTime(2024, 1, 2), 305.0)
        };

        // 准备嵌套测试数据
        var depts = new[] { new DeptDto { DeptId = 1, DeptName = "Tech" }, new DeptDto { DeptId = 2, DeptName = "Sales" } };
        var emps = new[] { new EmpDto { Name = "Alice", DeptId = 1 }, new EmpDto { Name = "Bob", DeptId = 1 }, new EmpDto { Name = "Charlie", DeptId = 2 } };

        using var dfStocks = DataFrame.From(stocks);
        using var dfDepts = DataFrame.From(depts);
        using var dfEmps = DataFrame.From(emps);

        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var stockQuery = db.RegisterTable("stocks", dfStocks, stocks);
        var deptQuery = db.RegisterTable("departments", dfDepts, depts);
        var empQuery = db.RegisterTable("employees", dfEmps, emps);

        // ==========================================
        // 测试 1：高级窗口函数 (Lag - 获取上一行的值)
        // 业务需求：计算每只股票每天的涨跌幅差异
        // linq2db 预期: LAG(s."Price") OVER(PARTITION BY s."Ticker" ORDER BY s."Date")
        // ==========================================
        var lagQuery = from s in stockQuery
                       select new
                       {
                           s.Ticker,
                           s.Date,
                           s.Price,
                           // Sql.Ext.Lag() 提取上一行的价格
                           PrevPrice = LinqToDB.Sql.Ext.Lag(s.Price)
                                              .Over()
                                              .PartitionBy(s.Ticker)
                                              .OrderBy(s.Date)
                                              .ToValue()
                       };

        var lagResult = lagQuery.ToList();

        Assert.Equal(5, lagResult.Count);
        
        // AAPL 第一天没有上一行，PrevPrice 应该为空 (null 或 0，取决于底层映射)
        var aaplDay2 = lagResult.First(s => s.Ticker == "AAPL" && s.Date.Day == 2);
        Assert.Equal(155.0, aaplDay2.Price);
        Assert.Equal(150.0, aaplDay2.PrevPrice); // 成功拿到第一天的价格！

        // ==========================================
        // 测试 2：层级投影平铺化 (STRING_AGG)
        // 业务需求：返回每个部门，并且对象内部包含一个员工名字的拼接字符串
        // linq2db 预期：LEFT JOIN + GROUP BY + ARRAY_TO_STRING(ARRAY_AGG(...))
        // ==========================================
        var nestedQuery = from d in deptQuery
                          join e in empQuery on d.DeptId equals e.DeptId into empGroup
                          from e in empGroup.DefaultIfEmpty() // 关键：展开为 LEFT JOIN
                          group e by d.DeptName into g      // 按部门名称分组
                          select new
                          {
                              DeptName = g.Key,
                              Employees = g.ListAgg(x => x.Name)
                          };

        var nestedResult = nestedQuery.ToList();

        Assert.Equal(2, nestedResult.Count);
        
        var techDept = nestedResult.First(d => d.DeptName == "Tech");
        // Tech 部门应该拼接了 Alice 和 Bob
        Assert.Contains("Alice", techDept.Employees);
        Assert.Contains("Bob", techDept.Employees);

        var salesDept = nestedResult.First(d => d.DeptName == "Sales");
        // Sales 部门只有 Charlie
        Assert.Equal("Charlie", salesDept.Employees);
    }
    [Fact]
    [Trait("Linq", "UnifiedCRUD")]
    public void Test_Polars_Linq_Unified_CRUD_UX()
    {
        // 1. 准备数据并注册到 Polars (标准流程)
        var emps = new[]
        {
            new EmployeeSalary { Name = "Alice", DeptId = 1, Salary = 5000.0 },
            new EmployeeSalary { Name = "Bob",   DeptId = 2, Salary = 4000.0 },
            new EmployeeSalary { Name = "Eve",   DeptId = 3, Salary = 3000.0 }
        };

        using var dfEmps = DataFrame.From(emps);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        
        // 2. 极其优雅的上下文初始化
        var table = db.RegisterTable<EmployeeSalary>("employees", dfEmps);

        // ==========================================
        // 【R: 查询】 (完美支持复杂 LINQ)
        // ==========================================
        var richEmps = table.Where(e => e.Salary >= 5000).ToList();
        Assert.Single(richEmps);
        Assert.Equal("Alice", richEmps[0].Name);

        // ==========================================
        // 【U: 更新】 (直接调用，再也不报 Provider 错误了！)
        // 预期: Polars 抛出不支持 Update 的异常，但 SQL 会完美生成并送到 ExecuteNonQuery
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
        // 【D: 删除】 (Polars 官方隐藏特性)
        // ==========================================
        // 删掉 3 号部门的 Eve
        int deleted = table.Where(e => e.DeptId == 3).Delete();
        Assert.True(deleted >= 0);
    }
    public record StaffRecord(string name, int age, int salary);
    [Fact]
    [Trait("Linq", "LazyIO")]
    public void Test_Polars_Linq_Lazy_Csv_Scan_And_Pushdown()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_lazy_data.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            // 2. 【核心改变】：不要用 ReadCsv！用 ScanCsv 创建 LazyFrame！
            // 此时磁盘根本没有真正开始读数据，仅仅是创建了一个文件指针和逻辑计划
            using var schema = new PolarsSchema();
            schema.Add("age",DataType.Int32).Add("salary",DataType.Int32);

            using var lf = LazyFrame.ScanCsv(fileName,schema:schema);
            
            using var ctx = new SqlContext();
            using var db = new PolarsDataContext(ctx);

            // 4. 将 LazyFrame 注册到 SQL 上下文中
            var query = db.RegisterTable<StaffRecord>("employees", lf)
                          .Where(e => e.age > 30)
                          .Select(e => new 
                          { 
                              e.name, 
                              e.salary 
                          });

            // ====================================================================
            // 5. 见证奇迹的时刻：触发 .ToList() 
            // 链路：LINQ -> SQL -> Polars AST -> CsvReader (带列裁剪和谓词过滤)
            // ====================================================================
            var results = query.ToList();

            // 6. 验证结果
            Assert.NotNull(results);
            Assert.Equal(2, results.Count); // 只剩 Charlie 和 David

            // 验证 Charlie
            Assert.Equal("Charlie", results[0].name);
            Assert.Equal(70000.0, results[0].salary);

            // 验证 David
            Assert.Equal("David", results[1].name);
            Assert.Equal(80000.0, results[1].salary);
        }
        finally
        {
            // 清理文件
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    [Fact]
    [Trait("Linq", "SeriesScalar")]
    public void Test_Polars_Linq_Series()
    {
        // 1. 生成一个纯数字的 Series (1 到 100)
        using var series = Series.From("my_numbers", Enumerable.Range(1, 100).ToArray());
        
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        // 2. 极其优雅的注册！不需要 dummy data，不需要 DTO！
        // 注意看返回值，它直接就是 IQueryable<int>！
        IQueryable<int> query = db.RegisterSeries<int>(series);

        // 3. 纯正的标量 LINQ 语法！
        var results = query.Where(x => x > 90)
                           .OrderByDescending(x => x)
                           .ToList();

        // 验证
        Assert.Equal(10, results.Count);
        Assert.Equal(100, results[0]);
    }
    
    public record StaffRecordWithBonus(string name, int age, int salary,double bonus);
    [Fact]
    [Trait("Linq", "HybridLazy")]
    public void Test_Polars_Linq_Hybrid_Native_And_Linq_Pushdown()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_hybrid_lazy_data.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            using var schema = new PolarsSchema();
            schema.Add("age",DataType.Int32).Add("salary",DataType.Int32);

            // 2. ScanCsv 创建文件指针和基础逻辑计划
            using var lf = LazyFrame.ScanCsv(fileName,schema:schema);
            
            // ====================================================================
            // 【核心混写阶段 1：Polars 原生 API】
            // 我们用原生表达式加一个新列 "bonus"，逻辑是 salary 的 10%
            // ====================================================================
            using var lfWithBonus = lf.WithColumns((Col("salary") * 0.1).Alias("bonus"));

            using var db = new PolarsDataContext(Sql(),true);

            // ====================================================================
            // 【核心混写阶段 2：C# LINQ】
            // 将带有原生计划的 LazyFrame 注册进来，用 LINQ 继续编写业务逻辑！
            // ====================================================================
            var query = db.RegisterTable<StaffRecordWithBonus>("employees", lfWithBonus)
                          // LINQ 过滤：使用原有列和原生生成的列
                          .Where(e => e.age > 30 && e.bonus >= 7000.0) 
                          // LINQ 投影：在 LINQ 层再做一次计算
                          .Select(e => new 
                          { 
                              e.name, 
                              TotalCompensation = e.salary + e.bonus 
                          });

            // ====================================================================
            // 4. 终极点火：触发 .ToList() 
            // 引擎会将 Native Plan 和 LINQ SQL 合并为单一 AST，执行极致优化并读取磁盘
            // ====================================================================
            var results = query.ToList();

            // 5. 验证结果
            Assert.NotNull(results);
            Assert.Equal(2, results.Count); // 只剩 Charlie (35岁, bonus=7000) 和 David (40岁, bonus=8000)

            // 验证 Charlie
            Assert.Equal("Charlie", results[0].name);
            Assert.Equal(77000.0, results[0].TotalCompensation); // 70000 + 7000

            // 验证 David
            Assert.Equal("David", results[1].name);
            Assert.Equal(88000.0, results[1].TotalCompensation); // 80000 + 8000
        }
        finally
        {
            // 清理文件
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    [Fact]
    [Trait("Linq","Sandwich")]
    public void Test_Polars_Double_Hybrid_Sandwich()
    {
        using var db = new PolarsDataContext(Sql(),true);

        using var schema = new PolarsSchema();
        schema.Add("age",DataType.Int32).Add("salary",DataType.Int32);
        string path = "/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv";
        // --- 1. 底层 Native (IO 阶段) ---
        using var rawLf = LazyFrame.ScanCsv(path,schema:schema);
        
        var query = db.RegisterTable<StaffRecord>("emps", rawLf)
                      // --- 2. LINQ 阶段 (业务表达阶段) ---
                      .Where(e => e.salary > 5000)
                      .Select(e => new { e.name, e.salary });
        string plan1 = query.Explain(true);
        Console.WriteLine(plan1);
        // --- 3. 截胡！回到 Native (后处理阶段) ---
        // 注意：这行代码执行完，磁盘根本没有动！所有的计划全被缝合在一起了！
        using LazyFrame lfWithLinq = query.ToLazyFrame();

        // 继续使用 Polars 原生 API 做一些 LINQ 很难表达的操作
        // 比如求每一列的 null 数量，或者做 Rolling 窗口计算
        using var finalLf = lfWithLinq.WithColumns(Col("salary").Std().Alias("salary_std"));
        string plan2 = finalLf.Explain(true);
        Console.WriteLine(plan2);
        // 4. 终极点火
        using var df = finalLf.Collect();
        // df.Show();       
        Assert.True(df.Height > 0);
    }
    [Fact]
    [Trait("Linq", "SqlTranslator")]
    public void Test_PolarsSqlTranslator_Borrowing_Linq2db()
    {
        // ==========================================
        // 场景 A：单表达式翻译 (ToSql)
        // ==========================================
        string snippet1 = PolarsExpr.ToSql<StaffRecord, int>(e => (int)Math.Pow(e.salary, 2));
        
        Assert.Contains("Power(salary::Float, 2)", snippet1, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("t1.salary", snippet1); 

        // ==========================================
        // 场景 B：单列匿名类型 (ToSql)
        // ==========================================
        string snippet2 = PolarsExpr.ToSql<StaffRecord, object>(e => new { salary_sq = Math.Pow(e.salary, 2) });
        Assert.Contains("salary_sq", snippet2, StringComparison.OrdinalIgnoreCase);

        // ==========================================
        // 场景 C：多列联合计算！(ToSqls)
        // ==========================================
        string[] multiSnippets = PolarsExpr.ToSqls<StaffRecord, object>(e => new 
        { 
            salary_sq = Math.Pow(e.salary, 2),
            salary_dbl = e.salary * 2
        });

        // 断言安全切分成功
        Assert.Equal(2, multiSnippets.Length);
        Assert.Contains("AS \"salary_sq\"", multiSnippets[0], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AS \"salary_dbl\"", multiSnippets[1], StringComparison.OrdinalIgnoreCase);

        // ==========================================
        // 终极点火：多列白嫖文本转化为原生 Expr 并执行！
        // ==========================================
        using var df = DataFrame.FromColumns(new
        {
            salary = new[] { 10, 20, 30 }
        });

        using var resultDf = df.Select(multiSnippets.Select(SqlExpr).ToArray());
        resultDf.Show();
        
        var sqArr = resultDf["salary_sq"].ToArray<double>();
        var dblArr = resultDf["salary_dbl"].ToArray<int>(); // 注意：原列乘以2推断为int
        
        Assert.Equal(3, sqArr.Length);
        
        // 验证平方结果
        Assert.Equal(100.0, sqArr[0]); 
        Assert.Equal(900.0, sqArr[2]); 

        // 验证乘法结果
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
        // 1. 准备极简的数据源
        using var df = DataFrame.FromColumns(new
        {
            salary = new[] { 10.0, 20.0, 30.0 }
        });

        using var resultDf = df.Select(SqlExprs(
            PolarsExpr.ToSqls<SalaryRecord, object>(e => new 
            { 
                salary_sq = Math.Pow(e.salary, 2),  // 复杂数学函数翻译
                salary_dbl = e.salary * 2,          // 基础算术翻译
                is_high = e.salary > 15             // 逻辑判断翻译 (生成布尔列)
            })
        ));

        // 打印出来欣赏一下底层的完美类型推断 (f64, f64, bool)
        resultDf.Show();

        // 3. 终极断言验证
        var sqArr = resultDf["salary_sq"].ToArray<double>();
        var dblArr = resultDf["salary_dbl"].ToArray<double>(); // 浮点数乘法推断为 double
        var isHighArr = resultDf["is_high"].ToArray<bool>();

        Assert.Equal(3, sqArr.Length);

        // 验证第一行 (salary = 10)
        Assert.Equal(100.0, sqArr[0]);
        Assert.Equal(20.0, dblArr[0]);
        Assert.False(isHighArr[0]); // 10 不大于 15

        // 验证第三行 (salary = 30)
        Assert.Equal(900.0, sqArr[2]);
        Assert.Equal(60.0, dblArr[2]);
        Assert.True(isHighArr[2]);  // 30 大于 15
    }
    [Fact]
    [Trait("Linq", "Async")]
    public async Task Test_Polars_Linq_ToListAsync_Support()
    {
        // 1. 准备内存数据
        var users = new[]
        {
            new SimpleUser { Id = 1, Name = "Alice" },
            new SimpleUser { Id = 2, Name = "Bob" },
            new SimpleUser { Id = 3, Name = "Charlie" }
        };

        using var df = DataFrame.From(users);
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx, ownsContext: true);

        // 2. 注册表
        var table = db.RegisterTable<SimpleUser>("users", df);

        // 3. 见证奇迹的时刻：使用异步 LINQ 查询！
        var query = table.Where(u => u.Id > 1).OrderByDescending(u => u.Id);
        
        // 调用底层的异步方法，这会触发 ExecuteDbDataReaderAsync
        // 把 IQueryable 明确转成 IAsyncEnumerable，让微软的扩展方法接管
        var results = await query.AsAsyncEnumerable().ToListAsync();

        // 4. 验证结果
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
    [Trait("Linq", "Async_Stress")]
    public async Task Test_Polars_Linq_High_Concurrency_Async_Stress()
    {
        // ==============================================================
        // 1. 制造“弹药”：10 万条测试数据
        // ==============================================================
        int recordCount = 100_000;
        var mockData = Enumerable.Range(0, recordCount).Select(i => new TrafficRecord
        {
            Id = i,
            Region = $"Region_{i % 50}", // 50 个不同区域
            Latency = Random.Shared.NextDouble() * 100.0
        }).ToArray();

        // 构建核心只读 DataFrame，在并发中它是绝对线程安全的！
        using var df = DataFrame.From(mockData);

        // ==============================================================
        // 2. 定义并发 Worker（模拟单个 ASP.NET Core 请求的作用域）
        // ==============================================================
        async Task<int> SimulateWebRequestAsync(int workerId)
        {
            // 每次请求创建独立的沙箱上下文，绝不串号！
            using var ctx = new SqlContext();
            using var db = new PolarsDataContext(ctx);
            
            var table = db.RegisterTable<TrafficRecord>("traffic", df);

            // 每个人查询不同的区域
            string targetRegion = $"Region_{workerId % 50}";

            var query = table.Where(t => t.Region == targetRegion && t.Latency > 10.0)
                             .OrderBy(t => t.Id);

            // 【真正的异步触发点】
            // 如果你的 ExecuteDbDataReaderAsync 完美把 FFI 扔给了 Task.Run，
            // 这里的 await 会立刻释放 .NET 工作线程，绝不阻塞！
            var results = await query.AsAsyncEnumerable().ToListAsync();
            
            return results.Count;
        }

        // ==============================================================
        // 3. 点火！瞬间发射 100 个并发异步查询！
        // ==============================================================
        int concurrencyLevel = 100;
        var tasks = new List<Task<int>>();

        for (int i = 0; i < concurrencyLevel; i++)
        {
            tasks.Add(SimulateWebRequestAsync(i));
        }

        // 挂起等待所有 Rust 引擎的底层运算异步完成
        var finalResults = await Task.WhenAll(tasks);

        // ==============================================================
        // 4. 终极断言：不死锁、不崩溃、数据完全一致！
        // ==============================================================
        Assert.Equal(concurrencyLevel, finalResults.Length);

        foreach (var count in finalResults)
        {
            // 10万条数据分 50 个区，每个区 2000 条。
            // 加上 Latency > 10.0 的随机过滤，数量应该大于 0 且小于 2000
            Assert.True(count > 0 && count <= 2000);
        }
        
        Console.WriteLine($"[Polars.NET] 成功扛住了 {concurrencyLevel} 个并发 LINQ 查询！");
    }
    [Fact]
    [Trait("Linq", "Async_Stress_ToDataFrame")]
    public async Task Test_Polars_Linq_High_Concurrency_ToDataFrameAsync_Stress()
    {
        // ==============================================================
        // 1. 制造弹药：10 万条测试数据
        // ==============================================================
        int recordCount = 100_000;
        var mockData = Enumerable.Range(0, recordCount).Select(i => new
        {
            Id = i,
            Region = $"Region_{i % 50}", 
            Latency = Random.Shared.NextDouble() * 100.0
        }).ToArray();

        using var df = DataFrame.From(mockData);

        // ==============================================================
        // 2. 并发 Worker：直接返回原生的 DataFrame
        // ==============================================================
        async Task<long> SimulateDataFrameQueryAsync(int workerId)
        {
            using var ctx = new SqlContext();
            using var db = new PolarsDataContext(ctx);
            
            // 使用匿名类型的动态注册（假设你的 RegisterTable 支持）
            var table = db.RegisterTable("traffic", df, mockData);

            string targetRegion = $"Region_{workerId % 50}";

            var query = table.Where(t => t.Region == targetRegion && t.Latency > 10.0)
                             .OrderBy(t => t.Id);

            using DataFrame resultDf = await query.ToDataFrameAsync();
            
            // 拿到 df 后，可以直接无缝调用 Polars 原生 API
            return resultDf.Height; 
        }

        // ==============================================================
        // 3. 点火！瞬间发射 100 个并发计算任务！
        // ==============================================================
        int concurrencyLevel = 1000;
        var tasks = new List<Task<long>>();

        for (int i = 0; i < concurrencyLevel; i++)
        {
            tasks.Add(SimulateDataFrameQueryAsync(i));
        }

        // 挂起等待 100 个底层的 LazyCollect 并发完成
        var finalHeights = await Task.WhenAll(tasks);

        // ==============================================================
        // 4. 断言验证
        // ==============================================================
        Assert.Equal(concurrencyLevel, finalHeights.Length);

        foreach (var height in finalHeights)
        {
            // 数据绝对不会串号，且长度符合逻辑预估
            Assert.True(height > 0 && height <= 2000);
        }
        
        Console.WriteLine($"[Polars.NET] ToDataFrameAsync 成功扛住了 {concurrencyLevel} 个并发底层执行！");
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
        // ==========================================
        // 1. 准备包含负数、小数和日期的测试数据
        // ==========================================
        var sales = new[]
        {
            new SalesRecord { Id = 1, Category = "Tech", Revenue = -150.5, SaleDate = new DateTime(2023, 1, 15) },
            new SalesRecord { Id = 2, Category = "Tech", Revenue = 200.2, SaleDate = new DateTime(2023, 5, 20) },
            new SalesRecord { Id = 3, Category = "Food", Revenue = 99.9,  SaleDate = new DateTime(2024, 2, 10) }
        };

        using var dfSales = DataFrame.From(sales);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var table = db.RegisterTable("sales", dfSales, sales);

        // ==========================================
        // 测试 1：数学函数 (Abs, Round, Ceiling)
        // 预警：Math.Ceiling 极有可能生成 CEILING，但 Polars 官方只认 CEIL！
        // ==========================================
        var mathQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                Absolute = Math.Abs(x.Revenue),
                Rounded = Math.Round(x.Revenue),
                Ceiled = Math.Ceiling(x.Revenue) 
            }).ToList();

        Assert.Equal(3, mathQuery.Count);
        Assert.Equal(150.5, mathQuery[0].Absolute);
        Assert.Equal(-150.0, mathQuery[0].Rounded); // -150.5 round 到偶数通常是 -150，视底层实现而定
        Assert.Equal(201.0, mathQuery[1].Ceiled);   // 200.2 向上取整是 201

        // ==========================================
        // 测试 2：日期函数 (原生属性 vs 自定义扩展)
        // 预警：原生 x.SaleDate.Year 可能会生成奇怪的函数，我们需要对比 PolarsSql.Year
        // ==========================================
        var dateQuery = table
            .Where(x => x.SaleDate.Year == 2023)
            // 你也可以在这里试着加上 x.SaleDate.Year 看 linq2db 默认生成啥
            .Select(x => x.Id)
            .ToList();

        Assert.Equal(2, dateQuery.Count); // 只有 Id 1 和 2 是 2023 年
        Assert.Contains(1, dateQuery);
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
        // 准备数据：制造一些用于排名的同薪水数据 (Eve 和 Dave)
        var data = new[]
        {
            new WindowStatsRecord { DeptId = 1, EmpName = "Alice",   Salary = 5000 },
            new WindowStatsRecord { DeptId = 1, EmpName = "Bob",     Salary = 6000 },
            new WindowStatsRecord { DeptId = 1, EmpName = "Charlie", Salary = 7000 },
            new WindowStatsRecord { DeptId = 2, EmpName = "Dave",    Salary = 4000 },
            new WindowStatsRecord { DeptId = 2, EmpName = "Eve",     Salary = 4000 }, // 薪水与 Dave 相同
            new WindowStatsRecord { DeptId = 2, EmpName = "Frank",   Salary = 8000 }
        };

        using var dfData = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var table = db.RegisterTable("staff", dfData, data);

        // ==========================================
        // 测试 1：高级统计聚合 (Median / StdDev)
        // 验证我们手写的 [Sql.Function] 是否完美工作
        // ==========================================
        var statsQuery = table
            .GroupBy(x => x.DeptId)
            .Select(g => new
            {
                DeptId = g.Key,
                MedianSalary = g.Median(x => x.Salary),
                StdDevSalary = g.StdDev(x => x.Salary)
            })
            .OrderBy(x => x.DeptId)
            .ToList();

        Assert.Equal(2, statsQuery.Count);
        
        // Dept 1: 5000, 6000, 7000 -> 中位数 6000
        Assert.Equal(1, statsQuery[0].DeptId);
        Assert.Equal(6000.0, statsQuery[0].MedianSalary);
        Assert.True(statsQuery[0].StdDevSalary > 0); // 标准差应为 1000

        // ==========================================
        // 测试 2：极度硬核的窗口函数 (Window Functions)
        // 需求：在每个部门内部，按薪水降序排名。验证 ROW_NUMBER 和 RANK
        // linq2db 预期生成: ROW_NUMBER() OVER(PARTITION BY DeptId ORDER BY Salary DESC)
        // ==========================================
        var windowQuery = table
            .Select(x => new
            {
                x.DeptId,
                x.EmpName,
                x.Salary,
                // 生成 ROW_NUMBER (绝对行号)
                RowNum = LinqToDB.Sql.Ext.RowNumber().Over().PartitionBy(x.DeptId).OrderByDesc(x.Salary).ToValue(),
                // 生成 RANK (并列排名)
                Rank = LinqToDB.Sql.Ext.Rank().Over().PartitionBy(x.DeptId).OrderByDesc(x.Salary).ToValue()
            })
            .OrderBy(x => x.DeptId)
            .ThenByDescending(x => x.Salary)
            .ThenBy(x => x.EmpName)
            .ToList();

        Assert.Equal(6, windowQuery.Count);

        // 验证 Dept 1 排名
        var charlie = windowQuery.First(x => x.EmpName == "Charlie");
        Assert.Equal(1, charlie.RowNum);
        Assert.Equal(1, charlie.Rank);

        // 验证 Dept 2 排名 (Dave 和 Eve 薪水一样，RANK 应该并列，ROW_NUMBER 会递增)
        var frank = windowQuery.First(x => x.EmpName == "Frank");
        Assert.Equal(1, frank.Rank); // Frank 8000 第一名

        var dave = windowQuery.First(x => x.EmpName == "Dave");
        var eve = windowQuery.First(x => x.EmpName == "Eve");
        
        // Dave 和 Eve 都是 4000，RANK 必须并列第 2
        Assert.Equal(2, dave.Rank);
        Assert.Equal(2, eve.Rank);
        
        // 但 RowNum 必须是不重复的连续序号
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
        // 准备一组偶数个的完美测试数据
        var data = new[]
        {
            new StatRecord { GroupId = 1, Value = 10.0 },
            new StatRecord { GroupId = 1, Value = 20.0 },
            new StatRecord { GroupId = 1, Value = 30.0 },
            new StatRecord { GroupId = 1, Value = 40.0 }
        };

        using var dfData = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var table = db.RegisterTable("stats", dfData, data);

        // ==========================================
        // 核心测试：验证方差和两种分位数算法
        // linq2db 预期生成: 
        // VARIANCE(x.Value), QUANTILE_CONT(x.Value, 0.5), QUANTILE_DISC(x.Value, 0.5)
        // ==========================================
        var statsQuery = table
            .GroupBy(x => x.GroupId)
            .Select(g => new
            {
                GroupId = g.Key,
                // 1. 方差 (Variance)
                Var = PolarsSql.Variance(g, x => x.Value),
                // 2. 连续分位数 P50 (会插值)
                Q50_Cont = PolarsSql.QuantileCont(g, x => x.Value, 0.5),
                // 3. 离散分位数 P50 (不插值，必须是原始数据中的一个)
                Q50_Disc = PolarsSql.QuantileDisc(g, x => x.Value, 0.5),
                // 4. 高级 P99 测算
                Q99_Cont = PolarsSql.QuantileCont(g, x => x.Value, 0.99)
            })
            .ToList();

        Assert.Single(statsQuery);
        var result = statsQuery[0];

        // 验证方差
        // {10, 20, 30, 40} 的均值是 25。
        // 样本方差 (ddof=1) = ((15^2) + (5^2) + (5^2) + (15^2)) / 3 = (225+25+25+225)/3 = 500/3 ≈ 166.666...
        Assert.Equal(166.6666, result.Var, precision: 3);

        // 验证连续型分位数 (0.5 中位数) -> 线性插值，应该是 (20 + 30) / 2 = 25.0
        Assert.Equal(25.0, result.Q50_Cont);

        // 验证离散型分位数 (0.5 中位数) -> 必须是实际值，一般 Polars 会取 lower (20) 或 nearest (视实现)
        Assert.True(result.Q50_Disc == 20.0 || result.Q50_Disc == 30.0, $"Actual Q50_Disc was {result.Q50_Disc}");
        Assert.NotEqual(25.0, result.Q50_Disc); // 绝对不能是插值出来的 25！

        // 验证 P99 -> 接近最大值 40.0
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
        // 准备测试数据
        // 5  的二进制: 0101
        // 3  的二进制: 0011
        // 12 的二进制: 1100
        // 10 的二进制: 1010
        var data = new[]
        {
            new BitwiseRecord { Id = 1, A = 5,  B = 3 },
            new BitwiseRecord { Id = 2, A = 12, B = 10 }
        };

        using var df = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var table = db.RegisterTable("bitwise_tb", df, data);

        // ==========================================
        // 测试：C# 原生位运算符
        // 预警：看看 linq2db 会生成符号还是函数，以及 Polars 认不认
        // ==========================================
        var bitQuery = table
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
                // CountResult = PolarsSql.BitCount(x.A)
            })
            .ToList();

        Assert.Equal(2, bitQuery.Count);

        // 验证 5 和 3 
        // 5 & 3 = 1 (0001)
        // 5 | 3 = 7 (0111)
        // 5 ^ 3 = 6 (0110)
        // ~5    = -6 (按位取反，带符号)
        var row1 = bitQuery[0];
        Assert.Equal(1, row1.AndResult);
        Assert.Equal(7, row1.OrResult);
        Assert.Equal(6, row1.XorResult);
        Assert.Equal(~5, row1.NotResult); 

        // 验证 12 和 10
        // 12 & 10 = 8 (1000)
        // 12 | 10 = 14 (1110)
        // 12 ^ 10 = 6 (0110)
        // ~12     = -13
        var row2 = bitQuery[1];
        Assert.Equal(8, row2.AndResult);
        Assert.Equal(14, row2.OrResult);
        Assert.Equal(6, row2.XorResult);
        Assert.Equal(~12, row2.NotResult);
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
        // 准备测试数据
        var data = new[]
        {
            new TemporalRecord { Id = 1, EventTime = new DateTime(2024, 3, 15, 14, 30, 0) },
            new TemporalRecord { Id = 2, EventTime = new DateTime(2025, 12, 1, 9, 15, 45) }
        };

        using var df = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var table = db.RegisterTable("temporal_tb", df, data);

        // ==========================================
        // 核心测试：C# 原生 DateTime 属性与方法
        // ==========================================
        var timeQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                // 测试 EXTRACT / DATE_PART
                Year = x.EventTime.Year,
                Month = x.EventTime.Month,
                Day = x.EventTime.Day,
                Hour = x.EventTime.Hour,
                
                // 测试 STRFTIME 格式化
                // 预警：.NET 的 "yyyy-MM-dd" 和 C 语言风格的 "%Y-%m-%d" 完全不一样！
                FormattedDate = x.EventTime.ToString("yyyy-MM-dd")
            })
            .ToList();

        Assert.Equal(2, timeQuery.Count);
        
        // 验证第一行解析结果
        Assert.Equal(2024, timeQuery[0].Year);
        Assert.Equal(3, timeQuery[0].Month);
        Assert.Equal(15, timeQuery[0].Day);
        Assert.Equal(14, timeQuery[0].Hour);
        // 如果 FormattedDate 居然跑通了，我们可以打印出来看看是啥神仙操作
        Assert.StartsWith("2024", timeQuery[0].FormattedDate);
    }
    public class StringNativeRecord
    {
        public int Id { get; set; }
        public string Text1 { get; set; } = "";
        public string Text2 { get; set; } = "";
    }

    [Fact]
    [Trait("Linq", "String")]
    public void Test_Polars_Linq_Native_String_Functions()
    {
        // 准备测试数据，故意加一些空格用来测试 Trim
        var data = new[]
        {
            new StringNativeRecord { Id = 1, Text1 = "  Hello  ", Text2 = "World" },
            new StringNativeRecord { Id = 2, Text1 = "Polars",    Text2 = "Data" }
        };

        using var df = DataFrame.From(data);
        using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
        var table = db.RegisterTable("string_tb", df, data);

        // ==========================================
        // 核心测试：C# 原生字符串方法大阅兵
        // 看看 linq2db 会生成哪些 Polars 函数
        // ==========================================
        var strQuery = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                // 测试 CONCAT (或 || 运算符)
                ConcatStr = x.Text1 + " " + x.Text2,
                
                // 测试 LTRIM 和 RTRIM
                LTrimStr = x.Text1.TrimStart(),
                RTrimStr = x.Text1.TrimEnd(),
                
                // 测试 REPLACE
                ReplaceStr = x.Text2.Replace("r", "x"),
                
                // 测试 STRPOS
                // 究极预警：C# 的 IndexOf 是从 0 开始的！SQL 的 STRPOS 通常是从 1 开始的！
                // linq2db 会极其聪明地在 SQL 里帮你自动减 1 吗？
                PosStr = x.Text2.IndexOf('o') 
            })
            .ToList();

        Assert.Equal(2, strQuery.Count);
        
        // 验证第一行: "  Hello  ", "World"
        var row1 = strQuery[0];
        Assert.Equal("  Hello   World", row1.ConcatStr);
        Assert.Equal("Hello  ", row1.LTrimStr);
        Assert.Equal("  Hello", row1.RTrimStr);
        Assert.Equal("Woxld", row1.ReplaceStr); // World -> Woxld
        Assert.Equal(1, row1.PosStr); // 'o' 在 "World" 的索引是 1 (W=0, o=1)

        // 验证第二行: "Polars", "Data"
        var row2 = strQuery[1];
        Assert.Equal("Polars Data", row2.ConcatStr);
        Assert.Equal("Polars", row2.LTrimStr);
        Assert.Equal("Polars", row2.RTrimStr);
        Assert.Equal("Data", row2.ReplaceStr); // 没有 'r'，保持不变
        Assert.Equal(-1, row2.PosStr); // 找不到 'o'，C# 期望返回 -1！SQL 通常返回 0。看 linq2db 怎么填坑！
    }
    [Fact]
    [Trait("Linq", "ControlFlow")]
    public void Test_Polars_Linq_Native_Control_Flow_Functions()
    {       
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        // 构造一点假数据
        var mockData = new[] 
        { 
            new { Id = 1, Val1 = (int?)10, Val2 = (int?)20 },
            new { Id = 2, Val1 = (int?)null, Val2 = (int?)30 },
            new { Id = 3, Val1 = (int?)40, Val2 = (int?)40 }
        };
        
        var table = db.RegisterTable("math_tb", DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                
                // 1. COALESCE 测试：C# 的 ?? 运算符
                CoalesceVal = x.Val1 ?? x.Val2 ?? 0,
                
                // 2. GREATEST 测试：C# 的 Math.Max
                // 注意：如果 Val1 和 Val2 是可空类型，Math.Max 可能需要显式 .Value 或者强转
                MaxVal = Math.Max(x.Val1 ?? 0, x.Val2 ?? 0),
                
                // 3. LEAST 测试：C# 的 Math.Min
                MinVal = Math.Min(x.Val1 ?? 0, x.Val2 ?? 0),
                
                // 4. IF 测试：C# 的三元运算符
                IfStr = x.Val1 > 15 ? "Big" : "Small",
                
                // 5. NULLIF 测试：C# 的条件判断逻辑
                // 如果两值相等返回 null，否则返回第一个值
                NullIfVal = x.Val1 == x.Val2 ? null : x.Val1
            })
            .ToList(); // 触发底层的 ToSql 和 执行

        // 如果能活着运行到这里，说明 Polars 完美消化了生成的 SQL！
        Assert.Equal(3, query.Count);
        
        // 可以稍微验证一下逻辑
        Assert.Equal(30, query[1].CoalesceVal); // null ?? 30 = 30
        Assert.Equal(20, query[0].MaxVal);      // Max(10, 20) = 20
        Assert.Null(query[2].NullIfVal);        // 40 == 40 -> null        
    }
    [Fact]
    [Trait("Linq", "MathTrig")]
    public void Test_Polars_Linq_Native_Math_Trig_Functions()
    {
        using var ctx = new SqlContext();
        using var db = new PolarsDataContext(ctx);

        // 构造安全范围内的测试数据，避免 Acos/Asin 溢出 (-1 <= V1 <= 1)
        var mockData = new[] 
        { 
            new { Id = 1, V1 = 0.5, V2 = 1.0 },
            new { Id = 2, V1 = 0.0, V2 = -0.5 },
            new { Id = 3, V1 = -1.0, V2 = 0.5 }
        };
        
        var table = db.RegisterTable("trig_tb", DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                // 标准三角函数 (期望被翻译为 SIN, COS, TAN)
                SinVal = Math.Sin(x.V1),
                CosVal = Math.Cos(x.V1),
                TanVal = Math.Tan(x.V1),
                
                // 反三角函数 (期望被翻译为 ASIN, ACOS, ATAN)
                AsinVal = Math.Asin(x.V1),
                AcosVal = Math.Acos(x.V1),
                AtanVal = Math.Atan(x.V1),
                
                // 双参数反正切 (期望被翻译为 ATAN2)
                Atan2Val = Math.Atan2(x.V1, x.V2)
            })
            .ToList();

        Assert.Equal(3, query.Count);
        
        // 简单验证第一行的 Sin(0.5)
        Assert.True(Math.Abs(query[0].SinVal - Math.Sin(0.5)) < 1e-6);

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
        
        var table = db.RegisterTable("polars_math_tb", DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                
                // 1. 度数与弧度转换
                DegVal = PolarsSql.Degrees(x.Rad),
                RadVal = PolarsSql.Radians(x.Deg),
                
                // 2. 基于角度的三角函数
                SindVal = PolarsSql.Sind(x.Deg),
                CosdVal = PolarsSql.Cosd(x.Deg),
                TandVal = PolarsSql.Tand(x.Deg),
                
                // 3. 余切
                CotVal = PolarsSql.Cot(x.Rad),
                CotdVal = PolarsSql.Cotd(x.Deg),
                
                // 4. 基于角度的反三角函数 (输入必须在 [-1, 1])
                AsindVal = PolarsSql.Asind(x.Ratio), 
                AcosdVal = PolarsSql.Acosd(x.Ratio),
                AtandVal = PolarsSql.Atand(x.Ratio),
                
                // 5. 双参数基于角度的反正切
                Atan2dVal = PolarsSql.Atan2d(x.Y, x.X)
            })
            .ToList();

        Assert.Equal(2, query.Count);
        
        // 简单验证第一行的 Sind(30度) 应该等于 0.5 (允许微小的浮点数误差)
        Assert.True(Math.Abs(query[0].SindVal - 0.5) < 1e-6);

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
        
        var table = db.RegisterTable("math_gen_tb", DataFrame.From(mockData), mockData);

        var query = table
            .OrderBy(x => x.Id)
            .Select(x => new
            {
                x.Id,
                
                // 1. 基础运算
                AbsVal = Math.Abs(x.V2 * -1),                    // ABS (原生支持完美，继续用)
                ModVal = PolarsSql.Mod(x.IntVal, x.Divisor),     // 强制生成 MOD(a, b)，完美避开 LinqToDB 的迷之 decimal 强转
                DivOpVal = x.IntVal / x.Divisor,                 // 原生整除 (/)
                DivFuncVal = PolarsSql.Div(x.IntVal, x.Divisor), // 强制生成 DIV(a, b) 函数
                
                // 2. 取整与截断
                CeilVal = PolarsSql.Ceil(x.V1 + 0.5),            // 强制生成 CEIL，避免 LinqToDB 翻成 CEILING
                FloorVal = Math.Floor(x.V1 + 0.5),               // FLOOR (原生支持完美，继续用)
                RoundVal = PolarsSql.Round(x.V1 + 0.54, 1),      // 拯救被 LinqToDB 彻底吃掉的 Math.Round
                
                // 3. 幂与根
                PowVal = Math.Pow(x.V1, x.V2),              // 强制生成 POW，避免原生的 POWER
                SqrtVal = Math.Sqrt(x.V1),                       // SQRT (原生支持完美)
                CbrtVal = PolarsSql.Cbrt(x.V1),                  // 拯救被 LinqToDB 完全忽略的 Math.Cbrt
                ExpVal = Math.Exp(x.V1),                         // EXP (原生支持完美)
                
                // 4. 对数家族
                LnVal = Math.Log(x.V1),                          // LN (C# 的单参数 Log 会被完美翻译为 Ln，继续用)
                Log10Val = PolarsSql.Log10(x.V1),                // 拯救被误翻为 Log(x) 的 Math.Log10
                Log2Val = PolarsSql.Log2(x.V1),                  // 拯救被误翻为 Log(2, x) 的 Math.Log2
                Log1pVal = PolarsSql.Log1p(x.V1),                // LOG1P (原生压根没有，走专属扩展)

                // 5. 其他
                SignVal = Math.Sign(x.V1 - 5.0),                 // SIGN (原生支持完美)
                
                PiFunc = PolarsSql.Pi()                          // 强制翻译成优雅的 PI() 函数
            });

        var df = query.ToDataFrame();
        Console.WriteLine(query.ToSqlString());
        df.Show();
        
        Assert.Equal(18L, df.Width);
        // Assert.Single(query);
    }
}