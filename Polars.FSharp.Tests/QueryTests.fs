namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.NET.Linq
open Polars.FSharp
open LinqToDB
open System

type Person = {Name: string;Age: int;Sales: float}
type Department = { DeptId: int; DeptName: string }
type Employee = { Name: string; DeptId: int }
type EmpDeptDto = { EmpName: string; DepartmentName: string }
type EmployeeSalary = { Name: string; DeptId: int; Salary: float }
type DeptStatsDto = { DeptId: int; TotalSalary: float; EmployeeCount: int }
type OrderDto = { OrderId: int; OrderDate: DateTime; Region: string; Revenue: float }
type ProductDto = { Id: int; Name: string; Category: string; Price: float }

module QueryTests =

    [<Fact>]
    [<Trait("Linq", "Where")>]
    let ``Test Polars FSharp Linq Where And OrderBy`` () =
        let data = [|
            { Name = "Alice";   Age = 25; Sales = 100.0 }
            { Name = "Bob";     Age = 30; Sales = 200.0 }
            { Name = "Charlie"; Age = 35; Sales = 300.0 }
            { Name = "David";   Age = 18; Sales = 50.0 }
        |]

        use df = DataFrame.ofRecords data

        use db = new PolarsDataContext(pl.sqlContext(),true)

        let ageLimit = 20
        let excludeName = "Alice"

        let table = db.RegisterTable<Person>("people", df)

        let queryable = 
            query {
                for p in table do
                where (p.Age > ageLimit && p.Name <> excludeName) 
                sortByDescending p.Sales
                select p
            }

        let results = queryable.ToArray()

        Assert.NotNull results
        Assert.Equal(2, results.Length) 

        Assert.Equal("Charlie", results.[0].Name)
        Assert.Equal(35, results.[0].Age)
        Assert.Equal(300.0, results.[0].Sales)

        Assert.Equal("Bob", results.[1].Name)
        Assert.Equal(30, results.[1].Age)
        Assert.Equal(200.0, results.[1].Sales)

    [<Fact>]
    [<Trait("Linq", "Join")>]
    let ``Test Polars Linq Inner Join`` () =
        // Arrange: 准备数据
        let depts = [|
            { DeptId = 1; DeptName = "Engineering" }
            { DeptId = 2; DeptName = "Sales" }
        |]

        let emps = [|
            { Name = "Alice"; DeptId = 1 }
            { Name = "Bob"; DeptId = 2 }
            { Name = "Charlie"; DeptId = 1 }
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        use ctx = new SqlContext()
        use db = new PolarsDataContext(ctx)

        let deptQuery = db.RegisterTable<Department>("departments", dfDepts)
        let empQuery = db.RegisterTable<Employee>("employees", dfEmps)

        // Act: F# 原生的 LINQ Join 语法
        let queryable = 
            query {
                for e in empQuery do
                join d in deptQuery on (e.DeptId = d.DeptId)
                where (e.Name <> "Bob")
                sortBy d.DeptName
                thenBy e.Name
                select { EmpName = e.Name; DepartmentName = d.DeptName }
            }

        // 触发物化
        let results = queryable.ToList()

        // Assert: 验证结果
        Assert.Equal(2, results.Count)
        
        // 应该只有 Alice 和 Charlie (因为 Bob 被过滤了)，且按部门名称、姓名排序
        Assert.Equal("Alice", results.[0].EmpName)
        Assert.Equal("Engineering", results.[0].DepartmentName)

        Assert.Equal("Charlie", results.[1].EmpName)
        Assert.Equal("Engineering", results.[1].DepartmentName)

    [<Fact>]
    [<Trait("Linq", "GroupByHaving")>]
    let ``Test Polars Linq GroupBy Aggregation With Having`` () =
        // Arrange: 准备数据
        let emps = [|
            { Name = "Alice";   DeptId = 1; Salary = 5000.0 }
            { Name = "Bob";     DeptId = 2; Salary = 4000.0 }
            { Name = "Charlie"; DeptId = 1; Salary = 6000.0 }
            { Name = "David";   DeptId = 2; Salary = 4500.0 }
            { Name = "Eve";     DeptId = 3; Salary = 3000.0 }
        |]

        use dfEmps = DataFrame.ofRecords emps

        use ctx = new SqlContext()
        use db = new PolarsDataContext(ctx)
        
        let empQuery = db.RegisterTable<EmployeeSalary>("employees_salary", dfEmps)

        // Act: F# 原生的 LINQ GroupBy + Having 语法
        // 预期 SQL: GROUP BY e."DeptId" HAVING SUM(e."Salary") > 5000
        let queryable = 
            query {
                for e in empQuery do
                groupBy e.DeptId into g
                
                where (g.Sum(fun x -> x.Salary) > 5000.0)
                
                sortBy g.Key
                
                select {
                    DeptId = g.Key
                    TotalSalary = g.Sum(fun x -> x.Salary)
                    EmployeeCount = g.Count()
                }
            }

        // 触发物化：生成并执行 SQL
        let results = queryable.ToList()

        // Assert: 验证聚合和过滤结果
        // 一共 3 个部门，但 Dept 3 (3000) 被 HAVING 过滤掉了，只剩下 2 个
        Assert.Equal(2, results.Count) 

        // 验证 Dept 1 (Alice + Charlie)
        Assert.Equal(1, results.[0].DeptId)
        Assert.Equal(11000.0, results.[0].TotalSalary) // 5000 + 6000
        Assert.Equal(2, results.[0].EmployeeCount)

        // 验证 Dept 2 (Bob + David)
        Assert.Equal(2, results.[1].DeptId)
        Assert.Equal(8500.0, results.[1].TotalSalary) // 4000 + 4500
        Assert.Equal(2, results.[1].EmployeeCount)
        
        // 确保 Dept 3 不存在
        let hasDept3 = results |> Seq.exists (fun r -> r.DeptId = 3)
        Assert.False hasDept3
    [<Fact>]
    [<Trait("Linq", "ScalarAndFirst")>]
    let ``Test Polars Linq Scalar And First`` () =
        // Arrange: 准备 F# 匿名记录数据 (Anonymous Records)
        let data = [|
            {| Id = 1; Name = "Alice"; Score = 80 |}
            {| Id = 2; Name = "Bob";   Score = 90 |}
            {| Id = 3; Name = "Charlie"; Score = 85 |}
        |]

        use df = DataFrame.ofRecords data

        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        
        // 注册表，沿用你巧妙的类型推断重载
        let table = db.RegisterTable("students", df, data)

        // ==========================================
        // 测试 1：标量聚合 (Scalar Aggregation)
        // 预期 SQL: SELECT COUNT(*) FROM students WHERE Score > 82
        // ==========================================
        // F# 原生语法使用 count 关键字：
        let highScorersCount = 
            query {
                for s in table do
                where (s.Score > 82)
                count
            }
        Assert.Equal(2, highScorersCount)

        // ==========================================
        // 测试 2：求最大值 (Max)
        // 预期 SQL: SELECT MAX(Score) FROM students
        // ==========================================
        // F# 原生语法使用 maxBy 关键字：
        let maxScore = 
            query {
                for s in table do
                maxBy s.Score
            }
        Assert.Equal(90, maxScore)

        // ==========================================
        // 测试 3：单行查询 (First / head)
        // 预期 SQL: SELECT Id, Name, Score FROM students WHERE Score > 82 ORDER BY Score DESC LIMIT 1
        // ==========================================
        // F# 原生语法使用 head 关键字 (对应 First)：
        let topStudent = 
            query {
                for s in table do
                where (s.Score > 82)
                sortByDescending s.Score
                head
            }
            
        // 注意：F# 匿名记录默认是引用类型，这里用 box 包装给 Assert
        Assert.NotNull(box topStudent)
        Assert.Equal("Bob", topStudent.Name)
        Assert.Equal(90, topStudent.Score)

    [<Fact>]
    [<Trait("Linq", "LeftJoin")>]
    let ``Test Polars Linq Left Join`` () =
        // Arrange: 使用原有的 Department 和 Employee 结构
        let depts = [|
            { DeptId = 1; DeptName = "Engineering" }
            { DeptId = 2; DeptName = "Sales" }
            { DeptId = 3; DeptName = "HR" } // 注意：HR 部门没有员工！
        |]

        let emps = [|
            { Name = "Alice"; DeptId = 1 }
            { Name = "Bob";   DeptId = 2 }
            { Name = "Charlie"; DeptId = 1 }
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)

        let deptQuery = db.RegisterTable<Department>("departments", dfDepts)
        let empQuery = db.RegisterTable<Employee>("employees", dfEmps)

        // Act: F# 原生的 Left Outer Join 语法
        let queryable = 
            query {
                for d in deptQuery do
                // 【核心】：使用 leftOuterJoin 关键字！
                // 语法糖：它会自动处理 into 分组，你只需要 DefaultIfEmpty 展开它
                leftOuterJoin e in empQuery on (d.DeptId = e.DeptId) into empGroup
                for e in empGroup.DefaultIfEmpty() do
                sortBy d.DeptId
                thenBy e.Name
                
                // 再次利用 F# 的匿名记录，极其干净
                select {|
                    DeptName = d.DeptName

                    EmployeeName = if box e = null then "NO_EMPLOYEE" else e.Name
                |}
            }

        let results = queryable.ToList()

        // Assert
        // Engineering有2人，Sales有1人，HR有0人(Left Join 保留)，共4条
        Assert.Equal(4, results.Count)

        // 验证 Engineering 部门 (Alice, Charlie)
        Assert.Equal("Engineering", results.[0].DeptName)
        Assert.Equal("Alice", results.[0].EmployeeName)
        
        Assert.Equal("Engineering", results.[1].DeptName)
        Assert.Equal("Charlie", results.[1].EmployeeName)

        // 验证 Sales 部门 (Bob)
        Assert.Equal("Sales", results.[2].DeptName)
        Assert.Equal("Bob", results.[2].EmployeeName)

        // 验证 HR 部门 (无匹配员工，触发空值处理)
        Assert.Equal("HR", results.[3].DeptName)
        Assert.Equal("NO_EMPLOYEE", results.[3].EmployeeName)
    [<Fact>]
    [<Trait("Linq", "UnionAndCrossJoin")>]
    let ``Test Polars Linq Union And Cross Join`` () =
        // Arrange: 准备匿名记录数据
        let depts = [|
            {| DeptId = 1; DeptName = "Engineering" |}
            {| DeptId = 2; DeptName = "Sales" |}
        |]

        let emps = [|
            {| Name = "Alice"; DeptId = 1 |}
            {| Name = "Bob";   DeptId = 2 |}
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        
        // 注册表
        let deptQuery = db.RegisterTable("departments", dfDepts, depts)
        let empQuery = db.RegisterTable("employees", dfEmps, emps)

        // ==========================================
        // 测试 1：交叉连接 (Cross Join / Cartesian Product)
        // 语法：连续使用多个 for
        // 预期 SQL: SELECT ... FROM departments d CROSS JOIN employees e
        // ==========================================
        let crossJoinQuery = 
            query {
                for d in deptQuery do
                for e in empQuery do
                select {| DeptName = d.DeptName; Name = e.Name |}
            }

        let crossResult = crossJoinQuery.ToList()
        
        // 2 个部门 * 2 个员工 = 4 条记录
        Assert.Equal(4, crossResult.Count)
        
        // F# 中推荐用 Seq.exists 结合 Assert.True 来替代 C# 的 Assert.Contains(collection, predicate)
        Assert.True(crossResult |> Seq.exists(fun x -> x.DeptName = "Engineering" && x.Name = "Alice"))
        Assert.True(crossResult |> Seq.exists(fun x -> x.DeptName = "Sales" && x.Name = "Bob"))

        // ==========================================
        // 测试 2：集合拼接 (Concat -> UNION ALL)
        // 预期 SQL: SELECT ... FROM employees WHERE ... UNION ALL SELECT ... FROM employees WHERE ...
        // ==========================================
        let query1 = query { for e in empQuery do where (e.DeptId = 1); select e }
        let query2 = query { for e in empQuery do where (e.DeptId = 2); select e }
        
        // Concat 对应 UNION ALL (不去重), Union 对应 UNION (去重)
        let unionResult = query1.Concat(query2).ToList()

        Assert.Equal(2, unionResult.Count)
        Assert.True(unionResult |> Seq.exists(fun x -> x.Name = "Alice"))
        Assert.True(unionResult |> Seq.exists(fun x -> x.Name = "Bob"))

        // ==========================================
        // 测试 3：内置函数映射 (String / Math Functions)
        // 预期: F# 的 ToUpper() 被成功翻译为 SQL 的 UPPER()
        // ==========================================
        let upperResult = 
            query {
                for e in empQuery do
                select (e.Name.ToUpper())
            }
            // F# 管道符风格：转为强类型 List
            |> Seq.toList 
        
        Assert.Contains("ALICE", upperResult)
        Assert.Contains("BOB", upperResult)
    [<Fact>]
    [<Trait("Linq", "AdvancedSetsAndLet")>]
    let ``Test Polars Linq Except Intersect And Let`` () =
        // Arrange
        let emps = [|
            { Name = "Alice";   DeptId = 1; Salary = 6000.0 }
            { Name = "Bob";     DeptId = 2; Salary = 4000.0 }
            { Name = "Charlie"; DeptId = 1; Salary = 4500.0 }
            { Name = "David";   DeptId = 3; Salary = 8000.0 }
            { Name = "Eve";     DeptId = 2; Salary = 5500.0 }
        |]

        use dfEmps = DataFrame.ofRecords emps
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        let empQuery = db.RegisterTable<EmployeeSalary>("employees", dfEmps)

        // ==========================================
        // 测试 1：交集 (Intersect)
        // 找出：既是 1 部门，又薪水大于 4000 的员工
        // 预期 SQL: SELECT ... INTERSECT SELECT ...
        // ==========================================
        // F# 中同样可以把基础查询拆分为变量，再利用 System.Linq 组合
        let q1 = query { for e in empQuery do where (e.DeptId = 1); select e }
        let q2 = query { for e in empQuery do where (e.Salary > 4000.0); select e }
        
        let intersectResult = q1.Intersect(q2).ToList()
        
        Assert.Equal(2, intersectResult.Count) // Alice 和 Charlie
        Assert.True(intersectResult |> Seq.exists (fun e -> e.Name = "Alice"))
        Assert.True(intersectResult |> Seq.exists (fun e -> e.Name = "Charlie"))

        // ==========================================
        // 测试 2：差集 (Except)
        // 找出：薪水大于 4000，但【排除】1 部门的员工
        // 预期 SQL: SELECT ... EXCEPT SELECT ...
        // ==========================================
        let exceptResult = q2.Except(q1).ToList()
        
        Assert.Equal(2, exceptResult.Count) // David (8000, 3) 和 Eve (5500, 2)
        // C#的 DoesNotContain 在 F# 里就是 Seq.exists 取反 (或直接 Assert.False)
        Assert.False(exceptResult |> Seq.exists (fun e -> e.DeptId = 1))

        // ==========================================
        // 测试 3：Let 关键字与派生计算
        // 场景：计算年终奖（薪水 * 1.5），并筛选出年终奖大于 8000 的人
        // ==========================================
        let letQuery = 
            query {
                for e in empQuery do

                let bonus = e.Salary * 1.5 
                
                where (bonus > 8000.0)
                
                select {| 
                    Name = e.Name
                    Bonus = bonus 
                |}
            }

        let letResult = letQuery.ToList()

        // 只有 Alice (9000), David (12000), Eve (8250) 的年终奖 > 8000
        Assert.Equal(3, letResult.Count)
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "Alice" && x.Bonus = 9000.0))
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "David" && x.Bonus = 12000.0))
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "Eve" && x.Bonus = 8250.0))
    [<Fact>]
    [<Trait("Linq", "WindowFunctions")>]
    let ``Test Polars Linq Window Functions`` () =
        // Arrange: 准备数据
        let emps = [|
            {| Name = "Alice";   DeptId = 1; Salary = 6000.0 |}
            {| Name = "Bob";     DeptId = 2; Salary = 4000.0 |}
            {| Name = "Charlie"; DeptId = 1; Salary = 4500.0 |}
            {| Name = "David";   DeptId = 3; Salary = 8000.0 |}
            {| Name = "Eve";     DeptId = 2; Salary = 5500.0 |}
        |]

        use dfEmps = DataFrame.ofRecords emps
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        let empQuery = db.RegisterTable("employees", dfEmps, emps)

        // ==========================================
        // 测试：窗口函数 (Window Functions)
        // 业务场景：计算每个人在自己部门内的薪水排名，以及该部门的总薪水
        // ==========================================
        let queryable = 
            query {
                for e in empQuery do
                select {|
                    Name = e.Name
                    DeptId = e.DeptId
                    Salary = e.Salary
                    
                    // 1. 排名窗口函数: RANK() OVER (PARTITION BY DeptId ORDER BY Salary DESC)
                    // F# 中同样完美支持这种链式调用，完全内联在 select 里
                    DeptRank = LinqToDB.Sql.Ext.Rank().Over().PartitionBy(e.DeptId).OrderByDesc(e.Salary).ToValue()
                    
                    // 2. 聚合窗口函数: SUM(Salary) OVER (PARTITION BY DeptId)
                    DeptTotalSalary = LinqToDB.Sql.Ext.Sum(e.Salary).Over().PartitionBy(e.DeptId).ToValue()
                |}
            }

        let results = queryable.ToList()

        // Assert
        Assert.Equal(5, results.Count)

        // 定义一个小 helper，用 Seq.find 替代 C# 的 First(...)
        let find name = results |> Seq.find (fun r -> r.Name = name)

        // 验证 1 部门 (Alice: 6000, Charlie: 4500)
        let alice = find "Alice"
        // 注意：LinqToDB 的 Rank() 通常映射为 Int64 (对应 F# 的 int64/1L)，这里稳妥起见我们用 int64 类型断言
        Assert.Equal(1L, int64 alice.DeptRank) 
        Assert.Equal(10500.0, alice.DeptTotalSalary) // 1部门总薪水 10500

        let charlie = find "Charlie"
        Assert.Equal(2L, int64 charlie.DeptRank)
        Assert.Equal(10500.0, charlie.DeptTotalSalary)

        // 验证 2 部门 (Eve: 5500, Bob: 4000)
        let eve = find "Eve"
        Assert.Equal(1L, int64 eve.DeptRank)
        Assert.Equal(9500.0, eve.DeptTotalSalary)

        let bob = find "Bob"
        Assert.Equal(2L, int64 bob.DeptRank)
        Assert.Equal(9500.0, bob.DeptTotalSalary)

        // 验证 3 部门 (David: 8000)
        let david = find "David"
        Assert.Equal(1L, int64 david.DeptRank)
        Assert.Equal(8000.0, david.DeptTotalSalary)
    [<Fact>]
    [<Trait("Linq", "TimeSeriesAndMultiGroup")>]
    let ``Test Polars Linq Time Series And MultiGroup`` () =
        // Arrange: 准备模拟数据
        let orders = [|
            { OrderId = 1; OrderDate = DateTime(2023, 1, 15); Region = "North"; Revenue = 100.0 }
            { OrderId = 2; OrderDate = DateTime(2023, 1, 20); Region = "North"; Revenue = 150.0 }
            { OrderId = 3; OrderDate = DateTime(2023, 2, 10); Region = "South"; Revenue = 200.0 }
            { OrderId = 4; OrderDate = DateTime(2024, 1, 5);  Region = "North"; Revenue = 300.0 }
            { OrderId = 5; OrderDate = DateTime(2023, 2, 25); Region = "South"; Revenue = 250.0 }
        |]

        use dfOrders = DataFrame.ofRecords orders
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        let orderQuery = db.RegisterTable<OrderDto>("orders", dfOrders)

        // ==========================================
        // 测试 1：多维分组 (Multi-key GroupBy)
        // 业务需求：按年份和地区统计总营收
        // 预期 SQL: GROUP BY EXTRACT(YEAR FROM o."OrderDate"), o."Region"
        // ==========================================
        let multiGroupQuery = 
            query {
                for o in orderQuery do
                groupBy {| Year = o.OrderDate.Year; Region = o.Region |} into g
                sortBy g.Key.Year
                thenBy g.Key.Region
                select {|
                    Year = g.Key.Year
                    Region = g.Key.Region
                    TotalRevenue = g.Sum(fun x -> x.Revenue)
                |}
            }
        let multiGroupResult = multiGroupQuery.ToList()

        // 预期结果：2023-North (250), 2023-South (450), 2024-North (300)
        Assert.Equal(3, multiGroupResult.Count)
        
        Assert.Equal(2023, multiGroupResult.[0].Year)
        Assert.Equal("North", multiGroupResult.[0].Region)
        Assert.Equal(250.0, multiGroupResult.[0].TotalRevenue)

        Assert.Equal(2023, multiGroupResult.[1].Year)
        Assert.Equal("South", multiGroupResult.[1].Region)
        Assert.Equal(450.0, multiGroupResult.[1].TotalRevenue)

        // ==========================================
        // 测试 2：时间序列筛选 (Date Functions)
        // 业务需求：找出 2023 年 2 月的所有订单
        // 预期翻译为 SQL 内置的日期提取函数
        // ==========================================
        let febOrders = 
            query {
                for o in orderQuery do
                where (o.OrderDate.Year = 2023 && o.OrderDate.Month = 2)
                select o
            }
            |> Seq.toList 

        Assert.Equal(2, febOrders.Length) // OrderId 3 和 5
        Assert.True(febOrders |> Seq.exists (fun o -> o.OrderId = 3))
        Assert.True(febOrders |> Seq.exists (fun o -> o.OrderId = 5))
    [<Fact>]
    [<Trait("Linq", "AdvancedFilters")>]
    let ``Test Polars Linq In And String Like`` () =
        // Arrange: 准备测试数据
        let data = [|
            { Id = 1; Name = "Apple";   Category = "Fruit";     Price = 1.2 }
            { Id = 2; Name = "Banana";  Category = "Fruit";     Price = 0.8 }
            { Id = 3; Name = "Carrot";  Category = "Vegetable"; Price = 0.5 }
            { Id = 4; Name = "Avocado"; Category = "Vegetable"; Price = 2.0 }
            { Id = 5; Name = "Beef";    Category = "Meat";      Price = 5.0 }
        |]

        use df = DataFrame.ofRecords data
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)

        let queryable = db.RegisterTable<ProductDto>("products", df)

        // ==========================================
        // 测试 1：集合包含 (映射为 IN 子句)
        // 预期 SQL: SELECT ... FROM products WHERE Category IN ('Fruit', 'Meat')
        // ==========================================
        // F# 中定义数组，直接调用 System.Linq 的 Contains
        let targetCategories = [| "Fruit"; "Meat" |]
        
        let inResult = 
            query {
                for p in queryable do
                where (targetCategories.Contains p.Category)
                select p
            } |> Seq.toList
        
        Assert.Equal(3, inResult.Length) // Apple, Banana, Beef
        // 验证蔬菜不在结果中
        Assert.False(inResult |> Seq.exists (fun p -> p.Category = "Vegetable"))

        // ==========================================
        // 测试 2：字符串前缀匹配 (映射为 LIKE 'A%')
        // ==========================================
        let likeResult = 
            query {
                for p in queryable do
                // .NET 原生方法直接映射
                where (p.Name.StartsWith "A")
                select p
            } |> Seq.toList
        
        Assert.Equal(2, likeResult.Length) // Apple, Avocado
        Assert.True(likeResult |> Seq.exists (fun p -> p.Name = "Apple"))
        Assert.True(likeResult |> Seq.exists (fun p -> p.Name = "Avocado"))

        // ==========================================
        // 测试 3：组合拳！IN + LIKE + 复杂条件
        // ==========================================
        let complexResult = 
            query {
                for p in queryable do
                where (
                    targetCategories.Contains p.Category && 
                    p.Name.Contains "e" && 
                    p.Price > 1.0
                )
                select p
            } |> Seq.toList

        // 只有 Apple 和 Beef 满足：类别是水果或肉，名字包含 e，且价格大于 1.0
        Assert.Equal(2, complexResult.Length) 
        Assert.True(complexResult |> Seq.exists (fun p -> p.Name = "Apple"))
        Assert.True(complexResult |> Seq.exists (fun p -> p.Name = "Beef"))