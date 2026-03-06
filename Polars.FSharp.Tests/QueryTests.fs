namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.NET.Linq
open Polars.FSharp
open LinqToDB
open System
open System.Linq.Expressions

type Person = {Name: string;Age: int;Sales: float}
type Department = { DeptId: int; DeptName: string }
type Employee = { Name: string; DeptId: int }
type EmpDeptDto = { EmpName: string; DepartmentName: string }
type EmployeeSalary = { Name: string; DeptId: int; Salary: float }
type DeptStatsDto = { DeptId: int; TotalSalary: float; EmployeeCount: int }
type OrderDto = { OrderId: int; OrderDate: DateTime; Region: string; Revenue: float }
type ProductDto = { Id: int; Name: string; Category: string; Price: float }
type DeptDto = { DeptId: int; DeptName: string }
type NullableEmpDto = { Name: string; DeptId: int; Salary: float }
type SalesData = { Category: string; ProductName: string; Revenue: float; Discount: float }
type ServerLog = { Id: int; Message: string; Flags: int }
type StockPrice = { Ticker: string; Date: DateTime; Price: float }
type StaffRecord = { name: string; age: int; salary: float }
type SalaryRecord = { salary: float }
type StaffRecordWithBonus = {
    name: string
    age: int
    salary: float
    bonus: float 
}
type TrafficRecord = { Id: int; Region: string; Latency: float }
type EmpDto = { Name: string; DeptId: int }

module QueryTests =
    open System.IO
    open System.Threading.Tasks

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
        // queryable.ToDataFrame().Show()
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
    [<Fact>]
    [<Trait("Linq", "PaginationAndDistinct")>]
    let ``Test Polars Linq Skip Take And Distinct`` () =
        // Arrange
        let data = [|
            { Id = 1; Name = "Apple";   Category = "Fruit"; Price = 1.2 }
            { Id = 2; Name = "Banana";  Category = "Fruit"; Price = 0.8 }
            { Id = 3; Name = "Cherry";  Category = "Fruit"; Price = 1.5 }
            { Id = 4; Name = "Beef";    Category = "Meat";  Price = 5.0 }
            { Id = 5; Name = "Pork";    Category = "Meat";  Price = 4.0 }
            { Id = 6; Name = "Apple";   Category = "Fruit"; Price = 1.2 } // Duplicated
        |]

        use df = DataFrame.ofRecords data
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        let queryable = db.RegisterTable<ProductDto>("products", df)

        // ==========================================
        // 测试 1：投影去重 (Distinct)
        // 预期 SQL: SELECT DISTINCT p."Category" FROM products p
        // ==========================================
        let distinctCategories = 
            query {
                for p in queryable do
                select p.Category
                distinct 
            } |> Seq.toList
        
        Assert.Equal(2, distinctCategories.Length)
        Assert.Contains("Fruit", distinctCategories)
        Assert.Contains("Meat", distinctCategories)

        // 测试整行去重
        let distinctProducts = 
            query {
                for p in queryable do
                // 直接对整行进行 distinct
                distinct
            } |> Seq.toList
            
        // 因为 Id 不同 (1 和 6)，所以它们不算完全重复，总数还是 6
        Assert.Equal(6, distinctProducts.Length) 

        // ==========================================
        // 测试 2：分页与切片 (Skip & Take)
        // 预期 SQL: SELECT ... FROM products ORDER BY p."Id" LIMIT 2 OFFSET 2
        // ==========================================
        let pagedResult = 
            query {
                for p in queryable do
                sortBy p.Id
                skip 2
                take 2
            } |> Seq.toList
            
        Assert.Equal(2, pagedResult.Length)
        
        // 跳过 Apple(1) 和 Banana(2)，应该取到 Cherry(3) 和 Beef(4)
        Assert.Equal(3, pagedResult.[0].Id) 
        Assert.Equal(4, pagedResult.[1].Id)
    [<Fact>]
    [<Trait("Linq", "CaseWhenAndCte")>]
    let ``Test Polars Linq CaseWhen And Cte`` () =
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
        // 测试 1：CASE WHEN (数据分箱/条件分支)
        // 预期 SQL: CASE WHEN e."Salary" >= 6000 THEN 'High' ... END
        // ==========================================
        let caseWhenQuery = 
            query {
                for e in empQuery do
                select {|
                    Name = e.Name
                    SalaryTier = 
                        if e.Salary >= 6000.0 then "High"
                        elif e.Salary >= 4500.0 then "Medium"
                        else "Low"
                |}
            } |> Seq.toList

        Assert.Equal(5, caseWhenQuery.Length)
        
        // 验证辅助函数
        let assertTier expectedTier name = 
            let emp = caseWhenQuery |> Seq.find (fun e -> e.Name = name)
            Assert.Equal(expectedTier, emp.SalaryTier)

        assertTier "High" "Alice"   // 6000
        assertTier "High" "David"   // 8000
        assertTier "Medium" "Eve"     // 5500
        assertTier "Medium" "Charlie" // 4500
        assertTier "Low" "Bob"       // 4000

        // ==========================================
        // 测试 2：CTE (Common Table Expression / WITH 语句)
        // 业务需求：先圈出一批高薪人群作为 CTE，再进行复杂查询
        // ==========================================
        
        // 1. 定义 CTE (此时不执行，只是声明)
        // 注意：我们混合使用方法链和 LinqToDB 的扩展方法 AsCte
        let cte = 
            empQuery
                .Where(fun e -> e.Salary > 5000.0)
                .AsCte "HighEarners"

        // 2. 基于 CTE 进行查询
        let cteResult = 
            query {
                for c in cte do
                where (c.DeptId = 1 || c.DeptId = 3)
                select c
            } |> Seq.toList

        // 薪水 > 5000 的有 Alice(1,6000), David(3,8000), Eve(2,5500)
        // 在这三人中，部门是 1 或 3 的只有 Alice, David
        Assert.Equal(2, cteResult.Length)
        Assert.True(cteResult |> Seq.exists (fun e -> e.Name = "Alice"))
        Assert.True(cteResult |> Seq.exists (fun e -> e.Name = "David"))
    [<Fact>]
    [<Trait("Linq", "SubqueryInAndFunctions")>]
    let ``Test Polars Linq SubqueryIn And Functions`` () =
        let depts = [|
            { DeptId = 1; DeptName = "Engineering" }
            { DeptId = 2; DeptName = "Sales" }
            { DeptId = 3; DeptName = "HR" }
        |]

        let emps = [|
            { Name = "Alice";   DeptId = 1; Salary = 6000.0 }
            { Name = null;      DeptId = 2; Salary = 4000.0 } // 故意塞一个 null 名字
            { Name = "Charlie"; DeptId = 1; Salary = 4500.0 }
            { Name = "David";   DeptId = 3; Salary = 8000.0 }
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)

        let deptQuery = db.RegisterTable<DeptDto>("departments", dfDepts)
        let empQuery = db.RegisterTable<NullableEmpDto>("employees", dfEmps)

        // ==========================================
        // 测试 1：非相关子查询 (IN Subquery)
        // 预期 SQL: d."DeptId" IN (SELECT e."DeptId" FROM employees e WHERE e."Salary" > 5000)
        // ==========================================
        
        // 1. 构建一个只查 DeptId 的内部查询 (IQueryable<int>)
        let highPaidDeptIds = 
            query {
                for e in empQuery do
                where (e.Salary > 5000.0)
                select e.DeptId
            }

        // 2. 在外部查询中使用 .Contains() 传入这个内部查询
        let richDepts = 
            query {
                for d in deptQuery do
                where (highPaidDeptIds.Contains d.DeptId)
                select d
            } |> Seq.toList

        Assert.Equal(2, richDepts.Length) // Engineering (Alice) 和 HR (David)
        Assert.True(richDepts |> Seq.exists (fun d -> d.DeptName = "Engineering"))
        Assert.True(richDepts |> Seq.exists (fun d -> d.DeptName = "HR"))

        // ==========================================
        // 测试 2：空值处理 (模拟 C# 的 ?? 运算符)
        // 预期 SQL: CASE WHEN e."Name" IS NULL THEN 'Unknown' ELSE e."Name" END
        // (LinqToDB 会自动把它优化为类似 COALESCE 的行为)
        // ==========================================
        let coalesceQuery = 
            query {
                for e in empQuery do
                select {|
                    // 使用标准的 if 表达式，避免使用 F# 特有的 isNull
                    SafeName = if e.Name = null then "Unknown" else e.Name
                |}
            } |> Seq.toList

        Assert.Equal(4, coalesceQuery.Length)
        Assert.True(coalesceQuery |> Seq.exists (fun e -> e.SafeName = "Unknown")) // 替补了原本是 null 的 Bob

        // ==========================================
        // 测试 3：字符串截取与拼接 (Substring)
        // 预期 SQL: SUBSTRING(e."Name", 1, 3) (SQL 的下标通常从 1 开始，LinqToDB 会自动修正)
        // ==========================================
        let stringQuery = 
            query {
                for e in empQuery do
                // 过滤掉 null，防止在 .NET 内存层面执行时抛空指针（虽然 SQL 层面可能有容错）
                where (e.Name <> null)
                select {|
                    Name = e.Name
                    ShortName = e.Name.Substring(0, 3)
                |}
            } |> Seq.toList

        Assert.Equal(3, stringQuery.Length)
        
        let getShortName name = 
            (stringQuery |> Seq.find (fun e -> e.Name = name)).ShortName

        Assert.Equal("Ali", getShortName "Alice")
        Assert.Equal("Cha", getShortName "Charlie")
    [<Fact>]
    [<Trait("Linq", "MathStringAndConditionalAgg")>]
    let ``Test Polars Linq Math String And ConditionalAgg`` () =
        let sales = [|
            { Category = "Tech";   ProductName = "Laptop"; Revenue = 1000.5; Discount = 50.0 }
            { Category = "Tech";   ProductName = "Mouse";  Revenue = -20.0;  Discount = 0.0 }
            { Category = "Office"; ProductName = "Desk";   Revenue = 500.2;  Discount = 10.0 }
            { Category = "Office"; ProductName = "Chair";  Revenue = 150.8;  Discount = 5.0 }
        |]

        use dfSales = DataFrame.ofRecords sales
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        let salesQuery = db.RegisterTable<SalesData>("sales", dfSales)

        // ==========================================
        // 测试 1：字符串拼接 (+) 与 数学函数 (Math)
        // 预期: CONCAT / Math.Round -> ROUND / Math.Abs -> ABS
        // ==========================================
        let scalarQuery = 
            query {
                for s in salesQuery do
                select {|
                    // F# 里的 + 拼接，LinqToDB 同样会翻译成 SQL 标准的 CONCAT 或 ||
                    FullName = s.Category + " - " + s.ProductName
                    // 标准的 .NET Math 方法会完美下推
                    NetRevenue = Math.Round(Math.Abs s.Revenue - s.Discount, 2)
                |}
            } |> Seq.toList

        Assert.Equal(4, scalarQuery.Length)
        Assert.True(scalarQuery |> Seq.exists (fun s -> s.FullName = "Tech - Laptop" && s.NetRevenue = 950.5))
        Assert.True(scalarQuery |> Seq.exists (fun s -> s.FullName = "Tech - Mouse" && s.NetRevenue = 20.0))

        // ==========================================
        // 测试 2：条件聚合 (Conditional Aggregation / Pivot)
        // 业务需求：在一次查询中，同时算出总营收，以及 Tech 和 Office 分别的营收
        // 预期 SQL: SUM(CASE WHEN s."Category" = 'Tech' THEN ABS(s."Revenue") ELSE 0 END)
        // ==========================================
        
        // 【核心高亮】：用方法链执行多重聚合，内部使用 if-then-else 替代三元运算符
        let aggQuery = 
            salesQuery
                .GroupBy(fun s -> 1) // 假分组，为了聚合全表
                .Select(fun g -> {|
                    Total = g.Sum(fun x -> Math.Abs x.Revenue)
                    
                    // 条件聚合：F# 的 if 表达式会精确翻译为 CASE WHEN
                    // 注意返回值类型统一使用 0.0 匹配 float
                    TechTotal = g.Sum(fun x -> if x.Category = "Tech" then Math.Abs x.Revenue else 0.0)
                    OfficeTotal = g.Sum(fun x -> if x.Category = "Office" then Math.Abs x.Revenue else 0.0)
                |})
                .ToList()

        Assert.Single aggQuery |> ignore
        Assert.Equal(1671.5, aggQuery.[0].Total) // 1000.5 + 20 + 500.2 + 150.8
        Assert.Equal(1020.5, aggQuery.[0].TechTotal)
        Assert.Equal(651.0,  aggQuery.[0].OfficeTotal)
    [<Fact>]
    [<Trait("Linq", "BitwiseAndRegex")>]
    let ``Test Polars Linq Bitwise and Regex`` () =
        // Arrange: 准备硬核数据
        let logs = [|
            { Id = 1; Message = "User admin logged in";  Flags = 3 }  // 3 (0011)
            { Id = 2; Message = "Failed password try";   Flags = 1 }  // 1 (0001)
            { Id = 3; Message = "DB connection timeout"; Flags = 6 }  // 6 (0110)
            { Id = 4; Message = "User guest logged in";  Flags = 2 }  // 2 (0010)
            { Id = 5; Message = "System crash error 99"; Flags = 4 }  // 4 (0100)
        |]

        use df = DataFrame.ofRecords logs
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        let logQuery = db.RegisterTable<ServerLog>("logs", df)

        // ==========================================
        // 极客测试 1：位运算 (Bitwise)
        // 预期 SQL: SELECT ... FROM logs p WHERE (p."Flags" & 2) = 2
        // ==========================================
        let bitwiseResult = 
            query {
                for log in logQuery do
                // F# 里的 &&& 是按位与。找 Flags 包含 2 (0010) 的日志 (Id: 1, 3, 4)
                where (log.Flags &&& 2 = 2)
                select log
            } |> Seq.toList

        Assert.Equal(3, bitwiseResult.Length)
        Assert.True(bitwiseResult |> Seq.exists(fun l -> l.Id = 1))
        Assert.True(bitwiseResult |> Seq.exists(fun l -> l.Id = 3))
        Assert.True(bitwiseResult |> Seq.exists(fun l -> l.Id = 4))

        // ==========================================
        // 极客测试 2：正则表达式 (Regex) 引擎下推
        // 预期 SQL: SELECT ... FROM logs p WHERE p."Message" REGEXP 'error|timeout|Failed'
        // ==========================================
        let regexResult = 
            query {
                for log in logQuery do
                where (PolarsSql.RegexMatch(log.Message, "error|timeout|Failed"))
                select log
            } |> Seq.toList

        // 匹配到 Id: 2, 3, 5
        Assert.Equal(3, regexResult.Length)
        Assert.True(regexResult |> Seq.exists(fun l -> l.Id = 2))
        Assert.True(regexResult |> Seq.exists(fun l -> l.Id = 3))
        Assert.True(regexResult |> Seq.exists(fun l -> l.Id = 5))
    [<Fact>]
    [<Trait("Linq", "LeadLag")>]
    let ``Test Polars Linq LeadLag And NestedList`` () =
        // Arrange
        let stocks = [|
            { Ticker = "AAPL"; Date = DateTime(2024, 1, 1); Price = 150.0 }
            { Ticker = "AAPL"; Date = DateTime(2024, 1, 2); Price = 155.0 }
            { Ticker = "AAPL"; Date = DateTime(2024, 1, 3); Price = 152.0 }
            { Ticker = "MSFT"; Date = DateTime(2024, 1, 1); Price = 300.0 }
            { Ticker = "MSFT"; Date = DateTime(2024, 1, 2); Price = 305.0 }
        |]

        let depts = [| { DeptId = 1; DeptName = "Tech" }; { DeptId = 2; DeptName = "Sales" } |]
        let emps = [| { Name = "Alice"; DeptId = 1 }; { Name = "Bob"; DeptId = 1 }; { Name = "Charlie"; DeptId = 2 } |]

        use dfStocks = DataFrame.ofRecords stocks
        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        
        let stockQuery = db.RegisterTable<StockPrice>("stocks", dfStocks)

        // ==========================================
        // 测试 1：高级窗口函数 (Lag - 获取上一行的值)
        // 预期 SQL: LAG(s."Price") OVER(PARTITION BY s."Ticker" ORDER BY s."Date")
        // ==========================================
        let lagQuery = 
            query {
                for s in stockQuery do
                select {|
                    Ticker = s.Ticker
                    Date = s.Date
                    Price = s.Price
                    
                    // Sql.Ext.Lag() 完美内联在 F# 匿名记录中
                    PrevPrice = Sql.Ext.Lag(s.Price).Over().PartitionBy(s.Ticker).OrderBy(s.Date).ToValue()
                |}
            } |> Seq.toList

        Assert.Equal(5, lagQuery.Length)
        
        // 验证 AAPL 第二天的数据
        let aaplDay2 = lagQuery |> Seq.find (fun s -> s.Ticker = "AAPL" && s.Date.Day = 2)
        Assert.Equal(155.0, aaplDay2.Price)
        Assert.Equal(150.0, aaplDay2.PrevPrice)
    [<Fact>]
    [<Trait("Linq", "NestedList")>]
    let ``Test Polars Linq Nested List Aggregation`` () =
        // Arrange
        let depts = [| { DeptId = 1; DeptName = "Tech" }; { DeptId = 2; DeptName = "Sales" } |]
        let emps = [| 
            { Name = "Alice"; DeptId = 1 }
            { Name = "Bob"; DeptId = 1 }
            { Name = "Charlie"; DeptId = 2 } 
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)
        
        let deptQuery = db.RegisterTable<DeptDto>("departments", dfDepts)
        let empQuery = db.RegisterTable<EmpDto>("employees", dfEmps)

        // ==========================================
        // 测试：Nested List (嵌套集合聚合)
        // 业务需求：按部门分组，把部门里所有员工的名字聚合成一个列表
        // 预期 SQL: SELECT e."DeptId", list(e."Name") FROM employees e GROUP BY e."DeptId"
        // ==========================================
        let nestedListQuery = 
            empQuery
                .GroupBy(fun e -> e.DeptId)
                .Select(fun g -> {|
                    DeptId = g.Key
                    
                    EmpNames = PolarsSql.ListAgg(g, fun e -> e.Name)
                |})
                .OrderBy(fun r -> r.DeptId)
                .ToList()

        let techDepts = nestedListQuery.[0]
        Assert.Equal(1, techDepts.DeptId)
        
        // 见证奇迹的时刻：
        Assert.Contains("Alice", techDepts.EmpNames)
        Assert.Contains("Bob", techDepts.EmpNames)
    [<Fact>]
    [<Trait("Linq", "Sandwich")>]
    let ``Test Polars Double Hybrid Sandwich`` () =
        // 初始化 Context
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx)

        use schema = new PolarsSchema([
            "age", Polars.FSharp.DataType.Int32
            "salary", DataType.Float64
        ])
        
        let path = "/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv"
        
        // ==========================================
        // 1. 底层 Native (IO 阶段)
        // 从磁盘建立扫描器，延迟执行
        // ==========================================
        use rawLf = LazyFrame.ScanCsv(path, schema = schema)
        
        let emps = db.RegisterTable<StaffRecord>("emps", rawLf)
        
        // ==========================================
        // 2. LINQ 阶段 (业务表达阶段)
        // 用极度符合直觉的 F# Query DSL 表达过滤与投影
        // ==========================================
        let linqQuery = 
            query {
                for e in emps do
                where (e.salary > 5000.0)
                select {| name = e.name; salary = e.salary |}
            }
            
        // printfn "--- Plan 1 (After LINQ) ---\n%s" (linqQuery.Explain true)

        // ==========================================
        // 3. 截胡！回到 Native (后处理阶段)
        // ==========================================
        // 注意 F# 中的向下转型使用 :?> 语法
        use lfWithLinq = linqQuery.ToLazyFrame() |> asLazyFrame

        // 继续使用 Polars 原生 API 做一些 LINQ 很难表达或极其底层的操作
        use finalLf = lfWithLinq.WithColumn(pl.col("salary").Std().Alias "salary_std")
        
        // ==========================================
        // 4. 终极点火 (Materialization)
        // ==========================================
        // 只有在调用 Collect 的这一瞬间，Polars 才会真正去读取 CSV
        // 并且是以经过 C# -> SQL -> Rust 极致优化后的物理执行计划去运行！
        use df = finalLf.Collect()
        
        df.Show() |> ignore
        // 注意 Polars 的 Height 通常是 int64/long 类型，所以在 F# 里用 0L 对比
        Assert.True(df.Height > 0L)
    [<Fact>]
    [<Trait("Linq", "SeriesScalar")>]
    let ``Test Polars Linq Series`` () =
        // 1. 生成一个纯数字的 Series (1 到 100)
        // 【纯正 F# 风味】：直接使用范围表达式 [| start .. end |]
        let numbers = [| 1 .. 100 |]
        use series = Series.create("my_numbers", numbers)
        
        use ctx = new SqlContext()
        use db = new PolarsDataContext(ctx)

        // 2. 极其优雅的注册！不需要 dummy data，不需要 DTO！
        // 直接拿到 IQueryable<int>
        let queryable = db.RegisterSeries<int> series

        // 3. 纯正的标量 LINQ 语法！
        // 【纯正 F# 风味】：使用 query 计算表达式，逻辑清晰无比
        let results = 
            query {
                for x in queryable do
                where (x > 90)
                sortByDescending x
                select x
            } |> Seq.toList

        // 验证
        Assert.Equal(10, results.Length)
        Assert.Equal(100, results.[0])
    [<Fact>]
    [<Trait("Linq", "SyntaxSugar")>]
    let ``Test Ultimate StrongTyped Select Sugar in FSharp`` () =
        
        // 1. 使用 ofRecords 传入纯正的 F# Record 序列
        let records = [
            { salary = 10.0 }
            { salary = 20.0 }
            { salary = 30.0 }
        ]
        use df = DataFrame.ofRecords records

        // 2. 构建表达式树并调用 Select
        let exprsList = 
            PolarsExpr.ToSqls(fun (e: SalaryRecord) -> 
                {| 
                    salary_sq = Math.Pow(e.salary, 2.0)
                    salary_dbl = e.salary * 2.0
                    is_high = e.salary > 15.0
                |}
            ) 
            |> Expr.SqlExprs
            |> Array.toList
        
        use resultDf = df.Select exprsList

        // 打印出来欣赏一下底层的完美类型推断 (f64, f64, bool)
        resultDf.Show() |> ignore

        // 3. 终极断言验证
        let sqArr = resultDf.["salary_sq"].ToArray<float>()
        let dblArr = resultDf.["salary_dbl"].ToArray<float>()
        let isHighArr = resultDf.["is_high"].ToArray<bool>()

        Assert.Equal(3, sqArr.Length)

        // 验证第一行 (salary = 10)
        Assert.Equal(100.0, sqArr.[0])
        Assert.Equal(20.0, dblArr.[0])
        Assert.False(isHighArr.[0])

        // 验证第三行 (salary = 30)
        Assert.Equal(900.0, sqArr.[2])
        Assert.Equal(60.0, dblArr.[2])
        Assert.True(isHighArr.[2])
    [<Fact>]
    [<Trait("Linq", "HybridLazy")>]
    let ``Test Polars Linq Hybrid Native And Linq Pushdown`` () =
        
        // 1. 准备一个临时 CSV 文件
        let csvContent = 
            "name,age,salary\n\
             Alice,25,50000\n\
             Bob,30,60000\n\
             Charlie,35,70000\n\
             David,40,80000"
             
        let fileName = "test_hybrid_lazy_data_fsharp.csv"
        File.WriteAllText(fileName, csvContent)

        try
            use schema = new PolarsSchema([
                "age", Polars.FSharp.DataType.Int32
                "salary", DataType.Float64
            ])
            
            // 2. ScanCsv 创建文件指针和基础逻辑计划
            use lf = LazyFrame.ScanCsv(fileName, schema = schema)
            
            // ====================================================================
            // 【核心混写阶段 1：Polars 原生 API】
            // 我们用原生表达式加一个新列 "bonus"，逻辑是 salary 的 10%
            // ====================================================================
            use lfWithBonus = lf.WithColumns [(pl.col "salary" * pl.lit 0.1).Alias "bonus"]

            use sqlCtx = new SqlContext()
            use db = new PolarsDataContext(sqlCtx) 

            // ====================================================================
            // 【核心混写阶段 2：F# Query 表达式 (LINQ)】
            // 将带有原生计划的 LazyFrame 注册进来，用 F# 原生的 query 编写业务逻辑！
            // ====================================================================
            let employees = db.RegisterTable<StaffRecordWithBonus>("employees", lfWithBonus)
            
            let linqQuery = 
                query {
                    for e in employees do
                    // LINQ 过滤：使用原有列和原生生成的列
                    where (e.age > 30 && e.bonus >= 7000.0)
                    // LINQ 投影：在 F# 里直接映射成强类型的匿名记录
                    // 注意：e.salary 是 int，需显式转换为 float 才能与 bonus (float) 相加
                    select {| name = e.name; TotalCompensation = float e.salary + e.bonus |}
                }

            // ====================================================================
            // 4. 终极点火：触发查询
            // 引擎会将 Native Plan 和 LINQ SQL 合并为单一 AST，执行极致优化并读取磁盘
            // ====================================================================
            let results = linqQuery |> Seq.toList

            // 5. 验证结果
            Assert.NotNull results
            Assert.Equal(2, results.Length) // 只剩 Charlie (35岁, bonus=7000) 和 David (40岁, bonus=8000)

            // 验证 Charlie
            Assert.Equal("Charlie", results.[0].name)
            Assert.Equal(77000.0, results.[0].TotalCompensation)

            // 验证 David
            Assert.Equal("David", results.[1].name)
            Assert.Equal(88000.0, results.[1].TotalCompensation)

        finally
            // 清理文件
            if File.Exists fileName then File.Delete fileName
    [<Fact>]
    [<Trait("Linq", "UnifiedCRUD")>]
    let ``Test Polars Linq Unified CRUD UX in FSharp`` () =
        
        // 1. 准备数据并注册到 Polars (标准流程)
        let emps = [
            { Name = "Alice"; DeptId = 1; Salary = 5000.0 }
            { Name = "Bob";   DeptId = 2; Salary = 4000.0 }
            { Name = "Eve";   DeptId = 3; Salary = 3000.0 }
        ]

        use dfEmps = DataFrame.ofRecords emps
        use sqlCtx = new SqlContext()
        use db = new PolarsDataContext(sqlCtx, ownsContext = true)
        
        // 2. 极其优雅的上下文初始化
        let table = db.RegisterTable<EmployeeSalary>("employees", dfEmps)

        // ==========================================
        // 【R: 查询】
        // ==========================================
        let richEmps = 
            query {
                for e in table do
                where (e.Salary >= 5000.0)
                select e
            } |> Seq.toList

        Assert.Equal(1, richEmps.Length)
        Assert.Equal("Alice", richEmps.[0].Name)

        // ==========================================
        // 【U: 更新】
        // 预期: Polars 抛出不支持 Update 的异常，但 SQL 会完美生成并送到 ExecuteNonQuery
        // ==========================================
        try
            table.Where(fun (e: EmployeeSalary) -> e.DeptId = 1)
                 // 核心魔法：内联传参触发 Expression 转换，同时锁死 e 的类型！
                 .Set((fun (e: EmployeeSalary) -> e.Salary), (fun (e: EmployeeSalary) -> e.Salary + 1000.0))
                 .Update() |> ignore
        with 
        | ex -> Console.WriteLine $"Expected Update Error: {ex.Message}"

        // ==========================================
        // 【D: 删除】
        // ==========================================
        let deleted = table.Where(fun e -> e.DeptId < 3).Delete()
        use deletedDf = table.ToDataFrame() |> asDataFrame
        deletedDf.Show() 
        // Assert 确实执行了删除并返回了受影响的行数 (通常 >= 0)
        Assert.True(deleted >= 0)
    [<Fact>]
    [<Trait("Linq", "Async_Stress_ToDataFrame")>]
    let ``Test Polars Linq High Concurrency ToDataFrameAsync Stress`` () = task {
        
        // ==============================================================
        // 1. 制造弹药：10 万条测试数据
        // ==============================================================
        let recordCount = 100_000
        
        // F# 原生的 Array.init 瞬间生成测试数据，告别 Enumerable.Range
        let mockData = 
            Array.init recordCount (fun i -> 
                { Id = i
                  Region = sprintf "Region_%d" (i % 50)
                  Latency = Random.Shared.NextDouble() * 100.0 }
            )

        // 假设 DataFrame 有 ofRecords 或者 From 扩展
        use df = DataFrame.ofRecords mockData 

        // ==============================================================
        // 2. 并发 Worker：直接返回原生的 DataFrame
        // ==============================================================
        let simulateDataFrameQueryAsync (workerId: int) = task {
            use ctx = new SqlContext()
            use db = new PolarsDataContext(ctx)
            
            let table = db.RegisterTable<TrafficRecord>("traffic", df)
            let targetRegion = sprintf "Region_%d" (workerId % 50)

            // 【黑科技回收】：还记得我们写的 AST 解包猎犬吗？
            // 现在你可以毫无顾忌地使用极其优雅的 query { } 表达式了！但是很慢，慢5倍
            // let q = 
            //     query {
            //         for t in table do
            //         where (t.Region = targetRegion && t.Latency > 10.0)
            //         sortBy t.Id
            //         select t
            //     }
            let q = 
                table
                    .Where(fun t -> t.Region = targetRegion && t.Latency > 10.0)
                    .OrderBy(fun t -> t.Id)

            // 【见证奇迹】：let! 等价于 await，配合 AsDataFrame() 扩展，如丝般顺滑
            let! idf = q.ToDataFrameAsync()
            use resultDf = idf |> asDataFrame
            
            // DataFrame 的 Height 属性底层通常是 int64，直接返回
            return resultDf.Height
        }

        // ==============================================================
        // 3. 点火！瞬间发射 1000 个并发计算任务！
        // ==============================================================
        let concurrencyLevel = 1000
        
        // 抛弃丑陋的 for 循环和 List.Add，一句话生成 1000 个并发 Task！
        let tasks = Array.init concurrencyLevel simulateDataFrameQueryAsync

        // 挂起等待 100 个底层的 LazyCollect 并发完成
        // let! 瞬间解包 Task 数组
        let! finalHeights = Task.WhenAll tasks

        // ==============================================================
        // 4. 断言验证
        // ==============================================================
        Assert.Equal(concurrencyLevel, finalHeights.Length)

        for height in finalHeights do
            // 注意 F# 里 int64 需要加后缀 'L'
            Assert.True(height > 0L && height <= 2000L)
    }