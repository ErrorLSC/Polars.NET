namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.FSharp.Query
open Polars.FSharp
open System

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
type PlayerOptionRecord = {
    Id: int
    Nickname: string option       
    Score: int voption            
    LastLogin: DateTime voption  
}

module QueryTests =
    open System.IO
    open System.Threading.Tasks
    [<Fact>]
    [<Trait("LINQ", "MultiJoinStressTest")>]
    let ``Test Ultimate Multi-Level Left and Inner Join Stress Scenario`` () =
        // -----------------------------------------------------------------
        // 1. Arrange: 4 dataframes
        // -----------------------------------------------------------------
        
        // Depts
        let depts = [|
            { DeptId = 10; DeptName = "Engineering" }
            { DeptId = 20; DeptName = "Sales" }
            { DeptId = 30; DeptName = "HR" }          // No Employee,no Sales，no Traffic
            { DeptId = 40; DeptName = "Marketing" }   // Has Sales，no Employee, no Traffic
        |]

        // Emps
        let emps = [|
            { Name = "Alice"; DeptId = 10 }
            { Name = "Bob";   DeptId = 20 }
            { Name = "Charlie"; DeptId = 10 }
        |]

        // Sales
        let sales = [|
            { Category = "Engineering"; ProductName = "Polars.NET"; Revenue = 5000.0; Discount = 500.0 }
            { Category = "Marketing"; ProductName = "Ad Campaign"; Revenue = 3000.0; Discount = 200.0 }
        |]

        // Traffic
        let traffic = [|
            { Id = 10; Region = "US-East"; Latency = 12.5 }
            { Id = 20; Region = "EU-West"; Latency = 45.0 }
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps
        use dfSales = DataFrame.ofRecords sales
        use dfTraffic = DataFrame.ofRecords traffic

        let deptQuery = dfDepts.AsQueryable<Department>()
        let empQuery = dfEmps.AsQueryable<Employee>()
        let salesQuery = dfSales.AsQueryable<SalesData>()
        let trafficQuery = dfTraffic.AsQueryable<TrafficRecord>()

        // -----------------------------------------------------------------
        // 2. Act: 4x Left Join + Let + Sort + Anno Record Projection
        // -----------------------------------------------------------------
        let queryable = 
            query {
                for d in deptQuery do
                
                // 1st Join: Left Outer Join Employees
                leftOuterJoin e in empQuery on (d.DeptId = e.DeptId) into empGroup
                for e in empGroup.DefaultIfEmpty() do

                // 2nd Join: Left Outer Join Traffic Records
                leftOuterJoin t in trafficQuery on (d.DeptId = t.Id) into trafficGroup
                for t in trafficGroup.DefaultIfEmpty() do

                // 3rd Join: Left Outer Join Sales Data
                leftOuterJoin s in salesQuery on (d.DeptName = s.Category) into salesGroup
                for s in salesGroup.DefaultIfEmpty() do

                // let
                let baseDeptCode = d.DeptId * 100
                
                // Sort
                sortBy d.DeptName
                thenByDescending (if box e = null then "" else e.Name)

                // Project
                select {|
                    Department = d.DeptName
                    EmpName = if box e = null then "NO_EMPLOYEE" else e.Name
                    Region = if box t = null then "UNKNOWN_REGION" else t.Region
                    Latency = if box t = null then -1.0 else t.Latency
                    ProductName = if box s = null then "NO_PRODUCT" else s.ProductName
                    ComputedDeptCode = baseDeptCode
                |}
            }

        let results = queryable.ToList()

        // -----------------------------------------------------------------
        // 3. Assert
        // -----------------------------------------------------------------
        
        // - Dept 10 (Engineering): 2 emps (Alice, Charlie) x 1 traffic (US-East) x 1 sales (Polars.NET) = 2 
        // - Dept 20 (Sales): 1 emp (Bob) x 1 traffic (EU-West) x 0 sales = 1 
        // - Dept 30 (HR): 0 emps x 0 traffic x 0 sales = 1 
        // - Dept 40 (Marketing): 0 emps x 0 traffic x 1 sales (Ad Campaign) = 1 

        Assert.Equal(5, results.Count)

        // 1. Engineering (Charlie)
        Assert.Equal("Engineering", results.[0].Department)
        Assert.Equal("Charlie", results.[0].EmpName)
        Assert.Equal("US-East", results.[0].Region)
        Assert.Equal(12.5, results.[0].Latency)
        Assert.Equal("Polars.NET", results.[0].ProductName)
        Assert.Equal(1000, results.[0].ComputedDeptCode)

        // 2. Engineering (Alice)
        Assert.Equal("Engineering", results.[1].Department)
        Assert.Equal("Alice", results.[1].EmpName)
        Assert.Equal("Polars.NET", results.[1].ProductName)

        // 3. HR
        Assert.Equal("HR", results.[2].Department)
        Assert.Equal("NO_EMPLOYEE", results.[2].EmpName)
        Assert.Equal("UNKNOWN_REGION", results.[2].Region)
        Assert.Equal(-1.0, results.[2].Latency)
        Assert.Equal("NO_PRODUCT", results.[2].ProductName)
        Assert.Equal(3000, results.[2].ComputedDeptCode)

        // 4. Marketing
        Assert.Equal("Marketing", results.[3].Department)
        Assert.Equal("NO_EMPLOYEE", results.[3].EmpName)
        Assert.Equal("Ad Campaign", results.[3].ProductName)

        // 5. Sales (Bob)
        Assert.Equal("Sales", results.[4].Department)
        Assert.Equal("Bob", results.[4].EmpName)
        Assert.Equal("EU-West", results.[4].Region)
        Assert.Equal(45.0, results.[4].Latency)
        Assert.Equal("NO_PRODUCT", results.[4].ProductName)
        Assert.Equal(2000, results.[4].ComputedDeptCode)

    [<Fact>]
    [<Trait("LINQ", "Join")>]
    let ``Test MultiKey and chained Join via LINQ and QueryBuilder``() =
        use empDf = 
            DataFrame.create [
                Series.create("Name", [ "Alice"; "Bob"; "Charlie"; "David" ])
                Series.create("DeptId", [ 10; 20; 10; 30 ])
            ]

        use salDf = 
            DataFrame.create [
                Series.create("Name", [ "Alice"; "Bob"; "Charlie"; "David" ])
                Series.create("DeptId", [ 10; 99; 10; 30 ])
                Series.create("Salary", [ 9500.0; 8000.0; 12000.0; 15000.0 ])
            ]

        use deptDf = 
            DataFrame.create [
                Series.create("DeptId", [ 10; 30 ])
                Series.create("DeptName", [ "Engineering"; "Finance" ])
            ]

        let qEmp = empDf.AsQueryable<Employee>()
        let qSal = salDf.AsQueryable<EmployeeSalary>()
        let qDept = deptDf.AsQueryable<Department>()

        // Chained Multi-Key Join
        let results =
            qEmp.Join(
                qSal,
                (fun (e: Employee) -> e.Name, e.DeptId),
                (fun (s: EmployeeSalary) -> s.Name, s.DeptId),
                (fun e s -> e.Name, e.DeptId, s.Salary)
            ).Join(
                qDept,
                (fun (name, deptId, salary) -> deptId),
                (fun (d: Department) -> d.DeptId),
                (fun (name, deptId, salary) d -> {| Name = name; Dept = d.DeptName; Salary = salary |})
            )
            |> Seq.sortBy (fun x -> x.Salary)
            |> Seq.toList

        Assert.Equal(3, results.Length)
        Assert.Equal("Alice", results.[0].Name)
        Assert.Equal("Engineering", results.[0].Dept)
        Assert.Equal(9500.0, results.[0].Salary)

        Assert.Equal("David", results.[2].Name)
        Assert.Equal("Finance", results.[2].Dept)
        Assert.Equal(15000.0, results.[2].Salary)
        
    [<Fact>]
    [<Trait("LINQ", "FSharpOptions")>]
    let ``Test Polars FSharp Option And ValueOption Accessor`` () =
        
        let data = [|
            { Id = 1; Nickname = Some "Alice";   Score = ValueSome 100; LastLogin = ValueSome DateTime.Now }
            { Id = 2; Nickname = None;           Score = ValueSome 80;  LastLogin = ValueSome DateTime.Now } 
            { Id = 3; Nickname = Some "Bob";     Score = ValueNone;     LastLogin = ValueSome DateTime.Now } 
            { Id = 4; Nickname = None;           Score = ValueNone;     LastLogin = ValueNone }              
        |]

        use df = DataFrame.ofRecords data

        let table = df.AsQueryable<PlayerOptionRecord>()

        let results = 
            query {
                for p in table do
                sortBy p.Id
                select p
            } |> Seq.toArray

        Assert.Equal(4, results.Length)

        Assert.Equal(1, results.[0].Id)
        Assert.True(results.[0].Nickname.IsSome)
        Assert.Equal("Alice", results.[0].Nickname.Value)
        Assert.Equal(ValueSome 100, results.[0].Score)

        Assert.Equal(2, results.[1].Id)
        Assert.True(results.[1].Nickname.IsNone) 
        Assert.Equal(ValueSome 80, results.[1].Score)

        Assert.Equal(3, results.[2].Id)
        Assert.Equal(Some "Bob", results.[2].Nickname)
        Assert.True(results.[2].Score.IsValueNone) 

        Assert.Equal(4, results.[3].Id)
        Assert.True(results.[3].Nickname.IsNone)
        Assert.True(results.[3].Score.IsValueNone)
        Assert.True(results.[3].LastLogin.IsValueNone)

    [<Fact>]
    [<Trait("LINQ", "Where")>]
    let ``Test Polars FSharp Linq Where And OrderBy`` () =
        let data = [|
            { Name = "Alice";   Age = 25; Sales = 100.0 }
            { Name = "Bob";     Age = 30; Sales = 200.0 }
            { Name = "Charlie"; Age = 35; Sales = 300.0 }
            { Name = "David";   Age = 18; Sales = 50.0 }
        |]

        use df = DataFrame.ofRecords data

        let ageLimit = 20
        let excludeName = "Alice"

        let table = df.AsQueryable<Person>()

        let queryable = 
            query {
                for p in table do
                where (p.Age > ageLimit && p.Name <> excludeName) 
                sortByDescending p.Sales
                select p
            }

        queryable.ToDataFrame().Show()

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
    [<Trait("LINQ", "Join")>]
    let ``Test Polars Linq Inner Join`` () =
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

        let deptQuery = dfDepts.AsQueryable<Department>()
        let empQuery = dfEmps.AsQueryable<Employee>()
        let queryable = 
            query {
                for e in empQuery do
                join d in deptQuery on (e.DeptId = d.DeptId)
                where (e.Name <> "Bob")
                sortBy d.DeptName
                thenBy e.Name
                select { EmpName = e.Name; DepartmentName = d.DeptName }
            }

        let results = queryable.ToList()

        Assert.Equal(2, results.Count)
        Assert.Equal("Alice", results.[0].EmpName)
        Assert.Equal("Engineering", results.[0].DepartmentName)

    [<Fact>]
    [<Trait("LINQ", "GroupByHaving")>]
    let ``Test Polars Linq GroupBy Aggregation With Having`` () =
        let emps = [|
            { Name = "Alice";   DeptId = 1; Salary = 5000.0 }
            { Name = "Bob";     DeptId = 2; Salary = 4000.0 }
            { Name = "Charlie"; DeptId = 1; Salary = 6000.0 }
            { Name = "David";   DeptId = 2; Salary = 4500.0 }
            { Name = "Eve";     DeptId = 3; Salary = 3000.0 }
        |]

        use dfEmps = DataFrame.ofRecords emps
        
        let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

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

        let results = queryable.ToList()

        Assert.Equal(2, results.Count) 

        Assert.Equal(1, results.[0].DeptId)
        Assert.Equal(11000.0, results.[0].TotalSalary) // 5000 + 6000
        Assert.Equal(2, results.[0].EmployeeCount)

        Assert.Equal(2, results.[1].DeptId)
        Assert.Equal(8500.0, results.[1].TotalSalary) // 4000 + 4500
        Assert.Equal(2, results.[1].EmployeeCount)
        
        let hasDept3 = results |> Seq.exists (fun r -> r.DeptId = 3)
        Assert.False hasDept3

    [<Fact>]
    [<Trait("LINQ", "ScalarAndFirst")>]
    let ``Test Polars Linq Scalar And First`` () =
        // Anonymous Records
        let data = [|
            {| Id = 1; Name = "Alice"; Score = 80 |}
            {| Id = 2; Name = "Bob";   Score = 90 |}
            {| Id = 3; Name = "Charlie"; Score = 85 |}
        |]

        use df = DataFrame.ofRecords data
        
        let table = df.AsQueryable data

        let highScorersCount = 
            query {
                for s in table do
                where (s.Score > 82)
                count
            }
        Assert.Equal(2, highScorersCount)

        let maxScore = 
            query {
                for s in table do
                maxBy s.Score
            }
        Assert.Equal(90, maxScore)

        // SQL: SELECT Id, Name, Score FROM students WHERE Score > 82 ORDER BY Score DESC LIMIT 1
        let topStudent = 
            query {
                for s in table do
                where (s.Score > 82)
                sortByDescending s.Score
                head
            }
            
        Assert.NotNull(topStudent)
        Assert.Equal(90, topStudent.Score)

    [<Fact>]
    [<Trait("LINQ", "LeftJoin")>]
    let ``Test Polars Linq Left Join`` () =
        let depts = [|
            { DeptId = 1; DeptName = "Engineering" }
            { DeptId = 2; DeptName = "Sales" }
            { DeptId = 3; DeptName = "HR" } 
        |]

        let emps = [|
            { Name = "Alice"; DeptId = 1 }
            { Name = "Bob";   DeptId = 2 }
            { Name = "Charlie"; DeptId = 1 }
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        let deptQuery = dfDepts.AsQueryable<Department>()
        let empQuery = dfEmps.AsQueryable<Employee>()

        let queryable = 
            query {
                for d in deptQuery do
                leftOuterJoin e in empQuery on (d.DeptId = e.DeptId) into empGroup
                for e in empGroup.DefaultIfEmpty() do
                sortBy d.DeptId
                thenBy e.Name
                
                select {|
                    DeptName = d.DeptName

                    EmployeeName = if box e = null then "NO_EMPLOYEE" else e.Name
                |}
            } 

        let results = queryable.ToList()

        // Assert
        Assert.Equal(4, results.Count)

        Assert.Equal("Engineering", results.[0].DeptName)
        Assert.Equal("Alice", results.[0].EmployeeName)
        
        Assert.Equal("Engineering", results.[1].DeptName)
        Assert.Equal("Charlie", results.[1].EmployeeName)

        Assert.Equal("Sales", results.[2].DeptName)
        Assert.Equal("Bob", results.[2].EmployeeName)

        Assert.Equal("HR", results.[3].DeptName)
        Assert.Equal("NO_EMPLOYEE", results.[3].EmployeeName)

    [<Fact>]
    [<Trait("LINQ", "UnionAndCrossJoin")>]
    let ``Test Polars Linq Union And Cross Join`` () =
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

        let deptQuery = dfDepts.AsQueryable depts
        let empQuery = dfEmps.AsQueryable emps

        // SQL: SELECT ... FROM departments d CROSS JOIN employees e
        let crossJoinQuery = 
            query {
                for d in deptQuery do
                for e in empQuery do
                select {| DeptName = d.DeptName; Name = e.Name |}
            }

        let crossResult = crossJoinQuery.ToList()
        
        Assert.Equal(4, crossResult.Count)
        
        Assert.True(crossResult |> Seq.exists(fun x -> x.DeptName = "Engineering" && x.Name = "Alice"))
        Assert.True(crossResult |> Seq.exists(fun x -> x.DeptName = "Sales" && x.Name = "Bob"))

        // SQL: SELECT ... FROM employees WHERE ... UNION ALL SELECT ... FROM employees WHERE ...
        let query1 = query { for e in empQuery do where (e.DeptId = 1); select e }
        let query2 = query { for e in empQuery do where (e.DeptId = 2); select e }
        
        let unionResult = query1.Concat(query2).ToList()

        Assert.Equal(2, unionResult.Count)
        Assert.True(unionResult |> Seq.exists(fun x -> x.Name = "Alice"))
        Assert.True(unionResult |> Seq.exists(fun x -> x.Name = "Bob"))

        let upperResult = 
            query {
                for e in empQuery do
                select (e.Name.ToUpper())
            } |> Seq.toList
        
        // upperResult.First() |> Console.WriteLine
        
        Assert.Contains("ALICE", upperResult)
        Assert.Contains("BOB", upperResult)

    [<Fact>]
    [<Trait("LINQ", "AdvancedSetsAndLet")>]
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
        let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

        let q1 = query { for e in empQuery do where (e.DeptId = 1); select e }
        let q2 = query { for e in empQuery do where (e.Salary > 4000.0); select e }
        
        let intersectResult = q1.Intersect(q2).ToList()
        
        Assert.Equal(2, intersectResult.Count) // Alice and Charlie
        Assert.True(intersectResult |> Seq.exists (fun e -> e.Name = "Alice"))
        Assert.True(intersectResult |> Seq.exists (fun e -> e.Name = "Charlie"))

        let exceptResult = q2.Except(q1).ToList()
        
        Assert.Equal(2, exceptResult.Count) // David (8000, 3)  Eve (5500, 2)
        Assert.False(exceptResult |> Seq.exists (fun e -> e.DeptId = 1))

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

        Assert.Equal(3, letResult.Count)
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "Alice" && x.Bonus = 9000.0))
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "David" && x.Bonus = 12000.0))
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "Eve" && x.Bonus = 8250.0))

    // [<Fact>]
    // [<Trait("LINQ", "WindowFunctions")>]
    // let ``Test Polars Linq Window Functions`` () =
    //     let emps = [|
    //         {| Name = "Alice";   DeptId = 1; Salary = 6000.0 |}
    //         {| Name = "Bob";     DeptId = 2; Salary = 4000.0 |}
    //         {| Name = "Charlie"; DeptId = 1; Salary = 4500.0 |}
    //         {| Name = "David";   DeptId = 3; Salary = 8000.0 |}
    //         {| Name = "Eve";     DeptId = 2; Salary = 5500.0 |}
    //     |]

    //     use dfEmps = DataFrame.ofRecords emps
    //     use sqlCtx = new SqlContext()
    //     use db = new PolarsDataContext(sqlCtx)
    //     let empQuery = db.RegisterTable(dfEmps, emps)

    //     let queryable = 
    //         query {
    //             for e in empQuery do
    //             select {|
    //                 Name = e.Name
    //                 DeptId = e.DeptId
    //                 Salary = e.Salary
                    
    //                 // RANK() OVER (PARTITION BY DeptId ORDER BY Salary DESC)
    //                 DeptRank = LinqToDB.Sql.Ext.Rank().Over().PartitionBy(e.DeptId).OrderByDesc(e.Salary).ToValue()
                    
    //                 // SUM(Salary) OVER (PARTITION BY DeptId)
    //                 DeptTotalSalary = LinqToDB.Sql.Ext.Sum(e.Salary).Over().PartitionBy(e.DeptId).ToValue()
    //             |}
    //         }
    //     let results = queryable.ToList()

    //     // Assert
    //     Assert.Equal(5, results.Count)

    //     let find name = results |> Seq.find (fun r -> r.Name = name)

    //     let alice = find "Alice"
    //     Assert.Equal(1L, int64 alice.DeptRank) 
    //     Assert.Equal(10500.0, alice.DeptTotalSalary) 

    //     let charlie = find "Charlie"
    //     Assert.Equal(2L, int64 charlie.DeptRank)
    //     Assert.Equal(10500.0, charlie.DeptTotalSalary)

    //     let eve = find "Eve"
    //     Assert.Equal(1L, int64 eve.DeptRank)
    //     Assert.Equal(9500.0, eve.DeptTotalSalary)

    //     let bob = find "Bob"
    //     Assert.Equal(2L, int64 bob.DeptRank)
    //     Assert.Equal(9500.0, bob.DeptTotalSalary)

    //     let david = find "David"
    //     Assert.Equal(1L, int64 david.DeptRank)
    //     Assert.Equal(8000.0, david.DeptTotalSalary)

    [<Fact>]
    [<Trait("LINQ", "TimeSeriesAndMultiGroup")>]
    let ``Test Polars Linq Time Series And MultiGroup`` () =
        let orders = [|
            { OrderId = 1; OrderDate = DateTime(2023, 1, 15); Region = "North"; Revenue = 100.0 }
            { OrderId = 2; OrderDate = DateTime(2023, 1, 20); Region = "North"; Revenue = 150.0 }
            { OrderId = 3; OrderDate = DateTime(2023, 2, 10); Region = "South"; Revenue = 200.0 }
            { OrderId = 4; OrderDate = DateTime(2024, 1, 5);  Region = "North"; Revenue = 300.0 }
            { OrderId = 5; OrderDate = DateTime(2023, 2, 25); Region = "South"; Revenue = 250.0 }
        |]

        use dfOrders = DataFrame.ofRecords orders
        let orderQuery = dfOrders.AsQueryable<OrderDto>()

        // SQL: GROUP BY EXTRACT(YEAR FROM o."OrderDate"), o."Region"
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

        Assert.Equal(3, multiGroupResult.Count)
        
        Assert.Equal(2023, multiGroupResult.[0].Year)
        Assert.Equal("North", multiGroupResult.[0].Region)
        Assert.Equal(250.0, multiGroupResult.[0].TotalRevenue)

        Assert.Equal(2023, multiGroupResult.[1].Year)
        Assert.Equal("South", multiGroupResult.[1].Region)
        Assert.Equal(450.0, multiGroupResult.[1].TotalRevenue)

        let febOrders = 
            query {
                for o in orderQuery do
                where (o.OrderDate.Year = 2023 && o.OrderDate.Month = 2)
                select o
            }
            |> Seq.toList 

        Assert.Equal(2, febOrders.Length) // OrderId 3 and 5
        Assert.True(febOrders |> Seq.exists (fun o -> o.OrderId = 3))
        Assert.True(febOrders |> Seq.exists (fun o -> o.OrderId = 5))

    [<Fact>]
    [<Trait("LINQ", "AdvancedFilters")>]
    let ``Test Polars Linq In And String Like`` () =
        let data = [|
            { Id = 1; Name = "Apple";   Category = "Fruit";     Price = 1.2 }
            { Id = 2; Name = "Banana";  Category = "Fruit";     Price = 0.8 }
            { Id = 3; Name = "Carrot";  Category = "Vegetable"; Price = 0.5 }
            { Id = 4; Name = "Avocado"; Category = "Vegetable"; Price = 2.0 }
            { Id = 5; Name = "Beef";    Category = "Meat";      Price = 5.0 }
        |]

        use df = DataFrame.ofRecords data

        let queryable = df.AsQueryable<ProductDto>()

        // SQL: SELECT ... FROM products WHERE Category IN ('Fruit', 'Meat')
        let targetCategories = [| "Fruit"; "Meat" |]
        
        let inResult = 
            query {
                for p in queryable do
                where (targetCategories.Contains p.Category)
                select p
            } |> Seq.toList
        
        Assert.Equal(3, inResult.Length) // Apple, Banana, Beef
        Assert.False(inResult |> Seq.exists (fun p -> p.Category = "Vegetable"))

        let likeResult = 
            query {
                for p in queryable do
                where (p.Name.StartsWith "A")
                select p
            } |> Seq.toList
        
        Assert.Equal(2, likeResult.Length) // Apple, Avocado
        Assert.True(likeResult |> Seq.exists (fun p -> p.Name = "Apple"))
        Assert.True(likeResult |> Seq.exists (fun p -> p.Name = "Avocado"))

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

        Assert.Equal(2, complexResult.Length) 
        Assert.True(complexResult |> Seq.exists (fun p -> p.Name = "Apple"))
        Assert.True(complexResult |> Seq.exists (fun p -> p.Name = "Beef"))

    [<Fact>]
    [<Trait("LINQ", "PaginationAndDistinct")>]
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
        let queryable = df.AsQueryable<ProductDto>()

        // SQL: SELECT DISTINCT p."Category" FROM products p
        let distinctCategories = 
            query {
                for p in queryable do
                select p.Category
                distinct 
            } |> Seq.toList
        
        Assert.Equal(2, distinctCategories.Length)
        Assert.Contains("Fruit", distinctCategories)
        Assert.Contains("Meat", distinctCategories)

        let distinctProducts = 
            query {
                for p in queryable do
                distinct
            } |> Seq.toList
            
        Assert.Equal(6, distinctProducts.Length) 

        // SQL: SELECT ... FROM products ORDER BY p."Id" LIMIT 2 OFFSET 2
        let pagedResult = 
            query {
                for p in queryable do
                sortBy p.Id
                skip 2
                take 2
            } |> Seq.toList
            
        Assert.Equal(2, pagedResult.Length)
        
        Assert.Equal(3, pagedResult.[0].Id) 
        Assert.Equal(4, pagedResult.[1].Id)
        
    [<Fact>]
    [<Trait("LINQ", "CaseWhenAndCte")>]
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
        let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

        // SQL: CASE WHEN e."Salary" >= 6000 THEN 'High' ... END
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
        
        let assertTier expectedTier name = 
            let emp = caseWhenQuery |> Seq.find (fun e -> e.Name = name)
            Assert.Equal(expectedTier, emp.SalaryTier)

        assertTier "High" "Alice"   // 6000
        assertTier "High" "David"   // 8000
        assertTier "Medium" "Eve"     // 5500
        assertTier "Medium" "Charlie" // 4500
        assertTier "Low" "Bob"       // 4000

        // let cte = 
        //     empQuery
        //         .Where(fun e -> e.Salary > 5000.0)
        //         .AsCte "HighEarners"

        // let cteResult = 
        //     query {
        //         for c in cte do
        //         where (c.DeptId = 1 || c.DeptId = 3)
        //         select c
        //     } |> Seq.toList

        // Assert.Equal(2, cteResult.Length)
        // Assert.True(cteResult |> Seq.exists (fun e -> e.Name = "Alice"))
        // Assert.True(cteResult |> Seq.exists (fun e -> e.Name = "David"))
    [<Fact>]
    [<Trait("LINQ", "SubqueryInAndFunctions")>]
    let ``Test Polars Linq SubqueryIn And Functions`` () =
        let depts = [|
            { DeptId = 1; DeptName = "Engineering" }
            { DeptId = 2; DeptName = "Sales" }
            { DeptId = 3; DeptName = "HR" }
        |]

        let emps = [|
            { Name = "Alice";   DeptId = 1; Salary = 6000.0 }
            { Name = null;      DeptId = 2; Salary = 4000.0 } 
            { Name = "Charlie"; DeptId = 1; Salary = 4500.0 }
            { Name = "David";   DeptId = 3; Salary = 8000.0 }
        |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        let deptQuery = dfDepts.AsQueryable<DeptDto>()
        let empQuery = dfEmps.AsQueryable<NullableEmpDto>()

        // SQL: d."DeptId" IN (SELECT e."DeptId" FROM employees e WHERE e."Salary" > 5000)
        
        let highPaidDeptIds = 
            query {
                for e in empQuery do
                where (e.Salary > 5000.0)
                select e.DeptId
            }

        let richDepts = 
            query {
                for d in deptQuery do
                where (highPaidDeptIds.Contains d.DeptId)
                select d
            } 
            |> Seq.toList

        Assert.Equal(2, richDepts.Length) // Engineering (Alice) HR (David)
        Assert.True(richDepts |> Seq.exists (fun d -> d.DeptName = "Engineering"))
        Assert.True(richDepts |> Seq.exists (fun d -> d.DeptName = "HR"))

        // SQL: CASE WHEN e."Name" IS NULL THEN 'Unknown' ELSE e."Name" END
        let coalesceQuery = 
            query {
                for e in empQuery do
                select {|
                    SafeName = if e.Name = null then "Unknown" else e.Name
                |}
            } 
        let coalesceResult = coalesceQuery |> Seq.toList
        Assert.Equal(4, coalesceResult.Length)
        Assert.True(coalesceResult |> Seq.exists (fun e -> e.SafeName = "Unknown")) 

        // SQL: SUBSTRING(e."Name", 1, 3)
        let stringQuery = 
            query {
                for e in empQuery do
                where (e.Name <> null)
                select {|
                    Name = e.Name
                    ShortName = e.Name.Substring(0, 3)
                |}
            } 
            |> Seq.toList

        Assert.Equal(3, stringQuery.Length)
        
        let getShortName name = 
            (stringQuery |> Seq.find (fun e -> e.Name = name)).ShortName

        Assert.Equal("Ali", getShortName "Alice")
        Assert.Equal("Cha", getShortName "Charlie")

    [<Fact>]
    [<Trait("LINQ", "MathStringAndConditionalAgg")>]
    let ``Test Polars Linq Math String And ConditionalAgg`` () =
        let sales = [|
            { Category = "Tech";   ProductName = "Laptop"; Revenue = 1000.5; Discount = 50.0 }
            { Category = "Tech";   ProductName = "Mouse";  Revenue = -20.0;  Discount = 0.0 }
            { Category = "Office"; ProductName = "Desk";   Revenue = 500.2;  Discount = 10.0 }
            { Category = "Office"; ProductName = "Chair";  Revenue = 150.8;  Discount = 5.0 }
        |]

        use dfSales = DataFrame.ofRecords sales
        let salesQuery = dfSales.AsQueryable<SalesData>()

        // CONCAT / Math.Round -> ROUND / Math.Abs -> ABS
        let scalarQuery = 
            query {
                for s in salesQuery do
                select {|
                    FullName = s.Category + " - " + s.ProductName
                    NetRevenue = Math.Round(Math.Abs s.Revenue - s.Discount, 2)
                |}
            } |> Seq.toList

        Assert.Equal(4, scalarQuery.Length)
        Assert.True(scalarQuery |> Seq.exists (fun s -> s.FullName = "Tech - Laptop" && s.NetRevenue = 950.5))
        Assert.True(scalarQuery |> Seq.exists (fun s -> s.FullName = "Tech - Mouse" && s.NetRevenue = 20.0))

        // SQL: SUM(CASE WHEN s."Category" = 'Tech' THEN ABS(s."Revenue") ELSE 0 END)
        
        let aggQuery = 
            salesQuery
                .GroupBy(fun s -> 1) 
                .Select(fun g -> {|
                    Total = g.Sum(fun x -> Math.Abs x.Revenue)
                    
                    TechTotal = g.Sum(fun x -> if x.Category = "Tech" then Math.Abs x.Revenue else 0.0)
                    OfficeTotal = g.Sum(fun x -> if x.Category = "Office" then Math.Abs x.Revenue else 0.0)
                |})
                .ToList()

        Assert.Single aggQuery |> ignore
        Assert.Equal(1671.5, aggQuery.[0].Total) // 1000.5 + 20 + 500.2 + 150.8
        Assert.Equal(1020.5, aggQuery.[0].TechTotal)
        Assert.Equal(651.0,  aggQuery.[0].OfficeTotal)

    [<Fact>]
    [<Trait("LINQ", "BitwiseAndRegex")>]
    let ``Test Polars Linq Bitwise and Regex`` () =
        let logs = [|
            { Id = 1; Message = "User admin logged in";  Flags = 3 }  // 3 (0011)
            { Id = 2; Message = "Failed password try";   Flags = 1 }  // 1 (0001)
            { Id = 3; Message = "DB connection timeout"; Flags = 6 }  // 6 (0110)
            { Id = 4; Message = "User guest logged in";  Flags = 2 }  // 2 (0010)
            { Id = 5; Message = "System crash error 99"; Flags = 4 }  // 4 (0100)
        |]

        use df = DataFrame.ofRecords logs
        let logQuery = df.AsQueryable<ServerLog>()

        let bitwiseResult = 
            query {
                for log in logQuery do
                where (log.Flags &&& 2 = 2)
                select log
            } |> Seq.toList

        Assert.Equal(3, bitwiseResult.Length)
        Assert.True(bitwiseResult |> Seq.exists(fun l -> l.Id = 1))
        Assert.True(bitwiseResult |> Seq.exists(fun l -> l.Id = 3))
        Assert.True(bitwiseResult |> Seq.exists(fun l -> l.Id = 4))

    //     let regexResult = 
    //         query {
    //             for log in logQuery do
    //             where (PolarsSql.RegexMatch(log.Message, "error|timeout|Failed"))
    //             select log
    //         } |> Seq.toList

    //     Assert.Equal(3, regexResult.Length)
    //     Assert.True(regexResult |> Seq.exists(fun l -> l.Id = 2))
    //     Assert.True(regexResult |> Seq.exists(fun l -> l.Id = 3))
    //     Assert.True(regexResult |> Seq.exists(fun l -> l.Id = 5))

    [<Fact>]
    [<Trait("LINQ", "Aggregate")>]
    let ``Test Polars Linq Aggregate Overloads with Person and EmployeeSalary`` () =
        // 1. Prepare base test data using existing Person record
        let people = [
            { Name = "Alice"; Age = 25; Sales = 1000.0 }
            { Name = "Bob"; Age = 30; Sales = 2500.0 }
            { Name = "Charlie"; Age = 35; Sales = 4500.0 }
        ]

        use df = DataFrame.ofRecords people
        let query = df.AsQueryable<Person>()

        // Overload 2: Aggregate(seed, folder) -> TAccum * TSource -> TAccum
        // Calculate total sales accumulated onto an initial seed of 500.0
        let totalSalesWithBonus =
            query.Aggregate(
                500.0,
                fun acc p -> acc + p.Sales
            )

        Assert.Equal(8500.0, totalSalesWithBonus)

        // Overload 3: Aggregate(seed, folder, resultSelector) -> TAccum * TSource -> TAccum, TAccum -> TResult
        // Calculate average sales via tuple accumulation, then project to formatted string
        let salesSummary =
            query.Aggregate(
                (0.0, 0),
                (fun (total, count) p -> total + p.Sales, count + 1),
                (fun (total, count) -> sprintf "Count=%d, Avg=%.1f" count (total / float count))
            )

        Assert.Equal("Count=3, Avg=2666.7", salesSummary)

        // 2. Prepare test data with EmployeeSalary and chained Native Filter
        let employees = [
            { Name = "David"; DeptId = 1; Salary = 5000.0 }
            { Name = "Eva"; DeptId = 1; Salary = 9000.0 }
            { Name = "Frank"; DeptId = 2; Salary = 12000.0 }
        ]

        use empDf = DataFrame.ofRecords employees
        let empQuery = empDf.AsQueryable<EmployeeSalary>()

        // Combine Native Where pushdown + Aggregate folding
        let highSalaryTotal =
            empQuery
                .Where(fun e -> e.Salary > 6000.0)
                .Aggregate(
                    0.0,
                    fun acc e -> acc + e.Salary
                )

        Assert.Equal(21000.0, highSalaryTotal)

    [<Fact>]
    [<Trait("LINQ", "AggregateBy")>]
    let ``Test Polars Linq AggregateBy with Static Seed using EmployeeSalary`` () =
        // Prepare test data using existing EmployeeSalary record
        let employees = [
            { Name = "Alice"; DeptId = 1; Salary = 5000.0 }
            { Name = "Bob"; DeptId = 2; Salary = 7000.0 }
            { Name = "Charlie"; DeptId = 1; Salary = 6000.0 }
            { Name = "David"; DeptId = 2; Salary = 8000.0 }
            { Name = "Eva"; DeptId = 1; Salary = 9000.0 }
        ]

        use df = DataFrame.ofRecords employees
        let query = df.AsQueryable<EmployeeSalary>()

        // Overload 1: AggregateBy(keySelector, seed, folder)
        // Group by DeptId and fold: (totalSalary, headCount)
        let results =
            query.AggregateBy(
                (fun (e: EmployeeSalary) -> e.DeptId),
                (0.0, 0),
                (fun (total, count) (e: EmployeeSalary) -> (total + e.Salary, count + 1))
            )
            |> Seq.map (fun kv -> kv.Key, kv.Value)
            |> Map.ofSeq

        // Assert Dept 1: Alice(5000) + Charlie(6000) + Eva(9000) = 20000.0, Count = 3
        Assert.True(results.ContainsKey 1)
        Assert.Equal((20000.0, 3), results.[1])

        // Assert Dept 2: Bob(7000) + David(8000) = 15000.0, Count = 2
        Assert.True(results.ContainsKey 2)
        Assert.Equal((15000.0, 2), results.[2])

    [<Fact>]
    [<Trait("LINQ", "AggregateBy")>]
    let ``Test Polars Linq AggregateBy with Dynamic SeedFactory using Person`` () =
        // Prepare test data using existing Person record
        let people = [
            { Name = "Alice"; Age = 25; Sales = 1000.0 }
            { Name = "Bob"; Age = 35; Sales = 1500.0 }
            { Name = "Charlie"; Age = 25; Sales = 2000.0 }
            { Name = "David"; Age = 45; Sales = 3000.0 }
        ]

        use df = DataFrame.ofRecords people
        let query = df.AsQueryable<Person>()

        // Overload 2: AggregateBy(keySelector, seedSelector, folder)
        // Group by Age: Dynamic baseline starting balance based on age (e.g. baseline = age * 10.0)
        let results =
            query.AggregateBy(
                (fun (p: Person) -> p.Age),
                (fun (age: int) -> float age * 10.0), // Dynamic seed: 25 -> 250.0, 35 -> 350.0, 45 -> 450.0
                (fun (accBalance: float) (p: Person) -> accBalance + p.Sales)
            )
            |> Seq.map (fun kv -> kv.Key, kv.Value)
            |> Map.ofSeq

        // Age 25: Baseline(250.0) + Alice(1000.0) + Charlie(2000.0) = 3250.0
        Assert.Equal(3250.0, results.[25])
        // Age 35: Baseline(350.0) + Bob(1500.0) = 1850.0
        Assert.Equal(1850.0, results.[35])
        // Age 45: Baseline(450.0) + David(3000.0) = 3450.0
        Assert.Equal(3450.0, results.[45])

    [<Fact>]
    [<Trait("LINQ", "AggregateBy")>]
    let ``Test Polars Linq AggregateBy with Native Filter Pushdown`` () =
        let employees = [
            { Name = "Alice"; DeptId = 10; Salary = 3000.0 }
            { Name = "Bob"; DeptId = 10; Salary = 8000.0 }
            { Name = "Charlie"; DeptId = 20; Salary = 4000.0 }
            { Name = "David"; DeptId = 20; Salary = 9000.0 }
        ]

        use df = DataFrame.ofRecords employees
        let query = df.AsQueryable<EmployeeSalary>()

        // Pipeline: Native Where Pushdown -> Fast Cursor AggregateBy
        let highSalaryByDept =
            query
                .Where(fun e -> e.Salary > 5000.0) // Bob and David will pass
                .AggregateBy(
                    (fun e -> e.DeptId),
                    0.0,
                    (fun totalSalary e -> totalSalary + e.Salary)
                )
                |> Seq.map (fun kv -> kv.Key, kv.Value)
                |> Map.ofSeq

        Assert.Equal(8000.0, highSalaryByDept.[10])
        Assert.Equal(9000.0, highSalaryByDept.[20])

    // [<Fact>]
    // [<Trait("LINQ", "LeadLag")>]
    // let ``Test Polars Linq LeadLag And NestedList`` () =
    //     // Arrange
    //     let stocks = [|
    //         { Ticker = "AAPL"; Date = DateTime(2024, 1, 1); Price = 150.0 }
    //         { Ticker = "AAPL"; Date = DateTime(2024, 1, 2); Price = 155.0 }
    //         { Ticker = "AAPL"; Date = DateTime(2024, 1, 3); Price = 152.0 }
    //         { Ticker = "MSFT"; Date = DateTime(2024, 1, 1); Price = 300.0 }
    //         { Ticker = "MSFT"; Date = DateTime(2024, 1, 2); Price = 305.0 }
    //     |]

    //     let depts = [| { DeptId = 1; DeptName = "Tech" }; { DeptId = 2; DeptName = "Sales" } |]
    //     let emps = [| { Name = "Alice"; DeptId = 1 }; { Name = "Bob"; DeptId = 1 }; { Name = "Charlie"; DeptId = 2 } |]

    //     use dfStocks = DataFrame.ofRecords stocks
    //     use dfDepts = DataFrame.ofRecords depts
    //     use dfEmps = DataFrame.ofRecords emps

    //     use sqlCtx = new SqlContext()
    //     use db = new PolarsDataContext(sqlCtx)
        
    //     let stockQuery = db.RegisterTable<StockPrice> dfStocks

    //     // SQL: LAG(s."Price") OVER(PARTITION BY s."Ticker" ORDER BY s."Date")
    //     let lagQuery = 
    //         query {
    //             for s in stockQuery do
    //             select {|
    //                 Ticker = s.Ticker
    //                 Date = s.Date
    //                 Price = s.Price
                    
    //                 PrevPrice = Sql.Ext.Lag(s.Price).Over().PartitionBy(s.Ticker).OrderBy(s.Date).ToValue()
    //             |}
    //         } |> Seq.toList

    //     Assert.Equal(5, lagQuery.Length)
        
    //     let aaplDay2 = lagQuery |> Seq.find (fun s -> s.Ticker = "AAPL" && s.Date.Day = 2)
    //     Assert.Equal(155.0, aaplDay2.Price)
    //     Assert.Equal(150.0, aaplDay2.PrevPrice)

    // [<Fact>]
    // [<Trait("LINQ", "NestedList")>]
    // let ``Test Polars Linq Nested List Aggregation`` () =
    //     let depts = [| { DeptId = 1; DeptName = "Tech" }; { DeptId = 2; DeptName = "Sales" } |]
    //     let emps = [| 
    //         { Name = "Alice"; DeptId = 1 }
    //         { Name = "Bob"; DeptId = 1 }
    //         { Name = "Charlie"; DeptId = 2 } 
    //     |]

    //     use dfDepts = DataFrame.ofRecords depts
    //     use dfEmps = DataFrame.ofRecords emps

    //     let deptQuery = dfDepts.AsQueryable<DeptDto>()
    //     let empQuery = dfEmps.AsQueryable<EmpDto>()

    //     // SQL: SELECT e."DeptId", list(e."Name") FROM employees e GROUP BY e."DeptId"
    //     let nestedListQuery = 
    //         empQuery
    //             .GroupBy(fun e -> e.DeptId)
    //             .Select(fun g -> {|
    //                 DeptId = g.Key
                    
    //                 EmpNames = PolarsSql.ListAgg(g, fun e -> e.Name)
    //             |})
    //             .OrderBy(fun r -> r.DeptId)
    //             .ToList()

    //     let techDepts = nestedListQuery.[0]
    //     Assert.Equal(1, techDepts.DeptId)
        
    //     Assert.Contains("Alice", techDepts.EmpNames)
    //     Assert.Contains("Bob", techDepts.EmpNames)

    [<Fact>]
    [<Trait("LINQ", "Sandwich")>]
    let ``Test Polars Double Hybrid Sandwich`` () =
        use schema = PolarsSchema.FromRecord<StaffRecord>()
        
        let path = "/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv"
        
        // ==========================================
        // Create Plan
        // ==========================================
        use rawLf = LazyFrame.ScanCsv(path, schema = schema)
        let emps = rawLf.AsQueryable<StaffRecord>()
        
        // ==========================================
        // Query Block 
        // ==========================================
        let linqQuery = 
            query {
                for e in emps do
                where (e.salary > 5000.0)
                select {| name = e.name; salary = e.salary |}
            }
            
        // printfn "--- Plan 1 (After LINQ) ---\n%s" (linqQuery.Explain true)

        // ==========================================
        // LazyFrame
        // ==========================================
        use lfWithLinq = linqQuery.ToLazyFrame()

        use finalLf = lfWithLinq.WithColumns(pl.col("salary").Std().Alias "salary_std")
        
        // ==========================================
        // Collect
        // ==========================================
        
        let df = finalLf |> LazyFrame.collect

        Assert.True(df.Height > 0L)

    [<Fact>]
    [<Trait("LINQ", "SeriesScalar")>]
    let ``Test Polars Linq Series`` () =
        let numbers = [| 1 .. 100 |]
        use series = Series.create("my_numbers", numbers)
        
        let queryable = series.As<int>().ToResizeArray()

        let seriesquery = 
            query {
                for x in queryable do
                where (x > 90)
                sortByDescending x
                select x
            } |> Seq.toList

        
        // let result = seriesquery.ToSeries()
        
        Assert.Equal(10, seriesquery.Count())

    // [<Fact>]
    // [<Trait("LINQ", "SyntaxSugar")>]
    // let ``Test Ultimate StrongTyped Select Sugar in FSharp`` () =
        
    //     let records = [
    //         { salary = 10.0 }
    //         { salary = 20.0 }
    //         { salary = 30.0 }
    //     ]
    //     use df = DataFrame.ofRecords records

    //     let exprsList = 
    //         PolarsExpr.ToSqls(fun (e: SalaryRecord) -> 
    //             {| 
    //                 salary_sq = Math.Pow(e.salary, 2.0)
    //                 salary_dbl = e.salary * 2.0
    //                 is_high = e.salary > 15.0
    //             |}
    //         ) 
    //         |> Expr.SqlExprs
    //         |> Array.toList
        
    //     use resultDf = df.Select exprsList

    //     resultDf.Show() |> ignore

    //     let sqArr = resultDf.["salary_sq"].ToArray<float>()
    //     let dblArr = resultDf.["salary_dbl"].ToArray<float>()
    //     let isHighArr = resultDf.["is_high"].ToArray<bool>()

    //     Assert.Equal(3, sqArr.Length)

    //     Assert.Equal(100.0, sqArr.[0])
    //     Assert.Equal(20.0, dblArr.[0])
    //     Assert.False(isHighArr.[0])

    //     Assert.Equal(900.0, sqArr.[2])
    //     Assert.Equal(60.0, dblArr.[2])
    //     Assert.True(isHighArr.[2])

    [<Fact>]
    [<Trait("LINQ", "HybridLazy")>]
    let ``Test Polars Linq Hybrid Native And Linq Pushdown`` () =
        
        let csvContent = 
             "Alice,25,50000\n\
             Bob,30,60000\n\
             Charlie,35,70000\n\
             David,40,80000"
             
        let fileName = "test_hybrid_lazy_data_fsharp.csv"
        File.WriteAllText(fileName, csvContent)

        try
            use overrideSchema = PolarsSchema.FromRecord<StaffRecord>()
            
            use lf = LazyFrame.ScanCsv(fileName, schema = overrideSchema,hasHeader=false)
            
            use lfWithBonus = lf.WithColumns [(pl.col "salary" * pl.lit 0.1).Alias "bonus"]

            let employees = lfWithBonus.AsQueryable<StaffRecordWithBonus>()
            
            let linqQuery = 
                query {
                    for e in employees do
                    where (e.age > 30 && e.bonus >= 7000.0)
                    select {| name = e.name; TotalCompensation = float e.salary + e.bonus |}
                }

            let results = linqQuery |> Seq.toList

            Assert.NotNull results
            Assert.Equal(2, results.Length) 

            Assert.Equal("Charlie", results.[0].name)
            Assert.Equal(77000.0, results.[0].TotalCompensation)

            Assert.Equal("David", results.[1].name)
            Assert.Equal(88000.0, results.[1].TotalCompensation)

        finally
            if File.Exists fileName then File.Delete fileName

    // [<Fact>]
    // [<Trait("LINQ", "UnifiedCRUD")>]
    // let ``Test Polars Linq Unified CRUD UX in FSharp`` () =
        
    //     let emps = [
    //         { Name = "Alice"; DeptId = 1; Salary = 5000.0 }
    //         { Name = "Bob";   DeptId = 2; Salary = 4000.0 }
    //         { Name = "Eve";   DeptId = 3; Salary = 3000.0 }
    //     ]

    //     use dfEmps = DataFrame.ofRecords emps
    //     use sqlCtx = new SqlContext()
    //     use db = new PolarsDataContext(sqlCtx, ownsContext = true)
        
    //     let table = db.RegisterTable<EmployeeSalary> dfEmps

    //     let richEmps = 
    //         query {
    //             for e in table do
    //             where (e.Salary >= 5000.0)
    //             select e
    //         } |> Seq.toList

    //     Assert.Equal(1, richEmps.Length)
    //     Assert.Equal("Alice", richEmps.[0].Name)

    //     try
    //         table.Where(fun (e: EmployeeSalary) -> e.DeptId = 1)
    //              .Set((fun (e: EmployeeSalary) -> e.Salary), (fun (e: EmployeeSalary) -> e.Salary + 1000.0))
    //              .Update() |> ignore
    //     with 
    //     | ex -> Console.WriteLine $"Expected Update Error: {ex.Message}"

    //     let deleted = table.Where(fun e -> e.DeptId < 3).Delete()
    //     use deletedDf = table.ToDataFrame()
    //     deletedDf.Show() 
    //     Assert.True(deleted >= 0)
    
    // [<Fact>]
    // [<Trait("LINQ", "Async_Stress_ToDataFrame")>]
    // let ``Test Polars Linq High Concurrency ToDataFrameAsync Stress`` () = task {
        
    //     let recordCount = 100_000
        
    //     let mockData = 
    //         Array.init recordCount (fun i -> 
    //             { Id = i
    //               Region = sprintf "Region_%d" (i % 50)
    //               Latency = Random.Shared.NextDouble() * 100.0 }
    //         )

    //     use df = DataFrame.ofRecords mockData 

    //     let simulateDataFrameQueryAsync (workerId: int) = task {
    //         use ctx = new SqlContext()
    //         use db = new PolarsDataContext(ctx)
            
    //         let table = db.RegisterTable<TrafficRecord> df
    //         let targetRegion = sprintf "Region_%d" (workerId % 50)

    //         // let q = 
    //         //     query {
    //         //         for t in table do
    //         //         where (t.Region = targetRegion && t.Latency > 10.0)
    //         //         sortBy t.Id
    //         //         select t
    //         //     }
    //         let q = 
    //             table
    //                 .Where(fun t -> t.Region = targetRegion && t.Latency > 10.0)
    //                 .OrderBy(fun t -> t.Id)

    //         let! idf = q.ToDataFrameAsync()
    //         use resultDf = idf
            
    //         return resultDf.Height
    //     }

    //     let concurrencyLevel = 1000
        
    //     let tasks = Array.init concurrencyLevel simulateDataFrameQueryAsync

    //     let! finalHeights = Task.WhenAll tasks

    //     Assert.Equal(concurrencyLevel, finalHeights.Length)

    //     for height in finalHeights do
    //         Assert.True(height > 0L && height <= 2000L)
    // }