namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.FSharp.Query
open Polars.FSharp
open System
open System.IO
open System.Collections.Generic

type Person =
    { Name: string; Age: int; Sales: float }

type Department = { DeptId: int; DeptName: string }
type Employee = { Name: string; DeptId: int }

type EmpDeptDto =
    {
        EmpName: string
        DepartmentName: string
    }

type EmployeeSalary =
    {
        Name: string
        DeptId: int
        Salary: float
    }

type DeptStatsDto =
    {
        DeptId: int
        TotalSalary: float
        EmployeeCount: int
    }

type OrderDto =
    {
        OrderId: int
        OrderDate: DateTime
        Region: string
        Revenue: float
    }

type ProductDto =
    {
        Id: int
        Name: string
        Category: string
        Price: float
    }

type DeptDto = { DeptId: int; DeptName: string }

type NullableEmpDto =
    {
        Name: string
        DeptId: int
        Salary: float
    }

type SalesData =
    {
        Category: string
        Item: string
        Revenue: float
        Discount: float
    }

type ServerLog =
    { Id: int; Message: string; Flags: int }

type StockPrice =
    {
        Ticker: string
        Date: DateTime
        Price: float
    }

type StaffRecord =
    {
        name: string
        age: int
        salary: float
    }

[<Struct>]
type SalaryRecord = { salary: float }

type StaffRecordWithBonus =
    {
        name: string
        age: int
        salary: float
        bonus: float
    }

type TrafficRecord =
    {
        Id: int
        Region: string
        Latency: float
    }

type EmpDto = { Name: string; DeptId: int }

type PlayerOptionRecord =
    {
        Id: int
        Nickname: string option
        Score: int voption
        LastLogin: DateTime voption
    }

type LinqSensorRecord =
    {
        SensorId: int
        Temperature: float
        Location: string
    }

type WideRecord =
    {
        Id: int
        Name: string
        Category: string
        ValFloat1: float
        ValFloat2: float
        ValDec1: decimal
        ValDec2: decimal
        ValInt64: int64
        ValInt32: int
        ValBool: bool
        Col11: string
        Col12: float
        Col13: decimal
        Col14: int
        Col15: int64
        Col16: bool
        Col17: string
        Col18: float
        Col19: decimal
        Col20: int
    }

type Address =
    {
        City: string
        Zip: int
    }

type PersonWithAddress =
    {
        Id: int
        Name: string
        Addr: Address
    }

type GeoLocation =
    {
        Lat: float
        Lon: float
    }

type DetailedAddress =
    {
        City: string
        Geo: GeoLocation
    }

type DeepUserRecord =
    {
        Id: int
        Username: string
        Location: DetailedAddress
    }

module QueryTests =
    [<Literal>]
    let nativePushdown = "100% Native Pushdown"

    [<Literal>]
    let hybridPushdown = "Hybrid Execution"

    [<Literal>]
    let clientFallback = "Full Fallback to In-Memory LINQ"

    let inline assertPlan (plan: string) (query: IQueryable<'T>) =
        let expl = query.Explain false

        if not (expl.Contains plan) then
            failwithf
                "assertPlan failed! Expected to contain: '%s'\nActual plan:\n%s\nExpression:\n%s"
                plan
                expl
                (query.Expression.ToString())

        Assert.Contains(plan, expl)
        query

    [<Fact; Trait("LINQ", "Option")>]
    let ``Test FSharp Option Scalar Aggregations with Empty Sequence`` () =
        let emptyRows: TrafficRecord option array = [||]
        use df = DataFrame.ofSeq emptyRows
        let query = df.AsQueryable emptyRows

        // Min / Max on empty option sequence returns None instead of throwing
        let minResult = query.Min()
        Assert.Equal(None, minResult)

        let maxResult = query.Max()
        Assert.Equal(None, maxResult)

    [<Fact; Trait("LINQ", "MultiJoinStressTest")>]
    let ``Test Ultimate Multi-Level Left and Inner Join Stress Scenario`` () =
        // -----------------------------------------------------------------
        // 1. Arrange: 4 dataframes
        // -----------------------------------------------------------------

        // Depts
        let depts =
            [|
                {
                    DeptId = 10
                    DeptName = "Engineering"
                }
                { DeptId = 20; DeptName = "Sales" }
                { DeptId = 30; DeptName = "HR" } // No Employee,no Sales，no Traffic
                { DeptId = 40; DeptName = "Marketing" } // Has Sales，no Employee, no Traffic
            |]

        // Emps
        let emps =
            [|
                { Name = "Alice"; DeptId = 10 }
                { Name = "Bob"; DeptId = 20 }
                { Name = "Charlie"; DeptId = 10 }
            |]

        // Sales
        let sales =
            [|
                {
                    Category = "Engineering"
                    Item = "Polars.NET"
                    Revenue = 5000.0
                    Discount = 500.0
                }
                {
                    Category = "Marketing"
                    Item = "Ad Campaign"
                    Revenue = 3000.0
                    Discount = 200.0
                }
            |]

        // Traffic
        let traffic =
            [|
                {
                    Id = 10
                    Region = "US-East"
                    Latency = 12.5
                }
                {
                    Id = 20
                    Region = "EU-West"
                    Latency = 45.0
                }
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
                                thenByDescending (if isNull (box e) then "" else e.Name)

                                // Project
                                select
                                    {|
                                        Department = d.DeptName
                                        EmpName = if isNull (box e) then "NO_EMPLOYEE" else e.Name
                                        Region = if isNull (box t) then "UNKNOWN_REGION" else t.Region
                                        Latency = if isNull (box t) then -1.0 else t.Latency
                                        ProductName = if isNull (box s) then "NO_PRODUCT" else s.Item
                                        ComputedDeptCode = baseDeptCode
                                    |}
            }
            |> assertPlan nativePushdown

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

    [<Fact; Trait("LINQ", "Join")>]
    let ``Test MultiKey and chained Join via LINQ and QueryBuilder`` () =
        use empDf =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob"; "Charlie"; "David" ])
                    Series.create ("DeptId", [ 10; 20; 10; 30 ])
                ]

        use salDf =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob"; "Charlie"; "David" ])
                    Series.create ("DeptId", [ 10; 99; 10; 30 ])
                    Series.create ("Salary", [ 9500.0; 8000.0; 12000.0; 15000.0 ])
                ]

        use deptDf =
            DataFrame.create
                [
                    Series.create ("DeptId", [ 10; 30 ])
                    Series.create ("DeptName", [ "Engineering"; "Finance" ])
                ]

        let qEmp = empDf.AsQueryable<Employee>()
        let qSal = salDf.AsQueryable<EmployeeSalary>()
        let qDept = deptDf.AsQueryable<Department>()

        // Chained Multi-Key Join
        let results =
            qEmp
                .Join(
                    qSal,
                    (fun (e: Employee) -> e.Name, e.DeptId),
                    (fun (s: EmployeeSalary) -> s.Name, s.DeptId),
                    (fun e s -> e.Name, e.DeptId, s.Salary)
                )
                .Join(
                    qDept,
                    (fun (name, deptId, salary) -> deptId),
                    (fun (d: Department) -> d.DeptId),
                    (fun (name, deptId, salary) d ->
                        {|
                            Name = name
                            Dept = d.DeptName
                            Salary = salary
                        |})
                )
            |> assertPlan nativePushdown
            |> Seq.sortBy (fun x -> x.Salary)
            |> Seq.toList

        Assert.Equal(3, results.Length)
        Assert.Equal("Alice", results.[0].Name)
        Assert.Equal("Engineering", results.[0].Dept)
        Assert.Equal(9500.0, results.[0].Salary)

        Assert.Equal("David", results.[2].Name)
        Assert.Equal("Finance", results.[2].Dept)
        Assert.Equal(15000.0, results.[2].Salary)

    [<Fact; Trait("LINQ", "FSharpOptions")>]
    let ``Test Polars FSharp Option And ValueOption Accessor`` () =

        let data =
            [|
                {
                    Id = 1
                    Nickname = Some "Alice"
                    Score = ValueSome 100
                    LastLogin = ValueSome DateTime.Now
                }
                {
                    Id = 2
                    Nickname = None
                    Score = ValueSome 80
                    LastLogin = ValueSome DateTime.Now
                }
                {
                    Id = 3
                    Nickname = Some "Bob"
                    Score = ValueNone
                    LastLogin = ValueSome DateTime.Now
                }
                {
                    Id = 4
                    Nickname = None
                    Score = ValueNone
                    LastLogin = ValueNone
                }
            |]

        use df = DataFrame.ofRecords data

        let table = df.AsQueryable<PlayerOptionRecord>()

        let results =
            query {
                for p in table do
                    sortBy p.Id
                    select p
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

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

    [<Fact; Trait("LINQ", "Where")>]
    let ``Test Polars FSharp Linq Where And OrderBy`` () =
        let data =
            [|
                {
                    Name = "Alice"
                    Age = 25
                    Sales = 100.0
                }
                {
                    Name = "Bob"
                    Age = 30
                    Sales = 200.0
                }
                {
                    Name = "Charlie"
                    Age = 35
                    Sales = 300.0
                }
                {
                    Name = "David"
                    Age = 18
                    Sales = 50.0
                }
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
            |> assertPlan nativePushdown

        let results = queryable.ToArray()

        Assert.NotNull results
        Assert.Equal(2, results.Length)

        Assert.Equal("Charlie", results.[0].Name)
        Assert.Equal(35, results.[0].Age)
        Assert.Equal(300.0, results.[0].Sales)

        Assert.Equal("Bob", results.[1].Name)
        Assert.Equal(30, results.[1].Age)
        Assert.Equal(200.0, results.[1].Sales)

    [<Fact; Trait("LINQ", "Join")>]
    let ``Test Polars Linq Inner Join`` () =
        let depts =
            [|
                { DeptId = 1; DeptName = "Engineering" }
                { DeptId = 2; DeptName = "Sales" }
            |]

        let emps =
            [|
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

                    select
                        {
                            EmpName = e.Name
                            DepartmentName = d.DeptName
                        }
            }
            |> assertPlan nativePushdown

        let results = queryable.ToList()

        Assert.Equal(2, results.Count)
        Assert.Equal("Alice", results.[0].EmpName)
        Assert.Equal("Engineering", results.[0].DepartmentName)

    [<Fact; Trait("LINQ", "GroupByHaving")>]
    let ``Test Polars Linq GroupBy Aggregation With Having`` () =
        let emps =
            [|
                {
                    Name = "Alice"
                    DeptId = 1
                    Salary = 5000.0
                }
                {
                    Name = "Bob"
                    DeptId = 2
                    Salary = 4000.0
                }
                {
                    Name = "Charlie"
                    DeptId = 1
                    Salary = 6000.0
                }
                {
                    Name = "David"
                    DeptId = 2
                    Salary = 4500.0
                }
                {
                    Name = "Eve"
                    DeptId = 3
                    Salary = 3000.0
                }
            |]

        use dfEmps = DataFrame.ofRecords emps

        let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

        let queryable =
            query {
                for e in empQuery do
                    groupBy e.DeptId into g

                    where (g.Sum(fun x -> x.Salary) > 5000.0)

                    sortBy g.Key

                    select
                        {
                            DeptId = g.Key
                            TotalSalary = g.Sum(fun x -> x.Salary)
                            EmployeeCount = g.Count()
                        }
            }
            |> assertPlan nativePushdown

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

    [<Fact; Trait("LINQ", "ScalarAndFirst")>]
    let ``Test Polars Linq Scalar And First`` () =
        // Anonymous Records
        let data =
            [|
                {| Id = 1; Name = "Alice"; Score = 80 |}
                {| Id = 2; Name = "Bob"; Score = 90 |}
                {|
                    Id = 3
                    Name = "Charlie"
                    Score = 85
                |}
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

    [<Fact; Trait("LINQ", "LeftJoin")>]
    let ``Test Polars Linq Left Join`` () =
        let depts =
            [|
                { DeptId = 1; DeptName = "Engineering" }
                { DeptId = 2; DeptName = "Sales" }
                { DeptId = 3; DeptName = "HR" }
            |]

        let emps =
            [|
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
                for d in deptQuery do
                    leftOuterJoin e in empQuery on (d.DeptId = e.DeptId) into empGroup

                    for e in empGroup.DefaultIfEmpty() do
                        sortBy d.DeptId
                        thenBy e.Name

                        select
                            {|
                                DeptName = d.DeptName

                                EmployeeName = if isNull (box e) then "NO_EMPLOYEE" else e.Name
                            |}
            }

        assertPlan nativePushdown queryable |> ignore

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

    [<Fact; Trait("LINQ", "UnionAndCrossJoin")>]
    let ``Test Polars Linq Union And Cross Join`` () =
        let depts =
            [|
                {|
                    DeptId = 1
                    DeptName = "Engineering"
                |}
                {| DeptId = 2; DeptName = "Sales" |}
            |]

        let emps = [| {| Name = "Alice"; DeptId = 1 |}; {| Name = "Bob"; DeptId = 2 |} |]

        use dfDepts = DataFrame.ofRecords depts
        use dfEmps = DataFrame.ofRecords emps

        let deptQuery = dfDepts.AsQueryable depts
        let empQuery = dfEmps.AsQueryable emps

        // SQL: SELECT ... FROM departments d CROSS JOIN employees e
        let crossJoinQuery =
            query {
                for d in deptQuery do
                    for e in empQuery do
                        select
                            {|
                                DeptName = d.DeptName
                                Name = e.Name
                            |}
            }
            |> assertPlan nativePushdown

        let crossResult = crossJoinQuery.ToList()

        Assert.Equal(4, crossResult.Count)

        Assert.True(
            crossResult
            |> Seq.exists (fun x -> x.DeptName = "Engineering" && x.Name = "Alice")
        )

        Assert.True(crossResult |> Seq.exists (fun x -> x.DeptName = "Sales" && x.Name = "Bob"))

        // SQL: SELECT ... FROM employees WHERE ... UNION ALL SELECT ... FROM employees WHERE ...
        let query1 =
            query {
                for e in empQuery do
                    where (e.DeptId = 1)
                    select e
            }
            |> assertPlan nativePushdown

        let query2 =
            query {
                for e in empQuery do
                    where (e.DeptId = 2)
                    select e
            }
            |> assertPlan nativePushdown

        let unionResult = query1.Concat(query2).ToList()

        Assert.Equal(2, unionResult.Count)
        Assert.True(unionResult |> Seq.exists (fun x -> x.Name = "Alice"))
        Assert.True(unionResult |> Seq.exists (fun x -> x.Name = "Bob"))

        let upperResult =
            query {
                for e in empQuery do
                    select (e.Name.ToUpper())
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Contains("ALICE", upperResult)
        Assert.Contains("BOB", upperResult)

    [<Fact; Trait("LINQ", "AdvancedSetsAndLet")>]
    let ``Test Polars Linq Except Intersect And Let`` () =
        // Arrange
        let emps =
            [|
                {
                    Name = "Alice"
                    DeptId = 1
                    Salary = 6000.0
                }
                {
                    Name = "Bob"
                    DeptId = 2
                    Salary = 4000.0
                }
                {
                    Name = "Charlie"
                    DeptId = 1
                    Salary = 4500.0
                }
                {
                    Name = "David"
                    DeptId = 3
                    Salary = 8000.0
                }
                {
                    Name = "Eve"
                    DeptId = 2
                    Salary = 5500.0
                }
            |]

        use dfEmps = DataFrame.ofRecords emps
        let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

        let q1 =
            query {
                for e in empQuery do
                    where (e.DeptId = 1)
                    select e
            }

        let q2 =
            query {
                for e in empQuery do
                    where (e.Salary > 4000.0)
                    select e
            }

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

                    select {| Name = e.Name; Bonus = bonus |}
            }

        let letResult = letQuery.ToList()

        Assert.Equal(3, letResult.Count)
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "Alice" && x.Bonus = 9000.0))
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "David" && x.Bonus = 12000.0))
        Assert.True(letResult |> Seq.exists (fun x -> x.Name = "Eve" && x.Bonus = 8250.0))

    [<Fact; Trait("LINQ", "TimeSeriesAndMultiGroup")>]
    let ``Test Polars Linq Time Series And MultiGroup`` () =
        let orders =
            [|
                {
                    OrderId = 1
                    OrderDate = DateTime(2023, 1, 15)
                    Region = "North"
                    Revenue = 100.0
                }
                {
                    OrderId = 2
                    OrderDate = DateTime(2023, 1, 20)
                    Region = "North"
                    Revenue = 150.0
                }
                {
                    OrderId = 3
                    OrderDate = DateTime(2023, 2, 10)
                    Region = "South"
                    Revenue = 200.0
                }
                {
                    OrderId = 4
                    OrderDate = DateTime(2024, 1, 5)
                    Region = "North"
                    Revenue = 300.0
                }
                {
                    OrderId = 5
                    OrderDate = DateTime(2023, 2, 25)
                    Region = "South"
                    Revenue = 250.0
                }
            |]

        use dfOrders = DataFrame.ofRecords orders
        let orderQuery = dfOrders.AsQueryable<OrderDto>()

        // SQL: GROUP BY EXTRACT(YEAR FROM o."OrderDate"), o."Region"
        let multiGroupQuery =
            query {
                for o in orderQuery do
                    groupBy
                        {|
                            Year = o.OrderDate.Year
                            Region = o.Region
                        |}
                        into
                        g

                    sortBy g.Key.Year
                    thenBy g.Key.Region

                    select
                        {|
                            Year = g.Key.Year
                            Region = g.Key.Region
                            TotalRevenue = g.Sum(fun x -> x.Revenue)
                        |}
            }
            |> assertPlan nativePushdown

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

    [<Fact; Trait("LINQ", "AdvancedFilters")>]
    let ``Test Polars Linq In And String Like`` () =
        let data =
            [|
                {
                    Id = 1
                    Name = "Apple"
                    Category = "Fruit"
                    Price = 1.2
                }
                {
                    Id = 2
                    Name = "Banana"
                    Category = "Fruit"
                    Price = 0.8
                }
                {
                    Id = 3
                    Name = "Carrot"
                    Category = "Vegetable"
                    Price = 0.5
                }
                {
                    Id = 4
                    Name = "Avocado"
                    Category = "Vegetable"
                    Price = 2.0
                }
                {
                    Id = 5
                    Name = "Beef"
                    Category = "Meat"
                    Price = 5.0
                }
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
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Equal(3, inResult.Length) // Apple, Banana, Beef
        Assert.False(inResult |> Seq.exists (fun p -> p.Category = "Vegetable"))

        let likeResult =
            query {
                for p in queryable do
                    where (p.Name.StartsWith "A")
                    select p
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Equal(2, likeResult.Length) // Apple, Avocado
        Assert.True(likeResult |> Seq.exists (fun p -> p.Name = "Apple"))
        Assert.True(likeResult |> Seq.exists (fun p -> p.Name = "Avocado"))

        let complexResult =
            query {
                for p in queryable do
                    where (targetCategories.Contains p.Category && p.Name.Contains "e" && p.Price > 1.0)
                    select p
            }
            |> Seq.toList

        Assert.Equal(2, complexResult.Length)
        Assert.True(complexResult |> Seq.exists (fun p -> p.Name = "Apple"))
        Assert.True(complexResult |> Seq.exists (fun p -> p.Name = "Beef"))

    [<Fact; Trait("LINQ", "PaginationAndDistinct")>]
    let ``Test Polars Linq Skip Take And Distinct`` () =
        // Arrange
        let data =
            [|
                {
                    Id = 1
                    Name = "Apple"
                    Category = "Fruit"
                    Price = 1.2
                }
                {
                    Id = 2
                    Name = "Banana"
                    Category = "Fruit"
                    Price = 0.8
                }
                {
                    Id = 3
                    Name = "Cherry"
                    Category = "Fruit"
                    Price = 1.5
                }
                {
                    Id = 4
                    Name = "Beef"
                    Category = "Meat"
                    Price = 5.0
                }
                {
                    Id = 5
                    Name = "Pork"
                    Category = "Meat"
                    Price = 4.0
                }
                {
                    Id = 6
                    Name = "Apple"
                    Category = "Fruit"
                    Price = 1.2
                } // Duplicated
            |]

        use df = DataFrame.ofRecords data
        let queryable = df.AsQueryable<ProductDto>()

        // SQL: SELECT DISTINCT p."Category" FROM products p
        let distinctCategories =
            query {
                for p in queryable do
                    select p.Category
                    distinct
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Equal(2, distinctCategories.Length)
        Assert.Contains("Fruit", distinctCategories)
        Assert.Contains("Meat", distinctCategories)

        let distinctProducts =
            query {
                for p in queryable do
                    distinct
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Equal(6, distinctProducts.Length)

        // SQL: SELECT ... FROM products ORDER BY p."Id" LIMIT 2 OFFSET 2
        let pagedResult =
            query {
                for p in queryable do
                    sortBy p.Id
                    skip 2
                    take 2
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Equal(2, pagedResult.Length)

        Assert.Equal(3, pagedResult.[0].Id)
        Assert.Equal(4, pagedResult.[1].Id)

    [<Fact; Trait("LINQ", "CaseWhenAndCte")>]
    let ``Test Polars Linq CaseWhen And Cte`` () =
        // Arrange
        let emps =
            [|
                {
                    Name = "Alice"
                    DeptId = 1
                    Salary = 6000.0
                }
                {
                    Name = "Bob"
                    DeptId = 2
                    Salary = 4000.0
                }
                {
                    Name = "Charlie"
                    DeptId = 1
                    Salary = 4500.0
                }
                {
                    Name = "David"
                    DeptId = 3
                    Salary = 8000.0
                }
                {
                    Name = "Eve"
                    DeptId = 2
                    Salary = 5500.0
                }
            |]

        use dfEmps = DataFrame.ofRecords emps
        let empQuery = dfEmps.AsQueryable<EmployeeSalary>()

        // SQL: CASE WHEN e."Salary" >= 6000 THEN 'High' ... END
        let caseWhenQuery =
            query {
                for e in empQuery do
                    select
                        {|
                            Name = e.Name
                            SalaryTier =
                                if e.Salary >= 6000.0 then "High"
                                elif e.Salary >= 4500.0 then "Medium"
                                else "Low"
                        |}
            }
            |> assertPlan nativePushdown

        let caseWhenResult = caseWhenQuery.ToList()
        Assert.Equal(5, caseWhenResult.Count)

        let assertTier expectedTier name =
            let emp = caseWhenResult |> Seq.find (fun e -> e.Name = name)
            Assert.Equal(expectedTier, emp.SalaryTier)

        assertTier "High" "Alice" // 6000
        assertTier "High" "David" // 8000
        assertTier "Medium" "Eve" // 5500
        assertTier "Medium" "Charlie" // 4500
        assertTier "Low" "Bob" // 4000

        let cte = empQuery.Where(fun e -> e.Salary > 5000.0)

        let cteQuery =
            query {
                for c in cte do
                    where (c.DeptId = 1 || c.DeptId = 3)
                    select c
            }
            |> assertPlan nativePushdown

        let cteResult = cteQuery.ToList()

        Assert.Equal(2, cteResult.Count)
        Assert.True(cteResult |> Seq.exists (fun e -> e.Name = "Alice"))
        Assert.True(cteResult |> Seq.exists (fun e -> e.Name = "David"))

    [<Fact; Trait("LINQ", "SubqueryInAndFunctions")>]
    let ``Test Polars Linq SubqueryIn And Functions`` () =
        let depts =
            [|
                { DeptId = 1; DeptName = "Engineering" }
                { DeptId = 2; DeptName = "Sales" }
                { DeptId = 3; DeptName = "HR" }
            |]

        let emps =
            [|
                {
                    Name = "Alice"
                    DeptId = 1
                    Salary = 6000.0
                }
                {
                    Name = null
                    DeptId = 2
                    Salary = 4000.0
                }
                {
                    Name = "Charlie"
                    DeptId = 1
                    Salary = 4500.0
                }
                {
                    Name = "David"
                    DeptId = 3
                    Salary = 8000.0
                }
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
                    select
                        {|
                            SafeName = if isNull e.Name then "Unknown" else e.Name
                        |}
            }
            |> assertPlan nativePushdown

        let coalesceResult = coalesceQuery.ToList()
        Assert.Equal(4, coalesceResult.Count)
        Assert.True(coalesceResult |> Seq.exists (fun e -> e.SafeName = "Unknown"))

        // SQL: SUBSTRING(e."Name", 1, 3)
        let stringQuery =
            query {
                for e in empQuery do
                    where (not (isNull e.Name))

                    select
                        {|
                            Name = e.Name
                            ShortName = e.Name.Substring(0, 3)
                        |}
            }
            |> assertPlan nativePushdown

        let stringResult = stringQuery.ToList()

        Assert.Equal(3, stringResult.Count)

        let getShortName name =
            (stringResult |> Seq.find (fun e -> e.Name = name)).ShortName

        Assert.Equal("Ali", getShortName "Alice")
        Assert.Equal("Cha", getShortName "Charlie")

    [<Fact; Trait("LINQ", "MathStringAndConditionalAgg")>]
    let ``Test Polars Linq Math String And ConditionalAgg`` () =
        let sales =
            [|
                {
                    Category = "Tech"
                    Item = "Laptop"
                    Revenue = 1000.5
                    Discount = 50.0
                }
                {
                    Category = "Tech"
                    Item = "Mouse"
                    Revenue = -20.0
                    Discount = 0.0
                }
                {
                    Category = "Office"
                    Item = "Desk"
                    Revenue = 500.2
                    Discount = 10.0
                }
                {
                    Category = "Office"
                    Item = "Chair"
                    Revenue = 150.8
                    Discount = 5.0
                }
            |]

        use dfSales = DataFrame.ofRecords sales
        let salesQuery = dfSales.AsQueryable<SalesData>()

        // CONCAT / Math.Round -> ROUND / Math.Abs -> ABS
        let scalarQuery =
            query {
                for s in salesQuery do
                    select
                        {|
                            FullName = s.Category + " - " + s.Item
                            NetRevenue = Math.Round(Math.Abs s.Revenue - s.Discount, 2)
                        |}
            }
            |> assertPlan nativePushdown

        let scalarResults = scalarQuery.ToList()

        Assert.Equal(4, scalarResults.Count)

        Assert.True(
            scalarResults
            |> Seq.exists (fun s -> s.FullName = "Tech - Laptop" && s.NetRevenue = 950.5)
        )

        Assert.True(
            scalarResults
            |> Seq.exists (fun s -> s.FullName = "Tech - Mouse" && s.NetRevenue = 20.0)
        )

        // SQL: SUM(CASE WHEN s."Category" = 'Tech' THEN ABS(s."Revenue") ELSE 0 END)

        let aggQuery =
            salesQuery
                .GroupBy(fun s -> 1)
                .Select(fun g ->
                    {|
                        Total = g.Sum(fun x -> Math.Abs x.Revenue)

                        TechTotal = g.Sum(fun x -> if x.Category = "Tech" then Math.Abs x.Revenue else 0.0)
                        OfficeTotal = g.Sum(fun x -> if x.Category = "Office" then Math.Abs x.Revenue else 0.0)
                    |})
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Single aggQuery |> ignore
        Assert.Equal(1671.5, aggQuery.[0].Total) // 1000.5 + 20 + 500.2 + 150.8
        Assert.Equal(1020.5, aggQuery.[0].TechTotal)
        Assert.Equal(651.0, aggQuery.[0].OfficeTotal)

    [<Fact; Trait("LINQ", "BitwiseAndRegex")>]
    let ``Test Polars Linq Bitwise and Regex`` () =
        let logs =
            [|
                {
                    Id = 1
                    Message = "User admin logged in"
                    Flags = 3
                } // 3 (0011)
                {
                    Id = 2
                    Message = "Failed password try"
                    Flags = 1
                } // 1 (0001)
                {
                    Id = 3
                    Message = "DB connection timeout"
                    Flags = 6
                } // 6 (0110)
                {
                    Id = 4
                    Message = "User guest logged in"
                    Flags = 2
                } // 2 (0010)
                {
                    Id = 5
                    Message = "System crash error 99"
                    Flags = 4
                } // 4 (0100)
            |]

        use df = DataFrame.ofRecords logs
        let logQuery = df.AsQueryable<ServerLog>()

        let bitwiseResult =
            query {
                for log in logQuery do
                    where (log.Flags &&& 2 = 2)
                    select log
            }
            |> assertPlan nativePushdown
            |> Seq.toList

        Assert.Equal(3, bitwiseResult.Length)
        Assert.True(bitwiseResult |> Seq.exists (fun l -> l.Id = 1))
        Assert.True(bitwiseResult |> Seq.exists (fun l -> l.Id = 3))
        Assert.True(bitwiseResult |> Seq.exists (fun l -> l.Id = 4))

    [<Fact; Trait("LINQ", "Aggregate")>]
    let ``Test Polars Linq Aggregate Overloads with Person and EmployeeSalary`` () =
        // 1. Prepare base test data using existing Person record
        let people =
            [
                {
                    Name = "Alice"
                    Age = 25
                    Sales = 1000.0
                }
                {
                    Name = "Bob"
                    Age = 30
                    Sales = 2500.0
                }
                {
                    Name = "Charlie"
                    Age = 35
                    Sales = 4500.0
                }
            ]

        use df = DataFrame.ofRecords people
        let query = df.AsQueryable<Person>()

        // Overload 2: Aggregate(seed, folder) -> TAccum * TSource -> TAccum
        // Calculate total sales accumulated onto an initial seed of 500.0
        let totalSalesWithBonus = query.Aggregate(500.0, fun acc p -> acc + p.Sales)

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
        let employees =
            [
                {
                    Name = "David"
                    DeptId = 1
                    Salary = 5000.0
                }
                {
                    Name = "Eva"
                    DeptId = 1
                    Salary = 9000.0
                }
                {
                    Name = "Frank"
                    DeptId = 2
                    Salary = 12000.0
                }
            ]

        use empDf = DataFrame.ofRecords employees
        let empQuery = empDf.AsQueryable<EmployeeSalary>()

        // Combine Native Where pushdown + Aggregate folding
        let highSalaryTotal =
            empQuery.Where(fun e -> e.Salary > 6000.0).Aggregate(0.0, fun acc e -> acc + e.Salary)

        Assert.Equal(21000.0, highSalaryTotal)

    [<Fact; Trait("LINQ", "AggregateBy")>]
    let ``Test Polars Linq AggregateBy with Static Seed using EmployeeSalary`` () =
        // Prepare test data using existing EmployeeSalary record
        let employees =
            [
                {
                    Name = "Alice"
                    DeptId = 1
                    Salary = 5000.0
                }
                {
                    Name = "Bob"
                    DeptId = 2
                    Salary = 7000.0
                }
                {
                    Name = "Charlie"
                    DeptId = 1
                    Salary = 6000.0
                }
                {
                    Name = "David"
                    DeptId = 2
                    Salary = 8000.0
                }
                {
                    Name = "Eva"
                    DeptId = 1
                    Salary = 9000.0
                }
            ]

        use df = DataFrame.ofRecords employees
        let query = df.AsQueryable<EmployeeSalary>()

        // Overload 1: AggregateBy(keySelector, seed, folder)
        // Group by DeptId and fold: (totalSalary, headCount)
        let results =
            query.AggregateBy(
                (fun (e: EmployeeSalary) -> e.DeptId),
                (0.0, 0),
                (fun (total, count) (e: EmployeeSalary) -> total + e.Salary, count + 1)
            )
            |> Seq.map (fun kv -> kv.Key, kv.Value)
            |> Map.ofSeq

        // Assert Dept 1: Alice(5000) + Charlie(6000) + Eva(9000) = 20000.0, Count = 3
        Assert.True(results.ContainsKey 1)
        Assert.Equal((20000.0, 3), results.[1])

        // Assert Dept 2: Bob(7000) + David(8000) = 15000.0, Count = 2
        Assert.True(results.ContainsKey 2)
        Assert.Equal((15000.0, 2), results.[2])

    [<Fact; Trait("LINQ", "AggregateBy")>]
    let ``Test Polars Linq AggregateBy with Dynamic SeedFactory using Person`` () =
        // Prepare test data using existing Person record
        let people =
            [
                {
                    Name = "Alice"
                    Age = 25
                    Sales = 1000.0
                }
                {
                    Name = "Bob"
                    Age = 35
                    Sales = 1500.0
                }
                {
                    Name = "Charlie"
                    Age = 25
                    Sales = 2000.0
                }
                {
                    Name = "David"
                    Age = 45
                    Sales = 3000.0
                }
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
        let employees =
            [
                {
                    Name = "Alice"
                    DeptId = 10
                    Salary = 3000.0
                }
                {
                    Name = "Bob"
                    DeptId = 10
                    Salary = 8000.0
                }
                {
                    Name = "Charlie"
                    DeptId = 20
                    Salary = 4000.0
                }
                {
                    Name = "David"
                    DeptId = 20
                    Salary = 9000.0
                }
            ]

        use df = DataFrame.ofRecords employees
        let query = df.AsQueryable<EmployeeSalary>()

        // Pipeline: Native Where Pushdown -> Fast Cursor AggregateBy
        let highSalaryByDept =
            query
                .Where(fun e -> e.Salary > 5000.0) // Bob and David will pass
                .AggregateBy((fun e -> e.DeptId), 0.0, (fun totalSalary e -> totalSalary + e.Salary))
            |> Seq.map (fun kv -> kv.Key, kv.Value)
            |> Map.ofSeq

        Assert.Equal(8000.0, highSalaryByDept.[10])
        Assert.Equal(9000.0, highSalaryByDept.[20])

    [<Fact>]
    [<Trait("LINQ", "NestedList")>]
    let ``Test Polars Linq Nested List Aggregation`` () =
        let emps =
            [|
                { Name = "Alice"; DeptId = 1 }
                { Name = "Bob"; DeptId = 1 }
                { Name = "Charlie"; DeptId = 2 }
            |]

        use dfEmps = DataFrame.ofRecords emps

        let empQuery = dfEmps.AsQueryable<EmpDto>()

        // g.Select(...).ToList()
        let nestedListQuery =
            empQuery
                .GroupBy(fun e -> e.DeptId)
                .Select(fun g ->
                    {|
                        DeptId = g.Key
                        EmpNames = g.Select(fun e -> e.Name).ToList()
                    |})
                .OrderBy(fun r -> r.DeptId)
            |> assertPlan nativePushdown

        let nestedListResults = nestedListQuery.ToList()

        Assert.Equal(2, nestedListResults.Count)

        let techDept = nestedListResults.[0]
        Assert.Equal(1, techDept.DeptId)
        Assert.Contains("Alice", techDept.EmpNames)
        Assert.Contains("Bob", techDept.EmpNames)

        let salesDept = nestedListResults.[1]
        Assert.Equal(2, salesDept.DeptId)
        Assert.Contains("Charlie", salesDept.EmpNames)

    [<Fact; Trait("LINQ", "Sandwich")>]
    let ``Test Polars Double Hybrid Sandwich`` () =
        use schema = PolarsSchema.FromRecord<StaffRecord>()

        let path =
            "/home/qinglei/Projects/Polars.NET/Polars.Integration.Tests/TestData/staffrecord.csv"

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
        let numbers = [| 1..100 |]
        use series = Series.create ("my_numbers", numbers)

        let queryable = series.As<int>().ToResizeArray()

        let seriesquery =
            query {
                for x in queryable do
                    where (x > 90)
                    sortByDescending x
                    select x
            }
            |> Seq.toList


        // let result = seriesquery.ToSeries()

        Assert.Equal(10, seriesquery.Count())

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

            use lf = LazyFrame.ScanCsv(fileName, schema = overrideSchema, hasHeader = false)

            use lfWithBonus = lf.WithColumns [ (pl.col "salary" * pl.lit 0.1).Alias "bonus" ]

            let employees = lfWithBonus.AsQueryable<StaffRecordWithBonus>()

            let linqQuery =
                query {
                    for e in employees do
                        where (e.age > 30 && e.bonus >= 7000.0)

                        select
                            {|
                                name = e.name
                                TotalCompensation = float e.salary + e.bonus
                            |}
                }

            let results = linqQuery |> Seq.toList

            Assert.NotNull results
            Assert.Equal(2, results.Length)

            Assert.Equal("Charlie", results.[0].name)
            Assert.Equal(77000.0, results.[0].TotalCompensation)

            Assert.Equal("David", results.[1].name)
            Assert.Equal(88000.0, results.[1].TotalCompensation)

        finally
            if File.Exists fileName then
                File.Delete fileName

    [<Fact>]
    [<Trait("LINQ", "Async")>]
    let ``FSharp query expression streams via RowsAsync successfully`` () =
        task {
            // Arrange
            use lf =
                let sId = pl.series "SensorId" [| 1..100 |]
                let sTemp = pl.series "Temperature" [| for i in 1..100 -> float i * 0.5 |]
                let sLoc = pl.series "Location" [| for i in 1..100 -> sprintf "Zone_%d" (i % 5) |]
                pl.dataframe [| sId; sTemp; sLoc |] |> DataFrame.asLazy

            let q =
                query {
                    for s in lf.AsQueryable<LinqSensorRecord>() do
                        where (s.Temperature > 40.0)
                        sortBy s.SensorId
                        select s
                }

            // Act: Cast to IAsyncEnumerable and pipe into RowsAsync.toList
            let! results = (q :?> IAsyncEnumerable<LinqSensorRecord>) |> RowsAsync.toList

            // Assert: 81..100 matched
            Assert.Equal(20, results.Length)
            Assert.Equal(81, results.Head.SensorId)
            Assert.Equal(100, (results |> List.last).SensorId)
        }

    [<Fact>]
    [<Trait("LINQ", "AsSeq")>]
    let ``FSharp AsSeq enables client-side UDF filtering and ToDataFrame`` () =
        use df =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob"; "Charlie" ])
                    Series.create ("Age", [ 25; 17; 30 ])
                    Series.create ("Sales", [ 100.0; 50.0; 200.0 ])
                ]

        // 1. Where (Age >= 18) pushes down natively
        // 2. AsSeq() cleanly switches to F# sequence
        // 3. Seq.filter executes custom F# client UDF
        // 4. Query.toDataFrame materializes back into Polars DataFrame
        let customPredicate (p: Person) = p.Sales > 150.0

        let q =
            query {
                for p in df.AsQueryable<Person>() do
                    where (p.Age >= 18)
                    select p
            }

        use filteredDf =
            q.AsSeq()
            |> Seq.filter customPredicate
            |> Query.toDataFrame

        Assert.Equal(1L, filteredDf.Height)
        Assert.Equal("Charlie", filteredDf.Column("Name").ToArray<string>().[0])

    [<Fact>]
    [<Trait("LINQ", "ToSeries")>]
    let ``FSharp Query asSeq and ToSeries with pipeline`` () =
        use df =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob"; "Charlie" ])
                    Series.create ("Age", [ 25; 17; 30 ])
                    Series.create ("Sales", [ 100.0; 50.0; 200.0 ])
                ]

        // Materialize projected sequence directly into a named Series
        use salesSeries =
            query {
                for p in df.AsQueryable<Person>() do
                    where (p.Age >= 18)
                    select p
            }
            |> Query.asSeq
            |> Seq.map (fun p -> p.Sales * 1.1)
            |> Query.toSeriesNamed "AdjustedSales"

        Assert.Equal("AdjustedSales", salesSeries.Name)
        Assert.Equal(2L, salesSeries.Length)
        let values = salesSeries.ToArray<float>()
        Assert.Equal(110.0, values.[0], 1)
        Assert.Equal(220.0, values.[1], 1)

    [<Fact>]
    [<Trait("LINQ", "ToSeries")>]
    let ``FSharp IQueryable ToSeries native pushdown`` () =
        use df =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob"; "Charlie" ])
                    Series.create ("Age", [ 25; 17; 30 ])
                    Series.create ("Sales", [ 100.0; 50.0; 200.0 ])
                ]

        // Direct native scalar IQueryable materialization into a Series
        let q =
            query {
                for p in df.AsQueryable<Person>() do
                    where (p.Age >= 18)
                    select p.Name
            }

        use nameSeries = q.ToSeries("AdultNames")

        Assert.Equal("AdultNames", nameSeries.Name)
        Assert.Equal(2L, nameSeries.Length)
        Assert.Equal<string seq>([| "Alice"; "Charlie" |], nameSeries.ToArray<string>())

    [<Fact>]
    [<Trait("LINQ", "InMemoryCollection_Transposition")>]
    let ``FSharp in-memory collection Concat transposition into DataFrame`` () =
        use df =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob" ])
                    Series.create ("Age", [ 25; 30 ])
                    Series.create ("Sales", [ 100.0; 200.0 ])
                ]

        let extraPeople =
            [
                { Name = "Charlie"; Age = 35; Sales = 300.0 }
                { Name = "David"; Age = 40; Sales = 400.0 }
            ]

        let q = df.AsQueryable<Person>().Concat(extraPeople)
        use resDf = q.ToDataFrame()

        Assert.Equal(4L, resDf.Height)

    [<Fact>]
    [<Trait("LINQ", "InMemoryCollection_Transposition")>]
    let ``FSharp in-memory collection Union transposition into DataFrame`` () =
        use df =
            DataFrame.create
                [
                    Series.create ("Name", [ "Alice"; "Bob" ])
                    Series.create ("Age", [ 25; 30 ])
                    Series.create ("Sales", [ 100.0; 200.0 ])
                ]

        let unionPeople =
            [
                { Name = "Bob"; Age = 30; Sales = 200.0 }
                { Name = "Charlie"; Age = 35; Sales = 300.0 }
            ]

        let q = df.AsQueryable<Person>().Union(unionPeople)
        use resDf = q.ToDataFrame()

        Assert.Equal(3L, resDf.Height)

    [<Fact>]
    [<Trait("LINQ", "InMemoryCollection_Transposition")>]
    let ``FSharp in-memory collection AsPolarsQueryable execution`` () =
        let people =
            [
                { Name = "Alice"; Age = 25; Sales = 100.0 }
                { Name = "Bob"; Age = 30; Sales = 200.0 }
                { Name = "Charlie"; Age = 35; Sales = 300.0 }
            ]

        let q =
            people.AsPolarsQueryable()
            |> fun s -> query {
                for p in s do
                    where (p.Age > 25)
                    select p
            }

        use resDf = q.ToDataFrame()
        Assert.Equal(2L, resDf.Height)

    [<Fact>]
    [<Trait("LINQ", "ToSeries_NonScalar")>]
    let ``FSharp in-memory collection ToSeries on non-scalar produces Struct Series`` () =
        let people =
            [
                { Name = "Alice"; Age = 25; Sales = 100.0 }
                { Name = "Bob"; Age = 30; Sales = 200.0 }
            ]

        use s = people.ToSeries("person_struct")
        Assert.Equal("person_struct", s.Name)
        Assert.Equal(2L, s.Length)
        match s.DataType.Kind with
        | DataTypeKind.Struct _ -> ()
        | other -> Assert.Fail($"Expected Struct but got {other}")

        use unnested = s.Unnest()
        Assert.Equal(3L, unnested.Width)
        Assert.Equal(2L, unnested.Height)
        Assert.Equal<string seq>([| "Alice"; "Bob" |], unnested.Column("Name").ToArray<string>())

    [<Fact>]
    [<Trait("LINQ", "ToSeries_NonScalar")>]
    let ``FSharp IQueryable ToSeries on multi-column query produces Struct Series`` () =
        let people =
            [
                { Name = "Alice"; Age = 25; Sales = 100.0 }
                { Name = "Bob"; Age = 30; Sales = 200.0 }
            ]

        use df = DataFrame.ofRecords people
        use s = df.AsQueryable<Person>().ToSeries("query_struct")

        Assert.Equal("query_struct", s.Name)
        Assert.Equal(2L, s.Length)
        match s.DataType.Kind with
        | DataTypeKind.Struct _ -> ()
        | other -> Assert.Fail($"Expected Struct but got {other}")

        use unnested = s.Unnest()
        Assert.Equal(3L, unnested.Width)
        Assert.Equal(2L, unnested.Height)
        Assert.Equal<string seq>([| "Alice"; "Bob" |], unnested.Column("Name").ToArray<string>())

    [<Fact>]
    [<Trait("LINQ", "Series_AsQueryable")>]
    let ``FSharp Series AsQueryable on scalar series`` () =
        use s = Series.create ("nums", [| 10; 20; 30; 40; 50 |])

        // 1. Where + Select pushdown
        let q = s.AsQueryable<int>()
        let filtered =
            q.Where(fun x -> x > 25).Select(fun x -> x * 2).ToList()
        Assert.Equal<int seq>([| 60; 80; 100 |], filtered)

        // 2. Native scalar aggregations
        Assert.Equal(5, s.AsQueryable<int>().Count())
        Assert.Equal(150, s.AsQueryable<int>().Sum())
        Assert.Equal(30.0, s.AsQueryable<int>().Average())
        Assert.Equal(50, s.AsQueryable<int>().Max())
        Assert.Equal(10, s.AsQueryable<int>().Min())

        // 3. Round-trip back to Series
        use roundTrip = s.AsQueryable<int>().Where(fun x -> x <= 30).ToSeries("small_nums")
        Assert.Equal("small_nums", roundTrip.Name)
        Assert.Equal<int seq>([| 10; 20; 30 |], roundTrip.ToArray<int>())

        // 4. AsSeq on Series
        let seqFiltered = s.AsSeq<int>() |> Seq.filter (fun x -> x % 20 = 0) |> Seq.toList
        Assert.Equal<int list>([ 20; 40 ], seqFiltered)

    [<Fact>]
    [<Trait("LINQ", "Series_AsQueryable")>]
    let ``FSharp Series AsQueryable on struct series`` () =
        let people =
            [
                { Name = "Alice"; Age = 25; Sales = 100.0 }
                { Name = "Bob"; Age = 30; Sales = 200.0 }
                { Name = "Charlie"; Age = 35; Sales = 300.0 }
            ]

        use structSeries = people.ToSeries("people_struct")
        Assert.Equal(3L, structSeries.Length)
        match structSeries.DataType.Kind with
        | DataTypeKind.Struct _ -> ()
        | other -> Assert.Fail($"Expected Struct but got {other}")

        // 1. Where filter on unnested struct query
        let q = structSeries.AsQueryable<Person>()
        let seniors = q.Where(fun p -> p.Age >= 30).ToList()
        Assert.Equal(2, seniors.Count)
        Assert.Equal("Bob", seniors.[0].Name)
        Assert.Equal("Charlie", seniors.[1].Name)

        // 2. Query.asQueryable module function
        let viaModule = Query.asQueryable<Person> structSeries
        let rich = viaModule.Where(fun p -> p.Sales > 250.0).ToList()
        Assert.Single(rich) |> ignore
        Assert.Equal("Charlie", rich.[0].Name)

        // 3. Round-trip back to Struct Series
        use filteredStruct = q.Where(fun p -> p.Age >= 30).ToSeries("seniors_struct")
        Assert.Equal(2L, filteredStruct.Length)
        match filteredStruct.DataType.Kind with
        | DataTypeKind.Struct _ -> ()
        | other -> Assert.Fail($"Expected Struct but got {other}")

        // 4. Re-query roundtripped Struct Series
        let reQueried = filteredStruct.AsQueryable<Person>().ToList()
        Assert.Equal(2, reQueried.Count)
        Assert.Equal("Bob", reQueried.[0].Name)

        // 5. AsSeq on Struct Series
        let asSeqList = structSeries.AsSeq<Person>() |> Seq.toList
        Assert.Equal(3, asSeqList.Length)
        Assert.Equal("Alice", asSeqList.[0].Name)

    [<Fact; Trait("LINQ", "FSharpAstRewriter")>]
    let ``Test FSharpAst betaReduce handles shadowed parameter names safely`` () =
        // Outer lambda: (x: int) => ((x: int) => x + 1)(10) + x
        // When beta-reducing outer lambda with argument 100, outer x becomes 100,
        // and inner lambda parameter x must not be replaced by 100!
        let outerParam = Linq.Expressions.Expression.Parameter(typeof<int>, "x")
        let innerParam = Linq.Expressions.Expression.Parameter(typeof<int>, "x")
        let innerBody = Linq.Expressions.Expression.Add(innerParam, Linq.Expressions.Expression.Constant(1))
        let innerLambda = Linq.Expressions.Expression.Lambda<Func<int, int>>(innerBody, innerParam)
        let innerInvoke = Linq.Expressions.Expression.Invoke(innerLambda, Linq.Expressions.Expression.Constant(10))
        let outerBody = Linq.Expressions.Expression.Add(innerInvoke, outerParam)
        let outerLambda = Linq.Expressions.Expression.Lambda<Func<int, int>>(outerBody, outerParam)

        let invokeOuter = Linq.Expressions.Expression.Invoke(outerLambda, Linq.Expressions.Expression.Constant(100))
        let rewritten = Polars.NET.Linq.FSharpAst.betaReduce invokeOuter

        // Result should be (10 + 1) + 100 = 111
        let compiled = Linq.Expressions.Expression.Lambda<Func<int>>(rewritten).Compile()
        Assert.Equal(111, compiled.Invoke())

    [<Fact; Trait("LINQ", "FSharpAstRewriter")>]
    let ``Test FSharpAst handles 8-tuple with Rest field in LetInliner and extraction`` () =
        // Construct an 8-tuple NewExpression:
        // Tuple<int, int, int, int, int, int, int, Tuple<int>>
        let restTupleType = typeof<Tuple<int>>
        let restNew = Linq.Expressions.Expression.New(restTupleType.GetConstructor([| typeof<int> |]), [| Linq.Expressions.Expression.Constant(800) :> Linq.Expressions.Expression |])

        let tuple8Type = typeof<Tuple<int, int, int, int, int, int, int, Tuple<int>>>
        let tuple8Ctor = tuple8Type.GetConstructors().[0]
        let tuple8Args: Linq.Expressions.Expression[] = [|
            Linq.Expressions.Expression.Constant(100) :> Linq.Expressions.Expression
            Linq.Expressions.Expression.Constant(200) :> Linq.Expressions.Expression
            Linq.Expressions.Expression.Constant(300) :> Linq.Expressions.Expression
            Linq.Expressions.Expression.Constant(400) :> Linq.Expressions.Expression
            Linq.Expressions.Expression.Constant(500) :> Linq.Expressions.Expression
            Linq.Expressions.Expression.Constant(600) :> Linq.Expressions.Expression
            Linq.Expressions.Expression.Constant(700) :> Linq.Expressions.Expression
            restNew :> Linq.Expressions.Expression
        |]
        let tuple8New = Linq.Expressions.Expression.New(tuple8Ctor, tuple8Args)

        // Access .Rest.Item1 from tuple8New
        let restAccess = Linq.Expressions.Expression.Property(tuple8New, "Rest")
        let item8Access = Linq.Expressions.Expression.Property(restAccess, "Item1")

        // Inlining member access on NewExpression
        let inlined = Polars.NET.Linq.FSharpAst.rewrite item8Access
        Assert.True(inlined :? Linq.Expressions.ConstantExpression)
        Assert.Equal(800, unbox<int> (inlined :?> Linq.Expressions.ConstantExpression).Value)

    [<Fact; Trait("LINQ", "FSharpAstRewriter")>]
    let ``Test FSharpAst PipelineNormalizer GroupBy reflection is cached and produces valid query`` () =
        let emps = [
            { Name = "Alice"; DeptId = 10; Salary = 1000.0 }
            { Name = "Bob"; DeptId = 10; Salary = 2000.0 }
            { Name = "Charlie"; DeptId = 20; Salary = 3000.0 }
        ]
        use df = DataFrame.ofRecords emps
        let queryable =
            query {
                for e in df.AsQueryable<EmployeeSalary>() do
                groupBy e.DeptId into g
                select (g.Key, g.Count())
            }
        let results = queryable.ToList()
        Assert.Equal(2, results.Count)

    [<Fact; Trait("LINQ", "WideTable")>]
    let ``Test FSharp Query on 20-Column Wide Table`` () =
        let wideData =
            [|
                {
                    Id = 1
                    Name = "Alpha"
                    Category = "Tech"
                    ValFloat1 = 10.5
                    ValFloat2 = 20.5
                    ValDec1 = 100.25m
                    ValDec2 = 200.75m
                    ValInt64 = 1000L
                    ValInt32 = 10
                    ValBool = true
                    Col11 = "RegionA"
                    Col12 = 1.1
                    Col13 = 10.1m
                    Col14 = 100
                    Col15 = 10000L
                    Col16 = true
                    Col17 = "Tag1"
                    Col18 = 0.5
                    Col19 = 5.5m
                    Col20 = 50
                }
                {
                    Id = 2
                    Name = "Beta"
                    Category = "Tech"
                    ValFloat1 = 25.0
                    ValFloat2 = 15.0
                    ValDec1 = 50.00m
                    ValDec2 = 150.00m
                    ValInt64 = 2000L
                    ValInt32 = 20
                    ValBool = false
                    Col11 = "RegionA"
                    Col12 = 2.2
                    Col13 = 20.2m
                    Col14 = 200
                    Col15 = 20000L
                    Col16 = true
                    Col17 = "Tag2"
                    Col18 = 1.5
                    Col19 = 15.5m
                    Col20 = 60
                }
                {
                    Id = 3
                    Name = "Gamma"
                    Category = "Finance"
                    ValFloat1 = 30.0
                    ValFloat2 = 40.0
                    ValDec1 = 300.50m
                    ValDec2 = 400.50m
                    ValInt64 = 3000L
                    ValInt32 = 30
                    ValBool = true
                    Col11 = "RegionB"
                    Col12 = 3.3
                    Col13 = 30.3m
                    Col14 = 300
                    Col15 = 30000L
                    Col16 = false
                    Col17 = "Tag1"
                    Col18 = 2.5
                    Col19 = 25.5m
                    Col20 = 70
                }
                {
                    Id = 4
                    Name = "Delta"
                    Category = "Tech"
                    ValFloat1 = 15.0
                    ValFloat2 = 35.0
                    ValDec1 = 80.00m
                    ValDec2 = 120.00m
                    ValInt64 = 4000L
                    ValInt32 = 40
                    ValBool = true
                    Col11 = "RegionB"
                    Col12 = 4.4
                    Col13 = 40.4m
                    Col14 = 400
                    Col15 = 40000L
                    Col16 = true
                    Col17 = "Tag2"
                    Col18 = 3.5
                    Col19 = 35.5m
                    Col20 = 80
                }
                {
                    Id = 5
                    Name = "Epsilon"
                    Category = "Finance"
                    ValFloat1 = 5.0
                    ValFloat2 = 50.0
                    ValDec1 = 500.00m
                    ValDec2 = 600.00m
                    ValInt64 = 5000L
                    ValInt32 = 50
                    ValBool = false
                    Col11 = "RegionB"
                    Col12 = 5.5
                    Col13 = 50.5m
                    Col14 = 500
                    Col15 = 50000L
                    Col16 = false
                    Col17 = "Tag3"
                    Col18 = 4.5
                    Col19 = 45.5m
                    Col20 = 90
                }
            |]

        use df = DataFrame.ofRecords wideData

        // 1. Filter across wide columns and scan full 20-column records
        let q1 =
            query {
                for r in df.AsQueryable<WideRecord>() do
                where (r.ValFloat1 >= 10.0 && r.ValBool && r.ValDec1 < 200.0m && r.Col16)
                sortBy r.Id
                select r
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, q1.Length)
        Assert.Equal(1, q1.[0].Id)
        Assert.Equal("Alpha", q1.[0].Name)
        Assert.Equal("Tech", q1.[0].Category)
        Assert.Equal(10.5, q1.[0].ValFloat1)
        Assert.Equal(20.5, q1.[0].ValFloat2)
        Assert.Equal(100.25m, q1.[0].ValDec1)
        Assert.Equal(200.75m, q1.[0].ValDec2)
        Assert.Equal(1000L, q1.[0].ValInt64)
        Assert.Equal(10, q1.[0].ValInt32)
        Assert.True(q1.[0].ValBool)
        Assert.Equal("RegionA", q1.[0].Col11)
        Assert.Equal(1.1, q1.[0].Col12)
        Assert.Equal(10.1m, q1.[0].Col13)
        Assert.Equal(100, q1.[0].Col14)
        Assert.Equal(10000L, q1.[0].Col15)
        Assert.True(q1.[0].Col16)
        Assert.Equal("Tag1", q1.[0].Col17)
        Assert.Equal(0.5, q1.[0].Col18)
        Assert.Equal(5.5m, q1.[0].Col19)
        Assert.Equal(50, q1.[0].Col20)

        Assert.Equal(4, q1.[1].Id)
        Assert.Equal("Delta", q1.[1].Name)
        Assert.Equal(4000L, q1.[1].ValInt64)
        Assert.Equal(80, q1.[1].Col20)

        // 2. Project 20 columns into an anonymous record with computed expressions
        let q2 =
            query {
                for r in df.AsQueryable<WideRecord>() do
                where (r.Id <= 2)
                sortBy r.Id
                select
                    {|
                        C1 = r.Id
                        C2 = r.Name
                        C3 = r.Category
                        C4 = r.ValFloat1 + 1.0
                        C5 = r.ValFloat2 * 2.0
                        C6 = r.ValDec1 + 10.0m
                        C7 = r.ValDec2
                        C8 = r.ValInt64 + 100L
                        C9 = r.ValInt32 * 2
                        C10 = not r.ValBool
                        C11 = r.Col11
                        C12 = r.Col12
                        C13 = r.Col13
                        C14 = r.Col14 + 1
                        C15 = r.Col15
                        C16 = r.Col16
                        C17 = r.Col17
                        C18 = r.Col18
                        C19 = r.Col19
                        C20 = r.Col20
                    |}
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, q2.Length)
        Assert.Equal(1, q2.[0].C1)
        Assert.Equal("Alpha", q2.[0].C2)
        Assert.Equal(11.5, q2.[0].C4)
        Assert.Equal(41.0, q2.[0].C5)
        Assert.Equal(110.25m, q2.[0].C6)
        Assert.Equal(1100L, q2.[0].C8)
        Assert.Equal(20, q2.[0].C9)
        Assert.False(q2.[0].C10)
        Assert.Equal(101, q2.[0].C14)
        Assert.Equal(50, q2.[0].C20)

        Assert.Equal(2, q2.[1].C1)
        Assert.Equal("Beta", q2.[1].C2)
        Assert.True(q2.[1].C10) // not false = true

        // 3. Multi-column GroupBy + Aggregations over wide table
        let q3 =
            query {
                for r in df.AsQueryable<WideRecord>() do
                groupBy
                    {|
                        Category = r.Category
                        Region = r.Col11
                    |}
                    into g
                where (g.Count() >= 1)
                sortBy g.Key.Category
                thenBy g.Key.Region
                select
                    {|
                        Category = g.Key.Category
                        Region = g.Key.Region
                        Count = g.Count()
                        SumFloat1 = g.Sum(fun x -> x.ValFloat1)
                        AvgFloat2 = g.Average(fun x -> x.ValFloat2)
                        SumDec1 = g.Sum(fun x -> x.ValDec1)
                        MaxInt64 = g.Max(fun x -> x.ValInt64)
                        MinInt32 = g.Min(fun x -> x.ValInt32)
                    |}
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(3, q3.Length)

        Assert.Equal("Finance", q3.[0].Category)
        Assert.Equal("RegionB", q3.[0].Region)
        Assert.Equal(2, q3.[0].Count)
        Assert.Equal(35.0, q3.[0].SumFloat1)
        Assert.Equal(45.0, q3.[0].AvgFloat2)
        Assert.Equal(800.50m, q3.[0].SumDec1)
        Assert.Equal(5000L, q3.[0].MaxInt64)
        Assert.Equal(30, q3.[0].MinInt32)

        Assert.Equal("Tech", q3.[1].Category)
        Assert.Equal("RegionA", q3.[1].Region)
        Assert.Equal(2, q3.[1].Count)
        Assert.Equal(35.5, q3.[1].SumFloat1)
        Assert.Equal(17.75, q3.[1].AvgFloat2)
        Assert.Equal(150.25m, q3.[1].SumDec1)
        Assert.Equal(2000L, q3.[1].MaxInt64)
        Assert.Equal(10, q3.[1].MinInt32)

        Assert.Equal("Tech", q3.[2].Category)
        Assert.Equal("RegionB", q3.[2].Region)
        Assert.Equal(1, q3.[2].Count)
        Assert.Equal(15.0, q3.[2].SumFloat1)
        Assert.Equal(35.0, q3.[2].AvgFloat2)
        Assert.Equal(80.00m, q3.[2].SumDec1)
        Assert.Equal(4000L, q3.[2].MaxInt64)
        Assert.Equal(40, q3.[2].MinInt32)

        // 4. Sorting & Top-K over wide table columns
        let q4 =
            query {
                for r in df.AsQueryable<WideRecord>() do
                sortByDescending r.ValDec1
                thenBy r.ValFloat1
                thenBy r.Id
                take 2
                select {| Id = r.Id; Name = r.Name; Dec = r.ValDec1; Float = r.ValFloat1 |}
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, q4.Length)
        Assert.Equal(5, q4.[0].Id) // ValDec1 = 500.00m
        Assert.Equal("Epsilon", q4.[0].Name)
        Assert.Equal(500.00m, q4.[0].Dec)
        Assert.Equal(3, q4.[1].Id) // ValDec1 = 300.50m
        Assert.Equal("Gamma", q4.[1].Name)
        Assert.Equal(300.50m, q4.[1].Dec)

    [<Fact; Trait("LINQ", "NestedRecord")>]
    let ``Test FSharp Query on Nested Records in Struct Column`` () =
        let data =
            [|
                {
                    Id = 1
                    Name = "Alice"
                    Addr = { City = "Beijing"; Zip = 100000 }
                }
                {
                    Id = 2
                    Name = "Bob"
                    Addr = { City = "Shanghai"; Zip = 200000 }
                }
                {
                    Id = 3
                    Name = "Charlie"
                    Addr = { City = "Beijing"; Zip = 100084 }
                }
            |]

        use df = DataFrame.ofRecords data

        // 1. Filter nested struct field
        let q1 =
            query {
                for p in df.AsQueryable<PersonWithAddress>() do
                where (p.Addr.City = "Beijing")
                sortBy p.Id
                select p
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, q1.Length)
        Assert.Equal(1, q1.[0].Id)
        Assert.Equal("Alice", q1.[0].Name)
        Assert.Equal("Beijing", q1.[0].Addr.City)
        Assert.Equal(100000, q1.[0].Addr.Zip)

        Assert.Equal(3, q1.[1].Id)
        Assert.Equal("Charlie", q1.[1].Name)
        Assert.Equal("Beijing", q1.[1].Addr.City)
        Assert.Equal(100084, q1.[1].Addr.Zip)

        // 2. Project nested fields into anonymous record with conditions
        let q2 =
            query {
                for p in df.AsQueryable<PersonWithAddress>() do
                where (p.Addr.Zip > 100050)
                sortBy p.Id
                select {| PersonName = p.Name; City = p.Addr.City; Zip = p.Addr.Zip |}
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, q2.Length)
        Assert.Equal("Bob", q2.[0].PersonName)
        Assert.Equal("Shanghai", q2.[0].City)
        Assert.Equal(200000, q2.[0].Zip)
        Assert.Equal("Charlie", q2.[1].PersonName)
        Assert.Equal("Beijing", q2.[1].City)
        Assert.Equal(100084, q2.[1].Zip)

        // 3. GroupBy on nested struct field
        let q3 =
            query {
                for p in df.AsQueryable<PersonWithAddress>() do
                groupBy p.Addr.City into g
                sortBy g.Key
                select {| City = g.Key; Count = g.Count(); MaxZip = g.Max(fun x -> x.Addr.Zip) |}
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, q3.Length)
        Assert.Equal("Beijing", q3.[0].City)
        Assert.Equal(2, q3.[0].Count)
        Assert.Equal(100084, q3.[0].MaxZip)
        Assert.Equal("Shanghai", q3.[1].City)
        Assert.Equal(1, q3.[1].Count)
        Assert.Equal(200000, q3.[1].MaxZip)

        // 4. Multi-level deep nesting: User -> DetailedAddress -> GeoLocation
        let deepData =
            [|
                {
                    Id = 1
                    Username = "Alice"
                    Location =
                        {
                            City = "Beijing"
                            Geo = { Lat = 39.9042; Lon = 116.4074 }
                        }
                }
                {
                    Id = 2
                    Username = "Bob"
                    Location =
                        {
                            City = "Shanghai"
                            Geo = { Lat = 31.2304; Lon = 121.4737 }
                        }
                }
                {
                    Id = 3
                    Username = "Charlie"
                    Location =
                        {
                            City = "Tokyo"
                            Geo = { Lat = 35.6762; Lon = 139.6503 }
                        }
                }
            |]

        use deepDf = DataFrame.ofRecords deepData

        let qDeep =
            query {
                for u in deepDf.AsQueryable<DeepUserRecord>() do
                where (u.Location.Geo.Lat > 35.0)
                sortBy u.Id
                select
                    {|
                        Username = u.Username
                        City = u.Location.City
                        Lat = u.Location.Geo.Lat
                        Lon = u.Location.Geo.Lon
                    |}
            }
            |> assertPlan nativePushdown
            |> Seq.toArray

        Assert.Equal(2, qDeep.Length)
        Assert.Equal("Alice", qDeep.[0].Username)
        Assert.Equal("Beijing", qDeep.[0].City)
        Assert.Equal(39.9042, qDeep.[0].Lat)

        Assert.Equal("Charlie", qDeep.[1].Username)
        Assert.Equal("Tokyo", qDeep.[1].City)
        Assert.Equal(35.6762, qDeep.[1].Lat)





