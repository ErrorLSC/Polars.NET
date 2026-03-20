
# Polars.NET

[![NuGet](https://img.shields.io/nuget/v/Polars.NET.svg)](https://www.nuget.org/packages/Polars.NET)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Polars.NET.svg)](https://www.nuget.org/packages/Polars.NET)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Polars.FSharp.svg)](https://www.nuget.org/packages/Polars.FSharp)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-online-brightgreen.svg?style=flat-square&logo=read-the-docs&logoColor=white)](https://errorlsc.github.io/Polars.NET/index.html)

**High-Performance, DataFrame Engine for .NET, powered by Rust & Apache Arrow. With cloud and deltalake features.**

![icon](assets/icon_lite.png)

Supported Platforms: Windows (x64), Linux (x64/ARM64, glibc/musl), macOS (ARM64).
Cloud: AWS, Azure and GCP
Data Lake: Delta Lake

## Why Polars.NET exists

This is the game I'd like to play: binding the lightning-fast Polars engine to the .NET ecosystem.
And it brings a lot of fun.

- Polars.NET vs Python Ecosystem

    <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/benchmark_python_dark.png">
    <img alt="Polars.NET vs Python" src="assets/benchmark_python_light.png">
    </picture>

- Speedup vs Legacy .NET

    <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/benchmark_summary_dark.png">
    <img alt="Speedup Summary" src="assets/benchmark_summary_light.png">
    </picture>

- Delta Lake Read
![pic](assets/read_benchmark.png)

- Delta Lake Write
![pic](assets/write_benchmark.png)

## Installation

C# Users:

```Bash
dotnet add package Polars.NET 
# And then add the native runtime for your current environment:
dotnet add package Polars.NET.Native.win-x64
# Add LINQ extension package once you need to write LINQ
dotnet add package Polars.NET.Linq
```

F# Users:

```Bash
dotnet add package Polars.FSharp
# And then add the native runtime for your current environment:
dotnet add package Polars.NET.Native.win-x64
# Add LINQ extension package once you need to write LINQ or computation expressions
dotnet add package Polars.NET.Linq
```

- Requirements: .NET 8+.
- Hardware: CPU with AVX2 support (x86-64-v3). Roughly Intel Haswell (2013+) or AMD Excavator (2015+). If you have AVX-512 supported CPU, please try to compile Rust core on your machine use RUSTFLAGS='-C target-cpu=native'

## Built Specially for .NET

Bringing .NET to Polars is not enough, it is the time to bring Polars to .NET.

- ADO.NET

Polars.NET DataReader is generic typed without boxing/unboxing on hot path.

```CSharp
// To DataReader
using var bulkReader = df.AsDataReader(bufferSize: 100, typeOverrides: overrides);
// From DataReader
using var sourceReader = sourceTable.CreateDataReader();
var df = DataFrame.ReadDatabase(sourceReader);
```

- C# LINQ & F# Computation Expression

With Polars.NET.Linq Extension package(Thanks to Linq2DB), playing DataFrame/Series with LINQ/Query block is available now.

```CSharp
using var dfDepts = DataFrame.From(depts);
using var dfEmps = DataFrame.From(emps);

using var db = new PolarsDataContext(new SqlContext(), ownsContext: true);
var deptQuery = dfDepts.AsQueryable<DeptDto>(db);
var empQuery = empQuery.AsQueryable<EmpDto>(db);

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
```

```FSharp
let queryResult = 
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
    |> Seq.toList 
```

- ADBC

Passing data between query engines and data sources like ping-pong ball as your wish.

```CSharp
var options = new DataOptions().UseConnectionString(ProviderName.PostgreSQL15, "Server=Dummy;");

var records = new[]
{
    new { id = 101, name = "Data", language = "C" },
    new { id = 102, name = "Frame", language = "C++" },
    new { id = 103, name = "Engine", language = "Rust" }
};
using var df = DataFrame.FromEnumerable(records);
df.WriteToAdbc(_connection, "stage1_table");

using var duckDbTranslator = new DataConnection(options); 

using var pushdownDf = duckDbTranslator.GetTable<AdbcE2ERecord>()
    .TableName("stage1_table")
    .Where(x => x.Id > 101) 
    .Select(x => new 
    {
        x.Id,
        x.Name,
        UpperLang = Sql.Upper(x.Language)
    })
    .ToDataFrameAdbc(_connection);
    
// shape: (2, 3)
// ┌─────┬────────┬───────────┐
// │ Id  ┆ Name   ┆ UpperLang │
// │ --- ┆ ---    ┆ ---       │
// │ i32 ┆ str    ┆ str       │
// ╞═════╪════════╪═══════════╡
// │ 102 ┆ Frame  ┆ C++       │
// │ 103 ┆ Engine ┆ RUST      │
// └─────┴────────┴───────────┘

using var finalPolarsDf = pushdownDf.AsQueryable<PushdownRecord>()
    .Select(x => new 
    {
        FinalId = x.Id + 1000,                            
        SuperName = x.Name + " Pro Max",                  
        LangStatus = x.UpperLang == "RUST" ? "Genshin" : "Impact" 
    })
    .ToDataFrame(); 

// shape: (2, 3)
// ┌─────────┬────────────────┬────────────┐
// │ FinalId ┆ SuperName      ┆ LangStatus │
// │ ---     ┆ ---            ┆ ---        │
// │ i32     ┆ str            ┆ str        │
// ╞═════════╪════════════════╪════════════╡
// │ 1102    ┆ Frame Pro Max  ┆ Impact     │
// │ 1103    ┆ Engine Pro Max ┆ Genshin    │
// └─────────┴────────────────┴────────────┘

finalPolarsDf.WriteToAdbc(_connection, "final_destination_table");

using var verifyFinalDf = DataFrame.ReadAdbc(_connection, "SELECT * FROM final_destination_table ORDER BY FinalId");

// Same as before
```

- Query Sandwich

LINQ query and Polars lazy-execuation plan is compatible with each other.

```CSharp
// Start with Polars lazy scan
using var rawLf = LazyFrame.ScanCsv(path,schema:schema);

// Query with LINQ
var query = rawLf.AsQueryable<StaffRecord>()
                .Where(e => e.salary > 5000)
                .Select(e => new { e.name, e.salary });

using LazyFrame lfWithLinq = query.ToLazyFrame();

// Then query with Polars again
using var finalLf = lfWithLinq.WithColumns(Col("salary").Std().Alias("salary_std"));

using var df = finalLf.Collect();

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
```

- Delta Lake (With Unity Catalog)

Python and JVM are not needed here. Stay comfortable with our dear CLR. Deletion Vector is also available.

```CSharp
// Create UnityCatalog instance
using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

// Set merge expresions
var updateCond = Delta.Source("Stock") > Delta.Target("Stock");
var matchDeleteCond = Delta.Source("Status") == "DeleteMe";
var insertCond = Delta.Source("Stock") > 0;
var srcDeleteCond = Delta.Target("Status") == "Obsolete";

// Merge
sourceDf.MergeCatalogRecords(uc,catalog, schema, table,
    mergeKeys: ["Id"],
    cloudOptions: options
)
    .WhenMatchedUpdate(updateCond)
    .WhenMatchedDelete(matchDeleteCond)
    .WhenNotMatchedInsert(insertCond)
    .WhenNotMatchedBySourceDelete(srcDeleteCond)
    .Execute();

// Read Back
using var resultDf = uc.ReadCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
```

- UDF(User Defined Function)

If LINQ or Polars Expression is not fit for your special need, feel free to write UDF.

```FSharp
let data = [
    {| Code = ValueSome "EMP-1024" |}  
    {| Code = ValueSome "EMP-0042" |}  
    {| Code = ValueSome "ADMIN-1" |}   
    {| Code = ValueSome "EMP-ERR" |}   
    {| Code = ValueNone |}        
]

let lf = DataFrame.ofRecords(data).Lazy()

//  string voption -> int voption
let parseEmpId (opt: string voption) =
    match opt with
    | ValueSome s when s.StartsWith "EMP-" ->
        match Int32.TryParse(s.Substring 4) with
        | true, num -> ValueSome num
        | _ -> ValueNone
    | _ -> ValueNone

let df = 
    lf 
    |> pl.withColumnLazy (
        pl.col "Code"
        |> fun e -> e.Map(Udf.mapValueOption parseEmpId, DataType.Int32)
        |> pl.alias "EmpId"
    )
    |> pl.collect
// shape: (5, 2)
// ┌──────────┬───────┐
// │ Code     ┆ EmpId │
// │ ---      ┆ ---   │
// │ str      ┆ i32   │
// ╞══════════╪═══════╡
// │ EMP-1024 ┆ 1024  │
// │ EMP-0042 ┆ 42    │
// │ ADMIN-1  ┆ null  │
// │ EMP-ERR  ┆ null  │
// │ null     ┆ null  │
// └──────────┴───────┘
```

## Quick Start

### C# Example

```csharp
using Polars.CSharp;
using static Polars.CSharp.Polars; // For Col(), Lit() helpers

// 1. Create a DataFrame
var data = new[] {
    new { Name = "Alice", Age = 25, Dept = "IT" },
    new { Name = "Bob", Age = 30, Dept = "HR" },
    new { Name = "Charlie", Age = 35, Dept = "IT" }
};
var df = DataFrame.From(data);

// 2. Filter & Aggregate
var res = df
    .Filter(Col("Age") > 28)
    .GroupBy("Dept")
    .Agg(
        Col("Age").Mean().Alias("AvgAge"),
        Col("Name").Count().Alias("Count")
    )
    .Sort("AvgAge", descending: true);

// 3. Output
res.Show();
// shape: (2, 3)
// ┌──────┬────────┬───────┐
// │ Dept ┆ AvgAge ┆ Count │
// │ ---  ┆ ---    ┆ ---   │
// │ str  ┆ f64    ┆ u32   │
// ╞══════╪════════╪═══════╡
// │ IT   ┆ 35.0   ┆ 1     │
// │ HR   ┆ 30.0   ┆ 1     │
// └──────┴────────┴───────┘
```

### F# Example

```fsharp

open Polars.FSharp

// 1. Scan CSV (Lazy)
let lf = LazyFrame.ScanCsv "users.csv"

// 2. Transform Pipeline
let res = 
    lf
    |> pl.filterLazy (pl.col "age" .> pl.lit 28)
    |> pl.groupByLazy 
        [ pl.col "dept" ]
        [ 
            pl.col("age").Mean().Alias "AvgAge" 
            pl.col("name").Count().Alias "Count"
        ]
    |> pl.collect
    |> pl.sort ("AvgAge", false)

// 3. Output
res.Show()

```

## Benchmark

- [Benchmark Code](https://github.com/ErrorLSC/Polars.NET-Benchmark)

## Architecture

3-Layer Architecture ensures API stability.

1. Hand-written Rust C ABI layer bridging .NET and Polars. (native_shim)
2. .NET Core layer for dirty works like unsafe ops, wrappers, LibraryImports. (Polars.NET.Core)
3. High level C# and F# API layer here. No unsafe blocks. (Polars.CSharp & Polars.FSharp)

## Roadmap

- Strong Typed Series<T\> with Source Generator.

- Documentation: [**Docs Here**](https://errorlsc.github.io/Polars.NET/index.html)

## Contributing

Contributions are welcome. Whether it's adding new expression mappings, improving documentation, or optimizing the FFI layer.

1. Fork the repo.

2. Create your feature branch.

3. Submit a Pull Request.

## License

MIT License. See LICENSE for details.
