
# Polars.NET

[![NuGet](https://img.shields.io/nuget/v/Polars.NET.svg)](https://www.nuget.org/packages/Polars.NET)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Polars.NET.svg)](https://www.nuget.org/packages/Polars.NET)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Polars.FSharp.svg)](https://www.nuget.org/packages/Polars.FSharp)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-online-brightgreen.svg?style=flat-square&logo=read-the-docs&logoColor=white)](https://errorlsc.github.io/Polars.NET/index.html)

[![Sponsor](https://img.shields.io/badge/Sponsor-ErrorLSC-pink?style=for-the-badge&logo=github-sponsors)](https://github.com/sponsors/ErrorLSC)

F# & C# E2E examples repo:[Polars.NET-Cookbook](https://github.com/ErrorLSC/Polars.NET-Cookbook)


**High-Performance, DataFrame Engine for .NET, powered by Rust Polars & Apache Arrow. With cloud and deltalake features.**

![icon](assets/icon_lite.png)

Supported Platforms: Windows (x64), Linux (x64/ARM64, glibc/musl), macOS (ARM64).
Cloud: AWS, Azure and GCP
Data Lake: Delta Lake

<p style="font-size:1.3em; font-weight:bold; background-color:#ffff99;">
99% API in python Polars now is available in both Polars.NET and Polars.FSharp.<br/>
</p>

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
# Add ML.NET integration if needed
dotnet add package Polars.NET.ML
```

F# Users:

```Bash
dotnet add package Polars.FSharp
# And then add the native runtime for your current environment:
dotnet add package Polars.NET.Native.win-x64
# Add LINQ extension package once you need to write LINQ or computation expressions
dotnet add package Polars.NET.Linq
# Add ML.NET integration if needed
dotnet add package Polars.NET.ML
```

- Requirements: .NET 8+.
- Hardware: CPU with AVX2 support (x86-64-v3). Roughly Intel Haswell (2013+) or AMD Excavator (2015+). If you have AVX-512 supported CPU, please try to compile Rust core on your machine use RUSTFLAGS='-C target-cpu=native'

## Built Specially for .NET

Bringing .NET to Polars is not enough, it is the time to bring Polars to .NET.

- ML.NET & Tensor Interop

Prepare data with polars, then train it with ML.NET & ONNX, finally analyze results back with polars.

Notice: Tensor Interop doesn't need any extra package but ML.NET integration requires **Polars.NET.ML** extension nuget package.

```CSharp
// ==========================================
// Data Loading
// ==========================================
var hfUrl = "https://huggingface.co/datasets/scikit-learn/iris/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet";
var options = CloudOptions.Http(new Dictionary<string, string>
{
    { "User-Agent", "Polars.NET-Test" }
});
using var lf = LazyFrame.ScanParquet(hfUrl, cloudOptions: options);

// sepal length (cm), sepal width (cm), petal length (cm), petal width (cm)     
using var cleanlf = lf.Cast((typeof(double),typeof(float)));
using var cleanDf = cleanlf.WithColumns(Pl.ConcatArray(Cs.Float().ToExpr().Alias("Features"))).Collect();
// ==========================================
// Polars -> ML.NET
// ==========================================
var dataView = cleanDf.AsDataView();

var mlContext = new MLContext(seed: 42);

// ==========================================
// ML.NET Pipeline
// ==========================================
// Form VBuffer<float> tensor
var pipeline = mlContext.Clustering.Trainers.KMeans("Features", numberOfClusters: 3);
var model = pipeline.Fit(dataView);
// ==========================================
// ML.NET Transform and Read Back
// ==========================================
var predictions = model.Transform(dataView);

// ML.NET -> Polars
using var resultDf = predictions.ToDataFrame();

// ==========================================
// TensorInterop
// ==========================================
float[,] matrix = new float[,]
{
    { 1.1f, 1.2f, 1.3f },
    { 2.1f, 2.2f, 2.3f }
};

using var series = Series.From("ffi_matrix", matrix);

var (ptr, shape) = series.AsDangerousUnmanagedTensor<float>();

int totalElements = (int)(shape[0] * shape[1]); 

float* rawFloatPtr = (float*)ptr.ToPointer();

var nativeSpan = new ReadOnlySpan<float>(rawFloatPtr, totalElements);
```

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

## Quick Start

### C# Example

```csharp
using Polars.CSharp;
using Pl = Polars.CSharp.Polars;

// 1. Create a DataFrame
var data = new[] {
    new { Name = "Alice", Age = 25, Dept = "IT" },
    new { Name = "Bob", Age = 30, Dept = "HR" },
    new { Name = "Charlie", Age = 35, Dept = "IT" }
};
var df = DataFrame.From(data);

// 2. Filter & Aggregate
var res = df
    .Filter(Pl.Col("Age") > 28)
    .GroupBy("Dept")
    .Agg(
        Pl.Col("Age").Mean().Alias("AvgAge"),
        Pl.Col("Name").Count().Alias("Count")
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
    |> pl.groupByLazy [ pl.col "dept" ]
    |> pl.aggLazy
        [ 
            pl.col("age").Mean().Alias "AvgAge" 
            pl.col("name").Count().Alias "Count"
        ]
    |> pl.sortAscendingLazy [pl.col "AvgAge"]
    |> pl.collect
    |> pl.show
```

## Benchmark

- [Benchmark Code](https://github.com/ErrorLSC/Polars.NET-Benchmark)

## Extension Nuget Package

- Polars.NET.Linq
- Polars.NET.ML

## Architecture

3-Layer Architecture ensures API stability.

1. Hand-written Rust C ABI layer bridging .NET and Polars. (native_shim)
2. .NET Core layer for dirty works like unsafe ops, wrappers, LibraryImports. (Polars.NET.Core)
3. High level C# and F# API layer here. No unsafe blocks. (Polars.CSharp & Polars.FSharp)

## Roadmap

- More code examples, user cases

- Native LINQ Provider

## Contributing

Contributions are welcome. Whether it's adding new expression mappings, improving documentation, or optimizing the FFI layer.

1. Fork the repo.

2. Create your feature branch.

3. Submit a Pull Request.

## License

MIT License. See LICENSE for details.
For Third party licenses please check THIRD_PARTY_LICENSES.html
