#r "../Polars.FSharp.Tests/bin/Debug/net10.0/Apache.Arrow.dll"
#r "../Polars.FSharp.Tests/bin/Debug/net10.0/Apache.Arrow.Adbc.dll"
#r "../Polars.FSharp.Tests/bin/Debug/net10.0/Polars.NET.Core.dll"
#r "../Polars.CSharp.Tests/bin/Debug/net10.0/Polars.NET.Linq.dll"
#r "../Polars.FSharp.Tests/bin/Debug/net10.0/Polars.FSharp.dll"

open System
open System.IO
open System.Text.RegularExpressions
open System.Linq
open Polars.FSharp
open Polars.FSharp.Query

let outDir = Path.Combine(__SOURCE_DIRECTORY__, "../docs/snippets")
Directory.CreateDirectory(outDir) |> ignore

let wrapPreview title content =
    sprintf """<div class="pl-preview-card">
  <div class="pl-preview-title">%s</div>
  %s
</div>""" title content

// Helper to strip embedded <style> tags from ToHtml() since VitePress global custom.css handles it
let stripStyle (html: string) =
    Regex.Replace(html, @"<style>[\s\S]*?</style>", "").Trim()

let writeSnippet name title (rawHtml: string) =
    let path = Path.Combine(outDir, sprintf "%s.html" name)
    let clean = stripStyle rawHtml
    let finalHtml = wrapPreview title clean
    File.WriteAllText(path, finalHtml)
    printfn "  [Preview] Generated %s -> %s" name path

printfn "=== Generating Polars.NET Documentation Previews ==="

// ==========================================
// 1. QuickStart Snippets
// ==========================================

let weatherDf =
    DataFrame.create [
        pl.series "Date" [ "2023-01-01"; "2023-01-01"; "2023-01-02" ]
        pl.series "City" [ "London"; "Manchester"; "London" ]
        pl.series "Temperature" [ 10.5; 9.0; 12.1 ]
        pl.series "Rain" [ true; true; false ]
    ]
    |> DataFrame.withColumn (pl.col("Date").Str.ToDate("%Y-%m-%d"))

writeSnippet "quickstart_df" "Polars DataFrame Output" (weatherDf.ToHtml())

let filteredAggDf =
    weatherDf
    |> DataFrame.filter (pl.col "Temperature" .> pl.lit 9.5)
    |> DataFrame.groupBy [ pl.col "City" ]
    |> DataFrame.agg [
        pl.col("Temperature").Mean().Alias "Avg_Temperature"
        pl.col("Temperature").Count().Alias "Record_Count"
    ]
    |> DataFrame.sortDescending [ pl.col "Avg_Temperature" ]

writeSnippet "quickstart_filter_agg" "Aggregated DataFrame Output" (filteredAggDf.ToHtml())

let schema = weatherDf.Schema
let diffExpr = (pl.col("Temperature") - pl.col("Temperature").Mean().Over(pl.col "City")).Alias("Temp_Diff")
writeSnippet "quickstart_expr_ast" "Polars Expr AST Tree & Formula" (diffExpr.ToHtml(schema))

let lazyPlan =
    weatherDf.Lazy()
    |> LazyFrame.filter (pl.col "Temperature" .> pl.lit 10.0)
    |> LazyFrame.groupBy [ pl.col "City" ]
    |> LazyFrame.agg [
        pl.col("Temperature").Mean().Alias "Avg_Temp"
    ]

writeSnippet "quickstart_lazy_plan" "Optimized LazyFrame Logical Plan" (lazyPlan.ToHtml(optimized = true))

// ==========================================
// 2. Expressions & Computations Snippets
// ==========================================

// 5. Selectors & Set Algebra
let salesDf =
    DataFrame.create [
        pl.series "Id" [ 101; 102; 103; 104; 105 ]
        pl.series "Product" [ "Laptop"; "Mouse"; "Keyboard"; "Monitor"; "Headphones" ]
        pl.series "Category" [ "Electronics"; "Accessories"; "Accessories"; "Electronics"; "Accessories" ]
        pl.series "Price" [ 1200.0; 25.5; 75.0; 350.0; 99.0 ]
        pl.series "Discount" [ 0.10; 0.05; 0.15; 0.20; 0.0 ]
        pl.series "Stock" [ 50; 200; 150; 40; 80 ]
    ]

let numericColsWithoutId =
    salesDf.Select(pl.cs.numeric() - pl.cs.byName [ "Id" ])

writeSnippet "expr_selectors" "Selectors Output (Numeric Columns Excluding Id)" (numericColsWithoutId.ToHtml())

// 6. When / Then / Otherwise Conditional Column
let gradedDf =
    salesDf.WithColumns(
        (pl.when'(pl.col "Price" .>= pl.lit 500.0)
            .Then(pl.lit "Premium")
            .When(pl.col "Price" .>= pl.lit 100.0)
            .Then(pl.lit "Mid-Range")
            .Otherwise(pl.lit "Budget"))
            .Alias "Price_Tier"
    )

writeSnippet "expr_when_then" "When / Then / Otherwise Classification" (gradedDf.ToHtml())

// 7. Window Functions (Over)
let windowSalaryDf =
    DataFrame.create [
        pl.series "Employee" [ "Alice"; "Bob"; "Charlie"; "David"; "Emma"; "Frank" ]
        pl.series "Dept" [ "Dev"; "Dev"; "Dev"; "Sales"; "Sales"; "Sales" ]
        pl.series "Salary" [ 95000.0; 105000.0; 110000.0; 70000.0; 85000.0; 90000.0 ]
    ]

let salaryDevDf =
    windowSalaryDf.Select([
        pl.col "Employee"
        pl.col "Dept"
        pl.col "Salary"
        (pl.col("Salary").Mean().Over(pl.col "Dept")).Alias "Dept_Avg"
        (pl.col "Salary" - pl.col("Salary").Mean().Over(pl.col "Dept")).Alias "Diff_From_Dept_Avg"
        (pl.col("Salary").Rank().Over(pl.col "Dept")).Alias "Dept_Salary_Rank"
    ])

writeSnippet "expr_window_over" "Window Over Group Calculations" (salaryDevDf.ToHtml())

// 8. Complex Expression AST Tree
let salesSchema = salesDf.Schema
let complexExpr =
    ((pl.col("Price") * (pl.lit 1.0 - pl.col("Discount"))).Round(2u)
     - pl.col("Price").Mean().Over(pl.col "Category"))
        .Alias("Discounted_Diff_From_Category_Avg")

writeSnippet "expr_ast_complex" "Complex Windowed Expression AST" (complexExpr.ToHtml(salesSchema))

// ==========================================
// 3. LazyFrame & Optimization Snippets
// ==========================================

let transactionsDf =
    DataFrame.create [
        pl.series "TxId" [ 1001; 1002; 1003; 1004; 1005; 1006; 1007; 1008 ]
        pl.series "Customer" [ "Alice"; "Bob"; "Alice"; "Charlie"; "Bob"; "David"; "Emma"; "Alice" ]
        pl.series "Category" [ "Tech"; "Books"; "Tech"; "Grocery"; "Tech"; "Books"; "Tech"; "Grocery" ]
        pl.series "Amount" [ 250.0; 45.0; 120.0; 85.0; 310.0; 60.0; 490.0; 30.0 ]
        pl.series "IsFlagged" [ false; false; false; true; false; false; false; false ]
    ]

let lazyQuery =
    transactionsDf.Lazy()
    |> LazyFrame.filter (pl.col "Amount" .> pl.lit 50.0)
    |> LazyFrame.withColumn ((pl.col "Amount" * pl.lit 0.1).Alias "Tax")
    |> LazyFrame.filter (pl.col "Category" .== pl.lit "Tech")
    |> LazyFrame.groupBy [ pl.col "Customer" ]
    |> LazyFrame.agg [
        pl.col("Amount").Sum().Alias "Total_Spend"
        pl.col("Tax").Sum().Alias "Total_Tax"
        pl.col("TxId").Count().Alias "Tx_Count"
    ]
    |> LazyFrame.sortDescending [ pl.col "Total_Spend" ]

writeSnippet "lazy_unoptimized_plan" "Unoptimized Query Plan (Raw Logical Plan)" (lazyQuery.ToHtml(optimized = false))
writeSnippet "lazy_optimized_plan" "Optimized Physical Execution Plan (Pushdown & CSE)" (lazyQuery.ToHtml(optimized = true))

let collectedDf = lazyQuery.Collect()
writeSnippet "lazy_collected_df" "Materialized DataFrame Output (.Collect())" (collectedDf.ToHtml())

// ==========================================
// 4. LINQ & Query CE Snippets
// ==========================================

[<CLIMutable>]
type OrderRecord = {
    OrderId: int
    Customer: string
    Region: string
    Revenue: float
}

let ordersDf =
    DataFrame.create [
        pl.series "OrderId" [ 1; 2; 3; 4; 5; 6 ]
        pl.series "Customer" [ "Alice"; "Bob"; "Charlie"; "David"; "Emma"; "Frank" ]
        pl.series "Region" [ "North"; "North"; "South"; "North"; "South"; "East" ]
        pl.series "Revenue" [ 1200.0; 450.0; 2100.0; 890.0; 1350.0; 600.0 ]
    ]

let qGrouped =
    query {
        for o in ordersDf.AsQueryable<OrderRecord>() do
        where (o.Revenue >= 500.0)
        groupValBy o o.Region into g
        select {|
            Region = g.Key
            TotalRevenue = g.Sum(fun x -> x.Revenue)
            OrderCount = g.Count()
        |}
    }

let linqDf = qGrouped.ToDataFrame()
writeSnippet "linq_groupby_agg" "LINQ to Polars 100% Native Pushdown Output" (linqDf.ToHtml())

let linqPlanHtml =
    sprintf """<div class="pl-lazy-container"><div class="pl-lazy-header">Polars.NET LINQ Plan <span class="pl-lazy-tag">100%% PUSHDOWN</span></div><pre class="pl-lazy-plan">%s</pre></div>"""
        (System.Net.WebUtility.HtmlEncode(qGrouped.Explain()))

writeSnippet "linq_plan_inspection" "Compiled LINQ Execution Plan (.Explain())" linqPlanHtml

// ==========================================
// 5. Ecosystem & Interop Snippets
// ==========================================

let arrowBatch =
    let allocator = Apache.Arrow.Memory.NativeMemoryAllocator()
    let idBuilder = Apache.Arrow.Int32Array.Builder()
    idBuilder.Append(101) |> ignore
    idBuilder.Append(102) |> ignore
    idBuilder.Append(103) |> ignore
    let idArray = idBuilder.Build(allocator)

    let valBuilder = Apache.Arrow.DoubleArray.Builder()
    valBuilder.Append(24.5) |> ignore
    valBuilder.Append(26.1) |> ignore
    valBuilder.Append(23.8) |> ignore
    let valArray = valBuilder.Build(allocator)

    let schema =
        Apache.Arrow.Schema.Builder()
            .Field(Apache.Arrow.Field("SensorId", Apache.Arrow.Types.Int32Type.Default, false))
            .Field(Apache.Arrow.Field("Temperature", Apache.Arrow.Types.DoubleType.Default, false))
            .Build()

    new Apache.Arrow.RecordBatch(schema, [ idArray :> Apache.Arrow.IArrowArray; valArray :> Apache.Arrow.IArrowArray ], 3)

let arrowDf = DataFrame.FromArrow(arrowBatch)
writeSnippet "interop_arrow_batch" "Zero-Copy Apache Arrow RecordBatch Ingestion" (arrowDf.ToHtml())

// ==========================================
// 6. Time Series Snippets
// ==========================================

let baseTime = DateTime(2024, 1, 1, 9, 0, 0)
let timeSeriesDf =
    DataFrame.create [
        pl.series "Timestamp" [
            baseTime
            baseTime.AddMinutes 30.0
            baseTime.AddHours 1.0
            baseTime.AddHours 1.5
            baseTime.AddHours 2.0
            baseTime.AddHours 2.5
        ]
        pl.series "DeviceId" [ "sensor_a"; "sensor_a"; "sensor_a"; "sensor_b"; "sensor_b"; "sensor_b" ]
        pl.series "Reading" [ 15.2; 18.7; 22.1; 30.4; 33.8; 29.5 ]
    ]

let tsFeaturesDf =
    timeSeriesDf.WithColumns [
        pl.col("Timestamp").Dt.Hour().Alias("Hour")
        pl.col("Timestamp").Dt.Truncate(Dur.String "1h").Alias("Hour_Bucket")
        pl.col("Reading").RollingMean(Dur.Index 2).Alias("Rolling_Mean_2")
    ]

writeSnippet "timeseries_features" "Time Series Feature Extraction & Rolling Window" (tsFeaturesDf.ToHtml())

let tsDynamicGrouped =
    timeSeriesDf.Lazy()
        .GroupByDynamic(
            indexCol = "Timestamp",
            every = Dur.TimeSpan(TimeSpan.FromHours 1.0),
            period = Dur.TimeSpan(TimeSpan.FromHours 1.0),
            by = [ pl.col "DeviceId" ]
        )
    |> LazyFrame.agg [
        pl.col("Reading").Mean().Alias("Avg_Reading")
        pl.col("Reading").Count().Alias("Sample_Count")
    ]
    |> LazyFrame.collect

writeSnippet "timeseries_dynamic_groupby" "Dynamic Time-Window GroupBy Output" (tsDynamicGrouped.ToHtml())

// ==========================================
// 7. Delta Lake Snippets
// ==========================================

let deltaTempDir = Path.Combine(Path.GetTempPath(), sprintf "preview_delta_%s" (Guid.NewGuid().ToString("N")))
try
    let deltaBatch1 =
        DataFrame.create [
            pl.series "Id" [ 1; 2; 3 ]
            pl.series "Metric" [ "CPU"; "Memory"; "Disk" ]
            pl.series "Usage" [ 45.2; 78.0; 22.5 ]
        ]
    deltaBatch1.WriteDelta(deltaTempDir, mode = DeltaSaveMode.Overwrite, mkdir = true)

    let deltaBatch2 =
        DataFrame.create [
            pl.series "Id" [ 4; 5 ]
            pl.series "Metric" [ "Network"; "GPU" ]
            pl.series "Usage" [ 12.0; 89.4 ]
        ]
    deltaBatch2.WriteDelta(deltaTempDir, mode = DeltaSaveMode.Append)

    let deltaHistory =
        Delta.History deltaTempDir
        |> DataFrame.select [ pl.col "timestamp"; pl.col "operation"; pl.col "mode"; pl.col "clientVersion" ]

    writeSnippet "delta_history" "Delta Lake ACID Transaction History (.History())" (deltaHistory.ToHtml())

    let timeTravelDf = LazyFrame.ScanDelta(deltaTempDir, version = 1UL).Collect()
    writeSnippet "delta_time_travel" "Delta Lake Time Travel (Read Version 1)" (timeTravelDf.ToHtml())
finally
    if Directory.Exists(deltaTempDir) then
        try Directory.Delete(deltaTempDir, true) with _ -> ()

printfn "=== Done generating previews ==="
