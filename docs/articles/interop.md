# Polars.NET Ecosystem Interoperability & Platform Runtimes

Polars.NET is designed as a foundational data engine for the .NET ecosystem. Beyond standalone dataframe queries, it provides zero-copy and streaming bridges to **Apache Arrow, ADBC, ADO.NET, ML.NET (IDataView), and modern .NET Tensors (`System.Numerics.Tensors`)**.

---

## 1. Native Runtime Packages (Different from Python!)

Unlike Python Polars (which distributes platform-specific self-contained wheels via `pip`), .NET NuGet separates managed API libraries from native binary packages to avoid package bloat.

### Required Package References
In any project using Polars.NET, you **must reference both**:
1. **The Managed API Library**: `Polars.CSharp` or `Polars.FSharp` (and optionally `Polars.NET.Linq`, `Polars.NET.ML`).
2. **The Native Runtime Package** matching your execution environment (OS and CPU Architecture):

```xml
<ItemGroup>
  <!-- 1. Managed API Library -->
  <PackageReference Include="Polars.CSharp" Version="1.0.0" />
  <PackageReference Include="Polars.NET.Linq" Version="1.0.0" />

  <!-- 2. Target Platform Native Binary (Pick according to your deployment OS/Arch) -->
  <!-- Windows x64: -->
  <PackageReference Include="Polars.NET.Native.win-x64" Version="1.0.0" Condition="$([MSBuild]::IsOSPlatform('Windows'))" />
  <!-- Linux x64 (glibc / Ubuntu / Debian / RHEL): -->
  <PackageReference Include="Polars.NET.Native.linux-x64" Version="1.0.0" Condition="$([MSBuild]::IsOSPlatform('Linux'))" />
  <!-- Linux ARM64: -->
  <!-- <PackageReference Include="Polars.NET.Native.linux-arm64" Version="1.0.0" /> -->
  <!-- Alpine Linux (musl): -->
  <!-- <PackageReference Include="Polars.NET.Native.linux-musl-x64" Version="1.0.0" /> -->
  <!-- macOS Apple Silicon (M1/M2/M3/M4): -->
  <PackageReference Include="Polars.NET.Native.osx-arm64" Version="1.0.0" Condition="$([MSBuild]::IsOSPlatform('OSX'))" />
</ItemGroup>
```

> [!IMPORTANT]
> If the native package is missing, you will receive `DllNotFoundException` for `libnative_shim.so`, `libnative_shim.dylib`, or `native_shim.dll` at runtime.

---

## 2. Managed Arrays, Spans & Collections

Polars.NET supports zero-copy span access for primitive numerics and fast hydration to/from standard .NET collections.

### Series to `ReadOnlySpan<T>` (Zero-Copy)
For contiguous numeric series (int, float, double, etc.), you can obtain a `ReadOnlySpan<T>` directly over native Arrow buffer memory without allocating a managed array:
```csharp
using var series = Series.From("temps", [20.5f, 21.0f, 22.3f]);

// Zero-copy span over native memory:
ReadOnlySpan<float> span = series.AsReadOnlySpan<float>();
float sum = 0f;
foreach (float val in span) { sum += val; }
```

### DataFrames to/from Rows & POCOs
```csharp
// 1. Ingest strongly-typed objects / records / List<T> into DataFrame (Most idiomatic .NET row pattern)
var records = new List<Order>
{
    new(1, "Widget", 19.99m),
    new(2, "Gadget", 49.50m)
};
using var df = DataFrame.FromRows(records);

// 2. Direct entry into the Polars native LINQ engine from any IEnumerable<T> / List<T>:
using Polars.CSharp.Linq;
using DataFrame topOrders = records.AsPolarsQueryable()
    .Where(o => o.Amount > 20.0m)
    .ToDataFrame();

// Seamlessly Join or Concat in-memory List<T> directly against a Polars query:
var extraOrders = new List<Order> { new(3, "Doohickey", 9.99m) };
using DataFrame allOrders = topOrders.AsQueryable<Order>()
    .Concat(extraOrders)
    .ToDataFrame();

// 3. Hydrate DataFrame back to strongly-typed sequence (or use LINQ)
IEnumerable<Order> orders = df.AsEnumerable<Order>();
```

---

## 3. Apache Arrow Integration (`Apache.Arrow`)

Polars columns are physically backed by Apache Arrow contiguous arrays. Polars.NET provides direct, zero-copy interoperability with the official `Apache.Arrow` .NET package.

### Zero-Copy `RecordBatch` Ingestion & Export

::: code-group

```csharp [C# (Arrow RecordBatch)]
using Apache.Arrow;
using Apache.Arrow.Memory;
using Polars.CSharp;

// 1. Build or receive a native Apache.Arrow RecordBatch:
var allocator = new NativeMemoryAllocator();
var batch = new RecordBatch.Builder(allocator)
    .Append("SensorId", false, col => col.Int32(arr => arr.AppendRange([101, 102, 103])))
    .Append("Temperature", false, col => col.Double(arr => arr.AppendRange([24.5, 26.1, 23.8])))
    .Build();

// 2. Zero-Copy import into Polars DataFrame:
using var df = DataFrame.FromArrow(batch);

// 3. Roundtrip back to Apache.Arrow RecordBatch:
using RecordBatch exportedBatch = df.ToArrow();
```

```fsharp [F# (Arrow RecordBatch)]
open Apache.Arrow
open Apache.Arrow.Memory
open Polars.FSharp

// Zero-copy ingestion from Apache.Arrow RecordBatch:
let df = DataFrame.FromArrow(batch)

// Roundtrip back to Apache.Arrow RecordBatch:
use exportedBatch = df.ToArrow()
```

:::

<!--@include: ../snippets/interop_arrow_batch.html-->

### Arrow-to-Arrow Vectorized UDFs
When implementing advanced custom vector processing, use `MapArrow` to bypass row-by-row boxing and process raw `Apache.Arrow.IArrowArray`:

```csharp
var customExpr = Pl.Col("features").MapArrow(
    arrowArray => MyCustomArrowKernel(arrowArray),
    outputType: DataType.Float64
);
```

---

## 4. ADBC (Arrow Database Connectivity)

[ADBC](https://arrow.apache.org/adbc/) is the next-generation database standard that transfers tabular data as binary Apache Arrow record batches, eliminating slow row-by-row ODBC/JDBC serialization.

Polars.NET includes built-in first-class ADBC support for DuckDB, PostgreSQL, SQLite, Snowflake, and BigQuery via `Apache.Arrow.Adbc`:

```csharp
using Apache.Arrow.Adbc;

// 1. Query an ADBC database directly into a zero-copy DataFrame
using var df = DataFrame.ReadAdbc(adbcConnection, "SELECT id, amount, customer FROM orders WHERE amount > 100");

// 2. Zero-Copy Bulk Ingest a DataFrame into an ADBC Database
// Ingest modes: AdbcIngestMode.Create, AdbcIngestMode.Append, AdbcIngestMode.Replace
var updateResult = df.WriteToAdbc(adbcConnection, "target_table", AdbcIngestMode.Append);
Console.WriteLine($"Inserted {updateResult.AffectedRows} rows");
```

---

## 5. ADO.NET (`System.Data`)

For traditional relational databases (SQL Server, Oracle, MySQL, PostgreSQL via Npgsql, SQLite via Microsoft.Data.Sqlite), Polars.NET integrates directly with `IDataReader` and `DbDataReader`.

### Reading Database Queries into DataFrames (`ReadDatabase` / `ReadDb`)

`DataFrame.ReadDatabase` accepts any open `IDataReader` and converts rows directly into Arrow record batches with prefetching:

::: code-group

```csharp [C# (ReadDatabase)]
using System.Data;
using Microsoft.Data.SqlClient; // Or Npgsql, SqliteConnection, etc.
using Polars.CSharp;

using var connection = new SqlConnection(connectionString);
connection.Open();

using var command = connection.CreateCommand();
command.CommandText = "SELECT Product, Price, CreatedAt FROM Sales WHERE Price > 100";

using IDataReader reader = command.ExecuteReader();

// Streams from IDataReader into Arrow batches -> Materialized Polars DataFrame:
using DataFrame df = DataFrame.ReadDatabase(reader, batchSize: 50_000);
```

```fsharp [F# (ReadDb)]
open System.Data
open Microsoft.Data.SqlClient
open Polars.FSharp

use connection = new SqlConnection(connectionString)
connection.Open()

use command = connection.CreateCommand()
command.CommandText <- "SELECT Product, Price, CreatedAt FROM Sales WHERE Price > 100"

use reader = command.ExecuteReader()

// Stream into Polars DataFrame
use df = DataFrame.ReadDb(reader, batchSize = 50_000)
```

:::

> [!NOTE] Automatic Data Type Mappings
> `ReadDatabase` automatically maps standard ADO.NET types to their native Polars equivalents:
> * `decimal` -> Polars `Decimal128` (arbitrary precision without rounding loss)
> * `DateTime` / `DateOnly` / `TimeOnly` -> Polars `Datetime` / `Date` / `Time`
> * `Guid` / `string` -> Polars `String`
> * `DBNull.Value` -> Polars nulls

---

### Lazy Database Scanning (`LazyFrame.ScanDatabase` / `ScanDb`)

For pipeline queries, prefer `LazyFrame.ScanDatabase`. It supports passing either an existing `IDataReader` or a **reader factory** `Func<IDataReader>`. 

> [!TIP] Why Use a Reader Factory?
> A factory allows Polars to probe schema upfront with a quick probe query and re-execute queries if the query plan requires multiple execution passes or reuse:

::: code-group

```csharp [C# (Lazy Scan with Reader Factory)]
using System.Data;
using Polars.CSharp;

// Define a factory that returns a fresh open IDataReader on demand:
Func<IDataReader> readerFactory = () =>
{
    var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT Name, Score, Dept FROM Employees WHERE Score >= 60.0";
    return cmd.ExecuteReader();
};

using LazyFrame lf = LazyFrame.ScanDatabase(readerFactory, batchSize: 50_000);

using DataFrame result = lf
    .Filter(Pl.Col("Dept") == "Engineering")
    .GroupBy("Dept")
    .Agg(Pl.Col("Score").Mean().Alias("AvgScore"))
    .Collect();
```

```fsharp [F# (Lazy Scan with Reader Factory)]
open System.Data
open Polars.FSharp

let readerFactory () =
    let cmd = connection.CreateCommand()
    cmd.CommandText <- "SELECT Name, Score, Dept FROM Employees WHERE Score >= 60.0"
    cmd.ExecuteReader() :> IDataReader

use lf = LazyFrame.ScanDb(readerFactory, batchSize = 50_000)

use result =
    lf
    |> LazyFrame.filter (pl.col "Dept" .== pl.lit "Engineering")
    |> LazyFrame.groupBy [ pl.col "Dept" ]
    |> LazyFrame.agg [ pl.col("Score").Mean().Alias "AvgScore" ]
    |> LazyFrame.collect
```

:::

---

### Direct LINQ Pipeline Entrypoint (`reader.AsPolarsQueryable<T>`)

Instead of materializing an intermediate `DataFrame`, you can stream an open `IDataReader` directly into a Polars LINQ queryable pipeline with native Rust plan compilation:

```csharp
using System.Data;
using Polars.CSharp;
using Polars.CSharp.Linq;

using IDataReader reader = command.ExecuteReader();

// Streams database cursor directly into Polars LINQ engine:
using DataFrame summaryDf = reader.AsPolarsQueryable<SaleDto>(batchSize: 50_000)
    .Where(s => s.Price > 100.0)
    .GroupBy(s => s.Product)
    .Select(g => new { Product = g.Key, TotalRevenue = g.Sum(x => x.Price) })
    .ToDataFrame();
```

---

### Asynchronous Chunk Streaming (`ReadDatabaseBatchesAsync`)

When dealing with large query results, use `DbDataReader.ReadDatabaseBatchesAsync` to stream materialized DataFrame batches asynchronously without blocking the thread pool:

::: code-group

```csharp [C# (Async Batch Ingestion)]
using System.Data.Common;
using Polars.CSharp;

using DbDataReader reader = await command.ExecuteReaderAsync();

// Stream database cursor records into chunks of native Polars DataFrames:
await foreach (DataFrame batch in reader.ReadDatabaseBatchesAsync(batchSize: 50_000))
{
    using (batch)
    {
        Console.WriteLine($"Ingested {batch.Height} rows.");
        // Process each batch or push to downstream worker
    }
}
```

```fsharp [F# (Async Batch Ingestion)]
open Polars.FSharp

task {
    use! reader = command.ExecuteReaderAsync()
    for batch in reader.ReadDatabaseBatchesAsync(batchSize = 50_000) do
        use b = batch
        printfn $"Ingested %d{b.Height} rows."
}
```

:::

---

### Direct LINQ Pipeline Entrypoint (`reader.AsPolarsQueryable<T>`)

Instead of materializing an intermediate `DataFrame`, you can stream an open `IDataReader` directly into a Polars LINQ queryable pipeline with native Rust plan compilation:

```csharp
using System.Data;
using Polars.CSharp;
using Polars.CSharp.Linq;

using IDataReader reader = command.ExecuteReader();

// Streams database cursor directly into Polars LINQ engine:
using DataFrame summaryDf = reader.AsPolarsQueryable<SaleDto>(batchSize: 50_000)
    .Where(s => s.Price > 100.0)
    .GroupBy(s => s.Product)
    .Select(g => new { Product = g.Key, TotalRevenue = g.Sum(x => x.Price) })
    .ToDataFrame();
```

---

### Exporting & Writing to Databases (`WriteTo`, `SinkTo`, `AsDataReader`)

To write Polars DataFrames into database tables (such as bulk insertion with `SqlBulkCopy`), Polars.NET provides two zero-memory-copy producer-consumer patterns:

#### Pattern 1: `df.WriteTo(Action<IDataReader>)` & `lf.SinkTo(Action<IDataReader>)`
`WriteTo` streams Arrow batches into an internal bounded queue and provides an active `IDataReader` to your writer callback:

::: code-group

```csharp [C# (SqlBulkCopy with WriteTo)]
using System.Data;
using Microsoft.Data.SqlClient;
using Polars.CSharp;

// Stream rows directly from Polars DataFrame into SQL Server SqlBulkCopy:
df.WriteTo(reader =>
{
    using var bulkCopy = new SqlBulkCopy(connectionString);
    bulkCopy.DestinationTableName = "dbo.TargetSales";
    bulkCopy.BatchSize = 10_000;
    bulkCopy.WriteToServer(reader);
}, bufferSize: 5);
```

```fsharp [F# (SqlBulkCopy with WriteTo)]
open System
open System.Data
open Microsoft.Data.SqlClient
open Polars.FSharp

df.WriteTo(
    Action<IDataReader>(fun reader ->
        use bulkCopy = new SqlBulkCopy(connectionString)
        bulkCopy.DestinationTableName <- "dbo.TargetSales"
        bulkCopy.BatchSize <- 10_000
        bulkCopy.WriteToServer reader
    ),
    bufferSize = 5
)
```

:::

#### Pattern 2: `df.AsDataReader()` (Zero-Copy `DbDataReader`)
Alternatively, obtain a live `DbDataReader` instance directly using `df.AsDataReader()`:

```csharp
using System.Data.Common;
using Microsoft.Data.SqlClient;

// Obtain a live DbDataReader backed by a Polars bounded channel:
using DbDataReader reader = df.AsDataReader(bufferSize: 5);

using var bulkCopy = new SqlBulkCopy(connectionString);
bulkCopy.DestinationTableName = "dbo.TargetSales";
bulkCopy.WriteToServer(reader);
```

---

## 6. Machine Learning: Microsoft.ML / ML.NET (`IDataView`)

Through the `Polars.NET.ML` package, Polars.NET bridges Polars DataFrames with ML.NET's `IDataView` pipeline.

```csharp
using Microsoft.ML;
using Polars.NET.ML.DataView;
using Polars.NET.ML.CSharpExtensions;

var mlContext = new MLContext();

// 1. Convert Polars DataFrame to ML.NET IDataView (Zero-copy stream)
IDataView dataView = df.AsDataView();

// 2. Train an ML.NET pipeline directly on Polars data
var pipeline = mlContext.Transforms.NormalizeMinMax("Features")
    .Append(mlContext.Regression.Trainers.Sdca(labelColumnName: "Target", featureColumnName: "Features"));
var model = pipeline.Fit(dataView);

// 3. Transform predictions back to a Polars DataFrame
IDataView predictions = model.Transform(dataView);
using DataFrame resultDf = predictions.ToDataFrame();
```

---

## 7. AI & Deep Learning: Tensors (`System.Numerics.Tensors`)

Modern .NET 8 / 9+ includes `System.Numerics.Tensors.Tensor<T>` and `ReadOnlyTensorSpan<T>`. Polars.NET provides direct, native conversions for feeding models in **ONNX Runtime, TorchSharp, or native C++ inference engines**.

### Series to Tensor / TensorSpan
```csharp
using System.Numerics.Tensors;

using var embSeries = df["embeddings"]; // e.g. Array[f32, 128] or flat Series

// 1. Zero-copy TensorSpan view
ReadOnlyTensorSpan<float> tensorSpan = embSeries.AsTensorSpan<float>();

// 2. Transposed view without moving bytes (zero-copy stride transposition)
ReadOnlyTensorSpan<float> transposed = embSeries.AsTransposedTensorSpan<float>();

// 3. Deep-copy into managed heap Tensor<T> (safe for async/background tasks)
Tensor<float> managedTensor = embSeries.AsTensor<float>();
```

### DataFrame to 2D Feature Matrix Tensor
Extract selected columns and format them into a 2D Row-Major `Tensor<T> [Rows, Columns]`:
```csharp
// Extracts "age", "income", "credit_score" into a 2D [N, 3] row-major tensor for ONNX
Tensor<float> featureMatrix = df.AsTensor<float>("age", "income", "credit_score");
```

### Raw Unmanaged Memory Pointer (Zero-Copy C++ / ONNX FFI)
```csharp
// Direct pointer to contiguous Arrow memory for native C++ backends
(nint ptr, long[] shape) = series.AsDangerousUnmanagedTensor<float>();

try
{
    CallNativeCppInferenceEngine(ptr, shape);
}
finally
{
    // Prevent GC from collecting Series while native engine reads the pointer
    GC.KeepAlive(series);
}
```

---

## 8. Asynchronous Streaming (`IAsyncEnumerable`)

Polars.NET provides first-class support for C# `IAsyncEnumerable<T>` and F# async row streams, enabling cooperative, cancellation-aware asynchronous processing without thread pool starvation.

### C# Row Streaming (`ToAsyncEnumerable`)
```csharp
using Polars.CSharp;

using var df = DataFrame.ReadParquet("large.parquet");

// Stream rows cooperatively (yields execution slots to avoid starving thread pool)
await foreach (var row in df.ToAsyncEnumerable<MyRecord>(chunkSize: 10_000, cancellationToken))
{
    await ProcessRowAsync(row);
}
```

### Lazy Native Batch Streaming (`CollectBatchesAsync`)

Stream materialized DataFrame chunks from a `LazyFrame` as soon as they are computed by the native Rust streaming engine:

::: code-group

```csharp [C# (Async Batch Streaming)]
using Polars.CSharp;

using var lf = LazyFrame.ScanParquet("huge_dataset/*.parquet");

// Asynchronously stream DataFrame batches:
await foreach (DataFrame batch in lf.CollectBatchesAsync(chunkSize: 50_000))
{
    using (batch)
    {
        Console.WriteLine($"Received streaming batch of {batch.Height} rows");
        await ExportBatchToCloudAsync(batch);
    }
}
```

```fsharp [F# (Async Batch Streaming)]
open Polars.FSharp

let lf = LazyFrame.ScanParquet "huge_dataset/*.parquet"

task {
    for batch in lf.CollectBatchesAsync(chunkSize = 50_000L) do
        use b = batch
        printfn $"Received streaming batch of %d{b.Height} rows"
        do! exportBatchToCloud b
}
```

:::

### LINQ & `IAsyncEnumerable` Adaptation
```csharp
using Polars.CSharp.Linq;

// 1. Convert LINQ query directly to IAsyncEnumerable:
IAsyncEnumerable<OrderDto> asyncStream = query.AsAsyncEnumerable();

// 2. Ingest foreign IAsyncEnumerable into a Polars DataFrame asynchronously:
IAsyncEnumerable<SensorData> externalStream = FetchDataFromGrpcAsync();
using DataFrame df = await externalStream.ToDataFrameAsync(cancellationToken);
```

### F# Async Row Streams (`RowsAsync`)
```fsharp
open Polars.FSharp

// Stream rows into F# list/array or functional pipeline:
let! records =
    df.RowsAsync<SensorRecord>(chunkSize = 5000)
    |> RowsAsync.filter (fun s -> s.Temperature > 25.0)
    |> RowsAsync.toList

// Aggregate over async stream without materializing entire dataset:
let! totalTemp =
    lazyDf.RowsAsync<SensorRecord>(chunkSize = 10000L, engine = Engine.Auto)
    |> RowsAsync.fold (fun acc s -> acc + s.Temperature) 0.0
```


