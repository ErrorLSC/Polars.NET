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
// 1. Ingest strongly-typed objects / records into DataFrame
var records = new[]
{
    new Order(1, "Widget", 19.99m),
    new Order(2, "Gadget", 49.50m)
};
using var df = DataFrame.FromRows(records);

// 2. Hydrate DataFrame back to strongly-typed sequence (or use LINQ)
IEnumerable<Order> orders = df.AsEnumerable<Order>();
```

---

## 3. Apache Arrow Integration (`Apache.Arrow`)

Polars columns are physically represented as Apache Arrow arrays. Polars.NET exposes native Arrow streams and arrays via the Arrow C Data Interface with zero-copy.

### Exporting & Importing Arrow Streams
```csharp
using Apache.Arrow.Ipc;

// 1. Export DataFrame as Arrow C Stream (Zero-Copy)
IArrowArrayStream stream = df.ToArrowStream();

// 2. Ingest any foreign Arrow C Stream into Polars
using var importedDf = DataFrame.FromArrowStream(stream);

// 3. Chunk/Batch Streaming
df.ExportBatches(batch =>
{
    Console.WriteLine($"Exported batch with {batch.Length} rows");
});
```

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

For traditional relational databases (SQL Server, Oracle, MySQL, Postgres via Npgsql, SQLite via Microsoft.Data.Sqlite), Polars.NET integrates seamlessly with `IDataReader` and `IDbConnection`.

### Reading Database Queries
```csharp
// 1. Stream from an open IDataReader into DataFrame
using var reader = command.ExecuteReader();
using var df = DataFrame.ReadDatabase(reader, batchSize: 50_000);

// 2. Direct query execution via connection
using var df2 = DataFrame.ReadDatabase(dbConnection, "SELECT * FROM sales");

// 3. Lazy Database Scan (Pipeline streaming)
using var lf = LazyFrame.ScanDatabase(dbConnection, "SELECT * FROM large_log_table");
using var res = lf.Filter(Pl.Col("level") == "ERROR").Collect();
```

### Exporting to `IDataReader` (e.g. for `SqlBulkCopy`)
You can expose any Polars `DataFrame` as an `IDataReader` without materializing in-memory `DataTable` or row objects:
```csharp
using System.Data.SqlClient; // or Microsoft.Data.SqlClient

using IDataReader dataReader = df.ToDataReader();

using var bulkCopy = new SqlBulkCopy(connectionString);
bulkCopy.DestinationTableName = "dbo.TargetTable";
bulkCopy.WriteToServer(dataReader); // Streams directly from Polars!
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
```csharp
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


