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

// 2. Ingest any Arrow stream into Polars. A stream is consumed once.
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

### Reading Database Queries into DataFrames (`ReadDatabase`)
```csharp
// 1. Stream from an open IDataReader into DataFrame
using var command = connection.CreateCommand();
command.CommandText = "SELECT id, product, price FROM sales";
using var reader = command.ExecuteReader();
using var df = DataFrame.ReadDatabase(reader, batchSize: 50_000);

// 2. Lazy Database Scan (Recommended: reader factory for multi-pass probing)
Func<IDataReader> readerFactory = () =>
{
    var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM large_log_table";
    return cmd.ExecuteReader();
};
using var lf = LazyFrame.ScanDatabase(readerFactory, batchSize: 50_000);
using var res = lf.Filter(Pl.Col("level") == "ERROR").Collect();
// Sink* / SinkTo / CollectBatchesAsync(engine: Engine.Streaming) spool a .NET source to a temporary IPC file in
// POLARS_TEMP_DIR and stream it (memory bounded by a batch); Collect() reads the whole source first.

// 3. Asynchronous batch ingestion from DbDataReader:
await foreach (DataFrame batch in dbReader.ReadDatabaseBatchesAsync(batchSize: 50_000))
{
    // Process batch
}
```

### Exporting & Writing to Database (`WriteTo`, `SinkTo`, `AsDataReader`)
To write Polars DataFrames into database tables (e.g. `SqlBulkCopy`), Polars provides zero-copy streaming:
```csharp
// Option A: WriteTo (Producer-Consumer DataReader stream)
df.WriteTo(reader =>
{
    using var bulkCopy = new SqlBulkCopy(connectionString);
    bulkCopy.DestinationTableName = "dbo.TargetTable";
    bulkCopy.WriteToServer(reader);
}, bufferSize: 5);

// Option B: AsDataReader (Obtain a live DbDataReader backed by bounded channel)
using DbDataReader dataReader = df.AsDataReader(bufferSize: 5);
using var bulkCopy2 = new SqlBulkCopy(connectionString);
bulkCopy2.DestinationTableName = "dbo.TargetTable";
bulkCopy2.WriteToServer(dataReader);
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
var pipeline = mlContext.Transforms.NormalizeMinMax("NormalizedFeatures", "Features")
    .Append(mlContext.Regression.Trainers.Sdca(labelColumnName: "Target", featureColumnName: "NormalizedFeatures"));
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

using var embSeries = df["embeddings"]; // An Array[f32, 128] column; AsTransposedTensorSpan requires an Array column

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
Stream materialized DataFrame chunks from a `LazyFrame` as the native Rust streaming engine computes them. Pass `Engine.Streaming`: the default `Engine.Auto` runs the query in memory first (polars 0.55 maps Auto to the in-memory engine):
```csharp
using var lf = LazyFrame.ScanParquet("huge_dataset/*.parquet");

// Asynchronously stream DataFrame batches:
await foreach (DataFrame batch in lf.CollectBatchesAsync(chunkSize: 50_000, engine: Engine.Streaming))
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

---

## 9. Off-Heap Writes: `NativeMemory` and `PipeWriter`

The `Write*Memory()` methods return a managed `byte[]`, which is a copy on the managed heap. For large payloads, or services that care about GC pauses, Polars.NET has two ways to serialize without that copy.

### `*NativeMemory`: Rust-Owned Buffers

Each method returns a `NativeBuffer` (`Polars.NET.Core`) that holds the serialized bytes in native memory, without touching the managed heap:

| Format | `DataFrame` | `LazyFrame` |
| :--- | :--- | :--- |
| Arrow IPC stream | `WriteIpcStreamNativeMemory` | None |
| Arrow IPC file | `WriteIpcNativeMemory` | `SinkIpcNativeMemory` |
| Parquet | `WriteParquetNativeMemory` | `SinkParquetNativeMemory` |
| CSV | `WriteCsvNativeMemory` | `SinkCsvNativeMemory` |
| JSON / NDJSON | `WriteJsonNativeMemory` (C# only) | `SinkJsonNativeMemory` (NDJSON) |
| Avro | `WriteAvroNativeMemory` | None |

`NativeBuffer` is a `MemoryManager<byte>`, so it is also an `IMemoryOwner<byte>`:

| Member | Description |
| :--- | :--- |
| `Length`, `IsDisposed` | Size in bytes, and whether the buffer was disposed |
| `GetSpan()`, `Memory` | Direct view of the native bytes |
| `Pin()` | A `MemoryHandle` for passing the pointer to native code |
| `AsStream()` | A read-only `Stream` over the bytes |
| `WriteTo(stream)`, `WriteToAsync(stream, token)` | Copy the bytes to a destination |
| `ToArray()` | Copy to a managed `byte[]` |

```csharp
using Polars.CSharp;
using Polars.NET.Core;

using var df = DataFrame.FromColumns(new { id = new[] { 1, 2, 3 }, name = new[] { "a", "b", "c" } });

using (NativeBuffer buffer = df.WriteIpcStreamNativeMemory())
{
    ReadOnlySpan<byte> bytes = buffer.GetSpan();
    Console.WriteLine($"{bytes.Length} bytes, none of them on the managed heap");

    await using var file = File.Create("out.arrows");
    await buffer.WriteToAsync(file);

    using var back = DataFrame.ReadIpcStream(buffer.GetSpan());
}

// LazyFrame: the query runs and its result is serialized in native memory.
using NativeBuffer parquet = df.Lazy().Filter(Polars.CSharp.Polars.Col("id") > 1).SinkParquetNativeMemory();
using var filtered = DataFrame.ReadParquet(parquet.AsStream());
```

```fsharp
open Polars.FSharp

let df = DataFrame.create [ pl.series "id" [ 1; 2; 3 ] ]

let writeToFile () =
    use buffer = df.WriteIpcStreamNativeMemory()   // Dispose is reached through `use`
    printfn "%d bytes" buffer.Length

    use file = System.IO.File.Create "out.arrows"
    buffer.WriteTo file

let parquetSize () =
    use parquet = df.Lazy().SinkParquetNativeMemory()
    parquet.Length
```

> [!WARNING] Lifetime
> A span or `Memory<byte>` from a `NativeBuffer` must not be used after `Dispose`. A pin from `Pin()` or a stream from `AsStream()` keeps the memory alive past `Dispose` until it is released itself. These methods are synchronous; there are no async variants. An IPC stream is read back with `DataFrame.ReadIpcStream`, which accepts a path or a span, and not with `ReadIpc`.

### Reading Bytes Back: Copy or Share

`ScanParquet` / `ScanIpc` / `ScanCsv` / `ScanNdjson` and `ReadParquet` / `ReadIpc` / `ReadCsv` / `ReadJson` / `ReadAvro` take:
* `byte[]`: copied (later changes to the array don't matter).
* `ReadOnlySpan<byte>`: copied once (C# only; a lazy scan cannot keep a span).
* `ReadOnlyMemory<byte>`: read in place, no copy. Pinned until the last frame reading it is released (clones and collected frames that point into it count). Don't change it meanwhile. Unmanaged memory goes in a `MemoryManager<byte>` and may be freed only after the engine calls `Unpin`, which can come from a background thread slightly after the frames are disposed. `NativeBuffer.Memory` is safe to pass, because its pins keep it alive past `Dispose`.

```csharp
using NativeBuffer parquet = df.Lazy().SinkParquetNativeMemory();
using var lf = LazyFrame.ScanParquet(parquet.Memory);   // zero-copy
```

### `PipeWriter`: Streaming Without Buffering the Payload

Every `Write*Async` on `DataFrame` and `Sink*Async` on `LazyFrame`, for Parquet, IPC, CSV, JSON and (on `DataFrame`) IPC stream and Avro, has an overload that takes a `System.IO.Pipelines.PipeWriter`. It streams each chunk to the writer as Rust produces it, and it honors the reader's backpressure, so the whole payload never sits in memory. This fits ASP.NET Core's `HttpResponse.BodyWriter` and `Socket` or Kestrel pipelines. Overloads that take a `Stream` also exist, in both sync and async forms.

```csharp
using System.IO.Pipelines;

var pipe = new Pipe();
using var collected = new MemoryStream();
Task reading = pipe.Reader.CopyToAsync(collected);

await df.WriteIpcStreamAsync(pipe.Writer);   // cancellationToken: is the last optional parameter

await pipe.Writer.CompleteAsync();           // The caller completes the writer
await reading;
```

```fsharp
open System.IO
open System.IO.Pipelines

let pipe = Pipe()
let collected = new MemoryStream()
let reading = pipe.Reader.CopyToAsync collected

df.WriteIpcStreamAsync(pipe.Writer).GetAwaiter().GetResult()
pipe.Writer.Complete()
reading.GetAwaiter().GetResult()
```

* The overloads flush the writer and never complete it, so you can append more data first. Completing it is the caller's job.
* There is no synchronous `PipeWriter` overload.
* `CancellationToken` cancels the write with `OperationCanceledException`, including during backpressure.
* If the query fails, the exception propagates and the writer is left open. If the reader stops early, the write fails with `IOException`.

---

## 10. Avro: Managed Reader and Writer

Avro runs on Seseragi.Files (managed, Avro 1.12), not the native library. Files stream block by block; codecs `Uncompressed`, `Deflate`, `Snappy`, `Zstandard`.

* `DataFrame.ReadAvro(path | byte[] | ReadOnlySpan<byte> | ReadOnlyMemory<byte> | Stream, nRows, columns, projection, parallel = true)` (F#: `?paralleling`). Columns in file order. A `Stream` is read block by block, not buffered, and left open.
* `LazyFrame.ScanAvro(path, ...)` streams each time the query runs; `LazyFrame.SinkAvro(path | Stream, compression, name)` writes the streaming result as batches arrive.
* Reading: `["null", T]` → nullable `T`; `[int, long]` → `Int64`; `[float, double]` → `Float64`; other unions → struct `member0..n`; map → `List(Struct{key, value})`; enum → `Categorical`; fixed → `Binary`; `duration` → `Struct{months: Int32, days: Int32, nanoseconds: Duration(ns)}`; decimals over 38 digits are refused; a non-record top-level schema → one column `value`; recursive records only when projected away.
* Writing: nullable → `["null", T]`; narrow ints → `int`, `UInt32`/`UInt64` → `long` (UInt64 above Int64.MaxValue refused); decimals → `bytes` decimal; Categorical/Enum → strings; zoned `Datetime` → `timestamp-*` plus the zone in `kaminari.timezone`; naive → `local-timestamp-*`; `Duration` → long micros with `spark.sql.catalyst.type` (sub-µs refused). Empty record name → `topLevelRecord`.

```csharp
df.WriteAvro("orders.avro", AvroCompression.Zstandard);
using var some = DataFrame.ReadAvro("orders.avro", columns: ["Id"]);
LazyFrame.ScanAvro("orders.avro").Filter(Col("Id") > 1).SinkAvro("copy.avro");
```

