
# Polars.NET 0.5.0 Release Note

![icon](assets/icon_lite.png)

## API

<p style="font-size:1.3em; font-weight:bold; background-color:#ffff99;">
<strong>ALMOST</strong> All API in python Polars now is available in both Polars.NET and Polars.FSharp.<br/>
The only module left is <strong>config module</strong> which is still under refactoring in rust core.<br/>
<strong>Config module will be introduced in next major release.</strong>
</p>

- GroupBy now should be executed by chained GroupByBuilder APIs(C#) or pl.groupBy pipes(F#).

C#:

```CSharp
var res = df
    .GroupByDynamic(
        indexColumn: "Time",
        every: "1h", 
        groupBy: [Pl.Col("Symbol")]
    )
    .Having(Pl.Col("Val").Mean() > 50) 
    .Agg(
        Pl.Col("Val").Mean().Alias("MeanVal"),
        Pl.Col("Val").First().Alias("FirstVal"),
        Pl.Col("Val").Last().Alias("LastVal")
    );
```

F#:

```FSharp
let res =
    df
    |> pl.groupBy 
        [ pl.col("birthdate").Dt.Year() / pl.lit 10 * pl.lit 10 
        |> pl.alias "decade" ]
    |> pl.having (pl.len() .> pl.lit 1)
    |> pl.agg [ pl.len() |> pl.alias "cnt" ]
    |> pl.sortAscending [pl.col "decade"] 
```

## New Features

- TensorInterop
    AsReadOnlySpan– Zero-copy view as a contiguous Span for flat numeric columns.

    AsTensorSpan – Automatically shaped 1D/2D/N-D tensor views (supports explicit reshaping or transposition via strides).

    AsTensor – Deep-copies data into a managed Tensor on the GC heap, safe for async/threading.

    AsDangerousUnmanagedTensor – Extracts raw pointer (nint, shape) for direct FFI with C++ libraries (ONNX Runtime, TorchSharp, MKL, CUDA). Requires Rechunk() and manual lifecycle management.

    Series.FromTensor – Converts any ReadOnlyTensorSpan back into a Polars Series, supporting nested array columns for N-D data.

All operations work zero-copy where possible, eliminating serialization overhead when moving data between Polars and high-performance .NET code or native ML backends.

Only numeric, null‑free, contiguous series are supported – strings and categoricals must be encoded first.
TensorInterop:

```C#
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

- ML.NET integration

ToDataFrame(this IDataView, batchSize = 64000)
Converts any ML.NET IDataView into a Polars DataFrame. Data flows through Arrow record batches without unnecessary serialization or type conversions.

AsDataView(this IPolarsDataFrame, enableMacroShuffle = false)
Wraps a Polars DataFrame as an ML.NET IDataView. The underlying Arrow memory is shared directly, enabling ML.NET trainers (e.g., SgdCalibrated, FastTree) to consume Polars data.

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
```

- SQL MERGE

Declarative actions – WhenMatchedUpdate(...), WhenMatchedDelete(...), WhenNotMatchedInsert(...), WhenNotMatchedBySourceDelete(...)

Automatic validation – checks for key type mismatches, nulls in join keys, and duplicate source keys before execution.

Execution plan inspection – call .Explain() or .InspectPlan() to see the logical/physical Polars plan, or .ToMergePlanString() for a human‑readable summary of the MERGE strategy.

Zero‑copy core – the entire operation is compiled into a Polars logical plan and executed by the native engine; no unnecessary copying of data.

Rich overloads – specify join keys by column names, a selector (Cs.Numeric()), or any column expression.

C# Example:

```C#
var targetDf = DataFrame.FromColumns(new
{
    Id = new[] { 1, 2, 3, 4, 5 },
    Category = new[] { "Seasonal", "Core", "Core", "Tech", "Tech" },
    Price = new[] { 10.0, 20.0, 30.0, 100.0, 200.0 },
    IsDiscontinued = new[] { false, false, false, false, false }
});

var sourceDf = DataFrame.FromColumns(new
{
    Id = new[] { 3, 4, 5, 6, 7 },
    Category = new[] { "Core", "Tech", "Tech", "New", "Trash" },
    Price = new[] { 30.0, 120.0, 190.0, 50.0, 0.0 },
    IsDiscontinued = new[] { true, false, false, false, false } 
});

var resultDf = targetDf.Merge(sourceDf, "Id")
    // If matched as IsDiscontinued delete
    .WhenMatchedDelete(m => m.Source("IsDiscontinued") == true)
    // If matched price up then update
    .WhenMatchedUpdate(m => m.Source("Price") > m.Target("Price"))
    // Only insert price > 0 in source table
    .WhenNotMatchedInsert(m => m.Source("Price") > 0.0)
    // If record in source table is missing then delete target
    .WhenNotMatchedBySourceDelete(m => m.Target("Category") == "Seasonal")
    // Inspect plan to console
    .InspectPlan(verbose: false) 
    .Execute(); 

// Id 1 (Target): Seasonal item missed in source table -> NotMatchedBySourceDelete -> Delete
// Id 2 (Target): Core item missed in source table -> Keep
// Id 3 (Both): IsDiscontinued=true -> MatchedDelete -> Delete
// Id 4 (Both): Source Price(120) > Target Price(100) -> WhenMatchedUpdate -> Price: 120.0
// Id 5 (Both): Source Price(190) < Target Price(200) -> Keep
// Id 6 (Source): New Item in source and price(50) > 0 -> WhenNotMatchedInsert -> Insert
// Id 7 (Source): New Item in source but price(0) is invalid -> Discard
```

F# Example:

```F#
[
    pl.series "Id"    [1; 2; 3]
    pl.series "Value" ["A"; "B"; "C"]
] 
|> pl.dataframe |> pl.asLazy
|> Merge.initiate 
    (
        [
            pl.series "Id"    [2; 3; 4]
            pl.series "Value" ["B_new"; "C_new"; "D"]
        ]
        |> pl.dataframe |> pl.asLazy
    ) 
    ["Id"]
|> Merge.whenMatchedUpdateSet (Set.build [
    Set.col "Value" (fun ctx -> ctx.SourceCol "Value")
])
|> Merge.whenNotMatchedInsertAll
|> Merge.printPlan
|> Merge.execute
|> pl.sortAscendingLazy [pl.col "Id"]
|> pl.collect

// Merge Plan
// MERGE ON: Id

// MATCH STRATEGY:
//   First Match Wins (Sequential Evaluation)

// WHEN MATCHED:
//   [1] UPDATE
//       SET (1 overrides):
//         - Value = col("Value.Source")

// WHEN NOT MATCHED:
//   [1] INSERT
//       SET: (All Source Columns)

// JOIN STRATEGY:
//   Type: Outer (Upgraded to Outer to support INSERT)
//   MaintainOrder: Left

// >>> MERGE Execution Result
// shape: (4, 2)
// ┌─────┬───────┐
// │ Id  ┆ Value │
// │ --- ┆ ---   │
// │ i32 ┆ str   │
// ╞═════╪═══════╡
// │ 1   ┆ A     │
// │ 2   ┆ B_new │
// │ 3   ┆ C_new │
// │ 4   ┆ D     │
// └─────┴───────┘
```

## BugFix

- Removed hard-coded dataframe html background color (19)

## Docs

- Examples in /example are updated.

## Others

- Since .NET Interactive and Polyglot Notebook are EOL, related code in Polars.NET won't be removed, but there might not be any updates about such features.
